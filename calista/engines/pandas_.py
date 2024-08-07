# Copyright 2024 Aubay.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pandas.core.groupby import DataFrameGroupBy

import calista.core._aggregate_conditions as aggregateCond
import calista.core._conditions as cond
import calista.core.rules as R
from calista.core.aggregates import AggregateDataset
from calista.core.catalogue import PythonTypes
from calista.core.engine import LazyEngine
from calista.core.metrics import Metrics
from calista.core.types_alias import ColumnName, PythonType


class Pandas_Engine(LazyEngine):

    mapping_operator: dict[str, str] = {
        "=": "__eq__",
        "!=": "__ne__",
        ">": "__gt__",
        ">=": "__ge__",
        "<": "__lt__",
        "<=": "__le__",
    }

    def __init__(self, config: Dict[str, Any]):
        self.dataset = None
        self._config = config

    @property
    def __name__(self):
        return f"{super().__name__}_"

    def _read_csv(self, path: str, options: Dict[str, Any] = None):
        path = Path(path)
        if path.is_dir():
            dfs = []
            file_list = list(path.glob("*.csv"))
            for file in file_list:
                df = pd.read_csv(file, **(options or {}))
                dfs.append(df)

            df_final = pd.concat(dfs, axis=0, ignore_index=True)
            df_final.reset_index(drop=True, inplace=True)
            return df_final

        elif path.is_file():
            if path.stat().st_size > 0:
                return pd.read_csv(path, **(options or {}))

            else:
                raise ValueError("Reading empty csv file")
        else:
            raise ValueError(f"Invalid Path {path}")

    def _read_json(self, path, options: Dict[str, Any] = None):

        path = Path(path)

        if path.is_dir():
            file_list = list(path.glob("*.json"))
            for index, file in enumerate(file_list):
                if index == 0:
                    df = pd.read_json(file, lines=True)
                else:
                    df = pd.concat([df, pd.read_json(file, lines=True)], axis=0)
            sorted_columns = sorted(df.columns)
            df = df[sorted_columns]
            df.reset_index(drop=True, inplace=True)
            return df

        elif path.is_file():
            if path.stat().st_size > 0:
                try:
                    return pd.read_json(path)
                except ValueError:
                    return pd.read_json(path, lines=True)

            else:
                raise ValueError("Reading empty json file")
        else:
            raise ValueError("Invalid Path")

    def _load_from_dataframe(self, dataframe: pd.DataFrame) -> None:
        if not isinstance(dataframe, pd.DataFrame):
            raise ValueError(f"Argument must be a valid {pd.DataFrame} object.")
        self.dataset = dataframe

    def _load_from_dict(self, data: Dict[str, List]) -> None:
        self.dataset = pd.DataFrame(data)

    def _load_from_path(
        self, path: str, file_format: str, options: Dict[str, Any] = None
    ) -> None:
        lowered_file_format = file_format.lower()
        if file_format == "parquet":
            self.dataset = pd.read_parquet(path)
        elif file_format == "csv":
            self.dataset = self._read_csv(path, options=options)
        elif file_format == "json":
            self.dataset = self._read_json(path, options=options)
        else:
            raise ValueError(f"I don't know how to read {lowered_file_format} yet.")

    def where(self, expression: Series) -> DataFrame:
        return self.dataset[expression]

    def filter(self, expression: Series) -> DataFrame:
        return self.where(expression)

    def show(self, n: int = 10) -> None:
        print(self.dataset.head(n))

    def and_condition(self, left_cond: Series, right_cond: Series) -> Series:
        return left_cond & right_cond

    def or_condition(self, left_cond: Series, right_cond: Series) -> Series:
        return left_cond | right_cond

    def not_condition(self, cond: Series) -> Series:
        return ~cond

    def add_column(self, col_name, col) -> DataFrame:
        new_dataset = self.dataset.copy()
        new_dataset[col_name] = col
        return new_dataset

    def get_schema(self) -> dict[ColumnName:str, PythonType:str]:
        mapping_type = {
            "int64": PythonTypes.INTEGER,
            "object": PythonTypes.STRING,
            "float64": PythonTypes.FLOAT,
            "bool": PythonTypes.BOOLEAN,
            "datetime64": PythonTypes.DATE,
        }

        return self.dataset.dtypes.apply(
            lambda x: mapping_type.get(x.name, PythonTypes.STRING)
        ).to_dict()

    def count_records(self) -> int:
        return len(self.dataset)

    def execute_conditions(self, conditions: dict[str, Series]) -> list[Metrics]:
        total_count = len(self.dataset.index)
        valid_counts_with_rule_name = {
            rule_name: condition.sum() for rule_name, condition in conditions.items()
        }

        metrics_timestamp = str(datetime.now())
        return [
            Metrics(
                rule=rule_name,
                total_row_count=total_count,
                valid_row_count=round(valid_count, 2),
                valid_row_count_pct=round((valid_count / total_count) * 100, 2),
                timestamp=metrics_timestamp,
            )
            for rule_name, valid_count in valid_counts_with_rule_name.items()
        ]

    def is_null(self, condition: cond.IsNull) -> Series:
        return self.dataset[condition.col_name].isna()

    def is_not_null(self, condition: cond.IsNotNull) -> Series:
        return self.dataset[condition.col_name].notna()

    def is_in(self, condition: cond.IsIn) -> Series:
        return self.dataset[condition.col_name].isin(condition.list_of_values)

    def rlike(self, condition: cond.Rlike) -> Series:
        return (
            self.dataset[condition.col_name]
            .astype("string")
            .str.contains(condition.pattern, na=False)
        )

    def compare_year_to_value(self, condition: cond.CompareYearToValue) -> Series:
        operator = self.mapping_operator.get(condition.operator, None)
        return self.dataset[condition.col_name].dt.year.__getattribute__(operator)(
            condition.value
        )

    def is_iban(self, condition: cond.IsIban) -> Series:
        alphabet_conversion = {chr(i + 65): str(i + 10) for i in range(26)}
        cleaned_col_str = self.dataset[condition.col_name].astype(str)
        cleaned_str_col = cleaned_col_str.str.replace("[^a-zA-Z0-9]", "", regex=True)
        cleaned_str_col = cleaned_str_col.str.upper()
        cleaned_col = cleaned_str_col.str[4:34] + cleaned_str_col.str[0:4]
        for letter, value in alphabet_conversion.items():
            cleaned_col = cleaned_col.str.replace(letter, value)
        cleaned_col = (
            (
                (
                    (cleaned_col.str[0:16].astype(np.int64) % 97).astype(str)
                    + cleaned_col.str[16:30]
                ).astype(np.int64)
                % 97
            ).astype(str)
            + cleaned_col.str[30:40]
        ).astype(np.int64) % 97
        return cleaned_col == 1

    def is_ip_address(self, condition: cond.IsIpAddress) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        ipv6_regex = r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
        ipv4_regex = r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"
        trimmed_ip_col = col.str.strip()
        return trimmed_ip_col.str.contains(
            ipv6_regex, regex=True
        ) | trimmed_ip_col.str.contains(ipv4_regex, regex=True)

    def count_occurences(self, rule: R.CountOccurences) -> dict[Any, int]:
        val_count_pd = self.dataset.groupby(rule.col_name).size()
        return dict(val_count_pd)

    def compute_percentile(self, rule: R.ComputePercentile) -> float:
        return float(
            self.dataset[rule.col_name].quantile(rule.percentile, interpolation="lower")
        )

    def get_col_values_superior_to_constant(
        self, rule: R.GetColValuesSuperiorToConstant
    ) -> list[float]:
        return (
            self.dataset.loc[self.dataset[rule.col_name] > rule.constant][rule.col_name]
            .astype(float)
            .tolist()
        )

    def get_col_values_inferior_to_constant(
        self, rule: R.GetColValuesInferiorToConstant
    ) -> list[float]:
        return (
            self.dataset.loc[self.dataset[rule.col_name] < rule.constant][rule.col_name]
            .astype(float)
            .tolist()
        )

    def is_float(self, condition: cond.IsFloat) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        return pd.to_numeric(col, errors="coerce").notnull() & col.str.contains(
            r"^[-+]?[0-9]*\.[0-9]*[1-9]+[0]*$", regex=True
        )

    def is_date(self, condition: cond.IsDate) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        date_pattern = r"(\d{4}-\d{2}-\d{2})|(\d{2}-\d{2}-\d{4})|(\d{2}\/\d{2}\/\d{4})|(\d{4}\/\d{2}\/\d{2})"
        return col.str.contains(date_pattern, regex=True)

    def is_phone_number(self, condition: cond.IsPhoneNumber) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        country_regex = {
            "fr": r"^(\+?33\s?|0)(\(0\)\s?)?(\d\s?){9}$",
            "be": r"^\+32[1-9][0-9]{7,8}$",
            "es": r"^\+34[6-9][0-9]{8}$",
            "pt": r"^\+351[1-9][0-9]{8}$",
            "gb": r"^\+44[1-9][0-9]{9,10}$",
            "it": r"^\+39[0-9]{6,12}$",
            "lu": r"^\+352[0-9]{3,11}$",
        }
        regex = (
            "|".join(f"({regex})" for regex in country_regex.values())
            if condition.filter_per_country is None
            else "|".join(
                f"({country_regex[country]})"
                for country in condition.filter_per_country
            )
        )
        return col.str.replace(r"[-\s]", "", regex=True).str.match(regex)

    def is_boolean(self, condition: cond.IsBoolean) -> Series:
        data_type = str(self.dataset[condition.col_name].dtype)
        map_boolean_type = {
            "int64": [0, 1],
            "float64": [0.0, 1.0],
            "object": ["0", "1", "true", "false"],
            "bool": [True, False],
        }
        boolean_type = map_boolean_type.get(data_type, [])
        if data_type == "object":
            col = self.dataset[condition.col_name].astype(str)
            return col.str.lower().isin(boolean_type)
        else:
            return self.dataset[condition.col_name].isin(boolean_type)

    def is_email(self, condition: cond.IsEmail) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        return col.str.match(r"^[\w\.-]+@[A-z\d\.-]+\.[A-z]{2,}$")

    def is_integer(self, condition: cond.IsInteger) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        return pd.to_numeric(col, errors="coerce").notnull() & (
            col.str.contains(r"^[-+]?[0-9]+(?:\.0)?$", regex=True)
        )

    def is_unique(self, condition: cond.IsUnique) -> Series:
        value_counts = self.dataset[condition.col_name].value_counts()
        return self.dataset[condition.col_name].map(value_counts) == 1

    def compare_column_to_value(self, condition: cond.CompareColumnToValue) -> Series:
        operator = self.mapping_operator.get(condition.operator, None)
        return (
            self.dataset[condition.col_name].__getattribute__(operator)(condition.value)
            & self.dataset[condition.col_name].notna()
        )

    def compare_column_to_column(self, condition: cond.CompareColumnToColumn) -> Series:
        operator = self.mapping_operator.get(condition.operator, None)
        return self.dataset[condition.col_left].__getattribute__(operator)(
            self.dataset[condition.col_right]
        )

    def count_decimal_digit(self, condition: cond.CountDecimalDigit) -> Series:
        operator = self.mapping_operator.get(condition.operator, None)
        return (
            self.dataset[condition.col_name]
            .astype(str)
            .str.split(".", expand=True)[1]
            .str.len()
            .__getattribute__(operator)(condition.digit)
        )

    def count_integer_digit(self, condition: cond.CountIntegerDigit) -> Series:
        operator = self.mapping_operator.get(condition.operator, None)
        return (
            self.dataset[condition.col_name]
            .astype(str)
            .str.split(".", expand=True)[0]
            .str.len()
            .__getattribute__(operator)(condition.digit)
            & self.dataset[condition.col_name].notna()
        )

    def is_between(self, condition: cond.IsBetween) -> Series:
        return self.dataset[condition.col_name].between(
            condition.min_value, condition.max_value
        )

    def compare_length(self, condition: cond.CompareLength) -> Series:
        operator = self.mapping_operator.get(condition.operator, None)
        return (
            self.dataset[condition.col_name]
            .str.len()
            .__getattribute__(operator)(condition.length)
        )

    def is_alphabetic(self, condition: cond.IsAlphabetic) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        return col.str.match(r"^[A-zÀ-ú]+$") & self.dataset[condition.col_name].notna()

    def is_negative(self, condition: cond.IsNegative) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        return col.str.match(r"^-\d*\.?\d+$") & self.dataset[condition.col_name].notna()

    def is_positive(self, condition: cond.IsPositive) -> Series:
        col = self.dataset[condition.col_name].astype(str)
        return (
            col.str.match(r"^[+]?[0-9]\d*(\.\d+)?$")
            & self.dataset[condition.col_name].notna()
        )

    def aggregate_dataset(
        self, keys: list[str], agg_cols_expr: list[tuple[str, tuple[str, str]]]
    ) -> DataFrameGroupBy:
        new_agg_cols_expr = {}
        for expr in agg_cols_expr:
            new_agg_cols_expr.update(expr)
        return self.dataset.groupby(*keys).agg(**new_agg_cols_expr)


class Pandas_AggregateDataset(AggregateDataset):
    @staticmethod
    def sum(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: Pandas_Engine,
    ) -> dict[ColumnName:str, Series]:
        return {agg_col_name: (agg_func.col_name, "sum")}

    @staticmethod
    def count(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: Pandas_Engine,
    ) -> dict[ColumnName:str, Series]:
        return {agg_col_name: (agg_func.col_name, "count")}

    @staticmethod
    def mean(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: Pandas_Engine,
    ) -> dict[ColumnName:str, Series]:
        return {agg_col_name: (agg_func.col_name, "mean")}

    @staticmethod
    def min(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: Pandas_Engine,
    ) -> dict[ColumnName:str, Series]:
        return {agg_col_name: (agg_func.col_name, "min")}

    @staticmethod
    def max(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: Pandas_Engine,
    ) -> dict[ColumnName:str, Series]:
        return {agg_col_name: (agg_func.col_name, "max")}

    @staticmethod
    def median(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: Pandas_Engine,
    ) -> dict[ColumnName:str, Series]:
        return {agg_col_name: (agg_func.col_name, "median")}
