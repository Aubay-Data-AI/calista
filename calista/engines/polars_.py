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
from typing import Any, Dict, List

import polars as pl
import pyarrow.dataset as ds
from polars import Expr, LazyFrame
from polars.lazyframe.group_by import LazyGroupBy

import calista.core._aggregate_conditions as aggregateCond
import calista.core._conditions as cond
import calista.core.rules as R
from calista.core.aggregates import AggregateDataset
from calista.core.catalogue import PythonTypes
from calista.core.engine import LazyEngine
from calista.core.metrics import Metrics
from calista.core.types_alias import ColumnName, PythonType


class Polars_Engine(LazyEngine):

    mapping_operator: dict[str, str] = {
        "=": "__eq__",
        "!=": "__ne__",
        ">": "__gt__",
        ">=": "__ge__",
        "<": "__lt__",
        "<=": "__le__",
    }

    def __init__(self, config: Dict[str, Any] = None):
        self.dataset = None
        self._config = config

    @property
    def __name__(self):
        return f"{super().__name__}_"

    def _load_from_dataframe(self, dataframe: pl.DataFrame) -> None:
        if not isinstance(dataframe, pl.DataFrame):
            raise ValueError(f"Argument must be a valid {pl.DataFrame} object.")
        self.dataset = dataframe.lazy()

    def _load_from_dict(self, data: Dict[str, List]) -> None:
        self.dataset = pl.LazyFrame(data)

    def _load_from_path(
        self, path: str, file_format: str, options: Dict[str, Any] = None
    ) -> None:
        lowered_file_format = file_format.lower()
        if file_format == "parquet":
            dset = ds.dataset(path)
            self.dataset = pl.scan_pyarrow_dataset(dset)
        elif file_format == "csv":
            self.dataset = pl.scan_csv(path)
        elif file_format == "json":
            self.dataset = pl.scan_ndjson(path)
        else:
            raise ValueError(f"I don't know how to read {lowered_file_format} yet.")

    def where(self, expression: Expr) -> LazyFrame:
        return self.dataset.filter(expression)

    def filter(self, expression: Expr) -> LazyFrame:
        return self.where(expression)

    def show(self, n: int = 10) -> None:
        print(self.dataset.head(n).collect())

    def and_condition(self, left_cond: Expr, right_cond: Expr) -> Expr:
        return left_cond.and_(right_cond)

    def or_condition(self, left_cond: Expr, right_cond: Expr) -> Expr:
        return left_cond.or_(right_cond)

    def not_condition(self, cond: Expr) -> Expr:
        return cond.not_()

    def add_columns(self, columns: dict[str, Expr]) -> pl.LazyFrame:
        new_dataset = self.dataset
        for col_name, col in columns.items():
            new_dataset = new_dataset.with_columns(col.alias(col_name))
        return new_dataset

    def execute_conditions(self, conditions: dict[str, Expr]) -> list[Metrics]:
        total_count = self.dataset.select(pl.len()).collect().item()
        valid_counts_with_rule_name = {
            rule_name: self.dataset.select(condition.sum()).collect().item()
            for rule_name, condition in conditions.items()
        }

        metrics_timestamp = str(datetime.now())
        return [
            Metrics(
                rule=rule_name,
                total_row_count=total_count,
                valid_row_count=valid_count,
                valid_row_count_pct=(valid_count * 100) / total_count,
                timestamp=metrics_timestamp,
            )
            for rule_name, valid_count in valid_counts_with_rule_name.items()
        ]

    def get_schema(self) -> dict[ColumnName:str, PythonType:str]:
        """return a dict with col names as key and python types as values"""
        mapping_type = {
            "Decimal": PythonTypes.DECIMAL,
            "Float32": PythonTypes.FLOAT,
            "Float64": PythonTypes.FLOAT,
            "Int8": PythonTypes.INTEGER,
            "Int32": PythonTypes.INTEGER,
            "Int64": PythonTypes.INTEGER,
            "UInt8": PythonTypes.INTEGER,
            "UInt16": PythonTypes.INTEGER,
            "UInt32": PythonTypes.INTEGER,
            "UInt64": PythonTypes.INTEGER,
            "Date": PythonTypes.DATE,
            "Datetime": PythonTypes.DATE,
            "Duration": PythonTypes.DATE,
            "Time": PythonTypes.DATE,
            "String": PythonTypes.STRING,
            "Categorical": PythonTypes.STRING,
            "Enum": PythonTypes.STRING,
            "Utf8": PythonTypes.STRING,
            "Binary": PythonTypes.STRING,
            "Object": PythonTypes.STRING,
            "Unknown": PythonTypes.STRING,
            "Boolean": PythonTypes.BOOLEAN,
        }
        return {
            col_info[0]: mapping_type.get(f"{col_info[1]}", PythonTypes.STRING)
            for col_info in list(dict(self.dataset.schema).items())
        }

    def count_records(self) -> int:
        return self.dataset.select(pl.len()).collect().item()

    def is_null(self, condition: cond.IsNull) -> Expr:
        return pl.col(condition.col_name).is_null()

    def is_not_null(self, condition: cond.IsNull) -> Expr:
        return pl.col(condition.col_name).is_not_null()

    def is_in(self, condition: cond.IsIn) -> Expr:
        return pl.col(condition.col_name).is_in(condition.list_of_values)

    def rlike(self, condition: cond.Rlike) -> Expr:
        return pl.col(condition.col_name).str.contains(condition.pattern)

    def compare_year_to_value(self, condition: cond.CompareYearToValue) -> Expr:
        operator = self.mapping_operator.get(condition.operator, None)
        return (
            pl.col(condition.col_name)
            .dt.year()
            .__getattribute__(operator)(pl.lit(condition.value))
        )

    def is_iban(self, condition: cond.IsIban) -> Expr:
        alphabet_conversion = {chr(i + 65): str(i + 10) for i in range(26)}
        cast_col = pl.col(condition.col_name).cast(pl.String)
        cleaned_str_col = cast_col.str.replace_all(
            "[^a-zA-Z0-9]", ""
        ).str.to_uppercase()

        cleaned_col = cleaned_str_col.str.slice(4, 34) + cleaned_str_col.str.slice(0, 4)
        for letter, value in alphabet_conversion.items():
            cleaned_col = cleaned_col.str.replace_all(letter, value)

        first_16_digit_mod_97 = (
            cleaned_col.str.slice(0, 16).cast(pl.Int64).mod(97).cast(pl.String)
        )
        other_digit = cleaned_col.str.slice(16, 16)
        update_cleaned_col = first_16_digit_mod_97 + other_digit
        is_iban_col = update_cleaned_col.cast(pl.Int64) % 97

        return is_iban_col == 1

    def is_ip_address(self, condition: cond.IsIpAddress) -> Expr:
        ipv6_regex = r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
        ipv4_regex = r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"
        trimmed_ip_col = pl.col(condition.col_name).str.strip_chars()
        return trimmed_ip_col.str.contains(ipv6_regex) | trimmed_ip_col.str.contains(
            ipv4_regex
        )

    def count_occurences(self, rule: R.CountOccurences) -> dict[Any, int]:
        val_count = self.dataset.group_by(rule.col_name).len().collect()
        keys = val_count.to_dict(as_series=False)[rule.col_name]
        values = val_count.to_dict(as_series=False)["len"]
        return dict(zip(keys, values))

    def compute_percentile(self, rule: R.ComputePercentile) -> float:
        quantile_value = self.dataset.select(
            pl.col(rule.col_name).quantile(rule.percentile, interpolation="lower")
        )
        return quantile_value.collect().item()

    def get_col_values_superior_to_constant(
        self, rule: R.GetColValuesSuperiorToConstant
    ) -> list[float]:
        return (
            self.dataset.filter(pl.col(rule.col_name) > pl.lit(rule.constant))
            .select(pl.col(rule.col_name))
            .unique()
            .collect()
            .to_series()
            .to_list()
        )

    def get_col_values_inferior_to_constant(
        self, rule: R.GetColValuesInferiorToConstant
    ) -> list[float]:
        return (
            self.dataset.filter(pl.col(rule.col_name) < pl.lit(rule.constant))
            .select(pl.col(rule.col_name))
            .unique()
            .collect()
            .to_series()
            .to_list()
        )

    def is_float(self, condition: cond.IsFloat) -> Expr:
        return pl.col(condition.col_name).str.contains(
            r"^[-+]?[0-9]*\.[0-9]*[1-9]+[0]*$"
        )

    def is_date(self, condition: cond.IsDate) -> Expr:
        date_regex_patterns = (
            r"\b(\d{4}-\d{2}-\d{2}|"  # yyyy-mm-dd or yyyy-dd-mm
            r"\d{2}-\d{2}-\d{4}|"  # dd-mm-yyyy or mm-dd-yyyy
            r"\d{2}/\d{2}/\d{4}|"  # dd/mm/yyyy or mm/dd/yyyy
            r"\d{2}/\d{1}/\d{4}|"  # dd/m/yyyy or m/dd/yyyy
            r"\d{4}/\d{2}/\d{2})\b"  # yyyy/mm/dd or yyyy/dd/mm
        )
        return pl.col(condition.col_name).str.contains(date_regex_patterns)

    def is_phone_number(self, condition: cond.IsPhoneNumber) -> Expr:
        country_regex = {
            "fr": "^(\+?33\s?|0)(\(0\)\s?)?(\d\s?){9}$",
            "be": "^\+32[1-9][0-9]{7,8}$",
            "es": "^\+34[6-9][0-9]{8}$",
            "pt": "^\+351[1-9][0-9]{8}$",
            "gb": "^\+44[1-9][0-9]{9,10}$",
            "it": "^\+39[0-9]{6,12}$",
            "lu": "^\+352[0-9]{3,11}$",
        }
        regex = (
            "|".join(f"({regex})" for regex in country_regex.values())
            if condition.filter_per_country is None
            else "|".join(
                f"({country_regex[country]})"
                for country in condition.filter_per_country
            )
        )
        return (
            pl.col(condition.col_name).str.replace_all("[-\s]", "").str.contains(regex)
        )

    def is_boolean(self, condition: cond.IsBoolean) -> Expr:
        col_as_string = pl.col(condition.col_name).cast(pl.String)
        boolean_string = ["0", "1", "true", "false", "True", "False"]
        return col_as_string.is_in(pl.Series(boolean_string))

    def is_integer(self, condition: cond.IsInteger) -> Expr:
        return pl.col(condition.col_name).str.contains("^[-+]?[0-9]*$")

    def is_email(self, condition: cond.IsEmail) -> Expr:
        return pl.col(condition.col_name).str.contains(
            "^[\w\.-]+@[A-z\d\.-]+\.[A-z]{2,}$"
        )

    def is_unique(self, condition: cond.IsUnique) -> Expr:
        return pl.col(condition.col_name).count().over(condition.col_name) == 1

    def compare_column_to_value(self, condition: cond.CompareColumnToValue) -> Expr:
        operator = self.mapping_operator.get(condition.operator, None)
        return pl.col(condition.col_name).__getattribute__(operator)(
            pl.lit(condition.value)
        )

    def compare_column_to_column(self, condition: cond.CompareColumnToColumn) -> Expr:
        operator = self.mapping_operator.get(condition.operator, None)
        return pl.col(condition.col_left).__getattribute__(operator)(
            pl.col(condition.col_right)
        )

    def count_decimal_digit(self, condition: cond.CountDecimalDigit) -> Expr:
        operator = self.mapping_operator.get(condition.operator, None)
        polars_cast = pl.col(condition.col_name).cast(pl.String)
        polars_decimal = polars_cast.str.split(by=".").list.get(1)
        polars_length = polars_decimal.str.len_chars()
        return polars_length.__getattribute__(operator)(condition.digit)

    def count_integer_digit(self, condition: cond.CountIntegerDigit) -> Expr:
        operator = self.mapping_operator.get(condition.operator, None)
        polars_cast = pl.col(condition.col_name).cast(pl.String)
        polars_integer = polars_cast.str.split(by=".").list.get(0)
        polars_length = polars_integer.str.len_chars()
        return polars_length.__getattribute__(operator)(condition.digit)

    def is_between(self, condition: cond.IsBetween) -> Expr:
        return pl.col(condition.col_name).is_between(
            lower_bound=condition.min_value, upper_bound=condition.max_value
        )

    def compare_length(self, condition: cond.CompareLength) -> Expr:
        operator = self.mapping_operator.get(condition.operator, None)
        return (
            pl.col(condition.col_name)
            .cast(pl.String)
            .str.len_chars()
            .__getattribute__(operator)(condition.length)
        )

    def is_alphabetic(self, condition: cond.IsAlphabetic) -> Expr:
        return pl.col(condition.col_name).str.contains(r"^[A-zÀ-ú]+$")

    def is_negative(self, condition: cond.IsNegative) -> Expr:
        return pl.col(condition.col_name).str.contains(r"^-\d*\.?\d+$")

    def is_positive(self, condition: cond.IsPositive) -> Expr:
        return pl.col(condition.col_name).str.contains(r"^[+]?[0-9]\d*(\.\d+)?$")

    def aggregate_dataset(
        self, keys: list[str], agg_cols_expr: list[Expr]
    ) -> LazyGroupBy:
        return self.dataset.group_by(*keys).agg(*agg_cols_expr)


class Polars_AggregateDataset(AggregateDataset):
    @staticmethod
    def sum(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: Polars_Engine,
    ) -> Expr:
        return pl.sum(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def count(
        agg_func: aggregateCond.CountBy,
        agg_col_name: str,
        keys: List[str],
        engine: Polars_Engine,
    ) -> Expr:
        return pl.count(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def mean(
        agg_func: aggregateCond.MeanBy,
        agg_col_name: str,
        keys: List[str],
        engine: Polars_Engine,
    ) -> Expr:
        return pl.mean(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def min(
        agg_func: aggregateCond.MinBy,
        agg_col_name: str,
        keys: List[str],
        engine: Polars_Engine,
    ) -> Expr:
        return pl.min(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def max(
        agg_func: aggregateCond.MaxBy,
        agg_col_name: str,
        keys: List[str],
        engine: Polars_Engine,
    ) -> Expr:
        return pl.max(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def median(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: Polars_Engine,
    ) -> Expr:
        return pl.median(agg_func.col_name).alias(agg_col_name)
