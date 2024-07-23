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


import re
from datetime import datetime
from typing import Any, Dict, List

import snowflake.snowpark.column as C
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark import Column, DataFrame, RelationalGroupedDataFrame, Session
from snowflake.snowpark.window import Window

import calista.core._aggregate_conditions as aggregateCond
import calista.core._conditions as cond
import calista.core.rules as R
from calista.core.aggregates import AggregateDataset
from calista.core.catalogue import PythonTypes
from calista.core.database import Database
from calista.core.metrics import Metrics
from calista.core.types_alias import ColumnName, PythonType


def _is_not_null(e: C.ColumnOrName) -> Column:
    c = C._to_col_if_str(e, "is_not_null")
    return c.is_not_null()


class SnowflakeEngine(Database):

    mapping_operator: dict[str, str] = {
        "=": "__eq__",
        "!=": "__ne__",
        ">": "__gt__",
        ">=": "__ge__",
        "<": "__lt__",
        "<=": "__le__",
    }

    def __init__(self, config: Dict[str, Any] = None):
        try:  # TODO à ameliorer
            credentials = config.get("credentials")
            self.snowflake = Session.builder.configs(credentials).create()
        except Exception as e:
            print(f"Error establishing connection: {e}")
        self.dataset = None
        self._config = config

    def _load_from_database(self, table: str, schema: str, database: str) -> None:
        self.snowflake.sql(f"USE {database}").collect()
        self.dataset = self.snowflake.table(f"{schema}.{table}")

    def where(self, expression: Column) -> DataFrame:
        return self.dataset.filter(expression)

    def filter(self, expression: Column) -> DataFrame:
        return self.where(expression)

    def show(self, n: int = 10) -> None:
        self.dataset.show(n)

    def and_condition(self, left_cond: Column, right_cond: Column) -> Column:
        return left_cond & right_cond

    def or_condition(self, left_cond: Column, right_cond: Column) -> Column:
        return left_cond | right_cond

    def not_condition(self, cond: Column) -> Column:
        return ~cond

    def get_schema(self) -> dict[ColumnName:str, PythonType:str]:
        mapping_type = {
            "ByteType": PythonTypes.INTEGER,
            "LongType": PythonTypes.INTEGER,
            "IntegerType": PythonTypes.INTEGER,
            "ShortType": PythonTypes.INTEGER,
            "StringType": PythonTypes.STRING,
            "GeographyType": PythonTypes.STRING,
            "FloatType": PythonTypes.FLOAT,
            "DoubleType": PythonTypes.DOUBLE,
            "DecimalType": PythonTypes.DECIMAL,
            "DateType": PythonTypes.DATE,
            "TimestampType": PythonTypes.TIMESTAMP,
            "Timestamp": PythonTypes.TIMESTAMP,
            "BooleanType": PythonTypes.BOOLEAN,
        }
        datatype = self.dataset.schema.fields
        pattern = r"^(.*?)\("
        return {
            datatype[i].name: mapping_type.get(
                re.match(pattern, str(datatype[i].datatype)).group(1),
                PythonTypes.STRING,
            )
            for i in range(len(datatype))
        }

    def count_records(self) -> int:
        return self.dataset.count()

    def execute_conditions(self, conditions: dict[str, Column]) -> list[Metrics]:
        cols_to_select = [F.count("*").alias("total_count")]
        for rule_name in conditions.keys():
            rule_metrics_col = [
                F.sum(F.when(F.col(rule_name), 1).otherwise(0)).alias(
                    rule_name + "_valid_count"
                ),
                (F.col(rule_name + "_valid_count") * 100 / F.col("total_count")).alias(
                    rule_name + "_valid_pct_count"
                ),
            ]
            cols_to_select.extend(rule_metrics_col)

        df_rule = self.dataset.with_columns(conditions.keys(), conditions.values())
        metrics = df_rule.select(*cols_to_select).first().asDict()
        upper_metrics = {
            metrics_name.upper(): metrics_value
            for metrics_name, metrics_value in metrics.items()
        }

        metrics_timestamp = str(datetime.now())
        return [
            Metrics(
                rule=rule_name,
                total_row_count=upper_metrics["TOTAL_COUNT"],
                valid_row_count=upper_metrics[rule_name.upper() + "_VALID_COUNT"],
                valid_row_count_pct=round(
                    upper_metrics[rule_name.upper() + "_VALID_PCT_COUNT"], 2
                ),
                timestamp=metrics_timestamp,
            )
            for rule_name in conditions.keys()
        ]

    def is_null(self, condition: cond.IsNull) -> Column:
        return F.is_null(condition.col_name)

    def is_not_null(self, condition: cond.IsNotNull) -> Column:
        return _is_not_null(condition.col_name)

    def is_in(self, condition: cond.IsIn) -> Column:
        return F.col(condition.col_name).isin(condition.list_of_values)

    def rlike(self, condition: cond.Rlike) -> Column:
        return F.col(condition.col_name).rlike(condition.pattern)

    def compare_year_to_value(self, condition: cond.CompareYearToValue) -> Column:
        operator = self.mapping_operator.get(condition.operator, None)
        return F.year(condition.col_name).__getattribute__(operator)(
            F.lit(condition.value)
        )

    def is_iban(self, condition: cond.IsIban) -> Column:
        alphabet_conversion = {chr(i + 65): str(i + 10) for i in range(26)}
        cleaned_str_col = F.regexp_replace(
            F.col(condition.col_name), "[^a-zA-Z0-9]", ""
        )
        cleaned_col = F.concat(
            F.substring(cleaned_str_col, 5, 34), F.substring(cleaned_str_col, 1, 4)
        )

        cleaned_col = F.upper(cleaned_col)

        for letter, value in alphabet_conversion.items():
            cleaned_col = F.regexp_replace(cleaned_col, letter, value)

        is_iban_col = (
            F.concat(
                (F.substring(cleaned_col, 1, 15) % 97).cast("long").cast("string"),
                F.substring(cleaned_col, 16, 34),
            ).cast("long")
            % 97
        )

        return is_iban_col == 1

    def is_ip_address(self, condition: cond.IsIpAddress) -> Column:
        ipv6_regex = (
            "^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,"
            "6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,"
            "4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,"
            "2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,"
            "7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,"
            "1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,"
            "4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$"
        )
        ipv4_regex = "^(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})(\.(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})){3}$"
        trimmed_ip_col = F.trim(F.col(condition.col_name))
        return trimmed_ip_col.rlike(ipv6_regex) | trimmed_ip_col.rlike(ipv4_regex)

    def count_occurences(self, rule: R.CountOccurences) -> dict[Any, int]:
        val_count = self.dataset.groupBy(rule.col_name).count().collect()
        return dict(val_count)

    def compute_percentile(self, rule: R.ComputePercentile) -> float:
        return self.dataset.approxQuantile(rule.col_name, [rule.percentile])[0]

    def get_col_values_superior_to_constant(
        self, rule: R.GetColValuesSuperiorToConstant
    ) -> list[float]:
        return (
            self.dataset.filter(F.col(rule.col_name) > F.lit(rule.constant))
            .select(rule.col_name)
            .distinct()
            .collect()
        )

    def get_col_values_inferior_to_constant(
        self, rule: R.GetColValuesInferiorToConstant
    ) -> list[float]:
        return (
            self.dataset.filter(F.col(rule.col_name) < F.lit(rule.constant))
            .select(rule.col_name)
            .distinct()
            .collect()
        )

    def is_float(self, condition: cond.IsFloat) -> Column:
        return F.col(condition.col_name).rlike("^[-+]?[0-9]*\.[0-9]*[1-9]+[0]*$")

    def is_date(self, condition: cond.IsDate) -> Column:

        date_regex_patterns = (
            r"\b(\d{4}-\d{2}-\d{2}|"  # yyyy-mm-dd or yyyy-dd-mm
            r"\d{2}-\d{2}-\d{4}|"  # dd-mm-yyyy or mm-dd-yyyy
            r"\d{2}/\d{2}/\d{4}|"  # dd/mm/yyyy or mm/dd/yyyy
            r"\d{4}/\d{2}/\d{2})\b"  # yyyy/mm/dd or yyyy/dd/mm
        )
        return F.col(condition.col_name).rlike(date_regex_patterns)

    def is_phone_number(self, condition: cond.IsPhoneNumber) -> Column:
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
        return F.regexp_replace(F.col(condition.col_name), "[-\s]", "").rlike(regex)

    def is_boolean(self, condition: cond.IsBoolean) -> Column:
        data_type = self.dataset.schema[condition.col_name].datatype
        map_boolean_type = {
            T.IntegerType(): [0, 1],
            T.LongType(): [0, 1],
            T.DoubleType(): [0.0, 1.0],
            T.FloatType(): [0.0, 1.0],
            T.StringType(): ["0", "1", "true", "false", "True", "False"],
            T.BooleanType(): [True, False],
        }
        boolean_type = map_boolean_type.get(data_type, [])
        return F.col(condition.col_name).isin(boolean_type)

    def is_email(self, condition: cond.IsEmail) -> Column:
        return F.col(condition.col_name).rlike("^[\w\.-]+@[A-z\d\.-]+\.[A-z]{2,}$")

    def is_integer(self, condition: cond.IsInteger) -> Column:
        data_type = self.dataset.schema[condition.col_name].datatype
        if isinstance(
            data_type, (T.IntegerType, T.DoubleType, T.DecimalType, T.LongType)
        ):
            return F.col(condition.col_name) % 1 == 0
        elif isinstance(data_type, T.StringType):
            return F.col(condition.col_name).rlike("^[-+]?[0-9]+$")
        elif isinstance(data_type, T.BooleanType):
            return F.lit(False)

    def is_unique(self, condition: cond.IsUnique) -> Column:
        window_col_name = Window.partitionBy(condition.col_name)
        count_column = F.count(condition.col_name).over(window_col_name)
        return count_column == 1

    def compare_column_to_value(self, condition: cond.CompareColumnToValue) -> Column:
        operator = self.mapping_operator.get(condition.operator, None)
        return F.col(condition.col_name).__getattribute__(operator)(
            F.lit(condition.value)
        )

    def compare_column_to_column(self, condition: cond.CompareColumnToColumn) -> Column:
        operator = self.mapping_operator.get(condition.operator, None)
        return F.col(condition.col_left).__getattribute__(operator)(
            F.col(condition.col_right)
        )

    def count_decimal_digit(self, condition: cond.CountDecimalDigit) -> Column:
        operator = self.mapping_operator.get(condition.operator, None)
        return F.length(
            F.split(F.col(condition.col_name), F.lit(".")).getItem(1)
        ).__getattribute__(operator)(condition.digit)

    def count_integer_digit(self, condition: cond.CountIntegerDigit) -> Column:
        operator = self.mapping_operator.get(condition.operator, None)
        return F.length(
            F.split(F.col(condition.col_name), F.lit(".")).getItem(0)
        ).__getattribute__(operator)(condition.digit)

    def is_between(self, condition: cond.IsBetween) -> Column:
        return F.col(condition.col_name).between(
            lower_bound=condition.min_value, upper_bound=condition.max_value
        )

    def compare_length(self, condition: cond.CompareLength) -> Column:
        operator = self.mapping_operator.get(condition.operator, None)
        return F.length(condition.col_name).__getattribute__(operator)(condition.length)

    def is_alphabetic(self, condition: cond.IsAlphabetic) -> Column:
        return F.col(condition.col_name).rlike(r"^[A-zÀ-ú]+$")

    def is_negative(self, condition: cond.IsNegative) -> Column:
        return F.col(condition.col_name).rlike(r"^-\d*\.?\d+$")

    def is_positive(self, condition: cond.IsPositive) -> Column:
        return F.col(condition.col_name).rlike(r"^[+]?[0-9]\d*(\.\d+)?$")

    def aggregate_dataset(
        self, keys: list[str], agg_cols_expr: list[Column]
    ) -> RelationalGroupedDataFrame:
        return self.dataset.groupBy(*keys).agg(*agg_cols_expr)


class SnowflakeAggregateDataset(AggregateDataset):
    @staticmethod
    def sum(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: SnowflakeEngine,
    ) -> Column:
        return F.sum(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def count(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: SnowflakeEngine,
    ) -> Column:
        return F.count(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def mean(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: SnowflakeEngine,
    ) -> Column:
        return F.mean(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def min(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: SnowflakeEngine,
    ) -> Column:
        return F.min(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def max(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: SnowflakeEngine,
    ) -> Column:
        return F.max(agg_func.col_name).alias(agg_col_name)

    @staticmethod
    def median(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: SnowflakeEngine,
    ) -> Column:
        return F.median(agg_func.col_name).alias(agg_col_name)
