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

import pandas as pd
from sqlalchemy import (
    VARCHAR,
    ColumnExpressionArgument,
    MetaData,
    String,
    and_,
    case,
    cast,
    create_engine,
    extract,
    func,
    or_,
    select,
)
from sqlalchemy import types as T
from sqlalchemy.sql.selectable import Select

import calista.core._aggregate_conditions as aggregateCond
import calista.core._conditions as cond
import calista.core.rules as R
from calista.core.aggregates import AggregateDataset
from calista.core.catalogue import PythonTypes
from calista.core.database import Database
from calista.core.metrics import Metrics
from calista.core.types_alias import ColumnName, PythonType


class SqlEngine(Database):

    mapping_operator: dict[str, str] = {
        "=": "__eq__",
        "!=": "__ne__",
        ">": "__gt__",
        ">=": "__ge__",
        "<": "__lt__",
        "<=": "__le__",
    }

    def __init__(self, config: Dict[str, Any] = None):
        self.engine = create_engine(
            config["connection_string"], credentials_path=config["credentials_path"]
        )
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.dataset = None
        self._config = config

    def _load_from_database(
        self, table: str, schema: str = None, database: str = None
    ) -> None:
        try:
            self.dataset = self.metadata.tables[table]
        except KeyError:
            raise KeyError(f"This table doesn't exist: {table}")

    def where(self, expression: ColumnExpressionArgument) -> Select:
        return self.dataset.where(expression)

    def filter(self, expression: ColumnExpressionArgument) -> Select:
        return self.where(expression)

    def show(self, n: int = 10):
        stmt = select(self.dataset)
        with self.engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
        print(df.head(n))

    def and_condition(
        self, left_cond: ColumnExpressionArgument, right_cond: ColumnExpressionArgument
    ) -> ColumnExpressionArgument:
        return and_(left_cond, right_cond)

    def or_condition(
        self, left_cond: ColumnExpressionArgument, right_cond: ColumnExpressionArgument
    ) -> ColumnExpressionArgument:
        return or_(left_cond, right_cond)

    def not_condition(self, cond: ColumnExpressionArgument) -> ColumnExpressionArgument:
        return ~cond

    def get_schema(self) -> dict[ColumnName:str, PythonType:str]:
        mapping_type = {
            "INTEGER": PythonTypes.INTEGER,
            "BIGINT": PythonTypes.INTEGER,
            "CHAR": PythonTypes.STRING,
            "VARCHAR": PythonTypes.STRING,
            "DOUBLE PRECISION": PythonTypes.FLOAT,
            "FLOAT": PythonTypes.FLOAT,
            "DOUBLE": PythonTypes.FLOAT,
            "DATE": PythonTypes.DATE,
            "TIMESTAMP": PythonTypes.TIMESTAMP,
            "BOOLEAN": PythonTypes.BOOLEAN,
            "TEXT": PythonTypes.STRING,
            "INT64": PythonTypes.INTEGER,
        }
        return {
            column.name: mapping_type.get(
                str(column.type).replace("()", ""), PythonTypes.STRING
            )
            for column in self.dataset.c
        }

    def count_records(self) -> int:
        total_count_query = func.count().select().select_from(self.dataset)
        with self.engine.connect() as conn:
            total_count = conn.execute(total_count_query).fetchall()[0][0]
            conn.close()
        return total_count

    def execute_conditions(
        self, conditions: dict[str, ColumnExpressionArgument]
    ) -> list[Metrics]:
        total_count_query = func.count().select().select_from(self.dataset)
        valid_count_queries_with_rule_name = {
            rule_name: select(func.count("*")).where(condition)
            for rule_name, condition in conditions.items()
        }

        with self.engine.connect() as conn:
            total_count = conn.execute(total_count_query).fetchone()[0]
            valid_count_with_rule_name = {
                rule_name: conn.execute(valid_count_query).fetchone()[0]
                for rule_name, valid_count_query in valid_count_queries_with_rule_name.items()
            }

        metrics_timestamp = str(datetime.now())
        return [
            Metrics(
                rule=rule_name,
                total_row_count=total_count,
                valid_row_count=valid_count,
                valid_row_count_pct=valid_count * 100 / total_count,
                timestamp=metrics_timestamp,
            )
            for rule_name, valid_count in valid_count_with_rule_name.items()
        ]

    def is_null(self, condition: cond.IsNull) -> ColumnExpressionArgument:
        return self.dataset.c[condition.col_name] == None

    def is_not_null(self, condition: cond.IsNotNull) -> ColumnExpressionArgument:
        return self.dataset.c[condition.col_name] != None

    def is_in(self, condition: cond.IsIn) -> ColumnExpressionArgument:
        return self.dataset.c[condition.col_name].in_(condition.list_of_values)

    def rlike(self, condition: cond.Rlike) -> ColumnExpressionArgument:
        raise Exception("rlike() function is not available")

    def compare_year_to_value(
        self, condition: cond.CompareYearToValue
    ) -> ColumnExpressionArgument:
        operator = self.mapping_operator.get(condition.operator, None)
        return extract("year", self.dataset.c[condition.col_name]).__getattribute__(
            operator
        )(condition.value)

    def is_iban(self, condition: cond.IsIban) -> ColumnExpressionArgument:

        alphabet_conversion = {chr(i + 65): str(i + 10) for i in range(26)}

        func_clean_cast_string = cast(self.dataset.c[condition.col_name], String)
        func_check_length = case(
            (func.length(func_clean_cast_string) <= 14, "0"),
            else_=func_clean_cast_string,
        )
        func_clean_none = func.coalesce(func_check_length, "0")
        func_clean_sp_char = func.regexp_replace(func_clean_none, "[^a-zA-Z0-9]+", "")
        func_clean = func.concat(
            func.substring(func_clean_sp_char, 5, 34),
            func.substring(func_clean_sp_char, 1, 4),
        )

        for letter, value in alphabet_conversion.items():
            func_clean = func.replace(func_clean, letter, value)
            func_clean = func.replace(func_clean, letter.lower(), value)

        func_start_of_str_mod = func.mod(
            func.substring(func_clean, 1, 15).cast(T.BigInteger), 97
        ).cast(T.String)
        func_end_of_str = func.substring(func_clean, 16, 19)
        func_concat = func.concat(func_start_of_str_mod, func_end_of_str)

        func_start_of_str_mod_2 = func.mod(
            func.substring(func_concat, 1, 15).cast(T.BigInteger), 97
        ).cast(T.String)
        func_end_of_str_2 = func.substring(func_concat, 16, 19)
        func_concat_2 = func.concat(func_start_of_str_mod_2, func_end_of_str_2).cast(
            T.BigInteger
        )

        func_iban = func.mod(func_concat_2, 97)

        return func_iban == 1

    def is_ip_address(self, condition: cond.IsIpAddress) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        ipv6_regex = (
            "^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,"
            "6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,"
            "4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,"
            "2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,"
            "7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,"
            "1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,"
            "4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$"
        )
        ipv4_regex = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
        trimmed_ip_col = func.trim(column_cast_string)
        return trimmed_ip_col.regexp_match(ipv6_regex) | trimmed_ip_col.regexp_match(
            ipv4_regex
        )

    def is_float(self, condition: cond.IsFloat) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        return column_cast_string.regexp_match("^[-+]?[0-9]*\.[0-9]+$")

    def is_date(self, condition: cond.IsDate) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        date_regex_patterns = r"(\d{4}-\d{2}-\d{2})|(\d{2}-\d{2}-\d{4})|(\d{2}\/\d{2}\/\d{4})|(\d{4}\/\d{2}\/\d{2})"
        return column_cast_string.regexp_match(date_regex_patterns)

    def is_phone_number(
        self, condition: cond.IsPhoneNumber
    ) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
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
        column_cast_string = func.regexp_replace(column_cast_string, "[-\s]", "")
        return column_cast_string.regexp_match(regex)

    def is_boolean(self, condition: cond.IsBoolean) -> ColumnExpressionArgument:
        data_type = self.get_schema()[condition.col_name]
        map_boolean_type = {
            PythonTypes.INTEGER: [0, 1],
            PythonTypes.FLOAT: [0.0, 1.0],
            PythonTypes.STRING: ["0", "1", "true", "false", "True", "False"],
            PythonTypes.BOOLEAN: [True, False],
        }
        boolean_type = map_boolean_type.get(data_type, [])
        return self.dataset.c[condition.col_name].in_(boolean_type)

    def is_email(self, condition: cond.IsEmail) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        return column_cast_string.regexp_match("^[\w\.-]+@[A-z\d\.-]+\.[A-z]{2,}$")

    def is_integer(self, condition: cond.IsInteger) -> ColumnExpressionArgument:
        return (
            self.dataset.c[condition.col_name]
            .cast(VARCHAR)
            .regexp_match("^[-+]?[0-9]*$")
        )

    def is_unique(self, condition: cond.IsUnique) -> ColumnExpressionArgument:
        count_column = (
            func.count(self.dataset.c[condition.col_name])
            .over(partition_by=self.dataset.c[condition.col_name])
            .label("count_column")
        )
        subquery = select(self.dataset, count_column).alias("subquery")
        return subquery.c.count_column == 1

    def count_occurences(self, rule: R.CountOccurences) -> dict[Any, int]:
        query = select(self.dataset.c[rule.col_name], func.count()).group_by(
            self.dataset.c[rule.col_name]
        )
        with self.engine.connect() as conn:
            val_count = conn.execute(query).fetchall()
            conn.close()
        return dict(val_count)

    def compute_percentile(self, rule: R.ComputePercentile) -> float:
        query = select(
            func.percentile_cont(rule.percentile).within_group(
                self.dataset.c[rule.col_name]
            )
        )
        with self.engine.connect() as conn:
            percentile = conn.execute(query).fetchall()
            conn.close()
        return percentile[0][0]

    def get_col_values_superior_to_constant(
        self, rule: R.GetColValuesSuperiorToConstant
    ) -> list[float]:
        query = select(self.dataset.c[rule.col_name]).where(
            self.dataset.c[rule.col_name] > rule.constant
        )
        with self.engine.connect() as conn:
            values = conn.execute(query).fetchall()
            conn.close()
        values = [value[0] for value in values]
        return list(set(values))

    def get_col_values_inferior_to_constant(
        self, rule: R.GetColValuesInferiorToConstant
    ) -> list[float]:
        query = select(self.dataset.c[rule.col_name]).where(
            self.dataset.c[rule.col_name] < rule.constant
        )
        with self.engine.connect() as conn:
            values = conn.execute(query).fetchall()
            conn.close()
        values = [value[0] for value in values]
        return list(set(values))

    def compare_column_to_value(
        self, condition: cond.CompareColumnToValue
    ) -> ColumnExpressionArgument:
        operator = self.mapping_operator.get(condition.operator, None)
        return self.dataset.c[condition.col_name].__getattribute__(operator)(
            condition.value
        )

    def compare_column_to_column(
        self, condition: cond.CompareColumnToColumn
    ) -> ColumnExpressionArgument:
        operator = self.mapping_operator.get(condition.operator, None)
        return self.dataset.c[condition.col_left].__getattribute__(operator)(
            self.dataset.c[condition.col_right]
        )

    def count_decimal_digit(
        self, condition: cond.CountDecimalDigit
    ) -> ColumnExpressionArgument:
        operator = self.mapping_operator.get(condition.operator, None)
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        func_regexp_decimal = func.regexp_extract(column_cast_string, r"\.(\d+)", 1)
        func_length = func.length(func_regexp_decimal)
        return func_length.__getattribute__(operator)(condition.digit)

    def count_integer_digit(
        self, condition: cond.CountIntegerDigit
    ) -> ColumnExpressionArgument:
        operator = self.mapping_operator.get(condition.operator, None)
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        func_regexp_integer = func.regexp_extract(column_cast_string, r"^(\d+)\.", 1)
        func_length = func.length(func_regexp_integer)
        return func_length.__getattribute__(operator)(condition.digit)

    def is_between(self, condition: cond.IsBetween) -> ColumnExpressionArgument:
        return self.dataset.c[condition.col_name].__getattribute__("__ge__")(
            condition.min_value
        ) & self.dataset.c[condition.col_name].__getattribute__("__le__")(
            condition.max_value
        )

    def compare_length(self, condition: cond.CompareLength) -> ColumnExpressionArgument:
        operator = self.mapping_operator.get(condition.operator, None)
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        func_length = func.length(column_cast_string)
        return func_length.__getattribute__(operator)(condition.length)

    def is_alphabetic(self, condition: cond.IsAlphabetic) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        return column_cast_string.regexp_match(r"^[A-zÀ-ú]+$")

    def is_negative(self, condition: cond.IsNegative) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        return column_cast_string.regexp_match(r"^-\d*\.?\d+$")

    def is_positive(self, condition: cond.IsPositive) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(String)
        return column_cast_string.regexp_match(r"^[+]?[0-9]\d*(\.\d+)?$")

    def aggregate_dataset(
        self, keys: list[str], agg_cols_expr: list[ColumnExpressionArgument]
    ) -> Select:
        keys_expr = [self.dataset.c[key] for key in keys]
        subquery = select(*agg_cols_expr).group_by(*keys_expr)
        return subquery


class SqlAggregateDataset(AggregateDataset):
    @staticmethod
    def sum(
        agg_func: aggregateCond.SumBy,
        agg_col_name: str,
        keys: List[str],
        engine: SqlEngine,
    ) -> ColumnExpressionArgument:
        return func.sum(engine.dataset.c[agg_func.col_name]).label(agg_col_name)

    @staticmethod
    def count(
        agg_func: aggregateCond.CountBy,
        agg_col_name: str,
        keys: List[str],
        engine: SqlEngine,
    ) -> ColumnExpressionArgument:
        return func.count(engine.dataset.c[agg_func.col_name]).label(agg_col_name)

    @staticmethod
    def mean(
        agg_func: aggregateCond.MeanBy,
        agg_col_name: str,
        keys: List[str],
        engine: SqlEngine,
    ) -> ColumnExpressionArgument:
        return func.avg(engine.dataset.c[agg_func.col_name]).label(agg_col_name)

    @staticmethod
    def min(
        agg_func: aggregateCond.MinBy,
        agg_col_name: str,
        keys: List[str],
        engine: SqlEngine,
    ) -> ColumnExpressionArgument:
        return func.min(engine.dataset.c[agg_func.col_name]).label(agg_col_name)

    @staticmethod
    def max(
        agg_func: aggregateCond.MaxBy,
        agg_col_name: str,
        keys: List[str],
        engine: SqlEngine,
    ) -> ColumnExpressionArgument:
        return func.max(engine.dataset.c[agg_func.col_name]).label(agg_col_name)

    @staticmethod
    def median(
        agg_func: aggregateCond.MedianBy,
        agg_col_name: str,
        keys: List[str],
        engine: SqlEngine,
    ) -> ColumnExpressionArgument:
        return func.median(engine.dataset.c[agg_func.col_name]).label(agg_col_name)
