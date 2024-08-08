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


from typing import List, TypeVar

from sqlalchemy import VARCHAR, ColumnExpressionArgument, case, func, select
from sqlalchemy.sql import text

import calista.core._conditions as cond
import calista.core.rules as R
from calista.core._aggregate_conditions import Median
from calista.engines.sql import SqlAggregateDataset, SqlEngine

GenericAggExpr = TypeVar("GenericAggExpr")


class BigqueryEngine(SqlEngine):
    def is_integer(self, condition: cond.IsInteger) -> ColumnExpressionArgument:
        return (
            self.dataset.c[condition.col_name]
            .cast(VARCHAR)
            .regexp_match("^[-+]?[0-9]*$")
        )

    def is_boolean(self, condition: cond.IsBoolean) -> ColumnExpressionArgument:
        column_cast_string = self.dataset.c[condition.col_name].cast(VARCHAR)
        boolean_type = ["0", "1", "true", "false", "True", "False"]
        return column_cast_string.in_(boolean_type)

    def is_unique(self, condition: cond.IsUnique) -> ColumnExpressionArgument:
        count_column = func.count(self.dataset.c[condition.col_name]).label(
            "count_column"
        )
        subquery = (
            select(self.dataset.c[condition.col_name], count_column)
            .group_by(self.dataset.c[condition.col_name])
            .alias("subquery")
        )
        return subquery.c.count_column == 1

    def compute_percentile(self, rule: R.ComputePercentile) -> float:
        query = text(
            f"""
                        SELECT
                            percentiles[offset({int(rule.percentile*100)})]
                        FROM
                            (SELECT
                                APPROX_QUANTILES(SALAIRE, 100) percentiles
                            FROM {self.dataset.name}
                            )
                     """
        )
        with self.engine.connect() as conn:
            percentile = conn.execute(query).fetchall()
            conn.close()
        return percentile[0][0]


class BigqueryAggregateDataset(SqlAggregateDataset):
    @staticmethod
    def median(
        agg_func: Median,
        agg_col_name: str,
        keys: List[str],
        engine: BigqueryEngine,
    ) -> ColumnExpressionArgument:
        col_expr = engine.dataset.c[agg_func.col_name]
        keys_expr = [engine.dataset.c[key] for key in keys]
        half1_column = (
            func.ntile(2).over(partition_by=keys_expr, order_by=col_expr).label("Half1")
        )
        half2_column = (
            func.ntile(2)
            .over(partition_by=keys_expr, order_by=col_expr.desc())
            .label("Half2")
        )
        engine.dataset = (
            select(engine.dataset, half1_column, half2_column)
            .where(col_expr.is_not(None))
            .alias("ntile_subquery")
        )

        case_expression_max = case(
            (engine.dataset.c["Half1"] == 1, engine.dataset.c[col_expr.name]),
            else_=None,
        )
        case_expression_min = case(
            (engine.dataset.c["Half2"] == 1, engine.dataset.c[col_expr.name]),
            else_=None,
        )
        median_expr = (
            func.max(case_expression_max) + func.min(case_expression_min)
        ) / 2.0
        return median_expr.label(agg_col_name)
