from copy import deepcopy

from calista.core._aggregate_conditions import AggregateCondition, Median
from calista.core._conditions import AndCondition, Condition, NotCondition, OrCondition
from calista.core.engine import GenericColumnType, GenericGroupedTableObject
from calista.core.metrics import Metrics
from calista.core.utils import import_engine
from calista.engines.bigquery import BigqueryEngine


def _get_agg_colname(agg_cond: AggregateCondition):
    agg_func_name = agg_cond.get_func_agg().__class__.__name__.lower()
    return f"{agg_func_name}_{agg_cond.col_name}"


class GroupedTable:
    def __init__(self, engine, agg_keys) -> None:
        _dataset = engine.dataset
        self._engine = deepcopy(engine)
        self._engine.dataset = _dataset
        self._agg_keys = agg_keys
        self._aggregate_dataset_utils = import_engine(
            self._engine.__name__.lower(), "AggregateDataset"
        )

    def _evaluate_aggregates(self, condition: Condition) -> GenericGroupedTableObject:
        """
        Generate the necessary aggregation expressions for computing the aggregate dataset.

        Args:
            condition (AggregateCondition): The condition to evaluate.

        Returns:
            list[GenericAggExpr]: The aggregation expressions list.
        """
        agg_cols_expr = []
        seen = set()

        def parse(agg_cond):
            match agg_cond:
                case combined_condition if isinstance(
                    agg_cond, AndCondition
                ) or isinstance(agg_cond, OrCondition):
                    parse(combined_condition.left)
                    parse(combined_condition.right)
                case _:
                    func_agg = agg_cond.get_func_agg()
                    agg_col_name = func_agg.agg_col_name
                    func_agg_name = func_agg.__class__.__name__.lower()
                    if agg_col_name not in seen:
                        agg_cols_expr.append(
                            getattr(
                                self._aggregate_dataset_utils, func_agg_name
                            )(
                                func_agg,
                                agg_col_name,
                                self._agg_keys,
                                self._engine,
                            )
                        )
                        seen.add(agg_col_name)

        parse(condition)
        return self._engine.aggregate_dataset(self._agg_keys, agg_cols_expr)

    def _evaluate_condition(self, condition: AggregateCondition) -> GenericColumnType:

        if isinstance(condition, AndCondition) or isinstance(condition, OrCondition):
            left_cond = self._evaluate_condition(condition.left)
            right_cond = self._evaluate_condition(condition.right)

            return self._engine[condition](left_cond, right_cond)

        if isinstance(condition, NotCondition):
            cond = self._evaluate_condition(condition.cond)
            return self._engine[condition](cond)

        return self._engine[condition](condition)

    def analyze(self, rule_name: str, condition: AggregateCondition) -> Metrics:
        """
        Compute :class:`~calista.core.metrics.Metrics` based on a condition.

        Args:
            rule_name (str): The name of the rule.
            condition (AggregateCondition): The condition to evaluate.

        Returns:
            :class:`~calista.core.metrics.Metrics`: The metrics resulting from the analysis.

        Raises:
            Any exceptions raised by the engine's execute_condition method.
        """

        self._engine.dataset = self._evaluate_aggregates(condition)
        condition_as_check = condition.get_conditions_as_func_check()

        # if isinstance(self._engine, BigqueryEngine):
        #     agg_cols_expr = self._engine.get_agg_columns_expr(condition, self._agg_keys)
        # else:
        #     agg_cols_expr = self._engine.get_agg_columns_expr(condition)
        # self._engine.dataset = self._engine.aggregate_dataset(
        #     self._agg_keys, agg_cols_expr
        # )

        return self._engine.execute_conditions(
            {rule_name: self._evaluate_condition(condition_as_check)}
        )[0]
