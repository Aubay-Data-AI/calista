from calista.core._aggregate_conditions import AggregateCondition
from calista.core._conditions import AndCondition, Condition, OrCondition
from calista.core.engine import DataFrameType, GenericGroupedTableObject
from calista.core.metrics import Metrics
from calista.core.utils import import_engine
from calista.table import CalistaTable


class GroupedTable:
    def __init__(self, engine, agg_keys) -> None:
        self._engine = engine.create_new_instance_from_dataset(engine.dataset)
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
                            getattr(self._aggregate_dataset_utils, func_agg_name)(
                                func_agg,
                                agg_col_name,
                                self._agg_keys,
                                self._engine,
                            )
                        )
                        seen.add(agg_col_name)

        parse(condition)
        return self._engine.aggregate_dataset(self._agg_keys, agg_cols_expr)

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

        return CalistaTable(self._engine).analyze(rule_name, condition_as_check)

    def get_rows(self, condition: Condition) -> DataFrameType:
        """
        Returns the dataset with new columns of booleans for given condition.

        Args:
            condition (Condition): The condition to execute.

        Returns:
            `DataFrameType`: The aggregated dataset with the new column resulting from the analysis.
        """
        self._engine.dataset = self._evaluate_aggregates(condition)
        condition_as_check = condition.get_conditions_as_func_check()
        return CalistaTable(self._engine).get_rows(condition_as_check)

    def get_valid_rows(self, condition: Condition) -> DataFrameType:
        """
        Returns the dataset filtered with the rows validating the rules.

        Args:
            condition (Condition): The condition to evaluate.

        Returns:
            `DataFrameType`: The aggregated dataset filtered with the rows where the condition is satisfied.
        """
        self._engine.dataset = self._evaluate_aggregates(condition)
        condition_as_check = condition.get_conditions_as_func_check()
        return CalistaTable(self._engine).get_valid_rows(condition_as_check)

    def get_invalid_rows(self, condition: Condition) -> DataFrameType:
        """
        Returns the dataset filtered with the rows not validating the rules.

        Args:
            condition (Condition): The condition to evaluate.

        Returns:
            `DataFrameType`: The aggregated dataset filtered with the rows where the condition is not satisfied.
        """
        self._engine.dataset = self._evaluate_aggregates(condition)
        condition_as_check = condition.get_conditions_as_func_check()
        return CalistaTable(self._engine).get_invalid_rows(condition_as_check)
