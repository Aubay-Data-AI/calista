from typing import Dict, List

from calista.core._aggregate_conditions import AggregateCondition
from calista.core._conditions import AndCondition, Condition, OrCondition
from calista.core.engine import DataFrameType, GenericGroupedTableObject
from calista.core.metrics import Metrics
from calista.core.types_alias import RuleName
from calista.core.utils import import_engine
from calista.table import CalistaTable


class GroupedTable:
    def __init__(self, engine, agg_keys) -> None:
        self._engine = engine.create_new_instance_from_dataset(engine.dataset)
        self._agg_keys = agg_keys
        self._aggregate_dataset_utils = import_engine(
            self._engine.__name__.lower(), "AggregateDataset"
        )

    def _evaluate_aggregates(
        self, conditions: List[AggregateCondition]
    ) -> GenericGroupedTableObject:
        """
        Generate the necessary aggregation expressions for computing the aggregate dataset.

        Args:
            condition (List[AggregateCondition]): The condition to evaluate.

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

        for condition in conditions:
            parse(condition)

        return self._aggregate_dataset_utils.aggregate_dataset(
            self._engine.dataset, self._agg_keys, agg_cols_expr
        )

    def analyze(self, rule_name: str, rule: AggregateCondition) -> Metrics:
        """
        Compute :class:`~calista.core.metrics.Metrics` based on a condition.

        Args:
            rule_name (str): The name of the rule.
            rule (AggregateCondition): The aggregate condition to evaluate.

        Returns:
            :class:`~calista.core.metrics.Metrics`: The metrics resulting from the analysis.

        Raises:
            Any exceptions raised by the engine's execute_condition method.
        """

        self._engine.dataset = self._evaluate_aggregates([rule])
        condition_as_check = rule.get_conditions_as_func_check()

        return CalistaTable(self._engine).analyze(rule_name, condition_as_check)

    def analyze_rules(self, rules: Dict[RuleName, AggregateCondition]) -> List[Metrics]:
        """
        Compute :class:`~calista.core.metrics.Metrics` based on a condition.

        Args:
            rules (dict[RuleName, AggregateCondition]): The name of the rules and the aggregate conditions to execute.

        Returns:
            :class:`List[Metrics]`: The metrics resulting from the analysis.

        Raises:
            Any exceptions raised by the engine's execute_condition method.
        """
        conditions_list = [
            rule_condition
            for rule_condition in rules.values()
            if isinstance(rule_condition, Condition)
        ]
        self._engine.dataset = self._evaluate_aggregates(conditions_list)

        conditions = {}
        for rule_name, rule_condition in rules.items():
            conditions[rule_name] = rule_condition.get_conditions_as_func_check()

        return CalistaTable(self._engine).analyze_rules(conditions)

    def apply_rule(self, rule: AggregateCondition) -> DataFrameType:
        """
        Returns the dataset with new columns of booleans for given condition.

        Args:
            rule (AggregateCondition): The aggregate condition to execute.

        Returns:
            `DataFrameType`: The aggregated dataset with the new column resulting from the analysis.
        """
        self._engine.dataset = self._evaluate_aggregates([rule])
        condition_as_check = rule.get_conditions_as_func_check()
        return CalistaTable(self._engine).apply_rule(condition_as_check)

    def apply_rules(self, rules: Dict[RuleName, AggregateCondition]) -> DataFrameType:
        """
        Returns the dataset with new columns of booleans for each rules or the given condition.

        Args:
            rules (Dict[RuleName, AggregateCondition]): The name of the rules and the aggregate conditions to execute.

        Returns:
            `DataFrameType`: The aggregate dataset with new columns resulting from the analysis.
        """
        # TODO: à corriger pour SQL
        conditions = {}
        aggregated_conditions = list(rules.values())
        self._engine.dataset = self._evaluate_aggregates(aggregated_conditions)

        conditions = {
            rule_name: rule_condition.get_conditions_as_func_check()
            for rule_name, rule_condition in rules.items()
        }
        return CalistaTable(self._engine).apply_rules(conditions)

    def get_valid_rows(self, rule: AggregateCondition, granular=False) -> DataFrameType:
        """
        Returns the dataset filtered with the rows validating the rules.

        Args:
            rule (AggregateCondition): The aggregate condition to evaluate.
            granular (bool, optional): default ``False``. Whether or not to retrieve the data at the granular level.

        Returns:
            `DataFrameType`: The aggregated dataset filtered with the rows where the rule is satisfied.
        """
        new_dataset = self._evaluate_aggregates([rule])
        if granular:
            self._engine.dataset = self._aggregate_dataset_utils.left_join(
                self._engine.dataset, new_dataset, on=self._agg_keys
            )
        else:
            self._engine.dataset = new_dataset
        condition_as_check = rule.get_conditions_as_func_check()
        return CalistaTable(self._engine).get_valid_rows(condition_as_check)

    def get_invalid_rows(
        self, rule: AggregateCondition, granular=False
    ) -> DataFrameType:
        """
        Returns the dataset filtered with the rows not validating the rules.

        Args:
            rule (AggregateCondition): The aggregate condition to evaluate.
            granular (bool, optional): default ``False``. Whether or not to retrieve the data at the granular level.

        Returns:
            `DataFrameType`: The aggregated dataset filtered with the rows where the rule is not satisfied.
        """
        new_dataset = self._evaluate_aggregates([rule])
        if granular:
            self._engine.dataset = self._aggregate_dataset_utils.left_join(
                self._engine.dataset, new_dataset, on=self._agg_keys
            )
        else:
            self._engine.dataset = new_dataset
        condition_as_check = rule.get_conditions_as_func_check()
        return CalistaTable(self._engine).get_invalid_rows(condition_as_check)
