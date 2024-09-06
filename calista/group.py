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
            - rule_name (str): The name of the rule.
            - rule (AggregateCondition): The aggregate condition to evaluate.

        Returns:
            :class:`~calista.core.metrics.Metrics`: The metrics resulting from the analysis.

        Raises:
            Any exceptions raised by the engine's execute_condition method.

        Example
        --------
        >>> from calista import CalistaEngine
        >>> from calista import functions as func
        >>>
        >>> # Create your CalistaTable
        >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
        >>>                                                                "POINTS": [10, 20, 30, 40, 20, 10]})
        >>>
        >>> # Define your rule
        >>> my_rule = func.sum_gt_value(col_name="POINTS", value=65)
        >>>
        >>> # Generate and print your metrics
        >>> metrics = calista_table.group_by("TEAM").analyze(rule_name="Total points higher than 65", rule=my_rule)
        >>> print(metrics)

        >>> rule_name : Total points higher than 65
        >>> total_row_count : 2
        >>> valid_row_count : 1
        >>> valid_row_count_pct : 50.0
        >>> timestamp : 2024-01-01 00:00:00.000000
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

        Example
        --------
        >>> from calista import CalistaEngine
        >>> from calista import functions as func
        >>>
        >>> # Create your CalistaTable
        >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
        >>>                                                                "POINTS": [10, 20, 30, 40, 20, 10]})
        >>>
        >>> # Define your rules
        >>> my_rule = func.sum_gt_value(col_name="POINTS", value=65)
        >>> my_rule_2 = func.median_eq_value(col_name="POINTS", value=20)
        >>>
        >>> # Generate and print your metrics
        >>> metrics = calista_table.group_by("TEAM").analyze_rules({"Total points higher than 65": my_rule,
        >>>                                                           "Median of the team equals 20": my_rule_2})
        >>> for metric in metrics:
        >>>     print(metrics)
        >>>     print("-----------------")

        >>> rule_name : Total points higher than 65
        >>> total_row_count : 2
        >>> valid_row_count : 1
        >>> valid_row_count_pct : 50.0
        >>> timestamp : 2024-01-01 00:00:00.000000
        >>> -----------------
        >>> rule_name : Median of the team equals 20
        >>> total_row_count : 2
        >>> valid_row_count : 2
        >>> valid_row_count_pct : 100.0
        >>> timestamp : 2024-01-01 00:00:00.000000
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

        Example
        --------
        >>> from calista import CalistaEngine
        >>> from calista import functions as func
        >>>
        >>> # Create your CalistaTable
        >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
        >>>                                                                "POINTS": [10, 20, 30, 40, 20, 10]})
        >>>
        >>> # Define your rule
        >>> my_rule = func.sum_gt_value(col_name="POINTS", value=65)
        >>>
        >>> # Generate and print the resulting dataframe
        >>> df_result = calista_table.group_by("TEAM").apply_rule(my_rule)
        >>> print(df_result)
        >>>          SUM_POINTS    RESULT
        >>>    TEAM
        >>>    blue          70      True
        >>>    red           60     False
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

        Example
        --------
        >>> from calista import CalistaEngine
        >>> from from calista import functions as func
        >>>
        >>> # Create your CalistaTable
        >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
        >>>                                                                "POINTS": [10, 20, 30, 40, 20, 10]})
        >>>
        >>> # Define your rules
        >>> my_rule = func.sum_gt_value(col_name="POINTS", value=65)
        >>> my_rule_2 = func.median_eq_value(col_name="POINTS", value=20)
        >>>
        >>> # Generate and print the resulting dataframe
        >>> df_result = calista_table.group_by("TEAM").apply_rules({"Total points higher than 65": my_rule,
        >>>                                                         "Median of the team equals 20": my_rule_2})
        >>> print(df_result)

        >>>          SUM_POINTS  MEDIAN_POINTS  Total points higher than 65  Median of the team equals 20
        >>>    TEAM
        >>>    blue          70           20.0                         True                          True
        >>>    red           60           20.0                        False                          True
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
            - rule (AggregateCondition): The aggregate condition to evaluate.
            - granular (bool, optional): default ``False``. Whether or not to retrieve the data at the granular level.

        Returns:
            `DataFrameType`: The aggregated dataset filtered with the rows where the condition is satisfied.

        Example
        --------
        >>> from calista import CalistaEngine
        >>> from calista import functions as func
        >>>
        >>> # Create your CalistaTable
        >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
        >>>                                                                "POINTS": [10, 20, 30, 40, 20, 10]})
        >>>
        >>> # Define your rule
        >>> my_rule = func.sum_gt_value(col_name="POINTS", value=65)
        >>>
        >>> # Generate and print the resulting dataframe
        >>> df_result = calista_table.group_by("TEAM").get_valid_rows(my_rule)
        >>> print(df_result)

        >>>          SUM_POINTS
        >>>    TEAM
        >>>    blue          70
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
            - rule (AggregateCondition): The aggregate condition to evaluate.
            - granular (bool, optional): default ``False``. Whether or not to retrieve the data at the granular level.

        Returns:
            `DataFrameType`: The aggregated dataset filtered with the rows where the condition is not satisfied.

        Example
        --------
        >>> from calista import CalistaEngine
        >>> from calista import functions as func
        >>>
        >>> # Create your CalistaTable
        >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
        >>>                                                                "POINTS": [10, 20, 30, 40, 20, 10]})
        >>>
        >>> # Define your rule
        >>> my_rule = func.sum_gt_value(col_name="POINTS", value=65)
        >>>
        >>> # Generate and print the resulting dataframe
        >>> df_result = calista_table.group_by("TEAM").get_invalid_rows(my_rule)
        >>> print(df_result)

        >>>          SUM_POINTS
        >>>    TEAM
        >>>    red           60
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
