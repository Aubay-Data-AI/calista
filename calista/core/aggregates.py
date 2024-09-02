from abc import ABC, abstractmethod
from typing import List, TypeVar

from calista.core._aggregate_conditions import Count, Max, Mean, Median, Min, Sum
from calista.core.engine import LazyEngine

GenericAggExpr = TypeVar("GenericAggExpr")
GenericGroupedTableObject = TypeVar("GenericGroupedTableObject")
GenericAggExpr = TypeVar("GenericAggExpr")
DataFrameType = TypeVar("DataFrameType")


class AggregateDataset(ABC):
    @staticmethod
    @abstractmethod
    def sum(
        agg_func: Sum, agg_col_name: str, keys: List[str], engine: LazyEngine
    ) -> GenericAggExpr:
        ...

    @staticmethod
    @abstractmethod
    def count(
        agg_func: Count, agg_col_name: str, keys: List[str], engine: LazyEngine
    ) -> GenericAggExpr:
        ...

    @staticmethod
    @abstractmethod
    def mean(
        agg_func: Mean, agg_col_name: str, keys: List[str], engine: LazyEngine
    ) -> GenericAggExpr:
        ...

    @staticmethod
    @abstractmethod
    def min(
        agg_func: Min, agg_col_name: str, keys: List[str], engine: LazyEngine
    ) -> GenericAggExpr:
        ...

    @staticmethod
    @abstractmethod
    def max(
        agg_func: Max, agg_col_name: str, keys: List[str], engine: LazyEngine
    ) -> GenericAggExpr:
        ...

    @staticmethod
    @abstractmethod
    def median(
        agg_func: Median, agg_col_name: str, keys: List[str], engine: LazyEngine
    ) -> GenericAggExpr:
        ...

    @staticmethod
    @abstractmethod
    def aggregate_dataset(
        dataset: DataFrameType, keys: list[str], agg_cols_expr: list[GenericAggExpr]
    ) -> GenericGroupedTableObject:
        """
        Aggregate a dataset. It will be used for aggregate conditions

        Args:
            dataset (DataFrameType): DataFrame type object to aggregate.
            keys (list[str]): The aggregation keys.
            agg_cols_expr: list[GenericAggExpr]: The aggregation expressions list.

        Returns:
            GenericGroupedTableObject: The aggregated dataset.
        """
        ...

    @staticmethod
    @abstractmethod
    def left_join(
        left: DataFrameType, right: DataFrameType, on: list[str]
    ) -> DataFrameType:
        """
        This function joins two tables using left join. It will be used for the reverse
        param of GroupedTable methods: get_valid_rows, get_invalid_rows.

        Args:
            left (DataFrameType): Left side of the join.
            right (DataFrameType): Right side of the join.
            on (list[str]): List of column names. The column(s) must exist on both sides.

        Returns:
            DataFrameType: Result of the join.
        """
        ...
