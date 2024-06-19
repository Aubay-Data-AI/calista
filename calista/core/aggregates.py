from abc import ABC, abstractmethod
from typing import List, TypeVar

from calista.core._aggregate_conditions import Count, Max, Mean, Median, Min, Sum
from calista.core.engine import LazyEngine

GenericAggExpr = TypeVar("GenericAggExpr")


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
