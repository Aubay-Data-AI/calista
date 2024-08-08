from typing import Any

from pydantic import BaseModel, computed_field

from calista.core._conditions import CompareColumnToValue, Condition


class AggregateCondition(Condition):
    is_aggregate: bool = True


class AggregateFunction(BaseModel):
    @computed_field
    @property
    def agg_col_name(self) -> str:
        return f"{self.__class__.__name__.upper()}_{self.col_name}"


class Sum(AggregateFunction):
    col_name: str


class Count(AggregateFunction):
    col_name: str


class Mean(AggregateFunction):
    col_name: str


class Min(AggregateFunction):
    col_name: str


class Max(AggregateFunction):
    col_name: str


class Median(AggregateFunction):
    col_name: str


class SumBy(AggregateCondition):
    col_name: str
    value: Any
    operator: str
    is_aggregate: bool = True

    def get_func_agg(self):
        return Sum(col_name=self.col_name)

    def get_func_check(self):
        agg_col_name = self.get_func_agg().agg_col_name
        return CompareColumnToValue(
            col_name=agg_col_name, operator=self.operator, value=self.value
        )


class CountBy(AggregateCondition):
    col_name: str
    value: Any
    operator: str
    is_aggregate: bool = True

    def get_func_agg(self):
        return Count(col_name=self.col_name)

    def get_func_check(self):
        agg_col_name = self.get_func_agg().agg_col_name
        return CompareColumnToValue(
            col_name=agg_col_name, operator=self.operator, value=self.value
        )


class MeanBy(AggregateCondition):
    col_name: str
    value: Any
    operator: str
    is_aggregate: bool = True

    def get_func_agg(self):
        return Mean(col_name=self.col_name)

    def get_func_check(self):
        agg_col_name = self.get_func_agg().agg_col_name
        return CompareColumnToValue(
            col_name=agg_col_name, operator=self.operator, value=self.value
        )


class MinBy(AggregateCondition):
    col_name: str
    value: Any
    operator: str
    is_aggregate: bool = True

    def get_func_agg(self):
        return Min(col_name=self.col_name)

    def get_func_check(self):
        agg_col_name = self.get_func_agg().agg_col_name
        return CompareColumnToValue(
            col_name=agg_col_name, operator=self.operator, value=self.value
        )


class MaxBy(AggregateCondition):
    col_name: str
    value: Any
    operator: str
    is_aggregate: bool = True

    def get_func_agg(self):
        return Max(col_name=self.col_name)

    def get_func_check(self):
        agg_col_name = self.get_func_agg().agg_col_name
        return CompareColumnToValue(
            col_name=agg_col_name, operator=self.operator, value=self.value
        )


class MedianBy(AggregateCondition):
    col_name: str
    value: Any
    operator: str
    is_aggregate: bool = True

    def get_func_agg(self):
        return Median(col_name=self.col_name)

    def get_func_check(self):
        agg_col_name = self.get_func_agg().agg_col_name
        return CompareColumnToValue(
            col_name=agg_col_name, operator=self.operator, value=self.value
        )
