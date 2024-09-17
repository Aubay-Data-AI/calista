import inspect
from typing import Any, Callable, Type, Union

from pydantic import create_model

from calista.core._conditions import Condition
from calista.core.engine import LazyEngine, _camel_to_snake

__all__ = [
    "register_spark_condition",
    "register_snowflake_condition",
    "register_polars_condition",
    "register_pandas_condition",
    "register_bigquery_condition",
]

_DataFrameType = Union[
    "pyspark.sql.DataFrame",
    "snowflake.snowpark.DataFrame",
    "pandas.DataFrame",
    "polars.LazyFrame",
    "sqlalchemy.sql.selectable.Select",
]


def _udc_to_condition_model(
    user_func: Callable, dataframe_type: _DataFrameType
) -> Type[Condition]:
    """Transform a user function into a Condition model"""
    model_params = dict()
    model_params["is_udc"] = (bool, True)
    params = inspect.signature(user_func).parameters
    for p in params.values():
        if issubclass(p.annotation, dataframe_type):
            continue

        if p.kind in {p.KEYWORD_ONLY, p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}:
            default = None if p.default is p.empty else p.default
            arg_type = Any if p.annotation is p.empty else p.annotation
            model_params[p.name] = (arg_type, default)
        else:
            raise Exception(
                "User defined condition is only available for functions"
                "with the following arguments type: KEYWORD_ONLY, "
                "POSITIONAL_ONLY and POSITIONAL_OR_KEYWORD"
            )

    return create_model(user_func.__name__, __base__=Condition, **model_params)


class UserDefinedCondition:
    def __init__(self, user_func: Callable):
        self.func = user_func

    def __get__(self, instance: LazyEngine, owner: Type[LazyEngine]):
        def user_defined_condition(cond: Condition):
            if owner.__name__ in ["Pandas_Engine", "BigqueryEngine"]:
                return self.func(
                    instance.dataset,
                    **cond.model_dump(exclude={"is_aggregate", "is_udc"}),
                )
            else:
                return self.func(**cond.model_dump(exclude={"is_aggregate", "is_udc"}))

        return user_defined_condition


def _register_function_as_condition(
    engine: Type[LazyEngine], dataframe_type: _DataFrameType
):
    def user_defined_condition(user_func: Callable) -> Callable[[Any], Condition]:
        func_name = _camel_to_snake(user_func.__name__)

        if hasattr(engine, func_name):
            msg = f"{func_name} condition already exists in Calista. Choose a different name for your function"
            raise AttributeError(msg)
        else:
            setattr(engine, func_name, UserDefinedCondition(user_func))

        def condition(*args, **kwargs) -> Condition:
            if args:
                raise AttributeError(
                    "You must call your user defined condition providing keyword argument"
                )
            return _udc_to_condition_model(user_func, dataframe_type)(**kwargs)

        return condition

    return user_defined_condition


def register_spark_condition(user_func: Callable) -> Callable:
    from pyspark.sql import DataFrame

    from calista.engines.spark import SparkEngine

    DataFrameType = DataFrame

    return _register_function_as_condition(SparkEngine, DataFrameType)(user_func)


def register_snowflake_condition(user_func: Callable) -> Callable:
    from snowflake.snowpark import DataFrame as SnowparkDataFrame

    from calista.engines.snowflake import SnowflakeEngine

    DataFrameType = SnowparkDataFrame

    return _register_function_as_condition(SnowflakeEngine, DataFrameType)(user_func)


def register_polars_condition(user_func: Callable) -> Callable:
    import polars as pl

    from calista.engines.polars_ import Polars_Engine

    DataFrameType = pl.LazyFrame

    return _register_function_as_condition(Polars_Engine, DataFrameType)(user_func)


def register_pandas_condition(user_func: Callable) -> Callable:
    import pandas as pd

    from calista.engines.pandas_ import Pandas_Engine

    DataFrameType = pd.DataFrame

    return _register_function_as_condition(Pandas_Engine, DataFrameType)(user_func)


def register_bigquery_condition(user_func: Callable) -> Callable:
    from sqlalchemy.sql.selectable import Select

    from calista.engines.bigquery import BigqueryEngine

    DataFrameType = Select

    return _register_function_as_condition(BigqueryEngine, DataFrameType)(user_func)
