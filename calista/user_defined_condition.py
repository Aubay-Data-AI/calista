import inspect
from typing import Any, Callable, Type

from pydantic import create_model

from calista.core._conditions import Condition
from calista.core.engine import LazyEngine, _camel_to_snake

__all__ = [
    "register_spark_condition",
    "register_snowflake_condition",
    "regisiter_polars_condition",
    "register_pandas_condition",
    "register_bigquery_condition",
]


def _udc_to_condition_model(user_func: Callable) -> Type[Condition]:
    """Transform a user function into a Condition model"""
    model_params = dict()
    params = inspect.signature(user_func).parameters
    for p in params.values():
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
            return self.func(**cond.model_dump(exclude={"is_aggregate"}))

        return user_defined_condition


class UserDefinedConditionWithDataset:
    def __init__(self, user_func: Callable):
        self.func = user_func

    def __get__(self, instance: LazyEngine, owner: Type[LazyEngine]):
        def user_defined_condition(cond: Condition):
            return self.func(
                instance.dataset, **cond.model_dump(exclude={"is_aggregate", "dataset"})
            )

        return user_defined_condition


def _register_function_as_condition(name: str, engine: Type[LazyEngine]):
    if hasattr(engine, name):
        msg = f"{name} condition already exist in Calista"
        raise AttributeError(msg)

    def user_defined_condition(user_func: Callable) -> Callable[[Any], Condition]:
        func_name = _camel_to_snake(
            user_func.__name__
        )  # Just to be sure that we are ok with our internal naming convention
        if engine.__name__ in ["Pandas_Engine", "BigqueryEngine"]:
            setattr(engine, func_name, UserDefinedConditionWithDataset(user_func))
        else:
            setattr(engine, func_name, UserDefinedCondition(user_func))

        def condition(*args, **kwargs) -> Condition:
            if args:
                raise AttributeError(
                    "You must call your user defined condition providing keyword argument"
                )
            return _udc_to_condition_model(user_func)(**kwargs)

        return condition

    return user_defined_condition


def register_spark_condition(name: str) -> Callable:
    from calista.engines.spark import SparkEngine

    return _register_function_as_condition(name, SparkEngine)


def register_snowflake_condition(name: str) -> Callable:
    from calista.engines.snowflake import SnowflakeEngine

    return _register_function_as_condition(name, SnowflakeEngine)


def register_polars_condition(name: str) -> Callable:
    from calista.engines.polars_ import Polars_Engine

    return _register_function_as_condition(name, Polars_Engine)


def register_pandas_condition(name: str) -> Callable:
    from calista.engines.pandas_ import Pandas_Engine

    return _register_function_as_condition(name, Pandas_Engine)


def register_bigquery_condition(name: str) -> Callable:
    from calista.engines.bigquery import BigqueryEngine

    return _register_function_as_condition(name, BigqueryEngine)
