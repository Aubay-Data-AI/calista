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


from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Dict, List, TypeVar, Callable

import numpy as np

import calista.core._conditions as cond
import calista.core.rules as R
from calista.core._conditions import Condition
from calista.core.metrics import Metrics
from calista.core.types_alias import ColumnName, PythonType, RuleName

GenericColumnType = TypeVar("GenericColumnType")
GenericGroupedTableObject = TypeVar("GenericGroupedTableObject")
GenericAggExpr = TypeVar("GenericAggExpr")
DataFrameType = TypeVar("DataFrameType")


def _camel_to_snake(condition_name: str):
    """
    Convert CamelCase to snake_case.

    Args:
        condition_name (str): The CamelCase string to convert.

    Returns:
        str: The snake_case string.
    """
    return "".join(
        ["_" + c.lower() if c.isupper() else c for c in condition_name]
    ).lstrip("_")


class LazyEngine(ABC):
    """
    Abstract base class representing an engine.

    Attributes:
        None
    """

    @abstractmethod
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the engine.

        Args:
            config (Dict[str, Any], optional): Configuration options for the engine.
        """
        self._dataset = None
        self._config = config

    @property
    def __name__(self):
        _class_base_name = "Engine"
        return self.__class__.__name__.removesuffix(_class_base_name).replace("_", "")

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, dataset):
        self._dataset = dataset

    @abstractmethod
    def _load_from_dataframe(self, dataframe: DataFrameType) -> None:
        """
        Load data into engine main object.

        Args:
            dataframe (DataFrameType): An existing dataframe.
        """

    @abstractmethod
    def _load_from_dict(self, data: Dict[str, List]) -> None:
        """
        Load data into engine main object.

        Args:
            data: The data to be loaded.
        """
        ...

    @abstractmethod
    def _load_from_path(
        self, path: str, file_format: str, options: Dict[str, Any] = None
    ) -> None:
        """
        Load data into engine main object.

        Args:
            path (str): The path of the file containing your table.
            file_format (str): The format of the file (e.g., 'csv', 'parquet').
        """
        ...

    def read_dataset(
        self,
        path=None,
        file_format=None,
        data=None,
        table=None,
        schema=None,
        database=None,
        dataframe=None,
        options: Dict[str, Any] = None,
    ):
        """
        Read the dataset.

        Args:
            path: The path to the dataset.
            file_format: The format of the dataset file.
            data: The dictionary containing the data of the table.
            table: The name of the table.
            schema: The schema containing the dataset.
            database: The database containing the dataset.
            dataframe: An existing dataframe.
            options (Dict[str, Any], optional): Configuration options for files.
        """
        if dataframe is not None:
            self._load_from_dataframe(dataframe=dataframe)
        elif data:
            self._load_from_dict(data=data)
        elif file_format:
            self._load_from_path(path=path, file_format=file_format, options=options)
        elif table:
            raise NotImplementedError(
                "Loading from a table is not possible for this engine"
            )
        else:
            raise ValueError("The arguments provided are incorrect.")

    def __getitem__(self, condition: Condition):
        """
        Get item from the engine based on condition.

        Args:
            condition (Condition): The condition to get item based on.

        Returns:
            Any: The item from the engine.
        """
        attr_name = _camel_to_snake(condition.__class__.__name__)
        return self.__getattribute__(attr_name)

    def __getattribute__(self, attr):
        funcs = {"add_column"}
        if attr in funcs:
            return self.transform_dataset_to_new_instance(super().__getattribute__(attr))

        return super().__getattribute__(attr)

    def transform_dataset_to_new_instance(self, attr: Callable):

        def _transform_dataset_to_new_instance(*args, **kwargs):
            new_dataset = attr(*args, **kwargs)
            return self.create_new_instance_from_dataset(new_dataset)

        return _transform_dataset_to_new_instance

    def __deepcopy__(self, memo):  # memo is a dict of id's to copies
        id_self = id(self)  # memoization avoids unnecesary recursion
        _copy = memo.get(id_self)
        if _copy is None:
            _copy = type(self)(deepcopy(self._config, memo))
            memo[id_self] = _copy
        return _copy

    def create_new_instance_from_dataset(self, dataset):
        new_instance = deepcopy(self)
        new_instance.dataset = dataset
        return new_instance

    @abstractmethod
    def where(self, expression: GenericColumnType) -> DataFrameType:
        ...

    def filter(self, expresion: GenericColumnType) -> DataFrameType:
        """Alias of where method"""
        return self.where(expresion)

    @abstractmethod
    def and_condition(
        self, left_cond: GenericColumnType, right_cond: GenericColumnType
    ) -> GenericColumnType:
        """
        Perform a logical AND operation between two conditions.

        Args:
            left_cond (GenericColumnType): The left condition.
            right_cond (GenericColumnType): The right condition.

        Returns:
            GenericColumnType: The result of the logical AND operation.
        """
        ...

    @abstractmethod
    def or_condition(
        self, left_cond: GenericColumnType, right_cond: GenericColumnType
    ) -> GenericColumnType:
        """
        Perform a logical OR operation between two conditions.

        Args:
            left_cond (GenericColumnType): The left condition.
            right_cond (GenericColumnType): The right condition.

        Returns:
            GenericColumnType: The result of the logical OR operation.
        """
        ...

    @abstractmethod
    def not_condition(self, cond: GenericColumnType) -> GenericColumnType:
        ...

    @abstractmethod
    def add_column(self, col_name: str, col: GenericColumnType) -> DataFrameType:
        """
        Add a column to the dataset.

        Args:
            col_name (str): The column name.
            col (GenericColumnType): The column to add to the dataset.

        Returns:
            DataFrameType: The modified DataFrame with additional columns
        """
        ...

    @abstractmethod
    def execute_conditions(
        self, conditions: dict[RuleName:str, GenericColumnType]
    ) -> list[Metrics]:
        """
        Execute conditions with given rules name.

        Args:
            conditions (dict[str, GenericColumnType]): The name of the rules and the conditions to execute.

        Returns:
            list[Metrics]: The metrics resulting from executing the conditions.
        """
        ...

    @abstractmethod
    def get_schema(
        self,
    ) -> dict[ColumnName, PythonType]:
        """return a dict with col names as key and python types as values"""
        ...

    @abstractmethod
    def count_records(self) -> int:
        """
        Count the number of records in the dataset.

        Returns:
            int: The number of records.
        """
        ...

    @abstractmethod
    def show(self, n: int = 10) -> None:
        """
        Prints the first n rows to the console.

        Args:
            n (int, optional): Number of rows to show
        """
        ...

    @abstractmethod
    def is_null(self, condition: cond.IsNull) -> GenericColumnType:
        """
        Check if a column is null.

        Args:
            condition (cond.IsNull): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the null check.
        """
        ...

    @abstractmethod
    def is_not_null(self, condition: cond.IsNotNull) -> GenericColumnType:
        """
        Check if a column is not null.

        Args:
            condition (cond.IsNotNull): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the not-null check.
        """
        ...

    @abstractmethod
    def is_in(self, condition: cond.IsIn) -> GenericColumnType:
        """
        Check if a column value is in a list of values.

        Args:
            condition (cond.IsIn): The condition specifying the column and list of values.

        Returns:
            GenericColumnType: The result of the membership check.
        """
        ...

    @abstractmethod
    def rlike(self, condition: cond.Rlike) -> GenericColumnType:
        """
        Check if a column value matches a regex.

        Args:
            condition (cond.Rlike): The condition specifying the column and the pattern.

        Returns:
            GenericColumnType: The result of the matching pattern check.
        """
        ...

    @abstractmethod
    def compare_year_to_value(
        self, condition: cond.CompareYearToValue
    ) -> GenericColumnType:
        """
        Compare a column value to a specific year.

        Args:
            condition (cond.CompareYearToValue): The condition specifying the column and value.

        Returns:
            GenericColumnType: The result of the comparison.
        """
        ...

    @abstractmethod
    def is_iban(self, condition: cond.IsIban) -> GenericColumnType:
        """
        Check if a column value is a valid IBAN.

        Args:
            condition (cond.IsIban): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the IBAN check.
        """
        ...

    @abstractmethod
    def is_ip_address(self, condition: cond.IsIpAddress) -> GenericColumnType:
        """
        Check if a column value is a valid IP address.

        Args:
            condition (cond.IsIpAddress): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the IP address check.
        """
        ...

    @abstractmethod
    def is_email(self, condition: cond.IsEmail) -> GenericColumnType:
        """
        Check if a column value is a valid email address.

        Args:
            condition (cond.IsEmail): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the email address check.
        """
        ...

    @abstractmethod
    def is_unique(self, condition: cond.IsUnique) -> GenericColumnType:
        """
        Check if a column has unique values.

        Args:
            condition (cond.IsUnique): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the uniqueness check.
        """
        ...

    @abstractmethod
    def count_occurences(self, rule: R.CountOccurences) -> dict[Any, int]:
        """
        Count the occurrences of values in a column.

        Args:
            rule (R.CountOccurences): The rule specifying the column.

        Returns:
            dict[Any, int]: A dictionary mapping values to their occurrence counts.
        """
        ...

    def get_outliers_for_discrete_var(self, rule: R.CountOccurences) -> list[Any]:
        """
        Get outliers for a discrete variable based on occurrence counts.

        Args:
            rule (R.CountOccurences): The rule specifying the column.

        Returns:
            list[Any]: A list of outliers.
        """
        val_count = self.count_occurences(rule)
        counts = [val for val in val_count.values()]
        under_threshold = np.mean(counts) - (2 * np.std(counts))
        outliers = [key for key, val in val_count.items() if val < under_threshold]
        return outliers

    @abstractmethod
    def compute_percentile(self, rule: R.ComputePercentile) -> float:
        """
        Compute the percentile of a column.

        Args:
            rule (R.ComputePercentile): The rule specifying the column and percentile.

        Returns:
            float: The computed percentile value.
        """
        ...

    @abstractmethod
    def get_col_values_superior_to_constant(
        self, rule: R.GetColValuesSuperiorToConstant
    ) -> list[float]:
        """
        Get column values superior to a constant.

        Args:
            rule (R.GetColValuesSuperiorToConstant): The rule specifying the column and constant.

        Returns:
            list[float]: A list of column values superior to the constant.
        """
        ...

    @abstractmethod
    def get_col_values_inferior_to_constant(
        self, rule: R.GetColValuesInferiorToConstant
    ) -> list[float]:
        """
        Get column values inferior to a constant.

        Args:
            rule (R.GetColValuesInferiorToConstant): The rule specifying the column and constant.

        Returns:
            list[float]: A list of column values inferior to the constant.
        """
        ...

    def get_outliers_for_continuous_var(self, rule: R.GetOutliersForContinuousVar):
        """
        Get outliers for a continuous variable.

        Args:
            rule (R.GetOutliersForContinuousVar): The rule specifying the column.

        Returns:
            list[float]: A list of outliers.
        """
        first_quartile = self.compute_percentile(
            R.ComputePercentile(col_name=rule.col_name, percentile=rule.first_quartile)
        )
        third_quartile = self.compute_percentile(
            R.ComputePercentile(col_name=rule.col_name, percentile=rule.third_quartile)
        )
        iqr = third_quartile - first_quartile
        lw_limit_outliers_r = R.GetColValuesInferiorToConstant(
            col_name=rule.col_name, constant=first_quartile - 1.5 * iqr
        )
        up_limit_outliers_r = R.GetColValuesInferiorToConstant(
            col_name=rule.col_name, constant=third_quartile + 1.5 * iqr
        )
        lw_limit_outliers = self.get_col_values_inferior_to_constant(
            lw_limit_outliers_r
        )
        up_limit_outliers = self.get_col_values_superior_to_constant(
            up_limit_outliers_r
        )
        outliers = lw_limit_outliers + up_limit_outliers
        return outliers

    @abstractmethod
    def is_float(self, condition: cond.IsFloat) -> GenericColumnType:
        """
        Check if a column value is a float.

        Args:
            condition (cond.IsFloat): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the float check.
        """
        ...

    @abstractmethod
    def is_date(self, condition: cond.IsDate) -> GenericColumnType:
        """
        Check if a column value is a date.

        Args:
            condition (cond.IsDate): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the date check.
        """
        ...

    @abstractmethod
    def is_phone_number(self, condition: cond.IsPhoneNumber) -> GenericColumnType:
        """
        Check if a column value is a phone number.

        Args:
            condition (cond.IsPhoneNumber): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the phone number check.
        """
        ...

    @abstractmethod
    def is_boolean(self, condition: cond.IsBoolean) -> GenericColumnType:
        """
        Check if a column value is a boolean.

        Args:
            condition (cond.IsBoolean): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the boolean check.
        """
        ...

    @abstractmethod
    def is_integer(self, condition: cond.IsInteger) -> GenericColumnType:
        """
        Check if a column value is an integer.

        Args:
            condition (cond.IsInteger): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the integer check.
        """
        ...

    @abstractmethod
    def compare_column_to_value(
        self, condition: cond.CompareColumnToValue
    ) -> GenericColumnType:
        """
        Check a column compare to a value.

        Args:
            condition (cond.CompareColumnToValue): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the compare colum to a value.
        """
        ...

    @abstractmethod
    def compare_column_to_column(
        self, condition: cond.CompareColumnToColumn
    ) -> GenericColumnType:
        """
        Check a column compare to another column.

        Args:
            condition (cond.CompareColumnToColumn): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the compare colum to another column.
        """
        ...

    @abstractmethod
    def count_decimal_digit(
        self, condition: cond.CountDecimalDigit
    ) -> GenericColumnType:
        """
        Check the decimal precision of a column.

        Args:
            condition (cond.CountDecimalDigit): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the decimal precision of a column.
        """
        ...

    @abstractmethod
    def count_integer_digit(
        self, condition: cond.CountIntegerDigit
    ) -> GenericColumnType:
        """
        Check the integer precision of a column.

        Args:
            condition (cond.CountIntegerDigit): The condition specifying the column.

        Returns:
            GenericColumnType: The result of the integer precision of a column.
        """
        ...

    @abstractmethod
    def is_between(self, condition: cond.IsBetween) -> GenericColumnType:
        """
        Check the column is between the lower bound and upper bound.

        Args:
            condition (cond.cond.IsBetween): The condition specifying the column.

        Returns:
            GenericColumnType: The result of between check.
        """
        ...

    @abstractmethod
    def compare_length(self, condition: cond.CompareLength) -> GenericColumnType:
        """
        Check the length of character of column.

        Args:
            condition (cond.CompareLength): The condition specifying the column.

        Returns:
            GenericColumnType: The result of length check.
        """
        ...

    @abstractmethod
    def is_alphabetic(self, condition: cond.IsAlphabetic) -> GenericColumnType:
        """
        Check if the column value is alphabetic.

        Args:
            condition (cond.IsAlphabetic): The condition specifying the column.

        Returns:
            GenericColumnType: The result of alphabetic check.
        """
        ...

    @abstractmethod
    def is_negative(self, condition: cond.IsNegative) -> GenericColumnType:
        """
        Check if the column value is negative.

        Args:
            condition (cond.IsNegative): The condition specifying the column.

        Returns:
            GenericColumnType: The result of negative check.
        """
        ...

    @abstractmethod
    def is_positive(self, condition: cond.IsPositive) -> GenericColumnType:
        """
        Check if the column value is positive.

        Args:
            condition (cond.IsPositive): The condition specifying the column.

        Returns:
            GenericColumnType: The result of positive check.
        """
        ...

    @abstractmethod
    def aggregate_dataset(
        self, keys: list[str], agg_cols_expr: list[GenericAggExpr]
    ) -> GenericGroupedTableObject:
        """
        Generate the aggregate dataset.

        Args:
            keys (list[str]): The aggregation keys.
            agg_cols_expr: list[GenericAggExpr]: The aggregation expressions list.

        Returns:
            GenericAggExpr: The aggregate dataset.
        """
        ...