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

from __future__ import annotations

from typing import Any, List, Literal, Optional, TypeAlias, Union

from pydantic import BaseModel


class Condition(BaseModel):
    """
    Base class representing a condition.

    Attributes:
        is_aggregate (bool): Indicates if the condition is an aggregate condition.
                             Default is False.
    """

    is_aggregate: bool = False

    def __and__(self, other):
        """
        Logical AND operation between two conditions.

        Args:
            other (Condition): The other condition to AND with.

        Returns:
            AndCondition: A new AndCondition instance representing the logical AND of the two conditions.

        Raises:
            Exception: If `other` is not an instance of Condition.
                       If `self.is_aggregate` does not match `other.is_aggregate`.
        """
        if isinstance(other, Condition):
            if self.is_aggregate == other.is_aggregate:
                return AndCondition(
                    is_aggregate=self.is_aggregate, left=other, right=self
                )

            raise Exception(f"Cannot combine Condition with AggregateCondition")

        raise Exception(f"type of {other} is not a Condition")

    def __or__(self, other):
        """
        Logical OR operation between two conditions.

        Args:
            other (Condition): The other condition to OR with.

        Returns:
            OrCondition: A new OrCondition instance representing the logical OR of the two conditions.

        Raises:
            Exception: If `other` is not an instance of Condition.
                       If `self.is_aggregate` does not match `other.is_aggregate`.
        """
        if isinstance(other, Condition):
            if self.is_aggregate == other.is_aggregate:
                return OrCondition(
                    is_aggregate=self.is_aggregate, left=other, right=self
                )

            raise Exception(f"Cannot combine Condition with AggregateCondition")

        raise Exception(f"type of {other} is not a Condition")

    def __invert__(self):
        """
        Logical NOT operation.

        Returns:
            NotCondition: The NOT condition.

        Raises:
            Exception: If `self` is not an instance of Condition.
        """
        if isinstance(self, Condition):
            return NotCondition(cond=self)

        raise Exception(f"type of {self} is not a Condition")

    def __str__(self, level=0):
        """
        String representation of the condition.

        Args:
            level (int): The indentation level.

        Returns:
            str: The string representation of the condition.
        """
        return f"{self.__repr__()}"

    def get_conditions_as_func_check(self):
        if self.is_aggregate:
            if isinstance(self, AndCondition):
                return AndCondition(
                    left=self.left.get_conditions_as_func_check(),
                    right=self.right.get_conditions_as_func_check(),
                )
            elif isinstance(self, OrCondition):
                return OrCondition(
                    left=self.left.get_conditions_as_func_check(),
                    right=self.right.get_conditions_as_func_check(),
                )
            else:
                return self.get_func_check()


class AndCondition(Condition):
    """
    Represents a logical AND condition between two conditions.

    Attributes:
        left (Condition): The left condition.
        right (Condition): The right condition.
    """

    left: Condition
    right: Condition

    def __str__(self, level=0):
        """
        String representation of the AND condition.

        Args:
            level (int): The indentation level.

        Returns:
            str: The string representation of the AND condition.
        """
        space = "\t" * level
        tree = (
            f"+{self.__class__.__name__} \n"
            f"{space}|___{self.left.__str__(level=level+1)} \n"
            f"{space}|___{self.right.__str__(level=level+1)}"
        )

        return tree


class OrCondition(Condition):
    """
    Represents a logical OR condition between two conditions.

    Attributes:
        left (Condition): The left condition.
        right (Condition): The right condition.
    """

    left: Condition
    right: Condition

    def __str__(self, level=0):
        """
        String representation of the OR condition.

        Args:
            level (int): The indentation level.

        Returns:
            str: The string representation of the OR condition.
        """
        space = "\t" * level
        tree = (
            f"+{self.__class__.__name__} \n"
            f"{space}|___{self.left.__str__(level=level+1)} \n"
            f"{space}|___{self.right.__str__(level=level+1)}"
        )

        return tree


class NotCondition(Condition):
    """
    Represents a logical NOT condition.

    Attributes:
        cond (Condition): The condition to negate.
    """

    cond: Condition

    def __str__(self, level=0):
        """
        String representation of the NOT condition.

        Args:
            level (int): The indentation level.

        Returns:
            str: The string representation of the NOT condition.
        """
        space = "\t" * level
        tree = (
            f"+{self.__class__.__name__} \n"
            f"{space}|___{self.cond.__str__(level=level+1)}"
        )

        return tree


class IsNull(Condition):
    """
    Condition to check if a column value is null.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsNotNull(Condition):
    """
    Condition to check if a column value is not null.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsIn(Condition):
    """
    Condition to check if a column value is in a list of values.

    Args:
        col_name (str): The name of the column.
        list_of_values (list[Any]): The list of values to check against.
    """

    col_name: str
    list_of_values: list[Any]


class CompareYearToValue(Condition):
    """
    Condition to compare a column value to a specific year.

    Args:
        col_name (str): The name of the column.
        value (int): The value to compare against.
    """

    col_name: str
    operator: str
    value: int


class IsIban(Condition):
    """
    Condition to check if a column value is an IBAN.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsIpAddress(Condition):
    """
    Condition to check if a column value is an IP address.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsFloat(Condition):
    """
    Condition to check if a column value is a float.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsDate(Condition):
    """
    Condition to check if a column value is a date.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsPhoneNumber(Condition):
    """
    Condition to check if a column value is a phone number.

    Args:
        col_name (str): The name of the column.
        filter_per_country (Optional[List[Literal["fr", "be", "es", "pt", "gb", "it", "lu"]]]):
            Optional list of country codes to filter phone numbers for.
    """

    col_name: str
    filter_per_country: Optional[
        List[Literal["fr", "be", "es", "pt", "gb", "it", "lu"]]
    ] = None


class IsBoolean(Condition):
    """
    Condition to check if a column value is a boolean.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsEmail(Condition):
    """
    Condition to check if a column value is an email.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsInteger(Condition):
    """
    Condition to check if a column value is an integer.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsUnique(Condition):
    """
    Condition to check if all values in a column are unique.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class CompareColumnToValue(Condition):
    """
    Condition to compare a column to a value.

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        value (Any): The value to compare against.
    """

    col_name: str
    operator: str
    value: Any


class CompareColumnToColumn(Condition):
    """
    Condition to compare a column to another column.

    Args:
        col_left (str): The name of the left column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        col_right (str): The name of the right column.
    """

    col_left: str
    operator: str
    col_right: str


class CountDecimalDigit(Condition):
    """
    Condition to compare the number of decimal digits in a column.

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        digit (int): The number of decimal digits to compare against.
    """

    col_name: str
    operator: str
    digit: int


class CountIntegerDigit(Condition):
    """
    Condition to compare the number of integer digits in a column.

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        digit (int): The number of integer digits to compare against.
    """

    col_name: str
    operator: str
    digit: int


class IsBetween(Condition):
    """
    Condition to check if the column values are between the given lower bound and upper bound.

    Args:
        col_name (str): The name of the column.
        min_value (Any): Lower bound
        max_value (Any): Upper bound
    """

    col_name: str
    min_value: Any
    max_value: Any


class CompareLength(Condition):
    """
    Condition to compare the length of the column values.

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        length (int): The length to compare against.
    """

    col_name: str
    operator: str
    length: int


class IsAlphabetic(Condition):
    """
    Condition to check if a column value is alphabetic.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str
    col_name: str


class IsPositive(Condition):
    """
    Condition to check if a column value is positive.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


class IsNegative(Condition):
    """
    Condition to check if a column value is negative.

    Args:
        col_name (str): The name of the column.
    """

    col_name: str


# TODO: rendre dynamique la création
ConditionExpression: TypeAlias = Union[
    IsBoolean,
    IsDate,
    IsEmail,
    IsFloat,
    IsIban,
    IsIn,
    IsInteger,
    IsIpAddress,
    IsNull,
    IsNotNull,
    IsPhoneNumber,
    IsUnique,
    CompareColumnToValue,
    CompareColumnToColumn,
    CountDecimalDigit,
    CountIntegerDigit,
    IsBetween,
    CompareLength,
    IsAlphabetic,
    IsPositive,
    IsNegative,
]
