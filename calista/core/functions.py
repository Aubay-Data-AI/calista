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

from typing import Any, List

import calista.core._aggregate_conditions as agg_cond
import calista.core._conditions as cond
from calista.core._conditions import ConditionExpression


def is_null(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column is null.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column is null.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_null
    >>>
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, None, 3, None]})
    >>> my_rule = is_null(col_name="ID")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsNull(col_name=col_name)


def is_not_null(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column is not null.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column is not null.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_not_null
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, None, 3, None]})
    >>> my_rule = is_not_null(col_name="ID")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsNotNull(col_name=col_name)


def is_in(col_name: str, list_of_values: list[Any]) -> ConditionExpression:
    """
    Create a condition to check if a column value is in a list of values.

    Args:
        col_name (str): The name of the column.
        list_of_values (list[Any]): The list of values to check against.

    Returns:
        ConditionExpression: The condition to check if the column value is in the list of values.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_in
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, None, 3, None]})
    >>> my_rule = is_in(col_name="ID", list_of_values=[1,5])
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsIn(col_name=col_name, list_of_values=list_of_values)


def compare_year_to_value(
    col_name: str, operator: str, value: int
) -> ConditionExpression:
    """
    Create a condition to compare a column value to a specific year.

    Args:
        col_name (str): The name of the column.
        value (int): The value to compare against.

    Returns:
        ConditionExpression: The condition to compare the column value to the specific year.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import compare_year_to_value   
    >>> from datetime import datetime
    >>>
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"DATE": [datetime.date(2024, 4, 11), datetime.date(2024, 5, 11), None, None]})
    >>> my_rule = compare_year_to_value(col_name="DATE", operator="<=", value=2026)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.CompareYearToValue(col_name=col_name, operator=operator, value=value)


def is_ip_address(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is an IP address.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column value is an IP address.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_ip_address
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"IP_ADDRESS": ["192.168.1.1", None, None, "192.168.2.1"]})
    >>> my_rule = is_ip_address(col_name="IP_ADDRESS")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsIpAddress(col_name=col_name)


def is_float(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is a float.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column value is a float.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_float
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 6000.34, 2345.3, None]})
    >>> my_rule = is_float(col_name="NUMBER")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 3
    valid_row_count_pct : 75.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsFloat(col_name=col_name)


def is_date(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is a date.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column value is a date.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_date 
    >>> from datetime import datetime
    >>>
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"DATE": datetime.date(2024, 4, 11), datetime.date(2024, 5, 11), None, None})
    >>> my_rule = is_date(col_name="DATE")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsDate(col_name=col_name)


def is_phone_number(
    col_name: str, filter_per_country: List[str] = None
) -> ConditionExpression:
    """
    Create a condition to check if a column value is a phone number.

    Args:
        col_name (str): The name of the column.
        filter_per_country (List[str], optional): List of country codes to filter phone numbers for.

    Returns:
        ConditionExpression: The condition to check if the column value is a phone number.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_phone_number
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PHONE_NUMBER": ["0666994412", "0763494412", None, "0460994412"]})
    >>> my_rule = is_phone_number(col_name="PHONE_NUMBER")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 3
    valid_row_count_pct : 75.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsPhoneNumber(col_name=col_name, filter_per_country=filter_per_country)


def is_boolean(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is a boolean.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column value is a boolean.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_boolean
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"BOOL": [True, None, True, None]})
    >>> my_rule = is_boolean(col_name="BOOL")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsBoolean(col_name=col_name)


def is_email(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is an email.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column value is an email.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_email
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"EMAIL": ["test@aubay.com", None, None, "test1@aubay.com"]})
    >>> my_rule = is_email(col_name="EMAIL")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsEmail(col_name=col_name)


def is_integer(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is an integer.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column value is an integer.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_integer
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, None, 3, None]})
    >>> my_rule = is_integer(col_name="ID")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsInteger(col_name=col_name)


def is_iban(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is an IBAN.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if the column value is an IBAN.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_iban
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"IBAN": ["FR7612548029989876543210917", "FR7630003035409876543210925", "None", None]})
    >>> my_rule = is_iban(col_name="IBAN")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsIban(col_name=col_name)


def is_unique(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if all values in a column are unique.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if all values in the column are unique.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_unique
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 2]})
    >>> my_rule = is_unique(col_name="ID")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsUnique(col_name=col_name)


def compare_column_to_value(
    col_name: str, operator: str, value: Any
) -> ConditionExpression:
    """
    Create a condition to compare a column to a value.

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        value (Any): The value to compare against.

    Returns:
        ConditionExpression: The condition to compare a column to a value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import compare_column_to_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 6000.34, 2345.3, None]})
    >>> my_rule = compare_column_to_value(col_name="NUMBER", operator=">=", value=2345)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.CompareColumnToValue(col_name=col_name, operator=operator, value=value)


def compare_column_to_column(
    col_left: str, operator: str, col_right: str
) -> ConditionExpression:
    """
    Create a condition to compare a column to another column.

    Args:
        col_left (str): The name of the left column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        col_right (str): The name of the right column.

    Returns:
        ConditionExpression: The condition to compare a column to a column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import compare_column_to_column
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4],
    >>>                                                        "ID_2": [2, 1, 3, 5]})
    >>> my_rule = compare_column_to_column(col_left="ID", operator=">=", col_right="ID_2")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.CompareColumnToColumn(
        col_left=col_left, operator=operator, col_right=col_right
    )


def count_decimal_digit(
    col_name: str, operator: str, digit: int
) -> ConditionExpression:
    """
    Create a condition to compare the number of decimal digits in a column

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        digit (int): The number of decimal digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of decimal digits in a column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import count_decimal_digit
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [1.2, 1.23, 1.234, 21.2]})
    >>> my_rule = count_decimal_digit(col_name="NUMBER", operator="=", digit=1)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.CountDecimalDigit(col_name=col_name, operator=operator, digit=digit)


def count_integer_digit(
    col_name: str, operator: str, digit: int
) -> ConditionExpression:
    """
    Create a condition to compare the number of integer digits in a column.

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        digit (int): The number of integer digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of integer digits in a column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import count_integer_digit
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [1.2, 1.23, 1.234, 21.2]})
    >>> my_rule = count_integer_digit(col_name="NUMBER")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule)
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 3
    valid_row_count_pct : 75.0
    timestamp : 2024-01-01 00:00:00.000000
    """

    return cond.CountIntegerDigit(col_name=col_name, operator=operator, digit=digit)


def is_between(col_name: str, min_value: Any, max_value: Any) -> ConditionExpression:
    """
    Create a condition to check if the column values are between the given lower bound and upper bound.

    Args:
        col_name (str): The name of the column.
        min_value (Any): Lower bound
        max_value (Any): Upper bound

    Returns:
        ConditionExpression: The condition to check if the column values are between the given lower bound and upper bound.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_between
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4]})
    >>> my_rule = is_between(col_name="ID", min_value=2, max_value=3
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """

    return cond.IsBetween(col_name=col_name, min_value=min_value, max_value=max_value)


def compare_length(col_name: str, operator: str, length: int) -> ConditionExpression:
    """
    Create a condition to compare the length of the column values.

    Args:
        col_name (str): The name of the column.
        operator (str): A comparison operator : "=", "!=", "<", "<=", ">", ">="
        length (int): The length to compare against.

    Returns:
        ConditionExpression: The condition to compare the length of the column values.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import compare_length
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PLANETE": ["mars", None, "jupiter", "terre"]})
    >>> my_rule = compare_length(col_name="PLANETE", operator=">=", length=5)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """

    return cond.CompareLength(col_name=col_name, operator=operator, length=length)


def is_alphabetic(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is alphabetic.

    Args:
        col_name (str): The name of the column.

    Returns:
        ConditionExpression: The condition to check if a column value is alphabetic.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_alphabetic
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PLANETE": ["mars", None, "jupiter", "terre"]})
    >>> my_rule = is_alphabetic(col_name="PLANETE")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 3
    valid_row_count_pct : 75.0
    timestamp : 2024-01-01 00:00:00.000000
    """

    return cond.IsAlphabetic(col_name=col_name)


def is_positive(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is positive.

    Args:
        col_name (str): The name of the column.

    Returns:
            ConditionExpression: The condition to check if a column value is positive.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_positive
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [1, 2, -3, -4]})
    >>> my_rule = is_positive(col_name="NUMBER")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsPositive(col_name=col_name)


def is_negative(col_name: str) -> ConditionExpression:
    """
    Create a condition to check if a column value is negative.

    Args:
        col_name (str): The name of the column.

    Returns:
            ConditionExpression: The condition to check if a column value is negative.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import is_negative
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [1, 2, -3, -4]})
    >>> my_rule = is_negative(col_name="NUMBER")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return cond.IsNegative(col_name=col_name)


#############################################################################
############### Comparison functions that use above functions ###############
#############################################################################


def year_equal_to_value(col_name: str, value: int) -> ConditionExpression:
    """
    Create a condition to check if the year of a date in a column is equal to a specific year.

    Args:
        col_name (str): The name of the column.
        value (int): The value to compare against.

    Returns:
        ConditionExpression: The condition to to check if the year of a date in a column is equal to a specific year.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import year_equal_to_value 
    >>> from datetime import datetime
    >>>
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict("DATE": [datetime.date(2023, 4, 11), datetime.date(2024, 5, 11), None, None],)
    >>> my_rule = year_equal_to_value(col_name="DATE", value=2023)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_year_to_value(col_name=col_name, operator="=", value=value)


def year_lt_value(col_name: str, value: int) -> ConditionExpression:
    """
    Create a condition to check if the year of a date in a column is lower than a specific year.

    Args:
        col_name (str): The name of the column.
        value (int): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if the year of a date in a column is lower than a specific year.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import year_lt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict("DATE": [datetime.date(2023, 4, 11), datetime.date(2024, 5, 11), None, None],)
    >>> my_rule = year_lt_value(col_name="DATE", value=2024)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_year_to_value(col_name=col_name, operator="<", value=value)


def year_le_value(col_name: str, value: int) -> ConditionExpression:
    """
    Create a condition to check if the year of a date in a column is lower or equal to a specific year.

    Args:
        col_name (str): The name of the column.
        value (int): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if the year of a date in a column is lower or equal to a specific year.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import year_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict("DATE": [datetime.date(2023, 4, 11), datetime.date(2024, 5, 11), None, None],)
    >>> my_rule = year_le_value(col_name="DATE", value=2024)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_year_to_value(col_name=col_name, operator="<=", value=value)


def year_gt_value(col_name: str, value: int) -> ConditionExpression:
    """
    Create a condition to check if the year of a date in a column is greater than a specific year.

    Args:
        col_name (str): The name of the column.
        value (int): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if the year of a date in a column is greater than a specific year.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import year_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict("DATE": [datetime.date(2023, 4, 11), datetime.date(2024, 5, 11), None, None],)
    >>> my_rule = year_gt_value(col_name="DATE", value=2023)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_year_to_value(col_name=col_name, operator=">", value=value)


def year_ge_value(col_name: str, value: int) -> ConditionExpression:
    """
    Create a condition to check if the year of a date in a column is greater or equal to a specific year.

    Args:
        col_name (str): The name of the column.
        value (int): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if the year of a date in a column is greater or equal to a specific year.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import year_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict("DATE": [datetime.date(2023, 4, 11), datetime.date(2024, 5, 11), None, None],)
    >>> my_rule = year_ge_value(col_name="DATE", value=2023)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_year_to_value(col_name=col_name, operator=">=", value=value)


def column_equal_to_value(col_name: str, value: Any) -> ConditionExpression:
    """
    Create a condition to check if a column is equal to a given value.

    Args:
        col_name (str): The name of the column.
        value (Any): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if a column is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_equal_to_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4]})
    >>> my_rule = column_equal_to_value(col_name="ID", value=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_value(col_name=col_name, operator="=", value=value)


def column_lt_value(col_name: str, value: Any) -> ConditionExpression:
    """
    Create a condition to check if a column is lower than a given value.

    Args:
        col_name (str): The name of the column.
        value (Any): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if a column is lower than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_lt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4]})
    >>> my_rule = column_lt_value(col_name="ID", value=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_value(col_name=col_name, operator="<", value=value)


def column_le_value(col_name: str, value: Any) -> ConditionExpression:
    """
    Create a condition to check if a column is lower or equal to a given value.

    Args:
        col_name (str): The name of the column.
        value (Any): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if a column is lower or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4]})
    >>> my_rule = column_le_value(col_name="ID", value=2)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_value(col_name=col_name, operator="<=", value=value)


def column_gt_value(col_name: str, value: Any) -> ConditionExpression:
    """
    Create a condition to check if a column is greater than a given value.

    Args:
        col_name (str): The name of the column.
        value (Any): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if a column is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4]})
    >>> my_rule = column_gt_value(col_name="ID", value=2)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_value(col_name=col_name, operator=">", value=value)


def column_ge_value(col_name: str, value: Any) -> ConditionExpression:
    """
    Create a condition to check if a column is greater or equal to a given value.

    Args:
        col_name (str): The name of the column.
        value (Any): The value to compare against.

    Returns:
        ConditionExpression: The condition to check if a column is greater or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4]})
    >>> my_rule = column_ge_value(col_name="ID", value=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_value(col_name=col_name, operator=">=", value=value)


def column_equal_to_column(col_left: str, col_right: str) -> ConditionExpression:
    """
    Create a condition to check if a column is equal to another column.

    Args:
        col_left (str): The name of the left column.
        col_right (str): The name of the right column.

    Returns:
        ConditionExpression: The condition to check if a column is equal to another column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_equal_to_column
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [10, 4, 5, 2],
    >>>                                                        "ID_2": [8, 4, 1, 3]})
    >>> my_rule = column_equal_to_column(col_left="ID", col_right="ID_2")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_column(
        col_left=col_left, operator="=", col_right=col_right
    )


def column_lt_column(col_left: str, col_right: str) -> ConditionExpression:
    """
    Create a condition to check if a column is lower than another column.

    Args:
        col_left (str): The name of the left column.
        col_right (str): The name of the right column.

    Returns:
        ConditionExpression: The condition to check if a column is lower than another column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_lt_column
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [10, 4, 5, 2],
    >>>                                                         "ID_2": [8, 4, 1, 3]})
    >>> my_rule = column_lt_column(col_left="ID", col_right="ID_2")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_column(
        col_left=col_left, operator="<", col_right=col_right
    )


def column_le_column(col_left: str, col_right: str) -> ConditionExpression:
    """
    Create a condition to check if a column is lower or equal to another column.

    Args:
        col_left (str): The name of the left column.
        col_right (str): The name of the right column.

    Returns:
        ConditionExpression: The condition to check if a column is lower or equal to another column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_le_column
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [10, 4, 5, 2],
    >>>                                                         "ID_2": [8, 4, 1, 3]})
    >>> my_rule = column_le_column(col_left="ID", col_right="ID_2")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_column(
        col_left=col_left, operator="<=", col_right=col_right
    )


def column_gt_column(col_left: str, col_right: str) -> ConditionExpression:
    """
    Create a condition to check if a column is greater than another column.

    Args:
        col_left (str): The name of the left column.
        col_right (str): The name of the right column.

    Returns:
        ConditionExpression: The condition to check if a column is greater than another column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_gt_column
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [10, 4, 5, 2],
    >>>                                                         "ID_2": [8, 4, 1, 3]})
    >>> my_rule = column_gt_column(col_left="ID", col_right="ID_2")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_column(
        col_left=col_left, operator=">", col_right=col_right
    )


def column_ge_column(col_left: str, col_right: str) -> ConditionExpression:
    """
    Create a condition to check if a column is greater or equal to another column.

    Args:
        col_left (str): The name of the left column.
        col_right (str): The name of the right column.

    Returns:
        ConditionExpression: The condition to check if a column is greater or equal to another column.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import column_ge_column
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [10, 4, 5, 2],
    >>>                                                         "ID_2": [8, 4, 1, 3]})
    >>> my_rule = column_ge_column(col_left="ID", col_right="ID_2")
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_column_to_column(
        col_left=col_left, operator=">=", col_right=col_right
    )


def decimal_digit_eq(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of decimal is equal to a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of decimal digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of decimal is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import decimal_digit_eq
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = decimal_digit_eq(col_name="NUMBER", digit=2)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_decimal_digit(col_name=col_name, operator="=", digit=digit)


def decimal_digit_lt(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of decimal is lower than a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of decimal digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of decimal is lower than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import decimal_digit_lt
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = decimal_digit_lt(col_name="NUMBER", digit=2)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_decimal_digit(col_name=col_name, operator="<", digit=digit)


def decimal_digit_le(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of decimal is lower or equal a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of decimal digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of decimal is lower or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import decimal_digit_le
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = decimal_digit_le(col_name="NUMBER", digit=2)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_decimal_digit(col_name=col_name, operator="<=", digit=digit)


def decimal_digit_gt(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of decimal is greater than a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of decimal digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of decimal is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import decimal_digit_gt
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = decimal_digit_gt(col_name="NUMBER", digit=2)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_decimal_digit(col_name=col_name, operator=">", digit=digit)


def decimal_digit_ge(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of decimal is greater or equal to a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of decimal digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of decimal is greater or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import decimal_digit_ge
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = decimal_digit_ge(col_name="NUMBER", digit=2)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_decimal_digit(col_name=col_name, operator=">=", digit=digit)


def integer_digit_equal(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of integer is equal to a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of integer digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of integer is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import integer_digit_equal
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = integer_digit_equal(col_name="NUMBER", digit=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_integer_digit(col_name=col_name, operator="=", digit=digit)


def integer_lt_digit(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of integer is lower than a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of integer digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of integer is lower than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import integer_lt_digit
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = integer_lt_digit(col_name="NUMBER", digit=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_integer_digit(col_name=col_name, operator="<", digit=digit)


def integer_digit_le(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of integer is lower or equal a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of integer digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of integer is lower or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import integer_digit_le
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({{"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = integer_digit_le(col_name="NUMBER", digit=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_integer_digit(col_name=col_name, operator="<=", digit=digit)


def integer_digit_gt(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of integer is greater than a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of integer digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of integer is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import integer_digit_gt
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = integer_digit_gt(col_name="NUMBER", digit=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_integer_digit(col_name=col_name, operator=">", digit=digit)


def integer_digit_ge(col_name: str, digit: int) -> ConditionExpression:
    """
    Create a condition to check if the number of integer is greater or equal to a given value.

    Args:
        col_name (str): The name of the column.
        digit (int): The number of integer digits to compare against.

    Returns:
        ConditionExpression: The condition to compare the number of integer is greater or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import integer_digit_ge
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"NUMBER": [2344.324, 600.34, 45.3, None]})
    >>> my_rule = integer_digit_ge(col_name="NUMBER", digit=3)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return count_integer_digit(col_name=col_name, operator=">=", digit=digit)


def length_eq(col_name: str, length: int) -> ConditionExpression:
    """
    Create a condition to check if the values of length is equal to a given value.

    Args:
        col_name (str): The name of the column.
        length (int): The length to compare against.

    Returns:
        ConditionExpression: The result of the values of length is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import length_eq
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PLANETE": ["mars", None, "jupiter", "terre"]})
    >>> my_rule = length_eq(col_name="PLANETE", length=4)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_length(col_name=col_name, operator="=", length=length)


def length_lt(col_name: str, length: int) -> ConditionExpression:
    """
    Create a condition to check if the values of length is lower than a given value.

    Args:
        col_name (str): The name of the column.
        length (int): The length to compare against.

    Returns:
        ConditionExpression: The result of the values of length is lower than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import length_lt
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PLANETE": ["mars", None, "jupiter", "terre"]})
    >>> my_rule = length_lt(col_name="PLANETE", length=4)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 0
    valid_row_count_pct : 0.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_length(col_name=col_name, operator="<", length=length)


def length_le(col_name: str, length: int) -> ConditionExpression:
    """
    Create a condition to check if the values of length is lower or equal to a given value.

    Args:
        col_name (str): The name of the column.
        length (int): The length to compare against.

    Returns:
        ConditionExpression: The result of the values of length is lower or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import length_le
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PLANETE": ["mars", None, "jupiter", "terre"]})
    >>> my_rule = length_le(col_name="PLANETE", length=4)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 1
    valid_row_count_pct : 25.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_length(col_name=col_name, operator="<=", length=length)


def length_gt(col_name: str, length: int) -> ConditionExpression:
    """
    Create a condition to check if the values of length is greater than a given value.

    Args:
        col_name (str): The name of the column.
        length (int): The length to compare against.

    Returns:
        ConditionExpression: The result of the values of length is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import length_gt
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PLANETE": ["mars", None, "jupiter", "terre"]})
    >>> my_rule = length_gt(col_name="PLANETE", length=4)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_length(col_name=col_name, operator=">", length=length)


def length_ge(col_name: str, length: int) -> ConditionExpression:
    """
    Create a condition to check if the values of length is greater or equal to a given value.

    Args:
        col_name (str): The name of the column.
        length (int): The length to compare against.

    Returns:
        ConditionExpression: The result of the values of length is greater or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import length_ge
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"PLANETE": ["mars", None, "jupiter", "terre"]})
    >>> my_rule = length_ge(col_name="PLANETE", length=4)
    >>> print(calista_table.analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 3
    valid_row_count_pct : 75.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return compare_length(col_name=col_name, operator=">=", length=length)


def sum_eq_value(col_name: str, value: Any) -> agg_cond.SumBy:
    """
    Create a condition to compare if the sum of a column is equal to a given value.

    Args:
        col_name (str): The name of the column to sum.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the sum is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import sum_eq_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = sum_eq_value(col_name="POINTS", value=60)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.SumBy(col_name=col_name, operator="=", agg_ope="sum", value=value)


def sum_lt_value(col_name: str, value: Any) -> agg_cond.SumBy:
    """
    Create a condition to compare if the sum of a column is less than a given value.

    Args:
        col_name (str): The name of the column to sum.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the sum is less than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import sum_lt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = sum_lt_value(col_name="POINTS", value=70)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.SumBy(col_name=col_name, operator="<", agg_ope="sum", value=value)


def sum_le_value(col_name: str, value: Any) -> agg_cond.SumBy:
    """
    Create a condition to compare if the sum of a column is less than or equal to a given value.

    Args:
        col_name (str): The name of the column to sum.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the sum is less than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import sum_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = sum_le_value(col_name="POINTS", value=60)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.SumBy(col_name=col_name, operator="<=", agg_ope="sum", value=value)


def sum_gt_value(col_name: str, value: Any) -> agg_cond.SumBy:
    """
    Create a condition to compare if the sum of a column is greater than a given value.

    Args:
        col_name (str): The name of the column to sum.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the sum is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import sum_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = sum_gt_value(col_name="POINTS", value=60)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.SumBy(col_name=col_name, operator=">", agg_ope="sum", value=value)


def sum_ge_value(col_name: str, value: Any) -> agg_cond.SumBy:
    """
    Create a condition to compare if the sum of a column is greater than or equal to a given value.

    Args:
        col_name (str): The name of the column to sum.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the sum is greater than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import sum_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = sum_ge_value(col_name="POINTS", value=60)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.SumBy(col_name=col_name, operator=">=", agg_ope="sum", value=value)


def count_eq_value(col_name: str, value: Any) -> agg_cond.CountBy:
    """
    Create a condition to compare if the number of elements of a column is equal to a given value.

    Args:
        col_name (str): The name of the column to count.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the number of elements is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import count_eq_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [10, 20, 30, 40, 20, 10,50]})
    >>> my_rule = count_eq_value(col_name="TEAM", value=3)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 4
    valid_row_count : 2
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.CountBy(
        col_name=col_name, operator="=", agg_ope="count", value=value
    )


def count_lt_value(col_name: str, value: Any) -> agg_cond.CountBy:
    """
    Create a condition to compare if the number of elements of a column is less than a given value.

    Args:
        col_name (str): The name of the column to count.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the number of elements is less than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import count_lt_value
    >>>     
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [10, 20, 30, 40, 20, 10,50]})
    >>> my_rule = count_lt_value(col_name="TEAM", value=3)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 0
    valid_row_count_pct : 0.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.CountBy(
        col_name=col_name, operator="<", agg_ope="count", value=value
    )


def count_le_value(col_name: str, value: Any) -> agg_cond.CountBy:
    """
    Create a condition to compare if the number of elements of a column is less than or equal to a given value.

    Args:
        col_name (str): The name of the column to count.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the number of elements is less than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import count_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [10, 20, 30, 40, 20, 10,50]})
    >>> my_rule = count_le_value(col_name="TEAM", value=3)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.CountBy(
        col_name=col_name, operator="<=", agg_ope="count", value=value
    )


def count_gt_value(col_name: str, value: Any) -> agg_cond.CountBy:
    """
    Create a condition to compare if the number of elements of a column is greater than a given value.

    Args:
        col_name (str): The name of the column to count.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the number of elements is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import count_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [10, 20, 30, 40, 20, 10,50]})
    >>> my_rule = count_gt_value(col_name="TEAM", value=3)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000
    """
    return agg_cond.CountBy(
        col_name=col_name, operator=">", agg_ope="count", value=value
    )


def count_ge_value(col_name: str, value: Any) -> agg_cond.CountBy:
    """
    Create a condition to compare if the number of elements of a column is greater than or equal to a given value.

    Args:
        col_name (str): The name of the column to count.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the number of elements is greater than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import count_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [10, 20, 30, 40, 20, 10,50]})
    >>> my_rule = count_ge_value(col_name="TEAM", value=3)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.CountBy(
        col_name=col_name, operator=">=", agg_ope="count", value=value
    )


def mean_eq_value(col_name: str, value: Any) -> agg_cond.MeanBy:
    """
    Create a condition to compare if the average of a column is equal to a value.

    Args:
        col_name (str): The name of the column to average.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the average is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import mean_eq_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = mean_eq_value(col_name="TEAM", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MeanBy(col_name=col_name, operator="=", agg_ope="mean", value=value)


def mean_lt_value(col_name: str, value: Any) -> agg_cond.MeanBy:
    """
    Create a condition to compare if the average of a column is less than a value.

    Args:
        col_name (str): The name of the column to average.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the average is less than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import mean_lt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = mean_lt_value(col_name="TEAM", value=25)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MeanBy(col_name=col_name, operator="<", agg_ope="mean", value=value)


def mean_le_value(col_name: str, value: Any) -> agg_cond.MeanBy:
    """
    Create a condition to compare if the average of a column is less than or equal to a value.

    Args:
        col_name (str): The name of the column to average.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the average is less than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import mean_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = mean_le_value(col_name="TEAM", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MeanBy(
        col_name=col_name, operator="<=", agg_ope="mean", value=value
    )


def mean_gt_value(col_name: str, value: Any) -> agg_cond.MeanBy:
    """
    Create a condition to compare if the average of a column is greater than a value.

    Args:
        col_name (str): The name of the column to average.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the average is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import mean_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = mean_gt_value(col_name="TEAM", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MeanBy(col_name=col_name, operator=">", agg_ope="mean", value=value)


def mean_ge_value(col_name: str, value: Any) -> agg_cond.MeanBy:
    """
    Create a condition to compare if the average of a column is greater than or equal to a value.

    Args:
        col_name (str): The name of the column to average.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the average is greater than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import mean_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = mean_ge_value(col_name="TEAM", value=30)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MeanBy(
        col_name=col_name, operator=">=", agg_ope="mean", value=value
    )


def median_eq_value(col_name: str, value: Any) -> agg_cond.MedianBy:
    """
    Create a condition to compare if the median of a column is equal to a value.

    Args:
        col_name (str): The name of the column onto compute the median.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the median is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import median_eq_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = median_eq_value(col_name="TEAM", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MedianBy(
        col_name=col_name, operator="=", agg_ope="median", value=value
    )


def median_lt_value(col_name: str, value: Any) -> agg_cond.MedianBy:
    """
    Create a condition to compare if the median of a column is lower than a value.

    Args:
        col_name (str): The name of the column onto compute the median.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the median is lower than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import median_lt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = median_lt_value(col_name="TEAM", value=25)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 0
    valid_row_count_pct : 0.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MedianBy(
        col_name=col_name, operator="<", agg_ope="median", value=value
    )


def median_le_value(col_name: str, value: Any) -> agg_cond.MedianBy:
    """
    Create a condition to compare if the median of a column is lower than or equal to a value.

    Args:
        col_name (str): The name of the column onto compute the median.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the median is lower than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import median_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = median_le_value(col_name="TEAM", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MedianBy(
        col_name=col_name, operator="<=", agg_ope="median", value=value
    )


def median_gt_value(col_name: str, value: Any) -> agg_cond.MedianBy:
    """
    Create a condition to compare if the median of a column is greater than a value.

    Args:
        col_name (str): The name of the column onto compute the median.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the median is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import median_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = median_gt_value(col_name="TEAM", value=15)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MedianBy(
        col_name=col_name, operator=">", agg_ope="median", value=value
    )


def median_ge_value(col_name: str, value: Any) -> agg_cond.MedianBy:
    """
    Create a condition to compare if the median of a column is greater than or equal to a value.

    Args:
        col_name (str): The name of the column onto compute the median.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the median is greater than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import median_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = median_ge_value(col_name="TEAM", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MedianBy(
        col_name=col_name, operator=">=", agg_ope="median", value=value
    )


def min_eq_value(col_name: str, value: Any) -> agg_cond.MinBy:
    """
    Create a condition to compare if the minimum of a column is equal to a value.

    Args:
        col_name (str): The name of the column onto compute the min.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the minimum is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import min_eq_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                    "POINTS": [5, 20, 30, 40, 20, 10]})
    >>> my_rule = min_eq_value(col_name="TEAM", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MinBy(col_name=col_name, operator="=", agg_ope="min", value=value)


def min_lt_value(col_name: str, value: Any) -> agg_cond.MinBy:
    """
    Create a condition to compare if the minimum of a column is lower than a value.

    Args:
        col_name (str): The name of the column onto compute the min.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the minimum is lower than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import min_lt_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [5, 20, 30, 40, 20, 10]})
    >>> my_rule = min_lt_value(col_name="POINTS", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 0
    valid_row_count_pct : 0.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MinBy(col_name=col_name, operator="<", agg_ope="min", value=value)


def min_le_value(col_name: str, value: Any) -> agg_cond.MinBy:
    """
    Create a condition to compare if the minimum of a column is lower than or equal to a value.

    Args:
        col_name (str): The name of the column onto compute the min.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the minimum is lower than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import min_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [5, 20, 30, 40, 20, 10]})
    >>> my_rule = min_le_value(col_name="POINTS", value=20)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MinBy(col_name=col_name, operator="<=", agg_ope="min", value=value)


def min_gt_value(col_name: str, value: Any) -> agg_cond.MinBy:
    """
    Create a condition to compare if the minimum of a column is greater than a value.

    Args:
        col_name (str): The name of the column onto compute the min.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the minimum is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import min_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [5, 20, 30, 40, 20, 10]})
    >>> my_rule = min_gt_value(col_name="POINTS", value=10)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MinBy(col_name=col_name, operator=">", agg_ope="min", value=value)


def min_ge_value(col_name: str, value: Any) -> agg_cond.MinBy:
    """
    Create a condition to compare if the minimum of a column is greater than or equal to a value.

    Args:
        col_name (str): The name of the column onto compute the min.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the minimum is greater than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import min_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine="pandas").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                      "POINTS": [5, 20, 30, 40, 20, 10]})
    >>> my_rule = min_ge_value(col_name="POINTS", value=10)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MinBy(col_name=col_name, operator=">=", agg_ope="min", value=value)


def max_eq_value(col_name: str, value: Any) -> agg_cond.MaxBy:
    """
    Create a condition to compare if the maximum of a column is equal to a value.

    Args:
        col_name (str): The name of the column onto compute the max.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the maximum is equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import max_eq_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = max_eq_value(col_name="TEAM", value=30)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MaxBy(col_name=col_name, operator="=", agg_ope="max", value=value)


def max_lt_value(col_name: str, value: Any) -> agg_cond.MaxBy:
    """
    Create a condition to compare if the maximum of a column is lower than a value.

    Args:
        col_name (str): The name of the column onto compute the max.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the maximum is lower than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import max_lt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = max_lt_value(col_name="TEAM", value=40)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MaxBy(col_name=col_name, operator="<", agg_ope="max", value=value)


def max_le_value(col_name: str, value: Any) -> agg_cond.MaxBy:
    """
    Create a condition to compare if the maximum of a column is lower than or equal to a value.

    Args:
        col_name (str): The name of the column onto compute the max.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the maximum is lower than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import max_le_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = max_le_value(col_name="TEAM", value=40)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MaxBy(col_name=col_name, operator="<=", agg_ope="max", value=value)


def max_gt_value(col_name: str, value: Any) -> agg_cond.MaxBy:
    """
    Create a condition to compare if the maximum of a column is greater than a value.

    Args:
        col_name (str): The name of the column onto compute the max.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the maximum is greater than a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import max_gt_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = max_gt_value(col_name="TEAM", value=30)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MaxBy(col_name=col_name, operator=">", agg_ope="max", value=value)


def max_ge_value(col_name: str, value: Any) -> agg_cond.MaxBy:
    """
    Create a condition to compare if the maximum of a column is greater than or equal to a value.

    Args:
        col_name (str): The name of the column onto compute the max.
        value (Any): The value to compare against.

    Returns:
        AggregateCondition: The aggregate condition to check if the maximum is greater than or equal to a given value.

    Example
    --------
    >>> from calista import CalistaEngine
    >>> from calista.core.functions import max_ge_value
    >>>    
    >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"TEAM": ["red", "red", "red", "blue", "blue", "blue"],
    >>>                                                        "POINTS": [10, 20, 30, 40, 20, 10]})
    >>> my_rule = max_ge_value(col_name="TEAM", value=30)
    >>> print(calista_table.groupBy("TEAM").analyze(rule_name="My Rule Name", condition=my_rule))
    rule_name : My Rule Name
    total_row_count : 2
    valid_row_count : 2
    valid_row_count_pct : 100.0
    timestamp : 2024-01-01 00:00:00.000000

    """
    return agg_cond.MaxBy(col_name=col_name, operator=">=", agg_ope="max", value=value)
