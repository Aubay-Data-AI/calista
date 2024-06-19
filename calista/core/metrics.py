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


from pydantic import BaseModel


class Metrics(BaseModel):
    """
    Represents metrics for data validation.

    Attributes:
        rule (str): The name of the validation rule.
        total_row_count (int): The total number of rows.
        valid_row_count (int): The number of valid rows.
        valid_row_count_pct (float): The percentage of valid rows.
        timestamp (str): The timestamp when the metrics were recorded.
    """

    rule: str
    total_row_count: int
    valid_row_count: int
    valid_row_count_pct: float
    timestamp: str

    def __str__(self):
        """
        Return a string representation of the metrics.

        Returns:
            str: A string representation of the metrics.
        """
        return (
            f"rule_name : {self.rule}\n"
            f"total_row_count : {self.total_row_count}\n"
            f"valid_row_count : {self.valid_row_count}\n"
            f"valid_row_count_pct : {self.valid_row_count_pct}\n"
            f"timestamp : {self.timestamp}"
        )
