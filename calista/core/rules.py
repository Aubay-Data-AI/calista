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


from typing import Any

from pydantic import BaseModel, Field


class Rule(BaseModel):
    pass


class GetColumns(Rule):
    dataset: Any


class ComputePercentile(Rule):
    col_name: str
    percentile: float = Field(le=1, gt=0)


class GetColValuesSuperiorToConstant(Rule):
    col_name: str
    constant: float


class GetColValuesInferiorToConstant(Rule):
    col_name: str
    constant: float


class CountOccurences(Rule):
    col_name: str


class GetOutliersForContinuousVar(Rule):
    col_name: str
    first_quartile: float = Field(le=1, gt=0, default=0.25)
    third_quartile: float = Field(le=1, gt=0, default=0.75)
