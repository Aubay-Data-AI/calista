from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class QualityWeights(BaseModel):
    completeness: float = 40.0
    uniqueness: float = 20.0
    type_conformity: float = 40.0


class ColumnProfile(BaseModel):
    col_name: str
    col_type: str | None = None
    count: int = 0
    null_count: int = 0
    null_ratio: float = 0.0
    distinct_count: int | None = None
    min: Any = None
    max: Any = None
    mean: float | None = None
    median: float | None = None
    std_dev: float | None = None
    top_values: dict = Field(default_factory=dict)
    outliers: list = Field(default_factory=list)
    quality_score: float = 0.0

    def compute_quality_score(self, weights: QualityWeights) -> float:
        total = weights.completeness + weights.uniqueness + weights.type_conformity
        scores = []

        if self.count > 0:
            scores.append(
                (self.count - self.null_count) / self.count * 100
                * weights.completeness
                / total
            )
        else:
            scores.append(0.0)

        if self.distinct_count is not None and self.count > 0:
            scores.append(
                self.distinct_count / self.count * 100 * weights.uniqueness / total
            )
        else:
            scores.append(0.0)

        scores.append(100.0 * weights.type_conformity / total)

        self.quality_score = round(sum(scores), 2)
        return self.quality_score


def _to_serializable(val: Any) -> Any:
    import numpy as np

    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    return val


class TableProfile(BaseModel):
    table_name: str = ""
    engine: str = ""
    timestamp: str = Field(default_factory=lambda: str(datetime.now()))
    total_row_count: int = 0
    total_column_count: int = 0
    columns: list[ColumnProfile] = Field(default_factory=list)
    overall_score: float = 0.0

    def compute_overall_score(self) -> float:
        if not self.columns:
            self.overall_score = 0.0
        else:
            self.overall_score = round(
                sum(c.quality_score for c in self.columns) / len(self.columns), 2
            )
        return self.overall_score

    def to_dict(self) -> dict:
        return self.model_dump()

    def to_json(self) -> str:
        return self.model_dump_json(indent=2)
