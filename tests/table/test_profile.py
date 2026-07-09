import pytest

from calista import CalistaEngine
from calista.core.profile import QualityWeights, TableProfile

_PROFILE_DATA = {
    "ID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "NAME": ["Alice", "Bob", "Alice", "Charlie", None, "Bob", "Alice", "David", "Eve", "Alice"],
    "SCORE": [85.0, 92.5, 78.0, 88.0, 95.0, 70.0, 88.0, 92.5, 100.0, 65.0],
}


def _get_pandas_engine():
    return CalistaEngine("pandas").load_from_dict(_PROFILE_DATA)._engine


def _get_polars_engine():
    return CalistaEngine("polars").load_from_dict(_PROFILE_DATA)._engine


@pytest.fixture(params=[_get_pandas_engine, _get_polars_engine], ids=["pandas", "polars"])
def engine(request):
    return request.param()


class TestProfileBasics:
    def test_profile_returns_table_profile(self, engine):
        result = engine.get_column_statistics("ID")
        assert isinstance(result, dict)
        assert "count" in result
        assert "null_count" in result

    def test_column_statistics_count(self, engine):
        result = engine.get_column_statistics("ID")
        assert result["count"] == 10

    def test_column_statistics_null_count(self, engine):
        result = engine.get_column_statistics("NAME")
        assert result["null_count"] == 1

    def test_column_statistics_distinct_count(self, engine):
        result = engine.get_column_statistics("NAME")
        assert result["distinct_count"] == 5

    def test_top_values(self, engine):
        top = engine.get_top_values("NAME", n=3)
        assert len(top) <= 3
        assert "Alice" in top
        assert top["Alice"] == 4

    def test_null_count(self, engine):
        assert engine.get_null_count("NAME") == 1
        assert engine.get_null_count("ID") == 0

    def test_std_dev_numeric(self, engine):
        result = engine.get_std_dev("SCORE")
        assert result > 0

    def test_top_values_null_excluded(self, engine):
        top = engine.get_top_values("NAME", n=10)
        assert None not in top


class TestProfileViaTable:
    def _make_table(self, engine_name):
        return CalistaEngine(engine_name).load_from_dict(_PROFILE_DATA)

    @pytest.mark.parametrize("engine_name", ["pandas", "polars"])
    def test_profile_returns_table_profile(self, engine_name):
        table = self._make_table(engine_name)
        result = table.profile()
        assert isinstance(result, TableProfile)
        assert result.total_row_count == 10
        assert result.total_column_count == 3

    @pytest.mark.parametrize("engine_name", ["pandas", "polars"])
    def test_profile_column_has_correct_null_ratio(self, engine_name):
        table = self._make_table(engine_name)
        result = table.profile()
        name_col = next(c for c in result.columns if c.col_name == "NAME")
        assert name_col.null_count == 1
        assert name_col.null_ratio == 0.1
        assert name_col.count == 10

    @pytest.mark.parametrize("engine_name", ["pandas", "polars"])
    def test_profile_with_custom_weights(self, engine_name):
        table = self._make_table(engine_name)
        result_default = table.profile()
        result_custom = table.profile(
            weights=QualityWeights(completeness=90, uniqueness=5, type_conformity=5)
        )
        assert result_custom.overall_score != result_default.overall_score

    @pytest.mark.parametrize("engine_name", ["pandas", "polars"])
    def test_profile_with_columns_filter(self, engine_name):
        table = self._make_table(engine_name)
        result = table.profile(columns=["ID", "SCORE"])
        assert result.total_column_count == 2
        col_names = [c.col_name for c in result.columns]
        assert "NAME" not in col_names

    @pytest.mark.parametrize("engine_name", ["pandas", "polars"])
    def test_profile_to_json(self, engine_name):
        table = self._make_table(engine_name)
        result = table.profile()
        json_str = result.to_json()
        assert '"table_name"' in json_str
        assert '"total_row_count"' in json_str
        assert '"columns"' in json_str

    @pytest.mark.parametrize("engine_name", ["pandas", "polars"])
    def test_profile_quality_score_in_range(self, engine_name):
        table = self._make_table(engine_name)
        result = table.profile()
        assert 0 <= result.overall_score <= 100
        for col in result.columns:
            assert 0 <= col.quality_score <= 100


class TestProfileWithFixtures:
    def test_pandas_table_profile(self, pandas_table):
        result = pandas_table.profile()
        assert isinstance(result, TableProfile)
        assert result.engine.lower() == "pandas"

    def test_polars_table_profile(self, polars_table):
        result = polars_table.profile()
        assert isinstance(result, TableProfile)
        assert result.engine.lower() == "polars"


class TestQualityWeights:
    def test_default_weights(self):
        w = QualityWeights()
        assert w.completeness == 40.0
        assert w.uniqueness == 20.0
        assert w.type_conformity == 40.0

    def test_custom_weights(self):
        w = QualityWeights(completeness=50, uniqueness=25, type_conformity=25)
        assert w.completeness == 50.0
