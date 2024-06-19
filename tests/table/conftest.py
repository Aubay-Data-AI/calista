import pathlib

import pytest

import calista
from calista.table import CalistaTable
from tests.table.parameters import BIGQUERY_CONN_PARAMS, SNOWFLAKE_CONN_PARAMS


def get_file_path(file_name: str) -> str:
    calista_p = pathlib.Path(calista.__file__)
    return f"{str(calista_p.parent.parent)}/ressources/{file_name}"


def get_bigquery_key_path(file_name: str) -> str:
    calista_p = pathlib.Path(calista.__file__)
    return f"{str(calista_p.parent.parent)}/tests/table/{file_name}"


@pytest.fixture(scope="module")
def bigquery_table(request):
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="module")
def spark_table():
    return CalistaTable("spark").load_from_path(
        get_file_path("TEST_DATASET_100.parquet"), "parquet"
    )


@pytest.fixture(scope="module")
def pandas_table():
    return CalistaTable("pandas").load_from_path(
        get_file_path("TEST_DATASET_100.parquet"), "parquet"
    )


@pytest.fixture(scope="module")
def polars_table():
    return CalistaTable("polars").load_from_path(
        get_file_path("TEST_DATASET_100.parquet"), "parquet"
    )


@pytest.fixture(scope="module")
def bigquery_table(bigquery_session):
    return bigquery_session.load_from_database(table="employees")


@pytest.fixture(scope="module")
def snowflake_table(snowflake_session):
    return snowflake_session.load_from_database(
        database="RESSOURCES", schema="TESTS_UNITAIRES", table="TEST_DATASET_100"
    )


@pytest.fixture(scope="module")
def calista_session(request):
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="module")
def bigquery_session():
    credentials_path = get_bigquery_key_path(BIGQUERY_CONN_PARAMS["credentials_path"])
    BIGQUERY_CONN_PARAMS["credentials_path"] = credentials_path

    return CalistaTable(engine="bigquery", config=BIGQUERY_CONN_PARAMS)


@pytest.fixture(scope="module")
def snowflake_session():
    return CalistaTable(
        "snowflake",
        SNOWFLAKE_CONN_PARAMS,
    )
