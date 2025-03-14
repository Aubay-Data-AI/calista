import pathlib

import pytest

import calista
from calista import CalistaEngine
from tests.table.parameters import BIGQUERY_CONN_PARAMS, SNOWFLAKE_CONN_PARAMS
from calista.label.snowflake.iban.iban  import init_udf as init_udf_email

from calista.label.snowflake.iban.iban import init_udf



def get_file_path(file_name: str) -> str:
    calista_p = pathlib.Path(calista.__file__)
    return f"{str(calista_p.parent.parent)}/ressources/{file_name}"


def get_bigquery_key_path(file_name: str) -> str:
    calista_p = pathlib.Path(calista.__file__)
    return f"{str(calista_p.parent.parent)}/tests/table/{file_name}"


@pytest.fixture(scope="module")
def bigquery_table(request):
    return request.getfixturevalue(request.param)


@pytest.mark.skip(reason="Ignoré temporairement")
def spark_table():
    return CalistaEngine("spark").load_from_path(
        get_file_path("TEST_DATASET_100.parquet"), "parquet"
    )


@pytest.mark.skip(reason="Ignoré temporairement")
def pandas_table():
    return CalistaEngine("pandas").load_from_path(
        get_file_path("TEST_DATASET_100.parquet"), "parquet"
    )


@pytest.mark.skip(reason="Ignoré temporairement")
def polars_table():
    return CalistaEngine("polars").load_from_path(
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

    return CalistaEngine(engine="bigquery", config=BIGQUERY_CONN_PARAMS)


@pytest.fixture(scope="module")
def snowflake_session():
    session = CalistaEngine(  
        "snowflake",
        SNOWFLAKE_CONN_PARAMS,
    )
    
    #session._engine.snowflake.sql("USE DATABASE RESSOURCES").collect()
    #session._engine.snowflake.sql("USE SCHEMA TESTS_UNITAIRES").collect()
    
    #init_udf_email(session._engine.snowflake)  

    return session  # ✅ Return the session after initialization
