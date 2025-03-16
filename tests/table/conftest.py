import pathlib

import pytest

import calista
from calista import CalistaEngine
from tests.table.parameters import BIGQUERY_CONN_PARAMS, SNOWFLAKE_CONN_PARAMS

from snowflake.snowpark.types import StringType, BooleanType
import shutil
import os
from email_validator import validate_email, EmailNotValidError

from snowflake.snowpark import Session



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
 
    """snowflake_engine = session._engine

    print("Available attributes on SnowflakeEngine:", dir(snowflake_engine))
    print("SnowflakeEngine internal attributes and values:", snowflake_engine.__dict__)

    # Temporarily raise to quickly inspect output
    raise Exception("Debug stop: Check printed output above")

    yield session"""

    # Tester la connexion
    session._engine.snowflake.sql("SELECT CURRENT_VERSION()").show()
    session._engine.snowflake.sql("USE DATABASE RESSOURCES").collect()
    session._engine.snowflake.sql("USE SCHEMA TESTS_UNITAIRES").collect()
    session._engine.snowflake.add_packages("email_validator")
    
    def validate_email_snowflake(email: str) -> bool:
        if email is None:
            return False
        try:
            validate_email(email, check_deliverability=False)
            return True
        except EmailNotValidError:
            return False
    session._engine.snowflake.udf.register(
        validate_email_snowflake,
        name="validate_email",
        input_types=[StringType()],
        return_type=BooleanType(),
        replace=True,
        packages=["email_validator"],  # critical dependency
    )
        # Fermer la session
      
    
    session_id = session._engine.snowflake.sql("SELECT CURRENT_SESSION()").collect()[0][0]
    print(f"✅ UDF registered in session {session_id}")   
    result = session._engine.snowflake.sql("SHOW USER FUNCTIONS").collect()
    udf_names = [row['name'].upper() for row in result]

    if "VALIDATE_EMAIL" in udf_names:
        print("✅ L'UDF 'validate_email' est bien enregistrée dans Snowflake !")
    else:
        print("❌ L'UDF 'validate_email' n'est pas enregistrée.")

    yield session
    

    return session  # ✅ Return the session after initialization
