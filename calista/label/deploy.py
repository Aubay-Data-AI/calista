import snowflake.snowpark.functions as F
from snowflake.snowpark.session import Session

def create_udf(session):
    session.sql("""
    CREATE STAGE IF NOT EXISTS python_libs
    """).collect()
    
    session.file.put(
        "requirements.txt",
        "@python_libs/requirements.txt",
        auto_compress=False,
        overwrite=True
    )
    
    session.file.put(
        "label_snowflake_schwifty.py",
        "@python_libs/my_function.py",
        auto_compress=False,
        overwrite=True
    )
    
    # Creation de l'UDF
    session.sql("""
    CREATE OR REPLACE FUNCTION validate_iban(iban_string STRING)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('schwifty')
    IMPORTS = ('@python_libs/my_function.py')
    HANDLER = 'validate_iban'
    """).collect()

# Connexion
connection_parameters = {
    "account": "",
    "user": "",
    "password": "",
    "role": "",
    "warehouse": "",
    "database": "",
    "schema": ""
}

# Creer session et envoyer sur snowflake
session = Session.builder.configs(connection_parameters).create()
create_udf(session)
session.close()