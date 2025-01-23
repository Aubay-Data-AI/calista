import re
from snowflake.snowpark.functions import col, call_udf
from snowflake.snowpark.session import Session
from snowflake.snowpark.column import Column
from typing import Dict, Any


IBAN_SPECIFICATIONS: Dict[str, Dict[str, Any]] = {
    "AD": {
        "country": "AD",
        "bban_spec": "4!n4!n12!c",
        "iban_length": 24,
    },
    "AE": {
        "country": "AE",
        "bban_spec": "3!n16!n",
        "iban_length": 23,
    }
    #....
}

def _convert_bban_spec_to_regex(spec: str) -> str:
    """Converts BBAN spec to regex pattern"""
    spec_to_re = {"n": r"\d", "a": r"[A-Z]", "c": r"[A-Za-z0-9]", "e": r" "}
    pattern = re.compile(r"(\d+)(!)?([nace])")
    
    def replacer(match: re.Match) -> str:
        count = int(match.group(1))
        force_exact = match.group(2) == "!"
        char_type = match.group(3)
        
        if force_exact:
            return f"{spec_to_re[char_type]}{{{count}}}"
        return f"{spec_to_re[char_type]}{{1,{count}}}"
    
    return f"^{pattern.sub(replacer, spec)}$"

def _create_iban_specs_table(session: Session):
    """Creates and populates the IBAN_SPECS table with precomputed regex patterns"""
    specs_data = [
        (country, spec["iban_length"], _convert_bban_spec_to_regex(spec["bban_spec"]))
        for country, spec in IBAN_SPECIFICATIONS.items()
    ]
    
    session.sql("CREATE OR REPLACE TABLE IBAN_SPECS (country_code STRING, iban_length NUMBER, bban_regex STRING)").collect()
    session.sql("DELETE FROM IBAN_SPECS").collect()
    
    for country, length, regex in specs_data:
        session.sql(f"""
            INSERT INTO IBAN_SPECS 
            VALUES ('{country}', {length}, '{regex}')
        """).collect()

def _create_sql_udfs(session: Session):
    """Creates optimized SQL UDFs for IBAN validation"""

    # Validate IBAN Checksum
    session.sql("""
    CREATE OR REPLACE FUNCTION VALIDATE_IBAN_CHECKSUM(IBAN STRING)
    RETURNS BOOLEAN
    AS $$
    SELECT 
        CASE 
            WHEN IBAN IS NULL OR LENGTH(IBAN) < 4 THEN FALSE
            ELSE (
                MOD(
                    TO_NUMBER(
                        REGEXP_REPLACE(
                            UPPER(SUBSTR(IBAN, 5) || SUBSTR(IBAN, 1, 4)),
                            '[A-Z]', 
                            TO_CHAR(ASCII(REGEXP_SUBSTR(UPPER(SUBSTR(IBAN, 5) || SUBSTR(IBAN, 1, 4)), '[A-Z]')) - 55)
                        )
                    ), 
                    97
                ) = 1
            )
        END
    $$
    """).collect()

    # Main IBAN Validation
    session.sql("""
    CREATE OR REPLACE FUNCTION VALIDATE_IBAN(IBAN STRING)
    RETURNS BOOLEAN
    AS $$
    WITH cleaned_iban AS (
        SELECT UPPER(REGEXP_REPLACE(IBAN, '[^A-Z0-9]', '')) AS cleaned
    ),
    country_check AS (
        SELECT 
            cleaned,
            SUBSTR(cleaned, 1, 2) AS country_code,
            LENGTH(cleaned) AS iban_length
        FROM cleaned_iban
    )
    SELECT
        cc.cleaned IS NOT NULL AND
        cc.iban_length = s.iban_length AND
        REGEXP_LIKE(SUBSTR(cc.cleaned, 5), s.bban_regex) AND
        VALIDATE_IBAN_CHECKSUM(cc.cleaned) 
    FROM country_check cc
    LEFT JOIN IBAN_SPECS s 
        ON cc.country_code = s.country_code
    $$
    """).collect()

def ensure_udfs_exist(session: Session):
    """Ensures all required database objects exist"""

    # Check if IBAN_SPECS table exists
    table_exists = session.sql("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = 'IBAN_SPECS'
        ) AS exists
    """).collect()[0]["EXISTS"]
    
    if not table_exists:
        _create_iban_specs_table(session)
        _create_sql_udfs(session)
    else:
        # Check if UDFs exist
        funcs_exist = session.sql("""
            SELECT COUNT(*) = 2 AS all_exist
            FROM information_schema.functions
            WHERE function_name IN ('VALIDATE_IBAN', 'VALIDATE_IBAN_CHECKSUM')
        """).collect()[0]["ALL_EXIST"]
        
        if not funcs_exist:
            _create_sql_udfs(session)

def check_iban(col_name: str) -> Column:
    """
    Public interface for IBAN validation
    Usage: df.withColumn("is_valid_iban", check_iban("iban_column"))
    """
    return call_udf("VALIDATE_IBAN", col(col_name))