from typing import Dict, Union
from snowflake.snowpark.functions import udf, call_udf, col
from snowflake.snowpark.column import Column
from snowflake.snowpark import Session

ALPHABET_CONVERSION = str.maketrans({chr(i + 65): str(i + 10) for i in range(26)})

IBAN_VALIDATION_RULES: Dict[str, Dict[str, Union[int, str]]] = {
    "AD": {
        "length": 24,
        "regex": r"^\d{4}\d{4}[A-Za-z0-9]{12}$"
    },
    "AE": {
        "length": 23,
        "regex": r"^\d{3}\d{16}$"
    },
    "AL": {
        "length": 28,
        "regex": r"^\d{8}[A-Za-z0-9]{16}$"
    },
    "AT": {
        "length": 20,
        "regex": r"^\d{5}\d{11}$"
    },
    "AZ": {
        "length": 28,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{20}$"
    },
    "BA": {
        "length": 20,
        "regex": r"^\d{3}\d{3}\d{8}\d{2}$"
    },
    "BE": {
        "length": 16,
        "regex": r"^\d{3}\d{7}\d{2}$"
    },
    "BG": {
        "length": 22,
        "regex": r"^[A-Z]{4}\d{4}\d{2}[A-Za-z0-9]{8}$"
    },
    "BH": {
        "length": 22,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{14}$"
    },
    "BI": {
        "length": 27,
        "regex": r"^\d{5}\d{5}\d{11}\d{2}$"
    },
    "BR": {
        "length": 29,
        "regex": r"^\d{8}\d{5}\d{10}[A-Z]{1}[A-Za-z0-9]{1}$"
    },
    "BY": {
        "length": 28,
        "regex": r"^[A-Za-z0-9]{4}\d{4}[A-Za-z0-9]{16}$"
    },
    "CH": {
        "length": 21,
        "regex": r"^\d{5}[A-Za-z0-9]{12}$"
    },
    "CR": {
        "length": 22,
        "regex": r"^\d{4}\d{14}$"
    },
    "CY": {
        "length": 28,
        "regex": r"^\d{3}\d{5}[A-Za-z0-9]{16}$"
    },
    "CZ": {
        "length": 24,
        "regex": r"^\d{4}\d{6}\d{10}$"
    },
    "DE": {
        "length": 22,
        "regex": r"^\d{8}\d{10}$"
    },
    "DJ": {
        "length": 27,
        "regex": r"^\d{5}\d{5}\d{11}\d{2}$"
    },
    "DK": {
        "length": 18,
        "regex": r"^\d{4}\d{9}\d{1}$"
    },
    "DO": {
        "length": 28,
        "regex": r"^[A-Za-z0-9]{4}\d{20}$"
    },
    "EE": {
        "length": 20,
        "regex": r"^\d{2}\d{2}\d{11}\d{1}$"
    },
    "EG": {
        "length": 29,
        "regex": r"^\d{4}\d{4}\d{17}$"
    },
    "ES": {
        "length": 24,
        "regex": r"^\d{4}\d{4}\d{1}\d{1}\d{10}$"
    },
    "FI": {
        "length": 18,
        "regex": r"^\d{3}\d{11}$"
    },
    "AX": {
        "length": 18,
        "regex": r"^\d{3}\d{11}$"
    },
    "FK": {
        "length": 18,
        "regex": r"^[A-Z]{2}\d{12}$"
    },
    "FO": {
        "length": 18,
        "regex": r"^\d{4}\d{9}\d{1}$"
    },
    "FR": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "GF": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "GP": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "MQ": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "RE": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "PF": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "TF": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "YT": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "NC": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "BL": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "MF": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "PM": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "WF": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "GB": {
        "length": 22,
        "regex": r"^[A-Z]{4}\d{6}\d{8}$"
    },
    "IM": {
        "length": 22,
        "regex": r"^[A-Z]{4}\d{6}\d{8}$"
    },
    "JE": {
        "length": 22,
        "regex": r"^[A-Z]{4}\d{6}\d{8}$"
    },
    "GG": {
        "length": 22,
        "regex": r"^[A-Z]{4}\d{6}\d{8}$"
    },
    "GE": {
        "length": 22,
        "regex": r"^[A-Z]{2}\d{16}$"
    },
    "GI": {
        "length": 23,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{15}$"
    },
    "GL": {
        "length": 18,
        "regex": r"^\d{4}\d{9}\d{1}$"
    },
    "GR": {
        "length": 27,
        "regex": r"^\d{3}\d{4}[A-Za-z0-9]{16}$"
    },
    "GT": {
        "length": 28,
        "regex": r"^[A-Za-z0-9]{4}[A-Za-z0-9]{20}$"
    },
    "HR": {
        "length": 21,
        "regex": r"^\d{7}\d{10}$"
    },
    "HU": {
        "length": 28,
        "regex": r"^\d{3}\d{4}\d{1}\d{15}\d{1}$"
    },
    "IE": {
        "length": 22,
        "regex": r"^[A-Z]{4}\d{6}\d{8}$"
    },
    "IL": {
        "length": 23,
        "regex": r"^\d{3}\d{3}\d{13}$"
    },
    "IQ": {
        "length": 23,
        "regex": r"^[A-Z]{4}\d{3}\d{12}$"
    },
    "IS": {
        "length": 26,
        "regex": r"^\d{4}\d{2}\d{6}\d{10}$"
    },
    "IT": {
        "length": 27,
        "regex": r"^[A-Z]{1}\d{5}\d{5}[A-Za-z0-9]{12}$"
    },
    "JO": {
        "length": 30,
        "regex": r"^[A-Z]{4}\d{4}[A-Za-z0-9]{18}$"
    },
    "KW": {
        "length": 30,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{22}$"
    },
    "KZ": {
        "length": 20,
        "regex": r"^\d{3}[A-Za-z0-9]{13}$"
    },
    "LB": {
        "length": 28,
        "regex": r"^\d{4}[A-Za-z0-9]{20}$"
    },
    "LC": {
        "length": 32,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{24}$"
    },
    "LI": {
        "length": 21,
        "regex": r"^\d{5}[A-Za-z0-9]{12}$"
    },
    "LT": {
        "length": 20,
        "regex": r"^\d{5}\d{11}$"
    },
    "LU": {
        "length": 20,
        "regex": r"^\d{3}[A-Za-z0-9]{13}$"
    },
    "LV": {
        "length": 21,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{13}$"
    },
    "LY": {
        "length": 25,
        "regex": r"^\d{3}\d{3}\d{15}$"
    },
    "MC": {
        "length": 27,
        "regex": r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$"
    },
    "MD": {
        "length": 24,
        "regex": r"^[A-Za-z0-9]{2}[A-Za-z0-9]{18}$"
    },
    "ME": {
        "length": 22,
        "regex": r"^\d{3}\d{13}\d{2}$"
    },
    "MK": {
        "length": 19,
        "regex": r"^\d{3}[A-Za-z0-9]{10}\d{2}$"
    },
    "MN": {
        "length": 20,
        "regex": r"^\d{4}\d{12}$"
    },
    "MR": {
        "length": 27,
        "regex": r"^\d{5}\d{5}\d{11}\d{2}$"
    },
    "MT": {
        "length": 31,
        "regex": r"^[A-Z]{4}\d{5}[A-Za-z0-9]{18}$"
    },
    "MU": {
        "length": 30,
        "regex": r"^[A-Z]{4}\d{2}\d{2}\d{12}\d{3}[A-Z]{3}$"
    },
    "NI": {
        "length": 28,
        "regex": r"^[A-Z]{4}\d{20}$"
    },
    "NL": {
        "length": 18,
        "regex": r"^[A-Z]{4}\d{10}$"
    },
    "NO": {
        "length": 15,
        "regex": r"^\d{4}\d{6}\d{1}$"
    },
    "OM": {
        "length": 23,
        "regex": r"^\d{3}[A-Za-z0-9]{16}$"
    },
    "PK": {
        "length": 24,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{16}$"
    },
    "PL": {
        "length": 28,
        "regex": r"^\d{8}\d{16}$"
    },
    "PS": {
        "length": 29,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{21}$"
    },
    "PT": {
        "length": 25,
        "regex": r"^\d{4}\d{4}\d{11}\d{2}$"
    },
    "QA": {
        "length": 29,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{21}$"
    },
    "RO": {
        "length": 24,
        "regex": r"^[A-Z]{4}[A-Za-z0-9]{16}$"
    },
    "RS": {
        "length": 22,
        "regex": r"^\d{3}\d{13}\d{2}$"
    },
    "RU": {
        "length": 33,
        "regex": r"^\d{9}\d{5}[A-Za-z0-9]{15}$"
    },
    "SA": {
        "length": 24,
        "regex": r"^\d{2}[A-Za-z0-9]{18}$"
    },
    "SC": {
        "length": 31,
        "regex": r"^[A-Z]{4}\d{2}\d{2}\d{16}[A-Z]{3}$"
    },
    "SD": {
        "length": 18,
        "regex": r"^\d{2}\d{12}$"
    },
    "SE": {
        "length": 24,
        "regex": r"^\d{3}\d{16}\d{1}$"
    },
    "SI": {
        "length": 19,
        "regex": r"^\d{5}\d{8}\d{2}$"
    },
    "SK": {
        "length": 24,
        "regex": r"^\d{4}\d{6}\d{10}$"
    },
    "SM": {
        "length": 27,
        "regex": r"^[A-Z]{1}\d{5}\d{5}[A-Za-z0-9]{12}$"
    },
    "SO": {
        "length": 23,
        "regex": r"^\d{4}\d{3}\d{12}$"
    },
    "ST": {
        "length": 25,
        "regex": r"^\d{4}\d{4}\d{11}\d{2}$"
    },
    "SV": {
        "length": 28,
        "regex": r"^[A-Z]{4}\d{20}$"
    },
    "TL": {
        "length": 23,
        "regex": r"^\d{3}\d{14}\d{2}$"
    },
    "TN": {
        "length": 24,
        "regex": r"^\d{2}\d{3}\d{13}\d{2}$"
    },
    "TR": {
        "length": 26,
        "regex": r"^\d{5}\d{1}[A-Za-z0-9]{16}$"
    },
    "UA": {
        "length": 29,
        "regex": r"^\d{6}[A-Za-z0-9]{19}$"
    },
    "VA": {
        "length": 22,
        "regex": r"^\d{3}\d{15}$"
    },
    "VG": {
        "length": 24,
        "regex": r"^[A-Z]{4}\d{16}$"
    },
    "XK": {
        "length": 20,
        "regex": r"^\d{4}\d{10}\d{2}$"
    }
}


def init_validation_rules(session: Session):
    """
    Initialize a temporary table or view to store IBAN validation rules.

    Args:
        session: Snowflake Snowpark session
    """
    rules_data = [
        ("AD", 24, r"^\d{4}\d{4}[A-Za-z0-9]{12}$"),
        ("AE", 23, r"^\d{3}\d{16}$"),
        ("AL", 28, r"^\d{8}[A-Za-z0-9]{16}$"),
        ("AT", 20, r"^\d{5}\d{11}$"),
        ("AZ", 28, r"^[A-Z]{4}[A-Za-z0-9]{20}$")
        #... test , ajouter les autres pays après
    ]

    session.create_dataframe(rules_data, schema=["country_code", "length", "regex"]) \
        .write.mode("overwrite").save_as_table("TEMP_IBAN_VALIDATION_RULES")


def init_udf(session: Session):
    """
    Initialize Snowflake UDFs for IBAN validation.

    Args:
        session: Snowflake Snowpark session
    """
    # UDF for checksum calculation using Snowflake SQL
    session.sql("""
        CREATE OR REPLACE FUNCTION calculate_checksum(iban VARCHAR)
        RETURNS STRING
        LANGUAGE SQL
        AS
        $$
        WITH rearranged AS (
            SELECT CONCAT(SUBSTRING(iban, 5), SUBSTRING(iban, 1, 4)) AS rearranged_iban
        ),
        converted AS (
            SELECT TRANSLATE(
                rearranged_iban,
                'ABCDEFGHIJKLMNOPQRSTUVWXYZ',
                '101112131415161718192021222324252635'
            ) AS numeric_iban
            FROM rearranged
        ),
        checksum_calc AS (
            SELECT CASE
                WHEN MOD(TO_NUMBER(numeric_iban), 97) = 1 THEN 'True'
                ELSE 'False'
            END AS checksum_result
            FROM converted
        )
        SELECT checksum_result FROM checksum_calc
        $$;
    """)

    # UDF for IBAN validation using SQL
    session.sql("""
        CREATE OR REPLACE FUNCTION validate_iban(iban VARCHAR)
        RETURNS BOOLEAN
        LANGUAGE SQL
        AS
        $$
        WITH cleaned AS (
            SELECT UPPER(REGEXP_REPLACE(iban, '[^a-zA-Z0-9]', '')) AS cleaned_iban
        ),
        country_check AS (
            SELECT
                cleaned_iban,
                SUBSTRING(cleaned_iban, 1, 2) AS country_code,
                LENGTH(cleaned_iban) AS iban_length
            FROM cleaned
        ),
        validation_params AS (
            SELECT
                c.cleaned_iban,
                c.country_code,
                c.iban_length,
                r.length AS expected_length,
                r.regex AS bban_regex
            FROM country_check c
            LEFT JOIN TEMP_IBAN_VALIDATION_RULES r
            ON c.country_code = r.country_code
        ),
        validation_checks AS (
            SELECT
                cleaned_iban,
                country_code,
                iban_length,
                expected_length,
                CASE
                    WHEN expected_length IS NULL THEN FALSE -- Invalid country code
                    WHEN iban_length != expected_length THEN FALSE -- Incorrect length
                    WHEN NOT REGEXP_LIKE(SUBSTRING(cleaned_iban, 5), bban_regex) THEN FALSE -- BBAN format mismatch
                    ELSE TRUE
                END AS is_format_valid
            FROM validation_params
        ),
        checksum_check AS (
            SELECT
                v.cleaned_iban,
                v.is_format_valid,
                CASE
                    WHEN v.is_format_valid THEN calculate_checksum(cleaned_iban)
                    ELSE 'False'
                END AS checksum_valid
            FROM validation_checks v
        )
        SELECT is_format_valid AND checksum_valid = 'True' AS is_iban_valid
        FROM checksum_check
        $$;
    """)

    #check UDFs
    session.sql("SHOW FUNCTIONS LIKE 'calculate_checksum'").show()
    session.sql("SHOW FUNCTIONS LIKE 'validate_iban'").show()


def check_ibans(col_name: str) -> Column:
    """
    Validates IBAN using the validate_iban UDF.

    Args:
        col_name: Column name containing IBAN.

    Returns:
        Column containing boolean values for each row.
    """
    return call_udf("TESTS_UNITAIRES.validate_iban", col(col_name))