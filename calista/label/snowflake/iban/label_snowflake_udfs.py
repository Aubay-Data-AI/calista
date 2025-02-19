from typing import Dict, Union
import re
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import StringType, BooleanType
from snowflake.snowpark.column import Column
from snowflake.snowpark import Session

ALPHABET_CONVERSION = str.maketrans({chr(i + 65): str(i + 10) for i in range(26)})

IBAN_VALIDATION_RULES: Dict[str, Dict[str, Union[int, str]]] = {
    "AD": {
        "length": 24,
        "regex": re.compile(r"^\d{4}\d{4}[A-Za-z0-9]{12}$")
    },
    "AE": {
        "length": 23,
        "regex": re.compile(r"^\d{3}\d{16}$")
    },
    "AL": {
        "length": 28,
        "regex": re.compile(r"^\d{8}[A-Za-z0-9]{16}$")
    },
    "AT": {
        "length": 20,
        "regex": re.compile(r"^\d{5}\d{11}$")
    },
    "AZ": {
        "length": 28,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{20}$")
    },
    "BA": {
        "length": 20,
        "regex": re.compile(r"^\d{3}\d{3}\d{8}\d{2}$")
    },
    "BE": {
        "length": 16,
        "regex": re.compile(r"^\d{3}\d{7}\d{2}$")
    },
    "BG": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{4}\d{4}\d{2}[A-Za-z0-9]{8}$")
    },
    "BH": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{14}$")
    },
    "BI": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}\d{11}\d{2}$")
    },
    "BR": {
        "length": 29,
        "regex": re.compile(r"^\d{8}\d{5}\d{10}[A-Z]{1}[A-Za-z0-9]{1}$")
    },
    "BY": {
        "length": 28,
        "regex": re.compile(r"^[A-Za-z0-9]{4}\d{4}[A-Za-z0-9]{16}$")
    },
    "CH": {
        "length": 21,
        "regex": re.compile(r"^\d{5}[A-Za-z0-9]{12}$")
    },
    "CR": {
        "length": 22,
        "regex": re.compile(r"^\d{4}\d{14}$")
    },
    "CY": {
        "length": 28,
        "regex": re.compile(r"^\d{3}\d{5}[A-Za-z0-9]{16}$")
    },
    "CZ": {
        "length": 24,
        "regex": re.compile(r"^\d{4}\d{6}\d{10}$")
    },
    "DE": {
        "length": 22,
        "regex": re.compile(r"^\d{8}\d{10}$")
    },
    "DJ": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}\d{11}\d{2}$")
    },
    "DK": {
        "length": 18,
        "regex": re.compile(r"^\d{4}\d{9}\d{1}$")
    },
    "DO": {
        "length": 28,
        "regex": re.compile(r"^[A-Za-z0-9]{4}\d{20}$")
    },
    "EE": {
        "length": 20,
        "regex": re.compile(r"^\d{2}\d{2}\d{11}\d{1}$")
    },
    "EG": {
        "length": 29,
        "regex": re.compile(r"^\d{4}\d{4}\d{17}$")
    },
    "ES": {
        "length": 24,
        "regex": re.compile(r"^\d{4}\d{4}\d{1}\d{1}\d{10}$")
    },
    "FI": {
        "length": 18,
        "regex": re.compile(r"^\d{3}\d{11}$")
    },
    "AX": {
        "length": 18,
        "regex": re.compile(r"^\d{3}\d{11}$")
    },
    "FK": {
        "length": 18,
        "regex": re.compile(r"^[A-Z]{2}\d{12}$")
    },
    "FO": {
        "length": 18,
        "regex": re.compile(r"^\d{4}\d{9}\d{1}$")
    },
    "FR": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "GF": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "GP": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "MQ": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "RE": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "PF": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "TF": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "YT": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "NC": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "BL": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "MF": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "PM": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "WF": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "GB": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{4}\d{6}\d{8}$")
    },
    "IM": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{4}\d{6}\d{8}$")
    },
    "JE": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{4}\d{6}\d{8}$")
    },
    "GG": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{4}\d{6}\d{8}$")
    },
    "GE": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{2}\d{16}$")
    },
    "GI": {
        "length": 23,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{15}$")
    },
    "GL": {
        "length": 18,
        "regex": re.compile(r"^\d{4}\d{9}\d{1}$")
    },
    "GR": {
        "length": 27,
        "regex": re.compile(r"^\d{3}\d{4}[A-Za-z0-9]{16}$")
    },
    "GT": {
        "length": 28,
        "regex": re.compile(r"^[A-Za-z0-9]{4}[A-Za-z0-9]{20}$")
    },
    "HR": {
        "length": 21,
        "regex": re.compile(r"^\d{7}\d{10}$")
    },
    "HU": {
        "length": 28,
        "regex": re.compile(r"^\d{3}\d{4}\d{1}\d{15}\d{1}$")
    },
    "IE": {
        "length": 22,
        "regex": re.compile(r"^[A-Z]{4}\d{6}\d{8}$")
    },
    "IL": {
        "length": 23,
        "regex": re.compile(r"^\d{3}\d{3}\d{13}$")
    },
    "IQ": {
        "length": 23,
        "regex": re.compile(r"^[A-Z]{4}\d{3}\d{12}$")
    },
    "IS": {
        "length": 26,
        "regex": re.compile(r"^\d{4}\d{2}\d{6}\d{10}$")
    },
    "IT": {
        "length": 27,
        "regex": re.compile(r"^[A-Z]{1}\d{5}\d{5}[A-Za-z0-9]{12}$")
    },
    "JO": {
        "length": 30,
        "regex": re.compile(r"^[A-Z]{4}\d{4}[A-Za-z0-9]{18}$")
    },
    "KW": {
        "length": 30,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{22}$")
    },
    "KZ": {
        "length": 20,
        "regex": re.compile(r"^\d{3}[A-Za-z0-9]{13}$")
    },
    "LB": {
        "length": 28,
        "regex": re.compile(r"^\d{4}[A-Za-z0-9]{20}$")
    },
    "LC": {
        "length": 32,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{24}$")
    },
    "LI": {
        "length": 21,
        "regex": re.compile(r"^\d{5}[A-Za-z0-9]{12}$")
    },
    "LT": {
        "length": 20,
        "regex": re.compile(r"^\d{5}\d{11}$")
    },
    "LU": {
        "length": 20,
        "regex": re.compile(r"^\d{3}[A-Za-z0-9]{13}$")
    },
    "LV": {
        "length": 21,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{13}$")
    },
    "LY": {
        "length": 25,
        "regex": re.compile(r"^\d{3}\d{3}\d{15}$")
    },
    "MC": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}[A-Za-z0-9]{11}\d{2}$")
    },
    "MD": {
        "length": 24,
        "regex": re.compile(r"^[A-Za-z0-9]{2}[A-Za-z0-9]{18}$")
    },
    "ME": {
        "length": 22,
        "regex": re.compile(r"^\d{3}\d{13}\d{2}$")
    },
    "MK": {
        "length": 19,
        "regex": re.compile(r"^\d{3}[A-Za-z0-9]{10}\d{2}$")
    },
    "MN": {
        "length": 20,
        "regex": re.compile(r"^\d{4}\d{12}$")
    },
    "MR": {
        "length": 27,
        "regex": re.compile(r"^\d{5}\d{5}\d{11}\d{2}$")
    },
    "MT": {
        "length": 31,
        "regex": re.compile(r"^[A-Z]{4}\d{5}[A-Za-z0-9]{18}$")
    },
    "MU": {
        "length": 30,
        "regex": re.compile(r"^[A-Z]{4}\d{2}\d{2}\d{12}\d{3}[A-Z]{3}$")
    },
    "NI": {
        "length": 28,
        "regex": re.compile(r"^[A-Z]{4}\d{20}$")
    },
    "NL": {
        "length": 18,
        "regex": re.compile(r"^[A-Z]{4}\d{10}$")
    },
    "NO": {
        "length": 15,
        "regex": re.compile(r"^\d{4}\d{6}\d{1}$")
    },
    "OM": {
        "length": 23,
        "regex": re.compile(r"^\d{3}[A-Za-z0-9]{16}$")
    },
    "PK": {
        "length": 24,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{16}$")
    },
    "PL": {
        "length": 28,
        "regex": re.compile(r"^\d{8}\d{16}$")
    },
    "PS": {
        "length": 29,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{21}$")
    },
    "PT": {
        "length": 25,
        "regex": re.compile(r"^\d{4}\d{4}\d{11}\d{2}$")
    },
    "QA": {
        "length": 29,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{21}$")
    },
    "RO": {
        "length": 24,
        "regex": re.compile(r"^[A-Z]{4}[A-Za-z0-9]{16}$")
    },
    "RS": {
        "length": 22,
        "regex": re.compile(r"^\d{3}\d{13}\d{2}$")
    },
    "RU": {
        "length": 33,
        "regex": re.compile(r"^\d{9}\d{5}[A-Za-z0-9]{15}$")
    },
    "SA": {
        "length": 24,
        "regex": re.compile(r"^\d{2}[A-Za-z0-9]{18}$")
    },
    "SC": {
        "length": 31,
        "regex": re.compile(r"^[A-Z]{4}\d{2}\d{2}\d{16}[A-Z]{3}$")
    },
    "SD": {
        "length": 18,
        "regex": re.compile(r"^\d{2}\d{12}$")
    },
    "SE": {
        "length": 24,
        "regex": re.compile(r"^\d{3}\d{16}\d{1}$")
    },
    "SI": {
        "length": 19,
        "regex": re.compile(r"^\d{5}\d{8}\d{2}$")
    },
    "SK": {
        "length": 24,
        "regex": re.compile(r"^\d{4}\d{6}\d{10}$")
    },
    "SM": {
        "length": 27,
        "regex": re.compile(r"^[A-Z]{1}\d{5}\d{5}[A-Za-z0-9]{12}$")
    },
    "SO": {
        "length": 23,
        "regex": re.compile(r"^\d{4}\d{3}\d{12}$")
    },
    "ST": {
        "length": 25,
        "regex": re.compile(r"^\d{4}\d{4}\d{11}\d{2}$")
    },
    "SV": {
        "length": 28,
        "regex": re.compile(r"^[A-Z]{4}\d{20}$")
    },
    "TL": {
        "length": 23,
        "regex": re.compile(r"^\d{3}\d{14}\d{2}$")
    },
    "TN": {
        "length": 24,
        "regex": re.compile(r"^\d{2}\d{3}\d{13}\d{2}$")
    },
    "TR": {
        "length": 26,
        "regex": re.compile(r"^\d{5}\d{1}[A-Za-z0-9]{16}$")
    },
    "UA": {
        "length": 29,
        "regex": re.compile(r"^\d{6}[A-Za-z0-9]{19}$")
    },
    "VA": {
        "length": 22,
        "regex": re.compile(r"^\d{3}\d{15}$")
    },
    "VG": {
        "length": 24,
        "regex": re.compile(r"^[A-Z]{4}\d{16}$")
    },
    "XK": {
        "length": 20,
        "regex": re.compile(r"^\d{4}\d{10}\d{2}$")
    }
}


def init_udf(session: Session):
    """
    Initialize Snowflake UDFs for IBAN validation
    
    Args:
        session: Snowflake Snowpark session
    
    Returns:
        Tuple of UDF function names
    """

    def calculate_checksum(iban: str) -> str:
        """
        Calculate IBAN checksum :
        1. Move the four initial characters to the end of the string
        2. Replace each letter with two digits, where A = 10, B = 11, ..., Z = 35
        3. Perform modulo 97 calculation chunk by chunk (to prevent overflow)
        
        Args:
            iban (str): IBAN to validate
        
        Returns:
            str: Checksum result as string
        """
        if not iban:
            return 'False'
        
        rearranged = iban[4:] + iban[:4]
        
        numeric_iban = rearranged.translate(ALPHABET_CONVERSION)
        
        current_value = numeric_iban
        for _ in range(4):
            chunk1 = int(current_value[:15]) % 97
            chunk2 = current_value[15:]
            current_value = str(chunk1) + chunk2
        
        return str(int(current_value) % 97 == 1)

    def validate_iban(iban: str) -> bool:
        """
        Validate IBAN using multiple criteria :
        1. Check if country code is valid
        2. Check if length is correct
        3. Check if BBAN format is correct
        4. Perform checksum validation
        Validation fails if any of the above criteria is not met
        
        Args:
            iban (str): IBAN to validate
        
        Returns:
            bool: Whether IBAN is valid
        """
        if not iban:
            return False
        
        cleaned_iban = ''.join(c for c in iban.upper() if c.isalnum())
        if len(cleaned_iban) < 4:
          return False
        
        # Country check
        country_code = cleaned_iban[:2]
        if country_code not in IBAN_VALIDATION_RULES:
            return False
        
        rules = IBAN_VALIDATION_RULES[country_code]
        
        # length check
        if len(cleaned_iban) != rules['length']:
            return False
        
        # BBAN format check
        if not rules['regex'].match(cleaned_iban[4:]):
            return False
        
        # Checksum validation
        return calculate_checksum(cleaned_iban) == 'True'
    
    try:
        session.udf.register(
            calculate_checksum, 
            name='calculate_checksum',
            input_types=[StringType()],
            return_type=StringType()
        )
    except Exception as e:
        print(f"Error registering UDF calculate_checksum: {e}")

    try:
        session.udf.register(
            validate_iban, 
            name="validate_iban",
            input_types=[StringType()],
            return_type=BooleanType()
        )
    except Exception as e:
        print(f"Error registering UDF validate_iban: {e}")


def check_ibans(col_name: str) -> Column:
    """
    Validates IBAN using the validate_iban UDF
    
    Args:
    col_name: Column name containing IBAN

    Returns:
    Column containing boolean values for each row
    """
    return F.call_udf('TESTS_UNITAIRES.validate_iban', F.col(col_name))
