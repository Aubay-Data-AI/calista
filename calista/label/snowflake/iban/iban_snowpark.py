import snowflake.snowpark.functions as F
from snowflake.snowpark.column import Column
from typing import Dict, Union


IBAN_VALIDATION_RULES: Dict[str, Dict[str, Union[int, str]]] = {
  "AD": {
    "length": 24,
    "regex": "^\\d{4}\\d{4}[A-Za-z0-9]{12}$"
  },
  "AE": {
    "length": 23,
    "regex": "^\\d{3}\\d{16}$"
  },
  "AL": {
    "length": 28,
    "regex": "^\\d{8}[A-Za-z0-9]{16}$"
  },
  "AT": {
    "length": 20,
    "regex": "^\\d{5}\\d{11}$"
  },
  "AZ": {
    "length": 28,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{20}$"
  },
  "BA": {
    "length": 20,
    "regex": "^\\d{3}\\d{3}\\d{8}\\d{2}$"
  },
  "BE": {
    "length": 16,
    "regex": "^\\d{3}\\d{7}\\d{2}$"
  },
  "BG": {
    "length": 22,
    "regex": "^[A-Z]{4}\\d{4}\\d{2}[A-Za-z0-9]{8}$"
  },
  "BH": {
    "length": 22,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{14}$"
  },
  "BI": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}\\d{11}\\d{2}$"
  },
  "BR": {
    "length": 29,
    "regex": "^\\d{8}\\d{5}\\d{10}[A-Z]{1}[A-Za-z0-9]{1}$"
  },
  "BY": {
    "length": 28,
    "regex": "^[A-Za-z0-9]{4}\\d{4}[A-Za-z0-9]{16}$"
  },
  "CH": {
    "length": 21,
    "regex": "^\\d{5}[A-Za-z0-9]{12}$"
  },
  "CR": {
    "length": 22,
    "regex": "^\\d{4}\\d{14}$"
  },
  "CY": {
    "length": 28,
    "regex": "^\\d{3}\\d{5}[A-Za-z0-9]{16}$"
  },
  "CZ": {
    "length": 24,
    "regex": "^\\d{4}\\d{6}\\d{10}$"
  },
  "DE": {
    "length": 22,
    "regex": "^\\d{8}\\d{10}$"
  },
  "DJ": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}\\d{11}\\d{2}$"
  },
  "DK": {
    "length": 18,
    "regex": "^\\d{4}\\d{9}\\d{1}$"
  },
  "DO": {
    "length": 28,
    "regex": "^[A-Za-z0-9]{4}\\d{20}$"
  },
  "EE": {
    "length": 20,
    "regex": "^\\d{2}\\d{2}\\d{11}\\d{1}$"
  },
  "EG": {
    "length": 29,
    "regex": "^\\d{4}\\d{4}\\d{17}$"
  },
  "ES": {
    "length": 24,
    "regex": "^\\d{4}\\d{4}\\d{1}\\d{1}\\d{10}$"
  },
  "FI": {
    "length": 18,
    "regex": "^\\d{3}\\d{11}$"
  },
  "AX": {
    "length": 18,
    "regex": "^\\d{3}\\d{11}$"
  },
  "FK": {
    "length": 18,
    "regex": "^[A-Z]{2}\\d{12}$"
  },
  "FO": {
    "length": 18,
    "regex": "^\\d{4}\\d{9}\\d{1}$"
  },
  "FR": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "GF": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "GP": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "MQ": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "RE": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "PF": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "TF": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "YT": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "NC": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "BL": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "MF": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "PM": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "WF": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "GB": {
    "length": 22,
    "regex": "^[A-Z]{4}\\d{6}\\d{8}$"
  },
  "IM": {
    "length": 22,
    "regex": "^[A-Z]{4}\\d{6}\\d{8}$"
  },
  "JE": {
    "length": 22,
    "regex": "^[A-Z]{4}\\d{6}\\d{8}$"
  },
  "GG": {
    "length": 22,
    "regex": "^[A-Z]{4}\\d{6}\\d{8}$"
  },
  "GE": {
    "length": 22,
    "regex": "^[A-Z]{2}\\d{16}$"
  },
  "GI": {
    "length": 23,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{15}$"
  },
  "GL": {
    "length": 18,
    "regex": "^\\d{4}\\d{9}\\d{1}$"
  },
  "GR": {
    "length": 27,
    "regex": "^\\d{3}\\d{4}[A-Za-z0-9]{16}$"
  },
  "GT": {
    "length": 28,
    "regex": "^[A-Za-z0-9]{4}[A-Za-z0-9]{20}$"
  },
  "HR": {
    "length": 21,
    "regex": "^\\d{7}\\d{10}$"
  },
  "HU": {
    "length": 28,
    "regex": "^\\d{3}\\d{4}\\d{1}\\d{15}\\d{1}$"
  },
  "IE": {
    "length": 22,
    "regex": "^[A-Z]{4}\\d{6}\\d{8}$"
  },
  "IL": {
    "length": 23,
    "regex": "^\\d{3}\\d{3}\\d{13}$"
  },
  "IQ": {
    "length": 23,
    "regex": "^[A-Z]{4}\\d{3}\\d{12}$"
  },
  "IS": {
    "length": 26,
    "regex": "^\\d{4}\\d{2}\\d{6}\\d{10}$"
  },
  "IT": {
    "length": 27,
    "regex": "^[A-Z]{1}\\d{5}\\d{5}[A-Za-z0-9]{12}$"
  },
  "JO": {
    "length": 30,
    "regex": "^[A-Z]{4}\\d{4}[A-Za-z0-9]{18}$"
  },
  "KW": {
    "length": 30,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{22}$"
  },
  "KZ": {
    "length": 20,
    "regex": "^\\d{3}[A-Za-z0-9]{13}$"
  },
  "LB": {
    "length": 28,
    "regex": "^\\d{4}[A-Za-z0-9]{20}$"
  },
  "LC": {
    "length": 32,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{24}$"
  },
  "LI": {
    "length": 21,
    "regex": "^\\d{5}[A-Za-z0-9]{12}$"
  },
  "LT": {
    "length": 20,
    "regex": "^\\d{5}\\d{11}$"
  },
  "LU": {
    "length": 20,
    "regex": "^\\d{3}[A-Za-z0-9]{13}$"
  },
  "LV": {
    "length": 21,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{13}$"
  },
  "LY": {
    "length": 25,
    "regex": "^\\d{3}\\d{3}\\d{15}$"
  },
  "MC": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}[A-Za-z0-9]{11}\\d{2}$"
  },
  "MD": {
    "length": 24,
    "regex": "^[A-Za-z0-9]{2}[A-Za-z0-9]{18}$"
  },
  "ME": {
    "length": 22,
    "regex": "^\\d{3}\\d{13}\\d{2}$"
  },
  "MK": {
    "length": 19,
    "regex": "^\\d{3}[A-Za-z0-9]{10}\\d{2}$"
  },
  "MN": {
    "length": 20,
    "regex": "^\\d{4}\\d{12}$"
  },
  "MR": {
    "length": 27,
    "regex": "^\\d{5}\\d{5}\\d{11}\\d{2}$"
  },
  "MT": {
    "length": 31,
    "regex": "^[A-Z]{4}\\d{5}[A-Za-z0-9]{18}$"
  },
  "MU": {
    "length": 30,
    "regex": "^[A-Z]{4}\\d{2}\\d{2}\\d{12}\\d{3}[A-Z]{3}$"
  },
  "NI": {
    "length": 28,
    "regex": "^[A-Z]{4}\\d{20}$"
  },
  "NL": {
    "length": 18,
    "regex": "^[A-Z]{4}\\d{10}$"
  },
  "NO": {
    "length": 15,
    "regex": "^\\d{4}\\d{6}\\d{1}$"
  },
  "OM": {
    "length": 23,
    "regex": "^\\d{3}[A-Za-z0-9]{16}$"
  },
  "PK": {
    "length": 24,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{16}$"
  },
  "PL": {
    "length": 28,
    "regex": "^\\d{8}\\d{16}$"
  },
  "PS": {
    "length": 29,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{21}$"
  },
  "PT": {
    "length": 25,
    "regex": "^\\d{4}\\d{4}\\d{11}\\d{2}$"
  },
  "QA": {
    "length": 29,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{21}$"
  },
  "RO": {
    "length": 24,
    "regex": "^[A-Z]{4}[A-Za-z0-9]{16}$"
  },
  "RS": {
    "length": 22,
    "regex": "^\\d{3}\\d{13}\\d{2}$"
  },
  "RU": {
    "length": 33,
    "regex": "^\\d{9}\\d{5}[A-Za-z0-9]{15}$"
  },
  "SA": {
    "length": 24,
    "regex": "^\\d{2}[A-Za-z0-9]{18}$"
  },
  "SC": {
    "length": 31,
    "regex": "^[A-Z]{4}\\d{2}\\d{2}\\d{16}[A-Z]{3}$"
  },
  "SD": {
    "length": 18,
    "regex": "^\\d{2}\\d{12}$"
  },
  "SE": {
    "length": 24,
    "regex": "^\\d{3}\\d{16}\\d{1}$"
  },
  "SI": {
    "length": 19,
    "regex": "^\\d{5}\\d{8}\\d{2}$"
  },
  "SK": {
    "length": 24,
    "regex": "^\\d{4}\\d{6}\\d{10}$"
  },
  "SM": {
    "length": 27,
    "regex": "^[A-Z]{1}\\d{5}\\d{5}[A-Za-z0-9]{12}$"
  },
  "SO": {
    "length": 23,
    "regex": "^\\d{4}\\d{3}\\d{12}$"
  },
  "ST": {
    "length": 25,
    "regex": "^\\d{4}\\d{4}\\d{11}\\d{2}$"
  },
  "SV": {
    "length": 28,
    "regex": "^[A-Z]{4}\\d{20}$"
  },
  "TL": {
    "length": 23,
    "regex": "^\\d{3}\\d{14}\\d{2}$"
  },
  "TN": {
    "length": 24,
    "regex": "^\\d{2}\\d{3}\\d{13}\\d{2}$"
  },
  "TR": {
    "length": 26,
    "regex": "^\\d{5}\\d{1}[A-Za-z0-9]{16}$"
  },
  "UA": {
    "length": 29,
    "regex": "^\\d{6}[A-Za-z0-9]{19}$"
  },
  "VA": {
    "length": 22,
    "regex": "^\\d{3}\\d{15}$"
  },
  "VG": {
    "length": 24,
    "regex": "^[A-Z]{4}\\d{16}$"
  },
  "XK": {
    "length": 20,
    "regex": "^\\d{4}\\d{10}\\d{2}$"
  }
}

ALPHABET_CONVERSION = {chr(i + 65): str(i + 10) for i in range(26)}

ALPHABET_CONVERSION_EXPR = [
    (letter, value) for letter, value in ALPHABET_CONVERSION.items()
]


def check_ibans(col_name: str) -> Column:
    """
    Validates IBAN using 3 criteria in a short-circuit manner:
    1. Country code exists in IBAN_VALIDATION_RULES
    2. Length matches country specification
    3. BBAN format using country-specific regex
    4. IBAN checksum

    Args:
    col_name: Column name containing IBAN

    Returns:
    Column containing boolean values for each row
    """
    cleaned_iban = F.upper(F.regexp_replace(F.col(col_name), r'[^A-Z0-9]', ''))
    country_code = F.substr(cleaned_iban, 1, 2)

    # Checksum calculation function
    def calculate_checksum(iban_col: Column) -> Column:
        rearranged = F.concat(F.substr(iban_col, 5), F.substr(iban_col, 1, 4))
        for letter, value in ALPHABET_CONVERSION_EXPR:
            rearranged = F.regexp_replace(rearranged, letter, value)
        
        current_value = rearranged
        for _ in range(4):
            chunk1 = F.substring(current_value, 1, 15).cast("bigint") % 97
            chunk2 = F.substring(current_value, 16, 55)
            current_value = F.concat(chunk1.cast("string"), chunk2)
        
        return current_value.cast("bigint") % 97 == 1

    validation_cond = F.lit(False)
    for country, rules in IBAN_VALIDATION_RULES.items():
        validation_cond = F.iff(
            (country_code == country) & 
            (F.length(cleaned_iban) == rules["length"]) &
            F.substr(cleaned_iban, 5).rlike(rules["regex"]),
            F.lit(True),
            validation_cond
        )

    # Validation chain: country + length + regex -> checksum
    # Return False if any condition fails
    result = F.when(
        validation_cond,
        calculate_checksum(cleaned_iban)
    ).otherwise(False)

    return result
