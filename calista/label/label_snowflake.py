import re
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType, BooleanType
from snowflake.snowpark.session import Session


# Check if a UDF exists in Snowflake
def check_udf_exists(session: Session, udf_name: str) -> bool:
    """
    Check if a UDF exists in Snowflake
    
    Args:
        session: Snowflake session
        udf_name: Name of the UDF to check
        
    Returns:
        bool: True if UDF exists, False otherwise
    """
    result = session.sql(f"""
        SELECT COUNT(*) as count 
        FROM information_schema.routines 
        WHERE routine_type = 'FUNCTION' 
        AND routine_name = '{udf_name.upper()}'
    """).collect()
    return result[0]['COUNT'] > 0


def create_iban_validation_udfs(session: Session):
    """Creates all required UDFs for IBAN validation in Snowflake"""
    
    # IBAN Specs
    IBAN_SPECIFICATIONS = {
        "AD": {
            "country": "AD",
            "in_sepa_zone": True,
            "bban_spec": "4!n4!n12!c",
            "bban_length": 20,
            "iban_spec": "AD2!n4!n4!n12!c",
            "iban_length": 24,
            "positions": {
                "account_code": [8, 20],
                "bank_code": [0, 4],
                "branch_code": [4, 8]
            }
        },
        "AE": {
            "country": "AE",
            "in_sepa_zone": False,
            "bban_spec": "3!n16!n",
            "bban_length": 19,
            "iban_spec": "AE2!n3!n16!n",
            "iban_length": 23,
            "positions": {
                "account_code": [3, 19],
                "bank_code": [0, 3]
            }
        }
    }
    
    #UDF to convert BBAN spec to regex
    @udf(name='CONVERT_BBAN_SPEC_TO_REGEX', is_permanent=True, stage_location='@%')
    def convert_bban_spec_to_regex(spec: str) -> str:
        spec_to_re = {"n": r"\d", "a": r"[A-Z]", "c": r"[A-Za-z0-9]", "e": r" "}
        spec_re = rf"(\d+)(!)?([{''.join(spec_to_re.keys())}])"
        
        def convert(match: re.Match) -> str:
            quantifier = ("{{{}}}" if match.group(2) else "{{1,{}}}").format(match.group(1))
            return spec_to_re[match.group(3)] + quantifier
            
        return rf"^{re.sub(spec_re, convert, spec)}$"

    #UDF to validate IBAN checksum
    @udf(name='VALIDATE_IBAN_CHECKSUM', is_permanent=True, stage_location='@%')
    def validate_iban_checksum_udf(iban: str) -> bool:
        if not iban:
            return False
        rearranged_iban = iban[4:] + iban[:4]
        expanded_iban = ''
        for char in rearranged_iban:
            if char.isdigit():
                expanded_iban += char
            else:
                expanded_iban += str(ord(char) - 55)
        return int(expanded_iban) % 97 == 1


    #Main validation function
    @udf(name='VALIDATE_IBAN', is_permanent=True, stage_location='@%')
    def validate_iban_udf(iban: str) -> bool:
        try:
            if not iban:
                return False
                
            iban = re.sub(r'[^a-zA-Z0-9]', '', iban)
            iban = iban.upper()
            
            country_code = iban[:2]
            
            if country_code not in IBAN_SPECIFICATIONS:
                return False
                
            spec = IBAN_SPECIFICATIONS[country_code]
            
            if len(iban) != spec["iban_length"]:
                return False
                
            bban_spec_regex = convert_bban_spec_to_regex(spec["bban_spec"])
            bban = iban[4:]
            
            if not re.match(bban_spec_regex, bban):
                return False
                
            return validate_iban_checksum_udf(iban)
            
        except Exception:
            return False

    # Register the UDFs with Snowflake
    session.udf.register(convert_bban_spec_to_regex)
    session.udf.register(validate_iban_checksum_udf)
    session.udf.register(validate_iban_udf)

def validate_iban(session: Session, iban: str) -> bool:
    """
    Validates an IBAN using Snowflake UDFs. Creates UDFs if they don't exist.
    
    Args:
        session: Snowflake session
        iban: IBAN string to validate
        
    Returns:
        bool: True if IBAN is valid, False otherwise
    """
    required_udfs = ['VALIDATE_IBAN', 'VALIDATE_IBAN_CHECKSUM', 'CONVERT_BBAN_SPEC_TO_REGEX']
    
    # Check if UDFs exists
    missing_udfs = any(not check_udf_exists(session, udf_name) for udf_name in required_udfs)
    # Create UDFs if they don't exist
    if missing_udfs:
        create_iban_validation_udfs(session)
    
    # query
    validation_query = f"""
    SELECT VALIDATE_IBAN('{iban}') as is_valid
    """
    result = session.sql(validation_query).collect()
    return bool(result[0]['IS_VALID'])