from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

from email_validator import validate_email, EmailNotValidError
import schwifty
import validators
import phonenumbers


@udf(returnType= BooleanType(), useArrow=True)
def is_valid_iban(iban: str) -> bool:
    """
        is valid iban It will be used to check an ip_adress

        Args:
            the function takes a string iban as a argument

        Returns:
        bool: the function returns a boolean.

    """
    if not iban:
        return False
    return bool(schwifty.IBAN(iban, allow_invalid=True).is_valid)


@udf(returnType= BooleanType(), useArrow=True)
def is_valid_email(email: str) -> bool:
    """
        is valid email It will be used to check an ip_adress

        Args:
            the function takes a string email as a argument

        Returns:
        bool: the function returns a boolean.

    """
    if not email: 
        return False
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False
    

@udf(returnType= BooleanType(), useArrow=True)
def email_validator(email: str) -> bool:
    """
        email_validator It will be used to check an email

        Args:
            the function takes a string email as a argument

        Returns:
        bool: the function returns a boolean.

    """
    return bool(validators.email(email))


@udf(returnType= BooleanType(), useArrow=True)
def is_valid_ip_address(address_ip: str) -> bool:
    """
        is valid ip_address It will be used to check an ip_address

        Args:
            the function takes a string address_ip as a argument

        Returns:
        bool: the function returns a boolean.

    """
    return bool(validators.ipv4(address_ip) or validators.ipv6(address_ip))


@udf(returnType= BooleanType(), useArrow=True)
def is_valid_phone_number(phone_number: str) -> bool:
    """
        is valid phone_number It will be used to check an phone_number

        Args:
            the function takes a string phone_number as a argument

        Returns:
        bool: the function returns a boolean.

    """
    if not phone_number:
        return False
    try:
        parsed_number = phonenumbers.parse(phone_number, None)
        return phonenumbers.is_valid_number(parsed_number)
    except phonenumbers.NumberParseException:
        return False