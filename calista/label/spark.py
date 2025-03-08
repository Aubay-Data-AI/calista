from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

from email_validator import validate_email, EmailNotValidError
import schwifty
import validators



@udf(returnType= BooleanType(), useArrow=True)
def is_valid_iban(iban: str) -> bool:
    """
        is valid iban It will be used to check an ip_adress

        Args:
            the function takes a string iban as a argument

        Returns:
        bool: the function returns a boolean.

    """
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
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False
    

@udf(returnType= BooleanType(), useArrow=True)
def is_valid_ip_adress(adress_ip: str) -> bool:
    """
        is valid ip_adress It will be used to check an ip_adress

        Args:
            the function takes a string adress_ip as a argument

        Returns:
        bool: the function returns a boolean.

    """
    return bool(validators.ipv4(adress_ip) or validators.ipv6(adress_ip))
