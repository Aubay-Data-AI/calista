from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

from email_validator import validate_email, EmailNotValidError
import schwifty


@udf(returnType=BooleanType(), useArrow=True)
def is_valid_iban(iban: str) -> bool:
    """ Vérifie si un IBAN est valide en utilisant schwifty """
    return bool(schwifty.IBAN(iban, allow_invalid=True).is_valid)


@udf(returnType=BooleanType(), useArrow=True)
def is_valid_email(email: str) -> bool:
    """Retourne True si l'email est valide, False sinon."""
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False