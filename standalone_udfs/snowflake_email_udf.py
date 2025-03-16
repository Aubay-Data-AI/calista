import re
from snowflake.snowpark.types import BooleanType, StringType
from email_validator import validate_email, EmailNotValidError




def validate_email_snowflake(email: str) -> bool:
    if email is None:
        return False
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False

