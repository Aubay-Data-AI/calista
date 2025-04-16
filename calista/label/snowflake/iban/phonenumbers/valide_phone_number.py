from typing import Optional
import snowflake.snowpark.functions as F
from snowflake.snowpark import Column, Session
from snowflake.snowpark.types import StringType, BooleanType
import phonenumbers


def init_udf(session: Session):
    def validate_phone_snowflake(phone: str) -> bool:
        if phone is None:
            return False
        try:
            parsed = phonenumbers.parse(phone, None)
            return phonenumbers.is_valid_number(parsed)
        except phonenumbers.NumberParseException:
            return False

    try:
        session.udf.register(
            validate_phone_snowflake,
            name="validate_phone",
            input_types=[StringType()],
            return_type=BooleanType()
        )
    except Exception as e:
        print(f"Error registering UDF validate_phone: {e}")


def is_phone(col_name: str) -> Column:
    return F.call_udf('validate_phone', F.col(col_name))
