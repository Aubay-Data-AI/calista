from typing import Optional
import snowflake.snowpark.functions as F
from snowflake.snowpark import Column, Session
from snowflake.snowpark.types import StringType, BooleanType
import validators


def init_udf(session: Session):
    def validate_ip_snowflake(ip: str) -> bool:
        if ip is None:
            return False
        return validators.ipv4(ip) or validators.ipv6(ip)

    try:
        session.udf.register(
            validate_ip_snowflake,
            name="validate_ip",
            input_types=[StringType()],
            return_type=BooleanType()
        )
    except Exception as e:
        print(f"Error registering UDF validate_ip: {e}")


def is_ip(col_name: str) -> Column:
    return F.call_udf('validate_ip', F.col(col_name))