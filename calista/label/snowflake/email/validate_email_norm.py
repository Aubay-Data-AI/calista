import re
import unicodedata
from typing import Optional, Union, Tuple
import snowflake.snowpark.functions as F
from snowflake.snowpark import Column
import calista.core._conditions as cond
from snowflake.snowpark.types import StringType, BooleanType
from email_validator import validate_email, EmailNotValidError
from snowflake.snowpark import Session


def init_udf(session: Session):
   

    

    def validate_email_snowflake(email: str) -> bool:    
        if email is None:
            return False
        try:
            validate_email(email, check_deliverability=False)
            return True
        except EmailNotValidError:
            return False
            
    
       
    try:
        session.udf.register(
            validate_email_snowflake, 
            name="validate_email",
            input_types=[StringType()],
            return_type=BooleanType()
        )
    except Exception as e:
        print(f"Error registering UDF validate_iban: {e}")

        
  
    
   
def is_email(col_name: str) -> Column:
    return F.call_udf('TESTS_UNITAIRES.validate_email', F.col(col_name))
