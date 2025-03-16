

from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType, BooleanType
import shutil
import os
from email_validator import validate_email, EmailNotValidError



def validate_email_snowflake(email: str) -> bool:
    
    if email is None:
        return False
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False

        # Fermer la session
      
    


def main():
    CONNECTION_PARAMETERS =  {
        'user':'RAMOSLENAMOS',
        'password':'',
        'account':'XYUWMIA-DL07627',
        'warehouse':'COMPUTE_WH',
        'database':'RESSOURCES',
        'schema':'TESTS_UNITAIRES'
        }
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    session.add_packages("email-validator")
    session.udf.register(
        validate_email_snowflake,
        name="test_validate",
        input_types=[StringType()],
        return_type=BooleanType(),
        replace=True,
        packages=["email_validator"],  # critical dependency
    )

    session_id = session.sql("SELECT CURRENT_SESSION()").collect()[0][0]
    print(f"✅ UDF registered in session {session_id}")   
    result = session.sql("SHOW USER FUNCTIONS").collect()
    udf_names = [row['name'].upper() for row in result]

    if "test_validate" in udf_names:
        print("✅ L'UDF 'validate_email' est bien enregistrée dans Snowflake !")
    else:
        print("❌ L'UDF 'validate_email' n'est pas enregistrée.")
    query_result = session.sql("SELECT test_validate(EMAIL) AS is_valid FROM TEST_DATASET_100").collect()

    # Display results
    for row in query_result:
        print(f"✅ Email validation result: {row['IS_VALID']}")  
       
    
if __name__ == "__main__":
    main()






    

    


