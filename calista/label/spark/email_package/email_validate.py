from email_validator import validate_email, EmailNotValidError
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf,col

spark = SparkSession.builder.appName("IBANValidation").getOrCreate()


def verifier_email(email:str)->bool:
    """Retourne True si l'email est valide, False sinon."""
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False


is_valid_email = udf(verifier_email, BooleanType())

data = [("test@example.com",), ("mic@example.com",), ("peter@example.com",),("mauvais-email@",),("@com",)]
df = spark.createDataFrame(data, ["email"])
df=df.withColumn("email_Valid",is_valid_email("email"))

df.show()
