import re
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf


#spark = SparkSession.builder.appName("IBANValidation").getOrCreate()


def verifier_email_regex(email:str) -> bool:

    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return bool(re.match(email_regex, email))

is_valid_email = udf(verifier_email_regex, BooleanType())


"""data = [("test@example.com",), ("mic@example.com",), ("peter@example.com",),("mauvais-email@",),("@com",)]
df = spark.createDataFrame(data, ["email"])
df=df.withColumn("emailValid",is_valid_email("email"))

df.show()"""