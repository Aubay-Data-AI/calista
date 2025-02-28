from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import schwifty

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import schwifty

# Initialiser Spark
#spark = SparkSession.builder.appName("IBANValidation").getOrCreate()

def is_valid_iban(iban: str) -> bool:
    """ Vérifie si un IBAN est valide en utilisant schwifty """
    return bool(schwifty.IBAN(iban, allow_invalid=True).is_valid)

# Convertir la fonction en UDF PySpark
is_valid_iban_udf = udf(is_valid_iban, BooleanType())