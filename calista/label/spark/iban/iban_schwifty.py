from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import schwifty

# Initialiser Spark
#spark = SparkSession.builder.appName("IBANValidation").getOrCreate()

def is_valid_iban(iban: str) -> bool:
    """ Vérifie si un IBAN est valide en utilisant schwifty """
    try:
        schwifty.IBAN(iban)  # Vérifie la structure et la validité avec schwifty
        return True
    except ValueError:
        return False

# Convertir la fonction en UDF PySpark
is_valid_iban_schwifty = udf(is_valid_iban, BooleanType())

# Exemple de DataFrame Spark
'''data = [("FR7630006000011234567890189",), ("GB29NWBK60161331926819",), ("123456789",)]
df = spark.createDataFrame(data, ["iban_column"])

# Appliquer la validation IBAN sur la colonne
df = df.withColumn("is_valid", is_valid_iban_udf(col("iban_column")))

# Afficher le résultat
df.show()
'''