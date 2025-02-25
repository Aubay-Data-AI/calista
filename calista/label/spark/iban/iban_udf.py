from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import re

# Initialiser Spark
#spark = SparkSession.builder.appName("IBANValidation").getOrCreate()

# Définir la regex pour l'IBAN
iban_regex = re.compile(r'^[A-Z]{2}[0-9]{2}[A-Z0-9]{1,30}$')

def is_valid_iban(iban: str) -> bool:
    """ Vérifie si un IBAN est valide """
    if not isinstance(iban, str) or not iban_regex.match(iban):
        return False
    try:
        # Réarranger l'IBAN
        rearranged_iban = iban[4:] + iban[:4]
        # Convertir les lettres en chiffres (A=10, ..., Z=35)
        numeric_iban = ''.join(str(int(char, 36)) for char in rearranged_iban)
        # Vérifier si divisible par 97
        return int(numeric_iban) % 97 == 1
    except:
        return False

# Convertir la fonction en UDF
is_valid_iban_udf = udf(is_valid_iban, BooleanType())
''' 
#Exemple de DataFrame Spark
data = [("FR7630006000011234567890189",), ("GB29NWBK60161331926819",), ("123456789",)]
df = spark.createDataFrame(data, ["iban_column"])

# Appliquer la validation IBAN sur la colonne
df = df.withColumn("is_valid", is_valid_iban_udf(col("iban_column")))

# Afficher le résultat
df.show()
 '''
