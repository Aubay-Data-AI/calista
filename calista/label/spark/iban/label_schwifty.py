from schwifty import IBAN
from pyspark.sql import Column
from pyspark.sql import functions as F


def is_iban(self, col_name : str) -> Column:
    def validate_iban_udf(value):
        try:
            if value is None:
                return False
            IBAN(str(value))
            return True
        except ValueError:
            return False
    validate_iban = F.udf(validate_iban_udf, "boolean")

    cleaned_col = F.regexp_replace(F.coalesce(F.col(col_name), F.lit("")), "[^a-zA-Z0-9]", "").alias(
        "cleaned_col")
    return validate_iban(cleaned_col)
 