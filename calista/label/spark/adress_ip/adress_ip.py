import validators
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, col
from pyspark.sql import SparkSession


def is_valid_ip(ip: str) -> bool:
    return bool(validators.ipv4(ip) or validators.ipv6(ip))

is_valid_ip_udf = udf(is_valid_ip, BooleanType())
