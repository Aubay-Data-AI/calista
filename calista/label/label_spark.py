import json
import re

from pyspark.sql.functions import explode, col, upper, expr, from_json, length, upper, when
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, MapType, ArrayType
from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.group import GroupedData
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType


def convert_bban_spec_to_regex(spec: str) -> str:
    _spec_to_re = {"n": r"\d", "a": r"[A-Z]", "c": r"[A-Za-z0-9]", "e": r" "}
    spec_re = rf"(\d+)(!)?([{''.join(_spec_to_re.keys())}])"
    def convert(match: re.Match) -> str:
        quantifier = ("{{{}}}" if match.group(2) else "{{1,{}}}").format(match.group(1))
        return _spec_to_re[match.group(3)] + quantifier
    return rf"^{re.sub(spec_re, convert, spec)}$"


def flatten_iban_data(iban_specifications):
    flattened_data = []
    for country_code, specs in iban_specifications.items():
        positions_str = json.dumps(specs['positions'])
        bban_spec = specs['bban_spec']
        bban_regex = convert_bban_spec_to_regex(bban_spec)
        flattened_data.append({
            'country': specs['country'],
            'in_sepa_zone': specs['in_sepa_zone'],
            'bban_spec': bban_spec,
            'bban_length': specs['bban_length'],
            'iban_spec': specs['iban_spec'],
            'iban_length': specs['iban_length'],
            'positions': positions_str,
            'bban_regex': bban_regex
        })
    return flattened_data


def create_df():
    spark = SparkSession.builder.appName("IBANValidation").getOrCreate()
    json_path = "/content/list_iban.json"
    with open(json_path, "r") as file:
        iban_specifications = json.load(file)
    schema = StructType([
    StructField("country", StringType(), False),
    StructField("in_sepa_zone", BooleanType(), False),
    StructField("bban_spec", StringType(), False),
    StructField("bban_length", IntegerType(), False),
    StructField("iban_spec", StringType(), False),
    StructField("iban_length", IntegerType(), False),
    StructField("positions", StringType(), False),
    StructField("bban_regex", StringType(), False)
    ])
    flattened_data = flatten_iban_data(iban_specifications)
    df = spark.createDataFrame(flattened_data, schema)
    return df


def is_iban(condition: cond.IsIban) -> Column:
    iban_specifications = create_df()
    cleaned_str_col = F.upper(F.regexp_replace(F.col(condition.col_name), "[^A-Z0-9]", ""))

    country_code_col = F.substring(F.col(condition.col_name), 1, 2)
    iban_length_map = {row["country"]: row["iban_length"] for row in iban_specifications.collect()}
    iban_length_col = F.create_map([F.lit(x) for pair in iban_length_map.items() for x in pair]).getItem(
        country_code_col)
    valid_length = (F.length(cleaned_str_col) == iban_length_col)

    bban_spec_map = {row["country"]: row["bban_regex"] for row in iban_specifications.collect()}
    bban_regex_col = F.create_map([F.lit(x) for pair in bban_spec_map.items() for x in pair]).getItem(country_code_col)
    string_length_col = F.length(cleaned_str_col)
    bban_col = cleaned_str_col.substr(F.lit(5), string_length_col - F.lit(4))
    valid_bban = F.regexp_like(bban_col, bban_regex_col)

    cleaned_col = F.concat(
        F.substring(cleaned_str_col, 5, 34), F.substring(cleaned_str_col, 1, 4)
    )
    alphabet_conversion = {chr(i + 65): str(i + 10) for i in range(26)}
    for letter, value in alphabet_conversion.items():
        cleaned_col = F.regexp_replace(cleaned_col, letter, value)
    is_iban_col = (
            F.concat(
                (F.substring(cleaned_col, 1, 15) % 97).cast("bigint").cast("string"),
                F.substring(cleaned_col, 16, 34),
            ).cast("bigint")
            % 97
    )
    valid_checksum = (is_iban_col == 1)
    return valid_length & valid_checksum & valid_bban