import json
import re
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType


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


def save_data(df, iban_length_map, bban_spec_map):
    if not os.path.exists('/content/iban_data'):
        os.makedirs('/content/iban_data')
    df.write.mode("overwrite").parquet("/content/iban_data/iban_data.parquet")
    with open("/content/iban_data/iban_length_map.json", "w") as f:
        json.dump(iban_length_map, f)
    with open("/content/iban_data/bban_spec_map.json", "w") as f:
        json.dump(bban_spec_map, f)


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
    iban_length_map = {row["country"]: row["iban_length"] for row in df.collect()}
    bban_spec_map = {row["country"]: row["bban_regex"] for row in df.collect()}

    save_data(df, iban_length_map, bban_spec_map)
    return df, iban_length_map, bban_spec_map


def load_saved_data():
    spark = SparkSession.builder.appName("IBANValidation").getOrCreate()
    df = spark.read.parquet("/content/iban_data/iban_data.parquet")
    with open("/content/iban_data/iban_length_map.json", "r") as f:
        iban_length_map = json.load(f)
    with open("/content/iban_data/bban_spec_map.json", "r") as f:
        bban_spec_map = json.load(f)
    return df, iban_length_map, bban_spec_map


def is_iban(self, condition: cond.IsIban) -> Column:
    iban_specifications, iban_length_map, bban_spec_map = load_saved_data()
    cleaned_str_col = F.upper(F.regexp_replace(F.col(condition.col_name), "[^A-Z0-9]", ""))

    country_code_col = F.substring(F.col(condition.col_name), 1, 2)
    iban_length_col = F.create_map([F.lit(x) for pair in iban_length_map.items() for x in pair]).getItem(
        country_code_col)
    valid_length = (F.length(cleaned_str_col) == iban_length_col)

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

    current_value = cleaned_col
    chunk1 = F.substring(current_value, 1, 15).cast("bigint") % 97
    chunk2 = F.substring(current_value, 16, 55)
    current_value = F.concat(chunk1.cast("string"), chunk2)

    chunk1 = F.substring(current_value, 1, 15).cast("bigint") % 97
    chunk2 = F.substring(current_value, 16, 55)
    current_value = F.concat(chunk1.cast("string"), chunk2)

    chunk1 = F.substring(current_value, 1, 15).cast("bigint") % 97
    chunk2 = F.substring(current_value, 16, 55)
    current_value = F.concat(chunk1.cast("string"), chunk2)

    chunk1 = F.substring(current_value, 1, 15).cast("bigint") % 97
    chunk2 = F.substring(current_value, 16, 55)
    current_value = F.concat(chunk1.cast("string"), chunk2)

    final_mod = current_value.cast("bigint") % 97
    valid_checksum = (final_mod == 1)

    return valid_length & valid_bban & valid_checksum