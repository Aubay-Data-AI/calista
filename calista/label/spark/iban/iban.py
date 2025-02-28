from typing import Dict, Tuple
import json
from pyspark.sql import Column
from pyspark.sql import functions as F
from calista.label.resources.tools import get_file_path


def load_saved_data() -> Tuple[Dict[str, int], Dict[str, str]]:
    json_path_iban_length_map = get_file_path("iban_length_map.json")
    with open(json_path_iban_length_map, "r") as f:
        iban_length_map = json.load(f)
    json_path_bban_spec_map = get_file_path("bban_spec_map.json")
    with open(json_path_bban_spec_map, "r") as f:
        bban_spec_map = json.load(f)
    return iban_length_map, bban_spec_map


def is_valid_iban(col_name : str) -> Column:
    iban_length_map, bban_spec_map = load_saved_data()
    cleaned_str_col = F.upper(F.regexp_replace(F.col(col_name), "[^A-Z0-9]", ""))
    country_code_col = F.substring(F.col(col_name), 1, 2)
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