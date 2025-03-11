# Copyright 2024 Aubay.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import json
from calista.label.resources.tools import get_file_path

def iban_isvalid(iban: str) -> bool:
    iban = re.sub(r'[^a-zA-Z0-9]', '', iban)
    iban = iban.upper()
    json_path = get_file_path("list_iban.json")
    with open(json_path, "r") as file:
        iban_specifications = json.load(file)
    return validate_iban(iban, iban_specifications)

def convert_bban_spec_to_regex(spec: str) -> str:
    _spec_to_re = {"n": r"\d", "a": r"[A-Z]", "c": r"[A-Za-z0-9]", "e": r" "}
    spec_re = rf"(\d+)(!)?([{''.join(_spec_to_re.keys())}])"
    def convert(match: re.Match) -> str:
        quantifier = ("{{{}}}" if match.group(2) else "{{1,{}}}").format(match.group(1))
        return _spec_to_re[match.group(3)] + quantifier
    return rf"^{re.sub(spec_re, convert, spec)}$"

def validate_iban(iban: str, iban_specifications: dict) -> bool:
    country_code = iban[:2]
    if country_code not in iban_specifications:
        raise ValueError(f"Unknown country code: {country_code}")
    spec = iban_specifications[country_code]
    if len(iban) != spec["iban_length"]:
        return False
    bban_spec_regex = convert_bban_spec_to_regex(spec["bban_spec"])
    bban = iban[4:]
    if not re.match(bban_spec_regex, bban):
        return False
    if not validate_iban_checksum(iban):
        return False
    return True

def validate_iban_checksum(iban: str) -> bool:
    rearranged_iban = iban[4:] + iban[:4]
    expanded_iban = ''
    for char in rearranged_iban:
        if char.isdigit():
            expanded_iban += char
        else:
            expanded_iban += str(ord(char) - 55)
    return int(expanded_iban) % 97 == 1