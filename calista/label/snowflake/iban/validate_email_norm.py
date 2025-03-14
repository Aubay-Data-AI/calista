import re
import unicodedata
from typing import Optional, Union, Tuple
import snowflake.snowpark.functions as F
from snowflake.snowpark import Column
import calista.core._conditions as cond

class EmailSyntaxError(ValueError):
    pass

class ValidatedEmail:
    def __init__(self, original, normalized, local_part, domain, ascii_email, display_name):
        self.original = original
        self.normalized = normalized
        self.local_part = local_part
        self.domain = domain
        self.ascii_email = ascii_email
        self.display_name = display_name

EMAIL_REGEX = re.compile(
    r"^(?P<local>[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+)@(?P<domain>[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$"
)

def split_email(email: str) -> Tuple[Optional[str], str, str, bool]:
    def split_string_at_unquoted_special(text: str, specials: Tuple[str, ...]) -> Tuple[str, str]:
        inside_quote = False
        escaped = False
        left_part = ""

        for i, c in enumerate(text):
            if inside_quote:
                left_part += c
                if c == '\\' and not escaped:
                    escaped = True
                elif c == '"' and not escaped:
                    inside_quote = False
                    escaped = False
                else:
                    escaped = False
            elif c == '"':
                left_part += c
                inside_quote = True
            elif c in specials:
                break
            else:
                left_part += c

        if len(left_part) == len(text):
            raise EmailSyntaxError("Invalid email syntax: missing '@' or malformed format.")

        return left_part, text[len(left_part):]

    def unquote_quoted_string(text: str) -> Tuple[str, bool]:
        quoted = False
        escaped = False
        value = ""

        for i, c in enumerate(text):
            if quoted:
                if escaped:
                    value += c
                    escaped = False
                elif c == '\\':
                    escaped = True
                elif c == '"':
                    if i != len(text) - 1:
                        raise EmailSyntaxError("Invalid closing quote position in email.")
                    break
                else:
                    value += c
            elif i == 0 and c == '"':
                quoted = True
            else:
                value += c

        return value, quoted

    left_part, right_part = split_string_at_unquoted_special(email, ("@", "<"))

    if right_part.startswith("<"):
        display_name = left_part.strip()
        if not right_part.endswith(">"):
            raise EmailSyntaxError("Invalid email format: unmatched angle brackets.")
        addr_spec = right_part[1:].rstrip(">")
        local_part, domain_part = split_string_at_unquoted_special(addr_spec, ("@",))
    else:
        display_name = None
        local_part, domain_part = left_part, right_part

    if domain_part.startswith("@"): 
        domain_part = domain_part[1:]

    local_part, is_quoted_local_part = unquote_quoted_string(local_part)
    
    return display_name, local_part, domain_part, is_quoted_local_part

def is_email(self, condition: cond.IsEmail) -> Column:
    return F.regexp_replace(
        F.col(condition.col_name),
        r"\s+", ""
    ).rlike(EMAIL_REGEX)
