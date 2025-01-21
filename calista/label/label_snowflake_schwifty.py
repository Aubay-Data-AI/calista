from schwifty import IBAN

def validate_iban(iban_string):
    try:
        iban = IBAN(iban_string)
        return {
            "valid": True,
            "country_code": iban.country_code,
            "bank_code": iban.bank_code,
            "branch_code": iban.branch_code,
            "account_code": iban.account_code
        }
    except ValueError:
        return {"valid": False}