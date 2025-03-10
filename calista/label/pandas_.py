from email_validator import validate_email, EmailNotValidError
import schwifty
import validators
import phonenumbers


def is_valid_iban(iban: str) -> bool:
    """
     
        is valid iban It will be used to check an Iban

        Args:
            the function takes a string iban as a argument

        Returns:
        bool: the function returns a boolean.

    """
    return bool(schwifty.IBAN(iban, allow_invalid=True).is_valid)


def is_valid_email(email: str) -> bool:
    """
        is valid email It will be used to check an email

        Args:
            the function takes a string email as a argument

        Returns:
        bool: the function returns a boolean.

    """
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False


def is_valid_ip_adress(adress_ip: str) -> bool:
    """
        is valid ip_adress It will be used to check an ip_adress

        Args:
            the function takes a string adress_ip as a argument

        Returns:
        bool: the function returns a boolean.

    """
    return bool(validators.ipv4(adress_ip) or validators.ipv6(adress_ip))


def is_valid_phone_number(phone_number: str) -> bool:
    """
        is valid ip_adress It will be used to check an ip_adress

        Args:
            the function takes a string phone_number as a argument

        Returns:
        bool: the function returns a boolean.

    """
    if phonenumbers.is_possible_number_string(phone_number, None):  
        parsed_number = phonenumbers.parse(phone_number, None)  
        return phonenumbers.is_valid_number(parsed_number)
    else :
        return False


