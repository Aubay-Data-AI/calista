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
    if not iban:
        return False
    return bool(schwifty.IBAN(iban, allow_invalid=True).is_valid)


def is_valid_email(email: str) -> bool:
    """
        is valid email It will be used to check an email

        Args:
            the function takes a string email as a argument

        Returns:
        bool: the function returns a boolean.

    """
    if not email or not email.strip():
        return False
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False


def is_valid_ip_address(address_ip: str) -> bool:
    """
        is valid ip_adress It will be used to check an ip_adress

        Args:
            the function takes a string adress_ip as a argument

        Returns:
        bool: the function returns a boolean.

    """
    if not address_ip : 
        return False
    return bool(validators.ipv4(address_ip) or validators.ipv6(address_ip))


def is_valid_phone_number(phone_number: str) -> bool:
    """
        is valid phone_number It will be used to check an phone_number

        Args:
            the function takes a string phone_number as a argument

        Returns:
        bool: the function returns a boolean.

    """
    if not phone_number :
        return False
    try:
        parsed_number = phonenumbers.parse(phone_number, None)
        return phonenumbers.is_valid_number(parsed_number)
    except phonenumbers.NumberParseException:
        return False