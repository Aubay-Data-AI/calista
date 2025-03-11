import re
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import StringType, BooleanType
from snowflake.snowpark import Session


# Regex stricte pour la validation email (RFC 5322 compatible)
EMAIL_REGEX = re.compile(
    r"^(?P<local>[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+)@(?P<domain>[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$"
)

DISPLAY_NAME_REGEX = re.compile(r"^(?P<display>.+?)\s*<(?P<email>.+?)>$")


class EmailSyntaxError(ValueError):
    pass


def split_email(email: str):
    
    email = email.strip()

    match = DISPLAY_NAME_REGEX.match(email)
    if match:
        display_name = match.group("display").strip()
        email = match.group("email").strip()
    else:
        display_name = None

    if "@" not in email:
        raise EmailSyntaxError("Invalid email format: missing '@'.")

    local_part, domain_part = email.rsplit("@", 1)

    # Vérifier si le local_part est entre guillemets
    is_quoted_local_part = local_part.startswith('"') and local_part.endswith('"')

    return display_name, local_part, domain_part, is_quoted_local_part


def validate_email_snowflake(email: str) -> bool:
    """
    Fonction UDF 
    """

    if not email:
        return False

    email = email.strip()

    try:
        # Séparer display name, local_part et domain_part
        display_name, local_part, domain_part, is_quoted_local_part = split_email(email)
    except EmailSyntaxError:
        return False

   
    if not EMAIL_REGEX.match(f"{local_part}@{domain_part}"):
        return False

   
    if not local_part or len(local_part) > 64:
        return False

   
    if is_quoted_local_part and '"' in local_part[1:-1]:
        return False

   
    if not domain_part or len(domain_part) > 255:
        return False

    
    if domain_part.startswith("[") and domain_part.endswith("]"):
        ip_address = domain_part[1:-1]
        if not re.fullmatch(r"(\d{1,3}\.){3}\d{1,3}", ip_address):
            return False

    # Vérification de la longueur totale
    if len(email) > 320:
        return False

    return True


def init_udf(session: Session):
    try:
        # ✅ Explicitly set the schema and database
        result = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
        current_db = result[0][0]
        current_schema = result[0][1]

        print(f"✅ Registering UDF in: {current_db}.{current_schema}")

        
        session.sql(f"USE SCHEMA {current_schema}").collect()

        # ✅ Register the UDF
        session.udf.register(
            validate_email_snowflake,
            name="validate_email",
            input_types=[StringType()],
            return_type=BooleanType(),
            replace=True,
            is_permanent=True,  # ✅ Ensures UDF is stored permanently
            stage_location="@MY_STAGE"  # ✅ Required for permanent UDFs
        )

        print("✅ UDF 'validate_email' enregistrée avec succès !")

        # ✅ Verify that the UDF is correctly saved
        result = session.sql("SHOW USER FUNCTIONS").collect()
        udf_names = [row['name'].upper() for row in result]

        if "VALIDATE_EMAIL" in udf_names:
            print("✅ L'UDF 'validate_email' est bien enregistrée dans Snowflake !")
        else:
            print("❌ L'UDF 'validate_email' n'est pas enregistrée.")

    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement de l'UDF : {e}")



def check_emails(col_name: str):
    """
    Valide les emails dans une colonne avec l'UDF validate_email.
    
    Args:
        col_name (str): Nom de la colonne contenant les emails.
    
    Returns:
        Column: Colonne contenant True/False pour chaque email.
    """
    return F.call_udf("validate_email", F.col(col_name))
