from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from abc import ABC, abstractmethod
import schwifty
from email_validator import validate_email, EmailNotValidError

class validator(ABC): 

    @abstractmethod
    def validate(self, value):
        pass 
        

class EmailValidator(validator):
    '''Valide les emails'''
    
    def validate(self, email: str) -> bool:
        try:
            validate_email(email, check_deliverability=False)
            return True
        except EmailNotValidError:
            return False
        
    @staticmethod
    def udf_validate():

        return udf(lambda email: EmailValidator().validate(email), BooleanType())



class IbanValidator(validator):
    '''Valide les ibans'''


    def validate(self, iban: str) -> bool:

        return bool(schwifty.IBAN(iban, allow_invalid=True).is_valid)

    @staticmethod
    def udf_validate():

        return udf(lambda iban: IbanValidator().validate(iban), BooleanType())


