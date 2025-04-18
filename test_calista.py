import calista
from calista import CalistaEngine
from calista import functions as func

def main():
    config = {
        "credentials": {
            "account": "ERDUDQY-ZB34790",
            "user": "OTHMANEWAFI",
            "password": "HamidHamid4p6laa-",
        }
    }

    table = CalistaEngine(engine="snowflake", config=config) \
        .load_from_database(database="RESSOURCES", schema="TESTS_UNITAIRES", table="TEST_DATASET_100")

   


    my_rule = func.is_phone_number(col_name="TELEPHONE")

    metrics = table.analyze(rule_name="phone rule", rule=my_rule)
    print(metrics)


    my_rule = func.is_email(col_name="EMAIL")

    metrics = table.analyze(rule_name="MailRule", rule=my_rule)
    print(metrics)


    my_rule = func.is_ip_address(col_name="ADRESSE_IP_V4")

    metrics = table.analyze(rule_name="adresse_ip_v4", rule=my_rule)
    print(metrics)

if __name__ == "__main__":
    main()
