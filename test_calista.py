import calista
from calista import CalistaEngine

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

    table.show()

if __name__ == "__main__":
    main()
