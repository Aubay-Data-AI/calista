import calista
from calista import CalistaEngine



config = {
    "credentials": {
        "account": "ERDUDQY-ZB34790",
        "user": "<user-name>",
        "password": "<password>"",
    }
}
table = CalistaEngine(engine="snowflake", config=config) \
    .load_from_database(database=<your_database_name>, schema=<your_schema_name>, table=<your_table_name>)