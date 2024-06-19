## How to run tests

### 1. Install pytest
```pip install pytest```

### 2. Add parameters.py file in tests/table
You need to write inside this file your snowflake and *bigquery credentials info as the following :
```python
SNOWFLAKE_CONN_PARAMS = {
            "credentials": {
                "account": "ACCOUNT-IDENTIFIER",
                "user": "USER-NAME",
                "password": "PASSWORD",
            }
        }

BIGQUERY_CONN_PARAMS = {
        "connection_string": "bigquery://<MY-PROJECT>/<MY-DATASET>",
        "credentials_path": "bigquery_private_key.json",
}
```
for bigquery you also need to put your credentials info into a json file
named "bigquery_private_key.json" in tests/table.

```json
{
  "type": "service_account",
  "project_id": "project-id",
  "private_key_id": "private_key_id",
  "private_key": "private_key",
  "client_email": "client_email",
  "client_id": "client_id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "client_x509_cert_url",
  "universe_domain": "googleapis.com"
}
```

### run tests
```pytest -v tests/table```
