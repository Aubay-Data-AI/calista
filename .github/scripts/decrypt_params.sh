#!/usr/bin/env bash

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" --output tests/table/parameters.py .github/workflows/params/parameters.py.gpg
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" --output tests/table/bigquery_private_key.json .github/workflows/params/bigquery_private_key.json.gpg
