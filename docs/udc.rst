User Defined Conditions (UDC)
-------------

Description
^^^^^^^^

User-Defined Conditions (UDCs) are a feature that allow users to extend the built-in validation and analysis capabilities of Calista by adding custom functions that can be used to create more complex rules not covered by existing features.

Defining and registering a UDC
^^^^^^^^

The first step in creating a UDC is defining a Python function with the appropriate parameters and return type, depending on the engine you are working with.

Your function should take parameters (e.g: column names or any threshold values) relevant to your validation logic. The return type should align with the type expected by the engine:

- **Spark and Snowflake:** Return a `Column` object.
- **Pandas:** Return a `Series` object.
- **Polars:** Return an `Expr` object.
- **BigQuery (using SQLAlchemy):** Return a `ColumnExpressionArgument` object.

.. note::

   It is crucial that your function returns a boolean column/expression (i.e., `True` or `False`) for each row in the dataset. This ensures that the condition can be properly evaluated for accurate metric calculations.

Once your function is created, it needs to be registered using the appropriate decorator based on the engine you are working with. The decorators available in Calista are:

- ``register_spark_condition`` for Spark.
- ``register_snowflake_condition`` for Snowflake.
- ``register_polars_condition`` for Polars.
- ``register_pandas_condition`` for Pandas.
- ``register_bigquery_condition`` for BigQuery (or any SQL Engine).

Each ``register_<ENGINE>_condition`` decorator takes a parameter ``name``, which is an arbitrary string representing the name of your UDC. However, this name is primarily for internal reference and does not replace the need to call the actual function you created when defining your rules. You will still need to use the function name you provided when writing your custom logic.

Additionally, the name you choose cannot be the same as an existing function in Calista. If you attempt to use a conflicting name, an exception will be raised to prevent any unexpected behavior.

The decorator automatically integrates your custom condition into Calista's validation and analysis workflow, allowing it to be combined with existing functions to create more complex rules.


.. note::

    When defining a UDC that requires access to the dataset *(e.g., to access specific columns which is necessary for Pandas or BigQuery)*, you should not include the dataset parameter when you create the rule.

    Calista will automatically handle passing the dataset internally. You only need to specify the additional parameters defined after the dataset, such as column names or threshold values.

Examples
^^^^^^^^

.. tabs::

   .. tab:: Pandas

      .. code-block:: python

        import numpy as np
        import pandas as pd

        from calista import register_pandas_condition
        from calista.table import CalistaEngine

        # Register and create your UDC here
        @register_pandas_condition(name="floor_udc")
        def floor_lt_value(df: pd.DataFrame, col_name: str, value: int):
            return np.floor(df[col_name]) < value

        # Create your CalistaTable as you would normally do, even without UDC
        pandas_table = CalistaEngine("pandas").load_from_path(path="ressources/my_parquet_file.parquet", file_format="parquet")

        # Create your rule from the function you defined with your parameters
        # Important: If your UDC requires access to the dataset (e.g., to access specific columns),
        # you do NOT need to include the dataset parameter here when creating the rule
        udc_pandas = floor_lt_value(col_name="SALAIRE", value=54000)

        # Compute the metrics and print it
        metrics = pandas_table.analyze("My First UDC", udc_pandas)

        print(metrics)

      .. code-block:: python

        rule_name : My First UDC
        total_row_count : 100
        valid_row_count : 30
        valid_row_count_pct : 30.0
        timestamp : 2024-08-21 11:48:49.530516

   .. tab:: Polars

      .. code-block:: python

        import polars as pl

        from calista import register_polars_condition
        from calista.table import CalistaEngine

        # Register and create your UDC here
        @register_polars_condition(name="floor_udc")
        def floor_lt_value(col_name: str, value: int):
            return pl.col(col_name).floor() < value

        # Create your CalistaTable as you would normally do, even without UDC
        polars_table = CalistaEngine("polars").load_from_path(path="ressources/my_parquet_file.parquet", file_format="parquet")

        # Create your rule from the function you defined with your parameters
        udc_polars = floor_lt_value(col_name="SALAIRE", value=54000)

        # Compute the metrics and print it
        metrics = polars_table.analyze("My First UDC", udc_polars)

        print(metrics)

      .. code-block:: python

        rule_name : My First UDC
        total_row_count : 100
        valid_row_count : 30
        valid_row_count_pct : 30.0
        timestamp : 2024-08-21 11:48:49.530516

   .. tab:: Spark

      .. code-block:: python

        from pyspark.sql import functions as F

        from calista import register_spark_condition
        from calista.table import CalistaEngine

        # Register and create your UDC here
        @register_spark_condition(name="floor_udc")
        def floor_lt_value(col_name: str, value: int):
            return F.col(col_name) < value

        # Create your CalistaTable as you would normally do, even without UDC
        spark_table = CalistaEngine("spark").load_from_path(path="ressources/my_parquet_file.parquet", file_format="parquet")

        # Create your rule from the function you defined with your parameters
        udc_spark = floor_lt_value(col_name="SALAIRE", value=54000)

        # Compute the metrics and print it
        metrics = spark_table.analyze("My First UDC", udc_spark)

        print(metrics)

      .. code-block:: python

        rule_name : My First UDC
        total_row_count : 100
        valid_row_count : 30
        valid_row_count_pct : 30.0
        timestamp : 2024-08-21 11:48:49.530516

   .. tab:: Snowflake

      .. code-block:: python

        from snowflake.snowpark import functions as F

        from calista import register_snowflake_condition
        from calista.table import CalistaEngine

        # Register and create your UDC here
        @register_snowflake_condition(name="floor_udc")
        def floor_lt_value(col_name: str, value: int):
            return F.col(col_name) < value

        # Create your CalistaTable as you would normally do, even without UDC
        config = {
            "credentials": {
                "account": <account-identifier>,
                "user": <user-name>,
                "password": <password>,
            }
        }
        snowflake_table = CalistaEngine(engine="snowflake", config=config) \
                    .load_from_database(database=<your_database_name>,
                                        schema=<your_schema_name>,
                                        table=<your_table_name>)

        # Create your rule from the function you defined with your parameters
        udc_snowflake = floor_lt_value(col_name="SALAIRE", value=54000)

        # Compute the metrics and print it
        metrics = snowflake_table.analyze("My First UDC", udc_snowflake)

        print(metrics)

      .. code-block:: python

        rule_name : My First UDC
        total_row_count : 100
        valid_row_count : 30
        valid_row_count_pct : 30.0
        timestamp : 2024-08-21 11:48:49.530516

   .. tab:: BigQuery

      .. code-block:: python

        from sqlalchemy import func
        from sqlalchemy.sql.selectable import Select

        from calista import register_bigquery_condition
        from calista.table import CalistaEngine

        # Register and create your UDC here
        @register_bigquery_condition(name="floor_udc")
        def floor_lt_value(dataset: Select, col_name: str, value:int):
            return func.floor(dataset.c[col_name]) < value

        # Create your CalistaTable as you would normally do, even without UDC
        connection_string = f'bigquery://<my-project>/<my-dataset>'
        credentials_path='<path_to_credentials>.json'
        config = {
            'connection_string': connection_string,
            'credentials_path': credentials_path
            }
        bigquery_table = CalistaEngine(engine="bigquery", config=config).load_from_database(table=<your_table_name>)

        # Create your rule from the function you defined with your parameters
        # Important: If your UDC requires access to the dataset (e.g., to access specific columns),
        # you do NOT need to include the dataset parameter here when creating the rule
        udc_bigquery = floor_lt_value(col_name="SALAIRE", value=54000)

        # Compute the metrics and print it
        metrics = bigquery_table.analyze("My First UDC", udc_bigquery)

        print(metrics)

      .. code-block:: python

        rule_name : My First UDC
        total_row_count : 100
        valid_row_count : 30
        valid_row_count_pct : 30.0
        timestamp : 2024-08-21 11:48:49.530516
