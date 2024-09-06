Getting started
===============
To use our library, simply install it via pip. This command will install the library along with the default engines pandas and polars:

.. code-block:: python

    pip install calista

If you require support for another engines such as Snowflake, Spark, or BigQuery, use the following command and replace EngineName with the name of your desired engine:

.. code-block:: python

    pip install calista[EngineName]

**Example:**
if you want to use Calista with Spark, Snowflake and BigQuery, use the following command:

.. code-block:: python

    pip install calista[spark,snowflake,bigquery]


To study the quality of your data with the ``Calista`` library, you have several engines at your disposal.
Here's how to use them.

How to specify an engine and load data
---------------------------------------

.. tabs::

   .. tab:: Pandas

    **Pandas** is a powerful Python library primarily used for data manipulation and analysis.

    Its key features include data structures like DataFrame and Series, which facilitate handling structured data effectively.

    Load a table with Calista:

    .. tabs::

        .. tab:: Load from a path

            .. code-block:: python

                from calista import CalistaEngine

                table = CalistaEngine(engine="pandas") \
                .load_from_path(<path_to_your_file>, file_format=<your_file_format>)


            *Supported file formats in Pandas are: parquet, csv, json.*

        .. tab:: Load from a DataFrame

            .. code-block:: python

                from calista import CalistaEngine

                # Assuming you already have a Pandas DataFrame 'df' defined.
                table = CalistaEngine(engine="pandas").load_from_dataframe(df)

        .. tab:: Load from a dictionary

            .. code-block:: python

                from calista import CalistaEngine

                data = {"ID": [1, 2, 3], "COLOR": ["RED", "GREEN", "BLUE"]}

                table = CalistaEngine(engine="pandas").load_from_dict(data)

   .. tab:: Polars

    **Polars** is a fast and efficient Rust library for data manipulation and analysis, with bindings available for Python.

    It offers similar functionalities to Pandas, such as DataFrame and Series structures, but with a focus on high-performance computing.
    It is optimized for **large datasets**, **multithreading**, and **lazy evaluation**.

    Load a table with Calista:

    .. tabs::

        .. tab:: Load from a path

            .. code-block:: python

                from calista import CalistaEngine

                table = CalistaEngine(engine="polars").load_from_path(<path_to_your_file>, file_format=<your_file_format>)


            *Supported file formats in Polars are: parquet, csv, json.*

        .. tab:: Load from a DataFrame

            .. code-block:: python

                from calista import CalistaEngine

                # Assuming you already have a Polars DataFrame 'df' defined.
                table = CalistaEngine(engine="polars").load_from_dataframe(df)

        .. tab:: Load from a dictionary

            .. code-block:: python

                from calista import CalistaEngine

                data = {"ID": [1, 2, 3], "COLOR": ["RED", "GREEN", "BLUE"]}

                table = CalistaEngine(engine="polars").load_from_dict(data)

   .. tab:: Spark

    **Spark** is a distributed computing framework designed for processing big data tasks.

    The key advantages include its speed, fault tolerance, and support for various data sources and processing engines.

    It excels in **iterative processing** and **real-time analytics**, making it suitable for a wide range of big data applications.

    Install Calista with the Spark engine:

    .. code-block:: bash

        pip install calista[spark]

    Load a table with Calista:

    .. tabs::

        .. tab:: Load from a path

            .. code-block:: python

                from calista import CalistaEngine

                table = CalistaEngine(engine="spark") \
                .load_from_path(<path_to_your_file>, file_format=<your_file_format>)


            *Supported file formats in Spark are: parquet, csv, json.*

        .. tab:: Load from a DataFrame

            .. code-block:: python

                from calista import CalistaEngine

                # Assuming you already have a Spark DataFrame 'df' defined.
                table = CalistaEngine(engine="spark").load_from_dataframe(df)

        .. tab:: Load from a dictionary

            .. code-block:: python

                from calista import CalistaEngine

                data = {"ID": [1, 2, 3], "COLOR": ["RED", "GREEN", "BLUE"]}

                table = CalistaEngine(engine="spark").load_from_dict(data)

   .. tab:: Snowflake

    As this engine is developed in **Snowpark**, before computing a rule, a configuration must be defined to connect to the Snowflake data warehouse.

    **Snowflake** is a cloud-based data warehousing platform designed for storing, processing, and analyzing large volumes of data. It offers a **scalable and elastic architecture**, allowing users to efficiently manage data across multiple clouds.

    Install Calista with the Snowflake engine:

    .. code-block:: bash

        pip install calista[snowflake]


    Load a table with Calista:

    .. code-block:: python

        from calista import CalistaEngine

        config = {
            "credentials": {
                "account": <account-identifier>,
                "user": <user-name>,
                "password": <password>,
            }
        }
        table = CalistaEngine(engine="snowflake", config=config) \
            .load_from_database(database=<your_database_name>, schema=<your_schema_name>, table=<your_table_name>)

    *With Snowflake engine, you can only load data from a database.*

   .. tab:: BigQuery

    As this engine is developed in **SQL**, before computing a rule, a configuration must be defined to connect to the BigQuery data warehouse.

    **BigQuery** is a fully managed, serverless data warehouse provided by **Google Cloud Platform**. It's designed for **storing and analyzing large datasets using SQL queries**, with scalable compute and storage resources.

    Install Calista with the BigQuery engine:

    .. code-block:: bash

        pip install calista[bigquery]


    Load a table with Calista:

    .. code-block:: python

        from calista import CalistaEngine

        connection_string = f'bigquery://<my-project>/<my-dataset>'
        credentials_path='<path_to_credentials>.json'
        config = {
            'connection_string': connection_string,
            'credentials_path': credentials_path
            }
        table = CalistaEngine(engine="bigquery", config=config).load_from_database(table=<your_table_name>)

    *With BigQuery engine, you can only load data from a database.*

How to compute metrics
----------------------

For the following examples, we will work with this table:

.. code-block:: python

    from calista import CalistaEngine

    table = CalistaEngine(engine="pandas").load_from_dict({"USERNAME":["Player 1", "Player 2", "Player 3", "Player 4", "Player 5",
                                                                       "Player 6", "Player 7", "Player 8", "Player 9", "Player 10"],
                                                            "TEAM": ["red", "red", "red", "red", "red",
                                                                     "blue", "blue", "blue", "blue", "blue"],
                                                            "POINTS": [10, 20, 30, 40, 50, 5, 10, 15, 25, 100],
                                                            "CITY": ["Paris", "Paris", "Marseille", "Lyon", "Nice",
                                                                     "Toulouse", "Paris", "Nantes", "Montpellier", "Strasbourg"],})

    table.show()

.. code-block:: python

        USERNAME  TEAM  POINTS         CITY
    0   Player 1   red      10        Paris
    1   Player 2   red      20        Paris
    2   Player 3   red      30    Marseille
    3   Player 4   red      40         Lyon
    4   Player 5   red      50         Nice
    5   Player 6  blue       5     Toulouse
    6   Player 7  blue      10        Paris
    7   Player 8  blue      15       Nantes
    8   Player 9  blue      25  Montpellier
    9  Player 10  blue     100   Strasbourg


You can create your own rules by **chaining several Calista's functions** with these operators:

- ``&`` : AND operator to use between two conditions
- ``|`` : OR operator to use between two conditions
- ``~`` : NOT operator to use in front of a condition or multiple conditions

.. code-block:: python

    from calista import functions as func

    my_rule = func.column_ge_value(col_name="POINTS", value=50) | func.column_equal_to_value(col_name="CITY", value="Paris")

    metrics = table.analyze(rule_name="Points >= 50 or player from Paris", rule=my_rule)
    print(metrics)

.. code-block:: python

    rule_name : Points >= 50
    total_row_count : 10
    valid_row_count : 5
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.573894

You can also **compute several rules** at the same time

.. code-block:: python

    from calista import functions as func

    rules = {
        "Points >= 50": func.column_ge_value(col_name="POINTS", value=50),
        "Player from Paris": func.column_equal_to_value(col_name="CITY", value="Paris"),
        "Username known": func.is_not_null(col_name="USERNAME")
    }

    metrics = table.analyze_rules(rules)
    print(metrics)

.. code-block:: python

    [
        Metrics(
            rule='Points >= 50',
            total_row_count=10, valid_row_count=2,
            valid_row_count_pct=20.0,
            timestamp='2024-09-06 11:20:56.647514'),
        Metrics(
            rule='Player from Paris',
            total_row_count=10,
            valid_row_count=3,
            valid_row_count_pct=30.0,
            timestamp='2024-09-06 11:20:56.647514'),
        Metrics(
            rule='Username known',
            total_row_count=10,
            valid_row_count=10,
            valid_row_count_pct=100.0,
            timestamp='2024-09-06 11:20:56.647514')
    ]

How to get enhanced data
------------------------

You have the possibility to get your enhanced data by **applying a rule**:

.. code-block:: python

    from calista import functions as func

    my_rule = func.column_ge_value(col_name="POINTS", value=50) | func.column_equal_to_value(col_name="CITY", value="Paris")

    result = table.apply_rule(rule_name="Points >= 50 or player from Paris", rule=my_rule)
    print(result)

.. code-block:: python

        USERNAME  TEAM  POINTS         CITY  Points >= 50 or player from Paris
    0   Player 1   red      10        Paris                               True
    1   Player 2   red      20        Paris                               True
    2   Player 3   red      30    Marseille                              False
    3   Player 4   red      40         Lyon                              False
    4   Player 5   red      50         Nice                               True
    5   Player 6  blue       5     Toulouse                              False
    6   Player 7  blue      10        Paris                               True
    7   Player 8  blue      15       Nantes                              False
    8   Player 9  blue      25  Montpellier                              False
    9  Player 10  blue     100   Strasbourg                               True

You can also do the same with a **list of rules**:

.. code-block:: python

    from calista import functions as func

    rules = {
        "Player from Paris": func.column_equal_to_value(col_name="CITY", value="Paris"),
        "Username known": func.is_not_null(col_name="USERNAME")
    }

    result = table.apply_rules(rules)
    print(result)

.. code-block:: python

        USERNAME  TEAM  POINTS         CITY  Player from Paris  Username known
    0   Player 1   red      10        Paris               True            True
    1   Player 2   red      20        Paris               True            True
    2   Player 3   red      30    Marseille              False            True
    3   Player 4   red      40         Lyon              False            True
    4   Player 5   red      50         Nice              False            True
    5   Player 6  blue       5     Toulouse              False            True
    6   Player 7  blue      10        Paris               True            True
    7   Player 8  blue      15       Nantes              False            True
    8   Player 9  blue      25  Montpellier              False            True
    9  Player 10  blue     100   Strasbourg              False            True

If you want to **retrieve the data not validating your rule** for some further analysis, it is possible:

.. code-block:: python

    from calista import functions as func

    my_rule = func.column_ge_value(col_name="POINTS", value=50) | func.column_equal_to_value(col_name="CITY", value="Paris")

    result = table.get_invalid_rows(my_rule)
    print(result)

.. code-block:: python

       USERNAME  TEAM  POINTS         CITY
    2  Player 3   red      30    Marseille
    3  Player 4   red      40         Lyon
    5  Player 6  blue       5     Toulouse
    7  Player 8  blue      15       Nantes
    8  Player 9  blue      25  Montpellier

*See also:* :func:`calista.table.CalistaTable.get_valid_rows`

Data filtering and aggregation
------------------------------

Sometimes you need to check a rule on a subset of a dataset and not the entire dataset.

With Calista, before checking a rule, you have the possibility to **filter data on which you want to apply it**. To do so, you can use following CalistaTable methods:

- :func:`calista.table.CalistaTable.where`
- :func:`calista.table.CalistaTable.filter`

.. code-block:: python

    from calista import functions as func

    my_filter = func.column_equal_to_value(col_name="TEAM", value="red")
    my_rule = func.column_ge_value(col_name="POINTS", value=50)

    metrics = table.where(my_filter).analyze(rule_name="Points >= 50 for red team", rule=my_rule)
    print(metrics)

.. code-block:: python

    rule_name : Points >= 50 for red team
    total_row_count : 5
    valid_row_count : 1
    valid_row_count_pct : 20.0
    timestamp : 2024-01-01 00:00:00.573894

When you need to **aggregate the data before checking a rule**, you can also do it:

.. code-block:: python

    from calista import functions as func

    my_rule = func.sum_ge_value(col_name="POINTS", value=155)

    metrics = table.group_by("TEAM").analyze(rule_name="Total points >= 155",rule=my_rule)
    print(metrics)

.. code-block:: python

    rule_name : Total points >= 155
    total_row_count : 2
    valid_row_count : 1
    valid_row_count_pct : 50.0
    timestamp : 2024-01-01 00:00:00.573894

After checking a rule on an aggregated data, you may need to **get the granular data** for some further analysis:

.. tabs::

    .. tab:: With granular

        .. code-block:: python

            from calista import functions as func

            my_rule = func.sum_ge_value(col_name="POINTS", value=155)

            # Use the granular parameter
            result = table.group_by("TEAM").get_invalid_rows(my_rule, granular=True)
            print(result)

        .. code-block:: python

               USERNAME TEAM  POINTS       CITY  SUM_POINTS
            0  Player 1  red      10      Paris         150
            1  Player 2  red      20      Paris         150
            2  Player 3  red      30  Marseille         150
            3  Player 4  red      40       Lyon         150
            4  Player 5  red      50       Nice         150

    .. tab:: Without granular

        .. code-block:: python

            from calista import functions as func

            my_rule = func.sum_ge_value(col_name="POINTS", value=155)

            # By default, granular is set as False
            result = table.group_by("TEAM").get_invalid_rows(my_rule)
            print(result)

        .. code-block:: python

                  SUM_POINTS
            TEAM
            red          150
