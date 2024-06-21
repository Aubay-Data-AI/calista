Getting started
===============
To use our framework, simply install it via pip. This command will install the framework along with the default engines pandas and polars:

.. code-block:: python

    pip install calista

If you require support for another engines such as Snowflake, Spark, or BigQuery, use the following command and replace EngineName with the name of your desired engine:

.. code-block:: python

    pip install calista[EngineName]

**Example:**
if you want to have Snowflake and Spark, use the following command:

.. code-block:: python

    pip install calista[snowflake, Spark]


To study the quality of your data with the framework ``Calista``, you have several engines at your disposal.
Here's how to use them.

How to load an engine
---------------------

Pandas
^^^^^^^^

Pandas is a powerful Python library primarily used for data manipulation and analysis.
Its key features include data structures like DataFrame and Series, which facilitate handling structured data effectively.

Load a table with Calista:

.. code-block:: python

    from calista import CalistaTable

    table = CalistaTable(engine="pandas") \
    .load_from_path(<path_to_your_file>, file_format=<your_file_format>)


Polars
^^^^^^^^

Polars is a fast and efficient Rust library for data manipulation and analysis, with bindings available for Python.
It offers similar functionalities to Pandas, such as DataFrame and Series structures, but with a focus on high-performance computing.
It is optimized for large datasets, multithreading, and lazy evaluation.

Load a table with Calista:

.. code-block:: python

    from calista import CalistaTable

    table = CalistaTable(engine="polars") \
    .load_from_path(<path_to_your_file>, file_format=<your_file_format>)


Spark
^^^^^^^^

Spark is a distributed computing framework designed for processing big data tasks.
The key advantages include its speed, fault tolerance, and support for various data sources and processing engines.
It excels in iterative processing and real-time analytics, making it suitable for a wide range of big data applications.

Install Calista with the spark engine:

.. code-block:: bash

    pip install calista[spark]


Load a table with Calista:

.. code-block:: python

    from calista import CalistaTable

    table = CalistaTable(engine="spark") \
    .load_from_path(<path_to_your_file>, file_format=<your_file_format>)


For the previous engines, you can also use the following functions to load your Calista table
from an existing dataframe or a dictionary.

:func:`calista.table.CalistaTable.load_from_dataframe`

:func:`calista.table.CalistaTable.load_from_dict`


Snowflake
^^^^^^^^^

As this engine is developed in Snowpark, before computing a diagnostic, a configuration must be defined to connect to the Snowflake data warehouse.
Snowflake is a cloud-based data warehousing platform designed for storing, processing, and analyzing large volumes of data. It offers a scalable and elastic architecture, allowing users to efficiently manage data across multiple clouds.

Install Calista with the snowflake engine:

.. code-block:: bash

    pip install calista[snowflake]


Load a table with Calista:

.. code-block:: python

    from calista import CalistaTable

    config = {
         "credentials": {
             "account": <account-identifier>,
             "user": <user-name>,
             "password": <password>,
         }
     }
     table = CalistaTable(engine="snowflake", config=config) \
         .load_from_database(database=<your_database_name>, schema=<your_schema_name>, table=<your_table_name>)

Bigquery
^^^^^^^^

As this engine is developed in SQL, before computing a diagnostic, a configuration must be defined to connect to the BigQuery data warehouse.
BigQuery is a fully managed, serverless data warehouse provided by Google Cloud Platform. It's designed for storing and analyzing large datasets using SQL queries, with scalable compute and storage resources.

Install Calista with the BigQuery engine:

.. code-block:: bash

    pip install calista[bigquery]


Load a table with Calista:

.. code-block:: python

    from calista import CalistaTable

    connection_string = f'bigquery://<my-project>/<my-dataset>'
    credentials_path='<path_to_credentials>.json'
    config = {
        'connection_string': connection_string,
        'credentials_path': credentials_path
        }
    table = CalistaTable(engine="bigquery", config=config).load_from_database(table=<your_table_name>)

How to compute metrics
----------------------

Rules
^^^^^^^^

* You can create your own rules by chaining several Calista's functions with these operators :
    ``& | ~``

.. code-block:: python

    my_rule = F.is_iban(col_name="IBAN") & F.is_float("SALAIRE") | ~F.is_iban(col_name="ADRESSE_IP_V4")
    print(table.analyze(rule_name=<your_rule_name>, condition=my_rule))

| rule_name : your_rule_name
| total_row_count : 100
| valid_row_count : 100
| valid_row_count_pct : 100.0
| timestamp : 2024-05-06 16:19:13.221048

* You can also compute several rules at the same time

.. code-block:: python

    rules = {
    "check_iban_quality": F.is_iban("IBAN"),
    "check_CDI_ID_are_integer": F.is_integer("CDI") & F.is_integer("ID"),
    "check_email_quality": F.is_email("EMAIL"),
    }
    print(table.analyze_rules(rules))


| [
| Metrics(
|        rule='check_iban_quality',
|        total_row_count=100,
|        valid_row_count=90,
|        valid_row_count_pct=90.0,
|        timestamp='2024-05-07 11:37:34.038035'
|   ),
|   Metrics(
|       rule='check_CDI_ID_are_integer',
|       total_row_count=100,
|       valid_row_count=98,
|       valid_row_count_pct=98.0,
|       timestamp='2024-05-07 11:37:34.038035'),
|   Metrics(
|       rule='check_email_quality',
|       total_row_count=100,
|       valid_row_count=92,
|       valid_row_count_pct=92.0,
|       timestamp='2024-05-07 11:37:34.038035')
| ]
