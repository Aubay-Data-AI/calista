Getting started
===============
To use our library, simply install it via pip. This command will install the library along with the default engines pandas and polars:

.. code-block:: python

    pip install calista

If you require support for another engines such as Snowflake, Spark, or BigQuery, use the following command and replace EngineName with the name of your desired engine:

.. code-block:: python

    pip install calista[EngineName]

**Example:**
if you want to have Snowflake and Spark, use the following command:

.. code-block:: python

    pip install calista[snowflake, spark]


To study the quality of your data with the ``Calista`` library, you have several engines at your disposal.
Here's how to use them.

How to specify an engine and load data
---------------------------------------

Pandas
^^^^^^^

Pandas is a powerful Python library primarily used for data manipulation and analysis.
Its key features include data structures like DataFrame and Series, which facilitate handling structured data effectively.

Load a table with Calista:

.. code-block:: python

    from calista import CalistaEngine

    table = CalistaEngine(engine="pandas") \
    .load_from_path(<path_to_your_file>, file_format=<your_file_format>)


Polars
^^^^^^^^

Polars is a fast and efficient Rust library for data manipulation and analysis, with bindings available for Python.
It offers similar functionalities to Pandas, such as DataFrame and Series structures, but with a focus on high-performance computing.
It is optimized for large datasets, multithreading, and lazy evaluation.

Load a table with Calista:

.. code-block:: python

    from calista import CalistaEngine

    table = CalistaEngine(engine="polars") \
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

    from calista import CalistaEngine

    table = CalistaEngine(engine="spark") \
    .load_from_path(<path_to_your_file>, file_format=<your_file_format>)


For the previous engines, you can also use the following functions to load your Calista table
from an existing dataframe or a dictionary.

:func:`calista.table.CalistaEngine.load_from_dataframe`

:func:`calista.table.CalistaEngine.load_from_dict`


Snowflake
^^^^^^^^^

As this engine is developed in Snowpark, before computing a rule, a configuration must be defined to connect to the Snowflake data warehouse.
Snowflake is a cloud-based data warehousing platform designed for storing, processing, and analyzing large volumes of data. It offers a scalable and elastic architecture, allowing users to efficiently manage data across multiple clouds.

Install Calista with the snowflake engine:

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

BigQuery
^^^^^^^^

As this engine is developed in SQL, before computing a rule, a configuration must be defined to connect to the BigQuery data warehouse.
BigQuery is a fully managed, serverless data warehouse provided by Google Cloud Platform. It's designed for storing and analyzing large datasets using SQL queries, with scalable compute and storage resources.

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

How to compute metrics
----------------------

Rules
^^^^^^^^

* You can create your own rules by chaining several Calista's functions with these operators :
    ``& | ~``

.. code-block:: python

    from calista import functions as func

    my_rule = func.is_iban(col_name="IBAN") & func.is_float("SALAIRE") | ~func.is_iban(col_name="ADRESSE_IP_V4")
    print(table.analyze(rule_name=<your_rule_name>, rule=my_rule))

.. code-block:: python

    rule_name : your_rule_name
    total_row_count : 100
    valid_row_count : 100
    valid_row_count_pct : 100.0
    timestamp : 2024-05-06 16:19:13.221048

* You can also compute several rules at the same time

.. code-block:: python

    from calista import functions as func

    rules = {
    "check_iban_quality": func.is_iban("IBAN"),
    "check_CDI_ID_are_integer": func.is_integer("CDI") & func.is_integer("ID"),
    "check_email_quality": func.is_email("EMAIL"),
    }
    print(table.analyze_rules(rules))

.. code-block:: python

    [
    Metrics(
           rule='check_iban_quality',
           total_row_count=100,
           valid_row_count=90,
           valid_row_count_pct=90.0,
           timestamp='2024-05-07 11:37:34.038035'
      ),
      Metrics(
          rule='check_CDI_ID_are_integer',
          total_row_count=100,
          valid_row_count=98,
          valid_row_count_pct=98.0,
          timestamp='2024-05-07 11:37:34.038035'),
      Metrics(
          rule='check_email_quality',
          total_row_count=100,
          valid_row_count=92,
          valid_row_count_pct=92.0,
          timestamp='2024-05-07 11:37:34.038035')
    ]

How to get enhanced data
------------------------

* You have the possibility to get your enhanced data by applying a rule

.. code-block:: python

    from calista import functions as func

    print(table.apply_rule(rule_name="check_iban_quality", rule=func.is_iban("IBAN"))[['IBAN', 'check_iban_quality']])

.. code-block:: python

                               IBAN  check_iban_quality
    0   FR4756356801990924110246661                True
    1   FR9152927592715361970259533                True
    2   FR6098743347361131022029548                True
    3   FR2371478023732554095214206                True
    4   FR0330875910858658779613722                True
    ..                          ...                 ...
    95  FR1773393443400319003480793                True
    96  FR0228768854412051157590266                True
    97  FR5869598054756805717971833                True
    98  FR6634213649058126775820977                True
    99                         None               False

* You can also do the same with a list of rules

.. code-block:: python

    from calista import functions as func

    rules = {
    "check_iban_quality": func.is_iban("IBAN"),
    "check_email_quality": func.is_email("EMAIL"),
    }
    print(table.apply_rules(rules)[['IBAN', 'check_iban_quality', 'EMAIL', 'check_email_quality']])

.. code-block:: python

                              IBAN  check_iban_quality                          EMAIL  check_email_quality
    0   FR4756356801990924110246661                True  aristidesgordillo@example.net                 True
    1   FR9152927592715361970259533                True     oceane.leclercq@orange.com                 True
    2   FR6098743347361131022029548                True        elodie.morel@icloud.com                 True
    3   FR2371478023732554095214206                True          therese04@example.com                 True
    4   FR0330875910858658779613722                True      bertrand.dijoux@yahoo.com                 True
    ..                          ...                 ...                            ...                  ...
    95  FR1773393443400319003480793                True         eugene.munoz@yahoo.com                 True
    96  FR0228768854412051157590266                True                           None                False
    97  FR5869598054756805717971833                True            aaron50@example.net                 True
    98  FR6634213649058126775820977                True         lucie.allard@gmail.com                 True
    99                         None               False     alexandria.petit@yahoo.com                 True

* If you want to retrieve the data not validating your rule for some analysis, it is possible.

.. code-block:: python

    from calista import functions as func

    my_rule = func.is_iban("IBAN")
    print(table.get_invalid_rows(rule=my_rule))

.. code-block:: python

              NOM      PRENOM SEXE DATE_ENTREE  CDI  IBAN  ...    CDD                          EMAIL             TELEPHONE   SALAIRE DEVISE   ID
    10  Chevalier    Adélaïde    M        None  1.0  None  ...  False  adelaide.chevalier@icloud.com  +33 (0)3 52 49 21 25  39630.16    GPB   11
    28      Petit  Antoinette    F  2016-04-04  1.0  None  ...  False     antoinette.petit@gmail.com  +33 (0)3 63 22 80 94  48302.80    EUR   29
    31      Gomez     Antoine    M  2015-04-04  0.0  None  ...   True     miguel-angel83@example.com  +33 (0)6 30 22 34 32  53213.86    EUR   32
    47     Lebrun      Xavier    M  2018-01-20  1.0  None  ...  False       xavier.lebrun@orange.com                  None  51289.21    EUR   48
    54    Ferrand     Chantal    M  2002-01-26  NaN  None  ...   True     chantal.ferrand@orange.com        06 82 99 40 77  89947.60    EUR   55
    59  Lemonnier    Éléonore    M  2011-12-22  1.0  None  ...  False  eleonore.lemonnier@orange.com            0329984138  58303.00    EUR   60
    62      Dupré    Frédéric    F  2022-07-21  0.0  None  ...   True       frederic.dupre@gmail.com            0385249100  53914.36    EUR   63
    64    Étienne    Nathalie    F  2008-07-17  0.0  None  ...   True     nathalie.etienne@gmail.com  +33 (0)3 51 82 62 52  48394.97    EUR   65
    78    Roussel         Luc    F  2013-11-27  0.0  None  ...   None          luc.roussel@gmail.com                  None  47089.29    EUR   79
    99      Petit  Alexandria    F  2003-11-18  0.0  None  ...   True     alexandria.petit@yahoo.com                  None  82053.90    EUR  100
