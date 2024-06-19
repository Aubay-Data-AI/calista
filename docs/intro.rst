Package overview
================

``Calista`` is a comprehensive Python package designed to perform data quality checks on various platforms while using the same syntax. Drawing inspiration from the modular approach of established libraries, Calista aims to simplify data quality tasks through a unified interface.

The core of Calista is built upon popular Python libraries like ``Pyspark`` and ``SQLAlchemy``, harnessing their power to handle large-scale data processing and manipulation. By abstracting away the intricacies of engine-specific operations, Calista empowers users to focus on their data quality tasks without getting bogged down in implementation details.

At its heart, Calista provides a cohesive set of classes and methods that consolidate the functionalities of disparate engine-specific modules. For instance, users can seamlessly execute operations typically performed by Spark or SQL engines through intuitive interfaces provided by Calista.

The current implementation has been developed in Python 3.10. For now, you can execute data quality checks using the following engines or platforms: spark, pandas, polars, snowflake, bigquery, postgre.

Whether you're orchestrating data pipelines or conducting data quality assessments, Calista equips you with the tools needed to navigate the complex terrain of data quality checks with ease and efficiency.

