![Python](https://img.shields.io/badge/python-3.10-blue.svg)
[![Tests](https://github.com/Aubay-Data-AI/calista/actions/workflows/tests.yml/badge.svg)](https://github.com/Aubay-Data-AI/calista/actions)
![License](https://img.shields.io/badge/License-Apache-blue.svg)
![Black](https://img.shields.io/badge/code_style-Black-black.svg)
[![Aubay](https://img.shields.io/badge/aubay-8A2BE2)](https://data.aubay.com/)

<div style="text-align:center;">
    <img src="ressources/calista_logo.png" alt="Logo calista" />
</div>

Table of contents
- [Calista](#calista)
  - [Installing from PyPI](#installing-from-pypi)
  - [Getting Started](#getting-started)
    - [Example](#example)
  - [License](#license)


# Calista
__Calista__ is a comprehensive Python package designed to simplify data quality checks across multiple platforms using a consistent syntax. Inspired by modular libraries, __Calista__ aims to streamline data quality tasks by providing a unified interface.

Built on popular Python libraries like Pyspark and SQLAlchemy, __Calista__ leverages their capabilities for efficient large-scale data processing. By abstracting engine-specific complexities, __Calista__ allows users to focus on data quality without dealing with implementation details.

At its core, __Calista__ offers a cohesive set of classes and methods that consolidate functionalities from various engine-specific modules. Users can seamlessly execute operations typically associated with Spark or SQL engines through intuitive __Calista__ interfaces.

Currently developed in Python 3.10, __Calista__ supports data quality checks using engines such as Spark, Pandas, Polars, Snowflake and BigQuery.

Whether orchestrating data pipelines or conducting assessments, __Calista__ provides the tools needed to navigate complex data quality checks with ease and efficiency.

## Installing from PyPI

To use our framework, simply install it via pip. This command will install the framework along with the default engines pandas and polars:

```bash
pip install calista
```
If you require support for another engines such as Snowflake, Spark, or BigQuery, use the following command and replace _EngineName_ with the name of your desired engine:

```bash
pip install calista[EngineName]
```
## Getting Started

To start using __Calista__, import the appropriate class:

```
from calista import CalistaTable
```

With __Calista__, you can easily analyze and diagnose your data quality, regardless of the underlying engine. The unified API streamlines your workflow and enables seamless integration across different environments.


### Example

Here's an example using the Pandas Engine. Suppose youhave a dataset represented as a table:

| ID        | status    |last increase | salary  |
|-----------|-----------|-----------|-----------|
| 0         |Célibataire|2022-12-31 | 36000     |
| 1         |           |2023-12-31 | 53000     |
| 2         | Marié     |2018-12-31 | 28000     |

You can load this table using CalistaTable with the Pandas engine and perform a diagnostic:
```
table_pandas = CalistaTable(engine="pandas").load(path="examples/demo_new_model.csv", file_format="parquet")

diagnostic_pandas_result = table_pandas.diagnostic()
```

You can define custom rules using __Calista__ to analyze specific conditions within your data:
```
my_rule = F.is_not_null(col_name="status") & F.is_not_null("salary")

print(table.analyze(rule_name="demo_new_model", condition=my_rule))
```

The output of the analysis provides insights into data quality based on the defined rule:
```
rule_name : demo_new_model
total_row_count : 3
valid_row_count : 2
valid_row_count_pct : 66.66
timestamp : 2024-04-23 10:00:59.449193
```

## License
Licensed under the Apache License
