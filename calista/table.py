# Copyright 2024 Aubay.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from calista.core._conditions import (
    AndCondition,
    CompareColumnToColumn,
    Condition,
    NotCondition,
    OrCondition,
)
from calista.core.engine import GenericColumnType, LazyEngine
from calista.core.metrics import Metrics
from calista.core.types_alias import ColumnName, PythonType, RuleName
from calista.core.utils import import_engine

if TYPE_CHECKING:
    from calista.group import GroupedTable


class CalistaTable:
    def __init__(self, engine: LazyEngine):
        """
        Initialize the class with the specified engine and configuration.

        Args:
            engine (str): The engine to use for data processing.
            config (Dict[str, Any], optional): Configuration options for the engine (default: None).

        Raises:
            Exception: If the specified engine is not supported.
        """
        self._engine = engine

    @property
    def schema(self) -> dict[ColumnName, PythonType]:
        """
        Returns the schema of the underlying dataset.

        Returns:
            Dict[ColumnName, PythonType]: Dict representing the schema of the underlying dataset.
        """
        return self._engine.get_schema()

    def show(self, n: int = 10) -> None:
        """
        Prints the first n rows to the console.

        Args:
            n (int, optional): Number of rows to show
        """
        self._engine.show(n)

    def group_by(self, *cols: str) -> GroupedTable:
        """
        Groups the :class:`CalistaTable` using the specified columns,
        so we can execute aggregation conditions on them. See :class:`GroupedTable`
        for all the available functions after calling group_by.

        Args:
            cols (list, str):columns to group by.
            Each element should be a column name (string).
        """
        for col in cols:
            if col not in self.schema.keys():
                raise Exception(
                    f"Column '{col}' not found in {list(self.schema.keys())}"
                )
        from calista.group import GroupedTable

        return GroupedTable(self._engine, cols)

    def where(self, condition: Condition) -> CalistaTable:
        """
        Filters rows using the given condition.

        :func:`filter` is an alias for :func:`where`.

        Args:
            condition : :class:`Condition`

        Returns:
            :class:`CalistaTable`: Filtered CalistaTable.
        """
        if condition.is_aggregate:
            raise Exception("Cannot apply filter for AggregateCondition")

        expr = self._evaluate_condition(condition)
        dataset_filtered = self._engine.filter(expr)

        new_engine = self._engine.create_new_instance_from_dataset(dataset_filtered)

        return CalistaTable(new_engine)

    def filter(self, condition: Condition) -> CalistaTable:
        return self.where(condition)

    def _evaluate_condition(self, condition: Condition) -> GenericColumnType:

        if isinstance(condition, AndCondition) or isinstance(condition, OrCondition):
            left_cond = self._evaluate_condition(condition.left)
            right_cond = self._evaluate_condition(condition.right)

            return self._engine[condition](left_cond, right_cond)

        if isinstance(condition, NotCondition):
            cond = self._evaluate_condition(condition.cond)
            return self._engine[condition](cond)

        if isinstance(condition, CompareColumnToColumn):
            if condition.col_left not in self.schema.keys():
                raise Exception(
                    f"Column '{condition.col_left}' not found in {list(self.schema.keys())}"
                )
            if condition.col_right not in self.schema.keys():
                raise Exception(
                    f"Column '{condition.col_right}' not found in {list(self.schema.keys())}"
                )
            return self._engine[condition](condition)

        if condition.col_name not in self.schema.keys():
            raise Exception(
                f"Column '{condition.col_name}' not found in {list(self.schema.keys())}"
            )

        return self._engine[condition](condition)

    def analyze(self, rule_name: str, condition: Condition) -> Metrics:
        """
        Compute :class:`~calista.core.metrics.Metrics` based on a condition.

        Args:
            rule_name (str): The name of the rule.
            condition (Condition): The condition to evaluate.

        Returns:
            :class:`~calista.core.metrics.Metrics`: The metrics resulting from the analysis.

        Raises:
            Any exceptions raised by the analyze_rules method.
        """
        return self.analyze_rules({rule_name: condition})[0]

    def analyze_rules(self, rules: dict[RuleName, Condition]) -> list[Metrics]:
        """
        Compute :class:`list[Metrics]` based on conditions.

        Args:
            rules (dict[RuleName, Condition]): The name of the rules and the conditions to execute.

        Returns:
            :class:`list[Metrics]`: The metrics resulting from the analysis.

        Raises:
            Any exceptions raised by the engine's execute_conditions method.
        """
        conditions = {
            rule_name: self._evaluate_condition(rule_condition)
            for rule_name, rule_condition in rules.items()
        }
        return self._engine.execute_conditions(conditions)

    def _get_type_format(
        self,
        col_name,
        conditions: list[tuple[Condition, str]],
        threshold,
        percentage=False,
    ) -> Optional[str]:
        type_scores = []
        type_format = None
        for cond, value in conditions:
            m = self.analyze(f"{col_name} is_{value}", cond)
            type_scores.append((m.valid_row_count_pct, value))
        max_score = max(type_scores, key=lambda x: x[0])
        if max_score[0] >= threshold:
            type_format = (
                f"{max_score[1]} ({max_score[0]}%)" if percentage else max_score[1]
            )

        return type_format


class CalistaEngine:
    """
    For now, you can execute data quality checks using the
    following engines or platforms: spark, pandas, polars,
    snowflake, bigquery.
    """

    def __init__(self, engine: str, config: Dict[str, Any] = None):
        """
        Initialize the class with the specified engine and configuration.

        Args:
            engine (str): The engine to use for data processing.
            config (Dict[str, Any], optional): Configuration options for the engine (default: None).

        Raises:
            Exception: If the specified engine is not supported.
        """
        self._engine_name = engine.lower()
        if engine.lower() == "pandas":
            engine = "pandas_"
        elif engine.lower() == "polars":
            engine = "polars_"
        if engine.lower() not in [
            "spark",
            "snowflake",
            "sql",
            "bigquery",
            "pandas_",
            "polars_",
        ]:
            raise Exception(
                f"Je ne sais pas faire avec le moteur {engine} pour l'instant"
            )
        self._engine = import_engine(engine.lower())(config=config)

    def load(
        self,
        path: str = None,
        file_format: str = None,
        data: Dict[str, List] = None,
        table: str = None,
        schema: str = None,
        database: str = None,
        dataframe: Any = None,
        options: Dict[str, Any] = None,
    ) -> CalistaTable:
        """
        Load data from a dataset into a :class:`~calista.table.CalistaTable`.

        :param (str, optional) path:
            The path if you're loading a file.
        :param (str, optional) file_format:
            The format of the file (e.g., 'csv', 'parquet').
        :param (dict) data:
            The dictionary containing the data of the table.
        :param (str, optional) table:
            The name of the table if you're not loading a file.
        :param (str, optional) schema:
            The schema containing the table.
        :param (str, optional) database:
            The database containing the table.
        :param (Any, optional) dataframe:
            An existing dataframe.
        :param (Dict[str, Any], optional) options:
            Additional configuration file options.

        Returns:
            :class:`~calista.table.CalistaTable`: The loaded table.

        Raises:
            Any exceptions raised by the engine's read_dataset method.
        """
        self._engine.read_dataset(
            path=path,
            file_format=file_format,
            data=data,
            table=table,
            schema=schema,
            database=database,
            dataframe=dataframe,
            options=options,
        )
        return CalistaTable(self._engine)

    def load_from_dict(self, data: Dict[str, List]) -> CalistaTable:
        """
        Load data from a dictionary into a :class:`~calista.table.CalistaTable`.

        :param (dict) data:
            The dictionary containing the data of the table.

        Returns:
            :class:`~calista.table.CalistaTable`: The loaded table.

        Raises:
            Any exceptions raised by the engine's read_dataset method.

        Example
        --------
        >>> calista_table = CalistaEngine(engine = "spark").load_from_dict({"ID": [1, 2, 3, 4]})
        >>> calista_table.show()
        +---+
        | ID|
        +---+
        |  1|
        |  2|
        |  3|
        |  4|
        +---+
        """
        return self.load(data=data)

    def load_from_path(
        self, path: str, file_format: str, options: Dict[str, Any] = None
    ) -> CalistaTable:
        """
        Load data from a path into a :class:`~calista.table.CalistaTable`.

        :param (str) path:
            The path of the file containing your table.
        :param (str, optional) file_format:
            The format of the file (e.g., 'csv', 'parquet').

        Returns:
            :class:`~calista.table.CalistaTable`: The loaded table.

        Raises:
            Any exceptions raised by the engine's read_dataset method.

        Example
        --------
        >>> csv_options = {
        >>> "delimiter": ",",
        >>> "header": "True"
        >>> }
        >>> calista_table = CalistaEngine(engine = "spark").load_from_path(path='my_csv.csv',file_format="csv",options=csv_options)
        """
        return self.load(path=path, file_format=file_format, options=options)

    def load_from_dataframe(self, dataframe: Any) -> CalistaTable:
        """
        Load data from a dataframe into a :class:`~calista.table.CalistaTable`.

        :param (Any) dataframe:
            An existing dataframe.

        Returns:
            :class:`~calista.table.CalistaTable`: The loaded table.

        Raises:
            Any exceptions raised by the engine's read_dataset method.
        """
        return self.load(dataframe=dataframe)

    def load_from_database(
        self, table: Any, schema: str = None, database: str = None
    ) -> CalistaTable:
        """
        Load data from a table into a :class:`~calista.table.CalistaTable`.

        :param (str) table:
            The name of the table.
        :param (str, optional) schema:
            The schema containing the table
        :param (str, optional) database:
            The database containing the table.

        Returns:
            :class:`~calista.table.CalistaTable`: The loaded table.

        Raises:
            Any exceptions raised by the engine's read_dataset method.
        """
        return self.load(table=table, schema=schema, database=database)
