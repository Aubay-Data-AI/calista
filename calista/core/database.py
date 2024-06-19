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

from abc import abstractmethod
from typing import Any, Dict, List

from calista.core.engine import LazyEngine


class Database(LazyEngine):
    def read_dataset(
        self,
        path=None,
        file_format=None,
        data=None,
        table=None,
        schema=None,
        database=None,
        dataframe=None,
        options: Dict[str, Any] = None,
    ):
        """
        Read the dataset.

        Args:
            path: The path to the dataset.
            file_format: The format of the dataset file.
            data: The dictionary containing the data of the table.
            table: The name of the table.
            schema: The schema containing the dataset.
            database: The database containing the dataset.
            dataframe: An existing dataframe.
            options (Dict[str, Any], optional): Configuration options for files.
        """
        if table:
            self._load_from_database(table=table, schema=schema, database=database)
        elif dataframe is not None:
            raise NotImplementedError(
                "Loading from a dataframe is not possible for a database."
            )
        elif data:
            raise NotImplementedError(
                "Loading from a dictionary is not possible for a database."
            )
        elif path or file_format:
            raise NotImplementedError(
                "Loading from a file path is not possible for a database."
            )
        else:
            raise ValueError("The arguments provided are incorrect.")

    def _load_from_dataframe(self, dataframe) -> None:
        raise NotImplementedError(
            "Cannot create CalistaTable from a dataframe for a Database object"
        )

    def _load_from_dict(self, data: Dict[str, List]) -> None:
        raise NotImplementedError(
            "Cannot create CalistaTable from scratch for a Database object"
        )

    def _load_from_path(
        self, path: str, file_format: str, options: Dict[str, Any] = None
    ) -> None:
        raise NotImplementedError(
            "Cannot create CalistaTable from path for a Database object"
        )

    @abstractmethod
    def _load_from_database(
        self, table: str, schema: str = None, database: str = None
    ) -> None:
        ...
