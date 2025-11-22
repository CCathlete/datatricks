"""
This module provides helper functions for common data operations.
"""

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union, TypeAlias, Tuple

import logging
import datetime
import pandas as pd

from sqlalchemy.engine import Engine
import sqlalchemy as sa

SQLGlotSchemaType = Dict[str, Any]

ParamType: TypeAlias = (
    str | datetime.date | datetime.datetime | int | float | bool | None
)

class Module:
    """
    A container for helper functions.
    """

    logger: logging.Logger
    engine: Engine
    python_dir: Path
    root_dir: Path
    log_dir: Path
    project_root: Path
    input_files_dir: Path

    @classmethod
    def setup_logging(
        cls,
        logger_name: str = __name__,
        log_level: int = logging.INFO,
        log_file_prefix: str = "datatricks",
        log_dir: Path = Path.cwd() / "logs",
    ) -> logging.Logger:
        """
        Sets up a logger.
        """

    @classmethod
    def init_locations_and_dotenv(
        cls,
        project_root: Optional[Path] = None,
        project_root_marker: str = "src",
        dotenv_location: Optional[Path] = None,
        logger_name: str = __name__,
        log_file_prefix: str = "datatricks",
        log_level: int = logging.INFO,
    ) -> Tuple[Engine, Path, Path, Path, logging.Logger]:
        """
        Initializes project locations, loads environment variables, sets up logging,
        and creates the default PostgreSQL engine.
        """

    @classmethod
    def execute_query(
        cls,
        sql_query: str,
        engine: Optional[Engine] = None,
        connection: Optional[sa.Connection] = None,
        params: Optional[
            Union[Dict[str, ParamType], List[Dict[str, ParamType]]]
        ] = None,
        chunksize: Optional[int] = None,
        logger: Optional[logging.Logger] = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame], int]:
        """
        Executes a SQL query using a SQLAlchemy engine.

        - For SELECT queries, it returns a pandas DataFrame or a generator of DataFrames.
        - For DML/DDL queries (INSERT, UPDATE, DELETE, GRANT, etc.), it executes the
        statement and returns the number of affected rows.
        - Supports parameterized queries to prevent SQL injection.
        - If the engine is not provided, it attempts to create a default PostgreSQL
        engine using credentials from a .env file.
        - Ignores empty/whitespace-only queries.
        - Robustly handles SQL comments (--, /* */) when determining query type.

        Args:
            sql_query (str): The SQL query to be executed.
            engine (Optional[Engine], optional): The SQLAlchemy engine instance. If None,
                a default engine is created from .env variables. Defaults to None.
            params (Optional[Union[Dict, List[Dict]]], optional): Parameters to bind to the
                query for safe execution. Use a dict for a single statement or a list of
                dicts for an "executemany" operation. Defaults to None.
            chunksize (Optional[int], optional): The number of rows to include in each chunk.
                Applicable only to SELECT queries. If None, the entire result is returned
                as a single DataFrame. If an integer is provided, a generator of
                DataFrames is returned.

        Returns:
            Union[pd.DataFrame, Generator[pd.DataFrame, None, None], int]:
                - For SELECT: A pandas DataFrame or an iterator of DataFrames.
                - For other queries: An integer representing the number of rows affected.
                - Returns 0 if the query string is empty or whitespace.

        Raises:
            ValueError: If the engine is not a valid SQLAlchemy Engine instance or
                        if a default engine cannot be created due to missing .env variables.
            SQLAlchemyError: For errors during query execution.
        """

    @classmethod
    def create_default_pg_engine(cls, logger: logging.Logger) -> Engine:
        """Creates a default SQLAlchemy engine for PostgreSQL from .env variables."""

    @classmethod
    def _check_for_errors(
        cls,
        sql_query: str,
        sql_dialect: str,
        logger: Optional[logging.Logger] = None,
    ) -> tuple[Optional[str], str]:
        """Checks for errors in the SQL query using sqlglotrs.

        Args:
        sql_query: The SQL query to check for errors.
        sql_dialect: The SQL dialect of the SQL query.
        schema_dict: The DDL schema to use for the translation. The DDL format is
            in the SQLGlot format. This field is optional.

        Returns:
        A tuple containing any errors in the SQL query (or None if no errors)
        and the optimized SQL query.
        """
