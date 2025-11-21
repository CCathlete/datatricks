"""
This module provides helper functions for common data operations.
"""

import logging
from pathlib import Path
import os
import re
from typing import Any, Dict, Iterator, List, Optional, Union, TypeAlias

import datetime
import pandas as pd

# import sqlglotrs
import sqlglot

# from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result, make_url
from sqlalchemy.exc import SQLAlchemyError
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
        # Set up a logger for the module
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename=log_dir
            / f"{log_file_prefix}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        )
        logger = logging.getLogger(logger_name)

        return logger

    # TODO: Implement the init_env method.

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
        if logger is None:
            logger = cls.logger

        if not sql_query.strip():
            logger.warning("Received an empty or whitespace-only SQL query. Ignoring.")
            return 0

        if engine is None:
            logger.info("Engine not provided, creating default PostgreSQL engine.")
            engine = cls.engine

        # Check for errors and optimize the query.
        error, sql_query = cls._check_for_errors(
            sql_query, sql_dialect=engine.dialect.name, logger=logger
        )
        if error:
            raise ValueError(f"SQL query has an error: {error}")

        # Robustly determine if the query is a SELECT statement by stripping comments
        # and checking the first significant keyword.
        # 1. Remove multi-line comments /* ... */
        sql_no_multiline: str = re.sub(r"/\*.*?\*/", "", sql_query, flags=re.DOTALL)
        # 2. Remove single-line comments -- ...
        sql_no_comments: str = re.sub(r"--[^\r\n]*", "", sql_no_multiline)
        # 3. Check the first word
        first_word: str = (
            sql_no_comments.strip().split(maxsplit=1)[0].lower()
            if sql_no_comments.strip()
            else ""
        )
        is_select_query: bool = first_word == "select"

        if connection is not None:
            from_engine: bool = False
            con_to_use: sa.Connection = connection
        else:
            from_engine = True
            con_to_use = engine.connect()

        try:
            if is_select_query:
                logger.info("Executing SELECT query...")
                # For SELECT, we can use pandas which handles chunking nicely.
                # `params` are also supported by read_sql.
                output: pd.DataFrame = pd.read_sql_query(
                    sql=sql_query,
                    con=con_to_use,
                    params=params,  # type: ignore
                    chunksize=chunksize,  # type: ignore
                )  # type: ignore
                logger.info("SELECT query executed successfully.")

                if from_engine:
                    con_to_use.close()

                return output if output is not None else pd.DataFrame()
            else:
                # For non-SELECT queries (DML/DDL)
                if chunksize:
                    logger.warning("`chunksize` is ignored for non-SELECT queries.")

                logger.info("Executing non-SELECT (DML/DDL) query...")
                with con_to_use.begin():  # .begin() starts a transaction
                    result: Result = con_to_use.execute(
                        text(sql_query), parameters=params
                    )
                logger.info("Query executed successfully.")

                if from_engine:
                    con_to_use.close()

                return pd.DataFrame(result.fetchall())

        except SQLAlchemyError as e:
            logger.error(f"An error occurred during SQL query execution: {e}")
            # Re-raise the exception to allow the caller to handle it
            if from_engine:
                con_to_use.close()
            raise

    @classmethod
    def create_default_pg_engine(cls, logger: logging.Logger) -> Engine:
        """Creates a default SQLAlchemy engine for PostgreSQL from .env variables."""
        if logger is None:
            logger = cls.logger

        required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
        env_vars = {var: os.getenv(var) for var in required_vars}

        missing_vars = [var for var, value in env_vars.items() if value is None]
        if missing_vars:
            raise ValueError(
                f"Cannot create default engine. Missing environment variables: {
                    (', '.join(missing_vars))
                }"
            )

        url_object = make_url(
            f"postgresql+psycopg2://{env_vars['DB_USER']}:{env_vars['DB_PASSWORD']}@"
            f"{env_vars['DB_HOST']}:{env_vars['DB_PORT']}/{env_vars['DB_NAME']}"
        )

        logger.info(
            f"Creating engine for database '{url_object.database}' on host '{url_object.host}'..."
        )
        return create_engine(url_object)

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
        if logger is None:
            logger = cls.logger

        try:
            # sqlglotrs.transpile can parse, optimize, and generate sql.
            # We can pass the schema to the transpile function.
            transpiled_sql: List[str] = sqlglot.transpile(
                sql=sql_query,
                read=sql_dialect.lower(),
                write=sql_dialect.lower(),
            )
            logger.info(f"Transpiled SQL: {transpiled_sql}")
            # The transpile function returns a list of strings
            sql_query = transpiled_sql[0]
            logger.info(f"Optimized SQL: {sql_query}")
        except Exception as e:
            return str(e), sql_query
        return None, sql_query
