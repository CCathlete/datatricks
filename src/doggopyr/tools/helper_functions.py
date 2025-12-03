"""
This module provides helper functions for common data operations, adapted for PySpark,
using Spark DataFrame transformations to manage serial IDs and modifiedAt timestamps.
"""

from pathlib import Path
import os
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Literal,
)
from IPython.core.getipython import get_ipython

import logging
import datetime
# PySpark imports
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import types as spark_types
# Spark functions for ETL-controlled columns
from pyspark.sql.functions import row_number, current_timestamp
from pyspark.sql.window import Window

# Legacy (for .env) and SQL parsing imports
from dotenv import load_dotenv
import sqlglot


SQLGlotSchemaType = Dict[str, Any]

# Type alias for JDBC properties dictionary
JDBCProperties = Dict[str, str]

# Recommended Maven coordinates for the PostgreSQL JDBC driver
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"


class Module:
    """
    A container for helper functions, adapted for PySpark (session.sql for SELECT, 
    DataFrame transformations for ID/Timestamp management).
    """

    logger: logging.Logger
    spark: SparkSession  # PySpark's entry point
    project_root: Path
    log_dir: Path
    input_files_dir: Path
    jdbc_properties: JDBCProperties
    
    # --------------------------------------------------------------------------
    ## 🆕 New Method: Environment Check
    # --------------------------------------------------------------------------
    @classmethod
    def is_jupy(cls) -> bool:
        """
        Checks if the Python script is running inside a Jupyter Notebook 
        (or IPython kernel).
        """
        try:
            # The __IPYTHON__ variable is set when running inside an IPython shell, 
            # including Jupyter Notebooks and JupyterLab.
            return get_ipython().__class__.__name__ == 'ZMQInteractiveShell' # pyright: ignore
        except NameError:
            # get_ipython() raises NameError if not in an IPython environment.
            return False
    # --------------------------------------------------------------------------
    
    @classmethod
    def setup_logging(
        cls,
        logger_name: str = __name__,
        log_level: int = logging.INFO,
        log_file_prefix: str = "datatricks",
        log_dir: Path = Path.cwd() / "logs",
    ) -> logging.Logger:
        """Sets up a logger."""
        # We can use is_jupy() here to potentially alter logging format or destination
        # if running interactively.
        
        # Determine if running in an interactive session
        is_interactive = cls.is_jupy()

        # Basic configuration (file logging, default behavior)
        logging_config: Dict[str, Any]= {
            "level": log_level,
            "format": "%(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
        
        if not is_interactive:
            # If not interactive, log to a file
            logging_config["filename"] = (
                log_dir
                / f"{log_file_prefix}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            )
        
        # Apply logging configuration
        logging.basicConfig(**logging_config)
        
        logger = logging.getLogger(logger_name)
        
        if is_interactive:
            # Ensure interactive logging is not suppressed by the file handler config
            logger.info("Running in Jupyter/IPython interactive mode.")

        return logger

    @classmethod
    def init_locations_and_dotenv(
        cls,
        project_root: Optional[Path] = None,
        project_root_marker: str = "src",
        dotenv_location: Optional[Path] = None,
        logger_name: str = __name__,
        log_file_prefix: str = "datatricks",
        log_level: int = logging.INFO,
        spark_app_name: str = "PySparkDataTricks",
    ) -> Tuple[SparkSession, Path, Path, Path, logging.Logger]:
        """
        Initializes project locations, loads environment variables, sets up logging,
        gets JDBC properties, and creates a SparkSession.
        """
        current_file_dir = Path(__file__).resolve().parent

        # 1. Determine Project Root Location (unchanged)
        if project_root is None:
            for parent in current_file_dir.parents:
                if (parent / project_root_marker).exists():
                    cls.project_root = parent
                    break
            else:
                raise FileNotFoundError(
                    f"Warning: Could not find marker '{project_root_marker}'."
                )
        else:
            cls.project_root = project_root

        # 2. Setup Directories (unchanged)
        cls.log_dir = cls.project_root / "logs"
        cls.input_files_dir = cls.project_root / "input_files"
        cls.log_dir.mkdir(exist_ok=True)
        cls.input_files_dir.mkdir(exist_ok=True)

        # 3. Setup Logging (uses updated setup_logging)
        cls.logger = cls.setup_logging(
            logger_name=logger_name,
            log_file_prefix=log_file_prefix,
            log_dir=cls.log_dir,
            log_level=log_level,
        )
        cls.logger.info("Project root set to: %s", cls.project_root)

        # 4. Load .env Variables (unchanged)
        if dotenv_location is not None:
            load_dotenv(dotenv_path=dotenv_location)
            cls.logger.info("Loaded .env from explicit path: %s", dotenv_location)
        else:
            dotenv_found = False
            for dotenv_path in cls.project_root.rglob(".env"):
                load_dotenv(dotenv_path=dotenv_path)
                cls.logger.info("Loaded .env from: %s", dotenv_path)
                dotenv_found = True
                break
            if not dotenv_found:
                cls.logger.warning(
                    "No .env file found via downward search from project root."
                )

        # 5. Get JDBC Connection Properties (unchanged)
        cls.jdbc_properties = cls.get_default_jdbc_properties(cls.logger)

        # 6. Create Spark Session (unchanged, uses spark.jars.packages)
        cls.logger.info("Configuring Spark to download JDBC package: %s", POSTGRES_JDBC_PACKAGE)
        
        cls.spark = (
            SparkSession.builder
            .appName(spark_app_name)
            .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
            .getOrCreate()
        )
        # Suppress verbose Spark logging unless running interactively (for better display)
        if not cls.is_jupy():
             cls.spark.sparkContext.setLogLevel("WARN")
        else:
             cls.spark.sparkContext.setLogLevel("ERROR") # Log only errors in interactive mode

        cls.logger.info("Spark Session created successfully.")

        return (
            cls.spark,
            cls.project_root,
            cls.input_files_dir,
            cls.log_dir,
            cls.logger,
        )

    @classmethod
    def get_default_jdbc_properties(cls, logger: logging.Logger | None = None) -> JDBCProperties:
        """
        Creates JDBC connection properties for PostgreSQL from .env variables. (unchanged)
        """
        if logger is None:
            logger = cls.logger

        required_vars = [
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
        ]
        env_vars = {var: os.getenv(var) for var in required_vars}

        missing_vars = [var for var, value in env_vars.items() if value is None]
        if missing_vars:
            raise ValueError(
                f"Cannot create default JDBC properties. Missing environment variables: {
                    (', '.join(missing_vars))
                }"
            )

        jdbc_url = (
            f"jdbc:postgresql://{env_vars['POSTGRES_HOST']}:{env_vars['POSTGRES_PORT']}"
            f"/{env_vars['POSTGRES_DB']}"
        )

        jdbc_properties: JDBCProperties = {
            "user": env_vars["POSTGRES_USER"],
            "password": env_vars["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
            "url": jdbc_url,
        } # pyright: ignore

        logger.info(
            "Created JDBC properties for database '%s' on host '%s'.",
            env_vars['POSTGRES_DB'],
            env_vars['POSTGRES_HOST'],
        )
        return jdbc_properties

    @classmethod
    def read_sql(
        cls,
        sql_query: str,
        jdbc_properties: Optional[JDBCProperties] = None,
        spark_session: Optional[SparkSession] = None,
        logger: Optional[logging.Logger] = None,
    ) -> SparkDataFrame:
        """
        Reads data from the database using a custom SQL query into a Spark DataFrame. (unchanged)
        """
        if logger is None:
            logger = cls.logger
        if spark_session is None:
            spark_session = cls.spark
        if jdbc_properties is None:
            jdbc_properties = cls.jdbc_properties

        logger.info("Executing SQL query to read data using PySpark JDBC...")

        try:
            df: SparkDataFrame = spark_session.read.jdbc(
                url=jdbc_properties["url"],
                table=f"({sql_query}) AS custom_query", # Wrap the query
                properties=jdbc_properties,
            )
            logger.info("SELECT query executed successfully, returned Spark DataFrame.")
            return df
        except Exception as e:
            logger.error("An error occurred during PySpark/JDBC query execution: %s", e)
            raise

    @classmethod
    def _check_for_errors(
        cls,
        sql_query: str,
        sql_dialect: str,
        logger: Optional[logging.Logger] = None,
    ) -> tuple[Optional[str], str]:
        """Checks for errors in the SQL query using sqlglot. (unchanged)"""
        if logger is None:
            logger = cls.logger

        try:
            transpiled_sql: list[str] = sqlglot.transpile(  # type: ignore
                sql=sql_query,
                read=sql_dialect.lower(),
                write=sql_dialect.lower(),
            )
            sql_query = transpiled_sql[0]
        except (ValueError, sqlglot.UnsupportedError, sqlglot.ParseError) as e:
            return str(e), sql_query
        return None, sql_query
    
    @classmethod
    def _add_etl_columns(
        cls,
        df: SparkDataFrame,
        add_modifiedat: bool,
        add_serial_id: bool,
        logger: logging.Logger,
    ) -> SparkDataFrame:
        """
        Adds 'id' and 'modifiedat' columns to the DataFrame using Spark functions.
        """
        if add_serial_id:
            logger.info("Adding ETL-controlled 'id' (BIGINT) column to DataFrame.")
            
            # Using row_number() for sequential ID generation
            # NOTE: For stability, a stable ordering column is preferred. 
            # If the source has no natural order, you might need to read the MAX(id) 
            # from the target table if this is an APPEND operation.
            window_spec = Window.orderBy("A_COLUMN_THAT_ENSURES_STABLE_ORDER") # Placeholder!
            
            # Use monotonically_increasing_id() for faster, non-sequential unique IDs:
            # df = df.withColumn("id", monotonically_increasing_id())
            
            df = df.withColumn(
                "id", 
                row_number().over(window_spec).cast(spark_types.LongType())
            )

        if add_modifiedat:
            logger.info("Adding ETL-controlled 'modifiedat' (TIMESTAMP) column to DataFrame.")
            # Use current_timestamp() to capture the exact time the ETL job is executing this step.
            df = df.withColumn(
                "modifiedat", 
                current_timestamp()
            )
            
        return df


    @classmethod
    def to_sql(
        cls,
        df: SparkDataFrame,
        name: str,
        jdbc_properties: Optional[JDBCProperties] = None,
        schema: Optional[str] = None,
        if_exists: Literal["fail", "replace", "append"] = "append",
        add_modifiedat: bool = True,
        add_serial_id: bool = True,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Writes records stored in a PySpark DataFrame to a SQL database using JDBC, 
        handling ID and timestamp generation in Spark before writing.
        """
        if logger is None:
            logger = cls.logger
        if jdbc_properties is None:
            jdbc_properties = cls.jdbc_properties

        full_table_name = f"{schema}.{name}" if schema else name
        
        # 1. Add ETL-Controlled Columns to the DataFrame
        df_to_write = cls._add_etl_columns(df, add_modifiedat, add_serial_id, logger)

        # 2. Handle 'if_exists' logic (unchanged)
        spark_mode: Literal["errorifexists", "overwrite", "append"]
        if if_exists == "replace":
            spark_mode = "overwrite"
        elif if_exists == "fail":
            spark_mode = "errorifexists"
        else: # "append"
            spark_mode = "append"

        # 3. Write data using Spark JDBC
        logger.info("Writing Spark DataFrame to table '%s' using JDBC mode '%s'...", full_table_name, spark_mode)
        try:
            df_to_write.write.jdbc(
                url=jdbc_properties["url"],
                table=full_table_name,
                mode=spark_mode, 
                properties=jdbc_properties,
            )
            logger.info(
                "Successfully wrote data to table '%s'.", full_table_name
            )

        except Exception as e:
            logger.error("Failed to write data to table '%s' using JDBC: %s", full_table_name, e)
            raise
            
        # 4. Cleanup/Post-Write (Empty in this model)
        logger.info("No database DDL or trigger creation required as ID/Timestamp is ETL-controlled.")