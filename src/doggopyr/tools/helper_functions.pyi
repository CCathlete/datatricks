from pathlib import Path
import logging
from typing import (
    Dict,
    Optional,
    Tuple,
    Literal,
    ClassVar,
)

# Type Aliases
JDBCProperties = Dict[str, str]

# PySpark Types (Assuming you use stubs for PySpark)
class SparkSession: ...
class SparkDataFrame: ...

class Module:
    """
    A container for helper functions, adapted for PySpark.
    """
    logger: ClassVar[logging.Logger]
    spark: ClassVar[SparkSession]
    project_root: ClassVar[Path]
    log_dir: ClassVar[Path]
    input_files_dir: ClassVar[Path]
    jdbc_properties: ClassVar[JDBCProperties]

    @classmethod
    def is_jupy(cls) -> bool: ...

    @classmethod
    def setup_logging(
        cls,
        logger_name: str = ...,
        log_level: int = ...,
        log_file_prefix: str = ...,
        log_dir: Path = ...,
    ) -> logging.Logger: ...

    @classmethod
    def init_locations_and_dotenv(
        cls,
        project_root: Optional[Path] = ...,
        project_root_marker: str = ...,
        dotenv_location: Optional[Path] = ...,
        logger_name: str = ...,
        log_file_prefix: str = ...,
        log_level: int = ...,
        spark_app_name: str = ...,
    ) -> Tuple[SparkSession, Path, Path, Path, logging.Logger]: ...

    @classmethod
    def get_default_jdbc_properties(cls, logger: Optional[logging.Logger] = ...) -> JDBCProperties: ...

    @classmethod
    def read_sql(
        cls,
        sql_query: str,
        jdbc_properties: Optional[JDBCProperties] = ...,
        spark_session: Optional[SparkSession] = ...,
        logger: Optional[logging.Logger] = ...,
    ) -> SparkDataFrame: ...

    @classmethod
    def _check_for_errors(
        cls,
        sql_query: str,
        sql_dialect: str,
        logger: Optional[logging.Logger] = ...,
    ) -> Tuple[Optional[str], str]: ...

    @classmethod
    def _add_etl_columns(
        cls,
        df: SparkDataFrame,
        add_modifiedat: bool,
        add_serial_id: bool,
        logger: logging.Logger,
    ) -> SparkDataFrame: ...

    @classmethod
    def to_sql(
        cls,
        df: SparkDataFrame,
        name: str,
        jdbc_properties: Optional[JDBCProperties] = ...,
        schema: Optional[str] = ...,
        if_exists: Literal["fail", "replace", "append"] = ...,
        add_modifiedat: bool = ...,
        add_serial_id: bool = ...,
        logger: Optional[logging.Logger] = ...,
    ) -> None: ...