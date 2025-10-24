"""
RTD (Rating Table Data) entities for converting XML rating tables to Iceberg Parquet format.

This module defines Pydantic models for:
- RTD table metadata (from rt_summary.xml and rtd file headers)
- Column metadata (from qualifier definitions)
- Row data structures
- AWS Glue catalog schema information
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class RTDColumnMetadata(BaseModel):
    """
    Metadata for a single column in an RTD table.

    This metadata is derived from the Qualifier entity and describes
    each column's data type, name, and purpose for AWS Glue catalog registration.
    """

    column_index: int = Field(
        description="Position of this column in the tilde-delimited value string (0-indexed)"
    )
    column_name: str = Field(
        description="Descriptive name for this column, sourced from qualifier definition"
    )
    data_type: str = Field(
        description="Data type for AWS Glue (string, int, bigint, double, date, boolean)"
    )
    glue_type: str = Field(
        description="AWS Glue/Athena compatible data type (STRING, INT, BIGINT, DOUBLE, DATE, BOOLEAN)"
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of what this column represents"
    )
    qualifier_index: Optional[int] = Field(
        default=None,
        description="Reference to the qualifier index (i) from the qualifier entity"
    )
    is_nullable: bool = Field(
        default=True,
        description="Whether this column can contain null/empty values"
    )

    model_config = ConfigDict(from_attributes=True)


class RTDTableMetadata(BaseModel):
    """
    Metadata for an RTD table extracted from rt_summary.xml and rtd file headers.

    This model captures all information needed to:
    1. Map rtd.dt<number> files to meaningful table names
    2. Register tables in AWS Glue catalog
    3. Track data lineage and changes via dlm hash
    4. Partition data by program_id and version
    """

    # Table identification
    table_index: int = Field(
        description="Table index number from rtd.dt<number> filename"
    )
    table_name: str = Field(
        description="Descriptive table name from rt_summary.xml 'name' attribute"
    )
    glue_table_name: str = Field(
        description="AWS Glue-compliant table name (lowercase, underscores, alphanumeric)"
    )

    # Program/version context
    program_id: str = Field(
        description="Insurance program identifier (e.g., '891', '611')"
    )
    program_version: str = Field(
        description="Program version number (e.g., '1.0000', '296.0000')"
    )
    line_of_business: str = Field(
        description="Line of business code (e.g., '118' for Property, '1' for Auto)"
    )
    carrier_id: str = Field(
        description="Insurance carrier identifier"
    )

    # Table metadata from rtd file header
    table_prod: Optional[str] = Field(
        default=None,
        description="Product code from rtd <lkupvars><l table_prod='...' />"
    )
    dlm_hash: str = Field(
        description="Data Last Modified hash from rtd header for change tracking"
    )
    dlm_table_timestamp: Optional[str] = Field(
        default=None,
        description="Table last modified timestamp from rtd header (dlm_table attribute)"
    )
    dlm_table_data_timestamp: Optional[str] = Field(
        default=None,
        description="Table data last modified timestamp (dlm_table_data attribute)"
    )
    row_count: int = Field(
        description="Total number of rows in this table (from table_count or actual count)"
    )

    # Column definitions
    columns: list[RTDColumnMetadata] = Field(
        description="List of column metadata derived from qualifier definitions"
    )

    # Continuation file tracking
    has_continuation_files: bool = Field(
        default=False,
        description="Whether this table has rtd.dt<number>.* continuation files"
    )
    continuation_file_count: int = Field(
        default=0,
        description="Number of continuation files (rtd.dt37.1, rtd.dt37.2, etc.)"
    )

    # Glue catalog information
    glue_database_name: str = Field(
        description="AWS Glue database name (Insbridge_PL_Release_<date_iso>)"
    )
    glue_s3_location: str = Field(
        description="S3 location for Iceberg table data"
    )

    # Processing metadata
    extracted_at: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp when this table was extracted and processed"
    )

    model_config = ConfigDict(from_attributes=True)


class RTDRow(BaseModel):
    """
    Represents a single row from an RTD file.

    The 'v' attribute contains tilde-delimited values that will be split
    based on the table's column metadata.
    """

    row_id: str = Field(
        description="Row identifier from 'i' attribute (e.g., '10000', '10001')"
    )
    values_raw: str = Field(
        description="Tilde-delimited values from 'v' attribute (e.g., '1~1~1~1~1~1~1~1~1~1~13~1~1')"
    )
    values_split: list[str] = Field(
        description="Values split by tilde delimiter into individual column values"
    )
    dlm_timestamp: Optional[str] = Field(
        default=None,
        description="Row last modified timestamp from 'd' attribute (decimal format)"
    )
    qualifier_data: Optional[str] = Field(
        default=None,
        description="Pipe-delimited qualifier data from 'q' attribute (e.g., 'AWE-1643||')"
    )

    model_config = ConfigDict(from_attributes=True)


class RTDTableData(BaseModel):
    """
    Complete RTD table with metadata and rows, ready for Iceberg conversion.
    """

    metadata: RTDTableMetadata = Field(
        description="Table metadata including schema and Glue catalog information"
    )
    rows: list[RTDRow] = Field(
        description="All rows from the table and its continuation files"
    )

    # Partition information
    partition_program_id: str = Field(
        description="Program ID for partitioning in S3/Glue"
    )
    partition_version: str = Field(
        description="Version for partitioning in S3/Glue"
    )

    model_config = ConfigDict(from_attributes=True)


class GlueTableSchema(BaseModel):
    """
    AWS Glue table schema definition for registering RTD tables.
    """

    database_name: str = Field(
        description="Glue database name (e.g., 'Insbridge_PL_Release_2025_03_31')"
    )
    table_name: str = Field(
        description="Glue table name (must be lowercase, alphanumeric, underscores)"
    )
    columns: list[dict[str, str]] = Field(
        description="List of column definitions with 'Name', 'Type', and 'Comment' keys"
    )
    partition_keys: list[dict[str, str]] = Field(
        description="Partition column definitions (program_id, version)"
    )
    location: str = Field(
        description="S3 location for table data (s3://bucket/path/)"
    )
    table_type: str = Field(
        default="ICEBERG",
        description="Table type (ICEBERG for Apache Iceberg Open Table Format)"
    )
    storage_format: str = Field(
        default="PARQUET",
        description="Storage format (PARQUET for Iceberg)"
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable table description"
    )

    model_config = ConfigDict(from_attributes=True)
