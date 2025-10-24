"""
RTD to Iceberg Parquet Converter.

This module converts RTDTableData to Apache Iceberg format with:
- Parquet storage format
- Partitioning by program_id and version
- AWS Glue catalog integration
- S3 storage location
"""

from pathlib import Path
from typing import Any, Optional
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

from kiro_insbridge.enterprise_rating.entities.rtd_table import (
    RTDTableData,
    RTDTableMetadata,
    GlueTableSchema,
)


class RTDToIcebergConverter:
    """Converter for RTD tables to Iceberg Parquet format."""

    @staticmethod
    def map_glue_type_to_arrow(glue_type: str) -> pa.DataType:
        """
        Map AWS Glue data type to PyArrow data type.

        Args:
            glue_type: Glue type (STRING, INT, BIGINT, DOUBLE, DATE, BOOLEAN)

        Returns:
            PyArrow data type
        """
        type_mapping = {
            "STRING": pa.string(),
            "INT": pa.int32(),
            "BIGINT": pa.int64(),
            "DOUBLE": pa.float64(),
            "FLOAT": pa.float32(),
            "DATE": pa.date32(),
            "TIMESTAMP": pa.timestamp("us"),
            "BOOLEAN": pa.bool_(),
        }
        return type_mapping.get(glue_type.upper(), pa.string())

    @staticmethod
    def build_arrow_schema(metadata: RTDTableMetadata) -> pa.Schema:
        """
        Build PyArrow schema from RTDTableMetadata.

        Args:
            metadata: RTDTableMetadata instance

        Returns:
            PyArrow schema with columns and partition fields
        """
        fields = []

        # Add data columns
        for col in metadata.columns:
            arrow_type = RTDToIcebergConverter.map_glue_type_to_arrow(col.glue_type)
            field = pa.field(col.column_name, arrow_type, nullable=col.is_nullable)
            fields.append(field)

        # Add system columns
        fields.extend([
            pa.field("row_id", pa.string(), nullable=False),
            pa.field("dlm_timestamp", pa.string(), nullable=True),
            pa.field("qualifier_data", pa.string(), nullable=True),
        ])

        # Add partition columns (these will be in the data but also used for partitioning)
        fields.extend([
            pa.field("program_id", pa.string(), nullable=False),
            pa.field("version", pa.string(), nullable=False),
        ])

        return pa.schema(fields)

    @staticmethod
    def convert_table_to_arrow(table_data: RTDTableData) -> pa.Table:
        """
        Convert RTDTableData to PyArrow Table.

        Args:
            table_data: RTDTableData instance

        Returns:
            PyArrow Table with data ready for Parquet write
        """
        metadata = table_data.metadata

        # Build column data
        columns_data = {col.column_name: [] for col in metadata.columns}
        row_ids = []
        dlm_timestamps = []
        qualifier_datas = []
        program_ids = []
        versions = []

        for row in table_data.rows:
            # Extract column values
            for idx, col in enumerate(metadata.columns):
                if idx < len(row.values_split):
                    value = row.values_split[idx]
                    # Convert empty strings to None for proper null handling
                    columns_data[col.column_name].append(value if value else None)
                else:
                    columns_data[col.column_name].append(None)

            # System columns
            row_ids.append(row.row_id)
            dlm_timestamps.append(row.dlm_timestamp)
            qualifier_datas.append(row.qualifier_data)

            # Partition columns
            program_ids.append(table_data.partition_program_id)
            versions.append(table_data.partition_version)

        # Build PyArrow arrays
        arrays = []
        schema_fields = []

        # Data columns
        for col in metadata.columns:
            arrow_type = RTDToIcebergConverter.map_glue_type_to_arrow(col.glue_type)
            arrays.append(pa.array(columns_data[col.column_name], type=arrow_type))
            schema_fields.append(pa.field(col.column_name, arrow_type, nullable=col.is_nullable))

        # System columns
        arrays.extend([
            pa.array(row_ids, type=pa.string()),
            pa.array(dlm_timestamps, type=pa.string()),
            pa.array(qualifier_datas, type=pa.string()),
        ])
        schema_fields.extend([
            pa.field("row_id", pa.string()),
            pa.field("dlm_timestamp", pa.string()),
            pa.field("qualifier_data", pa.string()),
        ])

        # Partition columns
        arrays.extend([
            pa.array(program_ids, type=pa.string()),
            pa.array(versions, type=pa.string()),
        ])
        schema_fields.extend([
            pa.field("program_id", pa.string()),
            pa.field("version", pa.string()),
        ])

        # Create schema and table
        schema = pa.schema(schema_fields)
        table = pa.Table.from_arrays(arrays, schema=schema)

        return table

    @staticmethod
    def write_parquet_local(
        table_data: RTDTableData,
        output_dir: Path,
        partition_cols: list[str] = ["program_id", "version"],
    ) -> Path:
        """
        Write RTDTableData to local Parquet file with partitioning.

        Args:
            table_data: RTDTableData instance
            output_dir: Local output directory
            partition_cols: Columns to partition by

        Returns:
            Path to output directory containing partitioned Parquet files
        """
        # Convert to Arrow table
        arrow_table = RTDToIcebergConverter.convert_table_to_arrow(table_data)

        # Create output directory
        table_output_dir = output_dir / table_data.metadata.glue_table_name
        table_output_dir.mkdir(parents=True, exist_ok=True)

        # Write partitioned Parquet
        pq.write_to_dataset(
            arrow_table,
            root_path=str(table_output_dir),
            partition_cols=partition_cols,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        )

        print(f"Wrote Parquet data to: {table_output_dir}")
        return table_output_dir

    @staticmethod
    def write_parquet_to_s3(
        table_data: RTDTableData,
        s3_bucket: str,
        s3_prefix: str,
        partition_cols: list[str] = ["program_id", "version"],
        aws_region: str = "us-east-2",
    ) -> str:
        """
        Write RTDTableData to S3 as partitioned Parquet.

        Args:
            table_data: RTDTableData instance
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix (e.g., "rtd_tables/2025_10_24/")
            partition_cols: Columns to partition by
            aws_region: AWS region

        Returns:
            S3 location (s3://bucket/prefix/table_name/)
        """
        # Convert to Arrow table
        arrow_table = RTDToIcebergConverter.convert_table_to_arrow(table_data)

        # Build S3 path
        table_name = table_data.metadata.glue_table_name
        s3_path = f"s3://{s3_bucket}/{s3_prefix.rstrip('/')}/{table_name}/"

        # Write to S3 with partitioning
        pq.write_to_dataset(
            arrow_table,
            root_path=s3_path,
            partition_cols=partition_cols,
            filesystem=pa.fs.S3FileSystem(region=aws_region),
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        )

        print(f"Wrote Parquet data to S3: {s3_path}")
        return s3_path

    @staticmethod
    def create_glue_database(
        database_name: str,
        description: str = "RTD tables from Insbridge SRP files",
        aws_region: str = "us-east-2",
    ) -> bool:
        """
        Create AWS Glue database if it doesn't exist.

        Args:
            database_name: Glue database name
            description: Database description
            aws_region: AWS region

        Returns:
            True if created or already exists, False on error
        """
        glue_client = boto3.client("glue", region_name=aws_region)

        try:
            # Check if database exists
            glue_client.get_database(Name=database_name)
            print(f"Glue database '{database_name}' already exists")
            return True
        except glue_client.exceptions.EntityNotFoundException:
            # Create database
            try:
                glue_client.create_database(
                    DatabaseInput={
                        "Name": database_name,
                        "Description": description,
                    }
                )
                print(f"Created Glue database: {database_name}")
                return True
            except ClientError as e:
                print(f"Error creating Glue database: {e}")
                return False
        except ClientError as e:
            print(f"Error checking Glue database: {e}")
            return False

    @staticmethod
    def register_table_in_glue(
        glue_schema: GlueTableSchema,
        aws_region: str = "us-east-2",
    ) -> bool:
        """
        Register table in AWS Glue Data Catalog.

        Args:
            glue_schema: GlueTableSchema with table definition
            aws_region: AWS region

        Returns:
            True if successful, False otherwise
        """
        glue_client = boto3.client("glue", region_name=aws_region)

        # Ensure database exists
        RTDToIcebergConverter.create_glue_database(
            glue_schema.database_name,
            aws_region=aws_region,
        )

        # Build table input
        table_input = {
            "Name": glue_schema.table_name,
            "Description": glue_schema.description or "",
            "StorageDescriptor": {
                "Columns": glue_schema.columns,
                "Location": glue_schema.location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
                "StoredAsSubDirectories": False,
            },
            "PartitionKeys": glue_schema.partition_keys,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "table_type": glue_schema.table_type,
                "storage_format": glue_schema.storage_format,
            },
        }

        try:
            # Check if table exists
            try:
                glue_client.get_table(
                    DatabaseName=glue_schema.database_name,
                    Name=glue_schema.table_name,
                )
                # Update existing table
                glue_client.update_table(
                    DatabaseName=glue_schema.database_name,
                    TableInput=table_input,
                )
                print(f"Updated Glue table: {glue_schema.database_name}.{glue_schema.table_name}")
            except glue_client.exceptions.EntityNotFoundException:
                # Create new table
                glue_client.create_table(
                    DatabaseName=glue_schema.database_name,
                    TableInput=table_input,
                )
                print(f"Created Glue table: {glue_schema.database_name}.{glue_schema.table_name}")

            return True

        except ClientError as e:
            print(f"Error registering table in Glue: {e}")
            return False

    @staticmethod
    def process_table_to_glue(
        table_data: RTDTableData,
        s3_bucket: str,
        s3_prefix: str,
        aws_region: str = "us-east-2",
        write_local: bool = False,
        local_output_dir: Optional[Path] = None,
    ) -> dict[str, Any]:
        """
        Complete pipeline: Convert RTD table to Parquet and register in Glue.

        Args:
            table_data: RTDTableData instance
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix (e.g., "rtd_tables/2025_10_24/")
            aws_region: AWS region
            write_local: Whether to also write Parquet locally
            local_output_dir: Local directory for Parquet files (if write_local=True)

        Returns:
            Dictionary with processing results
        """
        result = {
            "table_name": table_data.metadata.table_name,
            "glue_table_name": table_data.metadata.glue_table_name,
            "row_count": len(table_data.rows),
            "success": False,
            "s3_location": None,
            "local_path": None,
            "glue_registered": False,
        }

        try:
            # Write to S3
            s3_location = RTDToIcebergConverter.write_parquet_to_s3(
                table_data=table_data,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                aws_region=aws_region,
            )
            result["s3_location"] = s3_location

            # Write locally if requested
            if write_local and local_output_dir:
                local_path = RTDToIcebergConverter.write_parquet_local(
                    table_data=table_data,
                    output_dir=local_output_dir,
                )
                result["local_path"] = str(local_path)

            # Generate Glue schema
            from kiro_insbridge.enterprise_rating.repository.rtd_repository import RTDRepository
            glue_schema = RTDRepository.generate_glue_schema(table_data.metadata)

            # Register in Glue
            glue_success = RTDToIcebergConverter.register_table_in_glue(
                glue_schema=glue_schema,
                aws_region=aws_region,
            )
            result["glue_registered"] = glue_success
            result["success"] = glue_success

            return result

        except Exception as e:
            result["error"] = str(e)
            print(f"Error processing table {table_data.metadata.table_name}: {e}")
            return result
