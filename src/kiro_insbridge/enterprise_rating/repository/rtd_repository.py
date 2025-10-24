"""
RTD (Rating Table Data) Repository for parsing and converting rating tables.

This repository handles:
- Parsing rt_summary.xml to map rtd.dt<number> to table names
- Parsing individual rtd.dt files to extract table data
- Merging continuation files (rtd.dt37.1, rtd.dt37.2, etc.)
- Extracting dlm hash for change tracking
- Integrating with Qualifier entity for column metadata
- Converting to Iceberg Parquet format
- Registering tables in AWS Glue catalog
"""

import re
from pathlib import Path
from typing import Any, Optional
from datetime import datetime

import xmltodict

from kiro_insbridge.enterprise_rating.entities.rtd_table import (
    RTDColumnMetadata,
    RTDTableMetadata,
    RTDRow,
    RTDTableData,
    GlueTableSchema,
)
from kiro_insbridge.enterprise_rating.entities.qualifier import Qualifier


class RTDRepository:
    """Repository for parsing and managing RTD (Rating Table Data) files."""

    @staticmethod
    def parse_rt_summary(rt_summary_path: Path) -> dict[int, dict[str, Any]]:
        """
        Parse rt_summary.xml to extract table index-to-name mappings.

        Args:
            rt_summary_path: Path to rt_summary.xml file

        Returns:
            Dictionary mapping table index (int) to table metadata:
            {
                0: {"name": "rtd.dt0", "desc": "FRCTerritoryGroup_LT", "id": "...", "id_key": "..."},
                1: {"name": "rtd.dt1", "desc": "FRStateInd_GT", "id": "...", "id_key": "..."},
                ...
            }
        """
        with open(rt_summary_path, "r", encoding="utf-8") as f:
            doc = xmltodict.parse(f.read())

        # Extract rtd info
        rt_info = doc.get("rt_info", {})
        rtd_info = rt_info.get("rtd", {})
        files = rtd_info.get("f", [])

        # Ensure files is a list
        if not isinstance(files, list):
            files = [files] if files else []

        # Build mapping
        table_mapping = {}
        for file_entry in files:
            name = file_entry.get("@name", "")
            desc = file_entry.get("@desc", "")
            file_id = file_entry.get("@id", "")
            id_key = file_entry.get("@id_key", "")

            # Extract table index from name (e.g., "rtd.dt0" -> 0)
            match = re.match(r"rtd\.dt(\d+)", name)
            if match:
                table_index = int(match.group(1))
                table_mapping[table_index] = {
                    "name": name,
                    "desc": desc,
                    "id": file_id,
                    "id_key": id_key,
                }

        return table_mapping

    @staticmethod
    def parse_rtd_file(rtd_file_path: Path) -> dict[str, Any]:
        """
        Parse a single rtd.dt<number> file to extract table data.

        Args:
            rtd_file_path: Path to rtd.dt file (e.g., rtd.dt0, rtd.dt37)

        Returns:
            Dictionary containing:
            {
                "header": {
                    "prod": "118",
                    "table_prod": "118",
                    "p": "611",
                    "i": "1346",
                    "v": "6.0000",
                    "dlm_table": "2022-09-20 14:05:00.000",
                    "dlm_table_data": "2022-09-20 14:06:00.000",
                    "dlm": "44824.58750",
                    "table_count": "269691",
                    ...
                },
                "rows": [
                    {
                        "i": "10000",  # row ID
                        "v": "1~1~1~1~1~1~1~1~1~1~13~1~1",  # tilde-delimited values
                        "d": "44824.58681",  # dlm timestamp
                        "q": "AWE-1643||"  # pipe-delimited qualifier
                    },
                    ...
                ]
            }
        """
        with open(rtd_file_path, "r", encoding="utf-8") as f:
            doc = xmltodict.parse(f.read())

        # Extract header from <lkupvars><l ... />
        lkupvars = doc.get("lkupvars", {})
        header_elem = lkupvars.get("l", {})

        # Convert XML attributes (prefixed with @) to plain dict
        header = {}
        for key, value in header_elem.items():
            clean_key = key.lstrip("@")
            header[clean_key] = value

        # Extract rows from <r ... /> elements
        rows_elem = lkupvars.get("r", [])

        # Ensure rows is a list
        if not isinstance(rows_elem, list):
            rows_elem = [rows_elem] if rows_elem else []

        # Parse rows
        rows = []
        for row_elem in rows_elem:
            row_data = {}
            for key, value in row_elem.items():
                clean_key = key.lstrip("@")
                row_data[clean_key] = value
            rows.append(row_data)

        return {"header": header, "rows": rows}

    @staticmethod
    def find_continuation_files(
        rtd_dir: Path, table_index: int
    ) -> list[Path]:
        """
        Find all continuation files for a given table index.

        Args:
            rtd_dir: Directory containing rtd files
            table_index: Table index (e.g., 37 for rtd.dt37)

        Returns:
            List of continuation file paths sorted by split number
            (e.g., [rtd.dt37.1, rtd.dt37.2, ...])
        """
        pattern = f"rtd.dt{table_index}.*"
        continuation_files = sorted(rtd_dir.glob(pattern))

        # Filter out the base file itself (rtd.dt37)
        base_file = rtd_dir / f"rtd.dt{table_index}"
        continuation_files = [f for f in continuation_files if f != base_file]

        return continuation_files

    @staticmethod
    def merge_continuation_files(
        base_data: dict[str, Any],
        continuation_files: list[Path]
    ) -> dict[str, Any]:
        """
        Merge continuation files into the base table data.

        Args:
            base_data: Parsed data from base rtd.dt file
            continuation_files: List of continuation file paths

        Returns:
            Merged data with all rows combined
        """
        merged_data = base_data.copy()
        merged_rows = merged_data["rows"].copy()

        for cont_file in continuation_files:
            cont_data = RTDRepository.parse_rtd_file(cont_file)
            merged_rows.extend(cont_data["rows"])

        merged_data["rows"] = merged_rows
        return merged_data

    @staticmethod
    def extract_program_context_from_path(srp_path: Path) -> dict[str, str]:
        """
        Extract program context from SRP path or directory name.

        Args:
            srp_path: Path to extracted SRP directory or .srp file
                     (e.g., "/path/to/1_118_0_611_296.0000/")

        Returns:
            Dictionary with program context:
            {
                "carrier_id": "1",
                "line_of_business": "118",
                "program_id": "611",
                "program_version": "296.0000"
            }
        """
        # Extract from directory name pattern: {carrier}_{line}_{?}_{program_id}_{version}
        # Example: 1_118_0_611_296.0000
        dir_name = srp_path.name if srp_path.is_dir() else srp_path.stem

        match = re.match(r"(\d+)_(\d+)_(\d+)_(\d+)_([\d.]+)", dir_name)
        if not match:
            # Fallback to empty values
            return {
                "carrier_id": "",
                "line_of_business": "",
                "program_id": "",
                "program_version": "",
            }

        return {
            "carrier_id": match.group(1),
            "line_of_business": match.group(2),
            "program_id": match.group(4),
            "program_version": match.group(5),
        }

    @staticmethod
    def normalize_table_name_for_glue(desc: str) -> str:
        """
        Normalize table description to AWS Glue-compatible table name.

        Glue requirements:
        - Lowercase
        - Alphanumeric and underscores only
        - Must start with letter or underscore

        Args:
            desc: Table description from rt_summary.xml (e.g., "FRCTerritoryGroup_LT")

        Returns:
            Glue-compatible table name (e.g., "frc_territory_group_lt")
        """
        # Convert to lowercase
        name = desc.lower()

        # Replace non-alphanumeric characters with underscores
        name = re.sub(r"[^a-z0-9_]", "_", name)

        # Ensure it starts with letter or underscore
        if name and name[0].isdigit():
            name = f"table_{name}"

        # Remove consecutive underscores
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores
        name = name.strip("_")

        return name

    @staticmethod
    def build_table_metadata(
        table_index: int,
        table_info: dict[str, Any],
        parsed_data: dict[str, Any],
        program_context: dict[str, str],
        qualifiers: Optional[list[Qualifier]],
        glue_database_name: str,
        glue_s3_location: str,
        continuation_file_count: int = 0,
    ) -> RTDTableMetadata:
        """
        Build RTDTableMetadata from parsed components.

        Args:
            table_index: Table index number
            table_info: Table info from rt_summary.xml
            parsed_data: Parsed RTD file data (header + rows)
            program_context: Program/version context
            qualifiers: List of Qualifier entities for column metadata (optional)
            glue_database_name: AWS Glue database name
            glue_s3_location: S3 location for table data
            continuation_file_count: Number of continuation files

        Returns:
            RTDTableMetadata instance
        """
        header = parsed_data["header"]
        table_name = table_info["desc"]
        glue_table_name = RTDRepository.normalize_table_name_for_glue(table_name)

        # Determine number of columns from first row if no qualifiers provided
        num_columns = 0
        if qualifiers:
            num_columns = len(qualifiers)
        elif parsed_data["rows"]:
            # Get column count from first row's tilde-delimited values
            first_row = parsed_data["rows"][0]
            values = first_row.get("v", "")
            num_columns = len(values.split("~")) if values else 0

        # Build column metadata from qualifiers or defaults
        columns = []
        for idx in range(num_columns):
            # Try to get qualifier metadata if available
            if qualifiers and idx < len(qualifiers):
                qualifier = qualifiers[idx]
                col_meta = RTDColumnMetadata(
                    column_index=idx,
                    column_name=qualifier.c if hasattr(qualifier, "c") else f"Q{idx}",
                    data_type="string",  # Default, will be refined based on qualifier.t
                    glue_type="STRING",
                    description=qualifier.m if hasattr(qualifier, "m") else None,
                    qualifier_index=qualifier.i if hasattr(qualifier, "i") else None,
                    is_nullable=True,
                )
            else:
                # Default fallback when qualifier not found
                col_meta = RTDColumnMetadata(
                    column_index=idx,
                    column_name=f"Q{idx}",
                    data_type="string",
                    glue_type="STRING",
                    description=None,
                    qualifier_index=None,
                    is_nullable=True,
                )
            columns.append(col_meta)

        metadata = RTDTableMetadata(
            table_index=table_index,
            table_name=table_name,
            glue_table_name=glue_table_name,
            program_id=program_context["program_id"],
            program_version=program_context["program_version"],
            line_of_business=program_context["line_of_business"],
            carrier_id=program_context["carrier_id"],
            table_prod=header.get("table_prod"),
            dlm_hash=header.get("dlm", ""),
            dlm_table_timestamp=header.get("dlm_table"),
            dlm_table_data_timestamp=header.get("dlm_table_data"),
            row_count=len(parsed_data["rows"]),
            columns=columns,
            has_continuation_files=continuation_file_count > 0,
            continuation_file_count=continuation_file_count,
            glue_database_name=glue_database_name,
            glue_s3_location=glue_s3_location,
            extracted_at=datetime.now(),
        )

        return metadata

    @staticmethod
    def build_table_data(
        metadata: RTDTableMetadata,
        parsed_data: dict[str, Any],
    ) -> RTDTableData:
        """
        Build RTDTableData with metadata and rows.

        Args:
            metadata: RTDTableMetadata instance
            parsed_data: Parsed RTD file data (header + rows)

        Returns:
            RTDTableData instance ready for Iceberg conversion
        """
        rows = []
        for row_dict in parsed_data["rows"]:
            row_id = row_dict.get("i", "")
            values_raw = row_dict.get("v", "")
            values_split = values_raw.split("~") if values_raw else []
            dlm_timestamp = row_dict.get("d")
            qualifier_data = row_dict.get("q")

            row = RTDRow(
                row_id=row_id,
                values_raw=values_raw,
                values_split=values_split,
                dlm_timestamp=dlm_timestamp,
                qualifier_data=qualifier_data,
            )
            rows.append(row)

        table_data = RTDTableData(
            metadata=metadata,
            rows=rows,
            partition_program_id=metadata.program_id,
            partition_version=metadata.program_version,
        )

        return table_data

    @staticmethod
    def generate_glue_schema(metadata: RTDTableMetadata) -> GlueTableSchema:
        """
        Generate AWS Glue table schema from RTDTableMetadata.

        Args:
            metadata: RTDTableMetadata instance

        Returns:
            GlueTableSchema for AWS Glue catalog registration
        """
        # Build column definitions
        columns = []
        for col in metadata.columns:
            columns.append({
                "Name": col.column_name,
                "Type": col.glue_type,
                "Comment": col.description or "",
            })

        # Add system columns
        columns.extend([
            {"Name": "row_id", "Type": "STRING", "Comment": "Row identifier from RTD file"},
            {"Name": "dlm_timestamp", "Type": "STRING", "Comment": "Row last modified timestamp"},
            {"Name": "qualifier_data", "Type": "STRING", "Comment": "Pipe-delimited qualifier data"},
        ])

        # Define partition keys
        partition_keys = [
            {"Name": "program_id", "Type": "STRING", "Comment": "Program identifier"},
            {"Name": "version", "Type": "STRING", "Comment": "Program version"},
        ]

        schema = GlueTableSchema(
            database_name=metadata.glue_database_name,
            table_name=metadata.glue_table_name,
            columns=columns,
            partition_keys=partition_keys,
            location=metadata.glue_s3_location,
            table_type="ICEBERG",
            storage_format="PARQUET",
            description=f"RTD table: {metadata.table_name} (dlm: {metadata.dlm_hash})",
        )

        return schema

    @staticmethod
    def process_srp_rtd_tables(
        srp_extracted_dir: Path,
        qualifiers_by_table: Optional[dict[int, list[Qualifier]]],
        glue_database_name: str,
        glue_s3_base: str,
    ) -> list[RTDTableData]:
        """
        Process all RTD tables from an extracted SRP directory.

        Args:
            srp_extracted_dir: Path to extracted SRP directory
                              (e.g., /path/to/1_118_0_611_296.0000/)
            qualifiers_by_table: Mapping of table index to qualifier list
            glue_database_name: AWS Glue database name
            glue_s3_base: Base S3 location (e.g., s3://bucket/rtd_tables/)

        Returns:
            List of RTDTableData instances ready for Iceberg conversion
        """
        # Find rt_summary.xml and rtd directory
        rt_summary_path = srp_extracted_dir / "rt_summary.xml"
        rtd_dir = srp_extracted_dir / "rtd"

        if not rt_summary_path.exists():
            raise FileNotFoundError(f"rt_summary.xml not found in {srp_extracted_dir}")

        if not rtd_dir.exists() or not rtd_dir.is_dir():
            raise FileNotFoundError(f"rtd directory not found in {srp_extracted_dir}")

        # Parse rt_summary.xml
        table_mapping = RTDRepository.parse_rt_summary(rt_summary_path)

        # Extract program context
        program_context = RTDRepository.extract_program_context_from_path(srp_extracted_dir)

        # Process each table
        all_table_data = []
        for table_index, table_info in table_mapping.items():
            try:
                # Find base rtd file
                base_file = rtd_dir / f"rtd.dt{table_index}"
                if not base_file.exists():
                    print(f"Warning: Base file {base_file} not found, skipping")
                    continue

                # Parse base file
                parsed_data = RTDRepository.parse_rtd_file(base_file)

                # Find and merge continuation files
                continuation_files = RTDRepository.find_continuation_files(rtd_dir, table_index)
                if continuation_files:
                    parsed_data = RTDRepository.merge_continuation_files(
                        parsed_data, continuation_files
                    )

                # Get qualifiers for this table (use None if not provided)
                qualifiers = None
                if qualifiers_by_table:
                    qualifiers = qualifiers_by_table.get(table_index)

                # Build S3 location for this table
                glue_s3_location = f"{glue_s3_base}/{table_info['desc']}/"

                # Build metadata
                metadata = RTDRepository.build_table_metadata(
                    table_index=table_index,
                    table_info=table_info,
                    parsed_data=parsed_data,
                    program_context=program_context,
                    qualifiers=qualifiers,
                    glue_database_name=glue_database_name,
                    glue_s3_location=glue_s3_location,
                    continuation_file_count=len(continuation_files),
                )

                # Build table data
                table_data = RTDRepository.build_table_data(metadata, parsed_data)

                all_table_data.append(table_data)

                print(f"Processed table {table_index}: {table_info['desc']} ({len(table_data.rows)} rows)")

            except Exception as e:
                print(f"Error processing table {table_index}: {e}")
                continue

        return all_table_data
