#!/usr/bin/env python3
"""
Script to process RTD tables from extracted SRP files.

This script:
1. Finds extracted SRP directories
2. Parses rt_summary.xml and rtd files
3. Extracts table metadata and rows
4. Generates AWS Glue schema
5. Prepares data for Iceberg Parquet conversion

Usage:
    python scripts/process_rtd_tables.py --srp-dir /path/to/extracted/1_118_0_611_296.0000
    python scripts/process_rtd_tables.py --staging-dir /path/to/staging
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from kiro_insbridge.enterprise_rating.repository.rtd_repository import RTDRepository


def format_iso_date(dt: datetime) -> str:
    """Format datetime to ISO 8601 date string."""
    return dt.strftime("%Y_%m_%d")


def process_single_srp(srp_dir: Path, output_dir: Path) -> dict:
    """
    Process a single extracted SRP directory.

    Args:
        srp_dir: Path to extracted SRP directory
        output_dir: Path to output directory for results

    Returns:
        Dictionary with processing results
    """
    print(f"\n{'='*80}")
    print(f"Processing SRP: {srp_dir.name}")
    print(f"{'='*80}\n")

    # Extract program context
    program_context = RTDRepository.extract_program_context_from_path(srp_dir)
    print(f"Program Context:")
    print(f"  Carrier ID: {program_context['carrier_id']}")
    print(f"  Line of Business: {program_context['line_of_business']}")
    print(f"  Program ID: {program_context['program_id']}")
    print(f"  Version: {program_context['program_version']}\n")

    # Generate Glue database name with current date
    date_str = format_iso_date(datetime.now())
    glue_database_name = f"Insbridge_PL_Release_{date_str}"
    glue_s3_base = f"s3://local-packages-bucket/rtd_tables/{date_str}/"

    print(f"Glue Configuration:")
    print(f"  Database: {glue_database_name}")
    print(f"  S3 Base: {glue_s3_base}\n")

    try:
        # Process all RTD tables (without qualifiers - use defaults)
        all_table_data = RTDRepository.process_srp_rtd_tables(
            srp_extracted_dir=srp_dir,
            qualifiers_by_table=None,  # No qualifiers yet - will use Q0, Q1, Q2, etc.
            glue_database_name=glue_database_name,
            glue_s3_base=glue_s3_base,
        )

        print(f"\n{'='*80}")
        print(f"Processing Summary")
        print(f"{'='*80}")
        print(f"Total tables processed: {len(all_table_data)}")

        # Calculate total rows
        total_rows = sum(len(table.rows) for table in all_table_data)
        print(f"Total rows across all tables: {total_rows:,}")

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save summary
        summary = {
            "srp_directory": str(srp_dir),
            "program_context": program_context,
            "glue_database_name": glue_database_name,
            "glue_s3_base": glue_s3_base,
            "processed_at": datetime.now().isoformat(),
            "total_tables": len(all_table_data),
            "total_rows": total_rows,
            "tables": [],
        }

        # Save individual table metadata and generate Glue schemas
        for table_data in all_table_data:
            metadata = table_data.metadata

            table_info = {
                "table_index": metadata.table_index,
                "table_name": metadata.table_name,
                "glue_table_name": metadata.glue_table_name,
                "row_count": metadata.row_count,
                "column_count": len(metadata.columns),
                "dlm_hash": metadata.dlm_hash,
                "has_continuation_files": metadata.has_continuation_files,
                "continuation_file_count": metadata.continuation_file_count,
            }
            summary["tables"].append(table_info)

            # Generate Glue schema
            glue_schema = RTDRepository.generate_glue_schema(metadata)

            # Save Glue schema as JSON
            schema_file = output_dir / f"glue_schema_{metadata.glue_table_name}.json"
            with open(schema_file, "w") as f:
                json.dump(glue_schema.model_dump(), f, indent=2)

            # Save sample rows (first 10)
            sample_rows = []
            for row in table_data.rows[:10]:
                sample_rows.append({
                    "row_id": row.row_id,
                    "values_raw": row.values_raw,
                    "values_split": row.values_split,
                    "dlm_timestamp": row.dlm_timestamp,
                    "qualifier_data": row.qualifier_data,
                })

            sample_file = output_dir / f"sample_rows_{metadata.glue_table_name}.json"
            with open(sample_file, "w") as f:
                json.dump(sample_rows, f, indent=2)

            print(f"\n  [{metadata.table_index}] {metadata.table_name}")
            print(f"      Glue Table: {metadata.glue_table_name}")
            print(f"      Rows: {metadata.row_count:,}")
            print(f"      Columns: {len(metadata.columns)}")
            print(f"      Column Names: {', '.join([col.column_name for col in metadata.columns[:5]])}{'...' if len(metadata.columns) > 5 else ''}")
            print(f"      DLM Hash: {metadata.dlm_hash}")
            print(f"      Continuation Files: {metadata.continuation_file_count}")

        # Save summary
        summary_file = output_dir / "processing_summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"\n{'='*80}")
        print(f"Output saved to: {output_dir}")
        print(f"  Summary: {summary_file}")
        print(f"  Glue Schemas: {len(all_table_data)} files")
        print(f"  Sample Rows: {len(all_table_data)} files")
        print(f"{'='*80}\n")

        return summary

    except Exception as e:
        print(f"ERROR: Failed to process {srp_dir}: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


def process_staging_directory(staging_dir: Path, output_base: Path):
    """
    Process all extracted SRP directories in staging.

    Args:
        staging_dir: Path to staging directory containing extracted SRPs
        output_base: Base path for output files
    """
    print(f"Scanning staging directory: {staging_dir}")

    # Find all extracted SRP directories
    # Pattern: extracted_*_*_*_*_*.0000/
    srp_dirs = []
    for item in staging_dir.iterdir():
        if item.is_dir() and item.name.startswith("extracted_"):
            # Check if it contains rtd directory
            rtd_dir = None
            # Look for subdirectory with rtd/
            for subdir in item.iterdir():
                if subdir.is_dir():
                    rtd_check = subdir / "rtd"
                    if rtd_check.exists():
                        srp_dirs.append(subdir)
                        break

    print(f"Found {len(srp_dirs)} extracted SRP directories\n")

    if not srp_dirs:
        print("No SRP directories found. Expected pattern: staging/extracted_*/[program_dir]/rtd/")
        return

    # Process each SRP
    results = []
    for srp_dir in srp_dirs:
        output_dir = output_base / f"processed_{srp_dir.name}"
        result = process_single_srp(srp_dir, output_dir)
        results.append(result)

    # Save overall summary
    overall_summary = {
        "processed_at": datetime.now().isoformat(),
        "staging_directory": str(staging_dir),
        "total_srps_processed": len(results),
        "total_tables": sum(r.get("total_tables", 0) for r in results),
        "total_rows": sum(r.get("total_rows", 0) for r in results),
        "srps": results,
    }

    overall_file = output_base / "overall_summary.json"
    with open(overall_file, "w") as f:
        json.dump(overall_summary, f, indent=2)

    print(f"\n{'='*80}")
    print(f"OVERALL SUMMARY")
    print(f"{'='*80}")
    print(f"Total SRPs processed: {len(results)}")
    print(f"Total tables: {overall_summary['total_tables']}")
    print(f"Total rows: {overall_summary['total_rows']:,}")
    print(f"\nOverall summary saved to: {overall_file}")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Process RTD tables from extracted SRP files"
    )
    parser.add_argument(
        "--srp-dir",
        type=Path,
        help="Path to single extracted SRP directory (e.g., /path/to/1_118_0_611_296.0000/)",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        help="Path to staging directory containing multiple extracted SRPs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/rtd_processing_output"),
        help="Output directory for processing results (default: data/rtd_processing_output)",
    )

    args = parser.parse_args()

    if not args.srp_dir and not args.staging_dir:
        parser.error("Must provide either --srp-dir or --staging-dir")

    if args.srp_dir and args.staging_dir:
        parser.error("Cannot provide both --srp-dir and --staging-dir")

    # Process
    if args.srp_dir:
        if not args.srp_dir.exists():
            print(f"ERROR: SRP directory does not exist: {args.srp_dir}")
            sys.exit(1)

        output_dir = args.output_dir / f"processed_{args.srp_dir.name}"
        process_single_srp(args.srp_dir, output_dir)

    elif args.staging_dir:
        if not args.staging_dir.exists():
            print(f"ERROR: Staging directory does not exist: {args.staging_dir}")
            sys.exit(1)

        process_staging_directory(args.staging_dir, args.output_dir)


if __name__ == "__main__":
    main()
