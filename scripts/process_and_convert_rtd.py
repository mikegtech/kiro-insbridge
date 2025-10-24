#!/usr/bin/env python3
"""
Complete RTD processing pipeline: Extract → Parse → Convert to Iceberg → Register in Glue.

Usage:
    python scripts/process_and_convert_rtd.py --srp-dir /path/to/1_118_0_891_1.0000 --s3-bucket my-bucket
    python scripts/process_and_convert_rtd.py --srp-dir /path/to/1_118_0_891_1.0000 --local-only
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from kiro_insbridge.enterprise_rating.repository.rtd_repository import RTDRepository
from kiro_insbridge.enterprise_rating.converters.rtd_to_iceberg import RTDToIcebergConverter


def format_iso_date(dt: datetime) -> str:
    """Format datetime to ISO 8601 date string."""
    return dt.strftime("%Y_%m_%d")


def process_and_convert_srp(
    srp_dir: Path,
    s3_bucket: str,
    s3_prefix: str,
    output_dir: Path,
    aws_region: str = "us-east-2",
    local_only: bool = False,
) -> dict:
    """
    Complete pipeline for a single SRP directory.

    Args:
        srp_dir: Path to extracted SRP directory
        s3_bucket: S3 bucket name (ignored if local_only=True)
        s3_prefix: S3 prefix
        output_dir: Local output directory
        aws_region: AWS region
        local_only: If True, only write Parquet locally (no S3/Glue)

    Returns:
        Dictionary with processing results
    """
    print(f"\n{'='*80}")
    print(f"RTD Processing Pipeline")
    print(f"{'='*80}\n")
    print(f"SRP Directory: {srp_dir}")
    print(f"Local Output: {output_dir}")
    if not local_only:
        print(f"S3 Bucket: {s3_bucket}")
        print(f"S3 Prefix: {s3_prefix}")
    print(f"Mode: {'LOCAL ONLY' if local_only else 'S3 + Glue'}\n")

    # Extract program context
    program_context = RTDRepository.extract_program_context_from_path(srp_dir)
    print(f"Program Context:")
    print(f"  Carrier: {program_context['carrier_id']}")
    print(f"  Line: {program_context['line_of_business']}")
    print(f"  Program: {program_context['program_id']}")
    print(f"  Version: {program_context['program_version']}\n")

    # Generate Glue database name
    date_str = format_iso_date(datetime.now())
    glue_database_name = f"Insbridge_PL_Release_{date_str}"

    try:
        # Step 1: Parse RTD tables
        print(f"{'='*80}")
        print("STEP 1: Parsing RTD Tables")
        print(f"{'='*80}\n")

        all_table_data = RTDRepository.process_srp_rtd_tables(
            srp_extracted_dir=srp_dir,
            qualifiers_by_table=None,  # Using Q0, Q1, Q2... defaults
            glue_database_name=glue_database_name,
            glue_s3_base=f"s3://{s3_bucket}/{s3_prefix}",
        )

        print(f"\nParsed {len(all_table_data)} tables")
        total_rows = sum(len(table.rows) for table in all_table_data)
        print(f"Total rows: {total_rows:,}\n")

        # Step 2: Convert to Parquet and register in Glue
        print(f"{'='*80}")
        print("STEP 2: Converting to Iceberg Parquet")
        print(f"{'='*80}\n")

        output_dir.mkdir(parents=True, exist_ok=True)
        conversion_results = []

        for idx, table_data in enumerate(all_table_data):
            print(f"[{idx+1}/{len(all_table_data)}] {table_data.metadata.table_name}")
            print(f"  Rows: {len(table_data.rows):,}")

            if local_only:
                # Local-only mode
                local_path = RTDToIcebergConverter.write_parquet_local(
                    table_data=table_data,
                    output_dir=output_dir,
                )
                result = {
                    "table_name": table_data.metadata.table_name,
                    "glue_table_name": table_data.metadata.glue_table_name,
                    "row_count": len(table_data.rows),
                    "local_path": str(local_path),
                    "success": True,
                }
            else:
                # Full pipeline: S3 + Glue
                result = RTDToIcebergConverter.process_table_to_glue(
                    table_data=table_data,
                    s3_bucket=s3_bucket,
                    s3_prefix=s3_prefix,
                    aws_region=aws_region,
                    write_local=True,
                    local_output_dir=output_dir,
                )

            conversion_results.append(result)

            if result.get("success"):
                print(f"  ✓ Success")
                if result.get("s3_location"):
                    print(f"    S3: {result['s3_location']}")
                if result.get("local_path"):
                    print(f"    Local: {result['local_path']}")
                if result.get("glue_registered"):
                    print(f"    Glue: Registered in {glue_database_name}")
            else:
                print(f"  ✗ Failed: {result.get('error', 'Unknown error')}")

            print()

        # Step 3: Summary
        print(f"{'='*80}")
        print("PROCESSING SUMMARY")
        print(f"{'='*80}\n")

        successful = sum(1 for r in conversion_results if r.get("success"))
        failed = len(conversion_results) - successful

        print(f"Total Tables: {len(conversion_results)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")

        if not local_only:
            glue_registered = sum(1 for r in conversion_results if r.get("glue_registered"))
            print(f"  Glue Registered: {glue_registered}")

        # Save summary
        summary = {
            "srp_directory": str(srp_dir),
            "program_context": program_context,
            "glue_database_name": glue_database_name,
            "processed_at": datetime.now().isoformat(),
            "total_tables": len(conversion_results),
            "total_rows": total_rows,
            "successful": successful,
            "failed": failed,
            "mode": "local_only" if local_only else "s3_glue",
            "conversion_results": conversion_results,
        }

        summary_file = output_dir / "conversion_summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"\nSummary saved to: {summary_file}")
        print(f"{'='*80}\n")

        return summary

    except Exception as e:
        print(f"\nERROR: Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Process RTD tables and convert to Iceberg Parquet with Glue registration"
    )
    parser.add_argument(
        "--srp-dir",
        type=Path,
        required=True,
        help="Path to extracted SRP directory (e.g., /path/to/1_118_0_891_1.0000/)",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default="local-packages-bucket",
        help="S3 bucket name (default: local-packages-bucket)",
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        help="S3 prefix (default: rtd_tables/YYYY_MM_DD/)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/rtd_parquet_output"),
        help="Local output directory for Parquet files (default: data/rtd_parquet_output)",
    )
    parser.add_argument(
        "--aws-region",
        type=str,
        default="us-east-2",
        help="AWS region (default: us-east-2)",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Only write Parquet files locally, skip S3 upload and Glue registration",
    )

    args = parser.parse_args()

    if not args.srp_dir.exists():
        print(f"ERROR: SRP directory does not exist: {args.srp_dir}")
        sys.exit(1)

    # Generate default S3 prefix if not provided
    if not args.s3_prefix:
        date_str = format_iso_date(datetime.now())
        args.s3_prefix = f"rtd_tables/{date_str}/"

    # Process
    process_and_convert_srp(
        srp_dir=args.srp_dir,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        output_dir=args.output_dir,
        aws_region=args.aws_region,
        local_only=args.local_only,
    )


if __name__ == "__main__":
    main()
