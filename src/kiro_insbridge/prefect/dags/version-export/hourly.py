"""Prefect flow for processing SRP program versions from .srtp files."""

import json
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import boto3
import xmltodict
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

try:
    import pyzipper

    _HAS_PYZIPPER = True
except ImportError:
    _HAS_PYZIPPER = False

from kiro_insbridge.enterprise_rating.config import get_config
from kiro_insbridge.enterprise_rating.repository.program_version_repository import (
    ProgramVersionRepository,
)


@task
def find_unprocessed_srp_files(inbox_dir: Path) -> list[Path]:
    """Find all .srp files that haven't been processed for version export yet.

    A .srp file is considered processed if a corresponding .srp.version_processed
    marker file exists in the same directory.

    Args:
        inbox_dir: Directory containing .srp files to process

    Returns:
        List of unprocessed .srp file paths
    """
    srp_files = []

    if not inbox_dir.exists():
        print(f"Inbox directory does not exist: {inbox_dir}")
        return srp_files

    # Find all .srp files
    for srp_file in inbox_dir.glob("*.srp"):
        # Skip release zips (they contain nested .srp files)
        if "_REL_" in srp_file.name:
            continue

        marker_file = srp_file.with_suffix(".srp.version_processed")
        if not marker_file.exists():
            srp_files.append(srp_file)

    print(f"Found {len(srp_files)} unprocessed .srp file(s)")
    return srp_files


@task
def extract_srp_to_temp(srp_file: Path, password: str) -> Path:
    """Extract .srp (password-protected zip) to a thread-specific temp directory.

    Args:
        srp_file: Path to .srp file
        password: Password for extraction

    Returns:
        Path to temporary extraction directory
    """
    temp_dir = Path(tempfile.mkdtemp(prefix=f"srp_{srp_file.stem}_"))
    pwd = password.encode("utf-8")

    try:
        if _HAS_PYZIPPER:
            with pyzipper.AESZipFile(str(srp_file), "r") as zf:
                zf.setpassword(pwd)
                zf.extractall(str(temp_dir))
        else:
            with zipfile.ZipFile(str(srp_file), "r") as zf:
                zf.setpassword(pwd)
                zf.extractall(str(temp_dir))

        print(f"Extracted {srp_file.name} to {temp_dir}")
        return temp_dir

    except Exception as e:
        # Clean up temp dir on failure
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"Failed to extract {srp_file}: {e}") from e


@task
def find_rte_xml_file(temp_dir: Path) -> Path | None:
    """Find the RTE XML file in the extracted directory.

    The RTE XML file is the one that's not header.xml, srp.xml, or rt_summary.xml.
    It's usually named like AEBB715809.xml (the program key).

    Args:
        temp_dir: Directory containing extracted files

    Returns:
        Path to RTE XML file, or None if not found
    """
    # Skip known non-RTE XML files
    skip_files = {"header.xml", "srp.xml", "rt_summary.xml"}

    # Look for XML files in the root of the extraction
    for xml_file in temp_dir.glob("*.xml"):
        if xml_file.name not in skip_files:
            print(f"Found RTE XML file: {xml_file.name}")
            return xml_file

    # If not found in root, check subdirectories (some SRP structures vary)
    for xml_file in temp_dir.rglob("*.xml"):
        if xml_file.name not in skip_files:
            print(f"Found RTE XML file: {xml_file}")
            return xml_file

    print(f"No RTE XML file found in {temp_dir}")
    return None


@task
def find_rtd_files(temp_dir: Path) -> list[Path]:
    """Find all rtd (rating table data) files in the extracted directory.

    RTD files are usually in rtd/ subdirectory and named like rtd.dt0, rtd.dt1, etc.

    Args:
        temp_dir: Directory containing extracted files

    Returns:
        List of rtd file paths
    """
    rtd_files = []

    # Look for rtd directory
    for rtd_dir in temp_dir.rglob("rtd"):
        if rtd_dir.is_dir():
            # Find all rtd.dt* files
            for rtd_file in sorted(rtd_dir.glob("rtd.dt*")):
                rtd_files.append(rtd_file)

    if rtd_files:
        print(f"Found {len(rtd_files)} rtd file(s): {[f.name for f in rtd_files]}")
    else:
        print(f"No rtd files found in {temp_dir}")

    return rtd_files


@task
def parse_rtd_files(rtd_files: list[Path]) -> list[dict[str, Any]]:
    """Parse rtd (rating table data) XML files.

    Args:
        rtd_files: List of rtd file paths

    Returns:
        List of parsed rtd data dictionaries
    """
    parsed_tables = []

    for rtd_file in rtd_files:
        try:
            with open(rtd_file, 'r', encoding='utf-8') as f:
                doc = xmltodict.parse(f.read())

            # Extract table metadata and rows
            lkupvars = doc.get('lkupvars', {})
            table_meta = lkupvars.get('l', {})
            rows = lkupvars.get('r', [])

            # Ensure rows is a list
            if not isinstance(rows, list):
                rows = [rows] if rows else []

            parsed_table = {
                'filename': rtd_file.name,
                'metadata': table_meta,
                'row_count': len(rows),
                'rows': rows
            }

            parsed_tables.append(parsed_table)
            print(f"Parsed {rtd_file.name}: {len(rows)} rows")

        except Exception as e:
            print(f"Error parsing {rtd_file.name}: {e}")
            continue

    return parsed_tables


@task
def process_program_version(rte_xml_path: Path) -> dict[str, Any] | None:
    """Process the program version using ProgramVersionRepository.

    Args:
        rte_xml_path: Path to the RTE XML file

    Returns:
        Program version data as dict, or None if processing failed
    """
    if not rte_xml_path.exists():
        print(f"RTE XML file not found: {rte_xml_path}")
        return None

    try:
        program_version = ProgramVersionRepository.get_program_version_from_path(
            rte_xml_path
        )

        if program_version is None:
            print(f"Failed to parse program version from {rte_xml_path}")
            return None

        # Convert to dict for downstream processing
        version_dict = program_version.model_dump(mode="json")
        print(f"Successfully processed program version: {version_dict.get('primary_key', 'unknown')}")
        return version_dict

    except Exception as e:
        print(f"Error processing program version from {rte_xml_path}: {e}")
        return None


@task
def save_program_version_json(
    version_data: dict[str, Any], srp_file: Path
) -> Path | None:
    """Save program version data as JSON next to the .srp file.

    Args:
        version_data: Program version data as dict
        srp_file: Original .srp file path

    Returns:
        Path to saved JSON file, or None if save failed
    """
    if version_data is None:
        return None

    # Save as .srp.version.json
    json_path = srp_file.with_suffix(".srp.version.json")

    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(version_data, f, indent=2)

        print(f"Saved program version to {json_path}")
        return json_path

    except Exception as e:
        print(f"Error saving program version JSON: {e}")
        return None


@task
def cleanup_temp_dir(temp_dir: Path) -> None:
    """Clean up temporary extraction directory.

    Args:
        temp_dir: Temporary directory to remove
    """
    try:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"Cleaned up temp directory: {temp_dir}")
    except Exception as e:
        print(f"Warning: Failed to cleanup {temp_dir}: {e}")


@task
def mark_as_processed(srp_file: Path) -> Path:
    """Mark .srp file as processed by creating a marker file.

    Args:
        srp_file: .srp file to mark as processed

    Returns:
        Path to marker file
    """
    marker_file = srp_file.with_suffix(".srp.version_processed")
    marker_file.touch()
    print(f"Marked as processed: {marker_file}")
    return marker_file


@task
def get_date_from_header(version_data: dict[str, Any]) -> tuple[str, str, str] | None:
    """Extract YYYY/MM/DD from the program version effective date.

    Args:
        version_data: Program version data dict

    Returns:
        Tuple of (year, month, day) or None if not found
    """
    effective_date = version_data.get("effective_date")
    if not effective_date:
        print("Warning: No effective_date found in program version")
        # Fall back to current date
        from datetime import datetime
        now = datetime.now()
        return (str(now.year), f"{now.month:02d}", f"{now.day:02d}")

    # Parse date - could be various formats
    # Try common formats
    from datetime import datetime
    date_formats = [
        "%Y-%m-%dT%H:%M:%S",  # ISO format with time
        "%Y-%m-%d",            # ISO date only
        "%m/%d/%Y",            # US format
        "%Y/%m/%d",            # Year first
        "%d-%m-%Y"             # Day first
    ]

    for fmt in date_formats:
        try:
            dt = datetime.strptime(effective_date, fmt)
            return (str(dt.year), f"{dt.month:02d}", f"{dt.day:02d}")
        except ValueError:
            continue

    print(f"Warning: Could not parse effective_date: {effective_date}")
    # Fall back to current date
    now = datetime.now()
    return (str(now.year), f"{now.month:02d}", f"{now.day:02d}")


@task
def upload_version_to_s3(
    version_data: dict[str, Any],
    year: str,
    month: str,
    day: str,
    filename: str,
    bucket: str,
    prefix: str,
    region: str,
) -> str | None:
    """Upload program version JSON to S3 in date-partitioned structure.

    Args:
        version_data: Program version data as dict
        year: Year (e.g., "2025")
        month: Month (e.g., "03")
        day: Day (e.g., "31")
        filename: Filename for the JSON (e.g., "1_118_0_1224_9.0000.json")
        bucket: S3 bucket name
        prefix: S3 prefix (e.g., "program-versions")
        region: AWS region

    Returns:
        S3 key of uploaded file, or None if upload failed
    """
    if version_data is None:
        return None

    s3_key = f"{prefix}/{year}/{month}/{day}/{filename}"

    try:
        s3_client = boto3.client("s3", region_name=region)
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(version_data, indent=2),
            ContentType="application/json",
        )

        print(f"Uploaded program version to s3://{bucket}/{s3_key}")
        return s3_key

    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None


@task
def append_to_version_manifest(
    s3_key: str,
    version_data: dict[str, Any],
    year: str,
    month: str,
    day: str,
    bucket: str,
    prefix: str,
    region: str,
) -> None:
    """Append program version metadata to NDJSON manifest in S3.

    Args:
        s3_key: S3 key of uploaded program version file
        version_data: Program version data
        year: Year
        month: Month
        day: Day
        bucket: S3 bucket name
        prefix: S3 prefix
        region: AWS region
    """
    if s3_key is None or version_data is None:
        return

    manifest_key = f"{prefix}/{year}/{month}/{day}/_manifest.ndjson"

    try:
        s3_client = boto3.client("s3", region_name=region)

        # Try to read existing manifest
        try:
            response = s3_client.get_object(Bucket=bucket, Key=manifest_key)
            existing_content = response["Body"].read().decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            existing_content = ""

        # Create manifest record
        manifest_record = {
            "s3_key": s3_key,
            "primary_key": version_data.get("primary_key"),
            "program_id": version_data.get("program_id"),
            "version": version_data.get("version"),
            "line": version_data.get("line"),
            "carrier": version_data.get("subscriber"),
            "effective_date": version_data.get("effective_date"),
        }

        # Append new record
        new_content = existing_content + json.dumps(manifest_record) + "\n"

        # Write back to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=new_content,
            ContentType="application/x-ndjson",
        )

        print(f"Updated manifest: s3://{bucket}/{manifest_key}")

    except Exception as e:
        print(f"Error updating manifest: {e}")


@flow(
    name="version-export-hourly",
    description="Process SRP program versions from .srp files and upload to S3",
    task_runner=ConcurrentTaskRunner(),
    validate_parameters=False,
)


def version_export_flow(
    inbox_dir: str = "data/srps/inbox",
    zip_password: str = "",
    s3_bucket: str = "local-packages-bucket",
    s3_prefix: str = "program-versions",
    s3_region: str = "us-east-2",
    max_concurrent: int = 5,
):
    """Process unprocessed .srp files and extract program version data.

    Args:
        inbox_dir: Directory containing .srp files to process
        zip_password: Password for .srp files
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix for program versions (e.g., "program-versions")
        s3_region: AWS region
        max_concurrent: Maximum number of concurrent processing tasks
    """
    config = get_config()

    # Override defaults with config if available
    if config.ingest:
        zip_password = config.ingest.zip_password or zip_password
        inbox_dir = str(config.ingest.input_dir) or inbox_dir
    if config.s3:
        s3_bucket = config.s3.bucket_name or s3_bucket
        s3_region = config.s3.bucket_region or s3_region

    inbox_path = Path(inbox_dir)

    # Find all unprocessed .srp files
    srp_files = find_unprocessed_srp_files(inbox_path)

    if not srp_files:
        print("No unprocessed .srp files found")
        return

    print(f"Processing {len(srp_files)} .srp file(s)")

    # Process each .srp file
    for srp_file in srp_files:
        try:
            # Extract to temp directory
            temp_dir = extract_srp_to_temp(srp_file, zip_password)

            # Find RTE XML file (the program version file)
            rte_xml_path = find_rte_xml_file(temp_dir)

            if rte_xml_path is None:
                print(f"Skipping {srp_file.name} - no RTE XML file found")
                cleanup_temp_dir(temp_dir)
                continue

            # Find and parse rtd files
            rtd_files = find_rtd_files(temp_dir)
            rtd_data = parse_rtd_files(rtd_files) if rtd_files else []

            # Process program version
            version_data = process_program_version(rte_xml_path)

            if version_data is None:
                print(f"Skipping {srp_file.name} - processing failed")
                cleanup_temp_dir(temp_dir)
                continue

            # Add rtd data to version_data
            version_data['rating_tables'] = rtd_data
            version_data['rating_table_count'] = len(rtd_data)

            # Save program version JSON locally
            save_program_version_json(version_data, srp_file)

            # Get date from program version effective date
            date_tuple = get_date_from_header(version_data)

            if date_tuple is not None:
                year, month, day = date_tuple

                # Create filename from srp filename (replace .srp with .json)
                json_filename = srp_file.stem + ".json"

                # Upload to S3
                s3_key = upload_version_to_s3(
                    version_data=version_data,
                    year=year,
                    month=month,
                    day=day,
                    filename=json_filename,
                    bucket=s3_bucket,
                    prefix=s3_prefix,
                    region=s3_region,
                )

                # Update manifest
                if s3_key:
                    append_to_version_manifest(
                        s3_key=s3_key,
                        version_data=version_data,
                        year=year,
                        month=month,
                        day=day,
                        bucket=s3_bucket,
                        prefix=s3_prefix,
                        region=s3_region,
                    )

            # Clean up temp directory
            cleanup_temp_dir(temp_dir)

            # Mark as processed
            mark_as_processed(srp_file)

            print(f"Successfully processed: {srp_file.name}")

        except Exception as e:
            print(f"Error processing {srp_file.name}: {e}")
            continue

    print("Version export flow completed")


if __name__ == "__main__":
    version_export_flow()
