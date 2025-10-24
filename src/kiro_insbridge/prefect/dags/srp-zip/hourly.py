from __future__ import annotations

import io
import json
import mimetypes
import shutil
import urllib.parse
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from prefect import flow, get_run_logger, task

from kiro_insbridge.enterprise_rating.entities.srp_request import SrpRequest as Srp

# ZIP (AES optional)
try:
    import pyzipper

    _HAS_PYZIPPER = True
except Exception:
    _HAS_PYZIPPER = False
import zipfile

# AWS
import boto3

# Import config after other imports to ensure models are fully built
from kiro_insbridge.enterprise_rating.config import ProjectConfig, get_config

if TYPE_CHECKING:
    pass  # ProjectConfig already imported above

# ---- your repository (adjust import path if needed) ----
# e.g., put this class in enterprise_rating/repositories/srp_header_repository.py
from kiro_insbridge.enterprise_rating.repository.srp_header_repository import SrpHeaderRepository


# -----------------------------
# Helpers
# -----------------------------
def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten nested dict/list into { "a.b[0].c": value } form.
    Values are kept as-is (strings/numbers/bools) for JSON manifest; for S3 tags we coerce to str.
    """
    out: dict[str, Any] = {}

    def walk(x: Any, p: str = ""):
        if isinstance(x, dict):
            for k, v in x.items():
                key = f"{p}.{k}" if p else k
                walk(v, key)
        elif isinstance(x, list):
            for i, v in enumerate(x):
                key = f"{p}[{i}]"
                walk(v, key)
        else:
            out[p] = x

    walk(obj, prefix)
    return out


def _to_dict(model: Any) -> dict:
    """Support Pydantic v2 (.model_dump) and v1 (.dict)."""
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    raise TypeError("Unsupported SrpRequest type; expected a Pydantic model.")


def _pick_bucket_date(flat: dict[str, Any], candidates: list[str], date_fmt: str) -> date:
    for key in candidates:
        if key in flat and isinstance(flat[key], str):
            try:
                return datetime.strptime(flat[key], date_fmt).date()
            except ValueError:
                pass
    # fallback: scan any key containing 'date'
    for k, v in flat.items():
        if "date" in k.lower() and isinstance(v, str):
            try:
                return datetime.strptime(v, date_fmt).date()
            except ValueError:
                continue
    raise ValueError("Could not find/parse a date in header.xml for YYYY/MM/DD bucketing.")


# -----------------------------
# Tasks
# -----------------------------
@task(log_prints=True)
def find_zip_files(input_dir: Path, pattern: str) -> tuple[list[Path], list[Path]]:
    """Find zip files and separate release zips from regular zips.

    Returns:
        tuple: (regular_zips, release_zips) where release_zips contain '_REL_' in filename
    """
    logger = get_run_logger()

    # Ensure it's a Path (in case config passes a string)
    input_dir = Path(input_dir)

    if not input_dir.exists():
        logger.error(f"Input dir does not exist: {input_dir}")
        return [], []

    # Case-insensitive ZIP discovery if using the default pattern
    if pattern.lower() == "*.srp":
        zips = sorted(p for p in input_dir.rglob("*") if p.is_file() and p.suffix.lower() == ".srp")
    else:
        # Honor custom patterns as-is
        zips = sorted(input_dir.rglob(pattern))

    # Separate release zips (containing _REL_) from regular zips
    release_zips = [z for z in zips if "_REL_" in z.name]
    regular_zips = [z for z in zips if "_REL_" not in z.name]

    logger.info(f"Scanning {input_dir} with pattern '{pattern}' → found {len(zips)} total srp(s)")
    logger.info(f"  • {len(regular_zips)} regular zip(s)")
    logger.info(f"  • {len(release_zips)} release zip(s) containing nested zips")

    # Log a few examples for sanity
    for preview in regular_zips[:3]:
        logger.info(f"  Regular: {preview.name}")
    for preview in release_zips[:3]:
        logger.info(f"  Release: {preview.name}")

    return regular_zips, release_zips


@task
def extract_zip(zip_path: Path, staging_dir: Path, password: Optional[str]) -> Path:
    logger = get_run_logger()
    extract_dir = staging_dir / zip_path.stem
    extract_dir.mkdir(parents=True, exist_ok=True)

    pwd = password.encode("utf-8") if isinstance(password, str) else None
    if _HAS_PYZIPPER:
        with pyzipper.AESZipFile(zip_path) as zf:
            if pwd:
                zf.pwd = pwd
            zf.extractall(extract_dir)
    else:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(path=extract_dir, pwd=pwd)
    logger.info(f"Extracted {zip_path.name} → {extract_dir}")
    return extract_dir


@task(log_prints=True)
def extract_release_zip_and_collect_nested(
    release_zip: Path, staging_dir: Path, password: Optional[str], input_dir: Path
) -> list[Path]:
    """Extract a release zip, find nested .srp files, and archive the release zip.

    Args:
        release_zip: Path to the release zip file containing nested zips
        staging_dir: Temporary directory for extraction
        password: Password for encrypted zips
        input_dir: Input directory where we'll move extracted nested zips

    Returns:
        List of paths to extracted nested zip files
    """
    logger = get_run_logger()

    # Extract the release zip
    release_extract_dir = staging_dir / f"{release_zip.stem}_release"
    release_extract_dir.mkdir(parents=True, exist_ok=True)

    pwd = password.encode("utf-8") if isinstance(password, str) else None
    if _HAS_PYZIPPER:
        with pyzipper.AESZipFile(release_zip) as zf:
            if pwd:
                zf.pwd = pwd
            zf.extractall(release_extract_dir)
    else:
        with zipfile.ZipFile(release_zip) as zf:
            zf.extractall(path=release_extract_dir, pwd=pwd)

    logger.info(f"Extracted release zip {release_zip.name} → {release_extract_dir}")

    # Find all nested .srp files
    nested_zips = sorted(p for p in release_extract_dir.rglob("*") if p.is_file() and p.suffix.lower() == ".srp")
    logger.info(f"Found {len(nested_zips)} nested zip(s) in {release_zip.name}")

    # Move nested zips to input directory for processing
    moved_zips = []
    for nested_zip in nested_zips:
        # Create unique name to avoid conflicts
        target_path = input_dir / f"extracted_{nested_zip.name}"
        counter = 1
        while target_path.exists():
            target_path = input_dir / f"extracted_{counter}_{nested_zip.name}"
            counter += 1

        shutil.move(str(nested_zip), str(target_path))
        moved_zips.append(target_path)
        logger.info(f"  Moved nested zip: {nested_zip.name} → {target_path.name}")

    # Archive the release zip (move to processed subfolder)
    archive_dir = input_dir / "processed_releases"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / release_zip.name

    # Handle duplicate names
    counter = 1
    while archive_path.exists():
        archive_path = archive_dir / f"{release_zip.stem}_{counter}{release_zip.suffix}"
        counter += 1

    shutil.move(str(release_zip), str(archive_path))
    logger.info(f"Archived release zip: {release_zip.name} → {archive_path}")

    return moved_zips


@task
def parse_header_with_repo(extract_dir: Path, metadata_filename: str) -> Srp:
    """Use your SrpHeaderRepository to parse header.xml and return a dict (via Pydantic)."""
    matches = list(extract_dir.rglob(metadata_filename))
    if not matches:
        raise FileNotFoundError(f"{metadata_filename} not found under {extract_dir}")
    xml_path = matches[0]

    srp = SrpHeaderRepository.get_srp_header(str(xml_path))
    if srp is None:
        raise ValueError(f"SrpHeaderRepository returned None for {xml_path}")

    data = _to_dict(srp)  # nested dict from SrpRequest
    # keep a breadcrumb for debugging
    data["_metadata_xml_path"] = str(xml_path.relative_to(extract_dir))
    return srp


@task
def compute_bucket_date_from_srp(srp_data: Srp, candidates: list[str], date_fmt: str) -> date:
    """Prefer explicit SRP fields, then fall back to your generic candidates."""
    # 1) First try 'date_created' / 'date_created_split' directly from srp_header
    val = srp_data.srp_header.date_created
    d = _parse_us_datetime_with_suffix(val) if isinstance(val, str) else None
    if d:
        return d

    # 2) Fallback to the previous flatten + candidates approach
    flat = _flatten(_to_dict(srp_data))
    return _pick_bucket_date(flat, candidates, date_fmt)


@task
def mirror_to_local_output(extract_dir: Path, output_root: Path, d: date) -> Path:
    target = output_root / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.day:02d}"
    for src in extract_dir.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(extract_dir)
        dst = target / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    return target


@task
def upload_dir_to_s3(
    local_dir: Path, bucket: str, prefix_root: str, d: date, tag_keys: list[str], srp_data: dict
) -> list[str]:
    """Upload all files from local_dir to s3://bucket/prefix_root/YYYY/MM/DD/...
    Optionally apply up to 10 object tags sourced from flattened srp_data.
    """
    logger = get_run_logger()
    s3 = boto3.client("s3")

    date_prefix = f"{prefix_root}/{d.year:04d}/{d.month:02d}/{d.day:02d}"
    uploaded_keys: list[str] = []

    tagset = None
    if tag_keys:
        flat = _flatten(srp_data)
        selected = {k: str(flat.get(k, ""))[:256] for k in tag_keys[:10]}
        tagset = urllib.parse.urlencode(selected)

    for src in local_dir.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(local_dir)
        key = f"{date_prefix}/{rel.as_posix()}"
        content_type, _ = mimetypes.guess_type(str(src))

        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        if tagset:
            extra["Tagging"] = tagset

        s3.upload_file(Filename=str(src), Bucket=bucket, Key=key, ExtraArgs=extra)
        uploaded_keys.append(key)

    logger.info(f"Uploaded {len(uploaded_keys)} objects to s3://{bucket}/{date_prefix}")
    return uploaded_keys


@task
def write_manifest_ndjson(bucket: str, prefix_root: str, d: date, srp_data: dict, uploaded_keys: list[str]) -> str:
    """Store one NDJSON record per ZIP under the same date prefix:
    { ...all SrpRequest fields..., "s3_objects": [...] }
    """
    s3 = boto3.client("s3")
    record = dict(srp_data)
    record["s3_objects"] = uploaded_keys
    body = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
    key = f"{prefix_root}/{d.year:04d}/{d.month:02d}/{d.day:02d}/_manifest.ndjson"
    s3.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(body), ContentType="application/x-ndjson")
    return key


# -----------------------------
# Flow
# -----------------------------
# --- fix references and ensure Path casting in your flow ---
@flow(validate_parameters=False)
def zip_to_s3_flow(config: ProjectConfig) -> None:
    """1) find zips (separate release zips containing nested zips)
    2) extract release zips, collect nested zips, archive release zips
    3) process all zips (regular + extracted from releases):
       a) extract with password
       b) parse header.xml via SrpHeaderRepository → dict
       c) compute date → YYYY/MM/DD
       d) mirror extracted files locally under that date path
       e) upload to s3://bucket/prefix/YYYY/MM/DD/...
       f) write _manifest.ndjson
    """
    logger = get_run_logger()
    config = config or get_config()
    if config.ingest is None or config.s3 is None:
        raise ValueError("Both config.ingest and config.s3 are required.")

    # Cast to Path for safety if your config uses strings
    input_dir = Path(config.ingest.input_dir)
    staging_dir = Path(config.ingest.staging_dir)
    output_dir = Path(config.ingest.output_dir)

    staging_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Find all zips, separating release zips from regular zips
    regular_zips, release_zips = find_zip_files(input_dir, config.ingest.zip_glob)

    # Step 2: Process release zips first - extract nested zips and archive the release zip
    all_nested_zips = []
    for release_zip in release_zips:
        nested_zips = extract_release_zip_and_collect_nested(
            release_zip, staging_dir, config.ingest.zip_password, input_dir
        )
        all_nested_zips.extend(nested_zips)

    # Step 3: Combine all zips to process (regular + nested from releases)
    all_zips_to_process = regular_zips + all_nested_zips
    logger.info(f"Processing {len(all_zips_to_process)} total zip(s): {len(regular_zips)} regular + {len(all_nested_zips)} from releases")

    # Step 4: Process each zip
    for zip_path in all_zips_to_process:
        extracted = extract_zip(zip_path, staging_dir, config.ingest.zip_password)

        # FIX: use config.ingest.metadata_filename
        srp_data = parse_header_with_repo(extracted, config.ingest.metadata_filename)

        print(f"Parsed SRP data: {srp_data}")

        # FIX: use config.ingest.metadata_date_keys / metadata_date_format
        bucket_date = compute_bucket_date_from_srp(
            srp_data, config.ingest.metadata_date_key, config.ingest.metadata_date_format
        )

        # # FIX: use output_dir (not config.output_dir)
        local_dated = mirror_to_local_output(extracted, output_dir, bucket_date)

        print(f"Mirrored extracted files to: {local_dated}")

        # Convert Srp to dict for S3 operations
        srp_data_dict = _to_dict(srp_data)

        uploaded = upload_dir_to_s3(
            local_dated,
            bucket=config.s3.bucket_name,  # keep your S3Config names
            prefix_root=config.s3.prefix,
            d=bucket_date,
            tag_keys=getattr(config.s3, "tag_keys", []),
            srp_data=srp_data_dict,
        )

        write_manifest_ndjson(
            bucket=config.s3.bucket_name,
            prefix_root=config.s3.prefix,
            d=bucket_date,
            srp_data=srp_data_dict,
            uploaded_keys=uploaded,
        )


def _parse_us_datetime_with_suffix(s: str) -> Optional[date]:
    """Parse strings like '3/31/2025 1:11:55 PM_Auto' or '3/31/2025 13:11:55'.
    Returns a date or None if not parseable.
    """
    if not isinstance(s, str):
        return None
    base = s.split("_", 1)[0].strip()  # remove trailing "_Auto" etc.
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(base, fmt).date()
        except ValueError:
            pass
    return None


# Rebuild the flow's validation model to resolve ProjectConfig forward references
# This is required for Pydantic v2 when using complex nested models
if hasattr(zip_to_s3_flow, "_parameter_model"):
    try:
        zip_to_s3_flow._parameter_model.model_rebuild()
    except Exception:
        pass  # Fallback if Prefect internals change


if __name__ == "__main__":
    cfg = get_config()

    # print(f"Starting zip_to_s3_flow with config: {cfg.s3} and {cfg.ingest}")
    zip_to_s3_flow(cfg)
