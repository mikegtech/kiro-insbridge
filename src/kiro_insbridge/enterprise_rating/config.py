"""Configuration management for the SoftRater application."""

import configparser
import os
from datetime import timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytz
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

from kiro_insbridge import PROJECT_DIR

# Check if profile exists in AWS config


class S3Config(BaseModel):
    """S3 cold storage configuration."""

    # AWS Profile (for SSO)
    aws_profile: Optional[str] = Field(default=None, description="AWS SSO named profile (e.g., 'mycompany-dev')")

    # Bucket settings
    bucket_name: str = Field(default="property-embeddings", description="S3 bucket for cold storage")
    bucket_region: str = Field(default="us-west-2", description="AWS region for bucket")

    # Storage organization
    prefix: str = Field(default="embeddings", description="S3 key prefix for organization")

    # Partitioning strategy
    partition_by: str = Field(default="date", description="Partitioning strategy: date, month, year")

    # Performance settings
    batch_size: int = Field(default=1000, description="Records per Parquet file")
    flush_interval_minutes: int = Field(default=30, description="Max time before flushing buffer")
    max_workers: int = Field(default=4, description="Parallel workers for S3 operations")
    multipart_threshold: int = Field(default=100 * 1024 * 1024, description="Threshold for multipart upload")  # 100MB

    # Compression settings
    compression: str = Field(default="snappy", description="Parquet compression: snappy, gzip, brotli, lz4, zstd")
    compression_level: Optional[int] = Field(default=None, description="Compression level (for gzip, brotli, zstd)")

    # Lifecycle settings
    intelligent_tiering_days: int = Field(default=30, description="Days before moving to Intelligent-Tiering")
    glacier_days: int = Field(default=90, description="Days before moving to Glacier")
    expiration_days: int = Field(default=2555, description="Days before deletion")  # 7 years

    # File format settings
    use_dictionary_encoding: bool = Field(default=True, description="Use Parquet dictionary encoding")
    row_group_size: int = Field(default=100000, description="Rows per Parquet row group")

    @field_validator("aws_profile")
    @classmethod
    def validate_aws_profile(cls, v):
        """Validate AWS profile exists if specified."""
        if v is not None:
            aws_config_path = Path.home() / ".aws" / "config"
            if aws_config_path.exists():
                config = configparser.ConfigParser()
                config.read(aws_config_path)
                profile_name = f"profile {v}" if v != "default" else "default"
                if profile_name not in config:
                    print(f"Warning: AWS profile '{v}' not found in {aws_config_path}")
        return v

    def get_bucket_path(self, partition_date=None) -> str:
        """Get S3 path for a given partition date."""
        base_path = f"s3://{self.bucket_name}/{self.prefix}"

        if partition_date:
            if self.partition_by == "date":
                return f"{base_path}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}"
            elif self.partition_by == "month":
                return f"{base_path}/year={partition_date.year}/month={partition_date.month:02d}"
            elif self.partition_by == "year":
                return f"{base_path}/year={partition_date.year}"

        return base_path

    def get_session_config(self) -> Dict[str, Any]:
        """Get boto3 session configuration."""
        config = {"region_name": self.bucket_region}

        if self.aws_profile:
            config["profile_name"] = self.aws_profile

        return config


class IngestConfig(BaseModel):
    """Configuration for ingesting and processing zip files."""

    input_dir: Path = Field(default=Path("data/inbox"), description="Directory to scan for zip files")
    staging_dir: Path = Field(default=Path("data/staging"), description="Temporary extraction directory")
    output_dir: Path = Field(default=Path("data/output"), description="Output directory for processed files")
    zip_glob: str = Field(default="*.srp", description="Glob pattern for zip files")
    zip_password: Optional[str] = Field(default="", description="Password for encrypted zip files")
    metadata_filename: str = Field(default="header.xml", description="Metadata file name inside zip")
    metadata_date_key: list[str] = Field(default=["date_created_split"], description="Key in metadata file for date")
    metadata_date_format: str = Field(default="%Y-%m-%d", description="Date format in metadata file")
    tags_keys: list[str] = Field(default_factory=list, description="Metadata keys to use as S3 tags")


class ProjectConfig(BaseModel):
    """Enhanced project configuration with vector database support."""

    # Core settings (existing)
    catalog_name: str
    schema_name: str
    pipeline_id: str

    s3: Optional[S3Config] = None
    ingest: Optional[IngestConfig] = None

    # Data paths
    sr_packages_inbox: str = Field(default="data/srps/inbox")
    batch_size: int = Field(default=500)

    # Model versions (now in VectorConfig but kept for compatibility)
    model_version: str = Field(default="text-e5-base-v2@onnx17-fp32")
    transform_version: str = Field(default="ntreis-hourly-v1")

    # Optional RETS config
    rets_login_url: Optional[str] = None
    rets_username: Optional[str] = None
    rets_password: Optional[str] = None

    # Watermark settings
    watermark_file: Optional[Path] = None
    watermark_source: Optional[str] = None

    model_config = {"arbitrary_types_allowed": True}  # Allow Path objects

    @classmethod
    def from_yaml_and_env(
        cls, config_path: str = "project_config_insbridge.yml", env: str = "local", env_dir: str = "config"
    ) -> "ProjectConfig":
        """Load configuration from both YAML and environment files."""
        if env not in ["prd", "acc", "dev", "local"]:
            raise ValueError(f"Invalid environment: {env}")

        # Load environment-specific .env file
        env_file = Path(env_dir) / f".env.{env}"
        if env_file.exists():
            load_dotenv(env_file, override=True)
        else:
            # Fallback to root .env if exists
            load_dotenv(override=True)

        # Load YAML config
        with open(config_path) as f:
            yaml_config = yaml.safe_load(f)
            env_config = yaml_config[env]

        # Build S3 config
        s3_config = None
        if os.getenv("S3_BUCKET") or env_config.get("s3"):
            s3_config = S3Config(
                aws_profile=os.getenv("AWS_PROFILE"),
                bucket_name=os.getenv("S3_BUCKET", f"{env}-property-embeddings"),
                bucket_region=os.getenv("S3_REGION", "us-east-2"),
                prefix=os.getenv("S3_PREFIX", f"embeddings/{env}"),
                partition_by=os.getenv("S3_PARTITION_BY", "date"),
                batch_size=int(os.getenv("S3_BATCH_SIZE", 1000)),
                flush_interval_minutes=int(os.getenv("S3_FLUSH_INTERVAL", 30)),
                compression=os.getenv("S3_COMPRESSION", "snappy"),
                intelligent_tiering_days=int(os.getenv("S3_INTELLIGENT_TIERING_DAYS", 30)),
                glacier_days=int(os.getenv("S3_GLACIER_DAYS", 90)),
                expiration_days=int(os.getenv("S3_EXPIRATION_DAYS", 2555)),
            )

        # # Build Vector config
        # vector_config = VectorConfig(
        #     model_name=os.getenv("VECTOR_MODEL_NAME", "text-e5-base-v2"),
        #     model_version=os.getenv("MODEL_VERSION", "text-e5-base-v2@onnx17-fp32"),
        #     vector_dimensions=int(os.getenv("VECTOR_DIMENSIONS", 768)),
        #     normalize_embeddings=os.getenv("NORMALIZE_EMBEDDINGS", "true").lower() == "true",
        #     batch_encode_size=int(os.getenv("BATCH_ENCODE_SIZE", 32)),
        #     use_prefix=os.getenv("USE_E5_PREFIX", "true").lower() == "true",
        # )

        ingest_config = IngestConfig(
            input_dir=Path(os.path.join(PROJECT_DIR, "data/srps/inbox")),
            staging_dir=Path(os.path.join(PROJECT_DIR, "data/srps/staging")),
            output_dir=Path(os.path.join(PROJECT_DIR, "data/srps/output")),
            zip_glob="*.srp",
            zip_password=os.getenv("SRP_ZIP_PASSWORD"),  # or None if not needed
            metadata_filename="header.xml",  # inside each zip
            metadata_date_key=["date_created_split"],  # e.g., {"date": "2025-09-08"}
            metadata_date_format="%Y-%m-%d",
            tags_keys=[
                "prog_key",
                "program_id",
                "program_version",
                "email_address",
                "location",
                "line_id",
                "schema_id",
                "program_name",
                "version_desc",
            ],  # ≤10
        )

        # Combine all configurations
        return cls(
            catalog_name=env_config["catalog_name"],
            schema_name=env_config["schema_name"],
            pipeline_id=env_config.get("pipeline_id", ""),
            s3=s3_config,
            ingest=ingest_config,
            sr_packages_inbox=os.getenv("SOFTRATER_PACKAGES_INBOX", os.path.join(PROJECT_DIR, "data/srps/inbox")),
            batch_size=int(os.getenv("BATCH_SIZE", 500)),
            # model_version=vector_config.model_version,  # Use from vector config
            transform_version=os.getenv("TRANSFORM_VERSION", "ntreis-hourly-v1"),
            rets_login_url=os.getenv("RETS_LOGIN_URL"),
            rets_username=os.getenv("RETS_USERNAME"),
            rets_password=os.getenv("RETS_PASSWORD"),
        )

    def get_utc_now(self):
        """Get current time in UTC."""
        from datetime import datetime

        return datetime.now(timezone.utc)

    def format_for_display(self, dt):
        """Convert UTC datetime to display timezone."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        display_tz = pytz.timezone(self.display_timezone)
        return dt.astimezone(display_tz)


# Singleton pattern for config
_config: Optional[ProjectConfig] = None


def get_config(env: Optional[str] = None) -> ProjectConfig:
    """Get or create configuration singleton."""
    global _config
    if _config is None:
        env = env or os.getenv("ENVIRONMENT", "local")
        _config = ProjectConfig.from_yaml_and_env(env=env)
    return _config


def reset_config():
    """Reset configuration singleton (useful for testing)."""
    global _config
    _config = None


# Rebuild models to resolve forward references (required for Pydantic v2)
S3Config.model_rebuild()
IngestConfig.model_rebuild()
ProjectConfig.model_rebuild()
