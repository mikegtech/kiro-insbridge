# Kiro Insbridge Documentation

## Overview

This documentation covers the **SRP Processing Pipeline**, a two-stage data pipeline for extracting, processing, and uploading SoftRater Package (SRP) data to Amazon S3.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     SRP Processing Pipeline                      │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│ .srp files      │
│ (inbox)         │
└────────┬────────┘
         │
         ├──────────────────────────────────────┐
         │                                      │
         ▼                                      ▼
┌────────────────────┐              ┌────────────────────┐
│ Stage 1:           │              │ Stage 2:           │
│ SRP Zip to S3      │              │ Version Export     │
│                    │              │                    │
│ • Extract packages │              │ • Find RTE XML     │
│ • Parse headers    │              │ • Process versions │
│ • Upload to S3     │              │ • Upload to S3     │
└────────┬───────────┘              └────────┬───────────┘
         │                                   │
         ▼                                   ▼
┌────────────────────┐              ┌────────────────────┐
│ s3://bucket/       │              │ s3://bucket/       │
│ srp-data/          │              │ program-versions/  │
│ YYYY/MM/DD/        │              │ YYYY/MM/DD/        │
│ ├── packages/      │              │ ├── versions.json  │
│ └── _manifest      │              │ └── _manifest      │
└────────────────────┘              └────────────────────┘
```

---

## Documentation Files

### 1. [SRP Zip to S3 Flow](SRP_ZIP_FLOW_DOCUMENTATION.md)

**Purpose:** Extract SRP packages and upload to S3

**Key Features:**
- Processes .srp files from inbox
- Handles release zips (nested .srp files)
- Extracts headers and metadata
- Uploads to `s3://local-packages-bucket/srp-data/YYYY/MM/DD/`
- Creates NDJSON manifests

**Output:** SRP package files organized by date

**Run:** `task flow:srp-zip:dev`

---

### 2. [Version Export Flow](VERSION_EXPORT_FLOW_DOCUMENTATION.md)

**Purpose:** Extract and process program versions (RTE XML)

**Key Features:**
- Processes .srp files from inbox
- Finds RTE XML files (e.g., `AEBB715809.xml`)
- Parses program metadata, algorithms, instructions
- Generates Abstract Syntax Tree (AST) for instructions
- Uploads to `s3://local-packages-bucket/program-versions/YYYY/MM/DD/`
- Creates NDJSON manifests

**Output:** Program version JSON files organized by effective date

**Run:** `task flow:version-export:dev`

---

## Quick Start

### Prerequisites

```bash
# Install dependencies
uv sync --group dev

# Set environment variables
export ENVIRONMENT=local
export SRP_ZIP_PASSWORD=
```

### Running the Pipeline

```bash
# Run complete pipeline (both stages)
task flow:pipeline:dev

# Or run individually
task flow:srp-zip:dev
task flow:version-export:dev
```

### Viewing Results

```bash
# List SRP packages in S3
aws s3 ls s3://local-packages-bucket/srp-data/2025/03/31/ --recursive

# List program versions in S3
aws s3 ls s3://local-packages-bucket/program-versions/2025/03/31/ --recursive

# Download manifests
aws s3 cp s3://local-packages-bucket/srp-data/2025/03/31/_manifest.ndjson ./srp-manifest.ndjson
aws s3 cp s3://local-packages-bucket/program-versions/2025/03/31/_manifest.ndjson ./version-manifest.ndjson
```

---

## Task Commands

All available task commands are defined in `Taskfile.yml`:

### Pipeline Commands

```bash
# Complete pipeline
task flow:pipeline:dev        # Run both flows in local/dev mode
task flow:pipeline            # Run both flows in production mode

# Individual flows
task flow:srp-zip:dev         # Run SRP zip flow in local/dev mode
task flow:version-export:dev  # Run version export flow in local/dev mode

# Prefect server
task prefect:server           # Start local Prefect server
task prefect:deploy           # Deploy Prefect flows
```

---

## Data Outputs

### SRP Packages (`srp-data/`)

Contains extracted SRP package files:

```
s3://local-packages-bucket/srp-data/2025/03/31/
├── 1_118_0_1224_9.0000/
│   ├── header.xml          ← Package metadata
│   ├── srp.xml             ← Package structure
│   ├── signature           ← Digital signature
│   ├── AEBB715809.xml      ← RTE (Rating Engine) XML
│   ├── rt_summary.xml      ← Rating summary
│   ├── rtd/                ← Rating tables (data)
│   └── rto/                ← Rating tables (objects)
└── _manifest.ndjson        ← Package manifest
```

**Manifest Record:**
```json
{
  "srp_header": {
    "prog_key": "",
    "program_id": "",
    "program_name": "",
    "program_version": "",
    "line_desc": "",
    "carrier_name": ""
  },
  "srpuser": {...},
  "s3_objects": [...]
}
```

---

### Program Versions (`program-versions/`)

Contains processed program version data:

```
s3://local-packages-bucket/program-versions/2025/03/31/
├── extracted_2_1_118_0_891_1.0000.json   ← Full program version
├── extracted_1_1_1_0_758_1.0000.json
└── _manifest.ndjson                       ← Version manifest
```

**Manifest Record:**
```json
{
  "s3_key": "program-versions/2025/03/31/extracted_2_1_118_0_891_1.0000.json",
  "primary_key": "CEE9728349",
  "program_id": "891",
  "version": "1.0000",
  "line": "118",
  "carrier": "1",
  "effective_date": "2025-03-31T13:26:32"
}
```

**Program Version Structure:**
- Program metadata (ID, version, effective date)
- Data dictionary (input variables, categories)
- Algorithm sequences (execution order)
- Algorithms (business logic)
- Instructions with AST (Abstract Syntax Tree)

---

## Use Cases

### 1. Rate Calculation Analysis
Query program versions to understand premium calculation logic:
```python
import json, boto3

s3 = boto3.client('s3')
response = s3.get_object(
    Bucket='local-packages-bucket',
    Key='program-versions/2025/03/31/extracted_2_1_118_0_891_1.0000.json'
)

version = json.loads(response['Body'].read())
print(f"Program: {version['program_id']}")
print(f"Algorithms: {len(version['algorithm_seq'])}")
```

### 2. Package Auditing
Track all packages uploaded for a specific date:
```bash
aws s3 cp s3://local-packages-bucket/srp-data/2025/03/31/_manifest.ndjson - | \
  jq -r '.srp_header.program_name'
```

### 3. Version Comparison
Compare two versions of the same program:
```python
# Download two versions and diff their algorithms
version_old = get_program_version("891", "1.0000")
version_new = get_program_version("891", "2.0000")

# Compare algorithm counts, dependencies, etc.
```

---

## Configuration

### Environment Variables

Set in `.env.local`:

```bash
# AWS
AWS_PROFILE=your-profile
S3_BUCKET=local-packages-bucket
S3_REGION=us-east-2

# SRP Processing
SRP_ZIP_PASSWORD=
ENVIRONMENT=local
```

### Project Config

Edit `project_config_insbridge.yml`:

```yaml
local:
  catalog_name: "local_catalog"
  schema_name: "srp_data"
  pipeline_id: "srp-pipeline"
```

---

## Troubleshooting

### No files found
- Check `data/srps/inbox/` has .srp files
- Verify files don't have `.version_processed` markers

### Password errors
- Confirm password is ``
- Check if pyzipper is installed: `uv pip list | grep pyzipper`

### S3 upload failures
- Verify AWS credentials: `aws sts get-caller-identity`
- Check bucket exists: `aws s3 ls s3://local-packages-bucket/`
- Confirm permissions

### XML parsing errors
- Verify XML files are well-formed
- Check encoding (should be UTF-8)
- Look for corrupted files

---

## Monitoring

### Prefect UI

Start Prefect server to view flow runs:

```bash
task prefect:server
# Open http://localhost:4200
```

### Logs

Flow logs show detailed progress:
- File discovery
- Extraction status
- Processing results
- S3 upload confirmation
- Manifest updates

---

## Next Steps

1. **Scheduled Execution:** Set up Prefect schedules for automatic runs
2. **Data Quality:** Add validation rules for program versions
3. **Analytics:** Create Athena tables for querying manifests
4. **Alerting:** Configure notifications for failed runs
5. **Archival:** Set up S3 lifecycle policies

---

## Support

For issues or questions:
- Check flow documentation (linked above)
- Review Prefect logs
- Inspect S3 manifests for data quality
- Verify configuration settings

---

## File Locations

### Flows
- `src/kiro_insbridge/prefect/dags/srp-zip/hourly.py`
- `src/kiro_insbridge/prefect/dags/version-export/hourly.py`

### Repositories
- `src/kiro_insbridge/enterprise_rating/repository/srp_header_repository.py`
- `src/kiro_insbridge/enterprise_rating/repository/program_version_repository.py`

### Entities
- `src/kiro_insbridge/enterprise_rating/entities/srp_request.py`
- `src/kiro_insbridge/enterprise_rating/entities/program_version.py`

### Configuration
- `project_config_insbridge.yml`
- `Taskfile.yml`
