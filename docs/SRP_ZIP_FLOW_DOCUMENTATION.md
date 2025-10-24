# SRP Zip to S3 Flow Documentation

## Overview

The **SRP Zip to S3 Flow** processes SoftRater Package (SRP) files, extracts metadata, and uploads them to Amazon S3 with date-based partitioning.

**Flow File:** `src/kiro_insbridge/prefect/dags/srp-zip/hourly.py`

**Password:** `SrpT3st2025!`

---

## Flow Architecture

### 1. **Input Processing**

The flow monitors the inbox directory for two types of files:

- **Regular .srp files**: Individual package files
- **Release .srp files**: Batch releases containing multiple nested .srp files (identified by `_REL_` in filename)

**Inbox Location:** `data/srps/inbox/`

### 2. **Release Zip Handling**

When a release zip (e.g., `SRP_BATCH_REL_20251024_014256.srp`) is detected:

1. Extract all nested .srp files from the release
2. Move nested files to inbox with `extracted_` prefix
3. Archive the release zip to `data/srps/inbox/processed_releases/`

### 3. **Individual SRP Processing**

For each .srp file (regular or extracted from release):

#### Step 1: Extract
- Unzip to staging directory
- Password: `SrpT3st2025!`
- **Staging Location:** `data/srps/staging/{filename}/`

#### Step 2: Parse Header
- Locate `header.xml` inside the extracted folder
- Parse XML metadata using `SrpHeaderRepository`
- Extract SRP request details and user information

#### Step 3: Compute Date
- Parse `date_created` field from header
- Format: `"3/31/2025 1:51:06 PM_Property"`
- Used for date-based S3 partitioning (YYYY/MM/DD)

#### Step 4: Mirror Locally
- Copy all files from staging to output directory
- **Output Location:** `data/srps/output/YYYY/MM/DD/`

#### Step 5: Upload to S3
- Upload all files to S3 with date partitioning
- Apply metadata tags from SRP header

#### Step 6: Write Manifest
- Append NDJSON record to `_manifest.ndjson`

---

## S3 Structure

### Bucket Configuration

**Environment:** `local`
- **Bucket:** `local-packages-bucket`
- **Prefix:** `srp-data`
- **Region:** `us-east-2`

### S3 Path Structure

```
s3://local-packages-bucket/srp-data/
├── 2025/
│   ├── 03/
│   │   ├── 31/
│   │   │   ├── 1_118_0_1224_9.0000/
│   │   │   │   ├── header.xml
│   │   │   │   ├── srp.xml
│   │   │   │   ├── signature
│   │   │   │   ├── rtd/...
│   │   │   │   └── rto/...
│   │   │   ├── 1_118_0_1660_1.0000/...
│   │   │   ├── ...
│   │   │   └── _manifest.ndjson  ← Metadata index
```

---

## Manifest File

### Location

```
s3://local-packages-bucket/srp-data/YYYY/MM/DD/_manifest.ndjson
```

**Example:** `s3://local-packages-bucket/srp-data/2025/03/31/_manifest.ndjson`

### Format

NDJSON (Newline-Delimited JSON) - one JSON object per line per processed SRP file.

### Manifest Record Structure

Each line contains:

```json
{
  "srp_header": {
    "schema": "4.1",
    "prog_key": "",
    "build_type": "2",
    "location": "POC",
    "carrier_id": "1",
    "carrier_name": "",
    "line_id": "118",
    "line_desc": "",
    "schema_id": "0",
    "program_id": "",
    "program_name": "",
    "version_desc": "",
    "program_version": "9.0000",
    "parent_company": "",
    "notes": null,
    "date_created": ""
  },
  "srpuser": {
    "user_name": "",
    "full_name": "",
    "email_address": ""
  },
  "s3_objects": [
    "srp-data/2025/03/31/1_118_0_1224_9.0000/header.xml",
    "srp-data/2025/03/31/1_118_0_1224_9.0000/srp.xml",
    "srp-data/2025/03/31/1_118_0_1224_9.0000/signature",
    "srp-data/2025/03/31/1_118_0_1224_9.0000/2BFD728983.xml",
    "srp-data/2025/03/31/1_118_0_1224_9.0000/rt_summary.xml"
  ]
}
```

### Key Fields

| Field | Description | Example |
|-------|-------------|---------|
| `prog_key` | Unique program key | `""` |
| `program_id` | Program identifier | `""` |
| `program_name` | Descriptive program name | `""` |
| `program_version` | Version number | `""` |
| `line_desc` | Line of business | `""` |
| `carrier_name` | Insurance carrier | `""` |
| `date_created` | Creation timestamp | `""` |
| `s3_objects` | List of S3 keys for all files in this package | Array of strings |

---

## Accessing Data in AWS Console

### Method 1: Direct S3 Console Link

1. **Go to S3 Console:**
   ```
   https://s3.console.aws.amazon.com/s3/buckets/local-packages-bucket?region=us-east-2&prefix=srp-data/
   ```

2. **Navigate to specific date:**
   - Click: `2025/` → `03/` → `31/`
   - View all processed SRP packages for March 31, 2025

3. **Download manifest:**
   - Look for `_manifest.ndjson` in the date folder
   - Click filename → Download

### Method 2: AWS CLI

```bash
# List all files for a specific date
aws s3 ls s3://local-packages-bucket/srp-data/2025/03/31/ --recursive

# Download manifest
aws s3 cp s3://local-packages-bucket/srp-data/2025/03/31/_manifest.ndjson .

# Download entire date folder
aws s3 sync s3://local-packages-bucket/srp-data/2025/03/31/ ./local-download/
```

### Method 3: S3 Browser

Use S3 Browser application:
- **Bucket:** `local-packages-bucket`
- **Path:** `srp-data/2025/03/31/`

---

## Configuration

### Environment Variables

Set these in `.env.local` or environment:

```bash
# S3 Configuration
S3_BUCKET=local-packages-bucket
S3_PREFIX=srp-data
S3_REGION=us-east-2
AWS_PROFILE=your-profile-name

# SRP Processing
SRP_ZIP_PASSWORD=SrpT3st2025!
ENVIRONMENT=local
```

### Config File

**File:** `project_config_insbridge.yml`

```yaml
local:
  catalog_name: "local_catalog"
  schema_name: "srp_data"
  pipeline_id: "srp-zip-hourly"
```

---

## Running the Flow

### Manual Execution

```bash
# Using task runner
task flow:hourly:dev

# Direct execution
uv run python ./src/kiro_insbridge/prefect/dags/srp-zip/hourly.py
```

### Creating Test Release Zips

Use the provided script to create release zips:

```bash
# Create release zip from extracted folders
./scripts/zip_release.sh
```

Output: `~/Downloads/SRP/srp_for_oracle/Releases/SRP_BATCH_REL_[timestamp].srp`

---

## Monitoring & Logs

### Flow Output

The flow logs show:
- Number of files found (regular vs release)
- Extraction progress
- S3 upload status
- Manifest write confirmation

Example log:
```
03:05:09.188 | INFO | Flow run 'brown-shellfish' - Processing 111 total zip(s): 74 regular + 37 from releases
03:05:10.634 | INFO | Task run 'upload_dir_to_s3-7a8' - Uploaded 1 objects to s3://local-packages-bucket/srp-data/2025/03/31
03:05:10.918 | INFO | Task run 'write_manifest_ndjson-175' - Finished in state Completed()
```

### Tracking Processed Files

Processed release zips are moved to:
```
data/srps/inbox/processed_releases/
```

---

## Data Quality & Validation

### Pydantic Validation

All SRP headers are validated using Pydantic models:
- **Model:** `SrpRequest` (15+ fields)
- **User Model:** `SrpRequestUser` (3 fields)

### Error Handling

- Missing fields are logged but don't fail the flow (all fields are optional)
- Invalid XML structure raises an error
- Missing `header.xml` raises `FileNotFoundError`

---

## Query Examples

### Athena/SQL Query on Manifest

```sql
-- Create external table on manifest files
CREATE EXTERNAL TABLE srp_manifest (
  srp_header STRUCT<
    prog_key: STRING,
    program_id: STRING,
    program_name: STRING,
    program_version: STRING,
    line_desc: STRING,
    carrier_name: STRING,
    date_created: STRING
  >,
  s3_objects ARRAY<STRING>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://local-packages-bucket/srp-data/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Query for specific program
SELECT
  srp_header.prog_key,
  srp_header.program_name,
  srp_header.program_version,
  cardinality(s3_objects) as file_count
FROM srp_manifest
WHERE srp_header.program_id = '1224'
  AND date_partition = '2025/03/31';
```

---

## Troubleshooting

### Common Issues

1. **"No files found"**
   - Check inbox directory: `data/srps/inbox/`
   - Verify file pattern: `*.srp`

2. **"Password incorrect"**
   - Verify password in config: ``
   - Check if using AES encryption (requires `pyzipper`)

3. **"S3 upload failed"**
   - Verify AWS credentials
   - Check bucket permissions
   - Confirm bucket exists: `local-packages-bucket`

4. **"XML parsing error"**
   - Verify `header.xml` exists in zip
   - Check XML structure (root should be `<env>`)

---

## Complete Pipeline

This flow is **Stage 1** of a two-stage pipeline:

### Stage 1: SRP Zip to S3 Flow (This Flow)
- **Purpose:** Extract SRP packages and upload to S3
- **Input:** .srp files in `data/srps/inbox/`
- **Output:** SRP package files in `s3://local-packages-bucket/srp-data/YYYY/MM/DD/`
- **Manifest:** Metadata about uploaded packages

### Stage 2: Version Export Flow
- **Purpose:** Extract and process program versions (RTE XML)
- **Input:** Same .srp files from inbox
- **Output:** Program version JSON in `s3://local-packages-bucket/program-versions/YYYY/MM/DD/`
- **Manifest:** Metadata about program versions
- **Documentation:** See [VERSION_EXPORT_FLOW_DOCUMENTATION.md](VERSION_EXPORT_FLOW_DOCUMENTATION.md)

### Running the Complete Pipeline

```bash
# Run both flows sequentially
task flow:pipeline:dev

# Or run individually
task flow:srp-zip:dev
task flow:version-export:dev
```

### Data Outputs

After running the complete pipeline, you'll have:

```
s3://local-packages-bucket/
├── srp-data/                    ← SRP packages (headers, files)
│   └── 2025/03/31/
│       ├── 1_118_0_1224_9.0000/
│       │   ├── header.xml
│       │   ├── srp.xml
│       │   ├── signature
│       │   └── ...
│       └── _manifest.ndjson
│
└── program-versions/            ← Program versions (RTE XML processed)
    └── 2025/03/31/
        ├── extracted_2_1_118_0_891_1.0000.json
        ├── extracted_1_1_1_0_758_1.0000.json
        └── _manifest.ndjson
```

---

## Next Steps

- Set up scheduled Prefect deployment for hourly runs
- Configure S3 lifecycle policies for archival
- Add Athena table for querying manifests
- Implement data quality checks
- Add alerting for failed runs
- Integrate version export flow into automated pipeline
