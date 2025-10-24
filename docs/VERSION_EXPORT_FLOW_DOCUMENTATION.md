# Version Export Flow Documentation

## Overview

The **Version Export Flow** processes SoftRater Package (SRP) files to extract and upload program version data (RTE XML) to Amazon S3 with date-based partitioning.

**Flow File:** `src/kiro_insbridge/prefect/dags/version-export/hourly.py`

**Password:** ``

---

## Flow Architecture

### 1. **Input Processing**

The flow monitors the inbox directory for .srp files that haven't been processed yet.

**Inbox Location:** `data/srps/inbox/`

**Processing Marker:** `.srp.version_processed` files indicate completed processing

### 2. **SRP File Extraction**

For each unprocessed .srp file:

#### Step 1: Extract to Temp Directory
- Extract .srp file (password-protected zip) to thread-specific temp directory
- Password: ``
- **Temp Location:** `/tmp/srp_{filename}_XXXXX/`

#### Step 2: Find RTE XML File
- Locate the RTE XML file (Rating Engine XML)
- Skip known files: `header.xml`, `srp.xml`, `rt_summary.xml`
- RTE files are named like: `AEBB715809.xml`, `84EA728349.xml`, `5511719666.xml`

#### Step 3: Process Program Version
- Parse RTE XML using `ProgramVersionRepository.get_program_version_from_path()`
- Extract program metadata, algorithms, dependencies, and instructions
- Convert to JSON-serializable dictionary

#### Step 4: Extract Date
- Get effective date from program version data
- Supports formats: ISO (`2025-03-31T13:26:32`), US (`03/31/2025`), etc.
- Used for date-based S3 partitioning (YYYY/MM/DD)

#### Step 5: Save Locally
- Save program version JSON next to .srp file
- **Filename:** `{original}.srp.version.json`

#### Step 6: Upload to S3
- Upload program version JSON to S3 with date partitioning
- Apply metadata to manifest

#### Step 7: Write Manifest
- Append NDJSON record to `_manifest.ndjson`

#### Step 8: Cleanup
- Remove temporary extraction directory
- Mark .srp file as processed (`.srp.version_processed`)

---

## S3 Structure

### Bucket Configuration

**Environment:** `local`
- **Bucket:** `local-packages-bucket`
- **Prefix:** `program-versions`
- **Region:** `us-east-2`

### S3 Path Structure

```
s3://local-packages-bucket/program-versions/
├── 2025/
│   ├── 03/
│   │   ├── 31/
│   │   │   ├── extracted_2_1_118_0_891_1.0000.json
│   │   │   ├── extracted_1_1_1_0_758_1.0000.json
│   │   │   ├── extracted_3_1_118_0_611_296.0000.json
│   │   │   ├── ...
│   │   │   └── _manifest.ndjson  ← Metadata index
```

---

## Program Version Structure

### What's in the RTE XML?

The RTE (Rating Engine) XML file contains the complete program logic:

- **Program Metadata**: Primary key, version, effective date, line of business
- **Data Dictionary**: Input variables, categories, table variables
- **Algorithm Sequences**: Ordered algorithm execution
- **Algorithms**: Business logic with dependencies and instructions
- **Calculated Variables**: Nested dependencies with their own instruction sets
- **Instructions**: Step-by-step execution logic with AST (Abstract Syntax Tree) representation

### JSON Output Example

Each program version JSON file contains:

```json
{
  "primary_key": "CEE9728349",
  "program_id": "891",
  "version": "1.0000",
  "line": "118",
  "subscriber": "1",
  "effective_date": "2025-03-31T13:26:32",
  "version_name": "Manual Override",
  "global_primary_key": "84EA728349",
  "persisted": false,
  "data_dictionary": {
    "categories": [...],
    "inputs": [...]
  },
  "algorithm_seq": [
    {
      "sequence_number": "1",
      "algorithm": [
        {
          "prog_key": "CEE9728349",
          "index": "110",
          "description": "Manual Override Flag",
          "dependency_vars": [...],
          "steps": [
            {
              "n": "1",
              "t": "3",
              "ins": "Set ManualOverride = 1",
              "ast": [...]
            }
          ]
        }
      ]
    }
  ]
}
```

---

## Manifest File

### Location

```
s3://local-packages-bucket/program-versions/YYYY/MM/DD/_manifest.ndjson
```

**Example:** `s3://local-packages-bucket/program-versions/2025/03/31/_manifest.ndjson`

### Format

NDJSON (Newline-Delimited JSON) - one JSON object per line per processed program version.

### Manifest Record Structure

Each line contains:

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

### Key Fields

| Field | Description | Example |
|-------|-------------|---------|
| `s3_key` | Full S3 key to program version file | `""` |
| `primary_key` | Unique program version key | `""` |
| `program_id` | Program identifier | `""` |
| `version` | Version number | `""` |
| `line` | Line of business ID | `""` (Property) or `"1"` (Auto) |
| `carrier` | Insurance carrier ID | `"1"` () |
| `effective_date` | When this version became effective | `""` |

---

## Accessing Data in AWS Console

### Method 1: Direct S3 Console Link

1. **Go to S3 Console:**
   ```
   https://s3.console.aws.amazon.com/s3/buckets/local-packages-bucket?region=us-east-2&prefix=program-versions/
   ```

2. **Navigate to specific date:**
   - Click: `2025/` → `03/` → `31/`
   - View all processed program versions for March 31, 2025

3. **Download manifest:**
   - Look for `_manifest.ndjson` in the date folder
   - Click filename → Download

### Method 2: AWS CLI

```bash
# List all program versions for a specific date
aws s3 ls s3://local-packages-bucket/program-versions/2025/03/31/ --recursive

# Download manifest
aws s3 cp s3://local-packages-bucket/program-versions/2025/03/31/_manifest.ndjson .

# Download entire date folder
aws s3 sync s3://local-packages-bucket/program-versions/2025/03/31/ ./local-download/

# Download specific program version
aws s3 cp s3://local-packages-bucket/program-versions/2025/03/31/extracted_2_1_118_0_891_1.0000.json .
```

### Method 3: S3 Browser

Use S3 Browser application:
- **Bucket:** `local-packages-bucket`
- **Path:** `program-versions/2025/03/31/`

---

## Configuration

### Environment Variables

Set these in `.env.local` or environment:

```bash
# S3 Configuration
S3_BUCKET=local-packages-bucket
S3_REGION=us-east-2
AWS_PROFILE=your-profile-name

# SRP Processing
SRP_ZIP_PASSWORD=
ENVIRONMENT=local
```

### Config File

**File:** `project_config_insbridge.yml`

```yaml
local:
  catalog_name: "local_catalog"
  schema_name: "srp_data"
  pipeline_id: "version-export-hourly"
```

---

## Running the Flow

### Manual Execution

```bash
# Using task runner
task flow:version-export:dev

# Direct execution
uv run python ./src/kiro_insbridge/prefect/dags/version-export/hourly.py
```

### Running Complete Pipeline

```bash
# Run both SRP zip and version export flows
task flow:pipeline:dev
```

This will:
1. Process .srp files and upload to `srp-data/` prefix
2. Extract program versions and upload to `program-versions/` prefix

---

## Monitoring & Logs

### Flow Output

The flow logs show:
- Number of unprocessed files found
- Extraction progress
- RTE XML file discovery
- Program version processing
- S3 upload status
- Manifest write confirmation

Example log:
```
04:51:33.116 | INFO | Task run 'find_unprocessed_srp_files-9f6' - Found 148 unprocessed .srp file(s)
04:51:33.293 | INFO | Task run 'find_rte_xml_file-588' - Found RTE XML file: 84EA728349.xml
04:51:33.402 | INFO | Task run 'process_program_version-c72' - Successfully processed program version: CEE9728349
04:51:41.679 | INFO | Task run 'upload_version_to_s3-112' - Uploaded program version to s3://local-packages-bucket/program-versions/2025/03/31/extracted_2_1_118_0_891_1.0000.json
04:51:42.580 | INFO | Task run 'append_to_version_manifest-081' - Updated manifest: s3://local-packages-bucket/program-versions/2025/03/31/_manifest.ndjson
```

### Tracking Processed Files

Processed .srp files have marker files:
```
data/srps/inbox/extracted_2_1_118_0_891_1.0000.srp.version_processed
```

---

## Data Quality & Validation

### Pydantic Validation

All program versions are validated using Pydantic models:
- **Model:** `ProgramVersion` (comprehensive program structure)
- **Sub-models:** `Algorithm`, `DependencyBase`, `Instruction`, `Category`, `InputVariable`

### AST Processing

Instructions are decoded into Abstract Syntax Tree (AST) representations:
- Assignment nodes
- Arithmetic nodes
- Comparison nodes
- Function call nodes
- Conditional (if/then) nodes

### Error Handling

- Missing RTE XML file logs warning and skips
- Invalid XML structure raises an error
- Failed parsing logs error and continues to next file
- Temp directories are always cleaned up

---

## Query Examples

### Athena/SQL Query on Manifest

```sql
-- Create external table on manifest files
CREATE EXTERNAL TABLE program_version_manifest (
  s3_key STRING,
  primary_key STRING,
  program_id STRING,
  version STRING,
  line STRING,
  carrier STRING,
  effective_date STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://local-packages-bucket/program-versions/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Query for specific program
SELECT
  primary_key,
  program_id,
  version,
  effective_date,
  s3_key
FROM program_version_manifest
WHERE program_id = '891'
  AND effective_date LIKE '2025-03-31%';

-- Count versions by line of business
SELECT
  line,
  COUNT(*) as version_count
FROM program_version_manifest
GROUP BY line;
```

### Python Query Example

```python
import json
import boto3

s3 = boto3.client('s3', region_name='us-east-2')

# Read manifest
response = s3.get_object(
    Bucket='local-packages-bucket',
    Key='program-versions/2025/03/31/_manifest.ndjson'
)

# Parse NDJSON
for line in response['Body'].read().decode('utf-8').splitlines():
    record = json.loads(line)
    print(f"Program {record['program_id']} v{record['version']}: {record['s3_key']}")

# Download specific program version
response = s3.get_object(
    Bucket='local-packages-bucket',
    Key='program-versions/2025/03/31/extracted_2_1_118_0_891_1.0000.json'
)
program_version = json.loads(response['Body'].read().decode('utf-8'))

print(f"Primary Key: {program_version['primary_key']}")
print(f"Algorithms: {len(program_version['algorithm_seq'])}")
```

---

## Understanding Program Versions

### What is a Program Version?

A program version represents a specific version of rating logic for an insurance product. It contains:

1. **Program Metadata**: Identifies what product, carrier, and version
2. **Input Variables**: What data the program needs (e.g., coverage amount, ZIP code)
3. **Categories**: Groupings for variables (e.g., "Risk Factors", "Discounts")
4. **Algorithms**: Business logic that calculates rates, applies rules, etc.
5. **Instructions**: Step-by-step execution logic in a domain-specific language

### How Programs Execute

1. **Inputs are provided** (e.g., policy details, driver information)
2. **Algorithms run in sequence** (defined in `algorithm_seq`)
3. **Each algorithm has instructions** that perform calculations
4. **Dependencies are resolved** (some algorithms depend on outputs from others)
5. **Final outputs are produced** (premium, eligibility, scores, etc.)

### Use Cases

- **Rate calculation**: Compute insurance premiums
- **Eligibility determination**: Check if customer qualifies
- **Discount/surcharge application**: Apply pricing adjustments
- **Risk scoring**: Assess underwriting risk
- **Compliance rules**: Enforce regulatory requirements

---

## Troubleshooting

### Common Issues

1. **"No unprocessed .srp files found"**
   - Check inbox directory: `data/srps/inbox/`
   - Verify file pattern: `*.srp`
   - Remove `.srp.version_processed` markers to reprocess

2. **"No RTE XML file found"**
   - Verify .srp file contains an XML file besides header.xml, srp.xml, rt_summary.xml
   - Check if file extracted correctly
   - Verify password: ``

3. **"S3 upload failed"**
   - Verify AWS credentials
   - Check bucket permissions
   - Confirm bucket exists: `local-packages-bucket`
   - Check AWS region: `us-east-2`

4. **"Could not parse effective_date"**
   - Program version uses unsupported date format
   - Falls back to current date for S3 partitioning
   - Data is still saved correctly, just partitioned by current date

5. **"XML parsing error"**
   - Verify RTE XML is well-formed
   - Check for XML encoding issues
   - Ensure file is not corrupted

---

## Pipeline Integration

### Two-Stage Architecture

The complete pipeline consists of two flows:

**Stage 1: SRP Zip Flow** (`srp-zip/hourly.py`)
- Extracts .srp files
- Parses header metadata
- Uploads files to `srp-data/` prefix
- Creates .srtp export packages

**Stage 2: Version Export Flow** (`version-export/hourly.py`) ← **This Flow**
- Extracts .srp files (from inbox)
- Finds RTE XML files
- Processes program versions
- Uploads to `program-versions/` prefix
- Creates program version manifests

### Running Both Flows

```bash
# Sequential execution
task flow:pipeline:dev

# Or run individually
task flow:srp-zip:dev
task flow:version-export:dev
```

---

## Next Steps

- Set up scheduled Prefect deployment for hourly runs
- Configure S3 lifecycle policies for archival
- Add Athena table for querying program versions
- Implement data quality checks on AST completeness
- Add alerting for failed runs
- Create analytics dashboard for program version trends

---

## Technical Details

### Repository Used

**`ProgramVersionRepository.get_program_version_from_path()`**

This method:
- Parses RTE XML using xmltodict
- Maps XML attributes to Pydantic models
- Processes all instructions to generate AST
- Handles nested dependencies (calculated variables)
- Returns fully validated `ProgramVersion` object

### AST Decoder

Instructions are decoded using `decode_ins()` which:
- Parses domain-specific language (DSL)
- Creates AST nodes (Assignment, Arithmetic, Compare, Function, If)
- Resolves variable references
- Handles nested expressions
- Provides structured representation of business logic

### File Locations

- **Flow:** `src/kiro_insbridge/prefect/dags/version-export/hourly.py`
- **Repository:** `src/kiro_insbridge/enterprise_rating/repository/program_version_repository.py`
- **Entity Models:** `src/kiro_insbridge/enterprise_rating/entities/program_version.py`
- **AST Decoder:** `src/kiro_insbridge/enterprise_rating/ast_decoder/`
- **Config:** `project_config_insbridge.yml`
