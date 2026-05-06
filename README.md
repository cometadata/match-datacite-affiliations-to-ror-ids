# match-datacite-affiliations-to-ror-ids

CLI tool to extract the unique affiliation strings from the DataCite data, match them against ROR IDs via a match service, 
and reconcile matches back to DOI/author records. Affiliation matching is performed via the [`affiliation-single-search`](https://gitlab.com/crossref/marple/-/blob/main/crossref_matcher/strategies/affiliation/single_search/strategy.py) 
strategy from the Crossref matching service ([Marple](https://gitlab.com/crossref/marple)).

## Installation

### Prerequisites

- [DataCite public data file](https://support.datacite.org/docs/datacite-public-data-file)
- A running [Marple](https://gitlab.com/crossref/marple) matching service instance, indexed with the most recent ROR data, exposing `GET /match?task=affiliation&input=<name>`
- [ROR data dump](https://ror.readme.io/docs/data-dump) (for resolving ROR IDs to organization names in the reconcile step)

### Build

```bash
cd datacite-ror
cargo build --release
```

## Usage

The tool provides three subcommands that form a pipeline:

1. `extract` - Extract unique affiliations from DataCite JSONL files
2. `query` - Match affiliations against the Marple match service
3. `reconcile` - Reconcile ROR matches back to DOI/author records

### Options

```
-v, --verbose    Enable verbose logging
-h, --help       Print help
-V, --version    Print version
```

### Extract Command

Extract unique affiliations and DOI/author relationships from DataCite `.jsonl.gz` files.

```bash
datacite-ror extract --input <DIR> --output <DIR> [OPTIONS]
```

#### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Directory containing `.jsonl.gz` files | Required |
| `--output` | `-o` | Working directory for output files | Required |
| `--threads` | `-t` | Number of threads (0 = auto) | 0 |
| `--batch-size` | `-b` | Records per batch | 5000 |

#### Output Files

- `unique_affiliations.json` - JSON array of unique affiliation strings
- `doi_author_affiliations.jsonl` - JSONL file with DOI/author/affiliation relationships

#### Example

```bash
datacite-ror extract \
  --input /data/datacite/2024 \
  --output /work/affiliations \
  --threads 8
```

### Query Command

Match affiliations against the Marple match service to find ROR IDs.

```bash
datacite-ror query --input <DIR> --output <DIR> [OPTIONS]
```

#### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Working directory (reads `unique_affiliations.json`) | Required |
| `--output` | `-o` | Working directory (writes match files) | Required |
| `--base-url` | `-u` | Match service base URL | `http://localhost:8000` |
| `--task` |  | Match-service task name | `affiliation` |
| `--concurrency` | `-c` | Concurrent requests | 50 |
| `--timeout` | `-t` | Request timeout in seconds | 30 |
| `--resume` | `-r` | Resume from checkpoint | false |

#### Output Files

- `ror_matches.jsonl` - Successful ROR matches (includes `confidence`)
- `ror_matches.failed.jsonl` - Failed queries (no match or errors)
- `ror_matches.checkpoint` - Checkpoint file for resuming

#### Example

```bash
datacite-ror query \
  --input /work/affiliations \
  --output /work/affiliations \
  --base-url http://localhost:8000 \
  --concurrency 50 \
  --resume
```

### Reconcile Command

Reconcile ROR matches back to DOI/author records, producing enriched DataCite-compatible output. Handles existing ROR ID assignments and detects disagreements.

```bash
datacite-ror reconcile --input <DIR> --output <FILE> --ror-data <FILE> [OPTIONS]
```

#### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Working directory (reads relationship and match files) | Required |
| `--output` | `-o` | Output file path | `enriched_records.jsonl` or `enrichments.jsonl` |
| `--ror-data` | `-r` | Path to ROR data dump JSON file | Required |
| `--enrichment-format` | | Output in DataCite enrichment format (per-creator records) | false |
| `--enrichment-config` | | Path to enrichment config YAML file (required with `--enrichment-format`) | |

#### Input Files Required

- `doi_author_affiliations.jsonl` - From extract step
- `ror_matches.jsonl` - From query step
- ROR data dump JSON file - Download from [ROR Data Dumps](https://ror.readme.io/docs/data-dump)

#### Output Files

| File | Description |
|------|-------------|
| `enriched_records.jsonl` | DOIs enriched with ROR matches (default format, one record per DOI) |
| `enrichments.jsonl` | Per-creator enrichment records in [DataCite enrichment format](https://github.com/cometadata/datacite-enrichment) (when `--enrichment-format` is used) |
| `existing_assignments.jsonl` | Records where affiliation already had a ROR ID in source data |
| `existing_assignments_aggregated.jsonl` | Aggregated counts per affiliation/ROR ID pair |
| `disagreements.jsonl` | User disagreements (same affiliation → different ROR IDs) and match disagreements (existing differs from our match) |

#### Enrichment Config File

When using `--enrichment-format`, a YAML config file is required with `contributors` and `resources` arrays that provide provenance metadata for each enrichment record:

```yaml
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"

resources:
  - relatedIdentifier: "https://example.com/dataset"
    relatedIdentifierType: "URL"
    relationType: "IsDerivedFrom"
    resourceTypeGeneral: "Dataset"
```

#### Examples

Default format (one record per DOI):

```bash
datacite-ror reconcile \
  --input /work/affiliations \
  --output /work/enriched_records.jsonl \
  --ror-data /data/ror/v2.3-2026-02-24-ror-data.json
```

DataCite enrichment format (one record per creator):

```bash
datacite-ror reconcile \
  --input /work/affiliations \
  --ror-data /data/ror/v2.3-2026-02-24-ror-data.json \
  --enrichment-format \
  --enrichment-config enrichment_config.yaml
```

## Full Pipeline Example

Process a complete DataCite data dump:

```bash
# Set working directory
WORK_DIR=/work/datacite-ror-processing

# Step 1: Extract affiliations from DataCite files
datacite-ror extract \
  --input /data/datacite/DataCite_Public_Data_File_2024 \
  --output $WORK_DIR \
  --threads 16

# Step 2: Query Marple match service (with checkpoint support for large datasets)
datacite-ror query \
  --input $WORK_DIR \
  --output $WORK_DIR \
  --base-url http://localhost:8000 \
  --concurrency 50 \
  --timeout 60 \
  --resume

# Step 3: Reconcile matches to create enriched records
datacite-ror reconcile \
  --input $WORK_DIR \
  --output $WORK_DIR/enriched_datacite_records.jsonl \
  --ror-data /data/ror/v2.3-2026-02-24-ror-data.json

# Or, output in DataCite enrichment format
datacite-ror reconcile \
  --input $WORK_DIR \
  --ror-data /data/ror/v2.3-2026-02-24-ror-data.json \
  --enrichment-format \
  --enrichment-config enrichment_config.yaml
```

## Intermediate File Formats

### doi_author_affiliations.jsonl

Each line contains a relationship record:

```json
{
  "doi": "10.1234/example",
  "author_idx": 0,
  "author_name": "Jane Smith",
  "author_name_type": "Personal",
  "author_given_name": "Jane",
  "author_family_name": "Smith",
  "affiliation_idx": 0,
  "affiliation": "Example University, City, Country",
  "affiliation_hash": "a1b2c3d4e5f67890",
  "existing_ror_id": "https://ror.org/0123456789"
}
```

The `author_name_type`, `author_given_name`, `author_family_name`, and `existing_ror_id` fields are optional and omitted when not present in the source data.

### ror_matches.jsonl

Each line contains a successful match:

```json
{
  "affiliation": "Example University, City, Country",
  "affiliation_hash": "a1b2c3d4e5f67890",
  "ror_id": "https://ror.org/0123456789",
  "confidence": 0.92
}
```

### ror_matches.failed.jsonl

Each line contains a failed query:

```json
{
  "affiliation": "Unknown Organization",
  "affiliation_hash": "f0e1d2c3b4a59687",
  "error": "No match found"
}
```


## Checkpointing

The query command supports checkpointing for long-running jobs:

- Progress is saved to `ror_matches.checkpoint`
- Use `--resume` flag to continue from where you left off
- Checkpoint tracks processed affiliations by hash

## Convert & Upload

`scripts/convert_and_upload.py` converts pipeline output files to Parquet and uploads them to HuggingFace as a dataset.

### Install dependencies

```bash
uv pip install "pyarrow>=14.0.0" "huggingface_hub>=0.19.0" "orjson>=3.9.0" "tqdm>=4.66.0"
```

Or run directly with `uv` (handles deps automatically):

```bash
uv run scripts/convert_and_upload.py --stats-only --input-dir $WORK_DIR --output-dir $WORK_DIR/hf_upload
```

### Usage

```bash
# Collect statistics only
uv run scripts/convert_and_upload.py --stats-only --input-dir $WORK_DIR --output-dir $WORK_DIR/hf_upload

# Convert to Parquet without uploading
uv run scripts/convert_and_upload.py --convert-only --input-dir $WORK_DIR --output-dir $WORK_DIR/hf_upload

# Convert and upload to HuggingFace
uv run scripts/convert_and_upload.py --input-dir $WORK_DIR --output-dir $WORK_DIR/hf_upload --token $HF_TOKEN
```
