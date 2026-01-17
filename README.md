# match-datacite-affiliations-to-ror-ids

CLI tool to extract the unique affiliation strings from the DataCite data, query them against the ROR API, and reconcile matches back to DOI/author records.

## Installation

### Prerequisites

- [Obtain the DataCite public data file](https://support.datacite.org/docs/datacite-public-data-file)
- [Setup a local ROR API instance](https://github.com/ror-community/ror-api)

### Build

```bash
cd datacite-ror
cargo build --release
```

## Usage

The tool provides three subcommands that form a pipeline:

1. `extract` - Extract unique affiliations from DataCite JSONL files
2. `query` - Query affiliations against the ROR API
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

Query affiliations against the ROR API to find organization matches.

```bash
datacite-ror query --input <DIR> --output <DIR> [OPTIONS]
```

#### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Working directory (reads `unique_affiliations.json`) | Required |
| `--output` | `-o` | Working directory (writes match files) | Required |
| `--base-url` | `-u` | ROR API base URL | `http://localhost:9292` |
| `--concurrency` | `-c` | Concurrent requests | 50 |
| `--timeout` | `-t` | Request timeout in seconds | 30 |
| `--resume` | `-r` | Resume from checkpoint | false |
| `--fallback-multi` | `-f` | Enable fallback to standard affiliation endpoint | false |

#### Output Files

- `ror_matches.jsonl` - Successful ROR matches
- `ror_matches.failed.jsonl` - Failed queries (no match or errors)
- `ror_matches.checkpoint` - Checkpoint file for resuming

#### Example

```bash
datacite-ror query \
  --input /work/affiliations \
  --output /work/affiliations \
  --base-url http://localhost:9292 \
  --concurrency 50 \
  --resume
```

### Reconcile Command

Reconcile ROR matches back to DOI/author records, producing enriched DataCite-compatible output.

```bash
datacite-ror reconcile --input <DIR> --output <FILE> [OPTIONS]
```

#### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Working directory (reads relationship and match files) | Required |
| `--output` | `-o` | Output file path | `enriched_records.jsonl` |

#### Input Files Required

- `doi_author_affiliations.jsonl` - From extract step
- `ror_matches.jsonl` - From query step

#### Output Format

The output is a JSONL file where each line contains an enriched record:

```json
{
  "doi": "10.1234/example",
  "creators": [
    {
      "name": "Jane Smith",
      "affiliation": [
        {
          "name": "Example University",
          "affiliationIdentifier": "https://ror.org/0123456789",
          "affiliationIdentifierScheme": "ROR",
          "schemeUri": "https://ror.org"
        }
      ]
    }
  ]
}
```

#### Example

```bash
datacite-ror reconcile \
  --input /work/affiliations \
  --output /work/enriched_records.jsonl
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

# Step 2: Query ROR API (with checkpoint support for large datasets)
datacite-ror query \
  --input $WORK_DIR \
  --output $WORK_DIR \
  --concurrency 50 \
  --timeout 60 \
  --resume

# Step 3: Reconcile matches to create enriched records
datacite-ror reconcile \
  --input $WORK_DIR \
  --output $WORK_DIR/enriched_datacite_records.jsonl
```

## Intermediate File Formats

### doi_author_affiliations.jsonl

Each line contains a relationship record:

```json
{
  "doi": "10.1234/example",
  "author_idx": 0,
  "author_name": "Jane Smith",
  "affiliation_idx": 0,
  "affiliation": "Example University, City, Country",
  "affiliation_hash": "a1b2c3d4e5f67890"
}
```

### ror_matches.jsonl

Each line contains a successful match:

```json
{
  "affiliation": "Example University, City, Country",
  "affiliation_hash": "a1b2c3d4e5f67890",
  "ror_id": "https://ror.org/0123456789"
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
