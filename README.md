# Ingest from Source

A Python-based tool for ingesting CSV data into a PostgreSQL database.

## Features

- Creates database schemas and tables if they do not already exist.
  - **Recommended:** Tables are created using argument defined data dictionaries. Uses the `copy_data` command.
  - Tables are created by reading the data file header row. Use the `copy_data_wo_format` command.
- _Fully refreshes data_. Truncates existing data, before copying new data into an existing table.
- Performs column and row count checks to ensure data integrity between the source file and the database.
- Records every ingestion attempt (success or failure) into a `ingestion_log` table within the target schema.
- Configure database connections and AWS credentials via environment variables, or command-line arguments.

## Requirements

- Python 3.12+
- An accessible PostgreSQL database.
- Using a pyenv environment is recommended.
- Data Dictionary requirements 
    - Assumes the exact field names 'variable_name' and 'data_type' are present.


## Installation

Install the package from GitHub:

```bash
pip install git@github.com:carrollaboratory/inc-kf-src-ingest.git
```

Or install locally from source:

```bash
git clone git@github.com:carrollaboratory/inc-kf-src-ingest.git
cd inc-kf-src-ingest
pip install .
```

## Configuration

The scripts can be configured using environment variables.

### Environment Variables

- `DB_USER`: Database user
- `DB_PASSWORD`: Database password
- `DB_HOST`: Database host
- `DB_PORT`: Database port
- `DB_NAME`: Database name
- `DB_SCHEMA`: The target database schema for table creation and ingestion.
- `LOCAL_PORT`: (Optional) If you are using an SSH tunnel, specify the local port here. The script will automatically connect to `localhost` on this port.
- `AWS_ACCESS_KEY_ID`: (Optional) Your AWS access key for S3 access.
- `AWS_SECRET_ACCESS_KEY`: (Optional) Your AWS secret key for S3 access.
- `AWS_SESSION_TOKEN`: (Optional) Your AWS session token for S3 access.
---

## Commands

### 1. `copy_data`

This command ingests a data file using a corresponding data dictionary to define the table structure.

**Usage:**

```bash
copy_data -dd {data_dictionary_path} -df {datafile_path} [options]
```

**Arguments:**

- `-dd`, `--data-dictionary-path`: (Required) The local or S3 path to the data dictionary CSV file.
- `-df`, `--datafile-path`: (Required) The local or S3 path to the data file to be ingested.
- `--db-user`: Database user.
- `--db-password`: Database password.
- `--db-host`: Database host.
- `--db-port`: Database port.
- `--db-name`: Database name.
- `--db-schema`: Target database schema.
- `--local-port`: (Optional) Local port for an SSH tunnel.
- `--aws-access-key-id`, `--aws-secret-access-key`, `--aws-session-token`: (Optional) AWS credentials for S3 access.


### 2. `copy_data_wo_format`

This command ingests a data file without a data dictionary. It infers the table schema from the header row of the CSV file, treating all columns as `TEXT`.

**Usage:**

```bash
# Example assumes all other required arguments are set environment variables
copy_data_wo_format -df "s3://my_bucket/data.csv"
```

**Arguments:**

- `-df`, `--datafile-path`: (Required) The local or S3 path to the data file to be ingested.
- `--db-user`: Database user.
- `--db-password`: Database password.
- `--db-host`: Database host.
- `--db-port`: Database port.
- `--db-name`: Database name.
- `--db-schema`: Target database schema.
- `--local-port`: (Optional) Local port for an SSH tunnel.
- `--aws-access-key-id`, `--aws-secret-access-key`, `--aws-session-token`: (Optional) AWS credentials for S3 access.