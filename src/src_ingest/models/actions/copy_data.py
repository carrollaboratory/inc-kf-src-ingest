"""
This script copies data from a CSV file into a PostgreSQL database 
using a data dictionary for schema definition. 

It handles database connection, schema and table creation, data ingestion, 
and metadata logging. 
"""

import argparse
import os
from pathlib import Path
from src_ingest.models.file_utils import (
    get_csv_row_count, 
    open_file, 
    csv_to_dicts_chunked
)
from src_ingest.models.sql_generator import (
    gen_create_schema_query, 
    gen_create_table_query, 
    gen_truncate_table_query, 
    gen_create_metadata_table_query, 
    gen_insert_metadata_query
)
from src_ingest.models.connector import DatabaseConnection

def prepare_and_run_src_data_ingest_copy(
    data_dictionary_path: str,
    datafile_path: str,
    db_user: str,
    db_host: str,
    db_port: int,
    db_name: str,
    db_schema:str,
    db_password: str,
    local_port: int = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
):

    print("Starting data ingestion copy process...")

    aws_auth = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "aws_session_token": aws_session_token,
    }

    table_name = Path(datafile_path).stem
    # Load Data Dictionary, chunking large files if necessary.
    dd_chunks = csv_to_dicts_chunked(data_dictionary_path, **aws_auth)
    data_dictionary = list(next(dd_chunks))

    rows_ingested = 0
    was_successful = False
    details = ""
    db_conn = None

    try:
        db_conn = DatabaseConnection(
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port,
            local_port=local_port,
        )
        # Create Schema
        db_conn.execute_query(
            gen_create_schema_query(db_schema),
            query_name="Create Schema",
        )

        # Create Metadata Table
        db_conn.execute_query(
            gen_create_metadata_table_query(db_schema),
            query_name="Create Metadata Table",
        )

        # Create Table
        db_conn.execute_query(
            gen_create_table_query(table_name, data_dictionary, db_schema),
            query_name="Create Table",
        )

        # Column count validation
        dd_col_count = len(data_dictionary)
        db_col_count = db_conn.get_table_column_count(db_schema, table_name)
        if dd_col_count != db_col_count:
            raise ValueError(f"Column count mismatch: Data Dictionary has {dd_col_count} columns, but table {db_schema}.{table_name} has {db_col_count} columns.")
        print(f"✅ Column count verified: {db_col_count} columns.")

        # Truncate Table
        db_conn.execute_query(
            gen_truncate_table_query(db_schema, table_name),
            query_name="Truncate Table",
        )

        # Copy Data
        with open_file(datafile_path, 'r', **aws_auth) as f:
            next(f) 
            rows_ingested = db_conn.copy_from_file(f, f"{db_schema}.{table_name}")

        # Row count validation
        csv_row_count = get_csv_row_count(datafile_path, **aws_auth)
        if rows_ingested != csv_row_count:
            raise ValueError(f"Row count mismatch: CSV file has {csv_row_count} rows, but {rows_ingested} rows were ingested.")
        print(f"✅ Row count verified: {rows_ingested} rows.")

        was_successful = True
        details = f"Successfully ingested {rows_ingested} rows."

    except Exception as e:
        details = f"An error occurred during the ingestion process: {e}"
        print(f"❌ {details}")
        was_successful = False

    finally:
        if db_conn:
            try:
                db_conn.execute_query(
                    gen_insert_metadata_query(
                        schema=db_schema,
                        source_file_path=datafile_path,
                        target_schema=db_schema,
                        target_table=table_name,
                        rows_ingested=rows_ingested,
                        was_successful=was_successful,
                        details=details,
                    ),
                    query_name="Log Metadata",
                )
            except Exception as e:
                print(f"Failed to log metadata: {e}")
            finally:
                db_conn.close()

    print("Data ingestion copy process finished.")

def main():
    parser = argparse.ArgumentParser(
        description="Read csv files and ingest data into a PostgreSQL database."
    )
    parser.add_argument("-dd", "--data-dictionary-path", required=True, help="Path to the data dictionary.")
    parser.add_argument("-df", "--datafile-path", required=True, help="Path to the data file.")
    parser.add_argument("--db-user", help="Database user")
    parser.add_argument("--db-host", help="Database host")
    parser.add_argument("--db-port", type=int, help="Database port")
    parser.add_argument("--db-name", help="Database name")
    parser.add_argument("--db-schema", help="Database schema")
    parser.add_argument("--db-password", help="Database password")
    parser.add_argument("--local-port", type=int, help="Local port for tunnel")
    parser.add_argument("--aws-access-key-id", help="AWS access key ID for S3.")
    parser.add_argument("--aws-secret-access-key", help="AWS secret access key for S3.")
    parser.add_argument("--aws-session-token", help="AWS session token for S3.")

    args = parser.parse_args()

    db_user = os.environ.get("DB_USER") or args.db_user
    db_host = os.environ.get("DB_HOST") or args.db_host
    db_port = os.environ.get("DB_PORT") or args.db_port
    db_name = os.environ.get("DB_NAME") or args.db_name
    db_schema = os.environ.get("DB_SCHEMA") or args.db_schema
    db_password = os.environ.get("DB_PASSWORD") or args.db_password
    local_port = os.environ.get("LOCAL_PORT") or args.local_port

    connection_details = [db_user, db_host, db_port, db_name, db_schema, local_port]
    if not all(connection_details):
        raise ValueError(
            f"Database connection details must be provided via environment variables or command-line arguments. Current values: {connection_details}"
        )

    prepare_and_run_src_data_ingest_copy(
        data_dictionary_path=args.data_dictionary_path,
        datafile_path=args.datafile_path,
        db_user=db_user,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_schema=db_schema,
        db_password=db_password,
        local_port=args.local_port,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        aws_session_token=args.aws_session_token,
    )

if __name__ == "__main__":
    main()
