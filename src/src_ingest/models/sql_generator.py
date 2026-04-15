from jinja2 import Template


# Map common data types to PostgreSQL types
type_mapping = {
    "string": "text",
    "integer": "integer",
    "float": "float",
    "boolean": "boolean",
    "datetime": "timestamp",
}


def extract_table_schema(column_data_list: list):
    """Extracts column definitions from the data dictionary CSV.
    TODO: Hardcodes the expected column name for variable name and data type."""
    column_definitions = []
    for row in column_data_list:
        variable_name = row.get("variable_name")
        data_type = row.get("data_type")
        if variable_name:
            sql_type = type_mapping.get(data_type, "text")
            column_definitions.append(f'"{variable_name}" {sql_type}')

    return column_definitions


def gen_create_schema_query(
    schema: str,
):
    """
    Define the template for the CREATE SCHEMA statement

    """
    print(f"Start pipeline db, src schema creation {schema}")

    q_template = """
    CREATE SCHEMA IF NOT EXISTS {{ schema }};
    """
    try:
        sql_query = Template(q_template).render(
            schema=schema
        )
        return sql_query
    except Exception as e:
        print(f"Error generating CREATE SCHEMA query: {e}")
        raise


def gen_create_table_query(
    df: str,
    column_data_list: list,
    schema: str,
):
    """
    Define the template for the CREATE TABLE statement
    """
    # Extract column definitions (e.g., "column_name data_type")
    column_defs = extract_table_schema(column_data_list)
    print(f"Start pipeline db, src table creation {df}")

    q_template = """

    CREATE TABLE IF NOT EXISTS {{ schema }}.{{ table_name }} (
        {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    );
    """
    try:
        sql_query = Template(q_template).render(
            columns=column_defs, table_name=df, schema=schema
        )
        return sql_query
    except Exception as e:
        print(f"Error generating CREATE TABLE query: {e}")
        raise


def gen_create_table_query_from_header(
    table_name: str,
    header: list,
    schema: str,
):
    """
    Generates a CREATE TABLE statement from a CSV header, assuming all columns are text.
    """
    column_defs = [f'"{column_name}" text' for column_name in header]
    
    print(f"Start pipeline db, src table creation {table_name}")

    q_template = """

    CREATE TABLE IF NOT EXISTS {{ schema }}.{{ table_name }} (
        {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    );
    """
    try:
        sql_query = Template(q_template).render(
            columns=column_defs, table_name=table_name, schema=schema
        )
        return sql_query
    except Exception as e:
        print(f"Error generating CREATE TABLE query: {e}")
        raise


def gen_truncate_table_query(schema_name: str, table_name: str) -> str:
    """
    Generates a TRUNCATE TABLE statement.
    Restarts the identity sequence for the table.
    """
    return f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY;"


def gen_create_metadata_table_query(schema: str) -> str:
    """
    Generates a CREATE TABLE statement for the metadata logging table.
    """
    table_name = "ingestion_log"
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        ingestion_id SERIAL PRIMARY KEY,
        ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
        source_file_path TEXT,
        target_schema TEXT,
        target_table TEXT,
        rows_ingested INT,
        was_successful BOOLEAN,
        details TEXT
    );
    """

def gen_insert_metadata_query(
    schema: str,
    source_file_path: str,
    target_schema: str,
    target_table: str,
    rows_ingested: int,
    was_successful: bool,
    details: str,
) -> str:
    """
    Generates an INSERT statement for the metadata logging table.
    """
    table_name = "ingestion_log"

    details = details.replace("'", "''")

    return f"""
    INSERT INTO {schema}.{table_name} (source_file_path, target_schema, target_table, rows_ingested, was_successful, details)
    VALUES ('{source_file_path}', '{target_schema}', '{target_table}', {rows_ingested}, {was_successful}, '{details}');
    """
