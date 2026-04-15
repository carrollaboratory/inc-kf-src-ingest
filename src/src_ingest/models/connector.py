import psycopg2

class DatabaseConnection:
    def __init__(self, db_name, db_user, db_password, db_host, db_port, local_port=None):
        self.conn = None
        try:
            if local_port:
                db_host = "localhost"
                db_port = local_port
            
            conn_string_parts = [
                f"dbname='{db_name}'",
                f"user='{db_user}'",
                f"host='{db_host}'",
                f"port='{db_port}'",
                f"password='{db_password}'"
            ]

            conn_string = " ".join(conn_string_parts)
            self.conn = psycopg2.connect(conn_string)
            print("✅ Database connection established.")

        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed: {e}")

    def execute_query(self, query, query_name="Query"):
        """
        Executes a SQL query on the database.
        """
        if not self.conn:
            print("No database connection.")
            return
        try:
            self.conn.notices.clear()
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()

            if self.conn.notices:
                for notice in self.conn.notices:
                    if "already exists, skipping" in notice:
                        entity = "Schema" if "schema" in notice else "Table"
                        print(f"🔔 {query_name}: {entity} already exists, skipping.")
                    else:
                        print(f"🔔 {query_name}: {notice.strip()}")
            else:
                print(f"✅ {query_name}: Successfully executed.")

        except psycopg2.Error as e:
            if "does not exist" in str(e) and "TRUNCATE" in query:
                 print(f"🔔 {query_name}: Table does not exist, skipping.")
                 self.conn.rollback()
            else:
                print(f"❌ {query_name}: Failed to execute. {e}")
                self.conn.rollback()
                raise

    def copy_from_file(self, file_obj, table_name, sep=','):
        """
        Copies data from a file object into the specified table.
        Rolls back if there is an error during the copy operation.
        """
        if not self.conn:
            print("No database connection.")
            return 0
        try:
            with self.conn.cursor() as cursor:
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", file_obj)
                self.conn.commit()
                rowcount = cursor.rowcount
                print(f"✅ Copy Data: Copied {rowcount} rows.")
                return rowcount
        except psycopg2.Error as e:
            print(f"❌ Copy Data: Failed to copy data. {e}")
            self.conn.rollback()
            raise
        return 0

    def get_table_column_count(self, schema_name, table_name):
        """
        Gets the column count for the specified table.
        Rolls back if there is an error during the operation.
        """
        if not self.conn:
            print("No database connection.")
            return 0
        try:
            with self.conn.cursor() as cursor:
                query = """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s;
                """
                cursor.execute(query, (schema_name, table_name))
                count = cursor.fetchone()[0]
                return count
        except psycopg2.Error as e:
            print(f"❌ Failed to get column count for {schema_name}.{table_name}. {e}")
            self.conn.rollback()
            raise
        return 0

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def __enter__(self):
        """
        Setup method for the with statement. Returns the database connection object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit method for the with statement. Closes the database connection."""
        self.close()

