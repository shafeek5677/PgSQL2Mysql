from migrator.utils import load_config, connect_postgres, connect_mysql, close_connection
from rich import print
from rich.progress import track

def fetch_pg_data(pg_conn, table_name, batch_size=1000, offset=0, schema_name="cmrc"):
    """Fetch data and columns from PostgreSQL table in batches"""
    cursor = pg_conn.cursor()
    query = f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT {batch_size} OFFSET {offset};'
    cursor.execute(query)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    cursor.close()
    return colnames, rows

def fetch_pg_table_schema(pg_conn, table_name, schema_name="cmrc"):
    """Fetch the schema of a PostgreSQL table"""
    cursor = pg_conn.cursor()
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    cursor.execute(query)
    columns = cursor.fetchall()
    cursor.close()
    return columns

def translate_pg_to_mysql_type(pg_type):
    """Translate PostgreSQL data types to MySQL data types"""
    mapping = {
        "integer": "INT",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "serial": "INT AUTO_INCREMENT",
        "bigserial": "BIGINT AUTO_INCREMENT",
        "varchar": "VARCHAR(255)",
        "character varying": "VARCHAR(255)",
        "text": "TEXT",
        "boolean": "TINYINT(1)",
        "date": "DATE",
        "timestamp without time zone": "DATETIME",
        "timestamp with time zone": "DATETIME",
        "double precision": "DOUBLE",
        "numeric": "DECIMAL(18,2)",
        # Add more mappings as needed
    }
    return mapping.get(pg_type.lower(), "TEXT")  # Default to TEXT if unknown

def create_mysql_table(mysql_conn, table_name, pg_schema):
    """Create a table in MySQL based on PostgreSQL schema"""
    cursor = mysql_conn.cursor()
    columns_definitions = []
    for column_name, data_type in pg_schema:
        mysql_type = translate_pg_to_mysql_type(data_type)
        columns_definitions.append(f"`{column_name}` {mysql_type}")

    create_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(columns_definitions)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(create_query)
        mysql_conn.commit()
        print(f"[bold green]MySQL table `{table_name}` created successfully![/bold green]")
    except Exception as e:
        print(f"[bold red]Error creating MySQL table {table_name}:[/bold red] {e}")
        mysql_conn.rollback()
    cursor.close()

def insert_mysql_data(mysql_conn, table_name, columns, data):
    """Insert data into MySQL table"""
    cursor = mysql_conn.cursor()

    if data:
        columns_formatted = [f"`{col}`" for col in columns]
        values_placeholder = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO `{table_name}` ({', '.join(columns_formatted)}) VALUES ({values_placeholder})"
        
        try:
            cursor.executemany(insert_query, data)
            mysql_conn.commit()
            print(f"[bold green]Inserted {len(data)} rows into {table_name}[/bold green]")
        except Exception as e:
            print(f"[bold red]Error inserting data into MySQL for table {table_name}:[/bold red] {e}")
            mysql_conn.rollback()
    else:
        print(f"[bold yellow]No data to insert for table {table_name}.[/bold yellow]")
    cursor.close()

def migrate_table(pg_conn, mysql_conn, table_name, batch_size=1000, schema_name="cmrc"):
    """Migrate a single table from PostgreSQL to MySQL"""
    print(f"\n[bold cyan]Migrating Table: {table_name}[/bold cyan]")

    # Step 1: Fetch PostgreSQL table schema
    pg_schema = fetch_pg_table_schema(pg_conn, table_name, schema_name)

    # Step 2: Create MySQL table if it does not exist
    create_mysql_table(mysql_conn, table_name, pg_schema)

    # Step 3: Start batch migration
    offset = 0
    while True:
        columns, pg_data = fetch_pg_data(pg_conn, table_name, batch_size, offset, schema_name)
        
        if not pg_data:
            break  # No more data to migrate
        
        insert_mysql_data(mysql_conn, table_name, columns, pg_data)
        
        offset += batch_size  # Move to next batch

    print(f"[bold green]Data migration completed for table {table_name}![/bold green]")

def migrate_all_tables(pg_conn, mysql_conn, tables_to_migrate, batch_size=1000, schema_name="cmrc"):
    """Migrate all specified tables from PostgreSQL to MySQL"""
    for table_name in track(tables_to_migrate, description="Migrating tables..."):
        migrate_table(pg_conn, mysql_conn, table_name, batch_size, schema_name)

def main():
    config = load_config()
    pg_conn = connect_postgres(config)
    mysql_conn = connect_mysql(config)

    tables_to_migrate = ["users", "orders"]  # Example, modify as needed
    migrate_all_tables(pg_conn, mysql_conn, tables_to_migrate)

    close_connection(pg_conn, "PostgreSQL")
    close_connection(mysql_conn, "MySQL")

if __name__ == "__main__":
    main()