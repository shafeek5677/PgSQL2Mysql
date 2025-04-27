import os
import csv
import psycopg2
import yaml
from rich import print
from rich.progress import Progress
from mysql.connector import connect as mysql_connect

def load_config():
    """Load config.yml"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yml")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def connect_postgres(config):
    """Connect to PostgreSQL"""
    conn = psycopg2.connect(
        host=config['postgres']['host'],
        port=config['postgres']['port'],
        user=config['postgres']['user'],
        password=config['postgres']['password'],
        database=config['postgres']['database']
    )
    print("[bold green]✅ Connected to PostgreSQL![/bold green]")
    return conn

def connect_mysql(config):
    """Connect to MySQL"""
    conn = mysql_connect(
        host=config['mysql']['host'],
        port=config['mysql']['port'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    print("[bold green]✅ Connected to MySQL![/bold green]")
    return conn

def close_connection(conn, db_name="Database"):
    """Close database connection"""
    if conn:
        conn.close()
        print(f"✅ {db_name} connection closed.")

def fetch_pg_total_count(pg_conn, table_name, schema_name="cmrc"):
    """Fetch total row count for progress bar"""
    cursor = pg_conn.cursor()
    query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}";'
    cursor.execute(query)
    total = cursor.fetchone()[0]
    cursor.close()
    return total

def fetch_pg_data(pg_conn, table_name, batch_size=1000, offset=0, schema_name="cmrc"):
    """Fetch data from PostgreSQL with offset & limit"""
    cursor = pg_conn.cursor()
    query = f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT {batch_size} OFFSET {offset};'
    cursor.execute(query)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    cursor.close()
    return colnames, rows

def fetch_pg_table_schema(pg_conn, table_name, schema_name="cmrc"):
    """Fetch PostgreSQL table schema"""
    cursor = pg_conn.cursor()
    query = f'''
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    '''
    cursor.execute(query)
    columns = cursor.fetchall()
    cursor.close()
    return columns

def translate_pg_to_mysql_type(pg_type):
    """Map PostgreSQL types to MySQL types"""
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
    }
    return mapping.get(pg_type.lower(), "TEXT")

def create_mysql_table(mysql_conn, table_name, pg_schema):
    """Create MySQL table based on PostgreSQL schema"""
    cursor = mysql_conn.cursor()
    columns_definitions = []
    for column_name, data_type in pg_schema:
        mysql_type = translate_pg_to_mysql_type(data_type)
        columns_definitions.append(f"`{column_name}` {mysql_type}")

    create_query = f'''
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(columns_definitions)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    try:
        cursor.execute(create_query)
        mysql_conn.commit()
        print(f"[bold green]✅ MySQL table `{table_name}` created successfully![/bold green]")
    except Exception as e:
        print(f"[bold red]❌ Error creating MySQL table {table_name}: {e}[/bold red]")
        mysql_conn.rollback()
    cursor.close()

def insert_mysql_data(mysql_conn, table_name, columns, data, mini_batch_size=500):
    """Insert data into MySQL in mini-chunks"""
    cursor = mysql_conn.cursor()
    if not data:
        return

    columns_formatted = [f"`{col}`" for col in columns]
    values_placeholder = ", ".join(["%s"] * len(columns))
    insert_query = f"INSERT INTO `{table_name}` ({', '.join(columns_formatted)}) VALUES ({values_placeholder})"

    try:
        for i in range(0, len(data), mini_batch_size):
            chunk = data[i:i + mini_batch_size]
            cursor.executemany(insert_query, chunk)
            mysql_conn.commit()
    except Exception as e:
        print(f"[bold red]❌ Error inserting into MySQL for {table_name}: {e}[/bold red]")
        mysql_conn.rollback()
        raise e
    finally:
        cursor.close()

def migrate_table(pg_conn, mysql_conn, table_name, batch_size=10000, schema_name="cmrc"):
    """Migrate single table"""
    print(f"\n[bold cyan]🚀 Migrating Table: {table_name}[/bold cyan]")

    pg_schema = fetch_pg_table_schema(pg_conn, table_name, schema_name)
    create_mysql_table(mysql_conn, table_name, pg_schema)

    total_rows = fetch_pg_total_count(pg_conn, table_name, schema_name)
    print(f"[yellow]Total rows to migrate: {total_rows}[/yellow]")

    if total_rows == 0:
        print(f"[bold yellow]⚡ No rows to migrate for table {table_name}[/bold yellow]")
        return

    with Progress() as progress:
        task = progress.add_task(f"[green]Migrating {table_name}...", total=total_rows)

        for offset in range(0, total_rows, batch_size):
            columns, pg_data = fetch_pg_data(pg_conn, table_name, batch_size, offset, schema_name)
            if not pg_data:
                break
            insert_mysql_data(mysql_conn, table_name, columns, pg_data, mini_batch_size=500)
            progress.update(task, advance=len(pg_data))

    print(f"[bold green]✅ Data migration completed for table {table_name}![/bold green]")

def migrate_all_tables(pg_conn, mysql_conn, tables_to_migrate, batch_size=10000, schema_name="cmrc"):
    """Migrate multiple tables"""
    for table_name in tables_to_migrate:
        migrate_table(pg_conn, mysql_conn, table_name, batch_size, schema_name)

def main():
    config = load_config()
    pg_conn = connect_postgres(config)
    mysql_conn = connect_mysql(config)

    tables_to_migrate = ["users", "orders"]  # 🔵 Modify your table list
    migrate_all_tables(pg_conn, mysql_conn, tables_to_migrate)

    close_connection(pg_conn, "PostgreSQL")
    close_connection(mysql_conn, "MySQL")

if __name__ == "__main__":
    main()