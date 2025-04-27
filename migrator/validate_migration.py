from migrator.utils import load_config, connect_postgres, connect_mysql, close_connection
from rich import print
from rich.progress import track

def fetch_pg_count(pg_conn, table_name, schema_name="cmrc"):
    """Fetch row count from PostgreSQL table"""
    cursor = pg_conn.cursor()
    query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}";'
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else 0

def fetch_mysql_count(mysql_conn, table_name):
    """Fetch row count from MySQL table"""
    cursor = mysql_conn.cursor()
    query = f"SELECT COUNT(*) FROM `{table_name}`;"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else 0

def validate_table(pg_conn, mysql_conn, table_name, schema_name="cmrc"):
    """Compare row counts between PostgreSQL and MySQL for a table"""
    print(f"\n[bold magenta]Validating Table: {table_name}[/bold magenta]")
    
    pg_count = fetch_pg_count(pg_conn, table_name, schema_name)
    mysql_count = fetch_mysql_count(mysql_conn, table_name)
    
    if pg_count == mysql_count:
        print(f"[bold green]Success! Row count for table {table_name} matches: {pg_count} rows[/bold green]")
    else:
        print(f"[bold red]Mismatch! PostgreSQL: {pg_count} rows, MySQL: {mysql_count} rows[/bold red]")

def validate_all_tables(pg_conn, mysql_conn, tables_to_validate, schema_name="cmrc"):
    """Validate all specified tables from PostgreSQL to MySQL"""
    for table_name in track(tables_to_validate, description="Validating tables..."):
        validate_table(pg_conn, mysql_conn, table_name, schema_name)

def main():
    config = load_config()
    pg_conn = connect_postgres(config)
    mysql_conn = connect_mysql(config)

    # Here we specify the tables we want to validate. Could come from config or schema extract
    tables_to_validate = ["users", "orders"]  # Example tables
    validate_all_tables(pg_conn, mysql_conn, tables_to_validate)

    close_connection(pg_conn, "PostgreSQL")
    close_connection(mysql_conn, "MySQL")

if __name__ == "__main__":
    main()