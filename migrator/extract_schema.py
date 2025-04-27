from migrator.utils import load_config, connect_postgres, close_connection
from rich import print
from rich.table import Table

def extract_tables_and_columns(pg_conn):
    query = """
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM
        information_schema.columns
    WHERE
        table_schema = 'public'
    ORDER BY
        table_name, ordinal_position;
    """
    cursor = pg_conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    schema = {}
    for table_name, column_name, data_type, is_nullable, column_default in rows:
        if table_name not in schema:
            schema[table_name] = []
        schema[table_name].append({
            'column_name': column_name,
            'data_type': data_type,
            'is_nullable': is_nullable,
            'column_default': column_default
        })

    return schema

def print_schema(schema):
    for table, columns in schema.items():
        print(f"[bold cyan]Table:[/bold cyan] {table}")
        table_view = Table(show_header=True, header_style="bold magenta")
        table_view.add_column("Column Name")
        table_view.add_column("Data Type")
        table_view.add_column("Nullable")
        table_view.add_column("Default")
        for col in columns:
            table_view.add_row(
                col['column_name'],
                col['data_type'],
                col['is_nullable'],
                str(col['column_default']) if col['column_default'] else "-"
            )
        print(table_view)
        print("\n")

def main():
    config = load_config()
    pg_conn = connect_postgres(config)

    schema = extract_tables_and_columns(pg_conn)

    print_schema(schema)

    close_connection(pg_conn, "PostgreSQL")

if __name__ == "__main__":
    main()
