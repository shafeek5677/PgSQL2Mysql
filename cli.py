import argparse
from migrator.extract_schema import extract_tables_and_columns, print_schema
from migrator.translate_schema import translate_schema, print_translated_schema
from migrator.migrate_data import migrate_all_tables
from migrator.validate_migration import validate_all_tables
from migrator.utils import load_config, connect_postgres, connect_mysql, close_connection

def main():
    # Set up command-line arguments
    parser = argparse.ArgumentParser(description="PostgreSQL to MySQL Migration Tool")
    parser.add_argument("--extract-schema", action="store_true", help="Extract and print PostgreSQL schema")
    parser.add_argument("--translate-schema", action="store_true", help="Translate schema from PostgreSQL to MySQL")
    parser.add_argument("--migrate-data", action="store_true", help="Migrate data from PostgreSQL to MySQL")
    parser.add_argument("--validate-migration", action="store_true", help="Validate the migration by comparing row counts")
    parser.add_argument("--tables", type=str, nargs="+", help="Specify tables to migrate or validate (default: all tables)")

    # Parse arguments
    args = parser.parse_args()

    # Load configuration
    config = load_config()
    pg_conn = connect_postgres(config)
    mysql_conn = connect_mysql(config)

    # Extract schema from PostgreSQL
    if args.extract_schema:
        schema = extract_tables_and_columns(pg_conn)
        print_schema(schema)

    # Translate schema from PostgreSQL to MySQL
    if args.translate_schema:
        schema = extract_tables_and_columns(pg_conn)
        translated_schema = translate_schema(schema)
        print_translated_schema(translated_schema)

    # Migrate data from PostgreSQL to MySQL
    if args.migrate_data:
        tables_to_migrate = args.tables if args.tables else []  # Use specified tables, or all if none specified
        migrate_all_tables(pg_conn, mysql_conn, tables_to_migrate)

    # Validate migration by comparing row counts
    if args.validate_migration:
        tables_to_validate = args.tables if args.tables else []  # Use specified tables, or all if none specified
        validate_all_tables(pg_conn, mysql_conn, tables_to_validate)

    # Close database connections
    close_connection(pg_conn, "PostgreSQL")
    close_connection(mysql_conn, "MySQL")

if __name__ == "__main__":
    main()
