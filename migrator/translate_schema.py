from rich import print

# Mapping of PostgreSQL data types to MySQL data types
PG_TO_MYSQL_TYPE_MAPPING = {
    "integer": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "serial": "INT AUTO_INCREMENT",
    "bigserial": "BIGINT AUTO_INCREMENT",
    "boolean": "TINYINT(1)",
    "text": "TEXT",
    "varchar": "VARCHAR(255)",  # Default length; adjust if needed
    "character varying": "VARCHAR(255)",
    "timestamp without time zone": "DATETIME",
    "timestamp with time zone": "TIMESTAMP",
    "date": "DATE",
    "numeric": "DECIMAL(10,2)",  # Default precision; can enhance later
    "real": "FLOAT",
    "double precision": "DOUBLE",
    "bytea": "BLOB",
    "json": "JSON",
    "jsonb": "JSON"
    # You can add more mappings if needed
}

def translate_column(column):
    pg_type = column['data_type']
    mysql_type = PG_TO_MYSQL_TYPE_MAPPING.get(pg_type, "TEXT")  # Fallback to TEXT
    return {
        'column_name': column['column_name'],
        'mysql_type': mysql_type,
        'is_nullable': column['is_nullable'],
        'column_default': column['column_default']
    }

def generate_create_table_statement(table_name, columns):
    sql_lines = []
    pk_present = False

    for column in columns:
        col = translate_column(column)

        line = f"`{col['column_name']}` {col['mysql_type']}"

        if col['is_nullable'] == 'NO':
            line += " NOT NULL"
        else:
            line += " NULL"

        if col['column_default']:
            # Handle serial/auto_increment separately
            if 'nextval' in str(col['column_default']):
                if "AUTO_INCREMENT" not in col['mysql_type']:
                    line += " AUTO_INCREMENT"
                pk_present = True
            else:
                default_val = str(col['column_default']).strip("'")
                line += f" DEFAULT '{default_val}'"

        sql_lines.append(line)

    if pk_present:
        sql_lines.append("PRIMARY KEY (`id`)")  # Assuming 'id' is PK if serial is used

    create_stmt = f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(sql_lines) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    return create_stmt

def translate_schema(schema):
    translated = {}

    for table_name, columns in schema.items():
        create_sql = generate_create_table_statement(table_name, columns)
        translated[table_name] = create_sql

    return translated

def print_translated_schema(translated_schema):
    for table, sql in translated_schema.items():
        print(f"[bold cyan]Table:[/bold cyan] {table}")
        print(sql)
        print("\n")

def main():
    # Dummy data for testing
    schema = {
        'users': [
            {'column_name': 'id', 'data_type': 'serial', 'is_nullable': 'NO', 'column_default': "nextval('users_id_seq'::regclass)"},
            {'column_name': 'name', 'data_type': 'text', 'is_nullable': 'YES', 'column_default': None},
            {'column_name': 'email', 'data_type': 'character varying', 'is_nullable': 'NO', 'column_default': None},
            {'column_name': 'created_at', 'data_type': 'timestamp without time zone', 'is_nullable': 'YES', 'column_default': "now()"}
        ]
    }

    translated_schema = translate_schema(schema)
    print_translated_schema(translated_schema)

if __name__ == "__main__":
    main()
