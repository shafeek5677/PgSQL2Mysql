import psycopg2
import mysql.connector
import yaml
from rich import print
from rich.console import Console

# Global console for pretty output
console = Console()

# Load Configuration
def load_config(path='config.yaml'):
    try:
        with open(path, 'r') as file:
            config = yaml.safe_load(file)
            console.log("[bold green]Configuration loaded successfully![/bold green]")
            return config
    except Exception as e:
        console.log(f"[bold red]Error loading config file:[/bold red] {e}")
        raise

# Connect to PostgreSQL
def connect_postgres(config):
    try:
        pg_conf = config['postgresql']
        conn = psycopg2.connect(
            host=pg_conf['host'],
            port=pg_conf['port'],
            database=pg_conf['database'],
            user=pg_conf['user'],
            password=pg_conf['password']
        )
        console.log("[bold green]Connected to PostgreSQL![/bold green]")
        return conn
    except Exception as e:
        console.log(f"[bold red]Failed to connect to PostgreSQL:[/bold red] {e}")
        raise

# Connect to MySQL
def connect_mysql(config):
    try:
        mysql_conf = config['mysql']
        conn = mysql.connector.connect(
            host=mysql_conf['host'],
            port=mysql_conf['port'],
            database=mysql_conf['database'],
            user=mysql_conf['user'],
            password=mysql_conf['password']
        )
        console.log("[bold green]Connected to MySQL![/bold green]")
        return conn
    except Exception as e:
        console.log(f"[bold red]Failed to connect to MySQL:[/bold red] {e}")
        raise

# Simple helper to close connection
def close_connection(conn, db_name="Database"):
    if conn:
        conn.close()
        console.log(f"[bold yellow]{db_name} connection closed.[/bold yellow]")
