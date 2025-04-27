import os
import csv
import psycopg2
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# Hardcoded PostgreSQL Connection
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "{Hbeat123}"
}

OUTPUT_FOLDER = r"D:\\HIS-folder-datas\\cmrc_docs"
BATCH_SIZE = 100

# Make sure output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def connect_postgres():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    print("✅ PostgreSQL Connected!")
    return conn


def export_documents():
    conn = connect_postgres()
    cursor = conn.cursor(name='patient_docs_cursor')  # Named cursor for server-side streaming

    print("Fetching total document count...")
    count_cursor = conn.cursor()
    count_cursor.execute("""
        SELECT COUNT(*) FROM cmrc.patient_documents
        WHERE doc_format IN (
            'doc_fileupload',
            'application/msword',
            'application/octet-stream',
            'application/pdf',
            'application/zip',
            'image/jpeg',
            'image/png'
        );
    """)
    total_docs = count_cursor.fetchone()[0]
    count_cursor.close()
    print(f"📦 Total documents to export: {total_docs}")

    cursor.execute("""
        SELECT doc_id, filename, doc_content_bytea, content_type
        FROM cmrc.patient_documents
        WHERE doc_format IN (
            'doc_fileupload',
            'application/msword',
            'application/octet-stream',
            'application/pdf',
            'application/zip',
            'image/jpeg',
            'image/png'
        );
    """)

    csv_rows = []
    processed_docs = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
    ) as progress:

        task = progress.add_task("Exporting documents...", total=total_docs)

        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break

            for doc_id, filename, content, content_type in batch:
                if not content:
                    progress.console.print(f"⚠️ Skipping empty document: {doc_id}")
                    continue

                ext = ''
                if filename and '.' in filename:
                    ext = os.path.splitext(filename)[1]
                elif content_type:
                    content_type = content_type.lower()
                    if 'pdf' in content_type:
                        ext = '.pdf'
                    elif 'jpeg' in content_type or 'jpg' in content_type:
                        ext = '.jpg'
                    elif 'png' in content_type:
                        ext = '.png'
                    elif 'msword' in content_type:
                        ext = '.doc'
                    elif 'zip' in content_type:
                        ext = '.zip'

                safe_filename = f"{doc_id}{ext}"
                full_path = os.path.join(OUTPUT_FOLDER, safe_filename)

                try:
                    with open(full_path, 'wb') as f:
                        f.write(content)
                    csv_rows.append([doc_id, safe_filename])
                except Exception as e:
                    progress.console.print(f"❌ Error saving document {doc_id}: {e}")

                processed_docs += 1
                progress.update(task, advance=1)

    # Save CSV
    csv_path = os.path.join(OUTPUT_FOLDER, "exported_documents.csv")
    with open(csv_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['doc_id', 'filename'])
        writer.writerows(csv_rows)

    print(f"\n✅ Export completed. {processed_docs} files saved.")
    print(f"✅ CSV file created: {csv_path}")

    cursor.close()
    conn.close()
    print("✅ PostgreSQL connection closed.")


if __name__ == "__main__":
    export_documents()
