"""
Load Data to SQLite Database
Creates landing tables and loads data from Excel files
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime
import hashlib

# Configuration
EXCEL_DIR = r"C:\Users\X1Carbon\Documents\FleetAI\Files"
DB_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\fleetai.db"


def get_sqlite_type(dtype):
    """Map pandas dtype to SQLite type."""
    dtype_str = str(dtype)
    if 'int' in dtype_str:
        return 'INTEGER'
    elif 'float' in dtype_str:
        return 'REAL'
    elif 'datetime' in dtype_str:
        return 'TEXT'
    else:
        return 'TEXT'


def create_table_sql(table_name, df):
    """Generate CREATE TABLE statement for a DataFrame."""
    prefix = table_name

    columns = []
    # Metadata columns
    columns.append("extraction_id INTEGER PRIMARY KEY AUTOINCREMENT")
    columns.append("extraction_timestamp TEXT DEFAULT (datetime('now'))")

    # Source columns
    for col in df.columns:
        col_name = f"{prefix}_{col}"
        col_type = get_sqlite_type(df[col].dtype)
        columns.append(f"[{col_name}] {col_type}")

    sql = f"CREATE TABLE IF NOT EXISTS landing_{table_name} (\n    "
    sql += ",\n    ".join(columns)
    sql += "\n);"

    return sql


def create_etl_tables(conn):
    """Create ETL metadata tables."""
    cursor = conn.cursor()

    # Extraction log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS landing_etl_extraction_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            extraction_type TEXT NOT NULL,
            extraction_start TEXT NOT NULL,
            extraction_end TEXT,
            source_row_count INTEGER,
            extracted_row_count INTEGER,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # Checkpoint table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS landing_etl_checkpoint (
            checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT UNIQUE NOT NULL,
            last_successful_extraction TEXT,
            last_row_count INTEGER,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    cursor.close()


def load_excel_file(conn, filepath):
    """Load a single Excel file into SQLite."""
    filename = os.path.basename(filepath)
    table_name = filename.replace('.xlsx', '').upper()
    prefix = table_name
    sqlite_table = f"landing_{table_name}"

    print(f"  Loading {table_name}...", end=" ", flush=True)

    try:
        df = pd.read_excel(filepath)

        # Create table
        create_sql = create_table_sql(table_name, df)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {sqlite_table}")
        cursor.execute(create_sql)

        # Rename columns with prefix
        df.columns = [f"{prefix}_{col}" for col in df.columns]

        # Insert data
        placeholders = ", ".join(["?" for _ in df.columns])
        column_list = ", ".join([f"[{col}]" for col in df.columns])
        insert_sql = f"INSERT INTO {sqlite_table} ({column_list}) VALUES ({placeholders})"

        # Convert to list of tuples, handling NaN
        data = []
        for _, row in df.iterrows():
            values = []
            for val in row.values:
                if pd.isna(val):
                    values.append(None)
                elif isinstance(val, (int, float)):
                    if pd.isna(val):
                        values.append(None)
                    else:
                        values.append(val)
                else:
                    values.append(str(val) if val is not None else None)
            data.append(tuple(values))

        cursor.executemany(insert_sql, data)
        conn.commit()

        # Create index on extraction_timestamp
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{sqlite_table}_timestamp ON {sqlite_table}(extraction_timestamp)")
        conn.commit()

        cursor.close()
        print(f"{len(df)} rows OK")
        return len(df)

    except Exception as e:
        print(f"FAILED - {e}")
        return 0


def main():
    print("=" * 60)
    print("FleetAI Data Load Script (SQLite)")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"Source: {EXCEL_DIR}")
    print()

    # Create database directory if needed
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Connect to SQLite (creates file if not exists)
    conn = sqlite3.connect(DB_PATH)
    print("Connected to SQLite database.")
    print()

    # Create ETL tables
    print("Creating ETL metadata tables...")
    create_etl_tables(conn)
    print("  ETL tables ready.")
    print()

    # Load data from Excel files
    print("Loading data from Excel files...")
    excel_files = sorted([f for f in os.listdir(EXCEL_DIR) if f.endswith('.xlsx')])

    total_rows = 0
    successful = 0
    failed = 0

    for filename in excel_files:
        filepath = os.path.join(EXCEL_DIR, filename)
        rows = load_excel_file(conn, filepath)
        if rows > 0:
            total_rows += rows
            successful += 1
        else:
            failed += 1

    conn.close()

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Tables loaded: {successful}")
    print(f"Tables failed: {failed}")
    print(f"Total rows: {total_rows:,}")
    print(f"Database file: {DB_PATH}")
    print(f"Database size: {os.path.getsize(DB_PATH) / (1024*1024):.2f} MB")
    print()

    if failed == 0:
        print("All data loaded successfully!")
    else:
        print(f"WARNING: {failed} table(s) failed to load.")


if __name__ == "__main__":
    main()
