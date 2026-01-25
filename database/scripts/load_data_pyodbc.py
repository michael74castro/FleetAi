"""
Load Data to SQL Server using pyodbc
Reads Excel files and inserts directly into landing schema
"""

import pyodbc
import pandas as pd
import os
from datetime import datetime
import sys

# Configuration - modify these as needed
SERVER = "localhost"
DATABASE = "FleetAI"
TRUSTED_CONNECTION = True  # Windows Authentication
# USERNAME = "your_username"  # Uncomment if using SQL auth
# PASSWORD = "your_password"  # Uncomment if using SQL auth

EXCEL_DIR = r"C:\Users\X1Carbon\Documents\FleetAI\Files"
SCHEMA_FILE = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\schemas\01_landing.sql"
BATCH_SIZE = 500


def get_connection():
    """Create database connection."""
    if TRUSTED_CONNECTION:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    else:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"

    return pyodbc.connect(conn_str)


def run_schema_script(conn):
    """Execute the schema creation script."""
    print("Creating landing schema and tables...")

    with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
        script = f.read()

    # Split by GO statements
    batches = script.split('\nGO\n')

    cursor = conn.cursor()
    for i, batch in enumerate(batches):
        batch = batch.strip()
        if batch and not batch.startswith('/*') and not batch.startswith('--'):
            try:
                cursor.execute(batch)
                conn.commit()
            except pyodbc.Error as e:
                # Ignore "already exists" errors
                if "already exists" not in str(e):
                    print(f"  Warning in batch {i}: {e}")

    cursor.close()
    print("  Schema ready.")


def load_excel_file(conn, filepath):
    """Load a single Excel file into the corresponding landing table."""
    filename = os.path.basename(filepath)
    table_name = filename.replace('.xlsx', '').upper()
    prefix = table_name

    print(f"  Loading {table_name}...", end=" ", flush=True)

    try:
        df = pd.read_excel(filepath)

        # Build column names with prefix
        columns = [f"{prefix}_{col}" for col in df.columns]

        # Build INSERT statement
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join([f"[{col}]" for col in columns])
        insert_sql = f"INSERT INTO [landing].[{table_name}] ({column_list}) VALUES ({placeholders})"

        cursor = conn.cursor()
        cursor.fast_executemany = True

        # Convert DataFrame to list of tuples, handling NaN values
        data = []
        for _, row in df.iterrows():
            values = []
            for val in row.values:
                if pd.isna(val):
                    values.append(None)
                else:
                    values.append(val)
            data.append(tuple(values))

        # Insert in batches
        total_rows = len(data)
        for i in range(0, total_rows, BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            conn.commit()

        cursor.close()
        print(f"{total_rows} rows OK")
        return total_rows

    except Exception as e:
        print(f"FAILED - {e}")
        return 0


def main():
    print("=" * 60)
    print("FleetAI Data Load Script (pyodbc)")
    print("=" * 60)
    print(f"Server: {SERVER}")
    print(f"Database: {DATABASE}")
    print(f"Source: {EXCEL_DIR}")
    print()

    try:
        conn = get_connection()
        print("Connected to SQL Server successfully.")
        print()
    except pyodbc.Error as e:
        print(f"ERROR: Failed to connect to SQL Server: {e}")
        print()
        print("Please check:")
        print("  1. SQL Server is running")
        print("  2. Database 'FleetAI' exists")
        print("  3. ODBC Driver 17 for SQL Server is installed")
        sys.exit(1)

    # Step 1: Create schema
    run_schema_script(conn)
    print()

    # Step 2: Load data
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
    print()

    if failed == 0:
        print("All data loaded successfully!")
    else:
        print(f"WARNING: {failed} table(s) failed to load.")


if __name__ == "__main__":
    main()
