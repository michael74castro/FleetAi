"""
Load Excel Data into Landing Tables

This script loads data from Excel source files into the SQL Server landing tables.
Features:
- Chunk-based loading for large files
- Extraction logging
- Column renaming with table prefix
- Error handling and recovery
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Optional
import warnings
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import urllib

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Configuration
SOURCE_DIR = Path(r"C:\Users\X1Carbon\Documents\FleetAI\Files")
CHUNK_SIZE = 5000  # Rows per batch insert
LARGE_FILE_THRESHOLD = 50000  # Files with more rows use chunked reading

# Database connection string components
# Update these with your actual connection details
DB_SERVER = "localhost"
DB_NAME = "FleetAI"
DB_DRIVER = "ODBC Driver 17 for SQL Server"

# Use Windows Authentication (Trusted_Connection) or specify user/password
USE_WINDOWS_AUTH = True
DB_USER = ""
DB_PASSWORD = ""


def get_connection_string() -> str:
    """Build SQLAlchemy connection string."""
    if USE_WINDOWS_AUTH:
        params = urllib.parse.quote_plus(
            f"DRIVER={{{DB_DRIVER}}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"Trusted_Connection=yes;"
        )
    else:
        params = urllib.parse.quote_plus(
            f"DRIVER={{{DB_DRIVER}}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
        )
    return f"mssql+pyodbc:///?odbc_connect={params}"


def get_table_name(filename: str) -> str:
    """Extract table name from filename (uppercase, no extension)."""
    return Path(filename).stem.upper()


def log_extraction_start(engine: Engine, table_name: str, extraction_type: str = "FULL") -> int:
    """Log extraction start and return log_id."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            INSERT INTO landing.etl_extraction_log
            (table_name, extraction_type, extraction_start, status)
            OUTPUT INSERTED.log_id
            VALUES (:table_name, :extraction_type, GETUTCDATE(), 'RUNNING')
        """), {"table_name": table_name, "extraction_type": extraction_type})
        log_id = result.scalar()
        conn.commit()
        return log_id


def log_extraction_end(engine: Engine, log_id: int, row_count: int,
                       status: str = "SUCCESS", error_message: Optional[str] = None):
    """Log extraction completion."""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE landing.etl_extraction_log
            SET extraction_end = GETUTCDATE(),
                extracted_row_count = :row_count,
                status = :status,
                error_message = :error_message
            WHERE log_id = :log_id
        """), {
            "log_id": log_id,
            "row_count": row_count,
            "status": status,
            "error_message": error_message
        })
        conn.commit()


def update_checkpoint(engine: Engine, table_name: str, row_count: int):
    """Update or insert checkpoint for table."""
    with engine.connect() as conn:
        conn.execute(text("""
            MERGE landing.etl_checkpoint AS target
            USING (SELECT :table_name as table_name) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN
                UPDATE SET
                    last_successful_extraction = GETUTCDATE(),
                    last_row_count = :row_count,
                    updated_at = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (table_name, last_successful_extraction, last_row_count)
                VALUES (:table_name, GETUTCDATE(), :row_count);
        """), {"table_name": table_name, "row_count": row_count})
        conn.commit()


def truncate_table(engine: Engine, table_name: str):
    """Truncate landing table before load."""
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE landing.[{table_name}]"))
        conn.commit()


def load_excel_to_table(engine: Engine, filepath: Path, table_name: str) -> int:
    """
    Load Excel file data into landing table.

    Returns:
        Number of rows loaded
    """
    # Read Excel file
    df = pd.read_excel(filepath, engine='openpyxl')

    # Rename columns with table prefix
    new_columns = {}
    for col in df.columns:
        clean_col = str(col).strip().upper()
        new_columns[col] = f"{table_name}_{clean_col}"

    df = df.rename(columns=new_columns)

    # Handle NaN values - convert to None for SQL
    df = df.replace({np.nan: None})

    # Load to database in chunks
    total_rows = 0

    for i in range(0, len(df), CHUNK_SIZE):
        chunk = df.iloc[i:i + CHUNK_SIZE]

        chunk.to_sql(
            name=table_name,
            con=engine,
            schema='landing',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        total_rows += len(chunk)
        print(f"    Loaded {total_rows:,} / {len(df):,} rows", end="\r")

    print()  # New line after progress
    return total_rows


def main():
    """Main function to load data into landing tables."""
    print("=" * 80)
    print("Landing Data Loader")
    print("=" * 80)
    print(f"Source directory: {SOURCE_DIR}")
    print(f"Target database: {DB_SERVER}/{DB_NAME}")
    print()

    # Test database connection
    print("Testing database connection...", end=" ")
    try:
        connection_string = get_connection_string()
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("OK")
    except Exception as e:
        print(f"FAILED: {e}")
        print()
        print("Please check your database connection settings in the script.")
        sys.exit(1)

    # Find all Excel files
    excel_files = sorted(SOURCE_DIR.glob("*.xlsx"))
    print(f"Found {len(excel_files)} Excel files")
    print()

    if not excel_files:
        print("ERROR: No Excel files found!")
        sys.exit(1)

    # Process each file
    results = []
    errors = []

    for i, filepath in enumerate(excel_files, 1):
        table_name = get_table_name(filepath.name)
        print(f"[{i}/{len(excel_files)}] Loading {filepath.name} -> landing.{table_name}")

        log_id = None
        try:
            # Log extraction start
            log_id = log_extraction_start(engine, table_name)

            # Truncate table before load
            print(f"    Truncating table...", end=" ")
            truncate_table(engine, table_name)
            print("OK")

            # Load data
            print(f"    Loading data...")
            row_count = load_excel_to_table(engine, filepath, table_name)

            # Log success
            log_extraction_end(engine, log_id, row_count, "SUCCESS")
            update_checkpoint(engine, table_name, row_count)

            results.append((table_name, row_count, "SUCCESS"))
            print(f"    Loaded {row_count:,} rows - SUCCESS")

        except Exception as e:
            error_msg = str(e)
            errors.append((table_name, error_msg))
            results.append((table_name, 0, f"FAILED: {error_msg[:50]}..."))

            if log_id:
                log_extraction_end(engine, log_id, 0, "FAILED", error_msg)

            print(f"    ERROR: {error_msg}")

        print()

    # Summary
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"{'Table':<15} {'Rows':>15} {'Status':<30}")
    print("-" * 65)

    total_rows = 0
    success_count = 0

    for table_name, row_count, status in results:
        print(f"{table_name:<15} {row_count:>15,} {status:<30}")
        if "SUCCESS" in status:
            total_rows += row_count
            success_count += 1

    print("-" * 65)
    print(f"{'TOTAL':<15} {total_rows:>15,} {success_count}/{len(results)} tables loaded")
    print()

    if errors:
        print(f"ERRORS ({len(errors)} tables failed):")
        for table_name, error in errors:
            print(f"  - {table_name}: {error[:80]}")
        print()

    print("Data loading complete.")


if __name__ == "__main__":
    main()
