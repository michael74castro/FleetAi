"""
Generate Landing Schema SQL DDL from Excel Source Files

This script reads all Excel files from the source directory and generates
SQL Server DDL statements that match the DB2 source structure with:
- Column names prefixed with table name (e.g., CCCP_CPCPNO)
- Metadata columns for CDC (extraction_id, extraction_timestamp, row_hash)
- Auto-detected data types based on Excel data
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Configuration
SOURCE_DIR = Path(r"C:\Users\X1Carbon\Documents\FleetAI\Files")
OUTPUT_FILE = Path(r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\schemas\01_landing.sql")

# Data type inference thresholds
INT_MAX = 2_147_483_647
VARCHAR_THRESHOLDS = [50, 200, 1000]  # VARCHAR sizes


def infer_sql_type(series: pd.Series, col_name: str) -> str:
    """Infer SQL Server data type from pandas Series."""

    # Drop nulls for analysis
    non_null = series.dropna()

    if len(non_null) == 0:
        return "VARCHAR(200)"  # Default for empty columns

    dtype = series.dtype

    # Boolean type
    if dtype == bool or dtype == 'bool':
        return "BIT"

    # Datetime type
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME2"

    # Integer types
    if pd.api.types.is_integer_dtype(series):
        max_val = abs(non_null.max())
        min_val = abs(non_null.min()) if non_null.min() < 0 else 0
        max_abs = max(max_val, min_val)

        if max_abs <= INT_MAX:
            return "INT"
        else:
            return "BIGINT"

    # Float types - check if actually integers with nulls
    if pd.api.types.is_float_dtype(series):
        # Check if all non-null values are whole numbers
        is_whole = non_null.apply(lambda x: float(x).is_integer() if pd.notna(x) else True).all()

        if is_whole:
            max_val = abs(non_null.max())
            min_val = abs(non_null.min()) if non_null.min() < 0 else 0
            max_abs = max(max_val, min_val)

            if max_abs <= INT_MAX:
                return "INT"  # Nullable int represented as float
            else:
                return "BIGINT"
        else:
            # Calculate precision and scale for decimal
            try:
                # Get max precision needed
                str_vals = non_null.astype(str)
                max_len_before_decimal = 0
                max_scale = 0

                for val in str_vals:
                    if '.' in val:
                        parts = val.split('.')
                        before = parts[0].lstrip('-')
                        after = parts[1].rstrip('0') if len(parts) > 1 else ''
                        max_len_before_decimal = max(max_len_before_decimal, len(before))
                        max_scale = max(max_scale, len(after))
                    else:
                        max_len_before_decimal = max(max_len_before_decimal, len(val.lstrip('-')))

                precision = min(max_len_before_decimal + max_scale + 2, 38)  # SQL Server max is 38
                scale = min(max_scale + 2, 10)  # Cap scale at 10

                return f"DECIMAL({precision},{scale})"
            except:
                return "DECIMAL(18,6)"  # Default decimal

    # String/Object types
    if dtype == object or pd.api.types.is_string_dtype(series):
        # Check if it might be a date stored as string
        try:
            sample = non_null.head(100)
            pd.to_datetime(sample, errors='raise')
            return "DATETIME2"
        except:
            pass

        # Calculate max length
        max_len = non_null.astype(str).str.len().max()

        if pd.isna(max_len):
            return "VARCHAR(200)"

        max_len = int(max_len)

        for threshold in VARCHAR_THRESHOLDS:
            if max_len <= threshold:
                return f"VARCHAR({threshold})"

        if max_len <= 4000:
            return f"VARCHAR({min(((max_len // 100) + 1) * 100, 4000)})"

        return "VARCHAR(MAX)"

    # Default fallback
    return "VARCHAR(200)"


def get_table_name(filename: str) -> str:
    """Extract table name from filename (uppercase, no extension)."""
    return Path(filename).stem.upper()


def analyze_excel_file(filepath: Path) -> Tuple[str, List[Tuple[str, str]], int]:
    """
    Analyze an Excel file and return table info.

    Returns:
        Tuple of (table_name, [(column_name, sql_type), ...], row_count)
    """
    table_name = get_table_name(filepath.name)

    # Read Excel file
    df = pd.read_excel(filepath, engine='openpyxl')

    columns = []
    for col in df.columns:
        # Clean column name and add prefix
        clean_col = str(col).strip().upper()
        prefixed_col = f"{table_name}_{clean_col}"

        # Infer SQL type
        sql_type = infer_sql_type(df[col], clean_col)

        columns.append((prefixed_col, sql_type))

    return table_name, columns, len(df)


def generate_create_table_sql(table_name: str, columns: List[Tuple[str, str]]) -> str:
    """Generate CREATE TABLE SQL statement."""

    # Build column list for row_hash computation
    hash_columns = []
    for col_name, col_type in columns[:50]:  # Limit to first 50 columns for hash (SQL Server limit)
        hash_columns.append(f"COALESCE(CAST([{col_name}] AS VARCHAR(MAX)), '')")

    hash_expression = f"HASHBYTES('SHA2_256', CONCAT_WS('|', {', '.join(hash_columns)}))"

    # Build column definitions
    col_defs = []
    for col_name, col_type in columns:
        # Escape reserved words and special characters in column names
        col_defs.append(f"    [{col_name}] {col_type} NULL")

    # Join with comma+newline
    columns_sql = ",\n".join(col_defs)

    sql = f"""-- Table: {table_name}
-- Source: {table_name.lower()}.xlsx
-- Columns: {len(columns)}
CREATE TABLE [landing].[{table_name}] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS {hash_expression} PERSISTED,

    -- Source columns ({len(columns)} columns with {table_name}_ prefix)
{columns_sql},

    CONSTRAINT [PK_landing_{table_name}] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_{table_name}_timestamp] ON [landing].[{table_name}]([extraction_timestamp]);
CREATE INDEX [IX_landing_{table_name}_hash] ON [landing].[{table_name}]([row_hash]);
"""
    return sql


def generate_etl_metadata_tables() -> str:
    """Generate ETL metadata tables SQL."""
    return """-- ============================================================================
-- ETL Metadata Tables
-- ============================================================================

-- Extraction logging table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_extraction_log' AND schema_id = SCHEMA_ID('landing'))
CREATE TABLE [landing].[etl_extraction_log] (
    [log_id] BIGINT IDENTITY(1,1) PRIMARY KEY,
    [table_name] VARCHAR(50) NOT NULL,
    [extraction_type] VARCHAR(20) NOT NULL,  -- 'FULL' or 'INCREMENTAL'
    [extraction_start] DATETIME2 NOT NULL,
    [extraction_end] DATETIME2 NULL,
    [source_row_count] INT NULL,
    [extracted_row_count] INT NULL,
    [status] VARCHAR(20) NOT NULL,  -- 'RUNNING', 'SUCCESS', 'FAILED'
    [error_message] VARCHAR(MAX) NULL,
    [created_at] DATETIME2 DEFAULT GETUTCDATE()
);

CREATE INDEX [IX_etl_extraction_log_table] ON [landing].[etl_extraction_log]([table_name], [extraction_start]);

-- Checkpoint table for incremental loads
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_checkpoint' AND schema_id = SCHEMA_ID('landing'))
CREATE TABLE [landing].[etl_checkpoint] (
    [checkpoint_id] INT IDENTITY(1,1) PRIMARY KEY,
    [table_name] VARCHAR(50) UNIQUE NOT NULL,
    [last_successful_extraction] DATETIME2 NULL,
    [last_row_count] INT NULL,
    [last_hash] VARCHAR(64) NULL,
    [updated_at] DATETIME2 DEFAULT GETUTCDATE()
);

"""


def main():
    """Main function to generate landing schema."""
    print("=" * 80)
    print("Landing Schema Generator")
    print("=" * 80)
    print(f"Source directory: {SOURCE_DIR}")
    print(f"Output file: {OUTPUT_FILE}")
    print()

    # Find all Excel files
    excel_files = sorted(SOURCE_DIR.glob("*.xlsx"))
    print(f"Found {len(excel_files)} Excel files")
    print()

    if not excel_files:
        print("ERROR: No Excel files found!")
        sys.exit(1)

    # Analyze each file
    tables_info = []
    errors = []

    for i, filepath in enumerate(excel_files, 1):
        print(f"[{i}/{len(excel_files)}] Analyzing {filepath.name}...", end=" ")

        try:
            table_name, columns, row_count = analyze_excel_file(filepath)
            tables_info.append((table_name, columns, row_count, filepath.name))
            print(f"OK - {len(columns)} columns, {row_count:,} rows")
        except Exception as e:
            errors.append((filepath.name, str(e)))
            print(f"ERROR: {e}")

    print()

    if errors:
        print(f"WARNING: {len(errors)} files had errors:")
        for filename, error in errors:
            print(f"  - {filename}: {error}")
        print()

    # Generate SQL
    print("Generating SQL DDL...")

    sql_parts = []

    # Header
    sql_parts.append(f"""-- ============================================================================
-- Landing Schema - Auto-generated from Excel Source Files
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Source Directory: {SOURCE_DIR}
-- Tables: {len(tables_info)}
-- ============================================================================

-- Create landing schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'landing')
    EXEC('CREATE SCHEMA landing');
GO

""")

    # ETL metadata tables
    sql_parts.append(generate_etl_metadata_tables())
    sql_parts.append("GO\n\n")

    # Table summary
    sql_parts.append("-- ============================================================================\n")
    sql_parts.append("-- Table Summary\n")
    sql_parts.append("-- ============================================================================\n")
    sql_parts.append("/*\n")
    sql_parts.append(f"{'Table':<15} {'Columns':>10} {'Rows':>15} {'Source File':<20}\n")
    sql_parts.append("-" * 65 + "\n")
    for table_name, columns, row_count, filename in tables_info:
        sql_parts.append(f"{table_name:<15} {len(columns):>10} {row_count:>15,} {filename:<20}\n")
    sql_parts.append("-" * 65 + "\n")
    total_cols = sum(len(c) for _, c, _, _ in tables_info)
    total_rows = sum(r for _, _, r, _ in tables_info)
    sql_parts.append(f"{'TOTAL':<15} {total_cols:>10} {total_rows:>15,}\n")
    sql_parts.append("*/\n\n")

    # Drop existing tables (in reverse order for dependencies)
    sql_parts.append("-- ============================================================================\n")
    sql_parts.append("-- Drop Existing Tables (if recreating)\n")
    sql_parts.append("-- ============================================================================\n")
    sql_parts.append("/*\n")
    sql_parts.append("-- Uncomment to drop existing tables before recreation\n")
    for table_name, _, _, _ in reversed(tables_info):
        sql_parts.append(f"DROP TABLE IF EXISTS [landing].[{table_name}];\n")
    sql_parts.append("*/\n")
    sql_parts.append("GO\n\n")

    # Create tables
    sql_parts.append("-- ============================================================================\n")
    sql_parts.append("-- Landing Tables\n")
    sql_parts.append("-- ============================================================================\n\n")

    for table_name, columns, row_count, filename in tables_info:
        create_sql = generate_create_table_sql(table_name, columns)
        sql_parts.append(create_sql)
        sql_parts.append("GO\n\n")

    # Write output file
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(''.join(sql_parts))

    print(f"Generated SQL written to: {OUTPUT_FILE}")
    print()
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Total tables: {len(tables_info)}")
    print(f"Total columns: {total_cols}")
    print(f"Total rows: {total_rows:,}")
    if errors:
        print(f"Errors: {len(errors)}")
    print()
    print("Next steps:")
    print("1. Review the generated SQL in database/schemas/01_landing.sql")
    print("2. Execute against your SQL Server database")
    print("3. Run load_landing_data.py to load Excel data")


if __name__ == "__main__":
    main()
