"""
Load Initial Data from Excel Files to Landing Schema
Generates SQL INSERT statements for bulk loading into SQL Server
"""

import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import math

# Configuration
EXCEL_DIR = r"C:\Users\X1Carbon\Documents\FleetAI\Files"
OUTPUT_DIR = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\data"
BATCH_SIZE = 1000  # Rows per INSERT statement

def clean_value(val, col_name):
    """Convert a value to SQL-safe string representation."""
    if pd.isna(val) or val is None:
        return "NULL"

    if isinstance(val, str):
        # Escape single quotes
        escaped = val.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(val, (int, float)):
        if isinstance(val, float):
            if math.isnan(val) or math.isinf(val):
                return "NULL"
            # Check if it's a whole number
            if val == int(val):
                return str(int(val))
        return str(val)

    if isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"

    # Default: convert to string
    return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"


def generate_insert_statements(df, table_name, prefix):
    """Generate INSERT statements for a DataFrame."""
    statements = []

    # Get column names with prefix
    columns = [f"[{prefix}_{col}]" for col in df.columns]
    column_list = ", ".join(columns)

    # Process in batches
    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]

        values_list = []
        for _, row in batch.iterrows():
            values = [clean_value(val, col) for val, col in zip(row.values, df.columns)]
            values_list.append(f"({', '.join(values)})")

        insert = f"INSERT INTO [landing].[{table_name}] ({column_list})\nVALUES\n"
        insert += ",\n".join(values_list)
        insert += ";\n"
        statements.append(insert)

    return statements


def process_excel_file(filepath):
    """Process a single Excel file and return SQL statements."""
    filename = os.path.basename(filepath)
    table_name = filename.replace('.xlsx', '').upper()
    prefix = table_name

    print(f"Processing {filename}...")

    try:
        df = pd.read_excel(filepath)
        print(f"  - {len(df)} rows, {len(df.columns)} columns")

        statements = generate_insert_statements(df, table_name, prefix)
        return table_name, statements, len(df)

    except Exception as e:
        print(f"  - ERROR: {e}")
        return table_name, [], 0


def main():
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Get all Excel files
    excel_files = sorted([f for f in os.listdir(EXCEL_DIR) if f.endswith('.xlsx')])

    print(f"Found {len(excel_files)} Excel files")
    print("=" * 60)

    # Master script that runs all loads
    master_script = []
    master_script.append("-- ============================================================================")
    master_script.append("-- Master Data Load Script - Auto-generated")
    master_script.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    master_script.append("-- ============================================================================")
    master_script.append("")
    master_script.append("SET NOCOUNT ON;")
    master_script.append("SET XACT_ABORT ON;")
    master_script.append("")

    total_rows = 0

    for filename in excel_files:
        filepath = os.path.join(EXCEL_DIR, filename)
        table_name, statements, row_count = process_excel_file(filepath)
        total_rows += row_count

        if statements:
            # Write individual table file
            output_file = os.path.join(OUTPUT_DIR, f"load_{table_name.lower()}.sql")
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Load data for {table_name}\n")
                f.write(f"-- Rows: {row_count}\n")
                f.write(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("SET NOCOUNT ON;\n\n")
                f.write(f"PRINT 'Loading {table_name}...';\n\n")

                for stmt in statements:
                    f.write(stmt)
                    f.write("\n")

                f.write(f"\nPRINT '{table_name} loaded: {row_count} rows';\n")

            # Add to master script
            master_script.append(f"PRINT 'Loading {table_name} ({row_count} rows)...';")
            master_script.append(f":r {output_file}")
            master_script.append("")

    master_script.append("")
    master_script.append(f"PRINT 'All data loaded successfully. Total rows: {total_rows}';")

    # Write master script
    master_file = os.path.join(OUTPUT_DIR, "00_load_all_data.sql")
    with open(master_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(master_script))

    print("=" * 60)
    print(f"Total rows: {total_rows}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Master script: {master_file}")


if __name__ == "__main__":
    main()
