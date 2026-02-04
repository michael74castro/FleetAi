"""
Load Set2 and Set3 Data to SQLite Landing Tables
Loads 8 tables from Excel/CSV source files into SQLite landing tables.

Set2 tables: CCDT, CCES, CCMS, CCPI, CCRC, CCRP, CCSU
Set3 tables: CCCR (10 files)
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime
import time

# Configuration
SET2_DIR = r"C:\Users\X1Carbon\Documents\FleetAI\Files\Set2"
SET3_DIR = r"C:\Users\X1Carbon\Documents\FleetAI\Files\Set3"
DB_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\fleetai.db"

# Table configurations
# Each entry: table_name -> {file, file_type, columns}
TABLE_CONFIGS = {
    'landing_CCDT': {
        'file': os.path.join(SET2_DIR, 'ccdt.xlsx'),
        'file_type': 'xlsx',
        'columns': [
            'DTCYCD', 'DTDMID', 'DTDMVA', 'DTDMLN', 'DTDMTX',
        ],
    },
    'landing_CCES': {
        'file': os.path.join(SET2_DIR, 'cces.csv'),
        'file_type': 'csv',
        'columns': [
            'ESCUNO', 'ESPCNO', 'ESOBNO', 'ESESSQ', 'ESESCD', 'ESESCT',
            'ESESIV', 'ESIVSU', 'ESTMCT', 'ESTMIV', 'ESLPCD', 'ESRPPD',
            'ESCOUC', 'ESVOLC', 'ESDISC', 'ESCONC', 'ESCURC',
        ],
    },
    'landing_CCMS': {
        'file': os.path.join(SET2_DIR, 'ccms.xlsx'),
        'file_type': 'xlsx',
        'columns': [
            'MSOBNO', 'MSGDSQ', 'MSGDCC', 'MSGDYY', 'MSGDMM', 'MSGDDD',
            'MSGDKM', 'MSGDAM', 'MSGDDS', 'MSGDD2', 'MSGDD3', 'MSGDSC',
            'MSMSTY', 'MSNUFO', 'MSNAFO', 'MSMJCD', 'MSMNCD', 'MSRPPD',
            'MSCOUC', 'MSVOLC', 'MSDISC', 'MSCONC', 'MSCURC', 'MSSIRN',
            'MSDAFC', 'MSDAFY', 'MSDAFM', 'MSDAFD',
        ],
    },
    'landing_CCPI': {
        'file': os.path.join(SET2_DIR, 'ccpi.xlsx'),
        'file_type': 'xlsx',
        'sheets': 2,
        'columns': [
            'PICONO', 'PICUNO', 'PINACD', 'PIOBNO', 'PIPCNO', 'PIPIAM',
            'PIPICD', 'PIEBRP', 'PIPIDR', 'PIPIDS', 'PIPIGR', 'PIPIIV',
            'PIPILP', 'PIPIOB', 'PIPIOR', 'PIPIRN', 'PIPISC', 'PIPIVT',
            'PIRPPD', 'PICOUC',
        ],
    },
    'landing_CCRC': {
        'file': os.path.join(SET2_DIR, 'ccrc.xlsx'),
        'file_type': 'xlsx',
        'columns': [
            'RCOBNO', 'RCRCNO', 'RCRCSQ', 'RCDRNO', 'RCRCRN', 'RCRCBC',
            'RCRCBY', 'RCRCBM', 'RCRCBD', 'RCRCEC', 'RCRCEY', 'RCRCEM',
            'RCRCED', 'RCRCCD', 'RCRCKM', 'RCRCAM', 'RCRCRS', 'RCRCDS',
            'RCRCD2', 'RCRCD3', 'RCRCTY', 'RCRCDR', 'RCRCSC', 'RCRPPD',
            'RCCOUC',
        ],
    },
    'landing_CCRP': {
        'file': os.path.join(SET2_DIR, 'ccrp.xlsx'),
        'file_type': 'xlsx',
        'columns': [
            'RPRPCC', 'RPRPYY', 'RPRPMM', 'RPRPDD', 'RPRPPD', 'RPMTPD',
        ],
    },
    'landing_CCSU': {
        'file': os.path.join(SET2_DIR, 'ccsu.xlsx'),
        'file_type': 'xlsx',
        'columns': [
            'SUNUFO', 'SUNAFO', 'SUNOFO', 'SUNOF2', 'SUNOF3', 'SUCLFO',
            'SUCPFO', 'SUADFO', 'SULOFO', 'SUCAFO', 'SUNTFO', 'SUNFFO',
            'SUNEFO', 'SUCTFO', 'SURSFO', 'SURPPD', 'SUCOUC',
        ],
    },
    'landing_CCCR': {
        'file_pattern': os.path.join(SET3_DIR, 'cccr_{i}.xlsx'),
        'file_type': 'xlsx',
        'file_count': 10,
        'columns': [
            'CROBNO', 'CRBEAM', 'CRBELT', 'CRDIAM', 'CRDILT', 'CRGAAM',
            'CRGALT', 'CRFSBV', 'CRFSIR', 'CRFUCT', 'CRMACT', 'CRRCCT',
            'CRTRCT', 'CRFUIV', 'CRMAIV', 'CRRCIV', 'CRTRIV', 'CRFSKM',
            'CRKMCC', 'CRKMYY', 'CRKMMM', 'CRKMDD', 'CRFSIK', 'CRKMDR',
            'CRMMDR', 'CRKMTE', 'CRFUCK', 'CRFUIK', 'CRRECO', 'CRFUSL',
            'CRFUMD', 'CRMACK', 'CRMAIK', 'CRMASL', 'CRMAMD', 'CRRCCK',
            'CRRCIK', 'CRRCSL', 'CRRCMD', 'CRRCKM', 'CRRCAM', 'CRTRCK',
            'CRTRIK', 'CRTRSL', 'CRTRMD', 'CRTOCT', 'CRTOIV', 'CRCTKM',
            'CRTOSU', 'CRTOSA', 'CRTOTO', 'CRLWKM', 'CRUPKM', 'CRMANR',
            'CRFUNR', 'CRRCNR', 'CRTRNR', 'CRTNNR', 'CRTWNR', 'CRPRKT',
            'CRDANB', 'CRDARE', 'CRSG01', 'CRSG02', 'CRSG03', 'CRSG04',
            'CRSG05', 'CRSG06', 'CRSG07', 'CRSG08', 'CRSG09', 'CRSG10',
            'CRSG11', 'CRSG12', 'CRSG13', 'CRSG14', 'CRSG15', 'CRRPPD',
            'CRCOUC', 'CRVOLC', 'CRDISC', 'CRCONC', 'CRCURC', 'CRMIAM',
            'CRMIPY', 'CRMIRN', 'CRTSAM', 'CRTSPY', 'CRTSRN', 'CRTRNO',
            'CRPACT', 'CRPAIV', 'CRPAMD', 'CRPASL', 'CRUNCT', 'CRUNIV',
            'CRUNMD', 'CRUNSL', 'CRWACT', 'CRWAIV', 'CRWAMD', 'CRWASL',
        ],
    },
}


def get_connection():
    """Get database connection."""
    return sqlite3.connect(DB_PATH)


def create_landing_table(cursor, table_name, columns):
    """Drop and recreate a landing table with all TEXT columns."""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    col_defs = []
    for col in columns:
        col_defs.append(f"    [{col}] TEXT")
    col_defs.append("    extraction_timestamp TEXT")
    col_defs.append("    row_hash TEXT")

    create_sql = f"CREATE TABLE {table_name} (\n"
    create_sql += ",\n".join(col_defs)
    create_sql += "\n);"

    cursor.execute(create_sql)


def read_source_file(filepath, file_type, sheet_name=0):
    """Read a source file into a DataFrame."""
    if file_type == 'csv':
        return pd.read_csv(filepath, dtype=str)
    else:
        return pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)


def map_source_columns(df, expected_columns):
    """
    Map source DataFrame columns to expected landing column names.
    Source files may have columns with or without the table prefix.
    This function matches source columns to expected columns by stripping
    any known table prefix from source column names.
    """
    source_cols = list(df.columns)
    col_mapping = {}

    # Try direct match first (source column == expected column)
    for expected in expected_columns:
        if expected in source_cols:
            col_mapping[expected] = expected

    # If all matched directly, return
    if len(col_mapping) == len(expected_columns):
        return df[list(col_mapping.keys())].rename(columns=col_mapping)

    # Try matching by position if column count matches
    if len(source_cols) == len(expected_columns):
        col_mapping = {src: exp for src, exp in zip(source_cols, expected_columns)}
        return df.rename(columns=col_mapping)

    # Try matching by stripping common prefixes from source columns
    # e.g., source "CCDT_DTCYCD" -> expected "DTCYCD"
    col_mapping = {}
    for src_col in source_cols:
        for expected in expected_columns:
            if expected not in col_mapping.values():
                # Check if source ends with the expected column name
                if src_col.upper().endswith(expected.upper()):
                    col_mapping[src_col] = expected
                    break
                # Check if source matches expected after removing prefix
                parts = src_col.split('_', 1)
                if len(parts) > 1 and parts[1].upper() == expected.upper():
                    col_mapping[src_col] = expected
                    break

    if col_mapping:
        matched_cols = list(col_mapping.keys())
        return df[matched_cols].rename(columns=col_mapping)

    # Fallback: use positional mapping for available columns
    n = min(len(source_cols), len(expected_columns))
    col_mapping = {source_cols[i]: expected_columns[i] for i in range(n)}
    return df[list(col_mapping.keys())].rename(columns=col_mapping)


def prepare_data(df, extraction_ts):
    """
    Prepare DataFrame rows for batch insert.
    Converts all values to strings (or None for NaN), adds metadata columns.
    """
    data = []
    for _, row in df.iterrows():
        values = []
        for val in row.values:
            if pd.isna(val):
                values.append(None)
            else:
                values.append(str(val) if val is not None else None)
        values.append(extraction_ts)  # extraction_timestamp
        values.append(None)           # row_hash
        data.append(tuple(values))
    return data


def load_table(conn, table_name, config, extraction_ts):
    """
    Generic function to load a single table from its source file(s).
    Returns the number of rows loaded.
    """
    print(f"  Loading {table_name}...", end=" ", flush=True)

    try:
        cursor = conn.cursor()
        columns = config['columns']

        # Create the landing table
        create_landing_table(cursor, table_name, columns)
        conn.commit()

        # Build insert SQL
        all_cols = columns + ['extraction_timestamp', 'row_hash']
        placeholders = ", ".join(["?" for _ in all_cols])
        column_list = ", ".join([f"[{col}]" for col in all_cols])
        insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

        total_rows = 0

        # Handle CCPI special case: 2 sheets
        if config.get('sheets'):
            frames = []
            for sheet_idx in range(config['sheets']):
                df = read_source_file(config['file'], config['file_type'], sheet_name=sheet_idx)
                df = map_source_columns(df, columns)
                frames.append(df)
                print(f"sheet{sheet_idx}={len(df)}", end=" ", flush=True)
            df = pd.concat(frames, ignore_index=True)

        # Handle CCCR special case: 10 files
        elif config.get('file_count'):
            frames = []
            for i in range(1, config['file_count'] + 1):
                filepath = config['file_pattern'].format(i=i)
                if not os.path.exists(filepath):
                    print(f"\n    WARNING: {filepath} not found, skipping.", end=" ", flush=True)
                    continue
                df_part = read_source_file(filepath, config['file_type'])
                df_part = map_source_columns(df_part, columns)
                frames.append(df_part)
                print(f"f{i}={len(df_part)}", end=" ", flush=True)
            if not frames:
                print("NO FILES FOUND")
                return 0
            df = pd.concat(frames, ignore_index=True)

        # Standard single-file table
        else:
            df = read_source_file(config['file'], config['file_type'])
            df = map_source_columns(df, columns)

        # Prepare data for batch insert
        data = prepare_data(df, extraction_ts)

        # Batch insert using executemany
        cursor.executemany(insert_sql, data)
        conn.commit()

        total_rows = len(data)

        # Create index on extraction_timestamp
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp "
            f"ON {table_name}(extraction_timestamp)"
        )
        conn.commit()

        print(f"{total_rows:,} rows OK")
        return total_rows

    except Exception as e:
        print(f"FAILED - {e}")
        return 0


def main():
    start_time = time.time()
    extraction_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print("=" * 60)
    print("FleetAI: Load Set2 & Set3 to Landing (SQLite)")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"Set2 source: {SET2_DIR}")
    print(f"Set3 source: {SET3_DIR}")
    print(f"Extraction timestamp: {extraction_ts}")
    print()

    # Verify source directories exist
    for d, label in [(SET2_DIR, 'Set2'), (SET3_DIR, 'Set3')]:
        if not os.path.isdir(d):
            print(f"WARNING: {label} directory not found: {d}")

    conn = get_connection()
    print("Connected to SQLite database.")
    print()

    # Load all tables
    print("Loading landing tables...")
    results = {}

    for table_name, config in TABLE_CONFIGS.items():
        rows = load_table(conn, table_name, config, extraction_ts)
        results[table_name] = rows

    # Summary
    elapsed = time.time() - start_time
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)

    total_rows = 0
    successful = 0
    failed = 0

    for table_name, rows in results.items():
        status = "OK" if rows > 0 else "FAILED"
        print(f"  {table_name}: {rows:,} rows [{status}]")
        total_rows += rows
        if rows > 0:
            successful += 1
        else:
            failed += 1

    print()
    print(f"Tables loaded: {successful}")
    print(f"Tables failed: {failed}")
    print(f"Total rows: {total_rows:,}")
    print(f"Elapsed time: {elapsed:.1f}s")
    print(f"Database size: {os.path.getsize(DB_PATH) / (1024*1024):.2f} MB")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if failed == 0:
        print("\nAll tables loaded successfully!")
    else:
        print(f"\nWARNING: {failed} table(s) failed to load.")

    conn.close()


if __name__ == "__main__":
    main()
