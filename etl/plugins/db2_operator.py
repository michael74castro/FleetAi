"""
FleetAI - DB2 for i (IBM Power10) ODBC Operator for Airflow
Provides operators for extracting data from IBM i DB2 via ODBC
"""

from typing import Any, Optional, Sequence
import hashlib
import logging
from datetime import datetime

from airflow.models import BaseOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class DB2ExtractOperator(BaseOperator):
    """
    Operator to extract data from IBM DB2 for i via ODBC and load into MSSQL landing tables.

    :param source_table: Source table name in DB2
    :param target_table: Target table name in MSSQL landing schema
    :param db2_conn_id: Airflow connection ID for DB2 ODBC
    :param mssql_conn_id: Airflow connection ID for MSSQL
    :param columns: List of columns to extract (None = all)
    :param batch_size: Number of rows per batch insert
    :param extraction_type: 'daily' or 'monthly'
    """

    template_fields: Sequence[str] = ('source_table', 'target_table')
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(
        self,
        source_table: str,
        target_table: str,
        db2_conn_id: str = 'db2_iseries',
        mssql_conn_id: str = 'mssql_fleetai',
        columns: Optional[list] = None,
        batch_size: int = 10000,
        extraction_type: str = 'daily',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.source_table = source_table
        self.target_table = target_table
        self.db2_conn_id = db2_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.columns = columns
        self.batch_size = batch_size
        self.extraction_type = extraction_type

    def execute(self, context: Any) -> dict:
        """Execute the extraction"""
        extraction_start = datetime.utcnow()
        log_id = None

        try:
            # Log extraction start
            log_id = self._log_extraction_start()

            # Get hooks
            db2_hook = OdbcHook(odbc_conn_id=self.db2_conn_id)
            mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)

            # Build SELECT query
            columns_str = ', '.join(self.columns) if self.columns else '*'
            source_query = f"SELECT {columns_str} FROM {self.source_table}"

            logger.info(f"Extracting from DB2: {source_query}")

            # Execute query and fetch all data
            source_data = db2_hook.get_records(source_query)
            source_row_count = len(source_data)

            logger.info(f"Fetched {source_row_count} rows from {self.source_table}")

            if source_row_count == 0:
                self._log_extraction_end(log_id, 0, 0, 'success')
                return {'source_rows': 0, 'extracted_rows': 0}

            # Get column names from cursor
            cursor_description = db2_hook.get_cursor().description
            column_names = [col[0] for col in cursor_description] if cursor_description else self.columns

            # Insert into MSSQL landing table in batches
            extracted_count = 0
            for i in range(0, source_row_count, self.batch_size):
                batch = source_data[i:i + self.batch_size]
                self._insert_batch(mssql_hook, batch, column_names)
                extracted_count += len(batch)
                logger.info(f"Inserted batch {i // self.batch_size + 1}: {extracted_count}/{source_row_count}")

            # Log success
            self._log_extraction_end(log_id, source_row_count, extracted_count, 'success')

            return {
                'source_rows': source_row_count,
                'extracted_rows': extracted_count,
                'source_table': self.source_table,
                'target_table': self.target_table
            }

        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            if log_id:
                self._log_extraction_end(log_id, 0, 0, 'failed', str(e))
            raise

    def _insert_batch(self, hook: MsSqlHook, batch: list, columns: list) -> None:
        """Insert a batch of records into MSSQL"""
        if not batch:
            return

        placeholders = ', '.join(['?' for _ in columns])
        columns_str = ', '.join(columns)

        insert_sql = f"""
            INSERT INTO landing.{self.target_table} ({columns_str})
            VALUES ({placeholders})
        """

        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.executemany(insert_sql, batch)
            conn.commit()
        finally:
            cursor.close()

    def _log_extraction_start(self) -> int:
        """Log extraction start to tracking table"""
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)

        sql = """
            INSERT INTO landing.etl_extraction_log
            (table_name, extraction_type, extraction_start, status)
            OUTPUT INSERTED.log_id
            VALUES (?, ?, GETUTCDATE(), 'running')
        """

        conn = mssql_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (self.target_table, self.extraction_type))
            log_id = cursor.fetchone()[0]
            conn.commit()
            return log_id
        finally:
            cursor.close()

    def _log_extraction_end(
        self,
        log_id: int,
        source_count: int,
        extracted_count: int,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """Update extraction log with results"""
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)

        sql = """
            UPDATE landing.etl_extraction_log
            SET extraction_end = GETUTCDATE(),
                source_row_count = ?,
                extracted_row_count = ?,
                status = ?,
                error_message = ?
            WHERE log_id = ?
        """

        conn = mssql_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (source_count, extracted_count, status, error_message, log_id))
            conn.commit()
        finally:
            cursor.close()


class DB2ToMSSQLCDCOperator(BaseOperator):
    """
    Operator that performs CDC (Change Data Capture) logic:
    - Compares source data hash with existing landing data
    - Skip unchanged, Update modified, Insert new records

    :param source_table: Source table in DB2
    :param landing_table: Landing table in MSSQL
    :param staging_table: Staging table in MSSQL
    :param key_columns: List of columns that form the business key
    :param compare_columns: Columns to include in hash comparison
    """

    template_fields: Sequence[str] = ('source_table', 'landing_table', 'staging_table')
    ui_color = '#e8e4f0'

    @apply_defaults
    def __init__(
        self,
        source_table: str,
        landing_table: str,
        staging_table: str,
        key_columns: list,
        compare_columns: Optional[list] = None,
        db2_conn_id: str = 'db2_iseries',
        mssql_conn_id: str = 'mssql_fleetai',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.source_table = source_table
        self.landing_table = landing_table
        self.staging_table = staging_table
        self.key_columns = key_columns
        self.compare_columns = compare_columns
        self.db2_conn_id = db2_conn_id
        self.mssql_conn_id = mssql_conn_id

    def execute(self, context: Any) -> dict:
        """Execute CDC transformation"""
        db2_hook = OdbcHook(odbc_conn_id=self.db2_conn_id)
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)

        stats = {
            'inserts': 0,
            'updates': 0,
            'unchanged': 0,
            'source_table': self.source_table,
            'staging_table': self.staging_table
        }

        # Fetch source data
        source_query = f"SELECT * FROM {self.source_table}"
        source_records = db2_hook.get_records(source_query)

        if not source_records:
            logger.info(f"No records found in {self.source_table}")
            return stats

        # Get column names
        cursor_description = db2_hook.get_cursor().description
        columns = [col[0] for col in cursor_description]

        # Build key index
        key_indices = [columns.index(k) for k in self.key_columns]

        # Get existing staging data hashes
        existing_hashes = self._get_existing_hashes(mssql_hook)

        # Process each record
        inserts = []
        updates = []

        for record in source_records:
            # Build business key
            key = tuple(record[i] for i in key_indices)
            key_str = '|'.join(str(v) for v in key)

            # Calculate hash
            hash_input = '|'.join(str(v) if v is not None else '' for v in record)
            record_hash = hashlib.sha256(hash_input.encode()).hexdigest()

            existing_hash = existing_hashes.get(key_str)

            if existing_hash is None:
                # New record
                inserts.append((record, record_hash))
                stats['inserts'] += 1
            elif existing_hash != record_hash:
                # Modified record
                updates.append((record, record_hash, key))
                stats['updates'] += 1
            else:
                # Unchanged
                stats['unchanged'] += 1

        # Execute inserts
        if inserts:
            self._execute_inserts(mssql_hook, inserts, columns)

        # Execute updates
        if updates:
            self._execute_updates(mssql_hook, updates, columns)

        logger.info(f"CDC Complete: {stats}")
        return stats

    def _get_existing_hashes(self, hook: MsSqlHook) -> dict:
        """Get existing record hashes from staging table"""
        key_concat = "CONCAT_WS('|', " + ', '.join(self.key_columns) + ")"

        sql = f"""
            SELECT {key_concat} as business_key,
                   CONVERT(VARCHAR(64), source_hash, 2) as hash_value
            FROM staging.{self.staging_table}
            WHERE is_current = 1
        """

        records = hook.get_records(sql)
        return {r[0]: r[1] for r in records}

    def _execute_inserts(self, hook: MsSqlHook, records: list, columns: list) -> None:
        """Insert new records into staging"""
        # Build INSERT statement
        col_list = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])

        insert_sql = f"""
            INSERT INTO staging.{self.staging_table}
            ({col_list}, source_hash, valid_from, is_current)
            VALUES ({placeholders}, HASHBYTES('SHA2_256', ?), GETUTCDATE(), 1)
        """

        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            for record, hash_val in records:
                hash_input = '|'.join(str(v) if v is not None else '' for v in record)
                cursor.execute(insert_sql, (*record, hash_input))
            conn.commit()
        finally:
            cursor.close()

    def _execute_updates(self, hook: MsSqlHook, records: list, columns: list) -> None:
        """Update existing records using SCD Type 2"""
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            for record, hash_val, key in records:
                # Close existing record
                key_conditions = ' AND '.join([f"{k} = ?" for k in self.key_columns])
                close_sql = f"""
                    UPDATE staging.{self.staging_table}
                    SET valid_to = GETUTCDATE(), is_current = 0
                    WHERE {key_conditions} AND is_current = 1
                """
                cursor.execute(close_sql, key)

                # Insert new version
                col_list = ', '.join(columns)
                placeholders = ', '.join(['?' for _ in columns])
                hash_input = '|'.join(str(v) if v is not None else '' for v in record)

                insert_sql = f"""
                    INSERT INTO staging.{self.staging_table}
                    ({col_list}, source_hash, valid_from, is_current)
                    VALUES ({placeholders}, HASHBYTES('SHA2_256', ?), GETUTCDATE(), 1)
                """
                cursor.execute(insert_sql, (*record, hash_input))

            conn.commit()
        finally:
            cursor.close()


class DataQualityCheckOperator(BaseOperator):
    """
    Operator to perform data quality checks on extracted data.

    :param table_name: Table to check
    :param checks: List of check configurations
    """

    template_fields: Sequence[str] = ('table_name',)
    ui_color = '#f0e4e8'

    @apply_defaults
    def __init__(
        self,
        table_name: str,
        checks: list,
        mssql_conn_id: str = 'mssql_fleetai',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.checks = checks
        self.mssql_conn_id = mssql_conn_id

    def execute(self, context: Any) -> dict:
        """Execute data quality checks"""
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        results = []

        for check in self.checks:
            check_type = check.get('type')
            check_name = check.get('name', check_type)

            try:
                if check_type == 'row_count':
                    result = self._check_row_count(hook, check)
                elif check_type == 'null_check':
                    result = self._check_nulls(hook, check)
                elif check_type == 'duplicate_check':
                    result = self._check_duplicates(hook, check)
                elif check_type == 'freshness':
                    result = self._check_freshness(hook, check)
                else:
                    result = {'status': 'skipped', 'message': f'Unknown check type: {check_type}'}

                result['check_name'] = check_name
                results.append(result)

                if result.get('status') == 'failed' and check.get('fail_on_error', False):
                    raise ValueError(f"Data quality check failed: {check_name}")

            except Exception as e:
                if check.get('fail_on_error', False):
                    raise
                results.append({
                    'check_name': check_name,
                    'status': 'error',
                    'message': str(e)
                })

        logger.info(f"Data quality checks complete: {results}")
        return {'checks': results}

    def _check_row_count(self, hook: MsSqlHook, check: dict) -> dict:
        """Check minimum row count"""
        min_count = check.get('min_count', 1)

        sql = f"SELECT COUNT(*) FROM {self.table_name}"
        result = hook.get_first(sql)
        count = result[0] if result else 0

        return {
            'status': 'passed' if count >= min_count else 'failed',
            'actual': count,
            'expected_min': min_count
        }

    def _check_nulls(self, hook: MsSqlHook, check: dict) -> dict:
        """Check for null values in specified columns"""
        columns = check.get('columns', [])
        max_null_pct = check.get('max_null_percentage', 0)

        results = []
        for col in columns:
            sql = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_count
                FROM {self.table_name}
            """
            result = hook.get_first(sql)
            total, null_count = result if result else (0, 0)
            null_pct = (null_count / total * 100) if total > 0 else 0

            results.append({
                'column': col,
                'null_count': null_count,
                'null_percentage': null_pct,
                'passed': null_pct <= max_null_pct
            })

        all_passed = all(r['passed'] for r in results)
        return {
            'status': 'passed' if all_passed else 'failed',
            'details': results
        }

    def _check_duplicates(self, hook: MsSqlHook, check: dict) -> dict:
        """Check for duplicate records based on key columns"""
        key_columns = check.get('key_columns', [])
        key_str = ', '.join(key_columns)

        sql = f"""
            SELECT {key_str}, COUNT(*) as cnt
            FROM {self.table_name}
            GROUP BY {key_str}
            HAVING COUNT(*) > 1
        """
        duplicates = hook.get_records(sql)
        dup_count = len(duplicates)

        return {
            'status': 'passed' if dup_count == 0 else 'warning',
            'duplicate_groups': dup_count,
            'sample_duplicates': duplicates[:5] if duplicates else []
        }

    def _check_freshness(self, hook: MsSqlHook, check: dict) -> dict:
        """Check data freshness based on timestamp column"""
        timestamp_column = check.get('timestamp_column', 'extraction_timestamp')
        max_age_hours = check.get('max_age_hours', 24)

        sql = f"""
            SELECT MAX({timestamp_column}) as latest,
                   DATEDIFF(HOUR, MAX({timestamp_column}), GETUTCDATE()) as age_hours
            FROM {self.table_name}
        """
        result = hook.get_first(sql)
        latest, age_hours = result if result else (None, None)

        return {
            'status': 'passed' if age_hours is not None and age_hours <= max_age_hours else 'failed',
            'latest_timestamp': str(latest) if latest else None,
            'age_hours': age_hours,
            'max_age_hours': max_age_hours
        }
