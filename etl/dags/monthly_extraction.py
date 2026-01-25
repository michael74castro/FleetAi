"""
FleetAI - Monthly Data Extraction DAG
Extracts 4 large tables from IBM Power10 DB2 for i to MSSQL landing schema
Schedule: Monthly on 1st at 1:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

import sys
sys.path.insert(0, '/opt/airflow/plugins')
from db2_operator import DB2ExtractOperator, DB2ToMSSQLCDCOperator, DataQualityCheckOperator


default_args = {
    'owner': 'fleetai',
    'depends_on_past': False,
    'email': ['etl-alerts@fleetai.local'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=6),  # Longer timeout for monthly
}

# Monthly extraction tables - larger datasets
MONTHLY_TABLES = [
    {
        'source': 'CCEB',
        'target': 'CCEB',
        'key_cols': ['CCEB_CONTRACT_NO', 'CCEB_EQUIPMENT_ID'],
        'staging': 'vehicles',
        'description': 'Contract Equipment Base - Vehicle master data'
    },
    {
        'source': 'CWCT',
        'target': 'CWCT',
        'key_cols': ['CWCT_CONTRACT_NO', 'CWCT_TERM_ID'],
        'staging': 'contracts',
        'description': 'Contract Terms - Lease agreement terms'
    },
    {
        'source': 'CWES',
        'target': 'CWES',
        'key_cols': ['CWES_EQUIPMENT_ID', 'CWES_SPEC_CODE'],
        'staging': 'vehicle_specifications',
        'description': 'Equipment Specifications - Vehicle specs and options'
    },
    {
        'source': 'CWFU',
        'target': 'CWFU',
        'key_cols': ['CWFU_CARD_NO'],
        'staging': 'fuel_cards',
        'description': 'Fuel Card Master - Fuel card assignments'
    },
]


def check_if_first_of_month(**context):
    """Check if running on first of month or forced via variable"""
    execution_date = context['execution_date']
    force_run = Variable.get('force_monthly_extraction', default_var='false').lower() == 'true'

    if execution_date.day == 1 or force_run:
        return 'proceed_extraction'
    return 'skip_extraction'


def send_extraction_summary(**context):
    """Send summary email of monthly extraction"""
    ti = context['ti']
    results = {}

    for table in MONTHLY_TABLES:
        task_id = f"extract_landing.extract_{table['source']}"
        try:
            result = ti.xcom_pull(task_ids=task_id)
            results[table['source']] = result
        except Exception:
            results[table['source']] = {'status': 'no data'}

    # Log summary (in production, send email)
    print("=" * 60)
    print("MONTHLY EXTRACTION SUMMARY")
    print("=" * 60)
    for table, result in results.items():
        print(f"{table}: {result}")
    print("=" * 60)

    return results


with DAG(
    dag_id='fleetai_monthly_extraction',
    default_args=default_args,
    description='Monthly extraction of 4 large tables from IBM Power10 DB2 to MSSQL',
    schedule_interval='0 1 1 * *',  # 1:00 AM on 1st of month
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['fleetai', 'etl', 'monthly', 'db2'],
) as dag:

    start = EmptyOperator(task_id='start')

    # Check if should run
    check_run = BranchPythonOperator(
        task_id='check_if_should_run',
        python_callable=check_if_first_of_month,
    )

    skip_extraction = EmptyOperator(task_id='skip_extraction')

    proceed_extraction = EmptyOperator(task_id='proceed_extraction')

    # Pre-extraction: Archive previous month's data
    archive_previous = MsSqlOperator(
        task_id='archive_previous_month',
        mssql_conn_id='mssql_fleetai',
        sql="""
            DECLARE @archive_date DATE = DATEADD(MONTH, -1, GETDATE());
            DECLARE @year_month INT = CAST(FORMAT(@archive_date, 'yyyyMM') AS INT);

            -- Archive old landing data (keep last 3 months)
            DELETE FROM landing.CCEB
            WHERE extraction_timestamp < DATEADD(MONTH, -3, GETDATE());

            DELETE FROM landing.CWCT
            WHERE extraction_timestamp < DATEADD(MONTH, -3, GETDATE());

            DELETE FROM landing.CWES
            WHERE extraction_timestamp < DATEADD(MONTH, -3, GETDATE());

            DELETE FROM landing.CWFU
            WHERE extraction_timestamp < DATEADD(MONTH, -3, GETDATE());

            -- Log archive action
            INSERT INTO landing.etl_extraction_log (table_name, extraction_type, extraction_start, status)
            VALUES ('_ARCHIVE_MONTHLY', 'monthly', GETUTCDATE(), 'success');
        """
    )

    # Extract landing tables
    with TaskGroup(group_id='extract_landing') as extract_group:
        for table in MONTHLY_TABLES:
            DB2ExtractOperator(
                task_id=f"extract_{table['source']}",
                source_table=table['source'],
                target_table=table['target'],
                extraction_type='monthly',
                batch_size=50000,  # Larger batches for monthly
                pool='db2_extraction_pool',
            )

    # Quality checks with stricter thresholds for monthly
    with TaskGroup(group_id='quality_checks') as quality_group:
        for table in MONTHLY_TABLES:
            DataQualityCheckOperator(
                task_id=f"quality_{table['target']}",
                table_name=f"landing.{table['target']}",
                checks=[
                    {
                        'type': 'row_count',
                        'name': f"row_count_{table['target']}",
                        'min_count': 100,  # Monthly tables should have data
                        'fail_on_error': True
                    },
                    {
                        'type': 'duplicate_check',
                        'name': f"duplicates_{table['target']}",
                        'key_columns': table['key_cols']
                    },
                    {
                        'type': 'null_check',
                        'name': f"nulls_{table['target']}",
                        'columns': table['key_cols'],
                        'max_null_percentage': 0  # Keys should never be null
                    }
                ]
            )

    # CDC to staging
    with TaskGroup(group_id='transform_staging') as transform_group:
        for table in MONTHLY_TABLES:
            DB2ToMSSQLCDCOperator(
                task_id=f"cdc_{table['source']}",
                source_table=table['source'],
                landing_table=table['target'],
                staging_table=table['staging'],
                key_columns=table['key_cols'],
                pool='mssql_transform_pool',
            )

    # Refresh reporting dimensions with full rebuild
    refresh_dimensions = MsSqlOperator(
        task_id='refresh_dimensions_full',
        mssql_conn_id='mssql_fleetai',
        sql="""
            -- Full refresh of vehicle dimension
            -- Close all current records
            UPDATE reporting.dim_vehicle
            SET effective_to = CAST(GETDATE() AS DATE), is_current = 0
            WHERE is_current = 1;

            -- Insert fresh data
            INSERT INTO reporting.dim_vehicle (
                equipment_id, vin, license_plate, make, model, model_year,
                color, body_type, engine_type, fuel_type, transmission,
                acquisition_date, acquisition_cost, residual_value, status,
                effective_from, is_current
            )
            SELECT
                v.equipment_id,
                v.vin,
                v.license_plate,
                v.make,
                v.model,
                v.model_year,
                v.color,
                v.body_type,
                v.engine_type,
                v.fuel_type,
                v.transmission,
                v.acquisition_date,
                v.acquisition_cost,
                v.residual_value,
                v.status,
                CAST(GETDATE() AS DATE),
                1
            FROM staging.vehicles v
            WHERE v.is_current = 1;

            -- Full refresh of contract dimension
            UPDATE reporting.dim_contract
            SET effective_to = CAST(GETDATE() AS DATE), is_current = 0
            WHERE is_current = 1;

            INSERT INTO reporting.dim_contract (
                contract_no, customer_id, contract_type, start_date, end_date,
                term_months, monthly_rate, mileage_allowance, excess_mileage_rate,
                insurance_included, maintenance_included, status,
                effective_from, is_current
            )
            SELECT
                c.contract_no,
                c.customer_id,
                c.contract_type,
                c.start_date,
                c.end_date,
                c.term_months,
                c.monthly_rate,
                c.mileage_allowance,
                c.excess_mileage_rate,
                c.insurance_included,
                c.maintenance_included,
                c.status,
                CAST(GETDATE() AS DATE),
                1
            FROM staging.contracts c
            WHERE c.is_current = 1;

            -- Update statistics
            UPDATE STATISTICS reporting.dim_vehicle;
            UPDATE STATISTICS reporting.dim_contract;
        """
    )

    # Generate monthly snapshots
    generate_snapshots = MsSqlOperator(
        task_id='generate_monthly_snapshots',
        mssql_conn_id='mssql_fleetai',
        sql="""
            DECLARE @snapshot_date DATE = EOMONTH(DATEADD(MONTH, -1, GETDATE()));
            DECLARE @date_key INT = CAST(FORMAT(@snapshot_date, 'yyyyMMdd') AS INT);
            DECLARE @year_month INT = CAST(FORMAT(@snapshot_date, 'yyyyMM') AS INT);

            -- Generate contract fact snapshot
            INSERT INTO reporting.fact_contracts (
                date_key, customer_key, contract_key, vehicle_key,
                monthly_rate, contract_value, mileage_allowance,
                vehicles_on_contract, is_active, days_to_expiry, contract_age_days
            )
            SELECT
                @date_key,
                c.customer_key,
                ct.contract_key,
                v.vehicle_key,
                ct.monthly_rate,
                ct.total_contract_value,
                ct.mileage_allowance,
                1,
                CASE WHEN ct.status = 'active' THEN 1 ELSE 0 END,
                DATEDIFF(DAY, @snapshot_date, ct.end_date),
                DATEDIFF(DAY, ct.start_date, @snapshot_date)
            FROM reporting.dim_contract ct
            JOIN reporting.dim_customer c ON ct.customer_id = c.customer_id AND c.is_current = 1
            LEFT JOIN reporting.dim_vehicle v ON ct.contract_no = v.equipment_id -- Adjust join as needed
            WHERE ct.is_current = 1
              AND ct.start_date <= @snapshot_date
              AND (ct.end_date >= @snapshot_date OR ct.end_date IS NULL);

            -- Refresh monthly aggregates
            EXEC reporting.sp_refresh_monthly_aggregates @year_month;
        """
    )

    # Send summary
    send_summary = PythonOperator(
        task_id='send_extraction_summary',
        python_callable=send_extraction_summary,
    )

    # Clear force flag if set
    clear_force_flag = PythonOperator(
        task_id='clear_force_flag',
        python_callable=lambda: Variable.set('force_monthly_extraction', 'false'),
    )

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # Dependencies
    start >> check_run
    check_run >> skip_extraction >> end
    check_run >> proceed_extraction >> archive_previous >> extract_group
    extract_group >> quality_group >> transform_group >> refresh_dimensions
    refresh_dimensions >> generate_snapshots >> send_summary >> clear_force_flag >> end
