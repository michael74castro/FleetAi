"""
FleetAI - Daily Data Extraction DAG
Extracts 39 tables from IBM Power10 DB2 for i to MSSQL landing schema
Schedule: Daily at 2:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.task_group import TaskGroup

# Import custom operators
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from db2_operator import DB2ExtractOperator, DB2ToMSSQLCDCOperator, DataQualityCheckOperator


# Default arguments
default_args = {
    'owner': 'fleetai',
    'depends_on_past': False,
    'email': ['etl-alerts@fleetai.local'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Daily extraction tables configuration
DAILY_TABLES = [
    # Customer domain
    {'source': 'CCAU', 'target': 'CCAU', 'key_cols': ['CCAU_AUDIT_ID'], 'staging': 'customer_audit'},
    {'source': 'CCCA', 'target': 'CCCA', 'key_cols': ['CCCA_CONTACT_ID'], 'staging': 'customer_contacts'},
    {'source': 'CCCO', 'target': 'CCCO', 'key_cols': ['CCCO_CUSTOMER_ID'], 'staging': 'customers'},
    {'source': 'CCCP', 'target': 'CCCP', 'key_cols': ['CCCP_CHARGE_ID'], 'staging': 'contract_charges'},
    {'source': 'CCCU', 'target': 'CCCU', 'key_cols': ['CCCU_CUSTOMER_ID'], 'staging': 'customers'},
    {'source': 'CCBI', 'target': 'CCBI', 'key_cols': ['CCBI_BILLING_ID'], 'staging': 'customer_billing'},

    # Driver domain
    {'source': 'CCDA', 'target': 'CCDA', 'key_cols': ['CCDA_ASSIGNMENT_ID'], 'staging': 'driver_vehicle_assignments'},
    {'source': 'CCDR', 'target': 'CCDR', 'key_cols': ['CCDR_DRIVER_ID'], 'staging': 'drivers'},

    # Fuel domain
    {'source': 'CCFC', 'target': 'CCFC', 'key_cols': ['CCFC_CARD_ID'], 'staging': 'fuel_cards'},
    {'source': 'CCFIH', 'target': 'CCFIH', 'key_cols': ['CCFIH_INVOICE_NO'], 'staging': 'fuel_invoices'},
    {'source': 'CCFID', 'target': 'CCFID', 'key_cols': ['CCFID_LINE_ID'], 'staging': 'fuel_transactions'},
    {'source': 'CCFIM', 'target': 'CCFIM', 'key_cols': ['CCFIM_MISC_ID'], 'staging': 'fuel_misc'},
    {'source': 'CCFP', 'target': 'CCFP', 'key_cols': ['CCFP_PRICE_ID'], 'staging': 'fuel_prices'},

    # Groups
    {'source': 'CCGR', 'target': 'CCGR', 'key_cols': ['CCGR_GROUP_ID'], 'staging': 'groups'},
    {'source': 'CCGD', 'target': 'CCGD', 'key_cols': ['CCGD_GROUP_DETAIL_ID'], 'staging': 'group_members'},

    # Invoices
    {'source': 'CCIN', 'target': 'CCIN', 'key_cols': ['CCIN_INVOICE_NO'], 'staging': 'invoices'},
    {'source': 'CCIO', 'target': 'CCIO', 'key_cols': ['CCIO_LINE_ID'], 'staging': 'invoice_line_items'},
    {'source': 'CCPC', 'target': 'CCPC', 'key_cols': ['CCPC_CHARGE_ID'], 'staging': 'payment_charges'},
    {'source': 'CCRC', 'target': 'CCRC', 'key_cols': ['CCRC_RECEIPT_NO'], 'staging': 'receipts'},

    # Orders
    {'source': 'CCOB', 'target': 'CCOB', 'key_cols': ['CCOB_ORDER_NO'], 'staging': 'orders'},
    {'source': 'CCOBCP', 'target': 'CCOBCP', 'key_cols': ['CCOBCP_CONFIG_ID'], 'staging': 'order_configurations'},
    {'source': 'CCOR', 'target': 'CCOR', 'key_cols': ['CCOR_REQUEST_ID'], 'staging': 'order_requests'},
    {'source': 'CCOS', 'target': 'CCOS', 'key_cols': ['CCOS_STATUS_ID'], 'staging': 'order_status'},

    # Reservations & Exceptions
    {'source': 'CCRS', 'target': 'CCRS', 'key_cols': ['CCRS_RESERVATION_ID'], 'staging': 'reservations'},
    {'source': 'CCXC', 'target': 'CCXC', 'key_cols': ['CCXC_EXCEPTION_ID'], 'staging': 'exceptions'},

    # Work Orders (CW prefix)
    {'source': 'CWAU', 'target': 'CWAU', 'key_cols': ['CWAU_AUDIT_ID'], 'staging': 'wo_audit'},
    {'source': 'CWBI', 'target': 'CWBI', 'key_cols': ['CWBI_BILLING_ID'], 'staging': 'wo_billing'},
    {'source': 'CWCO', 'target': 'CWCO', 'key_cols': ['CWCO_COMMENT_ID'], 'staging': 'wo_comments'},
    {'source': 'CWCP', 'target': 'CWCP', 'key_cols': ['CWCP_COMPONENT_ID'], 'staging': 'wo_components'},
    {'source': 'CWCU', 'target': 'CWCU', 'key_cols': ['CWCU_RELATION_ID'], 'staging': 'wo_customers'},
    {'source': 'CWDA', 'target': 'CWDA', 'key_cols': ['CWDA_ASSIGNMENT_ID'], 'staging': 'wo_driver_assignments'},
    {'source': 'CWDD', 'target': 'CWDD', 'key_cols': ['CWDD_DOCUMENT_ID'], 'staging': 'wo_documents'},
    {'source': 'CWDR', 'target': 'CWDR', 'key_cols': ['CWDR_DRIVER_ID', 'CWDR_WORK_ORDER_NO'], 'staging': 'wo_drivers'},
    {'source': 'CWGR', 'target': 'CWGR', 'key_cols': ['CWGR_GROUP_ID', 'CWGR_WORK_ORDER_NO'], 'staging': 'wo_groups'},
    {'source': 'CWOA', 'target': 'CWOA', 'key_cols': ['CWOA_WORK_ORDER_NO'], 'staging': 'wo_archive'},
    {'source': 'CWOB', 'target': 'CWOB', 'key_cols': ['CWOB_WORK_ORDER_NO'], 'staging': 'work_orders'},
    {'source': 'CWOR', 'target': 'CWOR', 'key_cols': ['CWOR_REPAIR_ID'], 'staging': 'work_order_repairs'},
    {'source': 'CWPC', 'target': 'CWPC', 'key_cols': ['CWPC_CHARGE_ID'], 'staging': 'work_order_parts'},
    {'source': 'CWPO', 'target': 'CWPO', 'key_cols': ['CWPO_PO_NO'], 'staging': 'wo_purchase_orders'},
]


# DAG Definition
with DAG(
    dag_id='fleetai_daily_extraction',
    default_args=default_args,
    description='Daily extraction of 39 tables from IBM Power10 DB2 to MSSQL',
    schedule_interval='0 2 * * *',  # 2:00 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['fleetai', 'etl', 'daily', 'db2'],
) as dag:

    # Start marker
    start = EmptyOperator(task_id='start')

    # Pre-extraction validation
    pre_validation = MsSqlOperator(
        task_id='pre_extraction_validation',
        mssql_conn_id='mssql_fleetai',
        sql="""
            -- Check landing schema exists
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'landing')
                THROW 50001, 'Landing schema does not exist', 1;

            -- Check ETL log table exists
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_extraction_log' AND schema_id = SCHEMA_ID('landing'))
                THROW 50002, 'ETL extraction log table does not exist', 1;

            -- Log DAG start
            INSERT INTO landing.etl_extraction_log (table_name, extraction_type, extraction_start, status)
            VALUES ('_DAG_DAILY', 'daily', GETUTCDATE(), 'running');
        """
    )

    # Extract task groups
    with TaskGroup(group_id='extract_landing') as extract_group:
        extract_tasks = {}
        for table_config in DAILY_TABLES:
            extract_task = DB2ExtractOperator(
                task_id=f"extract_{table_config['source']}",
                source_table=table_config['source'],
                target_table=table_config['target'],
                extraction_type='daily',
                pool='db2_extraction_pool',
            )
            extract_tasks[table_config['source']] = extract_task

    # Data quality checks
    with TaskGroup(group_id='quality_checks') as quality_group:
        quality_tasks = []
        for table_config in DAILY_TABLES:
            quality_task = DataQualityCheckOperator(
                task_id=f"quality_{table_config['target']}",
                table_name=f"landing.{table_config['target']}",
                checks=[
                    {
                        'type': 'row_count',
                        'name': f"row_count_{table_config['target']}",
                        'min_count': 0  # Allow empty for some tables
                    },
                    {
                        'type': 'freshness',
                        'name': f"freshness_{table_config['target']}",
                        'timestamp_column': 'extraction_timestamp',
                        'max_age_hours': 25  # Allow some buffer
                    }
                ]
            )
            quality_tasks.append(quality_task)

    # Transform to staging (CDC)
    with TaskGroup(group_id='transform_staging') as transform_group:
        transform_tasks = []
        for table_config in DAILY_TABLES:
            if table_config.get('staging'):  # Only if staging table defined
                transform_task = DB2ToMSSQLCDCOperator(
                    task_id=f"cdc_{table_config['source']}",
                    source_table=table_config['source'],
                    landing_table=table_config['target'],
                    staging_table=table_config['staging'],
                    key_columns=table_config['key_cols'],
                    pool='mssql_transform_pool',
                )
                transform_tasks.append(transform_task)

    # Refresh reporting layer
    refresh_reporting = MsSqlOperator(
        task_id='refresh_reporting_layer',
        mssql_conn_id='mssql_fleetai',
        sql="""
            -- Refresh customer dimension
            MERGE reporting.dim_customer AS target
            USING (
                SELECT
                    c.customer_id,
                    c.customer_name,
                    c.legal_name,
                    c.account_type,
                    c.parent_customer_id,
                    pc.customer_name AS parent_customer_name,
                    c.tax_id,
                    c.industry,
                    CASE
                        WHEN c.employee_count < 50 THEN 'Small'
                        WHEN c.employee_count < 250 THEN 'Medium'
                        WHEN c.employee_count < 1000 THEN 'Large'
                        ELSE 'Enterprise'
                    END AS employee_count_tier,
                    c.credit_rating,
                    c.payment_terms,
                    c.account_manager,
                    c.region,
                    c.territory,
                    cb.city AS billing_city,
                    cb.state AS billing_state,
                    cb.country AS billing_country,
                    c.status,
                    c.created_date
                FROM staging.customers c
                LEFT JOIN staging.customers pc ON c.parent_customer_id = pc.customer_id AND pc.is_current = 1
                LEFT JOIN staging.customer_billing cb ON c.customer_id = cb.customer_id AND cb.is_current = 1
                WHERE c.is_current = 1
            ) AS source
            ON target.customer_id = source.customer_id AND target.is_current = 1
            WHEN MATCHED AND (
                target.customer_name != source.customer_name OR
                target.status != source.status OR
                target.account_manager != source.account_manager
            ) THEN
                UPDATE SET
                    effective_to = CAST(GETDATE() AS DATE),
                    is_current = 0
            WHEN NOT MATCHED THEN
                INSERT (customer_id, customer_name, legal_name, account_type, parent_customer_id,
                        parent_customer_name, tax_id, industry, employee_count_tier, credit_rating,
                        payment_terms, account_manager, region, territory, billing_city,
                        billing_state, billing_country, status, created_date, effective_from, is_current)
                VALUES (source.customer_id, source.customer_name, source.legal_name, source.account_type,
                        source.parent_customer_id, source.parent_customer_name, source.tax_id, source.industry,
                        source.employee_count_tier, source.credit_rating, source.payment_terms,
                        source.account_manager, source.region, source.territory, source.billing_city,
                        source.billing_state, source.billing_country, source.status, source.created_date,
                        CAST(GETDATE() AS DATE), 1);

            -- Similar MERGE for other dimensions (vehicles, drivers, contracts)
            -- ... (abbreviated for space)

            -- Update ETL log
            UPDATE landing.etl_extraction_log
            SET extraction_end = GETUTCDATE(), status = 'success'
            WHERE table_name = '_DAG_DAILY'
              AND status = 'running'
              AND extraction_start = (
                  SELECT MAX(extraction_start)
                  FROM landing.etl_extraction_log
                  WHERE table_name = '_DAG_DAILY'
              );
        """
    )

    # Refresh aggregates
    refresh_aggregates = MsSqlOperator(
        task_id='refresh_aggregates',
        mssql_conn_id='mssql_fleetai',
        sql="""
            DECLARE @year_month INT = CAST(FORMAT(GETDATE(), 'yyyyMM') AS INT);
            EXEC reporting.sp_refresh_monthly_aggregates @year_month;
        """
    )

    # End marker
    end = EmptyOperator(task_id='end')

    # Define dependencies
    start >> pre_validation >> extract_group >> quality_group >> transform_group >> refresh_reporting >> refresh_aggregates >> end
