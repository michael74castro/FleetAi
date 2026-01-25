"""
FleetAI - Data Transformation DAG
Transforms staging data to reporting layer (dimensional model)
Schedule: Daily at 4:00 AM (after daily extraction)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup


default_args = {
    'owner': 'fleetai',
    'depends_on_past': False,
    'email': ['etl-alerts@fleetai.local'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


with DAG(
    dag_id='fleetai_data_transformation',
    default_args=default_args,
    description='Transform staging data to reporting dimensional model',
    schedule_interval='0 4 * * *',  # 4:00 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['fleetai', 'etl', 'transformation', 'reporting'],
) as dag:

    start = EmptyOperator(task_id='start')

    # Wait for daily extraction to complete
    wait_for_extraction = ExternalTaskSensor(
        task_id='wait_for_daily_extraction',
        external_dag_id='fleetai_daily_extraction',
        external_task_id='end',
        execution_delta=timedelta(hours=2),  # Daily runs 2 hours before
        timeout=3600,
        mode='reschedule',
    )

    # Refresh Dimensions
    with TaskGroup(group_id='refresh_dimensions') as dim_group:

        refresh_dim_customer = MsSqlOperator(
            task_id='refresh_dim_customer',
            mssql_conn_id='mssql_fleetai',
            sql="""
                -- SCD Type 2 merge for customer dimension
                ;WITH source_data AS (
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
                        COALESCE(cb.country, 'USA') AS billing_country,
                        c.status,
                        c.created_date
                    FROM staging.customers c
                    LEFT JOIN staging.customers pc
                        ON c.parent_customer_id = pc.customer_id AND pc.is_current = 1
                    LEFT JOIN staging.customer_billing cb
                        ON c.customer_id = cb.customer_id AND cb.is_current = 1
                    WHERE c.is_current = 1
                )
                MERGE reporting.dim_customer AS target
                USING source_data AS source
                ON target.customer_id = source.customer_id AND target.is_current = 1
                WHEN MATCHED AND (
                    ISNULL(target.customer_name, '') != ISNULL(source.customer_name, '') OR
                    ISNULL(target.status, '') != ISNULL(source.status, '') OR
                    ISNULL(target.account_manager, '') != ISNULL(source.account_manager, '') OR
                    ISNULL(target.credit_rating, '') != ISNULL(source.credit_rating, '')
                ) THEN
                    UPDATE SET
                        effective_to = CAST(GETDATE() AS DATE),
                        is_current = 0
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        customer_id, customer_name, legal_name, account_type,
                        parent_customer_id, parent_customer_name, tax_id, industry,
                        employee_count_tier, credit_rating, payment_terms, account_manager,
                        region, territory, billing_city, billing_state, billing_country,
                        status, created_date, effective_from, is_current
                    )
                    VALUES (
                        source.customer_id, source.customer_name, source.legal_name, source.account_type,
                        source.parent_customer_id, source.parent_customer_name, source.tax_id, source.industry,
                        source.employee_count_tier, source.credit_rating, source.payment_terms, source.account_manager,
                        source.region, source.territory, source.billing_city, source.billing_state, source.billing_country,
                        source.status, source.created_date, CAST(GETDATE() AS DATE), 1
                    );

                -- Insert new versions for changed records
                INSERT INTO reporting.dim_customer (
                    customer_id, customer_name, legal_name, account_type,
                    parent_customer_id, parent_customer_name, tax_id, industry,
                    employee_count_tier, credit_rating, payment_terms, account_manager,
                    region, territory, billing_city, billing_state, billing_country,
                    status, created_date, effective_from, is_current
                )
                SELECT
                    s.customer_id, s.customer_name, s.legal_name, s.account_type,
                    s.parent_customer_id, s.parent_customer_name, s.tax_id, s.industry,
                    s.employee_count_tier, s.credit_rating, s.payment_terms, s.account_manager,
                    s.region, s.territory, s.billing_city, s.billing_state, s.billing_country,
                    s.status, s.created_date, CAST(GETDATE() AS DATE), 1
                FROM source_data s
                WHERE EXISTS (
                    SELECT 1 FROM reporting.dim_customer t
                    WHERE t.customer_id = s.customer_id
                    AND t.effective_to = CAST(GETDATE() AS DATE)
                    AND t.is_current = 0
                );
            """
        )

        refresh_dim_driver = MsSqlOperator(
            task_id='refresh_dim_driver',
            mssql_conn_id='mssql_fleetai',
            sql="""
                MERGE reporting.dim_driver AS target
                USING (
                    SELECT
                        d.driver_id,
                        d.customer_id,
                        d.first_name,
                        d.last_name,
                        CONCAT(d.first_name, ' ', d.last_name) AS full_name,
                        d.email,
                        d.department,
                        d.cost_center,
                        d.license_state,
                        d.license_expiry AS license_expiry_date,
                        d.status
                    FROM staging.drivers d
                    WHERE d.is_current = 1
                ) AS source
                ON target.driver_id = source.driver_id AND target.is_current = 1
                WHEN MATCHED AND (
                    ISNULL(target.full_name, '') != ISNULL(source.full_name, '') OR
                    ISNULL(target.status, '') != ISNULL(source.status, '') OR
                    ISNULL(target.license_expiry_date, '1900-01-01') != ISNULL(source.license_expiry_date, '1900-01-01')
                ) THEN
                    UPDATE SET effective_to = CAST(GETDATE() AS DATE), is_current = 0
                WHEN NOT MATCHED THEN
                    INSERT (driver_id, customer_id, first_name, last_name, full_name,
                            email, department, cost_center, license_state, license_expiry_date,
                            status, effective_from, is_current)
                    VALUES (source.driver_id, source.customer_id, source.first_name, source.last_name,
                            source.full_name, source.email, source.department, source.cost_center,
                            source.license_state, source.license_expiry_date, source.status,
                            CAST(GETDATE() AS DATE), 1);
            """
        )

        refresh_dim_vehicle = MsSqlOperator(
            task_id='refresh_dim_vehicle',
            mssql_conn_id='mssql_fleetai',
            sql="""
                MERGE reporting.dim_vehicle AS target
                USING (
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
                        v.status
                    FROM staging.vehicles v
                    WHERE v.is_current = 1
                ) AS source
                ON target.equipment_id = source.equipment_id AND target.is_current = 1
                WHEN MATCHED AND (
                    ISNULL(target.status, '') != ISNULL(source.status, '') OR
                    ISNULL(target.license_plate, '') != ISNULL(source.license_plate, '')
                ) THEN
                    UPDATE SET effective_to = CAST(GETDATE() AS DATE), is_current = 0
                WHEN NOT MATCHED THEN
                    INSERT (equipment_id, vin, license_plate, make, model, model_year,
                            color, body_type, engine_type, fuel_type, transmission,
                            acquisition_date, acquisition_cost, residual_value, status,
                            effective_from, is_current)
                    VALUES (source.equipment_id, source.vin, source.license_plate, source.make,
                            source.model, source.model_year, source.color, source.body_type,
                            source.engine_type, source.fuel_type, source.transmission,
                            source.acquisition_date, source.acquisition_cost, source.residual_value,
                            source.status, CAST(GETDATE() AS DATE), 1);
            """
        )

        refresh_dim_location = MsSqlOperator(
            task_id='refresh_dim_location',
            mssql_conn_id='mssql_fleetai',
            sql="""
                -- Insert unique locations from fuel transactions
                INSERT INTO reporting.dim_location (city, state, country)
                SELECT DISTINCT
                    ft.merchant_city,
                    ft.merchant_state,
                    'USA'
                FROM staging.fuel_transactions ft
                WHERE ft.merchant_city IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM reporting.dim_location l
                      WHERE l.city = ft.merchant_city
                        AND l.state = ft.merchant_state
                        AND l.country = 'USA'
                  );
            """
        )

    # Load Fact Tables
    with TaskGroup(group_id='load_facts') as fact_group:

        load_fact_invoices = MsSqlOperator(
            task_id='load_fact_invoices',
            mssql_conn_id='mssql_fleetai',
            sql="""
                -- Load new invoices to fact table
                INSERT INTO reporting.fact_invoices (
                    invoice_date_key, due_date_key, customer_key, contract_key,
                    invoice_no, subtotal, tax_amount, total_amount, paid_amount,
                    balance_amount, line_item_count, days_outstanding, is_overdue
                )
                SELECT
                    CONVERT(INT, FORMAT(i.invoice_date, 'yyyyMMdd')) AS invoice_date_key,
                    CONVERT(INT, FORMAT(i.due_date, 'yyyyMMdd')) AS due_date_key,
                    c.customer_key,
                    ct.contract_key,
                    i.invoice_no,
                    i.subtotal,
                    i.tax_amount,
                    i.total_amount,
                    i.paid_amount,
                    i.balance,
                    (SELECT COUNT(*) FROM staging.invoice_line_items il WHERE il.invoice_no = i.invoice_no),
                    DATEDIFF(DAY, i.due_date, GETDATE()),
                    CASE WHEN GETDATE() > i.due_date AND i.balance > 0 THEN 1 ELSE 0 END
                FROM staging.invoices i
                JOIN reporting.dim_customer c ON i.customer_id = c.customer_id AND c.is_current = 1
                LEFT JOIN reporting.dim_contract ct ON i.contract_no = ct.contract_no AND ct.is_current = 1
                WHERE NOT EXISTS (
                    SELECT 1 FROM reporting.fact_invoices f WHERE f.invoice_no = i.invoice_no
                );

                -- Update existing invoices (payment status)
                UPDATE f
                SET
                    paid_amount = i.paid_amount,
                    balance_amount = i.balance,
                    days_outstanding = DATEDIFF(DAY, i.due_date, GETDATE()),
                    is_overdue = CASE WHEN GETDATE() > i.due_date AND i.balance > 0 THEN 1 ELSE 0 END
                FROM reporting.fact_invoices f
                JOIN staging.invoices i ON f.invoice_no = i.invoice_no
                WHERE f.balance_amount != i.balance;
            """
        )

        load_fact_fuel = MsSqlOperator(
            task_id='load_fact_fuel',
            mssql_conn_id='mssql_fleetai',
            sql="""
                -- Load new fuel transactions
                INSERT INTO reporting.fact_fuel (
                    transaction_date_key, customer_key, vehicle_key, driver_key,
                    location_key, gallons, amount, price_per_gallon, odometer,
                    miles_since_last_fill, mpg, fuel_type, product_code
                )
                SELECT
                    CONVERT(INT, FORMAT(ft.transaction_date, 'yyyyMMdd')),
                    c.customer_key,
                    v.vehicle_key,
                    d.driver_key,
                    l.location_key,
                    ft.quantity,
                    ft.amount,
                    ft.unit_price,
                    ft.odometer,
                    -- Calculate miles since last fill
                    ft.odometer - LAG(ft.odometer) OVER (
                        PARTITION BY ft.equipment_id ORDER BY ft.transaction_date
                    ),
                    -- Calculate MPG
                    CASE
                        WHEN ft.quantity > 0 AND LAG(ft.odometer) OVER (
                            PARTITION BY ft.equipment_id ORDER BY ft.transaction_date
                        ) IS NOT NULL
                        THEN (ft.odometer - LAG(ft.odometer) OVER (
                            PARTITION BY ft.equipment_id ORDER BY ft.transaction_date
                        )) / NULLIF(ft.quantity, 0)
                        ELSE NULL
                    END,
                    ft.product_description,
                    ft.product_code
                FROM staging.fuel_transactions ft
                JOIN staging.vehicles sv ON ft.equipment_id = sv.equipment_id AND sv.is_current = 1
                JOIN staging.customers sc ON sv.contract_no IS NOT NULL -- Get customer from contract
                JOIN reporting.dim_vehicle v ON ft.equipment_id = v.equipment_id AND v.is_current = 1
                LEFT JOIN reporting.dim_customer c ON sc.customer_id = c.customer_id AND c.is_current = 1
                LEFT JOIN reporting.dim_driver d ON ft.driver_id = d.driver_id AND d.is_current = 1
                LEFT JOIN reporting.dim_location l ON ft.merchant_city = l.city AND ft.merchant_state = l.state
                WHERE ft.transaction_date >= DATEADD(DAY, -7, GETDATE())  -- Last 7 days
                  AND NOT EXISTS (
                      SELECT 1 FROM reporting.fact_fuel f
                      WHERE f.transaction_date_key = CONVERT(INT, FORMAT(ft.transaction_date, 'yyyyMMdd'))
                        AND f.vehicle_key = v.vehicle_key
                        AND f.amount = ft.amount
                  );
            """
        )

        load_fact_maintenance = MsSqlOperator(
            task_id='load_fact_maintenance',
            mssql_conn_id='mssql_fleetai',
            sql="""
                -- Load new/updated work orders
                MERGE reporting.fact_maintenance AS target
                USING (
                    SELECT
                        CONVERT(INT, FORMAT(wo.completed_date, 'yyyyMMdd')) AS completed_date_key,
                        CONVERT(INT, FORMAT(wo.scheduled_date, 'yyyyMMdd')) AS scheduled_date_key,
                        c.customer_key,
                        v.vehicle_key,
                        vd.vendor_key,
                        wo.work_order_no,
                        wo.order_type,
                        wo.priority,
                        COALESCE(SUM(r.labor_hours), 0) AS labor_hours,
                        COALESCE(SUM(r.labor_cost), 0) AS labor_cost,
                        COALESCE(SUM(r.parts_cost), 0) AS parts_cost,
                        wo.actual_cost AS total_cost,
                        wo.estimated_cost,
                        DATEDIFF(DAY, wo.requested_date, wo.completed_date) AS days_to_complete,
                        CASE WHEN wo.scheduled_date IS NOT NULL THEN 1 ELSE 0 END AS is_scheduled,
                        CASE WHEN wo.completed_date IS NOT NULL THEN 1 ELSE 0 END AS is_completed
                    FROM staging.work_orders wo
                    JOIN reporting.dim_vehicle v ON wo.equipment_id = v.equipment_id AND v.is_current = 1
                    LEFT JOIN reporting.dim_customer c ON wo.customer_id = c.customer_id AND c.is_current = 1
                    LEFT JOIN reporting.dim_vendor vd ON wo.vendor_id = vd.vendor_id
                    LEFT JOIN staging.work_order_repairs r ON wo.work_order_no = r.work_order_no
                    WHERE wo.is_current = 1
                    GROUP BY wo.work_order_no, wo.completed_date, wo.scheduled_date, wo.order_type,
                             wo.priority, wo.actual_cost, wo.estimated_cost, wo.requested_date,
                             c.customer_key, v.vehicle_key, vd.vendor_key
                ) AS source
                ON target.work_order_no = source.work_order_no
                WHEN MATCHED AND (
                    ISNULL(target.total_cost, 0) != ISNULL(source.total_cost, 0) OR
                    target.is_completed != source.is_completed
                ) THEN
                    UPDATE SET
                        completed_date_key = source.completed_date_key,
                        labor_hours = source.labor_hours,
                        labor_cost = source.labor_cost,
                        parts_cost = source.parts_cost,
                        total_cost = source.total_cost,
                        days_to_complete = source.days_to_complete,
                        is_completed = source.is_completed
                WHEN NOT MATCHED THEN
                    INSERT (completed_date_key, scheduled_date_key, customer_key, vehicle_key,
                            vendor_key, work_order_no, order_type, priority, labor_hours,
                            labor_cost, parts_cost, total_cost, estimated_cost, days_to_complete,
                            is_scheduled, is_completed)
                    VALUES (source.completed_date_key, source.scheduled_date_key, source.customer_key,
                            source.vehicle_key, source.vendor_key, source.work_order_no, source.order_type,
                            source.priority, source.labor_hours, source.labor_cost, source.parts_cost,
                            source.total_cost, source.estimated_cost, source.days_to_complete,
                            source.is_scheduled, source.is_completed);
            """
        )

    # Refresh Aggregates
    refresh_aggregates = MsSqlOperator(
        task_id='refresh_daily_aggregates',
        mssql_conn_id='mssql_fleetai',
        sql="""
            -- Refresh current month aggregates
            DECLARE @year_month INT = CAST(FORMAT(GETDATE(), 'yyyyMM') AS INT);
            EXEC reporting.sp_refresh_monthly_aggregates @year_month;

            -- Update statistics on fact tables
            UPDATE STATISTICS reporting.fact_invoices;
            UPDATE STATISTICS reporting.fact_fuel;
            UPDATE STATISTICS reporting.fact_maintenance;
        """
    )

    # Validate transformation results
    validate_transformation = MsSqlOperator(
        task_id='validate_transformation',
        mssql_conn_id='mssql_fleetai',
        sql="""
            -- Validate dimension counts
            DECLARE @customer_count INT = (SELECT COUNT(*) FROM reporting.dim_customer WHERE is_current = 1);
            DECLARE @vehicle_count INT = (SELECT COUNT(*) FROM reporting.dim_vehicle WHERE is_current = 1);
            DECLARE @driver_count INT = (SELECT COUNT(*) FROM reporting.dim_driver WHERE is_current = 1);

            -- Log validation results
            INSERT INTO landing.etl_extraction_log (table_name, extraction_type, extraction_start, extraction_end, source_row_count, status)
            VALUES ('_TRANSFORMATION_VALIDATION', 'daily', GETUTCDATE(), GETUTCDATE(),
                    @customer_count + @vehicle_count + @driver_count, 'success');

            -- Raise error if counts are suspicious
            IF @customer_count = 0 OR @vehicle_count = 0
            BEGIN
                RAISERROR('Dimension tables have zero current records', 16, 1);
            END
        """
    )

    end = EmptyOperator(task_id='end')

    # Dependencies
    start >> wait_for_extraction >> dim_group >> fact_group >> refresh_aggregates >> validate_transformation >> end
