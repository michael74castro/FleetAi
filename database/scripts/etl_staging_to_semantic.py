"""
ETL Script: Staging to Semantic Layer
Loads AI-ready semantic layer with metadata catalog
"""

import sqlite3
from datetime import datetime, timedelta

DB_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\fleetai.db"
SCHEMA_FILE = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\schemas\03_semantic_layer_sqlite.sql"


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_semantic_schema(conn):
    """Create semantic layer tables."""
    print("Creating semantic layer schema...")
    with open(SCHEMA_FILE, 'r') as f:
        schema_sql = f.read()
    cursor = conn.cursor()
    cursor.executescript(schema_sql)
    conn.commit()
    print("  Schema created.")


def populate_date_dimension(conn):
    """Populate date dimension with 10 years of dates."""
    print("  Populating date dimension...", end=" ", flush=True)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM dim_date")

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2030, 12, 31)
    current = start_date

    dates = []
    while current <= end_date:
        date_key = int(current.strftime('%Y%m%d'))
        dates.append((
            date_key,
            current.strftime('%Y-%m-%d'),
            current.weekday() + 1,
            current.strftime('%A'),
            current.day,
            current.timetuple().tm_yday,
            current.isocalendar()[1],
            current.month,
            current.strftime('%B'),
            (current.month - 1) // 3 + 1,
            f"Q{(current.month - 1) // 3 + 1}",
            current.year,
            1 if current.weekday() >= 5 else 0,
            0 if current.weekday() >= 5 else 1
        ))
        current += timedelta(days=1)

    cursor.executemany("""
        INSERT INTO dim_date (date_key, full_date, day_of_week, day_name,
            day_of_month, day_of_year, week_of_year, month_number, month_name,
            quarter_number, quarter_name, year_number, is_weekend, is_business_day)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, dates)
    conn.commit()
    print(f"{len(dates)} dates")


def load_dim_customer(conn):
    """Load customer dimension."""
    print("  Loading dim_customer...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_customer")

    cursor.execute("""
        INSERT INTO dim_customer (
            customer_id, customer_name, customer_name_line_2, customer_name_line_3,
            short_name, address, city, country_code, country_name,
            phone_number, fax_number, account_manager_name, is_active
        )
        SELECT
            customer_id,
            customer_name,
            customer_name_2,
            customer_name_3,
            call_name,
            address,
            city,
            country_code,
            CASE country_code
                WHEN 'AE' THEN 'United Arab Emirates'
                WHEN 'NO' THEN 'Norway'
                WHEN 'US' THEN 'United States'
                ELSE country
            END,
            phone,
            fax,
            account_manager,
            1
        FROM staging_customers
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    print(f"{count} rows")


def load_dim_vehicle(conn):
    """Load vehicle dimension with enriched data."""
    print("  Loading dim_vehicle...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_vehicle")

    cursor.execute("""
        INSERT INTO dim_vehicle (
            vehicle_id, vin_number, registration_number, make_name, model_name,
            make_and_model, color_name,
            fuel_code, fuel_type, is_electric,
            customer_id, customer_name,
            contract_position_number, lease_type, lease_type_description,
            purchase_price, residual_value, monthly_lease_amount,
            lease_duration_months, annual_km_allowance, current_odometer_km,
            lease_start_date, lease_end_date,
            expected_end_date, months_driven, months_remaining, days_to_contract_end,
            vehicle_status_code, vehicle_status, is_active
        )
        SELECT
            v.object_no,
            v.vin,
            v.registration_no,
            v.make_name,
            v.model_name,
            COALESCE(v.make_name, '') || ' ' || COALESCE(REPLACE(v.model_name, ',', ''), ''),
            v.color_name,
            v.fuel_code,
            fc.fuel_type,
            COALESCE(fc.is_electric, 0),
            v.customer_no,
            c.customer_name,
            v.contract_position_no,
            v.lease_type,
            CASE v.lease_type
                WHEN 'O' THEN 'Operational Lease'
                WHEN 'F' THEN 'Financial Lease'
                WHEN 'L' THEN 'Lease'
                ELSE v.lease_type
            END,
            v.purchase_price,
            v.residual_value,
            v.lease_amount,
            v.lease_duration_months,
            v.km_allowance,
            v.current_km,
            v.lease_start_date,
            v.lease_end_date,
            v.expected_end_date,
            CAST((julianday('now') - julianday(v.lease_start_date)) / 30.44 AS INTEGER),
            CAST((julianday(v.expected_end_date) - julianday('now')) / 30.44 AS INTEGER),
            CAST(julianday(v.expected_end_date) - julianday('now') AS INTEGER),
            v.object_status,
            CASE v.object_status
                WHEN 0 THEN 'Created'
                WHEN 1 THEN 'Active'
                WHEN 2 THEN 'Terminated - Invoicing Stopped'
                WHEN 3 THEN 'Terminated - Invoice Adjustment Made'
                WHEN 4 THEN 'Terminated - Mileage Adjustment Made'
                WHEN 5 THEN 'Terminated - De-investment Made'
                WHEN 8 THEN 'Terminated - Ready for Settlement'
                WHEN 9 THEN 'Terminated - Final Settlement Made'
                ELSE 'Unknown'
            END,
            CASE WHEN v.object_status IN (0, 1) THEN 1 ELSE 0 END
        FROM staging_vehicles v
        LEFT JOIN staging_customers c ON v.customer_no = c.customer_id
        LEFT JOIN ref_fuel_code fc ON v.fuel_code = fc.fuel_code
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM dim_vehicle").fetchone()[0]
    print(f"{count} rows")


def load_dim_driver(conn):
    """Load driver dimension."""
    print("  Loading dim_driver...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_driver")

    cursor.execute("""
        INSERT INTO dim_driver (
            vehicle_id, driver_sequence, driver_name, first_name, last_name,
            email_address, phone_private, phone_office, phone_mobile,
            address, city, country_code, is_primary_driver, is_active
        )
        SELECT
            object_no,
            driver_no,
            driver_name,
            first_name,
            last_name,
            email,
            private_phone,
            office_phone,
            mobile_phone,
            address,
            city,
            country_code,
            CASE WHEN driver_no = 1 THEN 1 ELSE 0 END,
            CASE WHEN active_driver = '*' THEN 1 ELSE 0 END
        FROM staging_drivers
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM dim_driver").fetchone()[0]
    print(f"{count} rows")


def load_dim_contract(conn):
    """Load contract dimension."""
    print("  Loading dim_contract...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_contract")

    cursor.execute("""
        INSERT INTO dim_contract (
            contract_position_number, customer_id, customer_name, contract_number,
            make_name, model_name, lease_type, lease_type_description,
            contract_duration_months, contract_start_date, contract_end_date,
            months_driven, months_remaining, days_to_contract_end,
            annual_km_allowance, purchase_price,
            residual_value, total_lease_amount, monthly_rate_total,
            monthly_rate_depreciation, monthly_rate_interest, monthly_rate_maintenance,
            monthly_rate_insurance, monthly_rate_fuel, monthly_rate_tires,
            monthly_rate_road_tax, monthly_rate_admin, monthly_rate_replacement_vehicle,
            per_km_rate_maintenance, per_km_rate_tires, excess_km_rate,
            interest_rate_percent, contract_status, is_active
        )
        SELECT
            ct.contract_position_no,
            ct.customer_no,
            c.customer_name,
            ct.contract_no,
            a.make_name,
            a.model_name,
            ct.lease_type,
            CASE ct.lease_type
                WHEN 'O' THEN 'Operational Lease'
                WHEN 'F' THEN 'Financial Lease'
                WHEN 'L' THEN 'Lease'
                ELSE ct.lease_type
            END,
            ct.duration_months,
            ct.start_date,
            ct.end_date,
            CAST((julianday('now') - julianday(ct.start_date)) / 30.44 AS INTEGER),
            CAST((julianday(ct.end_date) - julianday('now')) / 30.44 AS INTEGER),
            CAST(julianday(ct.end_date) - julianday('now') AS INTEGER),
            ct.km_per_year,
            ct.purchase_price,
            ct.residual_value,
            ct.total_amount,
            ct.monthly_rate_total,
            ct.monthly_rate_depreciation,
            ct.monthly_rate_interest,
            ct.monthly_rate_maintenance,
            ct.monthly_rate_insurance,
            ct.monthly_rate_fuel,
            ct.monthly_rate_tires,
            ct.monthly_rate_road_tax,
            ct.monthly_rate_admin,
            ct.monthly_rate_replacement,
            ct.unit_rate_maintenance,
            ct.unit_rate_tires,
            ct.excess_km_rate,
            ct.interest_rate,
            CASE WHEN ct.active_cp = '*' THEN 'Active' ELSE 'Inactive' END,
            CASE WHEN ct.active_cp = '*' THEN 1 ELSE 0 END
        FROM staging_contracts ct
        LEFT JOIN staging_customers c ON ct.customer_no = c.customer_id
        LEFT JOIN (
            SELECT make_code, model_code, make_name, model_name
            FROM staging_automobiles
            GROUP BY make_code, model_code
        ) a ON ct.make_code = a.make_code AND ct.model_code = a.model_code
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM dim_contract").fetchone()[0]
    print(f"{count} rows")


def load_dim_group(conn):
    """Load group dimension."""
    print("  Loading dim_group...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_group")

    cursor.execute("""
        INSERT INTO dim_group (group_id, group_name, customer_id, is_active)
        SELECT
            g.group_no,
            g.group_name,
            g.customer_no,
            1
        FROM staging_groups g
    """)

    # Update customer names
    cursor.execute("""
        UPDATE dim_group
        SET customer_name = (
            SELECT customer_name FROM dim_customer
            WHERE dim_customer.customer_id = dim_group.customer_id
        )
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM dim_group").fetchone()[0]
    print(f"{count} rows")


def load_dim_make_model(conn):
    """Load make/model reference."""
    print("  Loading dim_make_model...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_make_model")

    cursor.execute("""
        INSERT INTO dim_make_model (
            make_code, make_name, model_code, model_name, model_group, vehicle_type, is_active
        )
        SELECT
            make_code,
            make_name,
            model_code,
            model_name,
            model_group_high,
            type_description,
            1
        FROM staging_automobiles
        GROUP BY make_code, model_code
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM dim_make_model").fetchone()[0]
    print(f"{count} rows")


def load_fact_odometer(conn):
    """Load odometer fact table."""
    print("  Loading fact_odometer_reading...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fact_odometer_reading")

    cursor.execute("""
        INSERT INTO fact_odometer_reading (
            vehicle_id, reading_date, reading_date_key, odometer_km,
            transaction_amount, transaction_description, source_type, supplier_id
        )
        SELECT
            object_no,
            reading_date,
            CAST(REPLACE(reading_date, '-', '') AS INTEGER),
            odometer_km,
            amount,
            description,
            CASE source_code
                WHEN 1 THEN 'Manual Entry'
                WHEN 2 THEN 'Fuel Transaction'
                WHEN 3 THEN 'Service'
                ELSE 'Other'
            END,
            supplier_no
        FROM staging_odometer_history
        WHERE reading_date IS NOT NULL
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM fact_odometer_reading").fetchone()[0]
    print(f"{count} rows")


def load_fact_billing(conn):
    """Load billing fact table."""
    print("  Loading fact_billing...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fact_billing")

    cursor.execute("""
        INSERT INTO fact_billing (
            billing_id, vehicle_id, customer_id, billing_run_number,
            billing_owner_name, billing_recipient_name, billing_address,
            billing_city, billing_method, fixed_amount, variable_amount,
            monthly_amount, period_amount, currency_code
        )
        SELECT
            b.billing_no,
            b.object_no,
            v.customer_no,
            b.billing_run_no,
            b.billing_owner,
            b.billing_name,
            b.billing_address,
            b.billing_city,
            b.billing_method,
            b.billing_amount_1,
            b.billing_amount_variable,
            b.billing_amount_monthly,
            b.billing_amount_period,
            b.currency
        FROM staging_billing b
        LEFT JOIN staging_vehicles v ON b.object_no = v.object_no
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM fact_billing").fetchone()[0]
    print(f"{count} rows")


def load_fact_damages(conn):
    """Load damages fact table."""
    print("  Loading fact_damages...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fact_damages")

    cursor.execute("""
        INSERT INTO fact_damages (
            damage_id, vehicle_id, driver_number, damage_date,
            description, damage_amount, net_damage_cost,
            accident_location, accident_country_code,
            mileage, damage_type, fault_code, damage_status_code,
            damage_recourse, total_loss_code, country_code,
            reporting_period, insurance_company_number,
            third_party_name, repair_days,
            amount_own_risk, amount_refunded,
            claimed_deductible, refunded_deductible,
            salvage_amount, damage_fault_level, garage_name
        )
        SELECT
            d.damage_id,
            d.object_no,
            d.driver_no,
            d.damage_date,
            d.description,
            d.damage_amount,
            -- net_damage_cost = damage_amount - refunded - salvage
            COALESCE(d.damage_amount, 0)
                - COALESCE(d.amount_refunded, 0)
                - COALESCE(d.salvage_amount, 0),
            d.accident_location_address,
            d.accident_country_code,
            d.mileage,
            d.damage_type,
            d.fault_code,
            d.damage_status_code,
            d.damage_recourse,
            d.total_loss_code,
            d.country_code,
            d.reporting_period,
            d.insurance_co_number,
            d.third_party_name,
            d.repair_days,
            d.amount_own_risk,
            d.amount_refunded,
            d.claimed_deductible_repair,
            d.refunded_deductible_repair,
            d.salvage_amount,
            d.damage_fault_level,
            d.garage_name
        FROM staging_damages d
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM fact_damages").fetchone()[0]
    print(f"{count} rows")


def populate_metadata_catalog(conn):
    """Populate metadata catalog for AI discovery."""
    print("  Populating metadata catalog...", end=" ", flush=True)
    cursor = conn.cursor()

    # Clear existing metadata
    cursor.execute("DELETE FROM semantic_table_catalog")
    cursor.execute("DELETE FROM semantic_column_catalog")
    cursor.execute("DELETE FROM semantic_relationships")
    cursor.execute("DELETE FROM semantic_business_glossary")

    # Table catalog
    tables = [
        ('dim_customer', 'Customers', 'Master list of all customers/clients who lease vehicles', 'Customer', 'One row per customer', 'Who are our customers? How many customers do we have? Which customers are in Dubai?'),
        ('dim_vehicle', 'Vehicles', 'Master list of all vehicles in the fleet with specifications and current status', 'Fleet', 'One row per vehicle', 'How many vehicles? What cars do we have? Show me all Mercedes. Which vehicles does customer X have?'),
        ('dim_driver', 'Drivers', 'People assigned to drive specific vehicles', 'Driver', 'One row per driver-vehicle assignment', 'Who drives vehicle X? How many drivers? Driver contact information'),
        ('dim_contract', 'Contracts', 'Lease contracts with pricing, terms, and rate breakdowns', 'Contract', 'One row per contract position', 'Contract terms? Monthly rates? How much does customer X pay?'),
        ('dim_group', 'Customer Groups', 'Logical groupings of customers for reporting', 'Customer', 'One row per group', 'What groups exist? Which customers are in group X?'),
        ('dim_make_model', 'Vehicle Makes and Models', 'Reference data for vehicle manufacturers and models', 'Reference', 'One row per make-model combination', 'What brands do we have? What models are available?'),
        ('dim_date', 'Calendar Dates', 'Date dimension for time-based analysis', 'Reference', 'One row per calendar date', 'Date lookups, time-based filtering'),
        ('ref_vehicle_status', 'Vehicle Statuses', 'Reference table for vehicle status codes and descriptions', 'Reference', 'One row per status code', 'What are the vehicle statuses? Status code meanings?'),
        ('ref_order_status', 'Order Statuses', 'Reference table for order status codes and descriptions', 'Reference', 'One row per status code', 'What are the order statuses? Order phases?'),
        ('fact_odometer_reading', 'Odometer & Service Records', 'Historical odometer readings AND maintenance/service records for vehicles. Filter source_type = Service for maintenance records. transaction_description contains repair/service details.', 'Operations', 'One row per reading or service event', 'Vehicle mileage history? Maintenance history for vehicle X? What services were done? Service records for customer Y?'),
        ('fact_billing', 'Billing Records', 'Billing transactions and amounts for vehicles', 'Financial', 'One row per billing record', 'How much was billed? Billing by customer?'),
        ('fact_damages', 'Damage & Accident Records', 'Records of vehicle damages, accidents, and insurance claims including costs, repair details, and third party information', 'Operations', 'One row per damage/accident event', 'What damages occurred? Damage costs? Accident history for vehicle X? Which vehicles had accidents? Total damage costs by customer? Insurance claims?'),
        ('view_fleet_overview', 'Fleet Overview', 'Comprehensive view of all vehicles with customer and driver information', 'Fleet', 'One row per vehicle', 'Show me the fleet. List all vehicles with details.'),
        ('view_customer_fleet_summary', 'Customer Fleet Summary', 'Summary of fleet size and value by customer', 'Customer', 'One row per customer', 'Fleet size by customer? How many vehicles does each customer have?'),
        ('view_customer_contracts', 'Customer Contracts', 'Contract totals and values by customer', 'Contract', 'One row per customer', 'Total contract value by customer?'),
        ('view_make_model_distribution', 'Make/Model Distribution', 'Count of vehicles by make and model', 'Fleet', 'One row per make-model', 'Which makes are most common? Vehicle brand distribution?'),
        ('view_account_manager_portfolio', 'Account Manager Portfolio', 'Customers and vehicles managed by each account manager', 'Customer', 'One row per account manager', 'Who manages which customers? Account manager workload?'),
    ]

    cursor.executemany("""
        INSERT INTO semantic_table_catalog (table_name, display_name, description, business_domain, grain, typical_questions)
        VALUES (?, ?, ?, ?, ?, ?)
    """, tables)

    # Key column catalog entries
    columns = [
        # Customers
        ('dim_customer', 'customer_id', 'Customer ID', 'Unique identifier for the customer', 'INTEGER', '10001, 10002', None, 0, 1, 1),
        ('dim_customer', 'customer_name', 'Customer Name', 'Full legal name of the customer company', 'TEXT', 'LeasePlan Emirates, Ericsson AB', None, 0, 1, 0),
        ('dim_customer', 'city', 'City', 'City where customer is located', 'TEXT', 'Dubai, Abu Dhabi', None, 0, 1, 0),
        ('dim_customer', 'account_manager_name', 'Account Manager', 'Name of the account manager responsible for this customer', 'TEXT', 'Robert Juthenholtz', None, 0, 1, 0),

        # Vehicles
        ('dim_vehicle', 'vehicle_id', 'Vehicle ID', 'Unique identifier for the vehicle (object number)', 'INTEGER', '1000082, 1000106', None, 0, 1, 1),
        ('dim_vehicle', 'vin_number', 'VIN', 'Vehicle Identification Number - unique 17-character code', 'TEXT', 'WDF63981313185989', None, 0, 1, 0),
        ('dim_vehicle', 'registration_number', 'License Plate', 'Vehicle registration/license plate number', 'TEXT', '19907AY', None, 0, 1, 0),
        ('dim_vehicle', 'make_name', 'Make', 'Vehicle manufacturer/brand name', 'TEXT', 'Mercedes, Toyota, BMW', None, 0, 1, 0),
        ('dim_vehicle', 'model_name', 'Model', 'Vehicle model name', 'TEXT', 'Viano, Corolla, X5', None, 0, 1, 0),
        ('dim_vehicle', 'make_and_model', 'Make and Model', 'Combined make and model for display', 'TEXT', 'Mercedes Viano', None, 0, 1, 0),
        ('dim_vehicle', 'monthly_lease_amount', 'Monthly Lease', 'Monthly lease payment amount', 'REAL', '5000.00', None, 1, 0, 0),
        ('dim_vehicle', 'current_odometer_km', 'Current Odometer (km)', 'Latest recorded odometer reading in kilometers', 'INTEGER', '107163', None, 1, 0, 0),
        ('dim_vehicle', 'purchase_price', 'Purchase Price', 'Original purchase price of the vehicle', 'REAL', '205000.00', None, 1, 0, 0),
        ('dim_vehicle', 'lease_end_date', 'Lease End Date (Source)', 'Raw source-system recorded end date. WARNING: may be stale or missing. Do NOT use for expiration analysis — use expected_end_date instead.', 'TEXT', None, 'Do NOT use for expiration queries', 0, 1, 0),
        ('dim_vehicle', 'expected_end_date', 'Expected End Date', 'Expected contract termination date, computed as lease_start_date + lease_duration_months. USE THIS for all expiration/ending queries.', 'TEXT', '2026-06-15', 'date(lease_start_date, +duration months). Use this for expiring/ending questions.', 0, 1, 0),
        ('dim_vehicle', 'months_driven', 'Months Driven', 'Number of months from lease start date to today', 'INTEGER', '24, 36', 'months between lease_start_date and now', 1, 0, 0),
        ('dim_vehicle', 'months_remaining', 'Months Remaining', 'Number of months from today to expected end date. Negative means overdue. USE THIS for expiration queries (e.g. months_remaining BETWEEN 0 AND 3).', 'INTEGER', '12, 3, -2', 'months between now and expected_end_date. Use for expiring/ending filters.', 1, 0, 0),
        ('dim_vehicle', 'days_to_contract_end', 'Days to Contract End', 'Number of days from today to expected end date. Negative means overdue. Use for precise renewal timing and urgency analysis.', 'INTEGER', '90, 30, -5', 'julianday(expected_end_date) - julianday(now). Use for renewal intelligence.', 1, 0, 0),
        ('dim_vehicle', 'vehicle_status_code', 'Status Code', 'Numeric status code (0=Created, 1=Active, 2-9=Terminated stages)', 'INTEGER', '0, 1, 2, 3, 4, 5, 8, 9', None, 0, 1, 0),
        ('dim_vehicle', 'vehicle_status', 'Vehicle Status', 'Human-readable status description', 'TEXT', 'Active, Terminated - Invoicing Stopped', None, 0, 1, 0),

        # Contracts
        ('dim_contract', 'contract_position_number', 'Contract Position', 'Unique identifier for the contract position', 'INTEGER', '1000233', None, 0, 1, 1),
        ('dim_contract', 'monthly_rate_total', 'Total Monthly Rate', 'Total monthly lease rate including all components', 'REAL', '5392.00', None, 1, 0, 0),
        ('dim_contract', 'contract_duration_months', 'Contract Duration', 'Length of contract in months', 'INTEGER', '48', None, 1, 0, 0),
        ('dim_contract', 'contract_start_date', 'Contract Start Date', 'Date the contract/lease began', 'TEXT', '2022-01-15', None, 0, 1, 0),
        ('dim_contract', 'contract_end_date', 'Contract End Date', 'Expected end date, computed as start_date + duration_months', 'TEXT', '2026-01-15', 'date(contract_start_date, +duration months)', 0, 1, 0),
        ('dim_contract', 'months_driven', 'Months Driven', 'Number of months from contract start date to today', 'INTEGER', '24, 36', 'months between contract_start_date and now', 1, 0, 0),
        ('dim_contract', 'months_remaining', 'Months Remaining', 'Number of months from today to contract end date. Negative means overdue.', 'INTEGER', '12, 3, -2', 'months between now and contract_end_date', 1, 0, 0),
        ('dim_contract', 'days_to_contract_end', 'Days to Contract End', 'Number of days from today to contract end date. Negative means overdue. Use for precise renewal timing.', 'INTEGER', '90, 30, -5', 'julianday(contract_end_date) - julianday(now)', 1, 0, 0),
        ('dim_contract', 'annual_km_allowance', 'Annual KM Allowance', 'Kilometers allowed per year before excess charges', 'INTEGER', '25000', None, 1, 0, 0),
        ('dim_contract', 'excess_km_rate', 'Excess KM Rate', 'Charge per kilometer over the allowance', 'REAL', '0.66', None, 1, 0, 0),

        # Odometer / Service Records
        ('fact_odometer_reading', 'transaction_description', 'Service Description', 'Repair or service description from maintenance records (e.g. 10000km service, brake repair, tire replacement)', 'TEXT', '10000km service done, 20000km Service done', 'Contains maintenance details from CCGD repair records', 0, 1, 0),
        ('fact_odometer_reading', 'source_type', 'Record Source', "Type of record: 'Manual Entry', 'Fuel Transaction', or 'Service'. Filter source_type = 'Service' for maintenance records", 'TEXT', 'Manual Entry, Fuel Transaction, Service', "Use source_type = 'Service' to get maintenance records only", 0, 1, 0),
        ('fact_odometer_reading', 'transaction_amount', 'Service Cost', 'Cost amount of the maintenance or service event', 'REAL', '525.00, 100.00', None, 1, 0, 0),
        ('fact_odometer_reading', 'supplier_id', 'Supplier ID', 'Identifier of the service supplier/vendor who performed maintenance', 'INTEGER', None, None, 0, 1, 0),

        # Damages
        ('fact_damages', 'damage_id', 'Damage ID', 'Unique identifier for the damage/accident event', 'TEXT', None, None, 0, 1, 1),
        ('fact_damages', 'vehicle_id', 'Vehicle ID', 'Vehicle involved in the damage (links to dim_vehicle)', 'INTEGER', None, None, 0, 1, 0),
        ('fact_damages', 'damage_date', 'Damage Date', 'Date when the damage/accident occurred', 'TEXT', '2024-03-15', None, 0, 1, 0),
        ('fact_damages', 'description', 'Damage Description', 'Full description of the accident/damage event', 'TEXT', None, None, 0, 1, 0),
        ('fact_damages', 'damage_amount', 'Damage Amount', 'Total damage repair cost', 'REAL', '5000.00', None, 1, 0, 0),
        ('fact_damages', 'net_damage_cost', 'Net Damage Cost', 'Net cost after refunds and salvage (damage_amount - amount_refunded - salvage_amount)', 'REAL', '3500.00', 'damage_amount - amount_refunded - salvage_amount', 1, 0, 0),
        ('fact_damages', 'damage_type', 'Damage Type', 'Type/category of the damage', 'TEXT', None, None, 0, 1, 0),
        ('fact_damages', 'fault_code', 'Fault Code', 'Code indicating fault attribution', 'TEXT', None, None, 0, 1, 0),
        ('fact_damages', 'repair_days', 'Repair Days', 'Number of days the repair took', 'INTEGER', '5', None, 1, 0, 0),
        ('fact_damages', 'amount_own_risk', 'Own Risk Amount', 'Deductible amount (own risk) for the damage', 'REAL', '500.00', None, 1, 0, 0),
        ('fact_damages', 'amount_refunded', 'Refunded Amount', 'Amount refunded by insurance or third party', 'REAL', '2000.00', None, 1, 0, 0),
        ('fact_damages', 'salvage_amount', 'Salvage Amount', 'Amount recovered from salvage', 'REAL', None, None, 1, 0, 0),
        ('fact_damages', 'third_party_name', 'Third Party Name', 'Name of the third party involved in the accident', 'TEXT', None, None, 0, 1, 0),
        ('fact_damages', 'garage_name', 'Garage Name', 'Name of the repair garage', 'TEXT', None, None, 0, 1, 0),
        ('fact_damages', 'country_code', 'Country Code', 'Country where the damage record belongs', 'TEXT', None, None, 0, 1, 0),
    ]

    cursor.executemany("""
        INSERT INTO semantic_column_catalog (table_name, column_name, display_name, description, data_type, example_values, business_rules, is_measure, is_dimension, is_key)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, columns)

    # Relationships
    relationships = [
        ('dim_vehicle', 'customer_id', 'dim_customer', 'customer_id', 'many-to-one', 'Each vehicle belongs to one customer'),
        ('dim_driver', 'vehicle_id', 'dim_vehicle', 'vehicle_id', 'many-to-one', 'Each driver is assigned to one vehicle'),
        ('dim_contract', 'customer_id', 'dim_customer', 'customer_id', 'many-to-one', 'Each contract belongs to one customer'),
        ('dim_vehicle', 'contract_position_number', 'dim_contract', 'contract_position_number', 'many-to-one', 'Each vehicle may have one contract'),
        ('fact_odometer_reading', 'vehicle_id', 'dim_vehicle', 'vehicle_id', 'many-to-one', 'Each odometer reading belongs to one vehicle'),
        ('fact_billing', 'vehicle_id', 'dim_vehicle', 'vehicle_id', 'many-to-one', 'Each billing record is for one vehicle'),
        ('fact_billing', 'customer_id', 'dim_customer', 'customer_id', 'many-to-one', 'Each billing record is for one customer'),
        ('fact_odometer_reading', 'reading_date_key', 'dim_date', 'date_key', 'many-to-one', 'Each odometer reading links to a calendar date'),
        ('dim_vehicle', 'vehicle_status_code', 'ref_vehicle_status', 'status_code', 'many-to-one', 'Each vehicle has one status from the status reference table'),
        ('dim_group', 'customer_id', 'dim_customer', 'customer_id', 'many-to-one', 'Each group belongs to one customer'),
        ('fact_damages', 'vehicle_id', 'dim_vehicle', 'vehicle_id', 'many-to-one', 'Each damage record is for one vehicle'),
    ]

    cursor.executemany("""
        INSERT INTO semantic_relationships (from_table, from_column, to_table, to_column, relationship_type, description)
        VALUES (?, ?, ?, ?, ?, ?)
    """, relationships)

    # Business glossary
    glossary = [
        ('Fleet', 'The collection of all vehicles managed under lease contracts', 'vehicles, cars, fleet size', 'dim_vehicle', None),
        ('Customer', 'A company or organization that leases vehicles', 'client, account, lessee', 'dim_customer', None),
        ('Contract', 'A lease agreement specifying terms, duration, and pricing', 'lease, agreement', 'dim_contract', None),
        ('Monthly Rate', 'The total monthly payment for a lease, including all components', 'lease payment, monthly cost', 'dim_contract', 'Sum of depreciation + interest + maintenance + insurance + other'),
        ('Operational Lease', 'Lease type where lessor retains ownership and provides services', 'full service lease', 'dim_contract', None),
        ('Financial Lease', 'Lease type similar to financing where lessee may gain ownership', 'finance lease, capital lease', 'dim_contract', None),
        ('KM Allowance', 'Maximum kilometers per year included in lease rate', 'mileage allowance, distance limit', 'dim_contract', None),
        ('Excess KM', 'Kilometers driven beyond the allowance, charged at extra rate', 'overage, extra mileage', 'dim_contract', 'Current odometer - (allowance * years)'),
        ('VIN', 'Vehicle Identification Number - unique 17-character vehicle identifier', 'chassis number', 'dim_vehicle', None),
        ('Account Manager', 'Employee responsible for managing customer relationships', 'relationship manager, AM', 'dim_customer', None),
        ('Vehicle Status', 'Current lifecycle state of a vehicle (Created, Active, or Terminated stages)', 'status, state, lifecycle', 'dim_vehicle, ref_vehicle_status', 'Code 0=Created, 1=Active, 2-9=Various termination stages'),
        ('Active Vehicle', 'Vehicle with status code 0 (Created) or 1 (Active)', 'live vehicle, current vehicle', 'dim_vehicle', 'is_active = 1 when status_code IN (0, 1)'),
        ('Terminated Vehicle', 'Vehicle no longer in active fleet, going through settlement process', 'ended, closed, settled', 'dim_vehicle', 'status_code >= 2'),
        ('Order Status', 'Current stage of a vehicle order from creation to delivery', 'order phase, delivery phase', 'ref_order_status', 'Code 0-2=Order Phase, 3-7=Delivery Phase, 9=Cancelled'),
        ('Order Phase', 'Initial stages of order: Created, Sent to Dealer, Delivery Confirmed', 'ordering, procurement', 'ref_order_status', 'status_code IN (0, 1, 2)'),
        ('Delivery Phase', 'Final stages: Insurance, Registration, Driver Pack, Delivered, Lease Schedule', 'fulfillment, delivery', 'ref_order_status', 'status_code IN (3, 4, 5, 6, 7)'),
        ('Maintenance Record', 'A service or repair event recorded for a vehicle, including description, cost, and mileage at time of service', 'service record, repair, service history, maintenance history', 'fact_odometer_reading', "source_type = 'Service' in fact_odometer_reading"),
        ('Service Description', 'Free-text description of maintenance work performed on a vehicle, such as km-based services, part replacements, or inspections', 'repair description, service details, work done', 'fact_odometer_reading', "fact_odometer_reading.transaction_description contains the text"),
        ('Damage', 'A recorded damage or accident event for a vehicle, including repair costs, insurance claims, and third party information', 'accident, claim, incident, crash, collision', 'fact_damages', None),
        ('Net Damage Cost', 'The effective cost of a damage after deducting refunds and salvage amounts', 'net cost, effective damage cost', 'fact_damages', 'damage_amount - amount_refunded - salvage_amount'),
        ('Own Risk', 'The deductible amount the lessee must pay out of pocket for a damage claim', 'deductible, excess, franchise', 'fact_damages', None),
        ('Total Loss', 'A vehicle declared as a total loss where repair costs exceed the vehicle value', 'write-off, total write-off', 'fact_damages', "total_loss_code is set when vehicle is a total loss"),
        ('Fuel Code', 'Numeric code indicating the fuel or energy type of a vehicle: 1=Petrol Unleaded 91 E-Plus, 2=Petrol Unleaded 95 Special, 3=Diesel, 4=LPG, 6=Petrol Unleaded 98 Super, 7=Full Electric (BEV), 8=Plugin Hybrid (PHEV), 9=Hybrid (HEV)', 'fuel type, energy type, propulsion, powertrain', 'dim_vehicle, ref_fuel_code', 'Petrol: fuel_code IN (1,2,6), Diesel: fuel_code=3, LPG: fuel_code=4, Electric (all): fuel_code IN (7,8,9), BEV only: fuel_code=7, Hybrid: fuel_code IN (8,9)'),
        ('Electric Vehicle', 'A vehicle powered fully or partially by electricity, including BEV (full electric), PHEV (plugin hybrid), and HEV (hybrid)', 'EV, BEV, PHEV, HEV, zero emission, electric car', 'dim_vehicle, ref_fuel_code', 'fuel_code IN (7, 8, 9) or is_electric = 1'),
        ('Expected End Date', 'The expected contract termination date, computed as lease start date plus duration months', 'expected termination, contract end, lease end', 'dim_vehicle, dim_contract', 'date(lease_start_date, +lease_duration_months months)'),
        ('Months Driven', 'Number of months elapsed from the lease/contract start date to today', 'time on lease, months elapsed, contract age', 'dim_vehicle, dim_contract', 'months between start_date and now'),
        ('Months Remaining', 'Number of months from today to the expected contract end date. Negative means the contract is past its expected end.', 'time left, contract remaining, expiring soon', 'dim_vehicle, dim_contract', 'months between now and expected_end_date'),
        ('Days to Contract End', 'Number of days from today to the expected contract end date. Use for precise renewal timing and urgency-based prioritization. Negative values indicate overdue contracts. Ideal for AI-driven renewal intelligence and alerts.', 'days remaining, days left, renewal countdown, contract countdown, days until expiry', 'dim_vehicle, dim_contract', 'julianday(expected_end_date) - julianday(now)'),
    ]

    cursor.executemany("""
        INSERT INTO semantic_business_glossary (term, definition, synonyms, related_tables, calculation_formula)
        VALUES (?, ?, ?, ?, ?)
    """, glossary)

    conn.commit()
    print("done")


def calculate_fleet_kpis(conn):
    """Calculate and store fleet KPIs."""
    print("  Calculating fleet KPIs...", end=" ", flush=True)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM agg_fleet_kpis")

    cursor.execute("""
        INSERT INTO agg_fleet_kpis (
            kpi_date, total_customers, total_vehicles, active_vehicles,
            total_drivers, total_contracts, total_monthly_revenue,
            average_odometer_km, vehicles_by_fuel_type, top_makes
        )
        SELECT
            date('now'),
            (SELECT COUNT(*) FROM dim_customer),
            (SELECT COUNT(*) FROM dim_vehicle),
            (SELECT COUNT(*) FROM dim_vehicle WHERE is_active = 1),
            (SELECT COUNT(*) FROM dim_driver),
            (SELECT COUNT(*) FROM dim_contract),
            (SELECT SUM(monthly_rate_total) FROM dim_contract WHERE is_active = 1),
            (SELECT AVG(current_odometer_km) FROM dim_vehicle WHERE current_odometer_km > 0),
            (SELECT GROUP_CONCAT(fuel_type || ':' || cnt)
             FROM (SELECT fuel_type, COUNT(*) as cnt FROM dim_vehicle GROUP BY fuel_type ORDER BY cnt DESC LIMIT 5)),
            (SELECT GROUP_CONCAT(make_name || ':' || cnt)
             FROM (SELECT make_name, COUNT(*) as cnt FROM dim_vehicle GROUP BY make_name ORDER BY cnt DESC LIMIT 5))
    """)
    conn.commit()
    print("done")


def calculate_customer_kpis(conn):
    """Calculate and store customer KPIs."""
    print("  Calculating customer KPIs...", end=" ", flush=True)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM agg_customer_kpis")

    cursor.execute("""
        INSERT INTO agg_customer_kpis (
            customer_id, customer_name, total_vehicles, active_vehicles,
            total_drivers, total_monthly_lease_value, average_vehicle_value,
            total_fleet_value, primary_vehicle_makes
        )
        SELECT
            c.customer_id,
            c.customer_name,
            COUNT(DISTINCT v.vehicle_id),
            COUNT(DISTINCT CASE WHEN v.is_active = 1 THEN v.vehicle_id END),
            COUNT(DISTINCT d.driver_key),
            SUM(v.monthly_lease_amount),
            AVG(v.purchase_price),
            SUM(v.purchase_price),
            GROUP_CONCAT(DISTINCT v.make_name)
        FROM dim_customer c
        LEFT JOIN dim_vehicle v ON c.customer_id = v.customer_id
        LEFT JOIN dim_driver d ON v.vehicle_id = d.vehicle_id
        GROUP BY c.customer_id, c.customer_name
    """)
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM agg_customer_kpis").fetchone()[0]
    print(f"{count} customers")


def main():
    print("=" * 60)
    print("ETL: Staging to Semantic Layer")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    conn = get_connection()

    create_semantic_schema(conn)
    print()

    print("Loading dimension tables...")
    populate_date_dimension(conn)
    load_dim_customer(conn)
    load_dim_vehicle(conn)
    load_dim_driver(conn)
    load_dim_contract(conn)
    load_dim_group(conn)
    load_dim_make_model(conn)
    print()

    print("Loading fact tables...")
    load_fact_odometer(conn)
    load_fact_billing(conn)
    load_fact_damages(conn)
    print()

    print("Populating metadata and KPIs...")
    populate_metadata_catalog(conn)
    calculate_fleet_kpis(conn)
    calculate_customer_kpis(conn)

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)

    cursor = conn.cursor()
    tables = [
        'dim_date', 'dim_customer', 'dim_vehicle', 'dim_driver',
        'dim_contract', 'dim_group', 'dim_make_model',
        'fact_odometer_reading', 'fact_billing', 'fact_damages',
        'semantic_table_catalog', 'semantic_column_catalog',
        'agg_fleet_kpis', 'agg_customer_kpis'
    ]

    for table in tables:
        count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    print()
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    conn.close()


if __name__ == "__main__":
    main()
