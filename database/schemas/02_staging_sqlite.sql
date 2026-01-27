-- =============================================
-- FleetAI Staging Schema (SQLite Version)
-- Cleaned and transformed data
-- =============================================

-- =============================================
-- Customer Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    customer_name_2 TEXT,
    customer_name_3 TEXT,
    call_name TEXT,
    company_code TEXT,
    address TEXT,
    city TEXT,
    country_code TEXT,
    phone TEXT,
    fax TEXT,
    account_manager TEXT,
    report_period INTEGER,
    country TEXT,
    fleet_code INTEGER,
    cv_code INTEGER,
    nk_code INTEGER,
    status TEXT DEFAULT 'Active',

    -- CDC tracking
    source_hash TEXT,
    valid_from TEXT DEFAULT (datetime('now')),
    valid_to TEXT,
    is_current INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_staging_customers_name ON staging_customers(customer_name);
CREATE INDEX IF NOT EXISTS idx_staging_customers_country ON staging_customers(country_code);

-- =============================================
-- Driver Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_drivers (
    driver_key INTEGER PRIMARY KEY AUTOINCREMENT,
    object_no INTEGER NOT NULL,
    driver_no INTEGER NOT NULL,
    active_driver TEXT,
    driver_name TEXT,
    first_name TEXT,
    last_name TEXT,
    address TEXT,
    city TEXT,
    country_code TEXT,
    private_phone TEXT,
    office_phone TEXT,
    mobile_phone TEXT,
    email TEXT,
    license_number TEXT,
    license_expiry_date TEXT,
    date_of_birth TEXT,
    employee_id TEXT,
    cost_center TEXT,
    report_period INTEGER,
    country TEXT,
    status TEXT DEFAULT 'Active',

    source_hash TEXT,
    valid_from TEXT DEFAULT (datetime('now')),
    valid_to TEXT,
    is_current INTEGER DEFAULT 1,

    UNIQUE(object_no, driver_no)
);

CREATE INDEX IF NOT EXISTS idx_staging_drivers_object ON staging_drivers(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_drivers_name ON staging_drivers(driver_name);

-- =============================================
-- Vehicle/Equipment Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_vehicles (
    object_no INTEGER PRIMARY KEY,
    vin TEXT,
    make_code INTEGER,
    model_code INTEGER,
    make_name TEXT,
    model_name TEXT,
    customer_no INTEGER,
    pc_no INTEGER,
    contract_no INTEGER,
    contract_position_no INTEGER,
    color_code TEXT,
    color_name TEXT,
    body_type_code INTEGER,
    registration_no TEXT,
    purchase_price REAL,
    residual_value REAL,
    lease_start_date TEXT,
    lease_end_date TEXT,
    lease_duration_months INTEGER,
    lease_amount REAL,
    km_allowance INTEGER,
    km_budget INTEGER,
    current_km INTEGER,
    last_km_date TEXT,
    fuel_code INTEGER,
    fuel_type TEXT,
    lease_type TEXT,
    license_plate_code TEXT,
    group_no INTEGER,
    order_no INTEGER,
    sale_price REAL,
    report_period INTEGER,
    country TEXT,
    volume_unit TEXT,
    distance_unit TEXT,
    consumption_unit TEXT,
    currency TEXT,
    object_status INTEGER,  -- Source: CCOB_OBOBSC_Object_Status
                            -- 0 = Created
                            -- 1 = Active
                            -- 2 = Terminated - Invoicing Stopped
                            -- 3 = Terminated - Invoice adjustment is made
                            -- 4 = Terminated - Mileage variation adjustment is made
                            -- 5 = Terminated - De-investment is made (3 & 4 done)
                            -- 8 = Terminated - Ready for final settlement run
                            -- 9 = Terminated - Final settlement report is made
    status TEXT DEFAULT 'Active',

    source_hash TEXT,
    valid_from TEXT DEFAULT (datetime('now')),
    valid_to TEXT,
    is_current INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_staging_vehicles_vin ON staging_vehicles(vin);
CREATE INDEX IF NOT EXISTS idx_staging_vehicles_status ON staging_vehicles(object_status);
CREATE INDEX IF NOT EXISTS idx_staging_vehicles_customer ON staging_vehicles(customer_no);
CREATE INDEX IF NOT EXISTS idx_staging_vehicles_contract ON staging_vehicles(contract_position_no);
CREATE INDEX IF NOT EXISTS idx_staging_vehicles_registration ON staging_vehicles(registration_no);

-- =============================================
-- Contract Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_contracts (
    contract_position_no INTEGER PRIMARY KEY,
    group_no INTEGER,
    customer_no INTEGER NOT NULL,
    pc_no INTEGER,
    contract_no INTEGER,
    active_cp TEXT,
    delivery_days REAL,
    duration_months INTEGER,
    km_per_year INTEGER,
    purchase_price REAL,
    residual_value REAL,
    total_amount REAL,
    invoice_base REAL,
    interest_rate REAL,
    make_code INTEGER,
    model_code INTEGER,

    -- Monthly rates
    monthly_rate_admin REAL,
    monthly_rate_fuel REAL,
    monthly_rate_depreciation REAL,
    monthly_rate_insurance REAL,
    monthly_rate_interest REAL,
    monthly_rate_maintenance REAL,
    monthly_rate_replacement REAL,
    monthly_rate_road_tax REAL,
    monthly_rate_tires REAL,
    monthly_rate_total REAL,

    -- Unit rates
    unit_rate_maintenance REAL,
    unit_rate_tires REAL,
    unit_rate_replacement REAL,

    excess_km_rate REAL,
    report_period INTEGER,
    country TEXT,
    lease_type TEXT,

    -- Dates
    start_date TEXT,
    end_date TEXT,

    status TEXT DEFAULT 'Active',

    source_hash TEXT,
    valid_from TEXT DEFAULT (datetime('now')),
    valid_to TEXT,
    is_current INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_staging_contracts_customer ON staging_contracts(customer_no);
CREATE INDEX IF NOT EXISTS idx_staging_contracts_dates ON staging_contracts(start_date, end_date);

-- =============================================
-- Order Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_orders (
    order_no INTEGER PRIMARY KEY,
    contract_position_no INTEGER,
    group_no INTEGER,
    customer_no INTEGER NOT NULL,
    pc_no INTEGER,
    contract_no INTEGER,
    exterior_color TEXT,
    interior_color TEXT,
    location TEXT,
    order_date TEXT,
    requested_delivery_date TEXT,
    confirmed_delivery_date TEXT,
    actual_delivery_date TEXT,
    order_status_code INTEGER,  -- Source: CCOR_ORORSC_Order_Status_Code
                                -- 0 = Order Phase - Created into the system
                                -- 1 = Order Phase - Sent to dealer
                                -- 2 = Order Phase - Delivery confirmed by dealer
                                -- 3 = Delivery Phase - Arranged for insurance
                                -- 4 = Delivery Phase - Arranged for registration & modifications
                                -- 5 = Delivery Phase - Prepared driver information pack
                                -- 6 = Delivery Phase - Vehicle delivered to client
                                -- 7 = Delivery Phase - Generate lease schedule for invoicing
                                -- 9 = Order cancelled
    order_status TEXT,
    supplier_no INTEGER,
    supplier_amount REAL,
    report_period INTEGER,
    country TEXT,

    source_hash TEXT,
    valid_from TEXT DEFAULT (datetime('now')),
    valid_to TEXT,
    is_current INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_staging_orders_customer ON staging_orders(customer_no);
CREATE INDEX IF NOT EXISTS idx_staging_orders_status ON staging_orders(order_status_code);
CREATE INDEX IF NOT EXISTS idx_staging_orders_date ON staging_orders(order_date);

-- =============================================
-- Billing Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_billing (
    billing_key INTEGER PRIMARY KEY AUTOINCREMENT,
    billing_no INTEGER NOT NULL,
    object_no INTEGER NOT NULL,
    billing_run_no INTEGER,
    billing_owner TEXT,
    billing_name TEXT,
    billing_address TEXT,
    billing_city TEXT,
    billing_method TEXT,
    billing_amount_1 REAL,
    billing_amount_2 REAL,
    billing_amount_monthly REAL,
    billing_amount_variable REAL,
    billing_amount_period REAL,
    currency TEXT,
    report_period INTEGER,
    country TEXT,

    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_billing_object ON staging_billing(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_billing_no ON staging_billing(billing_no);

-- =============================================
-- Reference Data - Automobiles (Make/Model)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_automobiles (
    auto_key INTEGER PRIMARY KEY AUTOINCREMENT,
    make_code INTEGER,
    make_name TEXT,
    model_code INTEGER,
    model_name TEXT,
    model_group_high REAL,
    model_group_range INTEGER,
    model_group_size REAL,
    type_description TEXT,
    report_period INTEGER,
    country TEXT,

    source_hash TEXT,

    UNIQUE(make_code, model_code)
);

CREATE INDEX IF NOT EXISTS idx_staging_auto_make ON staging_automobiles(make_code);
CREATE INDEX IF NOT EXISTS idx_staging_auto_model ON staging_automobiles(model_code);

-- =============================================
-- Groups Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_groups (
    group_no INTEGER PRIMARY KEY,
    customer_no INTEGER,
    group_name TEXT,
    report_period INTEGER,
    country TEXT,

    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_groups_customer ON staging_groups(customer_no);

-- =============================================
-- Maintenance/Odometer History
-- =============================================

CREATE TABLE IF NOT EXISTS staging_odometer_history (
    record_key INTEGER PRIMARY KEY AUTOINCREMENT,
    object_no INTEGER NOT NULL,
    sequence_no INTEGER,
    reading_date TEXT,
    odometer_km INTEGER,
    amount REAL,
    description TEXT,
    source_code INTEGER,
    supplier_no INTEGER,
    supplier_ref TEXT,
    transaction_no INTEGER,
    transaction_make TEXT,
    transaction_spec TEXT,

    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_odometer_object ON staging_odometer_history(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_odometer_date ON staging_odometer_history(reading_date);

-- =============================================
-- Fuel Prices
-- =============================================

CREATE TABLE IF NOT EXISTS staging_fuel_prices (
    price_key INTEGER PRIMARY KEY AUTOINCREMENT,
    fuel_code INTEGER,
    fuel_type TEXT,
    price_date TEXT,
    price_per_unit REAL,
    currency TEXT,
    report_period INTEGER,
    country TEXT,

    source_hash TEXT,

    UNIQUE(fuel_code, price_date)
);

-- =============================================
-- ETL Tracking Tables
-- =============================================

CREATE TABLE IF NOT EXISTS staging_etl_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    load_type TEXT NOT NULL,
    load_start TEXT NOT NULL,
    load_end TEXT,
    source_rows INTEGER,
    inserted_rows INTEGER,
    updated_rows INTEGER,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_staging_etl_log_table ON staging_etl_log(table_name, load_start);
