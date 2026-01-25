-- =============================================
-- FleetAI Semantic Layer (AI-Ready)
-- SQLite Version
--
-- Design Principles:
-- 1. Clear, natural language column names (no abbreviations)
-- 2. Business-friendly terminology
-- 3. Pre-calculated metrics and KPIs
-- 4. Metadata catalog for AI discoverability
-- 5. Denormalized views for easy querying
-- =============================================

-- =============================================
-- METADATA CATALOG (for AI Discovery)
-- =============================================

-- Table descriptions for AI context
CREATE TABLE IF NOT EXISTS semantic_table_catalog (
    table_name TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    business_domain TEXT NOT NULL,
    grain TEXT,
    typical_questions TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Column descriptions for AI context
CREATE TABLE IF NOT EXISTS semantic_column_catalog (
    catalog_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    data_type TEXT,
    example_values TEXT,
    business_rules TEXT,
    is_measure INTEGER DEFAULT 0,
    is_dimension INTEGER DEFAULT 0,
    is_key INTEGER DEFAULT 0,

    UNIQUE(table_name, column_name)
);

-- Relationships for AI understanding
CREATE TABLE IF NOT EXISTS semantic_relationships (
    relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_table TEXT NOT NULL,
    from_column TEXT NOT NULL,
    to_table TEXT NOT NULL,
    to_column TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    description TEXT,

    UNIQUE(from_table, from_column, to_table, to_column)
);

-- Business glossary for AI terminology
CREATE TABLE IF NOT EXISTS semantic_business_glossary (
    term TEXT PRIMARY KEY,
    definition TEXT NOT NULL,
    synonyms TEXT,
    related_tables TEXT,
    calculation_formula TEXT
);

-- =============================================
-- DIMENSION TABLES
-- =============================================

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date TEXT NOT NULL,
    day_of_week INTEGER,
    day_name TEXT,
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name TEXT,
    quarter_number INTEGER,
    quarter_name TEXT,
    year_number INTEGER,
    is_weekend INTEGER,
    is_business_day INTEGER
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year_number, month_number);

-- Customer Dimension (denormalized, AI-friendly)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL UNIQUE,
    customer_name TEXT NOT NULL,
    customer_name_line_2 TEXT,
    customer_name_line_3 TEXT,
    short_name TEXT,
    address TEXT,
    city TEXT,
    country_code TEXT,
    country_name TEXT,
    phone_number TEXT,
    fax_number TEXT,
    account_manager_name TEXT,
    fleet_size_category TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_name ON dim_customer(customer_name);
CREATE INDEX IF NOT EXISTS idx_dim_customer_manager ON dim_customer(account_manager_name);
CREATE INDEX IF NOT EXISTS idx_dim_customer_country ON dim_customer(country_code);

-- Vehicle Dimension (denormalized, AI-friendly)
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER NOT NULL UNIQUE,
    vin_number TEXT,
    registration_number TEXT,
    make_name TEXT,
    model_name TEXT,
    make_and_model TEXT,
    vehicle_year INTEGER,
    color_name TEXT,
    body_type TEXT,
    fuel_type TEXT,
    customer_id INTEGER,
    customer_name TEXT,
    contract_position_number INTEGER,
    lease_type TEXT,
    lease_type_description TEXT,
    purchase_price REAL,
    residual_value REAL,
    monthly_lease_amount REAL,
    lease_duration_months INTEGER,
    annual_km_allowance INTEGER,
    current_odometer_km INTEGER,
    last_odometer_date TEXT,
    vehicle_status TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_vehicle_customer ON dim_vehicle(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_make ON dim_vehicle(make_name);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_status ON dim_vehicle(vehicle_status);

-- Driver Dimension
CREATE TABLE IF NOT EXISTS dim_driver (
    driver_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER NOT NULL,
    driver_sequence INTEGER NOT NULL,
    driver_name TEXT,
    first_name TEXT,
    last_name TEXT,
    email_address TEXT,
    phone_private TEXT,
    phone_office TEXT,
    phone_mobile TEXT,
    address TEXT,
    city TEXT,
    country_code TEXT,
    is_primary_driver INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),

    UNIQUE(vehicle_id, driver_sequence)
);

CREATE INDEX IF NOT EXISTS idx_dim_driver_vehicle ON dim_driver(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_dim_driver_name ON dim_driver(driver_name);

-- Contract Dimension
CREATE TABLE IF NOT EXISTS dim_contract (
    contract_key INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_position_number INTEGER NOT NULL UNIQUE,
    customer_id INTEGER,
    customer_name TEXT,
    contract_number INTEGER,
    make_name TEXT,
    model_name TEXT,
    lease_type TEXT,
    lease_type_description TEXT,
    contract_duration_months INTEGER,
    annual_km_allowance INTEGER,
    purchase_price REAL,
    residual_value REAL,
    total_lease_amount REAL,

    -- Monthly Rate Breakdown
    monthly_rate_total REAL,
    monthly_rate_depreciation REAL,
    monthly_rate_interest REAL,
    monthly_rate_maintenance REAL,
    monthly_rate_insurance REAL,
    monthly_rate_fuel REAL,
    monthly_rate_tires REAL,
    monthly_rate_road_tax REAL,
    monthly_rate_admin REAL,
    monthly_rate_replacement_vehicle REAL,

    -- Unit Rates
    per_km_rate_maintenance REAL,
    per_km_rate_tires REAL,
    excess_km_rate REAL,

    interest_rate_percent REAL,
    contract_status TEXT DEFAULT 'Active',
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_contract_customer ON dim_contract(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_contract_status ON dim_contract(contract_status);

-- Group Dimension
CREATE TABLE IF NOT EXISTS dim_group (
    group_key INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL UNIQUE,
    group_name TEXT,
    customer_id INTEGER,
    customer_name TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Make/Model Reference
CREATE TABLE IF NOT EXISTS dim_make_model (
    make_model_key INTEGER PRIMARY KEY AUTOINCREMENT,
    make_code INTEGER,
    make_name TEXT,
    model_code INTEGER,
    model_name TEXT,
    model_group TEXT,
    vehicle_type TEXT,
    is_active INTEGER DEFAULT 1,

    UNIQUE(make_code, model_code)
);

CREATE INDEX IF NOT EXISTS idx_dim_make_model_make ON dim_make_model(make_name);

-- =============================================
-- FACT TABLES
-- =============================================

-- Odometer/Mileage Facts
CREATE TABLE IF NOT EXISTS fact_odometer_reading (
    reading_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER NOT NULL,
    reading_date TEXT,
    reading_date_key INTEGER,
    odometer_km INTEGER,
    transaction_amount REAL,
    transaction_description TEXT,
    source_type TEXT,
    supplier_id INTEGER,

    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_odometer_vehicle ON fact_odometer_reading(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_odometer_date ON fact_odometer_reading(reading_date);

-- Billing Facts
CREATE TABLE IF NOT EXISTS fact_billing (
    billing_key INTEGER PRIMARY KEY AUTOINCREMENT,
    billing_id INTEGER NOT NULL,
    vehicle_id INTEGER NOT NULL,
    customer_id INTEGER,
    billing_run_number INTEGER,
    billing_owner_name TEXT,
    billing_recipient_name TEXT,
    billing_address TEXT,
    billing_city TEXT,
    billing_method TEXT,
    fixed_amount REAL,
    variable_amount REAL,
    monthly_amount REAL,
    period_amount REAL,
    currency_code TEXT,

    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_billing_vehicle ON fact_billing(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_billing_customer ON fact_billing(customer_id);

-- =============================================
-- PRE-BUILT ANALYTICAL VIEWS (AI-Optimized)
-- =============================================

-- Fleet Overview (answers: "Show me the fleet", "How many vehicles", "Fleet summary")
CREATE VIEW IF NOT EXISTS view_fleet_overview AS
SELECT
    v.vehicle_id,
    v.vin_number,
    v.registration_number,
    v.make_and_model,
    v.make_name,
    v.model_name,
    v.vehicle_year,
    v.color_name,
    v.fuel_type,
    v.customer_id,
    v.customer_name,
    v.lease_type_description,
    v.monthly_lease_amount,
    v.current_odometer_km,
    v.vehicle_status,
    d.driver_name AS primary_driver_name,
    d.email_address AS driver_email,
    d.phone_mobile AS driver_phone
FROM dim_vehicle v
LEFT JOIN dim_driver d ON v.vehicle_id = d.vehicle_id AND d.driver_sequence = 1;

-- Customer Fleet Summary (answers: "How many vehicles does X have", "Customer fleet size")
CREATE VIEW IF NOT EXISTS view_customer_fleet_summary AS
SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.country_name,
    c.account_manager_name,
    COUNT(v.vehicle_id) AS total_vehicles,
    COUNT(CASE WHEN v.is_active = 1 THEN 1 END) AS active_vehicles,
    SUM(v.monthly_lease_amount) AS total_monthly_lease_value,
    AVG(v.current_odometer_km) AS average_odometer_km,
    GROUP_CONCAT(DISTINCT v.make_name) AS vehicle_makes
FROM dim_customer c
LEFT JOIN dim_vehicle v ON c.customer_id = v.customer_id
GROUP BY c.customer_id, c.customer_name, c.city, c.country_name, c.account_manager_name;

-- Vehicle Details (answers: "Tell me about vehicle X", "Vehicle information")
CREATE VIEW IF NOT EXISTS view_vehicle_details AS
SELECT
    v.vehicle_id,
    v.vin_number,
    v.registration_number,
    v.make_name,
    v.model_name,
    v.make_and_model,
    v.vehicle_year,
    v.color_name,
    v.fuel_type,
    v.lease_type_description,
    v.purchase_price,
    v.residual_value,
    v.monthly_lease_amount,
    v.lease_duration_months,
    v.annual_km_allowance,
    v.current_odometer_km,
    c.customer_name,
    c.account_manager_name,
    ct.monthly_rate_total,
    ct.monthly_rate_depreciation,
    ct.monthly_rate_maintenance,
    ct.monthly_rate_insurance,
    ct.excess_km_rate
FROM dim_vehicle v
LEFT JOIN dim_customer c ON v.customer_id = c.customer_id
LEFT JOIN dim_contract ct ON v.contract_position_number = ct.contract_position_number;

-- Contract Summary (answers: "Show contracts", "Contract details")
CREATE VIEW IF NOT EXISTS view_contract_summary AS
SELECT
    ct.contract_position_number,
    ct.customer_name,
    ct.make_name,
    ct.model_name,
    ct.lease_type_description,
    ct.contract_duration_months,
    ct.annual_km_allowance,
    ct.purchase_price,
    ct.residual_value,
    ct.monthly_rate_total,
    ct.monthly_rate_depreciation,
    ct.monthly_rate_maintenance,
    ct.monthly_rate_insurance,
    ct.interest_rate_percent,
    ct.excess_km_rate,
    ct.contract_status,
    (ct.monthly_rate_total * ct.contract_duration_months) AS total_contract_value
FROM dim_contract ct;

-- Customer Contract Summary (answers: "What contracts does X have")
CREATE VIEW IF NOT EXISTS view_customer_contracts AS
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(ct.contract_position_number) AS total_contracts,
    SUM(ct.monthly_rate_total) AS total_monthly_value,
    AVG(ct.contract_duration_months) AS average_contract_duration,
    SUM(ct.purchase_price) AS total_fleet_value,
    SUM(ct.residual_value) AS total_residual_value
FROM dim_customer c
LEFT JOIN dim_contract ct ON c.customer_id = ct.customer_id
GROUP BY c.customer_id, c.customer_name;

-- Make/Model Distribution (answers: "What makes do we have", "Vehicle brands")
CREATE VIEW IF NOT EXISTS view_make_model_distribution AS
SELECT
    v.make_name,
    v.model_name,
    COUNT(*) AS vehicle_count,
    AVG(v.purchase_price) AS average_purchase_price,
    AVG(v.monthly_lease_amount) AS average_monthly_lease,
    AVG(v.current_odometer_km) AS average_odometer
FROM dim_vehicle v
WHERE v.is_active = 1
GROUP BY v.make_name, v.model_name
ORDER BY vehicle_count DESC;

-- Mileage Analysis (answers: "Vehicle mileage", "Odometer readings")
CREATE VIEW IF NOT EXISTS view_mileage_analysis AS
SELECT
    v.vehicle_id,
    v.registration_number,
    v.make_and_model,
    v.customer_name,
    v.annual_km_allowance,
    v.current_odometer_km,
    COUNT(o.reading_key) AS total_readings,
    MAX(o.odometer_km) AS latest_odometer,
    MIN(o.reading_date) AS first_reading_date,
    MAX(o.reading_date) AS last_reading_date
FROM dim_vehicle v
LEFT JOIN fact_odometer_reading o ON v.vehicle_id = o.vehicle_id
GROUP BY v.vehicle_id, v.registration_number, v.make_and_model,
         v.customer_name, v.annual_km_allowance, v.current_odometer_km;

-- Billing Summary by Customer (answers: "Billing amounts", "Customer billing")
CREATE VIEW IF NOT EXISTS view_customer_billing_summary AS
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT b.vehicle_id) AS vehicles_billed,
    SUM(b.monthly_amount) AS total_monthly_billing,
    SUM(b.fixed_amount) AS total_fixed_charges,
    SUM(b.variable_amount) AS total_variable_charges,
    AVG(b.monthly_amount) AS average_monthly_per_vehicle
FROM dim_customer c
LEFT JOIN fact_billing b ON c.customer_id = b.customer_id
GROUP BY c.customer_id, c.customer_name;

-- Driver Directory (answers: "Who drives vehicle X", "Driver information")
CREATE VIEW IF NOT EXISTS view_driver_directory AS
SELECT
    d.vehicle_id,
    v.registration_number,
    v.make_and_model,
    v.customer_name,
    d.driver_name,
    d.email_address,
    d.phone_mobile,
    d.phone_office,
    d.city,
    d.country_code,
    CASE WHEN d.driver_sequence = 1 THEN 'Primary' ELSE 'Secondary' END AS driver_type
FROM dim_driver d
JOIN dim_vehicle v ON d.vehicle_id = v.vehicle_id
ORDER BY v.customer_name, v.registration_number, d.driver_sequence;

-- Account Manager Portfolio (answers: "Who manages X", "Account manager customers")
CREATE VIEW IF NOT EXISTS view_account_manager_portfolio AS
SELECT
    c.account_manager_name,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(DISTINCT v.vehicle_id) AS total_vehicles,
    SUM(v.monthly_lease_amount) AS total_monthly_revenue,
    GROUP_CONCAT(DISTINCT c.customer_name) AS customer_list
FROM dim_customer c
LEFT JOIN dim_vehicle v ON c.customer_id = v.customer_id
WHERE c.account_manager_name IS NOT NULL
GROUP BY c.account_manager_name
ORDER BY total_monthly_revenue DESC;

-- =============================================
-- AGGREGATE TABLES FOR QUICK INSIGHTS
-- =============================================

-- Fleet KPIs (Key Performance Indicators)
CREATE TABLE IF NOT EXISTS agg_fleet_kpis (
    kpi_date TEXT PRIMARY KEY,
    total_customers INTEGER,
    total_vehicles INTEGER,
    active_vehicles INTEGER,
    total_drivers INTEGER,
    total_contracts INTEGER,
    total_monthly_revenue REAL,
    average_vehicle_age_years REAL,
    average_odometer_km REAL,
    vehicles_by_fuel_type TEXT,
    top_makes TEXT,
    calculated_at TEXT DEFAULT (datetime('now'))
);

-- Customer KPIs
CREATE TABLE IF NOT EXISTS agg_customer_kpis (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    total_vehicles INTEGER,
    active_vehicles INTEGER,
    total_drivers INTEGER,
    total_monthly_lease_value REAL,
    average_vehicle_value REAL,
    total_fleet_value REAL,
    primary_vehicle_makes TEXT,
    calculated_at TEXT DEFAULT (datetime('now'))
);
