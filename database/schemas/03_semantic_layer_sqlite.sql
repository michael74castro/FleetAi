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

-- Vehicle Status Reference Table
CREATE TABLE IF NOT EXISTS ref_vehicle_status (
    status_code INTEGER PRIMARY KEY,
    status_name TEXT NOT NULL,
    status_description TEXT NOT NULL,
    status_category TEXT NOT NULL,  -- 'Active', 'Created', 'Terminated'
    is_active_status INTEGER DEFAULT 0,
    display_order INTEGER
);

-- Insert vehicle status definitions
INSERT OR REPLACE INTO ref_vehicle_status (status_code, status_name, status_description, status_category, is_active_status, display_order) VALUES
(0, 'Created', 'Vehicle record created but not yet active', 'Created', 1, 1),
(1, 'Active', 'Vehicle is currently active in the fleet', 'Active', 1, 2),
(2, 'Terminated - Invoicing Stopped', 'Contract terminated, invoicing has stopped', 'Terminated', 0, 3),
(3, 'Terminated - Invoice Adjustment Made', 'Contract terminated, invoice adjustment completed', 'Terminated', 0, 4),
(4, 'Terminated - Mileage Adjustment Made', 'Contract terminated, mileage variation adjustment completed', 'Terminated', 0, 5),
(5, 'Terminated - De-investment Made', 'Contract terminated, de-investment completed (steps 3 & 4 done)', 'Terminated', 0, 6),
(8, 'Terminated - Ready for Settlement', 'Contract terminated, ready for first final settlement run', 'Terminated', 0, 7),
(9, 'Terminated - Final Settlement Made', 'Contract terminated, final settlement report completed', 'Terminated', 0, 8);

-- Order Status Reference Table
CREATE TABLE IF NOT EXISTS ref_order_status (
    status_code INTEGER PRIMARY KEY,
    status_name TEXT NOT NULL,
    status_description TEXT NOT NULL,
    status_phase TEXT NOT NULL,  -- 'Order Phase', 'Delivery Phase', 'Cancelled'
    is_active_order INTEGER DEFAULT 1,
    display_order INTEGER
);

-- Insert order status definitions
INSERT OR REPLACE INTO ref_order_status (status_code, status_name, status_description, status_phase, is_active_order, display_order) VALUES
(0, 'Created', 'Order created into the system', 'Order Phase', 1, 1),
(1, 'Sent to Dealer', 'Order sent to dealer', 'Order Phase', 1, 2),
(2, 'Delivery Confirmed', 'Delivery confirmed by dealer', 'Order Phase', 1, 3),
(3, 'Insurance Arranged', 'Arranged for insurance', 'Delivery Phase', 1, 4),
(4, 'Registration Arranged', 'Arranged for vehicle registration and other modifications', 'Delivery Phase', 1, 5),
(5, 'Driver Pack Prepared', 'Prepared driver information pack', 'Delivery Phase', 1, 6),
(6, 'Vehicle Delivered', 'Vehicle delivered to client', 'Delivery Phase', 1, 7),
(7, 'Lease Schedule Generated', 'Generate lease schedule in the system for invoicing', 'Delivery Phase', 0, 8),
(9, 'Cancelled', 'Order cancelled', 'Cancelled', 0, 9);

-- Fuel Code Reference Table
CREATE TABLE IF NOT EXISTS ref_fuel_code (
    fuel_code INTEGER PRIMARY KEY,
    fuel_type TEXT NOT NULL,         -- 'Petrol', 'Diesel', 'LPG', 'Electric'
    fuel_subtype TEXT NOT NULL,      -- Detailed variant name
    fuel_category TEXT NOT NULL,     -- 'ICE', 'Alternative', 'Electric'
    is_electric INTEGER DEFAULT 0,
    display_order INTEGER
);

-- Insert fuel code definitions
INSERT OR REPLACE INTO ref_fuel_code (fuel_code, fuel_type, fuel_subtype, fuel_category, is_electric, display_order) VALUES
(1, 'Petrol', 'Unleaded 91 E-Plus', 'ICE', 0, 1),
(2, 'Petrol', 'Unleaded 95 Special', 'ICE', 0, 2),
(3, 'Diesel', 'Diesel', 'ICE', 0, 3),
(4, 'LPG', 'LPG', 'Alternative', 0, 4),
(6, 'Petrol', 'Unleaded 98 Super', 'ICE', 0, 5),
(7, 'Electric', 'Full Electric Vehicle (BEV)', 'Electric', 1, 6),
(8, 'Electric', 'Plugin Hybrid Electric Vehicle (PHEV)', 'Electric', 1, 7),
(9, 'Electric', 'Hybrid Electric Vehicle (HEV)', 'Electric', 1, 8);

-- Domain Translation Reference Table
CREATE TABLE IF NOT EXISTS ref_domain_translation (
    translation_key INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT,
    domain_id INTEGER,
    domain_value TEXT,
    language_code TEXT,
    domain_text TEXT,
    UNIQUE(country_code, domain_id, domain_value, language_code)
);

CREATE INDEX IF NOT EXISTS idx_ref_dt_domain ON ref_domain_translation(domain_id);

-- Reporting Period Reference Table
CREATE TABLE IF NOT EXISTS ref_reporting_period (
    period_key INTEGER PRIMARY KEY AUTOINCREMENT,
    reporting_period REAL,
    month_period REAL,
    reporting_date TEXT
);

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
    fuel_code INTEGER,              -- FK to ref_fuel_code.fuel_code
    fuel_type TEXT,                  -- e.g. 'Petrol', 'Diesel', 'Electric'
    is_electric INTEGER DEFAULT 0,  -- 1 if fuel_code IN (7,8,9)
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
    lease_start_date TEXT,
    lease_end_date TEXT,
    expected_end_date TEXT,        -- date(lease_start_date, '+' || lease_duration_months || ' months')
    months_driven INTEGER,         -- months between lease_start_date and today
    months_remaining INTEGER,      -- months between today and expected_end_date
    days_to_contract_end INTEGER,  -- days between today and expected_end_date (negative = overdue)
    vehicle_status_code INTEGER,  -- FK to ref_vehicle_status.status_code
    vehicle_status TEXT,          -- Human-readable status name
    is_active INTEGER DEFAULT 1,  -- 1 if status_code IN (0, 1)
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_vehicle_customer ON dim_vehicle(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_make ON dim_vehicle(make_name);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_status ON dim_vehicle(vehicle_status);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_lease_start ON dim_vehicle(lease_start_date);

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
    contract_start_date TEXT,
    contract_end_date TEXT,         -- date(contract_start_date, '+' || contract_duration_months || ' months')
    months_driven INTEGER,
    months_remaining INTEGER,
    days_to_contract_end INTEGER,  -- days between today and contract_end_date (negative = overdue)
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

-- Supplier Dimension
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_no INTEGER NOT NULL,
    branch_no TEXT,
    supplier_name TEXT,
    name_line_2 TEXT,
    name_line_3 TEXT,
    full_name TEXT,
    class TEXT,
    country_code TEXT,
    address TEXT,
    city TEXT,
    category TEXT,
    phone TEXT,
    fax TEXT,
    email TEXT,
    contact_person TEXT,
    responsible_person TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(supplier_no, branch_no)
);

CREATE INDEX IF NOT EXISTS idx_dim_supplier_name ON dim_supplier(supplier_name);
CREATE INDEX IF NOT EXISTS idx_dim_supplier_no ON dim_supplier(supplier_no);

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

-- Damage/Accident Facts
CREATE TABLE IF NOT EXISTS fact_damages (
    damage_key INTEGER PRIMARY KEY AUTOINCREMENT,
    damage_id TEXT NOT NULL,
    vehicle_id INTEGER,
    driver_number INTEGER,
    damage_date TEXT,
    description TEXT,
    damage_amount REAL,
    net_damage_cost REAL,              -- calculated: amount - refunded - salvage
    accident_location TEXT,
    accident_country_code TEXT,
    mileage INTEGER,
    damage_type TEXT,
    fault_code TEXT,
    damage_status_code TEXT,
    damage_recourse TEXT,
    total_loss_code TEXT,
    country_code TEXT,
    reporting_period INTEGER,
    insurance_company_number INTEGER,
    third_party_name TEXT,
    repair_days INTEGER,
    amount_own_risk REAL,
    amount_refunded REAL,
    claimed_deductible REAL,
    refunded_deductible REAL,
    salvage_amount REAL,
    damage_fault_level TEXT,
    garage_name TEXT,

    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_damages_vehicle ON fact_damages(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_damages_date ON fact_damages(damage_date);
CREATE INDEX IF NOT EXISTS idx_fact_damages_type ON fact_damages(damage_type);

-- Maintenance Approval Facts
CREATE TABLE IF NOT EXISTS fact_maintenance_approvals (
    approval_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER,
    supplier_no INTEGER,
    supplier_name TEXT,
    approval_date TEXT,
    mileage_km REAL,
    amount REAL,
    description TEXT,
    maintenance_type INTEGER,
    major_code TEXT,
    minor_code TEXT,
    source_code TEXT,
    reporting_period INTEGER,
    country_code TEXT,
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_ma_vehicle ON fact_maintenance_approvals(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_ma_supplier ON fact_maintenance_approvals(supplier_no);
CREATE INDEX IF NOT EXISTS idx_fact_ma_date ON fact_maintenance_approvals(approval_date);

-- Exploitation Services Facts
CREATE TABLE IF NOT EXISTS fact_exploitation_services (
    service_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER,
    customer_no INTEGER,
    service_sequence INTEGER,
    service_code INTEGER,
    service_cost_total REAL,
    service_invoice REAL,
    total_monthly_cost REAL,
    total_monthly_invoice REAL,
    reporting_period INTEGER,
    country_code TEXT,
    currency_code TEXT,
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_es_vehicle ON fact_exploitation_services(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_es_customer ON fact_exploitation_services(customer_no);

-- Passed Invoice Facts
CREATE TABLE IF NOT EXISTS fact_passed_invoices (
    invoice_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER,
    customer_no INTEGER,
    contract_no INTEGER,
    amount REAL,
    cost_code REAL,
    description TEXT,
    gross_net TEXT,
    invoice_no REAL,
    origin_code TEXT,
    source_code TEXT,
    vat_type TEXT,
    reporting_period INTEGER,
    country_code TEXT,
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_pi_vehicle ON fact_passed_invoices(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_pi_customer ON fact_passed_invoices(customer_no);

-- Replacement Car Facts
CREATE TABLE IF NOT EXISTS fact_replacement_cars (
    rc_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER,
    rc_no INTEGER,
    driver_name TEXT,
    begin_date TEXT,
    end_date TEXT,
    rc_code TEXT,
    km REAL,
    amount REAL,
    reason TEXT,
    description TEXT,
    rc_type TEXT,
    reporting_period INTEGER,
    country_code TEXT,
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_rc_vehicle ON fact_replacement_cars(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_rc_dates ON fact_replacement_cars(begin_date, end_date);

-- Car Reports Facts (Monthly Vehicle Snapshot)
CREATE TABLE IF NOT EXISTS fact_car_reports (
    report_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id INTEGER,
    reporting_period INTEGER,
    -- Book Value & Depreciation
    book_value_begin_amount REAL,       -- Book value at beginning of period
    book_value_begin_lt REAL,           -- Book value begin (local transformation)
    first_start_book_value REAL,        -- Original budgeted investment / initial book value
    disinvestment_amount REAL,          -- Monthly depreciation / write-down amount
    disinvestment_lt REAL,              -- Disinvestment (local transformation)
    gain_amount REAL,                   -- Residual value gain/loss
    gain_lt REAL,                       -- Gain (local transformation)
    first_start_interest_rate REAL,     -- Initial interest rate at contract start
    -- Running Costs
    fuel_cost_total REAL,
    maintenance_cost_total REAL,
    replacement_car_cost_total REAL,
    tyre_cost_total REAL,
    fuel_invoice_total REAL,
    maintenance_invoice_total REAL,
    replacement_car_invoice_total REAL,
    tyre_invoice_total REAL,
    total_cost REAL,
    total_invoiced REAL,
    km_driven REAL,
    monthly_km_driven REAL,
    cost_per_km REAL,
    fuel_consumption REAL,
    fuel_cost_per_km REAL,
    maintenance_cost_per_km REAL,
    maintenance_count INTEGER,
    fuel_count INTEGER,
    replacement_car_count INTEGER,
    tyre_count INTEGER,
    damage_count INTEGER,
    total_surplus REAL,
    country_code TEXT,
    currency_code TEXT,
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_cr_vehicle ON fact_car_reports(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fact_cr_period ON fact_car_reports(reporting_period);

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
    v.expected_end_date,
    v.months_driven,
    v.months_remaining,
    v.days_to_contract_end,
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
    v.expected_end_date,
    v.months_driven,
    v.months_remaining,
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
    ct.contract_start_date,
    ct.contract_end_date,
    ct.months_driven,
    ct.months_remaining,
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

-- Maintenance Analysis (answers: "Maintenance costs", "Which suppliers do we use")
CREATE VIEW IF NOT EXISTS view_maintenance_analysis AS
SELECT
    ma.vehicle_id,
    v.registration_number,
    v.make_and_model,
    v.customer_name,
    ma.approval_date,
    ma.mileage_km,
    ma.amount,
    ma.description,
    ma.maintenance_type,
    ma.major_code,
    ma.minor_code,
    ma.supplier_no,
    ma.supplier_name,
    ma.reporting_period
FROM fact_maintenance_approvals ma
LEFT JOIN dim_vehicle v ON ma.vehicle_id = v.vehicle_id;

-- Vehicle Cost Analysis from Car Reports (answers: "Total cost of ownership", "Vehicle running costs")
CREATE VIEW IF NOT EXISTS view_vehicle_cost_analysis AS
SELECT
    cr.vehicle_id,
    v.registration_number,
    v.make_and_model,
    v.customer_name,
    cr.reporting_period,
    cr.fuel_cost_total,
    cr.maintenance_cost_total,
    cr.replacement_car_cost_total,
    cr.tyre_cost_total,
    cr.total_cost,
    cr.total_invoiced,
    cr.km_driven,
    cr.cost_per_km,
    cr.fuel_consumption,
    cr.maintenance_count,
    cr.fuel_count,
    cr.damage_count,
    cr.total_surplus
FROM fact_car_reports cr
LEFT JOIN dim_vehicle v ON cr.vehicle_id = v.vehicle_id;

-- Supplier Summary (answers: "Supplier spending", "Top suppliers")
CREATE VIEW IF NOT EXISTS view_supplier_summary AS
SELECT
    s.supplier_no,
    s.supplier_name,
    s.city,
    s.country_code,
    s.category,
    s.contact_person,
    COUNT(ma.approval_key) AS total_maintenance_events,
    SUM(ma.amount) AS total_maintenance_spending,
    AVG(ma.amount) AS average_event_cost,
    COUNT(DISTINCT ma.vehicle_id) AS vehicles_serviced
FROM dim_supplier s
LEFT JOIN fact_maintenance_approvals ma ON s.supplier_no = ma.supplier_no
GROUP BY s.supplier_no, s.supplier_name, s.city, s.country_code, s.category, s.contact_person;

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
