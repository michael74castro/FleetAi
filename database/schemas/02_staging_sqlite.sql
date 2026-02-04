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
    expected_end_date TEXT,        -- Computed: date(lease_start_date, '+' || lease_duration_months || ' months')
    months_driven INTEGER,         -- Computed: months between lease_start_date and current date
    months_remaining INTEGER,      -- Computed: months between current date and expected_end_date
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
    months_driven INTEGER,         -- Computed: months between start_date and current date
    months_remaining INTEGER,      -- Computed: months between current date and end_date

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
    previous_object_no INTEGER,  -- Source: CCOR_ORPVOB - links to vehicle being replaced (for renewals)
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
-- Damage Domain
-- =============================================

CREATE TABLE IF NOT EXISTS staging_damages (
    damage_id TEXT PRIMARY KEY,                 -- DADANO
    object_no INTEGER NOT NULL,                 -- DAOBNO
    driver_no INTEGER,                          -- DADADR
    damage_date TEXT,                           -- parsed from DADACC/YY/MM/DD
    description TEXT,                           -- concatenated DADADS+DAD2+DAD3+DAD4+DAD5
    damage_amount REAL,                         -- DADAAM
    accident_location_address TEXT,             -- DADAAD
    accident_country_code TEXT,                 -- DADALA
    mileage INTEGER,                            -- DADAMI
    damage_type TEXT,                           -- DADATY
    fault_code TEXT,                            -- DADAFF
    damage_status_code TEXT,                    -- DADASC
    damage_recourse TEXT,                       -- DASTCD
    total_loss_code TEXT,                       -- DATOCD
    country_code TEXT,                          -- DACOUC
    reporting_period INTEGER,                   -- DARPPD
    insurance_co_number INTEGER,                -- DAINCN
    third_party_name TEXT,                      -- DATPNM
    repair_days INTEGER,                        -- DAREPD
    amount_own_risk REAL,                       -- DADAMO
    amount_refunded REAL,                       -- DADAMR
    claimed_deductible_repair REAL,             -- DADARP
    refunded_deductible_repair REAL,            -- DARERP
    salvage_amount REAL,                        -- DASALD
    damage_fault_level TEXT,                    -- DADAFL
    garage_name TEXT,                           -- DARSGA

    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_damages_object ON staging_damages(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_damages_driver ON staging_damages(driver_no);
CREATE INDEX IF NOT EXISTS idx_staging_damages_date ON staging_damages(damage_date);

-- =============================================
-- Domain Translations (Reference)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_domain_translations (
    translation_key INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT,
    domain_id INTEGER,
    domain_value TEXT,
    language_code TEXT,
    domain_text TEXT,
    source_hash TEXT,
    UNIQUE(country_code, domain_id, domain_value, language_code)
);

CREATE INDEX IF NOT EXISTS idx_staging_dt_domain ON staging_domain_translations(domain_id);

-- =============================================
-- Exploitation Services (Transactional)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_exploitation_services (
    service_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_no INTEGER,
    contract_position_no INTEGER,
    object_no INTEGER,
    service_sequence INTEGER,
    service_code INTEGER,
    service_cost_total REAL,
    service_invoice REAL,
    invoice_supplier REAL,
    total_monthly_cost REAL,
    total_monthly_invoice REAL,
    lp_code TEXT,
    reporting_period INTEGER,
    country_code TEXT,
    volume_code TEXT,
    distance_code TEXT,
    consumption_code TEXT,
    currency_code TEXT,
    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_es_object ON staging_exploitation_services(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_es_customer ON staging_exploitation_services(customer_no);

-- =============================================
-- Maintenance Approvals (Transactional)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_maintenance_approvals (
    approval_key INTEGER PRIMARY KEY AUTOINCREMENT,
    object_no INTEGER NOT NULL,
    sequence INTEGER,
    approval_date TEXT,
    mileage_km REAL,
    amount REAL,
    description TEXT,
    description_2 TEXT,
    description_3 TEXT,
    source_code TEXT,
    maintenance_type INTEGER,
    supplier_no INTEGER,
    supplier_branch TEXT,
    major_code TEXT,
    minor_code TEXT,
    reporting_period INTEGER,
    country_code TEXT,
    volume_code TEXT,
    distance_code TEXT,
    consumption_code TEXT,
    currency_code TEXT,
    si_run_no INTEGER,
    date_from TEXT,
    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_ma_object ON staging_maintenance_approvals(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_ma_supplier ON staging_maintenance_approvals(supplier_no);
CREATE INDEX IF NOT EXISTS idx_staging_ma_date ON staging_maintenance_approvals(approval_date);

-- =============================================
-- Passed On Invoices (Financial)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_passed_invoices (
    invoice_key INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_no INTEGER,
    customer_no INTEGER,
    name_code TEXT,
    object_no INTEGER,
    contract_position_no INTEGER,
    amount REAL,
    cost_code REAL,
    eb_reporting_period REAL,
    driver_no REAL,
    description TEXT,
    gross_net TEXT,
    invoice_no REAL,
    lp_code TEXT,
    object_bridge REAL,
    origin_code TEXT,
    run_no REAL,
    source_code TEXT,
    vat_type TEXT,
    reporting_period INTEGER,
    country_code TEXT,
    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_pi_object ON staging_passed_invoices(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_pi_customer ON staging_passed_invoices(customer_no);

-- =============================================
-- Replacement Cars (Transactional)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_replacement_cars (
    rc_key INTEGER PRIMARY KEY AUTOINCREMENT,
    object_no INTEGER NOT NULL,
    rc_no INTEGER,
    sequence TEXT,
    driver_no REAL,
    rc_run_no REAL,
    begin_date TEXT,
    end_date TEXT,
    rc_code TEXT,
    km REAL,
    amount REAL,
    reason TEXT,
    description TEXT,
    description_2 TEXT,
    description_3 TEXT,
    type TEXT,
    driver_name TEXT,
    source_code TEXT,
    reporting_period INTEGER,
    country_code TEXT,
    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_rc_object ON staging_replacement_cars(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_rc_dates ON staging_replacement_cars(begin_date, end_date);

-- =============================================
-- Reporting Periods (Reference)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_reporting_periods (
    period_key INTEGER PRIMARY KEY AUTOINCREMENT,
    period_cc REAL,
    period_yy REAL,
    period_mm REAL,
    period_dd REAL,
    reporting_period REAL,
    month_period REAL,
    reporting_date TEXT,
    source_hash TEXT
);

-- =============================================
-- Suppliers (Reference/Master)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_suppliers (
    supplier_key INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_no INTEGER,
    branch_no TEXT,
    supplier_name TEXT,
    name_line_2 TEXT,
    name_line_3 TEXT,
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
    reporting_period REAL,
    country TEXT,
    source_hash TEXT,
    UNIQUE(supplier_no, branch_no)
);

CREATE INDEX IF NOT EXISTS idx_staging_su_name ON staging_suppliers(supplier_name);

-- =============================================
-- Car Reports (Monthly Vehicle Snapshot)
-- =============================================

CREATE TABLE IF NOT EXISTS staging_car_reports (
    report_key INTEGER PRIMARY KEY AUTOINCREMENT,
    object_no INTEGER NOT NULL,
    reporting_period INTEGER,
    -- Book values
    book_value_begin_amount REAL,
    book_value_begin_lt REAL,
    disinvestment_amount REAL,
    disinvestment_lt REAL,
    gain_amount REAL,
    gain_lt REAL,
    -- First start
    first_start_book_value REAL,
    first_start_interest_rate REAL,
    -- Cost totals
    fuel_cost_total REAL,
    maintenance_cost_total REAL,
    replacement_car_cost_total REAL,
    tyre_cost_total REAL,
    -- Invoice totals
    fuel_invoice_total REAL,
    maintenance_invoice_total REAL,
    replacement_car_invoice_total REAL,
    tyre_invoice_total REAL,
    -- Mileage
    first_start_km INTEGER,
    odometer_date TEXT,
    first_start_initial_km INTEGER,
    km_driven REAL,
    monthly_km_driven REAL,
    km_technical REAL,
    -- Fuel analysis
    fuel_cost_per_km REAL,
    fuel_invoice_per_km REAL,
    fuel_consumption REAL,
    fuel_slope REAL,
    fuel_monthly_deviation REAL,
    -- Maintenance analysis
    maintenance_cost_per_km REAL,
    maintenance_invoice_per_km REAL,
    maintenance_slope REAL,
    maintenance_monthly_deviation REAL,
    -- Replacement car analysis
    replacement_car_cost_per_km REAL,
    replacement_car_invoice_per_km REAL,
    replacement_car_slope REAL,
    replacement_car_monthly_deviation REAL,
    replacement_car_km REAL,
    replacement_car_amount REAL,
    -- Tyre analysis
    tyre_cost_per_km REAL,
    tyre_invoice_per_km REAL,
    tyre_slope REAL,
    tyre_monthly_deviation REAL,
    -- Running totals
    total_cost REAL,
    total_invoiced REAL,
    cost_per_km REAL,
    total_surplus REAL,
    total_surplus_absolute REAL,
    total_total REAL,
    -- Contract KM
    last_week_km REAL,
    update_km REAL,
    -- Event counts
    maintenance_count INTEGER,
    fuel_count INTEGER,
    replacement_car_count INTEGER,
    tyre_count INTEGER,
    tyre_new_count INTEGER,
    tyre_winter_count INTEGER,
    -- Private km & damage
    private_km_pct REAL,
    damage_count INTEGER,
    damage_reserve REAL,
    -- Segments
    segment_01 REAL, segment_02 REAL, segment_03 REAL, segment_04 REAL, segment_05 REAL,
    segment_06 REAL, segment_07 REAL, segment_08 REAL, segment_09 REAL, segment_10 REAL,
    segment_11 REAL, segment_12 REAL, segment_13 REAL, segment_14 REAL, segment_15 REAL,
    -- Metadata
    country_code TEXT,
    volume_code TEXT,
    distance_code TEXT,
    consumption_code TEXT,
    currency_code TEXT,
    -- Miscellaneous
    misc_insurance_amount REAL,
    misc_insurance_peryear REAL,
    misc_insurance_run_no REAL,
    misc_ts_amount REAL,
    misc_ts_peryear REAL,
    misc_ts_run_no REAL,
    traffic_fines_no INTEGER,
    -- Parking
    parking_cost_total REAL,
    parking_invoice_total REAL,
    parking_monthly_deviation REAL,
    parking_slope REAL,
    -- Unspecified
    unspecified_cost_total REAL,
    unspecified_invoice_total REAL,
    unspecified_monthly_deviation REAL,
    unspecified_slope REAL,
    -- Warranty
    warranty_cost_total REAL,
    warranty_invoice_total REAL,
    warranty_monthly_deviation REAL,
    warranty_slope REAL,
    source_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_cr_object ON staging_car_reports(object_no);
CREATE INDEX IF NOT EXISTS idx_staging_cr_period ON staging_car_reports(reporting_period);
CREATE INDEX IF NOT EXISTS idx_staging_cr_object_period ON staging_car_reports(object_no, reporting_period);

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
