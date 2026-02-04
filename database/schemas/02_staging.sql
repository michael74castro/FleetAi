-- =============================================
-- FleetAI Staging Schema
-- Cleaned and transformed data
-- =============================================

CREATE SCHEMA IF NOT EXISTS staging;
GO

-- =============================================
-- Customer Domain
-- =============================================

CREATE TABLE staging.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    legal_name VARCHAR(200),
    account_type VARCHAR(50),
    parent_customer_id VARCHAR(20),
    tax_id VARCHAR(30),
    industry VARCHAR(100),
    employee_count INT,
    annual_revenue DECIMAL(18,2),
    credit_rating VARCHAR(20),
    credit_limit DECIMAL(18,2),
    payment_terms VARCHAR(50),
    account_manager VARCHAR(50),
    region VARCHAR(50),
    territory VARCHAR(50),
    status VARCHAR(20) NOT NULL,
    created_date DATE,
    last_update DATETIME2,

    -- CDC tracking
    source_hash VARBINARY(32),
    valid_from DATETIME2 DEFAULT GETUTCDATE(),
    valid_to DATETIME2,
    is_current BIT DEFAULT 1,

    FOREIGN KEY (parent_customer_id) REFERENCES staging.customers(customer_id)
);

CREATE INDEX IX_staging_customers_parent ON staging.customers(parent_customer_id);
CREATE INDEX IX_staging_customers_status ON staging.customers(status);
CREATE INDEX IX_staging_customers_manager ON staging.customers(account_manager);

CREATE TABLE staging.customer_contacts (
    contact_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name AS CONCAT(first_name, ' ', last_name) PERSISTED,
    title VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(30),
    mobile VARCHAR(30),
    role VARCHAR(50),
    is_primary_contact BIT DEFAULT 0,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),
    valid_from DATETIME2 DEFAULT GETUTCDATE(),
    valid_to DATETIME2,
    is_current BIT DEFAULT 1,

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id)
);

CREATE INDEX IX_staging_contacts_customer ON staging.customer_contacts(customer_id);

CREATE TABLE staging.customer_billing (
    billing_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    billing_name VARCHAR(200),
    address_line_1 VARCHAR(200),
    address_line_2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    email VARCHAR(100),
    payment_method VARCHAR(50),
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),
    is_current BIT DEFAULT 1,

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id)
);

-- =============================================
-- Driver Domain
-- =============================================

CREATE TABLE staging.drivers (
    driver_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name AS CONCAT(first_name, ' ', last_name) PERSISTED,
    email VARCHAR(100),
    phone VARCHAR(30),
    mobile VARCHAR(30),
    license_number VARCHAR(30),
    license_state VARCHAR(10),
    license_expiry DATE,
    date_of_birth DATE,
    employee_id VARCHAR(30),
    department VARCHAR(100),
    cost_center VARCHAR(50),
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),
    valid_from DATETIME2 DEFAULT GETUTCDATE(),
    valid_to DATETIME2,
    is_current BIT DEFAULT 1,

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id)
);

CREATE INDEX IX_staging_drivers_customer ON staging.drivers(customer_id);
CREATE INDEX IX_staging_drivers_license_expiry ON staging.drivers(license_expiry);

-- =============================================
-- Vehicle/Equipment Domain
-- =============================================

CREATE TABLE staging.vehicles (
    equipment_id VARCHAR(20) PRIMARY KEY,
    contract_no VARCHAR(20),
    vin VARCHAR(17),
    license_plate VARCHAR(20),
    make VARCHAR(50),
    model VARCHAR(50),
    model_year INT,
    color VARCHAR(30),
    body_type VARCHAR(30),
    engine_type VARCHAR(30),
    fuel_type VARCHAR(20),
    transmission VARCHAR(20),
    current_odometer INT,
    acquisition_date DATE,
    acquisition_cost DECIMAL(15,2),
    residual_value DECIMAL(15,2),
    expected_end_date DATE,
    months_driven INT,
    months_remaining INT,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),
    valid_from DATETIME2 DEFAULT GETUTCDATE(),
    valid_to DATETIME2,
    is_current BIT DEFAULT 1
);

CREATE INDEX IX_staging_vehicles_contract ON staging.vehicles(contract_no);
CREATE INDEX IX_staging_vehicles_vin ON staging.vehicles(vin);
CREATE INDEX IX_staging_vehicles_status ON staging.vehicles(status);

CREATE TABLE staging.vehicle_specifications (
    spec_id INT IDENTITY(1,1) PRIMARY KEY,
    equipment_id VARCHAR(20) NOT NULL,
    spec_code VARCHAR(20),
    spec_category VARCHAR(50),
    spec_description VARCHAR(200),
    spec_value VARCHAR(100),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (equipment_id) REFERENCES staging.vehicles(equipment_id)
);

CREATE INDEX IX_staging_vehicle_specs_equipment ON staging.vehicle_specifications(equipment_id);

CREATE TABLE staging.driver_vehicle_assignments (
    assignment_id BIGINT PRIMARY KEY,
    driver_id VARCHAR(20) NOT NULL,
    equipment_id VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    is_primary_driver BIT DEFAULT 0,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (driver_id) REFERENCES staging.drivers(driver_id),
    FOREIGN KEY (equipment_id) REFERENCES staging.vehicles(equipment_id)
);

CREATE INDEX IX_staging_dva_driver ON staging.driver_vehicle_assignments(driver_id);
CREATE INDEX IX_staging_dva_equipment ON staging.driver_vehicle_assignments(equipment_id);
CREATE INDEX IX_staging_dva_dates ON staging.driver_vehicle_assignments(start_date, end_date);

-- =============================================
-- Contract Domain
-- =============================================

CREATE TABLE staging.contracts (
    contract_no VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    contract_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    term_months INT,
    months_driven INT,
    months_remaining INT,
    monthly_rate DECIMAL(15,2),
    mileage_allowance INT,
    excess_mileage_rate DECIMAL(10,4),
    early_termination_penalty DECIMAL(15,2),
    insurance_included BIT DEFAULT 0,
    maintenance_included BIT DEFAULT 0,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),
    valid_from DATETIME2 DEFAULT GETUTCDATE(),
    valid_to DATETIME2,
    is_current BIT DEFAULT 1,

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id)
);

CREATE INDEX IX_staging_contracts_customer ON staging.contracts(customer_id);
CREATE INDEX IX_staging_contracts_dates ON staging.contracts(start_date, end_date);
CREATE INDEX IX_staging_contracts_status ON staging.contracts(status);

CREATE TABLE staging.contract_charges (
    charge_id BIGINT PRIMARY KEY,
    contract_no VARCHAR(20) NOT NULL,
    charge_type VARCHAR(50),
    charge_code VARCHAR(20),
    description VARCHAR(200),
    amount DECIMAL(15,2),
    effective_date DATE,
    end_date DATE,
    frequency VARCHAR(20),
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (contract_no) REFERENCES staging.contracts(contract_no)
);

CREATE INDEX IX_staging_contract_charges_contract ON staging.contract_charges(contract_no);

-- =============================================
-- Invoice Domain
-- =============================================

CREATE TABLE staging.invoices (
    invoice_no VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    contract_no VARCHAR(20),
    invoice_date DATE NOT NULL,
    due_date DATE,
    billing_period_start DATE,
    billing_period_end DATE,
    subtotal DECIMAL(15,2),
    tax_amount DECIMAL(15,2),
    total_amount DECIMAL(15,2),
    paid_amount DECIMAL(15,2) DEFAULT 0,
    balance AS (total_amount - paid_amount) PERSISTED,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id),
    FOREIGN KEY (contract_no) REFERENCES staging.contracts(contract_no)
);

CREATE INDEX IX_staging_invoices_customer ON staging.invoices(customer_id);
CREATE INDEX IX_staging_invoices_date ON staging.invoices(invoice_date);
CREATE INDEX IX_staging_invoices_status ON staging.invoices(status);

CREATE TABLE staging.invoice_line_items (
    line_id BIGINT PRIMARY KEY,
    invoice_no VARCHAR(30) NOT NULL,
    equipment_id VARCHAR(20),
    charge_code VARCHAR(20),
    description VARCHAR(200),
    quantity DECIMAL(10,2),
    unit_price DECIMAL(15,4),
    amount DECIMAL(15,2),
    tax_code VARCHAR(10),
    tax_amount DECIMAL(15,2),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (invoice_no) REFERENCES staging.invoices(invoice_no),
    FOREIGN KEY (equipment_id) REFERENCES staging.vehicles(equipment_id)
);

CREATE INDEX IX_staging_invoice_lines_invoice ON staging.invoice_line_items(invoice_no);

CREATE TABLE staging.receipts (
    receipt_no VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    invoice_no VARCHAR(30),
    receipt_date DATE NOT NULL,
    amount DECIMAL(15,2),
    payment_method VARCHAR(50),
    reference VARCHAR(100),
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id),
    FOREIGN KEY (invoice_no) REFERENCES staging.invoices(invoice_no)
);

-- =============================================
-- Fuel Domain
-- =============================================

CREATE TABLE staging.fuel_cards (
    card_id VARCHAR(30) PRIMARY KEY,
    equipment_id VARCHAR(20),
    driver_id VARCHAR(20),
    card_number VARCHAR(30),
    card_type VARCHAR(20),
    provider VARCHAR(50),
    credit_limit DECIMAL(15,2),
    issue_date DATE,
    expiry_date DATE,
    pin_required BIT DEFAULT 0,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (equipment_id) REFERENCES staging.vehicles(equipment_id),
    FOREIGN KEY (driver_id) REFERENCES staging.drivers(driver_id)
);

CREATE INDEX IX_staging_fuel_cards_equipment ON staging.fuel_cards(equipment_id);
CREATE INDEX IX_staging_fuel_cards_driver ON staging.fuel_cards(driver_id);

CREATE TABLE staging.fuel_transactions (
    transaction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    invoice_no VARCHAR(30),
    card_no VARCHAR(30),
    equipment_id VARCHAR(20),
    driver_id VARCHAR(20),
    transaction_date DATE NOT NULL,
    merchant_name VARCHAR(100),
    merchant_city VARCHAR(100),
    merchant_state VARCHAR(50),
    product_code VARCHAR(20),
    product_description VARCHAR(100),
    quantity DECIMAL(15,3),
    unit_price DECIMAL(10,4),
    amount DECIMAL(15,2),
    odometer INT,
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (equipment_id) REFERENCES staging.vehicles(equipment_id),
    FOREIGN KEY (driver_id) REFERENCES staging.drivers(driver_id)
);

CREATE INDEX IX_staging_fuel_trans_equipment ON staging.fuel_transactions(equipment_id);
CREATE INDEX IX_staging_fuel_trans_date ON staging.fuel_transactions(transaction_date);

CREATE TABLE staging.fuel_prices (
    price_id BIGINT PRIMARY KEY,
    fuel_type VARCHAR(50),
    region VARCHAR(50),
    effective_date DATE,
    price_per_gallon DECIMAL(10,4),
    source VARCHAR(100),
    last_update DATETIME2,

    source_hash VARBINARY(32)
);

CREATE INDEX IX_staging_fuel_prices_date ON staging.fuel_prices(effective_date, fuel_type);

-- =============================================
-- Maintenance/Work Order Domain
-- =============================================

CREATE TABLE staging.work_orders (
    work_order_no VARCHAR(30) PRIMARY KEY,
    equipment_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20),
    order_type VARCHAR(50),
    priority VARCHAR(20),
    description VARCHAR(500),
    requested_date DATE,
    scheduled_date DATE,
    completed_date DATE,
    vendor_id VARCHAR(20),
    estimated_cost DECIMAL(15,2),
    actual_cost DECIMAL(15,2),
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),
    valid_from DATETIME2 DEFAULT GETUTCDATE(),
    valid_to DATETIME2,
    is_current BIT DEFAULT 1,

    FOREIGN KEY (equipment_id) REFERENCES staging.vehicles(equipment_id),
    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id)
);

CREATE INDEX IX_staging_work_orders_equipment ON staging.work_orders(equipment_id);
CREATE INDEX IX_staging_work_orders_customer ON staging.work_orders(customer_id);
CREATE INDEX IX_staging_work_orders_status ON staging.work_orders(status);
CREATE INDEX IX_staging_work_orders_dates ON staging.work_orders(scheduled_date, completed_date);

CREATE TABLE staging.work_order_repairs (
    repair_id BIGINT PRIMARY KEY,
    work_order_no VARCHAR(30) NOT NULL,
    repair_code VARCHAR(20),
    repair_description VARCHAR(500),
    labor_hours DECIMAL(10,2),
    labor_cost DECIMAL(15,2),
    parts_cost DECIMAL(15,2),
    total_cost DECIMAL(15,2),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (work_order_no) REFERENCES staging.work_orders(work_order_no)
);

CREATE INDEX IX_staging_wo_repairs_wo ON staging.work_order_repairs(work_order_no);

CREATE TABLE staging.work_order_parts (
    part_charge_id BIGINT PRIMARY KEY,
    work_order_no VARCHAR(30) NOT NULL,
    part_number VARCHAR(50),
    part_description VARCHAR(200),
    quantity DECIMAL(10,2),
    unit_cost DECIMAL(15,4),
    total_cost DECIMAL(15,2),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (work_order_no) REFERENCES staging.work_orders(work_order_no)
);

-- =============================================
-- Order Domain
-- =============================================

CREATE TABLE staging.orders (
    order_no VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    order_date DATE,
    order_type VARCHAR(50),
    vehicle_make VARCHAR(50),
    vehicle_model VARCHAR(50),
    vehicle_year INT,
    quantity INT DEFAULT 1,
    estimated_delivery DATE,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id)
);

CREATE INDEX IX_staging_orders_customer ON staging.orders(customer_id);
CREATE INDEX IX_staging_orders_status ON staging.orders(status);

CREATE TABLE staging.order_configurations (
    config_id BIGINT PRIMARY KEY,
    order_no VARCHAR(30) NOT NULL,
    option_code VARCHAR(20),
    option_description VARCHAR(200),
    option_price DECIMAL(15,2),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (order_no) REFERENCES staging.orders(order_no)
);

-- =============================================
-- Groups Domain
-- =============================================

CREATE TABLE staging.groups (
    group_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    group_name VARCHAR(100),
    group_type VARCHAR(50),
    description VARCHAR(500),
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (customer_id) REFERENCES staging.customers(customer_id)
);

CREATE TABLE staging.group_members (
    group_detail_id BIGINT PRIMARY KEY,
    group_id VARCHAR(20) NOT NULL,
    equipment_id VARCHAR(20) NOT NULL,
    added_date DATE,
    removed_date DATE,
    status VARCHAR(20),
    last_update DATETIME2,

    source_hash VARBINARY(32),

    FOREIGN KEY (group_id) REFERENCES staging.groups(group_id),
    FOREIGN KEY (equipment_id) REFERENCES staging.vehicles(equipment_id)
);

-- =============================================
-- Damage Domain
-- =============================================

CREATE TABLE staging.damages (
    damage_id VARCHAR(15) PRIMARY KEY,          -- DADANO
    object_number INT NOT NULL,                 -- DAOBNO
    driver_number INT,                          -- DADADR
    damage_date DATE,                           -- parsed from DADACC/YY/MM/DD
    description VARCHAR(1000),                  -- concatenated DADADS+DAD2+DAD3+DAD4+DAD5
    damage_amount DECIMAL(15,2),                -- DADAAM
    amount_recovered DECIMAL(15,2),             -- DADAAR (Accident Area Code reused as amount in staging)
    accident_location_address VARCHAR(30),      -- DADAAD
    accident_country_code VARCHAR(3),           -- DADALA
    mileage INT,                                -- DADAMI
    damage_type VARCHAR(3),                     -- DADATY
    fault_code VARCHAR(3),                      -- DADAFF
    damage_status_code VARCHAR(1),              -- DADASC
    damage_recourse VARCHAR(3),                 -- DASTCD
    total_loss_code VARCHAR(3),                 -- DATOCD
    country_code VARCHAR(3),                    -- DACOUC
    reporting_period INT,                       -- DARPPD
    insurance_co_number BIGINT,                 -- DAINCN
    third_party_name VARCHAR(35),               -- DATPNM
    repair_days INT,                            -- DAREPD
    amount_own_risk DECIMAL(15,2),              -- DADAMO
    amount_refunded DECIMAL(15,2),              -- DADAMR
    claimed_deductible_repair DECIMAL(15,2),    -- DADARP
    refunded_deductible_repair DECIMAL(15,2),   -- DARERP
    salvage_amount DECIMAL(15,2),               -- DASALD
    damage_fault_level VARCHAR(3),              -- DADAFL
    garage_name VARCHAR(35),                    -- DARSGA
    source_hash VARBINARY(32),
    last_update DATETIME2,

    FOREIGN KEY (object_number) REFERENCES staging.vehicles(equipment_id)
);

CREATE INDEX IX_staging_damages_object ON staging.damages(object_number);
CREATE INDEX IX_staging_damages_driver ON staging.damages(driver_number);
CREATE INDEX IX_staging_damages_date ON staging.damages(damage_date);

-- =============================================
-- CDC Tracking Tables
-- =============================================

CREATE TABLE staging.cdc_tracking (
    tracking_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL, -- INSERT, UPDATE, DELETE
    record_key VARCHAR(100) NOT NULL,
    changed_fields VARCHAR(MAX),
    old_values VARCHAR(MAX),
    new_values VARCHAR(MAX),
    processed_at DATETIME2 DEFAULT GETUTCDATE(),
    batch_id UNIQUEIDENTIFIER
);

CREATE INDEX IX_staging_cdc_table ON staging.cdc_tracking(table_name, processed_at);

-- =============================================
-- Domain Translations (Reference)
-- =============================================

CREATE TABLE staging.domain_translations (
    translation_key INT IDENTITY(1,1) PRIMARY KEY,
    country_code VARCHAR(10),
    domain_id INT,
    domain_value VARCHAR(50),
    language_code VARCHAR(10),
    domain_text VARCHAR(200),

    source_hash VARBINARY(32),

    CONSTRAINT UQ_staging_dt UNIQUE (country_code, domain_id, domain_value, language_code)
);

CREATE INDEX IX_staging_dt_domain ON staging.domain_translations(domain_id);

-- =============================================
-- Exploitation Services (Transactional)
-- =============================================

CREATE TABLE staging.exploitation_services (
    service_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_no INT,
    contract_position_no INT,
    object_no INT,
    service_sequence INT,
    service_code INT,
    service_cost_total DECIMAL(15,2),
    service_invoice DECIMAL(15,2),
    invoice_supplier DECIMAL(15,2),
    total_monthly_cost DECIMAL(15,2),
    total_monthly_invoice DECIMAL(15,2),
    lp_code VARCHAR(10),
    reporting_period INT,
    country_code VARCHAR(10),
    volume_code VARCHAR(10),
    distance_code VARCHAR(10),
    consumption_code VARCHAR(10),
    currency_code VARCHAR(10),

    source_hash VARBINARY(32)
);

CREATE INDEX IX_staging_es_object ON staging.exploitation_services(object_no);
CREATE INDEX IX_staging_es_customer ON staging.exploitation_services(customer_no);

-- =============================================
-- Maintenance Approvals (Transactional)
-- =============================================

CREATE TABLE staging.maintenance_approvals (
    approval_key INT IDENTITY(1,1) PRIMARY KEY,
    object_no INT NOT NULL,
    sequence INT,
    approval_date DATE,
    mileage_km DECIMAL(15,2),
    amount DECIMAL(15,2),
    description VARCHAR(200),
    description_2 VARCHAR(200),
    description_3 VARCHAR(200),
    source_code VARCHAR(10),
    maintenance_type INT,
    supplier_no INT,
    supplier_branch VARCHAR(50),
    major_code VARCHAR(10),
    minor_code VARCHAR(10),
    reporting_period INT,
    country_code VARCHAR(10),
    volume_code VARCHAR(10),
    distance_code VARCHAR(10),
    consumption_code VARCHAR(10),
    currency_code VARCHAR(10),
    si_run_no INT,
    date_from DATE,

    source_hash VARBINARY(32)
);

CREATE INDEX IX_staging_ma_object ON staging.maintenance_approvals(object_no);
CREATE INDEX IX_staging_ma_supplier ON staging.maintenance_approvals(supplier_no);
CREATE INDEX IX_staging_ma_date ON staging.maintenance_approvals(approval_date);

-- =============================================
-- Passed On Invoices (Financial)
-- =============================================

CREATE TABLE staging.passed_invoices (
    invoice_key INT IDENTITY(1,1) PRIMARY KEY,
    contract_no INT,
    customer_no INT,
    name_code VARCHAR(50),
    object_no INT,
    contract_position_no INT,
    amount DECIMAL(15,2),
    cost_code DECIMAL(15,2),
    eb_reporting_period DECIMAL(15,2),
    driver_no DECIMAL(15,2),
    description VARCHAR(200),
    gross_net VARCHAR(10),
    invoice_no DECIMAL(15,2),
    lp_code VARCHAR(10),
    object_bridge DECIMAL(15,2),
    origin_code VARCHAR(10),
    run_no DECIMAL(15,2),
    source_code VARCHAR(10),
    vat_type VARCHAR(10),
    reporting_period INT,
    country_code VARCHAR(10),

    source_hash VARBINARY(32)
);

CREATE INDEX IX_staging_pi_object ON staging.passed_invoices(object_no);
CREATE INDEX IX_staging_pi_customer ON staging.passed_invoices(customer_no);

-- =============================================
-- Replacement Cars (Transactional)
-- =============================================

CREATE TABLE staging.replacement_cars (
    rc_key INT IDENTITY(1,1) PRIMARY KEY,
    object_no INT NOT NULL,
    rc_no INT,
    sequence VARCHAR(10),
    driver_no DECIMAL(15,2),
    rc_run_no DECIMAL(15,2),
    begin_date DATE,
    end_date DATE,
    rc_code VARCHAR(10),
    km DECIMAL(15,2),
    amount DECIMAL(15,2),
    reason VARCHAR(200),
    description VARCHAR(200),
    description_2 VARCHAR(200),
    description_3 VARCHAR(200),
    type VARCHAR(10),
    driver_name VARCHAR(100),
    source_code VARCHAR(10),
    reporting_period INT,
    country_code VARCHAR(10),

    source_hash VARBINARY(32)
);

CREATE INDEX IX_staging_rc_object ON staging.replacement_cars(object_no);
CREATE INDEX IX_staging_rc_dates ON staging.replacement_cars(begin_date, end_date);

-- =============================================
-- Reporting Periods (Reference)
-- =============================================

CREATE TABLE staging.reporting_periods (
    period_key INT IDENTITY(1,1) PRIMARY KEY,
    period_cc DECIMAL(15,2),
    period_yy DECIMAL(15,2),
    period_mm DECIMAL(15,2),
    period_dd DECIMAL(15,2),
    reporting_period DECIMAL(15,2),
    month_period DECIMAL(15,2),
    reporting_date DATE,

    source_hash VARBINARY(32)
);

-- =============================================
-- Suppliers (Reference/Master)
-- =============================================

CREATE TABLE staging.suppliers (
    supplier_key INT IDENTITY(1,1) PRIMARY KEY,
    supplier_no INT,
    branch_no VARCHAR(50),
    supplier_name VARCHAR(200),
    name_line_2 VARCHAR(200),
    name_line_3 VARCHAR(200),
    class VARCHAR(10),
    country_code VARCHAR(10),
    address VARCHAR(200),
    city VARCHAR(100),
    category VARCHAR(50),
    phone VARCHAR(50),
    fax VARCHAR(50),
    email VARCHAR(100),
    contact_person VARCHAR(100),
    responsible_person VARCHAR(100),
    reporting_period DECIMAL(15,2),
    country VARCHAR(10),

    source_hash VARBINARY(32),

    CONSTRAINT UQ_staging_suppliers UNIQUE (supplier_no, branch_no)
);

CREATE INDEX IX_staging_su_name ON staging.suppliers(supplier_name);

-- =============================================
-- Car Reports (Monthly Vehicle Snapshot)
-- =============================================

CREATE TABLE staging.car_reports (
    report_key INT IDENTITY(1,1) PRIMARY KEY,
    object_no INT NOT NULL,
    reporting_period INT,
    -- Book values
    book_value_begin_amount DECIMAL(15,2),
    book_value_begin_lt DECIMAL(15,2),
    disinvestment_amount DECIMAL(15,2),
    disinvestment_lt DECIMAL(15,2),
    gain_amount DECIMAL(15,2),
    gain_lt DECIMAL(15,2),
    -- First start
    first_start_book_value DECIMAL(15,2),
    first_start_interest_rate DECIMAL(15,2),
    -- Cost totals
    fuel_cost_total DECIMAL(15,2),
    maintenance_cost_total DECIMAL(15,2),
    replacement_car_cost_total DECIMAL(15,2),
    tyre_cost_total DECIMAL(15,2),
    -- Invoice totals
    fuel_invoice_total DECIMAL(15,2),
    maintenance_invoice_total DECIMAL(15,2),
    replacement_car_invoice_total DECIMAL(15,2),
    tyre_invoice_total DECIMAL(15,2),
    -- Mileage
    first_start_km INT,
    odometer_date DATE,
    first_start_initial_km INT,
    km_driven DECIMAL(15,2),
    monthly_km_driven DECIMAL(15,2),
    km_technical DECIMAL(15,2),
    -- Fuel analysis
    fuel_cost_per_km DECIMAL(15,4),
    fuel_invoice_per_km DECIMAL(15,4),
    fuel_consumption DECIMAL(15,4),
    fuel_slope DECIMAL(15,2),
    fuel_monthly_deviation DECIMAL(15,2),
    -- Maintenance analysis
    maintenance_cost_per_km DECIMAL(15,4),
    maintenance_invoice_per_km DECIMAL(15,4),
    maintenance_slope DECIMAL(15,2),
    maintenance_monthly_deviation DECIMAL(15,2),
    -- Replacement car analysis
    replacement_car_cost_per_km DECIMAL(15,4),
    replacement_car_invoice_per_km DECIMAL(15,4),
    replacement_car_slope DECIMAL(15,2),
    replacement_car_monthly_deviation DECIMAL(15,2),
    replacement_car_km DECIMAL(15,2),
    replacement_car_amount DECIMAL(15,2),
    -- Tyre analysis
    tyre_cost_per_km DECIMAL(15,4),
    tyre_invoice_per_km DECIMAL(15,4),
    tyre_slope DECIMAL(15,2),
    tyre_monthly_deviation DECIMAL(15,2),
    -- Running totals
    total_cost DECIMAL(15,2),
    total_invoiced DECIMAL(15,2),
    cost_per_km DECIMAL(15,4),
    total_surplus DECIMAL(15,2),
    total_surplus_absolute DECIMAL(15,2),
    total_total DECIMAL(15,2),
    -- Contract KM
    last_week_km DECIMAL(15,2),
    update_km DECIMAL(15,2),
    -- Event counts
    maintenance_count INT,
    fuel_count INT,
    replacement_car_count INT,
    tyre_count INT,
    tyre_new_count INT,
    tyre_winter_count INT,
    -- Private km & damage
    private_km_pct DECIMAL(15,2),
    damage_count INT,
    damage_reserve DECIMAL(15,2),
    -- Segments
    segment_01 DECIMAL(15,2), segment_02 DECIMAL(15,2), segment_03 DECIMAL(15,2),
    segment_04 DECIMAL(15,2), segment_05 DECIMAL(15,2), segment_06 DECIMAL(15,2),
    segment_07 DECIMAL(15,2), segment_08 DECIMAL(15,2), segment_09 DECIMAL(15,2),
    segment_10 DECIMAL(15,2), segment_11 DECIMAL(15,2), segment_12 DECIMAL(15,2),
    segment_13 DECIMAL(15,2), segment_14 DECIMAL(15,2), segment_15 DECIMAL(15,2),
    -- Metadata
    country_code VARCHAR(10),
    volume_code VARCHAR(10),
    distance_code VARCHAR(10),
    consumption_code VARCHAR(10),
    currency_code VARCHAR(10),
    -- Miscellaneous
    misc_insurance_amount DECIMAL(15,2),
    misc_insurance_peryear DECIMAL(15,2),
    misc_insurance_run_no DECIMAL(15,2),
    misc_ts_amount DECIMAL(15,2),
    misc_ts_peryear DECIMAL(15,2),
    misc_ts_run_no DECIMAL(15,2),
    traffic_fines_no INT,
    -- Parking
    parking_cost_total DECIMAL(15,2),
    parking_invoice_total DECIMAL(15,2),
    parking_monthly_deviation DECIMAL(15,2),
    parking_slope DECIMAL(15,2),
    -- Unspecified
    unspecified_cost_total DECIMAL(15,2),
    unspecified_invoice_total DECIMAL(15,2),
    unspecified_monthly_deviation DECIMAL(15,2),
    unspecified_slope DECIMAL(15,2),
    -- Warranty
    warranty_cost_total DECIMAL(15,2),
    warranty_invoice_total DECIMAL(15,2),
    warranty_monthly_deviation DECIMAL(15,2),
    warranty_slope DECIMAL(15,2),

    source_hash VARBINARY(32)
);

CREATE INDEX IX_staging_cr_object ON staging.car_reports(object_no);
CREATE INDEX IX_staging_cr_period ON staging.car_reports(reporting_period);
CREATE INDEX IX_staging_cr_object_period ON staging.car_reports(object_no, reporting_period);

GO
