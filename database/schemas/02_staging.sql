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

GO
