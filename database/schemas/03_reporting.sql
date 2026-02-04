-- =============================================
-- FleetAI Reporting Schema
-- Dimensional model optimized for analytics
-- =============================================

CREATE SCHEMA IF NOT EXISTS reporting;
GO

-- =============================================
-- DIMENSION TABLES
-- =============================================

-- Date Dimension (pre-populated)
CREATE TABLE reporting.dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD format
    full_date DATE NOT NULL,
    day_of_week TINYINT,
    day_name VARCHAR(10),
    day_of_month TINYINT,
    day_of_year SMALLINT,
    week_of_year TINYINT,
    week_of_month TINYINT,
    month_number TINYINT,
    month_name VARCHAR(10),
    month_short VARCHAR(3),
    quarter_number TINYINT,
    quarter_name VARCHAR(6),
    year_number SMALLINT,
    fiscal_year SMALLINT,
    fiscal_quarter TINYINT,
    is_weekend BIT,
    is_holiday BIT,
    holiday_name VARCHAR(50),
    is_business_day BIT
);

CREATE INDEX IX_dim_date_full ON reporting.dim_date(full_date);

-- Customer Dimension
CREATE TABLE reporting.dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    legal_name VARCHAR(200),
    account_type VARCHAR(50),
    parent_customer_id VARCHAR(20),
    parent_customer_name VARCHAR(200),
    tax_id VARCHAR(30),
    industry VARCHAR(100),
    employee_count_tier VARCHAR(20), -- 'Small', 'Medium', 'Large', 'Enterprise'
    credit_rating VARCHAR(20),
    payment_terms VARCHAR(50),
    account_manager VARCHAR(50),
    region VARCHAR(50),
    territory VARCHAR(50),
    billing_city VARCHAR(100),
    billing_state VARCHAR(50),
    billing_country VARCHAR(50),
    status VARCHAR(20),
    created_date DATE,

    -- SCD Type 2 tracking
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BIT DEFAULT 1,

    -- Vector column for AI similarity search (MSSQL 2024+)
    -- description_vector VECTOR(1536),

    CONSTRAINT UQ_dim_customer_id_effective UNIQUE (customer_id, effective_from)
);

CREATE INDEX IX_dim_customer_id ON reporting.dim_customer(customer_id);
CREATE INDEX IX_dim_customer_current ON reporting.dim_customer(is_current) WHERE is_current = 1;
CREATE INDEX IX_dim_customer_manager ON reporting.dim_customer(account_manager);

-- Vehicle Dimension
CREATE TABLE reporting.dim_vehicle (
    vehicle_key INT IDENTITY(1,1) PRIMARY KEY,
    equipment_id VARCHAR(20) NOT NULL,
    vin VARCHAR(17),
    license_plate VARCHAR(20),
    make VARCHAR(50),
    model VARCHAR(50),
    model_year INT,
    make_model AS CONCAT(make, ' ', model) PERSISTED,
    color VARCHAR(30),
    body_type VARCHAR(30),
    engine_type VARCHAR(30),
    fuel_type VARCHAR(20),
    transmission VARCHAR(20),
    vehicle_age_years AS (DATEDIFF(YEAR, DATEFROMPARTS(model_year, 1, 1), GETDATE())),
    acquisition_date DATE,
    acquisition_cost DECIMAL(15,2),
    residual_value DECIMAL(15,2),
    status VARCHAR(20),

    -- SCD Type 2
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BIT DEFAULT 1,

    -- Vector for AI similarity
    -- description_vector VECTOR(1536),

    CONSTRAINT UQ_dim_vehicle_id_effective UNIQUE (equipment_id, effective_from)
);

CREATE INDEX IX_dim_vehicle_id ON reporting.dim_vehicle(equipment_id);
CREATE INDEX IX_dim_vehicle_current ON reporting.dim_vehicle(is_current) WHERE is_current = 1;
CREATE INDEX IX_dim_vehicle_make ON reporting.dim_vehicle(make, model);
CREATE INDEX IX_dim_vehicle_status ON reporting.dim_vehicle(status);

-- Driver Dimension
CREATE TABLE reporting.dim_driver (
    driver_key INT IDENTITY(1,1) PRIMARY KEY,
    driver_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(101),
    email VARCHAR(100),
    department VARCHAR(100),
    cost_center VARCHAR(50),
    license_state VARCHAR(10),
    license_expiry_date DATE,
    days_until_license_expiry AS (DATEDIFF(DAY, GETDATE(), license_expiry_date)),
    status VARCHAR(20),

    -- SCD Type 2
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BIT DEFAULT 1,

    CONSTRAINT UQ_dim_driver_id_effective UNIQUE (driver_id, effective_from)
);

CREATE INDEX IX_dim_driver_id ON reporting.dim_driver(driver_id);
CREATE INDEX IX_dim_driver_current ON reporting.dim_driver(is_current) WHERE is_current = 1;
CREATE INDEX IX_dim_driver_customer ON reporting.dim_driver(customer_id);

-- Contract Dimension
CREATE TABLE reporting.dim_contract (
    contract_key INT IDENTITY(1,1) PRIMARY KEY,
    contract_no VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20),
    contract_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    term_months INT,
    monthly_rate DECIMAL(15,2),
    total_contract_value AS (monthly_rate * term_months) PERSISTED,
    mileage_allowance INT,
    annual_mileage_allowance AS (mileage_allowance * 12),
    excess_mileage_rate DECIMAL(10,4),
    insurance_included BIT,
    maintenance_included BIT,
    days_remaining AS (DATEDIFF(DAY, GETDATE(), end_date)),
    status VARCHAR(20),

    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BIT DEFAULT 1,

    CONSTRAINT UQ_dim_contract_id_effective UNIQUE (contract_no, effective_from)
);

CREATE INDEX IX_dim_contract_id ON reporting.dim_contract(contract_no);
CREATE INDEX IX_dim_contract_current ON reporting.dim_contract(is_current) WHERE is_current = 1;
CREATE INDEX IX_dim_contract_customer ON reporting.dim_contract(customer_id);
CREATE INDEX IX_dim_contract_dates ON reporting.dim_contract(start_date, end_date);

-- Location/Geography Dimension
CREATE TABLE reporting.dim_location (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(50),
    state_code VARCHAR(10),
    country VARCHAR(50) DEFAULT 'USA',
    region VARCHAR(50),
    timezone VARCHAR(50),

    CONSTRAINT UQ_dim_location UNIQUE (city, state, country)
);

-- Vendor Dimension
CREATE TABLE reporting.dim_vendor (
    vendor_key INT IDENTITY(1,1) PRIMARY KEY,
    vendor_id VARCHAR(20) NOT NULL,
    vendor_name VARCHAR(200),
    vendor_type VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    status VARCHAR(20),
    is_current BIT DEFAULT 1,

    CONSTRAINT UQ_dim_vendor_id UNIQUE (vendor_id)
);

-- Time of Day Dimension (for detailed transaction analysis)
CREATE TABLE reporting.dim_time (
    time_key INT PRIMARY KEY, -- HHMM format
    hour_24 TINYINT,
    hour_12 TINYINT,
    minute TINYINT,
    am_pm VARCHAR(2),
    time_period VARCHAR(20), -- 'Morning', 'Afternoon', 'Evening', 'Night'
    is_business_hours BIT
);

-- =============================================
-- FACT TABLES
-- =============================================

-- Contract Facts (snapshot)
CREATE TABLE reporting.fact_contracts (
    contract_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key INT NOT NULL,
    contract_key INT NOT NULL,
    vehicle_key INT,

    -- Measures
    monthly_rate DECIMAL(15,2),
    contract_value DECIMAL(15,2),
    mileage_allowance INT,
    vehicles_on_contract INT DEFAULT 1,

    -- Snapshot flags
    is_active BIT,
    days_to_expiry INT,
    contract_age_days INT,

    CONSTRAINT FK_fact_contracts_date FOREIGN KEY (date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_contracts_customer FOREIGN KEY (customer_key) REFERENCES reporting.dim_customer(customer_key),
    CONSTRAINT FK_fact_contracts_contract FOREIGN KEY (contract_key) REFERENCES reporting.dim_contract(contract_key),
    CONSTRAINT FK_fact_contracts_vehicle FOREIGN KEY (vehicle_key) REFERENCES reporting.dim_vehicle(vehicle_key)
);

CREATE INDEX IX_fact_contracts_date ON reporting.fact_contracts(date_key);
CREATE INDEX IX_fact_contracts_customer ON reporting.fact_contracts(customer_key);
CREATE INDEX IX_fact_contracts_contract ON reporting.fact_contracts(contract_key);

-- Vehicle Fleet Facts (daily snapshot)
CREATE TABLE reporting.fact_vehicles (
    vehicle_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key INT,
    vehicle_key INT NOT NULL,
    driver_key INT,
    contract_key INT,

    -- Measures
    odometer_reading INT,
    daily_miles INT,
    mtd_miles INT,
    ytd_miles INT,

    -- Status flags
    is_assigned BIT,
    is_active BIT,
    days_in_service INT,

    CONSTRAINT FK_fact_vehicles_date FOREIGN KEY (date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_vehicles_customer FOREIGN KEY (customer_key) REFERENCES reporting.dim_customer(customer_key),
    CONSTRAINT FK_fact_vehicles_vehicle FOREIGN KEY (vehicle_key) REFERENCES reporting.dim_vehicle(vehicle_key),
    CONSTRAINT FK_fact_vehicles_driver FOREIGN KEY (driver_key) REFERENCES reporting.dim_driver(driver_key),
    CONSTRAINT FK_fact_vehicles_contract FOREIGN KEY (contract_key) REFERENCES reporting.dim_contract(contract_key)
);

CREATE INDEX IX_fact_vehicles_date ON reporting.fact_vehicles(date_key);
CREATE INDEX IX_fact_vehicles_vehicle ON reporting.fact_vehicles(vehicle_key);
CREATE INDEX IX_fact_vehicles_customer ON reporting.fact_vehicles(customer_key);

-- Invoice Facts (transactional)
CREATE TABLE reporting.fact_invoices (
    invoice_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    invoice_date_key INT NOT NULL,
    due_date_key INT,
    customer_key INT NOT NULL,
    contract_key INT,

    invoice_no VARCHAR(30) NOT NULL,

    -- Measures
    subtotal DECIMAL(15,2),
    tax_amount DECIMAL(15,2),
    total_amount DECIMAL(15,2),
    paid_amount DECIMAL(15,2),
    balance_amount DECIMAL(15,2),
    line_item_count INT,

    -- Aging
    days_outstanding INT,
    is_overdue BIT,

    CONSTRAINT FK_fact_invoices_invoice_date FOREIGN KEY (invoice_date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_invoices_due_date FOREIGN KEY (due_date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_invoices_customer FOREIGN KEY (customer_key) REFERENCES reporting.dim_customer(customer_key),
    CONSTRAINT FK_fact_invoices_contract FOREIGN KEY (contract_key) REFERENCES reporting.dim_contract(contract_key)
);

CREATE INDEX IX_fact_invoices_date ON reporting.fact_invoices(invoice_date_key);
CREATE INDEX IX_fact_invoices_customer ON reporting.fact_invoices(customer_key);

-- Fuel Consumption Facts (transactional)
CREATE TABLE reporting.fact_fuel (
    fuel_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_date_key INT NOT NULL,
    customer_key INT,
    vehicle_key INT NOT NULL,
    driver_key INT,
    location_key INT,

    -- Measures
    gallons DECIMAL(15,3),
    amount DECIMAL(15,2),
    price_per_gallon DECIMAL(10,4),
    odometer INT,
    miles_since_last_fill INT,
    mpg DECIMAL(10,2),

    -- Product
    fuel_type VARCHAR(20),
    product_code VARCHAR(20),

    CONSTRAINT FK_fact_fuel_date FOREIGN KEY (transaction_date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_fuel_customer FOREIGN KEY (customer_key) REFERENCES reporting.dim_customer(customer_key),
    CONSTRAINT FK_fact_fuel_vehicle FOREIGN KEY (vehicle_key) REFERENCES reporting.dim_vehicle(vehicle_key),
    CONSTRAINT FK_fact_fuel_driver FOREIGN KEY (driver_key) REFERENCES reporting.dim_driver(driver_key),
    CONSTRAINT FK_fact_fuel_location FOREIGN KEY (location_key) REFERENCES reporting.dim_location(location_key)
);

CREATE INDEX IX_fact_fuel_date ON reporting.fact_fuel(transaction_date_key);
CREATE INDEX IX_fact_fuel_vehicle ON reporting.fact_fuel(vehicle_key);
CREATE INDEX IX_fact_fuel_customer ON reporting.fact_fuel(customer_key);

-- Maintenance Facts (transactional)
CREATE TABLE reporting.fact_maintenance (
    maintenance_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    completed_date_key INT,
    scheduled_date_key INT,
    customer_key INT,
    vehicle_key INT NOT NULL,
    vendor_key INT,

    work_order_no VARCHAR(30) NOT NULL,
    order_type VARCHAR(50),
    priority VARCHAR(20),

    -- Measures
    labor_hours DECIMAL(10,2),
    labor_cost DECIMAL(15,2),
    parts_cost DECIMAL(15,2),
    total_cost DECIMAL(15,2),
    estimated_cost DECIMAL(15,2),
    cost_variance AS (total_cost - estimated_cost) PERSISTED,

    -- Timing
    days_to_complete INT,
    is_scheduled BIT,
    is_completed BIT,

    CONSTRAINT FK_fact_maint_completed_date FOREIGN KEY (completed_date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_maint_scheduled_date FOREIGN KEY (scheduled_date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_maint_customer FOREIGN KEY (customer_key) REFERENCES reporting.dim_customer(customer_key),
    CONSTRAINT FK_fact_maint_vehicle FOREIGN KEY (vehicle_key) REFERENCES reporting.dim_vehicle(vehicle_key),
    CONSTRAINT FK_fact_maint_vendor FOREIGN KEY (vendor_key) REFERENCES reporting.dim_vendor(vendor_key)
);

CREATE INDEX IX_fact_maintenance_date ON reporting.fact_maintenance(completed_date_key);
CREATE INDEX IX_fact_maintenance_vehicle ON reporting.fact_maintenance(vehicle_key);
CREATE INDEX IX_fact_maintenance_customer ON reporting.fact_maintenance(customer_key);

-- =============================================
-- AGGREGATE TABLES (for performance)
-- =============================================

-- Monthly Customer Summary
CREATE TABLE reporting.agg_customer_monthly (
    agg_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month INT NOT NULL, -- YYYYMM
    customer_key INT NOT NULL,

    -- Fleet metrics
    total_vehicles INT,
    active_vehicles INT,
    total_drivers INT,

    -- Financial metrics
    total_invoiced DECIMAL(18,2),
    total_paid DECIMAL(18,2),
    total_outstanding DECIMAL(18,2),

    -- Contract metrics
    active_contracts INT,
    contracts_expiring_30_days INT,
    total_monthly_rate DECIMAL(18,2),

    -- Fuel metrics
    total_fuel_gallons DECIMAL(18,3),
    total_fuel_cost DECIMAL(18,2),
    avg_mpg DECIMAL(10,2),

    -- Maintenance metrics
    maintenance_events INT,
    total_maintenance_cost DECIMAL(18,2),

    -- Calculated
    last_updated DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT UQ_agg_customer_monthly UNIQUE (year_month, customer_key)
);

CREATE INDEX IX_agg_customer_monthly_ym ON reporting.agg_customer_monthly(year_month);
CREATE INDEX IX_agg_customer_monthly_customer ON reporting.agg_customer_monthly(customer_key);

-- Monthly Vehicle Summary
CREATE TABLE reporting.agg_vehicle_monthly (
    agg_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month INT NOT NULL,
    vehicle_key INT NOT NULL,

    -- Mileage
    starting_odometer INT,
    ending_odometer INT,
    total_miles INT,

    -- Fuel
    fuel_transactions INT,
    total_gallons DECIMAL(15,3),
    total_fuel_cost DECIMAL(15,2),
    avg_mpg DECIMAL(10,2),

    -- Maintenance
    maintenance_events INT,
    total_maintenance_cost DECIMAL(15,2),

    -- Utilization
    days_in_use INT,
    utilization_pct DECIMAL(5,2),

    last_updated DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT UQ_agg_vehicle_monthly UNIQUE (year_month, vehicle_key)
);

-- =============================================
-- VIEWS FOR COMMON QUERIES
-- =============================================

GO

-- Current Fleet Status View
CREATE OR ALTER VIEW reporting.vw_fleet_status AS
SELECT
    c.customer_id,
    c.customer_name,
    c.account_manager,
    v.equipment_id,
    v.vin,
    v.make_model,
    v.model_year,
    v.license_plate,
    v.fuel_type,
    v.status AS vehicle_status,
    d.full_name AS driver_name,
    d.email AS driver_email,
    ct.contract_no,
    ct.end_date AS contract_end,
    ct.days_remaining AS contract_days_remaining,
    ct.monthly_rate
FROM reporting.dim_vehicle v
LEFT JOIN reporting.dim_customer c ON c.is_current = 1
LEFT JOIN reporting.dim_driver d ON d.is_current = 1
LEFT JOIN reporting.dim_contract ct ON ct.is_current = 1
WHERE v.is_current = 1;

GO

-- Contract Expiration View
CREATE OR ALTER VIEW reporting.vw_contracts_expiring AS
SELECT
    ct.contract_no,
    ct.customer_id,
    c.customer_name,
    c.account_manager,
    ct.start_date,
    ct.end_date,
    ct.days_remaining,
    ct.monthly_rate,
    ct.total_contract_value,
    ct.status,
    CASE
        WHEN ct.days_remaining <= 30 THEN 'Critical'
        WHEN ct.days_remaining <= 60 THEN 'Warning'
        WHEN ct.days_remaining <= 90 THEN 'Upcoming'
        ELSE 'OK'
    END AS expiry_urgency
FROM reporting.dim_contract ct
JOIN reporting.dim_customer c ON ct.customer_id = c.customer_id AND c.is_current = 1
WHERE ct.is_current = 1
  AND ct.status = 'active'
  AND ct.days_remaining <= 90;

GO

-- Fuel Consumption Trends View
CREATE OR ALTER VIEW reporting.vw_fuel_trends AS
SELECT
    dd.year_number,
    dd.month_number,
    dd.month_name,
    c.customer_id,
    c.customer_name,
    v.make,
    v.model,
    v.fuel_type,
    SUM(ff.gallons) AS total_gallons,
    SUM(ff.amount) AS total_cost,
    AVG(ff.price_per_gallon) AS avg_price,
    AVG(ff.mpg) AS avg_mpg,
    COUNT(*) AS transaction_count
FROM reporting.fact_fuel ff
JOIN reporting.dim_date dd ON ff.transaction_date_key = dd.date_key
JOIN reporting.dim_customer c ON ff.customer_key = c.customer_key
JOIN reporting.dim_vehicle v ON ff.vehicle_key = v.vehicle_key
GROUP BY
    dd.year_number,
    dd.month_number,
    dd.month_name,
    c.customer_id,
    c.customer_name,
    v.make,
    v.model,
    v.fuel_type;

GO

-- Invoice Aging View
CREATE OR ALTER VIEW reporting.vw_invoice_aging AS
SELECT
    c.customer_id,
    c.customer_name,
    c.account_manager,
    fi.invoice_no,
    dd.full_date AS invoice_date,
    fi.total_amount,
    fi.paid_amount,
    fi.balance_amount,
    fi.days_outstanding,
    CASE
        WHEN fi.balance_amount <= 0 THEN 'Paid'
        WHEN fi.days_outstanding <= 30 THEN 'Current'
        WHEN fi.days_outstanding <= 60 THEN '31-60 Days'
        WHEN fi.days_outstanding <= 90 THEN '61-90 Days'
        ELSE '90+ Days'
    END AS aging_bucket
FROM reporting.fact_invoices fi
JOIN reporting.dim_date dd ON fi.invoice_date_key = dd.date_key
JOIN reporting.dim_customer c ON fi.customer_key = c.customer_key;

GO

-- Maintenance Summary View
CREATE OR ALTER VIEW reporting.vw_maintenance_summary AS
SELECT
    v.equipment_id,
    v.make_model,
    v.model_year,
    c.customer_name,
    fm.work_order_no,
    fm.order_type,
    fm.priority,
    dd.full_date AS completed_date,
    fm.labor_cost,
    fm.parts_cost,
    fm.total_cost,
    fm.cost_variance,
    vd.vendor_name
FROM reporting.fact_maintenance fm
JOIN reporting.dim_vehicle v ON fm.vehicle_key = v.vehicle_key
LEFT JOIN reporting.dim_customer c ON fm.customer_key = c.customer_key
LEFT JOIN reporting.dim_date dd ON fm.completed_date_key = dd.date_key
LEFT JOIN reporting.dim_vendor vd ON fm.vendor_key = vd.vendor_key;

GO

-- =============================================
-- ADDITIONAL DIMENSION TABLES
-- =============================================

-- Supplier Dimension (SCD Type 2)
CREATE TABLE reporting.dim_supplier (
    supplier_key INT IDENTITY(1,1) PRIMARY KEY,
    supplier_no INT NOT NULL,
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

    -- SCD Type 2 tracking
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BIT DEFAULT 1
);

CREATE INDEX IX_dim_supplier_no ON reporting.dim_supplier(supplier_no);
CREATE INDEX IX_dim_supplier_current ON reporting.dim_supplier(is_current) WHERE is_current = 1;
CREATE INDEX IX_dim_supplier_name ON reporting.dim_supplier(supplier_name);

-- =============================================
-- ADDITIONAL FACT TABLES
-- =============================================

-- Maintenance Approvals Facts (transactional)
CREATE TABLE reporting.fact_maintenance_approvals (
    approval_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT,
    vehicle_key INT,
    supplier_key INT,
    object_no INT NOT NULL,
    approval_date DATE,
    mileage_km DECIMAL(15,2),
    amount DECIMAL(15,2),
    description VARCHAR(600),
    maintenance_type INT,
    major_code VARCHAR(10),
    minor_code VARCHAR(10),
    source_code VARCHAR(10),
    reporting_period INT,

    CONSTRAINT FK_fact_ma_date FOREIGN KEY (date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_ma_vehicle FOREIGN KEY (vehicle_key) REFERENCES reporting.dim_vehicle(vehicle_key),
    CONSTRAINT FK_fact_ma_supplier FOREIGN KEY (supplier_key) REFERENCES reporting.dim_supplier(supplier_key)
);

CREATE INDEX IX_fact_ma_date ON reporting.fact_maintenance_approvals(date_key);
CREATE INDEX IX_fact_ma_vehicle ON reporting.fact_maintenance_approvals(vehicle_key);
CREATE INDEX IX_fact_ma_object ON reporting.fact_maintenance_approvals(object_no);
CREATE INDEX IX_fact_ma_supplier ON reporting.fact_maintenance_approvals(supplier_key);

-- Replacement Cars Facts (transactional)
CREATE TABLE reporting.fact_replacement_cars (
    rc_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    begin_date_key INT,
    end_date_key INT,
    vehicle_key INT,
    object_no INT NOT NULL,
    rc_no INT,
    begin_date DATE,
    end_date DATE,
    km DECIMAL(15,2),
    amount DECIMAL(15,2),
    reason VARCHAR(200),
    rc_type VARCHAR(10),
    reporting_period INT,

    CONSTRAINT FK_fact_rc_begin_date FOREIGN KEY (begin_date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_rc_end_date FOREIGN KEY (end_date_key) REFERENCES reporting.dim_date(date_key),
    CONSTRAINT FK_fact_rc_vehicle FOREIGN KEY (vehicle_key) REFERENCES reporting.dim_vehicle(vehicle_key)
);

CREATE INDEX IX_fact_rc_begin_date ON reporting.fact_replacement_cars(begin_date_key);
CREATE INDEX IX_fact_rc_end_date ON reporting.fact_replacement_cars(end_date_key);
CREATE INDEX IX_fact_rc_vehicle ON reporting.fact_replacement_cars(vehicle_key);
CREATE INDEX IX_fact_rc_object ON reporting.fact_replacement_cars(object_no);

-- Car Reports Facts (periodic snapshot)
CREATE TABLE reporting.fact_car_reports (
    car_report_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    vehicle_key INT,
    object_no INT NOT NULL,
    reporting_period INT,
    fuel_cost_total DECIMAL(15,2),
    maintenance_cost_total DECIMAL(15,2),
    replacement_car_cost_total DECIMAL(15,2),
    tyre_cost_total DECIMAL(15,2),
    total_cost DECIMAL(15,2),
    total_invoiced DECIMAL(15,2),
    km_driven DECIMAL(15,2),
    cost_per_km DECIMAL(15,4),
    fuel_consumption DECIMAL(15,4),
    maintenance_count INT,
    fuel_count INT,
    damage_count INT,
    country_code VARCHAR(10),

    CONSTRAINT FK_fact_cr_vehicle FOREIGN KEY (vehicle_key) REFERENCES reporting.dim_vehicle(vehicle_key)
);

CREATE INDEX IX_fact_cr_vehicle ON reporting.fact_car_reports(vehicle_key);
CREATE INDEX IX_fact_cr_object ON reporting.fact_car_reports(object_no);
CREATE INDEX IX_fact_cr_period ON reporting.fact_car_reports(reporting_period);
CREATE INDEX IX_fact_cr_object_period ON reporting.fact_car_reports(object_no, reporting_period);

GO

-- =============================================
-- STORED PROCEDURES FOR ETL
-- =============================================

-- Populate Date Dimension
CREATE OR ALTER PROCEDURE reporting.sp_populate_dim_date
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @current_date DATE = @start_date;

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO reporting.dim_date (
            date_key,
            full_date,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            week_of_month,
            month_number,
            month_name,
            month_short,
            quarter_number,
            quarter_name,
            year_number,
            fiscal_year,
            fiscal_quarter,
            is_weekend,
            is_holiday,
            is_business_day
        )
        SELECT
            CONVERT(INT, FORMAT(@current_date, 'yyyyMMdd')),
            @current_date,
            DATEPART(WEEKDAY, @current_date),
            DATENAME(WEEKDAY, @current_date),
            DAY(@current_date),
            DATEPART(DAYOFYEAR, @current_date),
            DATEPART(WEEK, @current_date),
            (DAY(@current_date) - 1) / 7 + 1,
            MONTH(@current_date),
            DATENAME(MONTH, @current_date),
            LEFT(DATENAME(MONTH, @current_date), 3),
            DATEPART(QUARTER, @current_date),
            'Q' + CAST(DATEPART(QUARTER, @current_date) AS VARCHAR),
            YEAR(@current_date),
            CASE WHEN MONTH(@current_date) >= 7 THEN YEAR(@current_date) + 1 ELSE YEAR(@current_date) END,
            CASE
                WHEN MONTH(@current_date) BETWEEN 7 AND 9 THEN 1
                WHEN MONTH(@current_date) BETWEEN 10 AND 12 THEN 2
                WHEN MONTH(@current_date) BETWEEN 1 AND 3 THEN 3
                ELSE 4
            END,
            CASE WHEN DATEPART(WEEKDAY, @current_date) IN (1, 7) THEN 1 ELSE 0 END,
            0, -- Holiday flag needs manual update
            CASE WHEN DATEPART(WEEKDAY, @current_date) NOT IN (1, 7) THEN 1 ELSE 0 END
        WHERE NOT EXISTS (
            SELECT 1 FROM reporting.dim_date WHERE date_key = CONVERT(INT, FORMAT(@current_date, 'yyyyMMdd'))
        );

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END
END;

GO

-- Refresh Monthly Aggregates
CREATE OR ALTER PROCEDURE reporting.sp_refresh_monthly_aggregates
    @year_month INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Delete existing aggregates for this month
    DELETE FROM reporting.agg_customer_monthly WHERE year_month = @year_month;

    -- Insert new aggregates
    INSERT INTO reporting.agg_customer_monthly (
        year_month,
        customer_key,
        total_vehicles,
        active_vehicles,
        total_invoiced,
        total_paid,
        total_outstanding,
        total_fuel_gallons,
        total_fuel_cost,
        maintenance_events,
        total_maintenance_cost
    )
    SELECT
        @year_month,
        c.customer_key,
        COUNT(DISTINCT fv.vehicle_key) AS total_vehicles,
        COUNT(DISTINCT CASE WHEN fv.is_active = 1 THEN fv.vehicle_key END) AS active_vehicles,
        COALESCE(SUM(fi.total_amount), 0) AS total_invoiced,
        COALESCE(SUM(fi.paid_amount), 0) AS total_paid,
        COALESCE(SUM(fi.balance_amount), 0) AS total_outstanding,
        COALESCE(SUM(ff.gallons), 0) AS total_fuel_gallons,
        COALESCE(SUM(ff.amount), 0) AS total_fuel_cost,
        COUNT(DISTINCT fm.work_order_no) AS maintenance_events,
        COALESCE(SUM(fm.total_cost), 0) AS total_maintenance_cost
    FROM reporting.dim_customer c
    LEFT JOIN reporting.fact_vehicles fv ON c.customer_key = fv.customer_key
        AND fv.date_key / 100 = @year_month
    LEFT JOIN reporting.fact_invoices fi ON c.customer_key = fi.customer_key
        AND fi.invoice_date_key / 100 = @year_month
    LEFT JOIN reporting.fact_fuel ff ON c.customer_key = ff.customer_key
        AND ff.transaction_date_key / 100 = @year_month
    LEFT JOIN reporting.fact_maintenance fm ON c.customer_key = fm.customer_key
        AND fm.completed_date_key / 100 = @year_month
    WHERE c.is_current = 1
    GROUP BY c.customer_key;
END;

GO
