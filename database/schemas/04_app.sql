-- =============================================
-- FleetAI Application Schema
-- User management, dashboards, reports, AI
-- =============================================

CREATE SCHEMA IF NOT EXISTS app;
GO

-- =============================================
-- USER & AUTHENTICATION
-- =============================================

-- Roles lookup table
CREATE TABLE app.roles (
    role_id INT IDENTITY(1,1) PRIMARY KEY,
    role_name VARCHAR(50) NOT NULL UNIQUE,
    role_description VARCHAR(500),
    role_level INT NOT NULL, -- Higher = more permissions
    created_at DATETIME2 DEFAULT GETUTCDATE()
);

-- Pre-populate roles
INSERT INTO app.roles (role_name, role_description, role_level) VALUES
('driver', 'View own vehicle, fuel, maintenance data', 10),
('fleet_admin', 'View/manage all vehicles for their company', 20),
('client_contact', 'View company-wide fleet data', 30),
('account_manager', 'View assigned client data, create reports', 40),
('super_user', 'Full read access to all data', 50),
('admin', 'Full system access including user management', 100);

-- Users table (synced from Azure AD)
CREATE TABLE app.users (
    user_id INT IDENTITY(1,1) PRIMARY KEY,
    azure_ad_id VARCHAR(100) NOT NULL UNIQUE, -- Azure AD Object ID
    email VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(200),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role_id INT NOT NULL,
    is_active BIT DEFAULT 1,
    last_login DATETIME2,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_users_role FOREIGN KEY (role_id) REFERENCES app.roles(role_id)
);

CREATE INDEX IX_users_azure_ad ON app.users(azure_ad_id);
CREATE INDEX IX_users_email ON app.users(email);
CREATE INDEX IX_users_role ON app.users(role_id);

-- User-Customer access mapping (for row-level security)
CREATE TABLE app.user_customer_access (
    access_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    access_level VARCHAR(20) DEFAULT 'read', -- 'read', 'write', 'admin'
    granted_at DATETIME2 DEFAULT GETUTCDATE(),
    granted_by INT,

    CONSTRAINT FK_uca_user FOREIGN KEY (user_id) REFERENCES app.users(user_id),
    CONSTRAINT FK_uca_granted_by FOREIGN KEY (granted_by) REFERENCES app.users(user_id),
    CONSTRAINT UQ_uca_user_customer UNIQUE (user_id, customer_id)
);

CREATE INDEX IX_uca_user ON app.user_customer_access(user_id);
CREATE INDEX IX_uca_customer ON app.user_customer_access(customer_id);

-- User-Driver linking (for driver role)
CREATE TABLE app.user_driver_link (
    link_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL UNIQUE,
    driver_id VARCHAR(20) NOT NULL UNIQUE,
    linked_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_udl_user FOREIGN KEY (user_id) REFERENCES app.users(user_id)
);

-- Permissions table
CREATE TABLE app.permissions (
    permission_id INT IDENTITY(1,1) PRIMARY KEY,
    permission_name VARCHAR(100) NOT NULL UNIQUE,
    permission_category VARCHAR(50),
    description VARCHAR(500)
);

-- Role-Permission mapping
CREATE TABLE app.role_permissions (
    role_id INT NOT NULL,
    permission_id INT NOT NULL,

    PRIMARY KEY (role_id, permission_id),
    CONSTRAINT FK_rp_role FOREIGN KEY (role_id) REFERENCES app.roles(role_id),
    CONSTRAINT FK_rp_permission FOREIGN KEY (permission_id) REFERENCES app.permissions(permission_id)
);

-- Pre-populate permissions
INSERT INTO app.permissions (permission_name, permission_category, description) VALUES
-- Dashboard permissions
('dashboard:view', 'dashboard', 'View dashboards'),
('dashboard:create', 'dashboard', 'Create new dashboards'),
('dashboard:edit', 'dashboard', 'Edit existing dashboards'),
('dashboard:delete', 'dashboard', 'Delete dashboards'),
('dashboard:share', 'dashboard', 'Share dashboards with others'),
-- Report permissions
('report:view', 'report', 'View reports'),
('report:create', 'report', 'Create new reports'),
('report:edit', 'report', 'Edit existing reports'),
('report:delete', 'report', 'Delete reports'),
('report:export', 'report', 'Export reports to files'),
('report:schedule', 'report', 'Schedule report delivery'),
-- Data permissions
('data:view_own', 'data', 'View own data only'),
('data:view_company', 'data', 'View company-wide data'),
('data:view_assigned', 'data', 'View assigned clients data'),
('data:view_all', 'data', 'View all data'),
-- Admin permissions
('admin:users', 'admin', 'Manage users'),
('admin:roles', 'admin', 'Manage roles'),
('admin:system', 'admin', 'System configuration'),
-- AI permissions
('ai:assistant', 'ai', 'Use AI assistant'),
('ai:analytics', 'ai', 'Use AI analytics features');

-- Assign permissions to roles
INSERT INTO app.role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM app.roles r
CROSS JOIN app.permissions p
WHERE
    (r.role_name = 'driver' AND p.permission_name IN ('dashboard:view', 'report:view', 'data:view_own'))
    OR (r.role_name = 'fleet_admin' AND p.permission_name IN ('dashboard:view', 'dashboard:create', 'report:view', 'report:create', 'report:export', 'data:view_company', 'ai:assistant'))
    OR (r.role_name = 'client_contact' AND p.permission_name IN ('dashboard:view', 'report:view', 'report:export', 'data:view_company', 'ai:assistant'))
    OR (r.role_name = 'account_manager' AND p.permission_name IN ('dashboard:view', 'dashboard:create', 'dashboard:share', 'report:view', 'report:create', 'report:export', 'report:schedule', 'data:view_assigned', 'ai:assistant', 'ai:analytics'))
    OR (r.role_name = 'super_user' AND p.permission_name NOT LIKE 'admin:%')
    OR (r.role_name = 'admin');

-- =============================================
-- DASHBOARDS
-- =============================================

CREATE TABLE app.dashboards (
    dashboard_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    name VARCHAR(200) NOT NULL,
    description VARCHAR(1000),
    layout_type VARCHAR(20) DEFAULT 'grid', -- 'grid', 'freeform'
    config NVARCHAR(MAX) NOT NULL, -- JSON configuration
    is_template BIT DEFAULT 0,
    is_public BIT DEFAULT 0,
    created_by INT NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_dashboards_creator FOREIGN KEY (created_by) REFERENCES app.users(user_id),
    CONSTRAINT CK_dashboards_config_json CHECK (ISJSON(config) = 1)
);

CREATE INDEX IX_dashboards_creator ON app.dashboards(created_by);
CREATE INDEX IX_dashboards_template ON app.dashboards(is_template) WHERE is_template = 1;

-- Dashboard sharing/access
CREATE TABLE app.dashboard_access (
    access_id INT IDENTITY(1,1) PRIMARY KEY,
    dashboard_id UNIQUEIDENTIFIER NOT NULL,
    user_id INT,
    role_id INT,
    customer_id VARCHAR(20), -- Restrict to specific customer
    access_level VARCHAR(20) DEFAULT 'view', -- 'view', 'edit', 'admin'
    granted_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_da_dashboard FOREIGN KEY (dashboard_id) REFERENCES app.dashboards(dashboard_id) ON DELETE CASCADE,
    CONSTRAINT FK_da_user FOREIGN KEY (user_id) REFERENCES app.users(user_id),
    CONSTRAINT FK_da_role FOREIGN KEY (role_id) REFERENCES app.roles(role_id)
);

-- Dashboard widgets (denormalized for flexibility)
CREATE TABLE app.dashboard_widgets (
    widget_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    dashboard_id UNIQUEIDENTIFIER NOT NULL,
    widget_type VARCHAR(50) NOT NULL, -- 'kpi_card', 'line_chart', 'bar_chart', etc.
    position_x INT DEFAULT 0,
    position_y INT DEFAULT 0,
    width INT DEFAULT 3,
    height INT DEFAULT 2,
    config NVARCHAR(MAX) NOT NULL, -- JSON configuration
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_widgets_dashboard FOREIGN KEY (dashboard_id) REFERENCES app.dashboards(dashboard_id) ON DELETE CASCADE,
    CONSTRAINT CK_widgets_config_json CHECK (ISJSON(config) = 1)
);

CREATE INDEX IX_widgets_dashboard ON app.dashboard_widgets(dashboard_id);

-- =============================================
-- REPORTS
-- =============================================

CREATE TABLE app.reports (
    report_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    name VARCHAR(200) NOT NULL,
    description VARCHAR(1000),
    report_type VARCHAR(50) NOT NULL, -- 'tabular', 'summary', 'detail'
    dataset_name VARCHAR(200), -- Source table/view
    config NVARCHAR(MAX) NOT NULL, -- JSON configuration
    is_template BIT DEFAULT 0,
    is_public BIT DEFAULT 0,
    created_by INT NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_reports_creator FOREIGN KEY (created_by) REFERENCES app.users(user_id),
    CONSTRAINT CK_reports_config_json CHECK (ISJSON(config) = 1)
);

CREATE INDEX IX_reports_creator ON app.reports(created_by);
CREATE INDEX IX_reports_template ON app.reports(is_template) WHERE is_template = 1;

-- Report sharing/access
CREATE TABLE app.report_access (
    access_id INT IDENTITY(1,1) PRIMARY KEY,
    report_id UNIQUEIDENTIFIER NOT NULL,
    user_id INT,
    role_id INT,
    customer_id VARCHAR(20),
    access_level VARCHAR(20) DEFAULT 'view',
    granted_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_ra_report FOREIGN KEY (report_id) REFERENCES app.reports(report_id) ON DELETE CASCADE,
    CONSTRAINT FK_ra_user FOREIGN KEY (user_id) REFERENCES app.users(user_id),
    CONSTRAINT FK_ra_role FOREIGN KEY (role_id) REFERENCES app.roles(role_id)
);

-- Report schedules
CREATE TABLE app.report_schedules (
    schedule_id INT IDENTITY(1,1) PRIMARY KEY,
    report_id UNIQUEIDENTIFIER NOT NULL,
    schedule_name VARCHAR(200),
    cron_expression VARCHAR(100) NOT NULL,
    export_format VARCHAR(20) NOT NULL, -- 'excel', 'pdf', 'csv'
    recipients VARCHAR(MAX), -- JSON array of emails
    parameters NVARCHAR(MAX), -- JSON parameters
    is_active BIT DEFAULT 1,
    last_run DATETIME2,
    next_run DATETIME2,
    created_by INT NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_schedules_report FOREIGN KEY (report_id) REFERENCES app.reports(report_id) ON DELETE CASCADE,
    CONSTRAINT FK_schedules_creator FOREIGN KEY (created_by) REFERENCES app.users(user_id)
);

-- Report execution history
CREATE TABLE app.report_executions (
    execution_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    report_id UNIQUEIDENTIFIER NOT NULL,
    schedule_id INT,
    executed_by INT,
    execution_type VARCHAR(20) NOT NULL, -- 'manual', 'scheduled'
    parameters NVARCHAR(MAX),
    row_count INT,
    execution_time_ms INT,
    export_format VARCHAR(20),
    file_path VARCHAR(500),
    status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed'
    error_message VARCHAR(MAX),
    started_at DATETIME2 DEFAULT GETUTCDATE(),
    completed_at DATETIME2,

    CONSTRAINT FK_executions_report FOREIGN KEY (report_id) REFERENCES app.reports(report_id),
    CONSTRAINT FK_executions_schedule FOREIGN KEY (schedule_id) REFERENCES app.report_schedules(schedule_id),
    CONSTRAINT FK_executions_user FOREIGN KEY (executed_by) REFERENCES app.users(user_id)
);

CREATE INDEX IX_executions_report ON app.report_executions(report_id, started_at);

-- =============================================
-- DATASETS (for query builder)
-- =============================================

CREATE TABLE app.datasets (
    dataset_id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(200),
    description VARCHAR(1000),
    source_type VARCHAR(50) NOT NULL, -- 'table', 'view', 'query'
    source_object VARCHAR(200) NOT NULL, -- Schema.TableName or SQL query
    schema_definition NVARCHAR(MAX), -- JSON schema of available columns
    default_filters NVARCHAR(MAX), -- JSON default filters
    rbac_column VARCHAR(100), -- Column used for RLS (e.g., 'customer_id')
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT CK_datasets_schema_json CHECK (schema_definition IS NULL OR ISJSON(schema_definition) = 1)
);

-- Pre-populate available datasets
INSERT INTO app.datasets (name, display_name, description, source_type, source_object, rbac_column) VALUES
('fact_vehicles', 'Vehicle Fleet', 'Daily vehicle fleet metrics', 'table', 'reporting.fact_vehicles', 'customer_key'),
('fact_contracts', 'Contracts', 'Contract snapshot data', 'table', 'reporting.fact_contracts', 'customer_key'),
('fact_invoices', 'Invoices', 'Invoice transactions', 'table', 'reporting.fact_invoices', 'customer_key'),
('fact_fuel', 'Fuel Consumption', 'Fuel transaction data', 'table', 'reporting.fact_fuel', 'customer_key'),
('fact_maintenance', 'Maintenance', 'Work order and maintenance data', 'table', 'reporting.fact_maintenance', 'customer_key'),
('vw_fleet_status', 'Fleet Status', 'Current fleet status view', 'view', 'reporting.vw_fleet_status', 'customer_id'),
('vw_contracts_expiring', 'Expiring Contracts', 'Contracts expiring soon', 'view', 'reporting.vw_contracts_expiring', 'customer_id'),
('vw_fuel_trends', 'Fuel Trends', 'Fuel consumption trends', 'view', 'reporting.vw_fuel_trends', 'customer_id'),
('vw_invoice_aging', 'Invoice Aging', 'Invoice aging analysis', 'view', 'reporting.vw_invoice_aging', 'customer_id'),
('vw_maintenance_summary', 'Maintenance Summary', 'Maintenance summary', 'view', 'reporting.vw_maintenance_summary', 'customer_name'),
('agg_customer_monthly', 'Customer Monthly Summary', 'Monthly customer aggregates', 'table', 'reporting.agg_customer_monthly', 'customer_key');

-- =============================================
-- AI & VECTOR SEARCH
-- =============================================

-- AI Conversations
CREATE TABLE app.ai_conversations (
    conversation_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    user_id INT NOT NULL,
    title VARCHAR(200),
    context NVARCHAR(MAX), -- JSON context for conversation
    started_at DATETIME2 DEFAULT GETUTCDATE(),
    last_message_at DATETIME2,
    message_count INT DEFAULT 0,
    is_archived BIT DEFAULT 0,

    CONSTRAINT FK_conversations_user FOREIGN KEY (user_id) REFERENCES app.users(user_id)
);

CREATE INDEX IX_conversations_user ON app.ai_conversations(user_id, started_at DESC);

-- AI Messages
CREATE TABLE app.ai_messages (
    message_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    conversation_id UNIQUEIDENTIFIER NOT NULL,
    role VARCHAR(20) NOT NULL, -- 'user', 'assistant', 'system'
    content NVARCHAR(MAX) NOT NULL,
    metadata NVARCHAR(MAX), -- JSON: tokens, model, sql_query, etc.
    created_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_messages_conversation FOREIGN KEY (conversation_id) REFERENCES app.ai_conversations(conversation_id) ON DELETE CASCADE
);

CREATE INDEX IX_messages_conversation ON app.ai_messages(conversation_id, created_at);

-- AI Generated SQL (for audit)
CREATE TABLE app.ai_sql_audit (
    audit_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    conversation_id UNIQUEIDENTIFIER,
    user_id INT NOT NULL,
    user_query NVARCHAR(MAX),
    generated_sql NVARCHAR(MAX),
    was_executed BIT DEFAULT 0,
    row_count INT,
    execution_time_ms INT,
    was_safe BIT,
    safety_notes VARCHAR(500),
    created_at DATETIME2 DEFAULT GETUTCDATE(),

    CONSTRAINT FK_sql_audit_conversation FOREIGN KEY (conversation_id) REFERENCES app.ai_conversations(conversation_id),
    CONSTRAINT FK_sql_audit_user FOREIGN KEY (user_id) REFERENCES app.users(user_id)
);

-- Vector Embeddings Store (for semantic search)
CREATE TABLE app.vector_embeddings (
    embedding_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL, -- 'vehicle', 'customer', 'document', etc.
    entity_id VARCHAR(100) NOT NULL,
    content_text NVARCHAR(MAX), -- Original text
    embedding_model VARCHAR(100), -- e.g., 'text-embedding-ada-002'
    -- embedding VECTOR(1536), -- MSSQL 2024+ native vector
    embedding_json NVARCHAR(MAX), -- Fallback: JSON array of floats
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2,

    CONSTRAINT UQ_vector_entity UNIQUE (entity_type, entity_id)
);

CREATE INDEX IX_vector_entity ON app.vector_embeddings(entity_type, entity_id);

-- FAQ/Knowledge Base for AI
CREATE TABLE app.knowledge_base (
    kb_id INT IDENTITY(1,1) PRIMARY KEY,
    category VARCHAR(100),
    question NVARCHAR(1000),
    answer NVARCHAR(MAX),
    keywords VARCHAR(500),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2
);

-- =============================================
-- AUDIT & LOGGING
-- =============================================

CREATE TABLE app.audit_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id INT,
    action VARCHAR(100) NOT NULL,
    entity_type VARCHAR(100),
    entity_id VARCHAR(100),
    old_values NVARCHAR(MAX),
    new_values NVARCHAR(MAX),
    ip_address VARCHAR(50),
    user_agent VARCHAR(500),
    created_at DATETIME2 DEFAULT GETUTCDATE()
);

CREATE INDEX IX_audit_user ON app.audit_log(user_id, created_at);
CREATE INDEX IX_audit_entity ON app.audit_log(entity_type, entity_id);
CREATE INDEX IX_audit_action ON app.audit_log(action, created_at);

-- User sessions
CREATE TABLE app.user_sessions (
    session_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    user_id INT NOT NULL,
    access_token_hash VARCHAR(64),
    ip_address VARCHAR(50),
    user_agent VARCHAR(500),
    started_at DATETIME2 DEFAULT GETUTCDATE(),
    last_activity DATETIME2 DEFAULT GETUTCDATE(),
    expires_at DATETIME2,
    is_active BIT DEFAULT 1,

    CONSTRAINT FK_sessions_user FOREIGN KEY (user_id) REFERENCES app.users(user_id)
);

CREATE INDEX IX_sessions_user ON app.user_sessions(user_id, is_active);

-- =============================================
-- SYSTEM CONFIGURATION
-- =============================================

CREATE TABLE app.system_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value NVARCHAR(MAX),
    config_type VARCHAR(20) DEFAULT 'string', -- 'string', 'number', 'boolean', 'json'
    description VARCHAR(500),
    is_secret BIT DEFAULT 0,
    updated_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_by INT
);

-- Pre-populate system config
INSERT INTO app.system_config (config_key, config_value, config_type, description) VALUES
('app.name', 'FleetAI', 'string', 'Application name'),
('app.version', '1.0.0', 'string', 'Application version'),
('session.timeout_minutes', '480', 'number', 'Session timeout in minutes'),
('ai.model', 'gpt-4', 'string', 'Default AI model'),
('ai.max_tokens', '4000', 'number', 'Max tokens for AI responses'),
('export.max_rows', '100000', 'number', 'Maximum rows for report export'),
('email.enabled', 'true', 'boolean', 'Enable email notifications');

-- =============================================
-- ROW-LEVEL SECURITY FUNCTIONS
-- =============================================

GO

-- Function to check if user has access to customer
CREATE OR ALTER FUNCTION app.fn_user_has_customer_access(
    @user_id INT,
    @customer_id VARCHAR(20)
)
RETURNS BIT
AS
BEGIN
    DECLARE @has_access BIT = 0;
    DECLARE @role_level INT;

    -- Get user's role level
    SELECT @role_level = r.role_level
    FROM app.users u
    JOIN app.roles r ON u.role_id = r.role_id
    WHERE u.user_id = @user_id AND u.is_active = 1;

    -- Super users and admins have access to everything
    IF @role_level >= 50
    BEGIN
        SET @has_access = 1;
    END
    ELSE
    BEGIN
        -- Check explicit access
        IF EXISTS (
            SELECT 1 FROM app.user_customer_access
            WHERE user_id = @user_id AND customer_id = @customer_id
        )
        BEGIN
            SET @has_access = 1;
        END
    END

    RETURN @has_access;
END;

GO

-- Function to get user's accessible customer IDs
CREATE OR ALTER FUNCTION app.fn_get_user_customers(@user_id INT)
RETURNS TABLE
AS
RETURN
(
    SELECT customer_id
    FROM app.user_customer_access
    WHERE user_id = @user_id

    UNION

    -- For super users/admins, return all customers
    SELECT DISTINCT customer_id
    FROM staging.customers
    WHERE EXISTS (
        SELECT 1
        FROM app.users u
        JOIN app.roles r ON u.role_id = r.role_id
        WHERE u.user_id = @user_id
        AND r.role_level >= 50
        AND u.is_active = 1
    )
);

GO

-- Function to check user permission
CREATE OR ALTER FUNCTION app.fn_user_has_permission(
    @user_id INT,
    @permission_name VARCHAR(100)
)
RETURNS BIT
AS
BEGIN
    DECLARE @has_permission BIT = 0;

    IF EXISTS (
        SELECT 1
        FROM app.users u
        JOIN app.role_permissions rp ON u.role_id = rp.role_id
        JOIN app.permissions p ON rp.permission_id = p.permission_id
        WHERE u.user_id = @user_id
        AND u.is_active = 1
        AND p.permission_name = @permission_name
    )
    BEGIN
        SET @has_permission = 1;
    END

    RETURN @has_permission;
END;

GO
