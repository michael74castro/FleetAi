-- =============================================
-- FleetAI Application Schema (SQLite)
-- User management, dashboards, reports, AI
-- =============================================

-- =============================================
-- USER & AUTHENTICATION
-- =============================================

-- Vehicle Status Reference Table
CREATE TABLE IF NOT EXISTS app_vehicle_status (
    status_code INTEGER PRIMARY KEY,
    status_name TEXT NOT NULL,
    status_description TEXT NOT NULL,
    status_category TEXT NOT NULL,  -- 'Active', 'Created', 'Terminated'
    is_active_status INTEGER DEFAULT 0,
    display_order INTEGER
);

-- Insert vehicle status definitions
INSERT OR REPLACE INTO app_vehicle_status (status_code, status_name, status_description, status_category, is_active_status, display_order) VALUES
(0, 'Created', 'Vehicle record created but not yet active', 'Created', 1, 1),
(1, 'Active', 'Vehicle is currently active in the fleet', 'Active', 1, 2),
(2, 'Terminated - Invoicing Stopped', 'Contract terminated, invoicing has stopped', 'Terminated', 0, 3),
(3, 'Terminated - Invoice Adjustment Made', 'Contract terminated, invoice adjustment completed', 'Terminated', 0, 4),
(4, 'Terminated - Mileage Adjustment Made', 'Contract terminated, mileage variation adjustment completed', 'Terminated', 0, 5),
(5, 'Terminated - De-investment Made', 'Contract terminated, de-investment completed (steps 3 & 4 done)', 'Terminated', 0, 6),
(8, 'Terminated - Ready for Settlement', 'Contract terminated, ready for first final settlement run', 'Terminated', 0, 7),
(9, 'Terminated - Final Settlement Made', 'Contract terminated, final settlement report completed', 'Terminated', 0, 8);

-- Order Status Reference Table
CREATE TABLE IF NOT EXISTS app_order_status (
    status_code INTEGER PRIMARY KEY,
    status_name TEXT NOT NULL,
    status_description TEXT NOT NULL,
    status_phase TEXT NOT NULL,  -- 'Order Phase', 'Delivery Phase', 'Cancelled'
    is_active_order INTEGER DEFAULT 1,
    display_order INTEGER
);

-- Insert order status definitions
INSERT OR REPLACE INTO app_order_status (status_code, status_name, status_description, status_phase, is_active_order, display_order) VALUES
(0, 'Created', 'Order created into the system', 'Order Phase', 1, 1),
(1, 'Sent to Dealer', 'Order sent to dealer', 'Order Phase', 1, 2),
(2, 'Delivery Confirmed', 'Delivery confirmed by dealer', 'Order Phase', 1, 3),
(3, 'Insurance Arranged', 'Arranged for insurance', 'Delivery Phase', 1, 4),
(4, 'Registration Arranged', 'Arranged for vehicle registration and other modifications', 'Delivery Phase', 1, 5),
(5, 'Driver Pack Prepared', 'Prepared driver information pack', 'Delivery Phase', 1, 6),
(6, 'Vehicle Delivered', 'Vehicle delivered to client', 'Delivery Phase', 1, 7),
(7, 'Lease Schedule Generated', 'Generate lease schedule in the system for invoicing', 'Delivery Phase', 0, 8),
(9, 'Cancelled', 'Order cancelled', 'Cancelled', 0, 9);

-- Roles lookup table
CREATE TABLE IF NOT EXISTS app_roles (
    role_id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_name TEXT NOT NULL UNIQUE,
    role_description TEXT,
    role_level INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Users table
CREATE TABLE IF NOT EXISTS app_users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT UNIQUE,
    email TEXT NOT NULL UNIQUE,
    display_name TEXT,
    first_name TEXT,
    last_name TEXT,
    password_hash TEXT,
    role_id INTEGER NOT NULL,
    customer_id INTEGER,
    is_active INTEGER DEFAULT 1,
    last_login TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (role_id) REFERENCES app_roles(role_id)
);

CREATE INDEX IF NOT EXISTS idx_app_users_email ON app_users(email);
CREATE INDEX IF NOT EXISTS idx_app_users_role ON app_users(role_id);
CREATE INDEX IF NOT EXISTS idx_app_users_customer ON app_users(customer_id);

-- User-Customer access mapping (for row-level security)
CREATE TABLE IF NOT EXISTS app_user_customer_access (
    access_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    access_level TEXT DEFAULT 'read',
    granted_at TEXT DEFAULT (datetime('now')),
    granted_by INTEGER,

    FOREIGN KEY (user_id) REFERENCES app_users(user_id),
    UNIQUE(user_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_app_uca_user ON app_user_customer_access(user_id);
CREATE INDEX IF NOT EXISTS idx_app_uca_customer ON app_user_customer_access(customer_id);

-- User-Driver linking (for driver role)
CREATE TABLE IF NOT EXISTS app_user_driver_link (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL UNIQUE,
    driver_id INTEGER NOT NULL UNIQUE,
    vehicle_id INTEGER,
    linked_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

-- Permissions table
CREATE TABLE IF NOT EXISTS app_permissions (
    permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
    permission_name TEXT NOT NULL UNIQUE,
    permission_category TEXT,
    description TEXT
);

-- Role-Permission mapping
CREATE TABLE IF NOT EXISTS app_role_permissions (
    role_id INTEGER NOT NULL,
    permission_id INTEGER NOT NULL,

    PRIMARY KEY (role_id, permission_id),
    FOREIGN KEY (role_id) REFERENCES app_roles(role_id),
    FOREIGN KEY (permission_id) REFERENCES app_permissions(permission_id)
);

-- =============================================
-- DASHBOARDS
-- =============================================

CREATE TABLE IF NOT EXISTS app_dashboards (
    dashboard_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    layout_type TEXT DEFAULT 'grid',
    config TEXT NOT NULL,
    is_template INTEGER DEFAULT 0,
    is_public INTEGER DEFAULT 0,
    created_by INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (created_by) REFERENCES app_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_app_dashboards_creator ON app_dashboards(created_by);

-- Dashboard sharing/access
CREATE TABLE IF NOT EXISTS app_dashboard_access (
    access_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_id TEXT NOT NULL,
    user_id INTEGER,
    role_id INTEGER,
    customer_id INTEGER,
    access_level TEXT DEFAULT 'view',
    granted_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (dashboard_id) REFERENCES app_dashboards(dashboard_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

-- Dashboard widgets
CREATE TABLE IF NOT EXISTS app_dashboard_widgets (
    widget_id TEXT PRIMARY KEY,
    dashboard_id TEXT NOT NULL,
    widget_type TEXT NOT NULL,
    title TEXT,
    position_x INTEGER DEFAULT 0,
    position_y INTEGER DEFAULT 0,
    width INTEGER DEFAULT 3,
    height INTEGER DEFAULT 2,
    config TEXT NOT NULL,
    data_source TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (dashboard_id) REFERENCES app_dashboards(dashboard_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_app_widgets_dashboard ON app_dashboard_widgets(dashboard_id);

-- =============================================
-- SAVED REPORTS & QUERIES
-- =============================================

CREATE TABLE IF NOT EXISTS app_saved_reports (
    report_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    report_type TEXT NOT NULL,
    data_source TEXT,
    sql_query TEXT,
    config TEXT NOT NULL,
    is_template INTEGER DEFAULT 0,
    is_public INTEGER DEFAULT 0,
    created_by INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (created_by) REFERENCES app_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_app_reports_creator ON app_saved_reports(created_by);

-- Report sharing/access
CREATE TABLE IF NOT EXISTS app_report_access (
    access_id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id TEXT NOT NULL,
    user_id INTEGER,
    role_id INTEGER,
    customer_id INTEGER,
    access_level TEXT DEFAULT 'view',
    granted_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (report_id) REFERENCES app_saved_reports(report_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

-- Report schedules
CREATE TABLE IF NOT EXISTS app_report_schedules (
    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id TEXT NOT NULL,
    schedule_name TEXT,
    cron_expression TEXT NOT NULL,
    export_format TEXT NOT NULL,
    recipients TEXT,
    parameters TEXT,
    is_active INTEGER DEFAULT 1,
    last_run TEXT,
    next_run TEXT,
    created_by INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (report_id) REFERENCES app_saved_reports(report_id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES app_users(user_id)
);

-- Report execution history
CREATE TABLE IF NOT EXISTS app_report_executions (
    execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id TEXT NOT NULL,
    schedule_id INTEGER,
    executed_by INTEGER,
    execution_type TEXT NOT NULL,
    parameters TEXT,
    row_count INTEGER,
    execution_time_ms INTEGER,
    export_format TEXT,
    file_path TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT,

    FOREIGN KEY (report_id) REFERENCES app_saved_reports(report_id),
    FOREIGN KEY (schedule_id) REFERENCES app_report_schedules(schedule_id),
    FOREIGN KEY (executed_by) REFERENCES app_users(user_id)
);

-- =============================================
-- DATASETS (for query builder)
-- =============================================

CREATE TABLE IF NOT EXISTS app_datasets (
    dataset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    display_name TEXT,
    description TEXT,
    source_type TEXT NOT NULL,
    source_object TEXT NOT NULL,
    schema_definition TEXT,
    default_filters TEXT,
    rbac_column TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- =============================================
-- AI & CHAT HISTORY
-- =============================================

-- AI Conversations
CREATE TABLE IF NOT EXISTS app_ai_conversations (
    conversation_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title TEXT,
    context TEXT,
    model_used TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    last_message_at TEXT,
    message_count INTEGER DEFAULT 0,
    total_tokens_used INTEGER DEFAULT 0,
    is_archived INTEGER DEFAULT 0,

    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_app_conversations_user ON app_ai_conversations(user_id, started_at);

-- AI Messages
CREATE TABLE IF NOT EXISTS app_ai_messages (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    tokens_used INTEGER,
    model TEXT,
    metadata TEXT,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (conversation_id) REFERENCES app_ai_conversations(conversation_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_app_messages_conversation ON app_ai_messages(conversation_id, created_at);

-- AI Generated SQL (for audit)
CREATE TABLE IF NOT EXISTS app_ai_sql_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT,
    message_id INTEGER,
    user_id INTEGER NOT NULL,
    user_query TEXT,
    generated_sql TEXT,
    was_executed INTEGER DEFAULT 0,
    row_count INTEGER,
    execution_time_ms INTEGER,
    was_safe INTEGER,
    safety_notes TEXT,
    error_message TEXT,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (conversation_id) REFERENCES app_ai_conversations(conversation_id),
    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_app_sql_audit_user ON app_ai_sql_audit(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_app_sql_audit_conversation ON app_ai_sql_audit(conversation_id);

-- AI Feedback (for learning)
CREATE TABLE IF NOT EXISTS app_ai_feedback (
    feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    rating INTEGER,
    feedback_type TEXT,
    feedback_text TEXT,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (message_id) REFERENCES app_ai_messages(message_id),
    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

-- =============================================
-- KNOWLEDGE BASE (for AI Context)
-- =============================================

CREATE TABLE IF NOT EXISTS app_knowledge_base (
    kb_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT,
    question TEXT,
    answer TEXT,
    keywords TEXT,
    related_tables TEXT,
    sql_example TEXT,
    is_active INTEGER DEFAULT 1,
    usage_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_app_kb_category ON app_knowledge_base(category);

-- =============================================
-- ALERTS & NOTIFICATIONS
-- =============================================

CREATE TABLE IF NOT EXISTS app_alert_rules (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    alert_type TEXT NOT NULL,
    condition_sql TEXT NOT NULL,
    severity TEXT DEFAULT 'medium',
    is_active INTEGER DEFAULT 1,
    check_frequency_minutes INTEGER DEFAULT 60,
    last_checked TEXT,
    created_by INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (created_by) REFERENCES app_users(user_id)
);

CREATE TABLE IF NOT EXISTS app_alert_subscriptions (
    subscription_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    notification_method TEXT DEFAULT 'in_app',
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (rule_id) REFERENCES app_alert_rules(rule_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES app_users(user_id),
    UNIQUE(rule_id, user_id)
);

CREATE TABLE IF NOT EXISTS app_notifications (
    notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    rule_id INTEGER,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    severity TEXT DEFAULT 'info',
    link TEXT,
    is_read INTEGER DEFAULT 0,
    read_at TEXT,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES app_users(user_id),
    FOREIGN KEY (rule_id) REFERENCES app_alert_rules(rule_id)
);

CREATE INDEX IF NOT EXISTS idx_app_notifications_user ON app_notifications(user_id, is_read, created_at);

-- =============================================
-- AUDIT & LOGGING
-- =============================================

CREATE TABLE IF NOT EXISTS app_audit_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    old_values TEXT,
    new_values TEXT,
    ip_address TEXT,
    user_agent TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_app_audit_user ON app_audit_log(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_app_audit_entity ON app_audit_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_app_audit_action ON app_audit_log(action, created_at);

-- User sessions
CREATE TABLE IF NOT EXISTS app_user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    access_token_hash TEXT,
    refresh_token_hash TEXT,
    ip_address TEXT,
    user_agent TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    last_activity TEXT DEFAULT (datetime('now')),
    expires_at TEXT,
    is_active INTEGER DEFAULT 1,

    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_app_sessions_user ON app_user_sessions(user_id, is_active);

-- =============================================
-- SYSTEM CONFIGURATION
-- =============================================

CREATE TABLE IF NOT EXISTS app_system_config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT,
    config_type TEXT DEFAULT 'string',
    description TEXT,
    is_secret INTEGER DEFAULT 0,
    updated_at TEXT DEFAULT (datetime('now')),
    updated_by INTEGER
);

-- =============================================
-- USER PREFERENCES
-- =============================================

CREATE TABLE IF NOT EXISTS app_user_preferences (
    preference_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    preference_key TEXT NOT NULL,
    preference_value TEXT,
    updated_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES app_users(user_id),
    UNIQUE(user_id, preference_key)
);

CREATE INDEX IF NOT EXISTS idx_app_user_prefs ON app_user_preferences(user_id);

-- =============================================
-- FAVORITES & BOOKMARKS
-- =============================================

CREATE TABLE IF NOT EXISTS app_user_favorites (
    favorite_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    display_name TEXT,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES app_users(user_id),
    UNIQUE(user_id, entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_app_favorites_user ON app_user_favorites(user_id);

-- =============================================
-- RECENT ACTIVITY
-- =============================================

CREATE TABLE IF NOT EXISTS app_recent_activity (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    activity_type TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    entity_name TEXT,
    metadata TEXT,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_app_recent_user ON app_recent_activity(user_id, created_at);

-- =============================================
-- API KEYS (for integrations)
-- =============================================

CREATE TABLE IF NOT EXISTS app_api_keys (
    key_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    key_name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix TEXT NOT NULL,
    permissions TEXT,
    rate_limit INTEGER DEFAULT 1000,
    last_used TEXT,
    expires_at TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),

    FOREIGN KEY (user_id) REFERENCES app_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_app_api_keys_user ON app_api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_app_api_keys_hash ON app_api_keys(key_hash);
