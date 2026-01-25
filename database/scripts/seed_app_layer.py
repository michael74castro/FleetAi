"""
Seed Script: Application Layer
Populates app tables with initial data (roles, permissions, config, etc.)
"""

import sqlite3
import uuid
import hashlib
from datetime import datetime

DB_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\fleetai.db"
SCHEMA_FILE = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\schemas\04_app_sqlite.sql"


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_app_schema(conn):
    """Create app layer tables."""
    print("Creating app layer schema...")
    with open(SCHEMA_FILE, 'r') as f:
        schema_sql = f.read()
    cursor = conn.cursor()
    cursor.executescript(schema_sql)
    conn.commit()
    print("  Schema created.")


def seed_roles(conn):
    """Seed role definitions."""
    print("  Seeding roles...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM app_roles")

    roles = [
        ('driver', 'View own vehicle, fuel, maintenance data', 10),
        ('fleet_admin', 'View/manage all vehicles for their company', 20),
        ('client_contact', 'View company-wide fleet data', 30),
        ('account_manager', 'View assigned client data, create reports', 40),
        ('analyst', 'Full read access, create reports and dashboards', 50),
        ('super_user', 'Full read access to all data', 60),
        ('admin', 'Full system access including user management', 100),
    ]

    cursor.executemany("""
        INSERT INTO app_roles (role_name, role_description, role_level)
        VALUES (?, ?, ?)
    """, roles)
    conn.commit()
    print(f"{len(roles)} roles")


def seed_permissions(conn):
    """Seed permission definitions."""
    print("  Seeding permissions...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM app_permissions")

    permissions = [
        # Dashboard permissions
        ('dashboard:view', 'dashboard', 'View dashboards'),
        ('dashboard:create', 'dashboard', 'Create new dashboards'),
        ('dashboard:edit', 'dashboard', 'Edit existing dashboards'),
        ('dashboard:delete', 'dashboard', 'Delete dashboards'),
        ('dashboard:share', 'dashboard', 'Share dashboards with others'),
        # Report permissions
        ('report:view', 'report', 'View reports'),
        ('report:create', 'report', 'Create new reports'),
        ('report:edit', 'report', 'Edit existing reports'),
        ('report:delete', 'report', 'Delete reports'),
        ('report:export', 'report', 'Export reports to files'),
        ('report:schedule', 'report', 'Schedule report delivery'),
        # Data permissions
        ('data:view_own', 'data', 'View own data only'),
        ('data:view_company', 'data', 'View company-wide data'),
        ('data:view_assigned', 'data', 'View assigned clients data'),
        ('data:view_all', 'data', 'View all data'),
        ('data:export', 'data', 'Export data to files'),
        # AI permissions
        ('ai:chat', 'ai', 'Use AI chat assistant'),
        ('ai:query', 'ai', 'Use AI to query data'),
        ('ai:analytics', 'ai', 'Use AI analytics features'),
        # Admin permissions
        ('admin:users', 'admin', 'Manage users'),
        ('admin:roles', 'admin', 'Manage roles'),
        ('admin:system', 'admin', 'System configuration'),
        ('admin:audit', 'admin', 'View audit logs'),
    ]

    cursor.executemany("""
        INSERT INTO app_permissions (permission_name, permission_category, description)
        VALUES (?, ?, ?)
    """, permissions)
    conn.commit()
    print(f"{len(permissions)} permissions")


def seed_role_permissions(conn):
    """Assign permissions to roles."""
    print("  Assigning role permissions...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM app_role_permissions")

    # Define permission sets for each role
    role_perms = {
        'driver': ['dashboard:view', 'report:view', 'data:view_own'],
        'fleet_admin': ['dashboard:view', 'dashboard:create', 'report:view', 'report:create',
                        'report:export', 'data:view_company', 'data:export', 'ai:chat', 'ai:query'],
        'client_contact': ['dashboard:view', 'report:view', 'report:export', 'data:view_company',
                           'ai:chat', 'ai:query'],
        'account_manager': ['dashboard:view', 'dashboard:create', 'dashboard:share',
                            'report:view', 'report:create', 'report:export', 'report:schedule',
                            'data:view_assigned', 'data:export', 'ai:chat', 'ai:query', 'ai:analytics'],
        'analyst': ['dashboard:view', 'dashboard:create', 'dashboard:edit', 'dashboard:share',
                    'report:view', 'report:create', 'report:edit', 'report:export', 'report:schedule',
                    'data:view_all', 'data:export', 'ai:chat', 'ai:query', 'ai:analytics'],
        'super_user': ['dashboard:view', 'dashboard:create', 'dashboard:edit', 'dashboard:delete', 'dashboard:share',
                       'report:view', 'report:create', 'report:edit', 'report:delete', 'report:export', 'report:schedule',
                       'data:view_all', 'data:export', 'ai:chat', 'ai:query', 'ai:analytics', 'admin:audit'],
        'admin': None  # All permissions
    }

    # Get role and permission IDs
    cursor.execute("SELECT role_id, role_name FROM app_roles")
    roles = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT permission_id, permission_name FROM app_permissions")
    perms = {name: id for id, name in cursor.fetchall()}

    count = 0
    for role_name, perm_list in role_perms.items():
        role_id = roles.get(role_name)
        if not role_id:
            continue

        if perm_list is None:  # Admin gets all
            perm_list = list(perms.keys())

        for perm_name in perm_list:
            perm_id = perms.get(perm_name)
            if perm_id:
                cursor.execute("""
                    INSERT OR IGNORE INTO app_role_permissions (role_id, permission_id)
                    VALUES (?, ?)
                """, (role_id, perm_id))
                count += 1

    conn.commit()
    print(f"{count} mappings")


def seed_system_config(conn):
    """Seed system configuration."""
    print("  Seeding system config...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM app_system_config")

    config = [
        ('app.name', 'FleetAI', 'string', 'Application name', 0),
        ('app.version', '1.0.0', 'string', 'Application version', 0),
        ('app.environment', 'development', 'string', 'Environment (development/staging/production)', 0),
        ('session.timeout_minutes', '480', 'number', 'Session timeout in minutes', 0),
        ('session.max_concurrent', '5', 'number', 'Max concurrent sessions per user', 0),
        ('ai.enabled', 'true', 'boolean', 'Enable AI features', 0),
        ('ai.model', 'gpt-4', 'string', 'Default AI model', 0),
        ('ai.max_tokens', '4000', 'number', 'Max tokens for AI responses', 0),
        ('ai.temperature', '0.7', 'number', 'AI temperature setting', 0),
        ('ai.max_sql_rows', '1000', 'number', 'Max rows returned from AI-generated SQL', 0),
        ('export.max_rows', '100000', 'number', 'Maximum rows for report export', 0),
        ('export.formats', '["excel","csv","pdf"]', 'json', 'Available export formats', 0),
        ('email.enabled', 'false', 'boolean', 'Enable email notifications', 0),
        ('email.from_address', 'noreply@fleetai.local', 'string', 'From email address', 0),
        ('audit.retention_days', '365', 'number', 'Days to retain audit logs', 0),
        ('api.rate_limit', '1000', 'number', 'API calls per hour per user', 0),
    ]

    cursor.executemany("""
        INSERT INTO app_system_config (config_key, config_value, config_type, description, is_secret)
        VALUES (?, ?, ?, ?, ?)
    """, config)
    conn.commit()
    print(f"{len(config)} settings")


def seed_datasets(conn):
    """Seed available datasets for query builder."""
    print("  Seeding datasets...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM app_datasets")

    datasets = [
        # Dimension tables
        ('dim_customer', 'Customers', 'Customer master data including names, locations, and account managers',
         'table', 'dim_customer', 'customer_id'),
        ('dim_vehicle', 'Vehicles', 'Vehicle fleet data including make, model, registration, and lease details',
         'table', 'dim_vehicle', 'customer_id'),
        ('dim_driver', 'Drivers', 'Driver information linked to vehicles',
         'table', 'dim_driver', 'vehicle_id'),
        ('dim_contract', 'Contracts', 'Lease contracts with pricing breakdown and terms',
         'table', 'dim_contract', 'customer_id'),
        ('dim_group', 'Customer Groups', 'Customer groupings for reporting',
         'table', 'dim_group', 'customer_id'),
        ('dim_make_model', 'Vehicle Makes/Models', 'Reference data for vehicle makes and models',
         'table', 'dim_make_model', None),

        # Fact tables
        ('fact_odometer_reading', 'Odometer Readings', 'Historical odometer/mileage readings',
         'table', 'fact_odometer_reading', 'vehicle_id'),
        ('fact_billing', 'Billing Records', 'Billing transactions and amounts',
         'table', 'fact_billing', 'customer_id'),

        # Views
        ('view_fleet_overview', 'Fleet Overview', 'Comprehensive view of all vehicles with customer and driver info',
         'view', 'view_fleet_overview', 'customer_id'),
        ('view_customer_fleet_summary', 'Customer Fleet Summary', 'Summary of fleet size and value by customer',
         'view', 'view_customer_fleet_summary', 'customer_id'),
        ('view_vehicle_details', 'Vehicle Details', 'Detailed vehicle information with contract details',
         'view', 'view_vehicle_details', 'customer_id'),
        ('view_contract_summary', 'Contract Summary', 'Contract overview with calculated totals',
         'view', 'view_contract_summary', 'customer_id'),
        ('view_make_model_distribution', 'Make/Model Distribution', 'Vehicle counts by make and model',
         'view', 'view_make_model_distribution', None),
        ('view_driver_directory', 'Driver Directory', 'All drivers with vehicle assignments',
         'view', 'view_driver_directory', 'customer_name'),
        ('view_account_manager_portfolio', 'Account Manager Portfolio', 'Customer and vehicle counts by account manager',
         'view', 'view_account_manager_portfolio', None),

        # Aggregates
        ('agg_fleet_kpis', 'Fleet KPIs', 'High-level fleet metrics and KPIs',
         'table', 'agg_fleet_kpis', None),
        ('agg_customer_kpis', 'Customer KPIs', 'Customer-level metrics and KPIs',
         'table', 'agg_customer_kpis', 'customer_id'),
    ]

    cursor.executemany("""
        INSERT INTO app_datasets (name, display_name, description, source_type, source_object, rbac_column)
        VALUES (?, ?, ?, ?, ?, ?)
    """, datasets)
    conn.commit()
    print(f"{len(datasets)} datasets")


def seed_knowledge_base(conn):
    """Seed AI knowledge base with common questions and SQL examples."""
    print("  Seeding knowledge base...", end=" ", flush=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM app_knowledge_base")

    knowledge = [
        # Fleet questions
        ('fleet', 'How many vehicles do we have?',
         'Query the agg_fleet_kpis table or count from dim_vehicle',
         'fleet size, total vehicles, vehicle count',
         'agg_fleet_kpis, dim_vehicle',
         'SELECT total_vehicles, active_vehicles FROM agg_fleet_kpis'),

        ('fleet', 'What vehicles does a customer have?',
         'Query dim_vehicle filtered by customer_id or customer_name',
         'customer vehicles, fleet by customer',
         'dim_vehicle, view_customer_fleet_summary',
         "SELECT * FROM dim_vehicle WHERE customer_name LIKE '%CustomerName%'"),

        ('fleet', 'What makes and models do we have?',
         'Query view_make_model_distribution or dim_make_model',
         'makes, models, brands, vehicle types',
         'view_make_model_distribution, dim_make_model',
         'SELECT make_name, model_name, vehicle_count FROM view_make_model_distribution'),

        # Customer questions
        ('customer', 'List all customers',
         'Query dim_customer for customer master data',
         'customers, clients, accounts',
         'dim_customer',
         'SELECT customer_id, customer_name, city, account_manager_name FROM dim_customer'),

        ('customer', 'Who is the account manager for a customer?',
         'Query dim_customer.account_manager_name',
         'account manager, AM, relationship manager',
         'dim_customer',
         "SELECT customer_name, account_manager_name FROM dim_customer WHERE customer_name LIKE '%CustomerName%'"),

        ('customer', 'What customers does an account manager have?',
         'Query view_account_manager_portfolio or filter dim_customer',
         'AM customers, portfolio',
         'view_account_manager_portfolio, dim_customer',
         "SELECT * FROM view_account_manager_portfolio WHERE account_manager_name = 'ManagerName'"),

        # Contract questions
        ('contract', 'What are the contract details?',
         'Query dim_contract for lease terms, rates, and allowances',
         'lease terms, contract terms, monthly rate',
         'dim_contract, view_contract_summary',
         'SELECT * FROM view_contract_summary WHERE customer_name = ?'),

        ('contract', 'What is the monthly rate breakdown?',
         'Query dim_contract for monthly_rate_* columns',
         'rate breakdown, cost breakdown, depreciation, maintenance, insurance',
         'dim_contract',
         '''SELECT contract_position_number, monthly_rate_total, monthly_rate_depreciation,
            monthly_rate_maintenance, monthly_rate_insurance FROM dim_contract'''),

        # Driver questions
        ('driver', 'Who drives a vehicle?',
         'Query dim_driver filtered by vehicle_id or view_driver_directory',
         'driver, who drives, vehicle driver',
         'dim_driver, view_driver_directory',
         'SELECT * FROM view_driver_directory WHERE vehicle_id = ?'),

        ('driver', 'Driver contact information',
         'Query dim_driver for email and phone fields',
         'driver email, driver phone, contact',
         'dim_driver',
         'SELECT driver_name, email_address, phone_mobile FROM dim_driver'),

        # Billing questions
        ('billing', 'What is the billing for a customer?',
         'Query view_customer_billing_summary or fact_billing',
         'billing, invoice, charges, monthly billing',
         'view_customer_billing_summary, fact_billing',
         'SELECT * FROM view_customer_billing_summary WHERE customer_id = ?'),

        # Mileage questions
        ('mileage', 'What is the current odometer for a vehicle?',
         'Query dim_vehicle.current_odometer_km or fact_odometer_reading',
         'odometer, mileage, km, kilometers',
         'dim_vehicle, view_mileage_analysis',
         'SELECT vehicle_id, current_odometer_km FROM dim_vehicle WHERE vehicle_id = ?'),

        ('mileage', 'Vehicle mileage history',
         'Query fact_odometer_reading for historical readings',
         'mileage history, odometer history',
         'fact_odometer_reading',
         'SELECT reading_date, odometer_km FROM fact_odometer_reading WHERE vehicle_id = ? ORDER BY reading_date'),
    ]

    cursor.executemany("""
        INSERT INTO app_knowledge_base (category, question, answer, keywords, related_tables, sql_example)
        VALUES (?, ?, ?, ?, ?, ?)
    """, knowledge)
    conn.commit()
    print(f"{len(knowledge)} entries")


def seed_sample_users(conn):
    """Create sample users for testing."""
    print("  Creating sample users...", end=" ", flush=True)
    cursor = conn.cursor()

    # Check if users already exist
    existing = cursor.execute("SELECT COUNT(*) FROM app_users").fetchone()[0]
    if existing > 0:
        print("skipped (users exist)")
        return

    # Get role IDs
    cursor.execute("SELECT role_id, role_name FROM app_roles")
    roles = {name: id for id, name in cursor.fetchall()}

    users = [
        ('admin@fleetai.local', 'System Administrator', 'System', 'Admin', roles['admin'], None),
        ('analyst@fleetai.local', 'Fleet Analyst', 'Fleet', 'Analyst', roles['analyst'], None),
        ('manager@fleetai.local', 'Account Manager', 'Account', 'Manager', roles['account_manager'], None),
        ('client@fleetai.local', 'Client User', 'Client', 'User', roles['client_contact'], 10002),
    ]

    for email, display, first, last, role_id, customer_id in users:
        cursor.execute("""
            INSERT INTO app_users (email, display_name, first_name, last_name, role_id, customer_id, password_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (email, display, first, last, role_id, customer_id, hashlib.sha256(b'password123').hexdigest()))

    conn.commit()
    print(f"{len(users)} users")


def seed_default_dashboard(conn):
    """Create a default dashboard template."""
    print("  Creating default dashboard...", end=" ", flush=True)
    cursor = conn.cursor()

    # Get admin user
    cursor.execute("SELECT user_id FROM app_users WHERE email = 'admin@fleetai.local'")
    result = cursor.fetchone()
    if not result:
        print("skipped (no admin user)")
        return

    admin_id = result[0]
    dashboard_id = str(uuid.uuid4())

    # Check if default dashboard exists
    cursor.execute("SELECT COUNT(*) FROM app_dashboards WHERE name = 'Fleet Overview Dashboard'")
    if cursor.fetchone()[0] > 0:
        print("skipped (exists)")
        return

    # Create dashboard
    cursor.execute("""
        INSERT INTO app_dashboards (dashboard_id, name, description, layout_type, config, is_template, is_public, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        dashboard_id,
        'Fleet Overview Dashboard',
        'Default dashboard showing key fleet metrics',
        'grid',
        '{"columns": 12, "rowHeight": 100}',
        1, 1, admin_id
    ))

    # Create widgets
    widgets = [
        (str(uuid.uuid4()), 'kpi_card', 'Total Vehicles', 0, 0, 3, 2,
         '{"metric": "total_vehicles", "source": "agg_fleet_kpis"}', 'agg_fleet_kpis'),
        (str(uuid.uuid4()), 'kpi_card', 'Active Vehicles', 3, 0, 3, 2,
         '{"metric": "active_vehicles", "source": "agg_fleet_kpis"}', 'agg_fleet_kpis'),
        (str(uuid.uuid4()), 'kpi_card', 'Total Customers', 6, 0, 3, 2,
         '{"metric": "total_customers", "source": "agg_fleet_kpis"}', 'agg_fleet_kpis'),
        (str(uuid.uuid4()), 'kpi_card', 'Total Contracts', 9, 0, 3, 2,
         '{"metric": "total_contracts", "source": "agg_fleet_kpis"}', 'agg_fleet_kpis'),
        (str(uuid.uuid4()), 'bar_chart', 'Vehicles by Make', 0, 2, 6, 4,
         '{"x": "make_name", "y": "vehicle_count", "source": "view_make_model_distribution"}', 'view_make_model_distribution'),
        (str(uuid.uuid4()), 'table', 'Top Customers by Fleet Size', 6, 2, 6, 4,
         '{"columns": ["customer_name", "total_vehicles", "total_monthly_lease_value"], "limit": 10}', 'view_customer_fleet_summary'),
    ]

    for widget_id, wtype, title, x, y, w, h, config, source in widgets:
        cursor.execute("""
            INSERT INTO app_dashboard_widgets (widget_id, dashboard_id, widget_type, title, position_x, position_y, width, height, config, data_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (widget_id, dashboard_id, wtype, title, x, y, w, h, config, source))

    conn.commit()
    print("1 dashboard, 6 widgets")


def seed_alert_rules(conn):
    """Create default alert rules."""
    print("  Creating alert rules...", end=" ", flush=True)
    cursor = conn.cursor()

    # Get admin user
    cursor.execute("SELECT user_id FROM app_users WHERE email = 'admin@fleetai.local'")
    result = cursor.fetchone()
    if not result:
        print("skipped (no admin user)")
        return

    admin_id = result[0]

    cursor.execute("DELETE FROM app_alert_rules")

    alerts = [
        ('High Mileage Vehicles', 'Vehicles exceeding annual km allowance',
         'vehicle_mileage', "SELECT * FROM dim_vehicle WHERE current_odometer_km > annual_km_allowance", 'high'),
        ('Large Customers No Activity', 'Large customers with no recent billing',
         'customer_activity', "SELECT * FROM agg_customer_kpis WHERE total_vehicles > 10 AND total_monthly_lease_value = 0", 'medium'),
    ]

    for name, desc, alert_type, sql, severity in alerts:
        cursor.execute("""
            INSERT INTO app_alert_rules (name, description, alert_type, condition_sql, severity, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name, desc, alert_type, sql, severity, admin_id))

    conn.commit()
    print(f"{len(alerts)} rules")


def main():
    print("=" * 60)
    print("Seed: Application Layer")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    conn = get_connection()

    create_app_schema(conn)
    print()

    print("Seeding application data...")
    seed_roles(conn)
    seed_permissions(conn)
    seed_role_permissions(conn)
    seed_system_config(conn)
    seed_datasets(conn)
    seed_knowledge_base(conn)
    seed_sample_users(conn)
    seed_default_dashboard(conn)
    seed_alert_rules(conn)

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)

    cursor = conn.cursor()
    tables = [
        'app_roles', 'app_permissions', 'app_role_permissions',
        'app_users', 'app_system_config', 'app_datasets',
        'app_knowledge_base', 'app_dashboards', 'app_dashboard_widgets',
        'app_alert_rules'
    ]

    for table in tables:
        try:
            count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")
        except:
            print(f"  {table}: (not created)")

    print()
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    conn.close()


if __name__ == "__main__":
    main()
