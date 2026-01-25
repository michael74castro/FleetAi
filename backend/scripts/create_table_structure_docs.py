"""
Create Excel documentation for database table structures
"""

import pandas as pd
import os

# Output directory
output_dir = r'C:\Users\X1Carbon\Documents\FleetAI\Files'

# Define table structures based on the fleet models
tables = {
    'dim_customer': {
        'description': 'Customer dimension table',
        'fields': [
            ('customer_key', 'INTEGER', 'Primary Key', 'Auto-generated surrogate key'),
            ('customer_id', 'VARCHAR(20)', 'Required', 'Business key - Customer ID from source'),
            ('customer_name', 'VARCHAR(200)', 'Required', 'Customer display name'),
            ('legal_name', 'VARCHAR(200)', 'Optional', 'Legal/registered company name'),
            ('account_type', 'VARCHAR(50)', 'Optional', 'Type of account (e.g., Corporate, SMB)'),
            ('parent_customer_id', 'VARCHAR(20)', 'Optional', 'Parent customer ID for hierarchies'),
            ('parent_customer_name', 'VARCHAR(200)', 'Optional', 'Parent customer name'),
            ('tax_id', 'VARCHAR(30)', 'Optional', 'Tax identification number'),
            ('industry', 'VARCHAR(100)', 'Optional', 'Industry classification'),
            ('employee_count_tier', 'VARCHAR(20)', 'Optional', 'Employee count range'),
            ('credit_rating', 'VARCHAR(20)', 'Optional', 'Credit rating code'),
            ('payment_terms', 'VARCHAR(50)', 'Optional', 'Payment terms (e.g., Net 30)'),
            ('account_manager', 'VARCHAR(50)', 'Optional', 'Assigned account manager'),
            ('region', 'VARCHAR(50)', 'Optional', 'Geographic region'),
            ('territory', 'VARCHAR(50)', 'Optional', 'Sales territory'),
            ('billing_city', 'VARCHAR(100)', 'Optional', 'Billing address city'),
            ('billing_state', 'VARCHAR(50)', 'Optional', 'Billing address state'),
            ('billing_country', 'VARCHAR(50)', 'Optional', 'Billing address country'),
            ('status', 'VARCHAR(20)', 'Optional', 'Customer status (Active, Inactive)'),
            ('created_date', 'DATE', 'Optional', 'Date customer was created'),
            ('effective_from', 'DATE', 'Required', 'SCD2 effective from date'),
            ('effective_to', 'DATE', 'Optional', 'SCD2 effective to date'),
            ('is_current', 'BOOLEAN', 'Required', 'True if current record'),
        ]
    },
    'dim_vehicle': {
        'description': 'Vehicle/Equipment dimension table',
        'fields': [
            ('vehicle_key', 'INTEGER', 'Primary Key', 'Auto-generated surrogate key'),
            ('equipment_id', 'VARCHAR(20)', 'Required', 'Business key - Equipment/Unit ID'),
            ('vin', 'VARCHAR(17)', 'Optional', 'Vehicle Identification Number'),
            ('license_plate', 'VARCHAR(20)', 'Optional', 'License plate number'),
            ('make', 'VARCHAR(50)', 'Optional', 'Vehicle make (e.g., Ford, Toyota)'),
            ('model', 'VARCHAR(50)', 'Optional', 'Vehicle model (e.g., F-150, Camry)'),
            ('model_year', 'INTEGER', 'Optional', 'Model year'),
            ('color', 'VARCHAR(30)', 'Optional', 'Vehicle color'),
            ('body_type', 'VARCHAR(30)', 'Optional', 'Body type (Sedan, SUV, Truck)'),
            ('engine_type', 'VARCHAR(30)', 'Optional', 'Engine type'),
            ('fuel_type', 'VARCHAR(20)', 'Optional', 'Fuel type (Gasoline, Diesel, Electric)'),
            ('transmission', 'VARCHAR(20)', 'Optional', 'Transmission type'),
            ('acquisition_date', 'DATE', 'Optional', 'Date vehicle was acquired'),
            ('acquisition_cost', 'DECIMAL(15,2)', 'Optional', 'Original acquisition cost'),
            ('residual_value', 'DECIMAL(15,2)', 'Optional', 'Estimated residual value'),
            ('status', 'VARCHAR(20)', 'Optional', 'Vehicle status'),
            ('effective_from', 'DATE', 'Required', 'SCD2 effective from date'),
            ('effective_to', 'DATE', 'Optional', 'SCD2 effective to date'),
            ('is_current', 'BOOLEAN', 'Required', 'True if current record'),
        ]
    },
    'dim_driver': {
        'description': 'Driver dimension table',
        'fields': [
            ('driver_key', 'INTEGER', 'Primary Key', 'Auto-generated surrogate key'),
            ('driver_id', 'VARCHAR(20)', 'Required', 'Business key - Driver ID'),
            ('customer_id', 'VARCHAR(20)', 'Optional', 'Associated customer ID'),
            ('first_name', 'VARCHAR(50)', 'Optional', 'Driver first name'),
            ('last_name', 'VARCHAR(50)', 'Optional', 'Driver last name'),
            ('full_name', 'VARCHAR(101)', 'Optional', 'Full name (computed or source)'),
            ('email', 'VARCHAR(100)', 'Optional', 'Email address'),
            ('department', 'VARCHAR(100)', 'Optional', 'Department name'),
            ('cost_center', 'VARCHAR(50)', 'Optional', 'Cost center code'),
            ('license_state', 'VARCHAR(10)', 'Optional', 'License issuing state'),
            ('license_expiry_date', 'DATE', 'Optional', 'License expiration date'),
            ('status', 'VARCHAR(20)', 'Optional', 'Driver status'),
            ('effective_from', 'DATE', 'Required', 'SCD2 effective from date'),
            ('effective_to', 'DATE', 'Optional', 'SCD2 effective to date'),
            ('is_current', 'BOOLEAN', 'Required', 'True if current record'),
        ]
    },
    'dim_contract': {
        'description': 'Contract dimension table',
        'fields': [
            ('contract_key', 'INTEGER', 'Primary Key', 'Auto-generated surrogate key'),
            ('contract_no', 'VARCHAR(20)', 'Required', 'Business key - Contract number'),
            ('customer_id', 'VARCHAR(20)', 'Optional', 'Customer ID'),
            ('contract_type', 'VARCHAR(50)', 'Optional', 'Contract type code'),
            ('start_date', 'DATE', 'Optional', 'Contract start date'),
            ('end_date', 'DATE', 'Optional', 'Contract end date'),
            ('term_months', 'INTEGER', 'Optional', 'Contract term in months'),
            ('monthly_rate', 'DECIMAL(15,2)', 'Optional', 'Monthly lease rate'),
            ('mileage_allowance', 'INTEGER', 'Optional', 'Annual mileage allowance'),
            ('excess_mileage_rate', 'DECIMAL(10,4)', 'Optional', 'Rate per excess mile'),
            ('insurance_included', 'BOOLEAN', 'Optional', 'Insurance included flag'),
            ('maintenance_included', 'BOOLEAN', 'Optional', 'Maintenance included flag'),
            ('status', 'VARCHAR(20)', 'Optional', 'Contract status'),
            ('effective_from', 'DATE', 'Required', 'SCD2 effective from date'),
            ('effective_to', 'DATE', 'Optional', 'SCD2 effective to date'),
            ('is_current', 'BOOLEAN', 'Required', 'True if current record'),
        ]
    },
    'dim_date': {
        'description': 'Date dimension table (auto-generated)',
        'fields': [
            ('date_key', 'INTEGER', 'Primary Key', 'YYYYMMDD format key'),
            ('full_date', 'DATE', 'Required', 'Full date value'),
            ('day_of_week', 'INTEGER', 'Optional', 'Day of week (1-7)'),
            ('day_name', 'VARCHAR(10)', 'Optional', 'Day name (Monday, etc.)'),
            ('day_of_month', 'INTEGER', 'Optional', 'Day of month (1-31)'),
            ('day_of_year', 'INTEGER', 'Optional', 'Day of year (1-366)'),
            ('week_of_year', 'INTEGER', 'Optional', 'Week number (1-53)'),
            ('month_number', 'INTEGER', 'Optional', 'Month number (1-12)'),
            ('month_name', 'VARCHAR(10)', 'Optional', 'Month name'),
            ('month_short', 'VARCHAR(3)', 'Optional', 'Month abbreviation'),
            ('quarter_number', 'INTEGER', 'Optional', 'Quarter (1-4)'),
            ('quarter_name', 'VARCHAR(6)', 'Optional', 'Quarter name (Q1, Q2)'),
            ('year_number', 'INTEGER', 'Optional', 'Year'),
            ('fiscal_year', 'INTEGER', 'Optional', 'Fiscal year'),
            ('is_weekend', 'BOOLEAN', 'Optional', 'Weekend flag'),
            ('is_holiday', 'BOOLEAN', 'Optional', 'Holiday flag'),
            ('is_business_day', 'BOOLEAN', 'Optional', 'Business day flag'),
        ]
    },
    'fact_invoices': {
        'description': 'Invoice fact table',
        'fields': [
            ('invoice_fact_id', 'INTEGER', 'Primary Key', 'Auto-generated surrogate key'),
            ('invoice_date_key', 'INTEGER', 'Required', 'FK to dim_date'),
            ('due_date_key', 'INTEGER', 'Optional', 'FK to dim_date'),
            ('customer_key', 'INTEGER', 'Required', 'FK to dim_customer'),
            ('contract_key', 'INTEGER', 'Optional', 'FK to dim_contract'),
            ('invoice_no', 'VARCHAR(30)', 'Required', 'Invoice number'),
            ('subtotal', 'DECIMAL(15,2)', 'Optional', 'Subtotal before tax'),
            ('tax_amount', 'DECIMAL(15,2)', 'Optional', 'Tax amount'),
            ('total_amount', 'DECIMAL(15,2)', 'Optional', 'Total invoice amount'),
            ('paid_amount', 'DECIMAL(15,2)', 'Optional', 'Amount paid'),
            ('balance_amount', 'DECIMAL(15,2)', 'Optional', 'Outstanding balance'),
            ('line_item_count', 'INTEGER', 'Optional', 'Number of line items'),
            ('days_outstanding', 'INTEGER', 'Optional', 'Days since invoice'),
            ('is_overdue', 'BOOLEAN', 'Optional', 'Overdue flag'),
        ]
    },
    'fact_fuel': {
        'description': 'Fuel consumption fact table',
        'fields': [
            ('fuel_fact_id', 'INTEGER', 'Primary Key', 'Auto-generated surrogate key'),
            ('transaction_date_key', 'INTEGER', 'Required', 'FK to dim_date'),
            ('customer_key', 'INTEGER', 'Optional', 'FK to dim_customer'),
            ('vehicle_key', 'INTEGER', 'Required', 'FK to dim_vehicle'),
            ('driver_key', 'INTEGER', 'Optional', 'FK to dim_driver'),
            ('gallons', 'DECIMAL(15,3)', 'Optional', 'Gallons/liters purchased'),
            ('amount', 'DECIMAL(15,2)', 'Optional', 'Transaction amount'),
            ('price_per_gallon', 'DECIMAL(10,4)', 'Optional', 'Price per unit'),
            ('odometer', 'INTEGER', 'Optional', 'Odometer reading'),
            ('miles_since_last_fill', 'INTEGER', 'Optional', 'Miles since last fill'),
            ('mpg', 'DECIMAL(10,2)', 'Optional', 'Miles per gallon'),
            ('fuel_type', 'VARCHAR(20)', 'Optional', 'Fuel type'),
            ('product_code', 'VARCHAR(20)', 'Optional', 'Product code'),
        ]
    },
    'fact_maintenance': {
        'description': 'Maintenance fact table',
        'fields': [
            ('maintenance_fact_id', 'INTEGER', 'Primary Key', 'Auto-generated surrogate key'),
            ('completed_date_key', 'INTEGER', 'Optional', 'FK to dim_date'),
            ('scheduled_date_key', 'INTEGER', 'Optional', 'FK to dim_date'),
            ('customer_key', 'INTEGER', 'Optional', 'FK to dim_customer'),
            ('vehicle_key', 'INTEGER', 'Required', 'FK to dim_vehicle'),
            ('work_order_no', 'VARCHAR(30)', 'Required', 'Work order number'),
            ('order_type', 'VARCHAR(50)', 'Optional', 'Type of maintenance'),
            ('priority', 'VARCHAR(20)', 'Optional', 'Priority level'),
            ('labor_hours', 'DECIMAL(10,2)', 'Optional', 'Labor hours'),
            ('labor_cost', 'DECIMAL(15,2)', 'Optional', 'Labor cost'),
            ('parts_cost', 'DECIMAL(15,2)', 'Optional', 'Parts cost'),
            ('total_cost', 'DECIMAL(15,2)', 'Optional', 'Total cost'),
            ('estimated_cost', 'DECIMAL(15,2)', 'Optional', 'Estimated cost'),
            ('days_to_complete', 'INTEGER', 'Optional', 'Days to complete'),
            ('is_scheduled', 'BOOLEAN', 'Optional', 'Scheduled maintenance flag'),
            ('is_completed', 'BOOLEAN', 'Optional', 'Completed flag'),
        ]
    },
}


def main():
    print(f"Creating table structure documentation in: {output_dir}")
    print("=" * 60)

    # Create Excel files for each table
    for table_name, table_info in tables.items():
        df = pd.DataFrame(
            table_info['fields'],
            columns=['Field Name', 'Data Type', 'Constraint', 'Description']
        )

        # Add source mapping columns for user to fill in
        df['iSeries Source File'] = ''
        df['iSeries Field Name'] = ''
        df['Transformation Rules'] = ''

        filepath = os.path.join(output_dir, f'{table_name}_structure.xlsx')

        # Create Excel with formatting
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Field Mapping', index=False)

            # Add description sheet
            desc_df = pd.DataFrame({
                'Property': ['Table Name', 'Description', 'Source System'],
                'Value': [table_name, table_info['description'], 'iSeries / AS400']
            })
            desc_df.to_excel(writer, sheet_name='Table Info', index=False)

        print(f"Created: {table_name}_structure.xlsx")

    print("=" * 60)
    print(f"Created {len(tables)} Excel files")
    print("\nPlease fill in the following columns in each file:")
    print("  - iSeries Source File: The source Excel/table name (e.g., cccu.xlsx)")
    print("  - iSeries Field Name: The source column name (e.g., CUCUNO)")
    print("  - Transformation Rules: Any data transformation needed")


if __name__ == "__main__":
    main()
