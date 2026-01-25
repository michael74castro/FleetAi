"""
FleetAI - Excel Data Import Script
Imports test data from Excel files into SQLite database
"""

import os
import sys
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Set environment before importing app modules
os.environ.setdefault('DATABASE_TYPE', 'sqlite')
os.environ.setdefault('SECRET_KEY', 'local-dev-secret-key-change-in-production-12345678')

from app.core.database import Base, sync_engine
from app.models.fleet import (
    DimCustomer, DimVehicle, DimDriver, DimContract, DimDate,
    FactInvoice, FactFuel, FactMaintenance
)
from app.models.user import User, Role
from app.models.dashboard import Dashboard
from app.models.report import Report, Dataset

# Excel files directory
EXCEL_DIR = r"C:\Users\X1Carbon\Documents\FleetAI\Files"


def create_tables():
    """Create all database tables"""
    print("Creating database tables...")
    Base.metadata.create_all(sync_engine)
    print("Tables created successfully!")


def generate_date_dimension(start_year=2020, end_year=2026):
    """Generate date dimension data"""
    print(f"Generating date dimension ({start_year}-{end_year})...")

    Session = sessionmaker(bind=sync_engine)
    session = Session()

    try:
        dates = []
        current = date(start_year, 1, 1)
        end = date(end_year, 12, 31)
        key = 1

        while current <= end:
            dim_date = DimDate(
                date_key=key,
                full_date=current,
                day_of_week=current.weekday() + 1,
                day_name=current.strftime('%A'),
                day_of_month=current.day,
                day_of_year=current.timetuple().tm_yday,
                week_of_year=current.isocalendar()[1],
                month_number=current.month,
                month_name=current.strftime('%B'),
                month_short=current.strftime('%b'),
                quarter_number=(current.month - 1) // 3 + 1,
                quarter_name=f"Q{(current.month - 1) // 3 + 1}",
                year_number=current.year,
                fiscal_year=current.year if current.month >= 7 else current.year - 1,
                is_weekend=current.weekday() >= 5,
                is_holiday=False,
                is_business_day=current.weekday() < 5
            )
            session.add(dim_date)

            key += 1
            current = date.fromordinal(current.toordinal() + 1)

        session.commit()
        print(f"  Added {key - 1} date records")
    except Exception as e:
        session.rollback()
        print(f"  Error: {e}")
    finally:
        session.close()


def import_customers():
    """Import customer data from cccu.xlsx"""
    print("Importing customers from cccu.xlsx...")

    filepath = os.path.join(EXCEL_DIR, "cccu.xlsx")
    if not os.path.exists(filepath):
        print(f"  File not found: {filepath}")
        return

    df = pd.read_excel(filepath)
    print(f"  Read {len(df)} rows")

    Session = sessionmaker(bind=sync_engine)
    session = Session()

    try:
        for idx, row in df.iterrows():
            customer = DimCustomer(
                customer_key=idx + 1,
                customer_id=str(row.get('CUCUNO', idx + 1)),
                customer_name=str(row.get('CUCUDS', row.get('CUCUNM', f'Customer {idx + 1}'))),
                legal_name=str(row.get('CUCUNM', '')) if pd.notna(row.get('CUCUNM')) else None,
                account_type=str(row.get('CUTYCU', '')) if pd.notna(row.get('CUTYCU')) else None,
                billing_city=str(row.get('CUCUCI', '')) if pd.notna(row.get('CUCUCI')) else None,
                billing_country=str(row.get('CUCOUC', '')) if pd.notna(row.get('CUCOUC')) else None,
                status='Active',
                effective_from=date.today(),
                is_current=True
            )
            session.add(customer)

            if (idx + 1) % 500 == 0:
                session.flush()
                print(f"  Processed {idx + 1} customers...")

        session.commit()
        print(f"  Imported {len(df)} customers")
    except Exception as e:
        session.rollback()
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


def import_vehicles():
    """Import vehicle data from ccau.xlsx"""
    print("Importing vehicles from ccau.xlsx...")

    filepath = os.path.join(EXCEL_DIR, "ccau.xlsx")
    if not os.path.exists(filepath):
        print(f"  File not found: {filepath}")
        return

    df = pd.read_excel(filepath)
    print(f"  Read {len(df)} rows")

    Session = sessionmaker(bind=sync_engine)
    session = Session()

    try:
        for idx, row in df.iterrows():
            # Parse make/model from columns
            make = str(row.get('AUMKDS', '')) if pd.notna(row.get('AUMKDS')) else None
            model_desc = str(row.get('AUMDDS', '')) if pd.notna(row.get('AUMDDS')) else None

            # Extract model year from AUTYDS if available (e.g., "4.0L,4WD,2008,")
            model_year = None
            type_desc = str(row.get('AUTYDS', '')) if pd.notna(row.get('AUTYDS')) else ''
            for part in type_desc.split(','):
                part = part.strip()
                if part.isdigit() and len(part) == 4 and part.startswith('20'):
                    model_year = int(part)
                    break

            # Fuel type mapping
            fuel_code = row.get('AUFUCD')
            fuel_map = {1: 'Gasoline', 2: 'Diesel', 3: 'Electric', 4: 'Hybrid'}
            fuel_type = fuel_map.get(fuel_code, 'Gasoline')

            vehicle = DimVehicle(
                vehicle_key=idx + 1,
                equipment_id=str(row.get('AUMDCD', idx + 1)),
                make=make,
                model=model_desc.split(',')[0] if model_desc else None,
                model_year=model_year,
                fuel_type=fuel_type,
                status='Active',
                effective_from=date.today(),
                is_current=True
            )
            session.add(vehicle)

            if (idx + 1) % 1000 == 0:
                session.flush()
                print(f"  Processed {idx + 1} vehicles...")

        session.commit()
        print(f"  Imported {len(df)} vehicles")
    except Exception as e:
        session.rollback()
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


def import_drivers():
    """Import driver data from ccdr.xlsx"""
    print("Importing drivers from ccdr.xlsx...")

    filepath = os.path.join(EXCEL_DIR, "ccdr.xlsx")
    if not os.path.exists(filepath):
        print(f"  File not found: {filepath}")
        return

    df = pd.read_excel(filepath)
    print(f"  Read {len(df)} rows")

    Session = sessionmaker(bind=sync_engine)
    session = Session()

    try:
        for idx, row in df.iterrows():
            # Driver name handling
            first_name = str(row.get('DRDRFN', '')) if pd.notna(row.get('DRDRFN')) else None
            last_name = str(row.get('DRDRLN', '')) if pd.notna(row.get('DRDRLN')) else None
            full_name = str(row.get('DRDRNM', '')) if pd.notna(row.get('DRDRNM')) else None

            if not full_name and (first_name or last_name):
                full_name = f"{first_name or ''} {last_name or ''}".strip()

            driver = DimDriver(
                driver_key=idx + 1,
                driver_id=str(row.get('DRDRNO', idx + 1)),
                customer_id=str(row.get('DRCUNO', '')) if pd.notna(row.get('DRCUNO')) else None,
                first_name=first_name,
                last_name=last_name,
                full_name=full_name or f"Driver {idx + 1}",
                email=str(row.get('DRMAIL', '')) if pd.notna(row.get('DRMAIL')) else None,
                department=str(row.get('DRDEPT', '')) if pd.notna(row.get('DRDEPT')) else None,
                status='Active',
                effective_from=date.today(),
                is_current=True
            )
            session.add(driver)

            if (idx + 1) % 1000 == 0:
                session.flush()
                print(f"  Processed {idx + 1} drivers...")

        session.commit()
        print(f"  Imported {len(df)} drivers")
    except Exception as e:
        session.rollback()
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


def import_contracts():
    """Import contract data from ccco.xlsx"""
    print("Importing contracts from ccco.xlsx...")

    filepath = os.path.join(EXCEL_DIR, "ccco.xlsx")
    if not os.path.exists(filepath):
        print(f"  File not found: {filepath}")
        return

    df = pd.read_excel(filepath)
    print(f"  Read {len(df)} rows")

    Session = sessionmaker(bind=sync_engine)
    session = Session()

    try:
        for idx, row in df.iterrows():
            contract = DimContract(
                contract_key=idx + 1,
                contract_no=str(row.get('COCONO', idx + 1)),
                customer_id=str(row.get('COCUNO', '')) if pd.notna(row.get('COCUNO')) else None,
                contract_type=str(row.get('COTLCD', '')) if pd.notna(row.get('COTLCD')) else None,
                status='Active',
                effective_from=date.today(),
                is_current=True
            )
            session.add(contract)

            if (idx + 1) % 500 == 0:
                session.flush()
                print(f"  Processed {idx + 1} contracts...")

        session.commit()
        print(f"  Imported {len(df)} contracts")
    except Exception as e:
        session.rollback()
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


def create_sample_datasets():
    """Create sample dataset definitions for the report builder"""
    print("Creating sample datasets...")

    Session = sessionmaker(bind=sync_engine)
    session = Session()

    try:
        datasets = [
            Dataset(
                name='vehicles',
                display_name='Vehicles',
                description='Vehicle fleet inventory with make, model, and status',
                table_name='dim_vehicle',
                category='Fleet',
                is_active=True
            ),
            Dataset(
                name='customers',
                display_name='Customers',
                description='Customer accounts and billing information',
                table_name='dim_customer',
                category='Customers',
                is_active=True
            ),
            Dataset(
                name='drivers',
                display_name='Drivers',
                description='Driver information and assignments',
                table_name='dim_driver',
                category='Fleet',
                is_active=True
            ),
            Dataset(
                name='contracts',
                display_name='Contracts',
                description='Lease and service contracts',
                table_name='dim_contract',
                category='Contracts',
                is_active=True
            ),
        ]

        for ds in datasets:
            session.add(ds)

        session.commit()
        print(f"  Created {len(datasets)} datasets")
    except Exception as e:
        session.rollback()
        print(f"  Error: {e}")
    finally:
        session.close()


def print_summary():
    """Print summary of imported data"""
    print("\n" + "="*50)
    print("IMPORT SUMMARY")
    print("="*50)

    Session = sessionmaker(bind=sync_engine)
    session = Session()

    try:
        tables = [
            ('dim_date', 'Date Dimension'),
            ('dim_customer', 'Customers'),
            ('dim_vehicle', 'Vehicles'),
            ('dim_driver', 'Drivers'),
            ('dim_contract', 'Contracts'),
            ('datasets', 'Datasets'),
        ]

        for table, label in tables:
            try:
                result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"  {label}: {count:,} records")
            except Exception as e:
                print(f"  {label}: Error - {e}")
    finally:
        session.close()


def main():
    print("="*50)
    print("FleetAI Data Import Script")
    print("="*50)
    print(f"Excel directory: {EXCEL_DIR}")
    print()

    # Create tables
    create_tables()

    # Generate date dimension
    generate_date_dimension()

    # Import data
    import_customers()
    import_vehicles()
    import_drivers()
    import_contracts()

    # Create datasets
    create_sample_datasets()

    # Print summary
    print_summary()

    print("\nImport completed!")


if __name__ == "__main__":
    main()
