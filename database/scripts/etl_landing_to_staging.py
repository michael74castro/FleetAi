"""
ETL Script: Landing to Staging
Transforms and loads data from landing tables to staging tables
"""

import sqlite3
import hashlib
from datetime import datetime

DB_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\fleetai.db"
SCHEMA_FILE = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\schemas\02_staging_sqlite.sql"


def get_connection():
    """Get database connection."""
    return sqlite3.connect(DB_PATH)


def create_staging_schema(conn):
    """Create staging tables from schema file."""
    print("Creating staging schema...")
    with open(SCHEMA_FILE, 'r') as f:
        schema_sql = f.read()

    cursor = conn.cursor()
    cursor.executescript(schema_sql)
    conn.commit()
    print("  Staging schema created.")


def log_etl_start(conn, table_name, load_type):
    """Log ETL start."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO staging_etl_log (table_name, load_type, load_start, status)
        VALUES (?, ?, datetime('now'), 'RUNNING')
    """, (table_name, load_type))
    conn.commit()
    return cursor.lastrowid


def log_etl_end(conn, log_id, source_rows, inserted_rows, updated_rows=0, status='SUCCESS', error=None):
    """Log ETL completion."""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE staging_etl_log
        SET load_end = datetime('now'), source_rows = ?, inserted_rows = ?,
            updated_rows = ?, status = ?, error_message = ?
        WHERE log_id = ?
    """, (source_rows, inserted_rows, updated_rows, status, error, log_id))
    conn.commit()


def parse_date_components(cc, yy, mm, dd):
    """Parse date from century, year, month, day components."""
    if not cc or not yy or cc == 0:
        return None
    try:
        year = int(cc) * 100 + int(yy)
        month = int(mm) if mm else 1
        day = int(dd) if dd else 1
        if year < 1900 or year > 2100 or month < 1 or month > 12 or day < 1 or day > 31:
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"
    except:
        return None


def load_customers(conn):
    """Load customers from landing to staging."""
    print("  Loading customers...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_customers', 'FULL')

    try:
        cursor = conn.cursor()

        # Clear existing data
        cursor.execute("DELETE FROM staging_customers")

        # Transform and load - using documented column names
        cursor.execute("""
            INSERT INTO staging_customers (
                customer_id, customer_name, customer_name_2, customer_name_3,
                call_name, company_code, address, city, country_code,
                phone, fax, account_manager, report_period, country,
                fleet_code, cv_code, nk_code, source_hash
            )
            SELECT
                CCCU_CUNULO_Customer_Number,
                CCCU_CUNOLO_Name,
                CCCU_CUNOL2_Name_Cont,
                CCCU_CUNOL3_Name_Cont,
                CCCU_CUCLLO_Language_Code,
                CCCU_CUCPLO_Country_Code,
                CCCU_CUADLO_Address,
                CCCU_CULOLO_Locality,
                CCCU_CUCPLO_Country_Code,
                CCCU_CUNTLO_Telephone_Number,
                CCCU_CUNFLO_Fax_Number,
                CCCU_CURSLO_Account_Manager,
                CCCU_CURPPD_Reporting_Period,
                CCCU_CUCOUC_Country_Code,
                CCCU_CUFLCD,
                CCCU_CUCVLO,
                CCCU_CUNKLO,
                NULL
            FROM landing_CCCU
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCCU").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_customers").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_drivers(conn):
    """Load drivers from landing to staging."""
    print("  Loading drivers...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_drivers', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_drivers")

        # Using documented column names for clarity
        cursor.execute("""
            INSERT INTO staging_drivers (
                object_no, driver_no, active_driver, driver_name,
                first_name, last_name, address, city, country_code,
                private_phone, office_phone, mobile_phone, email,
                report_period, country, source_hash
            )
            SELECT
                CCDR_DROBNO_Object_Number,
                CCDR_DRDRNO_Driver_Number,
                CCDR_DRACDR_Actual_Driver_Code,
                CCDR_DRDRNM_Driver_Name,
                CCDR_DRDRFN_Driver_First_Name,
                CCDR_DRDRLN_Driver_Last_Name,
                CCDR_DRDRAD_Driver_Address,
                CCDR_DRDRLO_Driver_Locality,
                CCDR_DRDRLA_Driver_Country_Code,
                CCDR_DRPVTF_Private_Telephone,
                CCDR_DROFTF_Office_Telephone,
                CCDR_DRPVNF_Mobile_Phone,
                CCDR_DRDRNE_Email,
                CCDR_DRRPPD_Reporting_Period,
                CCDR_DRCOUC_Country_Code,
                NULL
            FROM landing_CCDR
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCDR").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_drivers").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_vehicles(conn):
    """Load vehicles from landing to staging."""
    print("  Loading vehicles...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_vehicles', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_vehicles")

        # Get automobile reference data for make/model names
        # Using documented column names for self-documenting ETL
        cursor.execute("""
            INSERT INTO staging_vehicles (
                object_no, vin, make_code, model_code, customer_no,
                pc_no, contract_no, contract_position_no, color_code, color_name,
                body_type_code, registration_no, purchase_price, residual_value,
                lease_duration_months, lease_amount, km_allowance, km_budget,
                current_km, fuel_code, lease_type, license_plate_code,
                group_no, order_no, sale_price, report_period, country,
                volume_unit, distance_unit, consumption_unit, currency,
                object_status,
                source_hash
            )
            SELECT
                ob.CCOB_OBOBNO_Object_Number,
                ob.CCOB_OBOBCX_Chassis_Number,
                ob.CCOB_OBMKCD_Make_Code,
                ob.CCOB_OBMDCD_Model_Code,
                ob.CCOB_OBCUNO_Customer_Number,
                ob.CCOB_OBPCNO_Profit_Centre_Number,
                ob.CCOB_OBCONO_Contract_Number,
                ob.CCOB_OBCPNO_Calculation_Number,
                ob.CCOB_OBOBUC_Upholstery_Color,
                ob.CCOB_OBOBEC_Exterior_Color,
                ob.CCOB_OBOBBC_Object_Begin_Date_Century,
                ob.CCOB_OBRGNO_Registration_Number,
                ob.CCOB_OBPRIC_Monthly_Rental,
                ob.CCOB_OBFSRV_Budgeted_Residual_Value,
                ob.CCOB_OBFSDU_Budgeted_Months,
                ob.CCOB_OBFSAM_Investment_Amount,
                ob.CCOB_OBKMDU_Total_Budgeted_Mileage,
                ob.CCOB_OBKMBU_Budgeted_Mileage_Per_Year,
                ob.CCOB_OBFSKM_Last_Mileage_Known,
                ob.CCOB_OBFUCD_Fuel_Code,
                ob.CCOB_OBTLCD_Contract_Type,
                ob.CCOB_OBLPCD_Company_Code,
                ob.CCOB_OBGRNO_Client_Group_Number,
                ob.CCOB_OBORNO_Order_Number,
                ob.CCOB_OBSLPR_Final_Sales_Price,
                ob.CCOB_OBRPPD_Reporting_Period,
                ob.CCOB_OBCOUC_Country_Code,
                ob.CCOB_OBVOLC_Volume_Measurement,
                ob.CCOB_OBDISC_Distance_Measurement,
                ob.CCOB_OBCONC_Consumption_Measurement,
                ob.CCOB_OBCURC_Currency_Unit,
                ob.CCOB_OBOBSC_Object_Status,  -- 1=Active, >1=Terminated
                NULL
            FROM landing_CCOB ob
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCOB").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_vehicles").fetchone()[0]

        # Update make/model names from automobile reference
        cursor.execute("""
            UPDATE staging_vehicles
            SET make_name = (
                SELECT au.CCAU_AUMKDS_Make_Description
                FROM landing_CCAU au
                WHERE au.CCAU_AUMKCD_Make_Code = staging_vehicles.make_code
                LIMIT 1
            ),
            model_name = (
                SELECT au.CCAU_AUMDDS_Model_Description
                FROM landing_CCAU au
                WHERE au.CCAU_AUMKCD_Make_Code = staging_vehicles.make_code
                  AND au.CCAU_AUMDCD_Model_Code = staging_vehicles.model_code
                LIMIT 1
            )
        """)
        conn.commit()

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_contracts(conn):
    """Load contracts from landing to staging."""
    print("  Loading contracts...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_contracts', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_contracts")

        # Using documented column names
        cursor.execute("""
            INSERT INTO staging_contracts (
                contract_position_no, group_no, customer_no, pc_no, contract_no,
                active_cp, delivery_days, duration_months, km_per_year,
                purchase_price, residual_value, total_amount, invoice_base,
                interest_rate, make_code, model_code,
                monthly_rate_admin, monthly_rate_fuel, monthly_rate_depreciation,
                monthly_rate_insurance, monthly_rate_interest, monthly_rate_maintenance,
                monthly_rate_replacement, monthly_rate_road_tax, monthly_rate_tires,
                monthly_rate_total, unit_rate_maintenance, unit_rate_tires,
                unit_rate_replacement, excess_km_rate, report_period, country,
                lease_type, source_hash
            )
            SELECT
                CCCP_CPCPNO_Contract_Position_Number,
                CCCP_CPGNLO_Group_Name,
                CCCP_CPCUNO_Customer_Number,
                CCCP_CPPCNO_Profit_Center_Number,
                CCCP_CPCONO_Contract_Number,
                CCCP_CPACCP_Active_Contract_Position,
                CCCP_CPCPDL_Delivery_Days,
                CCCP_CPCPDU_Contract_Duration_Months,
                CCCP_CPCPMI_Km_Per_Year,
                CCCP_CPCPPU_Purchase_Price,
                CCCP_CPCPRV_Residual_Value,
                CCCP_CPCPTA_Total_Amount,
                CCCP_CPINBS,
                CCCP_CPIRPT_Interest_Rate_Percent,
                CCCP_CPMKCD_Make_Code,
                CCCP_CPMDCD_Model_Code,
                CCCP_CPMRAA_Monthly_Rate_Admin,
                CCCP_CPMRAF_Monthly_Rate_Fuel,
                CCCP_CPMRDP_Monthly_Rate_Depreciation,
                CCCP_CPMRIN_Monthly_Rate_Insurance,
                CCCP_CPMRIR_Monthly_Rate_Interest,
                CCCP_CPMRMF_Monthly_Rate_Maintenance,
                CCCP_CPMRRA_Monthly_Rate_Replacement,
                CCCP_CPMRRO_Monthly_Rate_Road_Tax,
                CCCP_CPMRVI_Monthly_Rate_Tires,
                CCCP_CPMTCT_Monthly_Total_Cost,
                CCCP_CPUNMA_Unit_Rate_Maintenance,
                CCCP_CPUNTR_Unit_Rate_Tires,
                CCCP_CPUNRC_Unit_Rate_Replacement,
                CCCP_CPXMPT_Excess_Km_Rate,
                CCCP_CPRPPD_Reporting_Period,
                CCCP_CPCOUC_Country_Code,
                CCCP_CPLPCD_Lease_Type_Code,
                NULL
            FROM landing_CCCP
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCCP").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_contracts").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_orders(conn):
    """Load orders from landing to staging."""
    print("  Loading orders...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_orders', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_orders")

        # Using documented column names
        cursor.execute("""
            INSERT INTO staging_orders (
                order_no, contract_position_no, group_no, customer_no,
                pc_no, contract_no, exterior_color, interior_color, location,
                order_status_code, supplier_no, supplier_amount,
                report_period, country, source_hash
            )
            SELECT
                CCOR_ORORNO_Order_Number,
                CCOR_ORCPNO_Contract_Position_Number,
                CCOR_ORGNLO_Group_Name,
                CCOR_ORCUNO_Customer_Number,
                CCOR_ORPCNO_Profit_Center_Number,
                CCOR_ORCONO_Contract_Number,
                CCOR_ORECN1,
                CCOR_ORUCN1,
                CCOR_ORECN2,
                CCOR_ORORSC_Order_Status_Code,
                CCOR_ORNUFO_Supplier_Number,
                CCOR_ORNAFO_Supplier_Amount,
                CCOR_ORRPPD_Reporting_Period,
                CCOR_ORCOUC_Country_Code,
                NULL
            FROM landing_CCOR
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCOR").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_orders").fetchone()[0]

        # Update dates - Note: date columns not renamed, using original names
        cursor.execute("""
            UPDATE staging_orders
            SET order_date = (
                SELECT printf('%04d-%02d-%02d',
                    CCOR_ORORCC * 100 + CCOR_ORORYY,
                    CCOR_ORORMM, CCOR_ORORDD)
                FROM landing_CCOR
                WHERE landing_CCOR.CCOR_ORORNO_Order_Number = staging_orders.order_no
                AND CCOR_ORORCC > 0
            )
        """)
        conn.commit()

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_billing(conn):
    """Load billing from landing to staging."""
    print("  Loading billing...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_billing', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_billing")

        # Using documented column names
        cursor.execute("""
            INSERT INTO staging_billing (
                billing_no, object_no, billing_run_no, billing_owner,
                billing_name, billing_address, billing_city, billing_method,
                billing_amount_1, billing_amount_2, billing_amount_monthly,
                billing_amount_variable, billing_amount_period, currency,
                report_period, country, source_hash
            )
            SELECT
                CCBI_BIBINO_Billing_Number,
                CCBI_BIOBNO_Object_Number,
                CCBI_BIBIRN_Billing_Run_Number,
                CCBI_BIBIAO_Billing_Account_Owner,
                CCBI_BIBINM_Billing_Name,
                CCBI_BIBIAD_Billing_Address,
                CCBI_BIBILO_Billing_Locality,
                CCBI_BIBICM_Billing_Method,
                CCBI_BIBIA1_Billing_Amount_1,
                CCBI_BIBIA2,
                CCBI_BIBIAM_Billing_Amount_Monthly,
                CCBI_BIBIAV_Billing_Amount_Variable,
                CCBI_BIBIPL_Billing_Period_Length,
                CCBI_BICURC_Currency_Code,
                CCBI_BIRPPD_Reporting_Period,
                CCBI_BICOUC_Country_Code,
                NULL
            FROM landing_CCBI
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCBI").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_billing").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_automobiles(conn):
    """Load automobile reference data from landing to staging."""
    print("  Loading automobiles...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_automobiles', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_automobiles")

        # Using documented column names
        cursor.execute("""
            INSERT INTO staging_automobiles (
                make_code, make_name, model_code, model_name,
                model_group_high, model_group_range, model_group_size,
                type_description, report_period, country, source_hash
            )
            SELECT
                CCAU_AUMKCD_Make_Code,
                CCAU_AUMKDS_Make_Description,
                CCAU_AUMDCD_Model_Code,
                CCAU_AUMDDS_Model_Description,
                CCAU_AUMDGH_Model_Group_High,
                CCAU_AUMDGR_Model_Group_Range,
                CCAU_AUMDGS_Model_Group_Size,
                CCAU_AUTYDS_Type_Description,
                CCAU_AURPPD_Reporting_Period,
                CCAU_AUCOUC_Country_Code,
                NULL
            FROM landing_CCAU
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCAU").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_automobiles").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_groups(conn):
    """Load groups from landing to staging."""
    print("  Loading groups...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_groups', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_groups")

        # Using documented column names
        cursor.execute("""
            INSERT INTO staging_groups (
                group_no, customer_no, group_name, report_period, country, source_hash
            )
            SELECT
                CCGR_GRGNLO_Group_Number,
                CCGR_GRNKLO_Customer_Number,
                CCGR_GRNOLO_Group_Name,
                CCGR_GRRPPD_Reporting_Period,
                CCGR_GRCOUC_Country_Code,
                NULL
            FROM landing_CCGR
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCGR").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_groups").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_odometer_history(conn):
    """Load odometer/maintenance history from landing to staging."""
    print("  Loading odometer history...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_odometer_history', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_odometer_history")

        # Using documented column names
        cursor.execute("""
            INSERT INTO staging_odometer_history (
                object_no, sequence_no, reading_date, odometer_km, amount,
                description, source_code, supplier_no, supplier_ref,
                transaction_no, transaction_make, transaction_spec, source_hash
            )
            SELECT
                CCGD_MAOBNO_Object_Number,
                CCGD_MAGDSQ_Mileage_Sequence,
                CASE WHEN CCGD_MAGDCC_Mileage_Date_Century > 0 THEN
                    printf('%04d-%02d-%02d',
                        CCGD_MAGDCC_Mileage_Date_Century * 100 + CCGD_MAGDYY_Mileage_Date_Year,
                        CCGD_MAGDMM_Mileage_Date_Month, CCGD_MAGDDD_Mileage_Date_Day)
                ELSE NULL END,
                CCGD_MAGDKM_Mileage_Km,
                CCGD_MAGDAM_Mileage_Amount,
                CCGD_MAGDDS_Mileage_Description,
                CCGD_MAGDSC_Mileage_Source_Code,
                CCGD_MANUFO_Supplier_Number,
                CCGD_MANAFO_Supplier_Reference,
                CCGD_MAGDTNR,
                CCGD_MAGDTMK,
                CCGD_MAGDTSP,
                NULL
            FROM landing_CCGD
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCGD").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_odometer_history").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def main():
    print("=" * 60)
    print("ETL: Landing to Staging")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    conn = get_connection()

    # Create staging schema
    create_staging_schema(conn)
    print()

    # Load tables
    print("Loading staging tables...")
    load_customers(conn)
    load_drivers(conn)
    load_automobiles(conn)
    load_vehicles(conn)
    load_contracts(conn)
    load_orders(conn)
    load_billing(conn)
    load_groups(conn)
    load_odometer_history(conn)

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name, source_rows, inserted_rows, status
        FROM staging_etl_log
        WHERE load_start >= datetime('now', '-1 hour')
        ORDER BY log_id
    """)

    total_rows = 0
    for row in cursor.fetchall():
        status_icon = "OK" if row[3] == 'SUCCESS' else "FAILED"
        print(f"  {row[0]}: {row[2]:,} rows [{status_icon}]")
        total_rows += row[2] or 0

    print(f"\nTotal rows loaded: {total_rows:,}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    conn.close()


if __name__ == "__main__":
    main()
