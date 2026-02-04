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
                CCCU_CUNULO_Client_Number,
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
                registration_no, purchase_price, residual_value,
                lease_duration_months, lease_amount, km_allowance, km_budget,
                current_km, fuel_code, lease_type, license_plate_code,
                group_no, order_no, sale_price, report_period, country,
                volume_unit, distance_unit, consumption_unit, currency,
                object_status,
                lease_start_date, lease_end_date, last_km_date,
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
                ob.CCOB_OBOBSC_Object_Status,  -- 0=Created, 1=Active, 2-9=Terminated stages
                -- lease_start_date from Object Begin Date (century/year/month/day)
                CASE WHEN ob.CCOB_OBOBBC_Object_Begin_Date_Century > 0
                     AND ob.CCOB_OBOBBM_Object_Begin_Date_Month BETWEEN 1 AND 12
                     AND ob.CCOB_OBOBBD_Object_Begin_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    ob.CCOB_OBOBBC_Object_Begin_Date_Century * 100 + ob.CCOB_OBOBBY_Object_Begin_Date_Year,
                    ob.CCOB_OBOBBM_Object_Begin_Date_Month, ob.CCOB_OBOBBD_Object_Begin_Date_Day)
                ELSE NULL END,
                -- lease_end_date from Object End Date (century/year/month/day)
                CASE WHEN ob.CCOB_OBFSEC_Object_End_Date_Century > 0
                     AND ob.CCOB_OBFSEM_Object_End_Date_Month BETWEEN 1 AND 12
                     AND ob.CCOB_OBFSED_Object_End_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    ob.CCOB_OBFSEC_Object_End_Date_Century * 100 + ob.CCOB_OBFSEY_Object_End_Date_Year,
                    ob.CCOB_OBFSEM_Object_End_Date_Month, ob.CCOB_OBFSED_Object_End_Date_Day)
                ELSE NULL END,
                -- last_km_date from Mileage Date (century/year/month/day)
                CASE WHEN ob.CCOB_OBKMCC_Mileage_Date_Century > 0
                     AND ob.CCOB_OBKMMM_Mileage_Date_Month BETWEEN 1 AND 12
                     AND ob.CCOB_OBKMDD_Mileage_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    ob.CCOB_OBKMCC_Mileage_Date_Century * 100 + ob.CCOB_OBKMYY_Mileage_Date_Year,
                    ob.CCOB_OBKMMM_Mileage_Date_Month, ob.CCOB_OBKMDD_Mileage_Date_Day)
                ELSE NULL END,
                NULL
            FROM landing_CCOB ob
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCOB").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_vehicles").fetchone()[0]

        # Compute expected_end_date, months_driven, months_remaining
        cursor.execute("""
            UPDATE staging_vehicles
            SET expected_end_date = date(lease_start_date, '+' || lease_duration_months || ' months'),
                months_driven = CAST((julianday('now') - julianday(lease_start_date)) / 30.44 AS INTEGER),
                months_remaining = CAST((julianday(date(lease_start_date, '+' || lease_duration_months || ' months')) - julianday('now')) / 30.44 AS INTEGER)
            WHERE lease_start_date IS NOT NULL AND lease_duration_months IS NOT NULL
        """)
        conn.commit()

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
                lease_type, start_date, source_hash
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
                -- start_date from Calculation Date (century/year/month/day)
                CASE WHEN CCCP_CPCPCC > 0
                     AND CCCP_CPCPMM BETWEEN 1 AND 12
                     AND CCCP_CPCPDD BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCCP_CPCPCC * 100 + CCCP_CPCPYY,
                    CCCP_CPCPMM, CCCP_CPCPDD)
                ELSE NULL END,
                NULL
            FROM landing_CCCP
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCCP").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_contracts").fetchone()[0]

        # Compute end_date from start_date + duration_months
        cursor.execute("""
            UPDATE staging_contracts
            SET end_date = date(start_date, '+' || duration_months || ' months')
            WHERE start_date IS NOT NULL AND duration_months IS NOT NULL
        """)
        conn.commit()

        # Compute months_driven, months_remaining
        cursor.execute("""
            UPDATE staging_contracts
            SET months_driven = CAST((julianday('now') - julianday(start_date)) / 30.44 AS INTEGER),
                months_remaining = CAST((julianday(end_date) - julianday('now')) / 30.44 AS INTEGER)
            WHERE start_date IS NOT NULL AND end_date IS NOT NULL
        """)
        conn.commit()

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
                order_status_code, previous_object_no, supplier_no, supplier_amount,
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
                NULLIF(CCOR_ORPVOB, 0),  -- NULL if 0 (no previous vehicle)
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

        # Derive order_status text from order_status_code
        cursor.execute("""
            UPDATE staging_orders
            SET order_status = CASE order_status_code
                WHEN 0 THEN 'Created'
                WHEN 1 THEN 'Sent to Dealer'
                WHEN 2 THEN 'Delivery Confirmed'
                WHEN 3 THEN 'Insurance Arranged'
                WHEN 4 THEN 'Registration Arranged'
                WHEN 5 THEN 'Driver Pack Prepared'
                WHEN 6 THEN 'Vehicle Delivered'
                WHEN 7 THEN 'Lease Schedule Generated'
                WHEN 9 THEN 'Cancelled'
                ELSE 'Unknown'
            END
        """)
        conn.commit()

        # Populate actual_delivery_date from CCOR_ORDLCC/YY/MM/DD
        cursor.execute("""
            UPDATE staging_orders
            SET actual_delivery_date = (
                SELECT CASE WHEN CCOR_ORDLCC > 0
                            AND CCOR_ORDLMM BETWEEN 1 AND 12
                            AND CCOR_ORDLDD BETWEEN 1 AND 31
                       THEN printf('%04d-%02d-%02d',
                           CCOR_ORDLCC * 100 + CCOR_ORDLYY,
                           CCOR_ORDLMM, CCOR_ORDLDD)
                       ELSE NULL END
                FROM landing_CCOR
                WHERE landing_CCOR.CCOR_ORORNO_Order_Number = staging_orders.order_no
            )
        """)

        # Populate requested_delivery_date from CCOR_ORDLQC/QY/QM/QD
        cursor.execute("""
            UPDATE staging_orders
            SET requested_delivery_date = (
                SELECT CASE WHEN (CCOR_ORDLQC > 0 OR CCOR_ORDLQY > 0)
                            AND CCOR_ORDLQM BETWEEN 1 AND 12
                            AND CCOR_ORDLQD BETWEEN 1 AND 31
                       THEN printf('%04d-%02d-%02d',
                           CASE WHEN CCOR_ORDLQC > 0 THEN CCOR_ORDLQC ELSE 20 END * 100 + CCOR_ORDLQY,
                           CCOR_ORDLQM, CCOR_ORDLQD)
                       ELSE NULL END
                FROM landing_CCOR
                WHERE landing_CCOR.CCOR_ORORNO_Order_Number = staging_orders.order_no
            )
        """)

        # Populate confirmed_delivery_date from CCOR_OROKCC/YY/MM/DD
        cursor.execute("""
            UPDATE staging_orders
            SET confirmed_delivery_date = (
                SELECT CASE WHEN (CCOR_OROKCC > 0 OR CCOR_OROKYY > 0)
                            AND CCOR_OROKMM BETWEEN 1 AND 12
                            AND CCOR_OROKDD BETWEEN 1 AND 31
                       THEN printf('%04d-%02d-%02d',
                           CASE WHEN CCOR_OROKCC > 0 THEN CCOR_OROKCC ELSE 20 END * 100 + CCOR_OROKYY,
                           CCOR_OROKMM, CCOR_OROKDD)
                       ELSE NULL END
                FROM landing_CCOR
                WHERE landing_CCOR.CCOR_ORORNO_Order_Number = staging_orders.order_no
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


def load_damages(conn):
    """Load damages from landing to staging."""
    print("  Loading damages...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_damages', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_damages")

        # Using documented column names from CCDA (Damages by Object Number)
        cursor.execute("""
            INSERT INTO staging_damages (
                damage_id, object_no, driver_no, damage_date,
                description, damage_amount,
                accident_location_address, accident_country_code,
                mileage, damage_type, fault_code, damage_status_code,
                damage_recourse, total_loss_code, country_code,
                reporting_period, insurance_co_number, third_party_name,
                repair_days, amount_own_risk, amount_refunded,
                claimed_deductible_repair, refunded_deductible_repair,
                salvage_amount, damage_fault_level, garage_name,
                source_hash
            )
            SELECT
                CCDA_DADANO_Damage_Number,
                CCDA_DAOBNO_Object_Number,
                CCDA_DADADR_Driver_Number,
                -- Parse damage date from century/year/month/day components
                CASE WHEN CCDA_DADACC_Damage_Date_Century > 0
                     AND CCDA_DADAMM_Damage_Date_Month BETWEEN 1 AND 12
                     AND CCDA_DADADD_Damage_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCDA_DADACC_Damage_Date_Century * 100 + CCDA_DADAYY_Damage_Date_Year,
                    CCDA_DADAMM_Damage_Date_Month, CCDA_DADADD_Damage_Date_Day)
                ELSE NULL END,
                -- Concatenate description fields
                TRIM(
                    COALESCE(CCDA_DADADS_Accident_Description_1, '') || ' ' ||
                    COALESCE(CCDA_DADAD2_Accident_Description_2, '') || ' ' ||
                    COALESCE(CCDA_DADAD3_Accident_Description_3, '') || ' ' ||
                    COALESCE(CCDA_DADAD4_Accident_Description_4, '') || ' ' ||
                    COALESCE(CCDA_DADAD5_Accident_Description_5, '')
                ),
                CCDA_DADAAM_Damage_Repair_Amount,
                CCDA_DADAAD_Accident_Location_Address,
                CCDA_DADALA_Accident_Country_Code,
                CCDA_DADAMI_Damage_Mileage,
                CCDA_DADATY_Damage_Type,
                CCDA_DADAFF_Fault_Code,
                CCDA_DADASC_Damage_Status_Code,
                CCDA_DASTCD_Damage_Recourse,
                CCDA_DATOCD_Total_Loss_Code,
                CCDA_DACOUC_Country_Code,
                CCDA_DARPPD_Reporting_Period,
                CCDA_DAINCN_Insurance_Co_Number,
                CCDA_DATPNM_Third_Party_Name,
                CCDA_DAREPD_Number_Repair_Days,
                CCDA_DADAMO_Amount_Own_Risk,
                CCDA_DADAMR_Amount_Refunded,
                CCDA_DADARP_Claimed_Deductible_Repair,
                CCDA_DARERP_Refunded_Deductible_Repair,
                CCDA_DASALD_Saldo,
                CCDA_DADAFL_Damage_Fault_Level,
                CCDA_DARSGA_Garage_Name,
                NULL
            FROM landing_CCDA
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCDA").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_damages").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_domain_translations(conn):
    """Load domain translations from landing to staging."""
    print("  Loading domain translations...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_domain_translations', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_domain_translations")

        cursor.execute("""
            INSERT INTO staging_domain_translations (
                country_code, domain_id, domain_value, language_code, domain_text
            )
            SELECT
                CCDT_DTCYCD_Country_Code,
                CCDT_DTDMID_Domain_ID,
                CCDT_DTDMVA_Domain_Value,
                CCDT_DTDMLN_Language_Code,
                CCDT_DTDMTX_Domain_Text
            FROM landing_CCDT
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCDT").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_domain_translations").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_exploitation_services(conn):
    """Load exploitation services from landing to staging."""
    print("  Loading exploitation services...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_exploitation_services', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_exploitation_services")

        cursor.execute("""
            INSERT INTO staging_exploitation_services (
                customer_no, contract_position_no, object_no, service_sequence,
                service_code, service_cost_total, service_invoice,
                invoice_supplier, total_monthly_cost, total_monthly_invoice,
                lp_code, reporting_period, country_code, volume_code,
                distance_code, consumption_code, currency_code
            )
            SELECT
                CCES_ESCUNO_Customer_No,
                CCES_ESPCNO_Contract_Position_No,
                CCES_ESOBNO_Object_No,
                CCES_ESESSQ_Service_Sequence,
                CCES_ESESCD_Service_Code,
                CCES_ESESCT_Service_Cost_Total,
                CCES_ESESIV_Service_Invoice,
                CCES_ESIVSU_Invoice_Supplier,
                CCES_ESTMCT_Total_Monthly_Cost,
                CCES_ESTMIV_Total_Monthly_Invoice,
                CCES_ESLPCD_LP_Code,
                CCES_ESRPPD_Reporting_Period,
                CCES_ESCOUC_Country_Code,
                CCES_ESVOLC_Volume_Code,
                CCES_ESDISC_Distance_Code,
                CCES_ESCONC_Consumption_Code,
                CCES_ESCURC_Currency_Code
            FROM landing_CCES
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCES").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_exploitation_services").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_maintenance_approvals(conn):
    """Load maintenance approvals from landing to staging."""
    print("  Loading maintenance approvals...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_maintenance_approvals', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_maintenance_approvals")

        cursor.execute("""
            INSERT INTO staging_maintenance_approvals (
                object_no, sequence, approval_date, date_from,
                mileage_km, amount, description, description_2, description_3,
                source_code, maintenance_type, supplier_no, supplier_branch,
                major_code, minor_code, reporting_period, country_code,
                volume_code, distance_code, consumption_code, currency_code,
                si_run_no
            )
            SELECT
                CCMS_MSOBNO_Object_No,
                CCMS_MSGDSQ_Sequence,
                -- approval_date from century/year/month/day
                CASE WHEN CCMS_MSGDCC_Date_Century > 0
                     AND CCMS_MSGDMM_Date_Month BETWEEN 1 AND 12
                     AND CCMS_MSGDDD_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCMS_MSGDCC_Date_Century * 100 + CCMS_MSGDYY_Date_Year,
                    CCMS_MSGDMM_Date_Month, CCMS_MSGDDD_Date_Day)
                ELSE NULL END,
                -- date_from from approval date century/year/month/day
                CASE WHEN CCMS_MSDAFC_Approval_Date_Century > 0
                     AND CCMS_MSDAFM_Approval_Date_Month BETWEEN 1 AND 12
                     AND CCMS_MSDAFD_Approval_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCMS_MSDAFC_Approval_Date_Century * 100 + CCMS_MSDAFY_Approval_Date_Year,
                    CCMS_MSDAFM_Approval_Date_Month, CCMS_MSDAFD_Approval_Date_Day)
                ELSE NULL END,
                CCMS_MSGDKM_Mileage_Km,
                CCMS_MSGDAM_Amount,
                CCMS_MSGDDS_Description,
                CCMS_MSGDD2_Description_2,
                CCMS_MSGDD3_Description_3,
                CCMS_MSGDSC_Source_Code,
                CCMS_MSMSTY_Maintenance_Type,
                CCMS_MSNUFO_Supplier_No,
                CCMS_MSNAFO_Supplier_Branch,
                CCMS_MSMJCD_Major_Code,
                CCMS_MSMNCD_Minor_Code,
                CCMS_MSRPPD_Reporting_Period,
                CCMS_MSCOUC_Country_Code,
                CCMS_MSVOLC_Volume_Code,
                CCMS_MSDISC_Distance_Code,
                CCMS_MSCONC_Consumption_Code,
                CCMS_MSCURC_Currency_Code,
                CCMS_MSSIRN_SI_Run_No
            FROM landing_CCMS
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCMS").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_maintenance_approvals").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_passed_invoices(conn):
    """Load passed invoices from landing to staging."""
    print("  Loading passed invoices...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_passed_invoices', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_passed_invoices")

        cursor.execute("""
            INSERT INTO staging_passed_invoices (
                contract_no, customer_no, name_code, object_no,
                contract_position_no, amount, cost_code, eb_reporting_period,
                driver_no, description, gross_net, invoice_no, lp_code,
                object_bridge, origin_code, run_no, source_code, vat_type,
                reporting_period, country_code
            )
            SELECT
                CCPI_PICONO_Contract_No,
                CCPI_PICUNO_Customer_No,
                CCPI_PINACD_Name_Code,
                CCPI_PIOBNO_Object_No,
                CCPI_PIPCNO_Contract_Position_No,
                CCPI_PIPIAM_Amount,
                CCPI_PIPICD_Cost_Code,
                CCPI_PIEBRP_EB_Reporting_Period,
                CCPI_PIPIDR_Driver_No,
                CCPI_PIPIDS_Description,
                CCPI_PIPIGR_Gross_Net,
                CCPI_PIPIIV_Invoice_No,
                CCPI_PIPILP_LP_Code,
                CCPI_PIPIOB_Object_Bridge,
                CCPI_PIPIOR_Origin_Code,
                CCPI_PIPIRN_Run_No,
                CCPI_PIPISC_Source_Code,
                CCPI_PIPIVT_VAT_Type,
                CCPI_PIRPPD_Reporting_Period,
                CCPI_PICOUC_Country_Code
            FROM landing_CCPI
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCPI").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_passed_invoices").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_replacement_cars(conn):
    """Load replacement cars from landing to staging."""
    print("  Loading replacement cars...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_replacement_cars', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_replacement_cars")

        cursor.execute("""
            INSERT INTO staging_replacement_cars (
                object_no, rc_no, sequence, driver_no, begin_date, end_date,
                rc_run_no, rc_code, km, amount, reason,
                description, description_2, description_3,
                type, driver_name, source_code, reporting_period, country_code
            )
            SELECT
                CCRC_RCOBNO_Object_No,
                CCRC_RCRCNO_RC_No,
                CCRC_RCRCSQ_Sequence,
                CCRC_RCDRNO_Driver_No,
                -- begin_date from century/year/month/day
                CASE WHEN CCRC_RCRCBC_Begin_Date_Century > 0
                     AND CCRC_RCRCBM_Begin_Date_Month BETWEEN 1 AND 12
                     AND CCRC_RCRCBD_Begin_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCRC_RCRCBC_Begin_Date_Century * 100 + CCRC_RCRCBY_Begin_Date_Year,
                    CCRC_RCRCBM_Begin_Date_Month, CCRC_RCRCBD_Begin_Date_Day)
                ELSE NULL END,
                -- end_date from century/year/month/day
                CASE WHEN CCRC_RCRCEC_End_Date_Century > 0
                     AND CCRC_RCRCEM_End_Date_Month BETWEEN 1 AND 12
                     AND CCRC_RCRCED_End_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCRC_RCRCEC_End_Date_Century * 100 + CCRC_RCRCEY_End_Date_Year,
                    CCRC_RCRCEM_End_Date_Month, CCRC_RCRCED_End_Date_Day)
                ELSE NULL END,
                CCRC_RCRCRN_RC_Run_No,
                CCRC_RCRCCD_RC_Code,
                CCRC_RCRCKM_Km,
                CCRC_RCRCAM_Amount,
                CCRC_RCRCRS_Reason,
                CCRC_RCRCDS_Description,
                CCRC_RCRCD2_Description_2,
                CCRC_RCRCD3_Description_3,
                CCRC_RCRCTY_Type,
                CCRC_RCRCDR_Driver_Name,
                CCRC_RCRCSC_Source_Code,
                CCRC_RCRPPD_Reporting_Period,
                CCRC_RCCOUC_Country_Code
            FROM landing_CCRC
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCRC").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_replacement_cars").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_reporting_periods(conn):
    """Load reporting periods from landing to staging."""
    print("  Loading reporting periods...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_reporting_periods', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_reporting_periods")

        cursor.execute("""
            INSERT INTO staging_reporting_periods (
                reporting_period, month_period, reporting_date
            )
            SELECT
                CCRP_RPRPPD_Reporting_Period,
                CCRP_RPMTPD_Month_Period,
                -- reporting_date from century/year/month/day
                CASE WHEN CCRP_RPRPCC_Period_CC > 0
                     AND CCRP_RPRPMM_Period_MM BETWEEN 1 AND 12
                     AND CCRP_RPRPDD_Period_DD BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCRP_RPRPCC_Period_CC * 100 + CCRP_RPRPYY_Period_YY,
                    CCRP_RPRPMM_Period_MM, CCRP_RPRPDD_Period_DD)
                ELSE NULL END
            FROM landing_CCRP
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCRP").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_reporting_periods").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_suppliers(conn):
    """Load suppliers from landing to staging."""
    print("  Loading suppliers...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_suppliers', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_suppliers")

        cursor.execute("""
            INSERT INTO staging_suppliers (
                supplier_no, branch_no, supplier_name, name_line_2, name_line_3,
                class, country_code, address, city, category,
                phone, fax, email, contact_person, responsible_person,
                reporting_period, country
            )
            SELECT
                CCSU_SUNUFO_Supplier_No,
                CCSU_SUNAFO_Branch_No,
                CCSU_SUNOFO_Supplier_Name,
                CCSU_SUNOF2_Name_Line_2,
                CCSU_SUNOF3_Name_Line_3,
                CCSU_SUCLFO_Class,
                CCSU_SUCPFO_Country_Code,
                CCSU_SUADFO_Address,
                CCSU_SULOFO_City,
                CCSU_SUCAFO_Category,
                CCSU_SUNTFO_Phone,
                CCSU_SUNFFO_Fax,
                CCSU_SUNEFO_Email,
                CCSU_SUCTFO_Contact_Person,
                CCSU_SURSFO_Responsible_Person,
                CCSU_SURPPD_Reporting_Period,
                CCSU_SUCOUC_Country
            FROM landing_CCSU
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCSU").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_suppliers").fetchone()[0]

        log_etl_end(conn, log_id, source_rows, inserted_rows)
        print(f"{inserted_rows} rows")

    except Exception as e:
        log_etl_end(conn, log_id, 0, 0, status='FAILED', error=str(e))
        print(f"FAILED - {e}")


def load_car_reports(conn):
    """Load car reports from landing to staging."""
    print("  Loading car reports...", end=" ", flush=True)
    log_id = log_etl_start(conn, 'staging_car_reports', 'FULL')

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM staging_car_reports")

        cursor.execute("""
            INSERT INTO staging_car_reports (
                object_no, reporting_period,
                odometer_date,
                book_value_begin_amount, book_value_begin_lt,
                disinvestment_amount, disinvestment_lt,
                gain_amount, gain_lt,
                first_start_book_value, first_start_interest_rate,
                fuel_cost_total, maintenance_cost_total,
                replacement_car_cost_total, tyre_cost_total,
                fuel_invoice_total, maintenance_invoice_total,
                replacement_car_invoice_total, tyre_invoice_total,
                first_start_km, first_start_initial_km,
                km_driven, monthly_km_driven, km_technical,
                fuel_cost_per_km, fuel_invoice_per_km,
                fuel_consumption, fuel_slope, fuel_monthly_deviation,
                maintenance_cost_per_km, maintenance_invoice_per_km,
                maintenance_slope, maintenance_monthly_deviation,
                replacement_car_cost_per_km, replacement_car_invoice_per_km,
                replacement_car_slope, replacement_car_monthly_deviation,
                replacement_car_km, replacement_car_amount,
                tyre_cost_per_km, tyre_invoice_per_km,
                tyre_slope, tyre_monthly_deviation,
                total_cost, total_invoiced, cost_per_km,
                total_surplus, total_surplus_absolute, total_total,
                last_week_km, update_km,
                maintenance_count, fuel_count,
                replacement_car_count, tyre_count,
                tyre_new_count, tyre_winter_count,
                private_km_pct, damage_count, damage_reserve,
                segment_01, segment_02, segment_03, segment_04, segment_05,
                segment_06, segment_07, segment_08, segment_09, segment_10,
                segment_11, segment_12, segment_13, segment_14, segment_15,
                country_code, volume_code, distance_code,
                consumption_code, currency_code,
                misc_insurance_amount, misc_insurance_peryear, misc_insurance_run_no,
                misc_ts_amount, misc_ts_peryear, misc_ts_run_no,
                traffic_fines_no,
                parking_cost_total, parking_invoice_total,
                parking_monthly_deviation, parking_slope,
                unspecified_cost_total, unspecified_invoice_total,
                unspecified_monthly_deviation, unspecified_slope,
                warranty_cost_total, warranty_invoice_total,
                warranty_monthly_deviation, warranty_slope
            )
            SELECT
                CCCR_CROBNO_Object_No,
                CCCR_CRRPPD_Reporting_Period,
                -- odometer_date from century/year/month/day
                CASE WHEN CCCR_CRKMCC_Odometer_Date_Century > 0
                     AND CCCR_CRKMMM_Odometer_Date_Month BETWEEN 1 AND 12
                     AND CCCR_CRKMDD_Odometer_Date_Day BETWEEN 1 AND 31
                THEN printf('%04d-%02d-%02d',
                    CCCR_CRKMCC_Odometer_Date_Century * 100 + CCCR_CRKMYY_Odometer_Date_Year,
                    CCCR_CRKMMM_Odometer_Date_Month, CCCR_CRKMDD_Odometer_Date_Day)
                ELSE NULL END,
                CCCR_CRBEAM_Book_Value_Begin_Amount,
                CCCR_CRBELT_Book_Value_Begin_LT,
                CCCR_CRDIAM_Disinvestment_Amount,
                CCCR_CRDILT_Disinvestment_LT,
                CCCR_CRGAAM_Gain_Amount,
                CCCR_CRGALT_Gain_LT,
                CCCR_CRFSBV_First_Start_Book_Value,
                CCCR_CRFSIR_First_Start_Interest_Rate,
                CCCR_CRFUCT_Fuel_Cost_Total,
                CCCR_CRMACT_Maintenance_Cost_Total,
                CCCR_CRRCCT_Replacement_Car_Cost_Total,
                CCCR_CRTRCT_Tyre_Cost_Total,
                CCCR_CRFUIV_Fuel_Invoice_Total,
                CCCR_CRMAIV_Maintenance_Invoice_Total,
                CCCR_CRRCIV_Replacement_Car_Invoice_Total,
                CCCR_CRTRIV_Tyre_Invoice_Total,
                CCCR_CRFSKM_First_Start_Km,
                CCCR_CRFSIK_First_Start_Initial_Km,
                CCCR_CRKMDR_Km_Driven,
                CCCR_CRMMDR_Monthly_Km_Driven,
                CCCR_CRKMTE_Km_Technical,
                CCCR_CRFUCK_Fuel_Cost_Per_Km,
                CCCR_CRFUIK_Fuel_Invoice_Per_Km,
                CCCR_CRRECO_Fuel_Consumption,
                CCCR_CRFUSL_Fuel_Slope,
                CCCR_CRFUMD_Fuel_Monthly_Deviation,
                CCCR_CRMACK_Maintenance_Cost_Per_Km,
                CCCR_CRMAIK_Maintenance_Invoice_Per_Km,
                CCCR_CRMASL_Maintenance_Slope,
                CCCR_CRMAMD_Maintenance_Monthly_Deviation,
                CCCR_CRRCCK_RC_Cost_Per_Km,
                CCCR_CRRCIK_RC_Invoice_Per_Km,
                CCCR_CRRCSL_RC_Slope,
                CCCR_CRRCMD_RC_Monthly_Deviation,
                CCCR_CRRCKM_RC_Km,
                CCCR_CRRCAM_RC_Amount,
                CCCR_CRTRCK_Tyre_Cost_Per_Km,
                CCCR_CRTRIK_Tyre_Invoice_Per_Km,
                CCCR_CRTRSL_Tyre_Slope,
                CCCR_CRTRMD_Tyre_Monthly_Deviation,
                CCCR_CRTOCT_Total_Cost,
                CCCR_CRTOIV_Total_Invoiced,
                CCCR_CRCTKM_Cost_Per_Km,
                CCCR_CRTOSU_Total_Surplus,
                CCCR_CRTOSA_Total_Surplus_Absolute,
                CCCR_CRTOTO_Total_Total,
                CCCR_CRLWKM_Last_Week_Km,
                CCCR_CRUPKM_Update_Km,
                CCCR_CRMANR_Maintenance_Count,
                CCCR_CRFUNR_Fuel_Count,
                CCCR_CRRCNR_RC_Count,
                CCCR_CRTRNR_Tyre_Count,
                CCCR_CRTNNR_Tyre_New_Count,
                CCCR_CRTWNR_Tyre_Winter_Count,
                CCCR_CRPRKT_Private_Km_Pct,
                CCCR_CRDANB_Damage_Count,
                CCCR_CRDARE_Damage_Reserve,
                CCCR_CRSG01_Segment_01,
                CCCR_CRSG02_Segment_02,
                CCCR_CRSG03_Segment_03,
                CCCR_CRSG04_Segment_04,
                CCCR_CRSG05_Segment_05,
                CCCR_CRSG06_Segment_06,
                CCCR_CRSG07_Segment_07,
                CCCR_CRSG08_Segment_08,
                CCCR_CRSG09_Segment_09,
                CCCR_CRSG10_Segment_10,
                CCCR_CRSG11_Segment_11,
                CCCR_CRSG12_Segment_12,
                CCCR_CRSG13_Segment_13,
                CCCR_CRSG14_Segment_14,
                CCCR_CRSG15_Segment_15,
                CCCR_CRCOUC_Country_Code,
                CCCR_CRVOLC_Volume_Code,
                CCCR_CRDISC_Distance_Code,
                CCCR_CRCONC_Consumption_Code,
                CCCR_CRCURC_Currency_Code,
                CCCR_CRMIAM_Misc_Insurance_Amount,
                CCCR_CRMIPY_Misc_Insurance_PerYear,
                CCCR_CRMIRN_Misc_Insurance_Run_No,
                CCCR_CRTSAM_Misc_TS_Amount,
                CCCR_CRTSPY_Misc_TS_PerYear,
                CCCR_CRTSRN_Misc_TS_Run_No,
                CCCR_CRTRNO_Traffic_Fines_No,
                CCCR_CRPACT_Parking_Cost_Total,
                CCCR_CRPAIV_Parking_Invoice_Total,
                CCCR_CRPAMD_Parking_Monthly_Deviation,
                CCCR_CRPASL_Parking_Slope,
                CCCR_CRUNCT_Unspecified_Cost_Total,
                CCCR_CRUNIV_Unspecified_Invoice_Total,
                CCCR_CRUNMD_Unspecified_Monthly_Deviation,
                CCCR_CRUNSL_Unspecified_Slope,
                CCCR_CRWACT_Warranty_Cost_Total,
                CCCR_CRWAIV_Warranty_Invoice_Total,
                CCCR_CRWAMD_Warranty_Monthly_Deviation,
                CCCR_CRWASL_Warranty_Slope
            FROM landing_CCCR
        """)

        source_rows = cursor.execute("SELECT COUNT(*) FROM landing_CCCR").fetchone()[0]
        conn.commit()
        inserted_rows = cursor.execute("SELECT COUNT(*) FROM staging_car_reports").fetchone()[0]

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
    load_damages(conn)
    load_domain_translations(conn)
    load_exploitation_services(conn)
    load_maintenance_approvals(conn)
    load_passed_invoices(conn)
    load_replacement_cars(conn)
    load_reporting_periods(conn)
    load_suppliers(conn)
    load_car_reports(conn)

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
