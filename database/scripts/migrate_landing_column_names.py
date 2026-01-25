"""
Migration Script: Rename landing table columns to documented names.
This script renames columns from cryptic codes to self-documenting names.
"""

import sqlite3
import re
from pathlib import Path

DB_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\fleetai.db"

# Column name mappings for each table
# Format: old_name -> new_name

COLUMN_MAPPINGS = {
    'landing_CCOB': {
        # Object/Vehicle table - 95 columns
        'CCOB_OBOBNO': 'CCOB_OBOBNO_Object_Number',
        'CCOB_OBOBCX': 'CCOB_OBOBCX_Chassis_Number',
        'CCOB_OBMKCD': 'CCOB_OBMKCD_Make_Code',
        'CCOB_OBMDCD': 'CCOB_OBMDCD_Model_Code',
        'CCOB_OBCUNO': 'CCOB_OBCUNO_Customer_Number',
        'CCOB_OBPCNO': 'CCOB_OBPCNO_Profit_Centre_Number',
        'CCOB_OBCONO': 'CCOB_OBCONO_Contract_Number',
        'CCOB_OBCPNO': 'CCOB_OBCPNO_Calculation_Number',
        'CCOB_OBOBIV': 'CCOB_OBOBIV_Driver_Participation_Pct',
        'CCOB_OBUSCD': 'CCOB_OBUSCD_Object_Usage',
        'CCOB_OBCTIC': 'CCOB_OBCTIC_Tech_Control_Date_Century',
        'CCOB_OBCTIY': 'CCOB_OBCTIY_Tech_Control_Date_Year',
        'CCOB_OBCTIM': 'CCOB_OBCTIM_Tech_Control_Date_Month',
        'CCOB_OBCTID': 'CCOB_OBCTID_Tech_Control_Date_Day',
        'CCOB_OBOBUC': 'CCOB_OBOBUC_Upholstery_Color',
        'CCOB_OBOBUP': 'CCOB_OBOBUP_Upholstery_Code',
        'CCOB_OBOBEC': 'CCOB_OBOBEC_Exterior_Color',
        'CCOB_OBOBBC': 'CCOB_OBOBBC_Object_Begin_Date_Century',
        'CCOB_OBOBBY': 'CCOB_OBOBBY_Object_Begin_Date_Year',
        'CCOB_OBOBBM': 'CCOB_OBOBBM_Object_Begin_Date_Month',
        'CCOB_OBOBBD': 'CCOB_OBOBBD_Object_Begin_Date_Day',
        'CCOB_OBOBSK': 'CCOB_OBOBSK_Begin_Mileage',
        'CCOB_OBFUSK': 'CCOB_OBFUSK_Begin_Mileage_Fuel_Incl',
        'CCOB_OBOABC': 'CCOB_OBOABC_Recalc_Start_Date_Century',
        'CCOB_OBOABY': 'CCOB_OBOABY_Recalc_Start_Date_Year',
        'CCOB_OBOABM': 'CCOB_OBOABM_Recalc_Start_Date_Month',
        'CCOB_OBOABD': 'CCOB_OBOABD_Recalc_Start_Date_Day',
        'CCOB_OBMKTY': 'CCOB_OBMKTY_HSN_TSN',
        'CCOB_OBOBSC': 'CCOB_OBOBSC_Object_Status',
        'CCOB_OBSCCM': 'CCOB_OBSCCM_Status_Comment',
        'CCOB_OBRGNO': 'CCOB_OBRGNO_Registration_Number',
        'CCOB_OBRGBC': 'CCOB_OBRGBC_Reg_Begin_Date_Century',
        'CCOB_OBRGBY': 'CCOB_OBRGBY_Reg_Begin_Date_Year',
        'CCOB_OBRGBM': 'CCOB_OBRGBM_Reg_Begin_Date_Month',
        'CCOB_OBRGBD': 'CCOB_OBRGBD_Reg_Begin_Date_Day',
        'CCOB_OBRGEC': 'CCOB_OBRGEC_Reg_End_Date_Century',
        'CCOB_OBRGEY': 'CCOB_OBRGEY_Reg_End_Date_Year',
        'CCOB_OBRGEM': 'CCOB_OBRGEM_Reg_End_Date_Month',
        'CCOB_OBRGED': 'CCOB_OBRGED_Reg_End_Date_Day',
        'CCOB_OBRGPV': 'CCOB_OBRGPV_Previous_Registration',
        'CCOB_OBOBOR': 'CCOB_OBOBOR_Client_Reference',
        'CCOB_OBNDTA': 'CCOB_OBNDTA_Non_Deductable_Taxes',
        'CCOB_OBPRIC': 'CCOB_OBPRIC_Monthly_Rental',
        'CCOB_OBRCOB': 'CCOB_OBRCOB_Replacement_Car_Category',
        'CCOB_OBFSEC': 'CCOB_OBFSEC_Object_End_Date_Century',
        'CCOB_OBFSEY': 'CCOB_OBFSEY_Object_End_Date_Year',
        'CCOB_OBFSEM': 'CCOB_OBFSEM_Object_End_Date_Month',
        'CCOB_OBFSED': 'CCOB_OBFSED_Object_End_Date_Day',
        'CCOB_OBFSDU': 'CCOB_OBFSDU_Budgeted_Months',
        'CCOB_OBFSAM': 'CCOB_OBFSAM_Investment_Amount',
        'CCOB_OBFSRV': 'CCOB_OBFSRV_Budgeted_Residual_Value',
        'CCOB_OBKMDU': 'CCOB_OBKMDU_Total_Budgeted_Mileage',
        'CCOB_OBKMBU': 'CCOB_OBKMBU_Budgeted_Mileage_Per_Year',
        'CCOB_OBOBMC': 'CCOB_OBOBMC_Manufacturing_Date_Century',
        'CCOB_OBOBMY': 'CCOB_OBOBMY_Manufacturing_Date_Year',
        'CCOB_OBOBMM': 'CCOB_OBOBMM_Manufacturing_Date_Month',
        'CCOB_OBOBMD': 'CCOB_OBOBMD_Manufacturing_Date_Day',
        'CCOB_OBTLCD': 'CCOB_OBTLCD_Contract_Type',
        'CCOB_OBLPCD': 'CCOB_OBLPCD_Company_Code',
        'CCOB_OBGRNO': 'CCOB_OBGRNO_Client_Group_Number',
        'CCOB_OBORNO': 'CCOB_OBORNO_Order_Number',
        'CCOB_OBORCD': 'CCOB_OBORCD_Object_Takeover_Code',
        'CCOB_OBDTIC': 'CCOB_OBDTIC_Object_Data_Incomplete',
        'CCOB_OBSLPR': 'CCOB_OBSLPR_Final_Sales_Price',
        'CCOB_OBFLIN': 'CCOB_OBFLIN_Floating_Interest',
        'CCOB_OBRPPD': 'CCOB_OBRPPD_Reporting_Period',
        'CCOB_OBCOUC': 'CCOB_OBCOUC_Country_Code',
        'CCOB_OBVOLC': 'CCOB_OBVOLC_Volume_Measurement',
        'CCOB_OBDISC': 'CCOB_OBDISC_Distance_Measurement',
        'CCOB_OBCONC': 'CCOB_OBCONC_Consumption_Measurement',
        'CCOB_OBCURC': 'CCOB_OBCURC_Currency_Unit',
        'CCOB_OBINCL': 'CCOB_OBINCL_Insurance_Class',
        'CCOB_OBBYCD': 'CCOB_OBBYCD_Buyback_Code',
        'CCOB_OBBJRW': 'CCOB_OBBJRW_Service_Freq_Period',
        'CCOB_OBBIRW': 'CCOB_OBBIRW_Service_Freq_Mileage',
        'CCOB_OBMEPE': 'CCOB_OBMEPE_Timeout_Period_Days',
        'CCOB_OBSLCD': 'CCOB_OBSLCD_Sales_Reason_Code',
        'CCOB_OBOREF': 'CCOB_OBOREF_Object_Reference_Number',
        'CCOB_OBDRPC': 'CCOB_OBDRPC_Driver_Profit_Center',
        'CCOB_OBFUCD': 'CCOB_OBFUCD_Fuel_Code',
        'CCOB_OBOBAC': 'CCOB_OBOBAC_Admission_Date_Century',
        'CCOB_OBOBAY': 'CCOB_OBOBAY_Admission_Date_Year',
        'CCOB_OBOBAM': 'CCOB_OBOBAM_Admission_Date_Month',
        'CCOB_OBOBAD': 'CCOB_OBOBAD_Admission_Date_Day',
        'CCOB_OBOBSN': 'CCOB_OBOBSN_Supplier_Number',
        'CCOB_OBOBSQ': 'CCOB_OBOBSQ_Supplier_Rotation_Number',
        'CCOB_OBOBPU': 'CCOB_OBOBPU_Object_Purchase_Price',
        'CCOB_OBRCDA': 'CCOB_OBRCDA_Replacement_Days',
        'CCOB_OBFSKM': 'CCOB_OBFSKM_Last_Mileage_Known',
        'CCOB_OBKMDD': 'CCOB_OBKMDD_Mileage_Date_Day',
        'CCOB_OBKMMM': 'CCOB_OBKMMM_Mileage_Date_Month',
        'CCOB_OBKMCC': 'CCOB_OBKMCC_Mileage_Date_Century',
        'CCOB_OBKMYY': 'CCOB_OBKMYY_Mileage_Date_Year',
        'CCOB_OBLPPV': 'CCOB_OBLPPV_Previous_Company_Code',
        'CCOB_OBOBMN': 'CCOB_OBOBMN_Motor_Number',
    },
    'landing_CCCU': {
        # Customer table
        'CCCU_CUNULO': 'CCCU_CUNULO_Customer_Number',
        'CCCU_CUGNLO': 'CCCU_CUGNLO_Client_Group_Number',
        'CCCU_CUNOLO': 'CCCU_CUNOLO_Name',
        'CCCU_CUNOL2': 'CCCU_CUNOL2_Name_Cont',
        'CCCU_CUNOL3': 'CCCU_CUNOL3_Name_Cont',
        'CCCU_CUCLLO': 'CCCU_CUCLLO_Language_Code',
        'CCCU_CUCPLO': 'CCCU_CUCPLO_Country_Code',
        'CCCU_CUADLO': 'CCCU_CUADLO_Address',
        'CCCU_CULOLO': 'CCCU_CULOLO_Locality',
        'CCCU_CUCALO': 'CCCU_CUCALO_Area_Code',
        'CCCU_CUNTLO': 'CCCU_CUNTLO_Telephone_Number',
        'CCCU_CUNFLO': 'CCCU_CUNFLO_Fax_Number',
        'CCCU_CUNELO': 'CCCU_CUNELO_Email_Address',
        'CCCU_CURPPD': 'CCCU_CURPPD_Reporting_Period',
        'CCCU_CUCOUC': 'CCCU_CUCOUC_Country_Code',
        'CCCU_CURSLO': 'CCCU_CURSLO_Account_Manager',
    },
    'landing_CCDR': {
        # Driver table
        'CCDR_DROBNO': 'CCDR_DROBNO_Object_Number',
        'CCDR_DRDRNO': 'CCDR_DRDRNO_Driver_Number',
        'CCDR_DRACDR': 'CCDR_DRACDR_Actual_Driver_Code',
        'CCDR_DRDRNM': 'CCDR_DRDRNM_Driver_Name',
        'CCDR_DRDRFN': 'CCDR_DRDRFN_Driver_First_Name',
        'CCDR_DRDRLN': 'CCDR_DRDRLN_Driver_Last_Name',
        'CCDR_DRDRAD': 'CCDR_DRDRAD_Driver_Address',
        'CCDR_DRDRLO': 'CCDR_DRDRLO_Driver_Locality',
        'CCDR_DRDRLA': 'CCDR_DRDRLA_Driver_Country_Code',
        'CCDR_DRPVTF': 'CCDR_DRPVTF_Private_Telephone',
        'CCDR_DROFTF': 'CCDR_DROFTF_Office_Telephone',
        'CCDR_DRPVNF': 'CCDR_DRPVNF_Mobile_Phone',
        'CCDR_DRDRNE': 'CCDR_DRDRNE_Email',
        'CCDR_DRRPPD': 'CCDR_DRRPPD_Reporting_Period',
        'CCDR_DRCOUC': 'CCDR_DRCOUC_Country_Code',
    },
    'landing_CCAU': {
        # Automobile reference table
        'CCAU_AUMKCD': 'CCAU_AUMKCD_Make_Code',
        'CCAU_AUMKDS': 'CCAU_AUMKDS_Make_Description',
        'CCAU_AUMDCD': 'CCAU_AUMDCD_Model_Code',
        'CCAU_AUMDDS': 'CCAU_AUMDDS_Model_Description',
        'CCAU_AUMDGH': 'CCAU_AUMDGH_Model_Group_High',
        'CCAU_AUMDGR': 'CCAU_AUMDGR_Model_Group_Range',
        'CCAU_AUMDGS': 'CCAU_AUMDGS_Model_Group_Size',
        'CCAU_AUTYDS': 'CCAU_AUTYDS_Type_Description',
        'CCAU_AURPPD': 'CCAU_AURPPD_Reporting_Period',
        'CCAU_AUCOUC': 'CCAU_AUCOUC_Country_Code',
    },
    'landing_CCGD': {
        # Odometer/Mileage history table
        'CCGD_MAOBNO': 'CCGD_MAOBNO_Object_Number',
        'CCGD_MAGDSQ': 'CCGD_MAGDSQ_Mileage_Sequence',
        'CCGD_MAGDCC': 'CCGD_MAGDCC_Mileage_Date_Century',
        'CCGD_MAGDYY': 'CCGD_MAGDYY_Mileage_Date_Year',
        'CCGD_MAGDMM': 'CCGD_MAGDMM_Mileage_Date_Month',
        'CCGD_MAGDDD': 'CCGD_MAGDDD_Mileage_Date_Day',
        'CCGD_MAGDKM': 'CCGD_MAGDKM_Mileage_Km',
        'CCGD_MAGDAM': 'CCGD_MAGDAM_Mileage_Amount',
        'CCGD_MAGDDS': 'CCGD_MAGDDS_Mileage_Description',
        'CCGD_MAGDSC': 'CCGD_MAGDSC_Mileage_Source_Code',
        'CCGD_MANUFO': 'CCGD_MANUFO_Supplier_Number',
        'CCGD_MANAFO': 'CCGD_MANAFO_Supplier_Reference',
    },
    'landing_CCCP': {
        # Contract position table
        'CCCP_CPCPNO': 'CCCP_CPCPNO_Contract_Position_Number',
        'CCCP_CPGNLO': 'CCCP_CPGNLO_Group_Name',
        'CCCP_CPCUNO': 'CCCP_CPCUNO_Customer_Number',
        'CCCP_CPPCNO': 'CCCP_CPPCNO_Profit_Center_Number',
        'CCCP_CPCONO': 'CCCP_CPCONO_Contract_Number',
        'CCCP_CPACCP': 'CCCP_CPACCP_Active_Contract_Position',
        'CCCP_CPCPDL': 'CCCP_CPCPDL_Delivery_Days',
        'CCCP_CPCPDU': 'CCCP_CPCPDU_Contract_Duration_Months',
        'CCCP_CPCPMI': 'CCCP_CPCPMI_Km_Per_Year',
        'CCCP_CPCPPU': 'CCCP_CPCPPU_Purchase_Price',
        'CCCP_CPCPRV': 'CCCP_CPCPRV_Residual_Value',
        'CCCP_CPCPTA': 'CCCP_CPCPTA_Total_Amount',
        'CCCP_CPIRPT': 'CCCP_CPIRPT_Interest_Rate_Percent',
        'CCCP_CPMKCD': 'CCCP_CPMKCD_Make_Code',
        'CCCP_CPMDCD': 'CCCP_CPMDCD_Model_Code',
        'CCCP_CPMRAA': 'CCCP_CPMRAA_Monthly_Rate_Admin',
        'CCCP_CPMRAF': 'CCCP_CPMRAF_Monthly_Rate_Fuel',
        'CCCP_CPMRDP': 'CCCP_CPMRDP_Monthly_Rate_Depreciation',
        'CCCP_CPMRIN': 'CCCP_CPMRIN_Monthly_Rate_Insurance',
        'CCCP_CPMRIR': 'CCCP_CPMRIR_Monthly_Rate_Interest',
        'CCCP_CPMRMF': 'CCCP_CPMRMF_Monthly_Rate_Maintenance',
        'CCCP_CPMRRA': 'CCCP_CPMRRA_Monthly_Rate_Replacement',
        'CCCP_CPMRRO': 'CCCP_CPMRRO_Monthly_Rate_Road_Tax',
        'CCCP_CPMRVI': 'CCCP_CPMRVI_Monthly_Rate_Tires',
        'CCCP_CPMTCT': 'CCCP_CPMTCT_Monthly_Total_Cost',
        'CCCP_CPUNMA': 'CCCP_CPUNMA_Unit_Rate_Maintenance',
        'CCCP_CPUNTR': 'CCCP_CPUNTR_Unit_Rate_Tires',
        'CCCP_CPUNRC': 'CCCP_CPUNRC_Unit_Rate_Replacement',
        'CCCP_CPXMPT': 'CCCP_CPXMPT_Excess_Km_Rate',
        'CCCP_CPRPPD': 'CCCP_CPRPPD_Reporting_Period',
        'CCCP_CPCOUC': 'CCCP_CPCOUC_Country_Code',
        'CCCP_CPLPCD': 'CCCP_CPLPCD_Lease_Type_Code',
    },
    'landing_CCBI': {
        # Billing table
        'CCBI_BIBINO': 'CCBI_BIBINO_Billing_Number',
        'CCBI_BIOBNO': 'CCBI_BIOBNO_Object_Number',
        'CCBI_BIBIRN': 'CCBI_BIBIRN_Billing_Run_Number',
        'CCBI_BIBIAO': 'CCBI_BIBIAO_Billing_Account_Owner',
        'CCBI_BIBINM': 'CCBI_BIBINM_Billing_Name',
        'CCBI_BIBIAD': 'CCBI_BIBIAD_Billing_Address',
        'CCBI_BIBILO': 'CCBI_BIBILO_Billing_Locality',
        'CCBI_BIBICM': 'CCBI_BIBICM_Billing_Method',
        'CCBI_BIBIA1': 'CCBI_BIBIA1_Billing_Amount_1',
        'CCBI_BIBIAV': 'CCBI_BIBIAV_Billing_Amount_Variable',
        'CCBI_BIBIAM': 'CCBI_BIBIAM_Billing_Amount_Monthly',
        'CCBI_BIBIPL': 'CCBI_BIBIPL_Billing_Period_Length',
        'CCBI_BICURC': 'CCBI_BICURC_Currency_Code',
        'CCBI_BIRPPD': 'CCBI_BIRPPD_Reporting_Period',
        'CCBI_BICOUC': 'CCBI_BICOUC_Country_Code',
    },
    'landing_CCGR': {
        # Groups table
        'CCGR_GRGNLO': 'CCGR_GRGNLO_Group_Number',
        'CCGR_GRNKLO': 'CCGR_GRNKLO_Customer_Number',
        'CCGR_GRNOLO': 'CCGR_GRNOLO_Group_Name',
        'CCGR_GRRPPD': 'CCGR_GRRPPD_Reporting_Period',
        'CCGR_GRCOUC': 'CCGR_GRCOUC_Country_Code',
    },
    'landing_CCOR': {
        # Orders table
        'CCOR_ORORNO': 'CCOR_ORORNO_Order_Number',
        'CCOR_ORCPNO': 'CCOR_ORCPNO_Contract_Position_Number',
        'CCOR_ORGNLO': 'CCOR_ORGNLO_Group_Name',
        'CCOR_ORCUNO': 'CCOR_ORCUNO_Customer_Number',
        'CCOR_ORPCNO': 'CCOR_ORPCNO_Profit_Center_Number',
        'CCOR_ORCONO': 'CCOR_ORCONO_Contract_Number',
        'CCOR_ORORSC': 'CCOR_ORORSC_Order_Status_Code',
        'CCOR_ORNUFO': 'CCOR_ORNUFO_Supplier_Number',
        'CCOR_ORNAFO': 'CCOR_ORNAFO_Supplier_Amount',
        'CCOR_ORRPPD': 'CCOR_ORRPPD_Reporting_Period',
        'CCOR_ORCOUC': 'CCOR_ORCOUC_Country_Code',
    },
}


def rename_table_columns(conn, table_name, mapping):
    """Rename columns in a table using SQLite's ALTER TABLE."""
    cursor = conn.cursor()

    # Get current columns
    cursor.execute(f"PRAGMA table_info({table_name})")
    current_cols = {col[1]: col for col in cursor.fetchall()}

    renamed = 0
    for old_name, new_name in mapping.items():
        if old_name in current_cols and old_name != new_name:
            try:
                cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")
                renamed += 1
            except sqlite3.OperationalError as e:
                if "no such column" in str(e).lower():
                    pass  # Column doesn't exist, skip
                else:
                    print(f"    Warning: Could not rename {old_name}: {e}")

    conn.commit()
    return renamed


def migrate_landing_tables():
    """Migrate all landing tables to use documented column names."""
    print("=" * 60)
    print("Landing Table Column Migration")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print()

    conn = sqlite3.connect(DB_PATH)

    total_renamed = 0
    for table_name, mapping in COLUMN_MAPPINGS.items():
        print(f"Migrating {table_name}...")
        renamed = rename_table_columns(conn, table_name, mapping)
        print(f"  Renamed {renamed} columns")
        total_renamed += renamed

    conn.close()

    print()
    print(f"Total columns renamed: {total_renamed}")
    print("Migration complete!")


if __name__ == "__main__":
    migrate_landing_tables()
