-- ============================================================================
-- Landing Schema - Auto-generated from Excel Source Files
-- Generated: 2026-01-22 13:32:03
-- Source Directory: C:\Users\X1Carbon\Documents\FleetAI\Files
-- Tables: 36
-- ============================================================================

-- Create landing schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'landing')
    EXEC('CREATE SCHEMA landing');
GO

-- ============================================================================
-- ETL Metadata Tables
-- ============================================================================

-- Extraction logging table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_extraction_log' AND schema_id = SCHEMA_ID('landing'))
CREATE TABLE [landing].[etl_extraction_log] (
    [log_id] BIGINT IDENTITY(1,1) PRIMARY KEY,
    [table_name] VARCHAR(50) NOT NULL,
    [extraction_type] VARCHAR(20) NOT NULL,  -- 'FULL' or 'INCREMENTAL'
    [extraction_start] DATETIME2 NOT NULL,
    [extraction_end] DATETIME2 NULL,
    [source_row_count] INT NULL,
    [extracted_row_count] INT NULL,
    [status] VARCHAR(20) NOT NULL,  -- 'RUNNING', 'SUCCESS', 'FAILED'
    [error_message] VARCHAR(MAX) NULL,
    [created_at] DATETIME2 DEFAULT GETUTCDATE()
);

CREATE INDEX [IX_etl_extraction_log_table] ON [landing].[etl_extraction_log]([table_name], [extraction_start]);

-- Checkpoint table for incremental loads
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_checkpoint' AND schema_id = SCHEMA_ID('landing'))
CREATE TABLE [landing].[etl_checkpoint] (
    [checkpoint_id] INT IDENTITY(1,1) PRIMARY KEY,
    [table_name] VARCHAR(50) UNIQUE NOT NULL,
    [last_successful_extraction] DATETIME2 NULL,
    [last_row_count] INT NULL,
    [last_hash] VARCHAR(64) NULL,
    [updated_at] DATETIME2 DEFAULT GETUTCDATE()
);

GO

-- ============================================================================
-- Table Summary
-- ============================================================================
/*
Table              Columns            Rows Source File         
-----------------------------------------------------------------
CCAU                    65           9,357 ccau.xlsx           
CCBI                    31          22,934 ccbi.xlsx           
CCCA                    24           8,219 ccca.xlsx           
CCCO                    39           2,945 ccco.xlsx           
CCCP                   182          33,652 cccp.xlsx           
CCCU                    50           2,387 cccu.xlsx           
CCDA                   156               0 ccda.xlsx           
CCDR                    63          33,633 ccdr.xlsx           
CCFC                    65           9,357 ccfc.xlsx           
CCFID                   65           9,357 ccfid.xlsx          
CCFIH                   65           9,357 ccfih.xlsx          
CCFIM                   65           9,357 ccfim.xlsx          
CCFP                    27          16,255 ccfp.xlsx           
CCGD                    15         402,428 ccgd.xlsx           
CCGR                    26           1,961 ccgr.xlsx           
CCIN                    34              42 ccin.xlsx           
CCIO                    65           9,357 ccio.xlsx           
CCOB                    95          33,374 ccob.xlsx           
CCOBCP                   5          66,792 ccobcp.xlsx         
CCOR                   114          33,418 ccor.xlsx           
CCOS                    23          25,661 ccos.xlsx           
CCPC                    20           3,863 ccpc.xlsx           
CCRS                    11              44 ccrs.xlsx           
CCXC                    13          42,858 ccxc.xlsx           
CWAU                     2           9,357 cwau.xlsx           
CWBI                     2          21,920 cwbi.xlsx           
CWCO                     3           2,949 cwco.xlsx           
CWCP                     1          33,652 cwcp.xlsx           
CWCU                     2           2,387 cwcu.xlsx           
CWDR                     2          33,405 cwdr.xlsx           
CWGR                     1           1,961 cwgr.xlsx           
CWOA                     2          10,902 cwoa.xlsx           
CWOB                     4          33,374 cwob.xlsx           
CWOR                     1          33,418 cwor.xlsx           
CWPC                     3           3,863 cwpc.xlsx           
CWPO                     4              44 cwpo.xlsx           
-----------------------------------------------------------------
TOTAL                 1254         983,197
*/

-- ============================================================================
-- Drop Existing Tables (if recreating)
-- ============================================================================
/*
-- Uncomment to drop existing tables before recreation
DROP TABLE IF EXISTS [landing].[CWPO];
DROP TABLE IF EXISTS [landing].[CWPC];
DROP TABLE IF EXISTS [landing].[CWOR];
DROP TABLE IF EXISTS [landing].[CWOB];
DROP TABLE IF EXISTS [landing].[CWOA];
DROP TABLE IF EXISTS [landing].[CWGR];
DROP TABLE IF EXISTS [landing].[CWDR];
DROP TABLE IF EXISTS [landing].[CWCU];
DROP TABLE IF EXISTS [landing].[CWCP];
DROP TABLE IF EXISTS [landing].[CWCO];
DROP TABLE IF EXISTS [landing].[CWBI];
DROP TABLE IF EXISTS [landing].[CWAU];
DROP TABLE IF EXISTS [landing].[CCXC];
DROP TABLE IF EXISTS [landing].[CCRS];
DROP TABLE IF EXISTS [landing].[CCPC];
DROP TABLE IF EXISTS [landing].[CCOS];
DROP TABLE IF EXISTS [landing].[CCOR];
DROP TABLE IF EXISTS [landing].[CCOBCP];
DROP TABLE IF EXISTS [landing].[CCOB];
DROP TABLE IF EXISTS [landing].[CCIO];
DROP TABLE IF EXISTS [landing].[CCIN];
DROP TABLE IF EXISTS [landing].[CCGR];
DROP TABLE IF EXISTS [landing].[CCGD];
DROP TABLE IF EXISTS [landing].[CCFP];
DROP TABLE IF EXISTS [landing].[CCFIM];
DROP TABLE IF EXISTS [landing].[CCFIH];
DROP TABLE IF EXISTS [landing].[CCFID];
DROP TABLE IF EXISTS [landing].[CCFC];
DROP TABLE IF EXISTS [landing].[CCDR];
DROP TABLE IF EXISTS [landing].[CCDA];
DROP TABLE IF EXISTS [landing].[CCCU];
DROP TABLE IF EXISTS [landing].[CCCP];
DROP TABLE IF EXISTS [landing].[CCCO];
DROP TABLE IF EXISTS [landing].[CCCA];
DROP TABLE IF EXISTS [landing].[CCBI];
DROP TABLE IF EXISTS [landing].[CCAU];
*/
GO

-- ============================================================================
-- Landing Tables
-- ============================================================================

-- Table: CCAU
-- Source: ccau.xlsx
-- Columns: 65
CREATE TABLE [landing].[CCAU] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCAU_AUMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMKDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDGH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDGR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDGS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUOBTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTDN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTDN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMAM1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMAF1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMAF2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMAF3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUNODO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUNOGE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUNOSE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDTK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDKW] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUFCHP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUAUUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMXWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMXUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUSTRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUFUCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUFUCF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUAURO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYPR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUAUIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUAUI2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUDLCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AULGCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUWATM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUMDDC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYD1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUTYD2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUC9CD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AURPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUDISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUAUBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCAU_AUAUTH] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (65 columns with CCAU_ prefix)
    [CCAU_AUMKCD_Make_Code] INT NULL,
    [CCAU_AUMKDS_Make_Description] VARCHAR(50) NULL,
    [CCAU_AUMDCD_Model_Code] INT NULL,
    [CCAU_AUMDDS_Model_Description] VARCHAR(50) NULL,
    [CCAU_AUMDGH_Model_Group_Higher_Level] VARCHAR(200) NULL,
    [CCAU_AUMDGR_Model_Group_Code] INT NULL,
    [CCAU_AUMDGS_Model_Group_Description] VARCHAR(200) NULL,
    [CCAU_AUTYDS_Type_Description] VARCHAR(50) NULL,
    [CCAU_AUOBTY_Automobile_Category] VARCHAR(200) NULL,
    [CCAU_AUTDN1_Technical_Description_1] VARCHAR(50) NULL,
    [CCAU_AUTDN2_Technical_Description_2] VARCHAR(50) NULL,
    [CCAU_AUMAM1_1st_Maintenance_Mileage] INT NULL,
    [CCAU_AUMAF1_Maintenance_Type_1_Mileage] INT NULL,
    [CCAU_AUMAF2_Maintenance_Type_2_Mileage] INT NULL,
    [CCAU_AUMAF3_Maintenance_Type_3_Mileage] INT NULL,
    [CCAU_AUNODO_Number_Of_Doors] INT NULL,
    [CCAU_AUNOGE_Number_Of_Gears] VARCHAR(50) NULL,
    [CCAU_AUNOSE_Number_Of_Seats] INT NULL,
    [CCAU_AUMDTK_Tank_Capacity] INT NULL,
    [CCAU_AUTYCC_Cylinder_Capacity] INT NULL,
    [CCAU_AUMDKW_Kw] INT NULL,
    [CCAU_AUFCHP_Fiscal_Horse_Power] INT NULL,
    [CCAU_AUAUUN_Unit_Code] VARCHAR(50) NULL,
    [CCAU_AUMXWE_Maximum_Weight] INT NULL,
    [CCAU_AUMXUN_Maximum_Number_Of_Unitsauun] INT NULL,
    [CCAU_AUSTRD_Standard_Radio] VARCHAR(50) NULL,
    [CCAU_AUFUCD_Fuel_Code] INT NULL,
    [CCAU_AUFUCF_Standard_Consumption] DECIMAL(5,3) NULL,
    [CCAU_AUAURO_Road_Tax_Code] VARCHAR(50) NULL,
    [CCAU_AUTYPR_Catalog_Price] DECIMAL(11,4) NULL,
    [CCAU_AUAUIN_Insurance_Code] VARCHAR(50) NULL,
    [CCAU_AUAUI2_Insurance_Code_2] VARCHAR(200) NULL,
    [CCAU_AUDLCH_Delivery_Charges] DECIMAL(4,3) NULL,
    [CCAU_AUTYWE_Weight] INT NULL,
    [CCAU_AULGCH_Supplement_Lpg] INT NULL,
    [CCAU_AUWATM_Warranty_Length] INT NULL,
    [CCAU_AUMDDC_Reduction] DECIMAL(7,4) NULL,
    [CCAU_AUTYN1_Tyres_1_Number] INT NULL,
    [CCAU_AUTYD1_Tyres_1_Description] VARCHAR(50) NULL,
    [CCAU_AUTYN2_Tyres_2_Number] INT NULL,
    [CCAU_AUTYD2_Tyres_2_Description] VARCHAR(50) NULL,
    [CCAU_AUC9CD_Catalconverter_Cat] VARCHAR(50) NULL,
    [CCAU_AURPPD_Reportingperiod] INT NULL,
    [CCAU_AUCOUC_Country_Code] VARCHAR(50) NULL,
    [CCAU_AUVOLC_Volume_Measurement] VARCHAR(50) NULL,
    [CCAU_AUDISC_Distance_Measurement] VARCHAR(50) NULL,
    [CCAU_AUCONC_Consumption_Measurement] VARCHAR(50) NULL,
    [CCAU_AUCURC_Currency_Unit] VARCHAR(50) NULL,
    [CCAU_AUAUBC_Width_Extern] INT NULL,
    [CCAU_AUAUTH_Height_Extern] INT NULL,
    [CCAU_AUAUTL_Length_Extern] INT NULL,
    [CCAU_AUAUHE_Height_Intern] INT NULL,
    [CCAU_AUAUWI_Width_Intern] INT NULL,
    [CCAU_AUAUTG_Max_Train_Weight] INT NULL,
    [CCAU_AUTYCL_Type_Object] INT NULL,
    [CCAU_AUAUAC_Axe_Configuration] VARCHAR(200) NULL,
    [CCAU_AUAUDI_Horse_Power_Din] INT NULL,
    [CCAU_AUAULE_Length_Intern] INT NULL,
    [CCAU_AUMQNO_Residual_Matrix_Number] INT NULL,
    [CCAU_AUMQNR_Maintenace_Matrix_Number] INT NULL,
    [CCAU_AUTYSC_Type_Status_Code] INT NULL,
    [CCAU_AUENAV_Energyaverage] INT NULL,
    [CCAU_AUEUR_Eurostandard] INT NULL,
    [CCAU_AUCAT_Category] VARCHAR(50) NULL,
    [CCAU_AUCO2_Co2emission] INT NULL,

    CONSTRAINT [PK_landing_CCAU] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCAU_timestamp] ON [landing].[CCAU]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCAU_hash] ON [landing].[CCAU]([row_hash]);
GO

-- Table: CCBI
-- Source: ccbi.xlsx
-- Columns: 31
CREATE TABLE [landing].[CCBI] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCBI_BIBIA1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIA2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIAO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIAR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBICM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBILA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBILN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBILO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBINM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBINO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIRN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBISD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBISM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBISQ] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBICC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBISY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBITI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBITT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIV1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIV2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIT1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIT2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIAV] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIBIPL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BICURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BICOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCBI_BIRPPD] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (31 columns with CCBI_ prefix)
    [CCBI_BIBIA1_Billingamount1] DECIMAL(11,4) NULL,
    [CCBI_BIBIA2_Billingamount2] INT NULL,
    [CCBI_BIBIAD_Billingaddress] VARCHAR(50) NULL,
    [CCBI_BIBIAM_Billingamountmonthly] DECIMAL(11,4) NULL,
    [CCBI_BIBIAO_Billingaccountowner] VARCHAR(50) NULL,
    [CCBI_BIBIAR_Bidder_Area_Code] VARCHAR(50) NULL,
    [CCBI_BIBICM_Comments_On_The_Bid] VARCHAR(200) NULL,
    [CCBI_BIBILA_Billinglanguage] VARCHAR(200) NULL,
    [CCBI_BIBILN_Bidder_Language_Code_002n_A_Tab05] VARCHAR(200) NULL,
    [CCBI_BIBILO_Billinglocality] VARCHAR(50) NULL,
    [CCBI_BIBINM_Billingname] VARCHAR(50) NULL,
    [CCBI_BIBINO_Billingnumber] INT NULL,
    [CCBI_BIBIRN_Bidder_Rotation_Number] INT NULL,
    [CCBI_BIBISC_Status_Of_Bid] INT NULL,
    [CCBI_BIBISD_Billingstartday] INT NULL,
    [CCBI_BIBISM_Billingstartmonth] INT NULL,
    [CCBI_BIBISQ_Billingsequence] VARCHAR(200) NULL,
    [CCBI_BIBICC] INT NULL,
    [CCBI_BIBISY_Billingstartyear] INT NULL,
    [CCBI_BIBITI_Title_Of_The_Bidder_002n_A_Tab09] VARCHAR(200) NULL,
    [CCBI_BIBITT_Billingtotal] VARCHAR(200) NULL,
    [CCBI_BIBIV1_First_Vat_Cd_002n_A_Tab18] VARCHAR(200) NULL,
    [CCBI_BIBIV2_Second_Vat_Cd_002n_A_Tab18] VARCHAR(200) NULL,
    [CCBI_BIOBNO_Object_Number] INT NULL,
    [CCBI_BIBIT1_Billingitem1] INT NULL,
    [CCBI_BIBIT2_Billingitem2] INT NULL,
    [CCBI_BIBIAV_Billingamountvariable] DECIMAL(8,4) NULL,
    [CCBI_BIBIPL_Billingperiodlength] DECIMAL(8,4) NULL,
    [CCBI_BICURC_Currency_Unit] VARCHAR(50) NULL,
    [CCBI_BICOUC_Country_Code] VARCHAR(50) NULL,
    [CCBI_BIRPPD_Reportingperiod] INT NULL,

    CONSTRAINT [PK_landing_CCBI] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCBI_timestamp] ON [landing].[CCBI]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCBI_hash] ON [landing].[CCBI]([row_hash]);
GO

-- Table: CCCA
-- Source: ccca.xlsx
-- Columns: 24
CREATE TABLE [landing].[CCCA] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCCA_CACANO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACADS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACADR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACAOB] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACABC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACABY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACABM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACABD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACAEC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACAEY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACAEM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACAED] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACARC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACARY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACARM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACARD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CARRCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACASC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CARPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CALIDI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACATY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACARP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCA_CACAOC] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (24 columns with CCCA_ prefix)
    [CCCA_CACANO_Card_Number] BIGINT NULL,
    [CCCA_CACADS_Card_Description] VARCHAR(200) NULL,
    [CCCA_CACADR] INT NULL,
    [CCCA_CACAOB] INT NULL,
    [CCCA_CACABC_Card_Begin_Date_Century] INT NULL,
    [CCCA_CACABY_Card_Begin_Date_Year] INT NULL,
    [CCCA_CACABM_Card_Begin_Date_Month] INT NULL,
    [CCCA_CACABD_Card_Begin_Date_Day] INT NULL,
    [CCCA_CACAEC_Card_End_Date_Century] INT NULL,
    [CCCA_CACAEY_Card_End_Date_Year] INT NULL,
    [CCCA_CACAEM_Card_End_Date_Month] INT NULL,
    [CCCA_CACAED_Card_End_Date_Day] INT NULL,
    [CCCA_CACARC_Card_Return_Date_Century] INT NULL,
    [CCCA_CACARY_Card_Return_Date_Year] INT NULL,
    [CCCA_CACARM_Card_Return_Date_Month] INT NULL,
    [CCCA_CACARD_Card_Return_Date_Day] INT NULL,
    [CCCA_CARRCD_Card_Return_Code] VARCHAR(50) NULL,
    [CCCA_CACASC_Card_Status] INT NULL,
    [CCCA_CARPPD_Reportingperiod] INT NULL,
    [CCCA_CACOUC_Country_Code] VARCHAR(50) NULL,
    [CCCA_CALIDI_Maximun_Daily_Amount] INT NULL,
    [CCCA_CACATY_Card_Type] VARCHAR(200) NULL,
    [CCCA_CACARP_Card_Reporting_Status] VARCHAR(200) NULL,
    [CCCA_CACAOC_Oil_Company_Code] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCCA] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCCA_timestamp] ON [landing].[CCCA]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCCA_hash] ON [landing].[CCCA]([row_hash]);
GO

-- Table: CCCO
-- Source: ccco.xlsx
-- Columns: 39
CREATE TABLE [landing].[CCCO] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCCO_COCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCONO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COTLCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COAFMG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COMFMG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COAACD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCACD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCOFU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COIVCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COMACD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COMAMG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_CORACD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COROCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COINCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COINMP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COINNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COVICD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COVINO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COPSCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCOSC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COFUMG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COLPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_CORPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_CODISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COSSNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCOPK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_CORVMG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COBPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCDA2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCRPC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COCDAA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_CORCDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_COIRMG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCO_CORCCG] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (39 columns with CCCO_ prefix)
    [CCCO_COCUNO_Customer_Number] INT NULL,
    [CCCO_COCONO_Contract_Number] INT NULL,
    [CCCO_COTLCD_Contract_Type] VARCHAR(50) NULL,
    [CCCO_COAFMG_Administration_Fee_Margin] DECIMAL(8,4) NULL,
    [CCCO_COMFMG_Management_Fee_Margin] DECIMAL(8,4) NULL,
    [CCCO_COAACD_Automobile_Association_Code] VARCHAR(50) NULL,
    [CCCO_COCACD_Card_Code] VARCHAR(50) NULL,
    [CCCO_COCOFU_Fuel_Includedcard_Type_Code] VARCHAR(50) NULL,
    [CCCO_COIVCD_Invoice_Sending_Code] VARCHAR(50) NULL,
    [CCCO_COMACD_Maintenance_Included] VARCHAR(50) NULL,
    [CCCO_COMAMG_Maintenance_Margin] INT NULL,
    [CCCO_CORACD_Radio_Tax_Code] VARCHAR(50) NULL,
    [CCCO_COROCD_Road_Tax_Code] VARCHAR(50) NULL,
    [CCCO_COINCD_Standard_Insurance_Code] INT NULL,
    [CCCO_COINMP_Standard_Insurance_Margin] INT NULL,
    [CCCO_COINNO_Standard_Insurance_Company_A_Name] INT NULL,
    [CCCO_COVICD_Various_Insurance_Code] VARCHAR(200) NULL,
    [CCCO_COVINO_Various_Insurance_Company_Nam_A_E] VARCHAR(200) NULL,
    [CCCO_COPSCD_Passengers_Insurance_Code] VARCHAR(200) NULL,
    [CCCO_COCOSC_Contract_Status_Code] INT NULL,
    [CCCO_COFUMG_Fuel_Margin] INT NULL,
    [CCCO_COLPCD_Company_Code] VARCHAR(50) NULL,
    [CCCO_CORPPD_Reportingperiod] INT NULL,
    [CCCO_COCOUC_Country_Code] VARCHAR(50) NULL,
    [CCCO_COVOLC_Volume_Measurement] VARCHAR(50) NULL,
    [CCCO_CODISC_Distance_Measurement] VARCHAR(50) NULL,
    [CCCO_COCONC_Consumption_Measurement] VARCHAR(50) NULL,
    [CCCO_COCURC_Currency_Unit] VARCHAR(50) NULL,
    [CCCO_COSSNO_Sub_Subsidiary_Number] INT NULL,
    [CCCO_COCOPK_Product_Type_Code] VARCHAR(200) NULL,
    [CCCO_CORVMG_Residual_Value_Margin] INT NULL,
    [CCCO_COBPCD_Third_Party_Liability_Code] VARCHAR(200) NULL,
    [CCCO_COCPCD_Own_Damage_Code] VARCHAR(200) NULL,
    [CCCO_COCDA2] VARCHAR(200) NULL,
    [CCCO_COCRPC_Legal_Advice_Code] VARCHAR(200) NULL,
    [CCCO_COCDAA_Code_Type_Road_Assistance] VARCHAR(200) NULL,
    [CCCO_CORCDD_Yearly_Max_Nbr_Days_Replca] INT NULL,
    [CCCO_COIRMG_Interest_Deviation] DECIMAL(5,4) NULL,
    [CCCO_CORCCG_Rc_Category] VARCHAR(200) NULL,

    CONSTRAINT [PK_landing_CCCO] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCCO_timestamp] ON [landing].[CCCO]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCCO_hash] ON [landing].[CCCO]([row_hash]);
GO

-- Table: CCCP
-- Source: cccp.xlsx
-- Columns: 182
CREATE TABLE [landing].[CCCP] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCCP_CPCPNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPGNLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPPCNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCONO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPRS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPACCP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPDL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPDU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPMI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPPT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPPU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPRV] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPCPTA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPINBS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPIRPT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMDCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRAA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRAF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRDP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRIR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRMF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRPS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRRA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRRO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRVI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMTCT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMTFU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMTMA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMTTR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMTRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPPSIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPUNFU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPUNMA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPUNTR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPUNRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPXMPT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMTWT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRIT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRLE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRPK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRQG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRT9] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRWS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRWT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPMRZP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCP_CPRPPD] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (182 columns with CCCP_ prefix)
    [CCCP_CPCPNO_Calculation_Number] INT NULL,
    [CCCP_CPGNLO_Client_Group_Number] INT NULL,
    [CCCP_CPCUNO_Customer_Number] INT NULL,
    [CCCP_CPPCNO_Profit_Centre_Number] INT NULL,
    [CCCP_CPCONO_Contract_Number] INT NULL,
    [CCCP_CPCPRS_Calculation_Reason] VARCHAR(200) NULL,
    [CCCP_CPACCP_Actual_Calculation] VARCHAR(50) NULL,
    [CCCP_CPCPDL_Delivery_Cost] DECIMAL(7,3) NULL,
    [CCCP_CPCPDU_Duration] INT NULL,
    [CCCP_CPCPMI_Mileage_Per_Year] INT NULL,
    [CCCP_CPCPPT_Reduction] DECIMAL(7,4) NULL,
    [CCCP_CPCPPU_Calculation_Purchase_Price] DECIMAL(11,4) NULL,
    [CCCP_CPCPRV_Residual_Value] DECIMAL(10,4) NULL,
    [CCCP_CPCPTA_Total_Investment] DECIMAL(11,4) NULL,
    [CCCP_CPINBS_Insurance_Amount] DECIMAL(11,4) NULL,
    [CCCP_CPIRPT_Rente] DECIMAL(6,4) NULL,
    [CCCP_CPMKCD_Make_Code] INT NULL,
    [CCCP_CPMDCD_Model_Code] INT NULL,
    [CCCP_CPMRAA_Monthly_Rate_Automobile_Asso_A_Ciation] DECIMAL(8,4) NULL,
    [CCCP_CPMRAF_Monthly_Rate_Administration_A_Fee] DECIMAL(7,4) NULL,
    [CCCP_CPMRDP_Monthly_Rate_Depreciation] DECIMAL(9,4) NULL,
    [CCCP_CPMRIN_Monthly_Rate_Insurance] DECIMAL(8,4) NULL,
    [CCCP_CPMRIR_Monthly_Rate_Interest] DECIMAL(8,4) NULL,
    [CCCP_CPMRMF_Monthly_Rate_Management_Fee] DECIMAL(8,4) NULL,
    [CCCP_CPMRPS_Monthly_Rate_Insurance_Passen_A_Gers] INT NULL,
    [CCCP_CPMRRA_Monthly_Rate_Radio_Tax] DECIMAL(7,4) NULL,
    [CCCP_CPMRRO_Monthly_Rate_Road_Tax] DECIMAL(8,4) NULL,
    [CCCP_CPMRVI_Monthly_Rate_Various_Insuranc_A_E] DECIMAL(7,4) NULL,
    [CCCP_CPMTCT_Total_Cost_Per_Month] DECIMAL(9,4) NULL,
    [CCCP_CPMTFU_Total_Fuel_Cost_Per_Month] INT NULL,
    [CCCP_CPMTMA_Total_Maintenance_Per_Month] DECIMAL(8,4) NULL,
    [CCCP_CPMTTR_Total_Tyres_Per_Month] DECIMAL(8,4) NULL,
    [CCCP_CPMTRC_Total_Replacement_Car_Per_Mon_A_Th] DECIMAL(8,4) NULL,
    [CCCP_CPPSIN_Passengers_Insurance_Company_A_Name] INT NULL,
    [CCCP_CPUNFU_Fuel_Per_Km] INT NULL,
    [CCCP_CPUNMA_Maintenance_Per_Km] DECIMAL(7,6) NULL,
    [CCCP_CPUNTR_Tyres_Per_Km] DECIMAL(7,6) NULL,
    [CCCP_CPUNRC_Replacement_Car_Per_Km] DECIMAL(7,6) NULL,
    [CCCP_CPXMPT_Extra_Margin_Perc_A_Capitalisation_Cost] DECIMAL(5,4) NULL,
    [CCCP_CPMTWT_Total_Winter_Tyres_Per_Month] INT NULL,
    [CCCP_CPMRIT_Monthly_Rate_Casco] INT NULL,
    [CCCP_CPMRCH_Monthly_Rate_Bank_Charges] DECIMAL(9,4) NULL,
    [CCCP_CPMRLE_Monthly_Rate_Legal_Aid] DECIMAL(9,4) NULL,
    [CCCP_CPMRPK_Monthly_Rate_Parking] INT NULL,
    [CCCP_CPMRQG_Monthly_Rate_Garage] INT NULL,
    [CCCP_CPMRT9_Monthly_Rate_Toll] DECIMAL(9,4) NULL,
    [CCCP_CPMRWS_Monthly_Rate_Car_Wash] INT NULL,
    [CCCP_CPMRWT_Winter_Tyres_Per_Km] INT NULL,
    [CCCP_CPMRZP_Monthly_Rate_Postage_Cost] INT NULL,
    [CCCP_CPRPPD_Reportingperiod] INT NULL,
    [CCCP_CPCOUC_Country_Code] VARCHAR(50) NULL,
    [CCCP_CPLPCD_Company_Code] VARCHAR(50) NULL,
    [CCCP_CPBAMR_3th_Party_Liability_Per_Month_A] INT NULL,
    [CCCP_CPMRXD_Monthly_Rate_Extra_Depreciati_A_On] INT NULL,
    [CCCP_CPMRAC_Monthly_Rate_Excise_Acb] INT NULL,
    [CCCP_CPADLP_Advised_Term] VARCHAR(200) NULL,
    [CCCP_CPCPBG_Extra_Weight] INT NULL,
    [CCCP_CPCSMR_Monthly_Rate_Own_Damage] INT NULL,
    [CCCP_CPCPCF_Fuel_Card_Type_Ntcnu] VARCHAR(200) NULL,
    [CCCP_CPAORO_Anonymous_Trailer_Road_Tax] INT NULL,
    [CCCP_CPMRTC_Monthly_Rate_Techn_Test] INT NULL,
    [CCCP_CPMREV_Monthly_Rate_Eurovignet] INT NULL,
    [CCCP_CPRBMR_Rb_Monthly_Rate_Legal_Assist] INT NULL,
    [CCCP_CPCPTV_Transport_License_Number] VARCHAR(200) NULL,
    [CCCP_CPCPCC_Calculation_Date_Century] INT NULL,
    [CCCP_CPCPYY_Calculation_Date_Year] INT NULL,
    [CCCP_CPCPMM_Calculation_Date_Month] INT NULL,
    [CCCP_CPCPDD_Calculation_Date_Day] INT NULL,
    [CCCP_CPSTSC_Calculation_Followup_Status] VARCHAR(200) NULL,
    [CCCP_CPXTUN_Extra_Rate_For_Suppl_Units] DECIMAL(7,6) NULL,
    [CCCP_CPISEG_Insurance_Interest] INT NULL,
    [CCCP_CPIASC_Road_Assistance_Interest] INT NULL,
    [CCCP_CPCIMC_Tax_Interest] INT NULL,
    [CCCP_CPASEG_Insurance_Depreciation] INT NULL,
    [CCCP_CPAASC_Road_Assistance_Depreciation] INT NULL,
    [CCCP_CPAIMC_Taxe_Depreciation] INT NULL,
    [CCCP_CPMSEG_Monthly_Insurance_Interest] INT NULL,
    [CCCP_CPMASC_Monthly_Road_Assist_Interest] INT NULL,
    [CCCP_CPMIMC_Monthly_Taxe_Interest] INT NULL,
    [CCCP_CPVSEG_Insurance_Sales_Prices] INT NULL,
    [CCCP_CPVASC_Sales_Prices_Road_Assistance] INT NULL,
    [CCCP_CPVIMC_Taxe_Sales_Prices] INT NULL,
    [CCCP_CPAJUS_Ajustement_Yn] INT NULL,
    [CCCP_CPTSEG_Vat_Insurance] INT NULL,
    [CCCP_CPTASC_Vat_Road_Assistance] INT NULL,
    [CCCP_CPTIMC_Vat_Taxe] INT NULL,
    [CCCP_CPUSEG_Vat_Amount_Insurance] DECIMAL(8,4) NULL,
    [CCCP_CPUASC_Vat_Amount_Road_Assist] DECIMAL(7,4) NULL,
    [CCCP_CPUIMC_Vat_Amount_Taxe] DECIMAL(8,4) NULL,
    [CCCP_CPBMAN_Maintenance_Based_Amount] INT NULL,
    [CCCP_CPBVEH_Repl_Car_Based_Amount] INT NULL,
    [CCCP_CPBGAS_Fuel_Base_Amount_For_Taxes] INT NULL,
    [CCCP_CPBNEU_Tyres_Based_Amount] INT NULL,
    [CCCP_CPTMAN_Vat_Maintenance] INT NULL,
    [CCCP_CPTVEH_Vat_Replacement_Car] INT NULL,
    [CCCP_CPTGAS_Vat_Fuel] INT NULL,
    [CCCP_CPTNEU_Vat_Tyres] INT NULL,
    [CCCP_CPUMAN_Monthly_Maint_Based_Amount] INT NULL,
    [CCCP_CPUVEH_Monthly_Replacement_Car] INT NULL,
    [CCCP_CPUGAS_Monthly_Fuel] INT NULL,
    [CCCP_CPUNEU_Monthly_Tyres] INT NULL,
    [CCCP_CPGION_Location_Of_Quote] INT NULL,
    [CCCP_CPIIMA_Taxe_Interest] INT NULL,
    [CCCP_CPCIMA_Monthly_Taxe_Interest] INT NULL,
    [CCCP_CPMIMA_Taxe_Iedmt] INT NULL,
    [CCCP_CPVIMA_Iedmt_Amount] INT NULL,
    [CCCP_CPTIMA_Vat_Ratio_On_Iedmt] INT NULL,
    [CCCP_CPUIMA_Vat_On_Iedmt] INT NULL,
    [CCCP_CPIIMC_Monthly_Vat_On_Iedmt] INT NULL,
    [CCCP_CPIAMC_Monthly_Vat_On_Imc] INT NULL,
    [CCCP_CPFHVA_Monthly_Road_Assistance_Plus] INT NULL,
    [CCCP_CPFJVA_Sales_Prices_Road_Assistance_A_Plus] INT NULL,
    [CCCP_CPFKVA_Vat_Interest_Road_Assistance] INT NULL,
    [CCCP_CPFLVA_Vat_Amount_Road_Assistance] INT NULL,
    [CCCP_CPFMVA_Base_Amount_Iedmt] INT NULL,
    [CCCP_CPFNVA_Insurance_Adm_Fee] INT NULL,
    [CCCP_CPFOVA_Vat_Insurance_Adm_Fee] INT NULL,
    [CCCP_CPFIVA_Monthly_Amount_Road_Assist] INT NULL,
    [CCCP_CPINCO_Insurance_Contractor] VARCHAR(200) NULL,
    [CCCP_CPINNU] INT NULL,
    [CCCP_CPINNA] VARCHAR(200) NULL,
    [CCCP_CPCPCP_O_Partial_Coverage_Insurance_A] INT NULL,
    [CCCP_CPCPD1_Driver_Contribution_Amount] INT NULL,
    [CCCP_CPCPF1_Financial_Amount] INT NULL,
    [CCCP_CPCPF2_Financial_Factor] INT NULL,
    [CCCP_CPCPPS_Reduction] INT NULL,
    [CCCP_CPCPRD_Reference_Contribution] INT NULL,
    [CCCP_CPCPSD_Pre_Contract_Duration] INT NULL,
    [CCCP_CPCPSM_Pre_Contract_Kilometers] INT NULL,
    [CCCP_CPMRLS_Other_Cost_Lump_Sum] INT NULL,
    [CCCP_CPMWTP_Wintertyres] DECIMAL(8,4) NULL,
    [CCCP_CPPCPC_Comprehensive_Rate] INT NULL,
    [CCCP_CPPCSP_Comprehensive_Own_Part_Costs] INT NULL,
    [CCCP_CPPCVC_Car_Van_Code] VARCHAR(200) NULL,
    [CCCP_CPPINI_Interest_Insurance] INT NULL,
    [CCCP_CPPINT_Interest_Motor_Vehicule_Tax] INT NULL,
    [CCCP_CPPMKM_Monthly_Kilometers] INT NULL,
    [CCCP_CPPPPC_Part_Coverage_Rate] INT NULL,
    [CCCP_CPPPSP_Part_Coverage_Own_Part_Cost] INT NULL,
    [CCCP_CPPRAF_Fix_Refer_Amount] INT NULL,
    [CCCP_CPPRKM_Residual_Kilometers] INT NULL,
    [CCCP_CPPRVP_Residual_Value] INT NULL,
    [CCCP_CPPTKM_Total_Kilometers] INT NULL,
    [CCCP_CPPYKM_Year_Kilometers] INT NULL,
    [CCCP_CPPSKM_Fix_Day_Kilometers] INT NULL,
    [CCCP_CPMRI2] INT NULL,
    [CCCP_CPBANU] INT NULL,
    [CCCP_CPBANA] VARCHAR(200) NULL,
    [CCCP_CPCSNU] INT NULL,
    [CCCP_CPCSNA] VARCHAR(200) NULL,
    [CCCP_CPRBNU] INT NULL,
    [CCCP_CPRBNA] VARCHAR(200) NULL,
    [CCCP_CPPSNU] INT NULL,
    [CCCP_CPPSNA] VARCHAR(200) NULL,
    [CCCP_CPVINU] INT NULL,
    [CCCP_CPVINA] VARCHAR(200) NULL,
    [CCCP_CPXMDC_Extra_Margin_Amount_Discoun_A_T] INT NULL,
    [CCCP_CPOABC_Recalculation_Start_Date_Cent_A_Ury] INT NULL,
    [CCCP_CPOABY_Recalculation_Start_Date_Year_A] INT NULL,
    [CCCP_CPOABM_Recalculation_Start_Date_Mont_A_H] INT NULL,
    [CCCP_CPOABD_Recalculation_Start_Date_Day] INT NULL,
    [CCCP_CPOAUC_Recalculation_Process_Date_Ce_A_Ntury] INT NULL,
    [CCCP_CPOAUY_Recalculation_Process_Date_Ye_A_Ar] INT NULL,
    [CCCP_CPOAUM_Recalculation_Process_Date_Mo_A_Nth] INT NULL,
    [CCCP_CPOAUD_Recalculation_Process_Date_Da_A_Y] INT NULL,
    [CCCP_CPCPYD_Replacement_Car_Daysyear] INT NULL,
    [CCCP_CPCPCG_Replacement_Car_Category] VARCHAR(200) NULL,
    [CCCP_CPDURC_Planned_Replacement_Car] INT NULL,
    [CCCP_CPDUMA_Planned_Maintenance] INT NULL,
    [CCCP_CPDUTR_Planned_Tyres] INT NULL,
    [CCCP_CPCPNM_Number_Of_Tyres] INT NULL,
    [CCCP_CPCPAM_Calculation_Purchase_Vat_Incl_A] DECIMAL(11,4) NULL,
    [CCCP_CPUNWA_Car_Wash_Per_Km] INT NULL,
    [CCCP_CPXALP_Calculated_Extra_Amount_003n_A_Included_] INT NULL,
    [CCCP_CPXANL_Calculated_Extra_Amount_Not_003n_A_Inclu] INT NULL,
    [CCCP_CPXELP_Calculated_Extra_Amount_003n_A_Icluded_I] DECIMAL(10,4) NULL,
    [CCCP_CPXENL_Calculated_Extra_Amount_Not_003n_A_Iclud] INT NULL,
    [CCCP_CPTXB] INT NULL,
    [CCCP_CPTXA] INT NULL,
    [CCCP_CPXTDP_Extra_Deprsuppl_Units] DECIMAL(7,6) NULL,
    [CCCP_CPXLUN] DECIMAL(7,6) NULL,
    [CCCP_CPXLDP] DECIMAL(7,6) NULL,

    CONSTRAINT [PK_landing_CCCP] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCCP_timestamp] ON [landing].[CCCP]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCCP_hash] ON [landing].[CCCP]([row_hash]);
GO

-- Table: CCCU
-- Source: cccu.xlsx
-- Columns: 50
CREATE TABLE [landing].[CCCU] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCCU_CUNULO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUGNLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNOLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNOL2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNOL3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCLLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCPLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUADLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CULOLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCALO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNTLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNFLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNELO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCTLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CURPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CURPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPEC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPEY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPEM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPED] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPNC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPNV] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPNT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUPXNC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUPXNV] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUPXNT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUFLCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCVLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPRS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCLPE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CURSFL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CURSFN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CURSLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNEL2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNEL3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CURPAS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPBY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPBM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPBD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUBLKB] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUKBKB] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUKRKR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CULLKB] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPAC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPAV] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUAPAT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUCCLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCCU_CUNKLO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (50 columns with CCCU_ prefix)
    [CCCU_CUNULO_Client_Number] INT NULL,
    [CCCU_CUGNLO_Client_Group_Number] INT NULL,
    [CCCU_CUNOLO_Name] VARCHAR(50) NULL,
    [CCCU_CUNOL2_Name_Cont] VARCHAR(50) NULL,
    [CCCU_CUNOL3_Name_Cont] VARCHAR(50) NULL,
    [CCCU_CUCLLO_Language_Code] VARCHAR(50) NULL,
    [CCCU_CUCPLO_Country_Code] VARCHAR(50) NULL,
    [CCCU_CUADLO_Address] VARCHAR(50) NULL,
    [CCCU_CULOLO_Locality] VARCHAR(50) NULL,
    [CCCU_CUCALO_Area_Code] VARCHAR(50) NULL,
    [CCCU_CUNTLO_Telephone_Number] VARCHAR(50) NULL,
    [CCCU_CUNFLO_Fax_Number] VARCHAR(50) NULL,
    [CCCU_CUNELO_Email_Address] VARCHAR(200) NULL,
    [CCCU_CUCTLO_Title_Code] VARCHAR(200) NULL,
    [CCCU_CURPPD_Reportingperiod] INT NULL,
    [CCCU_CUCOUC_Country_Code] VARCHAR(50) NULL,
    [CCCU_CURPCD_Representative_Code] VARCHAR(50) NULL,
    [CCCU_CUAPEC_Approval_End_Date_Century] INT NULL,
    [CCCU_CUAPEY_Approval_End_Date_Year] INT NULL,
    [CCCU_CUAPEM_Approval_End_Date_Month] INT NULL,
    [CCCU_CUAPED_Approval_End_Date_Day] INT NULL,
    [CCCU_CUAPNC_Approved_Number_Of_Cars] INT NULL,
    [CCCU_CUAPNV_Approved_Number_Of_Vans] INT NULL,
    [CCCU_CUAPNT_Approved_Number_Of_Trucks] INT NULL,
    [CCCU_CUPXNC_Potential_Number_Of_Cars] INT NULL,
    [CCCU_CUPXNV_Potential_Number_Of_Vans] INT NULL,
    [CCCU_CUPXNT_Potential_Number_Of_Trucks] INT NULL,
    [CCCU_CUFLCD_Fleet_Conditions_Code] INT NULL,
    [CCCU_CUCVLO_Code_Branch_Client] INT NULL,
    [CCCU_CUAPRS_Approval_Comments] VARCHAR(200) NULL,
    [CCCU_CUCLPE_Office_Code] VARCHAR(200) NULL,
    [CCCU_CURSFL_Financial_Leasing_Responsible_A] VARCHAR(200) NULL,
    [CCCU_CURSFN_Fleet_Responsible] VARCHAR(200) NULL,
    [CCCU_CURSLO_Operational_Leasing_Responsib_A_Le] VARCHAR(50) NULL,
    [CCCU_CUNEL2] VARCHAR(200) NULL,
    [CCCU_CUNEL3] VARCHAR(200) NULL,
    [CCCU_CURPAS_After_Sales_Represent] VARCHAR(200) NULL,
    [CCCU_CUAPBC_Approval_Start_Date_Century] INT NULL,
    [CCCU_CUAPBY_Approval_Start_Date_Year] INT NULL,
    [CCCU_CUAPBM_Approval_Start_Date_Month] INT NULL,
    [CCCU_CUAPBD_Approval_Start_Date_Day] INT NULL,
    [CCCU_CUBLKB] VARCHAR(200) NULL,
    [CCCU_CUKBKB] VARCHAR(200) NULL,
    [CCCU_CUKRKR_Accountance_Customer_Service_A_Code] VARCHAR(200) NULL,
    [CCCU_CULLKB] VARCHAR(200) NULL,
    [CCCU_CUAPAC_Approval_Amount_Cars] INT NULL,
    [CCCU_CUAPAV_Approval_Amount_Vans] INT NULL,
    [CCCU_CUAPAT_Approval_Amount_Truks] INT NULL,
    [CCCU_CUCCLO_Contentment_Client_Code] INT NULL,
    [CCCU_CUNKLO_Sub_Branch_Code] INT NULL,

    CONSTRAINT [PK_landing_CCCU] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCCU_timestamp] ON [landing].[CCCU]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCCU_hash] ON [landing].[CCCU]([row_hash]);
GO

-- Table: CCDA
-- Source: CCDA.txt (Damages by Object Number)
-- Columns: 156
CREATE TABLE [landing].[CCDA] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCDA_DAOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADANO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADADR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADACC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADADD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADADS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAD2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAD3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAAR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADALA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAMI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAFF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAC1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAC2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAC3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DACOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DASTCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADVCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADASN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADATY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAPE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADASC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPIAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAMNFA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DATOCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADASQ] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADMM1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADMM2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADRG1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADRG2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAPS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DASVAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPEA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARSGA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARSAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARSCP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARSLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARSTL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADARP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADARC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARERP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DARERC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADRRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADRRY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADRRM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADRRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPPCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPPYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPPMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPPDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPRCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPRYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPRMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPRDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFOCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFOYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFOMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFODD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIRPC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIRPY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIRPM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIRPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRPC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRPY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRPM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRRY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRRM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAFRRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAREPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAICRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAICRY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAICRM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAICRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIPTC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIPTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIPTM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAIPTD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPPC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPPY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPPM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPRY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPRM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAPRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFOC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFOY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFOM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFOD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAIRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAIRY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAIRM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAIRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFPC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFPY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFPM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFRY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFRM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAFRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAREP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAICC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAICY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAICM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAICD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAD4] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAD5] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DATPAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DATPPC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DATPIC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DATPLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DATPNM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAGD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAGM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAGC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAGY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAI2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAAMBN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPRCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAXD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAXM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAXC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAXY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAID] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAIM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAIC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAIY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAED] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAEM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAEC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAEY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAVTCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAINCN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAINNA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAMOAL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAINRF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAWROF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DASALD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAFL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAMO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAMR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DATPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADRNX] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DADAC4] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DACPRV] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAPLDA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDA_DAINT2] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (156 columns with CCDA_ prefix, DA-prefix damage fields)
    [CCDA_DAOBNO_Object_Number] INT NULL,
    [CCDA_DADANO_Damage_Number] VARCHAR(15) NULL,
    [CCDA_DADADR_Driver_Number] INT NULL,
    [CCDA_DADACC_Damage_Date_Century] INT NULL,
    [CCDA_DADAYY_Damage_Date_Year] INT NULL,
    [CCDA_DADAMM_Damage_Date_Month] INT NULL,
    [CCDA_DADADD_Damage_Date_Day] INT NULL,
    [CCDA_DADADS_Accident_Description_1] VARCHAR(80) NULL,
    [CCDA_DADAD2_Accident_Description_2] VARCHAR(80) NULL,
    [CCDA_DADAD3_Accident_Description_3] VARCHAR(80) NULL,
    [CCDA_DADAAD_Accident_Location_Address] VARCHAR(30) NULL,
    [CCDA_DADAAR_Accident_Area_Code] VARCHAR(10) NULL,
    [CCDA_DADALA_Accident_Country_Code] VARCHAR(3) NULL,
    [CCDA_DADAAM_Damage_Repair_Amount] DECIMAL(17,6) NULL,
    [CCDA_DADAMI_Damage_Mileage] INT NULL,
    [CCDA_DADAFF_Fault_Code] VARCHAR(3) NULL,
    [CCDA_DADAC1_Damage_Comments_Code_1] VARCHAR(3) NULL,
    [CCDA_DADAC2_Damage_Comments_Code_2] VARCHAR(3) NULL,
    [CCDA_DADAC3_Damage_Comments_Code_3] VARCHAR(3) NULL,
    [CCDA_DARPPD_Reporting_Period] INT NULL,
    [CCDA_DACOUC_Country_Code] VARCHAR(3) NULL,
    [CCDA_DASTCD_Damage_Recourse] VARCHAR(3) NULL,
    [CCDA_DADVCD_Damage_Tow] VARCHAR(1) NULL,
    [CCDA_DADASN_Damage_Repair_Supplier] BIGINT NULL,
    [CCDA_DADATY_Damage_Type] VARCHAR(3) NULL,
    [CCDA_DADAPE_Responsible_Person_Insurance] VARCHAR(10) NULL,
    [CCDA_DADASC_Damage_Status_Code] VARCHAR(1) NULL,
    [CCDA_DAPIAM_Amount_Excl_VAT] DECIMAL(17,6) NULL,
    [CCDA_DAMNFA_Accident_Cost] DECIMAL(17,6) NULL,
    [CCDA_DATOCD_Total_Loss_Code] VARCHAR(3) NULL,
    [CCDA_DADASQ_Supplier_Rotation_Number] VARCHAR(3) NULL,
    [CCDA_DADMM1_Model_Car_Second_Part] VARCHAR(25) NULL,
    [CCDA_DADMM2_Model_Car_Third_Part] VARCHAR(25) NULL,
    [CCDA_DADRG1_Registration_Second_Part] VARCHAR(8) NULL,
    [CCDA_DADRG2_Registration_Third_Part] VARCHAR(8) NULL,
    [CCDA_DADAPS_Damage_Place_Comment] VARCHAR(30) NULL,
    [CCDA_DASVAM_Damage_Amount_Expected] DECIMAL(17,6) NULL,
    [CCDA_DAAPEA_European_Assistance_Process] VARCHAR(20) NULL,
    [CCDA_DARSGA_Garage_Name] VARCHAR(35) NULL,
    [CCDA_DARSAD_Garage_Address] VARCHAR(35) NULL,
    [CCDA_DARSCP_Garage_Post_Code] VARCHAR(10) NULL,
    [CCDA_DARSLC_Garage_Locality] VARCHAR(25) NULL,
    [CCDA_DARSTL_Garage_Telephone] VARCHAR(12) NULL,
    [CCDA_DADARP_Claimed_Deductible_Repair] DECIMAL(17,6) NULL,
    [CCDA_DADARC_Claimed_Rent_A_Car] DECIMAL(17,6) NULL,
    [CCDA_DARERP_Refunded_Deductible_Repair] DECIMAL(17,6) NULL,
    [CCDA_DARERC_Refunded_Rent_A_Car] DECIMAL(17,6) NULL,
    [CCDA_DADRRC_Refund_Request_Date_Century] INT NULL,
    [CCDA_DADRRY_Refund_Request_Date_Year] INT NULL,
    [CCDA_DADRRM_Refund_Request_Date_Month] INT NULL,
    [CCDA_DADRRD_Refund_Request_Date_Day] INT NULL,
    [CCDA_DAPPCC_Expected_Insurance_Date_Century] INT NULL,
    [CCDA_DAPPYY_Expected_Insurance_Date_Year] INT NULL,
    [CCDA_DAPPMM_Expected_Insurance_Date_Month] INT NULL,
    [CCDA_DAPPDD_Expected_Insurance_Date_Day] INT NULL,
    [CCDA_DAPRCC_Real_Insurance_Date_Century] INT NULL,
    [CCDA_DAPRYY_Real_Insurance_Date_Year] INT NULL,
    [CCDA_DAPRMM_Real_Insurance_Date_Month] INT NULL,
    [CCDA_DAPRDD_Real_Insurance_Date_Day] INT NULL,
    [CCDA_DAFOCC_Expected_Budget_Due_Date_Century] INT NULL,
    [CCDA_DAFOYY_Expected_Budget_Due_Date_Year] INT NULL,
    [CCDA_DAFOMM_Expected_Budget_Due_Date_Month] INT NULL,
    [CCDA_DAFODD_Expected_Budget_Due_Date_Day] INT NULL,
    [CCDA_DAIRPC_Expected_Begin_Repair_Date_Century] INT NULL,
    [CCDA_DAIRPY_Expected_Begin_Repair_Date_Year] INT NULL,
    [CCDA_DAIRPM_Expected_Begin_Repair_Date_Month] INT NULL,
    [CCDA_DAIRPD_Expected_Begin_Repair_Date_Day] INT NULL,
    [CCDA_DAFRPC_Expected_End_Repair_Date_Century] INT NULL,
    [CCDA_DAFRPY_Expected_End_Repair_Date_Year] INT NULL,
    [CCDA_DAFRPM_Expected_End_Repair_Date_Month] INT NULL,
    [CCDA_DAFRPD_Expected_End_Repair_Date_Day] INT NULL,
    [CCDA_DAFRRC_Real_End_Repair_Date_Century] INT NULL,
    [CCDA_DAFRRY_Real_End_Repair_Date_Year] INT NULL,
    [CCDA_DAFRRM_Real_End_Repair_Date_Month] INT NULL,
    [CCDA_DAFRRD_Real_End_Repair_Date_Day] INT NULL,
    [CCDA_DAREPD_Number_Repair_Days] INT NULL,
    [CCDA_DAICRC_Client_Informed_End_Repair_Century] INT NULL,
    [CCDA_DAICRY_Client_Informed_End_Repair_Year] INT NULL,
    [CCDA_DAICRM_Client_Informed_End_Repair_Month] INT NULL,
    [CCDA_DAICRD_Client_Informed_End_Repair_Day] INT NULL,
    [CCDA_DAIPTC_Inform_Insurance_Total_Loss_Century] INT NULL,
    [CCDA_DAIPTY_Inform_Insurance_Total_Loss_Year] INT NULL,
    [CCDA_DAIPTM_Inform_Insurance_Total_Loss_Month] INT NULL,
    [CCDA_DAIPTD_Inform_Insurance_Total_Loss_Day] INT NULL,
    [CCDA_DAAPPC_Add_Expected_Insurance_Century] INT NULL,
    [CCDA_DAAPPY_Add_Expected_Insurance_Year] INT NULL,
    [CCDA_DAAPPM_Add_Expected_Insurance_Month] INT NULL,
    [CCDA_DAAPPD_Add_Expected_Insurance_Day] INT NULL,
    [CCDA_DAAPRC_Add_Real_Insurance_Century] INT NULL,
    [CCDA_DAAPRY_Add_Real_Insurance_Year] INT NULL,
    [CCDA_DAAPRM_Add_Real_Insurance_Month] INT NULL,
    [CCDA_DAAPRD_Add_Real_Insurance_Day] INT NULL,
    [CCDA_DAAFOC_Add_Expected_Budget_Due_Century] INT NULL,
    [CCDA_DAAFOY_Add_Expected_Budget_Due_Year] INT NULL,
    [CCDA_DAAFOM_Add_Expected_Budget_Due_Month] INT NULL,
    [CCDA_DAAFOD_Add_Expected_Budget_Due_Day] INT NULL,
    [CCDA_DAAIRC_Add_Expected_Begin_Repair_Century] INT NULL,
    [CCDA_DAAIRY_Add_Expected_Begin_Repair_Year] INT NULL,
    [CCDA_DAAIRM_Add_Expected_Begin_Repair_Month] INT NULL,
    [CCDA_DAAIRD_Add_Expected_Begin_Repair_Day] INT NULL,
    [CCDA_DAAFPC_Add_Expected_End_Repair_Century] INT NULL,
    [CCDA_DAAFPY_Add_Expected_End_Repair_Year] INT NULL,
    [CCDA_DAAFPM_Add_Expected_End_Repair_Month] INT NULL,
    [CCDA_DAAFPD_Add_Expected_End_Repair_Day] INT NULL,
    [CCDA_DAAFRC_Add_Real_End_Repair_Century] INT NULL,
    [CCDA_DAAFRY_Add_Real_End_Repair_Year] INT NULL,
    [CCDA_DAAFRM_Add_Real_End_Repair_Month] INT NULL,
    [CCDA_DAAFRD_Add_Real_End_Repair_Day] INT NULL,
    [CCDA_DAAREP_Add_Number_Repair_Days] INT NULL,
    [CCDA_DAAICC_Add_Client_Informed_End_Repair_Century] INT NULL,
    [CCDA_DAAICY_Add_Client_Informed_End_Repair_Year] INT NULL,
    [CCDA_DAAICM_Add_Client_Informed_End_Repair_Month] INT NULL,
    [CCDA_DAAICD_Add_Client_Informed_End_Repair_Day] INT NULL,
    [CCDA_DADAD4_Accident_Description_4] VARCHAR(80) NULL,
    [CCDA_DADAD5_Accident_Description_5] VARCHAR(80) NULL,
    [CCDA_DATPAD_Third_Party_Address] VARCHAR(35) NULL,
    [CCDA_DATPPC_Third_Party_Postal_Code] VARCHAR(10) NULL,
    [CCDA_DATPIC_Insurance_Co_Third_Party] BIGINT NULL,
    [CCDA_DATPLO_Third_Party_Locality] VARCHAR(35) NULL,
    [CCDA_DATPNM_Third_Party_Name] VARCHAR(35) NULL,
    [CCDA_DADAGD_Garage_Entrance_Date_Day] INT NULL,
    [CCDA_DADAGM_Garage_Entrance_Date_Month] INT NULL,
    [CCDA_DADAGC_Garage_Entrance_Date_Century] INT NULL,
    [CCDA_DADAGY_Garage_Entrance_Date_Year] INT NULL,
    [CCDA_DADAIN_Damage_Coverage_Code] VARCHAR(10) NULL,
    [CCDA_DADAI2_Theoretical_Damage_Coverage_Code] VARCHAR(3) NULL,
    [CCDA_DAAMBN_Amount_Brutto_Netto_Code] VARCHAR(1) NULL,
    [CCDA_DAPRCD_Problem_Encountered_Code] VARCHAR(3) NULL,
    [CCDA_DADAXD_Date_X_Day] INT NULL,
    [CCDA_DADAXM_Date_X_Month] INT NULL,
    [CCDA_DADAXC_Date_X_Century] INT NULL,
    [CCDA_DADAXY_Date_X_Year] INT NULL,
    [CCDA_DADAID_Date_I_Day] INT NULL,
    [CCDA_DADAIM_Date_I_Month] INT NULL,
    [CCDA_DADAIC_Date_I_Century] INT NULL,
    [CCDA_DADAIY_Date_I_Year] INT NULL,
    [CCDA_DADAED_Date_E_Day] INT NULL,
    [CCDA_DADAEM_Date_E_Month] INT NULL,
    [CCDA_DADAEC_Date_E_Century] INT NULL,
    [CCDA_DADAEY_Date_E_Year] INT NULL,
    [CCDA_DAVTCD_VAT_Code] VARCHAR(3) NULL,
    [CCDA_DAINCN_Insurance_Co_Number] BIGINT NULL,
    [CCDA_DAINNA_Insurance_Co_Rotation_Number] VARCHAR(3) NULL,
    [CCDA_DAMOAL_Monthly_Allowance] DECIMAL(17,6) NULL,
    [CCDA_DAINRF_Reimbursed_Flow_Insurance] DECIMAL(17,6) NULL,
    [CCDA_DAWROF_Write_Off] DECIMAL(17,6) NULL,
    [CCDA_DASALD_Saldo] DECIMAL(17,6) NULL,
    [CCDA_DADAFL_Damage_Fault_Level] VARCHAR(3) NULL,
    [CCDA_DADAMO_Amount_Own_Risk] DECIMAL(17,6) NULL,
    [CCDA_DADAMR_Amount_Refunded] DECIMAL(17,6) NULL,
    [CCDA_DATPCD_Third_Party_Code] VARCHAR(3) NULL,
    [CCDA_DADRNX_Driver_Name_Occasionally] VARCHAR(30) NULL,
    [CCDA_DADAC4_Damage_Comments_Code_4] VARCHAR(5) NULL,
    [CCDA_DACPRV_Residual_Value] DECIMAL(17,6) NULL,
    [CCDA_DAPLDA_Amount_Profit_Lost] DECIMAL(17,6) NULL,
    [CCDA_DAINT2_Amount_Special_Own_Risk] FLOAT NULL,

    CONSTRAINT [PK_landing_CCDA] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCDA_timestamp] ON [landing].[CCDA]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCDA_hash] ON [landing].[CCDA]([row_hash]);
GO

-- Table: CCDR
-- Source: ccdr.xlsx
-- Columns: 63
CREATE TABLE [landing].[CCDR] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCDR_DROBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRACDR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRNM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRFN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRLN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRAR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRLA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRTI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRPVTF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DROFTF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRPVNF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DROFNF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRNE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRSN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDPNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRNINO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRPRNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDRBK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCANO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRINPC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCUCT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRMOAL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DREMGR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRENCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRENYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRENMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRENDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DREBCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DREBYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DREBMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DREBDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRJBTI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRLNMG] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDCCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDCYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDCMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRDCDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRTXYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRBUKM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCBBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCBBY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCBBM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCBBD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCBEC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCBEY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCDR_DRCBEM] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (63 columns with CCDR_ prefix)
    [CCDR_DROBNO_Object_Number] INT NULL,
    [CCDR_DRDRNO_Driver_Number] INT NULL,
    [CCDR_DRACDR_Actual_Driver_Code] VARCHAR(50) NULL,
    [CCDR_DRDRNM_Driver_Name] VARCHAR(50) NULL,
    [CCDR_DRDRFN_Driver_First_Name] VARCHAR(200) NULL,
    [CCDR_DRDRLN_Driver_Language_Code] VARCHAR(50) NULL,
    [CCDR_DRDRAD_Driver_Address] VARCHAR(50) NULL,
    [CCDR_DRDRAR_Driver_Area_Code] VARCHAR(50) NULL,
    [CCDR_DRDRLO_Driver_Locality] VARCHAR(50) NULL,
    [CCDR_DRDRLA_Driver_Country_Code] VARCHAR(50) NULL,
    [CCDR_DRDRTI_Driver_Title] VARCHAR(50) NULL,
    [CCDR_DRPVTF_Private_Telephone_Number] VARCHAR(50) NULL,
    [CCDR_DROFTF_Office_Telephone_Number] VARCHAR(50) NULL,
    [CCDR_DRPVNF_Private_Fax_Number] VARCHAR(50) NULL,
    [CCDR_DROFNF_Office_Fax_Number] VARCHAR(50) NULL,
    [CCDR_DRDRNE_Email_Number] VARCHAR(50) NULL,
    [CCDR_DRDRSN_Staff_Number] VARCHAR(50) NULL,
    [CCDR_DRDRAM_Participation_Amount] INT NULL,
    [CCDR_DRDPNO_Department_Number_Name] VARCHAR(50) NULL,
    [CCDR_DRNINO_National_Identification_Numbe_A_R] VARCHAR(50) NULL,
    [CCDR_DRPRNO_Payroll_Number] VARCHAR(200) NULL,
    [CCDR_DRDRBK_Bank_Account_Number] INT NULL,
    [CCDR_DRCANO_Card_Number] INT NULL,
    [CCDR_DRINPC_Customer_Internal_Cost_Centre_A] VARCHAR(50) NULL,
    [CCDR_DRCUCT] VARCHAR(50) NULL,
    [CCDR_DRMOAL_Monthly_Allowance] INT NULL,
    [CCDR_DREMGR_Employee_Gradee] VARCHAR(200) NULL,
    [CCDR_DRENCC_Employee_Termination_Date_Cen_A_Tury] INT NULL,
    [CCDR_DRENYY_Employee_Termination_Date_Yea_A_R] INT NULL,
    [CCDR_DRENMM_Employee_Termination_Date_Mon_A_Th] INT NULL,
    [CCDR_DRENDD_Employee_Termination_Date_Day_A] INT NULL,
    [CCDR_DREBCC_Employee_Begin_Date_Century] INT NULL,
    [CCDR_DREBYY_Employee_Begin_Date_Year] INT NULL,
    [CCDR_DREBMM_Employee_Begin_Date_Month] INT NULL,
    [CCDR_DREBDD_Employee_Begin_Date_Day] INT NULL,
    [CCDR_DRJBTI_Job_Tittle] VARCHAR(200) NULL,
    [CCDR_DRLNMG_Line_Manager] VARCHAR(50) NULL,
    [CCDR_DRDCCC_Department_Change_Date_Centur_A_Y] INT NULL,
    [CCDR_DRDCYY_Department_Change_Date_Year] INT NULL,
    [CCDR_DRDCMM_Department_Change_Date_Month] INT NULL,
    [CCDR_DRDCDD_Department_Change_Date_Day] INT NULL,
    [CCDR_DRTXYY_Tax_Year] INT NULL,
    [CCDR_DRBUKM_Actual_Business_Mileage] INT NULL,
    [CCDR_DRCBBC_Contribution_Start_Date_Centu_A_Ry] INT NULL,
    [CCDR_DRCBBY_Contribution_Start_Date_Year] INT NULL,
    [CCDR_DRCBBM_Contribution_Start_Date_Month_A] INT NULL,
    [CCDR_DRCBBD_Contribution_Start_Date_Day] INT NULL,
    [CCDR_DRCBEC_Contribution_End_Date_Century_A] INT NULL,
    [CCDR_DRCBEY_Contribution_End_Date_Year] INT NULL,
    [CCDR_DRCBEM_Contribution_End_Date_Month] INT NULL,
    [CCDR_DRCBED_Contribution_End_Date_Day] INT NULL,
    [CCDR_DRCBAM_Contribution_Amount] INT NULL,
    [CCDR_DRQTRT_Driver_Quoted_Rental] INT NULL,
    [CCDR_DRDRBC_Driver_Start_Date_Century] INT NULL,
    [CCDR_DRDRBY_Driver_Start_Date_Year] INT NULL,
    [CCDR_DRDRBM_Driver_Start_Date_Month] INT NULL,
    [CCDR_DRDRBD_Driver_Start_Date_Day] INT NULL,
    [CCDR_DRDREC] INT NULL,
    [CCDR_DRDREY] INT NULL,
    [CCDR_DRDREM_Driver_End_Date_Month] INT NULL,
    [CCDR_DRDRED] INT NULL,
    [CCDR_DRRPPD_Reportingperiod] INT NULL,
    [CCDR_DRCOUC_Country_Code] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCDR] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCDR_timestamp] ON [landing].[CCDR]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCDR_hash] ON [landing].[CCDR]([row_hash]);
GO

-- Table: CCFC
-- Source: ccfc.xlsx
-- Columns: 65
CREATE TABLE [landing].[CCFC] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCFC_AUMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMKDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDGH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDGR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDGS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUOBTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTDN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTDN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMAM1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMAF1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMAF2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMAF3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUNODO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUNOGE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUNOSE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDTK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDKW] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUFCHP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUAUUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMXWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMXUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUSTRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUFUCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUFUCF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUAURO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYPR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUAUIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUAUI2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUDLCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AULGCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUWATM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUMDDC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYD1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUTYD2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUC9CD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AURPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUDISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUAUBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFC_AUAUTH] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (65 columns with CCFC_ prefix)
    [CCFC_AUMKCD_Make_Code] INT NULL,
    [CCFC_AUMKDS_Make_Description] VARCHAR(50) NULL,
    [CCFC_AUMDCD_Model_Code] INT NULL,
    [CCFC_AUMDDS_Model_Description] VARCHAR(50) NULL,
    [CCFC_AUMDGH_Model_Group_Higher_Level] VARCHAR(200) NULL,
    [CCFC_AUMDGR_Model_Group_Code] INT NULL,
    [CCFC_AUMDGS_Model_Group_Description] VARCHAR(200) NULL,
    [CCFC_AUTYDS_Type_Description] VARCHAR(50) NULL,
    [CCFC_AUOBTY_Automobile_Category] VARCHAR(200) NULL,
    [CCFC_AUTDN1_Technical_Description_1] VARCHAR(50) NULL,
    [CCFC_AUTDN2_Technical_Description_2] VARCHAR(50) NULL,
    [CCFC_AUMAM1_1st_Maintenance_Mileage] INT NULL,
    [CCFC_AUMAF1_Maintenance_Type_1_Mileage] INT NULL,
    [CCFC_AUMAF2_Maintenance_Type_2_Mileage] INT NULL,
    [CCFC_AUMAF3_Maintenance_Type_3_Mileage] INT NULL,
    [CCFC_AUNODO_Number_Of_Doors] INT NULL,
    [CCFC_AUNOGE_Number_Of_Gears] VARCHAR(50) NULL,
    [CCFC_AUNOSE_Number_Of_Seats] INT NULL,
    [CCFC_AUMDTK_Tank_Capacity] INT NULL,
    [CCFC_AUTYCC_Cylinder_Capacity] INT NULL,
    [CCFC_AUMDKW_Kw] INT NULL,
    [CCFC_AUFCHP_Fiscal_Horse_Power] INT NULL,
    [CCFC_AUAUUN_Unit_Code] VARCHAR(50) NULL,
    [CCFC_AUMXWE_Maximum_Weight] INT NULL,
    [CCFC_AUMXUN_Maximum_Number_Of_Unitsauun] INT NULL,
    [CCFC_AUSTRD_Standard_Radio] VARCHAR(50) NULL,
    [CCFC_AUFUCD_Fuel_Code] INT NULL,
    [CCFC_AUFUCF_Standard_Consumption] DECIMAL(5,3) NULL,
    [CCFC_AUAURO_Road_Tax_Code] VARCHAR(50) NULL,
    [CCFC_AUTYPR_Catalog_Price] DECIMAL(11,4) NULL,
    [CCFC_AUAUIN_Insurance_Code] VARCHAR(50) NULL,
    [CCFC_AUAUI2_Insurance_Code_2] VARCHAR(200) NULL,
    [CCFC_AUDLCH_Delivery_Charges] DECIMAL(4,3) NULL,
    [CCFC_AUTYWE_Weight] INT NULL,
    [CCFC_AULGCH_Supplement_Lpg] INT NULL,
    [CCFC_AUWATM_Warranty_Length] INT NULL,
    [CCFC_AUMDDC_Reduction] DECIMAL(7,4) NULL,
    [CCFC_AUTYN1_Tyres_1_Number] INT NULL,
    [CCFC_AUTYD1_Tyres_1_Description] VARCHAR(50) NULL,
    [CCFC_AUTYN2_Tyres_2_Number] INT NULL,
    [CCFC_AUTYD2_Tyres_2_Description] VARCHAR(50) NULL,
    [CCFC_AUC9CD_Catalconverter_Cat] VARCHAR(50) NULL,
    [CCFC_AURPPD_Reportingperiod] INT NULL,
    [CCFC_AUCOUC_Country_Code] VARCHAR(50) NULL,
    [CCFC_AUVOLC_Volume_Measurement] VARCHAR(50) NULL,
    [CCFC_AUDISC_Distance_Measurement] VARCHAR(50) NULL,
    [CCFC_AUCONC_Consumption_Measurement] VARCHAR(50) NULL,
    [CCFC_AUCURC_Currency_Unit] VARCHAR(50) NULL,
    [CCFC_AUAUBC_Width_Extern] INT NULL,
    [CCFC_AUAUTH_Height_Extern] INT NULL,
    [CCFC_AUAUTL_Length_Extern] INT NULL,
    [CCFC_AUAUHE_Height_Intern] INT NULL,
    [CCFC_AUAUWI_Width_Intern] INT NULL,
    [CCFC_AUAUTG_Max_Train_Weight] INT NULL,
    [CCFC_AUTYCL_Type_Object] INT NULL,
    [CCFC_AUAUAC_Axe_Configuration] VARCHAR(200) NULL,
    [CCFC_AUAUDI_Horse_Power_Din] INT NULL,
    [CCFC_AUAULE_Length_Intern] INT NULL,
    [CCFC_AUMQNO_Residual_Matrix_Number] INT NULL,
    [CCFC_AUMQNR_Maintenace_Matrix_Number] INT NULL,
    [CCFC_AUTYSC_Type_Status_Code] INT NULL,
    [CCFC_AUENAV_Energyaverage] INT NULL,
    [CCFC_AUEUR_Eurostandard] INT NULL,
    [CCFC_AUCAT_Category] VARCHAR(50) NULL,
    [CCFC_AUCO2_Co2emission] INT NULL,

    CONSTRAINT [PK_landing_CCFC] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCFC_timestamp] ON [landing].[CCFC]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCFC_hash] ON [landing].[CCFC]([row_hash]);
GO

-- Table: CCFID
-- Source: ccfid.xlsx
-- Columns: 65
CREATE TABLE [landing].[CCFID] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCFID_AUMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMKDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDGH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDGR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDGS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUOBTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTDN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTDN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMAM1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMAF1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMAF2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMAF3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUNODO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUNOGE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUNOSE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDTK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDKW] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUFCHP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUAUUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMXWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMXUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUSTRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUFUCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUFUCF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUAURO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYPR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUAUIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUAUI2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUDLCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AULGCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUWATM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUMDDC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYD1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUTYD2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUC9CD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AURPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUDISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUAUBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFID_AUAUTH] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (65 columns with CCFID_ prefix)
    [CCFID_AUMKCD] INT NULL,
    [CCFID_AUMKDS] VARCHAR(50) NULL,
    [CCFID_AUMDCD] INT NULL,
    [CCFID_AUMDDS] VARCHAR(50) NULL,
    [CCFID_AUMDGH] VARCHAR(200) NULL,
    [CCFID_AUMDGR] INT NULL,
    [CCFID_AUMDGS] VARCHAR(200) NULL,
    [CCFID_AUTYDS] VARCHAR(50) NULL,
    [CCFID_AUOBTY] VARCHAR(200) NULL,
    [CCFID_AUTDN1] VARCHAR(50) NULL,
    [CCFID_AUTDN2] VARCHAR(50) NULL,
    [CCFID_AUMAM1] INT NULL,
    [CCFID_AUMAF1] INT NULL,
    [CCFID_AUMAF2] INT NULL,
    [CCFID_AUMAF3] INT NULL,
    [CCFID_AUNODO] INT NULL,
    [CCFID_AUNOGE] VARCHAR(50) NULL,
    [CCFID_AUNOSE] INT NULL,
    [CCFID_AUMDTK] INT NULL,
    [CCFID_AUTYCC] INT NULL,
    [CCFID_AUMDKW] INT NULL,
    [CCFID_AUFCHP] INT NULL,
    [CCFID_AUAUUN] VARCHAR(50) NULL,
    [CCFID_AUMXWE] INT NULL,
    [CCFID_AUMXUN] INT NULL,
    [CCFID_AUSTRD] VARCHAR(50) NULL,
    [CCFID_AUFUCD] INT NULL,
    [CCFID_AUFUCF] DECIMAL(5,3) NULL,
    [CCFID_AUAURO] VARCHAR(50) NULL,
    [CCFID_AUTYPR] DECIMAL(11,4) NULL,
    [CCFID_AUAUIN] VARCHAR(50) NULL,
    [CCFID_AUAUI2] VARCHAR(200) NULL,
    [CCFID_AUDLCH] DECIMAL(4,3) NULL,
    [CCFID_AUTYWE] INT NULL,
    [CCFID_AULGCH] INT NULL,
    [CCFID_AUWATM] INT NULL,
    [CCFID_AUMDDC] DECIMAL(7,4) NULL,
    [CCFID_AUTYN1] INT NULL,
    [CCFID_AUTYD1] VARCHAR(50) NULL,
    [CCFID_AUTYN2] INT NULL,
    [CCFID_AUTYD2] VARCHAR(50) NULL,
    [CCFID_AUC9CD] VARCHAR(50) NULL,
    [CCFID_AURPPD] INT NULL,
    [CCFID_AUCOUC] VARCHAR(50) NULL,
    [CCFID_AUVOLC] VARCHAR(50) NULL,
    [CCFID_AUDISC] VARCHAR(50) NULL,
    [CCFID_AUCONC] VARCHAR(50) NULL,
    [CCFID_AUCURC] VARCHAR(50) NULL,
    [CCFID_AUAUBC] INT NULL,
    [CCFID_AUAUTH] INT NULL,
    [CCFID_AUAUTL] INT NULL,
    [CCFID_AUAUHE] INT NULL,
    [CCFID_AUAUWI] INT NULL,
    [CCFID_AUAUTG] INT NULL,
    [CCFID_AUTYCL] INT NULL,
    [CCFID_AUAUAC] VARCHAR(200) NULL,
    [CCFID_AUAUDI] INT NULL,
    [CCFID_AUAULE] INT NULL,
    [CCFID_AUMQNO] INT NULL,
    [CCFID_AUMQNR] INT NULL,
    [CCFID_AUTYSC] INT NULL,
    [CCFID_AUENAV] INT NULL,
    [CCFID_AUEUR] INT NULL,
    [CCFID_AUCAT] VARCHAR(50) NULL,
    [CCFID_AUCO2] INT NULL,

    CONSTRAINT [PK_landing_CCFID] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCFID_timestamp] ON [landing].[CCFID]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCFID_hash] ON [landing].[CCFID]([row_hash]);
GO

-- Table: CCFIH
-- Source: ccfih.xlsx
-- Columns: 65
CREATE TABLE [landing].[CCFIH] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCFIH_AUMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMKDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDGH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDGR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDGS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUOBTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTDN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTDN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMAM1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMAF1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMAF2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMAF3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUNODO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUNOGE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUNOSE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDTK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDKW] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUFCHP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUAUUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMXWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMXUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUSTRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUFUCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUFUCF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUAURO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYPR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUAUIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUAUI2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUDLCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AULGCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUWATM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUMDDC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYD1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUTYD2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUC9CD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AURPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUDISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUAUBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIH_AUAUTH] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (65 columns with CCFIH_ prefix)
    [CCFIH_AUMKCD] INT NULL,
    [CCFIH_AUMKDS] VARCHAR(50) NULL,
    [CCFIH_AUMDCD] INT NULL,
    [CCFIH_AUMDDS] VARCHAR(50) NULL,
    [CCFIH_AUMDGH] VARCHAR(200) NULL,
    [CCFIH_AUMDGR] INT NULL,
    [CCFIH_AUMDGS] VARCHAR(200) NULL,
    [CCFIH_AUTYDS] VARCHAR(50) NULL,
    [CCFIH_AUOBTY] VARCHAR(200) NULL,
    [CCFIH_AUTDN1] VARCHAR(50) NULL,
    [CCFIH_AUTDN2] VARCHAR(50) NULL,
    [CCFIH_AUMAM1] INT NULL,
    [CCFIH_AUMAF1] INT NULL,
    [CCFIH_AUMAF2] INT NULL,
    [CCFIH_AUMAF3] INT NULL,
    [CCFIH_AUNODO] INT NULL,
    [CCFIH_AUNOGE] VARCHAR(50) NULL,
    [CCFIH_AUNOSE] INT NULL,
    [CCFIH_AUMDTK] INT NULL,
    [CCFIH_AUTYCC] INT NULL,
    [CCFIH_AUMDKW] INT NULL,
    [CCFIH_AUFCHP] INT NULL,
    [CCFIH_AUAUUN] VARCHAR(50) NULL,
    [CCFIH_AUMXWE] INT NULL,
    [CCFIH_AUMXUN] INT NULL,
    [CCFIH_AUSTRD] VARCHAR(50) NULL,
    [CCFIH_AUFUCD] INT NULL,
    [CCFIH_AUFUCF] DECIMAL(5,3) NULL,
    [CCFIH_AUAURO] VARCHAR(50) NULL,
    [CCFIH_AUTYPR] DECIMAL(11,4) NULL,
    [CCFIH_AUAUIN] VARCHAR(50) NULL,
    [CCFIH_AUAUI2] VARCHAR(200) NULL,
    [CCFIH_AUDLCH] DECIMAL(4,3) NULL,
    [CCFIH_AUTYWE] INT NULL,
    [CCFIH_AULGCH] INT NULL,
    [CCFIH_AUWATM] INT NULL,
    [CCFIH_AUMDDC] DECIMAL(7,4) NULL,
    [CCFIH_AUTYN1] INT NULL,
    [CCFIH_AUTYD1] VARCHAR(50) NULL,
    [CCFIH_AUTYN2] INT NULL,
    [CCFIH_AUTYD2] VARCHAR(50) NULL,
    [CCFIH_AUC9CD] VARCHAR(50) NULL,
    [CCFIH_AURPPD] INT NULL,
    [CCFIH_AUCOUC] VARCHAR(50) NULL,
    [CCFIH_AUVOLC] VARCHAR(50) NULL,
    [CCFIH_AUDISC] VARCHAR(50) NULL,
    [CCFIH_AUCONC] VARCHAR(50) NULL,
    [CCFIH_AUCURC] VARCHAR(50) NULL,
    [CCFIH_AUAUBC] INT NULL,
    [CCFIH_AUAUTH] INT NULL,
    [CCFIH_AUAUTL] INT NULL,
    [CCFIH_AUAUHE] INT NULL,
    [CCFIH_AUAUWI] INT NULL,
    [CCFIH_AUAUTG] INT NULL,
    [CCFIH_AUTYCL] INT NULL,
    [CCFIH_AUAUAC] VARCHAR(200) NULL,
    [CCFIH_AUAUDI] INT NULL,
    [CCFIH_AUAULE] INT NULL,
    [CCFIH_AUMQNO] INT NULL,
    [CCFIH_AUMQNR] INT NULL,
    [CCFIH_AUTYSC] INT NULL,
    [CCFIH_AUENAV] INT NULL,
    [CCFIH_AUEUR] INT NULL,
    [CCFIH_AUCAT] VARCHAR(50) NULL,
    [CCFIH_AUCO2] INT NULL,

    CONSTRAINT [PK_landing_CCFIH] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCFIH_timestamp] ON [landing].[CCFIH]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCFIH_hash] ON [landing].[CCFIH]([row_hash]);
GO

-- Table: CCFIM
-- Source: ccfim.xlsx
-- Columns: 65
CREATE TABLE [landing].[CCFIM] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCFIM_AUMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMKDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDGH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDGR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDGS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUOBTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTDN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTDN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMAM1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMAF1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMAF2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMAF3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUNODO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUNOGE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUNOSE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDTK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDKW] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUFCHP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUAUUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMXWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMXUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUSTRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUFUCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUFUCF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUAURO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYPR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUAUIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUAUI2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUDLCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AULGCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUWATM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUMDDC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYD1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUTYD2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUC9CD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AURPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUDISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUAUBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFIM_AUAUTH] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (65 columns with CCFIM_ prefix)
    [CCFIM_AUMKCD] INT NULL,
    [CCFIM_AUMKDS] VARCHAR(50) NULL,
    [CCFIM_AUMDCD] INT NULL,
    [CCFIM_AUMDDS] VARCHAR(50) NULL,
    [CCFIM_AUMDGH] VARCHAR(200) NULL,
    [CCFIM_AUMDGR] INT NULL,
    [CCFIM_AUMDGS] VARCHAR(200) NULL,
    [CCFIM_AUTYDS] VARCHAR(50) NULL,
    [CCFIM_AUOBTY] VARCHAR(200) NULL,
    [CCFIM_AUTDN1] VARCHAR(50) NULL,
    [CCFIM_AUTDN2] VARCHAR(50) NULL,
    [CCFIM_AUMAM1] INT NULL,
    [CCFIM_AUMAF1] INT NULL,
    [CCFIM_AUMAF2] INT NULL,
    [CCFIM_AUMAF3] INT NULL,
    [CCFIM_AUNODO] INT NULL,
    [CCFIM_AUNOGE] VARCHAR(50) NULL,
    [CCFIM_AUNOSE] INT NULL,
    [CCFIM_AUMDTK] INT NULL,
    [CCFIM_AUTYCC] INT NULL,
    [CCFIM_AUMDKW] INT NULL,
    [CCFIM_AUFCHP] INT NULL,
    [CCFIM_AUAUUN] VARCHAR(50) NULL,
    [CCFIM_AUMXWE] INT NULL,
    [CCFIM_AUMXUN] INT NULL,
    [CCFIM_AUSTRD] VARCHAR(50) NULL,
    [CCFIM_AUFUCD] INT NULL,
    [CCFIM_AUFUCF] DECIMAL(5,3) NULL,
    [CCFIM_AUAURO] VARCHAR(50) NULL,
    [CCFIM_AUTYPR] DECIMAL(11,4) NULL,
    [CCFIM_AUAUIN] VARCHAR(50) NULL,
    [CCFIM_AUAUI2] VARCHAR(200) NULL,
    [CCFIM_AUDLCH] DECIMAL(4,3) NULL,
    [CCFIM_AUTYWE] INT NULL,
    [CCFIM_AULGCH] INT NULL,
    [CCFIM_AUWATM] INT NULL,
    [CCFIM_AUMDDC] DECIMAL(7,4) NULL,
    [CCFIM_AUTYN1] INT NULL,
    [CCFIM_AUTYD1] VARCHAR(50) NULL,
    [CCFIM_AUTYN2] INT NULL,
    [CCFIM_AUTYD2] VARCHAR(50) NULL,
    [CCFIM_AUC9CD] VARCHAR(50) NULL,
    [CCFIM_AURPPD] INT NULL,
    [CCFIM_AUCOUC] VARCHAR(50) NULL,
    [CCFIM_AUVOLC] VARCHAR(50) NULL,
    [CCFIM_AUDISC] VARCHAR(50) NULL,
    [CCFIM_AUCONC] VARCHAR(50) NULL,
    [CCFIM_AUCURC] VARCHAR(50) NULL,
    [CCFIM_AUAUBC] INT NULL,
    [CCFIM_AUAUTH] INT NULL,
    [CCFIM_AUAUTL] INT NULL,
    [CCFIM_AUAUHE] INT NULL,
    [CCFIM_AUAUWI] INT NULL,
    [CCFIM_AUAUTG] INT NULL,
    [CCFIM_AUTYCL] INT NULL,
    [CCFIM_AUAUAC] VARCHAR(200) NULL,
    [CCFIM_AUAUDI] INT NULL,
    [CCFIM_AUAULE] INT NULL,
    [CCFIM_AUMQNO] INT NULL,
    [CCFIM_AUMQNR] INT NULL,
    [CCFIM_AUTYSC] INT NULL,
    [CCFIM_AUENAV] INT NULL,
    [CCFIM_AUEUR] INT NULL,
    [CCFIM_AUCAT] VARCHAR(50) NULL,
    [CCFIM_AUCO2] INT NULL,

    CONSTRAINT [PK_landing_CCFIM] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCFIM_timestamp] ON [landing].[CCFIM]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCFIM_hash] ON [landing].[CCFIM]([row_hash]);
GO

-- Table: CCFP
-- Source: ccfp.xlsx
-- Columns: 27
CREATE TABLE [landing].[CCFP] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCFP_FPOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPNOLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPCPNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPCONO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPSTAT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPTYPE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBBA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBBY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBBM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBBD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBEA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBEC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBEY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBEM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBED] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBDU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPMTCT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPYTMI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPOBKM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPKMDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPKMMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPKMYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPKMCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPDRNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCFP_FPDRNM] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (27 columns with CCFP_ prefix)
    [CCFP_FPOBNO_Object_Number] INT NULL,
    [CCFP_FPCUNO_Customer_Number] INT NULL,
    [CCFP_FPNOLO_Name] VARCHAR(50) NULL,
    [CCFP_FPCPNO_Calculation_Number] INT NULL,
    [CCFP_FPCONO_Contract_Number] INT NULL,
    [CCFP_FPSTAT_Status_Code] INT NULL,
    [CCFP_FPTYPE_Registration_Type] VARCHAR(50) NULL,
    [CCFP_FPOBBA] INT NULL,
    [CCFP_FPOBBC_Object_Begin_Date_Century] INT NULL,
    [CCFP_FPOBBY_Object_Begin_Date_Year] INT NULL,
    [CCFP_FPOBBM_Object_Begin_Date_Month] INT NULL,
    [CCFP_FPOBBD_Object_Begin_Date_Day] INT NULL,
    [CCFP_FPOBEA] INT NULL,
    [CCFP_FPOBEC_Exterior_Color] INT NULL,
    [CCFP_FPOBEY] INT NULL,
    [CCFP_FPOBEM] INT NULL,
    [CCFP_FPOBED] INT NULL,
    [CCFP_FPOBDU] INT NULL,
    [CCFP_FPMTCT_Total_Cost_Per_Month] DECIMAL(9,4) NULL,
    [CCFP_FPYTMI] INT NULL,
    [CCFP_FPOBKM] INT NULL,
    [CCFP_FPKMDD_Mileage_Date_Day] INT NULL,
    [CCFP_FPKMMM_Mileage_Date_Month] INT NULL,
    [CCFP_FPKMYY_Mileage_Date_Year] INT NULL,
    [CCFP_FPKMCC_Mileage_Date_Century] INT NULL,
    [CCFP_FPDRNO_Driver_Number] INT NULL,
    [CCFP_FPDRNM_Driver_Name] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCFP] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCFP_timestamp] ON [landing].[CCFP]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCFP_hash] ON [landing].[CCFP]([row_hash]);
GO

-- Table: CCGD
-- Source: ccgd.xlsx
-- Columns: 15
CREATE TABLE [landing].[CCGD] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCGD_MAOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDSQ] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDKM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDSC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MANUFO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MANAFO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDTNR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDTMK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGD_MAGDTSP] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (15 columns with CCGD_ prefix)
    [CCGD_MAOBNO_Object_Number] INT NULL,
    [CCGD_MAGDSQ_Maintenance_Stop_Sequence_Num_A_Ber] INT NULL,
    [CCGD_MAGDCC] INT NULL,
    [CCGD_MAGDYY_Maintenance_Stop_Date_Year] INT NULL,
    [CCGD_MAGDMM_Maintenance_Stop_Date_Month] INT NULL,
    [CCGD_MAGDDD_Maintenance_Stop_Date_Day] INT NULL,
    [CCGD_MAGDKM_Maintenance_Stop_Mileage] INT NULL,
    [CCGD_MAGDAM_Mileageamount] DECIMAL(10,4) NULL,
    [CCGD_MAGDDS_Repair_Description_Part_1] VARCHAR(1000) NULL,
    [CCGD_MAGDSC_Maintenance_Stop_Status_Code] INT NULL,
    [CCGD_MANUFO_Supplier_Number] INT NULL,
    [CCGD_MANAFO_Rotation_Number_Supplier] VARCHAR(50) NULL,
    [CCGD_MAGDTNR] INT NULL,
    [CCGD_MAGDTMK] VARCHAR(50) NULL,
    [CCGD_MAGDTSP] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCGD] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCGD_timestamp] ON [landing].[CCGD]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCGD_hash] ON [landing].[CCGD]([row_hash]);
GO

-- Table: CCGR
-- Source: ccgr.xlsx
-- Columns: 26
CREATE TABLE [landing].[CCGR] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCGR_GRGNLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNOLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNOL2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNOL3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCLLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCPLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRADLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRLOLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCALO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNTLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNFLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNELO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCTLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRRPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCLPE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRRSFL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRRSFN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRRSLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRRPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRRPAS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNEL2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNEL3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCCLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRNKLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCGR_GRCVLO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (26 columns with CCGR_ prefix)
    [CCGR_GRGNLO_Client_Group_Number] INT NULL,
    [CCGR_GRNOLO_Name] VARCHAR(50) NULL,
    [CCGR_GRNOL2_Name_Cont] VARCHAR(50) NULL,
    [CCGR_GRNOL3_Name_Cont] VARCHAR(50) NULL,
    [CCGR_GRCLLO_Language_Code] VARCHAR(50) NULL,
    [CCGR_GRCPLO_Country_Code] VARCHAR(50) NULL,
    [CCGR_GRADLO_Address] VARCHAR(50) NULL,
    [CCGR_GRLOLO_Locality] VARCHAR(50) NULL,
    [CCGR_GRCALO_Area_Code] VARCHAR(50) NULL,
    [CCGR_GRNTLO_Telephone_Number] VARCHAR(50) NULL,
    [CCGR_GRNFLO_Fax_Number] VARCHAR(50) NULL,
    [CCGR_GRNELO_Email_Address] VARCHAR(200) NULL,
    [CCGR_GRCTLO_Title_Code] VARCHAR(200) NULL,
    [CCGR_GRRPPD_Reportingperiod] INT NULL,
    [CCGR_GRCOUC_Country_Code] VARCHAR(50) NULL,
    [CCGR_GRCLPE_Office_Code] VARCHAR(200) NULL,
    [CCGR_GRRSFL_Financial_Leasing_Responsible_A] VARCHAR(200) NULL,
    [CCGR_GRRSFN_Fleet_Responsible] VARCHAR(200) NULL,
    [CCGR_GRRSLO_Operational_Leasing_Responsib_A_Le] VARCHAR(50) NULL,
    [CCGR_GRRPCD_Representative_Code] VARCHAR(200) NULL,
    [CCGR_GRRPAS_After_Sales_Represent] VARCHAR(200) NULL,
    [CCGR_GRNEL2] VARCHAR(200) NULL,
    [CCGR_GRNEL3] VARCHAR(200) NULL,
    [CCGR_GRCCLO_Contentment_Client_Code] INT NULL,
    [CCGR_GRNKLO_Sub_Branch_Code] INT NULL,
    [CCGR_GRCVLO_Code_Branch_Client] INT NULL,

    CONSTRAINT [PK_landing_CCGR] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCGR_timestamp] ON [landing].[CCGR]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCGR_hash] ON [landing].[CCGR]([row_hash]);
GO

-- Table: CCIN
-- Source: ccin.xlsx
-- Columns: 34
CREATE TABLE [landing].[CCIN] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCIN_ININAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININBY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININBM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININBD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININCL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININCO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININDE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININDU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININEC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININEY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININEM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININED] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININIC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININIY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININIM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININID] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININCN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININPE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININSC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INPONO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INPOSU] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INPVCL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INRTLE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INRTOT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INRPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_ININFI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INPON1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INPTFT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIN_INPTOW] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (34 columns with CCIN_ prefix)
    [CCIN_ININAM_Insured_Amount] INT NULL,
    [CCIN_ININBC_Insurance_Start_Date_Century] INT NULL,
    [CCIN_ININBY_Insurance_Start_Date_Year] INT NULL,
    [CCIN_ININBM_Insurance_Start_Date_Month] INT NULL,
    [CCIN_ININBD_Insurance_Start_Date_Day] INT NULL,
    [CCIN_ININCL_Insurance_Class_Bonusmal_A_Us] INT NULL,
    [CCIN_ININCO_Insurance_Contractor] VARCHAR(50) NULL,
    [CCIN_ININDE_Deductible_Amount] INT NULL,
    [CCIN_ININDU_Insurance_Duration_In_Months] INT NULL,
    [CCIN_ININEC_Insurance_End_Date_Century] INT NULL,
    [CCIN_ININEY_Insurance_End_Date_Year] INT NULL,
    [CCIN_ININEM_Insurance_End_Date_Month] INT NULL,
    [CCIN_ININED_Insurance_End_Date_Day] INT NULL,
    [CCIN_ININIC_Insurance_Reporting_Date_Cent_A_Ury] INT NULL,
    [CCIN_ININIY_Insurance_Reporting_Date_Year_A] INT NULL,
    [CCIN_ININIM_Insurance_Reporting_Date_Mont_A_H] INT NULL,
    [CCIN_ININID_Insurance_Reporting_Date_Day] INT NULL,
    [CCIN_ININCN_Insurance_Co_Number] INT NULL,
    [CCIN_ININPE_Insurance_Person] VARCHAR(50) NULL,
    [CCIN_ININSC_Insurance_Status_Code] INT NULL,
    [CCIN_ININTY_Insurance_Type] INT NULL,
    [CCIN_INOBNO_Object_Number] INT NULL,
    [CCIN_INPONO_Policy_Number] VARCHAR(200) NULL,
    [CCIN_INPOSU_Policy_Supplement_Number] INT NULL,
    [CCIN_INPVCL_Previous_Insurance_Class] INT NULL,
    [CCIN_INRTLE_Insurance_Premium_Aid] INT NULL,
    [CCIN_INRTOT_Insurance_Premium_Aid_Number] INT NULL,
    [CCIN_INCURC_Currency_Unit] VARCHAR(50) NULL,
    [CCIN_INCOUC_Country_Code] VARCHAR(50) NULL,
    [CCIN_INRPPD_Reportingperiod] INT NULL,
    [CCIN_ININFI_Insurance_Own_Risk_Minimum_Am_A_Ount] INT NULL,
    [CCIN_INPON1] VARCHAR(200) NULL,
    [CCIN_INPTFT_Insurance_Percentage_Fire_T_A_Heft] INT NULL,
    [CCIN_INPTOW_Insurance_Percentage_Own_Risk_A] INT NULL,

    CONSTRAINT [PK_landing_CCIN] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCIN_timestamp] ON [landing].[CCIN]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCIN_hash] ON [landing].[CCIN]([row_hash]);
GO

-- Table: CCIO
-- Source: ccio.xlsx
-- Columns: 65
CREATE TABLE [landing].[CCIO] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCIO_AUMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMKDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDGH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDGR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDGS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUOBTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTDN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTDN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMAM1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMAF1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMAF2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMAF3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUNODO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUNOGE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUNOSE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDTK] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDKW] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUFCHP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUAUUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMXWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMXUN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUSTRD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUFUCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUFUCF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUAURO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYPR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUAUIN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUAUI2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUDLCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYWE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AULGCH] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUWATM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUMDDC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYD1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUTYD2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUC9CD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AURPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUDISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUCURC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUAUBC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCIO_AUAUTH] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (65 columns with CCIO_ prefix)
    [CCIO_AUMKCD_Make_Code] INT NULL,
    [CCIO_AUMKDS_Make_Description] VARCHAR(50) NULL,
    [CCIO_AUMDCD_Model_Code] INT NULL,
    [CCIO_AUMDDS_Model_Description] VARCHAR(50) NULL,
    [CCIO_AUMDGH_Model_Group_Higher_Level] VARCHAR(200) NULL,
    [CCIO_AUMDGR_Model_Group_Code] INT NULL,
    [CCIO_AUMDGS_Model_Group_Description] VARCHAR(200) NULL,
    [CCIO_AUTYDS_Type_Description] VARCHAR(50) NULL,
    [CCIO_AUOBTY_Automobile_Category] VARCHAR(200) NULL,
    [CCIO_AUTDN1_Technical_Description_1] VARCHAR(50) NULL,
    [CCIO_AUTDN2_Technical_Description_2] VARCHAR(50) NULL,
    [CCIO_AUMAM1_1st_Maintenance_Mileage] INT NULL,
    [CCIO_AUMAF1_Maintenance_Type_1_Mileage] INT NULL,
    [CCIO_AUMAF2_Maintenance_Type_2_Mileage] INT NULL,
    [CCIO_AUMAF3_Maintenance_Type_3_Mileage] INT NULL,
    [CCIO_AUNODO_Number_Of_Doors] INT NULL,
    [CCIO_AUNOGE_Number_Of_Gears] VARCHAR(50) NULL,
    [CCIO_AUNOSE_Number_Of_Seats] INT NULL,
    [CCIO_AUMDTK_Tank_Capacity] INT NULL,
    [CCIO_AUTYCC_Cylinder_Capacity] INT NULL,
    [CCIO_AUMDKW_Kw] INT NULL,
    [CCIO_AUFCHP_Fiscal_Horse_Power] INT NULL,
    [CCIO_AUAUUN_Unit_Code] VARCHAR(50) NULL,
    [CCIO_AUMXWE_Maximum_Weight] INT NULL,
    [CCIO_AUMXUN_Maximum_Number_Of_Unitsauun] INT NULL,
    [CCIO_AUSTRD_Standard_Radio] VARCHAR(50) NULL,
    [CCIO_AUFUCD_Fuel_Code] INT NULL,
    [CCIO_AUFUCF_Standard_Consumption] DECIMAL(5,3) NULL,
    [CCIO_AUAURO_Road_Tax_Code] VARCHAR(50) NULL,
    [CCIO_AUTYPR_Catalog_Price] DECIMAL(11,4) NULL,
    [CCIO_AUAUIN_Insurance_Code] VARCHAR(50) NULL,
    [CCIO_AUAUI2_Insurance_Code_2] VARCHAR(200) NULL,
    [CCIO_AUDLCH_Delivery_Charges] DECIMAL(4,3) NULL,
    [CCIO_AUTYWE_Weight] INT NULL,
    [CCIO_AULGCH_Supplement_Lpg] INT NULL,
    [CCIO_AUWATM_Warranty_Length] INT NULL,
    [CCIO_AUMDDC_Reduction] DECIMAL(7,4) NULL,
    [CCIO_AUTYN1_Tyres_1_Number] INT NULL,
    [CCIO_AUTYD1_Tyres_1_Description] VARCHAR(50) NULL,
    [CCIO_AUTYN2_Tyres_2_Number] INT NULL,
    [CCIO_AUTYD2_Tyres_2_Description] VARCHAR(50) NULL,
    [CCIO_AUC9CD_Catalconverter_Cat] VARCHAR(50) NULL,
    [CCIO_AURPPD_Reportingperiod] INT NULL,
    [CCIO_AUCOUC_Country_Code] VARCHAR(50) NULL,
    [CCIO_AUVOLC_Volume_Measurement] VARCHAR(50) NULL,
    [CCIO_AUDISC_Distance_Measurement] VARCHAR(50) NULL,
    [CCIO_AUCONC_Consumption_Measurement] VARCHAR(50) NULL,
    [CCIO_AUCURC_Currency_Unit] VARCHAR(50) NULL,
    [CCIO_AUAUBC_Width_Extern] INT NULL,
    [CCIO_AUAUTH_Height_Extern] INT NULL,
    [CCIO_AUAUTL_Length_Extern] INT NULL,
    [CCIO_AUAUHE_Height_Intern] INT NULL,
    [CCIO_AUAUWI_Width_Intern] INT NULL,
    [CCIO_AUAUTG_Max_Train_Weight] INT NULL,
    [CCIO_AUTYCL_Type_Object] INT NULL,
    [CCIO_AUAUAC_Axe_Configuration] VARCHAR(200) NULL,
    [CCIO_AUAUDI_Horse_Power_Din] INT NULL,
    [CCIO_AUAULE_Length_Intern] INT NULL,
    [CCIO_AUMQNO_Residual_Matrix_Number] INT NULL,
    [CCIO_AUMQNR_Maintenace_Matrix_Number] INT NULL,
    [CCIO_AUTYSC_Type_Status_Code] INT NULL,
    [CCIO_AUENAV_Energyaverage] INT NULL,
    [CCIO_AUEUR_Eurostandard] INT NULL,
    [CCIO_AUCAT_Category] VARCHAR(50) NULL,
    [CCIO_AUCO2_Co2emission] INT NULL,

    CONSTRAINT [PK_landing_CCIO] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCIO_timestamp] ON [landing].[CCIO]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCIO_hash] ON [landing].[CCIO]([row_hash]);
GO

-- Table: CCOB
-- Source: ccob.xlsx
-- Columns: 95
CREATE TABLE [landing].[CCOB] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCOB_OBOBNO_Object_Number] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBCX_Chassis_Number] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBMKCD_Make_Code] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBMDCD_Model_Code] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBCUNO_Customer_Number] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBPCNO_Profit_Centre_Number] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBCONO_Contract_Number] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBCPNO_Calculation_Number] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBIV_Driver_Participation_Pct] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBUSCD_Object_Usage] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBCTIC_Tech_Control_Date_Century] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBCTIY_Tech_Control_Date_Year] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBCTIM_Tech_Control_Date_Month] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBCTID_Tech_Control_Date_Day] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBUC_Upholstery_Color] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBUP_Upholstery_Code] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBEC_Exterior_Color] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBBC_Object_Begin_Date_Century] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBBY_Object_Begin_Date_Year] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBBM_Object_Begin_Date_Month] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBBD_Object_Begin_Date_Day] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBSK_Begin_Mileage] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBFUSK_Begin_Mileage_Fuel_Incl] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOABC_Recalc_Start_Date_Century] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOABY_Recalc_Start_Date_Year] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOABM_Recalc_Start_Date_Month] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOABD_Recalc_Start_Date_Day] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBMKTY_HSN_TSN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBSC_Object_Status] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBSCCM_Status_Comment] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGNO_Registration_Number] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGBC_Reg_Begin_Date_Century] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGBY_Reg_Begin_Date_Year] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGBM_Reg_Begin_Date_Month] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGBD_Reg_Begin_Date_Day] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGEC_Reg_End_Date_Century] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGEY_Reg_End_Date_Year] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGEM_Reg_End_Date_Month] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGED_Reg_End_Date_Day] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRGPV_Previous_Registration] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBOBOR_Client_Reference] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBNDTA_Non_Deductable_Taxes] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBPRIC_Monthly_Rental] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBRCOB_Replacement_Car_Category] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBFSEC_Object_End_Date_Century] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBFSEY_Object_End_Date_Year] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBFSEM_Object_End_Date_Month] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBFSED_Object_End_Date_Day] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBFSDU_Budgeted_Months] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOB_OBFSAM_Investment_Amount] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (95 columns with CCOB_ prefix + description)
    -- Naming convention: CCOB_{source_code}_{Description} for self-documenting schema
    [CCOB_OBOBNO_Object_Number] INT NULL,
    [CCOB_OBOBCX_Chassis_Number] VARCHAR(50) NULL,
    [CCOB_OBMKCD_Make_Code] INT NULL,
    [CCOB_OBMDCD_Model_Code] INT NULL,
    [CCOB_OBCUNO_Customer_Number] INT NULL,
    [CCOB_OBPCNO_Profit_Centre_Number] INT NULL,
    [CCOB_OBCONO_Contract_Number] INT NULL,
    [CCOB_OBCPNO_Calculation_Number] INT NULL,
    [CCOB_OBOBIV_Driver_Participation_Pct] INT NULL,
    [CCOB_OBUSCD_Object_Usage] INT NULL,
    [CCOB_OBCTIC_Tech_Control_Date_Century] INT NULL,
    [CCOB_OBCTIY_Tech_Control_Date_Year] INT NULL,
    [CCOB_OBCTIM_Tech_Control_Date_Month] INT NULL,
    [CCOB_OBCTID_Tech_Control_Date_Day] INT NULL,
    [CCOB_OBOBUC_Upholstery_Color] VARCHAR(50) NULL,
    [CCOB_OBOBUP_Upholstery_Code] INT NULL,
    [CCOB_OBOBEC_Exterior_Color] VARCHAR(50) NULL,
    [CCOB_OBOBBC_Object_Begin_Date_Century] INT NULL,
    [CCOB_OBOBBY_Object_Begin_Date_Year] INT NULL,
    [CCOB_OBOBBM_Object_Begin_Date_Month] INT NULL,
    [CCOB_OBOBBD_Object_Begin_Date_Day] INT NULL,
    [CCOB_OBOBSK_Begin_Mileage] INT NULL,
    [CCOB_OBFUSK_Begin_Mileage_Fuel_Incl] INT NULL,
    [CCOB_OBOABC_Recalc_Start_Date_Century] INT NULL,
    [CCOB_OBOABY_Recalc_Start_Date_Year] INT NULL,
    [CCOB_OBOABM_Recalc_Start_Date_Month] INT NULL,
    [CCOB_OBOABD_Recalc_Start_Date_Day] INT NULL,
    [CCOB_OBMKTY_HSN_TSN] VARCHAR(200) NULL,
    [CCOB_OBOBSC_Object_Status] INT NULL,
    [CCOB_OBSCCM_Status_Comment] VARCHAR(200) NULL,
    [CCOB_OBRGNO_Registration_Number] VARCHAR(50) NULL,
    [CCOB_OBRGBC_Reg_Begin_Date_Century] INT NULL,
    [CCOB_OBRGBY_Reg_Begin_Date_Year] INT NULL,
    [CCOB_OBRGBM_Reg_Begin_Date_Month] INT NULL,
    [CCOB_OBRGBD_Reg_Begin_Date_Day] INT NULL,
    [CCOB_OBRGEC_Reg_End_Date_Century] INT NULL,
    [CCOB_OBRGEY_Reg_End_Date_Year] INT NULL,
    [CCOB_OBRGEM_Reg_End_Date_Month] INT NULL,
    [CCOB_OBRGED_Reg_End_Date_Day] INT NULL,
    [CCOB_OBRGPV_Previous_Registration] VARCHAR(50) NULL,
    [CCOB_OBOBOR_Client_Reference] VARCHAR(200) NULL,
    [CCOB_OBNDTA_Non_Deductable_Taxes] INT NULL,
    [CCOB_OBPRIC_Monthly_Rental] DECIMAL(9,4) NULL,
    [CCOB_OBRCOB_Replacement_Car_Category] VARCHAR(50) NULL,
    [CCOB_OBFSEC_Object_End_Date_Century] INT NULL,
    [CCOB_OBFSEY_Object_End_Date_Year] INT NULL,
    [CCOB_OBFSEM_Object_End_Date_Month] INT NULL,
    [CCOB_OBFSED_Object_End_Date_Day] INT NULL,
    [CCOB_OBFSDU_Budgeted_Months] INT NULL,
    [CCOB_OBFSAM_Investment_Amount] DECIMAL(11,4) NULL,
    [CCOB_OBFSRV_Budgeted_Residual_Value] DECIMAL(10,4) NULL,
    [CCOB_OBKMDU_Total_Budgeted_Mileage] INT NULL,
    [CCOB_OBKMBU_Budgeted_Mileage_Per_Year] INT NULL,
    [CCOB_OBOBMC_Manufacturing_Date_Century] INT NULL,
    [CCOB_OBOBMY_Manufacturing_Date_Year] INT NULL,
    [CCOB_OBOBMM_Manufacturing_Date_Month] INT NULL,
    [CCOB_OBOBMD_Manufacturing_Date_Day] INT NULL,
    [CCOB_OBTLCD_Contract_Type] VARCHAR(50) NULL,
    [CCOB_OBLPCD_Company_Code] VARCHAR(50) NULL,
    [CCOB_OBGRNO_Client_Group_Number] INT NULL,
    [CCOB_OBORNO_Order_Number] INT NULL,
    [CCOB_OBORCD_Object_Takeover_Code] VARCHAR(50) NULL,
    [CCOB_OBDTIC_Object_Data_Incomplete] VARCHAR(200) NULL,
    [CCOB_OBSLPR_Final_Sales_Price] DECIMAL(11,4) NULL,
    [CCOB_OBFLIN_Floating_Interest] INT NULL,
    [CCOB_OBRPPD_Reporting_Period] INT NULL,
    [CCOB_OBCOUC_Country_Code] VARCHAR(50) NULL,
    [CCOB_OBVOLC_Volume_Measurement] VARCHAR(50) NULL,
    [CCOB_OBDISC_Distance_Measurement] VARCHAR(50) NULL,
    [CCOB_OBCONC_Consumption_Measurement] VARCHAR(50) NULL,
    [CCOB_OBCURC_Currency_Unit] VARCHAR(50) NULL,
    [CCOB_OBINCL_Insurance_Class] INT NULL,
    [CCOB_OBBYCD_Buyback_Code] VARCHAR(200) NULL,
    [CCOB_OBBJRW_Service_Freq_Period] INT NULL,
    [CCOB_OBBIRW_Service_Freq_Mileage] INT NULL,
    [CCOB_OBMEPE_Timeout_Period_Days] INT NULL,
    [CCOB_OBSLCD_Sales_Reason_Code] VARCHAR(200) NULL,
    [CCOB_OBOREF_Object_Reference_Number] INT NULL,
    [CCOB_OBDRPC_Driver_Profit_Center] VARCHAR(200) NULL,
    [CCOB_OBFUCD_Fuel_Code] INT NULL,
    [CCOB_OBOBAC_Admission_Date_Century] INT NULL,
    [CCOB_OBOBAY_Admission_Date_Year] INT NULL,
    [CCOB_OBOBAM_Admission_Date_Month] INT NULL,
    [CCOB_OBOBAD_Admission_Date_Day] INT NULL,
    [CCOB_OBOBSN_Supplier_Number] INT NULL,
    [CCOB_OBOBSQ_Supplier_Rotation_Number] VARCHAR(200) NULL,
    [CCOB_OBOBPU_Object_Purchase_Price] INT NULL,
    [CCOB_OBRCDA_Replacement_Days] INT NULL,
    [CCOB_OBFSKM_Last_Mileage_Known] INT NULL,
    [CCOB_OBKMDD_Mileage_Date_Day] INT NULL,
    [CCOB_OBKMMM_Mileage_Date_Month] INT NULL,
    [CCOB_OBKMCC_Mileage_Date_Century] INT NULL,
    [CCOB_OBKMYY_Mileage_Date_Year] INT NULL,
    [CCOB_OBLPPV_Previous_Company_Code] VARCHAR(200) NULL,
    [CCOB_OBOBMN_Motor_Number] VARCHAR(200) NULL,

    CONSTRAINT [PK_landing_CCOB] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCOB_timestamp] ON [landing].[CCOB]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCOB_hash] ON [landing].[CCOB]([row_hash]);
GO

-- Table: CCOBCP
-- Source: ccobcp.xlsx
-- Columns: 5
CREATE TABLE [landing].[CCOBCP] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCOBCP_WVOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOBCP_WVCPNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOBCP_WVTYCP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOBCP_WVRPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOBCP_WVCOUC] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (5 columns with CCOBCP_ prefix)
    [CCOBCP_WVOBNO] INT NULL,
    [CCOBCP_WVCPNO] INT NULL,
    [CCOBCP_WVTYCP] INT NULL,
    [CCOBCP_WVRPPD] INT NULL,
    [CCOBCP_WVCOUC] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCOBCP] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCOBCP_timestamp] ON [landing].[CCOBCP]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCOBCP_hash] ON [landing].[CCOBCP]([row_hash]);
GO

-- Table: CCOR
-- Source: ccor.xlsx
-- Columns: 114
CREATE TABLE [landing].[CCOR] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCOR_ORORNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORCPNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORGNLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORPCNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORCONO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORECN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORECN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORC1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORC2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORC3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORPVOB] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRGQC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRGQY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRGQM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRGQD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORUCN1] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORUCN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORUPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLWC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLWY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLWW] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORDL] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLNM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLAR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLLA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORNUFO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORNAFO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORORSC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLQC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLQY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLQM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORDLQD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRGRC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRGRY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOR_ORRGRM] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (114 columns with CCOR_ prefix)
    [CCOR_ORORNO_Order_Number] INT NULL,
    [CCOR_ORCPNO_Calculation_Number] INT NULL,
    [CCOR_ORGNLO_Client_Group_Number] INT NULL,
    [CCOR_ORCUNO_Customer_Number] INT NULL,
    [CCOR_ORPCNO_Profit_Centre_Number] INT NULL,
    [CCOR_ORCONO_Contract_Number] INT NULL,
    [CCOR_ORECN1_Color_1] VARCHAR(50) NULL,
    [CCOR_ORECN2_Color_2] VARCHAR(50) NULL,
    [CCOR_ORORC1_Comment_1] VARCHAR(200) NULL,
    [CCOR_ORORC2_Comment_2] VARCHAR(200) NULL,
    [CCOR_ORORC3_Comment_3] VARCHAR(200) NULL,
    [CCOR_ORORCC_Order_Date_Century] INT NULL,
    [CCOR_ORORYY_Order_Date_Year] INT NULL,
    [CCOR_ORORMM_Order_Date_Month] INT NULL,
    [CCOR_ORORDD_Order_Date_Day] INT NULL,
    [CCOR_ORORCD_Clients_Reference] VARCHAR(200) NULL,
    [CCOR_ORPVOB_Previous_Object_Number] INT NULL,
    [CCOR_ORRGQC_Order_Send_Registration_Date_A_Century] INT NULL,
    [CCOR_ORRGQY_Order_Send_Registration_Date_A_Year] INT NULL,
    [CCOR_ORRGQM_Order_Send_Registration_Date_A_Month] INT NULL,
    [CCOR_ORRGQD_Order_Send_Registration_Date_A_Day] INT NULL,
    [CCOR_ORUCN1_1st_Upholstery_Colour] VARCHAR(50) NULL,
    [CCOR_ORUCN2_2nd_Upholstery_Colour] VARCHAR(50) NULL,
    [CCOR_ORUPCD_Upholstery_Code] INT NULL,
    [CCOR_ORDLWC_Requested_Delivery_Week_Centu_A_Ry] INT NULL,
    [CCOR_ORDLWY_Requested_Delivery_Week_Year] INT NULL,
    [CCOR_ORDLWW_Requested_Delivery_Week_Week] INT NULL,
    [CCOR_ORDLCC_Delivery_Date_Century] INT NULL,
    [CCOR_ORDLYY_Delivery_Date_Year] INT NULL,
    [CCOR_ORDLMM_Delivery_Date_Month] INT NULL,
    [CCOR_ORDLDD_Delivery_Date_Day] INT NULL,
    [CCOR_ORORDL_Drivers_Lump_Sum] INT NULL,
    [CCOR_ORDLNM_Delivery_Address_Name] VARCHAR(200) NULL,
    [CCOR_ORDLN2_Delivery_Address_Name_Part_2] VARCHAR(200) NULL,
    [CCOR_ORDLAD_Delivery_Address_Street] VARCHAR(200) NULL,
    [CCOR_ORDLAR_Delivery_Address_Area_Code] VARCHAR(200) NULL,
    [CCOR_ORDLLO_Delivery_Address_Locality] VARCHAR(200) NULL,
    [CCOR_ORDLLA_Delivery_Address_Country] VARCHAR(200) NULL,
    [CCOR_ORNUFO_Supplier_Number] INT NULL,
    [CCOR_ORNAFO_Rotation_Number_Supplier] INT NULL,
    [CCOR_ORORSC_Order_Status_Code] INT NULL,
    [CCOR_ORRPPD_Reportingperiod] INT NULL,
    [CCOR_ORCOUC_Country_Code] VARCHAR(50) NULL,
    [CCOR_ORDLQC_Delivery_Requested_Date_Centu_A_Ry] INT NULL,
    [CCOR_ORDLQY_Delivery_Requested_Date_Year] INT NULL,
    [CCOR_ORDLQM_Delivery_Requested_Date_Month_A] INT NULL,
    [CCOR_ORDLQD_Delivery_Requested_Date_Day] INT NULL,
    [CCOR_ORRGRC_Registratrequest_Return_Cent_A_Ury] INT NULL,
    [CCOR_ORRGRY_Registratrequest_Return_Year_A] INT NULL,
    [CCOR_ORRGRM_Registratrequest_Return_Mont_A_H] INT NULL,
    [CCOR_ORRGRD_Registratrequest_Return_Day] INT NULL,
    [CCOR_ORIHOB_Interim_Hire_Object_Number] INT NULL,
    [CCOR_OROKCC_Delivery_Accepted_Date_Centur_A_Y] INT NULL,
    [CCOR_OROKYY_Delivery_Accepted_Date_Year] INT NULL,
    [CCOR_OROKMM_Delivery_Accepted_Date_Month] INT NULL,
    [CCOR_OROKDD_Delivery_Accepted_Date_Day] INT NULL,
    [CCOR_ORA4TC_Iedmt_Payment_Forseen_Cent] INT NULL,
    [CCOR_ORA4TY_Iedmt_Payment_Forseen_Year] INT NULL,
    [CCOR_ORA4TM_Iedmt_Payment_Forseen_Month] INT NULL,
    [CCOR_ORA4TD_Iedmt_Payment_Forseen_Day] INT NULL,
    [CCOR_ORA7TC_Creation_Century] INT NULL,
    [CCOR_ORA7TY_Creation_Date_Year] INT NULL,
    [CCOR_ORA7TM_Creation_Date_Month] INT NULL,
    [CCOR_ORA7TD_Creationy_Day] INT NULL,
    [CCOR_ORA8TC_Proposed_To_Buy_Date_Century] INT NULL,
    [CCOR_ORA8TY_Proposed_To_Buy_Date_Year] INT NULL,
    [CCOR_ORA8TM_Proposed_To_Buy_Date_Month] INT NULL,
    [CCOR_ORA8TD_Proposed_To_Buy_Date_Day] INT NULL,
    [CCOR_ORA9TC_Order_Accepted_Date_Century] INT NULL,
    [CCOR_ORA9TY_Order_Accepted_Date_Year] INT NULL,
    [CCOR_ORA9TM_Order_Accepted_Date_Month] INT NULL,
    [CCOR_ORA9TD_Order_Accepted_Date_Day] INT NULL,
    [CCOR_ORAATC_Registration_Forseen_Date_Cen_A_Tury] INT NULL,
    [CCOR_ORAATY_Registration_Forseen_Date_Yea_A_R] INT NULL,
    [CCOR_ORAATM_Registration_Forseen_Date_Mon_A_Th] INT NULL,
    [CCOR_ORAATD_Registration_Forseen_Date_Day_A] INT NULL,
    [CCOR_ORB1TC_Registration_Date_Century] INT NULL,
    [CCOR_ORB1TY_Registration_Date_Year] INT NULL,
    [CCOR_ORB1TM_Registration_Date_Month] INT NULL,
    [CCOR_ORB1TD_Registration_Date_Day] INT NULL,
    [CCOR_ORBATC_Invoice_Reception_Date_Centur_A_Y] INT NULL,
    [CCOR_ORBATY_Invoice_Reception_Date_Year] INT NULL,
    [CCOR_ORBATM_Invoice_Reception_Date_Month] INT NULL,
    [CCOR_ORBATD_Invoice_Reception_Date_Day] INT NULL,
    [CCOR_ORBCTC_Insurance_Requested_Date_Cent_A_Ury] INT NULL,
    [CCOR_ORBCTY_Insurance_Requested_Date_Year_A] INT NULL,
    [CCOR_ORBCTM_Insurance_Requested_Date_Mont_A_H] INT NULL,
    [CCOR_ORBCTD_Insurance_Requested_Date_Day] INT NULL,
    [CCOR_ORBDTC_Availability_Date_Century] INT NULL,
    [CCOR_ORBDTY_Availability_Date_Year] INT NULL,
    [CCOR_ORBDTM_Availability_Date_Month] INT NULL,
    [CCOR_ORBDTD_Availability_Date_Day] INT NULL,
    [CCOR_ORBLTC_Insurance_Date_Century] INT NULL,
    [CCOR_ORBLTY_Insurance_Date_Year] INT NULL,
    [CCOR_ORBLTM_Insurance_Date_Month] INT NULL,
    [CCOR_ORBLTD_Insurance_Date_Day] INT NULL,
    [CCOR_ORC0ST_Registration_Province] VARCHAR(200) NULL,
    [CCOR_ORCRCD_Office_Code] VARCHAR(200) NULL,
    [CCOR_ORCSCD_Group_Code] VARCHAR(200) NULL,
    [CCOR_ORCTCD_Vendor_Code] VARCHAR(200) NULL,
    [CCOR_ORDOTX_Delivery_Place] VARCHAR(200) NULL,
    [CCOR_ORDVST_Delivery_Province] VARCHAR(200) NULL,
    [CCOR_OROREC_Order_Send_Date_Century] INT NULL,
    [CCOR_OROREY_Order_Send_Date_Year] INT NULL,
    [CCOR_OROREM_Order_Send_Date_Month] INT NULL,
    [CCOR_ORORED_Order_Send_Date_Day] INT NULL,
    [CCOR_ORORRC_Order_Confirmation_Return_Dat_A_E_Centur] INT NULL,
    [CCOR_ORORRY_Order_Confirmation_Return_Dat_A_E_Year] INT NULL,
    [CCOR_ORORRM_Order_Confirmation_Return_Dat_A_E_Month] INT NULL,
    [CCOR_ORORRD_Order_Confirmation_Return_Dat_A_E_Day] INT NULL,
    [CCOR_ORSNN2] INT NULL,
    [CCOR_ORSQN2] VARCHAR(50) NULL,
    [CCOR_ORSTCA_Start_Cause_Table_S] INT NULL,
    [CCOR_ORORMQ_Object_Started_With_Matrix_004n_A_Nbr] INT NULL,

    CONSTRAINT [PK_landing_CCOR] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCOR_timestamp] ON [landing].[CCOR]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCOR_hash] ON [landing].[CCOR]([row_hash]);
GO

-- Table: CCOS
-- Source: ccos.xlsx
-- Columns: 23
CREATE TABLE [landing].[CCOS] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCOS_OSOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSFIKM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSOSEC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSOSEY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSOSEM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSOSED] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSOSSC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRIAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRIDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRID2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRID3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSSLCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSSLCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSSLYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSSLMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSSLDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRGDD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRGMM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRGYY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRGCC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSOSCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRBWS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCOS_OSRBRS] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (23 columns with CCOS_ prefix)
    [CCOS_OSOBNO_Object_Number] INT NULL,
    [CCOS_OSFIKM_Final_Mileage] INT NULL,
    [CCOS_OSOSEC_Final_Date_Century] INT NULL,
    [CCOS_OSOSEY_Final_Date_Year] INT NULL,
    [CCOS_OSOSEM_Final_Date_Month] INT NULL,
    [CCOS_OSOSED_Final_Date_Day] INT NULL,
    [CCOS_OSOSSC_Object_Sales_Status] INT NULL,
    [CCOS_OSRIAD_Return_Adress_Object] VARCHAR(50) NULL,
    [CCOS_OSRIDS_Return_Adress_Description] VARCHAR(200) NULL,
    [CCOS_OSRID2] VARCHAR(200) NULL,
    [CCOS_OSRID3] VARCHAR(200) NULL,
    [CCOS_OSSLCD_Sales_Reason_Code] INT NULL,
    [CCOS_OSSLCC_Object_Sales_Date_Century] INT NULL,
    [CCOS_OSSLYY_Object_Sales_Date_Year] INT NULL,
    [CCOS_OSSLMM_Object_Sales_Date_Month] INT NULL,
    [CCOS_OSSLDD_Object_Sales_Date_Day] INT NULL,
    [CCOS_OSRGDD_Object_Due_Date_Registration_V027_A_Day] INT NULL,
    [CCOS_OSRGMM_Registration_Return_Month] INT NULL,
    [CCOS_OSRGYY_Registration_Return_Year] INT NULL,
    [CCOS_OSRGCC_Registration_Return_Century] INT NULL,
    [CCOS_OSOSCD_Code_Return_Of_Registration] VARCHAR(50) NULL,
    [CCOS_OSRBWS] DECIMAL(10,4) NULL,
    [CCOS_OSRBRS] DECIMAL(10,4) NULL,

    CONSTRAINT [PK_landing_CCOS] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCOS_timestamp] ON [landing].[CCOS]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCOS_hash] ON [landing].[CCOS]([row_hash]);
GO

-- Table: CCPC
-- Source: ccpc.xlsx
-- Columns: 20
CREATE TABLE [landing].[CCPC] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCPC_PCCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCNM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCLN] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCAD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCA2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCAR] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCLA] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCTF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCNF] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCNE] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCTI] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCRPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCRPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCTT] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCPC_PCPCAO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (20 columns with CCPC_ prefix)
    [CCPC_PCCUNO_Customer_Number] INT NULL,
    [CCPC_PCPCNO_Profit_Centre_Number] INT NULL,
    [CCPC_PCPCNM_Profit_Centre_Name] VARCHAR(50) NULL,
    [CCPC_PCPCN2_Profit_Centre_Name_2] VARCHAR(50) NULL,
    [CCPC_PCPCDS_Profit_Centre_Description] VARCHAR(50) NULL,
    [CCPC_PCPCLN_Profit_Centre_Language_Code] VARCHAR(50) NULL,
    [CCPC_PCPCAD_Profit_Centre_Address_Street] VARCHAR(50) NULL,
    [CCPC_PCPCA2_Profit_Centre_Address_2] VARCHAR(50) NULL,
    [CCPC_PCPCLO_Profit_Centre_Locality] VARCHAR(50) NULL,
    [CCPC_PCPCAR_Profit_Centre_Area_Code] VARCHAR(50) NULL,
    [CCPC_PCPCLA_Profit_Centre_Country] VARCHAR(50) NULL,
    [CCPC_PCPCTF_Telephone_Number] VARCHAR(50) NULL,
    [CCPC_PCPCNF_Fax_Number] VARCHAR(200) NULL,
    [CCPC_PCPCNE_Email_Number] VARCHAR(200) NULL,
    [CCPC_PCPCTI_Profit_Centre_Title] VARCHAR(200) NULL,
    [CCPC_PCRPPD_Reportingperiod] INT NULL,
    [CCPC_PCCOUC_Country_Code] VARCHAR(50) NULL,
    [CCPC_PCRPCD_Representative_Code] VARCHAR(50) NULL,
    [CCPC_PCPCTT_Sex_Cde_Of_Att_To_Person] VARCHAR(200) NULL,
    [CCPC_PCPCAO_Attention_To_Name_Or_Code] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCPC] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCPC_timestamp] ON [landing].[CCPC]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCPC_hash] ON [landing].[CCPC]([row_hash]);
GO

-- Table: CCRS
-- Source: ccrs.xlsx
-- Columns: 11
CREATE TABLE [landing].[CCRS] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCRS_RSRSCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSIFTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSRSNM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSRSN2] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSRSN3] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSCLLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSNTLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSNELO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSNFLO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSRPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCRS_RSCOUC] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (11 columns with CCRS_ prefix)
    [CCRS_RSRSCD_Representative_Code] VARCHAR(50) NULL,
    [CCRS_RSIFTY_Information_Type] INT NULL,
    [CCRS_RSRSNM_Representative_Name] VARCHAR(50) NULL,
    [CCRS_RSRSN2_Representative_Name_Part_2] VARCHAR(200) NULL,
    [CCRS_RSRSN3_Representative_Name_Part_3] VARCHAR(200) NULL,
    [CCRS_RSCLLO_Language_Code] VARCHAR(200) NULL,
    [CCRS_RSNTLO_Telephone_Number] VARCHAR(200) NULL,
    [CCRS_RSNELO_Email_Address] VARCHAR(200) NULL,
    [CCRS_RSNFLO_Fax_Number] VARCHAR(200) NULL,
    [CCRS_RSRPPD_Reportingperiod] INT NULL,
    [CCRS_RSCOUC_Country_Code] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCRS] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCRS_timestamp] ON [landing].[CCRS]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCRS_hash] ON [landing].[CCRS]([row_hash]);
GO

-- Table: CCXC
-- Source: ccxc.xlsx
-- Columns: 13
CREATE TABLE [landing].[CCXC] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CCXC_XCCPNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCASCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCXCAM] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCXCMP] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCXCTY] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCXCDS] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCXCTX] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCRPPD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCCOUC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCVOLC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCDISC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCCONC] AS VARCHAR(MAX)), ''), COALESCE(CAST([CCXC_XCCURC] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (13 columns with CCXC_ prefix)
    [CCXC_XCCPNO_Calculation_Number] INT NULL,
    [CCXC_XCASCD_Accessory_Code] INT NULL,
    [CCXC_XCXCAM] DECIMAL(10,4) NULL,
    [CCXC_XCXCMP_Reduction] DECIMAL(6,4) NULL,
    [CCXC_XCXCTY_Accessory_Type] VARCHAR(50) NULL,
    [CCXC_XCXCDS_Accessory_Description] VARCHAR(200) NULL,
    [CCXC_XCXCTX_Accessory_Text] VARCHAR(200) NULL,
    [CCXC_XCRPPD_Reportingperiod] INT NULL,
    [CCXC_XCCOUC_Country_Code] VARCHAR(50) NULL,
    [CCXC_XCVOLC_Volume_Measurement] VARCHAR(50) NULL,
    [CCXC_XCDISC_Distance_Measurement] VARCHAR(50) NULL,
    [CCXC_XCCONC_Consumption_Measurement] VARCHAR(50) NULL,
    [CCXC_XCCURC_Currency_Unit] VARCHAR(50) NULL,

    CONSTRAINT [PK_landing_CCXC] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCXC_timestamp] ON [landing].[CCXC]([extraction_timestamp]);
CREATE INDEX [IX_landing_CCXC_hash] ON [landing].[CCXC]([row_hash]);
GO

-- Table: CWAU
-- Source: cwau.xlsx
-- Columns: 2
CREATE TABLE [landing].[CWAU] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWAU_WAMKCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWAU_WAMDCD] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (2 columns with CWAU_ prefix)
    [CWAU_WAMKCD_Make_Code] INT NULL,
    [CWAU_WAMDCD_Model_Code] INT NULL,

    CONSTRAINT [PK_landing_CWAU] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWAU_timestamp] ON [landing].[CWAU]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWAU_hash] ON [landing].[CWAU]([row_hash]);
GO

-- Table: CWBI
-- Source: cwbi.xlsx
-- Columns: 2
CREATE TABLE [landing].[CWBI] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWBI_W6OBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWBI_W6SLPR] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (2 columns with CWBI_ prefix)
    [CWBI_W6OBNO_Object_Number] INT NULL,
    [CWBI_W6SLPR_Final_Sales_Price] DECIMAL(11,4) NULL,

    CONSTRAINT [PK_landing_CWBI] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWBI_timestamp] ON [landing].[CWBI]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWBI_hash] ON [landing].[CWBI]([row_hash]);
GO

-- Table: CWCO
-- Source: cwco.xlsx
-- Columns: 3
CREATE TABLE [landing].[CWCO] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWCO_WBCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWCO_WBLPCD] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWCO_WBCONO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (3 columns with CWCO_ prefix)
    [CWCO_WBCUNO_Customer_Number] INT NULL,
    [CWCO_WBLPCD_Company_Code] VARCHAR(50) NULL,
    [CWCO_WBCONO_Contract_Number] INT NULL,

    CONSTRAINT [PK_landing_CWCO] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWCO_timestamp] ON [landing].[CWCO]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWCO_hash] ON [landing].[CWCO]([row_hash]);
GO

-- Table: CWCP
-- Source: cwcp.xlsx
-- Columns: 1
CREATE TABLE [landing].[CWCP] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWCP_WHCPNO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (1 columns with CWCP_ prefix)
    [CWCP_WHCPNO_Calculation_Number] INT NULL,

    CONSTRAINT [PK_landing_CWCP] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWCP_timestamp] ON [landing].[CWCP]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWCP_hash] ON [landing].[CWCP]([row_hash]);
GO

-- Table: CWCU
-- Source: cwcu.xlsx
-- Columns: 2
CREATE TABLE [landing].[CWCU] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWCU_WUCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWCU_WUNGLO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (2 columns with CWCU_ prefix)
    [CWCU_WUCUNO_Customer_Number] INT NULL,
    [CWCU_WUNGLO] INT NULL,

    CONSTRAINT [PK_landing_CWCU] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWCU_timestamp] ON [landing].[CWCU]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWCU_hash] ON [landing].[CWCU]([row_hash]);
GO

-- Table: CWDR
-- Source: cwdr.xlsx
-- Columns: 2
CREATE TABLE [landing].[CWDR] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWDR_DROBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWDR_NRACDR] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (2 columns with CWDR_ prefix)
    [CWDR_DROBNO_Object_Number] INT NULL,
    [CWDR_NRACDR_Actual_Driver_Code] INT NULL,

    CONSTRAINT [PK_landing_CWDR] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWDR_timestamp] ON [landing].[CWDR]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWDR_hash] ON [landing].[CWDR]([row_hash]);
GO

-- Table: CWGR
-- Source: cwgr.xlsx
-- Columns: 1
CREATE TABLE [landing].[CWGR] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWGR_WGNGLO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (1 columns with CWGR_ prefix)
    [CWGR_WGNGLO] INT NULL,

    CONSTRAINT [PK_landing_CWGR] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWGR_timestamp] ON [landing].[CWGR]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWGR_hash] ON [landing].[CWGR]([row_hash]);
GO

-- Table: CWOA
-- Source: cwoa.xlsx
-- Columns: 2
CREATE TABLE [landing].[CWOA] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWOA_WICUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWOA_WIOBBY] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (2 columns with CWOA_ prefix)
    [CWOA_WICUNO_Customer_Number] INT NULL,
    [CWOA_WIOBBY_Object_Begin_Date_Year] INT NULL,

    CONSTRAINT [PK_landing_CWOA] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWOA_timestamp] ON [landing].[CWOA]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWOA_hash] ON [landing].[CWOA]([row_hash]);
GO

-- Table: CWOB
-- Source: cwob.xlsx
-- Columns: 4
CREATE TABLE [landing].[CWOB] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWOB_WOOBNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWOB_WOCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWOB_WOPCNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWOB_WONGLO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (4 columns with CWOB_ prefix)
    [CWOB_WOOBNO_Object_Number] INT NULL,
    [CWOB_WOCUNO_Customer_Number] INT NULL,
    [CWOB_WOPCNO_Profit_Centre_Number] INT NULL,
    [CWOB_WONGLO] INT NULL,

    CONSTRAINT [PK_landing_CWOB] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWOB_timestamp] ON [landing].[CWOB]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWOB_hash] ON [landing].[CWOB]([row_hash]);
GO

-- Table: CWOR
-- Source: cwor.xlsx
-- Columns: 1
CREATE TABLE [landing].[CWOR] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWOR_WKOBNO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (1 columns with CWOR_ prefix)
    [CWOR_WKOBNO_Object_Number] INT NULL,

    CONSTRAINT [PK_landing_CWOR] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWOR_timestamp] ON [landing].[CWOR]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWOR_hash] ON [landing].[CWOR]([row_hash]);
GO

-- Table: CWPC
-- Source: cwpc.xlsx
-- Columns: 3
CREATE TABLE [landing].[CWPC] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWPC_WPCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWPC_WPPCNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWPC_WPNGLO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (3 columns with CWPC_ prefix)
    [CWPC_WPCUNO_Customer_Number] INT NULL,
    [CWPC_WPPCNO_Profit_Centre_Number] INT NULL,
    [CWPC_WPNGLO] INT NULL,

    CONSTRAINT [PK_landing_CWPC] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWPC_timestamp] ON [landing].[CWPC]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWPC_hash] ON [landing].[CWPC]([row_hash]);
GO

-- Table: CWPO
-- Source: cwpo.xlsx
-- Columns: 4
CREATE TABLE [landing].[CWPO] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    [row_hash] AS HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(CAST([CWPO_WLCUNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWPO_WLPCNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWPO_WLORNO] AS VARCHAR(MAX)), ''), COALESCE(CAST([CWPO_WLCPNO] AS VARCHAR(MAX)), ''))) PERSISTED,

    -- Source columns (4 columns with CWPO_ prefix)
    [CWPO_WLCUNO_Customer_Number] INT NULL,
    [CWPO_WLPCNO_Profit_Centre_Number] INT NULL,
    [CWPO_WLORNO_Order_Number] INT NULL,
    [CWPO_WLCPNO_Calculation_Number] INT NULL,

    CONSTRAINT [PK_landing_CWPO] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CWPO_timestamp] ON [landing].[CWPO]([extraction_timestamp]);
CREATE INDEX [IX_landing_CWPO_hash] ON [landing].[CWPO]([row_hash]);
GO

-- ============================================================================
-- Landing Table Relationship Mapping
-- ============================================================================
/*
PURPOSE
-------
This section documents all implicit foreign-key relationships between landing
tables. The landing schema has no enforced FKs (data is loaded as-is from
Excel/CSV sources), but these logical relationships are critical for:
  - ETL join logic (landing -> staging)
  - Data quality validation
  - Understanding the fleet management domain model

TABLE CLASSIFICATION
--------------------
Hub / Master Tables (central entities):
  CCOB  - Vehicles (Objects)           33,374 rows
  CCCP  - Contract Positions           33,652 rows
  CCCU  - Customers (Clients)           2,387 rows

Reference / Lookup Tables:
  CCAU  - Automobile Catalog (Make/Model)  9,357 rows
  CCRS  - Representatives                     44 rows

Transactional / Detail Tables:
  CCBI  - Billing                       22,934 rows
  CCCA  - Fuel Cards                     8,219 rows
  CCCO  - Contract Options               2,945 rows
  CCDA  - Damages                            0 rows
  CCDR  - Drivers                       33,633 rows
  CCFP  - Fleet Positions              16,255 rows
  CCGD  - Maintenance/Service History  402,428 rows
  CCGR  - Client Groups                 1,961 rows
  CCIN  - Insurance                         42 rows
  CCOBCP- Object-Contract Link         66,792 rows
  CCOR  - Orders                        33,418 rows
  CCOS  - Object Sales/Returns         25,661 rows
  CCPC  - Profit Centres                3,863 rows
  CCXC  - Accessories                  42,858 rows

Financial Snapshot Tables:
  CCFC  - Financial Conditions (auto catalog variant)  9,357 rows
  CCFID - Financial ID variant                         9,357 rows
  CCFIH - Financial History variant                    9,357 rows
  CCFIM - Financial Model variant                      9,357 rows
  CCIO  - Insurance Options (auto catalog variant)     9,357 rows

Country-Wide (CW*) Supplement Tables:
  CW* tables carry additional country-specific fields for the corresponding
  CC* table. They share the same key columns (Object_Number, Customer_Number,
  etc.) and mirror the CC* relationships at country level.

RELATIONSHIP MAP
----------------
All relationships are Many-to-One unless otherwise noted.

Source Table   Source Column                           Target Table   Target Column                         Description
-------------- -------------------------------------- -------------- ------------------------------------- ------------------------------------------
CCBI           CCBI_BIOBNO_Object_Number              CCOB           CCOB_OBOBNO_Object_Number             Billing -> Vehicle
CCCA           CCCA_CACAOB (Object ref)               CCOB           CCOB_OBOBNO_Object_Number             Fuel Card -> Vehicle
CCCA           CCCA_CACADR (Driver ref)                CCDR           CCDR_DRDRNO_Driver_Number             Fuel Card -> Driver
CCCO           CCCO_COCUNO_Customer_Number             CCCU           CCCU_CUNULO_Client_Number             Contract Option -> Customer
CCCO           CCCO_COCONO_Contract_Number             CCOB           CCOB_OBCONO_Contract_Number           Contract Option -> Contract (via vehicle)
CCCP           CCCP_CPCUNO_Customer_Number             CCCU           CCCU_CUNULO_Client_Number             Contract Position -> Customer
CCCP           CCCP_CPMKCD_Make_Code                   CCAU           CCAU_AUMKCD_Make_Code                 Contract -> Make lookup
CCCP           CCCP_CPMDCD_Model_Code                  CCAU           CCAU_AUMDCD_Model_Code                Contract -> Model lookup (composite w/ Make)
CCDA           CCDA_DAOBNO_Object_Number               CCOB           CCOB_OBOBNO_Object_Number             Damage -> Vehicle
CCDA           CCDA_DADADR_Driver_Number               CCDR           CCDR_DRDRNO_Driver_Number             Damage -> Driver
CCDR           CCDR_DROBNO_Object_Number               CCOB           CCOB_OBOBNO_Object_Number             Driver -> Vehicle (current assignment)
CCFP           CCFP_FPOBNO_Object_Number               CCOB           CCOB_OBOBNO_Object_Number             Fleet Position -> Vehicle
CCFP           CCFP_FPCUNO_Customer_Number             CCCU           CCCU_CUNULO_Client_Number             Fleet Position -> Customer
CCFP           CCFP_FPCPNO_Calculation_Number          CCCP           CCCP_CPCPNO_Contract_Position_Number  Fleet Position -> Contract Position
CCFP           CCFP_FPCONO_Contract_Number             CCCP           CCCP_CPCONO_Contract_Number           Fleet Position -> Contract
CCFP           CCFP_FPDRNO_Driver_Number               CCDR           CCDR_DRDRNO_Driver_Number             Fleet Position -> Driver
CCGD           CCGD_MAOBNO_Object_Number               CCOB           CCOB_OBOBNO_Object_Number             Maintenance -> Vehicle
CCGD           CCGD_MANUFO_Supplier_Number             CCGR           CCGR_GRGNLO_Client_Group_Number       Maintenance -> Supplier/Group (soft ref)
CCGR           CCGR_GRNKLO_Sub_Branch_Code             CCCU           CCCU_CUNKLO_Sub_Branch_Code           Group -> Customer (branch-level link)
CCIN           CCIN_INOBNO_Object_Number               CCOB           CCOB_OBOBNO_Object_Number             Insurance -> Vehicle
CCOB           CCOB_OBCUNO_Customer_Number             CCCU           CCCU_CUNULO_Client_Number             Vehicle -> Customer (lessee)
CCOB           CCOB_OBCONO_Contract_Number             CCCP           CCCP_CPCONO_Contract_Number           Vehicle -> Contract
CCOB           CCOB_OBCPNO_Calculation_Number          CCCP           CCCP_CPCPNO_Contract_Position_Number  Vehicle -> Calculation/Contract Position
CCOB           CCOB_OBMKCD_Make_Code                   CCAU           CCAU_AUMKCD_Make_Code                 Vehicle -> Make lookup
CCOB           CCOB_OBMDCD_Model_Code                  CCAU           CCAU_AUMDCD_Model_Code                Vehicle -> Model lookup (composite w/ Make)
CCOB           CCOB_OBGRNO_Client_Group_Number         CCCU           CCCU_CUGNLO_Client_Group_Number       Vehicle -> Client Group
CCOB           CCOB_OBORNO_Order_Number                CCOR           CCOR_ORORNO_Order_Number              Vehicle -> Order
CCOBCP         CCOBCP_WVOBNO (Object Number)           CCOB           CCOB_OBOBNO_Object_Number             Object-Contract link -> Vehicle
CCOBCP         CCOBCP_WVCPNO (Calculation Number)      CCCP           CCCP_CPCPNO_Contract_Position_Number  Object-Contract link -> Contract Position
CCOR           CCOR_ORCUNO_Customer_Number             CCCU           CCCU_CUNULO_Client_Number             Order -> Customer
CCOR           CCOR_ORCPNO_Calculation_Number          CCCP           CCCP_CPCPNO_Contract_Position_Number  Order -> Contract Position
CCOS           CCOS_OSOBNO_Object_Number               CCOB           CCOB_OBOBNO_Object_Number             Object Sales -> Vehicle
CCPC           CCPC_PCCUNO_Customer_Number             CCCU           CCCU_CUNULO_Client_Number             Profit Centre -> Customer
CCXC           CCXC_XCCPNO_Calculation_Number          CCCP           CCCP_CPCPNO_Contract_Position_Number  Accessory -> Contract Position

DERIVED / COMPUTED FIELDS
-------------------------
  Expected Termination Date:
    Computed in staging and semantic layers as:
      expected_end_date = date(CCOB_Start_Date, '+' || CCCP_CPCPDU_Duration || ' months')
    Source columns: CCOB_OBOBBC/BY/BM/BD (start date parts) + CCCP_CPCPDU_Contract_Duration_Months
    Also available from: CCOB_OBFSDU_Budgeted_Months on the vehicle object itself
  Months Driven:
    months_driven = months between start_date and current date
  Months Remaining:
    months_remaining = months between current date and expected_end_date
    Negative values indicate contract is past expected termination.

NOTE ON COMPOSITE KEYS
----------------------
  - CCAU (Automobile Catalog) uses a composite key: (AUMKCD_Make_Code, AUMDCD_Model_Code).
    Tables referencing make/model (CCOB, CCCP) should join on BOTH columns.
  - CCOBCP links objects to contract positions and may have multiple rows
    per object (one per contract position type: WVTYCP).
  - CCDR is keyed by (DROBNO_Object_Number, DRDRNO_Driver_Number); a vehicle
    can have multiple driver records over time.

CW* (COUNTRY-WIDE) TABLE RELATIONSHIPS
---------------------------------------
Each CW* table supplements its CC* counterpart with country-specific data.
They share the same key columns and mirror the CC* relationships:

  CWAU  -> mirrors CCAU  (Make_Code, Model_Code)
  CWBI  -> mirrors CCBI  (Object_Number)
  CWCO  -> mirrors CCCO  (Customer_Number, Contract_Number)
  CWCP  -> mirrors CCCP  (Calculation_Number)
  CWCU  -> mirrors CCCU  (Customer_Number)
  CWDR  -> mirrors CCDR  (Object_Number)
  CWGR  -> mirrors CCGR  (Group_Number via WGNGLO)
  CWOA  -> supplements   (Customer_Number, Object_Begin_Year)
  CWOB  -> mirrors CCOB  (Object_Number, Customer_Number, Profit_Centre)
  CWOR  -> mirrors CCOR  (Object_Number via WKOBNO)
  CWPC  -> mirrors CCPC  (Customer_Number, Profit_Centre)
  CWPO  -> supplements   (Customer_Number, Order_Number, Calculation_Number)

ENTITY-RELATIONSHIP SUMMARY (hub-centric view)
-----------------------------------------------

  CCCU (Customer)
    |<-- CCOB.OBCUNO      (vehicles leased to this customer)
    |<-- CCCP.CPCUNO      (contract positions for this customer)
    |<-- CCCO.COCUNO      (contract options)
    |<-- CCOR.ORCUNO      (orders placed by this customer)
    |<-- CCPC.PCCUNO      (profit centres)
    |<-- CCFP.FPCUNO      (fleet positions)
    |<-- CCGR.GRNKLO      (groups/branches)

  CCOB (Vehicle/Object)
    |<-- CCBI.BIOBNO      (billing records)
    |<-- CCCA.CACAOB      (fuel cards assigned)
    |<-- CCDA.DAOBNO      (damage records)
    |<-- CCDR.DROBNO      (driver assignments)
    |<-- CCFP.FPOBNO      (fleet positions)
    |<-- CCGD.MAOBNO      (maintenance/service stops)
    |<-- CCIN.INOBNO      (insurance records)
    |<-- CCOBCP.WVOBNO    (contract position links)
    |<-- CCOS.OSOBNO      (sales/return records)
    |--> CCCU.CUNULO      (owning customer, via OBCUNO)
    |--> CCCP.CPCONO      (active contract, via OBCONO)
    |--> CCAU              (make/model, via OBMKCD+OBMDCD)
    |--> CCOR.ORORNO      (originating order, via OBORNO)

  CCCP (Contract Position)
    |<-- CCOB.OBCPNO      (vehicles on this contract position)
    |<-- CCOBCP.WVCPNO    (object-contract links)
    |<-- CCOR.ORCPNO      (orders referencing this position)
    |<-- CCXC.XCCPNO      (accessories on this contract)
    |<-- CCFP.FPCPNO      (fleet positions)
    |--> CCCU.CUNULO      (customer, via CPCUNO)
    |--> CCAU              (make/model, via CPMKCD+CPMDCD)

VERIFIED AGAINST
----------------
  - ETL script: database/scripts/etl_landing_to_staging.py
    (all JOIN keys confirmed in load_vehicles, load_contracts, load_orders,
     load_billing, load_groups, load_odometer_history, load_damages)
  - Staging FKs: database/schemas/02_staging.sql
    (staging FK constraints match the logical relationships above)
*/

-- ============================================================================
-- Additional Landing Tables (New Source Files)
-- ============================================================================

-- Table: CCDT
-- Source: ccdt.xlsx
-- Domain Translations (Reference)
CREATE TABLE [landing].[CCDT] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity

    -- Source columns with documented names
    [CCDT_DTCYCD_Country_Code] VARCHAR(10) NULL,
    [CCDT_DTDMID_Domain_ID] INT NULL,
    [CCDT_DTDMVA_Domain_Value] VARCHAR(50) NULL,
    [CCDT_DTDMLN_Language_Code] VARCHAR(10) NULL,
    [CCDT_DTDMTX_Domain_Text] VARCHAR(200) NULL,

    CONSTRAINT [PK_landing_CCDT] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCDT_timestamp] ON [landing].[CCDT]([extraction_timestamp]);
GO

-- Table: CCES
-- Source: cces.xlsx
-- Exploitation Services (Transactional)
CREATE TABLE [landing].[CCES] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity

    -- Source columns with documented names
    [CCES_ESCUNO_Customer_No] INT NULL,
    [CCES_ESPCNO_Contract_Position_No] INT NULL,
    [CCES_ESOBNO_Object_No] INT NULL,
    [CCES_ESESSQ_Service_Sequence] INT NULL,
    [CCES_ESESCD_Service_Code] INT NULL,
    [CCES_ESESCT_Service_Cost_Total] DECIMAL(15,2) NULL,
    [CCES_ESESIV_Service_Invoice] DECIMAL(15,2) NULL,
    [CCES_ESIVSU_Invoice_Supplier] DECIMAL(15,2) NULL,
    [CCES_ESTMCT_Total_Monthly_Cost] DECIMAL(15,2) NULL,
    [CCES_ESTMIV_Total_Monthly_Invoice] DECIMAL(15,2) NULL,
    [CCES_ESLPCD_LP_Code] VARCHAR(10) NULL,
    [CCES_ESRPPD_Reporting_Period] INT NULL,
    [CCES_ESCOUC_Country_Code] VARCHAR(10) NULL,
    [CCES_ESVOLC_Volume_Code] VARCHAR(10) NULL,
    [CCES_ESDISC_Distance_Code] VARCHAR(10) NULL,
    [CCES_ESCONC_Consumption_Code] VARCHAR(10) NULL,
    [CCES_ESCURC_Currency_Code] VARCHAR(10) NULL,

    CONSTRAINT [PK_landing_CCES] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCES_timestamp] ON [landing].[CCES]([extraction_timestamp]);
GO

-- Table: CCMS
-- Source: ccms.xlsx
-- Maintenance Approvals (Transactional)
CREATE TABLE [landing].[CCMS] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity

    -- Source columns with documented names
    [CCMS_MSOBNO_Object_No] INT NULL,
    [CCMS_MSGDSQ_Sequence] INT NULL,
    [CCMS_MSGDCC_Date_Century] INT NULL,
    [CCMS_MSGDYY_Date_Year] INT NULL,
    [CCMS_MSGDMM_Date_Month] INT NULL,
    [CCMS_MSGDDD_Date_Day] INT NULL,
    [CCMS_MSGDKM_Mileage_Km] DECIMAL(15,2) NULL,
    [CCMS_MSGDAM_Amount] DECIMAL(15,2) NULL,
    [CCMS_MSGDDS_Description] VARCHAR(200) NULL,
    [CCMS_MSGDD2_Description_2] VARCHAR(200) NULL,
    [CCMS_MSGDD3_Description_3] VARCHAR(200) NULL,
    [CCMS_MSGDSC_Source_Code] VARCHAR(10) NULL,
    [CCMS_MSMSTY_Maintenance_Type] INT NULL,
    [CCMS_MSNUFO_Supplier_No] INT NULL,
    [CCMS_MSNAFO_Supplier_Branch] VARCHAR(50) NULL,
    [CCMS_MSMJCD_Major_Code] VARCHAR(10) NULL,
    [CCMS_MSMNCD_Minor_Code] VARCHAR(10) NULL,
    [CCMS_MSRPPD_Reporting_Period] INT NULL,
    [CCMS_MSCOUC_Country_Code] VARCHAR(10) NULL,
    [CCMS_MSVOLC_Volume_Code] VARCHAR(10) NULL,
    [CCMS_MSDISC_Distance_Code] VARCHAR(10) NULL,
    [CCMS_MSCONC_Consumption_Code] VARCHAR(10) NULL,
    [CCMS_MSCURC_Currency_Code] VARCHAR(10) NULL,
    [CCMS_MSSIRN_SI_Run_No] INT NULL,
    [CCMS_MSDAFC_Approval_Date_Century] INT NULL,
    [CCMS_MSDAFY_Approval_Date_Year] INT NULL,
    [CCMS_MSDAFM_Approval_Date_Month] INT NULL,
    [CCMS_MSDAFD_Approval_Date_Day] INT NULL,

    CONSTRAINT [PK_landing_CCMS] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCMS_timestamp] ON [landing].[CCMS]([extraction_timestamp]);
GO

-- Table: CCPI
-- Source: ccpi.xlsx
-- Passed On Invoices (Financial)
CREATE TABLE [landing].[CCPI] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity

    -- Source columns with documented names
    [CCPI_PICONO_Contract_No] INT NULL,
    [CCPI_PICUNO_Customer_No] INT NULL,
    [CCPI_PINACD_Name_Code] VARCHAR(50) NULL,
    [CCPI_PIOBNO_Object_No] INT NULL,
    [CCPI_PIPCNO_Contract_Position_No] INT NULL,
    [CCPI_PIPIAM_Amount] DECIMAL(15,2) NULL,
    [CCPI_PIPICD_Cost_Code] DECIMAL(15,2) NULL,
    [CCPI_PIEBRP_EB_Reporting_Period] DECIMAL(15,2) NULL,
    [CCPI_PIPIDR_Driver_No] DECIMAL(15,2) NULL,
    [CCPI_PIPIDS_Description] VARCHAR(200) NULL,
    [CCPI_PIPIGR_Gross_Net] VARCHAR(10) NULL,
    [CCPI_PIPIIV_Invoice_No] DECIMAL(15,2) NULL,
    [CCPI_PIPILP_LP_Code] VARCHAR(10) NULL,
    [CCPI_PIPIOB_Object_Bridge] DECIMAL(15,2) NULL,
    [CCPI_PIPIOR_Origin_Code] VARCHAR(10) NULL,
    [CCPI_PIPIRN_Run_No] DECIMAL(15,2) NULL,
    [CCPI_PIPISC_Source_Code] VARCHAR(10) NULL,
    [CCPI_PIPIVT_VAT_Type] VARCHAR(10) NULL,
    [CCPI_PIRPPD_Reporting_Period] INT NULL,
    [CCPI_PICOUC_Country_Code] VARCHAR(10) NULL,

    CONSTRAINT [PK_landing_CCPI] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCPI_timestamp] ON [landing].[CCPI]([extraction_timestamp]);
GO

-- Table: CCRC
-- Source: ccrc.xlsx
-- Replacement Cars (Transactional)
CREATE TABLE [landing].[CCRC] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity

    -- Source columns with documented names
    [CCRC_RCOBNO_Object_No] INT NULL,
    [CCRC_RCRCNO_RC_No] INT NULL,
    [CCRC_RCRCSQ_Sequence] VARCHAR(10) NULL,
    [CCRC_RCDRNO_Driver_No] DECIMAL(15,2) NULL,
    [CCRC_RCRCRN_RC_Run_No] DECIMAL(15,2) NULL,
    [CCRC_RCRCBC_Begin_Date_Century] INT NULL,
    [CCRC_RCRCBY_Begin_Date_Year] INT NULL,
    [CCRC_RCRCBM_Begin_Date_Month] INT NULL,
    [CCRC_RCRCBD_Begin_Date_Day] INT NULL,
    [CCRC_RCRCEC_End_Date_Century] INT NULL,
    [CCRC_RCRCEY_End_Date_Year] INT NULL,
    [CCRC_RCRCEM_End_Date_Month] INT NULL,
    [CCRC_RCRCED_End_Date_Day] INT NULL,
    [CCRC_RCRCCD_RC_Code] VARCHAR(10) NULL,
    [CCRC_RCRCKM_Km] DECIMAL(15,2) NULL,
    [CCRC_RCRCAM_Amount] DECIMAL(15,2) NULL,
    [CCRC_RCRCRS_Reason] VARCHAR(200) NULL,
    [CCRC_RCRCDS_Description] VARCHAR(200) NULL,
    [CCRC_RCRCD2_Description_2] VARCHAR(200) NULL,
    [CCRC_RCRCD3_Description_3] VARCHAR(200) NULL,
    [CCRC_RCRCTY_Type] VARCHAR(10) NULL,
    [CCRC_RCRCDR_Driver_Name] VARCHAR(100) NULL,
    [CCRC_RCRCSC_Source_Code] VARCHAR(10) NULL,
    [CCRC_RCRPPD_Reporting_Period] INT NULL,
    [CCRC_RCCOUC_Country_Code] VARCHAR(10) NULL,

    CONSTRAINT [PK_landing_CCRC] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCRC_timestamp] ON [landing].[CCRC]([extraction_timestamp]);
GO

-- Table: CCRP
-- Source: ccrp.xlsx
-- Reporting Periods (Reference)
CREATE TABLE [landing].[CCRP] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity

    -- Source columns with documented names
    [CCRP_RPRPCC_Period_CC] DECIMAL(15,2) NULL,
    [CCRP_RPRPYY_Period_YY] DECIMAL(15,2) NULL,
    [CCRP_RPRPMM_Period_MM] DECIMAL(15,2) NULL,
    [CCRP_RPRPDD_Period_DD] DECIMAL(15,2) NULL,
    [CCRP_RPRPPD_Reporting_Period] DECIMAL(15,2) NULL,
    [CCRP_RPMTPD_Month_Period] DECIMAL(15,2) NULL,

    CONSTRAINT [PK_landing_CCRP] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCRP_timestamp] ON [landing].[CCRP]([extraction_timestamp]);
GO

-- Table: CCSU
-- Source: ccsu.xlsx
-- Suppliers (Reference/Master)
CREATE TABLE [landing].[CCSU] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity

    -- Source columns with documented names
    [CCSU_SUNUFO_Supplier_No] INT NULL,
    [CCSU_SUNAFO_Branch_No] VARCHAR(50) NULL,
    [CCSU_SUNOFO_Supplier_Name] VARCHAR(200) NULL,
    [CCSU_SUNOF2_Name_Line_2] VARCHAR(200) NULL,
    [CCSU_SUNOF3_Name_Line_3] VARCHAR(200) NULL,
    [CCSU_SUCLFO_Class] VARCHAR(10) NULL,
    [CCSU_SUCPFO_Country_Code] VARCHAR(10) NULL,
    [CCSU_SUADFO_Address] VARCHAR(200) NULL,
    [CCSU_SULOFO_City] VARCHAR(100) NULL,
    [CCSU_SUCAFO_Category] VARCHAR(50) NULL,
    [CCSU_SUNTFO_Phone] VARCHAR(50) NULL,
    [CCSU_SUNFFO_Fax] VARCHAR(50) NULL,
    [CCSU_SUNEFO_Email] VARCHAR(100) NULL,
    [CCSU_SUCTFO_Contact_Person] VARCHAR(100) NULL,
    [CCSU_SURSFO_Responsible_Person] VARCHAR(100) NULL,
    [CCSU_SURPPD_Reporting_Period] DECIMAL(15,2) NULL,
    [CCSU_SUCOUC_Country] VARCHAR(10) NULL,

    CONSTRAINT [PK_landing_CCSU] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCSU_timestamp] ON [landing].[CCSU]([extraction_timestamp]);
GO

-- Table: CCCR
-- Source: cccr.xlsx
-- Car Reports (Monthly Vehicle Snapshot)
CREATE TABLE [landing].[CCCR] (
    -- Metadata columns for CDC
    [extraction_id] BIGINT IDENTITY(1,1),
    [extraction_timestamp] DATETIME2 DEFAULT GETUTCDATE(),
    -- [row_hash] computed column omitted for brevity
    -- Key columns for hash: CROBNO, CRFUCT, CRMACT, CRRCCT, CRTOCT, CRFSKM, CRRPPD

    -- Source columns with documented names
    [CCCR_CROBNO_Object_No] INT NULL,
    [CCCR_CRBEAM_Book_Value_Begin_Amount] DECIMAL(15,2) NULL,
    [CCCR_CRBELT_Book_Value_Begin_LT] DECIMAL(15,2) NULL,
    [CCCR_CRDIAM_Disinvestment_Amount] DECIMAL(15,2) NULL,
    [CCCR_CRDILT_Disinvestment_LT] DECIMAL(15,2) NULL,
    [CCCR_CRGAAM_Gain_Amount] DECIMAL(15,2) NULL,
    [CCCR_CRGALT_Gain_LT] DECIMAL(15,2) NULL,
    [CCCR_CRFSBV_First_Start_Book_Value] DECIMAL(15,2) NULL,
    [CCCR_CRFSIR_First_Start_Interest_Rate] DECIMAL(15,2) NULL,
    [CCCR_CRFUCT_Fuel_Cost_Total] DECIMAL(15,2) NULL,
    [CCCR_CRMACT_Maintenance_Cost_Total] DECIMAL(15,2) NULL,
    [CCCR_CRRCCT_Replacement_Car_Cost_Total] DECIMAL(15,2) NULL,
    [CCCR_CRTRCT_Tyre_Cost_Total] DECIMAL(15,2) NULL,
    [CCCR_CRFUIV_Fuel_Invoice_Total] DECIMAL(15,2) NULL,
    [CCCR_CRMAIV_Maintenance_Invoice_Total] DECIMAL(15,2) NULL,
    [CCCR_CRRCIV_Replacement_Car_Invoice_Total] DECIMAL(15,2) NULL,
    [CCCR_CRTRIV_Tyre_Invoice_Total] DECIMAL(15,2) NULL,
    [CCCR_CRFSKM_First_Start_Km] INT NULL,
    [CCCR_CRKMCC_Odometer_Date_Century] INT NULL,
    [CCCR_CRKMYY_Odometer_Date_Year] INT NULL,
    [CCCR_CRKMMM_Odometer_Date_Month] INT NULL,
    [CCCR_CRKMDD_Odometer_Date_Day] INT NULL,
    [CCCR_CRFSIK_First_Start_Initial_Km] INT NULL,
    [CCCR_CRKMDR_Km_Driven] DECIMAL(15,2) NULL,
    [CCCR_CRMMDR_Monthly_Km_Driven] DECIMAL(15,2) NULL,
    [CCCR_CRKMTE_Km_Technical] DECIMAL(15,2) NULL,
    [CCCR_CRFUCK_Fuel_Cost_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRFUIK_Fuel_Invoice_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRRECO_Fuel_Consumption] DECIMAL(15,2) NULL,
    [CCCR_CRFUSL_Fuel_Slope] DECIMAL(15,2) NULL,
    [CCCR_CRFUMD_Fuel_Monthly_Deviation] DECIMAL(15,2) NULL,
    [CCCR_CRMACK_Maintenance_Cost_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRMAIK_Maintenance_Invoice_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRMASL_Maintenance_Slope] DECIMAL(15,2) NULL,
    [CCCR_CRMAMD_Maintenance_Monthly_Deviation] DECIMAL(15,2) NULL,
    [CCCR_CRRCCK_RC_Cost_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRRCIK_RC_Invoice_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRRCSL_RC_Slope] DECIMAL(15,2) NULL,
    [CCCR_CRRCMD_RC_Monthly_Deviation] DECIMAL(15,2) NULL,
    [CCCR_CRRCKM_RC_Km] DECIMAL(15,2) NULL,
    [CCCR_CRRCAM_RC_Amount] DECIMAL(15,2) NULL,
    [CCCR_CRTRCK_Tyre_Cost_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRTRIK_Tyre_Invoice_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRTRSL_Tyre_Slope] DECIMAL(15,2) NULL,
    [CCCR_CRTRMD_Tyre_Monthly_Deviation] DECIMAL(15,2) NULL,
    [CCCR_CRTOCT_Total_Cost] DECIMAL(15,2) NULL,
    [CCCR_CRTOIV_Total_Invoiced] DECIMAL(15,2) NULL,
    [CCCR_CRCTKM_Cost_Per_Km] DECIMAL(15,2) NULL,
    [CCCR_CRTOSU_Total_Surplus] DECIMAL(15,2) NULL,
    [CCCR_CRTOSA_Total_Surplus_Absolute] DECIMAL(15,2) NULL,
    [CCCR_CRTOTO_Total_Total] DECIMAL(15,2) NULL,
    [CCCR_CRLWKM_Last_Week_Km] DECIMAL(15,2) NULL,
    [CCCR_CRUPKM_Update_Km] DECIMAL(15,2) NULL,
    [CCCR_CRMANR_Maintenance_Count] INT NULL,
    [CCCR_CRFUNR_Fuel_Count] INT NULL,
    [CCCR_CRRCNR_RC_Count] INT NULL,
    [CCCR_CRTRNR_Tyre_Count] INT NULL,
    [CCCR_CRTNNR_Tyre_New_Count] INT NULL,
    [CCCR_CRTWNR_Tyre_Winter_Count] INT NULL,
    [CCCR_CRPRKT_Private_Km_Pct] DECIMAL(15,2) NULL,
    [CCCR_CRDANB_Damage_Count] INT NULL,
    [CCCR_CRDARE_Damage_Reserve] DECIMAL(15,2) NULL,
    [CCCR_CRSG01_Segment_01] DECIMAL(15,2) NULL,
    [CCCR_CRSG02_Segment_02] DECIMAL(15,2) NULL,
    [CCCR_CRSG03_Segment_03] DECIMAL(15,2) NULL,
    [CCCR_CRSG04_Segment_04] DECIMAL(15,2) NULL,
    [CCCR_CRSG05_Segment_05] DECIMAL(15,2) NULL,
    [CCCR_CRSG06_Segment_06] DECIMAL(15,2) NULL,
    [CCCR_CRSG07_Segment_07] DECIMAL(15,2) NULL,
    [CCCR_CRSG08_Segment_08] DECIMAL(15,2) NULL,
    [CCCR_CRSG09_Segment_09] DECIMAL(15,2) NULL,
    [CCCR_CRSG10_Segment_10] DECIMAL(15,2) NULL,
    [CCCR_CRSG11_Segment_11] DECIMAL(15,2) NULL,
    [CCCR_CRSG12_Segment_12] DECIMAL(15,2) NULL,
    [CCCR_CRSG13_Segment_13] DECIMAL(15,2) NULL,
    [CCCR_CRSG14_Segment_14] DECIMAL(15,2) NULL,
    [CCCR_CRSG15_Segment_15] DECIMAL(15,2) NULL,
    [CCCR_CRRPPD_Reporting_Period] INT NULL,
    [CCCR_CRCOUC_Country_Code] VARCHAR(10) NULL,
    [CCCR_CRVOLC_Volume_Code] VARCHAR(10) NULL,
    [CCCR_CRDISC_Distance_Code] VARCHAR(10) NULL,
    [CCCR_CRCONC_Consumption_Code] VARCHAR(10) NULL,
    [CCCR_CRCURC_Currency_Code] VARCHAR(10) NULL,
    [CCCR_CRMIAM_Misc_Insurance_Amount] DECIMAL(15,2) NULL,
    [CCCR_CRMIPY_Misc_Insurance_PerYear] DECIMAL(15,2) NULL,
    [CCCR_CRMIRN_Misc_Insurance_Run_No] DECIMAL(15,2) NULL,
    [CCCR_CRTSAM_Misc_TS_Amount] DECIMAL(15,2) NULL,
    [CCCR_CRTSPY_Misc_TS_PerYear] DECIMAL(15,2) NULL,
    [CCCR_CRTSRN_Misc_TS_Run_No] DECIMAL(15,2) NULL,
    [CCCR_CRTRNO_Traffic_Fines_No] INT NULL,
    [CCCR_CRPACT_Parking_Cost_Total] DECIMAL(15,2) NULL,
    [CCCR_CRPAIV_Parking_Invoice_Total] DECIMAL(15,2) NULL,
    [CCCR_CRPAMD_Parking_Monthly_Deviation] DECIMAL(15,2) NULL,
    [CCCR_CRPASL_Parking_Slope] DECIMAL(15,2) NULL,
    [CCCR_CRUNCT_Unspecified_Cost_Total] DECIMAL(15,2) NULL,
    [CCCR_CRUNIV_Unspecified_Invoice_Total] DECIMAL(15,2) NULL,
    [CCCR_CRUNMD_Unspecified_Monthly_Deviation] DECIMAL(15,2) NULL,
    [CCCR_CRUNSL_Unspecified_Slope] DECIMAL(15,2) NULL,
    [CCCR_CRWACT_Warranty_Cost_Total] DECIMAL(15,2) NULL,
    [CCCR_CRWAIV_Warranty_Invoice_Total] DECIMAL(15,2) NULL,
    [CCCR_CRWAMD_Warranty_Monthly_Deviation] DECIMAL(15,2) NULL,
    [CCCR_CRWASL_Warranty_Slope] DECIMAL(15,2) NULL,

    CONSTRAINT [PK_landing_CCCR] PRIMARY KEY ([extraction_id])
);

CREATE INDEX [IX_landing_CCCR_timestamp] ON [landing].[CCCR]([extraction_timestamp]);
GO
