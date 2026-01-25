"""
Generate self-documenting landing schema from data dictionary.
Reads NOLS-Myfleet files.txt and creates column names with descriptions.
"""

import re
from pathlib import Path

# Paths
DICT_PATH = r"C:\Users\X1Carbon\Documents\FleetAI\NOLS-Myfleet files.txt"
SCHEMA_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\schemas\01_landing.sql"
OUTPUT_PATH = r"C:\Users\X1Carbon\Documents\Projects\FleetAI\database\schemas\01_landing_documented.sql"

# Field reference definitions extracted from data dictionary
# Format: field_code -> (description, data_type_hint)
FIELD_DEFINITIONS = {}


def parse_data_dictionary(dict_path):
    """Parse the data dictionary to extract field definitions."""
    with open(dict_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    # Pattern to match field definitions with TEXT descriptions
    # Example: A            OBNO           9  0       TEXT('Object number')
    pattern = r"^\s*(?:[\w\d]+\s+)?A\s+(\w+)\s+[\d\s]+(?:\d)?\s+TEXT\('([^']+)'\)"

    for match in re.finditer(pattern, content, re.MULTILINE):
        field_code = match.group(1).strip()
        description = match.group(2).strip()
        # Clean up description
        description = description.replace('+', '').strip()
        description = re.sub(r'\s+', ' ', description)
        FIELD_DEFINITIONS[field_code] = description

    print(f"Parsed {len(FIELD_DEFINITIONS)} field definitions from data dictionary")
    return FIELD_DEFINITIONS


def get_description(field_code, table_prefix):
    """Get description for a field, trying various lookup strategies."""
    # Remove table prefix to get base field code
    # e.g., CCOB_OBOBNO -> OBOBNO, then try OBNO
    if field_code.startswith(table_prefix + '_'):
        base_code = field_code[len(table_prefix) + 1:]
    else:
        base_code = field_code

    # Try exact match first
    if base_code in FIELD_DEFINITIONS:
        return FIELD_DEFINITIONS[base_code]

    # Try without OB prefix (common in CCOB)
    if base_code.startswith('OB') and base_code[2:] in FIELD_DEFINITIONS:
        return FIELD_DEFINITIONS[base_code[2:]]

    # Try without first 2 chars
    if len(base_code) > 2 and base_code[2:] in FIELD_DEFINITIONS:
        return FIELD_DEFINITIONS[base_code[2:]]

    # Common pattern mappings
    common_suffixes = {
        'RPPD': 'Reporting_Period',
        'COUC': 'Country_Code',
        'CURC': 'Currency_Code',
    }

    for suffix, desc in common_suffixes.items():
        if base_code.endswith(suffix):
            return desc

    # Manual mappings for fields not in dictionary
    manual_mappings = {
        # CCAU - Automobiles
        'AUENAV': 'Energy_Average',
        'AUEUR': 'Euro_Standard',
        'AUCAT': 'Category',
        'AUCO2': 'CO2_Emission',
        # CCBI - Billing
        'BIBIA1': 'Billing_Amount_1',
        'BIBIA2': 'Billing_Amount_2',
        'BIBIAD': 'Billing_Address',
        'BIBIAM': 'Billing_Amount_Monthly',
        'BIBIAO': 'Billing_Account_Owner',
        'BIBILA': 'Billing_Language',
        'BIBILO': 'Billing_Locality',
        'BIBINM': 'Billing_Name',
        'BIBINO': 'Billing_Number',
        'BIBISD': 'Billing_Start_Day',
        'BIBISM': 'Billing_Start_Month',
        'BIBISQ': 'Billing_Sequence',
        'BIBISY': 'Billing_Start_Year',
        'BIBITT': 'Billing_Total',
        'BIBIT1': 'Billing_Item_1',
        'BIBIT2': 'Billing_Item_2',
        'BIBIPL': 'Billing_Period_Length',
        'BIBIRN': 'Billing_Run_Number',
        'BIBICM': 'Billing_Code_Method',
        'BIBIAV': 'Billing_Amount_Variable',
        'BIOBNO': 'Object_Number',
        # CCCA - Cards
        'CACANO': 'Card_Number',
        'CAOBNO': 'Object_Number',
        # CCCP - Contract Positions
        'CPGNLO': 'Group_Name',
        'CPMRAA': 'Monthly_Rate_Admin',
        'CPMRAF': 'Monthly_Rate_Fuel',
        'CPMRDP': 'Monthly_Rate_Depreciation',
        'CPMRIN': 'Monthly_Rate_Insurance',
        'CPMRIR': 'Monthly_Rate_Interest',
        'CPMRMF': 'Monthly_Rate_Maintenance',
        'CPMRRA': 'Monthly_Rate_Replacement',
        'CPMRRO': 'Monthly_Rate_Road_Tax',
        'CPMRVI': 'Monthly_Rate_Tires',
        'CPMTCT': 'Monthly_Total_Cost',
        # CCGD - General Data/Odometer
        'MAGDKM': 'Mileage_Km',
        'MAGDAM': 'Mileage_Amount',
        'MAGDDS': 'Mileage_Description',
        'MAGDSC': 'Mileage_Source_Code',
        'MAGDSQ': 'Mileage_Sequence',
        'MAOBNO': 'Object_Number',
        # CCGR - Groups
        'GRGNLO': 'Group_Name',
        'GRNKLO': 'Group_Customer_Number',
        'GRNOLO': 'Group_Description',
        # CCOR - Orders
        'ORORSC': 'Order_Status_Code',
        'ORCPNO': 'Contract_Position_Number',
        'ORGNLO': 'Group_Name',
        'ORCUNO': 'Customer_Number',
        # CCOS - Object Sales
        'OSOBNO': 'Object_Number',
        'OSSLST': 'Sales_Status',
        'OSSLAM': 'Sales_Amount',
        # Generic patterns
        'OBNO': 'Object_Number',
        'CPNO': 'Contract_Position_Number',
        'CUNO': 'Customer_Number',
        'PCNO': 'Profit_Center_Number',
        'GRNO': 'Group_Number',
        'ORNO': 'Order_Number',
        'CONO': 'Contract_Number',
        'NUFO': 'Supplier_Number',
        'NAFO': 'Supplier_Reference',
    }

    if base_code in manual_mappings:
        return manual_mappings[base_code]

    # Try with common prefixes removed
    for prefix in ['AU', 'BI', 'CA', 'CO', 'CP', 'CU', 'DA', 'DR', 'FC', 'FP', 'GD', 'GR', 'IN', 'IO', 'OB', 'OR', 'OS', 'PC', 'RS', 'XC']:
        if base_code.startswith(prefix) and base_code[len(prefix):] in manual_mappings:
            return manual_mappings[base_code[len(prefix):]]

    return None


def clean_description(desc):
    """Convert description to valid column name suffix."""
    if not desc:
        return None
    # Remove special characters, replace spaces with underscores
    clean = re.sub(r'[^a-zA-Z0-9\s]', '', desc)
    clean = re.sub(r'\s+', '_', clean.strip())
    # Title case
    clean = '_'.join(word.capitalize() for word in clean.split('_'))
    # Limit length
    if len(clean) > 40:
        clean = clean[:40]
    return clean


def process_schema(schema_path, output_path):
    """Process the landing schema and add descriptions to column names."""
    with open(schema_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Track statistics
    stats = {'updated': 0, 'not_found': 0, 'tables': set()}
    not_found_fields = []

    # Pattern to match column definitions
    # [CCOB_OBOBNO] INT NULL,
    col_pattern = r'\[([A-Z]{4}_\w+)\](\s+(?:INT|VARCHAR|DECIMAL|TEXT|REAL|BIGINT)[^\]]*NULL)'

    def replace_column(match):
        old_name = match.group(1)
        data_type = match.group(2)

        # Extract table prefix
        parts = old_name.split('_')
        if len(parts) >= 2:
            table_prefix = parts[0]
            stats['tables'].add(table_prefix)
        else:
            return match.group(0)

        # Skip if already has description (more than 2 parts separated by underscore after prefix)
        remaining = '_'.join(parts[1:])
        if len(remaining.split('_')) > 1 and any(c.islower() for c in remaining):
            return match.group(0)

        # Get description
        desc = get_description(old_name, table_prefix)
        if desc:
            clean_desc = clean_description(desc)
            if clean_desc:
                new_name = f"{old_name}_{clean_desc}"
                stats['updated'] += 1
                return f'[{new_name}]{data_type}'

        stats['not_found'] += 1
        not_found_fields.append(old_name)
        return match.group(0)

    # Process content
    new_content = re.sub(col_pattern, replace_column, content)

    # Also update the row_hash computed columns
    # This is more complex - for now, we'll note that they need manual update

    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(new_content)

    print(f"\nSchema Processing Results:")
    print(f"  Tables processed: {len(stats['tables'])}")
    print(f"  Columns updated: {stats['updated']}")
    print(f"  Columns without description: {stats['not_found']}")

    if not_found_fields:
        print(f"\nFields without descriptions (sample of first 20):")
        for field in not_found_fields[:20]:
            print(f"    {field}")

    return stats


def main():
    print("=" * 60)
    print("Landing Schema Documentation Generator")
    print("=" * 60)

    # Parse data dictionary
    print(f"\nReading data dictionary: {DICT_PATH}")
    parse_data_dictionary(DICT_PATH)

    # Process schema
    print(f"\nProcessing schema: {SCHEMA_PATH}")
    print(f"Output: {OUTPUT_PATH}")
    stats = process_schema(SCHEMA_PATH, OUTPUT_PATH)

    print(f"\nDone! Review the output file and copy to 01_landing.sql when ready.")


if __name__ == "__main__":
    main()
