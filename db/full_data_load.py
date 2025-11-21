# full_data_load.py (Complete Data Loading Script)
#
# NOTE: Please ensure the BASE_URL is the current, active address for the remote DuckDB server.
#
# This script performs a complete load (DROP and CREATE) for all
# CSV and Parquet data files into the remote DuckDB database.

from duckdb_client import DuckDBClient


BASE_URL = "https://duckdb.straddlyze.com/" 
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ"

client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)
print("Starting Complete Data Load to Remote DuckDB...")

# ========================================
# 1. CLEANUP (DROP)
# Drop all existing data tables to ensure a clean slate before recreation.
# ========================================

print("\n█ 1. Cleanup (DROP) Existing Tables █")
tables_to_drop = [
    # Parquet Data (Weather/Time-Series - Assuming cal_*.parquet and fl_*.parquet exist)
    "CAL_DAILY_DATA",
    "FL_DAILY_DATA",
    # CSV Data (Energy/Infrastructure)
    "REVENUE_EXPENSE_STATS",
    "FUEL_CONSUMPTION_ANNUAL",
    "FUEL_CONSUMPTION_MONTHLY",
    "GENERATING_UNITS_NEW",
    "NET_GENERATION_US",
    "GLOBAL_CONSUMPTION",
    "SUMMARY_CUSTOMER_STATS",
    "SALES_BY_PROVIDER",
    "SALES_BY_STATE_YTD"
]

for table_name in tables_to_drop:
    # Sends a DROP TABLE command to the remote server
    client.execute(f"DROP TABLE IF EXISTS {table_name}")

# ========================================
# 2. CREATE AND LOAD DATA (CREATE TABLE AS SELECT)
# ========================================

print("\n█ 2. Creating (CREATE) and Loading Data █")

### A) HOURLY/DAILY DATA (PARQUET FILES)
print("Loading Daily/Time-Series Data (Parquet)...")

# 2.1 California (CAL) - Uses Globbing to load all 'cal_*.parquet' files (e.g., 2021, 2022_2023)
client.execute("""
    CREATE TABLE CAL_DAILY_DATA AS
    SELECT *
    FROM read_parquet('cal_*.parquet')
""")

# 2.2 Florida (FL) - Uses Globbing to load all 'fl_*.parquet' files
client.execute("""
    CREATE TABLE FL_DAILY_DATA AS
    SELECT *
    FROM read_parquet('fl_*.parquet')
""")

### B) ENERGY AND INFRASTRUCTURE DATA (CSV FILES)
print("Loading Energy and Infrastructure Data (CSV)...")

# 2.3 Revenue and Expense Financial Data
client.execute("""
    CREATE TABLE REVENUE_EXPENSE_STATS AS
    SELECT *
    FROM read_csv_auto('revenue_expense.xlsx - epa_08_03.csv')
""")

# 2.4 Annual Fuel Consumption
client.execute("""
    CREATE TABLE FUEL_CONSUMPTION_ANNUAL AS
    SELECT *
    FROM read_csv_auto('Table_7.3a_Consumption_of_Combustible_Fuels_for_Electricity_Generation__Total_(All_Sectors).xlsx - Annual Data.csv')
""")

# 2.5 Monthly Fuel Consumption
client.execute("""
    CREATE TABLE FUEL_CONSUMPTION_MONTHLY AS
    SELECT *
    FROM read_csv_auto('Table_7.3a_Consumption_of_Combustible_Fuels_for_Electricity_Generation__Total_(All_Sectors).xlsx - Monthly Data.csv')
""")

# 2.6 New Generating Units by Company (Infrastructure Growth)
client.execute("""
    CREATE TABLE GENERATING_UNITS_NEW AS
    SELECT *
    FROM read_csv_auto('Generating Units by Operating Company.xlsx - Table_6_03.csv')
""")

# 2.7 US Net Generation by Source
client.execute("""
    CREATE TABLE NET_GENERATION_US AS
    SELECT *
    FROM read_csv_auto('Net_generation_for_all_sectors.csv')
""")

# 2.8 Global Net Consumption
client.execute("""
    CREATE TABLE GLOBAL_CONSUMPTION AS
    SELECT *
    FROM read_csv_auto('world_net_consumption.csv')
""")

# 2.9 Summary Customer Statistics (Number of Customers)
client.execute("""
    CREATE TABLE SUMMARY_CUSTOMER_STATS AS
    SELECT *
    FROM read_csv_auto('summary_2014_2024.xlsx - epa_01_02.csv')
""")

# 2.10 Sales by Sector and Provider
client.execute("""
    CREATE TABLE SALES_BY_PROVIDER AS
    SELECT *
    FROM read_csv_auto('by_sector_by_provider.xlsx - epa_02_02.csv')
""")

# 2.11 Year-to-Date Sales by Sector and State
client.execute("""
    CREATE TABLE SALES_BY_STATE_YTD AS
    SELECT *
    FROM read_csv_auto('by_sector_by_state.xlsx - Table_5_04_B.csv')
""")


# ========================================
# 3. FINAL VERIFICATION (READ)
# ========================================

print("\n█ 3. Verification of Load █")
# client.get_info() calls the API to list current tables and row counts
info = client.get_info()
print(f"Total tables now: {info['table_count']}")
for table in info['tables']:
    print(f"  - {table['table']:25s} {table['rows']:>10,} rows")

print("\n✅ Data loading process completed successfully!")