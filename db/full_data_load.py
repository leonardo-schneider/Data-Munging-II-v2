# full_data_load.py (Complete Data Loading Script)
#
# NOTE: Please ensure the BASE_URL is the current, active address.
# This script loads data from GitHub (Raw URLs) into the remote DuckDB.

from duckdb_client import DuckDBClient

# ============================================
# 1. CONFIGURAÇÃO (EDITAR AQUI!)
# ============================================

BASE_URL = "https://duckdb.straddlyze.com"
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ" 

# --- CONFIGURAÇÃO DO GITHUB ---
# Preencha com seus dados exatos do GitHub onde os arquivos estão:
GITHUB_USER = "leonardo-schneider"     # Ex: "leona-dev"
REPO_NAME = "Data-Munging-II-v2"           # Ex: "energy-project"
BRANCH = "main"                      # Geralmente "main" ou "master"

# Onde estão os arquivos no GitHub?
PASTA_CSV = "EDS"       # Pasta onde você colocou os CSVs
PASTA_PARQUETS = ""     # Deixe vazio "" se os parquets estiverem soltos na raiz do repo

# ============================================
# FUNÇÃO AUXILIAR PARA GERAR LINKS
# ============================================
def get_url(filename, folder=""):
    base = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/{BRANCH}"
    if folder:
        base += f"/{folder}"
    return f"'{base}/{filename}'"

# ============================================
# INÍCIO DO PROCESSO
# ============================================

client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)
print("Starting Complete Data Load to Remote DuckDB...")

# ========================================
# 2. CLEANUP (DROP)
# ========================================

print("\n█ 1. Cleanup (DROP) Existing Tables █")
tables_to_drop = [
    "CAL_DAILY_DATA",
    "FL_DAILY_DATA",
    "NY_DAILY_DATA", # Adicionei NY
    "REVENUE_EXPENSE_STATS",
    "NET_GENERATION_US",
    "GLOBAL_CONSUMPTION",
    "SUMMARY_CUSTOMER_STATS",
    "SALES_BY_STATE_YTD",
    # Removidos pois não temos os arquivos:
    "FUEL_CONSUMPTION_ANNUAL",
    "FUEL_CONSUMPTION_MONTHLY",
    "GENERATING_UNITS_NEW",
    "SALES_BY_PROVIDER"
]

for table_name in tables_to_drop:
    client.execute(f"DROP TABLE IF EXISTS {table_name}")

# ========================================
# 3. LOAD PARQUET DATA (Time-Series)
# ========================================

print("\n█ 2. Loading Parquet Data (Time-Series) █")

# Lista exata dos seus arquivos (baseado no que você enviou)
files_cal = ['cal_2021.parquet', 'cal_2022_2023.parquet', 'cal_2024.parquet']
files_fl  = ['fl_2021.parquet', 'fl_2022.parquet', 'fl_2023.parquet', 'fl_2024.parquet']
files_ny  = ['ny_2021.parquet', 'ny_2022.parquet', 'ny_2023.parquet']

# Gera as URLs formatadas para o SQL
urls_cal = ", ".join([get_url(f, PASTA_PARQUETS) for f in files_cal])
urls_fl  = ", ".join([get_url(f, PASTA_PARQUETS) for f in files_fl])
urls_ny  = ", ".join([get_url(f, PASTA_PARQUETS) for f in files_ny])

# 3.1 California
print(f"Creating CAL_DAILY_DATA ({len(files_cal)} files)...")
client.execute(f"""
    CREATE TABLE CAL_DAILY_DATA AS
    SELECT * FROM read_parquet([{urls_cal}])
""")

# 3.2 Florida
print(f"Creating FL_DAILY_DATA ({len(files_fl)} files)...")
client.execute(f"""
    CREATE TABLE FL_DAILY_DATA AS
    SELECT * FROM read_parquet([{urls_fl}])
""")

# 3.3 New York (Extra que você enviou)
print(f"Creating NY_DAILY_DATA ({len(files_ny)} files)...")
client.execute(f"""
    CREATE TABLE NY_DAILY_DATA AS
    SELECT * FROM read_parquet([{urls_ny}])
""")

# ========================================
# 4. LOAD CSV DATA (Energy/Infrastructure)
# ========================================

print("\n█ 3. Loading CSV Data (From EDS folder) █")

# 4.1 Revenue Expense
print("Creating REVENUE_EXPENSE_STATS...")
client.execute(f"""
    CREATE TABLE REVENUE_EXPENSE_STATS AS
    SELECT * FROM read_csv_auto({get_url('revenue_expense.csv', PASTA_CSV)})
""")

# 4.2 US Net Generation
print("Creating NET_GENERATION_US...")
client.execute(f"""
    CREATE TABLE NET_GENERATION_US AS
    SELECT * FROM read_csv_auto({get_url('Net_generation_for_all_sectors.csv', PASTA_CSV)})
""")

# 4.3 Global Consumption
print("Creating GLOBAL_CONSUMPTION...")
client.execute(f"""
    CREATE TABLE GLOBAL_CONSUMPTION AS
    SELECT * FROM read_csv_auto({get_url('world_net_consumption.csv', PASTA_CSV)})
""")

# 4.4 Summary Customer Stats (summary_2014.csv)
print("Creating SUMMARY_CUSTOMER_STATS...")
client.execute(f"""
    CREATE TABLE SUMMARY_CUSTOMER_STATS AS
    SELECT * FROM read_csv_auto({get_url('summary_2014.csv', PASTA_CSV)})
""")

# 4.5 Sales by State (by_sector_by_state.csv)
print("Creating SALES_BY_STATE_YTD...")
client.execute(f"""
    CREATE TABLE SALES_BY_STATE_YTD AS
    SELECT * FROM read_csv_auto({get_url('by_sector_by_state.csv', PASTA_CSV)})
""")


# ========================================
# 5. FINAL VERIFICATION
# ========================================

print("\n█ 4. Verification of Load █")
info = client.get_info()
print(f"Total tables now: {info['table_count']}")
for table in info['tables']:
    print(f"  - {table['table']:25s} {table['rows']:>10,} rows")

print("\n✅ Data loading process completed successfully!")