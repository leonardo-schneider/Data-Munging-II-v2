

from duckdb_client import DuckDBClient



BASE_URL = "https://duckdb.straddlyze.com"
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ"

client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)
print("Starting Data Transformation (JOIN) to Remote DuckDB...")


FINAL_TABLE = "DAILY_ENERGY_EMISSIONS"

REGION_COLUMN = "region" 




# ========================================
# 3. TRANSFORM (UNION e JOIN)
# ========================================



# sql_transform = f"""

#     CREATE TABLE {FINAL_TABLE} AS


#     WITH unified_daily AS (
#         SELECT * FROM CAL_DAILY_DATA
#         UNION ALL
#         SELECT * FROM FL_DAILY_DATA
#         UNION ALL
#         SELECT * FROM NY_DAILY_DATA
#     )
    
#     -- 2. Selecionar os campos e fazer o JOIN com a tabela de emissões
#     SELECT 
#         ud.*,
#         e.carbon_intensity_direct,
#         e.carbon_intensity_lifecycle,
#         e.cfe_pct,
#         e.re_pct
#     FROM unified_daily AS ud
#     -- O JOIN é feito pela coluna de data (agora usando CAST(e.datetime_utc AS DATE)) e pela região
#     LEFT JOIN EMISSIONS_SOURCE_DAILY AS e
#         ON ud.date = CAST(e.datetime_utc AS DATE) AND ud.{REGION_COLUMN} = e.region;
# """

# client.execute(sql_transform)
    


# ========================================
# 4. FINAL VERIFICATION
# ========================================



print(f"\n█ 3. Verification of Final Table: {FINAL_TABLE} █")


sql_count = f"SELECT COUNT(*) as total_rows FROM {FINAL_TABLE}"
total_rows = client.query(sql_count).iloc[0]['total_rows']

sql_check = f"""
    SELECT
        {REGION_COLUMN},
        COUNT(*) as record_count,
        ROUND(AVG(carbon_intensity_direct), 2) as avg_carbon
    FROM {FINAL_TABLE}
    GROUP BY {REGION_COLUMN}
    ORDER BY record_count DESC;
"""
results = client.query(sql_check)
print(results)

sql_sample = f"SELECT date, region, temp_mean, carbon_intensity_direct, re_pct FROM {FINAL_TABLE} LIMIT 5"
sample = client.query(sql_sample)
print(sample)

