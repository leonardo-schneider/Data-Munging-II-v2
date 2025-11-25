from duckdb_client import DuckDBClient
import pandas as pd

# --- CONFIGURAÇÃO ---
BASE_URL = "https://duckdb.straddlyze.com"
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ" # Use o seu token real
FINAL_TABLE = "DAILY_ENERGY_EMISSIONS"
SALES = "SALES_BY_STATE_YTD"
CONS = "eletricity_consumption"
GLOBAL = "GLOBAL_CONSUMPTION"

# 1. Inicializa o cliente
client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)

# ----------------------------------------------------
# A. CHECAR STATUS GERAL (TODAS AS TABELAS DISPONÍVEIS)
# ----------------------------------------------------
import pandas as pd
import plotly.express as px


df = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/EDS/by_sector_by_state.csv', header=3)


df_clean = df.iloc[:, [0, 1, 3, 5]].copy()

# Renomeando para facilitar
df_clean.columns = ['State', 'Residential', 'Commercial', 'Industrial']

# 3. FILTRAGEM E LIMPEZA
# Removemos espaços em branco extras dos nomes
df_clean['State'] = df_clean['State'].astype(str).str.strip()

# Filtramos apenas os estados de interesse
estados_alvo = ['New York', 'California', 'Florida']
df_final = df_clean[df_clean['State'].isin(estados_alvo)].copy()

# Garantimos que os números sejam lidos como números (removendo vírgulas de milhar se houver)
cols_numericas = ['Residential', 'Commercial', 'Industrial']
for col in cols_numericas:
    df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace(',', ''), errors='coerce')

# 4. TRANSFORMAÇÃO PARA O PLOT (Wide -> Long)
# O Plotly precisa dos dados "empilhados" para fazer o agrupamento por cor
df_long = df_final.melt(id_vars='State', 
                        value_vars=cols_numericas, 
                        var_name='Sector', 
                        value_name='Consumption (Thousand MWh)')

# 5. GERAR O GRÁFICO
fig = px.bar(df_long, 
             x='State', 
             y='Consumption (Thousand MWh)', 
             color='Sector', 
             barmode='group', # Barras lado a lado para comparar
             title='State-Level Consumption Profile by Sector (August 2025 YTD)',
             template='plotly_white',
             text_auto='.2s') # Mostra o valor em cima da barra formatado

fig.show()


