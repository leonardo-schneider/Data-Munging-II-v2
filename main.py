#%%
import pandas as pd
daily_2024 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2024_daily.csv')
hourly_2024 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2024_hourly.csv')
monthly_2024 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2024_monthly.csv')
yearly_2024 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2024_yearly.csv')

daily_2023 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2023_daily.csv')
hourly_2023 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2023_hourly.csv')
monthly_2023 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2023_monthly.csv')
yearly_2023 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2023_yearly.csv')

daily_2022 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2022_daily.csv')
hourly_2022 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2022_hourly.csv')
monthly_2022 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2022_monthly.csv')
yearly_2022 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2022_yearly.csv')

daily_2021 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2021_daily.csv')
hourly_2021 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2021_hourly.csv')
monthly_2021 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2021_monthly.csv')
yearly_2021 = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/US-FLA-FPL_2021_yearly.csv')

#%%
daily_2021

#%%
import pandas as pd

# Dicionário pra facilitar
files = {
    "daily": [daily_2021, daily_2022, daily_2023, daily_2024],
    "hourly": [hourly_2021, hourly_2022, hourly_2023, hourly_2024],
    "monthly": [monthly_2021, monthly_2022, monthly_2023, monthly_2024],
    "yearly": [yearly_2021, yearly_2022, yearly_2023, yearly_2024]
}

# Concatenar todos os anos por tipo
combined = {}
for name, dfs in files.items():
    combined[name] = pd.concat(dfs, ignore_index=True)
    print(f"{name} shape:", combined[name].shape)



#%%
combined["daily"]["year"] = pd.to_datetime(combined["daily"]["Datetime (UTC)"]).dt.year

#%%
for name, df in combined.items():
    df.to_csv(f"C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/cleaned/{name}_2021_2024.csv", index=False)

#%%

import pandas as pd

df = pd.read_parquet(
    r"C:\Users\leona\Desktop\Masters\Data Mungin\Second group Project\data\granularity=hourly\zone=US-FLA-FPL\year=2022\data.parquet",
    engine="pyarrow"
)
print(df.head())

#%%
import pandas as pd
from pathlib import Path

# base folder for California (with / instead of \)
base_path = Path("C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/NY")

years = [2021, 2022, 2023, 2024]
freqs = ["daily", "monthly", "yearly", "hourly"]

# dictionaries to store the final dataframes
ca_data = {}

for freq in freqs:
    frames = []
    for year in years:
        file_path = base_path / f"US-NY-NYIS_{year}_{freq}.csv"
        print(f"Reading: {file_path}")
        df = pd.read_csv(file_path)
        
        # (optional) add metadata columns
        df["year"] = year
        df["frequency"] = freq
        df["region"] = "US-NY-NYIS"
        
        frames.append(df)
    
    # concatenate all years for this frequency
    ca_data[freq] = pd.concat(frames, ignore_index=True)

# now you have:
# ca_data["daily"], ca_data["monthly"], ca_data["yearly"], ca_data["hourly"]

# if you prefer direct variables, you can also do:
ca_daily = ca_data["daily"]
ca_monthly = ca_data["monthly"]
ca_yearly = ca_data["yearly"]
ca_hourly = ca_data["hourly"]

#%%
import pandas as pd
from pathlib import Path

base_path = Path("C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/NY")

years = [2021, 2022, 2023, 2024]
freqs = ["daily", "monthly", "yearly", "hourly"]

cleaned_path = base_path / "cleaned"
cleaned_path.mkdir(exist_ok=True)

for freq in freqs:
    frames = []
    for year in years:
        file_path = base_path / f"US-NY-NYIS_{year}_{freq}.csv"
        print(f"Reading: {file_path}")
        df = pd.read_csv(file_path)
        df["year"] = year
        df["frequency"] = freq
        df["region"] = "US-NY-NYIS"
        frames.append(df)

    final_df = pd.concat(frames, ignore_index=True)
    final_df.to_csv(cleaned_path / f"{freq}.csv", index=False)
    print(f"Saved: {cleaned_path/f'{freq}.csv'}")


#%%


# 1. Base folder 
BASE_DIR = Path("C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets")


regions = {
    "US-FLA-FPL": "FL",
    "US-CAL-CISO": "CA",
    "US-NY-NYIS": "NY",
}

frequencies = ["hourly", "daily", "monthly", "yearly"]


rename_map = {
    "Datetime (UTC)": "datetime_utc",
    "Country": "country",
    "Zone name": "zone_name",
    "Zone id": "zone_id",
    "Carbon intensity gCO₂eq/kWh (direct)": "carbon_intensity_direct",
    "Carbon intensity gCO₂eq/kWh (Life cycle)": "carbon_intensity_lifecycle",
    "Carbon-free energy percentage (CFE%)": "cfe_pct",
    "Renewable energy percentage (RE%)": "re_pct",
    "Data source": "data_source",
    "Data estimated": "data_estimated",
    "Data estimation method": "data_estimation_method",
}


output_dir = BASE_DIR / "big"
output_dir.mkdir(exist_ok=True)

big_dfs = {}

for freq in frequencies:
    frames = []
    for region_id, folder in regions.items():
        file_path = BASE_DIR / folder / "cleaned" / f"{freq}.csv"
        print(f"Reading: {file_path}")
        df = pd.read_csv(file_path)

        
        df = df.rename(columns=rename_map)


        if "region" not in df.columns:
            df["region"] = region_id

        
        if "frequency" not in df.columns:
            df["frequency"] = freq

    
        if "year" not in df.columns:
        
            df["year"] = pd.to_datetime(df["datetime_utc"]).dt.year

        frames.append(df)


    big_df = pd.concat(frames, ignore_index=True)


    out_path = output_dir / f"big_{freq}.parquet"
    big_df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path}")

    big_dfs[freq] = big_df

print("Done")

#%%
import pandas as pd
daily = pd.read_parquet('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/datasets/big/big_monthly.parquet')
daily.head
#%%

import pandas as pd

df = pd.read_csv("/Users/muhammedaltindal/Data-Munging-II-v2-2/weather_2021-01-01_2021-03-31.csv")
df
# %%
import pandas as pd
df = pd.read_parquet("datasets\big\big_daily.parquet")
df.head(), df.tail(), df.shape

#%%
import pandas as pd

df = pd.read_parquet("weather_daily.parquet")

# drop the bad helper column
if "0" in df.columns:
    df = df.drop(columns=["0"])

# drop the broken first row (where date is NaN)
df = df.dropna(subset=["date"])
#%%
import pandas as pd
df = pd.read_parquet("cal_2021.parquet")
print(df.head(), df.tail(), df.shape)

#%%
import pandas as pd

# Carregar os três arquivos Parquet
df1 = pd.read_parquet('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/jan_oct_2022.parquet')
df2 = pd.read_parquet('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/weather_daily.parquet')
# df3 = pd.read_parquet('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/dec_2021.parquet')

# Concatenar os DataFrames
df_completo = pd.concat([df1, df2], ignore_index=True)

# Opcional: salvar o resultado em um novo arquivo Parquet
df_completo.to_parquet('fl_2022.parquet')
#%%
df_provider = pd.read_excel(
    "C:\\Users\\leona\\Desktop\\Masters\\Data Mungin\\Second group Project\\EDS\\by_sector_by_provider.xlsx"
)
print(df_provider.head(), df_provider.tail(), df_provider.shape)
#%%
import pandas as pd

path = r"C:\Users\leona\Desktop\Masters\Data Mungin\Second group Project\EDS\by_sector_by_provider.xlsx"

# Usa a segunda linha como cabeçalho (índice 1)
df_provider = pd.read_excel(path, header=1, skiprows=1)


print(df_provider.head(), df_provider.tail(), df_provider.shape)



#%%
import pandas as pd

# Load the Excel file
df = pd.read_excel("C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/EDS/summary_2014_2024.xlsx")

# Save as CSV
df.to_csv("summary_2014.csv", index=False)
#%%
import plotly.express as px
import pandas as pd
df = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/EDS/by_sector_by_state.csv', header=3)


df_clean = df.iloc[:, [0, 1, 3, 5]].copy()

#renamed
df_clean.columns = ['State', 'Residential', 'Commercial', 'Industrial']


df_clean['State'] = df_clean['State'].astype(str).str.strip()


target_states = ['New York', 'California', 'Florida']
df_final = df_clean[df_clean['State'].isin(target_states)].copy()


cols_numerics = ['Residential', 'Commercial', 'Industrial']
for col in cols_numerics:
    df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace(',', ''), errors='coerce')

# Tranformation for PLOT (Wide -> Long)

df_long = df_final.melt(id_vars='State', 
                        value_vars=cols_numericas, 
                        var_name='Sector', 
                        value_name='Consumption (Thousand MWh)')

fig = px.bar(df_long, 
             x='State', 
             y='Consumption (Thousand MWh)', 
             color='Sector', 
             barmode='group', 
             title='State-Level Consumption Profile by Sector (August 2025 YTD)',
             template='plotly_white',
             text_auto='.2s') # Show the value

fig.show()
#%%

df = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/EDS/Net_generation_for_all_sectors.csv', header=4)


mapa_fontes = {
    'United States : coal': 'Coal',
    'United States : natural gas': 'Natural Gas',
    'United States : nuclear': 'Nuclear',
    'United States : wind': 'Wind',
    'United States : all solar': 'Solar', 
    'United States : conventional hydroelectric': 'Hydro',
    'United States : geothermal': 'Geothermal',
    'United States : biomass': 'Biomass'
}


df_subset = df[df['description'].isin(mapa_fontes.keys())].copy()
df_subset['Category'] = df_subset['description'].map(mapa_fontes)

#Wide to long
df_subset.set_index('Category', inplace=True)
cols_years = [str(y) for y in range(2010, 2025)] # 2010 a 2024
df_transposed = df_subset[cols_years].transpose()
df_transposed.index.name = 'Year'
df_transposed.reset_index(inplace=True)


cols_ren = ['Wind', 'Solar', 'Hydro', 'Geothermal', 'Biomass']

for col in df_transposed.columns:
    df_transposed[col] = pd.to_numeric(df_transposed[col], errors='coerce')

df_transposed['Renewables'] = df_transposed[cols_ren].sum(axis=1)


cols_finals = ['Year', 'Coal', 'Natural Gas', 'Renewables', 'Nuclear']
df_plot = df_transposed[cols_finals].melt(id_vars='Year', 
                                          var_name='Source', 
                                          value_name='Generation (MWh)')


fig = px.line(df_plot, 
              x='Year', 
              y='Generation (MWh)', 
              color='Source',
              markers=True,
              title='U.S. Generation Mix Transition (2010–2024)',
              labels={'Generation (MWh)': 'Net Generation (Thousand MWh)'},
              template='plotly_white')


fig.update_traces(line=dict(width=3))

fig.show()
#%%

import pandas as pd
import plotly.express as px

df = pd.read_csv('C:/Users/leona/Desktop/Masters/Data Mungin/Second group Project/EDS/world_net_consumption.csv', header=1)


df.rename(columns={'Unnamed: 1': 'Country'}, inplace=True)

df_clean = df.drop([0]).drop(columns=['API'], errors='ignore')


cols_years = [c for c in df_clean.columns if c.isdigit()]
df_long = df_clean.melt(id_vars='Country', 
                        value_vars=cols_years, 
                        var_name='Year', 
                        value_name='Consumption')


df_long['Year'] = pd.to_numeric(df_long['Year'])
df_long['Consumption'] = pd.to_numeric(df_long['Consumption'], errors='coerce')
df_long.dropna(subset=['Consumption'], inplace=True)


df_countries = df_long[df_long['Country'].str.strip() != 'World'].copy()


df_race = df_countries.groupby('Year', group_keys=False).apply(
    lambda x: x.nlargest(15, 'Consumption')
).reset_index(drop=True)


fig = px.bar(df_race, 
             x='Consumption', 
             y='Country', 
             orientation='h', 
             color='Country', 
             animation_frame='Year', 
             animation_group='Country',
             range_x=[0, df_race['Consumption'].max() * 1.1],
             title='Global Energy Giants: Consumption Race (1980-2023)',
             labels={'Consumption': 'Net Consumption (Billion kWh)'},
             template='plotly_white')

# updates (legend gets bad)
fig.update_layout(yaxis={'categoryorder': 'total ascending'}, 
                  showlegend=False) 

# HTML interactive, fig.show() if on jupyter
fig.write_html("global_energy_race.html")
print("open in your browser!")
fig.show() 
#%%
from duckdb_client import DuckDBClient
import pandas as pd
import plotly.express as px


BASE_URL = "https://duckdb.straddlyze.com"
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ" 
client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)
import plotly.express as px


#%%
import plotly.express as px

tabela = "daily_energy_emissions"

target_regions = ['US-NY-NYIS', 'US-CAL-CISO', 'US-FLA-FPL']

# Transforming list to string "'US-NY', 'US-CAL-CISO', 'US-FLA-FPL'"
regioes_sql = "', '".join(target_regions)

print(f"Regions: {target_regions}")

query = f"""
SELECT 
    temp_mean, 
    carbon_intensity_direct,
    region 
FROM {tabela}
WHERE region IN ('{regioes_sql}')
LIMIT 5000 -- 
"""

try:
    df = client.query(query)
    
    if not df.empty:
        print(f"Sucesso! {len(df)} linhas baixadas.")
        fig = px.scatter(df, 
                         x="temp_mean", 
                         y="carbon_intensity_direct",
                         color="region", 
                         trendline="lowess", 
                         title="Environmental Impact Comparison: Heat vs. Grid Dirtiness (NY, CA, FL)",
                         labels={
                             "temp_mean": "Temperature (°C/°F)", 
                             "carbon_intensity_direct": "Carbon Intensity (gCO2/kWh)",
                             "region": "Grid Region"
                         },
                         opacity=0.4, 
                         template="plotly_white")
        fig.update_traces(selector=dict(mode='lines'), line=dict(width=4))
    
        fig.show()
        
    else:
        print("Query returned empty")

except Exception as e:
    print(f"Error: {e}")
#%%


