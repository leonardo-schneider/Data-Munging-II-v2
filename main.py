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
