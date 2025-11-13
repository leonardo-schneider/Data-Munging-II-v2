# Data Munging II — Electricity Data Project  
Team Project • 2025

## Overview  
This project focuses on collecting, cleaning, combining, and preparing electricity and carbon-intensity data for three major U.S. regions:

- **Florida (US-FLA-FPL)**
- **California (US-CAL-CISO)**
- **New York (US-NY-NYIS)**

The original plan was to extract data using the **Electricity Maps API**, but due to API limitations, missing values, and incomplete fields, the group switched to working with **local CSV datasets** provided by Electricity Maps.

The final goal was to build **four consolidated (“big”) datasets**:

- `big_hourly.parquet`  
- `big_daily.parquet`  
- `big_monthly.parquet`  
- `big_yearly.parquet`

Each one contains all three regions combined using a consistent schema.

---

## What We Tried First (And Why It Failed)

### 1. **Attempted Electricity Maps API Integration**
We originally tried to fetch carbon-intensity, CFE%, renewable %, and generation data directly from the API.  
However, we faced several major issues:

#### ❌ **1. API returned very limited fields for certain regions**
- Florida, in particular, only returned **carbon_intensity_direct**.
- Critical variables such as lifecycle emissions, CFE%, RE%, solar/wind/hydro shares were **not available** via API.

#### ❌ **2. Inconsistent coverage across regions**
- California and New York provided richer data.
- Florida had incomplete time series and missing values.

#### ❌ **3. Rate limits & authentication problems**
- The free API key often returned *429 Too Many Requests*.
- Some endpoints required paid access.

#### ❌ **4. Mismatch between API data and CSV data**
- CSVs contained more columns than the API returned.
- Meaningful analysis (especially generation by source) would not be consistent.

➡ **Conclusion:** API was insufficient for our project’s requirements.

---

## What We Did Instead (Successful Path)

### ✔ Switched to full CSV datasets for each region  
We downloaded the **hourly, daily, monthly and yearly** CSVs from:

- `datasets/FLA/`
- `datasets/CA/`
- `datasets/NY/`

Each region had files from **2021 to 2024**.

### ✔ Cleaned and standardized each dataset
We ensured:

- Consistent column naming  
- Renamed key fields (e.g., `Datetime (UTC)` → `datetime_utc`)
- Added `region`, `frequency`, and `year`
- Converted timestamps to UTC format  
- Standardized missing values across regions  

### ✔ Identified schema differences
- California & New York include **generation by source** (solar, wind, hydro, nuclear, gas, imports, storage)
- Florida does **not** provide generation-by-source data
- All three regions have the same yearly columns (CFE%, RE%, direct & lifecycle emissions)

### ✔ Built the final combined datasets
For each frequency (hourly/daily/monthly/yearly), we concatenated FL + CA + NY into a single file:

big/big_hourly.parquet
big/big_daily.parquet
big/big_monthly.parquet
big/big_yearly.parquet

Missing columns (e.g., solar generation for Florida) were filled with `NaN`, following standard data warehouse practices.

---

## Final Schema (Summary)

Every combined dataset includes:

### **Core columns (all regions)**
- `datetime_utc`
- `region`
- `frequency`
- `carbon_intensity_direct`
- `carbon_intensity_lifecycle`
- `cfe_pct`
- `re_pct`

### **Metadata**
- `data_source`
- `data_estimated`
- `data_estimation_method`

### **Generation-by-source (only CA & NY)**
- `solar_generation`
- `wind_generation`
- `hydro_generation`
- `nuclear_generation`
- `gas_generation`
- `imports`
- `storage_charge`
- `storage_discharge`
- (and other detailed fields available in raw CSVs)

Florida has `NaN` for these columns because the data is not provided.

---

## What Still Can Be Done (Future Work)

### 🔹 1. **Exploratory Data Analysis (EDA)**
Possible analyses:
- Hourly carbon-intensity patterns  
- Regional comparison (CA vs NY vs FL)  
- Seasonal trends (monthly/yearly)  
- Relationship between CFE% and carbon intensity  
- Renewable growth between 2021–2024  

### 🔹 2. **Visualization dashboard**
Using:
- Tableau  
- Power BI  
- Python (Plotly)  

### 🔹 3. **Machine Learning / Forecasting**
Examples:
- Predict hourly carbon intensity  
- Forecast renewable energy share  
- Detect anomalies (extreme spikes)  

### 🔹 4. **Additional regions**
We can easily extend the pipeline to include:
- ERCOT (Texas)  
- MISO  
- PJM  
- BPA

### 🔹 5. **Create a data dictionary**
A clean `.md` file documenting:
- each column
- its meaning
- units
- which frequency/region includes it

### 🔹 6. **Automate the pipeline**
Using:
- Python scripts  
- Prefect / Airflow  
- GitHub Actions  

---

## Team Summary

We successfully:

- Evaluated API feasibility  
- Identified limitations  
- Migrated to CSV-based ingestion  
- Cleaned and standardized all datasets  
- Built a unified schema  
- Combined all regions across four frequencies  
- Exported final `.parquet` datasets for efficient analysis  

The team adapted quickly to technical challenges and produced a robust, scalable data asset.

---


