# ingest.py
from __future__ import annotations
import os
import time
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
load_dotenv()  # load .env before reading token

import requests
import pandas as pd


# ---------- Config ----------
BASE_DIR = Path("data")          # root for Parquet output
REQUEST_TIMEOUT = 40
RETRY_MAX = 4
RETRY_SLEEP = 2                  # seconds (exponential backoff)
CHUNK_DAYS = 10                  # API limit window for hourly data


# ---------- Helpers ----------
def get_api_token() -> str:
    token = os.getenv("EM_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing EM_API_TOKEN environment variable.")
    return token

def month_window(year: int, month: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    end = (start + pd.offsets.MonthEnd(1)).normalize() + pd.Timedelta(days=1)
    return start, end

def ensure_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors="coerce")

def standardize_columns(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    """
    Normalize columns and types to a common schema:
    datetime_utc | zone | carbon_direct | carbon_lifecycle | cfe_pct | re_pct | estimated | estimation_method
    """
    df = df.copy()

    # normalize headers
    df.columns = (df.columns
                  .str.strip()
                  .str.lower()
                  .str.replace(" ", "_")
                  .str.replace(r"[()]", "", regex=True))

    # detect datetime-ish column
    candidates_datetime = [c for c in df.columns if c in (
        "datetime", "datetime_utc", "time", "timestamp", "utc_datetime", "datetime_(utc)", "date"
    )]
    if not candidates_datetime:
        candidates_datetime = [c for c in df.columns if "date" in c or "time" in c]
    if not candidates_datetime:
        raise ValueError("No datetime-like column found in API response.")
    dt_col = candidates_datetime[0]

    # map possible source columns to our target schema
    map_candidates = {
        "carbon_direct": ["carbon_intensity", "carbon_intensity_gco2eq/kwh",
                          "carbon_intensity_gco₂eq/kwh_(direct)", "carbonintensity", "direct"],
        "carbon_lifecycle": ["lifecycle", "carbon_intensity_gco2eq/kwh_(life_cycle)",
                             "life_cycle", "lifecycle_intensity"],
        "cfe_pct": ["cfe", "cfe_%", "carbon-free_energy_percentage",
                    "carbon-free_energy_percentage_(cfe%)"],
        "re_pct": ["re", "re_%", "renewable_energy_percentage",
                   "renewable_energy_percentage_(re%)"],
        "estimated": ["estimated", "data_estimated"],
        "estimation_method": ["estimation_method", "data_estimation_method", "estimation"]
    }

    out = pd.DataFrame({
        "datetime_utc": ensure_datetime(df[dt_col]),
        "zone": zone
    })

    def first_present(cols: List[str]) -> Optional[str]:
        for c in cols:
            if c in df.columns:
                return c
        return None

    for target, cand_list in map_candidates.items():
        c = first_present([x.lower() for x in cand_list])
        out[target] = df[c] if c is not None else pd.NA

    # numeric coercions
    for col in ["carbon_direct", "carbon_lifecycle", "cfe_pct", "re_pct"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # boolean-ish for estimated
    if out["estimated"].dtype == object:
        out["estimated"] = out["estimated"].astype(str).str.lower().isin(["1", "true", "yes", "sim", "y"])

    out = out.dropna(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True)
    return out

def upsert_parquet(df: pd.DataFrame, base_dir: Path, granularity: str):
    """
    Partitioned write by zone/year. If file exists, append and de-duplicate on (datetime_utc, zone).
    """
    if df.empty:
        return
    df = df.copy()
    df["year"] = df["datetime_utc"].dt.year

    for (zone, year), part in df.groupby(["zone", "year"]):
        out = base_dir / f"granularity={granularity}/zone={zone}/year={year}/data.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            old = pd.read_parquet(out)
            merged = (pd.concat([old, part], ignore_index=True)
                      .drop_duplicates(subset=["datetime_utc", "zone"])
                      .sort_values("datetime_utc"))
        else:
            merged = part.sort_values("datetime_utc")
        merged.to_parquet(out, index=False)


# ---------- Aggregation (from hourly) ----------
# --- mude a assinatura para NÃO exigir zones ---
def aggregate_from_hourly(base_dir: Path):
    import duckdb
    q = "SELECT * FROM read_parquet($path)"
    path = str(base_dir / "granularity=hourly/**/data.parquet")
    hourly = duckdb.query(q, params={"path": path}).to_df()
    if hourly.empty:
        print("No hourly data found to aggregate.")
        return

    hourly["datetime_utc"] = pd.to_datetime(hourly["datetime_utc"], utc=True)
    hourly = hourly.sort_values(["zone","datetime_utc"])

    for freq, gran in [("D", "daily"), ("MS", "monthly"), ("YS", "yearly")]:
        out_list = []
        for z, zdf in hourly.groupby("zone"):
            agg = (zdf.set_index("datetime_utc")
                      .resample(freq)
                      .mean(numeric_only=True)
                      .reset_index())
            agg["zone"] = z
            out_list.append(agg)

        merged = pd.concat(out_list, ignore_index=True)
        cols = ["datetime_utc","zone","carbon_direct","carbon_lifecycle","cfe_pct","re_pct","estimated","estimation_method"]
        for c in cols:
            if c not in merged.columns:  # segurança
                merged[c] = pd.NA

        upsert_parquet(merged[cols], base_dir, granularity=gran)
        print(f"Aggregated {gran}: {len(merged)} rows")




# ---------- API ----------

# --- troque a função que chama a API ---
def fetch_hourly_from_api(zone: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    token = get_api_token()
    headers = {
        # CORRETO: usa 'auth-token' em vez de Authorization: Bearer
        "auth-token": token,
        "Accept": "application/json",
    }

    base_url = "https://api.electricitymaps.com/v3/carbon-intensity/past-range"
    # CORRETO: 'hourly' (não 'hour'); end é exclusivo
    params = {
        "zone": zone,
        "start": start_iso,             # ex.: "2021-01-01T00:00:00Z"
        "end": end_iso,                 # ex.: "2021-01-11T00:00:00Z" (10 dias)
        "temporalGranularity": "hourly",
        # opcional: "emissionFactorType": "lifecycle" ou "direct"
        # opcional: "disableEstimations": "true"
    }

    for attempt in range(RETRY_MAX):
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        return pd.DataFrame(v)
                if "history" in data and isinstance(data["history"], list):
                    return pd.DataFrame(data["history"])
                return pd.DataFrame([data])
            elif isinstance(data, list):
                return pd.DataFrame(data)
            else:
                return pd.DataFrame()
        except requests.RequestException:
            if attempt == RETRY_MAX - 1:
                raise
            time.sleep(RETRY_SLEEP * (2 ** attempt))



def ingest_month(zone: str, year: int, month: int, base_dir: Path):
    start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    month_end_exclusive = (start + pd.offsets.MonthEnd(1)).normalize() + pd.Timedelta(days=1)

    # fatia em janelas de 10 dias (end exclusivo)
    cur = start
    collected = []
    while cur < month_end_exclusive:
        nxt = min(cur + pd.Timedelta(days=10), month_end_exclusive)
        raw = fetch_hourly_from_api(zone, cur.isoformat().replace("+00:00","Z"), nxt.isoformat().replace("+00:00","Z"))
        if not raw.empty:
            collected.append(raw)
        cur = nxt

    if not collected:
        print(f"[{zone}] No data {year}-{month:02d}")
        return

    raw_all = pd.concat(collected, ignore_index=True)
    std = standardize_columns(raw_all, zone=zone)
    upsert_parquet(std, base_dir, granularity="hourly")
    print(f"[{zone}] Ingested {year}-{month:02d}: {len(std)} rows")



# ---------- Runner ----------
if __name__ == "__main__":
    # choose your comparison set (feel free to change)
    ZONES = ["US-FLA-FPL", "US-GA-GA", "US-CA-CISO", "US-NY-NYIS", "FR", "BR"]
    YEARS = [2021, 2022, 2023, 2024]

    for z in ZONES:
        for y in YEARS:
            for m in range(1, 13):
                try:
                    ingest_month(z, y, m, BASE_DIR)
                except Exception as e:
                    print(f"Failed {z} {y}-{m:02d}: {e}")

    # build daily/monthly/yearly locally from hourly
    aggregate_from_hourly(BASE_DIR)
    print("Done.")
