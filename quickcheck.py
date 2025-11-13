# quickcheck.py
import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
REQUEST_TIMEOUT = 40

def get_api_token() -> str:
    token = os.getenv("EM_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing EM_API_TOKEN environment variable.")
    return token

def zone_quickcheck(zones: list[str]) -> dict[str, str]:
    """
    Testa 3 dias de histórico hourly em cada zona.
    Retorna {zone: 'OK' | 'HTTP <code>' | 'ERR <Exception>'}.
    """
    token = get_api_token()
    # ElectricityMaps usa este header (não Bearer):
    headers = {"auth-token": token, "Accept": "application/json"}
    base_url = "https://api.electricitymaps.com/v3/carbon-intensity/past-range"

    start = pd.Timestamp("2023-01-01", tz="UTC")
    end   = start + pd.Timedelta(days=3)  # end é exclusivo

    res = {}
    for z in zones:
        params = {
            "zone": z,
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
            "temporalGranularity": "hourly",
        }
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            res[z] = "OK" if r.ok else f"HTTP {r.status_code}"
        except Exception as e:
            res[z] = f"ERR {type(e).__name__}"
    return res

if __name__ == "__main__":
    ZONES = ["US-FLA-FPL", "US-GA-GA", "US-CA-CISO", "US-NY-NYIS", "FR", "BR"]
    print(zone_quickcheck(ZONES))
