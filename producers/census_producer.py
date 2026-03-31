"""
Census Producer — FinRisk 360
One-time script to fetch Census ACS 5-year data for VA, MD, and DC,
compute risk indexes, and save a Parquet lookup file to disk.
"""

import os
import sys

import pandas as pd
import requests
from dotenv import load_dotenv

# ── Configuration ────────────────────────────────────────────
load_dotenv()

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

BASE_URL = "https://api.census.gov/data/2021/acs/acs5"

STATE_FIPS = {
    "51": {"name": "Virginia", "code": "VA"},
    "24": {"name": "Maryland", "code": "MD"},
    "11": {"name": "DC", "code": "DC"},
}

VARIABLES = ["NAME", "B25077_001E", "B19013_001E", "B25003_002E"]

def fetch_state_data(fips: str) -> list[dict]:
    """Fetch ACS data for a specific state and return as a list of dicts."""
    params = {
        "get": ",".join(VARIABLES),
        "for": "county:*",
        "in": f"state:{fips}",
        "key": CENSUS_API_KEY
    }
    
    response = requests.get(BASE_URL, params=params, timeout=30)
    
    print("Status code:", response.status_code)
    print("Raw response:", response.text[:300])
    
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return []
        
    if not response.text.strip():
        print("Empty response from Census API")
        return []
        
    try:
        data = response.json()
    except ValueError as exc:
        print(f"JSON Decode Error: {exc}")
        return []
        
    headers = data[0]
    rows = data[1:]
    
    return [dict(zip(headers, row)) for row in rows]

def run() -> None:
    """Main pipeline: fetch data, clean it, compute indexes, and write to disk."""
    if not CENSUS_API_KEY:
        print("Error: CENSUS_API_KEY environment variable is missing.")
        sys.exit(1)
        
    all_data = []
    
    for fips, info in STATE_FIPS.items():
        print(f"Fetching {info['name']} counties...")
        state_data = fetch_state_data(fips)
        all_data.extend(state_data)
        
    if not all_data:
        print("No data fetched.")
        sys.exit(1)
        
    df = pd.DataFrame(all_data)
    
    # ── Process Data ────────────────────────────────────────────
    # 1. Create county_fips
    df["county_fips"] = df["state"] + df["county"]
    
    # 2. Extract county_name (clean up suffixes like ", Virginia")
    df["county_name"] = df["NAME"].apply(lambda x: str(x).split(",")[0].strip())
    
    # 3. Numeric conversions, Handle missing values (< 0), Fill NaN with median
    numeric_mappings = {
        "B25077_001E": "median_home_value",
        "B19013_001E": "median_income",
        "B25003_002E": "owner_occupied_units"
    }
    
    for raw_col, new_col in numeric_mappings.items():
        df[new_col] = pd.to_numeric(df[raw_col], errors="coerce").astype(float)
        
        # Census null sentinel
        df.loc[df[new_col] < 0, new_col] = float("nan")
        
        # Median fill
        median_val = df[new_col].median()
        df[new_col] = df[new_col].fillna(median_val)
        
    # 4. Define state_code mapping
    df["state_code"] = df["state"].map(lambda f: STATE_FIPS.get(f, {}).get("code", "Unknown"))
        
    # 5. Compute county_risk_index
    def compute_risk(row: pd.Series) -> float:
        inc = row["median_income"]
        home = row["median_home_value"]
        
        inc_score = min(inc / 150000.0, 1.0)
        home_score = min(home / 800000.0, 1.0)
        
        raw_idx = (inc_score * 0.6) + (home_score * 0.4)
        return min(max(raw_idx, 0.0), 1.0)
        
    df["county_risk_index"] = df.apply(compute_risk, axis=1)
    
    # ── Save Output ─────────────────────────────────────────────
    final_cols = [
        "county_fips", "county_name", "median_home_value", 
        "median_income", "owner_occupied_units", "state_code", 
        "county_risk_index"
    ]
    
    final_df = df[final_cols]
    
    os.makedirs("data", exist_ok=True)
    
    parquet_path = "data/census_county_lookup.parquet"
    csv_path = "data/census_county_lookup.csv"
    
    final_df.to_parquet(parquet_path, index=False)
    final_df.to_csv(csv_path, index=False)
    
    print(f"Saved {len(final_df)} counties to {parquet_path}")
    
    # ── Print Sample ────────────────────────────────────────────
    print("\nSample Output:")
    sample_cols = ["county_fips", "county_name", "median_income", "county_risk_index"]
    print(final_df[sample_cols].head(5).to_string(index=False))


if __name__ == "__main__":
    run()
