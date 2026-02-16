#!/usr/bin/env python3
"""
USEEIO xlsx → Supabase ETL.
Reads M, M_d, SectorCrosswalk, Rho, indicators, commodities_meta from xlsx;
melts to long where needed; inserts into Supabase with model_version.
Auto-detects economic_year (demands sheet) and satellite year range (Rho columns)
and writes to model_metadata (one row per model; is_active for UI).

To add commodities_meta table in Supabase (SQL Editor), run:
  create table commodities_meta (
    model_version text not null,
    code text,
    name text,
    category text,
    location text,
    unit text,
    primary key (model_version, code)
  );
  -- Add any extra columns to match the sheet (lowercase, underscores).

To add model_metadata table, run the statements in: supabase_model_metadata.sql
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from postgrest.exceptions import APIError
from supabase import create_client

load_dotenv()

XLSX_PATH_ENV = "USEEIO_XLSX_PATH"
MODEL_VERSION_ENV = "USEEIO_MODEL_VERSION"
SUPABASE_URL_ENV = "SUPABASE_URL"
SUPABASE_KEY_ENV = "SUPABASE_SERVICE_ROLE_KEY"

SHEET_INDICATORS = "indicators"
SHEET_SECTOR_CROSSWALK = "SectorCrosswalk"
SHEET_RHO = "Rho"
SHEET_M = "M"
SHEET_M_D = "M_d"
SHEET_COMMODITIES_META = "commodities_meta"
SHEET_DEMANDS = "demands"
BATCH_SIZE = 2000

# Columns expected in Supabase commodities_meta table (sheet may have more; we only send these)
COMMODITIES_META_COLUMNS = [
    "model_version", "code", "name", "description", "category", "location", "unit"
]

# Default xlsx path relative to this script's directory
_DEFAULT_XLSX = Path(__file__).resolve().parent / "ExtractFrom" / "USEEIOv2.6.0-phoebe-23 copy.xlsx"


def get_sector_region(s: str) -> tuple[str, str]:
    """Split '1111A0/US' -> (sector_code='1111A0', region='US')."""
    s = (s or "").strip()
    if "/" in s:
        code, region = s.rsplit("/", 1)
        return code.strip(), region.strip()
    return s, "US"


def detect_years_from_xlsx(xlsx_path: str) -> tuple[int | None, int | None, int | None]:
    """
    Open the Excel file and detect:
    - Economic Year: from the demands sheet (first non-null Year value).
    - Satellite year range: from Rho sheet column headers (min/max of numeric columns).
    Returns (economic_year, satellite_year_min, satellite_year_max).
    """
    economic_year: int | None = None
    satellite_min: int | None = None
    satellite_max: int | None = None

    xl = pd.ExcelFile(xlsx_path)
    sheet_names = [s.strip().lower() for s in xl.sheet_names]

    # Economic Year from demands sheet
    demands_name = next((s for s in xl.sheet_names if s.strip().lower() == SHEET_DEMANDS.lower()), None)
    if demands_name:
        try:
            df_d = pd.read_excel(xlsx_path, sheet_name=demands_name, header=0)
            df_d.columns = [str(c).strip() for c in df_d.columns]
            year_col = next(
                (c for c in df_d.columns if c.lower() in ("year", "economic year", "economic_year")),
                None,
            )
            if year_col:
                for val in df_d[year_col].dropna():
                    try:
                        economic_year = int(float(val))
                        break
                    except (ValueError, TypeError):
                        continue
        except Exception:
            pass

    # Satellite year range from Rho columns (first row is header; first col is sector, rest are years)
    rho_sheet_name = next((s for s in xl.sheet_names if s.strip().lower() == SHEET_RHO.lower()), None)
    if rho_sheet_name:
        try:
            rho_df = pd.read_excel(xlsx_path, sheet_name=rho_sheet_name, header=0, nrows=0)
            sector_col = rho_df.columns[0]
            years: list[int] = []
            for c in rho_df.columns:
                if c is sector_col or c == sector_col:
                    continue
                try:
                    y = int(float(c))
                    years.append(y)
                except (ValueError, TypeError):
                    continue
            if years:
                satellite_min = min(years)
                satellite_max = max(years)
        except Exception:
            pass

    return economic_year, satellite_min, satellite_max


def get_config() -> tuple[str, str, str, str, int | None, int | None, int | None]:
    """Return (xlsx_path, model_version, supabase_url, supabase_key, economic_year, satellite_year_min, satellite_year_max)."""
    xlsx = (
        os.environ.get(XLSX_PATH_ENV)
        or (sys.argv[1] if len(sys.argv) > 1 else None)
        or str(_DEFAULT_XLSX)
    )
    xlsx_path = Path(xlsx)
    if not xlsx_path.is_absolute():
        xlsx_path = Path(__file__).resolve().parent / xlsx_path
    if not xlsx_path.is_file():
        raise SystemExit(
            f"XLSX not found: {xlsx_path}\n"
            "Set USEEIO_XLSX_PATH or pass path as first argument.\n"
            "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env or environment."
        )
    xlsx = str(xlsx_path)
    model_version = os.environ.get(MODEL_VERSION_ENV)
    if not model_version:
        stem = Path(xlsx).stem
        model_version = re.sub(r"^USEEIO", "", stem).lstrip("v-_").strip() or "unknown"
    url = os.environ.get(SUPABASE_URL_ENV)
    key = os.environ.get(SUPABASE_KEY_ENV) or os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise SystemExit(
            f"Set {SUPABASE_URL_ENV} and {SUPABASE_KEY_ENV} (or SUPABASE_KEY) in .env"
        )
    economic_year, satellite_year_min, satellite_year_max = detect_years_from_xlsx(xlsx)
    return xlsx, model_version, url, key, economic_year, satellite_year_min, satellite_year_max


def load_indicators(xlsx_path: str, model_version: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_INDICATORS)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    # Excel "SimpleUnit" / "SimpleName" become "simpleunit" / "simplename"; map to DB names
    renames = {}
    if "simpleunit" in df.columns and "simple_unit" not in df.columns:
        renames["simpleunit"] = "simple_unit"
    if "simplename" in df.columns and "simple_name" not in df.columns:
        renames["simplename"] = "simple_name"
    if renames:
        df = df.rename(columns=renames)
    df["model_version"] = model_version
    return df


def load_sector_crosswalk(xlsx_path: str, model_version: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_SECTOR_CROSSWALK)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    df["model_version"] = model_version
    return df


def load_commodities_meta(xlsx_path: str, model_version: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_COMMODITIES_META)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    df["model_version"] = model_version
    first_col = df.columns[0]
    df = df.dropna(subset=[first_col])
    return df


def load_rho_long(xlsx_path: str, model_version: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_RHO, header=0)
    sector_col = df.columns[0]
    year_cols = [c for c in df.columns if c != sector_col]
    rows = []
    for _, row in df.iterrows():
        sector_region = row[sector_col]
        if pd.isna(sector_region):
            continue
        sector_code, region = get_sector_region(str(sector_region))
        for year_col in year_cols:
            try:
                year = int(float(year_col))
            except (ValueError, TypeError):
                continue
            val = row[year_col]
            if pd.isna(val):
                continue
            rows.append(
                {
                    "model_version": model_version,
                    "sector_code": sector_code,
                    "region": region,
                    "year": year,
                    "rho_value": float(val),
                }
            )
    return pd.DataFrame(rows)


def build_index_to_code(indicators_df: pd.DataFrame) -> dict[int, str]:
    if "index" in indicators_df.columns:
        return indicators_df.set_index("index")["code"].astype(str).to_dict()
    return indicators_df["code"].astype(str).to_dict()


def load_impact_long(
    xlsx_path: str,
    sheet: str,
    impact_type: str,
    model_version: str,
    index_to_code: dict[int, str],
) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=sheet, header=0)
    all_cols = list(df.columns)
    value_cols = [c for c in all_cols if c != all_cols[0]]
    rows = []
    for i, (_, row) in enumerate(df.iloc[1:].iterrows()):
        indicator_code = index_to_code.get(int(i))
        if indicator_code is None:
            continue
        for col in value_cols:
            sector_region = col
            sector_code, region = get_sector_region(str(sector_region))
            val = row.get(col)
            if pd.isna(val):
                continue
            try:
                value = float(val)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "model_version": model_version,
                    "impact_type": impact_type,
                    "indicator_code": indicator_code,
                    "sector_code": sector_code,
                    "region": region,
                    "value": value,
                }
            )
    return pd.DataFrame(rows)


def insert_in_batches(client, table: str, df: pd.DataFrame, batch_size: int = BATCH_SIZE):
    records = df.replace({pd.NA: None}).to_dict("records")
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        client.table(table).insert(chunk).execute()


def load_model_metadata(
    client,
    model_version: str,
    economic_year: int | None,
    satellite_year_min: int | None,
    satellite_year_max: int | None,
) -> None:
    """
    Insert or update model_metadata for this model_version and set is_active = true.
    All other model_versions are set to is_active = false.
    """
    record = {
        "model_version": model_version,
        "economic_year": economic_year,
        "satellite_year_min": satellite_year_min,
        "satellite_year_max": satellite_year_max,
        "is_active": True,
    }
    try:
        client.table("model_metadata").upsert(
            record,
            on_conflict="model_version",
            ignore_duplicates=False,
        ).execute()
        # Ensure only this model is active
        client.table("model_metadata").update({"is_active": False}).neq(
            "model_version", model_version
        ).execute()
    except APIError as e:
        if e.code == "PGRST205":
            print("  Skipped model_metadata (table not in Supabase yet). Run the provided SQL to create it.")
        else:
            raise


def delete_model_version(client, model_version: str) -> None:
    for table in ("impacts", "rho", "sector_crosswalk", "commodities_meta", "indicators", "model_metadata"):
        try:
            client.table(table).delete().eq("model_version", model_version).execute()
            print(f"  Cleared table: {table}")
        except APIError as e:
            if e.code == "PGRST205":
                print(f"  Skipped {table} (table not in Supabase yet)")
            else:
                raise


def main() -> None:
    xlsx_path, model_version, supabase_url, supabase_key, economic_year, satellite_year_min, satellite_year_max = get_config()
    client = create_client(supabase_url, supabase_key)

    print(f"Model version: {model_version}")
    print(f"XLSX: {xlsx_path}")
    print(f"Detected: economic_year={economic_year}, satellite_years={satellite_year_min}-{satellite_year_max}\n")

    print("Removing existing data for this model version...")
    delete_model_version(client, model_version)
    print("  Done.\n")

    print("Loading model_metadata...")
    load_model_metadata(client, model_version, economic_year, satellite_year_min, satellite_year_max)
    print("  Table 'model_metadata' updated.\n")

    print("Loading indicators...")
    ind_df = load_indicators(xlsx_path, model_version)
    cols_ind = ["model_version", "code", "id", "name", "unit", "group", "simple_unit", "simple_name"]
    ind_df = ind_df[[c for c in cols_ind if c in ind_df.columns]]
    ind_df = ind_df.dropna(subset=["code"])
    insert_in_batches(client, "indicators", ind_df)
    print(f"  Table 'indicators' updated: {len(ind_df)} rows inserted.\n")

    index_to_code = build_index_to_code(ind_df)

    print("Loading sector_crosswalk...")
    sc_df = load_sector_crosswalk(xlsx_path, model_version)
    sc_cols = ["model_version", "naics", "bea_sector", "bea_summary", "bea_detail", "bea_detail_waste_disagg"]
    sc_df = sc_df[[c for c in sc_cols if c in sc_df.columns]]
    sc_df = sc_df.dropna(subset=["naics"])
    insert_in_batches(client, "sector_crosswalk", sc_df)
    print(f"  Table 'sector_crosswalk' updated: {len(sc_df)} rows inserted.\n")

    print("Loading commodities_meta...")
    try:
        cm_df = load_commodities_meta(xlsx_path, model_version)
        cm_df = cm_df[[c for c in COMMODITIES_META_COLUMNS if c in cm_df.columns]]
        insert_in_batches(client, "commodities_meta", cm_df)
        print(f"  Table 'commodities_meta' updated: {len(cm_df)} rows inserted.\n")
    except APIError as e:
        if e.code == "PGRST205":
            print(f"  Skipped commodities_meta (table not in Supabase yet). Create it to load this sheet.\n")
        else:
            raise

    print("Loading rho...")
    rho_df = load_rho_long(xlsx_path, model_version)
    insert_in_batches(client, "rho", rho_df)
    print(f"  Table 'rho' updated: {len(rho_df)} rows inserted.\n")

    print("Loading M (total impacts)...")
    m_df = load_impact_long(xlsx_path, SHEET_M, "total", model_version, index_to_code)
    insert_in_batches(client, "impacts", m_df)
    print(f"  Table 'impacts' updated: {len(m_df)} rows inserted (total).\n")

    print("Loading M_d (domestic impacts)...")
    md_df = load_impact_long(xlsx_path, SHEET_M_D, "domestic", model_version, index_to_code)
    insert_in_batches(client, "impacts", md_df)
    print(f"  Table 'impacts' updated: {len(md_df)} rows inserted (domestic).\n")

    print("Done. All tables updated.")


if __name__ == "__main__":
    main()
