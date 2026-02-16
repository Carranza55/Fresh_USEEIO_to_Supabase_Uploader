#!/usr/bin/env python3
"""
USEEIO xlsx → Supabase ETL.
Reads M, M_d, C, SectorCrosswalk, Rho, indicators, commodities_meta from xlsx;
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
To add ipcc_ar_gwp table (IPCC AR GWP factors), run: supabase_ipcc_ar_gwp.sql
To add c table (characterization matrix for GWP per flow from sheet C), run: SQL Backup/supabase_c.sql
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
SHEET_C = "C"
SHEET_COMMODITIES_META = "commodities_meta"
SHEET_DEMANDS = "demands"
BATCH_SIZE = 2000

# Columns expected in Supabase commodities_meta table (sheet may have more; we only send these)
COMMODITIES_META_COLUMNS = [
    "model_version", "code", "name", "description", "category", "location", "unit"
]

# IPCC AR GWP reference data (gas_name, ar_version, gwp_value, category) — no Formula column
# Sourced from Global-Warming-Potential-Values_ONE_TABLE.xlsx (GWP_Table sheet)
IPCC_AR_GWP_ROWS = [
    ("Carbon dioxide", "AR4", 1.0, "Major GHG"),
    ("Carbon dioxide", "AR5", 1.0, "Major GHG"),
    ("Carbon dioxide", "AR6", 1.0, "Major GHG"),
    ("Methane – non-fossil", "AR4", 25.0, "Major GHG"),
    ("Methane – non-fossil", "AR5", 28.0, "Major GHG"),
    ("Methane – non-fossil", "AR6", 27.0, "Major GHG"),
    ("Methane – fossil", "AR5", 30.0, "Major GHG"),
    ("Methane – fossil", "AR6", 29.8, "Major GHG"),
    ("Nitrous oxide", "AR4", 298.0, "Major GHG"),
    ("Nitrous oxide", "AR5", 265.0, "Major GHG"),
    ("Nitrous oxide", "AR6", 273.0, "Major GHG"),
    ("Nitrogen trifluoride", "AR4", 17200.0, "Major GHG"),
    ("Nitrogen trifluoride", "AR5", 16100.0, "Major GHG"),
    ("Nitrogen trifluoride", "AR6", 17400.0, "Major GHG"),
    ("Sulfur hexafluoride", "AR4", 22800.0, "Major GHG"),
    ("Sulfur hexafluoride", "AR5", 23500.0, "Major GHG"),
    ("Sulfur hexafluoride", "AR6", 24300.0, "Major GHG"),
    ("HFC-23", "AR4", 14800.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-23", "AR5", 12400.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-23", "AR6", 14600.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-32", "AR4", 675.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-32", "AR5", 677.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-32", "AR6", 771.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-41", "AR4", 92.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-41", "AR5", 116.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-41", "AR6", 135.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-125", "AR4", 3500.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-125", "AR5", 3170.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-125", "AR6", 3740.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-134", "AR4", 1100.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-134", "AR5", 1120.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-134", "AR6", 1260.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-134a", "AR4", 1430.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-134a", "AR5", 1300.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-134a", "AR6", 1530.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-143", "AR4", 353.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-143", "AR5", 328.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-143", "AR6", 364.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-143a", "AR4", 4470.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-143a", "AR5", 4800.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-143a", "AR6", 5810.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-152", "AR4", 53.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-152", "AR5", 16.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-152", "AR6", 21.5, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-152a", "AR4", 124.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-152a", "AR5", 138.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-152a", "AR6", 164.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-161", "AR4", 12.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-161", "AR5", 4.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-161", "AR6", 4.84, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-227ca", "AR5", 2640.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-227ca", "AR6", 2980.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-227ea", "AR4", 3220.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-227ea", "AR5", 3350.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-227ea", "AR6", 3600.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236cb", "AR4", 1340.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236cb", "AR5", 1210.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236cb", "AR6", 1350.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236ea", "AR4", 1370.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236ea", "AR5", 1330.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236ea", "AR6", 1500.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236fa", "AR4", 9810.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236fa", "AR5", 8060.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-236fa", "AR6", 8690.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245ca", "AR4", 693.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245ca", "AR5", 716.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245ca", "AR6", 787.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245cb", "AR5", 4620.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245cb", "AR6", 4550.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245ea", "AR5", 235.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245ea", "AR6", 255.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245eb", "AR5", 290.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245eb", "AR6", 325.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245fa", "AR4", 1030.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245fa", "AR5", 858.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-245fa", "AR6", 962.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-263fb", "AR5", 76.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-263fb", "AR6", 74.8, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-272ca", "AR5", 144.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-272ca", "AR6", 599.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-329p", "AR5", 2360.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-329p", "AR6", 2890.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-365mfc", "AR4", 794.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-365mfc", "AR5", 804.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-365mfc", "AR6", 914.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-43-10mee", "AR4", 1640.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-43-10mee", "AR5", 1650.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFC-43-10mee", "AR6", 1600.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1123", "AR6", 0.005, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1132a", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1132a", "AR6", 0.052, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1141", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1141", "AR6", 0.024, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1225ye(Z)", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1225ye(Z)", "AR6", 0.344, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1225ye(E)", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1225ye(E)", "AR6", 0.118, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1234ze(Z)", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1234ze(Z)", "AR6", 0.315, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1234ze(E)", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1234ze(E)", "AR6", 1.37, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1234yf", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1234yf", "AR6", 0.501, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1336mzz(E)", "AR6", 18.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1336mzz(Z)", "AR5", 2.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1336mzz(Z)", "AR6", 2.08, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1243zf", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1243zf", "AR6", 0.261, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1345zfc", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("HFO-1345zfc", "AR6", 0.182, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("3,3,4,4,5,5,6,6,6-nonafluorohex-1-ene", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("3,3,4,4,5,5,6,6,6-nonafluorohex-1-ene", "AR6", 0.204, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,8- tridecafluorooct-1-ene", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,8- tridecafluorooct-1-ene", "AR6", 0.162, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,10- heptadecafluorodec-1-ene", "AR5", None, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*", "<1"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,10- heptadecafluorodec-1-ene", "AR6", 0.141, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("3,3,3-trifluoro-2-(trifluoromethyl) prop-1-ene", "AR6", 0.377, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("1,1,2,2,3,3-hexafluorocyclopentane", "AR6", 120.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("1,1,2,2,3,3,4- heptafluorocyclopentane", "AR6", 231.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("1,3,3,4,4,5,5-heptafluoro-cyclopentene", "AR6", 45.1, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("(4s,5s)-1,1,2,2,3,3,4,5- octafluorocyclopentane", "AR6", 258.0, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1438ezy(E)", "AR6", 8.22, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("HFO-1447fz", "AR6", 0.24, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("1,3,3,4,4-pentafluorocyclobutene", "AR6", 92.4, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("3,3,4,4-tetrafluorocyclobutene", "AR6", 25.6, "Hydrofluorocarbons (includes unsaturated hydrofluorocarbons)*"),
    ("PFC-14", "AR4", 7390.0, "Fully Fluorinated Species"),
    ("PFC-14", "AR5", 6630.0, "Fully Fluorinated Species"),
    ("PFC-14", "AR6", 7380.0, "Fully Fluorinated Species"),
    ("PFC-116", "AR4", 12200.0, "Fully Fluorinated Species"),
    ("PFC-116", "AR5", 11100.0, "Fully Fluorinated Species"),
    ("PFC-116", "AR6", 12400.0, "Fully Fluorinated Species"),
    ("PFC-218", "AR4", 8830.0, "Fully Fluorinated Species"),
    ("PFC-218", "AR5", 8900.0, "Fully Fluorinated Species"),
    ("PFC-218", "AR6", 9290.0, "Fully Fluorinated Species"),
    ("PFC-c216", "AR4", 17340.0, "Fully Fluorinated Species", ">17,340"),
    ("PFC-c216", "AR5", 9200.0, "Fully Fluorinated Species"),
    ("PFC-C-318 (PFC-318)", "AR4", 10300.0, "Fully Fluorinated Species"),
    ("PFC-C-318 (PFC-318)", "AR5", 9540.0, "Fully Fluorinated Species"),
    ("PFC-C-318 (PFC-318)", "AR6", 10200.0, "Fully Fluorinated Species"),
    ("PFC-31-10", "AR4", 8860.0, "Fully Fluorinated Species"),
    ("PFC-31-10", "AR5", 9200.0, "Fully Fluorinated Species"),
    ("PFC-31-10", "AR6", 10000.0, "Fully Fluorinated Species"),
    ("PFC-41-12", "AR4", 9160.0, "Fully Fluorinated Species"),
    ("PFC-41-12", "AR5", 8550.0, "Fully Fluorinated Species"),
    ("PFC-41-12", "AR6", 9220.0, "Fully Fluorinated Species"),
    ("PFC-51-14", "AR4", 9300.0, "Fully Fluorinated Species"),
    ("PFC-51-14", "AR5", 7910.0, "Fully Fluorinated Species"),
    ("PFC-51-14", "AR6", 8620.0, "Fully Fluorinated Species"),
    ("PFC-61-16", "AR5", 7820.0, "Fully Fluorinated Species"),
    ("PFC-61-16", "AR6", 8410.0, "Fully Fluorinated Species"),
    ("PFC-71-18", "AR5", 7620.0, "Fully Fluorinated Species"),
    ("PFC-71-18", "AR6", 8260.0, "Fully Fluorinated Species"),
    ("PFC-91-18", "AR5", 7190.0, "Fully Fluorinated Species"),
    ("PFC-91-18", "AR6", 7480.0, "Fully Fluorinated Species"),
    ("PFC-1114", "AR5", 0.5, "Fully Fluorinated Species"),
    ("PFC-1114", "AR6", 0.004, "Fully Fluorinated Species"),
    ("PFC-1216", "AR5", 0.5, "Fully Fluorinated Species"),
    ("PFC-1216", "AR6", 0.09, "Fully Fluorinated Species"),
    ("Pentadecafluorotriethylamine", "AR6", 10300.0, "Fully Fluorinated Species"),
    ("Perfluorotripropylamine", "AR6", 9030.0, "Fully Fluorinated Species"),
    ("Heptacosafluorotributylamine", "AR6", 8490.0, "Fully Fluorinated Species"),
    ("Perfluorotripentylamine", "AR6", 7260.0, "Fully Fluorinated Species"),
    ("Heptafluoroisobutyronitrile", "AR6", 2750.0, "Fully Fluorinated Species"),
    ("Trifluoromethylsulfur pentafluoride", "AR4", 17700.0, "Fully Fluorinated Species"),
    ("Trifluoromethylsulfur pentafluoride", "AR5", 17400.0, "Fully Fluorinated Species"),
    ("Trifluoromethylsulfur pentafluoride", "AR6", 18500.0, "Fully Fluorinated Species"),
    ("Sulfuryl fluoride", "AR5", 4090.0, "Fully Fluorinated Species"),
    ("Sulfuryl fluoride", "AR6", 4630.0, "Fully Fluorinated Species"),
    ("Hexafluoro-cyclobutene", "AR6", 126.0, "Fully Fluorinated Species"),
    ("Octafluoro-cyclopentene (Perfluorocyclopentene)", "AR5", 2.0, "Fully Fluorinated Species"),
    ("Octafluoro-cyclopentene (Perfluorocyclopentene)", "AR6", 78.1, "Fully Fluorinated Species"),
    ("1,1,2,2,3,3,4,4,4a,5,5,6,6,7,7,8,8,8a- octadecafluoronaphthalene (Perfluorodecalin (cis))", "AR5", 7240.0, "Fully Fluorinated Species"),
    ("1,1,2,2,3,3,4,4,4a,5,5,6,6,7,7,8,8,8a- octadecafluoronaphthalene (Perfluorodecalin (cis))", "AR6", 7800.0, "Fully Fluorinated Species"),
    ("1,1,2,2,3,3,4,4,4a,5,5,6,6,7,7,8,8,8a- octadecafluoronaphthalene (Perfluorodecalin (trans))", "AR5", 6290.0, "Fully Fluorinated Species"),
    ("1,1,2,2,3,3,4,4,4a,5,5,6,6,7,7,8,8,8a- octadecafluoronaphthalene (Perfluorodecalin (trans))", "AR6", 7120.0, "Fully Fluorinated Species"),
    ("1,1,2,3,4,4-hexafluorobuta-1,3-diene", "AR5", 0.5, "Fully Fluorinated Species"),
    ("1,1,2,3,4,4-hexafluorobuta-1,3-diene", "AR6", 0.004, "Fully Fluorinated Species"),
    ("Octafluoro-1-butene", "AR5", 0.5, "Fully Fluorinated Species"),
    ("Octafluoro-1-butene", "AR6", 0.102, "Fully Fluorinated Species"),
    ("Octafluoro-2-butene", "AR5", 2.0, "Fully Fluorinated Species"),
    ("Octafluoro-2-butene", "AR6", 1.97, "Fully Fluorinated Species"),
    ("CFC-11", "AR4", 4750.0, "Chlorofluorocarbons"),
    ("CFC-11", "AR5", 4660.0, "Chlorofluorocarbons"),
    ("CFC-11", "AR6", 6230.0, "Chlorofluorocarbons"),
    ("CFC-12", "AR4", 10900.0, "Chlorofluorocarbons"),
    ("CFC-12", "AR5", 10200.0, "Chlorofluorocarbons"),
    ("CFC-12", "AR6", 12500.0, "Chlorofluorocarbons"),
    ("CFC-13", "AR4", 14400.0, "Chlorofluorocarbons"),
    ("CFC-13", "AR5", 13900.0, "Chlorofluorocarbons"),
    ("CFC-13", "AR6", 16200.0, "Chlorofluorocarbons"),
    ("CFC-112", "AR6", 4620.0, "Chlorofluorocarbons"),
    ("CFC-112a", "AR6", 3550.0, "Chlorofluorocarbons"),
    ("CFC-113", "AR4", 6130.0, "Chlorofluorocarbons"),
    ("CFC-113", "AR5", 5820.0, "Chlorofluorocarbons"),
    ("CFC-113", "AR6", 6520.0, "Chlorofluorocarbons"),
    ("CFC-113a", "AR6", 3930.0, "Chlorofluorocarbons"),
    ("CFC-114", "AR4", 10000.0, "Chlorofluorocarbons"),
    ("CFC-114", "AR5", 8590.0, "Chlorofluorocarbons"),
    ("CFC-114", "AR6", 9430.0, "Chlorofluorocarbons"),
    ("CFC-114a", "AR6", 7420.0, "Chlorofluorocarbons"),
    ("CFC-115", "AR4", 7370.0, "Chlorofluorocarbons"),
    ("CFC-115", "AR5", 7670.0, "Chlorofluorocarbons"),
    ("CFC-115", "AR6", 9600.0, "Chlorofluorocarbons"),
    ("E-R316c", "AR6", 4230.0, "Chlorofluorocarbons"),
    ("Z-R316c", "AR6", 5660.0, "Chlorofluorocarbons"),
    ("CFC 1112", "AR6", 0.126, "Chlorofluorocarbons"),
    ("CFC 1112a", "AR6", 0.021, "Chlorofluorocarbons"),
    ("HCFC-21", "AR4", 151.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-21", "AR5", 148.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-21", "AR6", 160.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-22", "AR4", 1810.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-22", "AR5", 1760.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-22", "AR6", 1960.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-31", "AR6", 79.4, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-121", "AR6", 58.3, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-122", "AR5", 59.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-122", "AR6", 56.4, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-122a", "AR5", 258.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-122a", "AR6", 245.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-123", "AR4", 77.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-123", "AR5", 79.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-123", "AR6", 90.4, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-123a", "AR5", 370.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-123a", "AR6", 395.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-124", "AR4", 609.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-124", "AR5", 527.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-124", "AR6", 597.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-124a", "AR6", 2070.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-132", "AR6", 122.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-132a", "AR6", 70.4, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-132c", "AR5", 338.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-132c", "AR6", 342.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-133a", "AR6", 388.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-141", "AR6", 46.6, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-141b", "AR4", 725.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-141b", "AR5", 782.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-141b", "AR6", 860.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-142b", "AR4", 2310.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-142b", "AR5", 1980.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-142b", "AR6", 2300.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-225ca", "AR4", 122.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-225ca", "AR5", 127.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-225ca", "AR6", 137.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-225cb", "AR4", 595.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-225cb", "AR5", 525.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFC-225cb", "AR6", 568.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFO-1233zd(E)", "AR6", 3.88, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("HCFO-1233zd(Z)", "AR6", 0.454, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("(e)-1-chloro-2-fluoroethene", "AR6", 0.004, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("(E)-1-chloro-3,3,3-trifluoroprop-1-ene", "AR5", 1.0, "Hydrochlorofluorocarbon (includes unsaturated species)*"),
    ("Methyl chloroform", "AR4", 146.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methyl chloroform", "AR5", 160.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methyl chloroform", "AR6", 161.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Carbon tetrachloride", "AR4", 1400.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Carbon tetrachloride", "AR5", 1730.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Carbon tetrachloride", "AR6", 2200.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methyl chloride", "AR4", 13.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methyl chloride", "AR5", 12.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methyl chloride", "AR6", 5.54, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methylene chloride", "AR4", 9.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methylene chloride", "AR5", 9.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methylene chloride", "AR6", 11.2, "Chlorocarbons and Hydrochlorocarbons"),
    ("Chloroform", "AR4", 31.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Chloroform", "AR5", 16.0, "Chlorocarbons and Hydrochlorocarbons"),
    ("Chloroform", "AR6", 20.6, "Chlorocarbons and Hydrochlorocarbons"),
    ("Chloroethane", "AR6", 0.481, "Chlorocarbons and Hydrochlorocarbons"),
    ("1,2-dichloroethane", "AR5", 0.5, "Chlorocarbons and Hydrochlorocarbons"),
    ("1,2-dichloroethane", "AR6", 1.3, "Chlorocarbons and Hydrochlorocarbons"),
    ("1,1,2-trichloroethene", "AR6", 0.044, "Chlorocarbons and Hydrochlorocarbons"),
    ("1,1,2,2-tetrachloroethene", "AR6", 6.34, "Chlorocarbons and Hydrochlorocarbons"),
    ("2-chloropropane", "AR6", 0.18, "Chlorocarbons and Hydrochlorocarbons"),
    ("1-chlorobutane", "AR6", 0.007, "Chlorocarbons and Hydrochlorocarbons"),
    ("Methyl bromide", "AR4", 5.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Methyl bromide", "AR5", 2.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Methyl bromide", "AR6", 2.43, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Methylene bromide", "AR4", 2.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Methylene bromide", "AR5", 1.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Methylene bromide", "AR6", 1.51, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1201", "AR4", 404.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1201", "AR5", 376.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1201", "AR6", 380.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1202", "AR5", 231.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1202", "AR6", 216.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1211", "AR4", 1890.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1211", "AR5", 1750.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1211", "AR6", 1930.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1301", "AR4", 7140.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1301", "AR5", 6290.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1301", "AR6", 7200.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2301", "AR5", 173.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2301", "AR6", 177.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2311", "AR5", 41.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2311", "AR6", 45.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2401", "AR5", 184.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2401", "AR6", 201.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2402", "AR4", 1640.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2402", "AR5", 1470.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-2402", "AR6", 2170.0, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Tribromomethane", "AR6", 0.25, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Halon-1011", "AR6", 4.74, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("Bromoethane", "AR6", 0.487, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("1,2-dibromoethane", "AR6", 1.02, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("1-bromopropane", "AR6", 0.052, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("2-bromopropane", "AR6", 0.126, "Bromocarbons, Hydrobromocarbons and Halons"),
    ("HFE-125", "AR4", 14900.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-125", "AR5", 12400.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-125", "AR6", 14300.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-134", "AR4", 6320.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-134", "AR5", 5560.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-134", "AR6", 6630.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-143a", "AR4", 756.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-143a", "AR5", 523.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-143a", "AR6", 616.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-227ea", "AR4", 1540.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-227ea", "AR5", 6450.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-227ea", "AR6", 7520.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HCFE-235ca2", "AR5", 583.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HCFE-235ca2", "AR6", 654.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HCFE-235da2", "AR4", 350.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HCFE-235da2", "AR5", 491.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HCFE-235da2", "AR6", 539.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236ea2", "AR4", 989.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236ea2", "AR5", 1790.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236ea2", "AR6", 2590.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236fa", "AR4", 487.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236fa", "AR5", 979.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236fa", "AR6", 1100.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245cb2", "AR4", 708.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245cb2", "AR5", 654.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245cb2", "AR6", 747.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245fa1", "AR4", 286.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245fa1", "AR5", 828.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245fa1", "AR6", 934.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245fa2", "AR4", 659.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245fa2", "AR5", 812.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-245fa2", "AR6", 878.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3,3-pentafluoropropan-1-ol", "AR4", 42.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3,3-pentafluoropropan-1-ol", "AR5", 19.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3,3-pentafluoropropan-1-ol", "AR6", 34.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-254cb1", "AR5", 301.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-254cb1", "AR6", 328.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-254cb2", "AR4", 359.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236ca", "AR5", 4240.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-263mf (HFE-263fb2)", "AR4", 11.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-263mf (HFE-263fb2)", "AR5", 1.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-263mf (HFE-263fb2)", "AR6", 2.06, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-263m1", "AR5", 29.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-263m1", "AR6", 29.2, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-365mcf2", "AR5", 58.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,3-trifluoropropan-1-ol", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,3-trifluoropropan-1-ol", "AR6", 0.62, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-329mcc2", "AR4", 919.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-329mcc2", "AR5", 3070.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-329mcc2", "AR6", 3770.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338mmz1", "AR5", 2620.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338mmz1", "AR6", 3040.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338mcf2", "AR4", 552.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338mcf2", "AR5", 929.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338mcf2", "AR6", 1040.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mmz1", "AR5", 216.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mmz1", "AR6", 195.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mcc3", "AR4", 575.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mcc3", "AR5", 530.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mcc3", "AR6", 576.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mcf2", "AR4", 374.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mcf2", "AR5", 854.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mcf2", "AR6", 963.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347pcf2", "AR4", 580.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347pcf2", "AR5", 889.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347pcf2", "AR6", 980.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mmy1", "AR4", 343.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mmy1", "AR5", 363.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-347mmy1", "AR6", 392.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mec3", "AR4", 101.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mec3", "AR5", 387.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mec3", "AR6", 264.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mff2", "AR5", 17.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mff2", "AR6", 24.4, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcf2", "AR4", 265.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcf2", "AR5", 719.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcf2", "AR6", 831.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcf3", "AR4", 502.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcf3", "AR5", 446.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcf3", "AR6", 484.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcc3", "AR4", 110.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcc3", "AR5", 413.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356pcc3", "AR6", 277.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mmz1", "AR4", 27.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mmz1", "AR5", 14.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-356mmz1", "AR6", 8.13, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-365mcf3", "AR4", 11.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-365mcf3", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-365mcf3", "AR6", 1.6, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-374pc2", "AR4", 557.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-374pc2", "AR5", 627.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-374pc2", "AR6", 12.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("4,4,4-trifluorobutan-1-ol", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("4,4,4-trifluorobutan-1-ol", "AR6", 0.049, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3,4,4,5,5- octafluorocyclopentan-1-ol", "AR5", 13.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3,4,4,5,5- octafluorocyclopentan-1-ol", "AR6", 13.6, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-43-10pccc124", "AR4", 1870.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-43-10pccc124", "AR5", 2820.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-43-10pccc124", "AR6", 3220.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-449s1", "AR5", 421.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-449s1", "AR6", 460.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-449sl", "AR4", 297.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("n-HFE-7100", "AR5", 486.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("n-HFE-7100", "AR6", 544.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("n-HFE-7200", "AR5", 65.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("i-HFE-7100", "AR5", 407.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("i-HFE-7100", "AR6", 437.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-569sf2", "AR4", 59.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-569sf2", "AR5", 57.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-569sf2", "AR6", 60.7, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("i-HFE-7200", "AR5", 44.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("i-HFE-7200", "AR6", 34.3, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-7300", "AR6", 405.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-7500", "AR6", 13.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236ca12", "AR4", 2800.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236ca12", "AR5", 5350.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-236ca12", "AR6", 6060.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338pcc13", "AR4", 1500.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338pcc13", "AR5", 2910.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-338pcc13", "AR6", 3320.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1,3,3,3-hexafluoropropan-2-ol", "AR4", 195.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1,3,3,3-hexafluoropropan-2-ol", "AR5", 182.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1,3,3,3-hexafluoropropan-2-ol", "AR6", 206.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-02", "AR5", 2730.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-02", "AR6", 5730.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-03", "AR5", 2850.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-03", "AR6", 5350.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-20", "AR5", 5300.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-21", "AR5", 3890.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-30", "AR5", 7330.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Fluroxene", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Fluroxene", "AR6", 0.058, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,2,2-tetrafluoro-1-(fluoromethoxy)ethane", "AR5", 871.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2-ethoxy-3,3,4,4,5-pentafluorotetrahydro-2,5-bis[1,2,2,2-tetrafluoro-1- (trifluoromethyl)ethyl]furan", "AR5", 56.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2-ethoxy-3,3,4,4,5-pentafluorotetrahydro-2,5-bis[1,2,2,2-tetrafluoro-1- (trifluoromethyl)ethyl]furan", "AR6", 48.7, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Difluoro(methoxy)methane", "AR5", 144.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Difluoro(methoxy)methane", "AR6", 136.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG'-01", "AR5", 222.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG'-01", "AR6", 202.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG'-02", "AR5", 236.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG'-02", "AR6", 229.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG'-03", "AR5", 221.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG'-03", "AR6", 219.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-329me3", "AR5", 4550.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-329me3", "AR6", 4390.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,4,4,5,5,6,6,7,7,7- undecafluoroheptan-1-ol", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,4,4,5,5,6,6,7,7,7- undecafluoroheptan-1-ol", "AR6", 0.533, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,9,9,9- pentadecafluorononan-1-ol", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,9,9,9- pentadecafluorononan-1-ol", "AR6", 0.449, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11, 11,11-nonadecafluoroundecan-1-ol", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11, 11,11-nonadecafluoroundecan-1-ol", "AR6", 0.273, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2-chloro-1,1,2-trifluoro-1-methoxyethane", "AR5", 122.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2-chloro-1,1,2-trifluoro-1-methoxyethane", "AR6", 136.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("PFPMIE", "AR4", 10300.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("PFPMIE", "AR5", 9710.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("PFPMIE", "AR6", 10300.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-216", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HFE-216", "AR6", 0.01, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluoroethyl formate", "AR5", 580.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluoroethyl formate", "AR6", 597.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,2-trifluoroethyl formate", "AR5", 33.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,2-trifluoroethyl formate", "AR6", 54.8, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Formic acid;1,1,1,3,3,3-hexafluoro-propan-2-ol", "AR6", 269.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Ethenyl 2,2,2-trifluoroacetate", "AR6", 0.008, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Ethyl 2,2,2-trifluoroacetate", "AR5", 1.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Ethyl 2,2,2-trifluoroacetate", "AR6", 1.58, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Prop-2-enyl-2,2,2-trifluoroacetate", "AR6", 0.007, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Methyl 2,2,2-trifluoroacetate", "AR5", 52.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Methyl 2,2,2-trifluoroacetate", "AR6", 82.3, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3,4,4,4-heptafluorobutan-1-ol", "AR5", 34.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3,4,4,4-heptafluorobutan-1-ol", "AR6", 36.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,2-trifluoro-2-(trifluoromethoxy)-ethane", "AR5", 1240.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,2-trifluoro-2-(trifluoromethoxy)-ethane", "AR6", 1260.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1-ethoxy-1,1,2,3,3,3- hexafluoropropane", "AR5", 23.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1-ethoxy-1,1,2,3,3,3- hexafluoropropane", "AR6", 26.4, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1,2,2,3,3-heptafluoro-3-(1,2,2,2- tetrafluoroethoxy)propane", "AR5", 6490.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1,2,2,3,3-heptafluoro-3-(1,2,2,2- tetrafluoroethoxy)propane", "AR6", 6630.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3-tetrafluoropropan-1-ol", "AR5", 13.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,3-tetrafluoropropan-1-ol", "AR6", 14.4, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,4,4,4-hexafluorobutan-1-ol", "AR5", 17.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,3,4,4,4-hexafluorobutan-1-ol", "AR6", 30.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,2,2-tetrafluoro-3-methoxypropane", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,2,2-tetrafluoro-3-methoxypropane", "AR6", 1.68, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1,2,2,4,5,5,5-nonafluoro-4-(trifluoromethyl)pentan-3-one", "AR6", 0.114, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1-ethoxy-1,1,2,2,3,3,3- heptafluoropropane", "AR5", 61.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Fluoro(methoxy)methane", "AR5", 13.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Fluoro(fluoromethoxy)methane", "AR5", 130.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Difluoro(fluoromethoxy)methane", "AR5", 617.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Trifluoro(fluoromethoxy)methane", "AR5", 751.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Trifluoromethyl formate", "AR5", 588.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluoropropyl formate", "AR5", 376.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluorobutyl formate", "AR5", 392.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,3-trifluoropropyl formate", "AR5", 17.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,2,2,2-tetrafluoroethyl formate", "AR5", 470.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1,3,3,3-hexafluoropropan-2-yl formate", "AR5", 333.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluorobutyl acetate", "AR5", 2.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluoropropyl acetate", "AR5", 2.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluoroethyl acetate", "AR5", 2.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Trifluoromethyl acetate", "AR5", 2.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Methyl carbonofluoridate", "AR5", 95.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1-difluoroethyl carbonofluoridate", "AR5", 27.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1-difluoroethyl 2,2,2-trifluoroacetate", "AR5", 31.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,2-trifluoroethyl 2,2,2-trifluoroacetate", "AR5", 7.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Methyl 2,2-difluoroacetate", "AR5", 3.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Difluoromethyl 2,2,2-trifluoroacetate", "AR5", 27.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Perfluoro-2-methyl-3-pentanone", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1'-Oxybis[2-(difluoromethoxy)-1,1,2,2-tetrafluoroethane", "AR5", 4920.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,3,3,4,4,6,6,7,7,9,9,10,10,12,12- hexa-decafluoro-2,5,8,11-Tetraoxadodecane", "AR5", 4490.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,3,3,4,4,6,6,7,7,9,9,10,10,12,12, 13,13,15,15-eicosafluoro-2,5,8,11,14-pentaoxapentadecane", "AR5", 3630.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,3-trifluoropropanal", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("3,3,3-trifluoropropanal", "AR6", 0.025, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2-fluoroethanol", "AR5", 0.5, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2-fluoroethanol", "AR6", 0.53, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2-difluoroethanol", "AR5", 3.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2-difluoroethanol", "AR6", 6.18, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,2-trifluoroethanol", "AR5", 20.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("2,2,2-trifluoroethanol", "AR6", 35.7, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("HG-04", "AR6", 4380.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Methyl-perfluoro-heptene-ethers", "AR6", 15.1, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1-trifluoro-propan-2-one", "AR6", 0.09, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1,1,1-trifluoro-butan-2-one", "AR6", 0.095, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("1-chloro-2-ethen-oxyethane", "AR6", 0.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
    ("Octafluoro-oxolane", "AR6", 13900.0, "Halogenated Alcohols, Ethers, Furans, Aldehydes and Ketones"),
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


def load_c_matrix_long(
    xlsx_path: str,
    model_version: str,
    index_to_code: dict[int, str],
) -> pd.DataFrame:
    """
    Load the C (characterization) matrix from the workbook.
    C has flow names as columns (e.g. "Methane/emission/air/kg") and one row per impact indicator;
    values are characterization factors (e.g. GWP). Used to determine GWP per flow.
    Sheet layout: header = flow names; first column may be indicator code; data = factors.
    """
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_C, header=0)
    all_cols = list(df.columns)
    # First column may be indicator code or index; rest are flow names
    first_col = all_cols[0]
    flow_cols = [c for c in all_cols if c != first_col]
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        # Indicator: from first column if it looks like a code (not a number), else from row index
        ind_val = row.get(first_col)
        if pd.notna(ind_val):
            s = str(ind_val).strip()
            if s and not s.replace(".", "").replace("-", "").isdigit():
                indicator_code = s
            else:
                indicator_code = index_to_code.get(int(i))
        else:
            indicator_code = index_to_code.get(int(i))
        if indicator_code is None:
            continue
        for flow in flow_cols:
            val = row.get(flow)
            if pd.isna(val):
                continue
            try:
                value = float(val)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "model_version": model_version,
                    "indicator_code": indicator_code,
                    "flow": str(flow).strip(),
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


def load_ipcc_ar_gwp(client) -> None:
    """
    Load IPCC AR GWP reference data into ipcc_ar_gwp (gas_name, ar_version, gwp_value, category, optional gwp_display).
    Uses upsert so the table can be refreshed on each run. When a row has 5 elements, gwp_display (e.g. "<1") is used for display.
    """
    records = []
    for row in IPCC_AR_GWP_ROWS:
        gas_name, ar_version, gwp_value, category = row[0], row[1], row[2], row[3]
        # Strip trailing " a", " b", " c" from gas names (legacy Excel suffixes)
        for suffix in (" a", " b", " c"):
            if gas_name.endswith(suffix):
                gas_name = gas_name[: -len(suffix)]
                break
        gwp_display = row[4] if len(row) >= 5 else None
        records.append({
            "gas_name": gas_name,
            "ar_version": ar_version,
            "gwp_value": gwp_value,
            "gwp_display": gwp_display,
            "category": category,
        })
    try:
        # Remove legacy rows with trailing " a", " b", " c" in gas_name so they don't persist after we normalized names
        for suffix in (" a", " b", " c"):
            client.table("ipcc_ar_gwp").delete().like("gas_name", f"%{suffix}").execute()
        client.table("ipcc_ar_gwp").upsert(
            records,
            on_conflict="gas_name,ar_version",
            ignore_duplicates=False,
        ).execute()
    except APIError as e:
        if e.code == "PGRST205":
            print("  Skipped ipcc_ar_gwp (table not in Supabase yet). Run supabase_ipcc_ar_gwp.sql to create it.")
        else:
            raise


def delete_model_version(client, model_version: str) -> None:
    for table in ("impacts", "c", "rho", "sector_crosswalk", "commodities_meta", "indicators", "model_metadata"):
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

    print("Loading ipcc_ar_gwp...")
    load_ipcc_ar_gwp(client)
    print("  Table 'ipcc_ar_gwp' updated.\n")

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

    print("Loading C (characterization / GWP)...")
    try:
        c_df = load_c_matrix_long(xlsx_path, model_version, index_to_code)
        insert_in_batches(client, "c", c_df)
        print(f"  Table 'c' updated: {len(c_df)} rows inserted (flow × indicator factors).\n")
    except Exception as e:
        print(f"  Skipped C: {e}\n")

    print("Done. All tables updated.")


if __name__ == "__main__":
    main()
