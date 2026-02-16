-- Run this in the Supabase SQL Editor to create the IPCC AR GWP reference table.
-- The ETL script (load_useeio.py) populates it so you can look up GWP by gas and AR version.

create table if not exists ipcc_ar_gwp (
  gas_name text not null,
  ar_version text not null,
  gwp_value numeric,
  gwp_display text,
  category text,
  primary key (gas_name, ar_version)
);

comment on table ipcc_ar_gwp is 'IPCC Assessment Report GWP factors for GHG; used to resolve GWP by gas and AR version.';
comment on column ipcc_ar_gwp.gas_name is 'Gas name (e.g. Methane (non-fossil), Nitrous oxide).';
comment on column ipcc_ar_gwp.ar_version is 'IPCC AR version: AR4, AR5, or AR6.';
comment on column ipcc_ar_gwp.gwp_value is 'Global warming potential (CO2-equivalent). NULL when source was e.g. "<1"; use gwp_display for display.';
comment on column ipcc_ar_gwp.gwp_display is 'When source was e.g. "<1", use this for display instead of gwp_value.';
comment on column ipcc_ar_gwp.category is 'Category (e.g. Major GHG).';

-- If table already exists without gwp_display, run:
-- alter table ipcc_ar_gwp add column if not exists gwp_display text;

-- If table already exists with gwp_value not null (to allow blank for "<1" gases), run:
-- alter table ipcc_ar_gwp alter column gwp_value drop not null;
