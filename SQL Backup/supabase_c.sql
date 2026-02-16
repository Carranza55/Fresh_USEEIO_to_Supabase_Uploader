-- Run this in the Supabase SQL Editor to create the C (characterization) matrix table.
-- The ETL (load_useeio.py) loads sheet "C" from the workbook: flow columns and one row per
-- impact indicator; values are characterization factors (e.g. GWP) used to determine GWP per flow.

create table if not exists c (
  model_version text not null,
  indicator_code text not null,
  flow text not null,
  value numeric not null,
  primary key (model_version, indicator_code, flow)
);

comment on table c is 'Characterization matrix C: indicator × flow factors (e.g. GWP per flow), loaded from workbook sheet C.';
comment on column c.flow is 'Flow identifier (e.g. Methane/emission/air/kg, HFC-134a/emission/air/kg).';
comment on column c.value is 'Characterization factor (e.g. GWP for that flow).';
