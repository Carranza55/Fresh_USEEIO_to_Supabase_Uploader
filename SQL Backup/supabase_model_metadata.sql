-- Run this in the Supabase SQL Editor to create the model_metadata table.
-- The ETL script (load_useeio.py) will populate it with economic_year and
-- satellite year range; your UI can select the row where is_active = true
-- to build the Methodology paragraph.

create table if not exists model_metadata (
  model_version text primary key,
  economic_year integer,
  satellite_year_min integer,
  satellite_year_max integer,
  is_active boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

comment on table model_metadata is 'One row per USEEIO model version; economic and satellite year range plus active flag for UI.';
comment on column model_metadata.economic_year is 'Economic year from the demands sheet.';
comment on column model_metadata.satellite_year_min is 'Minimum year from Rho (price deflator) columns.';
comment on column model_metadata.satellite_year_max is 'Maximum year from Rho (price deflator) columns.';
comment on column model_metadata.is_active is 'When true, UI uses this row for the active methodology.';

-- Optional: trigger to keep updated_at current
create or replace function set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists model_metadata_updated_at on model_metadata;
create trigger model_metadata_updated_at
  before update on model_metadata
  for each row
  execute function set_updated_at();
