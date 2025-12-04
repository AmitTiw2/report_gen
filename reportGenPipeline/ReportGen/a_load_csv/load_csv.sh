#!/usr/bin/env bash
set -euo pipefail

# --- Connection (password from ~/.pgpass) ---
export PGHOST="your-db-endpoint.example.com"
export PGPORT="5432"
export PGUSER="your_user"
export PGDATABASE="your_db"
# Optional: TLS
# export PGSSLMODE="require"

# --- Input CSV (arg 1) ---
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /path/to/file.csv"
  exit 1
fi
CSV="$1"
if [[ ! -f "$CSV" ]]; then
  echo "File not found: $CSV"
  exit 1
fi

TABLE=public.new_car_wash_mrcleancw   # hardcoded per your request

echo "CSV     : $CSV"
echo "Table   : $TABLE"
echo "Started : $(date)"

# NOTE: we pass only the CSV path as a psql variable; table name is literal.
psql --no-psqlrc -v ON_ERROR_STOP=1 -v csvfile="$CSV" <<'PSQL'
\timing on
\conninfo
SET statement_timeout = 0;
SET synchronous_commit = off;

-- Create staging table (UNLOGGED)
CREATE UNLOGGED TABLE IF NOT EXISTS public.new_car_wash_mrcleancw (
  client_id TEXT, transaction_id TEXT, created_date_utc TEXT, created_date TEXT, modified_date TEXT,
  complete_date TEXT, site_id TEXT, billing_site_id TEXT, site_name TEXT, site_timezone TEXT,
  customer_id TEXT, wash_type TEXT, trans_id TEXT, trans_sales_device_id TEXT, trans_item_id TEXT,
  trans_item_sales_device_id TEXT, vehicle_id TEXT, vehicle_active TEXT, trans_type_name TEXT,
  trans_state_name TEXT, house_account_name TEXT, house_account_number TEXT,
  recurring_membership_offered TEXT, recurring_membership_accepted TEXT, trans_item_state_name TEXT,
  item_department_name TEXT, item_department_category_name TEXT, item_sku TEXT, item_name TEXT,
  item_quantity TEXT, item_amount TEXT, item_tax_amount TEXT, promo_amount TEXT, tendered_amount TEXT,
  is_recurring_redemption TEXT, is_prepaid_wash TEXT, discount_quantity TEXT, is_house_account_payment TEXT,
  trans_reason_code_category TEXT, trans_reason_code_description TEXT, item_has_tax TEXT, is_vacuum_redemption TEXT
);

TRUNCATE public.new_car_wash_mrcleancw;

-- Load CSV (NO comments; NO exotic escapes)
\copy public.new_car_wash_mrcleancw (
  client_id,transaction_id,created_date_utc,created_date,modified_date,complete_date,
  site_id,billing_site_id,site_name,site_timezone,customer_id,wash_type,trans_id,
  trans_sales_device_id,trans_item_id,trans_item_sales_device_id,vehicle_id,vehicle_active,
  trans_type_name,trans_state_name,house_account_name,house_account_number,
  recurring_membership_offered,recurring_membership_accepted,trans_item_state_name,
  item_department_name,item_department_category_name,item_sku,item_name,item_quantity,
  item_amount,item_tax_amount,promo_amount,tendered_amount,is_recurring_redemption,
  is_prepaid_wash,discount_quantity,is_house_account_payment,trans_reason_code_category,
  trans_reason_code_description,item_has_tax,is_vacuum_redemption
) FROM :'csvfile'
  WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '\N');

ALTER TABLE public.new_car_wash_mrcleancw SET LOGGED;
ANALYZE public.new_car_wash_mrcleancw;
PSQL

echo "Finished: $(date)"
