#!/usr/bin/env bash

operator_name=$1
if [ -z "$operator_name" ]; then
  echo "USAGE: $0 <operator_name>"
  exit 1
fi

LOG="run_${operator_name}_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

HOST="<host>"
USER="<username>"
DBNAME="<database>"

echo "=== Starting run for operator: $operator_name ==="
echo "Log file: $LOG"
echo "Started at: $(date)"
echo

next_table_name="new_car_wash_${operator_name}"

# --- helper to measure time for SQL files ---
run_sql() {
  local description="$1"
  local sql_file="$2"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] START: $description"
  local start_time=$(date +%s)
  psql -h "$HOST" -U "$USER" -d "$DBNAME" --set=ON_ERROR_STOP=1 -f "$sql_file"
  local end_time=$(date +%s)
  local elapsed=$(( end_time - start_time ))
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] DONE: $description (took ${elapsed}s)"
  echo
}

# --- Step 1: Rename to active name ---
echo "STEP 1: Renaming ${next_table_name} → car_wash_count"
start_time=$(date +%s)
psql -h "$HOST" -U "$USER" -d "$DBNAME" --set=ON_ERROR_STOP=1 \
  -c "ALTER TABLE ${next_table_name} RENAME TO car_wash_count;"
end_time=$(date +%s)
echo "TIME TAKEN: $(( end_time - start_time ))s"
echo

# --- Transformation steps ---
run_sql "b_conver_datatypes/create_ts.sql" ~/workspace/ReportGen/b_conver_datatypes/create_ts.sql
run_sql "b_conver_datatypes/sales_int.sql" ~/workspace/ReportGen/b_conver_datatypes/sales_int.sql
run_sql "c_new_columns/retail_or_mem.sql"  ~/workspace/ReportGen/c_new_columns/retail_or_mem.sql
run_sql "c_new_columns/gross_revenue.sql"  ~/workspace/ReportGen/c_new_columns/gross_revenue.sql

# --- Index creation & validation ---
run_sql "d_indexes/indexes.sql"      ~/workspace/ReportGen/d_indexes/indexes.sql
run_sql "d_indexes/check_index.sql"  ~/workspace/ReportGen/d_indexes/check_index.sql

# --- Star schema population ---
run_sql "f_populate_star/a_site_client.sql"  ~/workspace/ReportGen/f_populate_star/a_site_client.sql
run_sql "f_populate_star/b_quarter_year.sql" ~/workspace/ReportGen/f_populate_star/b_quarter_year.sql
run_sql "f_populate_star/c_count.sql"        ~/workspace/ReportGen/f_populate_star/c_count.sql
run_sql "f_populate_star/d_revenue.sql"      ~/workspace/ReportGen/f_populate_star/d_revenue.sql

# --- Step 9: Rename back to original name ---
echo "STEP 9: Renaming car_wash_count → ${next_table_name}"
start_time=$(date +%s)
psql -h "$HOST" -U "$USER" -d "$DBNAME" --set=ON_ERROR_STOP=1 \
  -c "ALTER TABLE car_wash_count RENAME TO ${next_table_name};"
end_time=$(date +%s)
echo "TIME TAKEN: $(( end_time - start_time ))s"
echo

echo "Completed at: $(date)"
echo "=== Finished run for $operator_name ==="
