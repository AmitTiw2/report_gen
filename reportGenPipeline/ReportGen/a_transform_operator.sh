#!/usr/bin/env bash

operator_name=$1
if [ -z "$operator_name" ]; then
  echo "USAGE: $0 <operator_name>"
  exit 1
fi

HOST="qv-repgendev-psql1.postgres.database.azure.com"
USER="sondbadmin"
DBNAME="reportgen"

# export PGHOST="qv-repgendev-psql1.postgres.database.azure.com"
# export PGPORT="5432"
# export PGUSER="sondbadmin"
# export PGDATABASE="reportgen"

bs_log_pfn="$HOME/workspace/logs/Count_Revenue-${operator_name}"`date +"%d-%b-%Y-%H-%M-%S"`".log"

echo "=== Starting TRANSFORM for operator: $operator_name ==="
echo "Started at: $(date)"
echo $bs_log_pfn

next_table_name="new_car_wash_${operator_name}"

# Helper function to measure time
run_sql() {
  local description="$1"
  local sql_file="$2"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] START: $description"
  local start_time=$(date +%s)
  psql -h "$HOST" -U "$USER" -d "$DBNAME" --set=ON_ERROR_STOP=1 -f "$sql_file"
  local end_time=$(date +%s)
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] DONE: $description (took $((end_time - start_time))s)"
  echo
}

echo "STEP 1: Renaming ${next_table_name} → car_wash_count"
start_time=$(date +%s)
psql -h "$HOST" -U "$USER" -d "$DBNAME" --set=ON_ERROR_STOP=1 \
  -c "ALTER TABLE ${next_table_name} RENAME TO car_wash_count;"
end_time=$(date +%s)
echo "TIME TAKEN: $((end_time - start_time))s"
echo

# run_sql "create_ts.sql"    ~/workspace/ReportGen/b_conver_datatypes/create_ts.sql
# run_sql "sales_int.sql"    ~/workspace/ReportGen/b_conver_datatypes/sales_int.sql
run_sql "retail_or_mem.sql" ~/workspace/ReportGen/c_new_columns/retail_or_mem.sql
run_sql "gross_revenue.sql" ~/workspace/ReportGen/c_new_columns/gross_revenue.sql

echo "STEP 9: Renaming car_wash_count → ${next_table_name}"
start_time=$(date +%s)
psql -h "$HOST" -U "$USER" -d "$DBNAME" --set=ON_ERROR_STOP=1 \
  -c "ALTER TABLE car_wash_count RENAME TO ${next_table_name};"
end_time=$(date +%s)
echo "TIME TAKEN: $((end_time - start_time))s"
echo

echo "Completed TRANSFORM phase for $operator_name at: $(date)"
