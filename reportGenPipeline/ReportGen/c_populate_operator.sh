#!/usr/bin/env bash

operator_name=$1
if [ -z "$operator_name" ]; then
  echo "USAGE: $0 <operator_name>"
  exit 1
fi


HOST="qv-repgendev-psql1.postgres.database.azure.com"
USER="sondbadmin"
DBNAME="reportgen"

echo "=== Starting POPULATE phase for operator: $operator_name ==="
echo "Started at: $(date)"
echo

next_table_name="new_car_wash_${operator_name}"

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

run_sql "a_site_client.sql"  ~/workspace/ReportGen/f_populate_star/a_site_client.sql
run_sql "b_quarter_year.sql" ~/workspace/ReportGen/f_populate_star/b_quarter_year.sql
run_sql "c_count.sql"        ~/workspace/ReportGen/f_populate_star/c_count.sql
run_sql "d_revenue.sql"      ~/workspace/ReportGen/f_populate_star/d_revenue.sql

echo "STEP 9: Renaming car_wash_count → ${next_table_name}"
start_time=$(date +%s)
psql -h "$HOST" -U "$USER" -d "$DBNAME" --set=ON_ERROR_STOP=1 \
  -c "ALTER TABLE car_wash_count RENAME TO ${next_table_name};"
end_time=$(date +%s)
echo "TIME TAKEN: $((end_time - start_time))s"
echo

echo "Completed POPULATE phase for $operator_name at: $(date)"
