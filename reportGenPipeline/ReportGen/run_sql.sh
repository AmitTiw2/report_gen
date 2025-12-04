#!/usr/bin/env bash

folder_sql=$1
if [ -z "$folder_sql" ]; then
  echo "USAGE: $0 b_convert_datatypes/create_ts.sql"
  echo "EXAMPLE: $0 <folder_sql>"
  exit 1
fi

HOST="qv-repgendev-psql1.postgres.database.azure.com"
USER="sondbadmin"
DBNAME="reportgen"

echo "=== Starting INDEX phase for operator: $folder_sql ==="
echo "Log file: $LOG"
echo "Started at: $(date)"
echo

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

run_sql "$folder_sql"     ~/workspace/ReportGen/$folder_sql


echo "Completed INDEX phase for $folder_sql at: $(date)"
