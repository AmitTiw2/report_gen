#!/usr/bin/env bash

operator_name=$1
if [ -z "$operator_name" ]; then
  echo "USAGE: $0 <operator_name>"
  exit 1
fi

LOG="index_${operator_name}_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

# export PGHOST="qv-repgendev-psql1.postgres.database.azure.com"
# export PGPORT="5432"
# export PGUSER="sondbadmin"
# export PGDATABASE="reportgen"

HOST="qv-repgendev-psql1.postgres.database.azure.com"
USER="sondbadmin"
DBNAME="reportgen"

echo "=== Starting INDEX phase for operator: $operator_name ==="
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

# run_sql "check_index.sql" ~/workspace/ReportGen/d_indexes/indexes_first.sql
# run_sql "indexes_bigdans.sql" ~/workspace/ReportGen/d_indexes/indexes_bigdans.sql
# run_sql "indexes_caliberocala.sql" ~/workspace/ReportGen/d_indexes/indexes_caliberocala.sql
# run_sql "indexes_championxpresscw.sql" ~/workspace/ReportGen/d_indexes/indexes_championxpresscw.sql
# run_sql "indexes_gateexpress.sql" ~/workspace/ReportGen/d_indexes/indexes_gateexpress.sql
# run_sql "indexes_luvcarwash.sql" ~/workspace/ReportGen/d_indexes/indexes_luvcarwash.sql
# run_sql "indexes_magnolia.sql" ~/workspace/ReportGen/d_indexes/indexes_magnolia.sql
# run_sql "indexes_mrcleancw.sql" ~/workspace/ReportGen/d_indexes/indexes_mrcleancw.sql
# run_sql "indexes_riptideneuse.sql" ~/workspace/ReportGen/d_indexes/indexes_riptideneuse.sql
# run_sql "indexes_splash.sql" ~/workspace/ReportGen/d_indexes/indexes_splash.sql
# run_sql "indexes_thoroughbredcw.sql" ~/workspace/ReportGen/d_indexes/indexes_thoroughbredcw.sql
# run_sql "indexes_tsunamicarwash.sql" ~/workspace/ReportGen/d_indexes/indexes_tsunamicarwash.sql
# run_sql "indexes_woodys.sql" ~/workspace/ReportGen/d_indexes/indexes_woodys.sql
# run_sql "indexes_washu.sql" ~/workspace/ReportGen/d_indexes/indexes_washu.sql
run_sql "indexes_zips.sql" ~/workspace/ReportGen/d_indexes/indexes_zips.sql


echo "Completed INDEX phase for $operator_name at: $(date)"
