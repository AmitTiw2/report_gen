chmod +x load_csv.sh
nohup ./load_csv.sh /path/to/washu.csv > load_washu.log 2>&1 &
# tail -f load_washu.log
