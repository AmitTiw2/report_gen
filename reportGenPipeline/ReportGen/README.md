### .pgpass entry format
hostname:port:database:username:password

### How to run .sql from command
nohup psql -h <host> -U <username> -d <database> -f /path/to/your/script.sql > /path/to/logfile.log 2>&1 &


nohup bash process_operator.sh clearwater > /dev/null 2>&1 &


first
bigdans
caliberocala
championxpresscw
gateexpress
luvcarwash
magnolia
mrcleancw
riptideneuse
splash
thoroughbredcw
tsunamicarwash
woodys
washu
zips

## For Creation of Index on all the new_car_wash tables
nohup b_index_operator.sh &

# Process to be followed for every operator

## change target
1.  nohup ./counts_revenue.sh

## Comment and un-comment
2.  ./run_sql indexes_washu.sql >$HOME/workspace/logs/indexes_washu.log 2>&1 &

## chnge target
3.  nohup ./operator_part_2.sh


###

nohup ./run_all.sh