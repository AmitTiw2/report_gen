operator_inp=$1
if [ -z "$operator_inp" ]; then
  echo "USAGE: $0 <operator_name>"
  exit 1
fi


mkdir -p $HOME/workspace/logs

run_func() {
    local operator_name="$1"
    echo "Processing operator: $operator_name"
    bs_log_pfn="$HOME/workspace/logs/Part2-${operator_name}"`date +"%d-%b-%Y-%H-%M-%S"`".log"
    echo $bs_log_pfn
    ./c_populate_operator.sh $operator_name >$bs_log_pfn 2>&1
}

run_func $operator_inp

#target="first bigdans caliberocala championxpresscw gateexpress luvcarwash magnolia mrcleancw riptideneuse splash thoroughbredcw tsunamicarwash woodys washu zips"
# target="washu"
# for operator in $target
# do
#     run_func $operator
# done