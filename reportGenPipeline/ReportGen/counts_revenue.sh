operator_inp=$1
if [ -z "$operator_inp" ]; then
  echo "USAGE: $0 <operator_name>"
  exit 1
fi

mkdir -p $HOME/workspace/logs


run_func() {
    local operator_name="$1"
    echo "Processing operator: $operator_name"
    bs_log_pfn="$HOME/workspace/logs/Count_Revenue-${operator_name}"`date +"%d-%b-%Y-%H-%M-%S"`".log"
    echo $bs_log_pfn
    ./a_transform_operator.sh $operator_name >$bs_log_pfn 2>&1
}


run_func $operator_inp


# ./a_transform_operator.sh first
# ./a_transform_operator.sh bigdans >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh caliberocala >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh championxpresscw >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh gateexpress >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh luvcarwash >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh magnolia >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh mrcleancw >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh riptideneuse >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh splash >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh thoroughbredcw >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh tsunamicarwash >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh woodys >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh washu >$bs_log_pfn 2>&1 &
# ./a_transform_operator.sh zips >$bs_log_pfn 2>&1 &

#target="first bigdans caliberocala championxpresscw gateexpress luvcarwash magnolia mrcleancw riptideneuse splash thoroughbredcw tsunamicarwash woodys washu zips"

