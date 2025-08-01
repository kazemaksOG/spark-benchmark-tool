set -e

EVENT_DIR=/var/scratch/$USER/eventlogs
OUTPUT_DIR=./target/bench_outputs
LOG_OUTPUT=./output.txt
DATE=$(date '+%Y-%m-%d')
RESULT_NAME="results_$DATE.tar.gz"

echo "Compressing results..."

tar -caf $RESULT_NAME $LOG_OUTPUT $EVENT_DIR $OUTPUT_DIR

#echo "deleting $OUTPUT_DIR and $EVENT_DIR"
rm -r $EVENT_DIR/app*
rm $OUTPUT_DIR/benc*
















