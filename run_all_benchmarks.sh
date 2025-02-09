#!/bin/bash

# Usage: source ./script <MASTER_URL> <RESERVATION_ID>


if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo "Hey, you should source this script, not execute it!"
    exit 1
fi



# Env
module load java/jdk-17
module load prun

export SPARK_HOME="/var/scratch/$USER/frameworks/spark-custom"

# Spark input variables
SCHEDULER_DIR="/var/scratch/$USER"
JOB_DIR="/var/scratch/$USER/performance_test"
WORKLOAD_DIR="$JOB_DIR/configs/workloads"
INDIVIDUAL_WORKLOAD_DIR="$JOB_DIR/configs/individual"
SPARK_JOB_FILE="$JOB_DIR/target/performance_test-1.0-SNAPSHOT.jar"
DEPLOY_MODE="client"
MAIN_CLASS="BenchRunner"  


# checking for correct input varibales

# we assume java 17 to be everywhere
if [ ! -d "/cm/shared/package/java/jdk-17" ]; then 
   echo "java 17 module not loaded or changed location, check whereis java"
   return 1
fi

if [ ! -d "$SPARK_HOME" ]; then
   echo "'$SPARK_HOME' directory does not exist"
   return 1
fi

if [ ! -d "$SCHEDULER_DIR" ]; then
   echo "'$SCHEDULER_DIR' directory does not exist"
   return 1
fi

if [ ! -d "$JOB_DIR" ]; then
   echo "'$JOB_DIR' directory does not exist"
   return 1
fi


if [ ! -d "$WORKLOAD_DIR" ]; then
    echo "Error: Directory '$WORKLOAD_DIR' does not exist."
    return 1
fi

if [ ! -f "$SPARK_JOB_FILE" ]; then
    echo "Error: file '$SPARK_JOB_FILE' does not exist."
    return 1
fi




# scheduler configs
CUSTOM_FAIR=(
    "--conf spark.scheduler.mode=CUSTOM"
    "--conf spark.customSchedulerContainer=UserFairSchedulerContainer"
    "--conf spark.driver.extraClassPath=$SCHEDULER_DIR/UserFairScheduler/target/UserFairScheduler-1.0-SNAPSHOT.jar"
)

CUSTOM_RANDOM=(
   "--conf spark.scheduler.mode=CUSTOM"
   "--conf spark.customSchedulerContainer=RandomSchedulerContainer"
   "--conf spark.driver.extraClassPath=$SCHEDULER_DIR/RandomScheduler/target/RandomScheduler-1.0-SNAPSHOT.jar"
)

FAIR=("--conf spark.scheduler.mode=FAIR")


FIFO=(
    "--conf spark.scheduler.mode=FIFO"
)


MASTER=$1
RESERVATION_ID=$2

if [ ! -n "$MASTER" ]; then
    echo "No master URL provided, Usage: source ./script <MASTER_URL> <RESERVATION_ID>"
    return 1
fi

if [ ! -n "$RESERVATION_ID" ]; then
    echo "No reservation provided, Usage: source ./script <MASTER_URL> <RESERVATION_ID>"
    return 1
fi

run_spark_job() {
    local scheduler_name=$1
    shift
    local file=$1
    shift
    local config_array=$@

    echo "=============================================="
    echo "Running Spark job with $scheduler_name scheduler and config: $file"
    echo "=============================================="
    query="--deploy-mode $DEPLOY_MODE --master $MASTER $config_array --class $MAIN_CLASS $SPARK_JOB_FILE $file $scheduler_name"
    echo "running job: spark-submit $query"
    $SPARK_HOME/bin/spark-submit $query

    if [ $? -eq 0 ]; then
        echo "$scheduler_name scheduler job completed successfully."
    else
        echo "$scheduler_name scheduler job failed."
        return 1
    fi
}

echo "Starting workloads"
for file in "$WORKLOAD_DIR"/*; do
    # Ensure it is a regular file
    if [ -f "$file" ]; then
        echo "running spark on $file"
        run_spark_job "CUSTOM_RANDOM" $file ${CUSTOM_RANDOM[@]}
        run_spark_job "CUSTOM_FAIR" $file ${CUSTOM_FAIR[@]}
        run_spark_job "FAIR" $file $FAIR
        run_spark_job "FIFO" $file $FIFO
    fi
done

if [ -d "$INDIVIDUAL_WORKLOAD_DIR" ]; then
    echo "Running individual workloads from $INDIVIDUAL_WORKLOAD_DIR"
    for file in "$INDIVIDUAL_WORKLOAD_DIR"/*; do
        # Ensure it is a regular file
        if [ -f "$file" ]; then
            echo "running spark on $file"
            run_spark_job "FIFO" $file $FIFO
        fi
    done
else
    echo "No directory for individual workloads found: $INDIVIDUAL_WORKLOAD_DIR"
fi


echo "=============================================="
echo "All Spark jobs completed."
echo "=============================================="


echo "Killing the reservation $RESERVATION_ID"
preserve -c $RESERVATION_ID
