# Usage: source ./script <MASTER_URL> <RESERVATION_ID>

# which benchmarks to run
RUN_DEFAULT=1 # runs all schedulers with /configs/base_DAS5_config.json config
RUN_INDIVIDUAL=1 # runs all jobs individually just to get expected runtimes without interference
RUN_COALESCE=0 # runs workloads from $COALESCE_WORKLOAD_DIR
RUN_AQE=0 # runs all schedulers with ./configs/base_AQE_config.json config

# spark configs
DEPLOY_MODE="client"
MAIN_CLASS="BenchRunner"
ITERATIONS=1

# scheduler configs to use in benchmarks
declare -A SCHEDULERS

#SCHEDULERS[CUSTOM_SHORT]="--conf spark.scheduler.mode=CUSTOM --conf spark.customSchedulerContainer=ShortestFirstSchedulerContainer --conf spark.driver.extraClassPath=$SCHEDULER_DIR/ShortestFirstScheduler/target/ShortestFirstScheduler-1.0-SNAPSHOT.jar"

#SCHEDULERS[CUSTOM_FAIR]="--conf spark.scheduler.mode=CUSTOM --conf spark.customSchedulerContainer=UserFairSchedulerContainer --conf spark.driver.extraClassPath=$SCHEDULER_DIR/UserFairScheduler/target/UserFairScheduler-1.0-SNAPSHOT.jar"

#SCHEDULERS[CUSTOM_RANDOM]="--conf spark.scheduler.mode=CUSTOM --conf spark.customSchedulerContainer=RandomSchedulerContainer --conf spark.driver.extraClassPath=$SCHEDULER_DIR/RandomScheduler/target/RandomScheduler-1.0-SNAPSHOT.jar"

SCHEDULERS[CUSTOM_CLUSTERFAIR]="--conf spark.scheduler.mode=CUSTOM --conf spark.customSchedulerContainer=ClusterFairSchedulerContainer --conf spark.driver.extraClassPath=$SCHEDULER_DIR/ClusterFairScheduler/target/ClusterFairScheduler-1.0-SNAPSHOT.jar"
SCHEDULERS[CUSTOM_USERCLUSTERFAIR]="--conf spark.scheduler.mode=CUSTOM --conf spark.customSchedulerContainer=UserClusterFairSchedulerContainer --conf spark.driver.extraClassPath=$SCHEDULER_DIR/UserClusterFairScheduler/target/UserClusterFairScheduler-1.0-SNAPSHOT.jar"

SCHEDULERS[DEFAULT_FAIR]="--conf spark.scheduler.mode=FAIR"
SCHEDULERS[DEFAULT_FIFO]="--conf spark.scheduler.mode=FIFO"

# Paths
PROJECT_DIR="/var/scratch/$USER/performance_test"
SCHEDULER_DIR="$PROJECT_DIR/schedulers"
SPARK_JOB_FILE="$PROJECT_DIR/target/performance_test-1.0-SNAPSHOT.jar"

WORKLOAD_DIR="$PROJECT_DIR/configs/workloads"
INDIVIDUAL_WORKLOAD_DIR="$PROJECT_DIR/configs/individual"
COALESCE_WORKLOAD_DIR="$PROJECT_DIR/configs/coalesce"


if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo "Hey, you should source this script, not execute it!"
    exit 1
fi

# Env
source master-env.sh

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

if [ ! -d "$PROJECT_DIR" ]; then
   echo "'$PROJECT_DIR' directory does not exist"
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

echo "Setting up default config"
cp "./configs/base_DAS5_config.json" "./configs/base_config.json"

echo "Starting workloads"
if [ "$RUN_DEFAULT" -eq 1 ]; then
    for file in "$WORKLOAD_DIR"/*; do
        # Ensure it is a regular file
        if [ -f "$file" ]; then
            echo "running spark on $file"
            for key in "${!SCHEDULERS[@]}"; do
                echo "Running with scheduler: $key"
                for i in $(seq 1 "$ITERATIONS"); do
                    echo "Iteration: $i"
                    run_spark_job "$key" "$file" ${SCHEDULERS[$key]}
                done
            done
        fi
    done
fi

if [ "$RUN_INDIVIDUAL" -eq 1 ]; then
    if [ -d "$INDIVIDUAL_WORKLOAD_DIR" ]; then
        echo "Running individual workloads from $INDIVIDUAL_WORKLOAD_DIR"
        for file in "$INDIVIDUAL_WORKLOAD_DIR"/*; do
            # Ensure it is a regular file
            if [ -f "$file" ]; then
                echo "running spark on $file"
                run_spark_job "BASE" $file ${SCHEDULERS[DEFAULT_FIFO]}
            fi
        done
    else
        echo "No directory for individual workloads found: $INDIVIDUAL_WORKLOAD_DIR"
    fi
fi

if [ "$RUN_COALESCE" -eq 1 ]; then
    if [ -d "$COALESCE_WORKLOAD_DIR" ]; then
        echo "Running coalesce workloads from $COALESCE_WORKLOAD_DIR"
        for file in "$COALESCE_WORKLOAD_DIR"/*; do
            if [ -f "$file" ]; then
                echo "running spark on $file"
                for key in "${!SCHEDULERS[@]}"; do
                    echo "Running with scheduler: $key"
                    for i in $(seq 1 "$ITERATIONS"); do
                        echo "Iteration: $i"
                        run_spark_job "$key" "$file" ${SCHEDULERS[$key]}
                    done
                done
            fi
        done
    else
        echo "No directory for individual workloads found: $COALESCE_WORKLOAD_DIR"
    fi
fi

if [ "$RUN_AQE" -eq 1 ]; then
  echo "Setting up AQE config"
  cp "./configs/base_AQE_config.json" "./configs/base_config.json"
  for file in "$WORKLOAD_DIR"/*; do
      # Ensure it is a regular file
      if [ -f "$file" ]; then
          echo "running spark on $file"
          for key in "${!SCHEDULERS[@]}"; do
              echo "Running with scheduler: $key"
              for i in $(seq 1 "$ITERATIONS"); do
                  echo "Iteration: $i"
                  run_spark_job "AQE_$key" "$file" ${SCHEDULERS[$key]}
              done
          done
      fi
  done
fi


echo "=============================================="
echo "All Spark jobs completed."
echo "=============================================="


echo "Killing the reservation $RESERVATION_ID"
preserve -c $RESERVATION_ID
