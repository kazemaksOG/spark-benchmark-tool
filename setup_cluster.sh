# Usage: source ./script <OPTIONAL_RESERVATION_ID>

# Deployer variables
DEPLOYER_HOME="/var/scratch/$USER/test_dir/das-bigdata-deployment-python3"
TIME="00:15:00" # time of the reservation. make sure experiments can finish within the allocated timeslot
MACHINES=5 # amount of machines used in the experiment. 1 is for driver program, 4 are executors
SPARK_VERSION="3.5.5-custom"


if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo "Hey, you should source this script, not execute it!"
    exit 1
fi



if [ ! -d "$DEPLOYER_HOME" ]; then
    echo "Error: Directory '$DEPLOYER_HOME' does not exist."
    return 1
fi


# Env
module load prun

# for python evnironment
source $DEPLOYER_HOME/venv/bin/activate



RESERVATION_ID=$1

if [ ! -n "$RESERVATION_ID" ]; then 
    echo "No reservation id passed as argument"


    echo "Checking for previous reservations..."
    RESERVATIONS_RAW=$($DEPLOYER_HOME/deployer preserve list-reservations)
    RESERVATIONS="${RESERVATIONS_RAW#*num_machines}"

    if [ -n "$RESERVATIONS" ]; then
        echo "Reservations exist, please kill them before starting experiments: $RESERVATIONS"
        return 1

    fi


    echo "Creating a new reservation"
    RESERVATION_ID=$($DEPLOYER_HOME/deployer preserve create-reservation -q -t $TIME $MACHINES)


    echo "Created reservation for duration of $TIME for $MACHINES machines, and waiting for it to propagate: $RESERVATION_ID"
    # needs some time for reservation to register properly
    sleep 2

fi

echo "Using reservation: $RESERVATION_ID"


echo "Started deployment..."
DEPLOY_OUTPUT=$($DEPLOYER_HOME/deployer deploy -q --preserve-id $RESERVATION_ID -s $DEPLOYER_HOME/env/das5-spark.settings spark $SPARK_VERSION)

if [[ "$DEPLOY_OUTPUT" == MASTER_NODE:* ]]; then
    MASTER_ADDR="${DEPLOY_OUTPUT#MASTER_NODE:}"

    if [ -n "$MASTER_ADDR" ]; then
        MASTER="spark://$MASTER_ADDR:7077"
        PWD=$(pwd)
        printf "Deployment successful. Performing: \n\n   ssh $MASTER_ADDR\n\n Then: \n\n    cd $PWD \n\n Then: \n\n  source ./run_all_benchmarks.sh $MASTER $RESERVATION_ID &> output.txt\n"
	ssh "$MASTER_ADDR" "source ~/.bashrc; source ~/.bash_profile; cd \"$PWD\"; pwd; source ./run_all_benchmarks.sh \"$MASTER\" \"$RESERVATION_ID\" &> output.txt"
    else
        echo "Error: Master Node URL is empty!"
        return 1
    fi

else
    echo "Deploy failed: $DEPLOY_OUTPUT"
    return 1
fi

