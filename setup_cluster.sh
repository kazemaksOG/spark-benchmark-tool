#!/bin/bash

# Usage: source ./script <OPTIONAL_RESERVATION_ID>

# Deployer variables
DEPLOYER_HOME="/home/$USER/das-bigdata-deployment-python3"
TIME="00:15:00"
MACHINES=5

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
DEPLOY_OUTPUT=$($DEPLOYER_HOME/deployer deploy -q --preserve-id $RESERVATION_ID -s $DEPLOYER_HOME/env/das5-spark.settings spark custom)

if [[ "$DEPLOY_OUTPUT" == MASTER_NODE:* ]]; then
    MASTER_ADDR="${DEPLOY_OUTPUT#MASTER_NODE:}"

    if [ -n "$MASTER_ADDR" ]; then
        MASTER="spark://$MASTER_ADDR:7077"
        printf "Deployment successful. First ssh to master node with: \n\n   ssh $MASTER_ADDR\n\n Then run the other script: \n\n  source ./run_all_benchmarks.sh $MASTER $RESERVATION_ID\n"
    else
        echo "Error: Master Node URL is empty!"
        return 1
    fi

else
    echo "Deploy failed: $DEPLOY_OUTPUT"
    return 1
fi

