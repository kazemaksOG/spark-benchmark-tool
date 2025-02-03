#!/bin/bash

# Usage: source ./script <OPTIONAL_RESERVATION_ID>

# Env
module load prun

# Deployer variables
DEPLOYER_HOME="/home/dkazemak/das-bigdata-deployment-python3"
TIME="00:15:00"
MACHINES=5

# for python evnironment
source $DEPLOYER_HOME/venv/bin/activate

if [ ! -d "$DEPLOYER_HOME" ]; then
    echo "Error: Directory '$DEPLOYER_HOME' does not exist."
    return 1
fi


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


	echo "Created reservation, and waiting for it to propoage: $RESERVATION_ID"
	# needs some time for reservation to register properly
	sleep 2

fi

echo "using reservation: $RESERVATION_ID"


echo "Started deployment..."
DEPLOY_OUTPUT=$($DEPLOYER_HOME/deployer deploy -q --preserve-id $RESERVATION_ID -s $DEPLOYER_HOME/env/das5-spark.settings spark custom)
MASTER=""
MASTER_ADDR=""

if [[ "$DEPLOY_OUTPUT" == MASTER_NODE:* ]]; then
    MASTER_ADDR="${DEPLOY_OUTPUT#MASTER_NODE:}"

    if [ -n "$MASTER_ADDR" ]; then
        MASTER="spark://$MASTER_ADDR:7077"
        echo "Deployment successful. Master Node URL: $MASTER"
    else
        echo "Error: Master Node URL is empty!"
        return 1
    fi

else
    echo "Deploy failed: $DEPLOY_OUTPUT"
    return 1
fi

