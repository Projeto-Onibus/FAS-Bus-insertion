#!/bin/bash

# Exits in case of error
set -e

# Definition of folder's path
FOLDER='./collect_bus'
COLLECTED=$FOLDER'/collected_data.txt'
NOT_COLLECTED=$FOLDER'/not_collected.txt'

# Define folder name based on current date
DIRNAME=$FOLDER'/'$(date +"%Y-%m-%d")

# Creates folder if not exists
mkdir -p $DIRNAME

# Creates filename based on datetime
FILENAME=$(date +"%Y-%m-%d-%H:%M:%S")".data"

# Gets information from Rio's API and outputs it to the file defined by previous variables
wget -q http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/csv/onibus.cfm -O $DIRNAME/$FILENAME 

# Counts lines of generated file
VAZIO=$(wc -l $DIRNAME/$FILENAME)
# keeps only line count value
VAZIO=${VAZIO%% *}

# If response is empty, removes file and marks the not collected file
if [[  $VAZIO == 0 ]]; then
    rm $DIRNAME/$FILENAME && echo $FILENAME >> $NOT_COLLECTED
    exit
fi

# Marks sucessful count on collected file
echo $FILENAME >> $COLLECTED