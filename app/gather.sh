#!/bin/sh
FOLDER='/collect_bus'
COLLECTED=$FOLDER'/collected_data.txt'
NOT_COLLECTED=$FOLDER'/not_collected.txt'

DIRNAME=$FOLDER'/'$(date +"%Y-%m-%d")
mkdir -p $DIRNAME
FILENAME=$(date +"%Y-%m-%d-%H:%M:%S")".data"
wget -q http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/csv/onibus.cfm -O $DIRNAME/$FILENAME 
# If response is empty, does not save file
if [[ -z $(wc -l $DIRNAME/$FILENAME) ]]; then
    rm $DIRNAME/$FILENAME && echo $FILENAME >> $NOT_COLLECTED
    exit
fi
echo $FILENAME >> $COLLECTED