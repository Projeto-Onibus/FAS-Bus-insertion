#!/bin/bash

# Exits in case of failure
set -e

# Sets path for files
FOLDER='/collect_bus'
COLLECTED=$FOLDER'/collected_data.txt'
NOT_COLLECTED=$FOLDER'/not_collected.txt'
OLD_FOLDER=$FOLDER'_old'

# Gets previous date for data collection
YESTERDAY=$(date --date="1 day ago" +"%Y-%m-%d")

# Compression of collected data and removal of tar file
tar -zcvf $OLD_FOLDER'/'$YESTERDAY".tar.gz" $FOLDER'/'$YESTERDAY
rm -r $FOLDER'/'$YESTERDAY

# Gets number for collected and not collected attempts
RESULTS_COLLECTED=$(wc -l $COLLECTED 2>/dev/null || echo 0 0)
RESULTS_NOT_COLLECTED=$(wc -l $NOT_COLLECTED 2>/dev/null || echo 0 0)

# Echo results to log
echo $YESTERDAY","${RESULTS_COLLECTED%% *}","${RESULTS_NOT_COLLECTED%% *} >> $OLD_FOLDER'/collection_logs.csv'

# Execute script per-day automaticaly
python3 /app/PopulateDatabase.py  -d $YESTERDAY bus
