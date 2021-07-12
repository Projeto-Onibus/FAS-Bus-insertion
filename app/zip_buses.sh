#!/bin/bash
set -e
FOLDER='/collect_bus'
COLLECTED=$FOLDER'/collected_data.txt'
NOT_COLLECTED=$FOLDER'/not_collected.txt'
OLD_FOLDER=$FOLDER'_old'

YESTERDAY=$(date --date="1 day ago" +"%Y-%m-%d")

tar -zcvf $OLD_FOLDER'/'$YESTERDAY".tar.gz" $FOLDER'/'$YESTERDAY
rm -r $FOLDER'/'$YESTERDAY

RESULTS_COLLECTED=$(wc -l $COLLECTED 2>/dev/null || echo 0 0)
RESULTS_NOT_COLLECTED=$(wc -l $NOT_COLLECTED 2>/dev/null || echo 0 0)

echo $YESTERDAY","${RESULTS_COLLECTED%% *}","${RESULTS_NOT_COLLECTED%% *} >> $OLD_FOLDER'/collection_logs.csv'

# Execute script per-day automaticaly
python3 /app/PopulateDatabase.py -b $OLD_FOLDER -d $YESTERDAY bus
