#!/bin/sh
FOLDER='/collect_bus'
COLLECTED=$FOLDER'/collected_data.txt'
NOT_COLLECTED=$FOLDER'/not_collected.txt'
OLD_FOLDER=$FOLDER'_old'

YESTERDAY=$(date --date="1 day ago" +"%Y-%m-%d")
tar -zcvf $OLD_FOLDER'/'$YESTERDAY".tar.gz" $FOLDER'/'$YESTERDAY
rm -r $FOLDER'/'$YESTERDAY
echo "collected $(wc -l $COLLECTED) entries" && rm $COLLECTED && touch $COLLECTED
echo "failed $(wc -l $NOT_COLLECTED) entries" && rm $NOT_COLLECTED && touch $NOT_COLLECTED
echo "$YESTERDAY,$(wc -l $COLLECTED),$(wc -l $NOT_COLLECTED)" >> $OLD_FOLDER'/collection_logs.csv'
# Execute script per-day automaticaly
python3 /app/PopulateDatabase.py -b $OLD_FOLDER -d $YESTERDAY bus
