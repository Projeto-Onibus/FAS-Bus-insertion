#!/bin/sh
FOLDER='/collect_bus'
COLLECTED=$FOLDER'/collected_data.txt'
NOT_COLLECTED=$FOLDER'/not_collected.txt'
OLD_FOLDER=$FOLDER'_old'

YESTERDAY=$(date --date="1 day ago" +"%Y-%m-%d")
tar -zcvf $OLD_FOLDER'/'$YESTERDAY".tar.gz" $FOLDER'/'$YESTERDAY
rm -r $FOLDER'/'$YESTERDAY


RESULTS="$YESTERDAY,"
if [[ -e $COLLECTED ]]; then
	RESULTS=$RESULTS$(wc -l $COLLECTED)","
	echo "collected $(wc -l $COLLECTED) entries" && rm $COLLECTED && touch $COLLECTED
else
	RESULTS=$RESULTS"0,"
	touch $COLLECTED
fi

if [[ -e $NOT_COLLECTED ]]; then
	RESULTS=$RESULTS$(wc -l $NOT_COLLECTED)
	echo "failed $(wc -l $NOT_COLLECTED) entries" && rm $NOT_COLLECTED && touch $NOT_COLLECTED
else
	RESULTS=$RESULTS"0"
	touch $NOT_COLLECTED
fi

echo $RESULTS >> $OLD_FOLDER'/collection_logs.csv'

# Execute script per-day automaticaly
python3 /app/PopulateDatabase.py -b $OLD_FOLDER -d $YESTERDAY bus
