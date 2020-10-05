#!/bin/bash

#./parms/.parms_env.sh

SDATE=`date`
STIME=`date +%s`
RUNDATE=`date +%C%y-%m-%d_%H.%M.%S`

PROJDIR="/edw/Reviews"
export PROJDIR
script_name="rating_ods_load"

log_file="$PROJDIR"/SourceCode/logs/"${script_name}-${RUNDATE}.log"
export log_file

. /$PROJDIR/SourceCode/parms/parms_env.sh

#function for capturing error
error_check ()
{
	if [ $? -gt 0 ]
	then 
		echo "Job Failed"
		echo "$log_file"
		exit 8
	fi
}

wget -O $PROJDIR/SourceCode/data/asin_reviews.json.gz https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz
error_check

gzip $PROJDIR/SourceCode/data/asin_reviews.json.gz
error_check

if [ ! -f $PROJDIR/SourceCode/data/asin_reviews.json ]; then
	echo "Missing source file" >> $log_file
	exit 8
fi

echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file                                                           
echo "Load to stage in progress" >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file                                                          

#PySpark load
$PROJDIR/spark-2.4.5-bin-hadoop2.7/bin/spark-submit $PROJDIR/SourceCode/python/rating_ods_load.py >> $log_file 2>&1
error_check

echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "Indexing target table" >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file                                                           

#$PROJDIR/SourceCode/sql/rating_ods_load010.sql >> $log_file 
#error_check

echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "File archival in progress" >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file

#gzip $PROJDIR/SourceCode/data/asin_reviews.json
#error_check

mv $PROJDIR/SourceCode/data/asin_reviews.json  $PROJDIr/SourceCode/data/Archival/
error_check

mv $PROJDIr/SourceCode/data/Archival/asin_reviews.json  "asin_reviews_${RUNDATE}.json"
error_check

echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "Job completed successfully" >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file
