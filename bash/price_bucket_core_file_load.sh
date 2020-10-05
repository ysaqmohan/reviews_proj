#!/bin/bash

#./parms/.parms_env.sh

SDATE=`date`
STIME=`date +%s`
RUNDATE=`date +%C%y-%m-%d_%H.%M.%S`

PROJDIR="/edw/Reviews"
export PROJDIR
script_name="price_bucket_core_file_load"

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

echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file                                                           
echo "Load to core in progress" >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file                                                          

PGPASSWORD=${PGPASSWD} psql  -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -c "\copy ${SCHEMA_WT}.PRICE_BUCKET_WT(category_nm, bucket_floor_am, bucket_ceiling_am , price_bucket_nm) FROM '${PROJDIR}/SourceCode/data/price_bucket_load.csv' DELIMITER '|' CSV HEADER" -a --set AUTOCOMMIT=on --set ON_ERROR_STOP=on   &>> $log_file

error_check

$PROJDIR/SourceCode/sql/$script_name.sql >> $log_file
error_check

echo "###################################################################################################################################"  >> $log_file
echo "File archival in progress" >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file

#gzip $PROJDIR/SourceCode/data/price_bucket_load.csv
#error_check

#mv $PROJDIR/SourceCode/data/price_bucket_load.csv  $PROJDIr/SourceCode/data/Archival/
#error_check

#mv $PROJDIr/SourceCode/data/Archival/price_bucket_load.csv  "price_bucket_load_${RUNDATE}.csv.gz"
#error_check

echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "Job completed successfully" >> $log_file
echo "###################################################################################################################################"  >> $log_file
echo "###################################################################################################################################"  >> $log_file
