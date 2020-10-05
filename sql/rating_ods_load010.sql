#!/bin/bash

PGPASSWORD=${PGPASSWD} psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -a --set AUTOCOMMIT=off --set ON_ERROR_STOP=on  << EOF &>> $log_file

begin;
	
	INSERT INTO ${SCHEMA_STG}.REVIEWS
	SELECT RS.reviewerID, RS.asin, RS.reviewerName, RS.reviewRating , RS.reviewDate, current_timestamp(0), current_timestamp(0)
	FROM ${SCHEMA_WT}.REVIEWS_WT RS
	WHERE RV.reviewerID IS NOT NULL
	AND RV.asin IS NOT NULL;

	TRUNCATE ${SCHEMA_WT}.REVIEWS_WT;
commit;

EOF
