#!/bin/bash

PGPASSWORD=${PGPASSWD} psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -a --set AUTOCOMMIT=off --set ON_ERROR_STOP=on  << EOF &>> $log_file

begin;
	  UPDATE ${SCHEMA_STG}.REVIEWS 
	  SET reviewerName = RS.reviewerName
	  , reviewRating = RS.reviewRating
	  , odsUpdateTd = current_timestamp(0)
	  FROM ${SCHEMA_WT}.REVIEWS_WT RS
	  WHERE REVIEWS.reviewerID = RS.reviewerID
	  AND REVIEWS.asin = RS.asin
	  AND REVIEWS.reviewDate = RS.reviewDate;
	
	INSERT INTO ${SCHEMA_STG}.REVIEWS
	SELECT RS.reviewerID, RS.asin, RS.reviewerName, RS.reviewRating , RS.reviewDate, current_timestamp(0), current_timestamp(0)
	FROM ${SCHEMA_WT}.REVIEWS_WT RS
	LEFT OUTER JOIN ${SCHEMA_STG}.REVIEWS RV
	ON RS.reviewerID = RV.reviewerID
	AND RS.asin = RV.asin
	AND RS.reviewDate = RV.reviewDate
	WHERE RV.reviewerID IS NULL
	AND RV.asin IS NULL
	AND RV.reviewDate IS NULL;

	TRUNCATE ${SCHEMA_WT}.REVIEWS_WT;
commit;

EOF
