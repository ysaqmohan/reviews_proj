#!/bin/bash

PGPASSWORD=${PGPASSWD} psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -a --set AUTOCOMMIT=off --set ON_ERROR_STOP=on  << EOF &>> $log_file

begin;
        CREATE TEMPORARY TABLE REVIEWTEMP AS
	SELECT reviewerid, reviewername, odsaddtd FROM (
	SELECT reviewerid, reviewername, odsaddtd, ROW_NUMBER() OVER( PARTITION BY reviewerid order by reviewdate desc, reviewername desc) as row_num 
	FROM ${SCHEMA_STG}.reviews
	WHERE odsaddtd > (SELECT LAST_LOAD_TD 
				FROM ${SCHEMA_META}.CONTROL_TBL 
				WHERE SCHEMA_NM = 'CORE_DB' 
				AND TABLE_NM = 'REVIEWER_DIM')
		)sub_REVIEWS 
	WHERE row_num = 1; 

	CREATE INDEX REVIEWTEMP_LOAD ON REVIEWTEMP(reviewerid);

	ANALYZE REVIEWTEMP;	
								
	UPDATE ${SCHEMA_CORE}.REVIEWER_DIM
	SET REVIEWER_NM = REVIEWTEMP.reviewername
	, MODIFIED_TD = current_timestamp(0)
	FROM REVIEWTEMP
	WHERE REVIEWER_DIM.REVIEWER_ID = REVIEWTEMP.reviewerid;
		 

        INSERT INTO ${SCHEMA_CORE}.REVIEWER_DIM
        SELECT REVIEWTEMP.reviewerid, REVIEWTEMP.reviewername, current_timestamp(0), current_timestamp(0)
        FROM REVIEWTEMP
        LEFT OUTER JOIN ${SCHEMA_CORE}.REVIEWER_DIM
        ON REVIEWER_DIM.REVIEWER_ID = REVIEWTEMP.reviewerid
        WHERE REVIEWER_DIM.REVIEWER_ID IS NULL
        ;

	UPDATE ${SCHEMA_META}.CONTROL_TBL
	SET LAST_LOAD_TD = MAXREVIEWTEMP.maxodsaddtd
	FROM (SELECT max(odsaddtd) as maxodsaddtd , count(*) as cnt FROM REVIEWTEMP) MAXREVIEWTEMP
	WHERE CONTROL_TBL.SCHEMA_NM = 'CORE_DB' 
	AND CONTROL_TBL.TABLE_NM = 'REVIEWER_DIM'
	AND MAXREVIEWTEMP.cnt <> 0;

commit;

EOF
