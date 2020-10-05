#!/bin/bash

PGPASSWORD=${PGPASSWD} psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -a --set AUTOCOMMIT=off --set ON_ERROR_STOP=on  << EOF &>> $log_file

begin;
        CREATE TEMPORARY TABLE CATEGORYTEMP AS
	SELECT  CASE WHEN trim(categories) = '' THEN 'UNCATEGORIZED' ELSE categories END AS categories
	, odsaddtd
        FROM ${SCHEMA_STG}.prodmeta
        WHERE odsaddtd > (SELECT LAST_LOAD_TD
                FROM ${SCHEMA_META}.CONTROL_TBL
                WHERE SCHEMA_NM = 'CORE_DB'
                AND TABLE_NM = 'CATEGORY_DIM')
		AND categories IS NOT NULL;


        INSERT INTO ${SCHEMA_CORE}.CATEGORY_DIM (CATEGORY_NM, ADD_TD, MODIFIED_TD)
        SELECT distinct CT.categories, current_timestamp(0), current_timestamp(0)
        FROM (SELECT categories FROM CATEGORYTEMP EXCEPT SELECT category_nm from ${SCHEMA_CORE}.CATEGORY_DIM) CT;

        UPDATE ${SCHEMA_META}.CONTROL_TBL
        SET LAST_LOAD_TD = MAXASINTEMP.maxodsupdatetd
        FROM (SELECT max(odsaddtd) as maxodsupdatetd, count(*) as cnt FROM CATEGORYTEMP) MAXASINTEMP
        WHERE SCHEMA_NM = 'CORE_DB' AND TABLE_NM = 'CATEGORY_DIM'
	AND MAXASINTEMP.cnt <> 0;

commit;

EOF
