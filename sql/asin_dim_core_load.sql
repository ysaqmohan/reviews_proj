#!/bin/bash

PGPASSWORD=${PGPASSWD} psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -a --set AUTOCOMMIT=off --set ON_ERROR_STOP=on  << EOF &>> $log_file

begin;
	CREATE TEMPORARY TABLE ASINTEMP_inter AS
	SELECT asin, title, brand, categories, odsaddtd
	FROM ${SCHEMA_STG}.prodmeta
	WHERE odsaddtd > (SELECT LAST_LOAD_TD
		FROM ${SCHEMA_META}.CONTROL_TBL
		WHERE SCHEMA_NM = 'CORE_DB'
		AND TABLE_NM = 'ASIN_DIM');

	CREATE TEMPORARY TABLE ASINTEMP AS
	SELECT inter.asin, inter.title, inter.brand, cdm.category_id, inter.odsaddtd
	FROM ASINTEMP_inter inter 
	LEFT OUTER JOIN ${SCHEMA_CORE}.CATEGORY_DIM cdm
	ON inter.categories = cdm.category_nm
	WHERE cdm.category_nm IS NOT NULL;

	CREATE INDEX ASINTEMP_LOAD ON ASINTEMP(asin);

	ANALYZE ASINTEMP_LOAD;

	UPDATE ${SCHEMA_CORE}.ASIN_DIM
	SET ASIN_NM = ASINTEMP.title
	, BRAND_NM = ASINTEMP.brand
	, CATEGORY_ID = ASINTEMP.CATEGORY_ID
	, MODIFIED_TD = current_timestamp(0)
	FROM ASINTEMP
	WHERE ASIN_DIM.ASIN_ID = ASINTEMP.asin;

	INSERT INTO ${SCHEMA_CORE}.ASIN_DIM
	SELECT ASINTEMP.asin, ASINTEMP.title, ASINTEMP.brand, ASINTEMP.CATEGORY_ID, current_timestamp(0), current_timestamp(0)
	FROM ASINTEMP
	LEFT OUTER JOIN ${SCHEMA_CORE}.ASIN_DIM
	ON ASIN_DIM.ASIN_ID = ASINTEMP.asin
	WHERE ASIN_DIM.ASIN_ID IS NULL	;

	UPDATE ${SCHEMA_META}.CONTROL_TBL
	SET LAST_LOAD_TD = MAXASINTEMP.maxaddtd
	FROM (SELECT max(odsaddtd) as maxaddtd , count(*) as cnt FROM ASINTEMP) MAXASINTEMP
	WHERE SCHEMA_NM = 'CORE_DB' AND TABLE_NM = 'ASIN_DIM'
	AND MAXASINTEMP.cnt <> 0;

commit;

EOF