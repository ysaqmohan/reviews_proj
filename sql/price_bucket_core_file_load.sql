#!/bin/bash


PGPASSWORD=${PGPASSWD} psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -a --set AUTOCOMMIT=off --set ON_ERROR_STOP=on  << EOF &>> $log_file

begin;

	DELETE FROM ${SCHEMA_WT}.PRICE_BUCKET_WT WHERE bucket_floor_am > bucket_ceiling_am;
	
	CREATE TEMPORARY TABLE PRICE_BUCKET_TEMP AS
	SELECT PB.price_bucket_nm, c_dim.category_id, PB.bucket_floor_am, PB.bucket_ceiling_am
	FROM ${SCHEMA_WT}.PRICE_BUCKET_WT PB
	INNER JOIN ${SCHEMA_CORE}.category_dim c_dim
	ON trim(PB.category_nm) = trim(c_dim.category_nm);

	UPDATE ${SCHEMA_CORE}.price_bucket_dim
	SET bucket_floor_am = tmp.bucket_floor_am 
	, bucket_ceiling_am = tmp.bucket_ceiling_am
	, effective_end_dt = current_date - 1
	, active_in = 'N'
	, modified_td = current_timestamp(0)
	FROM PRICE_BUCKET_TEMP tmp 
	WHERE trim(${SCHEMA_CORE}.price_bucket_dim.price_bucket_nm) = trim(tmp.price_bucket_nm)
	AND ${SCHEMA_CORE}.price_bucket_dim.category_id = tmp.category_id;

	INSERT INTO ${SCHEMA_CORE}.price_bucket_dim
	SELECT PB.price_bucket_id, PB.price_bucket_nm, PB.category_id, TMP.bucket_floor_am, 
	TMP.bucket_ceiling_am, current_date, '2045-12-31' ,'Y', current_timestamp(0), current_timestamp(0)
	FROM PRICE_BUCKET_TEMP TMP
	INNER JOIN ${SCHEMA_CORE}.price_bucket_dim PB
	ON TMP.price_bucket_nm = PB.price_bucket_nm
	AND TMP.category_id = PB.category_id;

	INSERT INTO ${SCHEMA_CORE}.price_bucket_dim
	SELECT CLK.MAX_PRICE_BUCKET_ID + ROW_NUMBER() OVER(ORDER BY TMP.price_bucket_nm, TMP.category_id) AS price_bucket_id
	, TMP.price_bucket_nm, TMP.category_id, TMP.bucket_floor_am
	, TMP.bucket_ceiling_am, current_date, '2045-12-31' ,'Y', current_timestamp(0), current_timestamp(0)
	FROM PRICE_BUCKET_TEMP TMP
	LEFT OUTER JOIN ${SCHEMA_CORE}.price_bucket_dim PB
	ON TMP.price_bucket_nm = PB.price_bucket_nm
	AND TMP.category_id = PB.category_id
	INNER JOIN ${SCHEMA_META}.CLOCK_PRICE_BUCKET CLK
	ON MAX_PRICE_BUCKET_ID >= 0
	WHERE PB.price_bucket_nm IS NULL;

	UPDATE ${SCHEMA_META}.CLOCK_PRICE_BUCKET
	SET MAX_PRICE_BUCKET_ID = (SELECT coalesce(max(price_bucket_id),0) FROM ${SCHEMA_CORE}.price_bucket_dim);

	TRUNCATE ${SCHEMA_WT}.PRICE_BUCKET_WT;

commit;

EOF
