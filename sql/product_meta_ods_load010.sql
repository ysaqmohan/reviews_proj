#!/bin/bash

PGPASSWORD=${PGPASSWD} psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSER} -d ${DB_DWH} -a --set AUTOCOMMIT=off --set ON_ERROR_STOP=on  << EOF &>> $log_file

begin;
	CREATE INDEX prodmeta_incr ON ${SCHEMA_STG}.PRODMETA(odsaddtd); 
commit;
EOF
