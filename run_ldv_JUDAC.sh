#!/bin/bash -i

## Jülich project related environment variables
export PROJECT=${PROJECT_chtb00}/htb006
export SCRATCH=${SCRATCH_chtb00}/htb006
export SOFTWARE=${PROJECT}/software_new
export LDV=${PROJECT}/ldv

## LDV specific environment variables
export LOG_DIR=${LDV}/logs
export ATDB_CONFIG=${LDV}/config/atdb_ldv_conf.ini
export ATDB_HOST='https://sdc-dev.astron.nl:5554/atdb/'
export SERVICE_FILTER='juelich-test'

alias atdb_service='${SOFTWARE}/envs/ldv/bin/atdb_service'

## running LDV services (datamanager)
atdb_service -o datamanager    --interval 30 --config ${ATDB_CONFIG} --atdb_host ${ATDB_HOST} -v2 --service_filter ${SERVICE_FILTER} >> ${LOG_DIR}/atdb_datamanager.log 2>> ${LOG_DIR}/atdb_datamanager.err &
atdb_service -o executor       --interval 30 --config ${ATDB_CONFIG} --atdb_host ${ATDB_HOST} -v2 --service_filter ${SERVICE_FILTER} >> ${LOG_DIR}/atdb_executor.log    2>> ${LOG_DIR}/atdb_executor.err &
atdb_service -o aggregator     --interval 30 --config ${ATDB_CONFIG} --atdb_host ${ATDB_HOST} -v2 --service_filter ${SERVICE_FILTER} >> ${LOG_DIR}/atdb_aggregator.log  2>> ${LOG_DIR}/atdb_aggregator.err &
atdb_service -o cleanup-spider --interval 30 --config ${ATDB_CONFIG} --atdb_host ${ATDB_HOST} -v2 --service_filter ${SERVICE_FILTER} >> ${LOG_DIR}/atdb_cleanup.log     2>> ${LOG_DIR}/atdb_cleanup.err

## kill LDV services
pkill -9 atdb_service
