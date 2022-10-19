#!/bin/bash
export LOG_DIR=/p/project/chtb00/htb006/ldv/datamanager/log
atdb_service -o datamanager --interval 60 --config $HOME/.atdb/atdb_ldv_conf.ini --atdb_host https://sdc-dev.astron.nl/ajul/atdb/ -v2 &
atdb_service -o stager --interval 60 --config $HOME/.atdb/atdb_ldv_conf.ini --atdb_host https://sdc-dev.astron.nl/ajul/atdb/
pkill -9 atdb_service
