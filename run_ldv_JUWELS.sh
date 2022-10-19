#!/bin/bash
export LOG_DIR=/p/project/chtb00/htb006/ldv/executor/log
atdb_service -o executor --interval 60 --config $HOME/.atdb/atdb_ldv_conf.ini --atdb_host https://sdc-dev.astron.nl/ajul/atdb/ -v2 > $LOG_DIR/atdb_executor.log 2> $LOG_DIR/atdb_executor.err
