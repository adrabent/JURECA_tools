#!/usr/bin/env sh

. $PROJECT_chtb00/htb006/env_lofar_GRID_stage2020.sh

SCRIPT=$PROJECT/SKSP_monitoring.py
PROXY=$PROJECT/launch_proxy.sh
LOCK=$SCRATCH/.lock

$PROXY &&
$SCRIPT &

while [ 1 ]
do
	if [ -f "$LOCK" ]; then
		ID=`(more $LOCK)`
		$SCRIPT --id $ID &
	fi
	sleep 60s
done
