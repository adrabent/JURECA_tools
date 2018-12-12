#!/usr/bin/env sh

. $PROJECT_chtb00/htb006/env_lofar_GRID_stage2017b.sh

file=$PROJECT/SKSP_monitoring.py
proxy=$PROJECT/launch_proxy.sh

$proxy
while [ 1 ]
do
	$file
	sleep 60s
done
