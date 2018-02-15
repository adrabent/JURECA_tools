#!/usr/bin/env sh

. /homea/htb00/htb006/env_lofar_GRID_stage2017b.sh

file=$HOME/SKSP_monitoring.py
proxy=$HOME/launch_proxy.sh

$proxy
while [ 1 ]
do
	$file
	sleep 60s
done