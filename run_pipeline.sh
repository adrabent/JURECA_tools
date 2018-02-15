#!/usr/bin/env sh

. /homea/htb00/htb006/env_lofar_GRID_stage2017b.sh

file=$WORK/SKSP_monitoring.py

while [ 1 ]
do
	$file
	sleep 900s
done