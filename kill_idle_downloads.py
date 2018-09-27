#!/usr/bin/env sh

process=globus-url-copy
timeout=2h
while [ 1 ]
do
	if [[ "$(uname)" = "Linux" ]]
		then 
			killall -v --older-than $timeout $process 
	fi
	sleep 60s
done
