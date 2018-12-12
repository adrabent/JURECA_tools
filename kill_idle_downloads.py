#!/usr/bin/env sh

process1=globus-url-copy
process2=uberftp

timeout=1h
while [ 1 ]
do
	if [[ "$(uname)" = "Linux" ]]
		then 
			killall -v --older-than $timeout $process1
			killall -v --older-than $timeout $process2
	fi
	sleep 60s
done
