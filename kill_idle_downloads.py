#!/usr/bin/env sh

process1=globus-url-copy
process2=uberftp
process3=java
process4=wget

timeout=4h
timeout_java=10s

while [ 1 ]
do

	if [[ "$(uname)" = "Linux" ]]
		then 
			killall -v --older-than $timeout      $process1
			killall -v --older-than $timeout      $process2
			killall -v --older-than $timeout      $process4
			killall -v --older-than $timeout_java $process3
	fi
	
	if [[ "$(voms-proxy-info  --all | grep timeleft | tail -1 | cut -f2 -d: | sed 's/ //g')" = "00" ]]
        then
            /p/project/chtb00/htb006/launch_proxy.sh
    fi
    
	sleep $timeout_java
	
done
