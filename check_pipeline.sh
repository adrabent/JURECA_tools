#!/usr/bin/env sh

. /homea/htb00/htb006/env_lofar_2.20.2_stage2017b.sh

file=$WORK/submit_job.sh
while [ 1 ]
do
	if [ -f "$file" ]
		then 
			$file
			rm -v $file
		else
			echo -n 'No submission script found: '; date
	fi
	sleep 60s
done