#!/usr/bin/env sh

. /gpfs/homea/htb00/htb006/env_lofar_2.20.2_juwels.sh

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