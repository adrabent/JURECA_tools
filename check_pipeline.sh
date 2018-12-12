#!/usr/bin/env sh

. $PROJECT_chtb00/htb006/env_lofar_2.20.2_juwels.sh

file=$SCRATCH_chtb00/htb006/submit_job.sh
while [ 1 ]
do
	if [ -f "$file" ]
		then 
			cp -rv $PROJECT_chtb00/htb006/*.Z $SCRATCH_chtb00/htb006/.
			$file
			rm -v $file
		else
			echo -n 'No submission script found: '; date
	fi
	sleep 60s
done