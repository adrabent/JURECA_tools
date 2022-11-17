#!/usr/bin/env sh

module use /gpfs/software/juwels/otherstages

. $PROJECT_chtb00/htb006/env_lofar_4.0_juwels.sh

SCRIPT=$SCRATCH_chtb00/htb006/submit_job.sh

while [ 1 ]
do
	if [ -f "$SCRIPT" ]
		then 
			OBSERVATION=`(more $SCRIPT | grep sbatch | awk '{print $9}')`
			cd $OBSERVATION
			pwd
			cp -rv $PROJECT_chtb00/htb006/*.Z $SCRATCH_chtb00/htb006/.
			$SCRIPT
			rm -v $SCRIPT
		else
			echo -n 'No submission script found: '; date
	fi
	sleep 60s
done
