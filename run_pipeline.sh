#!/usr/bin/env sh

env | grep SLURM

. $PROJECT_chtb00/htb006/env_lofar_3.2_juwels.sh

$PROJECT_chtb00/htb006/LOFAR.py -c $1 $2
