#!/usr/bin/env sh

env | grep SLURM

. $PROJECT_chtb00/htb006/env_lofar_2.20.2_juwels.sh

./LOFAR.py -c $1 $2
