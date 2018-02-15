#!/usr/bin/env sh

env |grep SLURM

. /homea/htb00/htb006/env_lofar_2.20.2_stage2017b.sh

./LOFAR.py -c $1 $2