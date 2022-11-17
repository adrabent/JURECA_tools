#!/bin/sh

env | grep SLURM

. $PROJECT_chtb00/htb006/env_lofar_4.0_juwels.sh

env | grep SINGULARITY
env | grep TOIL

linc $1 $2 $3
