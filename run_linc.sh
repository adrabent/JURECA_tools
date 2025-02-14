#!/bin/sh

env | grep SLURM

. $PROJECT_chtb00/htb006/env_lofar_4.1_juwels.sh

env | grep SINGULARITY
env | grep CWL

linc $1 $2 $3
