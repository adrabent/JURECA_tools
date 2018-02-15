#!/usr/bin/env sh

export PATH=$HOME/tools/parrot/bin:$PATH

export HTTP_PROXY="DIRECT;"
export PARROT_CVMFS_REPO="softdrive.nl:url=http://cvmfs01.nikhef.nl/cvmfs/softdrive.nl/,pubkey=$HOME/softdrive.nl.pub"

parrot_run rsync -auvPb --stats --delete /cvmfs/softdrive.nl/lofar_sw/ $HOME/software/.

## update files in the directories
source $HOME/env_lofar_2.20.2_stage2017b.sh

which losoto | awk '{print "ln -s "$1" $LOFARROOT/bin/losoto"}' | bash

