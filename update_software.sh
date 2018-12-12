#!/usr/bin/env sh

export PATH=$PROJECT_chtb00/htb006/tools/parrot/bin:$PATH

export HTTP_PROXY="DIRECT;"
export PARROT_CVMFS_REPO="softdrive.nl:url=http://cvmfs01.nikhef.nl/cvmfs/softdrive.nl/,pubkey=$PROJECT_chtb00/htb006/softdrive.nl.pub"

parrot_run rsync -auvPb --stats --delete /cvmfs/softdrive.nl/lofar_sw/ $PROJECT_chtb00/htb006/software/.

## update files in the directories
source $PROJECT_chtb00/htb006/env_lofar_2.20.2_juwels.sh

which losoto | awk '{print "ln -s "$1" $LOFARROOT/bin/losoto"}' | bash

