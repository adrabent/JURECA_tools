#!/usr/bin/env sh

module use /gpfs/software/juwels/otherstages
module load Stages/2018a

export PYTHONPATH=$HOME/tools/GRID_LRT/:${PYTHONPATH}
export PYTHONPATH=$HOME/tools/pyyaml/build/lib.linux-x86_64-2.7:${PYTHONPATH}

export LOFAR_HOME=/homea/htb00/htb006
LOFAR_INSTALL_DIR="${LOFAR_HOME}/software/LOFAR/2.20.2-centos7"


echo "Using $LOFAR_INSTALL_DIR as root for the LOFAR Software Stack, please change the LOFAR_INSTALL_DIR variable if this is not correct"
if [ ! -d "${LOFAR_INSTALL_DIR}/lofar/release" ]; then
    echo "Directory ${LOFAR_INSTALL_DIR}/lofar/release does not exist !"
    exit 1
fi

export LOCALROOT=${LOFAR_INSTALL_DIR}/local/release

export LD_LIBRARY_PATH=${LOCALROOT}/lib:${LD_LIBRARY_PATH}

export PATH=${LOCALROOT}/bin:${PATH}
