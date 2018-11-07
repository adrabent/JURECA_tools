#!/usr/bin/env sh

module use /gpfs/software/juwels/otherstages
module load Stages/2018a

module load GCC/5.5.0 MVAPICH2/2.3a-GDR
module load FFTW/3.3.7
module load Python/2.7.14

export LOFAR_HOME=/gpfs/homea/htb00/htb006
LOFAR_INSTALL_DIR="${LOFAR_HOME}/software/LOFAR/2.20.2-centos7"


echo "Using $LOFAR_INSTALL_DIR as root for the LOFAR Software Stack, please change the LOFAR_INSTALL_DIR variable if this is not correct"
if [ ! -d "${LOFAR_INSTALL_DIR}/lofar/release" ]; then
    echo "Directory ${LOFAR_INSTALL_DIR}/lofar/release does not exist !"
    exit 1
fi

export LOSOTOROOT="${LOFAR_HOME}/software/losoto/current"

export LOFARROOT=${LOFAR_INSTALL_DIR}/lofar/release 
export LOCALROOT=${LOFAR_INSTALL_DIR}/local/release

export LD_LIBRARY_PATH=${LOFARROOT}/lib64:${LD_LIBRARY_PATH}
export LD_LIBRARY_PATH=${LOCALROOT}/lib:${LD_LIBRARY_PATH}

export LD_LIBRARY_PATH=${LOFAR_HOME}/software/factor_prereqs/dysco/lib/usr/local/lib:${LD_LIBRARY_PATH}

export PATH=${LOFARROOT}/bin:${PATH}
export PATH=${LOCALROOT}/bin:${PATH}
export PATH=${LOSOTOROOT}/bin:${PATH}
 
export PATH=${LOFAR_HOME}/software/RMextract/RMextract:${PATH}
export PATH=${LOFAR_HOME}/software/factor_prereqs/dysco/lib/usr/local/bin:${PATH}
export PATH=${LOFAR_HOME}/software/wsclean/wsclean-2.3/build:${PATH}

export PYTHONPATH=${LOFARROOT}/lib/python2.7/site-packages:${PYTHONPATH}
export PYTHONPATH=${LOCALROOT}/lib/python2.7/site-packages:${PYTHONPATH}
export PYTHONPATH=${LOSOTOROOT}/lib/python2.7/site-packages:${PYTHONPATH}

export PYTHONPATH=${LOFAR_HOME}/software/RMextract:${PYTHONPATH}
