#!/usr/bin/env sh

# module use /gpfs/software/juwels/otherstages
# module load Stages/2018a

# export PROJECT=$PROJECT_chtb00/htb006

export PROJECT=/p/home/jusers/drabent1/juwels/

export PYTHONPATH=$PROJECT/tools/GRID_LRT/:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/GRID_PiCaS_Launcher:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/GRID_PiCaS_Launcher/GRID_PiCaS_Launcher:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/pyyaml/build/lib.linux-x86_64-2.7:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/lib/python2.7/site-packages/:${PYTHONPATH}

export LOFAR_HOME=$PROJECT_chtb00/htb006/software_test2/
export LOFAR_INSTALL_DIR=${LOFAR_HOME}/lofar

export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/hdf5/lib:${LD_LIBRARY_PATH}
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/casacore/lib:${LD_LIBRARY_PATH}
export LD_LIBRARY_PATH=${LOFAR_HOME}/../lib:${LD_LIBRARY_PATH}

export PATH=${LOFAR_INSTALL_DIR}/casacore/bin:$PATH


# LOFAR_INSTALL_DIR="${PROJECT}/software/LOFAR/2.20.2-centos7"
# 
# 
# echo "Using $LOFAR_INSTALL_DIR as root for the LOFAR Software Stack, please change the LOFAR_INSTALL_DIR variable if this is not correct"
# if [ ! -d "${LOFAR_INSTALL_DIR}/lofar/release" ]; then
#     echo "Directory ${LOFAR_INSTALL_DIR}/lofar/release does not exist !"
#     exit 1
# fi
# 
# export LOCALROOT=${LOFAR_INSTALL_DIR}/local/release
# 
# export LD_LIBRARY_PATH=${LOCALROOT}/lib:${LD_LIBRARY_PATH}
# 
# export PATH=${LOCALROOT}/bin:${PATH}
