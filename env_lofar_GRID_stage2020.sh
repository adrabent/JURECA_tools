#!/usr/bin/env sh

export PROJECT=$PROJECT_chtb00/htb006
export SCRATCH=$SCRATCH_chtb00/htb006

export PYTHONPATH=$PROJECT/tools/GRID_LRT:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/GRID_PiCaS_Launcher:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/cloudant/lib/python2.7/site-packages:${PYTHONPATH}

## source casacore
export LOFAR_HOME=$PROJECT_chtb00/htb006/software
export LOFAR_INSTALL_DIR=${LOFAR_HOME}/lofar
export PATH=${LOFAR_HOME}/casacore/bin:$PATH
export LD_LIBRARY_PATH=${LOFAR_HOME}/casacore/lib:${LD_LIBRARY_PATH}

## source missing libraries
export LD_LIBRARY_PATH=${PROJECT}/lib:${LD_LIBRARY_PATH}

## options for globus
export GLOBUS_GSSAPI_MAX_TLS_PROTOCOL=TLS1_2_VERSION 
