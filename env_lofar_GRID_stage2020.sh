#!/usr/bin/env sh

export PROJECT=$PROJECT_chtb00/htb006

export PYTHONPATH=$PROJECT/tools/GRID_LRT2:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/GRID_PiCaS_Launcher:${PYTHONPATH}
export PYTHONPATH=$PROJECT/tools/cloudant/lib/python2.7/site-packages:${PYTHONPATH}

## source casacore
export LOFAR_HOME=$PROJECT_chtb00/htb006/software_test2/
export LOFAR_INSTALL_DIR=${LOFAR_HOME}/lofar
export PATH=${LOFAR_INSTALL_DIR}/casacore/bin:$PATH
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/casacore/lib:${LD_LIBRARY_PATH}

## source HDF5
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/hdf5/lib:${LD_LIBRARY_PATH}
