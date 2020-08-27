module use /gpfs/software/juwels/otherstages
module load Stages/2018b

module load GCC/7.3.0  MVAPICH2/2.3-GDR
#module spider MVAPICH2/2.3-GDR
module load FFTW/3.3.8
#module spider FFTW/3.3.8

ulimit -n 4096
export HDF5_USE_FILE_LOCKING=FALSE
export OPENBLAS_NUM_THREADS=1

export LOFAR_HOME=$PROJECT_chtb00/htb006/software/
export LOFAR_INSTALL_DIR=${LOFAR_HOME}/lofar
export LOFARROOT=${LOFAR_INSTALL_DIR}/lofar

## source LOFAR base
export PATH=${LOFARROOT}/bin:${PATH}

## source DPPP
export PATH=${LOFAR_INSTALL_DIR}/DPPP/bin:$PATH

## source HDF5
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/hdf5/lib:${LD_LIBRARY_PATH}

## source casacore
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/casacore/lib:${LD_LIBRARY_PATH}
export PYTHONPATH=${LOFAR_INSTALL_DIR}/python-casacore/lib/python2.7/site-packages/:${PYTHONPATH}
export PATH=${LOFAR_INSTALL_DIR}/casacore/bin:$PATH

## source cfitsio
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/cfitsio/lib:${LD_LIBRARY_PATH}

## source openblas
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/openblas/lib/:${LD_LIBRARY_PATH}

## source LOFARbeam library
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/LOFARBeam/lib:${LD_LIBRARY_PATH}

## source aoflagger
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/aoflagger/lib:${LD_LIBRARY_PATH}
export PATH=${LOFAR_INSTALL_DIR}/aoflagger/bin:$PATH

## source boost
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/boost/lib/:${LD_LIBRARY_PATH}

## source armadillo
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/armadillo/lib64/:${LD_LIBRARY_PATH}

## source external C-libraries
export LD_LIBRARY_PATH=${LOFAR_HOME}/../lib:${LD_LIBRARY_PATH}

## source superLU
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/superlu/lib64/:${LD_LIBRARY_PATH}

## source PyBDSF
export PATH=${LOFAR_INSTALL_DIR}/PyBDSF/bin:$PATH
export PYTHONPATH=${LOFAR_INSTALL_DIR}/PyBDSF/lib64/python2.7/site-packages:${PYTHONPATH}

## source losoto + required python packages
export PATH=${LOFAR_INSTALL_DIR}/losoto/bin:$PATH
export PYTHONPATH=${LOFAR_INSTALL_DIR}/losoto/lib/python2.7/site-packages/:${PYTHONPATH}
export PYTHONPATH=${LOFAR_HOME}/lofar_python-2.7_venv/lib/python2.7/site-packages:${PYTHONPATH}

## source lsmtool + required LOFAR libraries
export PATH=${LOFAR_INSTALL_DIR}/lsmtool/bin/:${PATH}
export PYTHONPATH=${LOFAR_INSTALL_DIR}/lsmtool/lib/python2.7/site-packages/:${PYTHONPATH}
export PYTHONPATH=${LOFARROOT}/lib/python2.7/site-packages:${PYTHONPATH}
export LD_LIBRARY_PATH=${LOFARROOT}/lib64:${LD_LIBRARY_PATH}

# ## source RMextract
export PYTHONPATH=${LOFAR_INSTALL_DIR}/RMextract/lib/python2.7/site-packages/:${PYTHONPATH}

# ## source wsclean + IDG libraries
export PATH=${LOFAR_INSTALL_DIR}/wsclean/bin:$PATH
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/idg/lib:${LD_LIBRARY_PATH}
 
## source dysco
export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/dysco/lib:${LD_LIBRARY_PATH}

## add environment variables
# export RUNDIR=$SCRATCH_chtb00/htb006
export LOSOTO=${LOFAR_INSTALL_DIR}/losoto
export AOFLAGGER=${LOFAR_INSTALL_DIR}/aoflagger/bin/aoflagger

# export LD_LIBRARY_PATH=${LOFAR_INSTALL_DIR}/wcslib/:${LD_LIBRARY_PATH}

# export LD_LIBRARY_PATH=${LOFAR_HOME}/PyBDSF/src/c++:${LD_LIBRARY_PATH}
# export LDFLAGS=${LOFAR_INSTALL_DIR}/boost/lib/:${LDFLAGS}

## disable HDF5 version check
export HDF5_DISABLE_VERSION_CHECK=1
