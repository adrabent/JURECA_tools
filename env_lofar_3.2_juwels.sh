## installation settings
export STAGE=2018b

## directories for libraries
export INSTALLDIR=${PROJECT_chtb00}/htb006/software
export SOFTWARE=/gpfs/software/juwels/stages/${STAGE}/software
export LOFARROOT=${INSTALLDIR}/lofar
export LOSOTO=${LOFARROOT}
export AOFLAGGER=${INSTALLDIR}/aoflagger/bin/aoflagger

## load JUWELS environment
module use /gpfs/software/juwels/otherstages
module load Stages/${STAGE}


module load GCC/8.2.0  ParaStationMPI/5.2.1-1

module load HDF5
module load SciPy-Stack/2018b-Python-2.7.15
module load Boost/1.68.0-Python-2.7.15
module load FFTW/3.3.8
module load GSL

# module load Python/2.7.15
# module load CFITSIO
# module load Doxygen


ulimit -n 4096
export HDF5_USE_FILE_LOCKING=FALSE
export OPENBLAS_NUM_THREADS=1
export HDF5_DISABLE_VERSION_CHECK=1

## source LOFAR base
export PATH=${LOFARROOT}/bin:${PATH}
export LD_LIBRARY_PATH=${LOFARROOT}/lib:${LD_LIBRARY_PATH}
export LD_LIBRARY_PATH=${LOFARROOT}/lib64:${LD_LIBRARY_PATH}
export PYTHONPATH=${INSTALLDIR}/lofar/lib/python2.7/site-packages:${PYTHONPATH}

## source casacore
export LD_LIBRARY_PATH=${INSTALLDIR}/casacore/lib:${LD_LIBRARY_PATH}
export PATH=${INSTALLDIR}/casacore/bin:${PATH}

## source aoflagger
export LD_LIBRARY_PATH=${INSTALLDIR}/aoflagger/lib:${LD_LIBRARY_PATH}
export PATH=${INSTALLDIR}/aoflagger/bin:$PATH

## source armadillo
export LD_LIBRARY_PATH=${INSTALLDIR}/armadillo/lib64/:${LD_LIBRARY_PATH}

## source dysco
export LD_LIBRARY_PATH=${INSTALLDIR}/dysco/lib:${LD_LIBRARY_PATH}

## source DPPP
export PATH=${INSTALLDIR}/DPPP/bin:${PATH}

## source missing shared libraries
export LD_LIBRARY_PATH=${INSTALLDIR}/../lib:${LD_LIBRARY_PATH}
