#!/usr/bin/env bash

## LOFAR Installation script

## installation settings
export J=`nproc`
export STAGE=2018b

## directories for libraries
export INSTALLDIR=$PROJECT_chtb00/htb006/software
export SOFTWARE=/gpfs/software/juwels/stages/${STAGE}/software

## Define versions to install
export PYTHON_VERSION=2.7
export CASACORE_VERSION=v2.4.1
export PYTHON_CASACORE_VERSION=v2.2.1
export DYSCO_VERSION=v1.2
export AOFLAGGER_VERSION=v2.14.0
export LOFAR_VERSION=3_2_18
export DPPP_VERSION=v4.1
export RMEXTRACT_VERSION=4c61232
export LSMTOOL_VERSION=v1.4.2
export LOSOTO2_VERSION=c8fbd61

## run SVN before new stage is initialized
# rm -rfv ${INSTALLDIR}/lofar && mkdir -pv ${INSTALLDIR}/lofar/lib/python2.7/site-packages

# if [ "${LOFAR_VERSION}" = "latest" ];  then cd ${INSTALLDIR}/lofar && svn checkout https://svn.astron.nl/LOFAR/trunk src; fi
# if [ "${LOFAR_VERSION}" != "latest" ]; then cd ${INSTALLDIR}/lofar && svn checkout https://svn.astron.nl/LOFAR/tags/LOFAR-Release-${LOFAR_VERSION} src; fi
# cd ${INSTALLDIR}/lofar && svn update --depth=infinity ${INSTALLDIR}/lofar/src/CMake

## load JUWELS environment
module use /gpfs/software/juwels/otherstages
module load Stages/${STAGE}

module load GCC/8.2.0  ParaStationMPI/5.2.1-1

module load Python/2.7.15
module load Boost/1.68.0-Python-2.7.15
module load FFTW/3.3.8
module load SciPy-Stack/2018b-Python-2.7.15
module load CMake
module load HDF5
module load CFITSIO
module load Doxygen
module load GSL

# module load PGI

export CMAKE_PREFIX_PATH=`echo $LD_LIBRARY_PATH  | sed 's/\/lib//g'`
export CMAKE_PREFIX_PATH=${SOFTWARE}/libpng/1.6.35-GCCcore-8.2.0:${CMAKE_PREFIX_PATH}
export PYTHONPATH=${INSTALLDIR}/lofar/lib/python2.7/site-packages:${PYTHONPATH}

#
# Install WCSLIB
#

# rm -rfv ${INSTALLDIR}/wcslib && mkdir -pv ${INSTALLDIR}/wcslib

# cd ${INSTALLDIR}/wcslib
# wget ftp://ftp.atnf.csiro.au/pub/software/wcslib/wcslib.tar.bz2 && bzip2 -vd wcslib.tar.bz2 && tar xfv wcslib.tar
# cd wcslib-7.3.1 && ./configure --prefix ${INSTALLDIR}/wcslib
# gmake -j ${J} && gmake install -j ${J}

# rm -rfv ../wcslib-7.3.1 ../wcslib.tar

export LD_LIBRARY_PATH=${INSTALLDIR}/wcslib/lib:${LD_LIBRARY_PATH}
export CPATH=${INSTALLDIR}/wcslib/include:${CPATH}
export CMAKE_PREFIX_PATH=${INSTALLDIR}/wcslib:${CMAKE_PREFIX_PATH}

#
# Install superlu
#

# rm -rfv ${INSTALLDIR}/superlu && mkdir -pv ${INSTALLDIR}/superlu/build

# cd ${INSTALLDIR}/superlu && git clone https://github.com/xiaoyeli/superlu.git src
# cd ${INSTALLDIR}/superlu/build && cmake -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/superlu -DCMAKE_C_FLAGS="-fPIC" ../src
# make -s -j ${J}  && make install -j ${J}

# rm -rfv ${INSTALLDIR}/superlu/build && rm -rfv ${INSTALLDIR}/superlu/src

export LD_LIBRARY_PATH=${INSTALLDIR}/superlu/lib64:${LD_LIBRARY_PATH}
export CPATH=${INSTALLDIR}/superlu/include:${CPATH}
export CMAKE_PREFIX_PATH=${INSTALLDIR}/superlu:${CMAKE_PREFIX_PATH}

#
# Install armadillo
#

# rm -rfv ${INSTALLDIR}/armadillo 
# cd ${INSTALLDIR} && wget http://sourceforge.net/projects/arma/files/armadillo-9.900.4.tar.xz && tar xfv armadillo-9.900.4.tar.xz && rm -rfv armadillo-9.900.4.tar.xz
# mv -v ${INSTALLDIR}/armadillo-9.900.4 ${INSTALLDIR}/armadillo
# mkdir -pv ${INSTALLDIR}/armadillo/build && cd ${INSTALLDIR}/armadillo/build
# cmake -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/armadillo ..
# make -s -j ${J} && make install -j ${J}

# rm -rfv ${INSTALLDIR}/armadillo/build && rm -rfv ${INSTALLDIR}/armadillo/src

export LD_LIBRARY_PATH=${INSTALLDIR}/armadillo/lib64:${LD_LIBRARY_PATH}
export CPATH=${INSTALLDIR}/armadillo/include:${CPATH}
export CMAKE_PREFIX_PATH=${INSTALLDIR}/armadillo:${CMAKE_PREFIX_PATH}

#
# Install CASAcore
#

# rm -rfv ${INSTALLDIR}/casacore
# mkdir -pv ${INSTALLDIR}/casacore/build && mkdir -pv ${INSTALLDIR}/casacore/data

# cd ${INSTALLDIR}/casacore && git clone https://github.com/casacore/casacore.git src
# if [ "${CASACORE_VERSION}" != "latest" ]; then cd ${INSTALLDIR}/casacore/src && git checkout tags/${CASACORE_VERSION}; fi
# cd ${INSTALLDIR}/casacore/data && wget --retry-connrefused ftp://anonymous@ftp.astron.nl/outgoing/Measures/WSRT_Measures.ztar && tar xfv WSRT_Measures.ztar && rm -rv WSRT_Measures.ztar
# cd ${INSTALLDIR}/casacore/build && cmake -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/casacore/ -DDATA_DIR=${INSTALLDIR}/casacore/data -DBUILD_PYTHON=True -DUSE_OPENMP=True -DUSE_FFTW3=TRUE -DUSE_HDF5=True -DBUILD_PYTHON3=False -DBoost_PYTHON_LIBRARY_DEBUG=${SOFTWARE}/Boost/1.68.0-gpsmpi-2018b-Python-2.7.15/lib/libboost_python27.a -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/casacore ../src/ 
# make -s -j ${J} && make install -j ${J}

# rm -rfv ${INSTALLDIR}/casacore/build && rm -rfv ${INSTALLDIR}/casacore/src

export LD_LIBRARY_PATH=${INSTALLDIR}/casacore/lib:${LD_LIBRARY_PATH}
export CPATH=${INSTALLDIR}/casacore/include:${CPATH}
export CMAKE_PREFIX_PATH=${INSTALLDIR}/casacore:${CMAKE_PREFIX_PATH}


#
# Install Python-CASAcore
#

# cd ${INSTALLDIR} && git clone https://github.com/casacore/python-casacore
# if [ "$PYTHON_CASACORE_VERSION" != "latest" ]; then cd ${INSTALLDIR}/python-casacore && git checkout tags/${PYTHON_CASACORE_VERSION}; fi
# cd ${INSTALLDIR}/python-casacore && sed -i 's/boost_python/boost_python27/g' setup.py
# python2.7 setup.py build_ext -I${CPATH} -L${LD_LIBRARY_PATH}
# python2.7 setup.py install --prefix=${INSTALLDIR}/lofar

# rm -rfv $INSTALLDIR/python-casacore

#
# Install Dysco
#

# rm -rfv ${INSTALLDIR}/dysco/build && mkdir -pv ${INSTALLDIR}/dysco/build
# 
# cd ${INSTALLDIR}/dysco && git clone https://github.com/aroffringa/dysco.git src
# if [ "${DYDSCO_VERSION}" != "latest" ]; then cd src && git checkout ${DYSCO_VERSION}; fi
# cd ${INSTALLDIR}/dysco/build && cmake -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/dysco -DPORTABLE=True ../src
# make -s -j ${J} && make install -j ${J}
# 
# rm -rfv ${INSTALLDIR}/dysco/build && rm -rfv ${INSTALLDIR}/dysco/src


export LD_LIBRARY_PATH=${INSTALLDIR}/dysco/lib:${LD_LIBRARY_PATH}
export CMAKE_PREFIX_PATH=${INSTALLDIR}/dysco:${CMAKE_PREFIX_PATH}

#
# Install AOFlagger
#

# 
# rm -rfv ${INSTALLDIR}/aoflagger

# if [ "${AOFLAGGER_VERSION}" = "latest" ];  then cd ${INSTALLDIR} && git clone git://git.code.sf.net/p/aoflagger/code aoflagger && cd ${INSTALLDIR}/aoflagger; fi
# if [ "${AOFLAGGER_VERSION}" != "latest" ]; then cd ${INSTALLDIR} && git clone git://git.code.sf.net/p/aoflagger/code aoflagger && cd ${INSTALLDIR}/aoflagger && git checkout tags/${AOFLAGGER_VERSION}; fi
# mkdir -p ${INSTALLDIR}/aoflagger/build && cd ${INSTALLDIR}/aoflagger/build
# cmake -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/aoflagger/ -DBUILD_SHARED_LIBS=ON -DPORTABLE=True -DBoost_PYTHON_LIBRARY_DEBUG=${SOFTWARE}/Boost/1.68.0-gpsmpi-2018b-Python-2.7.15/lib/libboost_python27.a -DLAPACK_lapack_LIBRARY=${SOFTWARE}/NCL/6.5.0-gpsmkl-2018b/lib/liblapack_ncl.a ..
# cd ${INSTALLDIR}/aoflagger/build && make -s -j ${J}
# cd ${INSTALLDIR}/aoflagger/build && make install -j ${J}

#  -DGSL_LIB=${SOFTWARE}/GSL/2.5-GCC-8.2.0/lib/libgsl.a -DGSL_CBLAS_LIB=${SOFTWARE}/GSL/2.5-GCC-8.2.0/lib/libgslcblas.a -DBLAS_mkl_intel_thread_LIBRARY=${SOFTWARE}/imkl/2019.0.117/mkl/lib/intel64/libmkl_intel_thread.a
# 
# rm -rfv $INSTALLDIR/aoflagger/build && rm -rfv $INSTALLDIR/aoflagger/src

export LD_LIBRARY_PATH=${INSTALLDIR}/aoflagger/lib:${LD_LIBRARY_PATH}
export CPATH=${INSTALLDIR}/aoflagger/include:${CPATH}
export CMAKE_PREFIX_PATH=${INSTALLDIR}/aoflagger:${CMAKE_PREFIX_PATH}

#
# Install LOFAR-base
#

# mkdir -p ${INSTALLDIR}/lofar/build/gnucxx11_opt
# 
# cd ${INSTALLDIR}/lofar/build/gnucxx11_opt
# cmake -DBUILD_PACKAGES="Pipeline" -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/lofar/ -DUSE_LOG4CPLUS=OFF -DUSE_OPENMP=True -DBUILD_PYTHON3=OFF -DBoost_PYTHON_LIBRARY_DEBUG=${SOFTWARE}/Boost/1.68.0-gpsmpi-2018b-Python-2.7.15/lib/libboost_python27.a -DPYTHON_XMLRUNNER=${SOFTWARE}/Python/2.7.15-GCCcore-8.2.0/lib/python2.7/site-packages/Cython-0.28.5-py2.7-linux-x86_64.egg/Cython/Tests/xmlrunner.py ${INSTALLDIR}/lofar/src/
# make -s -j ${J} && make install -j ${J}
# wget https://raw.githubusercontent.com/adrabent/JURECA_tools/master/software/LOFAR/2.20.2-centos7/lofar/release/lib/python2.7/site-packages/lofarpipe/support/remotecommand.py -O ${INSTALLDIR}/lofar/lib/python2.7/site-packages/lofarpipe/support/remotecommand.py
# 
# rm -rfv ${INSTALLDIR}/lofar/build && rm -rfv ${INSTALLDIR}/lofar/src

#
# Install the standalone StationResponse libraries.
#   

# rm -rfv ${INSTALLDIR}/LOFARBeam/build && mkdir -pv ${INSTALLDIR}/LOFARBeam/build
# 
# cd ${INSTALLDIR}/LOFARBeam && git clone https://github.com/lofar-astron/LOFARBeam.git src
# cd build && cmake -DCMAKE_INSTALL_PREFIX=${INSTALLDIR}/lofar -DBoost_PYTHON_LIBRARY_DEBUG=${SOFTWARE}/Boost/1.68.0-gpsmpi-2018b-Python-2.7.15/lib/libboost_python27.a -DBoost_NUMPY_LIBRARY_DEBUG=${INSTALLDIR}/../lib/libboost_numpy.a ../src 
# cd ${INSTALLDIR}/src && make -j ${J} & make install -j ${J}
# touch ${INSTALLDIR}/lofar/lib64/python2.7/site-packages/lofar/__init__.py
# 
# rm -rfv ${INSTALLDIR}/LOFARBeam
# 
export LD_LIBRARY_PATH=${INSTALLDIR}/lofar/lib:${LD_LIBRARY_PATH}
export CPATH=${INSTALLDIR}/lofar/include:${CPATH}
export CMAKE_PREFIX_PATH=${INSTALLDIR}/lofar:${CMAKE_PREFIX_PATH}

#
# Install DPPP
#

# rm -rfv ${INSTALLDIR}/DPPP && mkdir -p ${INSTALLDIR}/DPPP/build 

# git clone https://github.com/lofar-astron/DP3.git ${INSTALLDIR}/DPPP/src
# cd ${INSTALLDIR}/DPPP/src && git checkout tags/${DPPP_VERSION}
# cd $INSTALLDIR/DPPP/build
# cmake -DCMAKE_CXX_FLAGS="-D_GLIB_USE_CXX_ABI=1 -DBOOST_NO_CXX11_SCOPED_ENUMS" -DCMAKE_INSTALL_PREFIX:PATH=${INSTALLDIR}/lofar -DBoost_PYTHON_LIBRARY_DEBUG=${SOFTWARE}/Boost/1.68.0-gpsmpi-2018b-Python-2.7.15/lib/libboost_python27.a ../src
# make -s -j ${J} && make install -j ${J}
ln -s -v ${INSTALLDIR}/lofar/bin/DPPP ${INSTALLDIR}/lofar/bin/NDPPP

# rm -rfv ${INSTALLDIR}/DPPP

#
# Install RMextract
#

# rm -rfv ${INSTALLDIR}/RMextract && mkdir -pv $INSTALLDIR/RMextract/build
# 
# cd ${INSTALLDIR}/RMextract && git clone https://github.com/lofar-astron/RMextract.git src
# cd src && if [ "${RMEXTRACT_VERSION}" != "latest" ]; then git checkout ${RMEXTRACT_VERSION}; fi
# python2.7 setup.py build --add-lofar-utils
# python2.7 setup.py install --add-lofar-utils --prefix=${INSTALLDIR}/lofar

# rm -rfv ${INSTALLDIR}/RMextract

#
# Install LSMTool.
#

# rm -rfv ${INSTALLDIR}/lsmtool && mkdir -pv ${INSTALLDIR}/lsmtool/build
# 
# cd ${INSTALLDIR}/lsmtool && git clone https://github.com/darafferty/LSMTool.git src
# cd src && if [ "${LSMTOOL_VERSION}" != "latest" ]; then git checkout ${LSMTOOL_VERSION}; fi
# python2.7 setup.py install --prefix=${INSTALLDIR}/lofar
# 
# rm -rfv ${INSTALLDIR}/lsmtool

#
# Install LoSoTo
#
# rm -rfv ${INSTALLDIR}/losoto && mkdir -pv ${INSTALLDIR}/losoto/build
# 
# cd ${INSTALLDIR}/losoto && git clone https://github.com/revoltek/losoto.git src
# cd src && if [ "${LOSOTO2_VERSION}" != "latest" ]; then git checkout ${LOSOTO2_VERSION}; fi
# python2.7 setup.py build
# python2.7 setup.py install --prefix=${INSTALLDIR}/lofar
# 
# rm -rfv ${INSTALLDIR}/losoto

