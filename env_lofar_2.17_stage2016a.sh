#!/bin/sh

module use /usr/local/software/jureca/OtherStages
module load Stages/2016a

export PYTHONPATH=/homea/htb00/htb003/local_jureca_stack2016a/lib/python2.7/site-packages
export PYTHONPATH=/homea/htb00/htb003/lofar_jureca_2.17_stack2016a/lib/python2.7/site-packages:$PYTHONPATH
#export PYTHONHOME=/homea/htb00/htb003/local_jureca_stack2016a
#
export PATH=/homea/htb00/htb003/local_jureca_stack2016a/bin:$PATH
export PATH=/homea/htb00/htb003/lofar_jureca_2.17_stack2016a/bin:$PATH
export PATH=/homea/htb00/htb003/lofar_jureca_2.17_stack2016a/sbin:$PATH
#
export LD_LIBRARY_PATH=/homea/htb00/htb003/lofar_jureca_2.17_stack2016a/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/homea/htb00/htb003/lofar_jureca_2.17_stack2016a/lib64:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/homea/htb00/htb003/local_jureca_stack2016a/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/homea/htb00/htb003/local_jureca_stack2016a/lib64:$LD_LIBRARY_PATH
#
export LOFAR_BUILD_DIR=/homea/htb00/htb003
export LOFAR_MAKER=release
export F77=gfortran
export FC=gfortran
export BLAS=/usr/local/software/jureca/Stages/2016a/software/OpenBLAS/0.2.15-GCC-5.3.0-2.26-LAPACK-3.6.0/lib/libopenblas.so
export LAPACK=/usr/local/software/jureca/Stages/2016a/software/OpenBLAS/0.2.15-GCC-5.3.0-2.26-LAPACK-3.6.0/lib/libopenblas.so
export LOFARROOT=${LOFAR_BUILD_DIR}/lofar_jureca_2.17_stack2016a
#
module load GCC/5.3.0-2.26  ParaStationMPI/5.1.5-1 
module load Python/2.7.11
module load CMake/3.4.3
module load Boost/1.60.0-Python-2.7.11
module load GSL/2.1
module load HDF5/1.8.16
module load flex/2.6.0
module load XML-LibXML/2.0124-Perl-5.22.1
module load SciPy-Stack/2016a-Python-2.7.11
export PYTHONSTARTUP=/homea/htb00/htb003/pythonstart
#
export CC=/usr/local/software/jureca/Stages/2016a/software/GCCcore/5.3.0/bin/gcc
export CXX=/usr/local/software/jureca/Stages/2016a/software/GCCcore/5.3.0/bin/g++
#
export PGPLOT_DIR=/homea/htb00/htb003/local_jureca/pgplot
export PGPLOT_DEV=/Xserve
#
export PKG_CONFIG_PATH=/homea/htb00/htb003/local_jureca_stack2016a/lib/pkgconfig:$PKG_CONFIG_PATH
# since Lofar 2.15. Flags for dependency building of aoflagger
# only for building aoflagger
export LDFLAGS=-L/homea/htb00/htb003/local_jureca_stack2016a/lib
export CPPFLAGS=-L/homea/htb00/htb003/local_jureca_stack2016a/include
#
#export LD_LIBRARY_PATH=/homea/htb00/htb003/local_aoflagger/lib:$LD_LIBRARY_PATH
#export LD_LIBRARY_PATH=/homea/htb00/htb003/source/aoflagger-2.7.1/build/src:$LD_LIBRARY_PATH
#export PATH=/homea/htb00/htb003/local_aoflagger/bin:$PATH
export GSETTINGS_SCHEMA_DIR=/homea/htb00/htb003/local_jureca/share/glib-2.0/schemas
