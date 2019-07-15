#!/usr/bin/env sh
module use /usr/local/software/jureca/OtherStages
module load Stages/2017b
module load CMake/3.9.4
export CC=/usr/local/software/jureca/Stages/2017b/software/GCCcore/5.4.0/bin/gcc
export CXX=/usr/local/software/jureca/Stages/2017b/software/GCCcore/5.4.0/bin/g++
cd ./tools/.
wget -O libuuid-1.0.3.tar.gz https://downloads.sourceforge.net/project/libuuid/libuuid-1.0.3.tar.gz?r=https%3A%2F%2Fsourceforge.net%2Fprojects%2Flibuuid%2F\&ts=1508405748\&use_mirror=kent
wget http://pyyaml.org/download/libyaml/yaml-0.1.7.tar.gz
wget http://pyyaml.org/download/pyyaml/PyYAML-3.12.tar.gz
git clone https://github.com/fuse4x/fuse.git
git clone -b libcvmfs-stable https://github.com/cvmfs/cvmfs.git
git clone https://github.com/cooperative-computing-lab/cctools.git
git clone https://github.com/apmechev/GRID_LRT.git
mv fuse/include/ fuse/fuse/
export CPATH=$PWD/fuse/:$CPATH
tar xfvz libuuid-1.0.3.tar.gz
tar xfvz yaml-0.1.7.tar.gz
tar xfvz PyYAML-3.12.tar.gz
cd libuuid-1.0.3
./configure --prefix=/tmp/cvmfs/UUID --enable-static --disable-shared
make
make install
cd ../cvmfs
sed -i 's/attr\/xattr.h/linux\/xattr.h/g' CMakeLists.txt
cmake -Wno-dev -DINSTALL_MOUNT_SCRIPTS=OFF -DBUILD_SERVER:BOOL=OFF -DBUILD_CVMFS:BOOL=OFF -DBUILD_LIBCVMFS:BOOL=ON -DINSTALL_BASH_COMPLETION:BOOL=OFF -DCMAKE_INSTALL_PREFIX:PATH=/tmp/cvmfs/CVMFS -DUUID_LIBRARY:FILE=/tmp/cvmfs/UUID/lib/libuuid.a -DUUID_INCLUDE_DIR:PATH=/tmp/cvmfs/UUID/include
make libpacparser
cd cvmfs
sed -i 's/attr\/xattr.h/linux\/xattr.h/g' platform_linux.h
make
make install
cd ../../cctools
./configure --with-cvmfs-path /tmp/cvmfs/CVMFS --prefix=$HOME/tools/parrot/
make clean
make
make install
cd ../yaml-0.1.7/
./configure --prefix=$HOME/tools/yaml/
make
make install
export CPATH=$HOME/tools/yaml/include:${CPATH}
cd ../PyYAML-3.12/
python setup.py build
python setup.py install --prefix=$HOME/tools/pyyaml/