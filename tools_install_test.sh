#!/usr/bin/env sh
module use /gpfs/software/juwels/otherstages
module load Stages/2018b
module load CMake/3.12.3 
module load Python/2.7.15
export CC=/gpfs/software/juwels/stages/2018b/software/GCCcore/7.3.0/bin/gcc
export CXX=/gpfs/software/juwels/stages/2018b/software/GCCcore/7.3.0/bin/g++
mkdir -pv $HOME/tools
cd $HOME/tools/.
wget -O libuuid-1.0.3.tar.gz https://downloads.sourceforge.net/project/libuuid/libuuid-1.0.3.tar.gz?r=https%3A%2F%2Fsourceforge.net%2Fprojects%2Flibuuid%2F\&ts=1508405748\&use_mirror=kent
wget http://pyyaml.org/download/libyaml/yaml-0.1.7.tar.gz
wget http://pyyaml.org/download/pyyaml/PyYAML-3.12.tar.gz
wget https://files.pythonhosted.org/packages/01/5d/82f43ef836ff6c50ce9bd730b4b44c93a8449a31f70bceeda38a5d51a7eb/cloudant-2.12.0.tar.gz
wget https://files.pythonhosted.org/packages/01/62/ddcf76d1d19885e8579acb1b1df26a852b03472c0e46d2b959a714c90608/requests-2.22.0.tar.gz
wget https://files.pythonhosted.org/packages/4c/13/2386233f7ee40aa8444b47f7463338f3cbdf00c316627558784e3f542f07/urllib3-1.25.3.tar.gz
wget https://files.pythonhosted.org/packages/fc/bb/a5768c230f9ddb03acc9ef3f0d4a3cf93462473795d18e9535498c8f929d/chardet-3.0.4.tar.gz
wget https://files.pythonhosted.org/packages/c5/67/5d0548226bcc34468e23a0333978f0e23d28d0b3f0c71a151aef9c3f7680/certifi-2019.6.16.tar.gz
wget https://files.pythonhosted.org/packages/ad/13/eb56951b6f7950cadb579ca166e448ba77f9d24efc03edd7e55fa57d04b7/idna-2.8.tar.gz
git clone https://github.com/fuse4x/fuse.git
git clone -b libcvmfs-stable https://github.com/cvmfs/cvmfs.git
git clone https://github.com/cooperative-computing-lab/cctools.git
git clone https://github.com/apmechev/GRID_LRT.git
git clone https://github.com/apmechev/GRID_PiCaS_Launcher.git
git clone https://github.com/cloudant/python-cloudant.git
mv fuse/include/ fuse/fuse/
export CPATH=$PWD/fuse/:$CPATH
tar xfvz libuuid-1.0.3.tar.gz
tar xfvz yaml-0.1.7.tar.gz
tar xfvz PyYAML-3.12.tar.gz
tar xfv cloudant-2.12.0.tar.gz
tar xfv requests-2.22.0.tar.gz
tar xfv urllib3-1.25.3.tar.gz
tar xfv chardet-3.0.4.tar.gz
tar xfv certifi-2019.6.16.tar.gz
tar xfv idna-2.8.tar.gz
cd libuuid-1.0.3
./configure --prefix=/tmp/cvmfs/UUID --enable-static --disable-shared
make -j 80
make install -j 80
cd ../cvmfs
sed -i 's/attr\/xattr.h/linux\/xattr.h/g' CMakeLists.txt
cmake -Wno-dev -DINSTALL_MOUNT_SCRIPTS=OFF -DBUILD_SERVER:BOOL=OFF -DBUILD_CVMFS:BOOL=OFF -DBUILD_LIBCVMFS:BOOL=ON -DINSTALL_BASH_COMPLETION:BOOL=OFF -DCMAKE_INSTALL_PREFIX:PATH=/tmp/cvmfs/CVMFS -DUUID_LIBRARY:FILE=/tmp/cvmfs/UUID/lib/libuuid.a -DUUID_INCLUDE_DIR:PATH=/tmp/cvmfs/UUID/include
make libpacparser -j 80
cd cvmfs
sed -i 's/attr\/xattr.h/linux\/xattr.h/g' platform_linux.h
make -j 80
make install -j 80
cd ../../cctools
./configure --with-cvmfs-path /tmp/cvmfs/CVMFS --prefix=$HOME/tools/parrot/
make clean -j 80
make -j 80
make install -j 80
cd ../yaml-0.1.7/
./configure --prefix=$HOME/tools/yaml/
make -j 80
make install -j 80
export CPATH=$HOME/tools/yaml/include:${CPATH}
cd ../PyYAML-3.12/
python setup.py build
python setup.py install --prefix=$HOME/tools/pyyaml/
cd ../cloudant-2.12.0/.
mkdir -pv $HOME/tools/lib/python2.7/site-packages
export PYTHONPATH=$HOME/tools/lib/python2.7/site-packages/:${PYTHONPATH}
python setup.py build
python setup.py install --prefix=$HOME/tools/
cd ../requests-2.22.0/.
python setup.py build
python setup.py install --prefix=$HOME/tools/
cd ../urllib3-1.25.3/.
python setup.py build
python setup.py install --prefix=$HOME/tools/
cd ../chardet-3.0.4/.
python setup.py build
python setup.py install --prefix=$HOME/tools/
cd ../ certifi-2019.6.16/
python setup.py build
python setup.py install --prefix=$HOME/tools/
cd ../idna-2.8/
python setup.py build
python setup.py install --prefix=$HOME/tools/