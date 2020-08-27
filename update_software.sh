#!/usr/bin/env sh

module use /gpfs/software/juwels/otherstages
module load Stages/2018a

module load GCC/5.5.0 MVAPICH2/2.3a-GDR

export PATH=$PROJECT_chtb00/htb006/tools/parrot/bin:$PATH

export HTTP_PROXY="DIRECT;"
export PARROT_CVMFS_REPO="softdrive.nl:url=http://cvmfs01.nikhef.nl/cvmfs/softdrive.nl/,pubkey=$PROJECT_chtb00/htb006/softdrive.nl.pub"

parrot_run rsync -auvP --stats --delete --force --delete-excluded /cvmfs/softdrive.nl/fsweijen/software/ $PROJECT_chtb00/htb006/software_test2/.

## update files in the directories

sed -i 's/#!\/cvmfs\/softdrive.nl\/fsweijen\/software\/python-2.7\/bin\/python/#!\/bin\/env\ python/g' $PROJECT_chtb00/htb006/software_test2/lofar/losoto/bin/losoto
sed -i 's/#!\/cvmfs\/softdrive.nl\/fsweijen\/software\/python-2.7\/bin\/python/#!\/bin\/env\ python/g' $PROJECT_chtb00/htb006/software_test2/lofar/losoto/bin/H5parm_collector.py
sed -i 's/#!\/cvmfs\/softdrive.nl\/fsweijen\/software\/python-2.7\/bin\/python/#!\/bin\/env\ python/g' $PROJECT_chtb00/htb006/software_test2/lofar/pybdsf/bin/pybdsf
sed -i 's/#!\/cvmfs\/softdrive.nl\/fsweijen\/software\/python-2.7\/bin\/python/#!\/bin\/env\ python/g' $PROJECT_chtb00/htb006/software_test2/lofar/pybdsf/bin/pybdsm
sed -i 's/#!\/cvmfs\/softdrive.nl\/fsweijen\/software\/python-2.7\/bin\/python/#!\/bin\/env\ python/g' $PROJECT_chtb00/htb006/software_test2/lofar/lsmtool/bin/lsmtool

rm -rfv $PROJECT_chtb00/htb006/software_test2/lofar_python-2.7_venv/lib/python2.7/site-packages/bdsf*
rm -rfv $PROJECT_chtb00/htb006/software_test2/lofar_python-2.7_venv/lib/python2.7/site-packages/casacore*

## Installing PyBDSF
source ./env_lofar_3.0_juwels.sh

export CPATH=${LOFAR_INSTALL_DIR}/boost/include:${CPATH}
export LIBRARY_PATH=${LOFAR_INSTALL_DIR}/boost/lib/:${LIBRARY_PATH}
export PYTHONPATH=${LOFAR_INSTALL_DIR}/PyBDSF/lib64/python2.7/site-packages/:${PYTHONPATH}

cd ${LOFAR_INSTALL_DIR}
git clone https://github.com/lofar-astron/PyBDSF.git
cd PyBDSF
git checkout tags/v1.9.0
mkdir -pv ${LOFAR_INSTALL_DIR}/PyBDSF/lib64/python2.7/site-packages
python setup.py install --prefix=${LOFAR_INSTALL_DIR}/PyBDSF/

## Overwrite remotecommand.py from the genericpipeline.py to match the JUWELS nomenclature
wget -O ${LOFARROOT}/lib/python2.7/site-packages/lofarpipe/support/remotecommand.py https://raw.githubusercontent.com/adrabent/JURECA_tools/master/software/LOFAR/2.20.2-centos7/lofar/release/lib/python2.7/site-packages/lofarpipe/support/remotecommand.py

