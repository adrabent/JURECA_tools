#!/bin/bash

export PROJECT=${PROJECT_chtb00}/htb006
export SOFTWARE=${PROJECT}/software_new

export SINGULARITY_CACHEDIR=${SOFTWARE}
export SINGULARITY_PULLDIR=${SINGULARITY_CACHEDIR}/pull
export CWL_SINGULARITY_CACHE=${SINGULARITY_PULLDIR}

## rebuild Singularity image
# singularity cache clean -f
# singularity pull --force --name astronrd_linc.sif docker://astronrd/linc

## running scripts to get necessary target data
# singularity exec docker://astronrd/linc createRMh5parm.py --ionexpath /p/scratch/chtb00/htb006/test/runtime --server ftp://ftp.aiub.unibe.ch/CODE/ --solsetName target /p/scratch/chtb00/htb006/test/L746864_SB001_uv.MS /p/scratch/chtb00/htb006/test/cal_solutions.h5
# singularity exec docker://astronrd/linc download_skymodel_target.py /p/scratch/chtb00/htb006/test/L746864_SB001_uv.MS /p/scratch/chtb00/htb006/test/target.skymodel

## submit job for calibrator
# sbatch --nodes=1 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=06:00:00 --account=htb00 /p/project/chtb00/htb006/run_prefactor.sh /p/scratch/chtb00/htb006/test HBA_calibrator /p/project/chtb00/htb006/prefactor.json

## submit job for target
sbatch --nodes=1 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=06:00:00 --account=htb00 /p/project/chtb00/htb006/run_prefactor.sh /p/scratch/chtb00/htb006/test HBA_target /p/project/chtb00/htb006/prefactor_target.json
