#!/bin/bash

export PROJECT=${PROJECT_chtb00}/htb006
export SOFTWARE=${PROJECT}/software_new

export SINGULARITY_CACHEDIR=${SOFTWARE}
export SINGULARITY_TMPDIR='/tmp'
export SINGULARITY_PULLDIR=${SINGULARITY_CACHEDIR}/pull
export CWL_SINGULARITY_CACHE=${SINGULARITY_PULLDIR}

## rebuild Singularity image
#singularity cache clean -f
#singularity pull --force --name astronrd_linc.sif docker://astronrd/linc


## submit job for calibrator
# sbatch --nodes=1 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=06:00:00 --account=htb00 /p/project/chtb00/htb006/run_linc_restart.sh /p/scratch/chtb00/htb006/test HBA_calibrator /p/project/chtb00/htb006/prefactor.json

## submit job for target
# sbatch --nodes=1 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=06:00:00 --account=htb00 /p/project/chtb00/htb006/run_prefactor.sh /p/scratch/chtb00/htb006/test HBA_target /p/project/chtb00/htb006/prefactor_target.json

## submit job for certain test
sbatch --nodes=1 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=00:10:00 --account=htb00 /p/project/chtb00/htb006/run_flagextend.sh
