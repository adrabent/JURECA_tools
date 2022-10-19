## installation settings
module use /gpfs/software/juwels/otherstages
export STAGE=Devel-2020

## load JUWELS environment
module load Stages/${STAGE}

module load GCCcore/.9.3.0
module load nodejs

## load software environment
export PROJECT=${PROJECT_chtb00}/htb006
export SCRATCH=${SCRATCH_chtb00}/htb006
export SOFTWARE=${PROJECT}/software_new

## Singularity software environment
export SINGULARITY_CACHEDIR=${SOFTWARE}
export SINGULARITY_TMPDIR='/tmp'
export SINGULARITY_PULLDIR=${SINGULARITY_CACHEDIR}/pull
export CWL_SINGULARITY_CACHE=${SINGULARITY_PULLDIR}


## Set PATH environment
export PATH=${SOFTWARE}/envs/toil/bin:${PATH}
# export PATH=${SOFTWARE}/envs/toil_srun/bin:${PATH}

## CWL software aliases
alias toil='toil-cwl-runner'

# export CWL_OPTIONS='--singularity --disableCaching --bypass-file-store --writeLogsFromAllJobs --stats --retryCount 3 --batchSystem single_machine'
export CWL_OPTIONS='--singularity --bypass-file-store --writeLogsFromAllJobs --retryCount 3 --batchSystem single_machine --maxCores 24'
export CWLTOOL_OPTIONS='--singularity --parallel'
#export TOIL_SLURM_ARGS='--account=htb00 --time=00:01:00 --export=ALL'
# export TOIL_SLURM_JOBID=${SLURM_JOB_ID}

## Prefactor function definition
linc () {
    export WORKDIR=$1/runtime
    export OUTDIR=$1/results
    export LOGDIR=$1/logs
    export PIPELINE=$2
    export INPUT_JSON=$3
#     export TMPDIR=${WORKDIR}
    mkdir -pv ${OUTDIR} ${WORKDIR}  ${LOGDIR}
    toil --version
#     toil ${CWL_OPTIONS} --tmp-outdir-prefix ${WORKDIR}/cwl- --tmpdir-prefix ${WORKDIR}/tmp- --outdir ${OUTDIR} --log-dir ${LOGDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${SOFTWARE}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
#     toil ${CWL_OPTIONS} --tmp-outdir-prefix ${WORKDIR}/cwl- --outdir ${OUTDIR} --log-dir ${LOGDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${SOFTWARE}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
    toil ${CWL_OPTIONS} --tmp-outdir-prefix ${WORKDIR}/cwl- --outdir ${OUTDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${SOFTWARE}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
}

linc_restart () {
    export WORKDIR=$1/runtime
    export OUTDIR=$1/results
    export LOGDIR=$1/logs
    export PIPELINE=$2
    export INPUT_JSON=$3
#     export TMPDIR=${WORKDIR}
    mkdir -pv ${OUTDIR} ${WORKDIR}  ${LOGDIR}
    toil --version
    toil ${CWL_OPTIONS} --restart --tmp-outdir-prefix ${WORKDIR}/cwl- --outdir ${OUTDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${SOFTWARE}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
}

linc_cwltool () {
    WORKDIR=$1/pipeline/
    OUTDIR=$1/results/
    LOGDIR=$1/logs/
    PIPELINE=$2
    INPUT_JSON=$3
    mkdir -pv ${OUTDIR} ${WORKDIR}
    cwltool ${CWLTOOL_OPTIONS} --tmpdir-prefix ${WORKDIR}/ --outdir ${OUTDIR} --log-dir ${LOGDIR} ${SOFTWARE}/workflows/${PIPELINE}.cwl  ${INPUT_JSON}
}
