## installation settings
export STAGE=2024

## load JUWELS environment
module load Stages/${STAGE}

module load GCCcore/.12.3.0
module load nodejs/.18.17.1

## load software environment
export PROJECT=${PROJECT_chtb00}/htb006
export SCRATCH=${SCRATCH_chtb00}/htb006
export SOFTWARE=${PROJECT}/software_new

## Singularity software environment
export SINGULARITY_CACHEDIR=${SOFTWARE}
# export SINGULARITY_TMPDIR='/tmp'
export SINGULARITY_TMPDIR=${HOME}
export SINGULARITY_PULLDIR=${SINGULARITY_CACHEDIR}/pull
export CWL_SINGULARITY_CACHE=${SINGULARITY_PULLDIR}


## Set PATH environment
export PATH=${SOFTWARE}/envs/toil/bin:${PATH}
# export PATH=${SOFTWARE}/envs/toil_trunk/bin:${PATH}

## CWL software aliases
alias toil='toil-cwl-runner'
# alias toil_trunk='${SOFTWARE}/envs/toil_trunk/bin/toil-cwl-runner'

# export CWL_OPTIONS='--singularity --disableCaching --bypass-file-store --writeLogsFromAllJobs --stats --disableJobStoreChecksumVerification --retryCount 3 --batchSystem single_machine'
export CWL_OPTIONS='--singularity --disableCaching --bypass-file-store --writeLogsFromAllJobs --no-compute-checksum --disableJobStoreChecksumVerification --moveExports --retryCount 5 --batchSystem single_machine --maxCores 48'
export CWLTOOL_OPTIONS='--singularity --parallel'
#export TOIL_SLURM_ARGS='--account=htb00 --time=00:01:00 --export=ALL'
# export TOIL_SLURM_JOBID=${SLURM_JOB_ID}

## Prefactor function definition
linc () {
    export WORKDIR=$1/runtime
    export OUTDIR=$1/output
    export LOGDIR=$1/logs
    export LINC=$1/linc
    export PIPELINE=$2
    export INPUT_JSON=$3
#     export TMPDIR=${WORKDIR}
    mkdir -pv ${OUTDIR} ${WORKDIR}  ${LOGDIR}
#     toil --version
#     toil ${CWL_OPTIONS} --tmp-outdir-prefix ${WORKDIR}/cwl- --tmpdir-prefix ${WORKDIR}/tmp- --outdir ${OUTDIR} --log-dir ${LOGDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${SOFTWARE}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
#     toil ${CWL_OPTIONS} --tmp-outdir-prefix ${WORKDIR}/cwl- --outdir ${OUTDIR} --log-dir ${LOGDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${SOFTWARE}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
    toil ${CWL_OPTIONS} --tmp-outdir-prefix ${WORKDIR}/cwl- --outdir ${OUTDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${LINC}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
}

linc_restart () {
    export WORKDIR=$1/runtime
    export OUTDIR=$1/output
    export LOGDIR=$1/logs
    export LINC=$1/linc
    export PIPELINE=$2
    export INPUT_JSON=$3
    mkdir -pv ${OUTDIR} ${WORKDIR}  ${LOGDIR}
    toil ${CWL_OPTIONS} --restart --tmp-outdir-prefix ${WORKDIR}/cwl- --outdir ${OUTDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${LINC}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
}

linc_restart_trunk () {
    export WORKDIR=$1/runtime
    export OUTDIR=$1/output
    export LOGDIR=$1/logs
    export LINC=$1/linc
    export PIPELINE=$2
    export INPUT_JSON=$3
    mkdir -pv ${OUTDIR} ${WORKDIR}  ${LOGDIR}
    toil_trunk ${CWL_OPTIONS} --restart --tmp-outdir-prefix ${WORKDIR}/cwl- --outdir ${OUTDIR} --workDir ${WORKDIR} --writeLogs ${LOGDIR} --jobStore ${WORKDIR}/jobStore ${LINC}/workflows/${PIPELINE}.cwl ${INPUT_JSON}
}

linc_cwltool () {
    export WORKDIR=$1/pipeline
    export OUTDIR=$1/output
    export LOGDIR=$1/logs
    export LINC=$1/linc
    export PIPELINE=$2
    export INPUT_JSON=$3
    mkdir -pv ${OUTDIR} ${WORKDIR}
    cwltool ${CWLTOOL_OPTIONS} --tmpdir-prefix ${WORKDIR}/ --outdir ${OUTDIR} --log-dir ${LOGDIR} --leave-tmpdir ${LINC}/workflows/${PIPELINE}.cwl  ${INPUT_JSON}
}
