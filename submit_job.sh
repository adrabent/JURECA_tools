#!/usr/bin/env sh

. /homea/htb00/htb006/env_lofar_2.20.2_stage2017b.sh

$HOME/scripts/download_IONEX.py --destination $WORK $WORK/*.MS
$HOME/scripts/download_tgss_skymodel_target.py $WORK/*.MS $WORK/pipeline.skymodel

sbatch --nodes=1 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=01:00:00 $HOME/run_pipeline.sh $PARSET $WORK