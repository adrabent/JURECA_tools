#!/bin/bash

source $HOME/.bashrc
sbatch --nodes=2 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=00:05:00 --account=htb00 /p/project/chtb00/htb006/run_prefactor.sh





#sbatch --nodes=25 --partition=batch --mail-user=alex@tls-tautenburg.de --mail-type=ALL --time=01:00:00 /homea/htb00/htb006/run_pipeline.sh /work/htb00/htb006/pipeline.parset /work/htb00/htb006
