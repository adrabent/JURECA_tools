# JURECA_tools
## Tools for automatic processing data on JUWELS ##

The `JURECA tools` provide a repository of tools to allow efficient processing of LOFAR data stored in the Jülich Long-Term-Archive.

Currently available tools:
------------------------------------------
* installation script for compiling third-party software
* automatic submission of jobs to the `JUWELS` queue

Installation and Usage
----------------------
Installation is done via the following scripts

    git clone https://github.com/adrabent/JURECA_tools.git
    ./lofar_install_juwels.sh
    ./surveys_install_juwels.sh

The script `SKSP_monitoring.py` is the master script and looks for new tokens, manages the jobs and updates tokens.
It is called on `JUDAC` via (in a 1 minute interval)

    ./run_monitoring_JUDAC.sh

The script `check_pipeline.sh` checks whether a new job (created by `SKSP_monitoring.py`) is available and will launch it.
It is called on `JUWELS` via (in a 1 minute interval)

    ./run_monitoring_JUWELS.sh

The outputs are logged locally on `JUWELS`.

### Dependencies

* GRID certificate
