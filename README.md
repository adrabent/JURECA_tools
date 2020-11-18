# JURECA_tools
## Tools for automatic processing data on JUWELS ##

The `JURECA tools` provide a repository of tools to allow efficient processing of LOFAR data stored in the Jülich Long-Term-Archive.
It makes use of the `GRID_LRT` tools by Alexandar Mechev.

Currently available tools:
------------------------------------------
* copying and updating a pre-compiled LOFAR installation from a `cvmfs`-directory
* installation script for compiling third-party software
* communication via a `couchDB` token server
* automatic submission of jobs to the `JUWELS` queue

Installation and Usage
----------------------
Installation is done via the following scripts

    git clone https://github.com/adrabent/JURECA_tools.git
    git clone https://github.com/apmechev/GRID_LRT.git
    git clone https://github.com/apmechev/GRID_PiCaS_Launcher.git
    mkdir -pv tools && mv GRID_LRT GRID_PiCaS_Launcher tools/.
    ./lofar_install_juwels.sh

The script `SKSP_monitoring.py` is the master script and looks for new tokens, manages the jobs and updates tokens.
It is called on `JUDAC` via (in a 1 minute interval)

    ./run_monitoring_JUDAC.sh
    
The script `check_pipeline.sh` checks whether a new job (created by `SKSP_monitoring.py`) is available and will launch it.
It is called on `JUWELS` via (in a 1 minute interval)

    ./run_monitoring_JUWELS.sh
    
The outputs are logged locally on `JUWELS`.

### Dependencies

* GRID certificate
* [cloudant](https://pypi.org/project/cloudant/)
