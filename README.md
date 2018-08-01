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
    ./tools_install.sh
    ./update_software.sh

The script `SKSP_monitoring.py` is the master script and looks for new tokens, manages the jobs and updates tokens.
It is called on `JUDAC` via (in a 1 minute interval)

    ./run_monitoring_JUDAC.sh
    
The script `check_pipeline.sh` checks whether a new job (created by `SKSP_monitoring.py`) is available and will launch it.
It is called on `JUWELS` via (in a 1 minute interval)

    ./run_monitoring_JUWELS.sh
    
The outputs are logged locally on `JUWELS`.

### Dependencies (downloaded via `tools_install`)

* GRID certificate
* [libuuid](https://downloads.sourceforge.net/project/libuuid/libuuid-1.0.3.tar.gz?r=https%3A%2F%2Fsourceforge.net%2Fprojects%2Flibuuid%2F\&ts=1508405748\&use_mirror=kent)
* [YAML](http://pyyaml.org/download/libyaml/yaml-0.1.7.tar.gz) (version 0.1.7)
* [pyYAML](http://pyyaml.org/download/pyyaml/PyYAML-3.12.tar.gz) (version 3.12)
* [fuse](https://github.com/fuse4x/fuse.git)
* [cvmfs](https://github.com/cvmfs/cvmfs.git)
* [cctools](https://github.com/cooperative-computing-lab/cctools.git)
* [GRID_LRT](https://github.com/apmechev/GRID_LRT)
