wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
sh Miniconda3-latest-Linux-x86_64.sh
rm -v Miniconda3-latest-Linux-x86_64.sh
conda config --set auto_activate_base false
conda update -n base -c defaults conda
conda create -n surveys sshtunnel pymysql numpy requests mysqlclient
conda create -n h5py h5py
conda clean --all
