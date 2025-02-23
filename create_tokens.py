#!/p/project1/chtb00/htb006/software_new/envs/surveys/bin/python3
# -*- coding: utf-8 -*-

"""
Surveys KSP monitoring script -- see https://github.com/adrabent/JURECA_tools
"""
import argparse, logging
import datetime, time
import os, sys, shutil
import subprocess, multiprocessing

from surveys_utils import *

_version = '1.0' ## program version

def add_coloring_to_emit_ansi(fn):

	def new(*args):
		levelno = args[0].levelno
		if(levelno>=50):
			color = '\x1b[31m' # red
		elif(levelno>=40):
			color = '\x1b[31m' # red
		elif(levelno>=30):
			color = '\x1b[33m' # yellow
		elif(levelno>=20):
			color = '\x1b[32m' # green
		elif(levelno>=10):
			color = '\x1b[35m' # pink
		else:
			color = '\x1b[0m' # normal
		args[0].msg = color + args[0].msg +  '\x1b[0m'  # normal
		return fn(*args)
	return new

def my_handler(type, value, tb):
	exception = logging.critical("{0}".format(str(value)))
	lock = os.environ['SCRATCH_chtb00'] + '/htb006' '/.lock'
	if os.path.exists(lock):
		os.remove(lock)
	time.sleep(300)

def main(working_directory = None, server = 'localhost:3306', database = 'Juelich', status = None):

	## load working environment
	if not working_directory or not os.path.exists(working_directory):
		logging.warning('No working directory was specified or could be found')

	#os.remove(working_directory + '/.submitted')
	field_id   = working_directory.rstrip('/').split('/')[-1]
	field_name = '_'.join(field_id.split('_')[:-1])
	obsid      = field_id.split('_')[-1].lstrip('L')

	if status:
		update_status(field_name, obsid, status, 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: ' + status)
	field = get_one_observation(field_name, obsid)

if __name__=='__main__':
	# Get command-line options.
	parser = argparse.ArgumentParser(description='Reset failed field in LoTSS MySQL database')

	parser.add_argument('workdir', type=str,    default = None,             help='Specify working directory to be reset.')
	parser.add_argument('--server', type=str,   default = 'localhost:3306', help='LoTSS MySQL server URL:port')
	parser.add_argument('--database', type=str, default = None,             help='Define which database to use.')
	parser.add_argument('--status', type=str,   default = None,             help='Define the status of the field.')

	args = parser.parse_args()

	format_stream = logging.Formatter("%(asctime)s\033[1m %(levelname)s:\033[0m %(message)s","%Y-%m-%d %H:%M:%S")
	format_file   = logging.Formatter("%(asctime)s %(levelname)s: %(message)s","%Y-%m-%d %H:%M:%S")
	logging.root.setLevel(logging.INFO)
	
	log      = logging.StreamHandler()
	log.setFormatter(format_stream)
	log.emit = add_coloring_to_emit_ansi(log.emit)
	logging.root.addHandler(log)

	# install exception handler
	sys.excepthook = my_handler

	# start running script
	main(args.workdir, args.server, args.database, args.status)

	sys.exit(0)
