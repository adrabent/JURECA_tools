#!/p/project/chtb00/htb006/software_new/envs/surveys/bin/python3
# -*- coding: utf-8 -*-

"""
Surveys KSP monitoring script -- see https://github.com/adrabent/JURECA_tools
"""

import argparse, logging
import os, sys, shutil
import subprocess, time

from surveys_utils import *

_version   = '1.0' ## program version
cal_dir    = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/diskonly/pipelines/SKSP/prefactor_v3.0/pref_cal'
targ_dir   = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/archive/SKSP_Spider_Pref3'
cal_prefix = 'pref3_cal_'  

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

def get_calibrator(cal_obsid, field_name, cal_results_dir, working_directory):

	logging.info('Checking calibrator observation: \033[35mL' + cal_obsid)
	cal_solution = cal_results_dir + '/L' + cal_obsid + '/' + cal_prefix + 'L' + cal_obsid + '.tar'
	existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode = existence.wait()
	if errorcode == 0:
		logging.info('Found calibrator results for this field from: \033[35m' + cal_solution)
	else:
		cal_solution = cal_results_dir + '/Spider/' + cal_prefix + 'L' + cal_obsid + '.tar'
		existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
		errorcode = existence.wait()
		if errorcode == 0:
			logging.info('Found calibrator results for this field from: \033[35m' + cal_solution)
		else:
			logging.warning('Could not find any calibrator results for this field in: \033[35m' + cal_solution)
			return (True, False)

	filename = working_directory + '/' + os.path.basename(cal_solution)
	transfer  = subprocess.Popen(['globus-url-copy', cal_solution, 'file://' + filename], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	if errorcode != 0:
		logging.error('\033[31mDownloading calibrator results has failed.')
		return (False, True)

	os.chdir(working_directory)
	logging.info('Unpacking calibrator results from: \033[35m' + filename)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode != 0:
		logging.error('\033[31m Unpacking calibrator results has failed.')
		return (False, True)

	os.remove(filename)
	logging.info('File \033[35m' + filename + '\033[32m was removed.')

	return (False, False)

def get_calibrator(cal_obsid, cal_results_dir, working_directory):

	logging.info('Checking calibrator observation: \033[35mL' + cal_obsid)
	cal_solution = cal_results_dir + '/L' + cal_obsid + '/' + cal_prefix + 'L' + cal_obsid + '.tar'
	existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode = existence.wait()
	if errorcode == 0:
		logging.info('Found calibrator results for this field from: \033[35m' + cal_solution)
	else:
		cal_solution = cal_results_dir + '/Spider/' + cal_prefix + 'L' + cal_obsid + '.tar'
		existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
		errorcode = existence.wait()
		if errorcode == 0:
			logging.info('Found calibrator results for this field from: \033[35m' + cal_solution)
		else:
			logging.warning('Could not find any calibrator results for this field in: \033[35m' + cal_solution)
			return False

	filename = working_directory + '/' + os.path.basename(cal_solution)
	transfer  = subprocess.Popen(['globus-url-copy', cal_solution, 'file://' + filename], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	if errorcode != 0:
		logging.error('\033[31mDownloading calibrator results has failed.')
		return False

	os.chdir(working_directory)
	logging.info('Unpacking calibrator results from: \033[35m' + filename)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode != 0:
		logging.error('\033[31m Unpacking calibrator results has failed.')
		return False

	os.remove(filename)
	logging.info('File \033[35m' + filename + '\033[32m was removed.')

	return True

def get_target(targ_obsid, cal_results_dir, working_directory):

	logging.info('Checking target observation: \033[35mL' + targ_obsid)
	cal_solution = cal_results_dir + '/L' + targ_obsid + '/inspection.tar'
	existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode = existence.wait()
	if errorcode == 0:
		logging.info('Found target results for this field from: \033[35m' + cal_solution)
	else:
		logging.warning('Could not find any target results for this field in: \033[35m' + cal_solution)
		return False

	filename = working_directory + '/' + os.path.basename(cal_solution)
	transfer  = subprocess.Popen(['globus-url-copy', cal_solution, 'file://' + filename], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	if errorcode != 0:
		logging.error('\033[31mDownloading target results has failed.')
		return False

	os.chdir(working_directory)
	logging.info('Unpacking target results from: \033[35m' + filename)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode != 0:
		logging.error('\033[31m Unpacking target results has failed.')
		return False

	os.remove(filename)
	logging.info('File \033[35m' + filename + '\033[32m was removed.')

	return True

def main(working_directory = os.environ['SCRATCH_chtb00'] + '/htb006', field_id = None):

	## load working environment
	if not working_directory or not os.path.exists(working_directory):
		logging.warning('No working directory was specified or could be find')

	#os.remove(working_directory + '/.submitted')
	if not field_id:
		logging.error('No field ID specified.')
		return False
	try:
		field_name = field_id.split('_')[0]
		targ_obsid = field_id.split('_')[1].lstrip('L')
	except:
		logging.error('Provided field ID is invalid.')
		return False

	field     = get_one_observation(field_name, targ_obsid)
	try:
		cal_obsid = str(field['calibrator_id'])
	except TypeError:
		cal_obsid = targ_obsid
	error = get_calibrator(cal_obsid, cal_dir, working_directory)
	error = get_target(targ_obsid, targ_dir, working_directory)
	
	return error


if __name__=='__main__':
	# Get command-line options.
	parser = argparse.ArgumentParser(description='Reset failed field in LoTSS MySQL database')

	parser.add_argument('field_id', type=str, default = None, help='Specify field ID to retrieve diagnostics from.')
	parser.add_argument('--workdir', type=str, default = os.environ['SCRATCH_chtb00'] + '/htb006', help='Specify working directory to be reset.')

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
	main(args.workdir, args.field_id)

	sys.exit(0)
