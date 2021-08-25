#!/p/project/chtb00/htb006/software_new/envs/surveys/bin/python3
# -*- coding: utf-8 -*-

"""
Surveys KSP monitoring script -- see https://github.com/adrabent/JURECA_tools
"""
import optparse, logging
import datetime, time
import os, sys, shutil
import subprocess, multiprocessing

from surveys_utils import *

_version                    = '1.0'                                                ## program version
pref_version                = 'v3.0'                                               ## prefactor version for gathering solutions from the GRID
cal_dir                     = 'diskonly/pipelines/SKSP/prefactor_' + pref_version  ## directory for the calibrator
targ_dir                    = 'archive'                                            ## directory for the target
cal_subdir                  = cal_dir  + '/pref_cal'                               ## subdirectory to look for calibrator files
targ_subdir                 = targ_dir + '/SKSP_Spider_Pref3'                      ## subdirectory to look for target files
srm_subdir                  = cal_dir + '/srmfiles'                                ## subdirectory to look for SRM files
cal_prefix                  = 'pref3_cal_'                                         ## prefix for calibrator solutions
prefactor                   = 'https://github.com/lofar-astron/prefactor.git'      ## location of prefactor
branch                      = 'master'                                             ## branch to be used
nodes                       = 24                                                   ## number of JUWELS nodes (higher number leads to a longer queueing time)
walltime                    = '02:00:00'                                           ## walltime for the JUWELS queue
mail                        = 'alex@tls-tautenburg.de'                             ## notification email address
IONEX_server                = 'ftp://ftp.aiub.unibe.ch/CODE/'                      ## URL for CODE downloads
num_SBs_per_group_var       = 10                                                   ## chunk size 
max_dppp_threads_var        = 24                                                   ## maximal threads per node per DPPP instance (max 96 on JUWELS)
max_proc_per_node_limit_var = 2                                                    ## maximal processes per node for DPPP
num_proc_per_node_var       = 10                                                   ## maximal processes per node for others
#error_tolerance             = 0                                                    ## number of failed tokens still acceptable for running pipelines


os.system('clear')
print('\033[30;1m################################################')
print('## Surveys KSP monitoring script              ##')
print('################################################\033[0m')


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
	exception = logger.critical("{0}".format(str(value)))
	lock = os.environ['SCRATCH_chtb00'] + '/htb006' '/.lock'
	if os.path.exists(lock):
		os.remove(lock)
	time.sleep(300)

def is_running(lock_file):
	if os.path.isfile(lock_file):
		return True
	else:
		return False

def is_staged(url):

	try:
		if 'ONLINE_AND_NEARLINE' in subprocess.check_output(['srmls', '-l', url]):
			return True
		elif 'ONLINE' in subprocess.check_output(['srmls', '-l', url]):
			logging.warning('The following file has no status NEARLINE: \033[35m' + url)
			return True
		else:
			return False
	except:
		return False

def gsi_replace(item):

	item.replace('srm://srm.grid.sara.nl:8443', 'gsiftp://gridftp.grid.sara.nl:2811')
	item.replace('srm://lofar-srm.fz-juelich.de:8443', 'gsiftp://lofar-gridftp.fz-juelich.de:2811')
	item.replace('srm://lta-head.lofar.psnc.pl:8443', 'gsiftp://gridftp.lofar.psnc.pl:2811')

	return item

def check_submitted_job(slurm_log, submitted):

	log_information = os.popen('tail -9 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'ERROR' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return 'failed'
	if 'error:' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return 'failed'
	if 'termination' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return 'failed'
	log_information = os.popen('tail -8 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'error:' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return 'failed'
	log_information = os.popen('tail -6 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'Error' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return 'failed'
	log_information = os.popen('tail -7 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'finished' in log_information:
		logging.info(log_information)
		os.remove(submitted)
		return 'processed'

	return 'processing'

def submit_results(calibrator, field_name, obsid, working_directory):

	parset            = working_directory + '/pipeline.parset'
	results_directory = working_directory + '/pipeline/results'
	results           = sorted(glob.glob(results_directory + '/*.ms'))
	shutil.copyfile(working_directory + '/pipeline.log', results_directory + '/pipeline.log')
	
	logging.info('Results of the pipeline will be submitted.')
	update_status(field_name, target_obsid, 'transferring', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: transferring')

	# upload calibration results
	if calibrator:
		transfer_dir = ftp + '/' + cal_subdir + '/L' + cal_obsid
		subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
		filename     = working_directory + '/' + cal_prefix + 'L' + cal_obsid + '.tar'
		transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
		pack_and_transfer(filename, results_directory, results_directory + '/..', transfer_fn, field_name, obsid)
	elif len(results) > 0:
		transfer_dir = ftp + '/' + targ_subdir + '/L' + cal_obsid
		subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
		results.append(results_directory + '/inspection')
		results.append(results_directory + '/cal_values')
		for result in results:
			to_pack     = result
			filename    = results_directory + '/' + topack + '.tar'
			transfer_fn = transfer_dir + '/' + filename.split('/')[-1]
			output = pool2.apply_async(pack_and_transfer, args = (filename, to_pack, to_pack + '/..', transfer_fn, field_name, obsid))

	### check status
	field = get_one_observation(field_name, target_obsid)
	if field['status'] == 'failed':
		logging.warning('Submitting results was incomplete.')
		return False

	logging.info('Submitting results has been finished.')
	update_status(field_name, target_obsid, 'DI_processed', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: DI_processed')

	return True

def create_submission_script(submit_job, parset, working_directory, submitted):
	
	home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'
	
	if os.path.isfile(submit_job):
		logging.warning('\033[33mFile for submission already exists. It will be overwritten.')
		os.remove(submit_job)
	
	jobfile = open(submit_job, 'w')
	
	## writing file header
	jobfile.write('#!/usr/bin/env sh\n')
	
	## extracting directories for IONEX and the TGSS ADR skymodel
	try:
		IONEX_script         = os.popen('find ' + working_directory  +                  ' -name createRMh5parm.py ').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"=" | cut -f1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
		cal_solutions        = os.popen('grep cal_solutions '        + parset + ' | cut -f2- -d"=" | cut -f1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
		jobfile.write(IONEX_script + ' --ionexpath ' + working_directory + '/pipeline/. --server ' + IONEX_server + ' --solsetName target ' + working_directory + '/' + target_input_pattern + ' ' + cal_solutions + '\n')
	except IndexError:
		pass
	try:
		skymodel_script      = os.popen('find ' + working_directory  +         ' -name download_skymodel_target.py').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"=" | cut -f1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
		target_skymodel      = working_directory + '/pipeline/target.skymodel'
		if os.path.exists(target_skymodel):
			os.remove(target_skymodel)
		jobfile.write(skymodel_script + ' ' + working_directory + '/' + target_input_pattern + ' ' + target_skymodel + '\n')
	except IndexError:
		pass
	
	## write-up of final command
	jobfile.write('\n')
	jobfile.write('sbatch --nodes=' + str(nodes) + ' --partition=batch --mail-user=' + mail + ' --mail-type=ALL --time=' + walltime + ' --account=htb00 ' + home_directory + '/run_pipeline.sh ' + parset + ' ' + working_directory)
	jobfile.close()
	
	os.system('chmod +x ' + submit_job)
	os.rename(submit_job, submit_job + '.sh')
	subprocess.Popen(['touch', submitted])

def run_prefactor(calibrator, field_name, target_obsid, working_directory, submitted, slurm_files):

	submit_job = working_directory + '/../submit_job'
	parset     = working_directory + '/pipeline.parset'

	## downloading prefactor
	filename = working_directory + '/prefactor.tar'
	logging.info('Downloading current prefactor version from \033[35m' + prefactor + '\033[32m to \033[35m' + working_directory + '/prefactor')
	if os.path.exists(working_directory + '/prefactor'):
		logging.warning('Overwriting old prefactor directory...')
		shutil.rmtree(working_directory + '/prefactor', ignore_errors = True)

	download = subprocess.Popen(['git', 'clone', '-b', branch, prefactor, working_directory + '/prefactor'], stdout=subprocess.PIPE)
	errorcode = download.wait()
	if errorcode != 0:
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
		logging.error('\033[31m Downloading prefactor has failed.')
		return True

	## applying necessary changes to the parset
	if calibrator:
		shutil.copyfile(working_directory + '/prefactor/Pre-Facet-Calibrator.parset', parset)
		input_path          = os.popen('grep "! cal_input_path" '          + parset).readlines()[0].rstrip('\n').replace('/','\/')
		input_pattern       = os.popen('grep "! cal_input_pattern" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
	else:
		shutil.copyfile(working_directory + '/prefactor/Pre-Facet-Target.parset'    , parset)
		input_path          = os.popen('grep "! target_input_path" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
		input_pattern       = os.popen('grep "! target_input_pattern" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
		cal_solutions       = os.popen('grep "! cal_solutions" '           + parset).readlines()[0].rstrip('\n').replace('/','\/')
	prefactor_directory     = os.popen('grep "! prefactor_directory" '     + parset).readlines()[0].rstrip('\n').replace('/','\/')
	losoto_directory        = os.popen('grep "! losoto_directory" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')
	aoflagger_executable    = os.popen('grep "! aoflagger" '               + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node       = os.popen('grep "! num_proc_per_node" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node_limit = os.popen('grep "! num_proc_per_node_limit" ' + parset).readlines()[0].rstrip('\n').replace('/','\/')
	max_dppp_threads        = os.popen('grep "! max_dppp_threads" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')

	if calibrator:
		os.system('sed -i "s/' + input_path              + '/! cal_input_path          = ' + working_directory.replace('/','\/')                                             + '/g" ' + parset)
		os.system('sed -i "s/' + input_pattern           + '/! cal_input_pattern       = *.MS'                                                                               + '/g" ' + parset)
	else:
		os.system('sed -i "s/' + input_path              + '/! target_input_path       = ' + working_directory.replace('/','\/')                                             + '/g" ' + parset)
		os.system('sed -i "s/' + input_pattern           + '/! target_input_pattern    = *.MS'                                                                               + '/g" ' + parset)
		os.system('sed -i "s/' + cal_solutions           + '/! cal_solutions           = ' + working_directory.replace('/','\/') + '\/results\/cal_values\/cal_solutions.h5' + '/g" ' + parset)
	os.system(    'sed -i "s/' + prefactor_directory     + '/! prefactor_directory     = ' + working_directory.replace('/','\/') + '\/prefactor'                             + '/g" ' + parset)
	os.system(    'sed -i "s/' + losoto_directory        + '/! losoto_directory        = \$LOSOTO'                                                                           + '/g" ' + parset)
	os.system(    'sed -i "s/' + aoflagger_executable    + '/! aoflagger               = \$AOFLAGGER'                                                                        + '/g" ' + parset)
	os.system(    'sed -i "s/' + num_proc_per_node       + '/! num_proc_per_node       = ' + str(num_proc_per_node_var)                                                      + '/g" ' + parset)
	os.system(    'sed -i "s/' + num_proc_per_node_limit + '/! num_proc_per_node_limit = ' + str(max_proc_per_node_limit_var)                                                + '/g" ' + parset)
	os.system(    'sed -i "s/' + max_dppp_threads        + '/! max_dppp_threads        = ' + str(max_dppp_threads_var)                                                       + '/g" ' + parset)

	logging.info('Creating submission script in \033[35m' + submit_job)
	create_submission_script(submit_job, parset, working_directory, submitted)
	
	if os.path.exists(working_directory + '/pipeline/statefile'):
		os.remove(working_directory + '/pipeline/statefile')
		logging.info('Statefile has been removed.')

	slurm_list = glob.glob(slurm_files)
	for slurm_file in slurm_list:
		os.remove(slurm_file)

	logging.info('\033[0mWaiting for submission\033[0;5m...')
	while os.path.exists(submit_job + '.sh'):
		time.sleep(5)
	
	update_status(field_name, target_obsid, 'submitted', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: submitted.')
	logging.info('Pipeline \033[35m' + pipeline + '\033[32m has been submitted.')
	
	return False

def unpack_data(filename, target_obsid, field_name, working_directory):
  
	os.chdir(working_directory)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode == 0:
		os.remove(filename)
		logging.info('File \033[35m' + filename + '\033[32m was unpacked and removed.')
	else:
		logging.error('\033[31mUnpacking failed for: \033[35m' + str(filename))
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')

def pack_and_transfer(filename, to_pack, pack_directory, transfer_fn, field_name, obsid):

	os.chdir(pack_directory)
	pack_directory = os.getcwd() + '/'
	logging.info('Packing file: \033[35m' + filename)

	pack = subprocess.Popen(['tar', 'cfvz', filename, to_pack.replace(pack_directory, '')], stdout = subprocess.PIPE)
	errorcode = pack.wait()
	if errorcode == 0:
		logging.info('Packing of \033[35m' + filename + '\033[32m finished.')
		transfer_data(filename, transfer_fn, field_name, obsid)
	else:
		logging.error('\033[31mPacking of \033[35m' + filename + '\033[31m failed.')
		update_status(field_name, obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')

def transfer_data(filename, transfer_fn, field_name, obsid):

	logging.info('\033[35m' + filename + '\033[32m is now transfered to: \033[35m' + transfer_fn)
	existence = subprocess.Popen(['uberftp', '-ls', transfer_fn])
	errorcode = existence.wait()
	if errorcode == 0:
		subprocess.Popen(['uberftp','-rm', transfer_fn])
	transfer  = subprocess.Popen(['globus-url-copy', 'file:' + filename, transfer_fn], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	if errorcode == 0:
		logging.info('File \033[35m' + filename + '\033[32m was transferred.')
	else:
		logging.error('\033[31mTransfer of \033[35m' + filename + '\033[31m failed.')
		update_status(field_name, obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')

def download_data(url, target_obsid, field_name, working_directory):

	filename   = working_directory + '/' + url.split('/')[-1]
	if '.psnc.pl' in url:
		logging.warning('Using wget for download!')
		url = 'https://lta-download.lofar.psnc.pl/lofigrid/SRMFifoGet.py?surl=srm://lta-head.lofar.psnc.pl:8443/' + '/'.join(url.split('/')[3:])
		download   = subprocess.Popen(['wget',  url, '-O' + filename], stdout=subprocess.PIPE)
	else:
		download   = subprocess.Popen(['globus-url-copy',  url, 'file:' + filename], stdout=subprocess.PIPE)
	errorcode  = download.wait()

	if errorcode == 0:
		unpack_data(filename, target_obsid, field_name, working_directory)
	else:
		logging.error('Download failed for: \033[35m' + str(url))
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')

def prepare_downloads(obsid, field_name, target_obsid, srm_dir, working_directory):

	logging.info('Retrieving SRM file for: \033[35mL' + obsid)
	srm_file = srm_dir + '/srm' + obsid + '.txt'
	existence = subprocess.Popen(['uberftp', '-ls', srm_file])
	errorcode = existence.wait()
	if errorcode == 0:
		logging.info('Transferring SRM file from: \033[35m' + srm_file)
	else:
		logging.error('Could not find SRM file for: \033[35m' + srm_file)
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
		return []

	filename = working_directory + '/' + os.path.basename(srm_file)
	transfer  = subprocess.Popen(['globus-url-copy', srm_file, 'file://' + filename], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	if errorcode != 0:
		logging.error('\033[31mDownloading SRM file has failed.')
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
		return []

	srm_list = []
	srm      = open(filename, 'r').readlines()
	for entry in srm:
		filename  = working_directory + '/' + '_'.join(os.path.basename(entry.rstrip()).split('_')[:-1])
		if os.path.exists(filename):
			logging.warning('\033[33mFile \033[35m' + entry.rstrip() + '\033[33m is already on disk.')
			continue
		srm_list.append(entry.rstrip())

	## check whether data is staged
	logging.info('Checking staging status of files remaining for download.')
	pool = multiprocessing.Pool(processes = int(multiprocessing.cpu_count() / 4))
	is_staged_list = pool.map(is_staged, list(srm_list))

	download_list = []
	staged        = True
	for entry in srm_list:
		if not is_staged_list[srm_list.index(entry)]:
			logging.warning('\033[33mFile \033[35m' + entry.rstrip() + '\033[33m is not staged.')
			staged = False
			continue
		logging.info('File \033[35m' + entry.rstrip() + '\033[32m is properly staged.')
		download_list.append(gsi_replace(entry.rstrip()))

	if not staged:
		update_status(field_name, target_obsid, 'not_staged', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[33mnot_staged.')

	return download_list

def get_calibrator(cal_obsid, field_name, target_obsid, cal_results_dir, working_directory):

	logging.info('Checking calibrator observation: \033[35mL' + cal_obsid)
	cal_solution = cal_results_dir + '/L' + cal_obsid + '/' + cal_prefix + 'L' + cal_obsid + '.tar'
	existence = subprocess.Popen(['uberftp', '-ls', cal_solution])
	errorcode = existence.wait()
	if errorcode == 0:
		logging.info('Transferring calibrator results for this field from: \033[35m' + cal_solution)
	else:
		cal_solution = cal_results_dir + '/Spider/' + cal_prefix + 'L' + cal_obsid + '.tar'
		existence = subprocess.Popen(['uberftp', '-ls', cal_solution])
		errorcode = existence.wait()
		if errorcode == 0:
			logging.info('Transferring calibrator results for this field from: \033[35m' + cal_solution)
		else:
			logging.warning('Could not find any calibrator results for this field in: \033[35m' + cal_solution)
			return (True, False)

	filename = working_directory + '/' + os.path.basename(cal_solution)
	transfer  = subprocess.Popen(['globus-url-copy', cal_solution, 'file://' + filename], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	if errorcode != 0:
		logging.error('\033[31mDownloading calibrator results has failed.')
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
		return (False, True)

	os.chdir(working_directory)
	logging.info('Unpacking calibrator results from: \033[35m' + filename)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode != 0:
		logging.error('\033[31m Unpacking calibrator results has failed.')
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
		return (False, True)

	os.remove(filename)
	logging.info('File \033[35m' + filename + '\033[32m was removed.')

	return (False, False)

def main(server = 'localhost:3306', database = 'Juelich', ftp = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp'):

	## load working environment
	working_directory = os.environ['SCRATCH_chtb00'] + '/htb006'
	home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'
	server_config     = os.environ['HOME']           + '/.surveys'
	#done              = working_directory + '/.done'
	last_observation  = working_directory + '/.observation'
	slurm_files       = home_directory    + '/slurm-*.out'
	logging.info('\033[0mWorking directory is ' + working_directory)

	## load PiCaS credentials and connect to server
	logging.info('\033[0mConnecting to server: ' + server)
	username = open(server_config).readlines()[1].rstrip()
	logging.info('Username: \033[35m' + username)
	logging.info('Database: \033[35m' + database)
	observation      = 1

	### check latest observation
	if is_running(last_observation):
		observation  = open(last_observation).readline().rstrip()
		field_name   = observation.split('_')[0]
		target_obsid = observation.split('_')[-1].lstrip('L')

	### check for new observations if necessary
	if not is_running(last_observation):
		logging.info('Looking for a new observation.')
		try:
			nextfield    = get_next_pref(status = 'Observed', location = database)
		except TypeError:
			try:
				nextfield    = get_next_pref(status = 'not_staged', location = database)
			except TypeError:
				logging.info('\033[0mNo tokens in database found to be processed.')
				return
		target_obsid = str(nextfield['target_OBSID'])
		field_name   = nextfield['field_name']
		observation  = field_name + '_L' + target_obsid

	if observation == 1:
		logging.info('\033[0mNo observations could be found. If database is not empty please check it for new or false tokens manually.')
		time.sleep(300)
		return

	### reserve following processes
	logging.info('Selected target field observation: \033[35m' + observation)
	observation_file = open(last_observation, 'w')
	observation_file.write(observation)
	observation_file.close()
	
	### create subdirectory
	working_directory += '/' + observation ## for subdirectories
	if not os.path.exists(working_directory):
		logging.info('Creating working directory: \033[35m' + working_directory)
		os.makedirs(working_directory)

	### load observation from database and check for calibrator observation
	field               = get_one_observation(field_name, target_obsid)
	cal_obsid           = str(field['calibrator_id'])
	(calibrator, error) = get_calibrator(cal_obsid, field_name, target_obsid, ftp + '/' + cal_subdir, working_directory)
	if calibrator:
		working_directory += '_L' + cal_obsid
		obsid = cal_obsid
		logging.info('Selected calibrator observation: \033[35mL' + obsid)
		if not os.path.exists(working_directory):
			logging.info('Creating working directory: \033[35m' + working_directory)
			os.makedirs(working_directory)
	else:
		obsid = target_obsid
		logging.info('Selected target observation: \033[35mL' + obsid)

	### check whether a job has been already submitted
	submitted = working_directory + '/.submitted'
	if is_running(submitted): 
		logging.info('\033[0mA pipeline has already been submitted.')
		slurm_list = glob.glob(slurm_files)
		if len(slurm_list) > 0:
			slurm_log = slurm_list[-1]
			job_id = os.path.basename(slurm_log).lstrip('slurm-').rstrip('.out')
			logging.info('Checking current status of the submitted job: \033[35m' + job_id)
			log_information = check_submitted_job(slurm_log, submitted).replace('\033[32m','').replace('\033[0m','')
			if log_information == 'failed':
				logging.error('Processing of pipeline has been failed. See error log for details.')
				update_status(field_name, target_obsid, 'failed', 'observations')
				logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
				error = True
			elif log_information == 'processing':
				logging.info('Pipeline is currently processing.')
				update_status(field_name, target_obsid, 'processing', 'observations')
				logging.info('Status of \033[35m' + field_name + '\033[32m has been set to processing.')
				return
			else:
				logging.info('Pipeline has finished successfully.')
				update_status(field_name, target_obsid, 'processed', 'observations')
				logging.info('Status of \033[35m' + field_name + '\033[32m has been set to processed.')
				error = submit_results(calibrator, field_name, obsid, ftp, working_directory)
				if error:
					pass
				else:
					logging.info('Cleaning working directory.')
					shutil.rmtree(working_directory, ignore_errors = True)
					os.remove(last_observation)
					return

	### download data
	download_list = prepare_downloads(obsid, field_name, target_obsid, ftp + '/' + srm_subdir, working_directory)
	pool          = multiprocessing.Pool(processes = int(multiprocessing.cpu_count() / 2))
	for url in download_list:
		if not error:
			update_status(field_name, target_obsid, 'downloading', 'observations')
			logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: downloading.')
		result = pool.apply_async(download_data, args = (url, obsid, field_name, working_directory,))
	pool.close()
	pool.join()

	### lock program
	field = get_one_observation(field_name, target_obsid)
	if field['status'] == 'failed' or field['status'] == 'not_staged':
		logging.error('Could not proceed with selected field. Please check database or logfiles for any errors.')
		#os.remove(last_observation)
		return

	### run prefactor
	update_status(field_name, target_obsid, 'unpacked', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: unpacked.')
	logging.info('Prefactor pipeline will be started.')
	run_prefactor(calibrator, field_name, target_obsid, working_directory, submitted, slurm_files)


if __name__=='__main__':
	# Get command-line options.
	opt = optparse.OptionParser(usage='%prog ', version='%prog '+_version, description=__doc__)
	opt.add_option('-s', '--server', help='LoTSS MySQL server URL:port', action='store_true', default='localhost:3306')
	opt.add_option('-d', '--database', help='Define which database to use', action='store_true', default='Juelich')
	opt.add_option('-f', '--ftp', help='FTP server hosting current prefactor version', action='store_true', default='gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp')
	(options, args) = opt.parse_args()

	format_stream = logging.Formatter("%(asctime)s\033[1m %(levelname)s:\033[0m %(message)s","%Y-%m-%d %H:%M:%S")
	format_file   = logging.Formatter("%(asctime)s %(levelname)s: %(message)s","%Y-%m-%d %H:%M:%S")
	logging.root.setLevel(logging.INFO)
	
	log      = logging.StreamHandler()
	log.setFormatter(format_stream)
	log.emit = add_coloring_to_emit_ansi(log.emit)
	logging.root.addHandler(log)

	home_directory = os.environ['PROJECT_chtb00'] + '/htb006'
	LOG_FILENAME = home_directory + '/logs/' + str(datetime.datetime.utcnow().replace(microsecond=0).isoformat()) + '.log'
	if not os.path.exists(home_directory + '/logs'):
		os.makedirs(home_directory + '/logs')
	logfile = logging.FileHandler(LOG_FILENAME)
	logfile.setFormatter(format_file)
	logfile.emit = add_coloring_to_emit_ansi(logfile.emit)
	logging.root.addHandler(logfile)

	# install exception handler
	logger = logging.getLogger(LOG_FILENAME)
	sys.excepthook = my_handler

	# location of logfile
	logging.info('\033[0mLog file is written to ' + LOG_FILENAME)

	# start running script
	main(options.server, options.database, options.ftp)

	# monitoring has been finished
	logging.info('\033[30;4mMonitoring has been finished.\033[0m')

	sys.exit(0)
