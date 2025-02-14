#!/p/project1/chtb00/htb006/software_new/envs/surveys/bin/python3
# -*- coding: utf-8 -*-

"""
Surveys KSP monitoring script -- see https://github.com/adrabent/JURECA_tools
"""
import optparse, logging
import datetime, time
import os, sys, shutil, glob, json
import subprocess, multiprocessing
import random

from surveys_utils import *
from get_juelich_setup import *

_version                    = '1.0'                                                ## program version
pref_version                = 'v3.0'                                               ## prefactor version for gathering solutions from the GRID
cal_dir                     = 'diskonly/pipelines/SKSP/prefactor_' + pref_version  ## directory for the calibrator
targ_dir                    = 'archive'                                            ## directory for the target
cal_subdir                  = cal_dir  + '/pref_cal'                               ## subdirectory to look for calibrator files
targ_subdir                 = targ_dir + '/SKSP_Spider_Pref3'                      ## subdirectory to look for target files
targ_subdir_linc            = targ_dir + '/SKSP_Spider_LINC'                       ## subdirectory to look for target files
srm_subdir                  = cal_dir + '/srmfiles'                                ## subdirectory to look for SRM files
cal_prefix                  = 'pref3_cal_'                                         ## prefix for calibrator solutions
cal_prefix_linc             = 'linc_cal_'                                          ## prefix for calibrator solutions
prefactor                   = 'https://github.com/lofar-astron/prefactor.git'      ## location of prefactor
LINC                        = 'https://git.astron.nl/RD/LINC.git'                  ## location of LINC
branch                      = 'hotfix/demix_crash'                                 ## branch to be used
nodes                       = 24                                                   ## number of JUWELS nodes (higher number leads to a longer queueing time)
walltime                    = '24:00:00'                                           ## walltime for the JUWELS queue
mail                        = 'alex@tls-tautenburg.de'                             ## notification email address
IONEX_server                = 'http://chapman.upc.es/'                             ## URL for CODE downloads
num_SBs_per_group_var       = 10                                                   ## chunk size 
max_dppp_threads_var        = 24                                                   ## maximal threads per node per DPPP instance (max 96 on JUWELS)
max_proc_per_node_limit_var = 2                                                    ## maximal processes per node for DPPP
num_proc_per_node_var       = 10                                                   ## maximal processes per node for others
error_tolerance             = 0                                                    ## number of unstaged files still acceptable for running pipelines
linc                        = True                                                 ## run LINC pipeline instead of prefactor genericpipeline version
clear_image                 = False                                                ## update the used Singularity image
keep_existing_files         = False                                                ## in case of upload problems keep existing uploaded files
make_structure_plot         = False                                                ## regular processing with making the structure function plot
max_number_of_fields        = 15                                                   ## maximum amount of fields to be processed in parallel
run_in_parallel             = True                                                 ## specify whether to run in parallel or not
special_fields              = ['343226', '823948', '573955', '576819', '806044', '689118', '2023483']   ## treat those fields specially

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
	time.sleep(300)

def write_file(lock, contents, force = False):
	while is_running(lock) and not force:
		logging.info('\033[0mWaiting for another process to launch\033[0;5m...')
		time.sleep(60)
	lock_file = open(lock, 'w')
	lock_file.write(str(contents))
	lock_file.close()
	
def is_running(lock_file):
	if os.path.isfile(lock_file):
		return True
	else:
		return False

def is_staged(url):

	home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'

	try:
		if 'ONLINE_AND_NEARLINE' in str(subprocess.check_output(['singularity', 'exec', home_directory + '/lta-client.sif', 'srmls', '-l', url], )):
			return True
		elif 'ONLINE' in str(subprocess.check_output(['srmls', '-l', url])):
			logging.warning('The following file has no status NEARLINE: \033[35m' + url)
			return True
		else:
			return False
	except:
		return False

def gsi_replace(item):

	item = item.replace('srm://srm.grid.sara.nl:8443', 'gsiftp://gridftp.grid.sara.nl:2811')
	item = item.replace('srm://lofar-srm.fz-juelich.de:8443', 'gsiftp://lofar-gridftp.fz-juelich.de:2811')
	item = item.replace('srm://lta-head.lofar.psnc.pl:8443', 'gsiftp://gridftp.lofar.psnc.pl:2811')

	return item

def check_submitted_job(slurm_log, submitted):

	log_information = os.popen('tail -9 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'ERROR' in log_information:
		logging.error(log_information)
		return 'failed'
	if 'error:' in log_information:
		logging.error(log_information)
		return 'failed'
	if 'termination' in log_information:
		logging.error(log_information)
		return 'failed'
	log_information = os.popen('tail -8 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'error:' in log_information:
		logging.error(log_information)
		return 'failed'
	log_information = os.popen('tail -6 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'Error' in log_information:
		logging.error(log_information)
		return 'failed'
	log_information = os.popen('tail -18 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'ERROR' in log_information:
		logging.error(log_information)
		return 'failed'
	log_information = os.popen('tail -2 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'finished' in log_information:
		logging.info(log_information)
		return 'processed'
	log_information = os.popen('tail -7 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'finished' in log_information:
		logging.info(log_information)
		return 'processed'
	log_information = os.popen('tail -12 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'finished' in log_information:
		logging.info(log_information)
		return 'processed'
	log_information = os.popen('tail -16 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'finished' in log_information:
		logging.info(log_information)
		return 'processed'
	log_information = os.popen('tail -18 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'finished' in log_information:
		logging.info(log_information)
		return 'processed'
	return 'processing'

def check_submitted_job_linc(slurm_log, submitted):

	with open(slurm_log) as logfile:
		log = logfile.read()
		if 'CANCELLED DUE TO TIME LIMIT' in log:
			logging.warning('Pipeline has been cancelled due to time limit.')
			return 'timeout'
		if 'Got exit code -15' in log:
			logging.warning('Pipeline has been cancelled due to time limit.')
			return 'timeout'
		if 'Job failed with exit value -15' in log:
			logging.warning('Pipeline has been cancelled due to time limit.')
			return 'timeout'
		if 'Final process status is success' in log:
			logging.info('Final process status is success')
			return 'processed'
		if 'Successfully deleted the job store:' in log:
			logging.info('Final process status is success')
			return 'processed'
		if 'is completely failed' in log:
			logging.error('Pipeline has returned an error. See logfiles for details.')
			return 'failed'
		if 'toil.jobStores.abstractJobStore.NoSuchFileException' in log:
			logging.error('Pipeline has returned an error. See logfiles for details.')
			return 'failed'
		if 'Workflow cannot make any more progress' in log:
			logging.error('Pipeline has returned an error. See logfiles for details.')
			return 'failed'
	return 'processing'


def submit_results_linc(calibrator, field_name, obsid, target_obsid, ftp, working_directory):

	results_directory = working_directory + '/output'
	results = sorted(glob.glob(results_directory + '/results/*.ms'))
	obsid_directory   = working_directory + '/L' + obsid
	summary           = glob.glob(results_directory + '/*.json')[0]

	logging.info('Results of the pipeline will be submitted.')
	update_status(field_name, target_obsid, 'transferring', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mtransferring')

	# upload calibration results
	if calibrator:
		os.rename(results_directory, obsid_directory)
		transfer_dir = ftp + '/' + cal_subdir + '/Spider'
		filename     = working_directory + '/' + cal_prefix_linc + 'L' + obsid + '.tar'
		transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
		pack_and_transfer(filename, obsid_directory, os.path.dirname(obsid_directory), transfer_fn, field_name, target_obsid)
	elif len(results) > 0:
		transfer_dir = ftp + '/' + targ_subdir_linc + '/L' + obsid
		subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE, env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
		results.append(results_directory + '/inspection')
		results.append(results_directory + '/cal_solutions.h5')
		results.append(results_directory + '/logs')
		results.append(summary)
		pool = multiprocessing.Pool(processes = int(multiprocessing.cpu_count() / 2))
		#pool = multiprocessing.Pool(processes = 1)
		for result in results:
			to_pack     = result
			filename    = to_pack + '.tar'
			transfer_fn = transfer_dir + '/' + filename.split('/')[-1]
			output = pool.apply_async(pack_and_transfer, args = (filename, to_pack, os.path.dirname(to_pack), transfer_fn, field_name, target_obsid))
		pool.close()
		pool.join()

	### check status
	field = get_one_observation(field_name, target_obsid)
	if field['status'] == 'failed':
		logging.warning('Submitting results was incomplete.')
		return True

	logging.info('Submitting results has been finished.')
	if calibrator:
		update_status(field_name, target_obsid, 'READY', 'observations') ## NEEDS TO BE CHANGED SOMEWHEN
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mREADY')
	else:
		if len(results) < 23:
			update_status(field_name, target_obsid, 'PREF_NUM_MS_FAIL', 'observations')
			logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mPREF_NUM_MS_FAIL')
		else:
			update_status(field_name, target_obsid, 'DI_Processed', 'observations')
			logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mDI_Processed')

	return False

def submit_results(calibrator, field_name, obsid, target_obsid, ftp, working_directory):

	parset            = working_directory + '/pipeline.parset'
	results_directory = working_directory + '/pipeline/results'
	logs_directory    = working_directory + '/pipeline/logs'
	skymodel          = working_directory + '/pipeline/target.skymodel'
	results           = sorted(glob.glob(results_directory + '/*.ms'))
	
	shutil.copyfile(working_directory + '/pipeline.log' , results_directory + '/inspection/pipeline.log')
	shutil.copytree(logs_directory                      , results_directory + '/logs', dirs_exist_ok = True)
	shutil.copyfile(parset                              , results_directory + '/logs/pipeline.parset')
	if os.path.isfile(skymodel):
		shutil.copyfile(skymodel                        , results_directory + '/inspection/pipeline.skymodel')
	
	logging.info('Results of the pipeline will be submitted.')
	update_status(field_name, target_obsid, 'transferring', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mtransferring')

	# upload calibration results
	if calibrator:
		transfer_dir = ftp + '/' + cal_subdir + '/Spider'
		filename     = working_directory + '/' + cal_prefix + 'L' + obsid + '.tar'
		transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
		pack_and_transfer(filename, results_directory, results_directory + '/..', transfer_fn, field_name, target_obsid)
	elif len(results) > 0:
		transfer_dir = ftp + '/' + targ_subdir + '/L' + obsid
		subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE, env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
		results.append(results_directory + '/inspection')
		results.append(results_directory + '/cal_values')
		results.append(results_directory + '/logs')
		pool = multiprocessing.Pool(processes = int(multiprocessing.cpu_count() / 2))
		for result in results:
			to_pack     = result
			filename    = to_pack + '.tar'
			transfer_fn = transfer_dir + '/' + filename.split('/')[-1]
			output = pool.apply_async(pack_and_transfer, args = (filename, to_pack, to_pack + '/..', transfer_fn, field_name, target_obsid))
		pool.close()
		pool.join()

	### check status
	field = get_one_observation(field_name, target_obsid)
	if field['status'] == 'failed':
		logging.warning('Submitting results was incomplete.')
		return True

	logging.info('Submitting results has been finished.')
	if calibrator:
		update_status(field_name, target_obsid, 'READY', 'observations') ## NEEDS TO BE CHANGED SOMEWHEN
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mREADY')
	else:
		update_status(field_name, target_obsid, 'DI_Processed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mDI_Processed')

	return False

def create_submission_script_linc(calibrator, submit_job, parset, cal_solutions, target_skymodel, working_directory, ms, submitted, restart):
	
	home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'
	
	logging.info('\033[0mWaiting for submission file to be created\033[0;5m...')
	time.sleep(random.randint(0,60))
	
	while os.path.isfile(submit_job) or os.path.isfile(submit_job + '.sh'):
		logging.warning('\033[33mFile for submission already exists.')
		time.sleep(60)
	
	jobfile = open(submit_job, 'w')
	
	## writing file header and environment
	jobfile.write('#!/usr/bin/env bash\n')
	jobfile.write('export PROJECT=${PROJECT_chtb00}/htb006\n')
	jobfile.write('export SOFTWARE=${PROJECT}/software_new\n')
	jobfile.write('export SINGULARITY_CACHEDIR=${SOFTWARE}\n')
	jobfile.write('export SINGULARITY_PULLDIR=${SINGULARITY_CACHEDIR}/pull\n')
	jobfile.write('export CWL_SINGULARITY_CACHE=${SINGULARITY_PULLDIR}\n')
	jobfile.write("export SINGULARITY_TMPDIR='/tmp'\n")
	
	## clear singularity cache
	if not restart and clear_image:
		jobfile.write('\n')
		jobfile.write('singularity cache clean -f\n')
		jobfile.write('singularity pull --force --name astronrd_linc.sif docker://astronrd/linc\n')
		jobfile.write('\n')
	
	## extracting directories for IONEX and the TGSS ADR skymodel
	if not calibrator:
		if not restart:
			jobfile.write('singularity exec docker://astronrd/linc createRMh5parm.py --ionexpath ' + working_directory + '/pipeline/ --server ' + IONEX_server + ' --solsetName target ' + os.path.abspath(ms) + ' ' + cal_solutions + '\n')
			if os.path.exists(target_skymodel):
				os.remove(target_skymodel)
			target_name = '_'.join(os.path.basename(working_directory).split('_')[:-1])
			jobfile.write('singularity exec docker://astronrd/linc download_skymodel_target.py --targetname ' + target_name + ' ' + os.path.abspath(ms) + ' ' + target_skymodel + '\n')
		pipeline = 'HBA_target'
	else:
		pipeline = 'HBA_calibrator'
	
	## overwrite script-wide defaults for LINC test purposes
	nodes = 1
	
	## write-up of final command
	jobfile.write('\n')
	if calibrator:
		walltime = '12:00:00'
		jobfile.write('sbatch --nodes=' + str(nodes) + ' --partition=batch --mail-user=' + mail + ' --mail-type=ALL --time=' + walltime + ' --account=htb00 ' + home_directory + '/run_linc_cwltool.sh ' + working_directory + ' ' + pipeline + ' ' + parset)
	elif restart:
		walltime = '24:00:00'
		jobfile.write('sbatch --nodes=' + str(nodes) + ' --partition=batch --mail-user=' + mail + ' --mail-type=ALL --time=' + walltime + ' --account=htb00 ' + home_directory + '/run_linc_restart.sh ' + working_directory + ' ' + pipeline + ' ' + parset)
	else:
		walltime = '24:00:00'
		jobfile.write('sbatch --nodes=' + str(nodes) + ' --partition=batch --mail-user=' + mail + ' --mail-type=ALL --time=' + walltime + ' --account=htb00 ' + home_directory + '/run_linc.sh '         + working_directory + ' ' + pipeline + ' ' + parset)
	jobfile.close()
	
	os.system('chmod +x ' + submit_job)
	os.rename(submit_job, submit_job + '.sh')
	subprocess.Popen(['touch', submitted])
	
	return(0)

def run_linc(calibrator, field_name, obsid, working_directory, submitted, slurm_files, restart):

	submit_job      = working_directory + '/../submit_job'
	parset          = working_directory + '/pipeline.json'
	target_skymodel = working_directory + '/pipeline/target.skymodel'
	cal_solutions   = None
	ms              = None

	slurm_list = glob.glob(slurm_files)
	for slurm_file in slurm_list:
		os.remove(slurm_file)
		if not calibrator:
			restart = True

	## downloading LINC
	if not restart:
		filename = working_directory + '/LINC.tar'
		logging.info('Downloading current LINC version from \033[35m' + LINC + '\033[32m to \033[35m' + working_directory + '/linc')
		if os.path.exists(working_directory + '/linc'):
			logging.warning('Overwriting old LINC directory...')
			shutil.rmtree(working_directory + '/linc', ignore_errors = True)

		download = subprocess.Popen(['git', 'clone', LINC, working_directory + '/linc'], stdout=subprocess.PIPE)
		errorcode = download.wait()
		if errorcode != 0:
			update_status(field_name, obsid, 'failed', 'observations')
			logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
			logging.error('\033[31m Downloading LINC has failed.')
			return True
		os.chdir(working_directory + '/linc')
		subprocess.Popen(['git', 'checkout', branch], stdout=subprocess.PIPE)
		commit = os.popen('git show | grep commit | cut -f2- -d" "').readlines()[0].strip()

		## create input JSON file
		config = {}
		msin = []
		for ms in sorted(glob.glob(working_directory + '/*.MS')):
			msin.append({"class": "Directory", "path": os.path.abspath(ms)})
		config['msin'] = msin
		config['max_dp3_threads'] = 10
		config['make_structure_plot'] = make_structure_plot
		config['min_unflagged_fraction'] = 0.05
		selfcal, demix = find_demix_selfcal(obsid)
		if not selfcal:
			selfcal = any(item in obsid for item in special_fields)
		if selfcal and not calibrator:
			config['selfcal'] = True
			config['gsmcal_step'] = 'scalarphase'
		if calibrator or not demix:
			config['demix'] = False
		if not calibrator:
			if os.path.exists(working_directory + '/results/cal_solutions.h5'):
				cal_solutions = working_directory + '/results/cal_solutions.h5'
			elif os.path.exists(working_directory + '/results/cal_values/cal_solutions.h5'):
				cal_solutions = working_directory + '/results/cal_values/cal_solutions.h5'
			else:
				logging.error('\033[31m Calibration solution set not found')
				update_status(field_name, obsid, 'failed', 'observations')
				logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
				return True
			config['cal_solutions']   = {"class": "File", "path": cal_solutions}
			config['target_skymodel'] = {"class": "File", "path": target_skymodel}

		with open(parset, 'w') as fp:
			json.dump(config, fp)

	logging.info('Creating submission script in \033[35m' + submit_job)
	create_submission_script_linc(calibrator, submit_job, parset, cal_solutions, target_skymodel, working_directory, ms, submitted, restart)

	logging.info('\033[0mWaiting for submission\033[0;5m...')
	while os.path.exists(submit_job + '.sh'):
		time.sleep(5)
	
	update_status(field_name, obsid, 'submitted', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35msubmitted.')
	logging.info('Pipeline has been submitted.')

	return False

def create_submission_script(calibrator, submit_job, parset, working_directory, obsid, submitted):
	
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
		jobfile.write('python2.7 ' + IONEX_script + ' --ionexpath ' + working_directory + '/pipeline/. --server ' + IONEX_server + ' --solsetName target ' + working_directory + '/' + target_input_pattern + ' ' + cal_solutions + '\n')
	except IndexError:
		pass
	try:
		skymodel_script      = os.popen('find ' + working_directory  +         ' -name download_skymodel_target.py').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"=" | cut -f1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
		target_skymodel      = working_directory + '/pipeline/target.skymodel'
		if os.path.exists(target_skymodel):
			os.remove(target_skymodel)
		jobfile.write('python2.7 ' + skymodel_script + ' ' + working_directory + '/' + target_input_pattern + ' ' + target_skymodel + '\n')
	except IndexError:
		pass
	
	walltime = '04:00:00'
	## exceptional processing
	if not calibrator:
		if '2003568' in obsid or '2003573' in obsid or '2005608' in obsid or '2005609' in obsid:
			walltime = '06:00:00'

	## write-up of final command
	jobfile.write('\n')
	jobfile.write('sbatch --nodes=' + str(nodes) + ' --partition=batch --mail-user=' + mail + ' --mail-type=ALL --time=' + walltime + ' --account=htb00 ' + home_directory + '/run_pipeline.sh ' + parset + ' ' + working_directory)
	jobfile.close()
	
	os.system('chmod +x ' + submit_job)
	os.rename(submit_job, submit_job + '.sh')
	subprocess.Popen(['touch', submitted])
	
	return(0)

def run_prefactor(calibrator, field_name, obsid, working_directory, submitted, slurm_files):

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
		update_status(field_name, obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
		logging.error('\033[31m Downloading prefactor has failed.')
		return True
	os.chdir(working_directory + '/prefactor')
	commit = os.popen('git show | grep commit | cut -f2- -d" "').readlines()[0].strip()

	## applying necessary changes to the parset
	if calibrator:
		shutil.copyfile(working_directory + '/prefactor/Pre-Facet-Calibrator.parset', parset)
		input_path                = os.popen('grep "! cal_input_path" '             + parset).readlines()[0].rstrip('\n').replace('/','\/')
		input_pattern             = os.popen('grep "! cal_input_pattern" '          + parset).readlines()[0].rstrip('\n').replace('/','\/')
	else:
		shutil.copyfile(working_directory + '/prefactor/Pre-Facet-Target.parset'    , parset)
		input_path                = os.popen('grep "! target_input_path" '          + parset).readlines()[0].rstrip('\n').replace('/','\/')
		input_pattern             = os.popen('grep "! target_input_pattern" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
		cal_solutions             = os.popen('grep "! cal_solutions" '              + parset).readlines()[0].rstrip('\n').replace('/','\/')
		min_unflagged_fraction    = os.popen('grep "! min_unflagged_fraction" '     + parset).readlines()[0].rstrip('\n').replace('/','\/')
		avg_timeresolution        = os.popen('grep "! avg_timeresolution" '         + parset).readlines()[0].rstrip('\n').replace('/','\/')
		avg_freqresolution        = os.popen('grep "! avg_freqresolution" '         + parset).readlines()[0].rstrip('\n').replace('/','\/')
		avg_timeresolution_concat = os.popen('grep "! avg_timeresolution_concat" '  + parset).readlines()[0].rstrip('\n').replace('/','\/')
		avg_freqresolution_concat = os.popen('grep "! avg_freqresolution_concat" '  + parset).readlines()[0].rstrip('\n').replace('/','\/')
	prefactor_directory           = os.popen('grep "! prefactor_directory" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')
	losoto_directory              = os.popen('grep "! losoto_directory" '           + parset).readlines()[0].rstrip('\n').replace('/','\/')
	aoflagger_executable          = os.popen('grep "! aoflagger" '                  + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node             = os.popen('grep "! num_proc_per_node" '          + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node_limit       = os.popen('grep "! num_proc_per_node_limit" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
	max_dppp_threads              = os.popen('grep "! max_dppp_threads" '           + parset).readlines()[0].rstrip('\n').replace('/','\/')
	

	if calibrator:
		os.system('sed -i "s/' + input_path                      + '/! cal_input_path            = ' + working_directory.replace('/','\/')                                             + '/g" ' + parset)
		os.system('sed -i "s/' + input_pattern.replace('*','\*') + '/! cal_input_pattern         = \*.MS'                                                                              + '/g" ' + parset)
	else:
		os.system('sed -i "s/' + input_path                      + '/! target_input_path         = ' + working_directory.replace('/','\/')                                             + '/g" ' + parset)
		os.system('sed -i "s/' + input_pattern.replace('*','\*') + '/! target_input_pattern      = \*.MS'                                                                              + '/g" ' + parset)
		os.system('sed -i "s/' + cal_solutions                   + '/! cal_solutions             = ' + working_directory.replace('/','\/') + '\/results\/cal_values\/cal_solutions.h5' + '/g" ' + parset)
		os.system('sed -i "s/' + min_unflagged_fraction          + '/! min_unflagged_fraction    = 0.05'                                                                               + '/g" ' + parset)
		if '2003568' in obsid or '2003573' in obsid or '2005608' in obsid or '2005609' in obsid:
			os.system('sed -i "s/' + avg_timeresolution              + '/! avg_timeresolution        = 1.'                                                                                 + '/g" ' + parset)
			os.system('sed -i "s/' + avg_timeresolution_concat       + '/! avg_timeresolution_concat = 1.'                                                                                 + '/g" ' + parset)
		else:
			os.system('sed -i "s/' + avg_timeresolution              + '/! avg_timeresolution        = 4.'                                                                                 + '/g" ' + parset)
			os.system('sed -i "s/' + avg_freqresolution              + '/! avg_freqresolution        = 48.82kHz'                                                                           + '/g" ' + parset)
			os.system('sed -i "s/' + avg_timeresolution_concat       + '/! avg_timeresolution_concat = 8.'                                                                                 + '/g" ' + parset)
			os.system('sed -i "s/' + avg_freqresolution_concat       + '/! avg_freqresolution_concat = 97.64kHz'                                                                           + '/g" ' + parset)

	os.system(    'sed -i "s/' + prefactor_directory             + '/! prefactor_directory       = ' + working_directory.replace('/','\/') + '\/prefactor'                             + '/g" ' + parset)
	os.system(    'sed -i "s/' + losoto_directory                + '/! losoto_directory          = \$LOSOTO'                                                                           + '/g" ' + parset)
	os.system(    'sed -i "s/' + aoflagger_executable            + '/! aoflagger                 = \$AOFLAGGER'                                                                        + '/g" ' + parset)
	os.system(    'sed -i "s/' + num_proc_per_node               + '/! num_proc_per_node         = ' + str(num_proc_per_node_var)                                                      + '/g" ' + parset)
	os.system(    'sed -i "s/' + num_proc_per_node_limit         + '/! num_proc_per_node_limit   = ' + str(max_proc_per_node_limit_var)                                                + '/g" ' + parset)
	os.system(    'sed -i "s/' + max_dppp_threads                + '/! max_dppp_threads          = ' + str(max_dppp_threads_var)                                                       + '/g" ' + parset)
	os.system(    'sed -i "1 i\#####GIT COMMIT '                                                     + commit                                                                          + '"   ' + parset)

	logging.info('Creating submission script in \033[35m' + submit_job)
	create_submission_script(calibrator, submit_job, parset, working_directory, obsid, submitted)
	
	if os.path.exists(working_directory + '/pipeline/statefile'):
		os.remove(working_directory + '/pipeline/statefile')
		logging.info('Statefile has been removed.')

	slurm_list = glob.glob(slurm_files)
	for slurm_file in slurm_list:
		os.remove(slurm_file)

	logging.info('\033[0mWaiting for submission\033[0;5m...')
	while os.path.exists(submit_job + '.sh'):
		time.sleep(5)
	
	update_status(field_name, obsid, 'submitted', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35msubmitted.')
	logging.info('Pipeline has been submitted.')

	return False

def unpack_data(filename, obsid, field_name, working_directory):
  
	os.chdir(working_directory)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode == 0:
		os.remove(filename)
		logging.info('File \033[35m' + filename + '\033[32m was unpacked and removed.')
	else:
		logging.error('\033[31mUnpacking failed for: \033[35m' + str(filename))
		update_status(field_name, obsid, 'failed', 'observations')
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
	existence = subprocess.Popen(['uberftp', '-ls', transfer_fn], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode = existence.wait()
	if errorcode == 0:
		if keep_existing_files:
			logging.warning('Will keep existing file.')
			return
		subprocess.Popen(['uberftp','-rm', transfer_fn], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	transfer  = subprocess.Popen(['globus-url-copy', 'file:' + filename, transfer_fn], stdout=subprocess.PIPE, env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode = transfer.wait()
	if errorcode == 0:
		logging.info('File \033[35m' + filename + '\033[32m was transferred.')
	else:
		subprocess.Popen(['uberftp','-rm', transfer_fn], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
		logging.error('\033[31mTransfer of \033[35m' + filename + '\033[31m failed.')
		update_status(field_name, obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')

def download_data(url, obsid, field_name, working_directory):

	filename   = working_directory + '/' + url.split('/')[-1]
	if '.psnc.pl' in url:
		logging.warning('Using wget for download!')
		url = 'https://lta-download.lofar.psnc.pl/lofigrid/SRMFifoGet.py?surl=srm://lta-head.lofar.psnc.pl:8443/' + '/'.join(url.split('/')[3:])
		logging.info('Downloading file: \033[35m' + filename)
		download   = subprocess.Popen(['wget', '--tries=3', '--no-check-certificate', url, '-O' + filename], stdout=subprocess.PIPE)
	else:
		logging.info('Downloading file: \033[35m' + filename)
		download   = subprocess.Popen(['globus-url-copy',  url, 'file:' + filename], stdout=subprocess.PIPE, env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode  = download.wait()
	if errorcode == 0:
		unpack_data(filename, obsid, field_name, working_directory)
	else:
		logging.error('Download failed for: \033[35m' + str(url))
		update_status(field_name, obsid, 'not_staged', 'observations') ## DEBUG
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[33mnot_staged.') ## DEBUG

def prepare_downloads(obsid, field_name, target_obsid, srm_dir, working_directory):

	logging.info('Retrieving SRM file for: \033[35mL' + obsid)
	srm_file = srm_dir + '/srm' + obsid + '.txt'
	existence = subprocess.Popen(['uberftp', '-ls', srm_file], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode = existence.wait()
	filename = working_directory + '/' + os.path.basename(srm_file)
	if errorcode == 0:
		logging.info('Transferring SRM file from: \033[35m' + srm_file)
	else:
		logging.error('Could not find SRM file for: \033[35m' + srm_file)
		if is_running(filename):
			logging.warning('Using existing SRM file: \033[35m' + filename)
		else:
			update_status(field_name, target_obsid, 'failed', 'observations')
			logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
			return ([], False)

	if errorcode == 0:
		transfer  = subprocess.Popen(['globus-url-copy', srm_file, 'file://' + filename], stdout=subprocess.PIPE, env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
		errorcode = transfer.wait()
	if errorcode != 0 and not is_running(filename):
		logging.error('\033[31mDownloading SRM file has failed.')
		update_status(field_name, target_obsid, 'failed', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
		return ([], False)

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
	unstaged      = 0
	for entry in srm_list:
		if not is_staged_list[srm_list.index(entry)]:
			unstaged += 1
			logging.warning('\033[33mFile \033[35m' + entry.rstrip() + '\033[33m is not staged.')
			if unstaged > error_tolerance:
				staged = False
			continue
		logging.info('File \033[35m' + entry.rstrip() + '\033[32m is properly staged.')
		download_list.append(gsi_replace(entry.rstrip()))

	if not staged:
		update_status(field_name, target_obsid, 'not_staged', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[33mnot_staged.')

	return download_list, staged

def get_calibrator(cal_obsid, field_name, target_obsid, cal_results_dir, working_directory, submitted):

	logging.info('Checking calibrator observation: \033[35mL' + cal_obsid)
	cal_solution = cal_results_dir + '/Spider/' + cal_prefix + 'L' + cal_obsid + '.tar'
	existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
	errorcode = existence.wait()
	if errorcode == 0:
		logging.info('Found calibrator results for this field from: \033[35m' + cal_solution)
	else:
		cal_solution = cal_results_dir + '/Spider/' + cal_prefix_linc + 'L' + cal_obsid + '.tar'
		existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
		errorcode = existence.wait()
		if errorcode == 0:
			logging.info('Found calibrator results for this field from: \033[35m' + cal_solution)
		else:
			cal_solution = cal_results_dir + '/L' + cal_obsid + '/' + cal_prefix + 'L' + cal_obsid + '.tar'
			existence = subprocess.Popen(['uberftp', '-ls', cal_solution], env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
			errorcode = existence.wait()
			if errorcode == 0:
				logging.info('Found calibrator results for this field from: \033[35m' + cal_solution)
			else:
				logging.warning('Could not find any calibrator results for this field in: \033[35m' + cal_solution)
				return (True, False)

	if os.path.isfile(submitted):
		return (False, False)

	filename = working_directory + '/' + os.path.basename(cal_solution)
	transfer  = subprocess.Popen(['globus-url-copy', cal_solution, 'file://' + filename], stdout=subprocess.PIPE, env = {'GLOBUS_GSSAPI_MAX_TLS_PROTOCOL' : 'TLS1_2_VERSION'})
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

	if os.path.exists(working_directory + '/L' + cal_obsid):
		if os.path.exists(working_directory + '/results'):
			shutil.rmtree(working_directory + '/results')
		shutil.move(working_directory + '/L' + cal_obsid, working_directory + '/results')
	os.remove(filename)
	logging.info('File \033[35m' + filename + '\033[32m was removed.')

	return (False, False)

def main(server = 'localhost:3306', database = 'Juelich', ftp = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp', process_id = 0, parallel = run_in_parallel):

	## load working environment
	working_directory = os.environ['SCRATCH_chtb00'] + '/htb006'
	home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'
	server_config     = os.environ['HOME']           + '/.surveys'
	last_observation  = working_directory + '/.observation_' + str(process_id)
	lock              = working_directory + '/.lock'
	logging.info('\033[0mWorking directory is ' + working_directory)
	logging.info('\033[0mThis is process ID ' + str(process_id))

	## removing existing lock file
	if is_running(lock):
		os.remove(lock)

	## load PiCaS credentials and connect to server
	logging.info('\033[0mConnecting to server: ' + server)
	username = open(server_config).readlines()[1].rstrip()
	logging.info('Username: \033[35m' + username)
	logging.info('Database: \033[35m' + database)
	observation = 1

	### check latest observation
	if is_running(last_observation):
		observation  = open(last_observation).readline().rstrip()
		field_name   = '_'.join(observation.split('_')[0:-1])
		target_obsid = observation.split('_')[-1].lstrip('L')

	### check for new observations if necessary
	while not is_running(last_observation) and observation == 1:
		logging.info('Looking for a new observation.')
		last_observations = glob.glob(working_directory + '/.observation_*')
		if len(last_observations) >= max_number_of_fields:
			logging.warning('\033[0mMore than ' + str(max_number_of_fields) + ' fields are currently being processed. Waiting for fields to finish \033[0;5m...')
			time.sleep(3600)
			continue
		observations = []
		for last_obs in last_observations:
			f = open(last_obs, 'r').readlines()[0]
			observations.append(f.split('_')[0])
		try:
			nextfield = get_next_pref(status = 'READY', location = database)
		except TypeError:
			try:
				nextfield = get_next_pref(status = 'not_staged', location = database)
			except TypeError:
				logging.info('\033[0mNo tokens in database found to be processed.')
				logging.info('Checking for observations in the working directory to be resumed.')
				working_directories = [ f.path for f in os.scandir(working_directory) if f.is_dir() ]
				for directory in working_directories:
					field_id   = directory.rstrip('/').split('/')[-1]
					try:
						field_name = field_id.split('_')[0]
						if field_name in observations:
							continue
						obsid      = field_id.split('_')[1].lstrip('L')
						update_status(field_name, obsid, 'READY', 'observations')
						logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: READY')
					except (IndexError, TypeError):
						continue
				if working_directories == []:
					logging.info('\033[0mNo observations could be found. If database is not empty please check it for new or false tokens manually.')
				time.sleep(3600)
				continue
		target_obsid = str(nextfield['target_OBSID'])
		field_name   = nextfield['field_name']
		observation  = field_name + '_L' + target_obsid

	### reserve following processes
	logging.info('Selected target field observation: \033[35m' + observation)
	write_file(last_observation, observation, force = True)

	### create subdirectory
	working_directory  = os.environ['SCRATCH_chtb00'] + '/htb006/' + observation ## for subdirectories
	submitted          = working_directory + '/.submitted'
	if not os.path.exists(working_directory):
		logging.info('Creating working directory: \033[35m' + working_directory)
		os.makedirs(working_directory)

	### load observation from database and check for calibrator observation
	field               = get_one_observation(field_name, target_obsid)
	cal_obsid           = str(field['calibrator_id'])
	(calibrator, error) = get_calibrator(cal_obsid, field_name, target_obsid, ftp + '/' + cal_subdir, working_directory, submitted)
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
	submitted   = working_directory + '/.submitted'
	slurm_files = working_directory + '/slurm-*.out'
	while is_running(submitted): 
		logging.info('\033[0mA pipeline has already been submitted.')
		slurm_list = glob.glob(slurm_files)
		while len(slurm_list) > 0 and error == False:
			slurm_log = slurm_list[-1]
			job_id = os.path.basename(slurm_log).lstrip('slurm-').rstrip('.out')
			logging.info('Checking current status of the submitted job: \033[35m' + job_id)
			if linc:
				log_information = check_submitted_job_linc(slurm_log, submitted).replace('\033[32m','').replace('\033[0m','')
			else:
				log_information = check_submitted_job(slurm_log, submitted).replace('\033[32m','').replace('\033[0m','')
			if log_information == 'failed' or (log_information == 'timeout' and calibrator):
				logging.error('Processing of pipeline has been failed. See errorlog for details.')
				update_status(field_name, target_obsid, 'failed', 'observations')
				logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[31mfailed.')
				error = True
				os.remove(submitted)
			elif log_information == 'timeout':
				logging.warning('Pipeline processing has encountered walltime limit.')
				logging.info('LINC pipeline will be restarted.')
				run_linc(calibrator, field_name, target_obsid, working_directory, submitted, slurm_files, restart = True)
				break
			elif log_information == 'processing':
				logging.info('Pipeline is currently processing.')
				update_status(field_name, target_obsid, 'processing', 'observations')
				logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mprocessing.')
				if parallel: 
					write_file(lock, str(random.randint(0, 1e5)))
					parallel = False
				time.sleep(60)
			else:
				logging.info('Pipeline has finished successfully.')
				if linc:
					error = submit_results_linc(calibrator, field_name, obsid, target_obsid, ftp, working_directory)
				else:
					error = submit_results(calibrator, field_name, obsid, target_obsid, ftp, working_directory)
				if not error:
					logging.info('Cleaning working directory.')
					shutil.rmtree(working_directory, ignore_errors = True)
				os.remove(last_observation)
				#if int(process_id) == 0:
					#write_file(lock, str(process_id))
				return
		time.sleep(60)

	### download data
	(download_list, staged) = prepare_downloads(obsid, field_name, target_obsid, ftp + '/' + srm_subdir, working_directory)
	pool                   = multiprocessing.Pool(processes = int(multiprocessing.cpu_count() / 4))
	if not (error or not staged) and len(download_list) != 0 and (field['status'] == 'READY' or field['status'] == 'not_staged'):
		update_status(field_name, target_obsid, 'downloading', 'observations')
		logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35mdownloading.')
	for url in download_list:
		result = pool.apply_async(download_data, args = (url, target_obsid, field_name, working_directory,))
	pool.close()
	pool.join()

	### lock program
	field = get_one_observation(field_name, target_obsid)
	if field['status'] == 'failed' or field['status'] == 'not_staged' or error:
		logging.error('Could not proceed with \033[35m' + field_name +'\033[31m. Please check database or logfiles for any errors.')
		if not error:
			os.remove(last_observation)
		#if int(process_id) == 0 or field['status'] == 'not_staged' or not error:
		if field['status'] == 'not_staged' or not error:
			write_file(lock, str(process_id))
		return

	### run prefactor or linc
	update_status(field_name, target_obsid, 'unpacked', 'observations')
	logging.info('Status of \033[35m' + field_name + '\033[32m has been set to: \033[35munpacked.')
	if linc:
		logging.info('LINC pipeline will be started.')
		run_linc(calibrator, field_name, target_obsid, working_directory, submitted, slurm_files, restart = False)
	else:
		logging.info('Prefactor pipeline will be started.')
		run_prefactor(calibrator, field_name, target_obsid, working_directory, submitted, slurm_files)

	### create lock file for keeping the monitoring active
	write_file(lock, str(process_id))

	return


if __name__=='__main__':
	# Get command-line options.
	opt = optparse.OptionParser(usage='%prog ', version='%prog '+_version, description=__doc__)
	opt.add_option('-s', '--server', help='LoTSS MySQL server URL:port', default='localhost:3306')
	opt.add_option('-d', '--database', help='Define which database to use', default='Juelich')
	opt.add_option('-f', '--ftp', help='FTP server hosting current prefactor version', default='gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp')
	opt.add_option('-p', '--id', help='Provide a process ID for the running job. Allows parallel runs', default = 0)
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
	main(options.server, options.database, options.ftp, options.id)

	# monitoring has been finished
	logging.info('\033[30;4mMonitoring has been finished.\033[0m')

	sys.exit(0)
