#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Surveys KSP monitoring script -- more details available later
"""

import os, sys
import logging
import resource
import optparse
import glob
import numpy

import time, datetime
import subprocess
import filecmp

from GRID_LRT.get_picas_credentials import picas_cred
from GRID_LRT import Token
from GRID_LRT.Staging import srmlist

from couchdb.client import Server

_version = '0.1beta'
observation = 'test_L556776'
software_version = 'env_lofar_2.20.2_stage2017b.sh'
nodes = 10
walltime = '05:00:00'
mail = 'alex@tls-tautenburg.de'

os.system('clear')
print '\033[30;1m################################################'
print '## Surveys KSP monitoring script              ##'
print '################################################\033[0m'

def add_coloring_to_emit_ansi(fn):

	def new(*args):
		levelno = args[0].levelno
		if(levelno>=50):
			color = '\x1b[31m' # red
			pass
		elif(levelno>=40):
			color = '\x1b[31m' # red
			pass
		elif(levelno>=30):
			color = '\x1b[33m' # yellow
			pass
		elif(levelno>=20):
			color = '\x1b[32m' # green
			pass
		elif(levelno>=10):
			color = '\x1b[35m' # pink
			pass
		else:
			color = '\x1b[0m' # normal
			pass
		args[0].msg = color + args[0].msg +  '\x1b[0m'  # normal
		return fn(*args)
		pass
	return new
	pass

def load_design_documents(database):
  
	designs = []
	for design_doc in database.view('_all_docs')['_design':'_design0']:
		designs.append(design_doc['id'].lstrip('_design/'))
		pass
	
	return designs
	pass

def get_observations_todo(user, password, database, designs):
  
	for design in designs
		tokens     = Token.Token_Handler( t_type=design, srv=server, uname=pc.user, pwd=pc.password, dbn=pc.database)
		list_todos = tokens.list_tokens_from_view('todo')
		if len(list_todos) > 0:
			observations.append(design)
			pass
		pass
	
	return observations
	pass

def check_for_new_observation(observations, observation_done):
    
	candidates = []
	for candidate in observations:
		try:
			candidates.append(int(filter(lambda x: x.isdigit(), candidate)))
			pass
		except ValueError:
			continue
			pass
		pass
	
	candidate = min(candidates, key=lambda x:abs(x - observation_done))
	
	for item in observation:
		if candidate in item:
			observation = item
			pass
		pass
  
	return observation
	pass
      
def lock_token(tokens, token_value):
	
	token = tokens.db[token_value]
	token['lock'] = time.time()
	tokens.db.update([token])
	logging.info('Token \033[35m' + token_value + '\033[32m has been locked.')
	pass

def set_token_status(tokens, token_value, status):
	
	token = tokens.db[token_value]
	times = token['times']
	
	if type(times) is not dict:
		times = {}
		pass
	
	times[status]   = time.time()
	token['times']  = times
	token['status'] = status
	tokens.db.update([token])
	logging.info('Status of token \033[35m' + token_value + '\033[32m has been set to \033[35m' + status + '\033[32m.')
	pass

def token_status(tokens, token_value):
	
	token = tokens.db[token_value]
	status = token['status']
	return status
	pass
	
def set_token_output(tokens, token_value, output):
	
	token = tokens.db[token_value]
	token['output'] = output
	tokens.db.update([token])
	logging.info('Output of token \033[35m' + token_value + '\033[32m has been set to \033[35m' + str(output) + '\033[32m.')
	pass
      
def set_token_progress(tokens, token_value, progress):
	
	token = tokens.db[token_value]
	token['progress'] = progress
	tokens.db.update([token])
	logging.info('Progress of token \033[35m' + token_value + '\033[32m has been set to \033[35m' + str(progress) + '\033[32m.')
	pass
      
def unlock_token(tokens, token_value):
	
	token = tokens.db[token_value]
	token['lock'] = 0
	tokens.db.update([token])
	logging.info('Token \033[35m' + token_value + '\033[32m has been unlocked.')
	pass

def token_output(tokens, token_value):
	
	token = tokens.db[token_value]
	output = token['output']
	return output
	pass

def is_staged(url):
	try:
		if 'ONLINE_AND_NEARLINE' in subprocess.check_output(['srmls', '-l', url]):
		#if 'NEARLINE' in subprocess.check_output(['srmls', '-l', url]):
			return True
			pass
		else:
			return False
			pass
	except:
		return False
		pass
	pass

def is_running(lock_file):
	if os.path.isfile(lock_file):
		return True
		pass
	else:
		return False
		pass
	pass

def unpack_data(tokens, token_value, filename, working_directory):
  
	token = tokens.db[token_value]
	observation_id = token['OBSID']

	set_token_status(tokens, token_value, 'unpacking')
	
	os.chdir(working_directory)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode == 0:
		set_token_status(tokens, token_value, 'unpacked')
		set_token_progress(tokens, token_value, 0)
		os.remove(filename)
		logging.info('File \033[35m' + filename + '\033[32m was removed.')
		pass
	else:
		logging.error('\033[31mUnpacking failed, error code: ' + str(errorcode))
		set_token_status(tokens, token_value, 'error')
		set_token_output(tokens, item['value'], 20)
		set_token_progress(tokens, item['value'], 'Unpacking failed, error code: ' + str(errorcode))
		unlock_token(tokens, item['value'])
		pass
	pass

def download_data(tokens, list_todos, pipeline_todownload, working_directory):
	
	download_list = srmlist.srmlist()                   # create list to download
	for item in list_todos:
		token = tokens.db[item['value']]
		pipeline = token['PIPELINE']
		if pipeline != pipeline_todownload:
			continue
			pass
		lock_token(tokens, item['value'])
		srm = tokens.db.get_attachment(item['value'], 'srm.txt').read().strip()
		if not is_staged(srm):
			logging.warning('\033[33mFile \033[35m' + srm + '\033[33m has not been staged yet.')
			set_token_status(tokens, item['value'], 'queued')
			set_token_output(tokens, item['value'], 0)
			set_token_progress(tokens, item['value'], 'File ' + srm + ' has not been staged yet.')
			unlock_token(tokens, item['value'])
			continue
			pass
		logging.info('File \033[35m' + srm + '\033[32m is properly staged.')
		status = token_status(tokens, item['value'])
		if status == 'unpacked' or  status == 'downloaded' or status == 'unpacking' or status == 'downloading':
			logging.warning('\033[33mFile \033[35m' + srm + '\033[33m is already \033[35m' + status)
			pass
		else:
			download_list.append(srm)
			pass
		pass
	
	
	gsilist = download_list.gsi_links() # convert the srm list to a GSI list (proper URLs for GRID download)
	gsilist = list(reversed(list(set(gsilist)))) # to re-reverse the list in order to match it for the upcoming loop and use only distinct files
	for url,item in zip(gsilist, list_todos):
		status = token_status(tokens, item['value'])
		set_token_status(tokens, item['value'], 'downloading')
		filename = working_directory + '/' + url.split('/')[-1]
		download = subprocess.Popen(['globus-url-copy', url, 'file:' + filename], stdout=subprocess.PIPE)
		errorcode = download.wait()
		if errorcode == 0:
			set_token_status(tokens, item['value'], 'downloaded')
			set_token_output(tokens, item['value'], 0)
			set_token_progress(tokens, item['value'], 0)
			unpack_data(tokens, item['value'], filename, working_directory)
			pass
		else:
			logging.error('Download failed, error code: ' + str(int(errorcode)))
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], 20)
			set_token_progress(tokens, item['value'], 'Download failed, error code: ' + str(int(errorcode)))
			unlock_token(tokens, item['value'])
			pass
		pass
	return 0
	pass
   
def create_submission_script(submit_job, parset, working_directory, submitted):
	
	home_directory = os.environ['HOME']
	
	if os.path.isfile(submit_job):
		logging.warning('\033[33mFile for submission already exists. It will be overwritten.')
		os.remove(submit_job)
		pass
	
	jobfile = open(submit_job, 'w')
	
	## writing file header
	jobfile.write('#!/usr/bin/env sh\n')
	
	## extracting directories for IONEX and the TGSS ADR skymodel
	try:
		IONEX_script         = os.popen('find ' + working_directory + ' | grep download_IONEX.py').readlines()[0].rstrip('\n').replace(' ','')
		IONEX_path           = os.popen('grep ionex_path '           + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		file.write(IONEX_script + ' --destination ' + IONEX_path + ' ' + working_directory + '/' + target_input_pattern)
		pass
	except IndexError:
		pass
	try:
		skymodel_script      = os.popen('find ' + working_directory + ' | grep download_tgss_skymodel_target.py').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		target_skymodel      = os.popen('grep target_skymodel '      + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		file.write(skymodel_script + ' ' + working_directory + '/' + target_input_pattern + ' ' + target_skymodel)
		pass
	except IndexError:
		pass
	
	## write-up of final command
	jobfile.write('\n')
	jobfile.write('sbatch --nodes=' + str(nodes) + ' --partition=batch --mail-user=' + mail + ' --mail-type=ALL --time=' + walltime + ' ' + home_directory + '/run_pipeline.sh ' + parset + ' ' + working_directory)
	jobfile.close()
	
	os.system('chmod +x ' + submit_job)
	os.rename(submit_job, submit_job + '.sh')
	subprocess.Popen(['touch', submitted])
	
	return 0
	pass
   
   
def run_prefactor(tokens, list_pipeline, working_directory, ftp, submitted, slurm_files, pipeline):
  
	parset     = working_directory + '/pipeline.parset'
	parset2    = working_directory + '/pipeline2.parset'
	submit_job = working_directory + '/submit_job'
	
	if os.path.isfile(parset):
		os.remove(parset)
		pass
	if os.path.isfile(parset2):
		os.remove(parset2)
		pass
	
	logging.info('Getting pipeline parset file for \033[35m' + observation)
	SBXloc = []
	for item in list_pipeline:
		token = tokens.db[item['value']] ## checkout location for pipeline
		SBXloc.append(token['SBXloc'])   ## save location for pipeline
		attachments = tokens.list_attachments(item['value'])
		parsets = [i for i in attachments if 'parset' in i]
		if len(parsets) != 1:
				logging.error('\033[31mMultiple or no parsets attached to: \033[35m' + item['value'])
				set_token_status(tokens, item['value'], 'error')
				set_token_output(tokens, item['value'], -1)
				set_token_progress(tokens, item['value'], 'Multiple or no parsets attached')
				pass
  		if os.path.isfile(parset):
			tokens.get_attachment(item['value'], parsets[0], parset2)
			if not filecmp.cmp(parset, parset2):
				logging.error('\033[31mParset file mismatches for: \033[35m' + item['value'])
				set_token_status(tokens, item['value'], 'error')
				set_token_output(tokens, item['value'], -1)
				set_token_progress(tokens, item['value'], 'Parset file mismatch')
				return 1
				pass
			if len(list(set(SBXloc))) > 1:
				logging.error('\033[31mSBXloc mismatches for: \033[35m' + item['value'])
				set_token_status(tokens, item['value'], 'error')
				set_token_output(tokens, item['value'], -1)
				set_token_progress(tokens, item['value'], 'SBXloc mismatch')
				return 1
				pass
			os.remove(parset2)
			pass
		else:
			tokens.get_attachment(item['value'], parsets[0], parset)
			pass
		pass
	
	SBXloc = str(list(set(SBXloc))[0])

	## applying necessary changes to the parset
	num_proc_per_node       = os.popen('grep "! num_proc_per_node" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node_limit = os.popen('grep "! num_proc_per_node_limit" ' + parset).readlines()[0].rstrip('\n').replace('/','\/')
	max_dppp_threads        = os.popen('grep "! max_dppp_threads" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')
	losoto_executable       = os.popen('grep "! losoto_executable" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
	cal_input_pattern       = os.popen('grep "! cal_input_pattern" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
		
	os.system('sed -i "s/' + losoto_executable       + '/! losoto_executable         = \$LOSOTOROOT\/bin\/losoto/g " ' + parset)
	os.system('sed -i "s/' + num_proc_per_node       + '/! num_proc_per_node         = input.output.max_per_node/g" '  + parset)
	os.system('sed -i "s/' + num_proc_per_node_limit + '/! num_proc_per_node_limit   = 10/g" '                         + parset)
	os.system('sed -i "s/' + max_dppp_threads        + '/! max_dppp_threads          = 10/g" '                         + parset)
	os.system('sed -i "s/PREFACTOR_SCRATCH_DIR/\$WORK/g" ' + parset)
	
	cal_input_path          = os.popen('grep "! cal_input_path" '          + parset).readlines()[0].rstrip('\n').replace('/','\/').replace('$','\$')
	if not 'MS' in cal_input_pattern:
		os.system('sed -i "s/' + cal_input_path  + '/! cal_input_path       = \$WORK\/pipeline/g" '                + parset)
		pass
	
	#sys.exit(0)
	## downloading prefactor
	sandbox = ftp + SBXloc
	filename = working_directory + '/prefactor.tar'
	logging.info('Downloading current prefactor version from \033[35m' + sandbox)
	download = subprocess.Popen(['globus-url-copy', sandbox , 'file:' + filename], stdout=subprocess.PIPE)
	#download = subprocess.Popen(['curl', ftp, '--output', filename], stdout=subprocess.PIPE)
	errorcode = download.wait()
	if errorcode != 0:
		logging.error('\033[31m Downloading prefactor has failed.')
		for item in list_pipeline: 
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], -1)
			set_token_progress(tokens, item['value'], 'download of prefactor failed, error code: ' + str(int(errorcode)))
			return 1
			pass
		pass
	      
	logging.info('Unpacking current prefactor version to \033[35m' + working_directory)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory])
	errorcode = unpack.wait()
	if errorcode != 0:
		for item in list_pipeline:
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], -1)
			set_token_progress(tokens, item['value'], 'unpacking of prefactor failed, error code: ' + str(int(errorcode)))
			pass
		logging.error('\033[31m Unpacking prefactor has failed.')
		return 1
		pass
	
	logging.info('Creating submission script in \033[35m' + submit_job)
	create_submission_script(submit_job, parset, working_directory, submitted)
	
	slurm_list = glob.glob(slurm_files)
	if len(slurm_list) > 0:
		os.remove(slurm_list[-1])
		pass

	logging.info('\033[0mWaiting for submission\033[0;5m...')
	while os.path.exists(submit_job + '.sh'):
		time.sleep(5)
		pass
	for item in list_pipeline:
		set_token_status(tokens, item['value'], 'submitted')
		set_token_output(tokens, item['value'], 0)
		set_token_progress(tokens, item['value'], 0)
		pass
	logging.info('Pipeline has been submitted.')
	
	return 0
	pass
	

def get_pipelines(tokens, list_locked):
	
	pipelines = []
	
	for item in list_locked:
		token = tokens.db[item['value']]
		pipeline = token['PIPELINE']
		if pipeline not in pipelines:
			pipelines.append(pipeline)
			pass
		pass
      
	return pipelines
	pass
	
def pipeline_status(tokens, list_pipeline):
	
	status = []
	for item in list_pipeline:
		status.append(token_status(tokens, item['value']))
		pass
	status = list(set(status))
	
	return status
	pass

def pipeline_output(tokens, list_pipeline):
	
	output = []
	for item in list_pipeline:
		output.append(token_output(tokens, item['value']))
		pass
	output = list(set(output))
	
	return output
	pass

def check_submitted_job(slurm_log, submitted):
	  
	log_information = os.popen('tail -9 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'ERROR' in log_information:
		logging.warning(log_information)
		os.remove(submitted)
		return log_information
		pass
	log_information = os.popen('tail -7 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'finished' in log_information:
		logging.info(log_information)
		os.remove(submitted)
		return log_information
		pass
	
	return 'processing'
	pass

def submit_error_log(tokens, token_value, slurm_log, log_information, working_directory, upload):

	attachments = tokens.list_attachments(token_value)
	old_slurm_logs = [i for i in attachments if 'slurm' in i]
	doc = tokens.db[token_value]
	for old_slurm_log in old_slurm_logs:
		tokens.db.delete_attachment(doc, old_slurm_log)
		pass
	tokens.add_attachment(token_value, open(slurm_log,'r'), os.path.basename(slurm_log))
	if 'ERROR' in log_information:
		set_token_status(tokens, token_value, 'error')
		set_token_output(tokens, token_value, -1)
		set_token_progress(tokens, token_value, log_information[log_information.find('genericpipeline:'):])
		pass
	elif 'finished' in log_information:
		set_token_status(tokens, token_value, 'done')
		set_token_output(tokens, token_value, 0)
		set_token_progress(tokens, token_value, log_information[log_information.find('genericpipeline:'):])
		if os.path.exists(working_directory + '/pipeline/statefile'):
			os.remove(working_directory + '/pipeline/statefile')
			pass
		subprocess.Popen(['touch', upload])
		logging.info('Statefile was removed.')
		pass
	return 0
	pass
      
def submit_results(tokens, token_value, working_directory, upload):

	parset               = working_directory + '/pipeline.parset'
	inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$WORK', working_directory)
	
	attachments = tokens.list_attachments(token_value)
	old_images  = [i for i in attachments if '.png' in i]
	doc = tokens.db[token_value]
	for old_image in old_images:
		tokens.db.delete_attachment(doc, old_image)
		pass
	      
	# upload inspection plots
	images = glob.glob(inspection_directory + '/*.png')
	for image in images:
		tokens.add_attachment(token_value, open(image,'r'), os.path.basename(image))
		pass

	os.remove(upload)
	logging.info('Inspection plots have been uploaded.')
	return 0
	pass

def main(server='https://picas-lofar.grid.sara.nl:6984', ftp='gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/sandbox/'):
	
	## load working environment
	working_directory = os.environ['WORK']
	lock_file         = working_directory + '/.lock'
	submitted         = working_directory + '/.submitted'
	done              = working_directory + '/.done'
	upload            = working_directory + '/.upload'
	last_observation  = working_directory + '/.observation'
	slurm_files       = 'slurm-*.out'
	log_information   = ''
	logging.info('\033[0mWorking directory is ' + working_directory)
	
	## check whether an instance of this program is already running
	if is_running(lock_file):
		logging.error('\033[31mAn instance of this program appears to be still running. If not, please remove the lock file: \033[0m' + lock_file)
		return 1
		pass
	elif is_running(submitted): 
		logging.info('\033[0mAnother pipeline has already been submitted.')
		slurm_list = glob.glob(slurm_files)
		if len(slurm_list) > 0:
			slurm_log = slurm_list[-1]
			job_id = os.path.basename(slurm_log).lstrip('slurm-').rstrip('.out')
			logging.info('Checking current status of the submitted job: \033[35m' + job_id)
			log_information = check_submitted_job(slurm_log, submitted)
			pass
		else:
			return 0
			pass
		pass
	           
	## load PiCaS credentials and connect to server
	logging.info('\033[0mConnecting to server: ' + server)
	couchdb_server = Server(server)
	pc = picas_cred()
	pc.get_picas_creds_from_file()
	logging.info('Username: \033[35m' + pc.user)
	logging.info('Database: \033[35m' + pc.database)
	couchdb_server.resource.credentials = (pc.user, pc.password)
	
	## load all design documents
	designs      = load_design_documents(couchdb_server[pc.database])
	observations = get_observations_todo(pc.user, pc.password, pc.database, designs)
	
	## check latest observation
	if is_running(last_observation):
		observation = open(last_observation).readline().rstrip()
		if observation not in designs:
			logging.error('\033[31mCould not find a corresponding token for the last observation \033[35m' + observation + '\033[31m . Please check the database for errors or remove the last observation.')
			return 1
			pass
		if is_running(done):
			logging.error('\033[31mChecking new observations.')
			observation_done = int(filter(lambda x: x.isdigit(), observation))
			observation = check_for_new_observation(observations, observation_done)
			pass
		pass
	
	
	## reserve following processes
	logging.info('Selected observation: \033[35m' + observation)
	subprocess.Popen(['touch', lock_file])
	numpy.savetxt(last_observation, observation)
	
	## load token of chosen design document
	tokens   = Token.Token_Handler( t_type=observation, srv=server, uname=pc.user, pwd=pc.password, dbn=pc.database) # load token of certain type
	
	## check for new data sets and get information about other tokens present
	list_locked = tokens.list_tokens_from_view('locked') # check which tokens of certain type are in the locked state
	list_error  = tokens.list_tokens_from_view('error')  # check which tokens of certain type show errors
	list_done   = tokens.list_tokens_from_view('done')   # check which tokens of certain type are done
	list_todos  = tokens.list_tokens_from_view('todo')   # check which tokens of certain type are in the todo state
	
	## check which pipelines are locked, done or show errors
	locked_pipelines = get_pipelines(tokens, list_locked)
	bad_pipelines    = get_pipelines(tokens, list_error)
	pipelines_done   = get_pipelines(tokens, list_done)
	pipelines_todo   = get_pipelines(tokens, list_todos)
	
	pipelines = list(reversed(list(set(locked_pipelines) - set(pipelines_done) - set(pipelines_todo))))

	#check what to download
	if len(list_todos) > 0 and len(list_done) == 0:
		download_data(tokens, list_todos, pipelines_todo[0], working_directory)
		pass
	
	#check errors of the pipelines
	if len(bad_pipelines) != 0:
		logging.warning('\033[33mPipeline(s) \033[35m' + str(bad_pipelines) + '\033[33m show errors. Please check their token status. Script will try to rerun them.')
		pass
	
	#check all finished pipelines
	if len(pipelines_done) !=0:
		logging.info('\033[0mPipeline(s) \033[35m' + str(pipelines_done) + ' \033[0m for this observation are done.')
		if len(pipelines) == 0 and len(pipelines_todo) != 0:
			for item in list_todos:
				lock_token(tokens, item['value'])
				pass
			pipelines = pipelines_todo
			pass
		if len(pipelines) == 0 and len(pipelines_todo) == 0:
			if is_running(upload):
				for i, item in enumerate(list_done):
					if i == len(list_done) - 1:
						submit_results(tokens, item['value'], working_directory, upload)
						pass
					pass
				pass
			else:
				logging.info('\033[0mObservation \033[35m' + observation + '\033[0m is done.')
				subprocess.Popen(['touch', done])
				pass
			pass
		pass


	# main pipeline loop
	for pipeline in pipelines:
		list_pipeline = tokens.list_tokens_from_view(pipeline)  ## get the pipeline list
		status = pipeline_status(tokens, list_pipeline)
		output = pipeline_output(tokens, list_pipeline)
		if len(status) > 1:
			logging.warning('\033[33mPipeline \033[35m' + pipeline + '\033[33m shows more than one status: \033[35m' + str(status) + '\033[33m. Script will proceed without it.')
			continue
			pass
		elif status[0] == 'todo':
			logging.warning('\033[33mAll data for the pipeline \033[35m' + pipeline + '\033[33m are not yet available. Check missing files.')
			continue
			pass
		elif status[0] == 'submitted':
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m has already been submitted.')
			for item in list_pipeline:
				if log_information == 'processing':
				  	set_token_status(tokens, item['value'], 'processing')
					set_token_output(tokens, item['value'], 0)
					set_token_progress(tokens, item['value'], 0)
					pass
				elif log_information != '':
					submit_error_log(tokens, item['value'], slurm_log, log_information, working_directory, upload)
					pass
				pass
			break
			pass
		elif status[0] == 'processing':
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m is currently processed.')
			if log_information == 'processing':
				pass
			elif log_information != '':
				for item in list_pipeline:
					submit_error_log(tokens, item['value'], slurm_log, log_information, working_directory, upload)
					pass
				pass
			break
			pass
		elif status[0] == 'unpacked' or status[0] == 'queued':
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be started.')
			run_prefactor(tokens, list_pipeline, working_directory, ftp, submitted, slurm_files, pipeline)
			break
			pass
		elif -1 in output:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be resumed.')
			run_prefactor(tokens, list_pipeline, working_directory, ftp, submitted, slurm_files, pipeline)
			break
			pass      
		else:
			logging.warning('\033[33mPipeline \033[35m' + pipeline + '\033[33m has an invalid status. Script will proceed without it.')
			continue
			pass
		pass
	pass
	
	## check which pipelines need further processing with prefactor
	if len(pipelines) == 0 and len(pipelines_todo) == 0:
		logging.info('\033[0mNo tokens found in database to be processed.')
		pass
	      
	## remove the lock file
	os.remove(lock_file)
	
	return 0
	pass

if __name__=='__main__':
	# Get command-line options.
	opt = optparse.OptionParser(usage='%prog ', version='%prog '+_version, description=__doc__)
	opt.add_option('-s', '--server', help='PiCaS server URL:port', action='store_true', default='https://picas-lofar.grid.sara.nl:6984')
	opt.add_option('-f', '--ftp', help='FTP server hosting current prefactor version', action='store_true', default='gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/sandbox/')
	(options, args) = opt.parse_args()
	
	format_stream = logging.Formatter("%(asctime)s\033[1m %(levelname)s:\033[0m %(message)s","%Y-%m-%d %H:%M:%S")
	format_file   = logging.Formatter("%(asctime)s %(levelname)s: %(message)s","%Y-%m-%d %H:%M:%S")
	logging.root.setLevel(logging.INFO)
	
	log      = logging.StreamHandler()
	log.setFormatter(format_stream)
	log.emit = add_coloring_to_emit_ansi(log.emit)
	logging.root.addHandler(log)
	
	#pwd            = os.getcwd()
	home_directory = os.environ['HOME']
	LOG_FILENAME = home_directory + '/logs/' + str(datetime.datetime.utcnow().replace(microsecond=0).isoformat()) + '.log'
	if not os.path.exists(home_directory + '/logs'):
		os.makedirs(home_directory + '/logs')
		pass
	logfile = logging.FileHandler(LOG_FILENAME)
	logfile.setFormatter(format_file)
	logfile.emit = add_coloring_to_emit_ansi(logfile.emit)
	logging.root.addHandler(logfile)

	#logging.info('\033[0mYou are starting the script from ' + pwd)
	logging.info('\033[0mLog file is written to ' + LOG_FILENAME)
	
	# start running script
	main(options.server)
	
	# monitoring has been finished
	logging.info('\033[30;4mMonitoring has been finished.\033[0m')
	
	sys.exit(0)
	pass
    
