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
import shutil

import time, datetime
import subprocess
import filecmp

from GRID_LRT.get_picas_credentials import picas_cred
from GRID_LRT import Token
from GRID_LRT.Staging import srmlist

from GRID_LRT.couchdb.client import Server

_version = '0.1beta'
software_version = 'env_lofar_2.20.2_stage2017b.sh'
nodes = 10
walltime = '02:00:00'
mail = 'alex@tls-tautenburg.de'
IONEX_server = 'ftp://ftp.aiub.unibe.ch/CODE/'
num_SBs_per_group_var = 10
max_dppp_threads_var = 10
max_proc_per_node_limit_var = 6
threshold = 10    ## what is the maximum difference between observation IDs for calibrator and targets
condition = 'cal' ## condition for the pipeline in order to be idenitified as new observations (usually the calibrator pipeline)

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

def get_observations_todo(user, password, database, designs, server):
  
	observations = []
	
	for design in designs:
		tokens     = Token.Token_Handler( t_type=design, srv=server, uname=user, pwd=password, dbn=database)
		list_todos = tokens.list_tokens_from_view('todo')
		if len(list_todos) > 0:
			observations.append(design)
			pass
		pass
	
	return observations
	pass

def get_observation_id(tokens, list_todos):
	
	obsids = []
	
	for item in list_todos:
		token = tokens.db[item['value']]
		obsid = token['OBSID']
		if obsid not in obsids:
			obsids.append(obsid)
			pass
		pass

	if len(obsids) > 1:
		return 1
		pass
	else:
		return obsids[0]
		pass
	pass

def check_for_corresponding_observation(observations, observation, server, user, password, database, tokens_done):
    
	candidates = []
	for candidate in observations:
		try:
			tokens_candidate = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database)
			tokens_candidate.add_view(v_name='overview_all', cond=' doc.lock > 0 | doc.lock == 0')
			list_all         = tokens_candidate.list_tokens_from_view('overview_all')
			obsid            = get_observation_id(tokens_candidate, list_all)
			candidates.append(int(filter(lambda x: x.isdigit(), obsid)))
			pass
		except ValueError or IndexError:
			continue
			pass
		pass
	
	try:
		tokens_done.add_view(v_name='overview_all', cond=' doc.lock > 0 | doc.lock == 0')
		list_all         = tokens_done.list_tokens_from_view('overview_all')
		obsid            = get_observation_id(tokens_done, list_all)
	        observation_done = int(filter(lambda x: x.isdigit(), obsid))
		candidate        = min(candidates, key=lambda x:abs(x - observation_done))
		pass
	except ValueError:
		return 1
		pass
	      
	if candidate > threshold:
		logging.warning('\033[33mNo corresponding observation found for \033[35mL' + str(observation_done))
		list_done = tokens_done.list_tokens_from_view('done')
		pipelines_done = get_pipelines(tokens_done, list_done)
		for pipeline in pipelines_done:
			if condition in pipeline:
				for item in list_done:
					set_token_progress(tokens_done, item['value'], 'No corresponding observation found.')
					pass
				pass
			pass
		return 1
		pass
	
	for item in observations:
		if candidate in item:
			observation = item
			pass
		pass
  
	return observation
	pass

def get_pipelines(tokens, list_locked):
	
	pipelines = []
	
	for item in list_locked:
		token = tokens.db[item['value']]
		pipeline = token['pipeline']
		if pipeline not in pipelines:
			pipelines.append(pipeline)
			pass
		pass
      
	return pipelines
	pass
      
def find_new_observation(observations, observation_done, server, user, password, database, working_directory):

	condition_observations = []
	
	for observation in observations:
		tokens         = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) # load token of certain type
		list_todos     = tokens.list_tokens_from_view('todo')   # check which tokens of certain type are in the todo state
		try:
			pipelines_todo = get_pipelines(tokens, list_todos)
		except KeyError:
			logging.warning('Observation: \033[35m' + observation + '\033[33m is invalid.')
			continue
			pass
		for pipeline in pipelines_todo:
			if condition in pipeline:
				condition_observations.append(observation)
				check = check_for_corresponding_observation(observations, observation, server, user, password, database, tokens)
				if check != 1:
					logging.info('Cleaning working directory.', ignore_errors=True)
					shutil.rmtree(working_directory)
					return observation
					pass
				pass
			pass
		pass
	
	if len(condition_observations) > 0:
	  	logging.info('Cleaning working directory.')
		shutil.rmtree(working_directory, ignore_errors=True)
		return condition_observations[0]
		pass
	else:
		return observation_done
		pass
	pass
              
def lock_token(tokens, token_value):
	
	token = tokens.db[token_value]
	token['lock'] = time.time()
	tokens.db.update([token])
	logging.info('Token \033[35m' + token_value + '\033[32m has been locked.')
	pass
      
def lock_token_done(tokens, token_value):
	
	token = tokens.db[token_value]
	token['done'] = time.time()
	tokens.db.update([token])
	logging.info('Token \033[35m' + token_value + '\033[32m is done.')
	pass
      
def add_time_stamp(tokens, token_value, status):
	
	token = tokens.db[token_value]
	times = token['times']
	
	times[status]   = time.time()
	token['times']  = times

	tokens.db.update([token])
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

def pack_data(tokens, token_value, filename, pack_directory):
  
	os.chdir(pack_directory)
	logging.info('Packing file: \033[35m' + filename)
	set_token_progress(tokens, token_value, 'Packing file: ' + filename)
	add_time_stamp(tokens, token_value, 'packing')
	pack = subprocess.Popen(['tar', 'cfv', filename + '.tar', filename], stdout=subprocess.PIPE)
	errorcode = pack.wait()
	if errorcode == 0:
		set_token_output(tokens, token_value, 0)
		logging.info('Packing of \033[35m' + filename + '\033[32m finished.')
		pass
	else:
		logging.error('\033[31mPacking failed, error code: ' + str(errorcode))
		set_token_output(tokens, token_value, -1)
		set_token_progress(tokens, token_value, 'Packing failed, error code: ' + str(errorcode))
		add_time_stamp(tokens, token_value, 'error')
		pass
	pass
      
def unpack_data(tokens, token_value, filename, working_directory):
  
	token = tokens.db[token_value]

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
		set_token_output(tokens, token_value, 20)
		set_token_progress(tokens, token_value, 'Unpacking failed, error code: ' + str(errorcode))
		unlock_token(tokens, token_value)
		pass
	pass

def download_data(tokens, list_todos, pipeline_todownload, working_directory):
	
	download_list = srmlist.srmlist()                   # create list to download
	for item in list_todos:
		token = tokens.db[item['value']]
		pipeline = token['pipeline']
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
	gsilist = sorted(list(set(gsilist))) # to re-reverse the list in order to match it for the upcoming loop and use only distinct files
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
		jobfile.write(IONEX_script + ' --destination ' + IONEX_path + ' --server ' + IONEX_server + ' ' + working_directory + '/' + target_input_pattern + '\n')
		pass
	except IndexError:
		pass
	try:
		skymodel_script      = os.popen('find ' + working_directory + ' | grep download_tgss_skymodel_target.py').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		target_skymodel      = os.popen('grep target_skymodel '      + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		jobfile.write(skymodel_script + ' ' + working_directory + '/' + target_input_pattern + ' ' + target_skymodel + '\n')
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
   
   
def run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline):
  
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

	try:
		cal_input_pattern    = os.popen('grep "! cal_input_pattern" '  + parset).readlines()[0].rstrip('\n').replace('/','\/')
		pass
	except IndexError:
		cal_input_pattern    = ''
		pass
	try:
		input_path           = os.popen('grep "! target_input_path" '    + parset).readlines()[0].rstrip('\n').replace('/','\/').replace('$','\$')
		target_input_pattern = os.popen('grep "! target_input_pattern" ' + parset).readlines()[0].rstrip('\n').replace('/','\/')
		makesourcedb         = os.popen('grep "! makesourcedb" '         + parset).readlines()[0].rstrip('\n').replace('/','\/')
		flagging_strategy    = os.popen('grep "! flagging_strategy" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
		num_SBs_per_group    = os.popen('grep "! num_SBs_per_group" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
		
		os.system('sed -i "s/' + makesourcedb            + '/! makesourcedb            = \$LOFARROOT\/bin\/makesourcedb/g " '                + parset)
		os.system('sed -i "s/' + flagging_strategy       + '/! flagging_strategy       = \$LOFARROOT\/share\/rfistrategies\/HBAdefault/g " ' + parset)
		os.system('sed -i "s/' + num_SBs_per_group       + '/! num_SBs_per_group       = ' + str(num_SBs_per_group_var)       + '/g" '       + parset)
			
		if not 'MS' in target_input_pattern:
			os.system('sed -i "s/' + input_path  + '/! target_input_path   = \$WORK\/pipeline/g" '                               + parset)
			pass
		pass
	except IndexError:
		input_path = os.popen('grep "! cal_input_path" '    + parset).readlines()[0].rstrip('\n').replace('/','\/').replace('$','\$')
		
		if not 'MS' in cal_input_pattern:
			os.system('sed -i "s/' + input_path  + '/! cal_input_path      = \$WORK\/pipeline/g" '                               + parset)
			pass
		pass
		
	os.system('sed -i "s/' + losoto_executable       + '/! losoto_executable       = \$LOFARROOT\/bin\/losoto/g " '                      + parset)
	os.system('sed -i "s/' + num_proc_per_node       + '/! num_proc_per_node       = input.output.max_per_node/g" '                      + parset)
	os.system('sed -i "s/' + num_proc_per_node_limit + '/! num_proc_per_node_limit = ' + str(max_proc_per_node_limit_var) + '/g" '       + parset)
	os.system('sed -i "s/' + max_dppp_threads        + '/! max_dppp_threads        = ' + str(max_dppp_threads_var)        + '/g" '       + parset)
	os.system('sed -i "s/PREFACTOR_SCRATCH_DIR/\$WORK/g" ' + parset)
	
	try:
		results_directory = os.popen('grep "! results_directory" ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace('$WORK', working_directory).replace(' ','')
		results = glob.glob(results_directory + '/*.ms')
		for result in results:
			shutil.move(result, working_directory + '/pipeline/.')
			pass
		pass
	except IndexError:
		pass

	## downloading prefactor
	sandbox = SBXloc
	filename = working_directory + '/prefactor.tar'
	logging.info('Downloading current prefactor version from \033[35m' + sandbox)
	download = subprocess.Popen(['globus-url-copy', sandbox , 'file:' + filename], stdout=subprocess.PIPE)
	errorcode = download.wait()
	if errorcode != 0:
		logging.error('\033[31m Downloading prefactor has failed.')
		for item in list_pipeline: 
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], -1)
			set_token_progress(tokens, item['value'], 'download of prefactor failed, error code: ' + str(int(errorcode)))
			pass
		return 1
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


def update_freq(tokens, token_value, freq):
    
	token = tokens.db[token_value] 
	
	A_SBN = int(round((((float(freq)/1e6 - 100.0) / 100.0) * 512.0),0))
	if 'FREQ' in token.keys():
		token['FREQ'] = freq
		pass
	
	token['ABN'] = A_SBN
	tokens.db.update([token])
	
	return A_SBN
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
		lock_token_done(tokens, token_value)
		set_token_progress(tokens, token_value, log_information[log_information.find('genericpipeline:'):])
		
		if os.path.exists(working_directory + '/pipeline/statefile'):
			os.remove(working_directory + '/pipeline/statefile')
			pass
		subprocess.Popen(['touch', upload])
		logging.info('Statefile was removed.')
		pass
	return 0
	pass
      
def submit_results(tokens, token_value, observation, list_done, working_directory, upload):

	parset               = working_directory + '/pipeline.parset'
	inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$WORK', working_directory)

	try:
		results_directory = os.popen('grep "! results_directory" ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace('$WORK', working_directory).replace(' ','')
		results     = glob.glob(results_directory + '/*.ms')
		calibration = glob.glob(results_directory + '/*.h5')
	except IndexError:
		results = []
		pass

	attachments = tokens.list_attachments(token_value)
	old_images  = [i for i in attachments if '.png' in i]
	doc = tokens.db[token_value]
	for old_image in old_images:
		tokens.db.delete_attachment(doc, old_image)
		pass
	      
	# upload inspection plots
	os.remove(upload)
	images = glob.glob(inspection_directory + '/*.png')
	for image in images:
		tokens.add_attachment(token_value, open(image,'r'), os.path.basename(image))
		pass
	logging.info('Inspection plots have been uploaded.')
	if len(images) > 1 and (len(results)> 0 or len(calibration) > 0):
		list_transfer = []
		for item in list_done:
			if token_output(tokens, item['value']) == 1:
				continue
				pass
			list_transfer.append(item)
			pass
		observation = 'L' + str(filter(lambda x: x.isdigit(), observation))        ## checkout location for pipeline
		for item, result in zip(list_transfer, results):
			pack_data(tokens, item['value'], result, inspection_directory)
			token = tokens.db[item['value']]
			transfer_dir = token['RESULTS_DIR'] + '/' + observation             ## get results directory
			subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
			freq = os.popen('taql "select distinct REF_FREQUENCY from ' + result + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
			ABN = update_freq(tokens, item['value'], freq)
			filename = transfer_dir + '/GSM_CAL_' + observation + '_ABN_' + str(ABN) + '.tar'
			logging.info('\033[35m' + result + '\033[32m is now transfered to: \033[35m' + filename)
			set_token_progress(tokens, item['value'], 'Transfer of data to: ' + filename)
			add_time_stamp(tokens, item['value'], 'transferring')
			transfer = subprocess.Popen(['globus-url-copy', 'file:' + result + '.tar', filename], stdout=subprocess.PIPE)
			errorcode = transfer.wait()
			if errorcode == 0:
				logging.info('File \033[35m' + result + '\033[32m was transfered.')
				shutil.move(result, working_directory + '/pipeline/.')
				set_token_output(tokens, item['value'], 1)
				set_token_progress(tokens, item['value'], 'Transfer of data has been finished.')
				add_time_stamp(tokens, item['value'], 'transferred')
				pass
			else:
				logging.error('\033[31mTransfer of \033[35m' + result + '\033[31m failed. Error code: \033[35m' + str(errorcode))
				set_token_output(tokens, token_value, -1)
				set_token_progress(tokens, token_value, 'Transfer of ' + str(result) + ' failed.')
				add_time_stamp(tokens, item['value'], 'error')
				subprocess.Popen(['touch', upload])
				pass
			pass
		if len(calibration) > 0:
			logging.info('Calibration results are transferred.')
			token = tokens.db[token_value]
			transfer_dir = token['RESULTS_DIR'] + '/' + observation             ## get results directory
			filename = transfer_dir + '/' + calibration[0].split('/')[-1]
			transfer = subprocess.Popen(['globus-url-copy', 'file:' + calibration[0], filename], stdout=subprocess.PIPE)
			errorcode = transfer.wait()
			if errorcode == 0:
				logging.info('File \033[35m' + calibration[0] + '\033[32m was transfered.')
				pass
			else:
				logging.error('\033[31mTransfer of \033[35m' + calibration[0] + '\033[31m failed. Error code: \033[35m' + str(errorcode))
				subprocess.Popen(['touch', upload])
				pass
			pass
		pass
	
	logging.info('Submitting results has been finished.')
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
	           
	## load PiCaS credentials and connect to server
	logging.info('\033[0mConnecting to server: ' + server)
	couchdb_server = Server(server)
	pc = picas_cred()
	pc.get_picas_creds_from_file()
	logging.info('Username: \033[35m' + pc.user)
	logging.info('Database: \033[35m' + pc.database)
	couchdb_server.resource.credentials = (pc.user, pc.password)
	
	## load all design documents
	designs          = load_design_documents(couchdb_server[pc.database])
	observations     = get_observations_todo(pc.user, pc.password, pc.database, designs, server)
	observation      = 1
	
	## check latest observation
	if is_running(last_observation):
		observation = open(last_observation).readline().rstrip()
		if is_running(done):
			logging.info('Checking for a corresponding observation for: \033[35m' + observation)
			tokens_done = Token.Token_Handler( t_type=observation, srv=server, uname=pc.user, pwd=pc.password, dbn=pc.database) # load token of done observation
			observation = check_for_corresponding_observation(observations, observation, server, user, password, database, tokens_done)
			pass
		pass
	
	## check for new observations if necessary
	if not is_running(last_observation) or observation == 1:
	        logging.info('Looking for a new observation.')
		observation = find_new_observation(observations, observation, server, pc.user, pc.password, pc.database, working_directory)
		if observation == 1:
			logging.info('\033[0mNo new observations could be found. If database is not empty please check it for new or false tokens manually.')
			return 1
			pass
		pass

	## check whether a job has been already submitted
	if is_running(submitted): 
		logging.info('\033[0mA pipeline has already been submitted.')
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

	## reserve following processes
	logging.info('Selected observation: \033[35m' + observation)
	observation_file = open(last_observation, 'w')
	observation_file.write(observation)
	observation_file.close()
	
	## load token of chosen design document
	tokens = Token.Token_Handler( t_type=observation, srv=server, uname=pc.user, pwd=pc.password, dbn=pc.database) # load token of certain type
	
	## check for new data sets and get information about other tokens present
	list_locked = tokens.list_tokens_from_view('locked') # check which tokens of certain type are in the locked state
	list_error  = tokens.list_tokens_from_view('error')  # check which tokens of certain type show errors
	list_done   = tokens.list_tokens_from_view('done')   # check which tokens of certain type are done
	list_todos  = tokens.list_tokens_from_view('todo')   # check which tokens of certain type are in the todo state
	
	## check which pipelines are locked, done or show errors
	try:
		locked_pipelines = get_pipelines(tokens, list_locked)
		bad_pipelines    = get_pipelines(tokens, list_error)
		pipelines_done   = get_pipelines(tokens, list_done)
		pipelines_todo   = get_pipelines(tokens, list_todos)
	except TypeError:
		logging.error('\033[31mCould not find a corresponding token for the last observation \033[35m' + observation + '\033[31m. Please check the database for errors or remove the last observation.')
		return 1
		pass
	
	## lock program
	subprocess.Popen(['touch', lock_file])

	## add views for users
	tokens.add_view(v_name='downloading', cond=' doc.status == "downloading" ')
	tokens.add_view(v_name='unpacking', cond=' doc.status == "unpacking" ')
	tokens.add_view(v_name='unpacked', cond=' doc.status == "unpacked" ')
	tokens.add_view(v_name='submitted', cond=' doc.status == "submitted" ')
	tokens.add_view(v_name='processing', cond=' doc.status == "processing" ')
	tokens.add_view(v_name='locked', cond=' doc.lock > 0 ')
	
	## check pipelines to run
	pipelines = list(reversed(list(set(locked_pipelines) - set(pipelines_done) - set(pipelines_todo))))

	## check what to download
	if len(list_todos) > 0 and len(list_done) == 0 and len(pipelines) == 0:
		download_data(tokens, list_todos, pipelines_todo[0], working_directory)
		pass

	## update pipelines to run
	list_locked = tokens.list_tokens_from_view('locked') # check which tokens of certain type are in the locked state
	list_todos  = tokens.list_tokens_from_view('todo')   # check which tokens of certain type are in the todo state
	locked_pipelines = get_pipelines(tokens, list_locked)
	pipelines_todo   = get_pipelines(tokens, list_todos)
	pipelines        = list(reversed(list(set(locked_pipelines) - set(pipelines_done) - set(pipelines_todo))))
	
	## check errors of the pipelines
	if len(bad_pipelines) != 0:
		logging.warning('\033[33mPipeline(s) \033[35m' + str(bad_pipelines) + '\033[33m show errors. Please check their token status. Script will try to rerun them.')
		pass
	
	## check all finished pipelines
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
				list_done = tokens.list_tokens_from_view(pipelines_done[-1])
				for i, item in enumerate(list_done):
					if i == 0:
						submit_results(tokens, item['value'], observation, list_done, working_directory, upload)
						pass
					pass
				pass
			else:
				logging.info('\033[0mObservation \033[35m' + observation + '\033[0m is done.')
				subprocess.Popen(['touch', done])
				pass
			pass
		pass
	
	## main pipeline loop
	for pipeline in pipelines:
		tokens.add_view(v_name=pipeline, cond=' doc.pipeline == "' + pipeline + '" ')
		list_pipeline = tokens.list_tokens_from_view(pipeline)  ## get the pipeline list
		status = pipeline_status(tokens, list_pipeline)
		output = pipeline_output(tokens, list_pipeline)
		if len(status) > 1:
			logging.error('\033[31mPipeline \033[35m' + pipeline + '\033[31m shows more than one status: \033[35m' + str(status) + '\033[31m. Script will not proceed.')
			for item in list_pipeline:
				set_token_status(tokens, item['value'], 'error')
				pass
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
			run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline)
			break
			pass
		elif -1 in output:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be resumed.')
			run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline)
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
    