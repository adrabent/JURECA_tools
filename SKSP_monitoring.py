#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Surveys KSP monitoring script -- see https://github.com/adrabent/JURECA_tools
"""


import os, sys
import logging
import resource
import optparse
import glob
import shutil
import math
import random

import time, datetime
import subprocess
import filecmp
import multiprocessing

from GRID_LRT.get_picas_credentials import picas_cred
from GRID_LRT import Token
from GRID_LRT.Staging import srmlist

from GRID_LRT.couchdb.client import Server


_version = '1.0'                               ## program version
nodes = 24                                     ## number of JUWELS nodes (higher number leads to a longer queueing time)
walltime = '01:30:00'                          ## walltime for the JUWELS queue
mail = 'alex@tls-tautenburg.de'                ## notification email address
IONEX_server = 'ftp://ftp.aiub.unibe.ch/CODE/' ## URL for CODE downloads
num_SBs_per_group_var = 10                     ## chunk size 
max_dppp_threads_var = 19                      ## maximal threads per node per DPPP instance
max_proc_per_node_limit_var = 5                ## maximal processes per node for DPPP
num_proc_per_node_var = 10                     ## maximal processes per node for others
error_tolerance = 3                            ## number of failed tokens still acceptable for running pipelines
condition = 'targ'                             ## condition for the pipeline in order to be idenitified as new observations (usually the target pipeline)
force_process = 'cal'                          ## if tokens only of that type exist, enforce processing
final_pipeline = 'pref_targ2'                  ## name of final pipeline
calibrator_results = 'pref_cal2'               ## name of pipeline where calibrator results might have been stored
min_staging_fraction = 0.5                     ## only process fields with this minimum fraction of staged data


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


def my_handler(type, value, tb):
	exception = logger.critical("{0}".format(str(value)))
	lock = os.environ['SCRATCH_chtb00'] + '/htb006' '/.lock'
	if os.path.exists(lock):
		os.remove(lock)
		pass
	time.sleep(300)
	pass

def is_running(lock_file):
	if os.path.isfile(lock_file):
		return True
		pass
	else:
		return False
		pass
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
		try:
			if len(list_todos) > 0:
				observations.append(design)
				pass
		except TypeError:
			logging.warning('\033[33mObservation \033[35m' + design + '\033[33m is invalid.')
			continue
			pass
		pass
	
	return observations
	pass


def get_observation_id(tokens, list_todos):
	
	obsids = []
	
	for item in list_todos:
		token = tokens.db[item['key']]
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


def get_cal_observation_id(tokens, list_todos):
	
	obsids = []
	
	for item in list_todos:
		token = tokens.db[item['key']]
		obsid = token['CAL_OBSID']
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

def check_for_corresponding_calibration_results(tokens, list_pipeline, cal_obsid, working_directory):
    
	for item in list_pipeline:
		token = tokens.db[item['key']]
		break
		pass
    
	results_dir = '/'.join(token['RESULTS_DIR'].split('/')[:-2]) + '/' + calibrator_results + '/' + cal_obsid
	results_fn  = results_dir + '/' + cal_obsid + '.tar'
	existence = subprocess.Popen(['uberftp', '-ls', results_fn])
	errorcode = existence.wait()
	if errorcode == 0:
		logging.info('Cleaning working directory.')
		shutil.rmtree(working_directory, ignore_errors = True)
		os.mkdir(working_directory)
		logging.info('Transferring calibrator results for this field from: \033[35m' + results_fn)
		filename = working_directory + '/' + cal_obsid + '.tar'
		transfer  = subprocess.Popen(['globus-url-copy', results_fn, 'file:' + filename], stdout=subprocess.PIPE)
		errorcode = transfer.wait()
		if errorcode != 0:
			logging.error('\033[31m Downloading calibrator results have failed.')
			for item in list_pipeline: 
				set_token_status(tokens, item['key'], 'error')
				set_token_output(tokens, item['key'], 23)
				set_token_progress(tokens, item['key'], 'Download of calibrator results failed, error code: ' + str(errorcode))
				pass
			return False
			pass
		else:
			os.chdir(working_directory)
			logging.info('Unpacking calibrator results from: \033[35m' + filename)
			unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
			errorcode = unpack.wait()
			if errorcode != 0:
				logging.error('\033[31m Unpacking calibrator results have failed.')
				for item in list_pipeline: 
					set_token_status(tokens, item['key'], 'error')
					set_token_output(tokens, item['key'], 23)
					set_token_progress(tokens, item['key'], 'Unpacking of calibrator results failed, error code: ' + str(errorcode))
					pass
				return False
				pass
			os.remove(filename)
			logging.info('File \033[35m' + filename + '\033[32m was removed.')
			return True
			pass
	else:
		logging.warning('Could not find any calibrator results for this field in: \033[35m' + results_fn)
		output = pipeline_output(tokens, list_pipeline)
		if not 23 in output:
			for item in list_pipeline: 
				set_token_status(tokens, item['key'], 'error')
				set_token_output(tokens, item['key'], 23)
				set_token_progress(tokens, item['key'], 'No calibrator solutions or data found.')
				pass
			pass
		return False
		pass
	pass

def check_for_corresponding_pipelines(tokens, pipeline, pipelines_todo, working_directory):
    
	obsid_list = []
	
	for pipeline_todo in pipelines_todo:
		tokens.add_view(view_name=pipeline_todo, cond=' doc.PIPELINE_STEP == "' + pipeline_todo + '" ')
		if pipeline == pipeline_todo:
			list_pipeline = tokens.list_tokens_from_view(pipeline_todo)
			cal_obsid = get_cal_observation_id(tokens, list_pipeline)
			output = pipeline_output(tokens, list_pipeline)
			pass
		else:
			list_pipeline = tokens.list_tokens_from_view(pipeline_todo)
			obsid_list.append(get_observation_id(tokens, list_pipeline))
			output = pipeline_output(tokens, list_pipeline)
			pass
		pass
            
	if -15 in output:
		logging.info('Resuming observation.')
		return True
		pass

	obsid = list(set(obsid_list))
        
	if len(obsid) == 0:
		logging.warning('\033[33mCould not find the following calibrator observation: \033[35m' + cal_obsid)
		return check_for_corresponding_calibration_results(tokens, list_pipeline, cal_obsid, working_directory)
		pass
	elif len(obsid) > 1 or obsid[0] != cal_obsid:
		logging.warning('\033[33mNo corresponding target pipeline found for: \033[35m' + obsid[0])
		return False
		pass

	logging.info('Cleaning working directory.')
	shutil.rmtree(working_directory, ignore_errors = True)
	
	return True
	pass
	
	
def get_pipelines(tokens, list_locked):
	
	pipelines = []
	
	for item in list_locked:
		token = tokens.db[item['key']]
		pipeline = token['PIPELINE_STEP']
		if pipeline not in pipelines:
			pipelines.append(pipeline)
			pass
		pass
      
	return pipelines
	pass
      
      
def find_new_observation(observations, observation_done, server, user, password, database, working_directory):

	staged_dict = {}
        
	for observation in observations:
		if observation == observation_done:
			continue
			pass
		logging.info('Checking staging status of files in observation: \033[35m' + observation)
		tokens         = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) ## load token of certain type
		list_todos     = tokens.list_tokens_from_view('todo')                                                         ## load all todo tokens        
		srm_list = []
		for item in list_todos:
			try:
				srm       = tokens.db.get_attachment(item['key'], 'srm.txt').read().strip()
				srm_list.append(srm)
				pass
			except AttributeError:
				logging.warning('Invalid download URL in: \033[35m' + str(item['key']))
				pass
			pass
		pool = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 2)
		is_staged_list   = pool.map(is_staged, srm_list)
		staged_files     = sum(is_staged_list)
		staging_fraction = staged_files / float(len(list_todos))
		logging.info('The staging status is: \033[35m' + str(100 * staging_fraction) + '%')
		if str(staging_fraction) in staged_dict.keys():
			staged_dict[str(staging_fraction + random.randint(1,100)/100000.)] = observation
			pass
		else:
			staged_dict[str(staging_fraction)] = observation
			pass
		pass
    
	observation_keys = [float(i) for i in staged_dict.keys()]
	observation_keys = list(reversed(sorted(observation_keys)))

	if len(observation_keys) == 0:
		return 1
		pass
    
	for observation_key in observation_keys:
		if observation_key < min_staging_fraction:
			logging.info('Waiting for data being staged...')
			time.sleep(3600)
			return 1
			pass
		observation = staged_dict[str(observation_key)]
		logging.info('Checking observation: \033[35m' + observation)
		tokens      = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) ## load token of certain type
		list_todos  = tokens.list_tokens_from_view('todo')                                                         ## load all todo tokens        
		try:
			pipelines_todo = get_pipelines(tokens, list_todos)  # check whether there are also todo pipelines
			pass
		except KeyError:
			logging.warning('Observation: \033[35m' + observation + '\033[33m is invalid.')
			continue
			pass
		valid = True
		for pipeline in pipelines_todo:       
			if condition in pipeline:
				check_passed = check_for_corresponding_pipelines(tokens, pipeline, pipelines_todo, working_directory)
				if check_passed:   # it is a valid observation
					return observation
					pass
				else:
					valid = False
					pass
				pass
			elif not force_process in pipeline:
				valid = False
				pass
			elif len(pipelines_todo) < 2:
				valid = False
				pass
			pass
		if valid:
			logging.warning('Observation: \033[35m' + observation + '\033[33m does not show a target pipeline.')
			return observation
			pass
		logging.warning('Observation: \033[35m' + observation + '\033[33m does not show a valid pipeline.')
		pass

	return observation_done

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
    
def token_progress(tokens, token_value):
	
	token = tokens.db[token_value]
	progress = str(token['progress'])
	return progress
	pass


def is_staged(url):
	try:
		if 'ONLINE_AND_NEARLINE' in subprocess.check_output(['srmls', '-l', url]):
			return True
			pass
		else:
			return False
			pass
	except:
		return False
		pass
	pass


def transfer_data(tokens, token_value, filename, transfer_fn, working_directory):
  
	logging.info('\033[35m' + filename + '\033[32m is now transfered to: \033[35m' + transfer_fn)
	set_token_status(tokens, token_value, 'transferring')
	set_token_output(tokens, token_value, 0)
	set_token_progress(tokens, token_value, 'Transfer of data to: ' + transfer_fn)
	existence = subprocess.Popen(['uberftp', '-ls', transfer_fn])
	errorcode = existence.wait()
	if errorcode == 0:
		subprocess.Popen(['uberftp','-rm', transfer_fn])
		pass
	transfer  = subprocess.Popen(['globus-url-copy', 'file:' + filename, transfer_fn], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	
	if errorcode == 0:
		logging.info('File \033[35m' + filename + '\033[32m was transferred.')
		set_token_status(tokens, token_value, 'transferred')
		set_token_output(tokens, token_value, 0)
		lock_token_done(tokens, token_value)
		set_token_progress(tokens, token_value, 'Transfer of data has been finished.')
		return True
		pass
	else:
		logging.error('\033[31mTransfer of \033[35m' + filename + '\033[31m failed. Error code: \033[35m' + str(errorcode))
		set_token_status(tokens, token_value, 'error')
		set_token_output(tokens, token_value, 31)
		set_token_progress(tokens, token_value, 'Transfer of ' + filename.split('/')[-1] + ' failed.')
		return False
		pass
	
	return False
	pass


def pack_data(tokens, token_value, filename, to_pack, transfer_fn, pack_directory, working_directory):
  
	os.chdir(pack_directory)
	logging.info('Packing file: \033[35m' + filename)
	set_token_status(tokens, token_value, 'packing')
	set_token_progress(tokens, token_value, 'Packing file(s): ' + to_pack)
	to_pack_list = to_pack.split(' ')
	if len(to_pack_list) > 1:
		pack = subprocess.Popen(['tar', 'cfv', filename, to_pack_list[0].replace(pack_directory,'')], stdout=subprocess.PIPE)
		pack.wait()
		for to_pack in to_pack_list[1:]:
			pack = subprocess.Popen(['tar', 'rfv', filename, to_pack.replace(pack_directory,'')], stdout=subprocess.PIPE)
			errorcode = pack.wait()
			pass
	else:
		pack = subprocess.Popen(['tar', 'cfvz', filename, to_pack.replace(pack_directory,'')], stdout=subprocess.PIPE)
		errorcode = pack.wait()
		pass
	      
	if errorcode == 0:
		set_token_status(tokens, token_value, 'packed')
		set_token_output(tokens, token_value, 0)
		logging.info('Packing of \033[35m' + filename + '\033[32m finished.')
		transfer_successful = transfer_data(tokens, token_value, filename, transfer_fn, working_directory)
		if transfer_successful:
			if 'prep_cal' in to_pack_list[0]:
				if os.path.exists(working_directory + '/' + to_pack_list[0].split('prep_cal')[0].split('/')[-1]+ 'prep_cal'):
					shutil.rmtree(working_directory + '/' + to_pack_list[0].split('prep_cal')[0].split('/')[-1]+ 'prep_cal', ignore_errors = True)
					pass
				shutil.move(to_pack_list[0].split('prep_cal')[0] + 'prep_cal', working_directory + '/.')
				pass
			if 'pre-cal' in to_pack_list[0]:
				if os.path.exists(working_directory + '/' + to_pack_list[0].split('/')[-1]):
					shutil.rmtree(working_directory + '/' + to_pack_list[0].split('/')[-1], ignore_errors = True)
					pass
				if 'MHz' in to_pack_list[0]:
					shutil.rmtree(to_pack_list[0])
					pass
				else:
					shutil.move(to_pack_list[0], working_directory + '/.')
					pass
				pass
			pass
		pass
	else:
		logging.error('\033[31mPacking failed, error code: ' + str(errorcode))
		set_token_status(tokens, token_value, 'error')
		set_token_output(tokens, token_value, 31)
		set_token_progress(tokens, token_value, 'Packing failed, error code: ' + str(errorcode))
		pass
	
	pass
      
      
def unpack_data(tokens, token_value, filename, working_directory):
  
	os.chdir(working_directory)
	set_token_status(tokens, token_value, 'unpacking')
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode == 0:
		set_token_status(tokens, token_value, 'unpacked')
		set_token_progress(tokens, token_value, 0)
		token = tokens.db[token_value]
		if 'ABN' in token.keys():
			freq = os.popen('taql "select distinct REF_FREQUENCY from ' + filename.split('.MS')[0] + '.MS' + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
			update_freq(tokens, token_value, freq)
			pass
		os.remove(filename)
		logging.info('File \033[35m' + filename + '\033[32m was removed.')
		pass
	else:
		logging.error('\033[31mUnpacking failed, error code: ' + str(errorcode))
		set_token_status(tokens, token_value, 'error')
		set_token_output(tokens, token_value, 20)
		set_token_progress(tokens, token_value, 'Unpacking failed, error code: ' + str(errorcode))
		pass
	pass


def prepare_downloads(tokens, list_todos, pipeline_todownload, working_directory):
  
	download_list    = srmlist.srmlist() # create list to download
	list_todownload  = [item for item in list_todos if tokens.db[item['key']]['PIPELINE_STEP'] == pipeline_todownload] # filter list to a certain pipeline
	srm_list         = []
	list_todownload2 = []
	
	for item in list_todownload:
		lock_token(tokens, item['key'])
		try:
			srm       = tokens.db.get_attachment(item['key'], 'srm.txt').read().strip()
			token     = tokens.db[item['key']]
			obsid     = str(token['OBSID'])
			startsb   = str(token['STARTSB'])
			filename  = working_directory + '/' + obsid + '_SB' + startsb + '_uv.MS'
			filename2 = working_directory + '/' + obsid + '_SB' + startsb + '_uv.dppp.MS'
			if os.path.exists(filename) or os.path.exists(filename2):
				logging.warning('\033[33mFile \033[35m' + srm + '\033[33m is already on disk.')
				set_token_status(tokens, item['key'], 'unpacked')
				set_token_output(tokens, item['key'], 0)
				if 'ABN' in token.keys():
					try:
						freq = os.popen('taql "select distinct REF_FREQUENCY from ' + filename + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
						pass
					except IndexError:
						try:
							freq = os.popen('taql "select distinct REF_FREQUENCY from ' + filename2 + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
							pass
						except IndexError:
							logging.warning('\033[33mFile \033[35m' + filename + '\033[33m is probably broken and will be removed.')
							shutil.rmtree(filename, ignore_errors = True)
							shutil.rmtree(filename2, ignore_errors = True)
							srm_list.append(srm)
							list_todownload2.append(item)
							continue
							pass
						pass
					update_freq(tokens, item['key'], freq)
					pass
				continue
				pass
			srm_list.append(srm)
			list_todownload2.append(item)
		except AttributeError:
			logging.warning('\033[33mToken \033[35m' + item['key'] + '\033[33m has no valid download URL.')
			set_token_progress(tokens, item['key'], 'No valid download URL')
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], 20)
			continue
			pass
		pass

	## check whether data is staged
	logging.info('Checking staging status of files in pipeline: \033[35m' + pipeline_todownload)
	pool = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 2)
	is_staged_list = pool.map(is_staged, srm_list)
	
	list_todownload = []
	
	for i, item in enumerate(list_todownload2):
		srm = tokens.db.get_attachment(item['key'], 'srm.txt').read().strip()
		if not is_staged_list[i]:
			logging.warning('\033[33mFile \033[35m' + srm + '\033[33m has not been staged yet.')
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], 22)
			set_token_progress(tokens, item['key'], 'File is not staged.')
			continue
			pass
      
		logging.info('File \033[35m' + srm + '\033[32m is properly staged.')
		download_list.append(srm)
		list_todownload.append(item)
		pass
	
	return (list_todownload, download_list)
	pass
     
     
def download_data(url, token_value, working_directory, observation, server, user, password, database):
  
	tokens = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) # load token of done observation
	set_token_status(tokens, token_value, 'downloading')
	filename = working_directory + '/' + url.split('/')[-1]
	download = subprocess.Popen(['globus-url-copy',  url, 'file:' + filename], stdout=subprocess.PIPE)
	errorcode = download.wait()
	
	if errorcode == 0:
		set_token_status(tokens, token_value, 'downloaded')
		set_token_output(tokens, token_value, 0)
		set_token_progress(tokens, token_value, 0)
		unpack_data(tokens, token_value, filename, working_directory)
		pass
	else:
		logging.error('Download failed, error code: ' + str(int(errorcode)))
		set_token_status(tokens, token_value, 'error')
		set_token_output(tokens, token_value, 21)
		set_token_progress(tokens, token_value, 'Download failed, error code: ' + str(int(errorcode)))
		pass
		      
	return 0
	pass
   
   
def create_submission_script(submit_job, parset, working_directory, submitted):
	
	home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'
	
	if os.path.isfile(submit_job):
		logging.warning('\033[33mFile for submission already exists. It will be overwritten.')
		os.remove(submit_job)
		pass
	
	jobfile = open(submit_job, 'w')
	
	## writing file header
	jobfile.write('#!/usr/bin/env sh\n')
	
	## extracting directories for IONEX and the TGSS ADR skymodel
	try:
		IONEX_script         = os.popen('find ' + working_directory + ' -name download_IONEX.py ').readlines()[0].rstrip('\n').replace(' ','')
		IONEX_path           = os.popen('grep ionex_path '           + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		jobfile.write(IONEX_script + ' --destination ' + IONEX_path + ' --server ' + IONEX_server + ' ' + working_directory + '/' + target_input_pattern + '\n')
		pass
	except IndexError:
		pass
	try:
		skymodel_script      = os.popen('find ' + working_directory + ' -name download_tgss_skymodel_target.py').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		target_skymodel      = os.popen('grep target_skymodel '      + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$SCRATCH_chtb00/htb006', working_directory)
		if os.path.exists(target_skymodel):
			os.remove(target_skymodel)
			pass
		jobfile.write(skymodel_script + ' ' + working_directory + '/' + target_input_pattern + ' ' + target_skymodel + '\n')
		pass
	except IndexError:
		pass
	
	## write-up of final command
	jobfile.write('\n')
	jobfile.write('sbatch --nodes=' + str(nodes) + ' --partition=batch --mail-user=' + mail + ' --mail-type=ALL --time=' + walltime + ' --account=htb00 ' + home_directory + '/run_pipeline.sh ' + parset + ' ' + working_directory)
	jobfile.close()
	
	os.system('chmod +x ' + submit_job)
	os.rename(submit_job, submit_job + '.sh')
	subprocess.Popen(['touch', submitted])
	
	return 0
	pass
   
   
def run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, ftp, last_observation):
  
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
	SBXloc      = []
	results_dir = []
	for item in list_pipeline:
		token = tokens.db[item['key']]           ## checkout location for pipeline
		SBXloc.append(token['SBXloc'])             ## save sandbox location for pipeline
		results_dir.append(token['RESULTS_DIR'])   ## save results location for pipeline
		attachments = tokens.list_attachments(item['key'])
		parsets = [i for i in attachments if 'parset' in i]
		if len(parsets) != 1:
			logging.error('\033[31mMultiple or no parsets attached to: \033[35m' + item['key'])
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], 3)
			set_token_progress(tokens, item['key'], 'Multiple or no parsets attached')
			pass
  		if os.path.isfile(parset):
			tokens.get_attachment(item['key'], parsets[0], parset2)
			if not filecmp.cmp(parset, parset2):
				logging.error('\033[31mParset file mismatches for: \033[35m' + item['key'])
				set_token_status(tokens, item['key'], 'error')
				set_token_output(tokens, item['key'], 3)
				set_token_progress(tokens, item['key'], 'Parset file mismatch')
				return 1
				pass
			if len(list(set(SBXloc))) > 1:
				logging.error('\033[31mSBXloc mismatches for: \033[35m' + item['key'])
				set_token_status(tokens, item['key'], 'error')
				set_token_output(tokens, item['key'], -1)
				set_token_progress(tokens, item['key'], 'SBXloc mismatch')
				return 1
				pass
			os.remove(parset2)
			pass
		else:
			try:
				tokens.get_attachment(item['key'], parsets[0], parset)
			except IndexError:
				logging.error('\033[31mNo valid parsets attached to: \033[35m' + observation + '\033[31m. Please check the tokens manually. Processing will be skipped.')
				os.remove(last_observation)
				return 1
			pass
		pass
	
	#SBXloc      = ftp + str(list(set(SBXloc))[0])
	SBXloc      = str(list(set(SBXloc))[0])
	results_dir = str(list(set(results_dir))[0])

	## applying necessary changes to the parset
	num_proc_per_node       = os.popen('grep "! num_proc_per_node" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node_limit = os.popen('grep "! num_proc_per_node_limit" ' + parset).readlines()[0].rstrip('\n').replace('/','\/')
	max_dppp_threads        = os.popen('grep "! max_dppp_threads" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')
	losoto_executable       = os.popen('grep "! losoto_executable" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')

	os.system('sed -i "s/' + losoto_executable       + '/! losoto_executable       = \$LOFARROOT\/bin\/losoto/g " '                      + parset)
	os.system('sed -i "s/' + num_proc_per_node       + '/! num_proc_per_node       = ' + str(num_proc_per_node_var)       + '/g" '       + parset)
	os.system('sed -i "s/' + num_proc_per_node_limit + '/! num_proc_per_node_limit = ' + str(max_proc_per_node_limit_var) + '/g" '       + parset)
	os.system('sed -i "s/' + max_dppp_threads        + '/! max_dppp_threads        = ' + str(max_dppp_threads_var)        + '/g" '       + parset)
	
	os.system('sed -i "s/\/Input//g " '                    + parset)
	os.system('sed -i "s/PREFACTOR_SCRATCH_DIR/\$SCRATCH_chtb00\/htb006/g" ' + parset)
	
	try:
		makesourcedb         = os.popen('grep "! makesourcedb" '         + parset).readlines()[0].rstrip('\n').replace('/','\/')
		flagging_strategy    = os.popen('grep "! flagging_strategy" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
		num_SBs_per_group    = os.popen('grep "! num_SBs_per_group" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
		
		os.system('sed -i "s/' + makesourcedb            + '/! makesourcedb            = \$LOFARROOT\/bin\/makesourcedb/g " '                + parset)
		os.system('sed -i "s/' + flagging_strategy       + '/! flagging_strategy       = \$LOFARROOT\/share\/rfistrategies\/HBAdefault/g " ' + parset)
		os.system('sed -i "s/' + num_SBs_per_group       + '/! num_SBs_per_group       = ' + str(num_SBs_per_group_var)       + '/g" '       + parset)
        
	except IndexError:
		pass

	## downloading prefactor
	sandbox = SBXloc
	filename = working_directory + '/prefactor.tar'
	logging.info('Downloading current prefactor version from \033[35m' + sandbox)
	download = subprocess.Popen(['globus-url-copy', sandbox , 'file:' + filename], stdout=subprocess.PIPE)
	errorcode = download.wait()
	if errorcode != 0:
		for item in list_pipeline: 
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], -1)
			set_token_progress(tokens, item['key'], 'Download of prefactor failed, error code: ' + str(errorcode))
			pass
		logging.error('\033[31m Downloading prefactor has failed.')
		time.sleep(600)
		return 1
		pass
	      
	logging.info('Unpacking current prefactor version to \033[35m' + working_directory)
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory])
	errorcode = unpack.wait()
	if errorcode != 0:
		for item in list_pipeline:
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], -1)
			set_token_progress(tokens, item['key'], 'Unpacking of prefactor failed, error code: ' + str(errorcode))
			pass
		logging.error('\033[31m Unpacking prefactor has failed.')
		return 1
		pass
	
	logging.info('Creating submission script in \033[35m' + submit_job)
	if is_running(submitted) or is_running(submit_job):
		logging.warning('Submission is already running')
		return 0
		pass
	create_submission_script(submit_job, parset, working_directory, submitted)
	
	if os.path.exists(working_directory + '/pipeline/statefile'):
		os.remove(working_directory + '/pipeline/statefile')
		logging.info('Statefile has been removed.')
		pass

	slurm_list = glob.glob(slurm_files)
	for slurm_file in slurm_list:
		os.remove(slurm_file)
		pass

	logging.info('\033[0mWaiting for submission\033[0;5m...')
	while os.path.exists(submit_job + '.sh'):
		time.sleep(5)
		pass
	for item in list_pipeline:
		set_token_status(tokens, item['key'], 'submitted')
		set_token_output(tokens, item['key'], 0)
		pass
	logging.info('Pipeline \033[35m' + pipeline + '\033[32m has been submitted.')
	
	return 0
	pass
	
	
def pipeline_status(tokens, list_pipeline):
	
	status = []
	for item in list_pipeline:
		status.append(token_status(tokens, item['key']))
		pass
	status = list(set(status))
	
	return status
	pass


def pipeline_output(tokens, list_pipeline):
	
	output = []
	for item in list_pipeline:
		output.append(token_output(tokens, item['key']))
		pass
	output = list(set(output))
	
	return output
	pass

def pipeline_progress(tokens, list_pipeline):
	
	progress = []
	for item in list_pipeline:
		progress.append(token_progress(tokens, item['key']))
		pass
	progress = list(set(progress))
	
	return progress
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
		logging.error(log_information)
		os.remove(submitted)
		return log_information
		pass
	if 'error:' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return log_information
		pass
	if 'termination' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return log_information
		pass
	log_information = os.popen('tail -8 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'error:' in log_information:
		logging.error(log_information)
		os.remove(submitted)
		return log_information
		pass
	log_information = os.popen('tail -6 ' + slurm_log).readlines()[0].rstrip('\n')
	if 'Error' in log_information:
		logging.error(log_information)
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


def submit_diagnostic_plots(tokens, token_value, images):
  
	attachments = tokens.list_attachments(token_value)
	old_images  = [i for i in attachments if '.png' in i]
	doc = tokens.db[token_value]
	for old_image in old_images:
		tokens.db.delete_attachment(doc, old_image)
		pass
	
	for image in images:
		logging.info('Upload inspection plot \033[35m' + image + '\033[32m to token \033[35m' + token_value)
		tokens.add_attachment(token_value, open(image,'r'), os.path.basename(image))
		os.remove(image)
		logging.info('File \033[35m' + image + '\033[32m was removed.')
		pass
	      
	return 0
	pass


def submit_error_log(tokens, list_pipeline, slurm_log, log_information, working_directory, last_observation):

	parset               = working_directory + '/pipeline.parset'
	inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$SCRATCH_chtb00/htb006', working_directory)

	for item in list_pipeline:
		token_value = item['key']
		break
		pass
	attachments = tokens.list_attachments(token_value)
	old_slurm_logs = [i for i in attachments if 'slurm' in i]
	doc = tokens.db[token_value]
	for old_slurm_log in old_slurm_logs:
		tokens.db.delete_attachment(doc, old_slurm_log)
		pass
	tokens.add_attachment(token_value, open(slurm_log,'r'), os.path.basename(slurm_log))
	
	skip = False
	if 'ERROR' in log_information:
		for item in list_pipeline:
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], 99)
			set_token_progress(tokens, item['key'], log_information[log_information.find('genericpipeline:'):])
			pass
		#time.sleep(57600)
		time.sleep(3600)
		skip = True
		pass
	if 'error:' in log_information:
		for item in list_pipeline:
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], 99)
			set_token_progress(tokens, item['key'], log_information[log_information.find('error:'):])
			pass
		#time.sleep(57600)
		time.sleep(3600)
		skip = True
		pass
	elif 'Error' in log_information:
		for item in list_pipeline:
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], 99)
			set_token_progress(tokens, item['key'], log_information[log_information.find('Error:'):])
			pass
		#time.sleep(57600)
		time.sleep(3600)
		skip = True
		pass
	elif 'termination' in log_information:
		for item in list_pipeline:
			set_token_status(tokens, item['key'], 'error')
			set_token_output(tokens, item['key'], 99)
			set_token_progress(tokens, item['key'], log_information[log_information.find('srun:'):])
			pass
		#time.sleep(57600)
		time.sleep(3600)
		skip = True
		pass
	elif 'finished' in log_information:
		for i, item in enumerate(list_pipeline):
			set_token_status(tokens, item['key'], 'processed')
			set_token_output(tokens, item['key'], 0)
			set_token_progress(tokens, item['key'], log_information[log_information.find('genericpipeline:'):])
			images  = sorted(glob.glob(inspection_directory + '/*.png'))
			images2 = sorted(glob.glob(working_directory + '/pipeline/*.png'))
			if i < len(list_pipeline) - 1:
				submit_diagnostic_plots(tokens, item['key'], images[:1])
				pass
			else:
				submit_diagnostic_plots(tokens, item['key'], images)
				pass
			for image in images2:
				os.remove(image)
				pass
			pass
                    
		if os.path.exists(working_directory + '/pipeline/statefile'):
			os.remove(working_directory + '/pipeline/statefile')
			logging.info('Statefile has been removed.')
			pass
		pass
	
	if skip:
		logging.error('Unresolved issue in the current processing of the observation occured. Processing will be cancelled.')
		os.remove(last_observation)
		pass
	
	return 0
	pass


def slice_dicts(srmdict, slice_size = 10):

	srmdict = dict(srmdict)

	keys  = sorted(srmdict.keys())
	start = int(keys[0] )
	end   = int(keys[-1])

	sliced={}
    
	for chunk in range(0, int(math.ceil((end - start) / float(slice_size)))):
		chunk_name = format(start + chunk * slice_size, '03')
		sliced[chunk_name] = []
		for i in range(slice_size):
			if format(start + chunk * slice_size + i,'03') in srmdict.keys():
				sliced[chunk_name].append(srmdict[format(start + chunk * slice_size + i, '03')])
				pass
			pass
		pass

	return sliced
	pass

def pack_and_transfer(token_value, filename, to_pack, pack_directory, transfer_fn, working_directory, observation, server, user, password, database):
  
	tokens = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) # load token of done observation
	pack_data(tokens, token_value, filename, to_pack, transfer_fn, pack_directory + '/', working_directory)
		
	pass
      
      
def submit_results(tokens, list_done, working_directory, observation, server, user, password, database, pipeline):

	parset               = working_directory + '/pipeline.parset'
	inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$SCRATCH_chtb00/htb006', working_directory)
	cal_values_directory = os.popen('grep cal_values_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$SCRATCH_chtb00/htb006', working_directory)
	calibration_h5       = glob.glob(inspection_directory + '/*.h5')
	calibration_npy      = glob.glob(cal_values_directory + '/*.npy')
	instrument_tables    = sorted(glob.glob(working_directory + '/pipeline/*cal/instrument'))
	antenna_tables       = sorted(glob.glob(working_directory + '/pipeline/*cal/ANTENNA'))
	field_tables         = sorted(glob.glob(working_directory + '/pipeline/*cal/FIELD'))
	
	try:
		results_directory = os.popen('grep "! results_directory" ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace('$SCRATCH_chtb00/htb006', working_directory).replace(' ','')
		results     = sorted(glob.glob(results_directory + '/*.ms'))
	except IndexError:
		results     = []
		pass
        
	tokens.add_view(view_name=final_pipeline, cond=' doc.PIPELINE_STEP == "' + final_pipeline + '" ')
        
	# upload calibration results
	if len(instrument_tables) > 0 and len(antenna_tables) > 0 and len(field_tables) > 0:
		tokens.delete_tokens(final_pipeline)
		for item, instrument_table, antenna_table, field_table in zip(list_done, instrument_tables, antenna_tables, field_tables):
			token        = tokens.db[item['key']]
			sbnumber     = str(token['STARTSB'])
			obsid        = str(token['OBSID'])
			transfer_dir = token['RESULTS_DIR'] + '/' + obsid
			subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
			to_pack      = instrument_table + ' ' + antenna_table + ' ' + field_table
			filename     = working_directory + '/instruments_' + obsid + '_' + sbnumber + '.tar'
			transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
			pack_and_transfer(item['key'], filename, to_pack, working_directory + '/pipeline', transfer_fn, working_directory, observation, server, user, password, database)
			pass
		pass
            
	elif len(results) > 0:
		tokens.add_view(view_name='temp2', cond=' doc.PIPELINE_STEP == "' + pipeline + '" && doc.done > 0')   ## select only tokens of pipeline which are done
		list_pipeline_done = tokens.list_tokens_from_view('temp2')                                    ## get the pipeline list
		if pipeline != final_pipeline and len(list_pipeline_done) == 0:
			list_final_pipeline = tokens.list_tokens_from_view(final_pipeline)                    ## get the final pipeline list
			if len(list_final_pipeline) > 0:
				tokens.delete_tokens(final_pipeline)
				pass
			logging.info('Tokens for the final pipeline \033[35m' + final_pipeline + '\033[32m are being created')
			home_directory = os.environ['PROJECT_chtb00'] + '/htb006'
			final_config   = home_directory + '/' + final_pipeline + '.cfg'
			final_parset   = home_directory + '/' + final_pipeline + '.parset'
			ts             = Token.TokenSet(tokens, tok_config = final_config)
			s_list         = {}
			obsid          = get_observation_id(tokens, list_done)
			for item in list_done:
				token = tokens.db[item['key']]
				srm = tokens.db.get_attachment(item['key'], 'srm.txt').read().strip()
				ABN   = str(token['ABN'])
				if ABN == '':
					filename = working_directory + '/' + srm.split('/')[-1]
					freq = os.popen('taql "select distinct REF_FREQUENCY from ' + filename.split('.MS')[0] + '.MS' + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
					update_freq(tokens, item['key'], freq)
					token = tokens.db[item['key']]
					ABN = str(token['ABN'])
					pass
				s_list[ABN] = srm
				pass
			ABNs = slice_dicts(s_list, slice_size = num_SBs_per_group_var)
			ts.create_dict_tokens(iterable = ABNs, id_prefix = 'ABN', id_append = final_pipeline, key_name = 'ABN', file_upload = 'srm.txt')
			list_final_pipeline = tokens.list_tokens_from_view(final_pipeline)  ## get the final pipeline list again
			for item in list_final_pipeline:
				token = tokens.db[item['key']]
				token['OBSID'] = obsid
				tokens.db.update([token])
				target_input_pattern = os.popen('grep "! target_input_pattern" ' + final_parset).readlines()[0].rstrip('\n').replace('/','\/')
				os.system('sed -i "s/' + target_input_pattern + '/! target_input_pattern = ' + obsid + '\*.ms/g " ' + final_parset)
				tokens.add_attachment(item.id, open(final_parset, 'rb'), final_parset.split('/')[-1])
				lock_token(tokens, item['key'])
				os.system('sed -i "s/! target_input_pattern = ' + obsid + '\*.ms/' + target_input_pattern + '/g " ' + final_parset)
				pass
			for item, result in zip(list_done, results):
				if os.path.exists(working_directory + '/' + result.split('/')[-1]):
					shutil.rmtree(working_directory + '/' + result.split('/')[-1])
					pass
				shutil.move(result, working_directory + '/.')
				set_token_output(tokens, item['key'], 0)
				lock_token_done(tokens, item['key'])
				pass
			pass
		elif pipeline == final_pipeline:
			list_done_todo = []  ## in case a download failes, repeat only those files 
			results_todo   = []  ## in case a download failes, repeat only those files 
			for result, item in zip(results, list_done):
				status = token_status(tokens, item['key'])
				if status != 'transferred':
					list_done_todo.append(item)
					results_todo.append(result)
					pass
				pass
			pool2 = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 2)
			for result, item in zip(results_todo, list_done_todo):
				token        = tokens.db[item['key']]
				obsid        = str(token['OBSID'])
				ABN          = str(token['ABN'])
				transfer_dir = token['RESULTS_DIR'] + '/' + obsid
				to_pack      = result
				subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
				filename     = working_directory + '/GSM_CAL_' + obsid + '_ABN_' + str(ABN) + '.tar.gz'
				transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
				pool2.apply_async(pack_and_transfer, args = (item['key'], filename, to_pack, working_directory, transfer_fn, working_directory, observation, server, user, password, database,))
				pass
			pool2.close()
			pool2.join()
			pass
		pass
    
	elif len(calibration_h5) == 1 and len(calibration_npy) > 0 and condition not in pipeline:
		h5parms = glob.glob(working_directory + '/pipeline/*.h5')
		for h5parm in h5parms:
			os.remove(h5parm)
			pass
		tokens.delete_tokens(final_pipeline)
		for item in list_done:
			token = tokens.db[item['key']]
			break
			pass
		obsid        = str(token['OBSID'])
		transfer_dir = token['RESULTS_DIR'] + '/' + obsid
		to_pack      = calibration_h5[0] + ' ' + ' '.join(calibration_npy)
		subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
		filename     = working_directory + '/' + obsid + '.tar'
		transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
		pack_and_transfer(item['key'], filename, to_pack, working_directory, transfer_fn, working_directory, observation, server, user, password, database)
		pass
	
	logging.info('Submitting results for \033[35m' + pipeline + '\033[32m has been finished.')
	return 0
	pass


def main(server='https://picas-lofar.grid.surfsara.nl:6984', ftp='gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/sandbox/', recursive = False):
	
	## load working environment
	working_directory = os.environ['SCRATCH_chtb00'] + '/htb006'
	home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'
	lock_file         = working_directory + '/.lock'
	submitted         = working_directory + '/.submitted'
	done              = working_directory + '/.done'
	last_observation  = working_directory + '/.observation'
	slurm_files       = home_directory    + '/slurm-*.out'
	log_information   = ''
	logging.info('\033[0mWorking directory is ' + working_directory)
	
	## check whether an instance of this program is already running
	if is_running(lock_file) and not recursive:
		logging.error('\033[31mAn instance of this program appears to be still running. If not, please remove the lock file: \033[0m' + lock_file)
		time.sleep(3600)
		os.remove(lock_file)
		return 1
		pass
	           
	## load PiCaS credentials and connect to server
	logging.info('\033[0mConnecting to server: ' + server)
	couchdb_server = Server(server)
	pc = picas_cred()
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
		pass
	
	## check for new observations if necessary
	if not is_running(last_observation) or is_running(done):
	        logging.info('Looking for a new observation.')
		observation = find_new_observation(observations, observation, server, pc.user, pc.password, pc.database, working_directory)
		if observation == 1:
			logging.info('\033[0mNo new observations could be found. If database is not empty please check it for new or false tokens manually.')
			time.sleep(300)
			return 1
			pass
		pass

	## remove old staging file
	#if is_running(working_directory + '/.' + observation):
		#os.remove(working_directory + '/.' + observation)
		#pass
	
	## reserve following processes
	logging.info('Selected observation: \033[35m' + observation)
	observation_file = open(last_observation, 'w')
	observation_file.write(observation)
	observation_file.close()
	
	## load token of chosen design document
	tokens = Token.Token_Handler( t_type=observation, srv=server, uname=pc.user, pwd=pc.password, dbn=pc.database) # load token of certain type
	
	## add views for usersSTvalue
	tokens.add_view(view_name='downloading', cond=' doc.status == "downloading" ')
	tokens.add_view(view_name='unpacking',   cond=' doc.status == "unpacking" '  )
	tokens.add_view(view_name='unpacked',    cond=' doc.status == "unpacked" '   )
	tokens.add_view(view_name='submitted',   cond=' doc.status == "submitted" '  )
	tokens.add_view(view_name='processing',  cond=' doc.status == "processing" ' )
	tokens.add_view(view_name='processed',   cond=' doc.status == "processed" '  )
	tokens.add_view(view_name='packing',     cond=' doc.status == "packing" '    )
	tokens.add_view(view_name='transferring', cond=' doc.status == "transferring" ')
	tokens.add_view(view_name='transferred',  cond=' doc.status == "transferred" ')
	tokens.add_view(view_name='done', cond=' doc.done > 0  && doc.output == 0')	
	tokens.add_view(view_name='overview_total', cond=' doc.lock > 0  || doc.lock == 0')
	
	## check for new data sets and get information about other tokens present
	list_error     = tokens.list_tokens_from_view('error')     # check which tokens of certain type show errors
	list_locked    = tokens.list_tokens_from_view('locked')    # check which tokens of certain type are in the locked state
	list_done      = tokens.list_tokens_from_view('done')      # check which tokens of certain type are done
	list_todos     = tokens.list_tokens_from_view('todo')      # check which tokens of certain type are in the todo state
	
	## check which pipelines are locked, done or show errors
	try:
		locked_pipelines    = sorted(list(set(get_pipelines(tokens, list_locked))))
		bad_pipelines       = sorted(list(set(get_pipelines(tokens, list_error ))))
		pipelines_done      = sorted(list(set(get_pipelines(tokens, list_done  ))))
		pipelines_todo      = sorted(list(set(get_pipelines(tokens, list_todos ))))
	except TypeError:
		logging.error('\033[31mCould not find a corresponding token for the last observation \033[35m' + observation + '\033[31m. Please check the database for errors.')# Script will check for new observations in the next run.')
                time.sleep(3600)
		os.remove(last_observation)
		return 1
		pass
	
	## lock program
	subprocess.Popen(['touch', lock_file])
	
	## check pipelines to run
	pipelines = sorted(list(set(locked_pipelines) - set(pipelines_todo)))

	## check what to download
	if len(list_todos) > 0 and not recursive:
		if os.path.exists(done):
			os.remove(done)
			pass
		(list_todownload, download_list) = prepare_downloads(tokens, list_todos, pipelines_todo[0], working_directory)
		gsilist   = download_list.gsi_links() # convert the srm list to a GSI list (proper URLs for GRID download)
		gsilist   = sorted(list(set(gsilist))) # to re-reverse the list in order to match it for the upcoming loop and use only distinct files
		pool      = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 2)
		for i, (url, item) in enumerate(zip(gsilist, list_todownload)):
			pool.apply_async(download_data, args = (url, item['key'], working_directory, observation, server, pc.user, pc.password, pc.database,))
			if (i + 1) % multiprocessing.cpu_count() == 0:
				pool.apply_async(main, args = (server, ftp, True,))
				pass
			pass
		pool.close()
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
		pass

	## check which pipelines are done and if observation is finished
	if len(pipelines_done) > 0 and not recursive:
		logging.info('\033[0mPipeline(s) \033[35m' + str(pipelines_done) + '\033[0m are done.')
		if (set(pipelines) < set(pipelines_done)) and (len(pipelines_todo) == 0) and (final_pipeline not in bad_pipelines):
			logging.info('\033[0mObservation \033[35m' + observation + '\033[0m is done.')
			tokens.del_view(view_name='downloading')
			tokens.del_view(view_name='unpacking')
			tokens.del_view(view_name='unpacked')
			tokens.del_view(view_name='submitted')
			tokens.del_view(view_name='processing')
			tokens.del_view(view_name='processed')
			tokens.del_view(view_name='packing')
			tokens.del_view(view_name='transferring')
			tokens.del_view(view_name='transferred')
			subprocess.Popen(['touch', done])
			pass
		pass
	
	## main pipeline loop
	for pipeline in pipelines:
		tokens.add_view(view_name=pipeline, cond=' doc.PIPELINE_STEP == "' + pipeline + '" ')                                         ## select all tokens of this pipeline
		tokens.add_view(view_name='temp',   cond=' doc.PIPELINE_STEP == "' + pipeline + '" && (doc.output < 20 |  doc.output > 22)')  ## select only tokens without download/upload error
		tokens.add_view(view_name='temp2',  cond=' doc.PIPELINE_STEP == "' + pipeline + '" && (doc.output > 19 && doc.output < 23)')  ## select only tokens with    download/upload error
		list_pipeline_all      = tokens.list_tokens_from_view(pipeline)  ## get the pipeline list
		list_pipeline          = tokens.list_tokens_from_view('temp')    ## get the pipeline list without download errors
		list_pipeline_download = tokens.list_tokens_from_view('temp2')   ## get the pipeline list with    download errors
		list_observation       = tokens.list_tokens_from_view('overview_total') ## get the list of the entire observation
		status = pipeline_status(tokens, list_pipeline_all)
		output = pipeline_output(tokens, list_pipeline_all)
		if pipeline in bad_pipelines:
			logging.warning('\033[33mPipeline \033[35m' + str(pipeline) + '\033[33m shows errors. Please check its token status.')
			if (len(list_pipeline_download) > error_tolerance):        ## count download errors and check whether there are too many
				logging.warning('\033[33mPipeline \033[35m' + str(pipeline) + '\033[33m shows more than ' + str(error_tolerance) + ' errors. Script will try to rerun them.')
				if recursive:
					break
					pass
				logging.info('Checking current staging status...')
				srm_list = []
				for item in list_observation:
					srm = tokens.db.get_attachment(item['key'], 'srm.txt').read().strip()
					srm_list.append(srm)
					pass
				pool2 = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 2)
				is_staged_list   = pool2.map(is_staged, srm_list)
				staged_files     = sum(is_staged_list)
				staging_fraction = staged_files / float(len(list_observation))
				logging.info('The current staging fraction is: \033[35m' + str(staging_fraction))
				if staging_fraction < min_staging_fraction:
					logging.error('\033[31mFiles for pipeline \033[35m' + str(pipeline) + '\033[31m appears to get unstaged. Processing of \033[35m' + str(observation) + '\033[31m will be cancelled.')
					#list_pipelines_total = tokens.list_tokens_from_view('overview_total')
					for item in list_pipeline_all:
						set_token_status(tokens, item['key'], 'error')
						set_token_output(tokens, item['key'], -15)
						set_token_progress(tokens, item['key'], 'Processing has been cancelled due to unstaged data.')
						unlock_token(tokens, item['key'])
						pass
					os.remove(last_observation)
					pass
				else:
					for item in list_pipeline_download:
						unlock_token(tokens, item['key'])
						pass
					if len(list_pipeline_download) <= (len(list_observation) - staged_files):
						logging.info('Waiting for data being staged...')
						time.sleep(3600)
						pass
					pass
				break
				pass
			pass
		if len(status) > 1:
			logging.warning('\033[33mPipeline \033[35m' + pipeline + '\033[33m shows more than one status: \033[35m' + str(status) + '\033[33m. Script will try to proceed.')
			pass
		if len(status) > 2:
			if recursive:
				continue
				pass
			logging.warning('\033[33mPipeline \033[35m' + pipeline + '\033[33m for this observation is broken.')
			#tokens.reset_tokens(view_name=pipeline)
			if status[0] == 'error':
				broken_tokens = tokens.list_tokens_from_view(status[1])
				pass
			else:
				broken_tokens = tokens.list_tokens_from_view(status[0])
				pass
			for item in broken_tokens:
				set_token_status(tokens, item['key'], 'error')
				set_token_progress(tokens, item['value'], 'Unknown error')
				pass
			break
			pass
		elif 'submitted' in status:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m has already been submitted.')
			if log_information == 'processing':
				for item in list_pipeline:
					set_token_status(tokens, item['key'], 'processing')
					set_token_output(tokens, item['key'], 0)
					set_token_progress(tokens, item['key'], 0)
					pass
			elif log_information != '':
				submit_error_log(tokens, list_pipeline, slurm_log, log_information, working_directory, last_observation)
				pass
			break
			pass
		elif 'processing' in status:
			if log_information == 'processing':
				logging.info('Pipeline \033[35m' + pipeline + '\033[32m is currently processed.')
				pass
			elif log_information != '':
				submit_error_log(tokens, list_pipeline, slurm_log, log_information, working_directory, last_observation)
				pass
			break
			pass
		elif 'processed' in status or 31 in output:
			logging.info('\033[0mPipeline \033[35m' + pipeline + ' \033[0m for this observation has been processed.')
			tokens.add_view(view_name='temp', cond=' (doc.status == "processed" || doc.output == 31) && doc.PIPELINE_STEP == "' + pipeline + '" ')
			list_done = tokens.list_tokens_from_view('temp')
			submit_results(tokens, list_done, working_directory, observation, server, pc.user, pc.password, pc.database, pipeline)
			continue
			pass
		elif ('unpacked' in status or 'queued' in status) and not 'unpacking' in status and not 'downloading' in status:
			logging.error(str(locked_pipelines))  ## keep this until bug re-appears
			logging.error(str(pipelines_done))    ## keep this until bug re-appears
			if pipeline != locked_pipelines[0] and locked_pipelines[0] not in pipelines_done:
				for item in list_pipeline:
					set_token_progress(tokens, item['key'], 'Previous pipeline has not been finished yet')
					if  len(pipelines_done) > 0:
						set_token_status(tokens, item['key'], 'unpacked')
						pass
					pass
				break
				pass
			else:
				logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be started.')
				run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, ftp, last_observation)
				break
				pass
		elif 99 in output or -1 in output or 3 in output:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be resumed.')
			run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, ftp, last_observation)
			break
			pass
		elif 'transferred' in status:
			continue
			pass
		elif 20 in output or 21 in output or 22 in output:
			logging.warning('\033[33mAll necessary data for the pipeline \033[35m' + pipeline + '\033[33m are not yet available.')
			if len(pipelines_done) > 0:
				logging.info('Resetting pipeline: \033[35m' + pipeline)
				tokens.reset_tokens(view_name=pipeline)
				pass
			break
			pass
		else:
			logging.error('\033[31mPipeline \033[35m' + pipeline + '\033[31m has an invalid status. Script will proceed without it.')
			pass
		pass
	 
	## clear database
	tokens.del_view(view_name='temp')
	tokens.del_view(view_name='temp2')
	
	## last check
	if set(pipelines) <= set(pipelines_done) and len(pipelines_todo) == 0:
		logging.info('\033[0mNo tokens in database found to be processed.')
		pass
	
	## wait for processes to be finished
	try:
		pool.join()
		pass
	except UnboundLocalError:
		pass
	
	## remove the lock file
	if is_running(lock_file):
		os.remove(lock_file)
		pass
	return 0
	pass


if __name__=='__main__':
	# Get command-line options.
	opt = optparse.OptionParser(usage='%prog ', version='%prog '+_version, description=__doc__)
	opt.add_option('-s', '--server', help='PiCaS server URL:port', action='store_true', default='https://picas-lofar.grid.surfsara.nl:6984')
	opt.add_option('-f', '--ftp', help='FTP server hosting current prefactor version', action='store_true', default='gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/sandbox/')
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
		pass
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
	main(options.server, options.ftp)
	
	# monitoring has been finished
	logging.info('\033[30;4mMonitoring has been finished.\033[0m')
	
	sys.exit(0)
	pass
    
