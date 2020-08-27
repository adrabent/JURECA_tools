#! /usr/bin/env python2
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

from GRID_PiCaS_Launcher.get_picas_credentials import PicasCred
from cloudant.client import CouchDB
from GRID_LRT.token import caToken, TokenList, TokenView, TokenJsonBuilder

from GRID_LRT.Staging import srmlist
from GRID_LRT.Staging.srmlist import slice_dicts

from GRID_PiCaS_Launcher.couchdb.client import Server

_version = '1.0'                               ## program version
pref_version = '1.0'                           ## prefactor version for gathering solutions from the GRID
nodes = 24                                     ## number of JUWELS nodes (higher number leads to a longer queueing time)
walltime = '01:30:00'                          ## walltime for the JUWELS queue
mail = 'alex@tls-tautenburg.de'                ## notification email address
IONEX_server = 'ftp://ftp.aiub.unibe.ch/CODE/' ## URL for CODE downloads
num_SBs_per_group_var = 10                     ## chunk size 
max_dppp_threads_var = 24                      ## maximal threads per node per DPPP instance (max 96 on JUWELS)
max_proc_per_node_limit_var = 3                ## maximal processes per node for DPPP
num_proc_per_node_var = 10                     ## maximal processes per node for others
error_tolerance = 0                            ## number of failed tokens still acceptable for running pipelines
condition = 'targ'                             ## condition for the pipeline in order to get checked for pre-existing data (usually the target pipeline)
force_process = 'cal'                          ## if tokens only of that type exist, enforce processing
final_pipeline = 'pref3_targ2'                 ## name of final pipeline
calibrator_results = 'cal'                     ## name of pipeline where calibrator results might have been stored
min_staging_fraction = 0.79                    ## only process fields with this minimum fraction of staged data


os.system('clear')
print('\033[30;1m################################################')
print('## Surveys KSP monitoring script              ##')
print('################################################\033[0m')


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
	lock = os.environ['SCRATCH_chtb00'] + '/htb006' '/.lock' ### DEBUG .. HAS TO BE REMOVED!!!
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


def get_observations_todo(db, designs):
  
	observations = []
	
	for design in designs:
		#if 'pref3' not in design:
			#continue
		tokens = TokenList(database = db, token_type = design)
		list_todos = tokens.list_view_tokens('todo')
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


def get_observation_id(list_todos):
	
	obsids = []
	
	for token in list_todos:
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


def get_cal_solutions(list_todos):
	
	cal_solutions = []
	
	for token in list_todos:
		cal_solution = token['CAL2_SOLUTIONS']
		if cal_solution not in cal_solutions:
			cal_solutions.append(cal_solution)
			pass
		pass

	if len(cal_solutions) > 1:
		return(1)
		pass
	else:
		return cal_solutions[0]
		pass
	pass

def check_for_corresponding_calibration_results(tokens, list_pipeline, cal_solution, working_directory):
    
	#for token in list_pipeline:
		#break
		#pass
    
	#results_dir = '/'.join(token['RESULTS_DIR'].split('/')[:-2]) + '/prefactor_v' + pref_version + '/' + calibrator_results + '/' + cal_obsid
	#results_dir = '/'.join(token['RESULTS_DIR'].split('/')[:-2]) + '/' + calibrator_results + '/' + cal_obsid
	#results_fn  = results_dir + '/' + cal_obsid + '.tar'
	existence = subprocess.Popen(['uberftp', '-ls', cal_solution])
	errorcode = existence.wait()
	if errorcode == 0:
		#logging.info('Cleaning working directory.')
		#shutil.rmtree(working_directory, ignore_errors = True)
		logging.info('Transferring calibrator results for this field from: \033[35m' + cal_solution)
		filename = working_directory + '/' + os.path.basename(cal_solution)
		if not os.path.exists(working_directory):
			os.makedirs(working_directory)
			pass
		transfer  = subprocess.Popen(['globus-url-copy', cal_solution, 'file://' + filename], stdout=subprocess.PIPE)
		errorcode = transfer.wait()
		if errorcode != 0:
			logging.error('\033[31m Downloading calibrator results have failed.')
			for item in list_pipeline: 
				set_token_status(item, 'error')
				set_token_output(item, 23)
				set_token_progress(item, 'Download of calibrator results failed, error code: ' + str(errorcode))
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
					set_token_status(item, 'error')
					set_token_output(item, 23)
					set_token_progress(item, 'Unpacking of calibrator results failed, error code: ' + str(errorcode))
					pass
				return False
				pass
			os.remove(filename)
			logging.info('File \033[35m' + filename + '\033[32m was removed.')
			return True
			pass
	else:
		logging.warning('Could not find any calibrator results for this field in: \033[35m' + cal_solution)
		output = pipeline_output(list_pipeline)
		if not 23 in output:
			for item in list_pipeline: 
				set_token_status(item, 'error')
				set_token_output(item, 23)
				set_token_progress(item, 'No calibrator solutions or data found.')
				pass
			pass
		return False
		pass
	pass

def check_for_corresponding_pipelines(tokens, pipeline, pipelines_todo, working_directory):
    
	obsid_list = []
	
	for pipeline_todo in pipelines_todo:
		tokens._design_doc.delete_view('pipeline_todo')
		tokens.add_view(TokenView(pipeline_todo, ' doc.PIPELINE_STEP == "' + pipeline_todo + '" '))
		if pipeline == pipeline_todo:
			list_pipeline = tokens.list_view_tokens(pipeline_todo)
			cal_solutions = get_cal_solutions(list_pipeline)
			output        = pipeline_output(list_pipeline)
			pass
		else:
			list_pipeline = tokens.list_view_tokens(pipeline_todo)
			obsid_list.append(get_observation_id(list_pipeline))
			output = pipeline_output(list_pipeline)
			pass
		pass
            
	if -15 in output:
		logging.info('Resuming observation.')
		return True
		pass

	obsid = list(set(obsid_list))
        
	if len(obsid) == 0:
		#logging.warning('\033[33mCould not find the following calibrator observation: \033[35m' + cal_obsid)
		logging.warning('Could not find a calibrator observation')
		return check_for_corresponding_calibration_results(tokens, list_pipeline, cal_solutions, working_directory)
		pass
	#elif len(obsid) > 1 or obsid[0] not in cal_obsid:
		#logging.warning('\033[33mNo corresponding target pipeline found for: \033[35m' + obsid[0])
		#return False
		#pass
	
	return True
	pass
	
	
def get_pipelines(list_locked):
	
	pipelines = []
	
	for item in list_locked:
		pipeline = item['PIPELINE_STEP']
		if pipeline not in pipelines:
			pipelines.append(pipeline)
			pass
		pass
      
	return pipelines
	pass
      
      
def find_new_observation(observations, observation_done, db, working_directory):

	staged_dict = {}
        
	for observation in observations:
		if observation == observation_done:
			continue
			pass
		logging.info('Checking staging status of remaining files in observation: \033[35m' + observation)
		tokens     = TokenList(database = db, token_type = observation)
		list_todos = tokens.list_view_tokens('todo')
		srm_list   = []
		for item in list_todos:
			try:
				srm = item.get_attachment('srm.txt').strip().split('\n')
				for entry in srm:
					srm_list.append(entry)
					pass
				pass
			except AttributeError:
				logging.warning('Invalid download URL in: \033[35m' + str(item['_id']))
				pass
			pass
		pool = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 4)
		is_staged_list   = pool.map(is_staged, srm_list)
		staged_files     = sum(is_staged_list)
		staging_fraction = staged_files / float(len(srm_list))
		logging.info('The staging status is: \033[35m' + str(100 * staging_fraction) + '%')
		if str(staging_fraction) in list(staged_dict.keys()):
			staged_dict[str(staging_fraction + random.randint(1,100)/100000.)] = observation
			pass
		else:
			staged_dict[str(staging_fraction)] = observation
			pass
		pass
    
	observation_keys = [float(i) for i in list(staged_dict.keys())]
	observation_keys = list(reversed(sorted(observation_keys)))

	if len(observation_keys) == 0:
		return 1
		pass
    
	for observation_key in observation_keys:
		observation = staged_dict[str(observation_key)]
		if observation_key < min_staging_fraction:
			old_observations = glob.glob(working_directory + '/*/')
			for old_observation in old_observations:
				if observation in old_observation:
					logging.info('Resuming observation: \033[35m' + observation)
					return observation
					pass
				pass
			logging.info('Waiting for data being staged...')
			time.sleep(3600)
			return 1
			pass
		logging.info('Checking observation: \033[35m' + observation)
		tokens     = TokenList(database = db, token_type = observation)
		list_todos = tokens.list_view_tokens('todo')
		try:
			pipelines_todo = get_pipelines(list_todos)  # check whether there are also todo pipelines
			pass
		except KeyError:
			logging.warning('Observation: \033[35m' + observation + '\033[33m is invalid.')
			continue
			pass
		valid = True
		for pipeline in pipelines_todo:       
			if condition in pipeline:
				check_passed = check_for_corresponding_pipelines(tokens, pipeline, pipelines_todo, working_directory + '/' + observation) ## for sub-directories
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
			pass
		if valid:
			logging.warning('Observation: \033[35m' + observation + '\033[33m does not show a target pipeline.')
			return observation
			pass
		logging.warning('Observation: \033[35m' + observation + '\033[33m does not show a valid pipeline.')
		pass

	return observation_done

	pass
              
              
def lock_token(token):
	
	token['lock'] = time.time()
	token.save()
	logging.info('Token \033[35m' + token['_id'] + '\033[32m has been locked.')
	pass
      
      
def lock_token_done(token):
	
	token['done'] = time.time()
	token.save()
	logging.info('Token \033[35m' + token['_id'] + '\033[32m is done.')
	pass
      
      
def add_time_stamp(token):
	
	times = token['times']
	
	times[status]   = time.time()
	token['times']  = times

	token.save()
	pass


def set_token_status(token, status):
	
	times = token['times']
	
	if type(times) is not dict:
		times = {}
		pass
	
	times[status]   = time.time()
	token.fetch()

	token['times']  = times
	token['status'] = status

	try:
		token.save()
		logging.info('Status of token \033[35m' + token['_id'] + '\033[32m has been set to \033[35m' + status + '\033[32m.')
		pass
	except:
		logging.warning('Status of token \033[35m' + token['_id'] + '\033[33m has not been changed.')
		pass
	pass


def set_token_output(token, output):
	
	token.fetch()
	token['output'] = output
	try:
		token.save()
		logging.info('Output of token \033[35m' + token['_id'] + '\033[32m has been set to \033[35m' + str(output) + '\033[32m.')
		pass
	except:
		logging.info('Output of token \033[35m' + token['_id'] + '\033[33m has not been changed.')
		pass
	pass
      
      
def set_token_progress(token, progress):
	
	token.fetch()
	token['progress'] = progress
	try:
		token.save()
		logging.info('Progress of token \033[35m' + token['_id'] + '\033[32m has been set to \033[35m' + str(progress) + '\033[32m.')
		pass
	except:
		logging.warning('Progress of token \033[35m' + token['_id'] + '\033[33m has not been changed.')
		pass
	pass
      
      
def unlock_token(token):
	
	token['lock'] = 0
	token.save()
	logging.info('Token \033[35m' + token['_id'] + '\033[32m has been unlocked.')
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


def transfer_data(token, filename, transfer_fn, to_pack):
  
	logging.info('\033[35m' + filename + '\033[32m is now transfered to: \033[35m' + transfer_fn)
	set_token_status(token, 'transferring')
	set_token_output(token, 0)
	set_token_progress(token, 'Transfer of data to: ' + transfer_fn)
	existence = subprocess.Popen(['uberftp', '-ls', transfer_fn])
	errorcode = existence.wait()
	if errorcode == 0:
		subprocess.Popen(['uberftp','-rm', transfer_fn])
		pass
	transfer  = subprocess.Popen(['globus-url-copy', 'file:' + filename, transfer_fn], stdout=subprocess.PIPE)
	errorcode = transfer.wait()
	
	if errorcode == 0:
		logging.info('File \033[35m' + filename + '\033[32m was transferred.')
		set_token_status(token, 'transferred')
		set_token_output(token, 0)
		lock_token_done(token)
		try:
			shutil.move(to_pack, to_pack + '/../../.')
			pass
		except:
			logging.warning('\033[33mMoving \033[35m' + to_pack + '\033[33m to \033[35m' + to_pack + '/../../. \033[33mhas failed.')
			pass
		set_token_progress(token, 'Transfer of data has been finished.')
		return True
		pass
	else:
		logging.error('\033[31mTransfer of \033[35m' + filename + '\033[31m failed. Error code: \033[35m' + str(errorcode))
		set_token_status(token, 'error')
		set_token_output(token, 31)
		set_token_progress(token, 'Transfer of ' + filename.split('/')[-1] + ' failed.')
		return False
		pass
	
	return False
	pass


def pack_and_transfer(token_id, filename, to_pack, pack_directory, transfer_fn, user, password, server, database, observation):
  
	client     = CouchDB(user, password, url = server, connect = True)
	db         = client[database]
	token      = caToken(token_id = token_id, token_type = observation, database = db)
	token.fetch()
	
	os.chdir(pack_directory)
	pack_directory = os.getcwd() + '/'
	logging.info('Packing file: \033[35m' + filename)
	set_token_status(token, 'packing')
	set_token_progress(token, 'Packing file(s): ' + to_pack)
	to_pack_list = to_pack.split(' ')
	if len(to_pack_list) > 1:
		pack = subprocess.Popen(['tar', 'cfv', filename, to_pack_list[0].replace(pack_directory ,'')], stdout = subprocess.PIPE)
		pack.wait()
		for to_pack in to_pack_list[1:]:
			pack = subprocess.Popen(['tar', 'rfv', filename, to_pack.replace(pack_directory, '')], stdout = subprocess.PIPE)
			errorcode = pack.wait()
			pass
	else:
		pack = subprocess.Popen(['tar', 'cfvz', filename, to_pack.replace(pack_directory, '')], stdout = subprocess.PIPE)
		errorcode = pack.wait()
		pass
	      
	if errorcode == 0:
		set_token_status(token, 'packed')
		set_token_output(token, 0)
		logging.info('Packing of \033[35m' + filename + '\033[32m finished.')
		transfer_successful = transfer_data(token, filename, transfer_fn, to_pack)
		#if transfer_successful:
			#if 'prep_cal' in to_pack_list[0]:
				#if os.path.exists(working_directory + '/' + to_pack_list[0].split('prep_cal')[0].split('/')[-1]+ 'prep_cal'):
					#shutil.rmtree(working_directory + '/' + to_pack_list[0].split('prep_cal')[0].split('/')[-1]+ 'prep_cal', ignore_errors = True)
					#pass
				#shutil.move(to_pack_list[0].split('prep_cal')[0] + 'prep_cal', working_directory + '/.')
				#pass
			#if 'pre-cal' in to_pack_list[0]:
				#if os.path.exists(working_directory + '/' + to_pack_list[0].split('/')[-1]):
					#shutil.rmtree(working_directory + '/' + to_pack_list[0].split('/')[-1], ignore_errors = True)
					#pass
				#if 'MHz' in to_pack_list[0]:
					#shutil.rmtree(to_pack_list[0])
					#pass
				#else:
					#shutil.move(to_pack_list[0], working_directory + '/.')
					#pass
				#pass
			#pass
		#pass
	else:
		logging.error('\033[31mPacking failed, error code: ' + str(errorcode))
		set_token_status(token, 'error')
		set_token_output(token, 31)
		set_token_progress(token, 'Packing failed, error code: ' + str(errorcode))
		pass
	
	pass
      
      
def unpack_data(token, filename, working_directory):
  
	os.chdir(working_directory)
	token.fetch()
	if token['output'] == 0 and token['status'] != 'unpacking':
		set_token_status(token, 'unpacking')
		pass
	unpack = subprocess.Popen(['tar', 'xfv', filename, '-C', working_directory], stdout=subprocess.PIPE)
	errorcode = unpack.wait()
	if errorcode == 0:
		token.fetch()
		if token['output'] == 0 and token['status'] != 'unpacked':
			set_token_status(token, 'unpacked')
			set_token_progress(token, 0)
			pass
		if 'ABN' in list(token.keys()):
			freq = os.popen('taql "select distinct REF_FREQUENCY from ' + filename.split('.MS')[0] + '.MS' + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
			update_freq(token, freq)
			pass
		os.remove(filename)
		logging.info('File \033[35m' + filename + '\033[32m was removed.')
		pass
	else:
		logging.error('\033[31mUnpacking failed for: \033[35m' + str(filename))
		if token['output'] == 0 and token['status'] != 'error':
			set_token_status(token, 'error')
			set_token_output(token, 20)
		token.fetch()
		progress  = token['progress']
		if type(progress) is not dict:
			progress = {}
			pass
		if 'Unpacking failed' not in progress.keys():
			progress['Unpacking failed'] = ''
			pass
		progress['Unpacking failed'] += '\n' + str(filename)
		set_token_progress(token, progress)
		pass
	pass


def prepare_downloads(list_todos, pipeline_todownload, working_directory):
  
	#download_list    = srmlist.srmlist() # create list to download
	list_todownload  = [item for item in list_todos if item['PIPELINE_STEP'] == pipeline_todownload] # filter list to a certain pipeline
	srm_list         = []
	list_todownload2 = []
	
	for item in list_todownload:
		lock_token(item)
		try:
			srm       = item.get_attachment('srm.txt').strip().split('\n')
			for entry in srm:
				filename  = working_directory + '/' + '_'.join(os.path.basename(entry).split('_')[:-1])
				if os.path.exists(filename):
					logging.warning('\033[33mFile \033[35m' + str(entry) + '\033[33m is already on disk.')
					set_token_status(item, 'unpacked')
					set_token_output(item, 0)
					if 'ABN' in list(item.keys()):
						try:
							freq = os.popen('taql "select distinct REF_FREQUENCY from ' + filename + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
							pass
						except IndexError:
							logging.warning('\033[33mFile \033[35m' + filename + '\033[33m is probably broken and will be removed.')
							shutil.rmtree(filename, ignore_errors = True)
							#shutil.rmtree(filename2, ignore_errors = True)
							srm_list.append(str(entry))
							list_todownload2.append(item)
							continue
							pass
						update_freq(item, freq)
						pass
					continue
					pass
				srm_list.append(entry)
				if item not in list_todownload2:
					list_todownload2.append(item)
					pass
				pass
			pass
		except AttributeError:
			logging.warning('\033[33mToken \033[35m' + item['_id'] + '\033[33m has no valid download URL(s).')
			set_token_progress(item, 'No valid download URL(s)')
			set_token_status(item, 'error')
			set_token_output(item, 20)
			continue
			pass
		pass

	## check whether data is staged
	logging.info('Checking staging status of files in pipeline: \033[35m' + pipeline_todownload)
	pool = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 4)
	is_staged_list = pool.map(is_staged, list(srm_list))
	
	list_todownload = {}
	download_list   = []
	
	for item in list_todownload2:
		srm = item.get_attachment('srm.txt').strip().split('\n')
		srm_files = list(set(srm).intersection(set(srm_list)))
		set_token_output(item, 0)
		set_token_progress(item, 0)
		for srm_file in srm_files:
			if not is_staged_list[srm_list.index(srm_file)]:
				logging.warning('\033[33mFile \033[35m' + str(srm_file) + '\033[33m has not been staged yet.')
				set_token_status(item, 'error')
				set_token_output(item, 22)
				progress  = item['progress']
				if type(progress) is not dict:
					progress = {}
					pass
				if 'Not staged' not in progress.keys():
					progress['Not staged'] = ''
					pass
				progress['Not staged'] += '\n' + str(srm_file)
				set_token_progress(item, progress)
				continue
				pass
			logging.info('File \033[35m' + str(srm_file) + '\033[32m is properly staged.')
			#set_token_progress(item, str(item['progress']).replace('\n' + str(srm_file),''))
			download_list2 = srmlist.srmlist()
			download_list2.append(str(srm_file))
			gsi = list(download_list2.gsi_links())[0]
			download_list.append(gsi)
			list_todownload[gsi] = item['_id']
			pass
		pass
	
	return (list_todownload, download_list)
	pass
     
     
def download_data(url, token_id, working_directory, observation, user, password, server, database):
#def download_data(url, token_id, working_directory, observation):
#def download_data(url, token, working_directory, observation, user, password, server, database):
  
	#tokens     = TokenList(database = db, token_type = observation)
	#token_list = tokens.list_view_tokens('overview_total')
	#index      = [ i for i,entry in enumerate(token_list) if entry['_id'] == token_id ][0]
	#print(token_list[index])
	#set_token_status(token, 'downloading')
	filename   = working_directory + '/' + url.split('/')[-1]
	download   = subprocess.Popen(['globus-url-copy',  url, 'file:' + filename], stdout=subprocess.PIPE)
	errorcode  = download.wait()
	
	client     = CouchDB(user, password, url = server, connect = True)
	db         = client[database]
	token      = caToken(token_id = token_id, token_type = observation, database = db)
	
	if errorcode == 0:
		unpack_data(token, filename, working_directory)
		pass
	else:
		token.fetch()
		logging.error('Download failed for: \033[35m' + str(url))
		if token['status'] != 'error':
			set_token_status(token, 'error')
		if token['output'] != 21:
			set_token_output(token, 21)
		token.fetch()
		progress  = token['progress']
		if type(progress) is not dict:
			progress = {}
			pass
		if 'Download failed' not in progress.keys():
			progress['Download failed'] = ''
			pass
		progress['Download failed'] += ' \n' + str(url)
		set_token_progress(token, progress)
		pass
	
	return(0)
	pass
   
   
def create_submission_script(submit_job, parset, working_directory, submitted, observation):
	
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
		IONEX_script         = os.popen('find ' + working_directory  +                  ' -name createRMh5parm.py ').readlines()[0].rstrip('\n').replace(' ','')
		#IONEX_path           = os.popen('grep ionex_path '           + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		#IONEX_server         = os.popen('grep ionex_server '         + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"=" | cut -f1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
		cal_solutions        = os.popen('grep cal_solutions '        + parset + ' | cut -f2- -d"=" | cut -f1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
		jobfile.write(IONEX_script + ' --ionexpath ' + working_directory + '/pipeline/. --server ' + IONEX_server + ' --solsetName target ' + working_directory + '/' + target_input_pattern + ' ' + cal_solutions + '\n')
		pass
	except IndexError:
		pass
	try:
		skymodel_script      = os.popen('find ' + working_directory  +         ' -name download_skymodel_target.py').readlines()[0].rstrip('\n').replace(' ','')
		target_input_pattern = os.popen('grep target_input_pattern ' + parset + ' | cut -f2- -d"=" | cut -f1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
		#target_skymodel      = os.popen('grep target_skymodel '      + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$SCRATCH_chtb00/htb006/' + observation, working_directory)
		target_skymodel      = working_directory + '/pipeline/target.skymodel'
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
   
   
def run_prefactor(list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, last_observation):
  
	parset     = working_directory + '/pipeline.parset'
	parset2    = working_directory + '/pipeline2.parset'
	submit_job = working_directory + '/../submit_job'
	
	if os.path.isfile(parset):
		os.remove(parset)
		pass
	if os.path.isfile(parset2):
		os.remove(parset2)
		pass
	
	logging.info('Getting pipeline parset file for \033[35m' + observation)
	sandbox      = []
	branch      = []
	for item in list_pipeline:
		sandbox.append(item['config.json']['sandbox']['scripts'][0]['prefactor']['url'])    ## save sandbox location for prefactor
		branch.append(item['config.json']['sandbox']['scripts'][0]['prefactor']['branch'])  ## save sandbox branch for prefactor
		parsets = [attachment for attachment in item['_attachments'] if 'parset' in attachment]
		if len(parsets) != 1:
			logging.error('\033[31mMultiple or no parsets attached to: \033[35m' + item['_id'])
			set_token_status(item, 'error')
			set_token_output(item, 3)
			set_token_progress(item, 'Multiple or no parsets attached')
			pass
		if os.path.isfile(parset):
			attachment = item.get_attachment(parsets[0])
			with open(parset2, 'wb') as attachment_file:
				attachment_file.write(attachment)
				pass
			if not filecmp.cmp(parset, parset2):
				logging.error('\033[31mParset file mismatches for: \033[35m' + item['_id'])
				set_token_status(item, 'error')
				set_token_output(item, 3)
				set_token_progress(item, 'Parset file mismatch')
				return 1
				pass
			if len(list(set(sandbox))) > 1:
				logging.error('\033[31mSBXloc mismatches for: \033[35m' + item['_id'])
				set_token_status(item, 'error')
				set_token_output(item, -1)
				set_token_progress(item, 'sandbox mismatch')
				return 1
				pass
			os.remove(parset2)
			pass
		else:
			try:
				attachment = item.get_attachment(parsets[0])
				with open(parset, 'wb') as attachment_file:
					attachment_file.write(attachment)
					pass
			except IndexError:
				logging.error('\033[31mNo valid parsets attached to: \033[35m' + observation + '\033[31m. Please check the tokens manually. Processing will be skipped.')
				os.remove(last_observation)
				return 1
			pass
		pass
	
	sandbox     = str(list(set(sandbox))[0])
	branch      = str(list(set(branch))[0])
	
	## applying necessary changes to the parset
	num_proc_per_node       = os.popen('grep "! num_proc_per_node" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node_limit = os.popen('grep "! num_proc_per_node_limit" ' + parset).readlines()[0].rstrip('\n').replace('/','\/')
	max_dppp_threads        = os.popen('grep "! max_dppp_threads" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')
	losoto_directory        = os.popen('grep "! losoto_directory" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')
	aoflagger_executable    = os.popen('grep "! aoflagger" '               + parset).readlines()[0].rstrip('\n').replace('/','\/')
	
	os.system('sed -i "s/' + losoto_directory        + '/! losoto_directory        = \$LOSOTO/g " '                                     + parset)
	os.system('sed -i "s/' + aoflagger_executable    + '/! aoflagger               = \$AOFLAGGER/g " '                                  + parset)
	os.system('sed -i "s/' + num_proc_per_node       + '/! num_proc_per_node       = ' + str(num_proc_per_node_var)            + '/g" ' + parset)
	os.system('sed -i "s/' + num_proc_per_node_limit + '/! num_proc_per_node_limit = ' + str(max_proc_per_node_limit_var)      + '/g" ' + parset)
	os.system('sed -i "s/' + max_dppp_threads        + '/! max_dppp_threads        = ' + str(max_dppp_threads_var)             + '/g" ' + parset)
	
	os.system('sed -i "s/\/Input//g                  " '                           + parset)
	#os.system('sed -i "s/Output/results/g        " '                               + parset)
	os.system('sed -i "s/\$RUNDIR/' + working_directory.replace('/','\/') + '/g" ' + parset)
	os.system('sed -i "s/PREFACTOR_SCRATCH_DIR/' + working_directory.replace('/','\/') + '/g" ' + parset)
	if pipeline == final_pipeline:
		os.system('sed -i "s/\/prefactor\//\/Output\/results\//g " '                            + parset)
		pass
	else:
		os.system('sed -i "s/\/prefactor\//\/results\//g " '                                    + parset)
		pass
	
	#try:
		#makesourcedb         = os.popen('grep "! makesourcedb" '         + parset).readlines()[0].rstrip('\n').replace('/','\/')
		#flagging_strategy    = os.popen('grep "! flagging_strategy" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
		#num_SBs_per_group    = os.popen('grep "! num_SBs_per_group" '    + parset).readlines()[0].rstrip('\n').replace('/','\/')
		
		#os.system('sed -i "s/' + makesourcedb            + '/! makesourcedb            = \$LOFARROOT\/bin\/makesourcedb/g " '                + parset)
		#os.system('sed -i "s/' + flagging_strategy       + '/! flagging_strategy       = \$LOFARROOT\/share\/rfistrategies\/HBAdefault/g " ' + parset)
		#os.system('sed -i "s/' + num_SBs_per_group       + '/! num_SBs_per_group       = ' + str(num_SBs_per_group_var)       + '/g" '       + parset)
        
	#except IndexError:
		#pass

	## downloading prefactor
	filename = working_directory + '/prefactor.tar'
	logging.info('Downloading current prefactor version from \033[35m' + sandbox + '\033[32m to \033[35m' + working_directory + '/prefactor')
	#download = subprocess.Popen(['globus-url-copy', sandbox , 'file:' + filename], stdout=subprocess.PIPE)
	if os.path.exists(working_directory + '/prefactor'):
		logging.warning('Overwriting old prefactor directory...')
		shutil.rmtree(working_directory + '/prefactor', ignore_errors = True)
		pass
            
	download = subprocess.Popen(['git', 'clone', '-b', branch, sandbox, working_directory + '/prefactor'], stdout=subprocess.PIPE)
	errorcode = download.wait()
	if errorcode != 0:
		for item in list_pipeline: 
			set_token_status(item, 'error')
			set_token_output(item, -1)
			set_token_progress(item, 'Download of prefactor failed, error code: ' + str(errorcode))
			pass
		logging.error('\033[31m Downloading prefactor has failed.')
		time.sleep(600)
		return 1
		pass
	      
	logging.info('Creating submission script in \033[35m' + submit_job)
	if is_running(submitted) or is_running(submit_job):
		logging.warning('Submission is already running')
		return 0
		pass
	create_submission_script(submit_job, parset, working_directory, submitted, observation)
	
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
		set_token_status(item, 'submitted')
		set_token_output(item, 0)
		pass
	logging.info('Pipeline \033[35m' + pipeline + '\033[32m has been submitted.')
	
	return 0
	pass
	
	
def pipeline_status(list_pipeline):
	
	status = []
	for item in list_pipeline:
		status.append(item['status'])
		pass
	status = list(set(status))
	
	return status
	pass


def pipeline_output(list_pipeline):
	
	output = []
	for item in list_pipeline:
		if 'output' in item.keys():
			output.append(item['output'])
			pass
		else:
			output.append(0)
			pass
		pass
	output = list(set(output))
	
	return output
	pass

#def pipeline_progress(list_pipeline):
	
	#progress = []
	#for item in list_pipeline:
		#progress.append(item['progress'])
		#pass
	#progress = list(set(progress))
	
	#return progress
	#pass


def update_freq(token, freq):
    
	A_SBN = int(round((((float(freq)/1e6 - 100.0) / 100.0) * 512.0),0))
	if 'FREQ' in list(token.keys()):
		token['FREQ'] = freq
		pass
	
	token['ABN'] = A_SBN
	token.save()
	
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


def submit_diagnostic_plots(token, images):
  
	for image in images:
		logging.info('Upload inspection plot \033[35m' + image + '\033[32m to token \033[35m' + token['_id'])
		token.put_attachment(os.path.basename(image), 'image/png', open(image, 'r'))
		#os.remove(image)
		#logging.info('File \033[35m' + image + '\033[32m was removed.')
		pass
	      
	return 0
	pass


def submit_error_log(list_pipeline, slurm_log, log_information, working_directory, last_observation, observation):

	parset               = working_directory + '/pipeline.parset'
	results_directory    = os.popen('grep results_directory '    + parset + ' | cut -f2- -d"=" | cut -f-1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
	inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"=" | cut -f-1 -d"#"').readlines()[0].rstrip('\n').replace(' ','').replace('{{results_directory}}', results_directory)

	for item in list_pipeline:
		break
		pass
	old_slurm_logs = [attachment for attachment in item['_attachments'] if 'slurm' in attachment]
	for old_slurm_log in old_slurm_logs:
		item.delete_attachment(old_slurm_log)
		pass
	item.put_attachment(os.path.basename(slurm_log), 'text/plain', open(slurm_log, 'r'))
	
	skip = False
	if 'ERROR' in log_information:
		for item in list_pipeline:
			set_token_status(item, 'error')
			set_token_output(item, 99)
			set_token_progress(item, log_information[log_information.find('genericpipeline:'):])
			pass
		time.sleep(3600)
		skip = True
		pass
	if 'error:' in log_information:
		for item in list_pipeline:
			set_token_status(item, 'error')
			set_token_output(item, 99)
			set_token_progress(item, log_information[log_information.find('error:'):])
			pass
		time.sleep(3600)
		skip = True
		pass
	elif 'Error' in log_information:
		for item in list_pipeline:
			set_token_status(item, 'error')
			set_token_output(item, 99)
			set_token_progress(item, log_information[log_information.find('Error:'):])
			pass
		time.sleep(3600)
		skip = True
		pass
	elif 'termination' in log_information:
		for item in list_pipeline:
			set_token_status(item, 'error')
			set_token_output(item, 99)
			set_token_progress(item, log_information[log_information.find('srun:'):])
			pass
		time.sleep(3600)
		skip = True
		pass
	elif 'finished' in log_information:
		for i, item in enumerate(list_pipeline):
			old_images = [attachment for attachment in item['_attachments'] if '.png' in attachment]
			for old_image in old_images:
				item.delete_attachment(old_image)
				pass
			images  = sorted(glob.glob(inspection_directory + '/*.png'))
			images2 = sorted(glob.glob(inspection_directory + '/*freq*.png'))
			images3 = [ image for image in images2 if 'dif' in image ]
			images  = sorted(list(set(images)  - set(images2)))
			images2 = sorted(list(set(images2) - set(images3)))
			set_token_status(item, 'processed')
			set_token_output(item, 0)
			set_token_progress(item, log_information[log_information.find('genericpipeline:'):])
			if i == 0 and len(images2) == 0:
				submit_diagnostic_plots(item, images)
				pass
			#if i < len(list_pipeline) - 1 and len(images2) > 0 and i < len(images2) - 1:
			if len(images2) > 0 and i < len(images2):
				if i == 0:
					submit_diagnostic_plots(item, images)
					pass
				submit_diagnostic_plots(item, [images2[i]])
				submit_diagnostic_plots(item, [images3[i]])
				#os.remove(images2[0])
				#os.remove(images3[0])
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
			if format(start + chunk * slice_size + i,'03') in list(srmdict.keys()):
				sliced[chunk_name].append(srmdict[format(start + chunk * slice_size + i, '03')])
				pass
			pass
		pass

	return sliced
	pass  
      
def submit_results(tokens, list_done, working_directory, observation, user, password, server, database, pipeline):

	parset            = working_directory + '/pipeline.parset'
	results_directory = os.popen('grep results_directory '    + parset + ' | cut -f2- -d"=" | cut -f-1 -d"#"').readlines()[0].rstrip('\n').replace(' ','')
	results           = sorted(glob.glob(results_directory + '/*.ms'))
	
	print(results_directory)
	#print results
	#inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$SCRATCH_chtb00/htb006/' + observation, working_directory)
	#cal_values_directory = os.popen('grep cal_values_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$SCRATCH_chtb00/htb006/' + observation, working_directory)

	#calibration_h5       = glob.glob(inspection_directory + '/*.h5')
	#calibration_npy      = glob.glob(cal_values_directory + '/*.npy')
	#instrument_tables    = sorted(glob.glob(working_directory + '/pipeline/*cal/instrument'))
	#antenna_tables       = sorted(glob.glob(working_directory + '/pipeline/*cal/ANTENNA'))
	#field_tables         = sorted(glob.glob(working_directory + '/pipeline/*cal/FIELD'))
	
	#try:
		#results_directory = os.popen('grep "! results_directory" ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace('$SCRATCH_chtb00/htb006/' + observation, working_directory).replace(' ','')
		#logging.error(results_directory)
		
	#except IndexError:
		#results     = []
		#pass
              
	# upload calibration results
	#if len(instrument_tables) > 0 and len(antenna_tables) > 0 and len(field_tables) > 0:
	if calibrator_results in pipeline:
		for item in list_done:
			sbnumber     = str(item['STARTSB'])
			obsid        = str(item['OBSID'])
			#transfer_dir = (item['RESULTS_DIR'] + '/' + obsid).replace(' ','')
			transfer_dir = (item['RESULTS_DIR']).replace(' ','')
			subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
			#to_pack      = instrument_table + ' ' + antenna_table + ' ' + field_table
			filename     = working_directory + '/' + pipeline + '_' + obsid + '.tar'
			transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
			pack_and_transfer(item['_id'], filename, results_directory, results_directory + '/..', transfer_fn, user, password, server, database, observation)
			pass
		pass
            
	elif len(results) > 0:
		tokens.add_view(TokenView('temp2',  'doc.PIPELINE_STEP == "' + pipeline + '" && doc.done > 0'))  ## select only tokens with    download/upload error
		tokens.add_view(TokenView(final_pipeline,  'doc.PIPELINE_STEP == "' + final_pipeline + '" '))  ## select only tokens with    download/upload error
		list_pipeline_done = tokens.list_view_tokens('temp2')                                    ## get the pipeline list
		if pipeline != final_pipeline and len(list_pipeline_done) == 0:
			list_final_pipeline = tokens.list_view_tokens(final_pipeline)                    ## get the final pipeline list
			if len(list_final_pipeline) > 0:
				#tokens.delete_tokens(final_pipeline)
				tokens.delete_all(final_pipeline)
				pass
			logging.info('Tokens for the final pipeline \033[35m' + final_pipeline + '\033[32m are being created')
			home_directory = os.environ['PROJECT_chtb00'] + '/htb006'
			client   = CouchDB(user, password, url = server, connect = True)
			db = client[database]
			#final_config   = home_directory + '/tools/GRID_LRT2/GRID_LRT/data/config/steps/targ2_pref3.json'
			#final_parset   = home_directory + '/tools/GRID_LRT2/GRID_LRT/data/parsets/Pre-Facet-Target2-v3.parset'
			final_config   = home_directory + '/targ2_pref3.json'
			final_parset   = home_directory + '/Pre-Facet-Target2-v3.parset'
			#ts             = Token.TokenSet(tokens, tok_config = final_config)
			s_list         = {}
			obsid          = get_observation_id(list_done)
			for item in list_done:
				srm = item.get_attachment('srm.txt').strip().split('\n')[0]
				ABN   = str(item['ABN'])
				if ABN == '':
					filename = working_directory + '/' + srm.split('/')[-1]
					freq = os.popen('taql "select distinct REF_FREQUENCY from ' + filename.split('.MS')[0] + '.MS' + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
					logging.error(str(freq))
					update_freq(item, freq)
					ABN = str(item['ABN'])
					logging.error(str(ABN))
					pass
				s_list[ABN] = srm
				pass
			ABNs   = slice_dicts(s_list, slice_size = num_SBs_per_group_var)
			for ABN, srms in ABNs.iteritems():
				tokid = observation + '_' + final_pipeline + '_' + obsid + '_ABN' + str(ABN)
				token = caToken(database = db, token_type = observation, token_id = tokid)
				token.build(TokenJsonBuilder(final_config))
				token['ABN'] = str(ABN)
				token['_id'] = tokid
				token['STARTSB'] = str(ABN)
				token['OBSID'] = obsid
				token['PIPELINE_STEP'] = final_pipeline
				token['output'] = 0
				#token['CAL2_SOLUTIONS'] = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/diskonly/pipelines/SKSP/prefactor_v3.0/pref_cal/' + cal_obsid + '/' + cal_obsid + '_pref3_cal_000_.tar'
				token['status'] = 'queued'
				token.save()
				with open('temp_srm.txt', 'wb') as f:
					f.write('\n'.join(srms))
					pass
				token.add_attachment(attachment_name = 'srm.txt', filename = 'temp_srm.txt')
				token.add_attachment(attachment_name = 'Pre-Facet-Target2-v3.parset', filename = final_parset)
				lock_token(token)
				tokens.append(token)
			#list_final_pipeline = tokens.list_view_tokens(final_pipeline)  ## get the final pipeline list again
			#for item in list_final_pipeline:
				#token['OBSID'] = obsid
				#tokens.db.update([token])
				#target_input_pattern = os.popen('grep "! target_input_pattern" ' + final_parset).readlines()[0].rstrip('\n').replace('/','\/')
				#os.system('sed -i "s/' + target_input_pattern + '/! target_input_pattern = ' + obsid + '\*.ms/g " ' + final_parset)
				#tokens.add_attachment(item.id, open(final_parset, 'rb'), final_parset.split('/')[-1])
				#lock_token(item)
				#os.system('sed -i "s/! target_input_pattern = ' + obsid + '\*.ms/' + target_input_pattern + '/g " ' + final_parset)
				#pass
			for item, result in zip(list_done, results):
				if os.path.exists(working_directory + '/' + result.split('/')[-1]):
					shutil.rmtree(working_directory + '/' + result.split('/')[-1])
					pass
				shutil.move(result, working_directory + '/.')
				set_token_output(item, 0)
				lock_token_done(item)
				pass
			pass
		elif pipeline == final_pipeline:
			list_done_todo = []  ## in case a download failes, repeat only those files 
			results_todo   = []  ## in case a download failes, repeat only those files 
			for result, item in zip(results, list_done):
				if item['status'] != 'transferred':
					list_done_todo.append(item)
					results_todo.append(result)
					pass
				pass
			pool2 = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 2)
			for result, item in zip(results_todo, list_done_todo):
				obsid        = str(item['OBSID'])
				ABN          = str(item['ABN'])
				transfer_dir = (item['RESULTS_DIR'] + '/' + obsid).replace(' ','')
				to_pack      = result
				subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
				filename     = working_directory + '/' + final_pipeline + '_' + obsid + '_ABN' + str(ABN) + '.tar.gz'
				transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
				output = pool2.apply_async(pack_and_transfer, args = (item['_id'], filename, to_pack, to_pack + '/../..', transfer_fn, user, password, server, database, observation))
				pass
			filename     = working_directory + '/' + final_pipeline + '_' + obsid + '.tar.gz'
			transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
			pool2.close()
			pool2.join()
			results = sorted(glob.glob(results_directory + '/*.ms'))
			print(results)
			if len(results) == 0:
				shutil.move(working_directory + '/pipeline.log', results_directory + '/.')
				pack_and_transfer(item['_id'], filename, results_directory, results_directory + '/..', transfer_fn, user, password, server, database, observation)
				pass
			pass
		pass
    
	#elif len(calibration_h5) == 1 and len(calibration_npy) > 0 and condition not in pipeline:
		#h5parms = glob.glob(working_directory + '/pipeline/*.h5')
		#for h5parm in h5parms:
			#os.remove(h5parm)
			#pass
		##tokens.delete_tokens(final_pipeline)
		#for item in list_done:
			#token = tokens.db[item['key']]
			#break
			#pass
		#obsid        = str(token['OBSID'])
		#transfer_dir = (token['RESULTS_DIR'] + '/' + obsid).replace(' ','')
		#to_pack      = calibration_h5[0] + ' ' + ' '.join(calibration_npy)
		#subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
		#filename     = working_directory + '/' + obsid + '.tar'
		#transfer_fn  = transfer_dir + '/' + filename.split('/')[-1]
		#pack_and_transfer(item, filename, to_pack, working_directory, transfer_fn)
		#pass
	
	logging.info('Submitting results for \033[35m' + pipeline + '\033[32m has been finished.')
	return 0
	pass


#def token_list(tokens, view):
	#return tokens.list_view_tokens(view)
	#pass
    
#def 


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
	pc     = PicasCred(home_directory + '/.picasrc')
	logging.info('Username: \033[35m' + pc.user)
	logging.info('Database: \033[35m' + pc.database)
	client   = CouchDB(pc.user, pc.password, url = server, connect = True)
	db = client[pc.database]
	couchdb_server.resource.credentials = (pc.user, pc.password)
	
	## load all design documents
	designs          = load_design_documents(couchdb_server[pc.database])
	observations     = get_observations_todo(db, designs)
	observation      = 1
	
	## check latest observation
	if is_running(last_observation):
		observation = open(last_observation).readline().rstrip()
		pass
	
	## check for new observations if necessary
	if not is_running(last_observation) or is_running(done):
	        logging.info('Looking for a new observation.')
		observation = find_new_observation(observations, observation, db, working_directory)
		if observation == 1:
			logging.info('\033[0mNo new observations could be found. If database is not empty please check it for new or false tokens manually.')
			time.sleep(300)
			return 1
			pass
		pass
	
	## reserve following processes
	logging.info('Selected observation: \033[35m' + observation)
	observation_file = open(last_observation, 'w')
	observation_file.write(observation)
	observation_file.close()
	
	## load token of chosen design document
	tokens = TokenList(database = db, token_type = observation) # load token of certain type
	
	## check for new data sets and get information about other tokens present
	list_error     = tokens.list_view_tokens('error')     # check which tokens of certain type show errors
	list_locked    = tokens.list_view_tokens('locked')    # check which tokens of certain type are in the locked state
	list_done      = tokens.list_view_tokens('done')      # check which tokens of certain type are done
	list_todos     = tokens.list_view_tokens('todo')      # check which tokens of certain type are in the todo state
	
	## add views for usersSTvalue
	tokens.add_view(TokenView('downloading',    'doc.status == "downloading" '))
	tokens.add_view(TokenView('unpacking',      'doc.status == "unpacking" '  ))
	tokens.add_view(TokenView('unpacked',       'doc.status == "unpacked" '   ))
	tokens.add_view(TokenView('submitted',      'doc.status == "submitted" '  ))
	tokens.add_view(TokenView('processing',     'doc.status == "processing" ' ))
	tokens.add_view(TokenView('processed',      'doc.status == "processed" '  ))
	tokens.add_view(TokenView('packing',        'doc.status == "packing" '    ))
	tokens.add_view(TokenView('transferring',   'doc.status == "transferring" '))
	tokens.add_view(TokenView('transferred',    'doc.status == "transferred" '))
	tokens._design_doc.delete_view('done')
	tokens.add_view(TokenView('done',           'doc.done > 0  && doc.output == 0'))
	tokens.add_view(TokenView('overview_total', 'doc.lock > 0  || doc.lock == 0'))
	
	## check which pipelines are locked, done or show errors
	try:
		locked_pipelines    = sorted(list(set(get_pipelines(list_locked))))
		bad_pipelines       = sorted(list(set(get_pipelines(list_error ))))
		pipelines_done      = sorted(list(set(get_pipelines(list_done  ))))
		pipelines_todo      = sorted(list(set(get_pipelines(list_todos ))))
	except TypeError:
		logging.error('\033[31mCould not find a corresponding token for the last observation \033[35m' + observation + '\033[31m. Please check the database for errors.')# Script will check for new observations in the next run.')
		time.sleep(3600)
		os.remove(last_observation)
		return 1
		pass
	
	## lock program
	subprocess.Popen(['touch', lock_file])
	
	## create subdirectory
	working_directory += '/' + observation ## for subdirectories
	if not os.path.exists(working_directory):
		os.makedirs(working_directory)
		pass
	
	## check pipelines to run
	pipelines = sorted(list(set(locked_pipelines) - set(pipelines_todo)))
	
	## check what to download
	if len(list_todos) > 0 and not recursive:
		if os.path.exists(done):
			os.remove(done)
			pass
		(list_todownload, download_list) = prepare_downloads(list_todos, pipelines_todo[0], working_directory)
		#gsilist   = download_list.gsi_links()  # convert the srm list to a GSI list (proper URLs for GRID download)
		#gsilist   = sorted(list(set(gsilist))) # to re-reverse the list in order to match it for the upcoming loop and use only distinct files
		pool      = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 2)
		#print(download_list)
		for url in download_list:
		#for token, url in zip(list_todownload, download_list):
			#result = pool.apply_async(download_data, args = (url, list_todownload[url], working_directory, observation, ))
			token = caToken(token_id = list_todownload[url], token_type = observation, database = db)
			token.fetch()
			if token['output'] == 0 and token['status'] != 'downloading':
				set_token_status(token, 'downloading')
				pass
			result = pool.apply_async(download_data, args = (url, list_todownload[url], working_directory, observation, pc.user, pc.password, server, pc.database,))
			#result = pool.apply_async(download_data, args = (url, item, working_directory, observation, pc.user, pc.password, server, pc.database,))
			#result.get()
			pass
		#result.get()
		pool.close()
		pool.join()
		pass
	
	## check whether a job has been already submitted
	if is_running(submitted): 
		logging.info('\033[0mA pipeline has already been submitted.')
		slurm_list = glob.glob(slurm_files)
		if len(slurm_list) > 0:
			slurm_log = slurm_list[-1]
			job_id = os.path.basename(slurm_log).lstrip('slurm-').rstrip('.out')
			logging.info('Checking current status of the submitted job: \033[35m' + job_id)
			log_information = check_submitted_job(slurm_log, submitted).replace('\033[32m','').replace('\033[0m','')
			pass
		pass
	
	## check which pipelines are done and if observation is finished
	if len(pipelines_done) > 0 and not recursive:
		logging.info('\033[0mPipeline(s) \033[35m' + str(pipelines_done) + '\033[0m are done.')
		if (set(pipelines) < set(pipelines_done)) and (len(pipelines_todo) == 0) and (final_pipeline not in bad_pipelines):
			logging.info('\033[0mObservation \033[35m' + observation + '\033[0m is done.')
			tokens._design_doc.delete_view('downloading')
			tokens._design_doc.delete_view('unpacking')
			tokens._design_doc.delete_view('unpacked')
			tokens._design_doc.delete_view('submitted')
			tokens._design_doc.delete_view('processing')
			tokens._design_doc.delete_view('processed')
			tokens._design_doc.delete_view('packing')
			tokens._design_doc.delete_view('transferring')
			tokens._design_doc.delete_view('transferred')
			tokens._design_doc.delete_view('temp')
			tokens._design_doc.delete_view('temp2')
			tokens._design_doc.delete_view('overview_total')
			tokens._design_doc.save()
			subprocess.Popen(['touch', done])
			logging.info('Cleaning working directory.')
			shutil.rmtree(working_directory, ignore_errors = True)
			pipelines = []
			pass
		pass
	
	## main pipeline loop
	for pipeline in pipelines:
		#print pipeline
		tokens.add_view(TokenView(pipeline, 'doc.PIPELINE_STEP == "' + pipeline + '"'))                                          ## select all tokens of this pipeline
		tokens.add_view(TokenView('temp',   'doc.PIPELINE_STEP == "' + pipeline + '" && (doc.output < 20 |  doc.output > 22)'))  ## select only tokens without download/upload error
		tokens.add_view(TokenView('temp2',  'doc.PIPELINE_STEP == "' + pipeline + '" && (doc.output > 19 && doc.output < 23)'))  ## select only tokens with    download/upload error
		list_pipeline_all      = tokens.list_view_tokens(pipeline)         ## get the pipeline list
		list_pipeline          = tokens.list_view_tokens('temp')           ## get the pipeline list without download errors
		#print list_pipeline
		list_pipeline_download = tokens.list_view_tokens('temp2')          ## get the pipeline list with    download errors
		list_observation       = tokens.list_view_tokens('overview_total') ## get the list of the entire observation
		status = pipeline_status(list_pipeline_all)
		output = pipeline_output(list_pipeline_all)
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
					srm = item.get_attachment('srm.txt').strip().split('\n')
					for entry in srm:
						srm_list.append(entry)
						pass
					pass
				pool2 = multiprocessing.Pool(processes = multiprocessing.cpu_count() / 4)
				is_staged_list   = pool2.map(is_staged, srm_list)
				staged_files     = sum(is_staged_list)
				#staging_fraction = staged_files / float(len(list_observation))
				staging_fraction = staged_files / float(len(srm_list))
				logging.info('The current staging fraction is: \033[35m' + str(staging_fraction))
				for item in list_pipeline_download:
					unlock_token(item)
					pass
				if len(list_pipeline_download) <= (len(list_observation) - staged_files):
					logging.info('Waiting for data being staged...')
					time.sleep(3600)
					os.remove(last_observation)
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
			if status[0] == 'error':
				broken_tokens = tokens.list_view_tokens(status[1])
				pass
			else:
				broken_tokens = tokens.list_view_tokens(status[0])
				pass
			for item in broken_tokens:
				set_token_status(item, 'error')
				set_token_progress(item, 'Unknown error')
				pass
			break
			pass
		elif 'submitted' in status:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m has already been submitted.')
			if log_information == 'processing':
				for item in list_pipeline:
					set_token_status(item, 'processing')
					set_token_output(item, 0)
					set_token_progress(item, 0)
					pass
			elif log_information != '':
				submit_error_log(list_pipeline, slurm_log, log_information, working_directory, last_observation, observation)
				pass
			break
			pass
		elif 'processing' in status:
			if log_information == 'processing':
				logging.info('Pipeline \033[35m' + pipeline + '\033[32m is currently processed.')
				pass
			elif log_information != '':
				submit_error_log(list_pipeline, slurm_log, log_information, working_directory, last_observation, observation)
				pass
			break
			pass
		elif 'processed' in status or 31 in output:
			logging.info('\033[0mPipeline \033[35m' + pipeline + ' \033[0m for this observation has been processed.')
			tokens.add_view(TokenView('temp', ' (doc.status == "processed" || doc.output == 31) && doc.PIPELINE_STEP == "' + pipeline + '" '))
			list_done = tokens.list_view_tokens('temp')
			submit_results(tokens, list_done, working_directory, observation, pc.user, pc.password, server, pc.database, pipeline)
			continue
			pass
		elif ('unpacked' in status or 'queued' in status) and not 'unpacking' in status and not 'downloading' in status:
			if pipeline != locked_pipelines[0] and locked_pipelines[0] not in pipelines_done:
				for item in list_pipeline:
					set_token_progress(item, 'Previous pipeline has not been finished yet')
					if  len(pipelines_done) > 0:
						set_token_status(item, 'unpacked')
						pass
					pass
				break
				pass
			else:
				logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be started.')
				run_prefactor(list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, last_observation)
				break
				pass
		elif 99 in output or -1 in output or 3 in output:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be resumed.')
			run_prefactor(list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, last_observation)
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
	tokens.fetch()
	tokens._design_doc.delete_view('temp')
	tokens._design_doc.delete_view('temp2')
	tokens._design_doc.save()
	
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
    
