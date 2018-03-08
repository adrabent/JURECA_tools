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

import time, datetime
import subprocess
import filecmp
import multiprocessing

from GRID_LRT.get_picas_credentials import picas_cred
from GRID_LRT import Token
from GRID_LRT.Staging import srmlist

from GRID_LRT.couchdb.client import Server


_version = '0.5beta'                           ## program version
nodes = 23                                     ## number of JURECA nodes (higher number leads to higher queueing time)
walltime = '01:00:00'                          ## walltime for the JURECA queue
mail = 'alex@tls-tautenburg.de'                ## notification email address
IONEX_server = 'ftp://ftp.aiub.unibe.ch/CODE/' ## URL for CODE downloads
num_SBs_per_group_var = 10                     ## chunk size 
max_dppp_threads_var = 10                      ## maximal threads per node per DPPP instance
max_proc_per_node_limit_var = 6                ## maximal processes per node
error_tolerance = 10                           ## number of failed tokens still acceptable for running pipelines
condition = 'targ'                             ## condition for the pipeline in order to be idenitified as new observations (usually the target pipeline)
final_pipeline = 'pref_targ2'                  ## name of final pipeline


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


def get_cal_observation_id(tokens, list_todos):
	
	obsids = []
	
	for item in list_todos:
		token = tokens.db[item['value']]
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


def check_for_corresponding_pipelines(tokens, pipeline, pipelines_todo):
    
	obsid_list = []
	
	for pipeline_todo in pipelines_todo:
		tokens.add_view(v_name=pipeline_todo, cond=' doc.PIPELINE == "' + pipeline_todo + '" ')
		if pipeline == pipeline_todo:
			list_pipeline = tokens.list_tokens_from_view(pipeline_todo)
			cal_obsid = get_cal_observation_id(tokens, list_pipeline)
			pass
		else:
			list_pipeline = tokens.list_tokens_from_view(pipeline_todo)
			obsid_list.append(get_observation_id(tokens, list_pipeline))
			pass
		pass
	
	obsid = list(set(obsid_list))
        
	if len(obsid) == 0:
		logging.warning('\033[33mNo corresponding pipeline found for: \033[35m' + cal_obsid)
		return False                
		pass
	elif len(obsid) > 1 or obsid[0] != cal_obsid:
		logging.warning('\033[33mNo corresponding pipeline found for: \033[35m' + obsid[0])
		return False
		pass
	
	return True
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
      
      
def find_new_observation(observations, observation_done, server, user, password, database, working_directory):

	for observation in observations:
		logging.info('Checking observation: \033[35m' + observation)
		tokens         = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) # load token of certain type
		list_todos     = tokens.list_tokens_from_view('todo')       # load all todo tokens
		try:
			pipelines_todo = get_pipelines(tokens, list_todos)  # check whether there are also todo pipelines
			pass
		except KeyError:
			logging.warning('Observation: \033[35m' + observation + '\033[33m is invalid.')
			continue
			pass
		for pipeline in pipelines_todo:
			if condition in pipeline:
				check_passed = check_for_corresponding_pipelines(tokens, pipeline, pipelines_todo)
				if check_passed:   # it is a valid observation
					logging.info('Cleaning working directory.')
					shutil.rmtree(working_directory, ignore_errors = True)
					return observation
					pass
				pass
			pass
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


def is_running(lock_file):
	if os.path.isfile(lock_file):
		return True
		pass
	else:
		return False
		pass
	pass


def transfer_data(tokens, token_value, filename, transfer_fn, working_directory):
  
	logging.info('\033[35m' + filename + '\033[32m is now transfered to: \033[35m' + transfer_fn)
	set_token_status(tokens, token_value, 'transferring')
	set_token_output(tokens, token_value, 0)
	set_token_progress(tokens, token_value, 'Transfer of data to: ' + transfer_fn)
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
		set_token_progress(tokens, token_value, 'Transfer of ' + str(filename) + ' failed.')
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
				shutil.move(to_pack_list[0].split('prep_cal')[0] + 'prep_cal', working_directory + '/.')
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


def prepare_downloads(tokens, list_todos, pipeline_todownload):
  
	download_list    = srmlist.srmlist() # create list to download
	list_todownload  = [item for item in list_todos if tokens.db[item['value']]['PIPELINE'] == pipeline_todownload] # filter list to a certain pipeline
	srm_list         = []
	list_todownload2 = []
	
	logging.info('Checking staging status of files in pipeline: \033[35m' + pipeline_todownload)
	for item in list_todownload:
		lock_token(tokens, item['value'])
		try:
			srm = tokens.db.get_attachment(item['value'], 'srm.txt').read().strip()
			srm_list.append(srm)
			list_todownload2.append(item)
		except AttributeError:
			logging.warning('\033[33mToken \033[35m' + item['value'] + '\033[33m has no valid download URL.')
			set_token_progress(tokens, item['value'], 'No valid download URL')
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], 20)
			continue
			pass
		pass

	## check whether data is staged
	pool = multiprocessing.Pool(processes = multiprocessing.cpu_count())
	is_staged_list = pool.map(is_staged, srm_list)
	
	for i, item in enumerate(list_todownload2):
		srm = tokens.db.get_attachment(item['value'], 'srm.txt').read().strip()
		if not is_staged_list[i]:
			logging.warning('\033[33mFile \033[35m' + srm + '\033[33m has not been staged yet.')
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], 22)
			set_token_progress(tokens, item['value'], 'File ' + srm + ' has not been staged yet.')
			#unlock_token(tokens, item['value'])
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
	
	return (list_todownload2, download_list)
	pass
     
     
def download_data(url, token_value, working_directory, observation, server, user, password, database):
  
	tokens = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) # load token of done observation
	set_token_status(tokens, token_value, 'downloading')
	filename = working_directory + '/' + url.split('/')[-1]
	download = subprocess.Popen(['globus-url-copy', url, 'file:' + filename], stdout=subprocess.PIPE)
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
   
   
def run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, ftp):
  
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
				set_token_output(tokens, item['value'], 3)
				set_token_progress(tokens, item['value'], 'Multiple or no parsets attached')
				pass
  		if os.path.isfile(parset):
			tokens.get_attachment(item['value'], parsets[0], parset2)
			if not filecmp.cmp(parset, parset2):
				logging.error('\033[31mParset file mismatches for: \033[35m' + item['value'])
				set_token_status(tokens, item['value'], 'error')
				set_token_output(tokens, item['value'], 3)
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
	
	SBXloc = ftp + str(list(set(SBXloc))[0])

	## applying necessary changes to the parset
	num_proc_per_node       = os.popen('grep "! num_proc_per_node" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')
	num_proc_per_node_limit = os.popen('grep "! num_proc_per_node_limit" ' + parset).readlines()[0].rstrip('\n').replace('/','\/')
	max_dppp_threads        = os.popen('grep "! max_dppp_threads" '        + parset).readlines()[0].rstrip('\n').replace('/','\/')
	losoto_executable       = os.popen('grep "! losoto_executable" '       + parset).readlines()[0].rstrip('\n').replace('/','\/')

	os.system('sed -i "s/' + losoto_executable       + '/! losoto_executable       = \$LOFARROOT\/bin\/losoto/g " '                      + parset)
	os.system('sed -i "s/' + num_proc_per_node       + '/! num_proc_per_node       = input.output.max_per_node/g" '                      + parset)
	os.system('sed -i "s/' + num_proc_per_node_limit + '/! num_proc_per_node_limit = ' + str(max_proc_per_node_limit_var) + '/g" '       + parset)
	os.system('sed -i "s/' + max_dppp_threads        + '/! max_dppp_threads        = ' + str(max_dppp_threads_var)        + '/g" '       + parset)
	os.system('sed -i "s/PREFACTOR_SCRATCH_DIR/\$WORK/g" ' + parset)
	
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
		logging.error('\033[31m Downloading prefactor has failed.')
		for item in list_pipeline: 
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], -1)
			set_token_progress(tokens, item['value'], 'download of prefactor failed, error code: ' + str(errorcode))
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
			set_token_progress(tokens, item['value'], 'unpacking of prefactor failed, error code: ' + str(errorcode))
			pass
		logging.error('\033[31m Unpacking prefactor has failed.')
		return 1
		pass
	
	logging.info('Clearing pipeline directory.')
	shutil.rmtree(working_directory + '/pipeline', ignore_errors = True)
	
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
	time.sleep(600)
	
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


def submit_error_log(tokens, list_pipeline, slurm_log, log_information, working_directory):

	parset               = working_directory + '/pipeline.parset'
	inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$WORK', working_directory)

	for item in list_pipeline:
		token_value = item['value']
		break
		pass
	attachments = tokens.list_attachments(token_value)
	old_slurm_logs = [i for i in attachments if 'slurm' in i]
	doc = tokens.db[token_value]
	for old_slurm_log in old_slurm_logs:
		tokens.db.delete_attachment(doc, old_slurm_log)
		pass
	tokens.add_attachment(token_value, open(slurm_log,'r'), os.path.basename(slurm_log))
	
	if 'ERROR' in log_information:
		for item in list_pipeline:
			set_token_status(tokens, item['value'], 'error')
			set_token_output(tokens, item['value'], 99)
			set_token_progress(tokens, item['value'], log_information[log_information.find('genericpipeline:'):])
			pass
	elif 'finished' in log_information:
		for i, item in enumerate(list_pipeline):
			set_token_status(tokens, item['value'], 'processed')
			set_token_output(tokens, item['value'], 0)
			set_token_progress(tokens, item['value'], log_information[log_information.find('genericpipeline:'):])
			images = glob.glob(inspection_directory + '/*.png')
			if i < len(list_pipeline) - 1:
				submit_diagnostic_plots(tokens, item['value'], images[:1])
				pass
			else:
				submit_diagnostic_plots(tokens, item['value'], images)
				pass
			pass
		if os.path.exists(working_directory + '/pipeline/statefile'):
			os.remove(working_directory + '/pipeline/statefile')
			logging.info('Cleaning pipeline directory.')
			pass
		
		pass
	      
	return 0
	pass


def pack_and_transfer(token_value, filename, to_pack, pack_directory, transfer_fn, working_directory, observation, server, user, password, database):
  
	tokens = Token.Token_Handler( t_type=observation, srv=server, uname=user, pwd=password, dbn=database) # load token of done observation
	pack_data(tokens, token_value, filename, to_pack, transfer_fn, pack_directory + '/', working_directory)
		
	pass
      
      
def submit_results(tokens, list_done, list_pipeline_all, working_directory, observation, server, user, password, database):

	parset               = working_directory + '/pipeline.parset'
	inspection_directory = os.popen('grep inspection_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$WORK', working_directory)
	cal_values_directory = os.popen('grep cal_values_directory ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','').replace('$WORK', working_directory)
	calibration_h5       = glob.glob(inspection_directory + '/*.h5')
	calibration_npy      = glob.glob(cal_values_directory + '/*.npy')
	instrument_tables    = sorted(glob.glob(working_directory + '/pipeline/*cal/instrument'))
	antenna_tables       = sorted(glob.glob(working_directory + '/pipeline/*cal/ANTENNA'))
	field_tables         = sorted(glob.glob(working_directory + '/pipeline/*cal/FIELD'))
	
	try:
		results_directory = os.popen('grep "! results_directory" ' + parset + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace('$WORK', working_directory).replace(' ','')
		results     = sorted(glob.glob(results_directory + '/*.ms'))
	except IndexError:
		results     = []
		pass
        
	# upload calibration results
	if len(results) > 0:
		tokens.add_view(v_name=final_pipeline, cond=' doc.PIPELINE == "' + final_pipeline + '" ')
		list_final_pipeline = tokens.list_tokens_from_view(final_pipeline)  ## get the final pipeline list
		if len(list_final_pipeline) == 0:
			logging.info('Tokens for the final pipeline \033[35m' + final_pipeline + '\033[32m are being created')
			home_directory = os.environ['HOME']
			final_config   = home_directory + '/' + final_pipeline + '.cfg'
			final_parset   = home_directory + '/' + final_pipeline + '.parset'
			ts             = Token.TokenSet(tokens, tok_config = final_config)
			s_list         = srmlist.srmlist()
			obsid          = get_observation_id(tokens, list_done)
			for item in list_done:
				srm = tokens.db.get_attachment(item['value'], 'srm.txt').read().strip()
				s_list.append(srm)
				pass
			sbns = srmlist.slice_dicts(s_list.sbn_dict(), slice_size = num_SBs_per_group_var)
			ts.create_dict_tokens(iterable = sbns, id_append=final_pipeline + '_' + obsid, key_name='start_SB', file_upload='srm.txt')
			list_final_pipeline = tokens.list_tokens_from_view(final_pipeline)  ## get the final pipeline list again
			for item in list_final_pipeline:
				token = tokens.db[item['value']]
				token['OBSID'] = obsid
				tokens.db.update([token])
				target_input_pattern = os.popen('grep "! target_input_pattern" ' + final_parset).readlines()[0].rstrip('\n').replace('/','\/')
				os.system('sed -i "s/' + target_input_pattern + '/! target_input_pattern = ' + obsid + '\*.ms/g " ' + final_parset)
				tokens.add_attachment(item.id, open(final_parset, 'rb'), final_parset.split('/')[-1])
				lock_token(tokens, item['value'])
				os.system('sed -i "s/! target_input_pattern = ' + obsid + '\*.ms/' + target_input_pattern + '/g " ' + final_parset)
				pass
                        for item, result in zip(list_done, results):
                                shutil.move(result, working_directory + '/.')
                                set_token_output(tokens, item['value'], 0)
                                lock_token_done(tokens, item['value'])
                                pass
			pass
		else:
			pool2 = multiprocessing.Pool(processes = multiprocessing.cpu_count())
			for result, item in zip(results, list_done):
				token        = tokens.db[item['value']]
				obsid        = str(token['OBSID'])
				transfer_dir = token['RESULTS_DIR'] + '/' + obsid
				to_pack      = result
				subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
				freq         = os.popen('taql "select distinct REF_FREQUENCY from ' + result + '/SPECTRAL_WINDOW" | tail -1').readlines()[0].rstrip('\n')
				ABN          = update_freq(tokens, item['value'], freq)
				filename     = working_directory + '/GSM_CAL_' + obsid + '_ABN_' + str(ABN) + '.tar.gz'
				transfer_fn  = transfer_dir + '/' + filename.split('/')[0]
				pool2.apply_async(pack_and_transfer, args = (item['value'], filename, to_pack, working_directory, transfer_fn, working_directory, observation, server, user, password, database,))
				pass
			pool2.close()
			pool2.join()
			pass
		      
		return 0
		pass
	
	elif len(calibration_h5) == 1 and len(calibration_npy) > 0:
		for item in list_done:
			token = tokens.db[item['value']]
			break
			pass
		obsid        = str(token['OBSID'])
		transfer_dir = token['RESULTS_DIR'] + '/' + obsid
		to_pack      = calibration_h5[0] + ' ' + ' '.join(calibration_npy)
		subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
		filename     = working_directory + '/' + obsid + '.tar'
		transfer_fn  = transfer_dir + '/' + filename.split('/')[0]
		pack_and_transfer(item['value'], filename, to_pack, working_directory, transfer_fn, working_directory, observation, server, user, password, database)
		pass
	
	elif len(instrument_tables) > 0 and len(antenna_tables) > 0 and len(field_tables) > 0:
		pool2 = multiprocessing.Pool(processes = multiprocessing.cpu_count())
		for item, instrument_table, antenna_table, field_table in zip(list_done, instrument_tables, antenna_tables, field_tables):
			token        = tokens.db[item['value']]
			sbnumber     = str(token['STARTSB'])
			obsid        = str(token['OBSID'])
			transfer_dir = token['RESULTS_DIR'] + '/' + obsid
			subprocess.Popen(['uberftp', '-mkdir', transfer_dir], stdout=subprocess.PIPE)
			to_pack      = instrument_table + ' ' + antenna_table + ' ' + field_table
			filename     = working_directory + '/instruments_' + obsid + '_' + sbnumber + '.tar'
			transfer_fn  = transfer_dir + '/' + filename.split('/')[0]
			pool2.apply_async(pack_and_transfer, args = (item['value'], filename, to_pack, working_directory + '/pipeline', transfer_fn, working_directory, observation, server, user, password, database,))
			pass
		pool2.close()
		pool2.join()
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
	#tokens.del_view(view_name='pref_targ2')
	
	## add views for users
	tokens.add_view(v_name='downloading', cond=' doc.status == "downloading" ')
	tokens.add_view(v_name='unpacking',   cond=' doc.status == "unpacking" ')
	tokens.add_view(v_name='unpacked',    cond=' doc.status == "unpacked" ')
	tokens.add_view(v_name='submitted',   cond=' doc.status == "submitted" ')
	tokens.add_view(v_name='processing',  cond=' doc.status == "processing" ')
	tokens.add_view(v_name='processed',  cond=' doc.status == "processed" ')
	tokens.add_view(v_name='transferring',  cond=' doc.status == "transferring" ')
	tokens.add_view(v_name='done', cond=' doc.done > 0  && doc.output == 0')	
	
	## check for new data sets and get information about other tokens present
	list_error     = tokens.list_tokens_from_view('error')     # check which tokens of certain type show errors
	list_locked    = tokens.list_tokens_from_view('locked')    # check which tokens of certain type are in the locked state
	list_done      = tokens.list_tokens_from_view('done')      # check which tokens of certain type are done
	list_todos     = tokens.list_tokens_from_view('todo')      # check which tokens of certain type are in the todo state
		
	## check which pipelines are locked, done or show errors
	try:
		locked_pipelines    = get_pipelines(tokens, list_locked)
		bad_pipelines       = get_pipelines(tokens, list_error)
		pipelines_done      = get_pipelines(tokens, list_done)
		pipelines_todo      = get_pipelines(tokens, list_todos)
	except TypeError:
		logging.error('\033[31mCould not find a corresponding token for the last observation \033[35m' + observation + '\033[31m. Please check the database for errors or remove the last observation.')
		return 1
		pass
	
	## lock program
	subprocess.Popen(['touch', lock_file])
	
	## check pipelines to run
	pipelines = sorted(list(set(locked_pipelines) - set(pipelines_done) - set(pipelines_todo)))

	## check what to download
	if len(list_todos) > 0:
		(list_todownload, download_list) = prepare_downloads(tokens, list_todos, pipelines_todo[0])
		gsilist   = download_list.gsi_links() # convert the srm list to a GSI list (proper URLs for GRID download)
		gsilist   = sorted(list(set(gsilist))) # to re-reverse the list in order to match it for the upcoming loop and use only distinct files
		pool      = multiprocessing.Pool(processes = multiprocessing.cpu_count())
		for url, item in zip(gsilist, list_todownload):
			pool.apply_async(download_data, args = (url, item['value'], working_directory, observation, server, pc.user, pc.password, pc.database,))
			pass
		pool.close()
		pass
	
	## check which pipelines are done and if observation is finished
	if len(pipelines_done) > 0:
		logging.info('\033[0mPipeline(s) \033[35m' + str(pipelines_done) + ' \033[0m are done.')
		if len(pipelines) == 0 and len(pipelines_todo) == 0:
			logging.info('\033[0mObservation \033[35m' + observation + ' \033[0m is done.')
			subprocess.Popen(['touch', done])
			pass
		pass
	
	## main pipeline loop
	for pipeline in pipelines:
		tokens.add_view(v_name=pipeline, cond=' doc.PIPELINE == "' + pipeline + '" ')                                        ## select all tokens of this pipeline
		tokens.add_view(v_name='temp',   cond=' doc.PIPELINE == "' + pipeline + '" && (doc.output < 20 |  doc.output > 22)')  ## select only tokens without download/upload error
		tokens.add_view(v_name='temp2',  cond=' doc.PIPELINE == "' + pipeline + '" && (doc.output > 19 && doc.output < 23)')  ## select only tokens with    download/upload error
		list_pipeline_all      = tokens.list_tokens_from_view(pipeline)  ## get the pipeline list
		list_pipeline          = tokens.list_tokens_from_view('temp')    ## get the pipeline list without download errors
		list_pipeline_download = tokens.list_tokens_from_view('temp2')   ## get the pipeline list with    download errors
		status = pipeline_status(tokens, list_pipeline_all)
		output = pipeline_output(tokens, list_pipeline_all)
		if pipeline in bad_pipelines:
			logging.warning('\033[33mPipeline \033[35m' + str(pipeline) + '\033[33m show errors. Please check their token status.')
			if len(list_pipeline_download) > error_tolerance:        ## count download errors and check whether there are too many
				logging.warning('\033[33mPipeline \033[35m' + str(pipeline) + '\033[33m shows more than ' + str(error_tolerance) + ' errors. Script will try to rerun them.')
				for item in list_pipeline_download:
					unlock_token(tokens, item['value'])
					pass                                    
				break
				pass
			pass
		if len(status) > 1:
			logging.warning('\033[33mPipeline \033[35m' + pipeline + '\033[33m shows more than one status: \033[35m' + str(status) + '\033[33m. Script will try to proceed.')
			pass
		if 'submitted' in status:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m has already been submitted.')
			if log_information == 'processing':
				for item in list_pipeline:
					set_token_status(tokens, item['value'], 'processing')
					set_token_output(tokens, item['value'], 0)
					set_token_progress(tokens, item['value'], 0)
					pass
			elif log_information != '':
				submit_error_log(tokens, list_pipeline, slurm_log, log_information, working_directory)
				pass
			break
			pass
		elif 'processing' in status:
			if log_information == 'processing':
				logging.info('Pipeline \033[35m' + pipeline + '\033[32m is currently processed.')
				pass
			elif log_information != '':
				submit_error_log(tokens, list_pipeline, slurm_log, log_information, working_directory)
				pass
			break
			pass
		elif 'processed' in status or 31 in output:
			logging.info('\033[0mPipeline \033[35m' + pipeline + ' \033[0m for this observation has been processed.')
			tokens.add_view(v_name='temp', cond=' (doc.status == "processed" || doc.output == 31) && doc.PIPELINE == "' + pipeline + '" ')
			list_done = tokens.list_tokens_from_view('temp')
			submit_results(tokens, list_done, list_pipeline_all, working_directory, observation, server, pc.user, pc.password, pc.database)
			tokens.del_view(view_name='temp')
			continue
			pass
		elif 'unpacked' in status or 'queued' in status:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be started.')
			run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, ftp)
			break
			pass
		elif 99 in output or -1 in output or 3 in output:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be resumed.')
			run_prefactor(tokens, list_pipeline, working_directory, observation, submitted, slurm_files, pipeline, ftp)
			break
			pass
		elif len(list_pipeline_all) == 1 and output[0] == 20:
			logging.info('Pipeline \033[35m' + pipeline + '\033[32m will be started.')
			run_prefactor(tokens, list_pipeline_all, working_directory, observation, submitted, slurm_files, pipeline, ftp)
			break
			pass
		elif 20 in output or 21 in output or 22 in output:
			logging.warning('\033[33mAll necessary data for the pipeline \033[35m' + pipeline + '\033[33m are not yet available.')
			continue
			pass

		else:
			logging.warning('\033[33mPipeline \033[35m' + pipeline + '\033[33m has an invalid status. Script will proceed without it.')
			pass
		tokens.del_view(view_name='temp')
		tokens.del_view(view_name='temp2')    
		pass
	pass
	
      
	## last check
	if len(pipelines) == 0 and len(pipelines_todo) == 0:
		logging.info('\033[0mNo tokens found in database to be processed.')
		pass
	 
	## wait for processes to be finished
	try:
		pool.join()
		pass
	except UnboundLocalError:
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
	main(options.server, options.ftp)
	
	# monitoring has been finished
	logging.info('\033[30;4mMonitoring has been finished.\033[0m')
	
	sys.exit(0)
	pass
    