#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Surveys KSP monitoring script -- more details available later
"""

import os, sys, glob
import logging
import resource
import optparse

import time, datetime
import subprocess
import filecmp

from GRID_LRT.get_picas_credentials import picas_cred
from GRID_LRT import Token
from GRID_LRT.Staging import srmlist

_version = '0.1beta'
observation = 'test_L556782'

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

def lock_token(tokens, token_value):
	
	token = tokens.db[token_value]
	token['lock'] = time.time()
	tokens.db.update([token])
	logging.info('Token \033[35m' + token_value + '\033[32m has been locked.')
	pass

def set_token_status(tokens, token_value, status):
	
	token = tokens.db[token_value]
	token['status'] = status
	tokens.db.update([token])
	logging.info('Status of token \033[35m' + token_value + '\033[32m has been set to \033[35m' + status + '\033[32m.')
	pass
      
def set_token_output(tokens, token_value, output):
	
	token = tokens.db[token_value]
	token['output'] = output
	tokens.db.update([token])
	logging.info('Output of token \033[35m' + token_value + '\033[32m has been set to \033[35m' + str(output) + '\033[32m.')
	pass


def unpack_data(tokens, token_value, working_directory):
  
	token = tokens.db[token_value]
	observation_id = token['OBSID']
	unpack_filename = glob.glob(working_directory + '/*' + observation_id + '*.tar')

	if len(unpack_filename) > 1:
		logging.warning('\033[33m Detected duplicate files with the same token. Will unpack only one.')
		pass
	set_token_status(tokens, token_value, 'unpacking')
	
	try:
		os.chdir(working_directory)
		unpack = subprocess.Popen(['tar','xfv',unpack_filename[0]], stdout=subprocess.PIPE)
		errorcode = unpack.wait()
		if errorcode == 0:
			set_token_status(tokens, token_value, 'unpacked')
			os.remove(unpack_filename[0])
			logging.info('File \033[35m' + unpack_filename[0] + '\033[32m was removed.')
			pass
		else:
			set_token_status(tokens, token_value, 'unpacking failed, error code: ' + str(int(errorcode)))
			pass
		pass
	except (OSError) as exception:
		logging.error('\033[31mException raised: ' + str(exception))
		logging.warning('\031[31mUnpacking failed')
		set_token_status(tokens, item['value'], 'unpacking failed, exception raised: ' + str(exception))
		pass
	pass

def download_data(tokens, list_todos, working_directory):
	
	download_list = srmlist.srmlist()                   # create list to download
	for item in list_todos:
		lock_token(tokens, item['value'])
		srm = tokens.db.get_attachment(item['value'], 'srm.txt').read().strip()
		download_list.append(srm)
		pass
	
	#gsilist = download_list.gsi_links() # convert the srm list to a GSI list (proper URLs for GRID download)
	httplist = download_list.http_links()
	httplist = list(reversed(list(httplist))) # to re-reverse the list in order to match it for the upcoming loop
	#for item in gsilist:
	for url,item in zip(httplist,list_todos):
		set_token_status(tokens, item['value'], 'downloading')
		#download = subprocess.Popen(['globus-url-copy', item, '$WORK'])
		try:
			download = subprocess.Popen(['wget','-P',working_directory,url], stdout=subprocess.PIPE, shell=False)
			#(out, err) =  download.communicate()
			#logging.info(out)
			errorcode = download.wait()
			if errorcode == 0:
				set_token_status(tokens, item['value'], 'downloaded')
				set_token_output(tokens, item['value'], 0)
				unpack_data(tokens, item['value'], working_directory)
				pass
			else:
				set_token_status(tokens, item['value'], 'download failed, error code: ' + str(int(errorcode)))
				set_token_output(tokens, item['value'], 20)
				pass

			pass
		except (OSError) as exception:
			logging.error('\033[31mException raised: ' + str(exception))
			logging.warning('\033[31mDownload failed')
			set_token_status(tokens, item['value'], 'download failed, exception raised: ' + str(exception))
			pass
		pass
	return 0
	pass
   
def run_prefactor(tokens, list_todos, working_directory, ftp):
  
	logging.info('Downloading current prefactor version from \033[35m' + ftp)
	download = subprocess.Popen(['curl', ftp, '--output', working_directory + '/prefactor.tar'], stdout=subprocess.PIPE, shell=False)
	errorcode = download.wait()
	if errorcode != 0:
		set_token_status(tokens, item['value'], 'download of prefactor failed, error code: ' + str(int(errorcode)))
		logging.error('\033[31m Downloading prefactor has failed. Programm will be terminated.')
		for item in list_todos: 
			set_token_output(tokens, item['value'], -1)
			pass
		raise RuntimeError('Downloading prefactor has failed.')
		pass
	try:
		logging.info('Unpacking current prefactor version to \033[35m' + working_directory)
		os.chdir(working_directory)
		unpack = subprocess.Popen(['tar','xfv','prefactor.tar'])
		errorcode = unpack.wait()
		if errorcode != 0:
			for item in list_todos:
				set_token_status(tokens, item['value'], 'unpacking of prefactor failed, error code: ' + str(int(errorcode)))
				set_token_output(tokens, item['value'], -1)
				pass
			logging.error('\033[31m Unpacking prefactor has failed. Programm will be terminated.')
			raise RuntimeError('Unpacking prefactor has failed.')
			pass
	except (OSError) as exception:
		logging.error('\033[31mException raised: ' + str(exception))
		logging.warning('\033[31mUnpacking of prefactor failed')
		for item in list_todos:
			set_token_status(tokens, item['value'], 'unpacking of prefactor failed, exception raised: ' + str(exception))
			set_token_output(tokens, item['value'], -1)
			pass
		pass
	logging.info('Getting pipeline parset file for \033[35m' + observation)
	for item in list_todos:
  		if os.path.isfile('pipeline.parset'):
			tokens.get_attachment(item['value'],'Pre-Facet-Calibrator-1.parset','__pipeline__.parset')
			if not filecmp.cmp('pipeline.parset', '__pipeline__.parset'):
				logging.error('\033[31mParset file mismatches for: \033[35m' + item['value'])
				set_token_status(tokens, item['value'], 'parset file mismatch')
				set_token_output(tokens, item['value'], -1)
				pass
			os.remove('__pipeline__.parset')
			pass
		else:
			tokens.get_attachment(item['value'],'Pre-Facet-Calibrator-1.parset','pipeline.parset')
			pass
		pass
	os.system('sed -i "s/PREFACTOR_SCRATCH_DIR/\$PREFACTOR_SCRATCH_DIR/g" pipeline.parset')
	pass

def main(server='https://picas-lofar.grid.sara.nl:6984', ftp='ftp://ftp.strw.leidenuniv.nl/pub/apmechev/sandbox/SKSP/prefactor/pref_cal1.tar'):
	
	## load working environment
	working_directory = os.environ['WORK']
	logging.info('\033[0mWorking directory is ' + working_directory)
	
	## load PiCaS credentials
	logging.info('\033[0mConnecting to server: ' + server)
	pc = picas_cred()
	pc.get_picas_creds_from_file()
	logging.info('Username: \033[35m' + pc.user)
	logging.info('Database: \033[35m' + pc.database)
	
	tokens        = Token.Token_Handler( t_type=observation, srv=server, uname=pc.user, pwd=pc.password, dbn=pc.database) # load token of certain type
	tokens.reset_tokens('done')
	tokens.reset_tokens('locked')
	list_todos    = tokens.list_tokens_from_view('todo') # check which tokens of certain type are in the todo state
	
	if len(list_todos) == 0:
		logging.info('\033[0mNo tokens found in database to be processed.')
		return None
		pass
	
	#download_data(tokens, list_todos, working_directory)
	run_prefactor(tokens, list_todos, working_directory, ftp)

	
	pass

if __name__=='__main__':
	# Get command-line options.
	opt = optparse.OptionParser(usage='%prog ', version='%prog '+_version, description=__doc__)
	opt.add_option('-s', '--server', help='PiCaS server URL:port', action='store_true', default='https://picas-lofar.grid.sara.nl:6984')
	opt.add_option('-f', '--ftp', help='FTP server hosting current prefactor version', action='store_true', default='ftp://ftp.strw.leidenuniv.nl/pub/apmechev/sandbox/SKSP/prefactor/pref_cal1.tar')
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
    