#!/usr/bin/env python2

from GRID_LRT.Staging import srmlist

from cloudant.client import CouchDB
from GRID_PiCaS_Launcher.get_picas_credentials import PicasCred
from GRID_LRT.token import caToken, TokenList, TokenView, TokenJsonBuilder

import time
import logging
import os

server         = "https://picas-lofar.grid.surfsara.nl:6984"

def set_token_status(token, status):
	
	times = token['times']
	
	if type(times) is not dict:
		times = {}
		pass
	
	times[status]   = time.time()
	token.fetch()
	token['times']  = times
	token['status'] = status
	token.save()
	print('Status of token \033[35m' + token['_id'] + '\033[32m has been set to \033[35m' + status + '\033[32m.')

def undone_token(token):
	
	token['done'] = 0
	token.save()
	logging.info('Token \033[35m' + token['_id'] + '\033[32m is undone.')
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

def unlock_token(token):
	
	token['lock'] = 0
	token.save()
	print('Token \033[35m' + token['_id'] + '\033[32m has been unlocked.')
	pass

observation = 'pref3_targ_P126_02_L745146'
home_directory    = os.environ['PROJECT_chtb00'] + '/htb006'
pc     = PicasCred(home_directory + '/.picasrc')
client   = CouchDB(pc.user, pc.password, url = server, connect = True)
db = client[pc.database]
tokens = TokenList(database = db, token_type = observation)

#tokens._design_doc.delete_view('overview_total')
#tokens._design_doc.delete_view('pipeline_todo')

list_p = tokens.list_view_tokens('pref3_targ1')
#list_p = tokens.list_view_tokens('pref3_cal')

#for item in list_p:
    #item.delete()
    #undone_token(item)
    #set_token_status(item, 'processed')
    
#list_p = tokens.list_view_tokens('pref3_targ1')

for item in list_p:
    #item.delete()
    #undone_token(item)
    unlock_token(item)
    #lock_token(item)
    #lock_token_done(item)
    #set_token_status(item, 'transferred')
    #set_token_status(item, 'submitted')
    set_token_status(item, 'queued')
