#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
See http://www.astron.nl/citt/genericpipeline/ for further information on parsets.
"""

import os, sys
import logging
import resource
import optparse

_version = '1.0'

os.system('clear')
print '\033[30;1m################################################'
print '## LOFAR HBA calibration and imaging pipeline ##'
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
   
    
def create_pipeline_config(working_directory):
     
	try:
		default_config      = os.environ['LOFARROOT'] + '/share/pipeline/pipeline.cfg'
		default_lofarroot   = os.popen('grep lofarroot          ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_casaroot    = os.popen('grep casaroot           ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_pyraproot   = os.popen('grep pyraproot          ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_hdf5root    = os.popen('grep hdf5root           ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_wcsroot     = os.popen('grep wcsroot            ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_pythonpath  = os.popen('grep pythonpath         ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_runtime     = os.popen('grep runtime_directory  ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_recipe      = os.popen('grep recipe_directories ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_working     = os.popen('grep working_directory  ' + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_clusterdesc = os.popen('grep clusterdesc '        + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_logfile     = os.popen('grep log_file '           + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_xml         = os.popen('grep xml_stat_file '      + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		default_tasks       = os.popen('grep task_files '         + default_config + ' | cut -f2- -d"="').readlines()[0].rstrip('\n').replace(' ','')
		pipeline_cfg        = working_directory + '/pipeline.cfg'
		plugins_directory   = os.popen('find ' + working_directory           + ' -type d | grep plugins').readlines()[0].rstrip('\n').replace(' ','')
		recipes_directory   = plugins_directory[:plugins_directory.find(plugins_directory.split('/')[-1])]
		with open(pipeline_cfg, 'w') as outfile:
			with open(default_config, 'r') as infile:
				for line in infile:
					outfile.write(line.replace(default_runtime, working_directory)\
							  .replace(default_lofarroot, os.environ['LOFARROOT'])\
							  .replace(default_casaroot, '')\
							  .replace(default_pyraproot, '')\
							  .replace(default_hdf5root, '')\
							  .replace(default_wcsroot, '')\
							  .replace(default_pythonpath, os.environ['LOFARROOT'] + '/lib/python2.7/site-packages')\
							  .replace(default_working, '%(runtime_directory)s')\
							  .replace(default_recipe, default_recipe.rstrip(']') + ',' + recipes_directory + ']')\
							  .replace(default_clusterdesc, '%(lofarroot)s/share/local.clusterdesc')\
							  .replace(default_logfile, '%(runtime_directory)s/%(job_name)s/logs/%(start_time)s/pipeline.log')\
							  .replace(default_logfile, '%(lofarroot)s/share/pipeline/tasks.cfg')\
							  .replace(default_xml, '%(runtime_directory)s/%(job_name)s/logs/%(start_time)s/statistics.xml'))
					pass

		try:
			max_per_node    = os.popen('nproc').readlines()[0].rstrip('\n')
			os.system('echo >> '                                     + pipeline_cfg)
			os.system('echo [remote] >> '                            + pipeline_cfg)
			os.system('echo method = slurm_srun      >> '            + pipeline_cfg)
			os.system('echo max_per_node = ' + max_per_node + ' >> ' + pipeline_cfg)
			pass
		except IndexError:
			logging.error('The number of available CPUs could not be determined. Please check your installation of nproc.')
			sys.exit(1)
			pass

	except IOError or IndexError:
		logging.error('LOFAR pipeline configuration not found. Please check your installation.')
		sys.exit(1)
		pass
	
	infile.close()
	outfile.close()
	
	pass
            
if __name__=='__main__':
	# Get command-line options.
	opt = optparse.OptionParser(usage='%prog <pipeline.parset> <output_directory> ', version='%prog '+_version, description=__doc__)
	opt.add_option('-c', '--clobber', help='clobber output directory', action='store_true', default=False)
	(options, args) = opt.parse_args()

	logging.root.setLevel(logging.INFO)
	log    = logging.StreamHandler()
	format = logging.Formatter('\033[1m%(levelname)s\033[0m: %(message)s')
	log.setFormatter(format)
	log.emit = add_coloring_to_emit_ansi(log.emit)
	
	logging.root.addHandler(log)
  
	# Get inputs
	if len(args) != 2:
		logging.error('Wrong number of arguments.')
		opt.print_help()
		sys.exit(1)
		pass
	logging.info('Checking pipeline parset: \033[34m' + args[0])
	if not os.path.isfile(args[0]):
		logging.error('Pipeline parset does not exist.')	        
		sys.exit(1)
		pass
	working_directory = args[1].rstrip('.').rstrip('/')
	logging.info('Checking working directory: \033[34m' + working_directory)
	if os.path.isdir(working_directory) and not options.clobber:
		prompt = "\033[1;35mWARNING\033[0m: Output directory already exists. Press enter to clobber or 'q' to quit : "
		answer = raw_input(prompt)
		while answer != '':
			if answer == 'q':
				sys.exit(0)
				pass
			answer = raw_input(prompt)      
			pass
		#logging.info('Cleaning working directory \033[5m...')
		#os.system('rm -rfv ' + working_directory)
		#pass
	os.system('mkdir -pv ' + working_directory)
	
	# checking number of files limit
	try:
		nof_files_limits = resource.getrlimit(resource.RLIMIT_NOFILE)
		logging.info('Setting limit for number of open files to: {}.'.format(nof_files_limits[1]))
		resource.setrlimit(resource.RLIMIT_NOFILE,(nof_files_limits[1],nof_files_limits[1]))
		nof_files_limits = resource.getrlimit(resource.RLIMIT_NOFILE)
		logging.info('Active limit for number of open files is {0}, maximum limit is {1}.'.format(nof_files_limits[0],nof_files_limits[1]))
		if nof_files_limits[0] < 2048:
			logging.error('The limit for number of open files is small, this could results in a "Too many open files" problem when running PiLL.')
			logging.error('The active limit can be increased to the maximum for the user with: "ulimit -Sn <number>" (bash) or "limit descriptors 1024" (csh).')
			pass
		pass
	except resource.error:
		logging.error('Cannot check limits for number of open files.')
		pass
	      
	
	# creating pipeline configuration
	create_pipeline_config(working_directory)
	logging.info('Created pipeline configuration file: \033[34m' + working_directory + '/pipeline.cfg')

	## creating pipeline parameter set
	#logging.info('Created pipeline parset file: \033[34m' + working_directory + '/pipeline.parset')

	# starting of generic pipeline
	logging.info('Calibration is starting \033[5m...')
	os.system('genericpipeline.py ' + args[0] + ' -d -c ' + working_directory + '/pipeline.cfg')
	
	# calibration has been finished
	logging.info('\033[30;4mCalibration has been finished.')
	
	sys.exit(0)
	pass
