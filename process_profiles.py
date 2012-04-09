#!/usr/bin/env python
""" Process all profiles in the config dir for the given timeframe
    For each profile in the web directory call the appropriate pig/jython script to process the logs.
    When this is finished use matplotlib to create graphs for all matching graph templates.

    Each report has a template in graph_scripts that defines the graph to be run. These templates
    take as input the report name, report dir and graph dir. I run these in parallel.
   
    All .py files in the profile directory are assumed to be profiles.
    Jython is quite limited in what python modules it supports so this parent script
    must be run in actual python, rather than pigs embeded python.
"""

from datetime import datetime
from glob import glob
import imp
from multiprocessing import Process
import os
import shutil
import subprocess
import sys
import tempfile

#Depending on your setup you may need to specify the environment variable JYTHON_HOME
JYTHON_HOME = '/usr/share/jython'


def main(argv=None):
    if argv is None:
        argv = sys.argv
    if len(argv) != 3:
        print "Usage: " + argv[0] + " <profile directory> <daily|weekly|monthly>"
        return 1

    profile_dir = argv[1]
    timeframe = argv[2]
    code_dir = os.path.dirname(argv[0])

    #Set the common environment variables needed for the jython scripts
    os.environ['timeframe'] = timeframe
    os.environ['JYTHON_HOME'] = JYTHON_HOME

    profiles = glob(os.path.join(profile_dir, '*.py'))
    for profile_file in profiles:
        os.environ['config_file'] = profile_file
        #!!I plan to support different types of profiles but for now it is just web_process.py
        profile_processor = 'web_process.py'
        try:
            subprocess.check_output(['pig', '-l', '/var/log/pig', '-f', \
                profile_processor], cwd=code_dir)
        except subprocess.CalledProcessError, err:
            print 'Error running profile %s for the timeframe %s, skipping stats generation.' \
                % (profile_file, timeframe)
            print 'Error output: %s' % err.output
            continue
        else:
            #Load the config
            profile = {}
            execfile(profile_file, {'timeframe':timeframe}, profile)

            if timeframe == 'daily':
                purge_old_logs(profile['oldest_log'], os.path.dirname(profile['LOGDIR']))

            generate_graphs(profile['graph_dir'], profile['REPORTDIR'], \
                os.path.join(code_dir, 'graph_scripts'))

def generate_graphs(graph_dir, report_dir, graph_scripts):
    """Generate graphs for for all report types found both in the report_dir and the report scripts dir.
        Store the graphs in the graph_dir
        report_dir is in hdfs, graph_dir isn't
        I use multiprocessing to get many reports running at once.
    """
    if not os.path.exists(graph_dir):
        os.makedirs(graph_dir)

    temp_dir = tempfile.mkdtemp()
    #I copy all reports locally to make them easier to work with
    try:
        subprocess.check_output(['hadoop', 'fs', '-get', report_dir, temp_dir])
    except subprocess.CalledProcessError, err:
        print 'Error retrieving reports from hdfs for %s' % report_dir
        print 'Error output: %s' % err.output
        return

    local_report_dir = glob(temp_dir + '/*')[0]
    reports = os.listdir(local_report_dir)
    template_list = os.listdir(graph_scripts)
    started = []
    for report in reports:
        if (report + '.py') in template_list:
            report_mod = imp.load_source(report, os.path.join(graph_scripts, report + '.py'))
            #templates take the name, report_dir, and graph dir as args
            #since the command line always supplies the command as argv[0] I do also.
            argv = (report + '.py', report, os.path.join(local_report_dir, report), graph_dir)
            new_process = Process(target=report_mod.main, kwargs={'argv': argv})
            new_process.start()
            started.append(new_process)

    for running in started:
        running.join()

    #cleanup my dir
    shutil.rmtree(temp_dir)

def purge_old_logs(oldest_log, log_base_dir):
    """ Clean out any subdirs in hdfs log_base_dir that are older than oldest_log.
        The subdir names are assumed to follow the pattern %Y_%m_%d
    """
    try:
        output = subprocess.check_output(['hadoop', 'fs', '-ls', log_base_dir])
    except subprocess.CalledProcessError, err:
        print 'Error with ls in hdfs for %s' % log_base_dir
        print 'Error output: %s' % err.output
        return

    #Parse the hadoop ls output to get the current log dirs
    dirs = []
    for line in output.splitlines():
        split = line.split('/', 1)
        if len(split) == 2:
            dirs.append(os.path.basename(split[1]))

    for log_dir in dirs:
        log_date = datetime.strptime(log_dir, '%Y_%m_%d')
        if log_date > oldest_log: 
            try:
                output = subprocess.check_output(['hadoop', 'fs', '-rmr', log_dir])
            except subprocess.CalledProcessError, err:
                print 'Error with rmr in hdfs for %s' % log_dir
                print 'Error output: %s' % err.output


if __name__ == "__main__":
    sys.exit(main())
