#!/usr/bin/env python
""" Web process
    Using pig to process a set of web logs for the given profile in either a daily, weekly or monthly fashion.
    The profile file is a python config file, see profile-example.py
    I assume all the pig files are in the same dir as this script.
    See http://ofps.oreilly.com/titles/9781449302641/embedding.html
"""

import os
import sys

from org.apache.pig.scripting import Pig
import org.apache.pig.tools.pigstats.SimplePigStats #For type comparison

def main(argv=None):
#Ideally I want to use arguments, ie 'pig -l /var/log/pig web_process.py /etc/rgpig/www.iresis.com.py daily'
#however it just doesn't work, I'm not sure why the code has been applied in my version, and I can get it to
#work with a test .py that only has two lines, import sys, and print sys.argv. Here is the case
#https://issues.apache.org/jira/browse/PIG-2548
#    if argv is None:
#        argv = sys.argv
#    if len(argv) != 3:
#        print "Usage: " + argv[0] + " <profile config> <daily|weekly|monthly>"
#        return 1
#
#    profile_file = argv[1]
#    timeframe = argv[2]
    
    profile_file = os.environ['config_file']
    timeframe = os.environ['timeframe']

    if not (timeframe == 'daily' or timeframe == 'weekly' or timeframe == 'monthly'):
        print 'The time frame must be either daily, weekly or monthly.'
        return 1

    #Load the config
    profile = {}
    execfile(profile_file, {'timeframe':timeframe}, profile)

    #Clean up incomplete runs and create dir
    Pig.fs('rmr ' + profile['REPORTDIR'])
    Pig.fs('mkdir ' + profile['REPORTDIR'])

    #Start pig processing
    pig_init()
    if timeframe == 'daily':
        #Clean up incomplete runs and create dir
        Pig.fs('rmr %s' % profile['LOGDIR'])
        Pig.fs('mkdir %s' % profile['LOGDIR'])
        import_logs(profile['logs'])
    #The web_load.pig script is run by the processing scripts
    pstats = Pig.compileFromFile('web_%s.pig' % timeframe)
    bstats = pstats.bind(profile)
    stats = bstats.run()
    if isinstance(stats, org.apache.pig.tools.pigstats.SimplePigStats):
        if not stats.isSuccessful():
            print 'Error in web log stats, %s' % run.getErrorMessage()
            sys.exit(1)
    else:
        for run in stats:
            if not run.isSuccessful():
                print 'Error in web log stats, %s' % run.getErrorMessage()
                sys.exit(1)

def import_logs(profile):
    """ Import all the log files for a given day and processed them putting each in a log dir.
        If the profile is a list there are multiple files otherwise only a single one.
        The files are combined when running web_load.pig
    """
    #Clean up any left over files from the last run
    for logfile in profile:
        Pig.fs('rmr %s/%s' % (logfile['TMPDIR'], logfile['NAME']))
    pload = Pig.compileFromFile('web_import.pig')
    bload = pload.bind(profile)
    load = bload.run()
    #Check for load errors
    if isinstance(load, org.apache.pig.tools.pigstats.SimplePigStats):
        if not load.isSuccessful():
            print 'Error in web log load, %s' % load.getErrorMessage()
            sys.exit(1)
    else:
        for run in load:
            if not run.isSuccessful():
                print 'Error in web log load, %s' % run.getErrorMessage()
                sys.exit(1)

def pig_init():
    """ Setup the pig settings used for all runs."""
    #I'm using the piggybank from s3://elasticmapreduce/libs/pig/0.9.1/piggybank-0.9.1-amzn.jar
    Pig.registerJar('/usr/share/pig/contrib/piggybank/piggybank.jar')

    Pig.define('DATE_TIME', 'org.apache.pig.piggybank.evaluation.datetime.DATE_TIME()')
    Pig.define('EXTRACT', 'org.apache.pig.piggybank.evaluation.string.EXTRACT()')
    Pig.define('FORMAT', 'org.apache.pig.piggybank.evaluation.string.FORMAT()')
    Pig.define('FORMAT_DT', 'org.apache.pig.piggybank.evaluation.datetime.FORMAT_DT()')
    Pig.define('REPLACE', 'org.apache.pig.piggybank.evaluation.string.REPLACE()')

    #The box I use is dedicated to pig so use a lot of memory
    Pig.set('pig.cachedbag.memusage', '0.6')
    #Pig.set('default_parallel', '10')
    #Pig.set('io.sort.mb', '2048')

if __name__ == "__main__":
    main()
