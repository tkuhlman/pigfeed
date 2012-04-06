""" An example profile config file to be used with web-process.py
    The variables defined here become the params for pig runs.
    This script assumes that timeframe is defined as daily|weekly|monthly when this is called.
    In the case of a daily run, the variable logs is used as its own params list or dictionary
    for the loading of the web log files.
    Upper case variables are used as params, lowercase are not expected to be
"""

from datetime import datetime, timedelta
from glob import glob
import os

TMPDIR = '/tmp'
yesterday = datetime.today() - timedelta(1)
date = yesterday.strftime('%Y_%m_%d')
SITE = 'www.domain.com'
profile_dir = '/profiles/%s' % SITE
graph_dir = '/var/www/html/pig/%s/%s/%s' % (SITE, timeframe, date)

#REPORTDIR is needed for each timeframe and have timeframe as a subdir of reports

##Daily only
if timeframe == 'daily':
    logs = []
    LOGDIR = '%s/logs/%s' % (profile_dir, date)
    REPORTDIR = '%s/reports/daily/%s' % (profile_dir, date)
    oldest_log = datetime.today() + timedelta(35) #Delete logs older than this
    # For each log directory find the log and add it to the logs params
    for raw_dir in glob('/home/stats/web?.viawest.iresis.com'):
        log_dict = {'LOGDIR': LOGDIR, 'TMPDIR': TMPDIR, 'SITE': SITE}
        log_dict['INPUT'] = os.path.join(raw_dir, 'access_log_%s.gz' % date)
        #Compression will be used if the files end in .gz
        log_dict['NAME'] = '%s.gz' % os.path.basename(raw_dir)
        logs.append(log_dict)
