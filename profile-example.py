""" An example profile config file to be used with web-process.py
    The variables defined here become the params for pig runs.
    This script assumes that timeframe is defined as daily|weekly|monthly when this is called.
    In the case of a daily run, the variable logs is used as its own params list or dictionary
    for the loading of the web log files.
    Upper case variables are used as params, lowercase are not expected to be
    For the cases of multiple Log files being loaded LOGDIR should include them all.
"""

from datetime import datetime, timedelta
from glob import glob
import os

TMPDIR = '/tmp'
SITE = 'www.domain.com'
profile_dir = '/profiles/%s' % SITE

#date is generated based on the timeframe
if timeframe == 'daily':
    yesterday = datetime.today() - timedelta(1)
    date = yesterday.strftime('%Y_%m_%d')
    LOGDIR = '%s/logs/%s' % (profile_dir, date)
elif timeframe == 'weekly':
    yesterday = datetime.today() - timedelta(1)
    offset = int(yesterday.strftime('%w'))
    sunday = yesterday - timedelta(offset)
    week = [sunday.strftime('%Y_%m_%d'), ]
    for days in range(1, 7):
        week.append((sunday + timedelta(days)).strftime('%Y_%m_%d'))
    #Set the date to week-%Y_%m_%d where the day is the Sunday
    date = 'week-%s' % sunday.strftime('%Y_%m_%d')
    LOGDIR = '%s/logs/{' % profile_dir
    for day in week:
        LOGDIR += '%s,' % day
    LOGDIR = LOGDIR[:-1] + '}'
elif timeframe == 'monthly':
    #Set the date to month-%Y_%m of the previous month
    today = datetime.today()
    month = today.replace(month=today.month() - 1)
    date = 'month-%s' % month.strftime('%Y_%m_%d')
    LOGDIR = '%s/logs/%s*' % (profile_dir, month)

REPORTDIR = '%s/reports/%s/%s' % (profile_dir, timeframe, date)
graph_dir = '/var/www/html/pig/%s/%s/%s' % (SITE, timeframe, date)

##Daily only needs to process the raw logs
if timeframe == 'daily':
    oldest_log = datetime.today() + timedelta(35) #Delete logs older than this

    logs = []
    # For each log directory find the log and add it to the logs params
    for raw_dir in glob('/home/stats/web?.viawest.iresis.com'):
        log_dict = {'LOGDIR': LOGDIR, 'TMPDIR': TMPDIR, 'SITE': SITE}
        log_dict['INPUT'] = os.path.join(raw_dir, 'access_log_%s.gz' % date)
        #Compression will be used if the files end in .gz
        log_dict['NAME'] = '%s.gz' % os.path.basename(raw_dir)
        logs.append(log_dict)

