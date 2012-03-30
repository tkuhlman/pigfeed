""" Graph the top 50 ips.
    I assume only one part* file in the report_dir
"""

import csv
from glob import glob
import os
import shutil
import sys

import matplotlib
matplotlib.use('cairo') #Set a non-interactive backend
from matplotlib import pylab

def main(argv=None):
    if argv is None:
        argv = sys.argv
    if len(argv) != 4:
        print "Usage: " + argv[0] + " <report name> <report dir> <graph dir>"
        return 1

    report = argv[1]
    report_dir = argv[2]
    graph_dir = argv[3]

    file_list = glob(os.path.join(report_dir, 'part*'))
    #Copy the raw data file to the graph_dir
    raw_file = os.path.join(graph_dir, report + '.tsv')
    shutil.copyfile(file_list[0], raw_file)

    #Process the file into a graph, ideally I would combine the two into one but for now I'll stick with two
    data_file = csv.DictReader(open(raw_file, 'rb'), fieldnames = ['IP', 'Requests', 'Bytes'], delimiter="\t")
    ips = []
    requests = []
    num_bytes = []
    for row in data_file:
        ips.append(row['IP'])
        requests.append(int(row['Requests']))
        num_bytes.append(int(row['Bytes']))

    fig = pylab.figure(1)
    pos = pylab.arange(25) + .5
    pylab.barh(pos, requests[:25], align='center', aa=True, ecolor='r')
    pylab.yticks(pos, ips[:25])
    pylab.xlabel('Requests')
    pylab.title('Top 25 ips ordered by # of requests')
    pylab.grid(True)

    #Save the figure
    pylab.savefig(os.path.join(graph_dir, report + '.png'), bbox_inches='tight', pad_inches=1)

    #bytes listed 
    fig = pylab.figure(2)
    pos = pylab.arange(25) + .5
    pylab.barh(pos, num_bytes[:25], align='center', aa=True, ecolor='r')
    pylab.yticks(pos, ips[:25])
    pylab.xlabel('Bytes')
    pylab.title('Bytes of the top 25 ips ordered by # of requests')
    pylab.grid(True)

    #Save the figure
    pylab.savefig(os.path.join(graph_dir, report + '_bytes.png'), bbox_inches='tight', pad_inches=1)

if __name__ == "__main__":
    sys.exit(main())
