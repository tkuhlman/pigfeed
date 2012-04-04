""" Graph the top 50 ips.
    I assume only one part* file in the report_dir
"""

import csv
from glob import glob
import os
import shutil
import sys

import matplotlib
matplotlib.use('cairo', warn=False) #Set a non-interactive backend
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

    if len(ips) > 25:
        length = 25
    else:
        length = len(ips)

    fig = pylab.figure(1)
    pos = pylab.arange(length) + .5
    pylab.barh(pos, requests[:length], align='center', aa=True, ecolor='r')
    pylab.yticks(pos, ips[:length])
    pylab.xlabel('Requests')
    pylab.title('Top %d ips ordered by # of requests' % length)
    pylab.grid(True)

    #Save the figure
    pylab.savefig(os.path.join(graph_dir, report + '.png'), bbox_inches='tight', pad_inches=1)

if __name__ == "__main__":
    sys.exit(main())
