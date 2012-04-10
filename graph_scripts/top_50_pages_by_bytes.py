""" Graph the top 50 pages by bytes.
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
    data_file = csv.DictReader(open(raw_file, 'rb'), fieldnames = ['page', 'Bytes'], delimiter="\t")
    pages = []
    num_bytes = []
    for row in data_file:
        pages.append(row['page'])
        num_bytes.append(int(row['Bytes']))

    if len(pages) > 25:
        length = 25
    else:
        length = len(pages)

    fig = pylab.figure(1)
    pos = pylab.arange(length) + .5
    pylab.barh(pos, num_bytes[:length], align='center', aa=True, ecolor='r')
    pylab.yticks(pos, pages[:length])
    pylab.xlabel('Bytes')
    pylab.title('Top %d pages ordered by bytes' % length)
    pylab.grid(True)

    #Save the figure
    pylab.savefig(os.path.join(graph_dir, report + '.pdf'), bbox_inches='tight', pad_inches=2)

if __name__ == "__main__":
    sys.exit(main())
