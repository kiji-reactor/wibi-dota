"""
WibiDota - parse_counts. Parses the field,value,start_rang,end_range,count files 
produced by express job MultHistogram. Formats the times, aggregates the values to
smaller intervals, and prints the data is csv form. Can also create a matlibplot
stacked area plot, but it does not look great. I have had better luck with open's offices
stacked area plot. 
"""

from os import listdir
from os.path import isfile
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from collections import defaultdict
from operator import itemgetter
import datetime

DATE_FORMAT = '%m-%d-%Y'
INTERVAL = 60*60*24*7 # One Week
TYPES = [str, str, int, int, int]
FIELD = 0
VALUE = 1
START = 2
END = 3
COUNT = 4

# TODO should rename kv_counts to something else 

def kv_counts_from_file(file_name, kv_counts):
  for line in open(file_name):
    counts = []
    for i, part in enumerate(line.rstrip().split(",")):
      counts.append(TYPES[i](part))
    if(counts[START] == 0):
      # Some matches have a start_time of 0, this is probably a bug at
      # Valve's end, currently we just ignore those values
      print("FOUND A ZERO VALUE, (line: " + line.rstrip() + " continuing")
      continue
    kv_counts[counts[FIELD]][counts[VALUE]].append((counts[START], counts[COUNT]))

def kv_counts_from_folder(folder_name):
  kv_counts = defaultdict(lambda : defaultdict(list))
  for file_name in listdir(folder_name):
    file_name = folder_name + "/" + file_name
    if(isfile(file_name) and not file_name.startswith("_")):
      kv_counts_from_file(file_name, kv_counts)
  return kv_counts

def print_csv(kv_counts):
  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():        
    x_values = x_lst
    print(kv_str + "," + ",".join(map(str, y_lst)))
  print("X," + ",".join(map(lambda x : (datetime.datetime.fromtimestamp(x).strftime(DATE_FORMAT)), x_values)))

def graph(kv_counts):
  for x_lst, y_lst in kv_counts.itervalues():
    y_values = np.zeros((len(kv_counts), len(x_lst)))
    break
  for i, (x_lst, y_lst) in enumerate(kv_counts.itervalues()):
    y_values[i] = y_lst
  fig = plt.figure()
  ax = fig.add_subplot(111)
  ax.stackplot(x_lst, y_values)
  plt.show()

def to_data_points(value_counts):
  max_x = -1
  min_x = None
  for value, data in value_counts.iteritems():
    x, _ = zip(*data)
    max_x = max(max_x, max(x))
    if(min_x == None):
      min_x = min(x)
    else:
      min_x = min(min_x, min(x))
  x_values = range(min_x, max_x, INTERVAL)

  all_data_points = {}
  for value, data in value_counts.iteritems():
    all_data_points[value] = ([], [])
    data_points = defaultdict(int)
    for start, count in data:
      data_points[(start - min_x) / INTERVAL] += count
    for x in x_values:
      all_data_points[value][0].append(x)
      all_data_points[value][1].append(data_points[(x - min_x) / INTERVAL])
  return all_data_points

if __name__ == "__main__":
  folder_name = sys.argv[1]
  kv_counts = kv_counts_from_folder(folder_name)    
  for field, value_counts in kv_counts.iteritems():
    data_points = to_data_points(value_counts)
    graph(data_points)

