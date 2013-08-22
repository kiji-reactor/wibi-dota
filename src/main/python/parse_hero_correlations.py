"""
Script for parsing and sorting hero correlation stats. Pass in the name 
of a folder with the correlation stored in (hero_id, hero_id, value) triples
as outputted by a hadoop job.
"""

import sys
import util
import dota_values
from operator import itemgetter

def get_corr(folder_name):
  correlations = []
  for line in util.get_lines(folder_name):
    parts = line.rstrip().split(",")
    if(parts[2] != "NaN"):
      correlations.append((dota_values.get_hero_name(int(parts[0]), True),
                           dota_values.get_hero_name(int(parts[1]), True),
                           float(parts[2])))
  return correlations

if __name__ == "__main__":
  folder_name = sys.argv[1]  
  corrs = get_corr(folder_name)
  corrs.sort(key = lambda x : abs(x[2]))
  print("\n".join(map(lambda x : ",".join(map(str, x)), corrs)))
