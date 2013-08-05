"""
Script used strictly for testing, adds some common account_ids and 
staggers the start time of some json dota matches. Takes a file of dota_matches
in json form through standared in or as a second command line arguement identifying
a txt or gzip file and writes a new file with the adjusted values. Used
to generate data that ensures we get several players with multiple matches.
Useful for testing the dota_players table.
"""

from random import sample
import sys
import gzip
from collections import defaultdict
try:
  import simplejson as json
except ImportError:
  import json


NEW_IDS = range(0, 200)

if __name__ == "__main__":
  outfile = sys.argv[1]
  if(len(sys.argv) > 2):
    file_name = sys.argv[2]
    if(file_name.endswith(".gz")):
      in_lines = gzip.open(file_name)
    else:
      in_lines = open(file_name, 'r')
  else:
    in_lines = sys.stdin

  out = open(outfile, 'w')

  account_ids = defaultdict(int)
  lines = []
  start_time = 0
  for line in in_lines:
    line = line.rstrip()
    obj = json.loads(line)
    obj['start_time'] = start_time
    start_time += obj['duration'] + 600
    line_added = False
    players = obj["players"]
    ids = sample(NEW_IDS, 2)
    for i in xrange(min(len(players), 2)):
      players[i]["account_id"] = ids[i]
      account_ids[ids[i]] += 1
    for i in xrange(2, len(players)):
      players[i]["account_id"] = -1
    out.write(json.dumps(obj) + "\n")

  in_lines.close()
  out.close()
  
  print("Users: " + str(len(account_ids)))
  print("Matches: " + str(sum(account_ids.itervalues())))
