"""
Script used strictly for testing, forces some json encoded matches to have 
common account_ids and play heroes from either a more strict pool.
Run the same way add_account_ids.py is.
Useful for testing hero win rates and correlations statistics.
"""

from random import sample
import sys
import gzip
from collections import defaultdict
try:
  import simplejson as json
except ImportError:
  import json


POOLS = [range(1, 50), range(45,52)]
NEW_IDS = range(0, 300)


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

  lines = []
  num_lines = 0
  for line in in_lines:
    num_lines += 1
    line = line.rstrip()
    obj = json.loads(line)
    players = obj["players"]
    ids = sample(NEW_IDS, len(players))
    for i in xrange(len(players)):
      if(ids[i] < 50):
        pool = POOLS[1]
      else:
        pool = POOLS[0]
      players[i]["hero_id"] = sample(pool, 1)[0]
      players[i]["account_id"] = ids[i]
    out.write(json.dumps(obj) + "\n")
  in_lines.close()
  out.close()

  print("Adjusted %d lines" % num_lines)
