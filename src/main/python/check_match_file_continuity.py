"""
Checks that the match files with name that include 
matches_<start_seq_num>-<end_seq_num> covers continious match sequence
numbers. 

Ex:
hadoop fs -ls hdfs://path/to/dota/files | python check_match_file_continuity.py
"""

import sys
from operator import itemgetter

if __name__ == "__main__":
  times = []
  for line in sys.stdin:
    i = line.rfind("matches_")
    if(i != -1):
      start, end = map(int, line[(i + len("matches_")):len(line) - 1].rstrip(".gz").split("-"))
      times.append((start, end))
  times.sort()
  for i in range(1, len(times)):
    prev_start, prev_end = times[i - 1]
    cur_start, cur_end = times[i]
    if(prev_end != cur_start):
      print("Misalignment %d to %d" % (prev_end, cur_start))
