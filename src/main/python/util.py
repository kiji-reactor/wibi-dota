from os import listdir
from os.path import isfile

def get_lines(folder_name):
  for file_name in listdir(folder_name):
    file_name = folder_name + "/" + file_name
    if(isfile(file_name) and not file_name.startswith("_")):
      for line in open(file_name):
        yield line
