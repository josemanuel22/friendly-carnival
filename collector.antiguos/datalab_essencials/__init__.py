
import sys
import os.path
import os
basepath = os.path.dirname(__file__)
sys.path.append(os.path.abspath(basepath))
print(basepath)

def insert_dir_in_path(parent_dirpath):
    for (dirpath, dirnames, filenames) in os.walk(parent_dirpath):
        for dirc in dirnames:
            sys.path.append(os.path.join(dirpath,dirc))


insert_dir_in_path(basepath)
