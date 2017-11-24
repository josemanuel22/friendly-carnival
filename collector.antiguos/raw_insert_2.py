import pandas as pd
import numpy as np
from time import time, gmtime, mktime, strptime
from pyparsing import Literal,Word,Combine,Optional,ZeroOrMore,Forward,nums, alphas

from datalab_essencials import *
import dask.dataframe as dd
import socket
import math

def logtype_parser(x,y):
    if x[8:] == ">":
        return "FPAR"
    elif x[8:] == ">-":
        return "FEVT"
    elif x[8:]== ">/":
        return "FLOG"
    #else:
    #    if  pd.isnull(y):
    #        return "LOG"
    #    else:
    #        if (-1!=y.find('ERR_')):
    #            return "ERR"
    #        else:
    #            return "LOG"

def tm_parser(row):
    try:
        tm = strptime(str(row['0'])+" "+str(row['1'])+" "+str(row['6'][:8]), "%b %d %H:%M:%S")
        return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
    except:
        return "TODO"

def get_loghost(raw_file):
    i = raw_file.find('/')
    while -1 != i:
        j = i;
        i = raw_file.find('/', i+1 )
    k = raw_file.find('.', j+1)
    return raw_file[j+1:k]

m_hostname = socket.gethostname()
m_loghost = get_loghost("/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wcnnaco.2016-10-01.log")
def loghost_parser():
    if m_loghost != None:
       return m_loghost
    else :
       return m_hostname

def env_parser(row):
    i=len(row)-1
    while str(row[i])=='nan':
        i-=1 
    x = str(row[i]).find('[')
    y = str(row[i]).find(']')
    if -1!=x and -1!=y:
        return row[i][x+1:y]
    else:
        return ""

def procname_parser(row):
    logType = row
    if logType == "ERR" or logType == "LOG":
        return line_split[9]
    else:
        return remove_duplicated_white_space(raw_line[16:]).split(" ")[1].replace(":","")

def data_raw_parser(*arg):
    print(*arg)
    if  len(arg) == 2:
        return logtype_parser(arg[0],arg[1])
    elif len(arg) == 3:
        return tm_parser(arg[0],arg[1],arg[2])
    elif len(arg)>3:
        return env_parser(arg)

row_index=['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21']
row_j=['A','B']
parse_dates={'logtype':[6,17]}#, 'tm':[0,1,6]}
s=time.time()
#df = pd.read_table("/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wcnnaco.2016-10-01.log", delim_whitespace=True, names=row_index, encoding='latin-1', skiprows=0, keep_date_col=True,usecols=[6,17])
df = pd.read_table("/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wcnnaco.2016-10-01.log", delim_whitespace=True, parse_dates=parse_dates, date_parser=logtype_parser ,names=row_index, encoding='latin-1', skiprows=0, keep_date_col=True,usecols=['6','17'])
#logtype = df.apply(logtype_parser,axis=1)
#tm = df.apply(tm_parser,axis=1)
#env = df.apply(env_parser, axis=1)
#logtype.compute()
#print(df)
#logtype.to_csv("/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/prueba-*.csv")
print(time.time()-s)
print(df[0:60])
#print(logtype.compute()[0:5])
#print(tm.compute()[0:5])
#print(env.compute()[10:20])
#print(df.compute()['6'])
#print(time.time()-s)


