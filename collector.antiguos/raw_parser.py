from time import time, gmtime, mktime, strptime
import re
import gzip
import socket
from multiprocessing import Pool

def get_loghost(raw_file):
    i = raw_file.find('/')
    while -1 != i:
        j = i;
        i = raw_file.find('/', i+1 )
    k = raw_file.find('.', j+1)
    return raw_file[j+1:k]


def raw_parser(raw_file):
#    m_loghost=get_loghost(raw_file)    
    parser = RawLine_parser()
    with open(raw_file, encoding='utf-8') as f:
        for raw_line in f:
            raw_line_parsed = parser.parse_line(raw_line)
            yield (raw_line_parsed.typelog, raw_line_parsed.tm, raw_line_parsed.loghost, raw_line_parsed.env, raw_line_parsed.procname, raw_line_parsed.procid, raw_line_parsed.module, raw_line_parsed.keyw, raw_line_parsed.keyv, raw_line_parsed.logtext, raw_line_parsed.errstack, raw_line_parsed.errstackidx, raw_line_parsed.errlocation, raw_line_parsed.errseverity)
        
class RawLine_parser:
    
    def __init__(self):
       self.raw_line_parsed = RawLineParsed()


    def parse_line(self, raw_line):
        self.line_split=remove_duplicated_white_space(raw_line).split(" ")
        
        self.raw_line_parsed.set_typelog(self.alogtype_parser(raw_line));         # print(self.raw_line_parsed.typelog)
        self.raw_line_parsed.set_tm(self.tm_parser(raw_line));                    # print(self.raw_line_parsed.tm)
        self.raw_line_parsed.set_loghost(self.loghost_parser());                  # print(self.raw_line_parsed.loghost)
        self.raw_line_parsed.set_env(self.env_parser(raw_line));                  # print(self.raw_line_parsed.env)
        self.raw_line_parsed.set_procname(self.procname_parser(raw_line));        # print(self.raw_line_parsed.procname)
        self.raw_line_parsed.set_procid(self.procid_parser(raw_line));            # print(self.raw_line_parsed.procid)
        self.raw_line_parsed.set_module(self.module_parser(raw_line));            # print(self.raw_line_parsed.module)
        self.raw_line_parsed.set_keyw(self.keyw_parser(raw_line));                # print(self.raw_line_parsed.keyw)
        self.raw_line_parsed.set_keyv(self.keyv_parser(raw_line));                # print(self.raw_line_parsed.keyv)
        self.raw_line_parsed.set_logtext(self.logtext_parser(raw_line));          # print(self.raw_line_parsed.logtext)
        self.raw_line_parsed.set_errstack(self.errstack_parser(raw_line));        # print(self.raw_line_parsed.errstack)
        self.raw_line_parsed.set_errstackidx(self.errstackidx_parser(raw_line));  # print(self.raw_line_parsed.errstackidx)
        self.raw_line_parsed.set_errlocation(self.errlocation_parser(raw_line));  # print(self.raw_line_parsed.errlocation)
        self.raw_line_parsed.set_errseverity(self.errseverity_parser(raw_line));  # print(self.raw_line_parsed.errseverity)
        return self.raw_line_parsed

    def tm_parser(self, raw_line):
        tm = strptime(self.line_split[0]+" "+self.line_split[1]+" "+self.line_split[6][:8], "%b %d %H:%M:%S")
        return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)                    

    def loghost_parser(self): 
        if m_loghost != None:
            return m_loghost
        else :
            return m_hostname 

    def env_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if None != re.search('\[.*\]',raw_line):
            return re.search('\[.*\]',raw_line).group(0).replace('[','').replace(']','')
        if logType == "ERR" or logType == "LOG":
            return self.line_split[7]
        else:
            return (m_loghost if m_loghost else m_hostname)
        
    def procname_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "ERR" or logType == "LOG":
            return self.line_split[9] 
        else:
            return remove_duplicated_white_space(raw_line[16:]).split(" ")[1].replace(":","")

    def procid_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "ERR" or logType == "LOG":
            return self.line_split[10]
        else:
            return 0

    def module_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "ERR" or logType == "LOG":
            return self.line_split[8]
        else:
            return ""
    
    def keyw_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "FPAR":
            x=self.line_split[7];i=7
            while self.line_split[i+1] != '=' :
                i+=1
                x+="."+self.line_split[i]
            return x    
        elif logType == "FEVT":
            x="";i=7
            while self.line_split[i] != '/' :
                x+="."+self.line_split[i]; i+=1
            return x
        else:
            return ""  
    
    def keyv_parser(self, raw_line):
       logType = self.alogtype_parser(raw_line)
       if logType == "FPAR":
          i = self.line_split.index('='); i+=1
          x=self.line_split[i]
          while self.line_split[i+1] != '/' :
              i+=1;
              x+="."+self.line_split[i+1];
          return x.replace('\'','')
       else:
          return ""

    def logtext_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "FPAR" or logType == "FEVT" :
            i = self.line_split.index("/")
            log = convert_list_to_simple_string(self.line_split[i+1:]) 
            return re.sub('\[.*\]', '', log) 
        elif logType == "FLOG":
            return convert_list_to_simple_string([re.sub('.*>/', '', self.line_split[6])]+self.line_split[7:-1]+[re.sub('\[.*\]','',self.line_split[-1])])
        elif logType == "ERR":
            return convert_list_to_simple_string(self.line_split[17:]) 
        elif logType == "LOG":
            return convert_list_to_simple_string(self.line_split[12:])
        
    def errstack_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "ERR":        
            return self.line_split[13]
        else:
            return ""
 
    def errstackidx_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "ERR":
            return self.line_split[14]
        else :
            return 0        
        
    def errlocation_parser(self, raw_line):
        logType = self.alogtype_parser(raw_line)
        if logType == "ERR": 
            return self.line_split[11]
        else:
            return ""     
        
    def errseverity_parser(self, raw_line):
        return self.line_split[15] if self.alogtype_parser(raw_line)  == "ERR" else ""

    def alogtype_parser(self, raw_line):
        tmp = remove_duplicated_white_space(raw_line[16:]).split(" ")[3]
        if tmp[len(tmp)-1:]== ">":
            return "FPAR"
        elif tmp[len(tmp)-2:] == ">-":
            return "FEVT"
        elif tmp[len(tmp)-2:]== ">/":
            return "FLOG"
        elif (-1 != raw_line[16:].find("ERR_")):
            return "ERR"
        else:
            return "LOG"

        
def remove_duplicated_white_space(line):
    return " ".join(line.split())

def convert_list_to_simple_string(l):
    return " ".join(l)

class RawLineParsed:
    #type,tm,loghost,env,proc,procid,module,keyw,keyv,log,errs,erridx,errloc,errsev
    def __init__(self):
        self.typelog = str() 
        self.tm = str()
        self.loghost = str()
        self.env = str(),
        self.procname = str(),
        self.procid = str()
        self.module = str()
        self.keyw = str()
        self.keyv = str()
        self.logtext = str()
        self.errstack = str()
        self.errstackidx = str()
        self.errlocation = str()
        self.errseverity = str()
    

    def set_typelog(self, typelog):
        self.typelog=typelog
 
    def set_tm(self, tm):
        self.tm=tm

    def set_loghost(self, loghost):
        self.loghost=loghost

    def set_env(self, env):
        self.env=env

    def set_procname(self, procname):
        self.procname=procname

    def set_procid(self, procid):
        self.procid=procid

    def set_module(self, module):
        self.module=module

    def set_keyv(self, keyv):
        self.keyv=keyv

    def set_keyw(self, keyw):
        self.keyw=keyw

    def set_logtext(self, logtext):
        self.logtext=logtext

    def set_errstack(self, errstack):
        self.errstack=errstack

    def set_errstackidx(self, errstackidx):
        self.errstackidx=errstackidx

    def set_errlocation(self, errlocation):
        self.errlocation=errlocation

    def set_errseverity(self, errseverity):
        self.errseverity=errseverity

m_hostname = socket.gethostname()
m_loghost = get_loghost("/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wamber.2016-10-01.log")
def parse_file(raw_file):
    start = time()
    for l in raw_parser(raw_file):
        pass#print(l)
    print(time()-start)

start = time()
p = Pool(4)
p.map(parse_file, ["/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wamber.2016-10-01.log","/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wamber.2016-10-01.log","/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wamber.2016-10-01.log","/data/datalake/fitslake/datalab_backup/vltlogs/raw_logs/2016/10/wamber.2016-10-01.log"])
print(time()-start)



