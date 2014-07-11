#!/usr/bin/python
# author - raj bains, rbains@hortonworks.com

import sys
import threading
import subprocess
from subprocess import*
import time


# variables to change
tmp_dir = '/tmp/rbains'
out_file_name = 'run_query_repeat'
prepend_cmd = '''set hive.execution.engine=tez;
              '''
DEBUG=1

# utilities, no business logic    
from os import listdir
from os.path import isfile, join
def getFiles(mypath):
    onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
    return onlyfiles

import datetime
def getTimeStamp():
    i = datetime.datetime.now()
    return "_{0}_{1}_{2}__{3}h".format(i.year, i.month, i.day, i.hour)

class PopenThread(threading.Thread):
    def __init__(self, cmd):
        threading.Thread.__init__(self)
        self.cmd=cmd
    def run(self):
        if DEBUG == 1:
            print self.cmd
        Popen(self.cmd, shell=True)
        
def runCmd(rcmd):
    if DEBUG == 1:
        print rcmd
    subprocess.call(rcmd, shell=True)





    
def createTempFile(n_times, src_file, tmp_dir):
    print 'Creating 1 file, from {1} into {2} directory'.format(n_times, src_file, tmp_dir)
    out_file =  "{0}/{1}_{2}.sql".format(tmp_dir, out_file_name, n_times)

    onetime = "echo \"{0}\" > {1}".format(prepend_cmd, out_file)
    runCmd(onetime)

    for f in range(n_times):
        cmd = "cat {0} >> {2}".format(src_file, f, out_file)
        runCmd(cmd)
    return out_file


def outFileName(n_times, tmp_dir):
    tt = getTimeStamp()
    log_file="{0}/log{1}{2}.txt".format(tmp_dir, n_times, tt)
    return log_file


def fireCommand(file, outFile):
    driver = 'org.apache.hive.jdbc.HiveDriver'
    connect = 'jdbc:hive2://cn041:10000'
    database = 'tpcds4_bin_partitioned_orc_200'
    theThreads = [ ]
    # Fire in parallel threads
    i = 1
    user = 'user{0}'.format(i)
    command = 'beeline -d {0} -n {5} -u {1}/{2} -f {3} >& {4}'.format(driver, connect, database, file, outFile, user)
    tt = PopenThread(command) 
    tt.start() 
    theThreads.append(tt)
    for tt in theThreads:
        tt.join()
    time.sleep( 150 )

def logsLight(logs):
    for log in logs:
        cmmd = "grep -i seconds {0}".format(log)
        runCmd(cmmd)

def parseLogs(logs):
    times = [ ]
    
    for log in logs:
        file = open(log, 'r')
        lTimes = [ ]
        for line in file:
#            if line[:19] == "100 rows selected (":
             if line[:21] == "5,719 rows selected (":
                a = len(line)-9
#                num = line[19:a]
                num = line[21:a]
                value = float(num)
                lTimes.append(value)
        times.append(lTimes)        
    return times


def statLogs1(times):
    sums = [ ]
    avgs = [ ]
    allValues = [ ]
    bigSum = 0
    
    for lTimes in times:
        sum = 0
        i = 0

        for value in lTimes:
            allValues.append(value)
            sum = sum + value
            i = i + 1

        sums.append(sum)
        avgs.append(sum/i)
        bigSum = bigSum + sum

    nQueries = len(allValues)
    nUsers = len(sums)
    average_time = bigSum / nQueries
    allValues.sort()
    cnt = nQueries
    print "Number of users {0}".format(nUsers)
    print "Queries fired {0}, per user {1}".format(nQueries, int(nQueries/nUsers))
    print "User latency: Max {0:.2f} Min {1:.2f}".format(max(sums), min(sums))
    print "All Queries: Max {0:.2f} Min {1:.2f}".format(allValues[cnt-1], allValues[0])
    print "All Queries: 90th percentile {0:.2f}".format(allValues[int(cnt*0.90)])
    print "All Queries: 50th percentile {0:.2f}".format(allValues[int(cnt*0.5)])
    print "All Queries: 10th percentile {0:.2f}".format(allValues[int(cnt*0.1)])
    print "All Queries: Average {0:.2f}".format(average_time)
    print allValues
        
def usage():
    print 'fire_repeatedly.py -n <times> -f <sql_file>'
    sys.exit(2)
          
def main():
      # Get arguments
      if len(sys.argv) < 5:
            usage();
      arg_times = int(sys.argv[2])
      arg_dir = sys.argv[4]
            
      print arg_times
      print arg_dir

      file = createTempFile(arg_times, arg_dir, tmp_dir)
      ofile = outFileName(arg_times, tmp_dir)
      fireCommand(file, ofile)
      # Parse the logs produced
      ofiles = [ ]
      ofiles.append(ofile)
      logsLight(ofiles)
      #times = parseLogs(ofiles)
      
      #statLogs(times)
      
if __name__ == "__main__":
   main()
