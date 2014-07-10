#!/usr/bin/python
# author - raj bains, rbains@hortonworks.com

import sys
import threading
import subprocess
import time
from subprocess import*

# variables to change
tmp_dir = '/tmp/rbains'
out_file_name = 'run_query_21'
prepend_cmd = '''set hive.execution.engine=tez;
              '''
batch_files=['query28.sql','query32','query50','query88']

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



def createBatchFiles(n_users):
    a = [ ]
    out_file =  "{0}/{1}_{2}.sql".format(tmp_dir, out_file_name, 'batch')

    # Write header to every file (not append)
    onetime = "echo \"{0}\" > {1}".format(prepend_cmd, out_file)
    runCmd(onetime)
    
    for f in batch_files:
        cmd = "cat {0}/{1} >> {2}".format('.', f, out_file)
        runCmd(cmd)
        
    tt = getTimeStamp()    
    log_file="{0}/log_batch{1}.txt".format(tmp_dir, tt)
    a.append(out_file)
    a.append(log_file)
    return a

        
# Merge available files into 1 per user

def createTempFiles(n_users, src_dir, tmp_dir):
    print 'Creating {0} files, from {1} into {2} directory'.format(n_users, src_dir, tmp_dir)
    files = getFiles(src_dir)
    n_files_per_user = len(files)/n_users
    user_id = -1
    id = -1
    a = [ ]
    for f in files:
        id = id + 1
        out_file =  "{0}/{1}_{2}.sql".format(tmp_dir, out_file_name, user_id)
        if id % n_files_per_user == 0:
            user_id = user_id + 1
            out_file =  "{0}/{1}_{2}.sql".format(tmp_dir, out_file_name, user_id)
            a.append(out_file)

            # Write header to every file (not append)
            onetime = "echo \"{0}\" > {1}".format(prepend_cmd, out_file)
            runCmd(onetime)

        # Append source SQL file
        cmd = "cat {0}/{1} >> {2}".format(src_dir, f, out_file)
        runCmd(cmd)
    return a

def outFileNames(n_users, tmp_dir):
    tt = getTimeStamp()
    a = [ ]
    for f in range(n_users):
        log_file="{0}/log{1}{2}.txt".format(tmp_dir, f, tt)
        a.append(log_file) 
    return a

# Parse the logs

def statLogs(times):
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


def parseLogs(logs):
    times = [ ]
    
    for log in logs:
        file = open(log, 'r')
        lTimes = [ ]
        
        for line in file:
#            if line[:19] == "100 rows selected (":
            if line[:16] == "1 row selected (":
                a = len(line)-9
#                num = line[19:a]
                num = line[16:a]
                value = float(num)
                lTimes.append(value)
        times.append(lTimes)        
    return times


# Fire commands in parallel

def fireCommands(n_users, files, outFiles, batchFiles):
    driver = 'org.apache.hive.jdbc.HiveDriver'
    connect = 'jdbc:hive2://cn041:10000'
    database = 'tpcds4_bin_partitioned_orc_200'
    theThreads = [ ]

    # Fire in parallel threads
    i = 1

    # Fire the batch job, should go to default queue
    bfile = batchFiles[0]
    boutFile = batchFiles[1]
    command = 'hive --service cli -f {0} >& {1}'.format(bfile, boutFile)
    tt = PopenThread(command) 
    tt.start() 
    theThreads.append(tt)

    # Give the batch job a headstart
    time.sleep(5)

    # Fire the interactive jobs
    for x in range(n_users):
        user = 'user{0}'.format(i)
        i = i + 1
        command = 'beeline -d {0} -n {5} -u {1}/{2} -f {3} >& {4}'.format(driver, connect, database, files[x], outFiles[x], user)
        tt = PopenThread(command) 
        tt.start() 
        theThreads.append(tt)
        
    for tt in theThreads:
        tt.join()
    time.sleep( 500 )

def usage():
    print 'fire_concurrent_tasks.py -n <num_users> -d <sql_files_directory>'
    sys.exit(2)
          
def main():
      # Get arguments
      if len(sys.argv) < 5:
            usage();
      arg_users = int(sys.argv[2])
      arg_dir = sys.argv[4]
            
      print arg_users
      print arg_dir

      files = createTempFiles(arg_users, arg_dir, tmp_dir)
      ofiles = outFileNames(arg_users, tmp_dir)
      batchFiles = createBatchFiles(arg_users)
      fireCommands(arg_users, files, ofiles, batchFiles)

      # Parse the logs produced
      times = parseLogs(ofiles)
      statLogs(times)


if __name__ == "__main__":
   main()
