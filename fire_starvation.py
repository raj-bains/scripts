#!/usr/bin/python
# author - raj bains, rbains@hortonworks.com


# 5 users fire 10 back-to-back small queries (each gets session in different Q)
# 5 users then fire 5 back-to-back larger queries (each gets  another session in each queue) 

import sys
import threading
import subprocess
import time
from subprocess import*

DEBUG=1
tmp_dir = '/tmp/rbains'
out_file_name = 'run_query'
prepend_cmd = '''set hive.execution.engine=tez;
              '''
driver = 'org.apache.hive.jdbc.HiveDriver'
connect = 'jdbc:hive2://localhost:10000'
database = 'tpcds_bin_partitioned_orc_200'

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

################

# Copy file multiple times into one file

def creatLargeFiles(n_users, ifile):
    a = [ ]
    # Write header to every file (not append)
    onetime = "echo \"{0}\" > {1}".format(prepend_cmd, out_file)
    runCmd(onetime)
    for f in range(n_users):
        out_file =  "{0}/{1}_{2}_{3}.sql".format(tmp_dir, out_file_name, 'heavy', f)
        a.append(out_file)
        for i in range(5):
            cmd = "cat {0}/{1} >> {2}".format('.', ifile, out_file)
            runCmd(cmd)
    return a

def createLargeOut(n_users)
    a = [ ]
    tt = getTimeStamp()
    for f in range(n_users):
        log_file="{0}/log_batch{1}.txt".format(tmp_dir, tt)
        a.append(log_file)
    return a

# Merge available files into 1 per user

def createTempFiles(n_users, src_dir, tmp_dir):
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

################

def getBeelineCmd(infile, outfile, user)
    command = 'beeline -d {0} -n {5} -u {1}/{2} -f {3} >& {4}'.format(driver, connect, database, infile, outfiles, user)
    return command        
# Fire commands in parallel

def fireCommands(n_users, files, outFiles, bigFiles, bigOutFiles, batchFirst):
    theThreads = [ ]

    # Fire in parallel threads
    i = 1

    # Fire the batch job, should go to default queue
    #bfile = batchFiles[0]
    #boutFile = batchFiles[1]
    #command = 'hive --service cli -f {0} >& {1}'.format(bfile, boutFile)

    # Fire bigger interactive jobs
    if batchFirst = 1:
        for x in range(n_users):
            user = 'user{0}'.format(i)
            i = i + 1
            command = getBeelineCmd(bugFiles[x], bigOutFiles[x], user)
            tt = PopenThread(command) 
        tt.start() 
        theThreads.append(tt)
        # Give a headstart
        time.sleep(5)
        
    i = 1
    
    # Fire the interactive jobs
    for x in range(n_users):
        user = 'user{0}'.format(i)
        i = i + 1
        command = getBeelineCmd(files[x], outFiles[x], user)
        tt = PopenThread(command) 
        tt.start() 
        theThreads.append(tt)

    i = 1
    
    # Fire bigger interactive jobs
    if batchFirst = 0:
        # Give a headstart
        time.sleep(5)
        for x in range(n_users):
            user = 'user{0}'.format(i)
            i = i + 1
            command = getBeelineCmd(bugFiles[x], bigOutFiles[x], user)
            tt = PopenThread(command) 
        tt.start() 
        theThreads.append(tt)
        
    for tt in theThreads:
        tt.join()
    time.sleep( 500 )



################

def usage():
    print 'fire_concurrent_tasks.py -n <num_users> -d <sql_files_directory> -l <large_sql_file>'
    sys.exit(2)
          
def main():
      # Get arguments
      if len(sys.argv) < 7:
            usage();
      arg_users = int(sys.argv[2])
      arg_dir = sys.argv[4]
      arg_lf = sys.argv[6]

      # create temporary files for half the users, these will be interactive
      files = createTempFiles(arg_users, arg_dir, tmp_dir)
      ofiles = outFileNames(arg_users, tmp_dir)

      # create temporary files for other half, these are larger queries
      batchFiles = createBatchFiles(arg_users)
      obatch = createLargeOut(n_users)

      # run both sets together
      fireCommands(arg_users, files, batchFiles, obatch, 1)

      # Parse the logs produced
      times = parseLogs(ofiles)
      statLogs(times)

      # run both sets together
      fireCommands(arg_users, files, batchFiles, obatch, 0)

      # Parse the logs produced
      times = parseLogs(ofiles)
      statLogs(times)


if __name__ == "__main__":
   main()
