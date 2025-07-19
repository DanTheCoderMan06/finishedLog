import os
import sys
import log_parser
import html_parser
import file_parser
import datetime
import gzip
import shutil

# ./scratch/reports C:\\Users\\danii\\OneDrive\\Documents\\mytar2\\lrgdbcongsmshsnr17






# Parses the log files in a given directory and generates an HTML report.
# Args:
#     rmdbsDirectory (str): The name of the directory containing the log files.
#     directoryName (str): The directory to port report to.
def parseLog(logDirectory, directoryName):
    fileName = ""
    logContents = {}
    rmdbs = []
    shardGroups = set()
    dbCounter = 1
    ruidLists = {}
    logFiles = []
    allRUIDs = set()
    dbIds = {}

    fileName = "sdbdeploy_gdsctl.lst"

    print("Found gdsctl log file!")

    try:
        with open(os.path.join(directoryName, fileName), 'r', encoding='utf-8', errors='ignore') as file:
            logFileLines = file.readlines()
    except FileNotFoundError:
       gz_path = os.path.join(directoryName, fileName + ".gz")
       if os.path.exists(gz_path):
           unzipped_path = os.path.join(directoryName, fileName)
           with gzip.open(gz_path, 'rb') as f_in:
               with open(unzipped_path, 'wb') as f_out:
                   shutil.copyfileobj(f_in, f_out)
           print(f"Unzipped {gz_path} to {unzipped_path}")
           with open(unzipped_path, 'r', encoding='utf-8', errors='ignore') as file:
               logFileLines = file.readlines()
       else:
           print("Error: The file '{}' could not be found.".format(fileName))
           return
    except OSError:
        print("Error: The file '{}' could not be opened.".format(fileName))
        return

    if not logFileLines:
        print("Error: No lines in the log file  file '{}'!".format(fileName))
        return
    
    extractionDirectory = directoryName

    for i in range(len(logFileLines)):
        if log_parser.ADDSHARD_PREFIX in logFileLines[i]:
            targetLines = log_parser.fetchAddShardInfo(logFileLines, i)
            if len(targetLines) < 2:
                print("Error from add shard command on line {}, failed to fetch db + shardgroup info lines!".format(i + 1))
                return
            shardGroup, rmdb = log_parser.parseAddShard(targetLines)
            if shardGroup == "NULL":
                print("Error from add shard command on line {}, failed to parse db + shardgroup info lines!".format(i + 1))
                return

            shardGroups.add(shardGroup)
            dbIds[rmdb] = dbCounter
            rmdbs.append({'dbName': rmdb, 'dbID': dbCounter, 'shardGroup': shardGroup, 'logFolderName' : file_parser.findMainDir(os.path.join(extractionDirectory, 'diag', 'rdbms', rmdb))})
            dbCounter += 10

    print("SHARD GROUPS: ", shardGroups)
    print("RMDBS", rmdbs)

    for rmdb in rmdbs:
        rmdbName = rmdb['dbName']
        targetLog = os.path.join(extractionDirectory, 'diag', 'rdbms', rmdbName)
        report_dir = os.path.normpath(os.path.join(logDirectory, os.path.basename(directoryName)))
        print(file_parser.findLogFile(targetLog, report_dir))
        try:
            unzipped_log_file = file_parser.findLogFile(targetLog, report_dir)
            logFiles.append({'dbName': rmdbName, 'logFile': unzipped_log_file, 'originalLogFile': targetLog})
        except Exception as e:
            print("Error: Failed to find log file for {}, {}".format(rmdbName, type(e).__name__))

    for logFile in logFiles:
        try:
            with open(logFile['logFile'], 'r', encoding='utf-8', errors='ignore') as file:
                logLines = file.readlines()
            ruidLists[logFile['dbName']] = set()
            for i in range(len(logLines)):
                ruID = log_parser.parseRUIDLine(logLines[i])
                if ruID > 0:
                    ruidLists[logFile['dbName']].add(ruID)
        except Exception as e:
            print("Error: Failed to parse log file for {}, {}".format(logFile['dbName'], type(e).__name__))
            return

        print("RUIDS for {}".format(logFile['dbName']), ruidLists[logFile['dbName']])

    for ruids in ruidLists.values():
        for ruid in ruids:
            allRUIDs.add(ruid)

    print("Parsing History")

    logContents['rmdbs'] = rmdbs
    logContents['shardGroups'] = shardGroups
    logContents['history'], logContents['incidents'] = log_parser.parseHistory(allRUIDs, rmdbs, logFiles, dbIds)
    logContents['allRUIDS'] = allRUIDs
    logContents['logDirectory'] = directoryName
    breakpoint()
    logContents['watson_errors'] = log_parser.parseWatsonLog(extractionDirectory)

    print("Creating Log Folder")

    html_parser.createLogFolder(logContents, os.path.join(logDirectory, os.path.basename(directoryName)))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <report_directory> <target_directory(Optional)>")
    else:
        directoryName = sys.argv[1]
        rmdbsDirectory = None
        if len(sys.argv) > 2:
            rmdbsDirectory = sys.argv[2]
        else:
            rmdbsDirectory = '.'
        parseLog(directoryName, rmdbsDirectory)