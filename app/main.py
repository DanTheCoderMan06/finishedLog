import os
import sys
import log_parser
import html_parser
import file_parser
import datetime

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

    try:
        with os.scandir(directoryName) as entries:
            for entry in entries:
                if entry.is_file():
                    if "gdsctl.lst" in entry.name:
                        fileName = entry.name
    except FileNotFoundError:
        print("Error: The directory '{}' does not exist.".format(directoryName))
        return

    if fileName == "":
        print("Error: No .lst file was detected (Gdsctl file)!")
        return

    print("Found gdsctl log file!")

    try:
        with open(os.path.join(directoryName, fileName)) as file:
            logFileLines = file.readlines()
    except OSError:
        print("Error: The file '{}' could not be opened.".format(fileName))
        return
    except FileNotFoundError:
        print("Error: The file '{}' could not be found.".format(fileName))
        return

    if not logFileLines:
        print("Error: No lines in the log file  file '{}'!".format(fileName))
        return

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
            rmdbs.append({'dbName': rmdb, 'dbID': dbCounter, 'shardGroup': shardGroup})
            dbCounter += 10

    print("SHARD GROUPS: ", shardGroups)
    print("RMDBS", rmdbs)

    extractionDirectory = os.path.join(directoryName, file_parser.findLogFolder(directoryName))

    for rmdb in rmdbs:
        rmdbName = rmdb['dbName']
        targetLog = os.path.join(extractionDirectory, 'diag', 'rdbms', rmdbName)
        print(targetLog)
        try:
            logFiles.append({'dbName': rmdbName, 'logFile': file_parser.findLogFile(targetLog)})
        except Exception as e:
            print("Error: Failed to find log file for {}, {}".format(rmdbName, type(e).__name__))

    for logFile in logFiles:
        try:
            with open(logFile['logFile'], 'r') as file:
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

    logContents['rmdbs'] = rmdbs
    logContents['shardGroups'] = shardGroups
    logContents['history'], logContents['incidents'] = log_parser.parseHistory(allRUIDs, rmdbs, logFiles, dbIds)
    logContents['allRUIDS'] = allRUIDs
    logContents['logDirectory'] = directoryName

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