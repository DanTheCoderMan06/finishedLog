import os
import re 
import datetime
import bisect

ROLE_CHANGE_STRING = "SNR role change "
RU_ID_STRING = "RU_ID"
RU_STRING = "RU"
LOADED_STRING = "Sharding Replication "
ROLE_CHANGE_STRING_RUID = "SNR role change RU_ID"
LEADER_STRING = "LEADER"
TERM_STRING = "Term"
RECOVERY_EVENT_STRING = "with event=RECOVER"
CANDIDATE_STRING = "CANDIDATE"
ERROR_STRING = "error="
REASON_STRING = "Reason"
HEARTBEAT_PARAMETERS_STRING = "Heatbeat parameters:"
OSP_STRING = "ospid="


# Checks if a string is a valid ISO 8601 timestamp.
# Args:
#     timestamp_str (str): The string to check.
# Returns:
#     bool: True if the string is a valid timestamp, False otherwise.
def isTimeStamp(timestamp_str):
    try:
        datetime.datetime.fromisoformat(timestamp_str.strip())
        return True
    except ValueError:
        return False

# Fetches the timestamp from a list of lines, searching backwards from a given index.
# Args:
#     lines (list): A list of strings representing the lines in a file.
#     index (int): The index to start searching from.
# Returns:
#     int: The index of the line containing the timestamp, or None if not found.
def fetchTimestampFromIndex(lines, index):
    index -= 1
    while not isTimeStamp(lines[index]):
        index -= 1
        if index < 0:
            print("Error: No Timestamp found for line{}".format(index))
            return
    return index

# Parses a line from a log file.
# Args:
#     lines (list): A list of strings representing the lines in a file.
#     index (int): The index of the line to parse.
# Returns:
#     dict: A dictionary containing the parsed information, or None if the line is not relevant.
def parseLine(lines, index):
    currentLine = lines[index]
    if ROLE_CHANGE_STRING not in currentLine and LOADED_STRING not in currentLine:
        return
    info = {}
    timestampIndex = fetchTimestampFromIndex(lines, index)
    info['timestamp'] = lines[timestampIndex]

def fetchRUIDFromLine(line):
    try:
        lineWords = line.split(' ')
        if RU_ID_STRING in lineWords:
            ruidWordIndex = lineWords.index(RU_ID_STRING)
        elif RU_STRING in lineWords:
            ruidWordIndex = lineWords.index(RU_STRING)
        if ruidWordIndex == None:
            return -1
        target = "".join([char for char in lineWords[ruidWordIndex + 1] if char.isdigit()])
        return int(target)
    except:
        return -1

def parseLineData(line, timestamp, dbName, dbId):
    data = dict()

    lineWords = [item for item in line.split(' ') if item and not item.isspace()]
    
    data['term'] = int("".join(char for char in lineWords[lineWords.index(TERM_STRING) + 1] if char.isdigit()))
    data['timestamp'] = timestamp.strip()
    data['dbName'] = dbName
    data['dbId'] = dbId
    data['history'] = list()

    return data

def fetchTermSlot(eventsLog, timestamp, eventsLogTimestamps):
    timestampTime = datetime.datetime.fromisoformat(timestamp).timestamp()
    ip = bisect.bisect_right(eventsLogTimestamps, timestampTime)
    if ip == 0:
        return 0
    return ip - 1
       
    
# Only leadership changes for now
def parseLogFile(logFileContent, dbName, dbId):
    result = dict()
    for i in range(len(logFileContent)):
      readLine = logFileContent[i]
      
      ruid = fetchRUIDFromLine(readLine)

      if ruid == -1:
         continue

      if ruid not in result:
         result[ruid] = list()
          
      if ROLE_CHANGE_STRING_RUID in readLine:
          if LEADER_STRING in readLine:
            result[ruid].append(parseLineData(readLine, logFileContent[i-1], dbName, dbId))

      if RECOVERY_EVENT_STRING in readLine and len(result[ruid]) > 0:
        if 'recoveryTime' in result[ruid][-1]:
           continue
        previousTimestamp = logFileContent[(fetchTimestampFromIndex(logFileContent, i))].strip()
        result[ruid][-1]['recoveryTime'] = datetime.datetime.fromisoformat(previousTimestamp).timestamp() - datetime.datetime.fromisoformat(result[ruid][-1]['timestamp']).timestamp()
          
    return result

def parseCandidateChange(lines, index):
    line = lines[index]
    result = dict()
    result['type'] = "candidate"
    lineWords = [item for item in line.split(' ') if item and not item.isspace()]
    
    result['parameters'] = list()

    for word in lineWords:
        if REASON_STRING in word:
            result['reason'] = line[line.find(word):-1]

    og = index
    while HEARTBEAT_PARAMETERS_STRING not in lines[index]:
        index += 1
        if index - og > 3:
            return result

    while not isTimeStamp(lines[index]):
        result['parameters'].append(lines[index])
        index += 1

    return result

def parseErrorLog(lines, index):
    line = lines[index]
    result = dict()
    result['type'] = "error"
    lineWords = [item for item in line.split(' ') if item and not item.isspace()]
    result['parameters'] = list()

    for word in lineWords:
        if ERROR_STRING in word:
            result['code'] = int("".join([char for char in word if char.isdigit()]))
        elif OSP_STRING in word:
            result['ospid'] = int("".join([char for char in word if char.isdigit()]))
            
    return result

def findParentWithSubdir(target_subdir, start_path):
    current_path = os.path.abspath(start_path)
    while True:
        if os.path.isdir(os.path.join(current_path, target_subdir)):
            return current_path
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path:
            return None
        current_path = parent_path

def findOspFile(trace_dir, targetOsp, ruid):
    for item in os.listdir(trace_dir):
        fullPath = os.path.join(trace_dir, item)
        if os.path.isfile(fullPath):
            filename, extension = os.path.splitext(item)
            fileWords = filename.split('_')
            if extension == '.trc' and str(targetOsp) in fileWords:
                if len(fileWords) - fileWords.index(str(targetOsp)) >= 2:
                    fileRUID = fileWords[fileWords.index(str(targetOsp)) + 2]
                    fileRUID = int("".join([char for char in fileRUID if char.isdigit()]))
                    if fileRUID == ruid:
                        return "file:///{}".format(fullPath)


# Finds the first .trc file in a directory.
# Args:
#     incident_dir (str): The directory to search.
# Returns:
#     str: The path to the .trc file, or None if not found.
def findIncidentFile(incident_dir):
    for item in os.listdir(incident_dir):
        full_path = os.path.join(incident_dir, item)
        if os.path.isdir(full_path):
            for sub_item in os.listdir(full_path):
                if sub_item.endswith('.trc'):
                    newPath = os.path.join(full_path, sub_item)
                    return "file:///{}".format(newPath)

# Gets all incidents from a given path.
# Args:
#     incidentPath (str): The path to the incident directory.
# Returns:
#     list: A list of dictionaries, where each dictionary represents an incident.
def getAllIncidents(incidentPath):
    results = []
    for item in os.listdir(incidentPath):
        full_path = os.path.join(incidentPath, item)
        if os.path.isdir(full_path):
            newincident = {}
            newincident['folderName'] = item
            newincident['fileName'] = findIncidentFile(incidentPath)
            results.append(newincident)
    return results

# Parses all other events from a log file.
# Args:
#     logFileContent (list): A list of strings representing the lines in a log file.
#     ruidList (list): A list of all RUIDs.
#     dbName (str): The name of the database.
#     dbId (int): The ID of the database.
#     logFilePath (str): The path to the log file.
#     incidents (list): A list to store any incidents found.
# Returns:
#     dict: A dictionary where the keys are RUIDs and the values are lists of events.
def parseAllOtherEvents(logFileContent, ruidList, dbName, dbId, logFilePath, incidents):
    result = {ruid: [] for ruid in ruidList}
    for i in range(len(logFileContent)):
        line = logFileContent[i]
        if CANDIDATE_STRING in line and ROLE_CHANGE_STRING_RUID in line:
            lineInfo = parseCandidateChange(logFileContent, i)
        elif ERROR_STRING in line:
            lineInfo = parseErrorLog(logFileContent, i)
            if lineInfo['code'] == 0:
                continue
            if 'ospid' in lineInfo:
                lineInfo['ospFile'] = findOspFile(os.path.join(findParentWithSubdir('trace', logFilePath), 'trace'), lineInfo['ospid'], fetchRUIDFromLine(line))
        else:
            continue
        lineInfo['original'] = line
        lineInfo['timestamp'] = logFileContent[fetchTimestampFromIndex(logFileContent, i)].strip()
        lineInfo['dbName'] = dbName
        lineInfo['dbId'] = dbId
        result[fetchRUIDFromLine(line)].append(lineInfo)

    incidentPath = os.path.join(findParentWithSubdir('trace', logFilePath), 'incident')
    for item in getAllIncidents(incidentPath):
        incidents.append(item)

    return result
            


def parseHistory(allRUIDs, rmdbs, logFiles, dbIds):
    history = {ruid: {rmdb['shardGroup']: [] for rmdb in rmdbs} for ruid in allRUIDs}
    incidents = list()
    dbLogsCache = dict()
    shardGroups = dict()
    logFilePaths = {logFile['dbName']: logFile['logFile'] for logFile in logFiles}
    print("--- Starting parseHistory ---")
    print(f"allRUIDs: {allRUIDs}")
    print(f"rmdbs: {rmdbs}")
    print(f"logFiles: {logFiles}")
    print(f"dbIds: {dbIds}")

    for logFile in logFiles:
        print(f"Processing log file: {logFile['logFile']} for db: {logFile['dbName']}")
        with open(logFile['logFile'], 'r') as fp:
            dbLogsCache[logFile['dbName']] = fp.readlines()
            parsed_log = parseLogFile(dbLogsCache[logFile['dbName']], logFile['dbName'], dbIds[logFile['dbName']])
            print(f"Parsed leadership changes for {logFile['dbName']}: {parsed_log}")
            for ruid, events in parsed_log.items():
                if ruid in history:
                    for rmdb in rmdbs:
                        if rmdb['dbName'] == logFile['dbName']:
                            history[ruid][rmdb['shardGroup']].extend(events)
                            if rmdb['shardGroup'] not in shardGroups:
                                shardGroups[rmdb['shardGroup']] = list()
                            shardGroups[rmdb['shardGroup']].append(rmdb['dbName'])

    for ruid in history:
        for shard_group in history[ruid]:
            history[ruid][shard_group].sort(key=lambda result: datetime.datetime.fromisoformat(result['timestamp'].strip()).timestamp(), reverse=False)

    for dbName, logFileContents in dbLogsCache.items():
        print(f"Processing other events for DB: {dbName}")
        current_shard_group = None
        for rmdb in rmdbs:
            if rmdb['dbName'] == dbName:
                current_shard_group = rmdb['shardGroup']
                break
        
        if not current_shard_group:
            continue

        logFilePath = logFilePaths.get(dbName)
        if not logFilePath:
            continue

        otherEvents = parseAllOtherEvents(logFileContents, allRUIDs, dbName, dbIds[dbName], logFilePath, incidents)
        print(f"Parsed other events for {dbName}: {otherEvents}")

        for ruid in allRUIDs:
            ruidEvents = otherEvents[ruid]
            ruidShardGroupHistory = history[ruid][current_shard_group]
            
            leader_event_timestamps = [datetime.datetime.fromisoformat(e['timestamp']).timestamp() for e in ruidShardGroupHistory]

            for event in ruidEvents:
                if not ruidShardGroupHistory:
                    continue
                targetSlot = fetchTermSlot(ruidShardGroupHistory, event['timestamp'], leader_event_timestamps)
                ruidShardGroupHistory[targetSlot]['history'].append(event)
    
    for ruid in history:
        for shard_group in history[ruid]:
            for event in history[ruid][shard_group]:
                if 'history' not in event:
                    event['history'] = []

    print("--- Finished parseHistory ---")
    return history, incidents
