import os
import re 
import datetime
import bisect
import time
import gzip
import shutil

ROLE_CHANGE_STRING = "SNR role change "
RU_ID_STRING = "RU_ID"
RU_ID_STRING_LOWER = "ru_id"
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
PROCESS_STRING = "process_name="
CONTINUE_FILE_STRING = "*** TRACE CONTINUES IN FILE "
CONTINUED_FROM_FILE_DUMP_STRING = "Dump continued from file: "
FILE_STRING = "FILE"

def rmdbExists(rmdbList, target):
    for rmdb in rmdbList:
        if rmdb['dbName'] == target:
            return True
    return False

def logExists(rmdbList, target):
    for rmdb in rmdbList:
        if target in rmdb['logFolderNames']:
            return True
    return False

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
        if RU_ID_STRING_LOWER in lineWords:
            ruidWordIndex = lineWords.index(RU_ID_STRING_LOWER)
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
    try:
        result = dict()
        result['type'] = "candidate"
        lineWords = [item for item in line.split(' ') if item and not item.isspace()]
        
        result['parameters'] = list()

        for word in lineWords:
            if REASON_STRING in word:
                result['reason'] = line[line.find(word):-1]

        offset = 0
        while HEARTBEAT_PARAMETERS_STRING not in lines[index + offset]:
            offset += 1
            if offset > 7:
                return result
            
        while not isTimeStamp(lines[index + offset]):
            result['parameters'].append(lines[index + offset])
            offset += 1
            if index + offset >= len(lines) or offset > 7:
                return result

        return result
    except IndexError:
        raise IndexError(f"Index out of bounds on line: {line.strip()}")

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
        elif PROCESS_STRING in word:
            result['process_name'] = "".join([char for char in word.split(PROCESS_STRING)[-1] if char.isalpha()]) 

    return result

def findParentWithSubdir(target_subdir, start_path):
    print(f"[{time.time()}] findParentWithSubdir: searching for '{target_subdir}' starting from '{start_path}'")
    current_path = os.path.abspath(start_path)
    while True:
        if os.path.isdir(os.path.join(current_path, target_subdir)):
            print(f"[{time.time()}] findParentWithSubdir: found at '{current_path}'")
            return current_path
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path:
            print(f"[{time.time()}] findParentWithSubdir: not found for '{start_path}'")
            return None
        current_path = parent_path

find_osp_file_cache = {}

def findOspFile(trace_dir, targetOsp, ruid, dbName, dbId, processName, targetUnzipDirectory):
   mainOSPFile = f"{dbName}_{processName}_{targetOsp}.trc"
   osp_path = os.path.join(trace_dir, mainOSPFile)
   
   is_gzipped = False
   if not os.path.exists(osp_path):
       if os.path.exists(osp_path + ".gz"):
           osp_path += ".gz"
           is_gzipped = True
       else:
           return ""

   read_path = osp_path
   if is_gzipped:
       dest_path = os.path.join(targetUnzipDirectory, mainOSPFile)
       with gzip.open(osp_path, 'rb') as f_in:
           if not os.path.exists(dest_path):
               with open(dest_path, 'wb') as f_out:
                   shutil.copyfileobj(f_in, f_out)
       read_path = dest_path

   continued_filename = ""
   try:
       with open(read_path, 'r', encoding='utf-8', errors='ignore') as fp:
           for line in fp.readlines():
               if CONTINUE_FILE_STRING in line:
                   words = line.split(" ")
                   for word in words:
                       if dbName in word:
                           continued_filename = os.path.basename(word.strip())
                           break
                   if continued_filename:
                       break
   except Exception as e:
       print(f"Error processing file {read_path}: {e}")

   if not continued_filename:
       return read_path

   continued_path_source = os.path.join(trace_dir, continued_filename)
   
   if os.path.exists(continued_path_source):
       return continued_path_source
   elif os.path.exists(continued_path_source + ".gz"):
       continued_path_dest = os.path.join(targetUnzipDirectory, continued_filename)
       with gzip.open(continued_path_source + ".gz", 'rb') as f_in:
           if not os.path.exists(continued_path_dest):
               with open(continued_path_dest, 'wb') as f_out:
                   shutil.copyfileobj(f_in, f_out)
       return continued_path_dest
   
   return read_path


# Finds the first .trc file in a directory.
# Args:
#     incident_dir (str): The directory to search.
# Returns:
#     str: The path to the .trc file, or None if not found.
def findIncidentFile(incident_dir, targetUnzip):
    print(f"[{time.time()}] findIncidentFile: searching in '{incident_dir}'")
    with os.scandir(incident_dir) as items:
        for item in items:
            if item.name.endswith('.trc.gz'):
                gz_path = os.path.join(incident_dir, item.name)
                unzipped_path = gz_path[:-3]
                with gzip.open(gz_path, 'rb') as f_in:
                    dest_path = os.path.join(targetUnzip, os.path.basename(unzipped_path))
                    if not os.path.exists(dest_path):
                        with open(dest_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                print(f"[{time.time()}] findIncidentFile: found and unzipped '{gz_path}' to '{unzipped_path}'")
                return unzipped_path
            elif item.name.endswith('.trc'):
                newPath = os.path.join(incident_dir,item.name)
                print(f"[{time.time()}] findIncidentFile: found '{newPath}'")
                return newPath
    print(f"[{time.time()}] findIncidentFile: no .trc file found in '{incident_dir}'")


def fetchFileInfo(incidentObject, targetLog, rmdbs, ruids, targetUnzip):
    try:
        try:
            with open(targetLog, 'r', encoding='utf-8', errors='ignore') as fp:
                lines = fp.readlines()
        except FileNotFoundError:
            gz_path = targetLog + ".gz"
            if os.path.exists(gz_path):
                with gzip.open(gz_path, 'rb') as f_in:
                    dest_path = os.path.join(targetUnzip, os.path.basename(targetLog))
                    if not os.path.exists(dest_path):
                        with open(dest_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                with open(targetLog, 'r', encoding='utf-8', errors='ignore') as fp:
                    lines = fp.readlines()
                
            else:
                print(f"Error processing file {targetLog}: File not found")
                return

        for line in lines:
            if CONTINUED_FROM_FILE_DUMP_STRING in line:
                    lineWords = line.split(' ')
                    for word in lineWords:
                        if ".trc" in word:
                            filePath = word.strip()
                            incidentObject['mainFile'] = "file:///{}".format(filePath)
                            baseName = os.path.basename(filePath)
                            pathWords = baseName.split('_')
                            for info in pathWords:
                                if logExists(rmdbs, info):
                                        incidentObject['dbLogFolderName'] = info
                                try:
                                    onlyNumbers = "".join([char for char in info if char.isdigit()])
                                    if int(onlyNumbers) in ruids:
                                        incidentObject['ruid'] = int(onlyNumbers)
                                    elif int(onlyNumbers) < 10000:
                                        incidentObject['dbId'] = int(onlyNumbers)
                                except:
                                    pass
    except Exception as e:
        print(f"Error processing file {targetLog}: {e}")



# Gets all incidents from a given path.
# Args:
#     incidentPath (str): The path to the incident directory.
# Returns:
#     list: A list of dictionaries, where each dictionary represents an incident.
def getAllIncidents(incidentPath, rmdbs, ruids, targetUnzip):
    print(f"[{time.time()}] getAllIncidents: getting incidents from '{incidentPath}'")
    results = []
    with os.scandir(incidentPath) as items:
        for item in items:
            full_path = os.path.join(incidentPath, item.name)
            if os.path.isdir(full_path):
                newincident = {}
                targetFile = findIncidentFile(full_path, targetUnzip)
                newincident['folderName'] = item.name
                newincident['fileName'] = "file:///{}".format(targetFile)
                newincident['folderPath'] = full_path
                fetchFileInfo(newincident,targetFile, rmdbs, ruids, targetUnzip)
                results.append(newincident)
    return results

def getLogName(rmdbList, target):
    for rmdb in rmdbList:
        if rmdb['dbName'] == target:
            return rmdb['logFolderNames']
        

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
def parseAllOtherEvents(logFileContent, ruidList, dbName, dbId, logFilePath, incidents, rmdbs, targetUnzipDirectory):
    dbLogNames = getLogName(rmdbs, dbName)
    result = {ruid: [] for ruid in ruidList}
    for i in range(len(logFileContent)):
        line = logFileContent[i]
        if CANDIDATE_STRING in line and ROLE_CHANGE_STRING_RUID in line:
            lineInfo = parseCandidateChange(logFileContent, i)
        elif ERROR_STRING in line:
            lineInfo = parseErrorLog(logFileContent, i)
            if lineInfo['code'] == 0:
                continue
            
            if 'ospid' in lineInfo and 'process_name' in lineInfo:
                trace_parent_dir = findParentWithSubdir('trace', logFilePath)
                if not trace_parent_dir:
                    for dbLogName in dbLogNames:
                        unzipPath = os.path.join(logFilePath, dbLogName)
                        if os.path.exists(unzipPath):
                            trace_parent_dir = unzipPath
                            break
                if trace_parent_dir:
                    lineInfo['ospFile'] = findOspFile(os.path.join(trace_parent_dir, 'trace'), lineInfo['ospid'], fetchRUIDFromLine(line), dbLogNames[0], dbId,lineInfo['process_name'], targetUnzipDirectory)
                else:
                    print(f"[{time.time()}] parseAllOtherEvents: 'trace' parent directory not found for '{logFilePath}' when searching for ospFile")
        else:
            continue
        lineInfo['original'] = line
        lineInfo['timestamp'] = logFileContent[fetchTimestampFromIndex(logFileContent, i)].strip()
        lineInfo['dbName'] = dbName
        lineInfo['dbId'] = dbId
        ruid = fetchRUIDFromLine(line)
        if ruid == -1:
            raise ValueError(f"Could not parse RUID from line: {line.strip()}")
        result[ruid].append(lineInfo)

    print(f"[{time.time()}] parseAllOtherEvents: searching for incidents for log file '{logFilePath}'")
    incident_parent_dir = findParentWithSubdir('trace', logFilePath)
    if not incident_parent_dir:
        for dbLogName in dbLogNames:
            unzipPath = os.path.join(logFilePath, dbLogName)
            if os.path.exists(unzipPath):
                incident_parent_dir = unzipPath
                break
    if incident_parent_dir:
        incidentPath = os.path.join(incident_parent_dir, 'incident')
        print(f"[{time.time()}] parseAllOtherEvents: incident path is '{incidentPath}'")
        if os.path.isdir(incidentPath):
            for item in getAllIncidents(incidentPath, rmdbs, ruidList, targetUnzipDirectory):
                if 'mainFile' in item:
                    item['mainFile'] = os.path.join(os.path.join(incident_parent_dir,'trace'), os.path.basename(item['mainFile']))
                incidents.append(item)
        else:
            print(f"[{time.time()}] parseAllOtherEvents: incident directory not found at '{incidentPath}'")
    else:
        print(f"[{time.time()}] parseAllOtherEvents: 'trace' parent directory not found for '{logFilePath}'")
    return result
            


def parseHistory(allRUIDs, rmdbs, logFiles, dbIds, directoryName):
    history = {ruid: {rmdb['shardGroup']: [] for rmdb in rmdbs} for ruid in allRUIDs}
    incidents = list()
    dbLogsCache = dict()
    shardGroups = dict()
    logFilePaths = {logFile['dbName']: logFile['originalLogFile'] for logFile in logFiles}
    print(f"[{time.time()}] --- Starting parseHistory ---")
    print(f"[{time.time()}] allRUIDs: {allRUIDs}")
    print(f"[{time.time()}] rmdbs: {rmdbs}")
    print(f"[{time.time()}] logFiles: {logFiles}")
    print(f"[{time.time()}] dbIds: {dbIds}")

    for logFile in logFiles:
        print(f"[{time.time()}] Processing log file: {logFile['logFile']} for db: {logFile['dbName']}")
        try:
            if logFile['dbName'] not in dbLogsCache:
                dbLogsCache[logFile['dbName']] = []
            with open(logFile['logFile'], 'r', encoding='utf-8', errors='ignore') as fp:
                dbLogsCache[logFile['dbName']].extend(fp.readlines())
        except Exception as e:
            print(f"Error processing log file {logFile['logFile']}: {e}")
            continue

    for dbName, logFileContents in dbLogsCache.items():
        parsed_log = parseLogFile(logFileContents, dbName, dbIds[dbName])
        print(f"[{time.time()}] Parsed leadership changes for {dbName}: {parsed_log}")
        for ruid, events in parsed_log.items():
            if ruid in history:
                for rmdb in rmdbs:
                    if rmdb['dbName'] == dbName:
                        for event in events:
                            if event not in history[ruid][rmdb['shardGroup']]:
                                history[ruid][rmdb['shardGroup']].append(event)
                        if rmdb['shardGroup'] not in shardGroups:
                            shardGroups[rmdb['shardGroup']] = list()
                        shardGroups[rmdb['shardGroup']].append(rmdb['dbName'])

    for ruid in history:
        for shard_group in history[ruid]:
            history[ruid][shard_group].sort(key=lambda result: datetime.datetime.fromisoformat(result['timestamp'].strip()).timestamp(), reverse=False)

    for dbName, logFileContents in dbLogsCache.items():
        print(f"[{time.time()}] Processing other events for DB: {dbName}")
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

        otherEvents = parseAllOtherEvents(logFileContents, allRUIDs, dbName, dbIds[dbName], logFilePath, incidents, rmdbs, directoryName)
        print(f"[{time.time()}] Parsed other events for {dbName}: {otherEvents}")

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

    print(f"[{time.time()}] --- Finished parseHistory ---")
    return history, incidents

def checkFile(filePath, unzipTo):
    if os.path.exists(filePath):
        return filePath
    elif os.path.exists(filePath + ".gz"):
        gz_path = filePath + ".gz"
        dest_path = os.path.join(unzipTo, os.path.basename(filePath))
        with gzip.open(gz_path, 'rb') as f_in:
            if not os.path.exists(dest_path):
                with open(dest_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        return dest_path
    else:
        return None

def parseWatsonLog(logDirectory, unzipTo):
    watsonLogPath = os.path.join(logDirectory, 'watson.log')
    watsonExists = os.path.exists(watsonLogPath)

    if not watsonExists:
        return []

    errors = []
    with open(watsonLogPath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f.readlines():
            if "DIF" in line and "FAIL" in line and ".log" in line:
                line_parts = line.split()
                dif_file = None
                for part in line_parts:
                    if ".dif" in part:
                        dif_file = part
                        break
                
                if dif_file:
                    base_name = dif_file.split('.dif')[0]
                    log_file = base_name + ".log"
                    errors.append({
                        "dif_file": os.path.normpath(checkFile(os.path.join(logDirectory, dif_file), unzipTo)),
                        "log_file": os.path.normpath(checkFile(os.path.join(logDirectory, log_file), unzipTo))
                    })
    
    return errors
