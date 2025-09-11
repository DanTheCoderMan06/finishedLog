import os
import re 
import datetime
import bisect
import time
import gzip
import shutil
import html

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

def convert_file_to_html(source_path, output_dir):
    """
    Converts a text file to an HTML file with each line in a <p> tag with an ID.
    """
    if not os.path.exists(source_path):
        return None

    base_name = os.path.basename(source_path)
    html_file_name = f"{base_name}.html"
    html_path = os.path.join(output_dir, html_file_name)

    try:
        with open(source_path, 'r', encoding='utf-8', errors='ignore') as f_in:
            lines = f_in.readlines()

        with open(html_path, 'w', encoding='utf-8') as f_out:
            f_out.write('<!DOCTYPE html>\n<html lang="en">\n<head>\n')
            f_out.write(f'<title>{html.escape(base_name)}</title>\n')
            f_out.write('<meta charset="UTF-8">\n')
            f_out.write('<style>p { margin: 0; padding: 0; }</style>\n')
            f_out.write('</head>\n<body>\n')
            for i, line in enumerate(lines):
                f_out.write(f'<p id="line{i+1}">{html.escape(line)}</p>\n')
            f_out.write('</body>\n</html>')
        
        return html_path
    except Exception as e:
        print(f"Error converting {source_path} to HTML: {e}")
        breakpoint()
        return None

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
    data['errors'] = list()

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

def findNearestTimestamp(filePath, target):
    targetTimeStamp = datetime.datetime.fromisoformat(target).replace(tzinfo=None)
    file_size = os.path.getsize(filePath)

    def get_line_at_byte(fp, byte_pos):
        fp.seek(byte_pos)
        if byte_pos > 0:
            fp.readline()  # skip partial line
        line_start = fp.tell()
        line = fp.readline().decode('utf-8', errors='ignore').strip()
        return line, line_start

    def extract_timestamp(line):
        for word in line.split():
            try:
                return datetime.datetime.fromisoformat(word.strip()).replace(tzinfo=None)
            except ValueError:
                pass
        return None

    with open(filePath, 'rb') as fp:
        low = 0
        high = file_size
        best_byte = 0
        best_delta = float('inf')

        # Binary search for the closest timestamp
        while low < high:
            mid = (low + high) // 2
            line, line_start = get_line_at_byte(fp, mid)
            ts = extract_timestamp(line)
            if ts is None:
                low = mid + 1
                continue

            delta = abs((ts - targetTimeStamp).total_seconds())
            if delta < 2:
                # Close enough, return this line number
                fp.seek(0)
                line_num = 0
                current_pos = 0
                while current_pos < line_start:
                    line_bytes = fp.readline()
                    if not line_bytes:
                        break
                    line_num += 1
                    current_pos += len(line_bytes)
                return line_num

            if delta < best_delta:
                best_delta = delta
                best_byte = line_start

            if ts < targetTimeStamp:
                low = mid + 1
            else:
                high = mid

        # If best_delta is still inf, no timestamps found, return 0
        if best_delta == float('inf'):
            return 0

        # Count lines from 0 to best_byte
        fp.seek(0)
        line_num = 0
        current_pos = 0
        while current_pos < best_byte:
            line_bytes = fp.readline()
            if not line_bytes:
                break
            line_num += 1
            current_pos += len(line_bytes)
        return line_num



def findOspFile(trace_dir, targetOsp, ruid, dbName, dbId, processName, targetUnzipDirectory, foundTimestamp):
   mainOSPFile = f"{dbName}_{processName.lower()}_{targetOsp}.trc"
   osp_path = os.path.join(trace_dir, mainOSPFile)
   
   is_gzipped = False
   if not os.path.exists(osp_path):
       if os.path.exists(osp_path + ".gz"):
           osp_path += ".gz"
           is_gzipped = True
       else:
           return "", 0
 
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
       html_path = convert_file_to_html(read_path, targetUnzipDirectory)
       targetLine = findNearestTimestamp(read_path, foundTimestamp)
       return html_path, targetLine + 1
 
   continued_path_source = os.path.join(trace_dir, continued_filename)
   if os.path.exists(continued_path_source):
       new_html_file = convert_file_to_html(continued_path_source, targetUnzipDirectory)
       targetLine = findNearestTimestamp(continued_path_source, foundTimestamp)
       return new_html_file, targetLine + 1
   elif os.path.exists(continued_path_source + ".gz"):
       continued_path_dest = os.path.join(targetUnzipDirectory, continued_filename)
       with gzip.open(continued_path_source + ".gz", 'rb') as f_in:
           if not os.path.exists(continued_path_dest):
               with open(continued_path_dest, 'wb') as f_out:
                   shutil.copyfileobj(f_in, f_out)
       
       new_html_file = convert_file_to_html(continued_path_dest, targetUnzipDirectory)
       targetLine = findNearestTimestamp(continued_path_dest, foundTimestamp)
       return new_html_file, targetLine + 1
   
   html_path = convert_file_to_html(read_path, targetUnzipDirectory)
   targetLine = findNearestTimestamp(read_path, foundTimestamp)
   return html_path, targetLine + 1



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
            lineInfo['isNew'] = True
            if 'ospid' in lineInfo and 'process_name' in lineInfo:
                trace_parent_dir = findParentWithSubdir('trace', logFilePath)
                if not trace_parent_dir:
                    for dbLogName in dbLogNames:
                        unzipPath = os.path.join(logFilePath, dbLogName)
                        if os.path.exists(unzipPath):
                            trace_parent_dir = unzipPath
                            break
                if trace_parent_dir:
                    lineInfo['ospFile'], lineInfo['scrollIndex'] = findOspFile(os.path.join(trace_parent_dir, 'trace'), lineInfo['ospid'], fetchRUIDFromLine(line), dbLogNames[0], dbId,lineInfo['process_name'], targetUnzipDirectory, logFileContent[fetchTimestampFromIndex(logFileContent, i)].strip())
                else:
                    print(f"[{time.time()}] parseAllOtherEvents: 'trace' parent directory not found for '{logFilePath}' when searching for ospFile")
        else:
            continue
        lineInfo['timestamp'] = logFileContent[fetchTimestampFromIndex(logFileContent, i)].strip()
        lineInfo['original'] = line
        lineInfo['dbName'] = dbName
        lineInfo['dbId'] = dbId
        ruid = fetchRUIDFromLine(line)
        if ruid == -1:
            continue
        result[ruid].append(lineInfo)

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
                if event.get('type') == 'error':
                    history_event = ruidShardGroupHistory[targetSlot]
                    if 'errors' not in history_event:
                        history_event['errors'] = []
                    history_event['errors'].append(event)
                else:
                    ruidShardGroupHistory[targetSlot]['history'].append(event)
    
    for ruid in history:
        for shard_group in history[ruid]:
            for event in history[ruid][shard_group]:
                if 'history' not in event:
                    event['history'] = []
                if 'errors' not in event:
                    event['errors'] = []

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
    
def listRightIndex(alist, value):
    return len(alist) - alist[-1::-1].index(value) -1

def parseWatsonLog(logDirectory, unzipTo):
    watsonDifPath = os.path.join(logDirectory, 'watson.dif')
    if not os.path.exists(watsonDifPath):
        return [], []

    trace_errors = []
    watson_errors = []
    seen_errors = list() #set

    with open(watsonDifPath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f.readlines():
            trc_match = re.search(r'(\S+\.trc)', line)
            dif_match = re.search(r'(\S+\.dif)', line)
            log_match = re.search(r'(\S+\.log)', line)

            if trc_match:
                trc_file = trc_match.group(1)
                trc_path = checkFile(os.path.join(logDirectory, trc_file), unzipTo)
                if trc_path:
                    entry = {'file': trc_path}
                    continued_log_path_str = ''
                    try:
                        with open(trc_path, 'r', encoding='utf-8', errors='ignore') as trc_fp:
                            for trc_line in trc_fp.readlines():
                                if CONTINUED_FROM_FILE_DUMP_STRING in trc_line:
                                    continued_log_path_str = trc_line.split(CONTINUED_FROM_FILE_DUMP_STRING, 1)[1].strip()
                                    break
                    except Exception as e:
                        print(f"Error reading {trc_path} to find continued log: {e}")
                    
                    if continued_log_path_str:
                        try:
                            path_parts = continued_log_path_str.split('/')
                            rdbms_index = listRightIndex(path_parts,'rdbms')
                            relative_path = os.path.join(*path_parts[rdbms_index:])
                            diag_path = os.path.join(logDirectory, 'diag')
                            continued_log_full_path = os.path.join(diag_path, relative_path)
                            entry['log_file'] = checkFile(continued_log_full_path, unzipTo)
                        except ValueError:
                             entry['log_file'] = ''
                    else:
                        entry['log_file'] = ''
                    
                    entry_tuple = tuple(sorted(entry.items()))
                    if entry_tuple not in seen_errors:
                        trace_errors.append(entry)
                        seen_errors.append(entry_tuple)

            elif dif_match:
                dif_file = dif_match.group(1)
                base_name = dif_file.rsplit('.dif', 1)[0]
                log_file = f"{base_name}.log"
                
                dif_path = checkFile(os.path.join(logDirectory, dif_file), unzipTo)
                log_path = checkFile(os.path.join(logDirectory, log_file), unzipTo)
                
                if dif_path:
                    entry = {'dif_file': dif_path, 'log_file': log_path if log_path else ''}
                    entry_tuple = tuple(sorted(entry.items()))
                    if entry_tuple not in seen_errors:
                        watson_errors.append(entry)
                        seen_errors.append(entry_tuple)

            elif log_match and not dif_match and not trc_match:
                log_file = log_match.group(1)
                base_name = log_file.rsplit('.log', 1)[0]
                dif_file = f"{base_name}.dif"

                log_path = checkFile(os.path.join(logDirectory, log_file), unzipTo)
                dif_path = checkFile(os.path.join(logDirectory, dif_file), unzipTo)

                if log_path:
                    entry = {'dif_file': dif_path if dif_path else '', 'log_file': log_path}
                    entry_tuple = tuple(sorted(entry.items()))
                    if entry_tuple not in seen_errors:
                        watson_errors.append(entry)
                        seen_errors.append(entry_tuple)

    return trace_errors, watson_errors
