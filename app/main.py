import os
import sys
import log_parser
import html_parser
import file_parser
import gzip
import shutil
import pandas as pd
# ./scratch/reports C:\\Users\\danii\\OneDrive\\Documents\\mytar2\\lrgdbcongsmshsnr17





# Parses the log files in a given directory and generates an HTML report.
# Args:
#     rmdbsDirectory (str): The name of the directory containing the log files.
#     directoryName (str): The directory to port report to.
def parseLog(logDirectory, directoryName):
    fileName = ""
    logContents = {}
    rmdbs = []
    shardGroups = list() #set
    dbCounter = 1
    ruidLists = {}
    logFiles = []
    allRUIDs = list() #set
    dbIds = {}

    if directoryName == '.':
        dir_base_name = os.path.basename(os.getcwd())
    else:
        dir_base_name = os.path.basename(os.path.normpath(directoryName))
    
    report_dir = os.path.join(logDirectory, dir_base_name)

    fileName = "sdbdeploy_gdsctl.lst"
    gdsctl_path = os.path.join(directoryName, fileName)

    if not os.path.exists(gdsctl_path) and not os.path.exists(gdsctl_path + ".gz"):
        print(f"'{fileName}' not found. Scanning directory for 'gdsctl.lst' files...")
        found_gdsctl_file = None
        for f in os.listdir(directoryName):
            if "gdsctl.lst" in f:
                found_gdsctl_file = f
                break
        
        if found_gdsctl_file:
            fileName = found_gdsctl_file
            print(f"Found gdsctl log file: {fileName}")
        else:
            raise FileNotFoundError(f"Error: No gdsctl log file found in '{directoryName}'.")
    else:
        print("Found gdsctl log file!")

    try:
        filepath = os.path.join(directoryName, fileName)
        if filepath.endswith('.gz'):
            unzipped_path = filepath[:-3]
            if not os.path.exists(unzipped_path):
                with gzip.open(filepath, 'rb') as f_in:
                    with open(unzipped_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            print(f"Unzipped {filepath} to {unzipped_path}")
            filepath = unzipped_path
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
            logFileLines = file.readlines()

    except (FileNotFoundError, OSError) as e:
        raise FileNotFoundError(f"Error: Could not open or read file '{fileName}': {e}")

    if not logFileLines:
        raise ValueError("Error: No lines in the log file  file '{}'!".format(fileName))
    
    extractionDirectory = directoryName

    for i in range(len(logFileLines)):
        if log_parser.ADDSHARD_PREFIX in logFileLines[i]:
            targetLines = log_parser.fetchAddShardInfo(logFileLines, i)
            if len(targetLines) < 2:
                raise ValueError("Error from add shard command on line {}, failed to fetch db + shardgroup info lines!".format(i + 1))
            shardGroup, rmdb = log_parser.parseAddShard(targetLines)
            if shardGroup == "NULL":
                raise ValueError("Error from add shard command on line {}, failed to parse db + shardgroup info lines!".format(i + 1))

            if shardGroup not in shardGroups:
                shardGroups.append(shardGroup)
            dbIds[rmdb] = dbCounter
            rmdbs.append({'dbName': rmdb, 'dbID': dbCounter, 'shardGroup': shardGroup, 'logFolderNames' : file_parser.findMainDirs(os.path.join(extractionDirectory, 'diag', 'rdbms', rmdb))})
            dbCounter += 10

    print("SHARD GROUPS: ", shardGroups)
    print("RMDBS", rmdbs)

    for rmdb in rmdbs:
        rmdbName = rmdb['dbName']
        targetLog = os.path.join(extractionDirectory, 'diag', 'rdbms', rmdbName)
        try:
            unzipped_log_files = file_parser.findLogFilesInDir(targetLog, report_dir)
            for log_file in unzipped_log_files:
                logFiles.append({'dbName': rmdbName, 'logFile': log_file, 'originalLogFile': targetLog})
        except Exception as e:
            raise FileNotFoundError("Error: Failed to find log file for {}, {}".format(rmdbName, type(e).__name__))

    for logFile in logFiles:
        try:
            with open(logFile['logFile'], 'r', encoding='utf-8', errors='ignore') as file:
                logLines = file.readlines()
            if logFile['dbName'] not in ruidLists:
                ruidLists[logFile['dbName']] = list() #set
            for i in range(len(logLines)):
                ruID = log_parser.parseRUIDLine(logLines[i])
                if ruID > 0:
                    if ruID not in ruidLists[logFile['dbName']]:
                        ruidLists[logFile['dbName']].append(ruID)
        except Exception as e:
            raise ValueError("Error: Failed to parse log file for {}, {}".format(logFile['dbName'], type(e).__name__))

        print("RUIDS for {}".format(logFile['dbName']), ruidLists[logFile['dbName']])

    for ruids in ruidLists.values():
        for ruid in ruids:
            if ruid not in allRUIDs:
                allRUIDs.append(ruid)

    print("Parsing History")

    toUnzip = report_dir
    if "C:" not in report_dir:
        toUnzip = os.path.join(os.getcwd(), report_dir)

    logContents['rmdbs'] = rmdbs
    logContents['shardGroups'] = shardGroups
    logContents['history'], _ = log_parser.parseHistory(allRUIDs, rmdbs, logFiles, dbIds, report_dir)

    cache_path = os.path.join(os.path.dirname(logDirectory), 'clean_run_errors_cache.parquet')
    clean_run_errors = []
    if os.path.exists(cache_path):
        print(f"Reading clean run error cache from {cache_path}")
        df = pd.read_parquet(cache_path)
        clean_run_errors = df.to_dict('records')
        for error in clean_run_errors:
            ruid = error.get('ruid')
            shard_group = error.get('shard_group')
            term = error.get('term')
            code = error.get('code')
            ospid = error.get('ospid', -1)
            breakpoint()    
            termGroup = logContents['history'][ruid][shard_group][term]

            for event in termGroup.get('errors', []):
                isEqual = (event.get('code') == code) and (event.get('ospid', -1) == ospid)
                #breakpoint()
                if isEqual:
                    event['isOld'] = True
                    break

    logContents['allRUIDS'] = allRUIDs
    logContents['logDirectory'] = directoryName
    logContents['trace_errors'], logContents['watson_errors'] = log_parser.parseWatsonLog(directoryName, toUnzip)
    logContents['gsm_errors'] = log_parser.parse_gsm_logs(report_dir, directoryName)

    # Read clean run cache if it exists
    cache_path = os.path.join(os.path.dirname(logDirectory), 'clean_run_errors_cache.parquet')
    clean_run_errors = []
    all_new_errors = []
    if os.path.exists(cache_path):
        print(f"Reading clean run error cache from {cache_path}")
        df = pd.read_parquet(cache_path)
        clean_run_errors = df.to_dict('records')
        for error in clean_run_errors:
            ruid = error.get('ruid')
            shard_group = error.get('shard_group')
            term = error.get('term')
            timestamp = error.get('timestamp')
            if ruid and shard_group and term and ruid in logContents['history'] and shard_group in logContents['history'][ruid]:
                for event in logContents['history'][ruid][shard_group]:
                    if event['timestamp'] == timestamp:
                        event['isNew'] = False

    # Calculate Clean Run Diff
    all_current_errors = logContents.get('trace_errors', []) + logContents.get('watson_errors', []) + logContents.get('gsm_errors', [])
    
    def is_error_in_list(error, error_list):
        # Simplified comparison based on a few key fields
        for e in error_list:
            if error.get('code') == e.get('code') and error.get('original') == e.get('original'):
                return True
        return False

    clean_run_diff = [error for error in all_current_errors if not is_error_in_list(error, clean_run_errors)]
    logContents['clean_run_diff'] = clean_run_diff

    # Identify term histories with new errors
    terms_with_new_errors = set()
    for error in clean_run_diff:
        ruid = error.get('ruid')
        shard_group = error.get('shard_group')
        term = error.get('term')
        if ruid and shard_group and term:
            terms_with_new_errors.add((ruid, shard_group, term))
    logContents['terms_with_new_errors'] = terms_with_new_errors

    print("Creating Log Folder")

    html_parser.createLogFolder(logContents, report_dir)

    return logContents



if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            raise ValueError("Usage: python main.py <report_directory> <target_directory(Optional)>")
        else:
            directoryName = sys.argv[1]
            rmdbsDirectory = None
            if len(sys.argv) > 2:
                rmdbsDirectory = sys.argv[2]
            else:
                rmdbsDirectory = '.'
            parseLog(directoryName, rmdbsDirectory)
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting...")
        sys.exit(0)