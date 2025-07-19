import tarfile
import os
import gzip
import shutil
MIN_LINES_FOR_LOG = 30
DEBUG_STRING = "debug_"
AIME_STRING = "aime"
# Opens and extracts a tar.gz file to a specified destination.
# Args:
#     filePath (str): The path to the tar.gz file.
#     destination (str): The destination directory for the extracted files.
def openTarDirectory(filePath, destination):
    try:
        with tarfile.open(filePath, 'r:gz') as tar:
            for member in tar.getmembers():
                target_path = os.path.join(destination, member.name)
                if not os.path.exists(target_path):
                    tar.extract(member, path=destination)
        print("Successfully extracted new files from '{}' to '{}'".format(filePath, destination))
    except tarfile.ReadError as e:
        print("Error reading tar file: {}".format(e))
    except FileNotFoundError:
        print("Error: Tar file '{}' not found.".format(filePath))
    except Exception as e:
        print("An unexpected error occurred: {}".format(e))

# Finds the first log file in a directory with more than MIN_LINES_FOR_LOG lines.
# Args:
#     directory (str): The directory to search.
# Returns:
#     str: The path to the log file.
# Raises:
#     FileNotFoundError: If no log file is found.
def findLogFile(directory, destination_dir):
    targetDir = ""
    with os.scandir(directory) as entries:
        for item in entries:
            print("Scanning: ", item.name)
            if item.is_dir():
                if AIME_STRING in item.name:
                    targetDir = item.name
                    break
    
    log_path = os.path.join(directory, targetDir, 'log', f"debug_{targetDir}.log")
    gz_log_path = log_path + ".gz"

    if os.path.exists(gz_log_path):
        os.makedirs(destination_dir, exist_ok=True)
        unzipped_log_filename = f"debug_{targetDir}.log"
        unzipped_log_path = os.path.join(destination_dir, unzipped_log_filename)
        with gzip.open(gz_log_path, 'rb') as f_in:
            with open(unzipped_log_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Unzipped {gz_log_path} to {unzipped_log_path}")
        return unzipped_log_path
    
    print(log_path)
    return log_path

def findMainDir(directory):
    with os.scandir(directory) as entries:
        for item in entries:
            print("Scanning: ", item.name)
            if item.is_dir():
                if AIME_STRING in item.name:
                    return item.name

# Finds the log/diag folder in a directory.
# Args:
#     directory (str): The directory to search.
# Returns:
#     str: The name of the log folder.
# Raises:
#     FileNotFoundError: If no log folder is found.
def findLogFolder(directory):
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.is_dir():
                if entry.name == 'diag':
                    return entry.name
    raise FileNotFoundError

    