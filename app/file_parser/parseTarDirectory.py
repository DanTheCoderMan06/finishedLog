import tarfile
import os

MIN_LINES_FOR_LOG = 30
DEBUG_STRING = "debug_"
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
def findLogFile(directory):
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.log'):
                full_path = os.path.join(dirpath, filename)
                if DEBUG_STRING in filename:
                    return full_path
    raise FileNotFoundError

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

    