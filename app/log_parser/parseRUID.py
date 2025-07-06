import os
import re

RU_ID_STRING = "RU_ID"

# Parses a line to find the RUID.
# Args:
#     line (str): The line to parse.
# Returns:
#     int: The RUID if found, otherwise -1.
def parseRUIDLine(line):
    if RU_ID_STRING not in line:
        return -1

    lineWords = line.split(" ")
    for i in range(len(lineWords)):
        if lineWords[i] == RU_ID_STRING:
            ruID = lineWords[i + 1].strip()
            ruID = re.sub(r"\D", "", ruID)
            return int(ruID)
