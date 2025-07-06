# args: list[str] -> lines composing an add shard command,
# return: list[dict] -> Found rmdbs
ADDSHARD_PREFIX = "Command name: add shard "
SHARDGROUP_PREFIX = "shardgroup :"
DBNAME_PREFIX = "cdb : "

# Fetches shard group and database name lines from a list of file lines.
# Args:
#     fileLines (list): A list of strings representing the lines in a file.
#     start (int): The line number to start searching from.
# Returns:
#     list: A list containing the shard group and database name lines, or an empty list if not found.
def fetchAddShardInfo(fileLines, start):
    targetLines = ["NULL", "NULL"]
    try:
        while "NULL" in targetLines:
            start += 1
            nextLine = fileLines[start]
            if SHARDGROUP_PREFIX in nextLine:
                targetLines[0] = nextLine
            if DBNAME_PREFIX in nextLine:
                targetLines[1] = nextLine
    except IndexError:
        return []
    return targetLines

# Parses the shard group and database name from a list of lines.
# Args:
#     lines (list): A list of strings containing the shard group and database name information.
# Returns:
#     tuple: A tuple containing the shard group and database name.
def parseAddShard(lines):
    dbGroup = ""
    dbName = ""

    try:
        shardGroupLine = lines[0]
        shardGroupWords = shardGroupLine.split(' ')
        for i in range(len(shardGroupWords)):
            if shardGroupWords[i] == ":":
                dbGroup = shardGroupWords[i + 1]
    except IndexError:
        return "NULL", "NULL"

    try:
        dbNameLine = lines[1]
        dbNameWords = dbNameLine.split(' ')
        for i in range(len(dbNameWords)):
            if dbNameWords[i] == ":":
                dbName = dbNameWords[i + 1]
    except IndexError:
        return "NULL", "NULL"

    return dbGroup, dbName

        

    