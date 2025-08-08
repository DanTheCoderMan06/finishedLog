import datetime

def findNearestTimestamp(filePath, target):
    last_line_before = 0
    targetTimeStamp = datetime.datetime.fromisoformat(target).replace(tzinfo=None)

    with open(filePath, 'r', encoding='utf-8', errors='ignore') as fp:
        for i, line in enumerate(fp):
            for word in line.split():
                try:
                    currentStamp = datetime.datetime.fromisoformat(word.strip()).replace(tzinfo=None)
                    delta = abs((currentStamp - targetTimeStamp).total_seconds())
                    if delta < 2:
                        return i
                    
                    if currentStamp <= targetTimeStamp:
                        last_line_before = i
                except ValueError:
                    pass
    
    return last_line_before

testFile = "C:/Users/danii/OneDrive/Documents/cs/finishedLog/scratch/reports/lrgdbcongsmshsnr17/aime15_ora_792879_gwr_4_21.trc"

print(findNearestTimestamp(testFile,"2025-07-04T15:43:01.511127+00:00"))
