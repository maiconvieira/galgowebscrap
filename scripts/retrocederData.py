def retrocederData(parameter):
    partsOfDate = parameter.split('-')
    lastLineYear = int(partsOfDate[0])
    lastLineMonth = int(partsOfDate[1])
    lastLineDay = int(partsOfDate[2])
    if lastLineDay == 26 and lastLineMonth == 12:
        dayToScrap = str(lastLineDay - 2).zfill(2)
        monthToScrap = str(lastLineMonth).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay > 1 and lastLineMonth > 1:
        dayToScrap = str(lastLineDay - 1).zfill(2)
        monthToScrap = str(lastLineMonth).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay > 1 and lastLineMonth == 1:
        dayToScrap = str(lastLineDay - 1).zfill(2)
        monthToScrap = str(lastLineMonth).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth == 3 and ((lastLineYear%4 == 0 and lastLineYear%100 != 0) or (lastLineYear%400 == 0)):
        dayToScrap = str(29)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth == 3 and ((lastLineYear%4 != 0 and lastLineYear%100 == 0) or (lastLineYear%400 != 0)):
        dayToScrap = str(28)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth in(2, 4, 6, 8, 9, 11):
        dayToScrap = str(31)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth in(5, 7, 10, 12):
        dayToScrap = str(30)
        monthToScrap = str(lastLineMonth - 1).zfill(2)
        yearToScrap = str(lastLineYear).zfill(4)
    elif lastLineDay == 1 and lastLineMonth == 1:
        dayToScrap = str(31)
        monthToScrap = str(12)
        yearToScrap = str(lastLineYear -1).zfill(4)
    racingDate = yearToScrap + '-' + monthToScrap + '-' + dayToScrap
    return racingDate