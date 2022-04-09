#update process written in python 3, meant to run with daily to repair records that failed to enter in service request system
#This works in combination of eActivityCheckerRepair class and EmailUtilities class
#sends an email report of the successfuly update and failures of records being inserted into database.


#written by Warren Kunkler 2021
import json, sys
from eActivityCheckerRepair import Crk
from eActivityCheckerRepair import WAT
from EmailUtilities import EmailUtil

email = EmailUtil("gis_data_update_process@someplace.come", ["Warren.Kunkler@someplace.com"])




def runUpdate(DatabaseName):
    if DatabaseName == "2WAT232":
        dbObject = WAT(userName, passWord, DatabaseName)
    elif DatabaseName == "2CRK232":
        dbObject=Crk(userName, passWord, DatabaseName)
    else:
        print("Not a valid DB")
        return
    
    dbObject.runUpdateEact()
    Lines = dbObject.getMessages().split('\n')

    Subject = "Run Update on Eactivities in {0}".format(DatabaseName)

    Message= ""
    lineCount = 0
    for line in Lines:

        print(line)
        Message += line + '\n'
        lineCount += 1
    
    msg = """Summary of Output for {0}\n
    \t\tResults\n\n
    _______________________________________\n\n
    Number of Records Processed {1}\n\n
    \t\tFailures\n
    _______________________________________\n\n
    {2}\nServer: Mocha""".format(DatabaseName, lineCount, Message)

    email.sendMessage(Subject, msg)
    return msg

def formatNotinEactQuery(DatabaseName):
    if DatabaseName == "2WAT232":
        dbObject = WAT(userName, passWord, DatabaseName)
    elif DatabaseName == "2CRK232":
        dbObject=Crk(userName, passWord, DatabaseName)
    else:
        print("Not a valid DB")
        return
   
    dbObject.runGetNotinEact()
    Lines = dbObject.getMessages().split("\n")
    
    Subject = "Run eactivities not in {0}".format(DatabaseName)
    APDCountOnly = 0
    APDSiteIDCount = 0
    ResultCount = 0
    ErrorCount = 0
    outputMessage = ""
    ResultMessage = ""
    ErrorMessages = []
    error500 = 0
    errorLine = ""
    errorXY = 0
    LineCount = 0
    for Line in Lines:
        LineCount += 1
        
        nLine = list(map(lambda x: x.strip(), Line.split(',')))
        if len(nLine) == 2 and nLine[1] == "":
            APDCountOnly += 1
            outputMessage += nLine[0] + " NO SITE ID" + '\n'
            
        elif len(nLine) == 2 and nLine[1] != "":
            APDSiteIDCount += 1
            #outputMessage += nLine[0] + " " + nLine[1] + '\n'
        elif len(nLine) >= 3 and "RESULT" in nLine[0]:
            #print(nLine)
            ResultMessage += ", ".join(nLine) + '\n'
            ResultCount += 1
            
        elif len(nLine) >= 3 and "error" in nLine[0]:
            ErrorCount += 1
        
            stringErrorObj = ", ".join(nLine)
            ErrorMessages.append(json.loads(stringErrorObj))
            
            
            
            

        elif "200" not in nLine:
            try:
                stringErrorObj = ", ".join(nLine).replace("f=json&features=[","").replace("]","")
                
                ErrorMessages.append(json.loads(stringErrorObj))
            except:
                continue      


    
    for elem in ErrorMessages:
        print(elem, type(elem))
        if type(elem) != int:
            for key in elem.keys():
                if "ADDR_FRACTION" in elem[key].keys():
                    print("COMP TYPE Description: "+elem[key]["COMP_TYPE_DESC"].strip() + ", Data Status: " + elem[key]["DATA_STATUS_DISPLAY"].strip() + ", Work subtype: "+ elem[key]["WORK_SUBTYPE"].strip() + ", Display Agency: " + elem[key]["DISPLAY_AGENCY"].strip() + ", URL: " + elem[key]["URL"].strip() + ", Number Key: " + elem[key]["NUMBER_KEY"].strip())
                    errorLine+= "COMP TYPE Description: "+elem[key]["COMP_TYPE_DESC"].strip() + ", Data Status: " + elem[key]["DATA_STATUS_DISPLAY"].strip() + ", Work subtype: "+ elem[key]["WORK_SUBTYPE"].strip() + ", Display Agency: " + elem[key]["DISPLAY_AGENCY"].strip() + ", URL: " + elem[key]["URL"].strip() + ", Number Key: " + elem[key]["NUMBER_KEY"].strip() + '\n\n'
                elif "code" in elem[key].keys():
                    error500 += 1
                elif len(elem[key].keys()) ==2:
                    errorXY += 1
                else:
                    print("")
                    print("still left, ", elem[key])
        else:
            errorLine += str(elem) + '\n\n'
    err = APDCountOnly + ErrorCount + error500 + errorXY


    msg = """Summary of Output for {0}\n
    \t\tResults\n\n
    _______________________________________\n\n
    Number of Records Processed {1}\n
    Number of Records added/updated: {2}\n
    Number of Records that Failed: {3}\n\n
    \t\tFailures\n
    _______________________________________\n\n
    {4}\n
    {5}\n
    {6}\nServer: Mocha""".format(DatabaseName, LineCount, APDSiteIDCount, err, outputMessage, ResultMessage, errorLine)



    """msg = Summary Of Output for {0} Query\n
    Number of Records with only APD and no Site ID: {1}\n
    Number of Records with Both APD and Site ID: {2}\n
    Number of Records with returning exception with Result: {3}\n
    Number of Records with Error messages: {4}\n
    Number of Records with 500 error codes: {5}\n
    Number of Records with just xy coordinates: {6}
    --------------------------------------------------------\n
    APDS with associated Site IDs\n\n
    {7}\n_______________________________________________________\nResult Data from issued, finaled or ADR_Base\n\n{8}
    \n\n_________________________________________________________\n
    \t\tErrors\n\n{9}\n\n\t\tServer: Mocha.format(DatabaseName,APDCountOnly, APDSiteIDCount, ResultCount, ErrorCount, error500, errorXY, outputMessage, ResultMessage, errorLine)"""

    email.sendMessage(Subject, msg)
    return msg


print(str(sys.argv))


def afterProcess():
    print("Do you want to do other processing? (y/n)")
    strAns = str(input())
    if strAns == "y":
        runQuestion()
    else:
        print("exiting")
        return


def runQuestion():
    print("Press the corresponding number to:\n(1) Query eactivities that are not in 2CRK232 \n(2) Query eactivities that are not in 2WAT232\n(3) Run an update on 2CRK232\n(4) Run an update on 2WAT232 \nIf None press any other key to exit")
    answer = str(input())

    if answer == "1":

        print("running query of eactivities not in 2CRK232")
        msg = formatNotinEactQuery("2CRK232")
        print(msg)
        afterProcess()
    elif answer == "2":
        print("running query of eactivities not in 2WAT232")
        msg=formatNotinEactQuery("2WAT232")
        print(msg)
        afterProcess()
    elif answer == "3":
        print("running update for 2CRK232")
        msg = runUpdate("2CRK232")
        print(msg)
        afterProcess()
    elif answer == "4":
        print("running update for 2WAT232")
        msg = runUpdate("2WAT232")
        print(msg)
        afterProcess()
    else:
        print("leaving")

if len(sys.argv) > 1 and sys.argv[1] == "OnDemand":
    runQuestion()
else:
    print("starting check of crk")
    
    msg = formatNotinEactQuery("2CRK232")
    print(msg)
    print("starting check of wat")
    msg2 = formatNotinEactQuery("2WAT232")
    print(msg2)
    print("updating 2crk232")
    msg3 = runUpdate("2CRK232")
    print(msg3)
    print("updating 2wat232")
    msg4 = runUpdate("2WAT232")

