#! /usr/bin/env python

#/******************************************************************************/
#/*                                                                            */
#/*    Copyright (c) 1990-2016, KAIST                                          */
#/*    All rights reserved.                                                    */
#/*                                                                            */
#/*    Redistribution and use in source and binary forms, with or without      */
#/*    modification, are permitted provided that the following conditions      */
#/*    are met:                                                                */
#/*                                                                            */
#/*    1. Redistributions of source code must retain the above copyright       */
#/*       notice, this list of conditions and the following disclaimer.        */
#/*                                                                            */
#/*    2. Redistributions in binary form must reproduce the above copyright    */
#/*       notice, this list of conditions and the following disclaimer in      */
#/*       the documentation and/or other materials provided with the           */
#/*       distribution.                                                        */
#/*                                                                            */
#/*    3. Neither the name of the copyright holder nor the names of its        */
#/*       contributors may be used to endorse or promote products derived      */
#/*       from this software without specific prior written permission.        */
#/*                                                                            */
#/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
#/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
#/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
#/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
#/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
#/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
#/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
#/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
#/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
#/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
#/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
#/*    POSSIBILITY OF SUCH DAMAGE.                                             */
#/*                                                                            */
#/******************************************************************************/
#/******************************************************************************/
#/*                                                                            */
#/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
#/*    Version 5.0                                                             */
#/*                                                                            */
#/*    Developed by Professor Kyu-Young Whang et al.                           */
#/*                                                                            */
#/*    Advanced Information Technology Research Center (AITrc)                 */
#/*    Korea Advanced Institute of Science and Technology (KAIST)              */
#/*                                                                            */
#/*    e-mail: odysseus.oosql@gmail.com                                        */
#/*                                                                            */
#/*    Bibliography:                                                           */
#/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
#/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
#/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
#/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
#/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
#/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
#/*        Demonstration Award.                                                */
#/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
#/*        Storage Structure Using Subindexes and Large Objects for Tight      */
#/*        Coupling of Information Retrieval with Database Management          */
#/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
#/*        (1999)).                                                            */
#/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
#/*        J., "Tightly-Coupled Spatial Database Features in the               */
#/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
#/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
#/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
#/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
#/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
#/*                                                                            */
#/******************************************************************************/

import string
import sys
import os
import time
import thread
import PyOOSQL

# total number of exited processes
nExitedProcesses = 0

def Usage():
    print "USAGE : OOSQL_LoaderParallelExtract.py"
    print "        <database name> <volume name> [-temporary <database name> <volume name>]"
    print "        -datafile <data file name> ... [-mergedfile <merged data file name>]"
    print "        -dividedby <number to divide> [-storeMergedPosting]"

def CheckArgument(count, argv):
    if count >= len(argv):
        Usage()
        sys.exit(1)
        
def ParseArgument(argv):
    count = 1

    CheckArgument(count, argv)
    databaseName = argv[count]
    count = count + 1

    CheckArgument(count, argv)
    volumeName = argv[count]
    count = count + 1

    if string.lower(argv[count]) == '-temporary':
        count = count + 1

        CheckArgument(count, argv)
        temporaryDatabaseName = argv[count]
        count = count + 1

        CheckArgument(count, argv)
        temporaryVolumeName = argv[count]
        count = count + 1
    else:
        temporaryDatabaseName = databaseName
        temporaryVolumeName   = volumeName

    dataFileNameList = []
    
    while 1:
        if string.lower(argv[count]) == '-datafile':
            count = count + 1
            
            CheckArgument(count, argv)
            dataFileNameList.append(argv[count])
            count = count + 1
        else:
            break

    if string.lower(argv[count]) == '-mergedfile':
        count = count + 1

        CheckArgument(count, argv)
        mergedFileName = argv[count]
        count = count + 1
    else:
        mergedFileName = dataFileNameList[0] + '_merged'
  
    if string.lower(argv[count]) == '-dividedby':
        count = count + 1

        CheckArgument(count, argv)
        divideNumber = string.atoi(argv[count])
        count = count + 1
    else:
        divideNumber = 4
        
    if count < len(argv) and string.lower(argv[count]) == '-storemergedposting':
        mergedPostingFlag = 1
    else:
        mergedPostingFlag = 0        
    
    return databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileNameList, mergedFileName, divideNumber, mergedPostingFlag

def CheckIfMergedPostingExist(dataFileName, className, attrName):
    # make posting and sorted posting file name
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"
    postingFileName = "%s_TEXT_%s_%s_Posting" % (dataFileName, className, attrName);
    postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
    postingFileName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName)
    sortedPostingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, attrName);
    sortedPostingFileName = string.join(string.split(sortedPostingFileName, dirSeparator), '_')
    sortedPostingFileName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, sortedPostingFileName)
    if os.access(postingFileName, 0) or os.access(sortedPostingFileName, 0):
        return 1
    else:
        return 0

def CheckIfSortedPostingExist(dataFileName, className, attrName):
    # make sorted posting file name
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"
    sortedPostingFileName = "%s_TEXT_%s_%s_SortedPosting_0" % (dataFileName, className, attrName);
    sortedPostingFileName = string.join(string.split(sortedPostingFileName, dirSeparator), '_')
    sortedPostingFileName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, sortedPostingFileName)
    if os.access(sortedPostingFileName, 0):
        return 1
    else:
        return 0

def GetPostingFileNameList(dataFileName, className, attrName, sortedPostingExist):
    nPostingFiles = 0
    postingFileNameList = []

    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"
        
    if not sortedPostingExist:
        while 1:
            postingFileName = "%s_TEXT_%s_%s_Posting_%d" % (dataFileName, className, attrName, nPostingFiles);
            postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            postingFilePath = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName)
            if os.access(postingFilePath, 0):
                nPostingFiles = nPostingFiles + 1
                postingFileNameList.append(postingFileName)
            else:
                break
    else:
        while 1:
            postingFileName = "%s_TEXT_%s_%s_SortedPosting_%d" % (dataFileName, className, attrName, nPostingFiles);
            postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            postingFilePath = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName)
            if os.access(postingFilePath, 0):
                nPostingFiles = nPostingFiles + 1
                postingFileNameList.append(postingFileName)
            else:
                break
                
    return postingFileNameList

def LoaderMergePosting(databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileName):
    # mount database
    oosqlSystem = PyOOSQL.OOSQL_System()
    databaseID  = oosqlSystem.MountDB(databaseName)
    volumeID    = oosqlSystem.GetVolumeID(databaseID, volumeName)
    temporaryVolumeID = oosqlSystem.MountVolumeByVolumeName(temporaryDatabaseName, temporaryVolumeName)

    # begin transaction
    oosqlSystem.TransBegin(PyOOSQL.X_RR_RR)

    # get className and attribute list in the datafile
    f = open(dataFileName, "r")
    classLine = f.readline()
    f.close()
    # replace '(' and ')' into <space>
    classLine = string.join(string.split(classLine, '('), ' ')
    classLine = string.join(string.split(classLine, ')'), ' ')
    # retrives className and attributesInDatafile    
    classLineSplitted = string.split(classLine)
    className = classLineSplitted[1]
    attributesInDatafile = classLineSplitted[2:]
    
    # get text attributes in the database schema
    (className, classId, attributeInfos, indexInfos) = oosqlSystem.GetTableDescription(volumeID, className)
    attributes = []
    for attributeInfo in attributeInfos:
        if attributeInfo[2] == PyOOSQL.OOSQL_TYPE_TEXT:
            if attributeInfo[0] in attributesInDatafile:
                attributes.append(attributeInfo[0])

    # merging
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"

    for attrName in attributes:
        print "-" * 79
        print "Merge posting for class '%s', attribute '%s'" % (className, attrName)
        # if merged posting already exists, skip merging
        mergedPostingExist = CheckIfMergedPostingExist(dataFileName, className, attrName)
        if mergedPostingExist:
            continue

        sortedPostingExist = CheckIfSortedPostingExist(dataFileName, className, attrName)
        postingFileNameList = GetPostingFileNameList(dataFileName, className, attrName, sortedPostingExist)

        if not sortedPostingExist:
            newPostingFileName = "%s_TEXT_%s_%s_Posting" % (dataFileName, className, attrName);
            newPostingFileName = string.join(string.split(newPostingFileName, dirSeparator), '_')
            # merge divided posting files 
            oosqlSystem.Tool_MergePosting(postingFileNameList, newPostingFileName)
        else:
            newPostingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, attrName);
            newPostingFileName = string.join(string.split(newPostingFileName, dirSeparator), '_')
            # merge divided posting files 
            oosqlSystem.Tool_MergePosting(postingFileNameList, newPostingFileName)
    
    print "-" * 79
          
    # commit transaction
    oosqlSystem.TransCommit()

    # dismount database
    oosqlSystem.Dismount(temporaryVolumeID)
    oosqlSystem.DismountDB(databaseID)

    # unlink posting file and sorted posting file
    for attrName in attributes:
        sortedPostingExist = CheckIfSortedPostingExist(dataFileName, className, attrName)
        postingFileNameList = GetPostingFileNameList(dataFileName, className, attrName, sortedPostingExist)
        for postingFileName in postingFileNameList:
            destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName)
            try:
                os.unlink(destName)
            except OSError:
                pass

def LoaderParallelExtract(argv):
    (databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileNameList, mergedFileName, divideNumber, mergedPostingFlag) = ParseArgument(argv)

    if len(dataFileNameList) == 1:
        dataFileName = dataFileNameList[0]
    else:
        numSourceDataFiles = PyOOSQL.MergeDataInLoadDbFiles(dataFileNameList, mergedFileName)
        dataFileName = mergedFileName

    numObjectsInFile    = PyOOSQL.CountObjectsInLoadDbFile(dataFileName)
    numObjectsToExtract = numObjectsInFile / divideNumber
    
    extractorName = '_ExtractKeyword.py'
    
    startObjectNo = 0
    endObjectNo   = numObjectsToExtract - 1

    for i in range(0, divideNumber):

        if i == divideNumber - 1:
            endObjectNo = -1
        
        arguments = []
        arguments.append(extractorName)			# program
        arguments.append(databaseName)			# database name
        arguments.append(volumeName)			# volume name
        arguments.append('-temporary')			# -temporary
        arguments.append(temporaryDatabaseName)		# temporary database name
        arguments.append(temporaryVolumeName)		# temporary volume name
        arguments.append(dataFileName)			# data file name
        arguments.append(str(startObjectNo))		# start object no
        arguments.append(str(endObjectNo))		# end object no
        arguments.append(str(i))			# process no

        pid = os.fork()
        if not pid:
            # child process execution part
            # execute keyword extractor
            os.execvp(extractorName, arguments)
        else:
            # parent process execution part
            # execute monitoring thread
            thread.start_new_thread(MonitorProcess, (i, pid, extractorName, arguments, divideNumber,))

            # adjust start and end object no.
            startObjectNo = endObjectNo + 1
            endObjectNo   = startObjectNo + numObjectsToExtract - 1

    # execute polling
    while 1:
        # if all child process are exited
        if nExitedProcesses == divideNumber:
            break
        else:
            time.sleep(1)

    # handle -storeMergedPosting argument
    if mergedPostingFlag:
        # merge sorted posting files which are extracted in parallel
        LoaderMergePosting(databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileName)
        print "Parallel keyword extraction is done"
    else:
        # nothing to do
        print "Parallel keyword extraction is done"

def MonitorProcess(no, pid, extractorName, arguments, divideNumber):
    # wait for completion of a child process
    childpid, status = os.waitpid(pid, 0)
    # if the process exited using the exit() system call
    if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
        global nExitedProcesses
        nExitedProcesses = nExitedProcesses + 1
        print "%d of %d extraction are completed" % (nExitedProcesses, divideNumber)
    # if the process exited due to a signal
    elif os.WIFSIGNALED(status) or os.WIFSTOPPED(status) or \
        (os.WIFEXITED(status) and os.WEXITSTATUS(status) != 0):
        print "process %d was exited due to a segmentation fault" % (no)
        print "process %d is restarting automatically" % (no)
        childpid = os.fork()
        if not childpid:
            # child process execution part
            # execute keyword extractor
            os.execvp(extractorName, arguments)
        else:
            # parent process execution part
            # execute monitoring thread
            thread.start_new_thread(MonitorProcess, (no, childpid, extractorName, arguments, divideNumber,))

LoaderParallelExtract(sys.argv)

