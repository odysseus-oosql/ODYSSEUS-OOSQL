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
import PyOOSQL

# define various operating modes
MERGE_OPERATION    = "Merge"
BUILD_OPERATION    = "Build"
STANDARD_OPERATION = "Standard"

def Usage():
    print "USAGE : OOSQL_LoaderMergeBuildPhase2.py"
    print "        <database name> <volume name> [-temporary <database name> <volume name>] [-merge|-build]"
    print "        <data file name> <data file name> ..."

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

    if string.lower(argv[count]) == '-merge':
        count = count + 1
        operatingMode = MERGE_OPERATION
    elif string.lower(argv[count]) == '-build':
        count = count + 1
        operatingMode = BUILD_OPERATION
    else:
        operatingMode = STANDARD_OPERATION

    dataFileNameList = []

    while count < len(argv):        
        CheckArgument(count, argv)
        dataFileNameList.append(argv[count])
        count = count + 1

    return databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, operatingMode, dataFileNameList

def CheckIfDataFileLoaded(historyLines, dataFileName):
    # find a data file name in loading history file
    dataFileLoaded = 0
    for line in historyLines:
        if string.find(line, dataFileName) != -1:
            dataFileLoaded = 1
            break
    return dataFileLoaded

def CheckIfSortedPostingExist(dataFileName, className, attrName):
    # make sorted posting file name
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"
    sortedPostingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, attrName)
    sortedPostingFileName = string.join(string.split(sortedPostingFileName, dirSeparator), '_')
    sortedPostingFileName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, sortedPostingFileName)
    if os.access(sortedPostingFileName, 0):
        return 1
    else:
        return 0

def GetPostingFileNameList(dataFileNameList, className, attrName, sortedPostingExist):
    # get posting file names of input data files
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"

    postingFileNameList = []

    for fileName in dataFileNameList:
        if not sortedPostingExist:
            postingFileName = "%s_TEXT_%s_%s_Posting" % (fileName, className, attrName)
            postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            postingFileNameList.append(postingFileName)
        else:
            postingFileName = "%s_TEXT_%s_%s_SortedPosting" % (fileName, className, attrName)
            postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            postingFileNameList.append(postingFileName)
                
    return postingFileNameList

def LoaderMergeBuild(argv):
    (databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, operatingMode, dataFileNameList) = ParseArgument(argv)

    # mount database
    print "-" * 79
    print "Mount database '%s'" % (databaseName)
    oosqlSystem = PyOOSQL.OOSQL_System()
    databaseID  = oosqlSystem.MountDB(databaseName)
    volumeID    = oosqlSystem.GetVolumeID(databaseID, volumeName)
    temporaryVolumeID = oosqlSystem.MountVolumeByVolumeName(temporaryDatabaseName, temporaryVolumeName)
    
    # set bulk flush mode
    oosqlSystem.SetCfgParam("USE_BULKFLUSH", "TRUE")

    # begin transaction
    print "-" * 79
    print "Transaction begin"
    oosqlSystem.TransBegin(PyOOSQL.X_BROWSE_BROWSE)

    # get className and attribute list in the datafile
    f = open(dataFileNameList[0], "r")
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

    # prepare temporary path
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"

    # checking previous loading history
    print "-" * 79
    print "Checking necessary data files..."
    historyFileName = "%s%s%s_%s_history" % (env_ODYS_TEMP_PATH, dirSeparator, databaseName, volumeName)
    f = open(historyFileName, "r")
    historyLines = f.readlines()
    f.close()
    for dataFileName in dataFileNameList:
        dataFileLoaded = CheckIfDataFileLoaded(historyLines, dataFileName)
        if not dataFileLoaded:
            print "Checking done. '%s' was not loaded. So, abort building..." % (dataFileName)
            oosqlSystem.TransAbort()
            sys.exit(1)

    print "Checking done. All necessary data files are loaded."

    # checking posting files
    print "-" * 79
    print "Checking necessary posting files..."
    for attrName in attributes:
        sortedPostingExist = CheckIfSortedPostingExist(dataFileNameList[0], className, attrName)
        postingFileNameList = GetPostingFileNameList(dataFileNameList, className, attrName, sortedPostingExist)
        for postingFileName in postingFileNameList:
            if not os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName), 0):
                print "Checking done. '%s' does not exist. So, abort building..." % (postingFileName)
                oosqlSystem.TransAbort()
                sys.exit(1)

    print "Checking done. All necessary posting files exist."

    if operatingMode == MERGE_OPERATION or operatingMode == STANDARD_OPERATION:
    print "Start Merging"
    os.system("date")
    # merging
    for attrName in attributes:
        print "-" * 79
        print "Merging posting for class '%s', attribute '%s'" % (className, attrName)
        sortedPostingExist = CheckIfSortedPostingExist(dataFileNameList[0], className, attrName)
        postingFileNameList = GetPostingFileNameList(dataFileNameList, className, attrName, sortedPostingExist)

        if not sortedPostingExist:
            # merge posting files
            newPostingFileName = "TEXT_%s_%s_Posting" % (className, attrName)
            if os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, newPostingFileName), 0):
                print "Skip merging of class '%s', attribute '%s'" % (className, attrName)
            else:
                    if operatingMode == MERGE_OPERATION:
                        destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, newPostingFileName)
                        os.mkfifo(destName)
                        print "The FIFO file has been created. Run OOSQL_LoaderMergeBuildPhase2.py with -build option."
                oosqlSystem.Tool_MergePosting(postingFileNameList, newPostingFileName)
        else:
            # sort and merge posting files
            newPostingFileName = "TEXT_%s_%s_SortedPosting" % (className, attrName)
            if os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, newPostingFileName), 0):
                print "Skip merging of class '%s', attribute '%s'" % (className, attrName)
            else:
                    if operatingMode == MERGE_OPERATION:
                        destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, newPostingFileName)
                        os.mkfifo(destName)
                        print "The FIFO file has been created. Run OOSQL_LoaderMergeBuildPhase2.py with -build option."
                oosqlSystem.Tool_MergePosting(postingFileNameList, newPostingFileName)
    print "End Merging"
    os.system("date")
            
    if operatingMode == BUILD_OPERATION or operatingMode == STANDARD_OPERATION:
    # build text index
    config = PyOOSQL.lom_Text_ConfigForInvertedIndexBuild()
    config.isUsingBulkLoading                    = 1
    config.isUsingKeywordIndexBulkLoading        = 1
    config.isUsingReverseKeywordIndexBulkLoading = 1
    config.isBuildingExternalReverseKeywordFile  = 0
    config.isBuildingDocIdIndex                  = 1
    config.isSortingPostingFile                  = 1
    config.isUsingStoredPosting                  = 0
    
    print "Start Building Keyword Index"
    os.system("date")
    for attrName in attributes:
        print "-" * 79
        print "Build text index for class '%s', attribute '%s'" % (className, attrName)
        sortedPostingExist = CheckIfSortedPostingExist(dataFileNameList[0], className, attrName)
        if sortedPostingExist:
            config.isSortingPostingFile = 0
        else:
            config.isSortingPostingFile = 1
        oosqlSystem.Tool_BuildTextIndex(volumeID, temporaryVolumeID, className, attrName, config)
    print "End Building Keyword Index"
    os.system("date")

    # commit transaction
    print "-" * 79
    print "Transaction commit"
    oosqlSystem.TransCommit()

    # dismount database
    print "-" * 79
    print "Dismount database"
    oosqlSystem.Dismount(temporaryVolumeID)
    oosqlSystem.DismountDB(databaseID)

    if operatingMode == MERGE_OPERATION:
        # wait indefinitely
        print "-" * 79
        ch = raw_input("Press the ENTER key when the bulkloading is completed. ")
    elif operatingMode == BUILD_OPERATION or operatingMode == STANDARD_OPERATION:
    # unlink posting file and sorted posting file
    for attrName in attributes:
        postingFileName = "TEXT_%s_%s_Posting" % (className, attrName);
        destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName)
        try:
            os.unlink(destName)
        except OSError:
            pass
        sortedPostingFileName = "TEXT_%s_%s_SortedPosting" % (className, attrName);
        destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, sortedPostingFileName)
        try:
            os.unlink(destName)
        except OSError:
            pass
        sortedPostingExist = CheckIfSortedPostingExist(dataFileNameList[0], className, attrName)
        postingFileNameList = GetPostingFileNameList(dataFileNameList, className, attrName, sortedPostingExist)
        for postingFileName in postingFileNameList:
            destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName)
            try:
                    pass
            except OSError:
                pass

    return

LoaderMergeBuild(sys.argv)

