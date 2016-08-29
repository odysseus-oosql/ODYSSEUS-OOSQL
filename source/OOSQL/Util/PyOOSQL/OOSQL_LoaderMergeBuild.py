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

# define various loading modes
INITIAL_BULKLOADING         = "Initial Bulkloading"         
SMALL_APPEND_BULKLOADING    = "Small Append Bulkloading" 
MEDIUM_APPEND_BULKLOADING   = "Medium Append Bulkloading"
LARGE_APPEND_BULKLOADING    = "Large Append Bulkloading"

#define various merging modes
NO_MERGE       = 0
UNSORTED_MERGE = 1
SORTED_MERGE   = 2

def Usage():
    print "USAGE : OOSQL_LoaderMergeBuild.py"
    print "        [-pagerank <pagerank file name>] <database name> <volume name> [-temporary <database name> <volume name>] <data file name>"

def CheckArgument(count, argv):
    if count >= len(argv):
        Usage()
        sys.exit(1)
        
def ParseArgument(argv):
    count = 1

    CheckArgument(count, argv)
    # PageRank Option 
    if string.lower(argv[count]) == '-pagerank':
        count = count + 1

        CheckArgument(count, argv)
        pagerankFileName = argv[count]
        count = count + 1
        
        pagerankMode = 1
    else:
        pagerankFileName = ""
        pagerankMode = 0


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
        
    CheckArgument(count, argv)
    dataFileName = argv[count]
    count = count + 1

    return databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileName, pagerankFileName, pagerankMode

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
    if os.access(sortedPostingFileName, 0) or os.access(sortedPostingFileName[:-2], 0):
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
    
def LoaderMergeBuild(argv):
    (databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileName, pagerankFileName, pagerankMode) = ParseArgument(argv)

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

    # prepare temporary path
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"
   
    # determine loadingMode : INITIAL_BULKLOADING, SMALL_APPEND_BULKLOADING, MEDIUM_APPEND_BULKLOADING, LARGE_APPEND_BULKLOADING
    numObjectsInDatabase = oosqlSystem.GetNumObjectsInClass(volumeID, className)
    numObjectsInFile     = PyOOSQL.CountObjectsInLoadDbFile(dataFileName)
    print "Objects in the class '%s' is %d" % (className, numObjectsInDatabase)
    print "Objects in the file '%s' is %d" % (dataFileName, numObjectsInFile)
    if numObjectsInDatabase == 0:
        loadingMode = INITIAL_BULKLOADING
    elif numObjectsInFile < 2000:
        loadingMode = SMALL_APPEND_BULKLOADING
    elif numObjectsInDatabase * 0.1 > numObjectsInFile:
        loadingMode = MEDIUM_APPEND_BULKLOADING
    else:
        loadingMode = LARGE_APPEND_BULKLOADING
    print "Loading Mode :", loadingMode

    # loaddb
    print "-" * 79
    print "Load data from '%s'" % (dataFileName)
    isDeferredTextIndexMode = 1
    useBulkloading          = 1
    useDescriptorUpdating   = 1
    if loadingMode == SMALL_APPEND_BULKLOADING or loadingMode == MEDIUM_APPEND_BULKLOADING:
        smallUpdateFlag     = 1
    else:
        smallUpdateFlag     = 0
    print "Start Data Loading"
    os.system("date")

    oosqlSystem.Tool_LoadDB(volumeID, temporaryVolumeID, isDeferredTextIndexMode, smallUpdateFlag, useBulkloading, 
                            useDescriptorUpdating, dataFileName, pagerankFileName, pagerankMode)

    print "End Data Loading"
    os.system("date")

    # mapping
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"
        
    print "Start Map Posting"
    os.system("date")
    for attrName in attributes:
        print "-" * 79
        print "Map posting for class '%s', attribute '%s'" % (className, attrName)
        mergedPostingExist = CheckIfMergedPostingExist(dataFileName, className, attrName)
        sortedPostingExist = CheckIfSortedPostingExist(dataFileName, className, attrName)

        if not mergedPostingExist:
            print "Merge posting files during mapping"
            postingFileNameList = GetPostingFileNameList(dataFileName, className, attrName, sortedPostingExist)
        else:
            print "Use merged posting file for mapping"
            if not sortedPostingExist:
                postingFileName = "%s_TEXT_%s_%s_Posting" % (dataFileName, className, attrName);
                postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
                postingFileNameList = [postingFileName]
            else:
                postingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, attrName);
                postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
                postingFileNameList = [postingFileName]

        if not sortedPostingExist:
            # map posting
            oosqlSystem.Tool_MapPosting(volumeID, className, attrName,
                                        postingFileNameList,
                                        "TEXT_%s_%s_Posting_Mapped" % (className, attrName),
                                        "TEXT_%s_OID" % (className),
                                        UNSORTED_MERGE,
                                        pagerankFileName, pagerankMode)

            # rename mapped posting into posting
            srcName   = "%s%sTEXT_%s_%s_Posting_Mapped" % (env_ODYS_TEMP_PATH, dirSeparator, className, attrName)
            destName  = "%s%sTEXT_%s_%s_Posting"        % (env_ODYS_TEMP_PATH, dirSeparator, className, attrName)
            try:
                os.unlink(destName)
            except OSError:
                pass
            os.rename(srcName, destName)
        else:
            # map posting
            oosqlSystem.Tool_MapPosting(volumeID, className, attrName,
                                        postingFileNameList,
                                        "TEXT_%s_%s_SortedPosting_Mapped" % (className, attrName),
                                        "TEXT_%s_OID" % (className),
                                        SORTED_MERGE,
                                        pagerankFileName, pagerankMode)

            # rename mapped posting into posting
            srcName   = "%s%sTEXT_%s_%s_SortedPosting_Mapped" % (env_ODYS_TEMP_PATH, dirSeparator, className, attrName)
            destName  = "%s%sTEXT_%s_%s_SortedPosting"        % (env_ODYS_TEMP_PATH, dirSeparator, className, attrName)
            try:
                os.unlink(destName)
            except OSError:
                pass
            os.rename(srcName, destName)
    print "End Map Posting"
    os.system("date")
            
    # build text index
    config = PyOOSQL.lom_Text_ConfigForInvertedIndexBuild()
    if loadingMode == SMALL_APPEND_BULKLOADING:
        config.isUsingBulkLoading                    = 0
        config.isUsingKeywordIndexBulkLoading        = 0
        config.isUsingReverseKeywordIndexBulkLoading = 0
    elif loadingMode == MEDIUM_APPEND_BULKLOADING:
        config.isUsingBulkLoading                    = 1
        config.isUsingKeywordIndexBulkLoading        = 0
        config.isUsingReverseKeywordIndexBulkLoading = 0
    elif loadingMode == INITIAL_BULKLOADING or loadingMode == LARGE_APPEND_BULKLOADING: 
        config.isUsingBulkLoading                    = 1
        config.isUsingKeywordIndexBulkLoading        = 1
        config.isUsingReverseKeywordIndexBulkLoading = 1
    config.isBuildingExternalReverseKeywordFile  = 0
    config.isBuildingDocIdIndex                  = 1
    config.isSortingPostingFile                  = 1
    config.isUsingStoredPosting                  = 0
    
    print "Start Text Index Building"
    os.system("date")
    for attrName in attributes:
        print "-" * 79
        print "Build text index for class '%s', attribute '%s'" % (className, attrName)
        sortedPostingExist = CheckIfSortedPostingExist(dataFileName, className, attrName)
        if sortedPostingExist:
            config.isSortingPostingFile = 0
        else:
            config.isSortingPostingFile = 1
        oosqlSystem.Tool_BuildTextIndex(volumeID, temporaryVolumeID, className, attrName, config)
    print "End Text Index Building"
    os.system("date")

    # commit transaction
    print "-" * 79
    print "Transaction commit"
    print "Start Commit"
    os.system("date")
    oosqlSystem.TransCommit()
    print "End Commit"
    os.system("date")

    # dismount database
    print "-" * 79
    print "Dismount database"
    oosqlSystem.Dismount(temporaryVolumeID)
    oosqlSystem.DismountDB(databaseID)

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
        mergedPostingExist = CheckIfMergedPostingExist(dataFileName, className, attrName)
        sortedPostingExist = CheckIfSortedPostingExist(dataFileName, className, attrName)
        if not mergedPostingExist:
            postingFileNameList = GetPostingFileNameList(dataFileName, className, attrName, sortedPostingExist)
        else:
            if not sortedPostingExist:
                postingFileName = "%s_TEXT_%s_%s_Posting" % (dataFileName, className, attrName);
                postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            else:
                postingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, attrName);
                postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            postingFileNameList = [postingFileName]
        for postingFileName in postingFileNameList:
            destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName)
            try:
                os.unlink(destName)
            except OSError:
                pass

    # unlink oid file    
    oidFileName = "TEXT_%s_OID" % (className)
    destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, oidFileName)
    try:
        os.unlink(destName)
    except OSError:
        pass

LoaderMergeBuild(sys.argv)

