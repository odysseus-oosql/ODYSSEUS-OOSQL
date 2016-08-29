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
import PyOOSQL

def Usage():
    print "USAGE : OOSQL_LoaderMergeBuildPhase1.py"
    print "        [-pagerank <pagerank file name>] <database name> <volume name> [-temporary <database name> <volume name>] <data file name>"

def CheckArgument(count, argv):
    if count >= len(argv):
        Usage()
        sys.exit(1)

def ShowLoadingHistory(databaseName, volumeName):
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"

    print "-" * 79
    print "Previous Loading History"
    print "-" * 79

    tempFileName = "%s%s%s_%s_history" % (env_ODYS_TEMP_PATH, dirSeparator, databaseName, volumeName)
    if (os.access(tempFileName, 0)):
        f = open(tempFileName, "r")
        line = f.readline()
        while line:
           print line[:-1]
           line = f.readline()
        f.close()
    else:
        print "No data file has been loaded."

    print "-" * 79

def WriteLoadingHistory(databaseName, volumeName, dataFileName):
    env_ODYS_TEMP_PATH = os.environ["ODYS_TEMP_PATH"]
    if sys.platform == "win32":
        dirSeparator = "\\"
    else:
        dirSeparator = "/"

    now = time.localtime(time.time())
    timeString = "%4d-%2d-%2d %02d:%02d:%02d" % (now[0], now[1], now[2], now[3], now[4], now[5])

    tempFileName = "%s%s%s_%s_history" % (env_ODYS_TEMP_PATH, dirSeparator, databaseName, volumeName)
    f = open(tempFileName, "a")
    f.write("Data file '%s' was loaded at %s\n" % (dataFileName, timeString))
    f.close()
        
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
    
def LoaderMergeBuild(argv):
    (databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileName, pagerankFileName, pagerankMode) = ParseArgument(argv)

    # show previous loading history
    ShowLoadingHistory(databaseName, volumeName)

    line = raw_input("Are you sure ([y]es / [n]o) ? ")
    if string.lower(line[0]) == 'y':
        print "-" * 79
        print "Now, Start Loading..."
    elif string.lower(line[0]) == 'n':
        print "-" * 79
        print "Restart this command after confirmation."
        sys.exit(1)

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
   
    # loaddb
    print "-" * 79
    print "Load data from '%s'" % (dataFileName)
    isDeferredTextIndexMode = 1
    useBulkloading          = 1
    useDescriptorUpdating   = 1
    smallUpdateFlag         = 0		

    print "Start Loading"
    os.system("date")

    oosqlSystem.Tool_LoadDB(volumeID, temporaryVolumeID, isDeferredTextIndexMode, smallUpdateFlag, useBulkloading, 
                            useDescriptorUpdating, dataFileName, pagerankFileName, pagerankMode)

    print "End Loading"
    os.system("date")

    print "Start Mapping"
    os.system("date")
    # mapping
    for attrName in attributes:
        print "-" * 79
        print "Map posting for class '%s', attribute '%s'" % (className, attrName)
        sortedPostingExist = CheckIfSortedPostingExist(dataFileName, className, attrName)

        if not sortedPostingExist:
            postingFileName = "%s_TEXT_%s_%s_Posting" % (dataFileName, className, attrName)
            postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            newPostingFileName = "%s_TEXT_%s_%s_Posting_Mapped" % (dataFileName, className, attrName)
            newPostingFileName = string.join(string.split(newPostingFileName, dirSeparator), '_')

            oosqlSystem.Tool_MapPosting(volumeID, className, attrName,
                                        [postingFileName],
                                        newPostingFileName,
                                        "TEXT_%s_OID" % (className),
                                        0,
                                        pagerankFileName, pagerankMode)

            # rename mapped posting into posting
            srcName   = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, newPostingFileName)
            destName  = srcName[:-7]		# remove trailing "_Mapped"
            try:
                os.unlink(destName)
            except OSError:
                pass
            os.rename(srcName, destName)
        else:
            postingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, attrName)
            postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
            newPostingFileName = "%s_TEXT_%s_%s_SortedPosting_Mapped" % (dataFileName, className, attrName)
            newPostingFileName = string.join(string.split(newPostingFileName, dirSeparator), '_')            

            oosqlSystem.Tool_MapPosting(volumeID, className, attrName,
                                        [postingFileName],
                                        newPostingFileName,
                                        "TEXT_%s_OID" % (className),
                                        0,
                                        pagerankFileName, pagerankMode)

            # rename mapped posting into posting
            srcName   = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, newPostingFileName)
            destName  = srcName[:-7]		# remove trailing "_Mapped"
            try:
                os.unlink(destName)
            except OSError:
                pass
            os.rename(srcName, destName)
    print "End Mapping"
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

    # unlink oid file    
    oidFileName = "TEXT_%s_OID" % (className)
    destName = "%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, oidFileName)
    try:
        os.unlink(destName)
    except OSError:
        pass

    # log file information which has been successfully loaded
    WriteLoadingHistory(databaseName, volumeName, dataFileName)

    return

LoaderMergeBuild(sys.argv)

