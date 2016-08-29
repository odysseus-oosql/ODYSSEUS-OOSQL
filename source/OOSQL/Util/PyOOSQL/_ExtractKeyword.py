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

def Usage():
    print "USAGE : This program must be called by only OOSQL_LoaderParallelExtract.py" 

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
        
    CheckArgument(count, argv)
    dataFileName = argv[count]
    count = count + 1

    CheckArgument(count, argv)
    startObjectNo = string.atoi(argv[count])
    count = count + 1

    CheckArgument(count, argv)
    endObjectNo = string.atoi(argv[count])
    count = count + 1

    CheckArgument(count, argv)
    processNo = string.atoi(argv[count])
    count = count + 1
    
    return databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileName, startObjectNo, endObjectNo, processNo

def LoaderExtract(argv):
    (databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, dataFileName, startObjectNo, endObjectNo, processNo) = ParseArgument(argv)

    # use external sort utility
    useExternalSortUtility = 1
    
    # mount database
    print "-" * 79
    print "Mount database '%s'" % (databaseName)
    oosqlSystem = PyOOSQL.OOSQL_System()
    databaseID  = oosqlSystem.MountDB(databaseName)
    volumeID    = oosqlSystem.GetVolumeID(databaseID, volumeName)
    temporaryVolumeID = oosqlSystem.MountVolumeByVolumeName(temporaryDatabaseName, temporaryVolumeName)
    
    # begin transaction
    print "-" * 79
    print "Transaction begin"
    oosqlSystem.TransBegin(PyOOSQL.X_BROWSE_BROWSE)

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
   
    # extract keyword
    nAttributes = len(attributes)
    for i in range(nAttributes):
        attrName = attributes[i]
        if i < nAttributes - 1:
            nextAttrName = attributes[i + 1]
            nextPostingFileName = "%s_TEXT_%s_%s_Posting_%d" % (dataFileName, className, nextAttrName, processNo)
            nextPostingFileName = string.join(string.split(nextPostingFileName, dirSeparator), '_')
            nextSortedPostingFileName = "%s_TEXT_%s_%s_SortedPosting_%d" % (dataFileName, className, nextAttrName, processNo)
            nextSortedPostingFileName = string.join(string.split(nextSortedPostingFileName, dirSeparator), '_')
            nextMergedPostingFileName = "%s_TEXT_%s_%s_Posting" % (dataFileName, className, nextAttrName)
            nextMergedPostingFileName = string.join(string.split(nextMergedPostingFileName, dirSeparator), '_')
            nextMergedSortedPostingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, nextAttrName)
            nextMergedSortedPostingFileName = string.join(string.split(nextMergedSortedPostingFileName, dirSeparator), '_')
            if os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, nextPostingFileName), 0) or \
               os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, nextSortedPostingFileName), 0) or \
               os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, nextMergedPostingFileName), 0) or \
               os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, nextMergedSortedPostingFileName), 0):
                print "Skip extracting keyword from class '%s', attribute '%s'" % (className, attrName)
                continue
            
        postingFileName = "%s_TEXT_%s_%s_Posting_%d" % (dataFileName, className, attrName, processNo)
        postingFileName = string.join(string.split(postingFileName, dirSeparator), '_')
        sortedPostingFileName = "%s_TEXT_%s_%s_SortedPosting_%d" % (dataFileName, className, attrName, processNo)
        sortedPostingFileName = string.join(string.split(sortedPostingFileName, dirSeparator), '_')
        mergedSortedPostingFileName = "%s_TEXT_%s_%s_SortedPosting" % (dataFileName, className, attrName)
        mergedSortedPostingFileName = string.join(string.split(mergedSortedPostingFileName, dirSeparator), '_')
        if os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, sortedPostingFileName), 0) or \
           os.access("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, mergedSortedPostingFileName), 0):
            print "Skip extracting keyword from class '%s', attribute '%s'" % (className, attrName)
        else:
            print "Start Keyword Extraction"
            os.system("date")
            oosqlSystem.Tool_ExtractKeyword(volumeID, className, attrName, dataFileName, postingFileName, 
                                            startObjectNo,      # from start
                                            endObjectNo,	# to end
                                            1)      		# alwaysUsePreviousPostingFile = false
            print "End Keyword Extraction"
            os.system("date")
            print "Start External Sorting"
            os.system("date")
            if useExternalSortUtility:
                print "Do external sorting for %s" % (postingFileName)
                oosqlSystem.Tool_SortPosting(postingFileName, sortedPostingFileName)
                os.unlink("%s%s%s" % (env_ODYS_TEMP_PATH, dirSeparator, postingFileName))
            print "End External Sorting"
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

    # exit by exit() system call
    sys.exit(0)

LoaderExtract(sys.argv)

