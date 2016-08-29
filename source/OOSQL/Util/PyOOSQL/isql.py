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
import PyOOSQL

def Usage():
    print \
"""
USAGE : isql [-cclevel <cuncurrency level>] <database name> [<volume name>] [-temporary <database name> [<volume name>]]
        currency level : 0, 1, 2, 3, 4, 5
                         0 means X_BROWSE_BROWSE,
                         1 means X_CS_BROWSE,
                         2 means X_CS_CS,
                         3 means X_RR_BROWSE,
                         4 means X_RR_CS,
                         5 means X_RR_RR. Default value is 5 (X_RR_RR)
        temporary : indicating that isql uses separate volume to process
                    orderby, group by, distinct
"""

def CheckArgument(count, argv):
    if count >= len(argv):
        Usage()
        sys.exit(1)
        
def ParseArgument(argv):
    cclevel = 0
    databaseName = ""
    volumeName = ""
    
    count = 1
    CheckArgument(count, argv)
    if string.lower(argv[count]) == "-cclevel":
        count = count + 1
        cclevel = int(argv[count])
        count = count + 1
    else:
        cclevel = 5

    CheckArgument(count, argv)
    databaseName = argv[count]
    count = count + 1

    if (len(argv) > count + 1 and string.lower(argv[count + 1]) == "-temporary") or len(argv) - 1 == count:
        # next argument is -temporary or there are one more argument to parse.  
        # these argument consist of volumeName, datafilename
        volumeName = argv[count]
        count = count + 1
    else:
        volumeName = databaseName

    if count < len(argv) and string.lower(argv[count]) == "-temporary":
        count = count + 1
        temporaryDatabaseName = argv[count]
        count = count + 1
        if len(argv) > count:
            temporaryVolumeName = argv[count]
            count = count + 1
        else:
            temporaryVolumeName = temporaryDatabaseName
    else:
        temporaryDatabaseName = ""
        temporaryVolumeName = ""

    return databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, cclevel

def MakeDatabaseValueToString(oosqlSystem, data, type):
    if data == None:
        return "NULL"
    
    if type == PyOOSQL.OOSQL_TYPE_STRING or type == PyOOSQL.OOSQL_TYPE_VARSTRING or type == PyOOSQL.OOSQL_TYPE_TEXT:
        return data
    elif type == PyOOSQL.OOSQL_TYPE_DATE:
        return str(oosqlSystem.GetYear(data)) + "-" + str(oosqlSystem.GetMonth(data)) + "-" + str(oosqlSystem.GetDay(data))
    elif type == PyOOSQL.OOSQL_TYPE_TIME:
        return str(oosqlSystem.GetHour(data)) + ":" + str(oosqlSystem.GetMinute(data)) + ":" + str(oosqlSystem.GetSecond(data))
    elif type == PyOOSQL.OOSQL_TYPE_TIMESTAMP:
        return str(oosqlSystem.GetYear(data.d)) + "-" + str(oosqlSystem.GetMonth(data.d)) + "-" + str(oosqlSystem.GetDay(data.d)) + "-" +\
               str(oosqlSystem.GetHour(data.t)) + ":" + str(oosqlSystem.GetMinute(data.t)) + ":" + str(oosqlSystem.GetSecond(data.t))
    elif type == PyOOSQL.OOSQL_TYPE_SHORT or type == PyOOSQL.OOSQL_TYPE_INT or type == PyOOSQL.OOSQL_TYPE_LONG:
        return str(data)
    elif type == PyOOSQL.OOSQL_TYPE_FLOAT or type == PyOOSQL.OOSQL_TYPE_DOUBLE:
        return str(data)
    elif type == PyOOSQL.OOSQL_TYPE_OID:
        return oosqlSystem.OIDToOIDString(data)
    else:
        return "..."
    
def ExecSQL(oosqlSystem, volumeID, sortBufferInfo, queryString):
    oosqlSystem.ResetTimeElapsed()
    
    query = oosqlSystem.CreateQuery(volumeID)
    try:
        query.Prepare(queryString, sortBufferInfo)
    except PyOOSQL.OOSQL_ERROR, msg:
        print msg
        return
    
    numResultCols = query.GetNumResultCols()

    try:
        query.Execute()
    except PyOOSQL.OOSQL_ERROR, msg:
        print msg
        return

    if numResultCols == 0:
        return

    print "----------+" * numResultCols
    resultColNames = query.GetResultColNamesAsTuple()
    formatString   = "%10s|" * numResultCols
    print formatString % resultColNames
    print "----------+" * numResultCols

    formatString    = "%10s|" * numResultCols    
    while query.Next() != PyOOSQL.ENDOFEVAL:
        resultColValues = ()
        for i in range(numResultCols):
            resultString = MakeDatabaseValueToString(oosqlSystem, query.GetData(i), query.GetResultColType(i))
            resultColValues = resultColValues + (resultString,)
        print formatString % resultColValues
    print "----------+" * numResultCols

    print "Time elapsed " + str(oosqlSystem.GetTimeElapsed()) + " ms"

    query = None
    
def ExecISQL(oosqlSystem, volumeID, sortBufferInfo, cclevel):
    oosqlSystem.TransBegin(cclevel)
    while 1:
        queryString = ""
        input = raw_input("OOSQL> ")
        input = string.strip(input)
        
        if string.lower(input) == "quit":
            break
        elif string.lower(input) == "commit":
            oosqlSystem.TransCommit()
            oosqlSystem.TransBegin(cclevel)
        elif string.lower(input) == "abort":
            oosqlString.TransAbort()
            oosqlSystem.TransBegin(cclevel)
            
        while 1:
            if len(input) > 0:
                if input[-1] == ';':
                    queryString = queryString + ' ' + input[:-1]
                    break
                else:
                    queryString = queryString + ' ' + input

            input = raw_input("       ")
            input = string.strip(input)

        ExecSQL(oosqlSystem, volumeID, sortBufferInfo, queryString)
        
    oosqlSystem.TransCommit()
                
def ISQLmain(argv):
    (databaseName, volumeName, temporaryDatabaseName, temporaryVolumeName, cclevel) = ParseArgument(argv)

    oosqlSystem = PyOOSQL.OOSQL_System()
    databaseID  = oosqlSystem.MountDB(databaseName)
    volumeID    = oosqlSystem.GetVolumeID(databaseID, volumeName)
    if temporaryVolumeName != "":
        temporaryVolumeID = oosqlSystem.MountVolumeByVolumeName(temporaryDatabaseName, temporaryVolumeName)
    else:
        temporaryVolumeID = volumeID

    print oosqlSystem.GetVersionString()
    print
    
    sortBufferInfo = PyOOSQL.OOSQL_SortBufferInfo()
    sortBufferInfo.mode = PyOOSQL.OOSQL_SB_USE_DISK
    sortBufferInfo.diskInfo.sortVolID = temporaryVolumeID
        
    ExecISQL(oosqlSystem, volumeID, sortBufferInfo, cclevel)

    oosqlSystem.DismountDB(databaseID)
    if temporaryVolumeName != "":
        oosqlSystem.Dismount(temporaryVolumeID)

ISQLmain(sys.argv)
