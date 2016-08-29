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
import re
import PyOOSQL

# catalog table name pattern
LRDSCatalogPattern = "lrdsSys[a-zA-Z]+"
LOMCatalogPattern  = "(lomSys[a-zA-Z]+)|(lom[a-zA-Z]+)|(LOM\_SYS\_[a-zA-Z]+)"
TextCatalogPattern = "([a-zA-Z\_]+Inverted)|([a-zA-Z\_]+docId)"

# sub-index name pattern
SubIndexPattern = "([a-zA-Z\_]+LogicalIdIndex)|([a-zA-Z\_]+Inverted)"

def Usage():
    print "USAGE : OOSQL_GetTableInfo.py <database name> <volume name> [<table name>,...]"

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

    tableNameList = []
    for i in range(count, len(argv)):
        tableNameList.append(argv[i])
    
    return databaseName, volumeName, tableNameList

def RemoveSystemCatalogTable(allTableNameList):
    userTableNameList = [] 
    for tableName in allTableNameList:
        # ignore LRDS system catalogs
        if re.search(LRDSCatalogPattern, tableName[0]):
            continue
        # ignore LOM system catalogs
        if re.search(LOMCatalogPattern, tableName[0]):
            continue
        # ignore Inverted Index catalog and document ID catalog
        if re.search(TextCatalogPattern, tableName[0]):
            continue
        userTableNameList.append(tableName[0])

    return userTableNameList

def DisplayAttributeInfo(attributeInfos):
    # display column header
    print "-" * 79
    print "%-5s%-20s%-15s%-15s" % ("#", "name", "type", "complex type")
    print "-" * 79

    for attributeInfo in attributeInfos:
        # get the type of attribute
        if attributeInfo[2] == PyOOSQL.OOSQL_TYPE_SMALLINT:
            attributeType = "smallint"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_INTEGER:
            attributeType = "integer"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_LONG:
            attributeType = "long"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_FLOAT:
            attributeType = "float"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_DOUBLE:
            attributeType = "double"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_CHAR:
            attributeType = "char"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_VARCHAR:
            attributeType = "varchar"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_OID:
            attributeType = "OID"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_TEXT:
            attributeType = "text"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_DATE:
            attributeType = "date"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_TIME:
            attributeType = "time"
        elif attributeInfo[2] == PyOOSQL.OOSQL_TYPE_TIMESTAMP:
            attributeType = "timestamp"

        # get the complex type of attribute
        if attributeInfo[3] == PyOOSQL.OOSQL_COMPLEXTYPE_BASIC:
            attributeComplexType = "basic"
        if attributeInfo[3] == PyOOSQL.OOSQL_COMPLEXTYPE_SET:
            attributeComplexType = "set"
        if attributeInfo[3] == PyOOSQL.OOSQL_COMPLEXTYPE_BAG:
            attributeComplexType = "bag"
        if attributeInfo[3] == PyOOSQL.OOSQL_COMPLEXTYPE_LIST:
            attributeComplexType = "list"

        # display each column information
        print "%-5d%-20s%-15s%-15s" % (attributeInfo[1], attributeInfo[0], attributeType, attributeComplexType)

    print "-" * 79

def DisplayIndexInfo(indexInfos, attributeInfos):
    if len(indexInfos) == 0:
        print "*** No B-Tree index is created. ***"
    else:
        for indexInfo in indexInfos:
            if re.search(SubIndexPattern, indexInfo[0]):
                continue
            else:
                print " " * 5 + "'%s' is created on ()." % (indexInfo[0])

def GetTableInfo(argv):
    (databaseName, volumeName, tableNameList) = ParseArgument(argv)

    # mount database
    oosqlSystem = PyOOSQL.OOSQL_System()
    databaseID  = oosqlSystem.MountDB(databaseName)
    volumeID    = oosqlSystem.GetVolumeID(databaseID, volumeName)

    # begin transaction
    oosqlSystem.TransBegin(PyOOSQL.X_BROWSE_BROWSE)

    # get all tables in database
    if len(tableNameList) == 0:
        query = oosqlSystem.CreateQuery(volumeID)
        query.ExecDirect("select className from lomSysClasses")
        results = query.FetchAll()
        # remove system catalog table from list
        tableNameList = RemoveSystemCatalogTable(results)
        query = None
        # display the number of tables
        print "=" * 79
        print "SUMMARY:" 
        print "The Number of Tables in Database '%s', Volume '%s': %d" % (databaseName, volumeName, len(tableNameList))
        print "=" * 79
        print

    # display the description of tables
    for tableName in tableNameList:
        (tableName, tableId, attributeInfos, indexInfos) = oosqlSystem.GetTableDescription(volumeID, tableName)
        print
        print "=" * 79
        print "I. Table Name: %s" % (tableName)
        print "=" * 79
        print "II. Attribute Information"
        DisplayAttributeInfo(attributeInfos)
        print "=" * 79
        print "III. B-Tree Index Information"
        DisplayIndexInfo(indexInfos, attributeInfos)
        print "=" * 79
        print

    # commit transaction
    oosqlSystem.TransCommit()

    # dismount database
    oosqlSystem.DismountDB(databaseID)

GetTableInfo(sys.argv)
