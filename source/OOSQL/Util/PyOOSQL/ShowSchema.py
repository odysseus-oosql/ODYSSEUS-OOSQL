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
import PyOOSQL
import sys

def TypeName(typeId):
    if typeId == PyOOSQL.OOSQL_TYPE_SMALLINT:
        return "SMALLINT"
    elif typeId == PyOOSQL.OOSQL_TYPE_INTEGER:
        return "INTEGER"
    elif typeId == PyOOSQL.OOSQL_TYPE_LONG:
        return "LONG"
    elif typeId == PyOOSQL.OOSQL_TYPE_FLOAT:
        return "FLOAT"
    elif typeId == PyOOSQL.OOSQL_TYPE_DOUBLE:
        return "DOUBLE"
    elif typeId == PyOOSQL.OOSQL_TYPE_CHAR:
        return "CHAR"
    elif typeId == PyOOSQL.OOSQL_TYPE_VARCHAR:
        return "VARCHAR"
    elif typeId == PyOOSQL.OOSQL_TYPE_OID:
        return "OID"
    elif typeId == PyOOSQL.OOSQL_TYPE_TEXT:
        return "TEXT"
    elif typeId == PyOOSQL.OOSQL_TYPE_DATE:
        return "DATE"
    elif typeId == PyOOSQL.OOSQL_TYPE_TIME:
        return "TIME"
    elif typeId == PyOOSQL.OOSQL_TYPE_TIMESTAMP:
        return "TIMESTAMP"
    else:
        return "UNKNOWN"

def ComplexTypeName(complexTypeId):
    if complexTypeId == PyOOSQL.OOSQL_COMPLEXTYPE_SET:
        return "SET"
    elif complexTypeId == PyOOSQL.OOSQL_COMPLEXTYPE_BAG:
        return "BAG"
    elif complexTypeId == PyOOSQL.OOSQL_COMPLEXTYPE_LIST:
        return "LIST"
    else:
        return "BASIC"

def ShowAllClasses(databaseName, volumeName):
    oosqlSystem = PyOOSQL.OOSQL_System()
    databaseID  = oosqlSystem.MountDB(databaseName)
    volumeID    = oosqlSystem.GetVolumeID(databaseID, volumeName)

    oosqlSystem.TransBegin(PyOOSQL.X_BROWSE_BROWSE)

    classNames = oosqlSystem.GetAllClassNames(volumeID)

    print "Total %d classes are defined" % len(classNames)
    i = 0
    for className in classNames:
        print "%4d %30s" % (i, className)
        i = i + 1
        
    oosqlSystem.TransCommit()
    oosqlSystem.DismountDB(databaseID)
    
def ShowClassSchema(databaseName, volumeName, className):
    oosqlSystem = PyOOSQL.OOSQL_System()
    databaseID  = oosqlSystem.MountDB(databaseName)
    volumeID    = oosqlSystem.GetVolumeID(databaseID, volumeName)

    oosqlSystem.TransBegin(PyOOSQL.X_BROWSE_BROWSE)

    description = oosqlSystem.GetTableDescription(volumeID, className)

    print "ClassName :", description[0]
    print "ClassId   :", description[1]
    print "Attribute Infomation :"
    for attrInfo in description[2]:
        print "%4d %35s  %-10s %s" % (attrInfo[1], attrInfo[0], TypeName(attrInfo[2]), ComplexTypeName(attrInfo[3]))
    print "Index Information :"
    i = 0
    for indexInfo in description[3]:
        print "%4s %35s  %-12s %10s" % (i, indexInfo[0], indexInfo[1], indexInfo[2])
        for attrInfo in indexInfo[4]:
            print "%40s  %s" % (description[2][attrInfo[0]][0], attrInfo[1])
        i = i + 1

    oosqlSystem.TransCommit()
    oosqlSystem.DismountDB(databaseID)

def Usage():
    print """
USAGE > ShowSchema.py <database name> <volume name> <class name>
        ShowSchema.py <database name> <volume name>
    """

if len(sys.argv) == 4:
    ShowClassSchema(sys.argv[1], sys.argv[2], sys.argv[3])
elif len(sys.argv) == 3:
    ShowAllClasses(sys.argv[1], sys.argv[2])
else:
    Usage()
