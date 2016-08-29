/******************************************************************************/
/*                                                                            */
/*    Copyright (c) 1990-2016, KAIST                                          */
/*    All rights reserved.                                                    */
/*                                                                            */
/*    Redistribution and use in source and binary forms, with or without      */
/*    modification, are permitted provided that the following conditions      */
/*    are met:                                                                */
/*                                                                            */
/*    1. Redistributions of source code must retain the above copyright       */
/*       notice, this list of conditions and the following disclaimer.        */
/*                                                                            */
/*    2. Redistributions in binary form must reproduce the above copyright    */
/*       notice, this list of conditions and the following disclaimer in      */
/*       the documentation and/or other materials provided with the           */
/*       distribution.                                                        */
/*                                                                            */
/*    3. Neither the name of the copyright holder nor the names of its        */
/*       contributors may be used to endorse or promote products derived      */
/*       from this software without specific prior written permission.        */
/*                                                                            */
/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
/*    POSSIBILITY OF SUCH DAMAGE.                                             */
/*                                                                            */
/******************************************************************************/
/******************************************************************************/
/*                                                                            */
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
/*                                                                            */
/*    Developed by Professor Kyu-Young Whang et al.                           */
/*                                                                            */
/*    Advanced Information Technology Research Center (AITrc)                 */
/*    Korea Advanced Institute of Science and Technology (KAIST)              */
/*                                                                            */
/*    e-mail: odysseus.oosql@gmail.com                                        */
/*                                                                            */
/*    Bibliography:                                                           */
/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
/*        Demonstration Award.                                                */
/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
/*        Storage Structure Using Subindexes and Large Objects for Tight      */
/*        Coupling of Information Retrieval with Database Management          */
/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
/*        (1999)).                                                            */
/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
/*        J., "Tightly-Coupled Spatial Database Features in the               */
/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
/*                                                                            */
/******************************************************************************/

#ifndef _DBINTF_H
#define _DBINTF_H

#include "DBIntf_Internal.h"

/* Class Information Functions */
Four DB_GetClassID(const char *name, Four *classID);
Four DB_GetClassInfo(Four classID, db_Class **classInfo);
Four DB_GetClassName(db_Class *classInfo, char *className);

Four DB_GetAttribute(db_Class *classInfo, const char *attrName, db_Attribute **attributeInfo);
Four DB_GetAttributes(db_Class *classInfo, db_Attribute **attributeInfo);
Four DB_GetMethod(db_Class *classInfo, const char *methodName,db_Method **methodInfo);
Four DB_GetMethods(db_Class *classInfo, db_Method **methodInfo);
/* not yet implemented */
Four DB_GetSuperClasses(db_Class *classInfo, db_List **classInfoList);

/* Attribute Functions */
Four DB_AttributeInhertedFromClass(db_Attribute *attributeInfo, db_Class **classInfo);
Four DB_AttributeDomain(db_Attribute *attributeInfo, db_Domain *domain);
Boolean DB_AttributeHasIndex(db_Attribute *attributeInfo);
Four DB_AttributeLength(db_Attribute *attributeInfo, Four *length);
Four DB_AttributeName(db_Attribute *attributeInfo, char *attrName);
Four DB_AttributeNext(db_Attribute *attributeInfo1, db_Attribute **attributeInfo2);
Four DB_AttributeNumber(db_Attribute *attributeInfo, Four *attrNum);
Four DB_AttributeType(db_Attribute *attributeInfo, Four *type);

/* Method Functions */
Four DB_MethodArgCount(db_Method *methodInfo, Two *argCount);
Four DB_MethodArgDomain(db_Method *methodInfo, db_Domain *domain);
Four DB_MethodInheritedFromClass(db_Method *methodInfo, db_Class **classInfo);
Four DB_MethodFunctionName(db_Method *methodInfo, char *functionName);
Four DB_MethodName(db_Method *methodInfo, char *methodName);
Four DB_MethodNext(db_Method *methodInfo1, db_Method **methodInfo2);

/* Object Predicate Functions */
Boolean DB_IsSuperClass(db_Class *classInfo, db_Class *superClassInfo);
Boolean DB_IsSubClass(db_Class *classInfo, db_Class *SubClassInfo);

/* Error function */
char *DB_Err(Four);

/* object manipulation */
Four DB_CreateObject( db_Class *class,  userDescriptor *usrDesc);
Four DB_DestroyObject(userDescriptor *usrDesc);

Four DB_Start(char *databaseName);
Four DB_End();

Four DB_InitFetch( db_Class *class, db_Cursor *db_cursor, userDescriptor *usrDesc);
Four DB_FetchNext( db_Cursor *db_cursor, userDescriptor *usrDesc);
Four DB_FinalFetch( db_Cursor *db_cursor);

/* user descriptor manipulation */
userDescriptor *DB_GetUserDescriptor();
Four DB_FreeUserDescriptor( userDescriptor *usrDesc);

/* attribute manipulation */
Four DB_GetAttr( userDescriptor *usrDesc, const char *pathExp, db_Value *attrValue);
Four DB_PutAttr( userDescriptor *usrDesc, const char *attrName, db_Value *attrValue);

/* transaction */
Four DB_TransAbort();
Four DB_TransBegin();
Four DB_TransCommit();

/* method dispatching */
Four DB_Send( userDescriptor *od, char *methodName, db_Value *returnVal, ...);


#endif _DBINTF_H
