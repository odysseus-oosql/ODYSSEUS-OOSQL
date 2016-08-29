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

#include "OBfM_Internal.h"

/* Error function */
char *OBfM_Err(Four);

/* Object Manager Initialization/Finalization */
Four OBfM_Finalize();
Four OBfM_Initialize();

/* Object Manipulation Part */
Four OBfM_CreateObject(Four classId, objectDescriptor **objDesc);
Four OBfM_DestroyObject(objectDescriptor *objDesc);
Four OBfM_UpdateObject(objectDescriptor *objDesc);
Four OBfM_FetchObject(OID *oid, objectDescriptor **objDesc);
Four OBfM_AllocObject(Four classId , objectDescriptor **objDesc);
Four OBfM_FreeObject(objectDescriptor *objDesc);

/* Attribute Manipulation Part */
Four OBfM_GetAttr(objectDescriptor *objDesc, const char *pathExp, obfm_Value *attrVal);
Four OBfM_PutAttr(objectDescriptor *objDesc, const char *attrName, obfm_Value *attrVal);

/* OID Table Manipulation */
Four obfm_OIDTblDelete(OID *oid);
Four obfm_OIDTblInitialize();
Four obfm_OIDTblFinalize();
Four obfm_OIDTblInsert(OID *oid, objectDescriptor *objDesc);
Four obfm_OIDTblLookUp(OID *oid , objectDescriptor **objDesc);

/* Allocation / DeAllocation of object and value */
Four OBfM_ObjectAlloc(objectDescriptor **objDesc, Four size);
Four OBfM_ObjectDealloc(objectDescriptor *objDesc);
Four OBfM_ValueAlloc(void **p, Four size);
Four OBfM_ValueDealloc(char *p);

/* Allocation / DeAllocation of user descriptor */
Four OBfM_UserDescriptorAlloc(userDescriptor **ud);
Four OBfM_UserDescriptorDealloc(userDescriptor *ud);

/* user Descriptor Memory Management */
Four obfm_userDescriptorPoolInitialize();
Four obfm_userDescriptorPoolFinalize();
Four obfm_userDescriptorGC();
Boolean obfm_IsValidUserDescriptorAddr(char *);

/* Object Buffer Management */
Four OBfM_ObjectPoolGC();
Four OBfM_ObjectPoolCompact();
Four OBfM_ObjectPoolRM();
Four OBfM_FlushObjects();
Four obfm_ObjectPoolInitialize();
Four obfm_ObjectPoolFinalize();
Four obfm_GCMark(objectDescriptor *objDesc);
Boolean obfm_IsValidObjectAddr(char *);

/* method dispatching function */
Four OBfM_Send( objectDescriptor *od, char *methodName, 
				obfm_Value *returnVal, ...);

/* misc functions */
Boolean OBfM_IsNull( objectDescriptor *objDesc, const char *atrrName);

#define obfm_GetClassId lom_GetClassId


