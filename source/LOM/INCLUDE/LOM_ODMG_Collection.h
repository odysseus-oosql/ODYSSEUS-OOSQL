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

#ifndef _LOM_ODMG_COLLECTION_H_
#define _LOM_ODMG_COLLECTION_H_

#include "LOM_Internal.h"

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_CreateData(
	LOM_Handle*               handle,
    Four                      ocnOrScanId,
	Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionColStruct* collData,
    ODMG_CollectionDesc*      collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_DestroyData(
	LOM_Handle*               handle,
    Four                      ocnOrScanId,
	Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionDesc*      collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_GetDescriptor(
	LOM_Handle*               handle,
    Four                      ocnOrScanId,
	Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionDesc*      collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_SetDescriptor(
	LOM_Handle*               handle,
    Four                      ocnOrScanId,
	Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionDesc*      collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_Assign(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc,
    Four                        assignedOcnOrScanId,
	Boolean                     assignedUseScanFlag,
    OID*                        assignedOid,
    Two                         assignedColNo,
    ODMG_CollectionDesc*        assignedCollDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_AssignElements(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_InsertElements(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_DeleteElements(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_DeleteAll(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_IsMember(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_IsEqual(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc,
	Four                        comparedOcnOrScanId,
	Boolean                     comparedUseScanFlag,
    OID*                        comparedOid,
    Two                         comparedColNo,
    ODMG_CollectionDesc*        comparedCollDesc);

/* collectionKind : Set, Bag */
Four LOM_ODMG_Collection_IsSubset(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc,
    Four                        comparedOcnOrScanId,
	Boolean                     comparedUseScanFlag,
    OID*                        comparedOid,
    Two                         comparedColNo,
    ODMG_CollectionDesc*        comparedCollDesc);

/* collectionKind : Set, Bag */
Four LOM_ODMG_Collection_Union(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanIdA,
	Boolean                     useScanFlagA,
    OID*                        oidA,
    Two                         colNoA,
    ODMG_CollectionDesc*        collDescA,
    Four                        ocnOrScanIdB,
	Boolean                     useScanFlagB,
    OID*                        oidB,
    Two                         colNoB,
    ODMG_CollectionDesc*        collDescB,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag */
Four LOM_ODMG_Collection_Intersect(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanIdA,
	Boolean                     useScanFlagA,
    OID*                        oidA,
    Two                         colNoA,
    ODMG_CollectionDesc*        collDescA,
    Four                        ocnOrScanIdB,
	Boolean                     useScanFlagB,
    OID*                        oidB,
    Two                         colNoB,
    ODMG_CollectionDesc*        collDescB,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag */
Four LOM_ODMG_Collection_Difference(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanIdA,
	Boolean                     useScanFlagA,
    OID*                        oidA,
    Two                         colNoA,
    ODMG_CollectionDesc*        collDescA,
    Four                        ocnOrScanIdB,
	Boolean                     useScanFlagB,
    OID*                        oidB,
    Two                         colNoB,
    ODMG_CollectionDesc*        collDescB,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag */
Four LOM_ODMG_Collection_UnionWith(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanIdA,
	Boolean                     useScanFlagA,
    OID*                        oidA,
    Two                         colNoA,
    ODMG_CollectionDesc*        collDescA,
    Four                        ocnOrScanIdB,
	Boolean                     useScanFlagB,
    OID*                        oidB,
    Two                         colNoB,
    ODMG_CollectionDesc*        collDescB);

/* collectionKind : Set, Bag */
Four LOM_ODMG_Collection_IntersectWith(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanIdA,
	Boolean                     useScanFlagA,
    OID*                        oidA,
    Two                         colNoA,
    ODMG_CollectionDesc*        collDescA,
    Four                        ocnOrScanIdB,
	Boolean                     useScanFlagB,
    OID*                        oidB,
    Two                         colNoB,
    ODMG_CollectionDesc*        collDescB);

/* collectionKind : Set, Bag */
Four LOM_ODMG_Collection_DifferenceWith(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanIdA,
	Boolean                     useScanFlagA,
    OID*                        oidA,
    Two                         colNoA,
    ODMG_CollectionDesc*        collDescA,
    Four                        ocnOrScanIdB,
	Boolean                     useScanFlagB,
    OID*                        oidB,
    Two                         colNoB,
    ODMG_CollectionDesc*        collDescB);

/* collectionKind : List, Array */
Four LOM_ODMG_Collection_AppendElements(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_RetrieveElements(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : List, Array */
Four LOM_ODMG_Collection_UpdateElements(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : List, Array */
Four LOM_ODMG_Collection_Concatenate(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanIdA,
	Boolean                     useScanFlagA,
    OID*                        oidA,
    Two                         colNoA,
    ODMG_CollectionDesc*        collDescA,
    Four                        ocnOrScanIdB,
	Boolean                     useScanFlagB,
    OID*                        oidB,
    Two                         colNoB,
    ODMG_CollectionDesc*        collDescB);

/* collectionKind : List, Array */
Four LOM_ODMG_Collection_Resize(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    Four			            size,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_Scan_Open(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
	Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_Scan_Close(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        collectionScanId);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_Scan_NextElements(
	LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        collectionScanId,
    ODMG_CollectionColStruct*   collData);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_Scan_GetSizeOfNextElements(
	LOM_Handle*                 handle,
	Four						collectionKind,
	Four						collectionScanId,
	Four						nElements,
	Four*						elementsLen);

/* collectionKind : Set, Bag, List, Array */
Four LOM_ODMG_Collection_GetSizeOfElements(
	LOM_Handle*                 handle,
	Four						collectionKind,
	Four                        ocnOrScanId,
	Boolean                     useScanFlag,
	OID*						oid,
	Four						colNo,
	Four						ith,
	Four						nElements,
	Four*						elementsLen);

#endif /* _LOM_ODMG_COLLECTION_H_ */

