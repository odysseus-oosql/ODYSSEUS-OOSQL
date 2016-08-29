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

#ifndef _LOM_COLLECTION_H_
#define _LOM_COLLECTION_H_

#include "LOM_Internal.h"

#define LOM_CollectionSet_Create(handle, ornOrRelScanId, useScanFlag, oid, colNo, keySize) LRDS_CollectionSet_Create(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), keySize)
#define LOM_CollectionSet_Destroy(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionSet_Destroy(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionSet_GetN_Elements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements) LRDS_CollectionSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements)
#define LOM_CollectionSet_Assign(handle, ornOrScanId, useScanFlag, oid, colNo, assignedOrnOrScanId, assignedUseScanFlag, assignedOid, assignedColNo) LRDS_CollectionSet_Assign(LOM_GET_LRDS_HANDLE(handle), ornOrScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), assignedOrnOrScanId, assignedUseScanFlag, assignedOid, GET_SYSTEMLEVEL_COLNO(assignedColNo))
#define LOM_CollectionSet_AssignElements(handle, ornOrScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionSet_AssignElements(LOM_GET_LRDS_HANDLE(handle), ornOrScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionSet_InsertElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionSet_InsertElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionSet_DeleteElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionSet_DeleteElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionSet_DeleteAll(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionSet_DeleteAll(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionSet_IsMember(handle, ornOrRelScanId, useScanFlag, oid, colNo, elementSize, element) LRDS_CollectionSet_IsMember(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), elementSize, element)
#define LOM_CollectionSet_GetSizeOfElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementsSize) LRDS_CollectionSet_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementsSize)
#define LOM_CollectionSet_RetrieveElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementSizes, sizeOfElements, elements) LRDS_CollectionSet_RetrieveElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementSizes, sizeOfElements, elements)
#define LOM_CollectionSet_IsEqual(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionSet_IsEqual(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionSet_IsSubset(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionSet_IsSubset(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionSet_Union(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2, ornOrRelScanId3, useScanFlag3, oid3, colNo3) LRDS_CollectionSet_Union(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2), ornOrRelScanId3, useScanFlag3, oid3, GET_SYSTEMLEVEL_COLNO(colNo3))
#define LOM_CollectionSet_Intersect(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2, ornOrRelScanId3, useScanFlag3, oid3, colNo3) LRDS_CollectionSet_Intersect(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2), ornOrRelScanId3, useScanFlag3, oid3, GET_SYSTEMLEVEL_COLNO(colNo3))
#define LOM_CollectionSet_Difference(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2, ornOrRelScanId3, useScanFlag3, oid3, colNo3) LRDS_CollectionSet_Difference(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2), ornOrRelScanId3, useScanFlag3, oid3, GET_SYSTEMLEVEL_COLNO(colNo3))
#define LOM_CollectionSet_UnionWith(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionSet_UnionWith(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionSet_IntersectWith(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionSet_IntersectWith(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionSet_DifferenceWith(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionSet_DifferenceWith(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionSet_Scan_Open(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionSet_Scan_Open(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionSet_Scan_Close(handle, collectionScanId) LRDS_CollectionSet_Scan_Close(LOM_GET_LRDS_HANDLE(handle), collectionScanId)
#define LOM_CollectionSet_Scan_NextElements(handle, collectionScanId, nElements, elementSizes, sizeOfElements, elements) LRDS_CollectionSet_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes, sizeOfElements, elements)
#define LOM_CollectionSet_Scan_GetSizeOfNextElements(handle, collectionScanId, nElements, elementSizes) LRDS_CollectionSet_Scan_GetSizeOfNextElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes)
#define LOM_CollectionSet_Scan_InsertElements(handle, collectionScanId, nElements, elementSizes, elements) LRDS_CollectionSet_Scan_InsertElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes, elements)
#define LOM_CollectionSet_Scan_DeleteElements(handle, collectionScanId) LRDS_CollectionSet_Scan_DeleteElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId)
#define LOM_CollectionSet_IsNull(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionSet_IsNull(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))

#define LOM_CollectionBag_Create(handle, ornOrRelScanId, useScanFlag, oid, colNo, keySize) LRDS_CollectionBag_Create(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), keySize)
#define LOM_CollectionBag_Destroy(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionBag_Destroy(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionBag_GetN_Elements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements) LRDS_CollectionBag_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements)
#define LOM_CollectionBag_Assign(handle, ornOrScanId, useScanFlag, oid, colNo, assignedOrnOrScanId, assignedUseScanFlag, assignedOid, assignedColNo) LRDS_CollectionBag_Assign(LOM_GET_LRDS_HANDLE(handle), ornOrScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), assignedOrnOrScanId, assignedUseScanFlag, assignedOid, GET_SYSTEMLEVEL_COLNO(assignedColNo))
#define LOM_CollectionBag_AssignElements(handle, ornOrScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionBag_AssignElements(LOM_GET_LRDS_HANDLE(handle), ornOrScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionBag_InsertElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionBag_InsertElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionBag_DeleteElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionBag_DeleteElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionBag_DeleteAll(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionBag_DeleteAll(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionBag_IsMember(handle, ornOrRelScanId, useScanFlag, oid, colNo, elementSize, element) LRDS_CollectionBag_IsMember(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), elementSize, element)
#define LOM_CollectionBag_GetSizeOfElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementsSize) LRDS_CollectionBag_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementsSize)
#define LOM_CollectionBag_RetrieveElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementSizes, sizeOfElements, elements) LRDS_CollectionBag_RetrieveElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementSizes, sizeOfElements, elements)
#define LOM_CollectionBag_IsEqual(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionBag_IsEqual(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionBag_IsSubset(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionBag_IsSubset(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionBag_Union(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2, ornOrRelScanId3, useScanFlag3, oid3, colNo3) LRDS_CollectionBag_Union(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2), ornOrRelScanId3, useScanFlag3, oid3, GET_SYSTEMLEVEL_COLNO(colNo3))
#define LOM_CollectionBag_Intersect(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2, ornOrRelScanId3, useScanFlag3, oid3, colNo3) LRDS_CollectionBag_Intersect(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2), ornOrRelScanId3, useScanFlag3, oid3, GET_SYSTEMLEVEL_COLNO(colNo3))
#define LOM_CollectionBag_Difference(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2, ornOrRelScanId3, useScanFlag3, oid3, colNo3) LRDS_CollectionBag_Difference(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2), ornOrRelScanId3, useScanFlag3, oid3, GET_SYSTEMLEVEL_COLNO(colNo3))
#define LOM_CollectionBag_UnionWith(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionBag_UnionWith(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionBag_IntersectWith(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionBag_IntersectWith(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionBag_DifferenceWith(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionBag_DifferenceWith(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionBag_Scan_Open(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionBag_Scan_Open(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionBag_Scan_Close(handle, collectionScanId) LRDS_CollectionBag_Scan_Close(LOM_GET_LRDS_HANDLE(handle), collectionScanId)
#define LOM_CollectionBag_Scan_NextElements(handle, collectionScanId, nElements, elementSizes, sizeOfElements, elements) LRDS_CollectionBag_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes, sizeOfElements, elements)
#define LOM_CollectionBag_Scan_GetSizeOfNextElements(handle, collectionScanId, nElements, elementSizes) LRDS_CollectionBag_Scan_GetSizeOfNextElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes)
#define LOM_CollectionBag_Scan_InsertElements(handle, collectionScanId, nElements, elementSizes, elements) LRDS_CollectionBag_Scan_InsertElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes, elements)
#define LOM_CollectionBag_Scan_DeleteElements(handle, collectionScanId) LRDS_CollectionBag_Scan_DeleteElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId)
#define LOM_CollectionBag_IsNull(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionBag_IsNull(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))

#define LOM_CollectionList_Create(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionList_Create(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionList_Destroy(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionList_Destroy(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionList_GetN_Elements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements) LRDS_CollectionList_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements);
#define LOM_CollectionList_Assign(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionList_Assign(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionList_AssignElements(handle, ornOrScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionList_AssignElements(LOM_GET_LRDS_HANDLE(handle), ornOrScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionList_InsertElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementSizes, elements) LRDS_CollectionList_InsertElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementSizes, elements)
#define LOM_CollectionList_DeleteElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements) LRDS_CollectionList_DeleteElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements)
#define LOM_CollectionList_DeleteAll(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionList_DeleteAll(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionList_AppendElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, nElements, elementSizes, elements) LRDS_CollectionList_AppendElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), nElements, elementSizes, elements)
#define LOM_CollectionList_GetSizeOfElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementsSize) LRDS_CollectionList_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementsSize)
#define LOM_CollectionList_RetrieveElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementSizes, sizeOfElements, elements) LRDS_CollectionList_RetrieveElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementSizes, sizeOfElements, elements)
#define LOM_CollectionList_UpdateElements(handle, ornOrRelScanId, useScanFlag, oid, colNo, ith, nElements, elementSizes, elements) LRDS_CollectionList_UpdateElements(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ith, nElements, elementSizes, elements)
#define LOM_CollectionList_Concatenate(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionList_Concatenate(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionList_Resize(handle, ornOrRelScanId, useScanFlag, oid, colNo, size) LRDS_CollectionList_Resize(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), size)
#define LOM_CollectionList_IsMember(handle, ornOrRelScanId, useScanFlag, oid, colNo, size, element, index) LRDS_CollectionList_IsMember(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), size, element, index)
#define LOM_CollectionList_IsEqual(handle, ornOrRelScanId, useScanFlag, oid, colNo, ornOrRelScanId2, useScanFlag2, oid2, colNo2) LRDS_CollectionList_IsEqual(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo), ornOrRelScanId2, useScanFlag2, oid2, GET_SYSTEMLEVEL_COLNO(colNo2))
#define LOM_CollectionList_Scan_Open(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionList_Scan_Open(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_CollectionList_Scan_Close(handle, collectionScanId) LRDS_CollectionList_Scan_Close(LOM_GET_LRDS_HANDLE(handle), collectionScanId)
#define LOM_CollectionList_Scan_NextElements(handle, collectionScanId, nElements, elementSizes, sizeOfElements, elements) LRDS_CollectionList_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes, sizeOfElements, elements)
#define LOM_CollectionList_Scan_GetSizeOfNextElements(handle, collectionScanId, nElements, elementsSize) LRDS_CollectionList_Scan_GetSizeOfNextElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementsSize)
#define LOM_CollectionList_Scan_InsertElements(handle, collectionScanId, nElements, elementSizes, elements) LRDS_CollectionList_Scan_InsertElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId, nElements, elementSizes, elements)
#define LOM_CollectionList_Scan_DeleteElements(handle, collectionScanId) LRDS_CollectionList_Scan_DeleteElements(LOM_GET_LRDS_HANDLE(handle), collectionScanId)
#define LOM_CollectionList_IsNull(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_CollectionList_IsNull(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))

#endif /* _LOM_COLLECTION_H_ */
