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

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"
#include <string.h>

#define ODMG_COLLECTION_COL_NO      0
#define ODMG_COLLECTION_REF_SIZE    36
#define ODMG_COLLECTION_REF_KEYSIZE 16

static Four lom_ODMG_Collection_EncodeElements(Four type, Four length, Four nElements, void* elements);
static Four lom_ODMG_Collection_DecodeElements(Four type, Four length, Four nElements, void* elements);

Four LOM_ODMG_Collection_CreateData(
    LOM_Handle*               handle,
    Four                      ocnOrScanId,
    Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionColStruct* collData,
    ODMG_CollectionDesc*      collDesc)
{
    Four    ocn;
    Four    dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four    e;
    Four    attrInfo;
    Four    mv;
    Four    i;

    /* check parameter */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        ocn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        /* get data file's orn and scan id */
        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* initialize descritpion */
    collDesc->nElements = 0;
    
    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* create data tuple */
    e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, ODMG_COLLECTION_COL_NO, NULL, &collDesc->dataTid);
    if(e < 0) LOM_ERROR(handle, e);

    /* initialize data tuple and insert initial data */
    /* For LOM_REF and LOM_LINK type, encode it's OID part data */
    e = lom_ODMG_Collection_EncodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    switch(CATALOG_GET_ATTRCOMPLEXTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]))
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        if(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]) == LOM_REF ||
           CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]) == LOM_LINK)
            e = LRDS_CollectionSet_Create(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, ODMG_COLLECTION_REF_KEYSIZE);
        else
            e = LRDS_CollectionSet_Create(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, ALL_VALUE);
        if(e < 0) LOM_ERROR(handle, e);

        if(collData->nElements > 0)
        {
            collData->ith = 0;
            e = LOM_ODMG_Collection_InsertElements(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONSET,
                                                   ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collData, collDesc);
            if(e < 0) LOM_ERROR(handle, e);
        }
        break;

    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        if(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]) == LOM_REF ||
           CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]) == LOM_LINK)
            e = LRDS_CollectionBag_Create(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, ODMG_COLLECTION_REF_KEYSIZE);
        else
            e = LRDS_CollectionBag_Create(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, ALL_VALUE);
        if(e < 0) LOM_ERROR(handle, e);

        if(collData->nElements > 0)
        {
            collData->ith = 0;
            e = LOM_ODMG_Collection_InsertElements(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG,
                                                   ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collData, collDesc);
            if(e < 0) LOM_ERROR(handle, e);
        }
        break;

    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
        e = LRDS_CollectionList_Create(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < 0) LOM_ERROR(handle, e);

        if(collData->nElements > 0)
        {
            collData->ith = 0;
            e = LOM_ODMG_Collection_InsertElements(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST,
                                               ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collData, collDesc);
            if(e < 0) LOM_ERROR(handle, e);
        }
        break;

    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Create(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < 0) LOM_ERROR(handle, e);

        if(collData->nElements > 0)
        {
            collData->ith = 0;
            e = LOM_ODMG_Collection_InsertElements(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY,
                                                   ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collData, collDesc);
            if(e < 0) LOM_ERROR(handle, e);
        }
        break;

    default:
        LOM_ERROR(handle, eINTERNAL_LOM);
    }

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_DestroyData(
    LOM_Handle*               handle,
    Four                      ocnOrScanId,
    Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionDesc*      collDesc)
{
    Four ocn;
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four e;
    Four attrInfo;
    Four mv;
    Four i;

    /* check parameter */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        ocn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        /* get data file's orn and scan id */
        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* initialize descritpion */
    collDesc->nElements = 0;
    
    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

 
    /* destroy collection */
    switch(CATALOG_GET_ATTRCOMPLEXTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]))
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Destroy(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < 0) LOM_ERROR(handle, e);
        break;

    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Destroy(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < 0) LOM_ERROR(handle, e);
        break;

    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
        e = LRDS_CollectionList_Destroy(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < 0) LOM_ERROR(handle, e);
        break;

    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Destroy(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < 0) LOM_ERROR(handle, e);
        break;

    default:
        LOM_ERROR(handle, eINTERNAL_LOM);
    }

    /* destroy data tuple */
    e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid);
    if(e < 0) LOM_ERROR(handle, e);

    collDesc->nElements = 0;   
    SET_NILTUPLEID(collDesc->dataTid);
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_GetDescriptor(
    LOM_Handle*               handle,
    Four                      ocnOrScanId,
    Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionDesc*      collDesc)
{
    Four          e;
    LOM_ColListStruct clist[1];

    clist[0].colNo = colNo;
    clist[0].start = ALL_VALUE;
    clist[0].data.ptr = collDesc;
    clist[0].length = sizeof(ODMG_CollectionDesc);
    clist[0].dataLength = sizeof(ODMG_CollectionDesc);

    e = LOM_FetchObjectByColList(handle, ocnOrScanId, useScanFlag, oid, 1, clist);
    if(e < 0) LOM_ERROR(handle, e);

    if (clist[0].nullFlag == SM_TRUE) 
        return eNULLCOLLECTION_LOM;

    return eNOERROR;
}

Four LOM_ODMG_Collection_SetDescriptor(
    LOM_Handle*               handle,
    Four                      ocnOrScanId,
    Boolean                   useScanFlag,
    OID*                      oid,
    Two                       colNo,
    ODMG_CollectionDesc*      collDesc)
{
    Four          e;
    LOM_ColListStruct clist[1];

    clist[0].colNo = colNo;
    clist[0].start = ALL_VALUE;
    clist[0].data.ptr = collDesc;
    clist[0].length = sizeof(ODMG_CollectionDesc);
    clist[0].dataLength = sizeof(ODMG_CollectionDesc);
    clist[0].nullFlag = SM_FALSE;

    e = LOM_UpdateObjectByColList(handle, ocnOrScanId, useScanFlag, oid, 1, clist);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_AssignElements(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four attrInfo;
    Four mv;

    if(IS_NILTUPLEID(collDesc->dataTid))
        return LOM_ODMG_Collection_CreateData(handle, ocnOrScanId, useScanFlag, oid, colNo, collData, collDesc);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* For LOM_REF and LOM_LINK type, encode it's OID part data */
    e = lom_ODMG_Collection_EncodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_AssignElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                              collData->nElements, collData->elementSizes,
                                              collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_AssignElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                              collData->nElements, collData->elementSizes,
                                              collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_AssignElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                               collData->nElements, collData->elementSizes,
                                               collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    /* update collection desctiption */
    collDesc->nElements = collData->nElements;
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

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
    ODMG_CollectionDesc*        assignedCollDesc)
{
    Four i;                     /* loop counter */
    Four e;                     /* error code */
    Four orn;                   /* open relation number */
    Four assignedOrn;           /* open relation number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four assignedDataOrnOrScanId;
    Boolean assignedDataUseScanFlag;

    if(IS_NILTUPLEID(assignedCollDesc->dataTid))
    {
        ODMG_CollectionColStruct collData;

        collData.ith             = 0;
        collData.nElements       = 0;
        collData.elementSizes    = NULL;
        collData.sizeOfElements  = 0;
        collData.elements        = NULL;

        e = LOM_ODMG_Collection_CreateData(handle, assignedOcnOrScanId, assignedUseScanFlag, assignedOid, assignedColNo, &collData, assignedCollDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }
   
    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get scan id for assigned collection data */
    assignedColNo = GET_SYSTEMLEVEL_COLNO(assignedColNo);
    if(assignedUseScanFlag)
    {
        assignedOrn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[assignedOcnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[assignedOrn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[assignedOrn].odmgCollColNo[i] == assignedColNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[assignedOrn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        assignedDataOrnOrScanId = LOM_SCANTABLE(handle)[assignedOcnOrScanId].odmgCollDataScanId[i];
        assignedDataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[assignedOcnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[assignedOcnOrScanId].odmgCollColNo[i] == assignedColNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[assignedOcnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        assignedDataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[assignedOcnOrScanId].odmgCollDataOrn[i];
        assignedDataUseScanFlag = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Assign(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                      assignedDataOrnOrScanId, assignedDataUseScanFlag, &assignedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Assign(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                      assignedDataOrnOrScanId, assignedDataUseScanFlag, &assignedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Assign(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                      assignedDataOrnOrScanId, assignedDataUseScanFlag, &assignedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* update collection desctiption */
    assignedCollDesc->nElements = collDesc->nElements;
    e = LOM_ODMG_Collection_SetDescriptor(handle, assignedOcnOrScanId, assignedUseScanFlag, assignedOid, (Two)(assignedColNo - 1), assignedCollDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);
    
    return eNOERROR;
}

Four LOM_ODMG_Collection_InsertElements(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four mv;
    Four attrInfo;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* do actual call */
    /* For LOM_REF and LOM_LINK type, encode it's OID part data */
    e = lom_ODMG_Collection_EncodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_InsertElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                              collData->nElements, collData->elementSizes,
                                              collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_InsertElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                              collData->nElements, collData->elementSizes,
                                              collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_InsertElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                               collData->ith, collData->nElements, collData->elementSizes,
                                               collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    /* update collection desctiption */
    collDesc->nElements += collData->nElements;
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_DeleteElements(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four mv;
    Four attrInfo;
    
    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }
    
    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    if(collectionKind == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET || 
       collectionKind == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG)
    {
        /* For LOM_REF and LOM_LINK type, encode it's OID part data */
        e = lom_ODMG_Collection_EncodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                               CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                               collData->nElements, collData->elements);
        if(e < 0) LOM_ERROR(handle, e);
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_DeleteElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                              collData->nElements, collData->elementSizes,
                                              collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_DeleteElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                              collData->nElements, collData->elementSizes,
                                              collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_DeleteElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                               collData->ith, collData->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    if(collectionKind == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET || 
       collectionKind == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG)
    {
        /* decode */
        e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                               CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                               collData->nElements, collData->elements);
        if(e < 0) LOM_ERROR(handle, e);
    }

    /* update collection desctiption */
    collDesc->nElements -= collData->nElements;
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_DeleteAll(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_DeleteAll(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_DeleteAll(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_DeleteAll(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }
    
    /* update collection desctiption */
    collDesc->nElements = 0;
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_IsMember(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four mv;
    Four attrInfo;
    Four result;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* when the set is empty, then return false */
    if(collDesc->nElements == 0)
        return SM_FALSE;

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* For LOM_REF and LOM_LINK type, encode it's OID part data */
    e = lom_ODMG_Collection_EncodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_IsMember(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                        collData->elementSizes[0], collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_IsMember(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                        collData->elementSizes[0], collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_IsMember(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                         collData->elementSizes[0], collData->elements, &collData->ith);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    result = e;

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    return result;
}

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
    ODMG_CollectionDesc*        comparedCollDesc)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four orn;                   /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four comparedOrn;           /* open class number */
    Four comparedDataOrnOrScanId;
    Boolean comparedDataUseScanFlag;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(comparedCollDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* when two set's size is not equal, then return SM_FALSE */
    if(collDesc->nElements != comparedCollDesc->nElements)
        return SM_FALSE;

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get scan id for collection compared data */
    comparedColNo = GET_SYSTEMLEVEL_COLNO(comparedColNo);
    if(comparedUseScanFlag)
    {
        comparedOrn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[comparedOcnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[comparedOrn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[comparedOrn].odmgCollColNo[i] == comparedColNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[comparedOrn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        comparedDataOrnOrScanId = LOM_SCANTABLE(handle)[comparedOcnOrScanId].odmgCollDataScanId[i];
        comparedDataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].odmgCollColNo[i] == comparedColNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        comparedDataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].odmgCollDataOrn[i];
        comparedDataUseScanFlag = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_IsEqual(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                       comparedDataOrnOrScanId, comparedDataUseScanFlag, &comparedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_IsEqual(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                       comparedDataOrnOrScanId, comparedDataUseScanFlag, &comparedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_IsEqual(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                        comparedDataOrnOrScanId, comparedDataUseScanFlag, &comparedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    return e;
}

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
    ODMG_CollectionDesc*        comparedCollDesc)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four orn;                   /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four comparedOrn;           /* open class number */
    Four comparedDataOrnOrScanId;
    Boolean comparedDataUseScanFlag;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    
    if(IS_NILTUPLEID(comparedCollDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* when compared set's size is more than the other set, then return SM_FALSE */
    if(collDesc->nElements < comparedCollDesc->nElements)
        return SM_FALSE;

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get scan id for collection compared data */
    comparedColNo = GET_SYSTEMLEVEL_COLNO(comparedColNo);
    if(comparedUseScanFlag)
    {
        comparedOrn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[comparedOcnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[comparedOrn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[comparedOrn].odmgCollColNo[i] == comparedColNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[comparedOrn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        comparedDataOrnOrScanId = LOM_SCANTABLE(handle)[comparedOcnOrScanId].odmgCollDataScanId[i];
        comparedDataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].odmgCollColNo[i] == comparedColNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        comparedDataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[comparedOcnOrScanId].odmgCollDataOrn[i];
        comparedDataUseScanFlag = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_IsSubset(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                        comparedDataOrnOrScanId, comparedDataUseScanFlag, &comparedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_IsSubset(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                        comparedDataOrnOrScanId, comparedDataUseScanFlag, &comparedCollDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    return e;
}

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
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four orn;                   /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four ornA;                  /* open class number */
    Four dataOrnOrScanIdA;
    Boolean dataUseScanFlagA;
    Four ornB;                  /* open class number */
    Four dataOrnOrScanIdB;
    Boolean dataUseScanFlagB;

    if(IS_NILTUPLEID(collDescA->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDescB->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDesc->dataTid))
    {
        ODMG_CollectionColStruct collData;

        collData.ith             = 0;
        collData.nElements       = 0;
        collData.elementSizes    = NULL;
        collData.sizeOfElements  = 0;
        collData.elements        = NULL;

        e = LOM_ODMG_Collection_CreateData(handle, ocnOrScanId, useScanFlag, oid, colNo, &collData, collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }

    /* get scan id for collection data A */
    colNoA = GET_SYSTEMLEVEL_COLNO(colNoA);
    if(useScanFlagA)
    {
        ornA = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdA].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_SCANTABLE(handle)[ocnOrScanIdA].odmgCollDataScanId[i];
        dataUseScanFlagA = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollDataOrn[i];
        dataUseScanFlagA = SM_FALSE;
    }

    /* get scan id for collection data B */
    colNoB = GET_SYSTEMLEVEL_COLNO(colNoB);
    if(useScanFlagB)
    {
        ornB = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdB].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_SCANTABLE(handle)[ocnOrScanIdB].odmgCollDataScanId[i];
        dataUseScanFlagB = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollDataOrn[i];
        dataUseScanFlagB = SM_FALSE;
    }

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }


    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Union(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                     dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO,
                                     dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, &collDesc->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Union(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                     dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO,
                                     dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionBag_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, &collDesc->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);


    return eNOERROR;
}

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
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four orn;                   /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four ornA;                  /* open class number */
    Four dataOrnOrScanIdA;
    Boolean dataUseScanFlagA;
    Four ornB;                  /* open class number */
    Four dataOrnOrScanIdB;
    Boolean dataUseScanFlagB;

    if(IS_NILTUPLEID(collDescA->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    
    if(IS_NILTUPLEID(collDescB->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    
    if(IS_NILTUPLEID(collDesc->dataTid))
    {
        ODMG_CollectionColStruct collData;

        collData.ith             = 0;
        collData.nElements       = 0;
        collData.elementSizes    = NULL;
        collData.sizeOfElements  = 0;
        collData.elements        = NULL;

        e = LOM_ODMG_Collection_CreateData(handle, ocnOrScanId, useScanFlag, oid, colNo, &collData, collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }

    /* get scan id for collection data A */
    colNoA = GET_SYSTEMLEVEL_COLNO(colNoA);
    if(useScanFlagA)
    {
        ornA = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdA].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_SCANTABLE(handle)[ocnOrScanIdA].odmgCollDataScanId[i];
        dataUseScanFlagA = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollDataOrn[i];
        dataUseScanFlagA = SM_FALSE;
    }

    /* get scan id for collection data B */
    colNoB = GET_SYSTEMLEVEL_COLNO(colNoB);
    if(useScanFlagB)
    {
        ornB = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdB].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_SCANTABLE(handle)[ocnOrScanIdB].odmgCollDataScanId[i];
        dataUseScanFlagB = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollDataOrn[i];
        dataUseScanFlagB = SM_FALSE;
    }

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Intersect(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                         dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO,
                                         dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, &collDesc->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Intersect(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                         dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO,
                                         dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionBag_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, &collDesc->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

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
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four orn;                   /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four ornA;                  /* open class number */
    Four dataOrnOrScanIdA;
    Boolean dataUseScanFlagA;
    Four ornB;                  /* open class number */
    Four dataOrnOrScanIdB;
    Boolean dataUseScanFlagB;

    if(IS_NILTUPLEID(collDescA->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDescB->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDesc->dataTid))
    {
        ODMG_CollectionColStruct collData;

        collData.ith             = 0;
        collData.nElements       = 0;
        collData.elementSizes    = NULL;
        collData.sizeOfElements  = 0;
        collData.elements        = NULL;

        e = LOM_ODMG_Collection_CreateData(handle, ocnOrScanId, useScanFlag, oid, colNo, &collData, collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }

    /* get scan id for collection data A */
    colNoA = GET_SYSTEMLEVEL_COLNO(colNoA);
    if(useScanFlagA)
    {
        ornA = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdA].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_SCANTABLE(handle)[ocnOrScanIdA].odmgCollDataScanId[i];
        dataUseScanFlagA = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollDataOrn[i];
        dataUseScanFlagA = SM_FALSE;
    }

    /* get scan id for collection data B */
    colNoB = GET_SYSTEMLEVEL_COLNO(colNoB);
    if(useScanFlagB)
    {
        ornB = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdB].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_SCANTABLE(handle)[ocnOrScanIdB].odmgCollDataScanId[i];
        dataUseScanFlagB = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollDataOrn[i];
        dataUseScanFlagB = SM_FALSE;
    }

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }


    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Difference(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                          dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO,
                                          dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, &collDesc->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Difference(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                          dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO,
                                          dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionBag_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, &collDesc->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);


    return eNOERROR;
}

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
    ODMG_CollectionDesc*        collDescB)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four ornA;                  /* open class number */
    Four dataOrnOrScanIdA;
    Boolean dataUseScanFlagA;
    Four ornB;                  /* open class number */
    Four dataOrnOrScanIdB;
    Boolean dataUseScanFlagB;
    
    if(IS_NILTUPLEID(collDescA->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDescB->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data A */
    colNoA = GET_SYSTEMLEVEL_COLNO(colNoA);
    if(useScanFlagA)
    {
        ornA = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdA].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_SCANTABLE(handle)[ocnOrScanIdA].odmgCollDataScanId[i];
        dataUseScanFlagA = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollDataOrn[i];
        dataUseScanFlagA = SM_FALSE;
    }

    /* get scan id for collection data B */
    colNoB = GET_SYSTEMLEVEL_COLNO(colNoB);
    if(useScanFlagB)
    {
        ornB = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdB].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_SCANTABLE(handle)[ocnOrScanIdB].odmgCollDataScanId[i];
        dataUseScanFlagB = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollDataOrn[i];
        dataUseScanFlagB = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_UnionWith(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                         dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO, &collDescA->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_UnionWith(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                         dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionBag_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO, &collDescA->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanIdA, useScanFlagA, oidA, (Two)(colNoA - 1), collDescA);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

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
    ODMG_CollectionDesc*        collDescB)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four ornA;                  /* open class number */
    Four dataOrnOrScanIdA;
    Boolean dataUseScanFlagA;
    Four ornB;                  /* open class number */
    Four dataOrnOrScanIdB;
    Boolean dataUseScanFlagB;

    if(IS_NILTUPLEID(collDescA->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDescB->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data A */
    colNoA = GET_SYSTEMLEVEL_COLNO(colNoA);
    if(useScanFlagA)
    {
        ornA = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdA].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_SCANTABLE(handle)[ocnOrScanIdA].odmgCollDataScanId[i];
        dataUseScanFlagA = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollDataOrn[i];
        dataUseScanFlagA = SM_FALSE;
    }

    /* get scan id for collection data B */
    colNoB = GET_SYSTEMLEVEL_COLNO(colNoB);
    if(useScanFlagB)
    {
        ornB = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdB].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_SCANTABLE(handle)[ocnOrScanIdB].odmgCollDataScanId[i];
        dataUseScanFlagB = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollDataOrn[i];
        dataUseScanFlagB = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_IntersectWith(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                             dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO, &collDescA->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_IntersectWith(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                             dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionBag_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO, &collDescA->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanIdA, useScanFlagA, oidA, (Two)(colNoA - 1), collDescA);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

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
    ODMG_CollectionDesc*        collDescB)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four ornA;                  /* open class number */
    Four dataOrnOrScanIdA;
    Boolean dataUseScanFlagA;
    Four ornB;                  /* open class number */
    Four dataOrnOrScanIdB;
    Boolean dataUseScanFlagB;

    if(IS_NILTUPLEID(collDescA->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDescB->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data A */
    colNoA = GET_SYSTEMLEVEL_COLNO(colNoA);
    if(useScanFlagA)
    {
        ornA = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdA].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_SCANTABLE(handle)[ocnOrScanIdA].odmgCollDataScanId[i];
        dataUseScanFlagA = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollDataOrn[i];
        dataUseScanFlagA = SM_FALSE;
    }

    /* get scan id for collection data B */
    colNoB = GET_SYSTEMLEVEL_COLNO(colNoB);
    if(useScanFlagB)
    {
        ornB = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdB].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_SCANTABLE(handle)[ocnOrScanIdB].odmgCollDataScanId[i];
        dataUseScanFlagB = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollDataOrn[i];
        dataUseScanFlagB = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_DifferenceWith(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                              dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO, &collDescA->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_DifferenceWith(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                              dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* update collection desctiption */
        e = LRDS_CollectionBag_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO, &collDescA->nElements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanIdA, useScanFlagA, oidA, (Two)(colNoA - 1), collDescA);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_AppendElements(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four mv;
    Four attrInfo;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }


    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* For LOM_REF and LOM_LINK type, encode it's OID part data */
    e = lom_ODMG_Collection_EncodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_AppendElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                               collData->nElements, collData->elementSizes,
                                               collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    /* update collection desctiption */
    collDesc->nElements += collData->nElements;
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_RetrieveElements(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four nElements;
    Four mv;
    Four attrInfo;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* do actual call */
    nElements = collData->nElements;
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                                  collData->ith, collData->nElements, collData->elementSizes);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        e = LRDS_CollectionList_RetrieveElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                                 collData->ith, nElements, collData->elementSizes,
                                                 collData->sizeOfElements, collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        nElements = e;

        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                                collData->ith, collData->nElements, collData->elementSizes);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        e = LRDS_CollectionSet_RetrieveElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                                collData->ith, nElements, collData->elementSizes,
                                                collData->sizeOfElements, collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        nElements = e;
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                                 collData->ith, collData->nElements, collData->elementSizes);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        e = LRDS_CollectionBag_RetrieveElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                                collData->ith, nElements, collData->elementSizes,
                                                collData->sizeOfElements, collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        nElements = e;
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    collData->retN_Elements = nElements;

    return eNOERROR;
}

Four LOM_ODMG_Collection_UpdateElements(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionColStruct*   collData,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four mv;
    Four attrInfo;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* get catalog information of the column */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* do actual call */
    /* For LOM_REF and LOM_LINK type, encode it's OID part data */
    e = lom_ODMG_Collection_EncodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_UpdateElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO,
                                               collData->ith, collData->nElements, collData->elementSizes,
                                               collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]),
                                           collData->nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

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
    ODMG_CollectionDesc*        collDescB)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four ornA;                  /* open class number */
    Four dataOrnOrScanIdA;
    Boolean dataUseScanFlagA;
    Four ornB;                  /* open class number */
    Four dataOrnOrScanIdB;
    Boolean dataUseScanFlagB;

    if(IS_NILTUPLEID(collDescA->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(IS_NILTUPLEID(collDescB->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data A */
    colNoA = GET_SYSTEMLEVEL_COLNO(colNoA);
    if(useScanFlagA)
    {
        ornA = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdA].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_SCANTABLE(handle)[ocnOrScanIdA].odmgCollDataScanId[i];
        dataUseScanFlagA = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollColNo[i] == colNoA)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdA = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdA].odmgCollDataOrn[i];
        dataUseScanFlagA = SM_FALSE;
    }

    /* get scan id for collection data B */
    colNoB = GET_SYSTEMLEVEL_COLNO(colNoB);
    if(useScanFlagB)
    {
        ornB = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanIdB].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ornB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ornB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_SCANTABLE(handle)[ocnOrScanIdB].odmgCollDataScanId[i];
        dataUseScanFlagB = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollColNo[i] == colNoB)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanIdB = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanIdB].odmgCollDataOrn[i];
        dataUseScanFlagB = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Concatenate(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanIdA, dataUseScanFlagA, &collDescA->dataTid, ODMG_COLLECTION_COL_NO,
                                            dataOrnOrScanIdB, dataUseScanFlagB, &collDescB->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    collDescA->nElements += collDescB->nElements;
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanIdA, useScanFlagA, oidA, (Two)(colNoA - 1), collDescA);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_Resize(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    Four                        size,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                 /* loop index */
    Four e;                 /* error code */
    Four orn;               /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Resize(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO, size);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* update collection desctiption */
    collDesc->nElements = size;
    e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)(colNo - 1), collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_Scan_Open(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Two                         colNo,
    ODMG_CollectionDesc*        collDesc)
{
    Four i;                     /* loop index */
    Four e;                     /* error code */
    Four orn;                   /* open class number */
    Four dataOrnOrScanId;
    Boolean dataUseScanFlag;
    Four odmgCollectionScanId;
    Four attrInfo;
    Four mv;
    Four collectionScanId;

    if(IS_NILTUPLEID(collDesc->dataTid))
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }

    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Scan_Open(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Scan_Open(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Scan_Open(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc->dataTid, ODMG_COLLECTION_COL_NO);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }
    collectionScanId = e;

    /* get type information */
    e = Catalog_GetMountTableInfo(handle, oid->volNo, &mv);
    if(e < 0) CATALOG_ERROR(handle, e);
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, colNo, &attrInfo);
    if(e < 0) CATALOG_ERROR(handle, e);

    /* get new scan id */
	odmgCollectionScanId = e = lom_ODMGcollectionScanTableAllocEntry(handle);
	if (e < 0) LOM_ERROR(handle, e);

    /* fill LOM ODMG Collection Scan Table */
    LOM_ODMG_COLLECTIONSCANTABLE(handle)[odmgCollectionScanId].lrdsCollectionScanId = collectionScanId;
    LOM_ODMG_COLLECTIONSCANTABLE(handle)[odmgCollectionScanId].complexType          = collectionKind;
    LOM_ODMG_COLLECTIONSCANTABLE(handle)[odmgCollectionScanId].type                 = CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]);
    LOM_ODMG_COLLECTIONSCANTABLE(handle)[odmgCollectionScanId].length               = CATALOG_GET_ATTRLENGTH(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]);

    return e;
}

Four LOM_ODMG_Collection_Scan_Close(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        collectionScanId)
{
    Four e;

    /* check parameters */
    if (!LOM_ODMG_VALID_COLLECTIONSCANID(handle, collectionScanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].complexType != collectionKind) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* do actual call */
    switch(LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].complexType)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Scan_Close(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Scan_Close(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Scan_Close(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* Close the given set scan. */
	e = lom_ODMGcollectionScanTableFreeEntry(handle, collectionScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_ODMG_Collection_Scan_NextElements(
    LOM_Handle*                      handle,    
    Four                             collectionKind,
    Four                             collectionScanId,
    ODMG_CollectionColStruct*        collData)
{
    Four e;
    Four nElements;

    /* check parameters */
    if (!LOM_ODMG_VALID_COLLECTIONSCANID(handle, collectionScanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].complexType != collectionKind) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* do actual call */
    switch(LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].complexType)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId,
                                                 collData->nElements, 
                                                 collData->elementSizes,
                                                 collData->sizeOfElements,
                                                 collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        nElements = e;
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId,
                                                 collData->nElements, 
                                                 collData->elementSizes,
                                                 collData->sizeOfElements,
                                                 collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        nElements = e;
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId,
                                                  collData->nElements, 
                                                  collData->elementSizes,
                                                  collData->sizeOfElements,
                                                  collData->elements);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        nElements = e;
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* decode */
    e = lom_ODMG_Collection_DecodeElements(LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].type,
                                           LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].length,
                                           nElements, collData->elements);
    if(e < 0) LOM_ERROR(handle, e);


    collData->retN_Elements = nElements;

    return eNOERROR;
}

Four LOM_ODMG_Collection_Scan_GetSizeOfNextElements(
    LOM_Handle*     handle,
    Four            collectionKind,
    Four            collectionScanId,
    Four            nElements,
    Four*           elementsLen)
{
    Four e;

    /* check parameters */
    if (!LOM_ODMG_VALID_COLLECTIONSCANID(handle, collectionScanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].complexType != collectionKind) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* do actual call */
    switch(LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].complexType)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_Scan_GetSizeOfNextElements(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId, nElements, elementsLen);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_Scan_GetSizeOfNextElements(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId, nElements, elementsLen);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_Scan_GetSizeOfNextElements(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE(handle)[collectionScanId].lrdsCollectionScanId, nElements, elementsLen);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }
    return eNOERROR;
}

Four LOM_ODMG_Collection_GetSizeOfElements(
    LOM_Handle*                 handle,
    Four                        collectionKind,
    Four                        ocnOrScanId,
    Boolean                     useScanFlag,
    OID*                        oid,
    Four                        colNo,
    Four                        ith,
    Four                        nElements,
    Four*                       elementsLen)
{
    Four                    e;
    ODMG_CollectionDesc     collDesc;
    Four                    orn;
    Four                    dataOrnOrScanId;
    Boolean                 dataUseScanFlag;
    Four                    i;

    /* get desctiptor */
    e = LOM_ODMG_Collection_GetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)colNo, &collDesc);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* get scan id for collection data */
    colNo = GET_SYSTEMLEVEL_COLNO(colNo);
    if(useScanFlag)
    {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].odmgCollDataScanId[i];
        dataUseScanFlag = SM_TRUE;
    }
    else
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs; i++)
            if(LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollColNo[i] == colNo)
                break;
        if(i == LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].numOfodmgCollAttrs)
            LOM_ERROR(handle, eBADPARAMETER_LOM);

        dataOrnOrScanId = LOM_USEROPENCLASSTABLE(handle)[ocnOrScanId].odmgCollDataOrn[i];
        dataUseScanFlag = SM_FALSE;
    }
    
    /* do actual call */
    switch(collectionKind)
    {
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LRDS_CollectionSet_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc.dataTid, ODMG_COLLECTION_COL_NO,
                                                 ith, nElements, elementsLen);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LRDS_CollectionBag_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc.dataTid, ODMG_COLLECTION_COL_NO,
                                                 ith, nElements, elementsLen);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LRDS_CollectionList_GetSizeOfElements(LOM_GET_LRDS_HANDLE(handle), dataOrnOrScanId, dataUseScanFlag, &collDesc.dataTid, ODMG_COLLECTION_COL_NO,
                                                  ith, nElements, elementsLen);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    return eNOERROR;
}

Four lom_ODMG_Collection_CreateCollectionTable(
    LOM_Handle*                 handle,
    Four                        volId
)
{
    Four e;
    Four i, j;

    char* prefix[] = {LOM_ODMG_COLLECTION_SET_PREFIX,
                      LOM_ODMG_COLLECTION_BAG_PREFIX,
                      LOM_ODMG_COLLECTION_LIST_PREFIX,
                      LOM_ODMG_COLLECTION_VARARRAY_PREFIX};

    Four prefixtype[] = {SM_COMPLEXTYPE_COLLECTIONSET,
                         SM_COMPLEXTYPE_COLLECTIONBAG,
                         SM_COMPLEXTYPE_COLLECTIONLIST,
                         SM_COMPLEXTYPE_COLLECTIONLIST};

    char* postfix[] = {LOM_ODMG_COLLECTION_SHORT_POSTFIX,
                       LOM_ODMG_COLLECTION_INT_POSTFIX,
                       LOM_ODMG_COLLECTION_LONG_POSTFIX,
                       LOM_ODMG_COLLECTION_LONG_LONG_POSTFIX,
                       LOM_ODMG_COLLECTION_FLOAT_POSTFIX,
                       LOM_ODMG_COLLECTION_DOUBLE_POSTFIX,
                       LOM_ODMG_COLLECTION_STRING_POSTFIX,
                       LOM_ODMG_COLLECTION_VARSTRING_POSTFIX,
                       LOM_ODMG_COLLECTION_PAGEID_POSTFIX,
                       LOM_ODMG_COLLECTION_FILEID_POSTFIX,
                       LOM_ODMG_COLLECTION_INDEXID_POSTFIX,
                       LOM_ODMG_COLLECTION_OID_POSTFIX,
                       LOM_ODMG_COLLECTION_MBR_POSTFIX,
                       LOM_ODMG_COLLECTION_REF_POSTFIX,
                       LOM_ODMG_COLLECTION_LINK_POSTFIX,
                       LOM_ODMG_COLLECTION_TEXT_POSTFIX,
                       LOM_ODMG_COLLECTION_DATE_POSTFIX,
                       LOM_ODMG_COLLECTION_TIME_POSTFIX,
                       LOM_ODMG_COLLECTION_TIMESTAMP_POSTFIX,
                       LOM_ODMG_COLLECTION_INTERVAL_POSTFIX};
    
    Four postfixtype[] = {LOM_SHORT,
                          LOM_INT,
                          LOM_LONG,
                          LOM_LONG_LONG,
                          LOM_FLOAT,
                          LOM_DOUBLE,
                          LOM_VARSTRING,        /* Map String to VarString */
                          LOM_VARSTRING,
                          LOM_PAGEID,
                          LOM_FILEID,
                          LOM_INDEXID,
                          LOM_OID,
                          LOM_MBR,
                          LOM_STRING,           /* LOM_REF */
                          LOM_STRING,           /* LOM_LINK */
                          LOM_STRING,           /* LOM_TEXT */
                          LOM_LONG,             /* LOM_DATE */
                          LOM_STRING,           /* LOM_TIME */
                          LOM_STRING,           /* LOM_TIMESTAMP */
                          LOM_DOUBLE};          /* LOM_INTERVAL */

    Four postfixsize[] = {LOM_SHORT_SIZE,
                          LOM_INT_SIZE,
                          LOM_LONG_SIZE,
                          LOM_LONG_LONG_SIZE,
                          LOM_FLOAT_SIZE,
                          LOM_DOUBLE_SIZE,
                          LONG_MAX,               /* Maximum size of Varstring */
                          LONG_MAX,               /* Maximum size of Varstring */
                          LOM_PAGEID_SIZE,
                          LOM_FILEID_SIZE,
                          LOM_INDEXID,
                          LOM_OID_SIZE,
                          LOM_MBR_SIZE,
                          ODMG_COLLECTION_REF_SIZE, /* LOM_REF size  */
                          ODMG_COLLECTION_REF_SIZE, /* LOM_LINK size */
                          sizeof(LOM_TextDesc),
                          LOM_DATE_SIZE,
                          LOM_TIME_SIZE,
                          LOM_TIMESTAMP_SIZE,
                          LOM_INTERVAL_SIZE};

    char relationName[LOM_MAXCLASSNAME];
    ColInfo colInfo[1];

    for(i = 0; i < sizeof(prefixtype) / sizeof(prefixtype[0]); i++)
    {
        for(j = 0; j < sizeof(postfixtype) / sizeof(postfixtype[0]); j++)
        {
            colInfo[0].complexType = (Two)prefixtype[i];
            colInfo[0].type        = (Two)postfixtype[j];
            colInfo[0].length      = postfixsize[j];

            strcpy(relationName, prefix[i]);
            strcat(relationName, postfix[j]);

            e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, relationName, NULL, 1, colInfo, SM_FALSE);
            if(e < eNOERROR) LOM_ERROR(handle, e);
        }
    }

    return eNOERROR;    
}

Four lom_ODMG_Collection_EncodeElements(
    Four        type,
    Four        length,
    Four        nElements,
    void*       elements)
{
    OID     oid;
    char*   p;
    Four    i;

    return eNOERROR;

    if(type == LOM_REF || type == LOM_LINK)
    {
        p = elements;
        for(i = 0; i < nElements; i++)
        {
            memcpy(&oid, p, sizeof(OID));
            
            /* structure of OID 
             *  (pageNo, volNo, slotNo, unique, classID)
                   4       2      2       4       4
             */
            /* encoded structure
             *  (volNo, pageNo, slotNo, unique, classID)
                   2      4       2       4        4
             */
            memcpy(p, &oid.volNo, sizeof(Two));
			p += sizeof(Two);
            memcpy(p, &oid.pageNo, sizeof(Four));
			p += sizeof(Four);
            memcpy(p, &oid.slotNo, sizeof(Two));
			p += sizeof(Two);
            memcpy(p, &oid.unique, sizeof(Four));
			p += sizeof(Four);
            memcpy(p, &oid.classID, sizeof(Four));

            p += ODMG_COLLECTION_REF_SIZE;
        }
    }

    return eNOERROR;
}

Four lom_ODMG_Collection_DecodeElements(
    Four    type,
    Four    length,
    Four    nElements,
    void*   elements)
{
    OID     oid;
    char*   p;
    Four    i;

    return eNOERROR;

    if(type == LOM_REF || type == LOM_LINK)
    {
        p = elements;
        for(i = 0; i < nElements; i++)
        {
            /* structure of OID 
             *  (pageNo, volNo, slotNo, unique, classID)
                   4        2     2       4       4
             */
            /* encoded structure
             *  (volNo, pageNo, slotNo, unique, classID)
                   2      4       2       4        4
             */
            memcpy(&oid.volNo, p,  sizeof(Two));
			p += sizeof(Two);
            memcpy(&oid.pageNo, p, sizeof(Four));
			p += sizeof(Four);
            memcpy(&oid.slotNo, p, sizeof(Two));
			p += sizeof(Two);
            memcpy(&oid.unique, p, sizeof(Four));
			p += sizeof(Four);
            memcpy(&oid.classID, p, sizeof(Four));

            memcpy(p, &oid, sizeof(OID));

            p += ODMG_COLLECTION_REF_SIZE;
        }
    }

    return eNOERROR;
}
