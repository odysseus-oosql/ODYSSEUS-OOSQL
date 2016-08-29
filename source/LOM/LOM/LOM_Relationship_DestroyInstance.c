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

/* local function declaration */
static Four lom_Relationship_ToOneSide_DestroyInstance(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID* oid, Two attrNum, OID* oppositeSideOID);
static Four lom_Relationship_ToManySide_DestroyInstance(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID* oid, Two attrNum, OID* oppositeSideOID);

Four LOM_Relationship_DestroyInstance(
    LOM_Handle* handle,             /* IN LOM system handle */
    Four        fromOcnOrScanId,    /* IN from ocn or scan ID */
    Boolean     fromUseScanFlag,    /* IN use scan ID flag */
    Four        toOcnOrScanId,      /* IN to ocn or scan ID */
    Boolean     toUseScanFlag,      /* IN use scan ID flag */
    Four        relationshipId,     /* IN relationship ID */
    OID*        fromOID,            /* IN OID */
    OID*        toOID)              /* IN OID */
{
    Four            e;              /* error code */
    Four            volId;          /* volume ID */
    One             cardinality;    /* cardinality */
    One             direction;      /* direction of relationship */
    Two             fromAttrNum;    /* attribute num of from-class */
    Two             toAttrNum;      /* attribute num of to-class */
    Four            mv;             /* catalog mount volume index */
    Four            relationshipInfo;

    /* get volume id from oid */
    volId = fromOID->volNo;

    e = Catalog_GetMountTableInfo(handle, volId, &mv);
    if(e < 0) CATALOG_ERROR(handle, e);

    /* get relationship information */
    e = Catalog_GetRelationshipInfo(handle, volId, fromOID->classID,
                                    toOID->classID, relationshipId,
                                    &relationshipInfo);
    if(e < 0) CATALOG_ERROR(handle, e);
    if(relationshipInfo == -1)
        return eNOSUCHRELATIONSHIP_LOM;

    fromAttrNum = CATALOG_GET_RELATIONSHIPFROMATTRNUM(&CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo]) - 1;
    toAttrNum   = CATALOG_GET_RELATIONSHIPTOATTRNUM(&CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo]) - 1;
    direction   = CATALOG_GET_RELATIONSHIPDIRECTION(&CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo]);
    cardinality = CATALOG_GET_RELATIONSHIPCARDINALITY(&CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo]);

    /* check fromOID and toOID */
    if(direction == LOM_RELATIONSHIP_UNIDIRECTIONAL)
    {
        /* unidirectional relationship */
        switch(cardinality)
        {
        case LOM_RELATIONSHIP_ONE_TO_ONE:
        case LOM_RELATIONSHIP_MANY_TO_ONE:
            e = lom_Relationship_ToOneSide_DestroyInstance(handle, fromOcnOrScanId, fromUseScanFlag, fromOID, fromAttrNum, toOID);
            if (e < 0) LOM_ERROR(handle, e);
            break;
            
        case LOM_RELATIONSHIP_ONE_TO_MANY:
        case LOM_RELATIONSHIP_MANY_TO_MANY:
            e = lom_Relationship_ToManySide_DestroyInstance(handle, fromOcnOrScanId, fromUseScanFlag, fromOID, fromAttrNum, toOID);
            if (e < 0) LOM_ERROR(handle, e);
            break;
        }
    }
    else
    {   /* bidirectional relationship */
        switch(cardinality) 
        {
        case LOM_RELATIONSHIP_ONE_TO_ONE:
            e = lom_Relationship_ToOneSide_DestroyInstance(handle, fromOcnOrScanId, fromUseScanFlag, fromOID, fromAttrNum, toOID);
            if (e < 0) LOM_ERROR(handle, e);

            e = lom_Relationship_ToOneSide_DestroyInstance(handle, toOcnOrScanId, toUseScanFlag, toOID, toAttrNum, fromOID);
            if (e < 0) LOM_ERROR(handle, e);
            
            break;
        case LOM_RELATIONSHIP_MANY_TO_ONE:
            e = lom_Relationship_ToOneSide_DestroyInstance(handle, fromOcnOrScanId, fromUseScanFlag, fromOID, fromAttrNum, toOID);
            if (e < 0) LOM_ERROR(handle, e);

            e = lom_Relationship_ToManySide_DestroyInstance(handle, toOcnOrScanId, toUseScanFlag, toOID, toAttrNum, fromOID);
            if (e < 0) LOM_ERROR(handle, e);
            break;
        case LOM_RELATIONSHIP_ONE_TO_MANY:
            e = lom_Relationship_ToManySide_DestroyInstance(handle, fromOcnOrScanId, fromUseScanFlag, fromOID, fromAttrNum, toOID);
            if (e < 0) LOM_ERROR(handle, e);

            e = lom_Relationship_ToOneSide_DestroyInstance(handle, toOcnOrScanId, toUseScanFlag, toOID, toAttrNum, fromOID);
            if (e < 0) LOM_ERROR(handle, e);
            break;
        case LOM_RELATIONSHIP_MANY_TO_MANY:
            e = lom_Relationship_ToManySide_DestroyInstance(handle, fromOcnOrScanId, fromUseScanFlag, fromOID, fromAttrNum, toOID);
            if (e < 0) LOM_ERROR(handle, e);

            e = lom_Relationship_ToManySide_DestroyInstance(handle, toOcnOrScanId, toUseScanFlag, toOID, toAttrNum, fromOID);
            if (e < 0) LOM_ERROR(handle, e);
            break;
        }
    }

    return eNOERROR;
}

Four lom_Relationship_ToOneSide_DestroyInstance(
    LOM_Handle* handle,         /* IN LOM system handle */  
    Four        ocnOrScanId,    /* IN from ocn or scan ID */
    Boolean     useScanFlag,    /* IN use scan ID flag */
    OID*        oid, 
    Two         attrNum,
    OID*        oppositeSideOID)
{
    Four                e;
    LOM_ColListStruct   clist[1];

    clist[0].colNo    = attrNum;
    clist[0].start    = ALL_VALUE;
    MAKE_NULL_OID(clist[0].data.oid);
		clist[0].nullFlag = SM_FALSE;

    e = LOM_UpdateObjectByColList(handle, ocnOrScanId, useScanFlag, oid, 1, clist);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Relationship_ToManySide_DestroyInstance(
    LOM_Handle* handle,         /* IN LOM system handle */  
    Four        ocnOrScanId,    /* IN from ocn or scan ID */
    Boolean     useScanFlag,    /* IN use scan ID flag */
    OID*        oid, 
    Two         attrNum, 
    OID*        oppositeSideOID)
{
    Four                        mv;
    Four                        attrInfo;
    Four                        e;
    Four                        ith;
    ODMG_CollectionColStruct    collData;
    ODMG_CollectionDesc         collDesc;
    Four                        volId;
    Four                        complexType;
    Boolean                     isNull;


    /* get attribute type */
    volId = oid->volNo;
    e = Catalog_GetMountTableInfo(handle, volId, &mv);
    if(e < 0) CATALOG_ERROR(handle, e);
    
    e = Catalog_GetAttrInfo(handle, volId, oid->classID, attrNum, &attrInfo);
    if(e < 0) CATALOG_ERROR(handle, e);

    complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));

    switch(complexType)
    {
    case LOM_COMPLEXTYPE_SET:
        isNull = LOM_Set_IsNull(handle, ocnOrScanId, useScanFlag, oid, attrNum);
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_Set_DeleteElements(handle, ocnOrScanId, useScanFlag, oid, attrNum, 1, oppositeSideOID);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONSET:
        isNull = LOM_CollectionSet_IsNull(handle, ocnOrScanId, useScanFlag, oid, attrNum);
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_CollectionSet_DeleteElements(handle, ocnOrScanId, useScanFlag, oid, attrNum, 1, NULL, oppositeSideOID);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONBAG:
        isNull = LOM_CollectionBag_IsNull(handle, ocnOrScanId, useScanFlag, oid, attrNum);
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_CollectionBag_DeleteElements(handle, ocnOrScanId, useScanFlag, oid, attrNum, 1, NULL, oppositeSideOID);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONLIST:
        isNull = LOM_CollectionList_IsNull(handle, ocnOrScanId, useScanFlag, oid, attrNum);
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_CollectionList_IsMember(handle, ocnOrScanId, useScanFlag, oid, attrNum, sizeof(OID), oppositeSideOID, &ith);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        
        e = LOM_CollectionList_DeleteElements(handle, ocnOrScanId, useScanFlag, oid, attrNum, ith, 1);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LOM_ODMG_Collection_GetDescriptor(handle, ocnOrScanId, useScanFlag, oid, attrNum, &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        collData.nElements = 1;
        collData.elements  = oppositeSideOID;
        e = LOM_ODMG_Collection_DeleteElements(handle, complexType, ocnOrScanId, useScanFlag, oid, attrNum, &collData, &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;

    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LOM_ODMG_Collection_GetDescriptor(handle, ocnOrScanId, useScanFlag, oid, attrNum, &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        collData.nElements = 1;
        collData.elements  = oppositeSideOID;
        e = LOM_ODMG_Collection_IsMember(handle, complexType, ocnOrScanId, useScanFlag, oid, attrNum, &collData, &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        collData.ith       = ith;
        e = LOM_ODMG_Collection_DeleteElements(handle, complexType, ocnOrScanId, useScanFlag, oid, attrNum, &collData, &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    return eNOERROR;
}

