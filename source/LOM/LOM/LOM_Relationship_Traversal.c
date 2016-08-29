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

/*
 * Module: LOM_CreateClass.c
 *
 * Description:
 *  Create a Relationship.
 *
 * Imports:
 *
 * Exports:
 *  Four LOM_Relationship_OpenScan( Four, OID*, Four)
 *  Four LOM_Relationship_CloseScan( Four )
 *  Four LOM_Relationship_NextInstance( Four, Four, OID*)
 *
 * Returns:
 *  Error code
 */


#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"
#include <string.h>

Four LOM_Relationship_OpenScan(
    LOM_Handle* handle,         /* IN LOM system handle */
    Four        ocnOrScanId,    /* IN from ocn or scan ID */
    Boolean     useScanFlag,    /* IN use scan ID flag */
    OID*        oid,            /* IN OID */
    Four        relationshipId  /* IN relationship ID */
)
{
    Four            e;              /* error code */
    Four            volId;          /* volume ID */
    One             cardinality;    /* cardinality */
    One             direction;      /* direction of relationship */
    Two             fromAttrNum;    /* attribute num of from-class */
    Four            mv;             /* catalog mount volume index */
    Four            relationshipInfo;
    ODMG_CollectionDesc     collDesc;
    Boolean         found;
    Four            i;
    Four            attrInfo;
    Four            complexType;
    Four            type;
    Four            baseScanId;
    Four            relationshipScanId;
    Boolean         isNull;


    volId = oid->volNo;

    e = Catalog_GetMountTableInfo(handle, volId, &mv);
    if(e < 0) CATALOG_ERROR(handle, e);

    /* get relationship information */
    found = SM_FALSE;
    for (i = 0; i < CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, mv); i++) 
    {
        if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[i].inUse   == SM_TRUE)
        {
            if(CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[i].fromClassId    == oid->classID &&
               CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[i].relationshipID == relationshipId) 
            {
                found = SM_TRUE;
                break;
            }
        }
    }
    if(!found)
        return eNOSUCHRELATIONSHIP_LOM;
    else
        relationshipInfo = i;

    fromAttrNum = CATALOG_GET_RELATIONSHIPFROMATTRNUM(&CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo]);
    direction   = CATALOG_GET_RELATIONSHIPDIRECTION(&CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo]);
    cardinality = CATALOG_GET_RELATIONSHIPCARDINALITY(&CATALOG_GET_RELATIONSHIPINFOTBL(handle, mv)[relationshipInfo]);

    /* check cardinality */
    if (cardinality == LOM_RELATIONSHIP_ONE_TO_ONE || cardinality == LOM_RELATIONSHIP_MANY_TO_ONE)
        return (eRELATIONSHIPSCANCARDINALITY_LOM);
    
    /* open scan */
    e = Catalog_GetAttrInfo(handle, oid->volNo, oid->classID, fromAttrNum, &attrInfo);
    if(e < 0) CATALOG_ERROR(handle, e);

    complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]);
    type        = CATALOG_GET_ATTRTYPE(&CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]);

    switch(complexType)
    {
    case LOM_COMPLEXTYPE_SET:
        isNull = LOM_Set_IsNull(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_Set_Scan_Open(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONSET:
        isNull = LOM_CollectionSet_IsNull(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_CollectionSet_Scan_Open(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONBAG:
        isNull = LOM_CollectionBag_IsNull(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_CollectionBag_Scan_Open(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONLIST:
        isNull = LOM_CollectionList_IsNull(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if (isNull == SM_TRUE) LOM_ERROR(handle, eNULLCOLLECTION_LOM);

        e = LOM_CollectionList_Scan_Open(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum));
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LOM_ODMG_Collection_GetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        e = LOM_ODMG_Collection_Scan_Open(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONSET, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
		if(e == eNULLCOLLECTION_LOM)
			return eNULLRELATIONSHIP_LOM;
        else if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LOM_ODMG_Collection_GetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        e = LOM_ODMG_Collection_Scan_Open(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
        if(e == eNULLCOLLECTION_LOM)
			return eNULLRELATIONSHIP_LOM;
        else if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
        e = LOM_ODMG_Collection_GetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        e = LOM_ODMG_Collection_Scan_Open(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
        if(e == eNULLCOLLECTION_LOM)
			return eNULLRELATIONSHIP_LOM;
        else if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LOM_ODMG_Collection_GetDescriptor(handle, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        e = LOM_ODMG_Collection_Scan_Open(handle, LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY, ocnOrScanId, useScanFlag, oid, (Two)GET_USERLEVEL_COLNO(fromAttrNum), &collDesc);
        if(e == eNULLCOLLECTION_LOM)
			return eNULLRELATIONSHIP_LOM;
        else if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        return eBADOIDRELATIONSHIPINSTANCE_LOM;
    }

    baseScanId = e;

    /* find empty scan entry */
	relationshipScanId = e = lom_RelationshipScanTableAllocEntry(handle);
	if (e < 0) LOM_ERROR(handle, e);

    LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId  = baseScanId;
    LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType = complexType;

    return relationshipScanId;
}

Four LOM_Relationship_CloseScan(
    LOM_Handle* handle,         /* IN LOM system handle */
    Four        relationshipScanId  /* IN relationship Scan ID */
)
{
    Four e;

    /* check parameters */
    if (!LOM_VALID_RELATIONSHIPSCANID(handle, relationshipScanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* do actual call */
    switch(LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType)
    {
    case LOM_COMPLEXTYPE_SET:
        e = LOM_Set_Scan_Close(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONSET:
        e = LOM_CollectionSet_Scan_Close(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONBAG:
        e = LOM_CollectionBag_Scan_Close(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_COLLECTIONLIST:
        e = LOM_CollectionList_Scan_Close(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        e = LOM_ODMG_Collection_Scan_Close(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType,
                                           LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        e = LOM_ODMG_Collection_Scan_Close(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType,
                                           LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        e = LOM_ODMG_Collection_Scan_Close(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType,
                                           LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    /* Close the given set scan. */
	e = lom_RelationshipScanTableFreeEntry(handle, relationshipScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four LOM_Relationship_NextInstance( 
    LOM_Handle* handle,             /* IN LOM system handle */
    Four        relationshipScanId, /* IN relationship Scan ID */
    Four        nOIDs,              /* IN number of OIDs to read */
    OID*        OIDs                /* OUT OIDs read */
)
{
    Four e;
    ODMG_CollectionColStruct collData;
	Four nElementsRead;

    /* check parameters */
    if (!LOM_VALID_RELATIONSHIPSCANID(handle, relationshipScanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* do actual call */
    switch(LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType)
    {
    case LOM_COMPLEXTYPE_SET:
        e = LOM_Set_Scan_NextElements(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId,
                                      nOIDs, OIDs);
        if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;
        break;
    case LOM_COMPLEXTYPE_COLLECTIONSET:
        e = LOM_CollectionSet_Scan_NextElements(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId,
                                                nOIDs, NULL, sizeof(OID) * nOIDs, OIDs);
        if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;
        break;
    case LOM_COMPLEXTYPE_COLLECTIONBAG:
        e = LOM_CollectionBag_Scan_NextElements(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId,
                                                nOIDs, NULL, sizeof(OID) * nOIDs, OIDs);
        if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;
        break;
    case LOM_COMPLEXTYPE_COLLECTIONLIST:
        e = LOM_CollectionList_Scan_NextElements(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId,
                                                 nOIDs, NULL, sizeof(OID) * nOIDs, OIDs);
        if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
        collData.nElements      = nOIDs;
        collData.elements       = OIDs;
        collData.sizeOfElements = sizeof(OID) * nOIDs;
        e = LOM_ODMG_Collection_Scan_NextElements(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType,
                                                  LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId,
                                                  &collData);
        if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = collData.retN_Elements;
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
        collData.nElements      = nOIDs;
        collData.elements       = OIDs;
        collData.sizeOfElements = sizeof(OID) * nOIDs;
        e = LOM_ODMG_Collection_Scan_NextElements(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType,
                                                  LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId,
                                                  &collData);
        if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = collData.retN_Elements;
        break;
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
        collData.nElements      = nOIDs;
        collData.elements       = OIDs;
        collData.sizeOfElements = sizeof(OID) * nOIDs;
        e = LOM_ODMG_Collection_Scan_NextElements(handle, LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].complexType,
                                                  LOM_RELATIONSHIPSCANTABLE(handle)[relationshipScanId].baseScanId,
                                                  &collData);
        if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = collData.retN_Elements;
        break;
    default:
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    return nElementsRead;
}
