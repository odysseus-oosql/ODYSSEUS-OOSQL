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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Fine-Granule Locking Version                                            */
/*    Version 3.0                                                             */
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
 * Module: LRDS_OrderedSet.c
 *
 * Description:
 *  Implements the SM_COMPLEXTYPE_ORDEREDSET type.
 *
 * Notice:
 *  The input parameter relScanId is used to identify the relation which
 *  contains the given set. We don't use a scan identified by the relScanId.
 *  I suggest that the relScanId be replaced with the open relation number.
 *  When this change is made, the input parameter `tuple id' should not be NULL.
 */


#include <assert.h>
#include <memory.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>

#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM.h"
#include "LRDS.h"
#include "BL_LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


#ifdef ORDEREDSET_BACKWARD_SCAN

/*
 * LRDS Ordered Set Forward & Backward Scan APIs
 */

/*
 * Exports:
 *  Four LRDS_OrderedSet_SpecifyKeyOfElement(Four, Four, char*, Four, LRDS_KeyDesc)
 *  Four LRDS_OrderedSet_Create(Four, Four, TupleID*, Four)
 *  Four LRDS_OrderedSet_Destroy(Four, Four, TupleID*, Four)
 *  Four LRDS_OrderedSet_IsMember(Four, Four, TupleID*, Four, KeyValue*, char*)
 *  Four LRDS_OrderedSet_InsertElement(Four)
 *  Four LRDS_OrderedSet_DeleteElement(Four)
 *  Four LRDS_OrderedSet_InsertElements(Four, Four, TupleID*, Four, Four, char*)
 *  Four LRDS_OrderedSet_DeleteElements(Four, Four, TupleID*, Four, Four, char*)
 *  Four LRDS_OrderedSet_Scan_Open(Four, Four, Boolean, TupleID*, Four, Four, LockParameter*);
 *  Four LRDS_OrderedSet_Scan_Close(Four, Four)
 *  Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*, Four, char*)
 *  Four LRDS_OrderedSet_Scan_InsertElements(Four, Four, Four, char*)
 *  Four LRDS_OrderedSet_Scan_DeleteElements(Four, Four)
 *  Four lrds_OrderedSet_RemoveFromIndex(Four, Four, Four, TupleID*, Four, Four, Four, LockParameter*)
 *  Four lrds_OrderedSet_InsertIntoIndex(Four, Four, IndexInfo*, TupleID*, LockParameter*)
 *
 */



/* To header file ... */
#define SM_COMPLEXTYPE_ORDEREDSET	2

#define LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(_handle) (perThreadTable[_handle].lrdsDS.lrdsOrderedSetAuxColInfoLocalPool)

#define LRDS_HAS_AUXCOLINFO(_complexType, _type) \
    ((_complexType) == SM_COMPLEXTYPE_ORDEREDSET)

#define LRDS_VALID_COLNO(_c, _nColsInRelation) ((_c) >= 0 && (_c) < (_nColsInRelation))

#define DELETED_ELEMENT_POS_ARRAY_SIZE 1024

/* ... To header file */


Four LRDS_OrderedSet_SpecifyKeyOfElement(Four, Four, char*, Four, KeyDesc*);
Four LRDS_OrderedSet_Create(Four, Four, Boolean, TupleID*, Four, LockParameter*);
Four LRDS_OrderedSet_Destroy(Four, Four, Boolean, TupleID*, Four, LockParameter*);
Four LRDS_OrderedSet_IsMember(Four, Four, Boolean, TupleID*, Four, KeyValue*, Four, char*, LockParameter*);
Four LRDS_OrderedSet_GetTotalLengthOfElements(Four, Four, Boolean, TupleID*, Four, Four*, LockParameter*);
Four LRDS_OrderedSet_InsertElement(Four, Four, Boolean, TupleID*, Four, char*, LockParameter*);
Four LRDS_OrderedSet_AppendSortedElements(Four, Four, Boolean, TupleID*, Four, Four, Four, char*, LockParameter*);
Four LRDS_OrderedSet_DeleteElement(Four, Four, Boolean, TupleID*, Four, KeyValue*, LockParameter*);
Four LRDS_OrderedSet_DeleteElements(Four, Four, Boolean, TupleID*, Four, Four, KeyValue*, LockParameter*);
Four LRDS_OrderedSet_Scan_Open(Four, Four, Boolean, TupleID*, Four, Four, LockParameter*); 
Four LRDS_OrderedSet_Scan_Close(Four, Four);
#ifndef COMPRESSION  
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*, Four, char*); 
#else
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*, Four, char*, Four*); 
#endif
Four LRDS_OrderedSet_CreateNestedIndex(Four, Four, Four);
Four LRDS_OrderedSet_DestroyNestedIndex(Four, Four, Four);
#ifdef COMPRESSION  
Four LRDS_OrderedSet_SpecifyVolNo(Four, Four, Boolean, TupleID*, Four, VolNo, LockParameter*);
Four LRDS_OrderedSet_GetVolNo(Four, Four, Boolean, TupleID*, Four, VolNo*, LockParameter*);
#endif

/* end of ... */

#define ORDEREDSET_ELEMENT_FETCH_CHUNK_SIZE PAGESIZE
#define LRDS_ORDEREDSET_CHUNK_OF_SLOTS      128

typedef Four OrderedSet_ElementOffset_T;

#define NO_MORE_SLOT (LONG_MIN+1)
#define UNUSED_SLOT  LONG_MIN
#define HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX  OFFSET_OF(OrderedSetColHdr_T,nestedIndexId)
#define HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX  sizeof(OrderedSetColHdr_T)
#define LRDS_ORDEREDSET_ITH_SLOT_OFFSET(_colStartOffset, _slotNo) (_colStartOffset + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX + sizeof(OrderedSet_ElementOffset_T)*(_slotNo))

#define LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(_nSlots) (sizeof(OrderedSet_ElementOffset_T)*(_nSlots))

#define LRDS_ORDEREDSET_KEYDESC(_cdesc) (&(((OrderedSetAuxColInfo_T*)(PHYSICAL_PTR((_cdesc)->auxInfo)))->kdesc)) 

#define LRDS_ORDEREDSET_NESTED_INDEX_FLAG(_cdesc) (((OrderedSetAuxColInfo_T*)(PHYSICAL_PTR((_cdesc)->auxInfo)))->nestedIndexFlag) 

#define IS_LONG_ORDEREDSET(_colSize) (_colSize > (3*TRAINSIZE))
#define LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(_nElements) ((_nElements >= 0) ? HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX:HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX)


/*
 * Internal Function Prototypes
 */
#ifndef COMPRESSION 
Four lrds_OrderedSet_FindElementUsingSearch(Four, Four, Boolean, TupleID*, Four, Four, OrderedSetColHdr_T*, KeyDesc*, KeyValue*, Four*, Boolean*);
Four lrds_OrderedSet_FindElementUsingNestedIndex(Four, Four, Boolean, TupleID*, Four, Four, OrderedSetColHdr_T*, KeyDesc*, KeyValue*, Four*, Boolean*);
#else
Four lrds_OrderedSet_FindElementUsingSearch(Four, Four, Boolean, TupleID*, Four, Four, OrderedSetColHdr_T*, KeyDesc*, KeyValue*, Four*, Boolean*, Four*, Four*, Four*);
Four lrds_OrderedSet_FindElementUsingNestedIndex(Four, Four, Boolean, TupleID*, Four, Four, OrderedSetColHdr_T*, KeyDesc*, KeyValue*, Four*, Boolean*, Four*);
#endif
Four lrds_OrderedSet_ReadSlotValue(Four, Four, Boolean, TupleID*, Four, Four, Four*);
Four lrds_OrderedSet_WriteSlotValue(Four, Four, Boolean, TupleID*, Four, Four, OrderedSet_ElementOffset_T);
Four lrds_OrderedSet_IncreaseSlotValues(Four, Four, Boolean, TupleID*, Four, Four, OrderedSet_ElementOffset_T, Four);
Four lrds_OrderedSet_DecreaseSlotValues(Four, Four, Boolean, TupleID*, Four, Four, OrderedSet_ElementOffset_T, Four);
Four lrds_OrderedSet_DeallocateSlot(Four, Four, Boolean, TupleID*, Four, OrderedSetColHdr_T*, Four);
Four lrds_OrderedSet_AllocateSlot(Four, Four, Boolean, TupleID*, Four, OrderedSetColHdr_T*, Four*);
Four lrds_OrderedSet_AddSlots(Four, Four, Boolean, TupleID*, Four, OrderedSetColHdr_T*);
Four lrds_OrderedSet_CompareKeyValues(Four, KeyDesc*, KeyValue*, KeyValue*);
Four lrds_OrderedSet_CreateNestedIndex(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, OrderedSetColHdr_T*, KeyDesc*); 
Four lrds_OrderedSet_DestroyNestedIndex(Four, Four, Boolean, TupleID*, TupleHdr*, Four, OrderedSetColHdr_T*, Boolean);
Four lrds_OrderedSet_InsertElementIntoNestedIndex(Four, IndexID*, KeyDesc*, KeyValue*, Four);
Four lrds_OrderedSet_DeleteElementFromNestedIndex(Four, IndexID*, KeyDesc*, KeyValue*, Four);
Four lrds_OrderedSet_DeleteElementsWithNestedIndex(Four, Four, Boolean, TupleID*, Four, Four, KeyValue*, LockParameter*);
Four lrds_CreateNestedIndexByBulkLoad(Four, Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, OrderedSetColHdr_T*, KeyDesc*, LockParameter*); 
Four lrds_OrderedSet_DumpSlotValues(Four, Four, Boolean, TupleID*, Four);


#define LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(_nElements) ((_nElements >= 0) ? (sizeof(OrderedSet_ElementLength) * (_nElements)) : (-1 * sizeof(OrderedSet_ElementLength) * (_nElements)))

#define LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(_orderedSetColHdr) (((_orderedSetColHdr).nElements >= 0) ? (HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX) : (HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX + LRDS_ORDEREDSET_SLOT_ARRAY_SIZE((_orderedSetColHdr).nSlots)))

#define LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(_orderedSetColHdr) (LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(_orderedSetColHdr) + LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE((_orderedSetColHdr).nElements))

Four lrds_OrderedSet_UpdateElementOrderedOffset(Four, lrds_RelTableEntry*, Four, Boolean, TupleID*, OrderedSetColHdr_T*, Four, Four, Four);
Four lrds_OrderedSet_FetchElement(Four, lrds_RelTableEntry*, Four, Boolean, TupleID*, Four, Four, char*, OrderedSetColHdr_T*, Four, Four, Four*);
Four lrds_OrderedSet_FetchElementFromOrderedSetWithSlotArrayWithoutNestedIndex(Four, lrds_RelTableEntry*, Four, Boolean, TupleID*, Four, Four, char*, OrderedSetColHdr_T*, Four, Four, Four, Four*); 
Four lrds_OrderedSet_DumpAllElement(Four handle, Four ornOrScanId, Boolean useScanFlag, TupleID *tid, Four colNo);

#ifdef COMPRESSION  
/*@================================
 * LRDS_OrderedSet_SpecifyVolNo()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_GetVolNo(Four, Boolean, TupleID*, Four, Four, LockParameter*)
 *
 * Description:
 *  Get volNo.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_SpecifyVolNo(
    Four 			handle, 
    Four 			ornOrScanId,          		/* IN open relation no or scan id*/
    Boolean 			useScanFlag,        		/* IN TRUE if above parameter is scan id */
    TupleID 			*tid,               		/* IN tuple containing the set */
    Four 			colNo,                 		/* IN column on which the set is defined */
    VolNo 			volNo,                		/* IN volNo */
    LockParameter 		*lockupPtr)   			/* IN lock parameter */
{
    Four 			e;				/* error code */
    Four 			orn;
    Four 			smScanId;
    Four 			tupHdrSize;			/* size of tuple header */
    Four 			start;				/* starting offset of fetch */
    Four 			length;				/* length of the set column */
    Four 			totalDeletedBytes;		/* amount of bytes deleted from the column */
    ColDesc 			*cdesc;				/* pointer to the current column descriptor */
    TupleHdr 			tupHdr;				/* a tuple header */
    unsigned char 		*nullVector;			/* bit array of null flags */
    lrds_RelTableEntry 		*relTableEntry; 		/* pointer to an entry of relation table */
    OrderedSetColHdr_T 		orderedSetColHdr;
    LockParameter 		fileLockup;			/* lockup for SM_Fetch Tuple */
    LockParameter 		objLockup;			/* lockup for SM_Fetch Tuple */
    LockParameter 		*fileLockupPtr; 		/* pointer to the lockup value */
    LockParameter 		*objLockupPtr; 			/* pointer to the lockup value */
    ColDesc 			*relTableEntry_cdesc; 
    

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_SpecifyVolNo()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_S || lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = (lockupPtr->mode == L_S) ? L_IS:L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = lockupPtr->mode;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    orderedSetColHdr.volNo = volNo;
    
    /* Write the updated column header. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                          (char*)&orderedSetColHdr, sizeof(OrderedSetColHdr_T), NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);
    
    return(eNOERROR);

} /* LRDS_OrderedSet_SpecifyVolNo() */

/*@================================
 * LRDS_OrderedSet_GetVolNo()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_GetVolNo(Four, Boolean, TupleID*, Four, Four, LockParameter*)
 *
 * Description:
 *  Get volNo.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_GetVolNo(
    Four 			handle, 
    Four 			ornOrScanId,           		/* IN open relation no or scan id*/
    Boolean 			useScanFlag,        		/* IN TRUE if above parameter is scan id */
    TupleID 			*tid,               		/* IN tuple containing the set */
    Four 			colNo,                 		/* IN column on which the set is defined */
    VolNo 			*volNo,               		/* OUT volNo */
    LockParameter 		*lockupPtr)   			/* IN lock parameter */
{
    Four 			e;				/* error code */
    Four 			orn;
    Four 			smScanId;
    Four 			tupHdrSize;			/* size of tuple header */
    Four 			start;				/* starting offset of fetch */
    Four 			length;				/* length of the set column */
    Four 			totalDeletedBytes;		/* amount of bytes deleted from the column */
    ColDesc 			*cdesc;				/* pointer to the current column descriptor */
    TupleHdr 			tupHdr;				/* a tuple header */
    unsigned char 		*nullVector;			/* bit array of null flags */
    lrds_RelTableEntry 		*relTableEntry; 		/* pointer to an entry of relation table */
    OrderedSetColHdr_T 		orderedSetColHdr;
    LockParameter 		fileLockup;			/* lockup for SM_Fetch Tuple */
    LockParameter 		objLockup;			/* lockup for SM_Fetch Tuple */
    LockParameter 		*fileLockupPtr; 		/* pointer to the lockup value */
    LockParameter 		*objLockupPtr; 			/* pointer to the lockup value */
    ColDesc 			*relTableEntry_cdesc; 
    

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_GetVolNo()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_S || lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = (lockupPtr->mode == L_S) ? L_IS:L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = lockupPtr->mode;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    *volNo = orderedSetColHdr.volNo;
    
    return(eNOERROR);

} /* LRDS_OrderedSet_GetVolNo() */

#endif 

/*
 * Function: Four lrds_OrderedSet_UpdateElementOrderedOffset(Four, lrds_RelTableEntry*, Four, Boolean, TupleID*, OrderedSetColHdr_T*, 
 *					                     Four, Four, Four)
 *
 * Description:
 *  Update the elment ordered offset. 
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_UpdateElementOrderedOffset(
    Four                        handle, 
    lrds_RelTableEntry*		relTableEntry, 			/* IN  pointer to an entry of relation table */
    Four			smScanId, 			/* IN  open relation no or scan id */
    Boolean			useScanFlag, 			/* IN  TRUE if above parameter is scan id */
    TupleID*			tid, 				/* IN  tuple containing the given column */
    OrderedSetColHdr_T*		orderedSetColHdr, 		/* IN  column header for the ordered set */
    Four			colStart, 			/* IN  offset from the tuple to the column */
    Four                        startOffset,			/* IN  offset to update the ordered offset */
    Four			elementLength			/* IN  delta of updating */
)
{
    Four			e;				/* error code */
    Four			i;				/* for loop */
    Four			offset;				/* offset */
    Four			remainBytes;			/* remain bytes to update */
    Four			bytesToRead;			/* bytes to be read */
    OrderedSet_ElementLength	buffer[PAGESIZE/sizeof(OrderedSet_ElementLength)]; /* ordered offset buffer */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_UpdateElementOrderedOffset()"));


    offset = colStart;
    offset += (orderedSetColHdr->nElements < 0) ? HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
						 LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr->nSlots)
                                               : HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    offset+= startOffset;

    remainBytes = LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr->nElements) - startOffset;

    while (remainBytes > 0) {

	bytesToRead = MIN(sizeof(buffer), remainBytes);

        /* Fetch the ordered offset */
	e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                             offset, bytesToRead, (char*)buffer, NULL, NULL);
	if (e < eNOERROR) ERR(handle, e);

	for (i = 0; i < bytesToRead/sizeof(OrderedSet_ElementLength); i++) {
	    buffer[i] += elementLength;
	}

        /* Update the ordered offset */
	e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
			      offset, bytesToRead, buffer, bytesToRead, NULL, NULL);
	if (e < eNOERROR) ERR(handle, e);

	remainBytes -= bytesToRead;
	offset      += bytesToRead;

    }

    return (eNOERROR);
}

/*
 * Function: Four lrds_OrderedSet_FetchElement(Four, lrds_RelTableEntry*, Four, Boolean, TupleID*, Four, Four, char*, OrderedSetColHdr_T*,
 *					       Four, Four, Four*)
 *
 * Description:
 *  Fetch the OrderedSet Element and the Element Start Offset from the column.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_FetchElement(
    Four                        handle, 
    lrds_RelTableEntry*		relTableEntry, 					/* IN  pointer to an entry of relation table */
    Four			smScanId, 					/* IN  open relation no or scan id */
    Boolean			useScanFlag, 					/* IN  TRUE if above parameter is scan id */
    TupleID*			tid, 						/* IN  tuple containing the given column */
    Four                        startOffset,					/* IN  offset from the tuple to the ordered offset */
    Four                        length,                 			/* IN  data length to fetch the ordered element */
    char*                       buf,						/* OUT buffer used to fetch*/
    OrderedSetColHdr_T*		orderedSetColHdr, 				/* IN  column header for the ordered set */
    Four			colStart, 					/* IN  offset from the tuple to the column */
    Four			colLength, 					/* IN  column length */
    Four*			elementStartOffset				/* OUT offset from the column to the element */
)
{
    Four			sizeOfElementLength;				/* size of the ordered offset type */
    Four			existNestedIndex;				/* flag that indicates existing of nested index */
    Four			nElements;					/* number of elements */
    Four			offsetFromColStartToElement;			/* offset from the column to the element */
    char			buffer[2*sizeof(OrderedSet_ElementLength)]; 	/* buffer used to fetch the ordered offset */
    Four			offset1, offset2;				/* offset */
    Four 			elementLength;					/* ordered set element length */
    Four			e;						/* error code */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_FetchElement()"));


    /* get information */
    sizeOfElementLength = sizeof(OrderedSet_ElementLength);
    existNestedIndex = (orderedSetColHdr->nElements < 0)? (1) : (0);
    nElements        = (existNestedIndex == 1) ? ( -1 * orderedSetColHdr->nElements) : (orderedSetColHdr->nElements);
    offsetFromColStartToElement = (existNestedIndex == 1) ? (HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
							     LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr->nSlots))
							  : (HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX);
    offsetFromColStartToElement += LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr->nElements);

    /* fetch the ordered offset */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
			 startOffset, 2*sizeOfElementLength, buffer, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    memcpy(&offset1, buffer, sizeOfElementLength);
    memcpy(&offset2, &buffer[sizeOfElementLength], sizeOfElementLength);

    if (colStart + offsetFromColStartToElement  - sizeOfElementLength == startOffset)
	offset2 = colLength - offsetFromColStartToElement;

    elementLength = offset2 - offset1;
    memcpy(buf, &elementLength, sizeOfElementLength);

    if (length > sizeOfElementLength) {
    	/* fetch the ordered element */
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
			     colStart+offsetFromColStartToElement+offset1, length-sizeOfElementLength, &buf[sizeOfElementLength], 
			     NULL, NULL);
    	if (e < eNOERROR) ERR(handle, e);
    }

    if (elementStartOffset != NULL) *elementStartOffset = offsetFromColStartToElement+offset1;

    return(eNOERROR);

}

/*
 * Function: Four lrds_OrderedSet_FetchElementFromOrderedSetWithSlotArrayWithoutNestedIndex(Four, lrds_RelTableEntry*, Four, Boolean, 
 *	     TupleID*, Four, Four, char*, OrderedSetColHdr_T*, Four, Four, Four, Four*)
 *
 * Description:
 *  Fetch the OrderedSet Element and the Element Start Offset from the column from ordered set with slot array but without nested index.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_FetchElementFromOrderedSetWithSlotArrayWithoutNestedIndex(
    Four                        handle,
    lrds_RelTableEntry*		relTableEntry, 					/* IN  pointer to an entry of relation table */
    Four			smScanId, 					/* IN  open relation no or scan id */
    Boolean			useScanFlag, 					/* IN  TRUE if above parameter is scan id */
    TupleID*			tid, 						/* IN  tuple containing the given column */
    Four                        startOffset,					/* IN  offset from the tuple to the ordered offset */
    Four                        length,                 			/* IN  data length to fetch the ordered element */
    char*                       buf,						/* OUT buffer used to fetch*/
    OrderedSetColHdr_T*		orderedSetColHdr, 				/* IN  column header for the ordered set */
    Four			colStart, 					/* IN  offset from the tuple to the column */
    Four			colLength, 					/* IN  column length */
    Four                        nSlots,                                         /* IN  number of slots */
    Four*			elementStartOffset				/* OUT offset from the column to the element */
)
{
    Four			sizeOfElementLength;				/* size of the ordered offset type */
    Four			nElements;					/* number of elements */
    Four			offsetFromColStartToElement;			/* offset from the column to the element */
    char			buffer[2*sizeof(OrderedSet_ElementLength)]; 	/* buffer used to fetch the ordered offset */
    Four			offset1, offset2;				/* offset */
    Four 			elementLength;					/* ordered set element length */
    Four			e;						/* error code */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_FetchElementFromOrderedSetWithSlotArrayWithoutNestedIndex()"));


    /* paremeter check */
    if (orderedSetColHdr->nElements < 0) return(eBADPARAMETER);

    /* get information */
    sizeOfElementLength = sizeof(OrderedSet_ElementLength);
    nElements        = orderedSetColHdr->nElements;
    offsetFromColStartToElement = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX +
			          LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(nSlots);
    offsetFromColStartToElement += LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr->nElements);

    /* fetch the ordered offset */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
			 startOffset, 2*sizeOfElementLength, buffer, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    memcpy(&offset1, buffer, sizeOfElementLength);
    memcpy(&offset2, &buffer[sizeOfElementLength], sizeOfElementLength);

    if (colStart + offsetFromColStartToElement  - sizeOfElementLength == startOffset)
	offset2 = colLength - offsetFromColStartToElement;

    elementLength = offset2 - offset1;
    memcpy(buf, &elementLength, sizeOfElementLength);

    if (length > sizeOfElementLength) {
	/* fetch the ordered element */
    	e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
			     colStart+offsetFromColStartToElement+offset1, length-sizeOfElementLength, &buf[sizeOfElementLength],
			     NULL, NULL);
    	if (e < eNOERROR) ERR(handle, e);
    }

    if (elementStartOffset != NULL) *elementStartOffset = offsetFromColStartToElement+offset1;

    return(eNOERROR);

}


/*
 * Function: Four lrds_UpdateAuxColInfoInSysColumns(Four, FileID*, IndexID*, FileID*, Four, void*)
 *
 * Description:
 *  Update the auxiliary column information of the given column.
 *
 * Returns:
 *  error code
 */
Four lrds_UpdateAuxColInfoInSysColumns(
    Four handle,                /* IN handle */
    Four user_v,                /* IN index on the LRDS Mount Table*/
    FileID *fid,                /* IN file id for the given relation */
    Four colNo,                 /* IN column to read the description about */
    void *auxInfo)                /* IN auxiliary column information for the given column */
{
    Four e;                     /* error code */
    Four catScanId;		/* a SM level scan */
    ColListStruct clist[1]; /* column list structure */
    BoundCond lb;		/* bounary condition of range scan */
    Two keyLen;			/* should be 'Two' to store the key length */
    Four i, j;			/* index variables */
    BoolExp bool;               /* a boolean expression */
#ifndef NDEBUG
    ColInfo cinfo;
#endif /* NDEBUG */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_UpdateAuxColInfoInSysColumns()"));


    /*
    ** Extract a catalog entry from the catalog table LRDS_SYSCOLUMNS.
    */
    lb.op = SM_EQ;
    lb.key.len = SM_FILEID_SIZE;
    memcpy(&(lb.key.val[0]), (char*)fid, SM_FILEID_SIZE);

    bool.op = SM_EQ;
    bool.colNo = LRDS_SYSCOLUMNS_COLUMNNO_COLNO;
    ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, bool, colNo, Two); 


    /* The upper bound is same with the lower bound 'lb'. */
    e = catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[LRDS_SYSCOLUMNS],
                                       &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, user_v,LRDS_SYSCOLUMNS)->ii))[0].iid), 
                                       &lb, &lb, 1, &bool, NULL);
    if (e < eNOERROR) ERR(handle, e);

    e = LRDS_NextTuple(handle, catScanId, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    assert(e != EOS);

    /*
     * Construct 'clist'.
     */
#ifndef NDEBUG
    clist[0].colNo = LRDS_SYSCOLUMNS_COLINFO_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = sizeof(ColInfo);
    clist[0].data.ptr = &cinfo;

    e = LRDS_FetchTuple(handle, catScanId, TRUE, (TupleID*)NULL, 1, &(clist[0]));
    if (e < eNOERROR) ERR(handle, e);

    assert(cinfo.complexType == SM_COMPLEXTYPE_ORDEREDSET);
#endif /* NDEBUG */

    clist[0].colNo = LRDS_SYSCOLUMNS_AUXCOLINFO_COLNO;
    clist[0].nullFlag = FALSE;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = sizeof(OrderedSetAuxColInfo_T);
    clist[0].data.ptr = auxInfo;

    e = LRDS_UpdateTuple(handle, catScanId, TRUE, (TupleID*)NULL, 1, &clist[0]);
    if (e < eNOERROR) ERR(handle, e);

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_UpdateAuxColInfoInSysColumns() */



/*@================================
 * LRDS_OrderedSet_Create()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_Create(Four, Four, TupleID*, Four)
 *
 * Description:
 *  Create a ordered set on the given column of the given tuple of the given relation.
 *  Before creating a ordered set the column has NULL value.
 *
 * Retuns:
 *  error code
 */
Four LRDS_OrderedSet_Create(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four colNo,			/* IN column on which the ordered set is created */
    LockParameter *lockupPtr)      /* IN lock parameter */
{
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four oldTupHdrSize;		/* old size of tuple header */
    Four newTupHdrSize;		/* new size of tuple header */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four start;			/* starting offset of the set column */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    OrderedSetColHdr_T orderedSetColHdr;
    Four i;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_Create()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /*
     * Fetch the tuple header.
     */
    oldTupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, oldTupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    newTupHdrSize = oldTupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols) {
        e = lrds_AddColumns(handle, ornOrScanId, useScanFlag, tid, &tupHdr);
        if (e < eNOERROR) ERR(handle, e);

        newTupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);
    }

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (!BITTEST(nullVector, colNo)) ERR(handle, eORDEREDSET_EXIST_LRDS);

    /*
     * Initialize the ordered set.
     */
    orderedSetColHdr.nElements = 0;

#ifdef COMPRESSION 
    orderedSetColHdr.lastDocId = 0;
#endif    

    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, start, 0, &orderedSetColHdr,
                          HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Update the tuple header.
     */
    /* Reset the corresponding bit of array of null flags. */
    BITRESET(nullVector, colNo);

    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
        tupHdr.varColOffset[i] += HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, oldTupHdrSize, (char*)&tupHdr,
                          newTupHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_OrderedSet_Create() */



/*@================================
 * LRDS_OrderedSet_Destroy()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_Destroy(Four, Four, TupleID*, Four)
 *
 * Description:
 *  Destroy the ordered set on the given column of the given tuple of the given relation.
 *  The column will be set to NULL value after destroying it.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_Destroy(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four colNo,			/* IN column on which we destroy the ordered set  */
    LockParameter *lockupPtr)      /* IN lock parameter */
{
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of the set column */
    Four length;		/* length of the set column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four i, j;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr; /* column header for the ordered set */
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    ColDesc *relTableEntry_cdesc; 

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_Destroy()"));


    /*
    ** check parameters
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }


    /* Is there any opened scan on the given ordered set? */
    for (i = 0; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++) {
	if (LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

	if (EQUAL_TUPLEID(LRDS_ORDEREDSETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_ORDEREDSETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    ERR(handle, eORDEREDSET_OPENSCAN_LRDS);
	}
    }


    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);

    /* Get the starting offset and length of the column. */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);


    /*
    ** Destroy the ordered set.
    */
    /*
     * If nested index exist, destroy it.
     */
    if (LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc)) {
        /*
         * Read the ordered set column header
         */
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                             (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                             (char*)&orderedSetColHdr, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        if (orderedSetColHdr.nElements < 0) { /* this has the nested index */
            e = lrds_OrderedSet_DestroyNestedIndex(handle, ornOrScanId, useScanFlag,
                                                   tid, &tupHdr, start,
                                                   &orderedSetColHdr, FALSE);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    /* Delete all data in the given column. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, length, NULL, 0, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Set the corresponding bit of array of null flags. */
    BITSET(nullVector, colNo);

    /* Adjust the ending offset of the variable-length columns. */
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
	tupHdr.varColOffset[i] -= length;

    /* Update the tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         tupHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_OrderedSet_Destroy() */



/*@================================
 * LRDS_OrderedSet_GetTotalLengthOfElements()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_GetTotalLengthOfElements(Four, Four, TupleID*, Four, Four*, LockParameter)
 *
 * Description:
 *  Get the total length of elements.
 *  The length exclude the slot array size.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_GetTotalLengthOfElements(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the set */
    Four colNo,			/* IN column on which the set is defined */
    Four *totalLength,          /* OUT total length of elements */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Two  s;			/* for setting of key length */
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    Four totalDeletedBytes;	/* amount of bytes deleted from the column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_GetTotalLengthOfElements()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_S || lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = (lockupPtr->mode == L_S) ? L_IS:L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = lockupPtr->mode;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    if (orderedSetColHdr.nElements >= 0) { /* nested index does not exist */
        *totalLength = length - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    } else {                    /* nested index exists */
        *totalLength = length - HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX - LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);
    }

    return(eNOERROR);

} /* LRDS_OrderedSet_GetTotalLengthOfElements() */


/*@================================
 * LRDS_OrderedSet_GetN_Elements()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_GetN_Elements(Four, TupleID*, Four, Four*, LockParameter)
 *
 * Description:
 *  Get the total length of elements.
 *  The length exclude the slot array size.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_GetN_Elements(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the set */
    Four colNo,			/* IN column on which the set is defined */
    Four *nElements,          /* OUT total length of elements */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Two  s;			/* for setting of key length */
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    Four totalDeletedBytes;	/* amount of bytes deleted from the column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_GetTotalLengthOfElements()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_S || lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = (lockupPtr->mode == L_S) ? L_IS:L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = lockupPtr->mode;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);

    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    if (orderedSetColHdr.nElements >= 0) { /* nested index does not exist */
        *nElements = orderedSetColHdr.nElements;
    } else {                    /* nested index exists */
        *nElements = orderedSetColHdr.nElements * -1;
    }

    return(eNOERROR);

} /* LRDS_OrderedSet_GetN_Elements() */


/*@================================
 * LRDS_OrderedSet_IsMember()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_IsMember(Four, Four, TupleID*, Four, Four, KeyValue*)
 *
 * Description:
 *  Check if there is an element with the given key
 *
 * Returns:
 *  Membership if the return value is greater than or equal to 0
 *     TRUE if it is a member
 *     FALSE otherwise
 *  error code if the return value is less than 0
 *
 * Notice:
 *  The bufSize should be sizeof(OrderedSet_ElementLength) at least.
 */
Four LRDS_OrderedSet_IsMember(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the set */
    Four colNo,			/* IN column on which the set is defined */
    KeyValue *kval,		/* IN key value to search for */
    Four bufSize,               /* IN element buffer length */
    char *elementBuf,           /* OUT return the element if not NULL */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Two  s;			/* for setting of key length */
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four indexIdx;		/* index on array of index informations */
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    Four totalDeletedBytes;	/* amount of bytes deleted from the column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four nBytesToRead;		/* amount of bytes to read from disk */
    Four size;			/* amount of data in the buffer */
    Four updateStart;		/* starting offset of update within a buffer */
    Four nBytesDeleted;		/* amount of deleted bytes within a buffer */
    char buf[PAGESIZE];		/* buffer for holding the elements */
    Four current;		/* points to the current element */
    char *ptr;			/* points to an element among the given elements */
    OrderedSet_ElementLength elementLength; /* length of an element */
    OrderedSet_ElementLength elementLength2; /* length of an element among the given element */
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr;
    KeyDesc *kdesc;             /* key descriptor for elements */
    OrderedSet_ElementOffset_T offset;
    Four offsetFromColStart;    /* offset of an element from column start */
    Four slotNo;
    Boolean membershipFlag;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    Four          elementStartOffset; /* element offset from tuple */
    Four          elemLen; 
    Four          elemLenWithLengthField; 
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four          prevDocId;
    Four          docId;
    Four          docIdGapSize = 0;
#endif    


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_IsMember()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Is the key value valid? */
    if (kval == NULL) ERR(handle, eBADPARAMETER);

    /* Is the element buffer valid? */
    if (bufSize < 0 || bufSize > 0 && elementBuf == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_S || lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = (lockupPtr->mode == L_S) ? L_IS:L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = lockupPtr->mode;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }


    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Get the key descriptor for the ordered set element
     */
    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    if (orderedSetColHdr.nElements >= 0) { /* nested index does not exist */
        e = lrds_OrderedSet_FindElementUsingSearch(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, kval, &offsetFromColStart, &membershipFlag);
#else
            kdesc, kval, &offsetFromColStart, &membershipFlag, &prevDocId, &docId, &docIdGapSize);
#endif   
        if (e < eNOERROR) ERR(handle, e);
    } else {                    /* nested index exists */
        e = lrds_OrderedSet_FindElementUsingNestedIndex(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, kval, &slotNo, &membershipFlag);
#else
            kdesc, kval, &slotNo, &membershipFlag, &docId);
#endif            
        if (e < eNOERROR) ERR(handle, e);

        if (elementBuf != NULL && membershipFlag) {
            e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, tid,
                                              start, slotNo, &offset);
            if (e < eNOERROR) ERR(handle, e);

            /* Now 'offsetFromColStart' is distance from the begining of column. */
            offsetFromColStart = offset + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
                LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);
        }
    }


    /*
     * Read the element.
     */
    if (elementBuf != NULL && membershipFlag) { 

        e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid, 
                                         start+offsetFromColStart, sizeof(OrderedSet_ElementLength), elementBuf, 
                                         &orderedSetColHdr, start, length, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        memcpy(&elemLen, elementBuf, sizeof(OrderedSet_ElementLength));
        elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

#ifdef COMPRESSION 
        /* the size of decoded docId could be increased by sizeof(Four) - 1 */
        if (bufSize >= elemLenWithLengthField + sizeof(Four) - 1) 
        {
            Four        inIndex; 
            Four        outIndex;
            Four        elemValueLength; 
#else
        if (bufSize >= elemLenWithLengthField) {
#endif

            e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                                 start+elementStartOffset, elemLen, &elementBuf[sizeof(OrderedSet_ElementLength)], NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);

#ifdef COMPRESSION 
            /* get element value */
            inIndex = sizeof(OrderedSet_ElementLength);

            if(docIdGapSize == 0)
                LRDS_VARIABLE_BYTE_DECODING(&elementBuf[inIndex], docIdGapSize, docId);

            inIndex += docIdGapSize;
            outIndex = sizeof(OrderedSet_ElementLength) + sizeof(docId);
            elemValueLength = elemLen - docIdGapSize;

            memmove(&elementBuf[outIndex], &elementBuf[inIndex], elemValueLength);

            /* copy elemLen */
            elemLen = sizeof(docId) + elemValueLength;
            memcpy(elementBuf, &elemLen, sizeof(OrderedSet_ElementLength));

            /* copy docId */
            outIndex = sizeof(OrderedSet_ElementLength);
            memcpy(&elementBuf[outIndex], kval->val, sizeof(docId));
#endif
        }
    }

    return(membershipFlag);

} /* LRDS_OrderedSet_IsMember() */



/*@================================
 * LRDS_OrderedSet_AppendSortedElements()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_AppendSortedElements(Four, Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Append a chunk of elements which are sorted in the order.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_AppendSortedElements(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the posting */
    Four colNo,			/* IN column on which the posting is defined */
    Four nElements,             /* IN # of elements to append */
    Four elementsBufSize,       /* IN buffer size */
    char *elementsBuf,		/* IN elements to append */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four constLength, length;	/* length of the set column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    KeyValue kval;		/* key value of an index entry */
    Four keyLen;                /* length of element key */
    KeyDesc *kdesc;             /* pointer to the key descriptor for element */
    unsigned char *nullVector;	/* bit array of null flags */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr; /* ordered set column header */
    OrderedSet_ElementOffset_T offset, offset1, offset2; 
    OrderedSet_ElementLength elementLength; /* element length */ 
    OrderedSet_ElementLength prevElementLength; /* element length */ 
    OrderedSet_ElementLength *ptrOfElementOrderedOffset; 
    OrderedSet_ElementLength elemLen; /* element length */
    OrderedSet_ElementLength elemLenWithLengthField; /* element length including length field size */
    Four oldOrderedSetColHdrSize; /* old header size of ordered set column */
    Four orderedSetColHdrSize;  /* header size of ordered set column */
    Boolean nestedIndexFlag;    /* TRUE if nested index is allowed */
    Four slotNo;
    char *ptr;
    Four i;
    Four count;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    char          *buffer; 
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four                docId, docIdGap, oldDocId;
    char                tempData[sizeof(Four)+2];
    char                encodedData[sizeof(Four)+2];
    Four                encodedDataLength;
    Four                encodedElementLen;
    Four                encodedLength;
#endif
    

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_AppendSortedElements()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (nElements == 0) return(eNOERROR);

    if (elementsBufSize <= 0 || elementsBuf == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /* Is there any opened scan on the given ordered set? */
    for (i = 0; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++) {
	if (LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

	if (EQUAL_TUPLEID(LRDS_ORDEREDSETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_ORDEREDSETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    ERR(handle, eORDEREDSET_OPENSCAN_LRDS);
	}
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    constLength = length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr); 

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* save the old ordered set column header length because an ordered set
       can have nested index during this operation */
    oldOrderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);

    /*
     * Append the elements.
     */

    buffer = (char*)malloc(elementsBufSize);
    if(buffer == NULL) ERR(handle, eMEMORYALLOCERR); 

    /* Write the element ordered offset. */
    ptrOfElementOrderedOffset = (OrderedSet_ElementLength*)buffer;

    if (orderedSetColHdr.nElements >= 0) {
    	offset = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX +
                 LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements);
    }
    else {
    	offset = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
                 LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots) +
                 LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements);
    }

#ifndef COMPRESSION 
    offset1 = 0;
    ptrOfElementOrderedOffset[0] = constLength - offset;
    memcpy(&prevElementLength, &elementsBuf[0], sizeof(OrderedSet_ElementLength));

    for(i = 1; i < nElements; i++) {
    	ptrOfElementOrderedOffset[i] = ptrOfElementOrderedOffset[i-1] + prevElementLength;

	offset1 += (sizeof(OrderedSet_ElementLength) + prevElementLength);
        memcpy(&prevElementLength, &elementsBuf[offset1], sizeof(OrderedSet_ElementLength));
    }

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          start+offset, 0, buffer, sizeof(OrderedSet_ElementLength) * nElements, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Write the element. */
    offset1 = 0;
    offset2 = 0;

    for(i = 0; i < nElements; i++) {
        memcpy(&elementLength, &elementsBuf[offset1], sizeof(OrderedSet_ElementLength));
        memcpy(&buffer[offset2], &elementsBuf[offset1+sizeof(OrderedSet_ElementLength)], elementLength);

	offset1 += (sizeof(OrderedSet_ElementLength) + elementLength);
	offset2 += elementLength;
    }

    offset = constLength + sizeof(OrderedSet_ElementLength) * nElements;

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          start+offset, 0, buffer, offset2, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

#else
    ptrOfElementOrderedOffset[0] = constLength - offset;
    offset2 = sizeof(OrderedSet_ElementLength) * nElements;

    ptr = elementsBuf;
    oldDocId = orderedSetColHdr.lastDocId;
    
    for (i = 0; i < nElements; i++ ) 
    {
        memcpy(&elementLength, ptr, sizeof(OrderedSet_ElementLength));
        memcpy(&docId, &ptr[sizeof(OrderedSet_ElementLength)], sizeof(docId));
        
        docIdGap = docId - oldDocId;
        oldDocId = docId;
        
        LRDS_VARIABLE_BYTE_ENCODING(docIdGap, tempData, encodedData, encodedDataLength);
        
        encodedElementLen = elementLength - sizeof(docId) + encodedDataLength;

        /* is not last element */
        if(i < nElements -1)
        {
           ptrOfElementOrderedOffset[i+1] = ptrOfElementOrderedOffset[i] + encodedElementLen;
        }
        else
        {
            memcpy(&orderedSetColHdr.lastDocId, &docId, sizeof(docId));
        }
        
        if(offset2 + encodedDataLength > elementsBufSize)
        {
           if(buffer != NULL) free(buffer);
           ERR(handle, eINTERNAL);
        }
        memcpy(&buffer[offset2], encodedData, encodedDataLength);
        offset2 += encodedDataLength;
        
        if(offset2 + elementLength - sizeof(docId) > elementsBufSize)
        {
           if(buffer != NULL) free(buffer);
           ERR(handle, eINTERNAL);
        }
        memcpy(&buffer[offset2], &ptr[sizeof(OrderedSet_ElementLength)+sizeof(docId)], elementLength - sizeof(docId));
        offset2 += elementLength - sizeof(docId);
        
        ptr += elementLength + sizeof(OrderedSet_ElementLength);
    }

    /* Write the element ordered offset. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                          start+offset, 0, buffer, sizeof(OrderedSet_ElementLength) * nElements, NULL, NULL);
    if (e < eNOERROR) 
    {
        if(buffer != NULL) free(buffer);
        ERR(handle, e);
    }

    /* Write the element. */
    offset = constLength + sizeof(OrderedSet_ElementLength) * nElements; 

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, start+offset, 0, 
                          &buffer[sizeof(OrderedSet_ElementLength) * nElements], 
                          offset2 - sizeof(OrderedSet_ElementLength) * nElements, NULL, NULL);
    if (e < eNOERROR) 
    {
        if(buffer != NULL) free(buffer);
        ERR(handle, e);
    }
#endif

    free(buffer);

    /* Update the tuple header. */
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
#ifndef COMPRESSION 
        tupHdr.varColOffset[i] += elementsBufSize;
#else
        tupHdr.varColOffset[i] += offset2;
#endif        

    /*
     * Get the key descriptor for the ordered set element
     */
    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);
    nestedIndexFlag = LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc);

    if (orderedSetColHdr.nElements >= 0) { /* nested index does not exist */
        orderedSetColHdr.nElements += nElements;

        if (IS_LONG_ORDEREDSET(length + elementsBufSize) && nestedIndexFlag) {
            Four addedBytes;

            e = addedBytes = lrds_OrderedSet_CreateNestedIndex(handle,
                ornOrScanId, useScanFlag, tid,
                &tupHdr, start, constLength, &orderedSetColHdr, kdesc); 
            if (e < eNOERROR) ERR(handle, e);

            length += addedBytes;

            for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                tupHdr.varColOffset[i] += addedBytes + (HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX);
        }

    } else {
        /*
         * Nested index exist. Insert the keys of the appendted elements into the nested index.
         */
        offset = LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements); 
        ptr = elementsBuf;

        /* Get the key descriptor and key length */
        /* NOTICE: the key length is fixed for all elements */
        /* The line which gets 'kdesc' was moved upward. */
        keyLen = 0;
        for (i = 0; i < kdesc->nparts; i++) keyLen += kdesc->kpart[i].length;

        for (count = 0; count < nElements; count++) {

	    /* before allocating slot, check error situation */
	    /* 다음은 도서관 데이타베이스에서 발경된 문제를 임시로 해결하기 위한 코드로, 반드시 이런
	       현상을 재현하여 이를 고쳐야 한다. */
            /* Following codes are temporary for fixing a bug that happens in library database system. 
               Must reproduce the bug and fix it. */
	    if (orderedSetColHdr.freeSlotListHdr == UNUSED_SLOT || orderedSetColHdr.freeSlotListHdr >= 0)
	    {
                printf("fixing ordered set slot free list (nElements = %ld, nSlots = %ld, freeSlotListHdr = %ld)\n",
			orderedSetColHdr.nElements * (-1), orderedSetColHdr.nSlots, orderedSetColHdr.freeSlotListHdr);
		orderedSetColHdr.freeSlotListHdr = NO_MORE_SLOT;
	    }

            /* allocate a slot */
            if (orderedSetColHdr.freeSlotListHdr == NO_MORE_SLOT) {
                Four addedBytes;

                e = addedBytes = lrds_OrderedSet_AddSlots(handle, ornOrScanId, useScanFlag, tid, start, &orderedSetColHdr);
                if (e < eNOERROR) ERR(handle, e);

                for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                    tupHdr.varColOffset[i] += addedBytes;
            }

            e = lrds_OrderedSet_AllocateSlot(handle, ornOrScanId, useScanFlag, tid, start, &orderedSetColHdr, &slotNo);
            if (e < eNOERROR) ERR(handle, e);

            /* Write the slot value. */
            e = lrds_OrderedSet_WriteSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, offset);
            if (e < eNOERROR) ERR(handle, e);

            /* Get the element length. */
            memcpy(&elemLen, ptr, sizeof(OrderedSet_ElementLength));

            /* Get the key value from the element. */
            kval.len = keyLen;
            memcpy(kval.val, ptr+sizeof(OrderedSet_ElementLength), keyLen);

            /* Insert into the nested index the pointer to the newly inserted element */
            e = lrds_OrderedSet_InsertElementIntoNestedIndex(handle, &orderedSetColHdr.nestedIndexId,
                                                             kdesc, &kval, slotNo);
            if (e < eNOERROR) ERR(handle, e);

            offset += sizeof(OrderedSet_ElementLength); 
            ptr += elemLen + sizeof(OrderedSet_ElementLength);
        }
    }


    /* Write the updated column header. */
    orderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, start, oldOrderedSetColHdrSize,
                          (char*)&orderedSetColHdr, orderedSetColHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Write the updated tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                          tupHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_OrderedSet_AppendSortedElements() */



/*@================================
 * LRDS_OrderedSet_InsertElement()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_InsertElement(Four, Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Insert an element.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_InsertElement(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the posting */
    Four colNo,			/* IN column on which the posting is defined */
    char *element,		/* IN element to insert */
    LockParameter *lockupPtr)      /* IN lock parameter */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four constLength, length;	/* length of the set column */ 
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    KeyValue kval;		/* key value of an index entry */
    KeyDesc *kdesc;             /* pointer to the key descriptor for element */
    unsigned char *nullVector;	/* bit array of null flags */
    OrderedSetColHdr_T orderedSetColHdr; /* ordered set column header */
    OrderedSet_ElementLength elemLen; /* element length */
    OrderedSet_ElementLength elemLenWithLengthField; /* element length including length field size */
    Four oldOrderedSetColHdrSize; /* old header size of ordered set column */
    Four orderedSetColHdrSize;  /* header size of ordered set column */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Boolean nestedIndexFlag;    /* TRUE if nested index is allowed */
    Four keyLen;                /* length of element key */
    Four i;
    OrderedSet_ElementOffset_T offset; /* offset to the element */
    Four offsetFromColStart;    /* offset of an element from column start */
    Boolean membershipFlag;     /* TRUE if the element exist */
    Four slotNo;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    char          buffer[sizeof(OrderedSet_ElementLength)]; 
    Four          elementStartOffset; 
    Four          elementOrderedOffset; 
    Four          elementOffset; 
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four                docId, docIdGap;
    Four                prevDocId, nextDocId, nextDocIdGap;
    Four                nextDocIdGapSize;
    char                tempData[sizeof(Four)+2];
    char                encodedData[sizeof(Four)+2];
    Four                encodedDataLength;
    Four                deltaOfUpdate;                          
#endif

    
    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_InsertElement()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (element == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }


    /* Is there any opened scan on the given ordered set? */
    for (i = 0; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++) {
	if (LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

	if (EQUAL_TUPLEID(LRDS_ORDEREDSETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_ORDEREDSETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    ERR(handle, eORDEREDSET_OPENSCAN_LRDS);
	}
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    constLength = length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* save the old ordered set column header length because an ordered set
       can have nested index during this operation */
    oldOrderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);


    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);
    nestedIndexFlag = LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc);

    /* Get the key value from the element. */
    keyLen = 0;
    for (i = 0; i < kdesc->nparts; i++) keyLen += kdesc->kpart[i].length;
    kval.len = keyLen;
    memcpy(kval.val, (char*)element+sizeof(OrderedSet_ElementLength), keyLen);

    /* get the element length. */
    memcpy(&elemLen, element, sizeof(OrderedSet_ElementLength));
    elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

    if (orderedSetColHdr.nElements >= 0) { /* nested index does not exist */
        e = lrds_OrderedSet_FindElementUsingSearch(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, &kval, &offsetFromColStart, &membershipFlag);
#else
            kdesc, &kval, &offsetFromColStart, &membershipFlag, &prevDocId, &nextDocId, &nextDocIdGapSize);
#endif  

        if (e < eNOERROR) ERR(handle, e);

        assert(membershipFlag == FALSE);

        /* Write the element. */
        if (offsetFromColStart >= HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX +
                                  LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements)) {
            elementStartOffset = constLength;
        }
	else {
            e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                             start+offsetFromColStart, sizeof(OrderedSet_ElementLength), buffer,
                                             &orderedSetColHdr, start, constLength, &elementStartOffset);
            if (e < eNOERROR) ERR(handle, e);
	}

        elementOrderedOffset = elementStartOffset - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX -
                               LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements);

        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                              start+offsetFromColStart, 0, &elementOrderedOffset, sizeof(OrderedSet_ElementLength), NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
        
#ifndef COMPRESSION 
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementStartOffset+sizeof(OrderedSet_ElementLength), 0, 
                              &element[sizeof(OrderedSet_ElementLength)], elemLen, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* Update the tuple header. */
        for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
            tupHdr.varColOffset[i] += elemLenWithLengthField;

        /* Increase the # of elements */
        orderedSetColHdr.nElements ++;

        e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr,
                                                       start, offsetFromColStart+sizeof(OrderedSet_ElementLength)
                                                                                -HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX, elemLen);
        if (e < eNOERROR) ERR(handle, e);

#else                                  
        memcpy(&docId, kval.val, sizeof(docId));
    
        docIdGap = docId - prevDocId;
        LRDS_VARIABLE_BYTE_ENCODING(docIdGap, tempData, encodedData, encodedDataLength);
        
        elemLen = elemLen - sizeof(docId) + encodedDataLength;
        elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementStartOffset+sizeof(OrderedSet_ElementLength), 0, 
                              encodedData, encodedDataLength, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementStartOffset+sizeof(OrderedSet_ElementLength)+encodedDataLength, 0, 
                              &element[sizeof(OrderedSet_ElementLength)+sizeof(docId)], elemLen-encodedDataLength, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);                                      

        /* update next docid gap */        
        if(nextDocId != NIL)
        {
            nextDocIdGap = nextDocId - docId;
            LRDS_VARIABLE_BYTE_ENCODING(nextDocIdGap, tempData, encodedData, encodedDataLength);
            
            e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementStartOffset+sizeof(OrderedSet_ElementLength)+elemLen, nextDocIdGapSize, 
                              encodedData, encodedDataLength, NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);

            deltaOfUpdate = encodedDataLength - nextDocIdGapSize;
        }
        else
        {
            orderedSetColHdr.lastDocId = docId;
            deltaOfUpdate = 0;
        }

        /* Update the tuple header. */
        for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
            tupHdr.varColOffset[i] += elemLenWithLengthField + deltaOfUpdate;

        /* Increase the # of elements */
        orderedSetColHdr.nElements ++;
 
        e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr, 
                                                       start, offsetFromColStart+sizeof(OrderedSet_ElementLength)
                                                                                -HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX, elemLen);
        if (e < eNOERROR) ERR(handle, e);

        /* update next element's order offset */        
        if(deltaOfUpdate != 0)
        {
            e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr, 
                                                           start, offsetFromColStart+2*sizeof(OrderedSet_ElementLength)
                                                                                    -HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX, deltaOfUpdate);
            if (e < eNOERROR) ERR(handle, e);
        }
#endif            

        if (IS_LONG_ORDEREDSET(length + elemLenWithLengthField) && nestedIndexFlag) {
            Four addedBytes;

            e = addedBytes = lrds_OrderedSet_CreateNestedIndex(handle,
                ornOrScanId, useScanFlag, tid,
                &tupHdr, start, constLength, &orderedSetColHdr, kdesc); 
            if (e < eNOERROR) ERR(handle, e);

            length += addedBytes;

            for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                tupHdr.varColOffset[i] += addedBytes + (HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX);
        }

    } else {                    /* nested index is defined */
        e = lrds_OrderedSet_FindElementUsingNestedIndex(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, &kval, &slotNo, &membershipFlag);
#else
            kdesc, &kval, &slotNo, &membershipFlag, &nextDocId);
#endif            
        if (e < eNOERROR) ERR(handle, e);

        assert(membershipFlag == FALSE);

        if (slotNo == -1) {     /* this element has the largest key value */
            /* this elements should be located at the end */
            offset = LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements);
            elementStartOffset = constLength - LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr);
        } else {
            /* Read the slot value. */
            e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, &offset);
            if (e < eNOERROR) ERR(handle, e);

            e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                             start+LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(orderedSetColHdr)+offset,
                                             sizeof(OrderedSet_ElementLength), buffer,
                                             &orderedSetColHdr, start, constLength, &elementStartOffset);
            if (e < eNOERROR) ERR(handle, e);
            elementStartOffset -= LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr);

            /* Increase the slot values of the slots which will be located after the newly inserted element. */
            e = lrds_OrderedSet_IncreaseSlotValues(handle, ornOrScanId, useScanFlag, tid, start,
                                                   orderedSetColHdr.nSlots, offset,
                                                   sizeof(OrderedSet_ElementLength)); 
            if (e < eNOERROR) ERR(handle, e);
        }

	/* before allocating slot, check error situation */
	/* 다음은 도서관 데이타베이스에서 발경된 문제를 임시로 해결하기 위한 코드로, 반드시 이런
	   현상을 재현하여 이를 고쳐야 한다. */
        /* Following codes are temporary for fixing a bug that happens in library database system. 
           Must reproduce the bug and fix it. */
	if (orderedSetColHdr.freeSlotListHdr == UNUSED_SLOT || orderedSetColHdr.freeSlotListHdr >= 0)
	{
	    printf("fixing ordered set slot free list (nElements = %ld, nSlots = %ld, freeSlotListHdr = %ld)\n",
		    orderedSetColHdr.nElements * (-1), orderedSetColHdr.nSlots, orderedSetColHdr.freeSlotListHdr);
	    orderedSetColHdr.freeSlotListHdr = NO_MORE_SLOT;
	}

	/* allocate a slot */
        if (orderedSetColHdr.freeSlotListHdr == NO_MORE_SLOT) {
            Four addedBytes;

            e = addedBytes = lrds_OrderedSet_AddSlots(handle, ornOrScanId, useScanFlag, tid, start, &orderedSetColHdr);
            if (e < eNOERROR) ERR(handle, e);

            for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                tupHdr.varColOffset[i] += addedBytes;
        }

        e = lrds_OrderedSet_AllocateSlot(handle, ornOrScanId, useScanFlag, tid, start, &orderedSetColHdr, &slotNo);
        if (e < eNOERROR) ERR(handle, e);

        /* Write the slot value. */
        e = lrds_OrderedSet_WriteSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, offset);
        if (e < eNOERROR) ERR(handle, e);

        /* Now 'offsetFromColStart' is distance from the begining of column. */
        offsetFromColStart = offset + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
            LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);

        /* Write the element. */ /* CAUTION: orderedSetColHdr.nElements is already increased */
        elementOffset = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr) + elementStartOffset;

        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, start+offsetFromColStart, 0,
                              &elementStartOffset, sizeof(OrderedSet_ElementLength), NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

#ifndef COMPRESSION 
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, start+elementOffset, 0,
                              &element[sizeof(OrderedSet_ElementLength)], elemLen, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr,
                                                       start, offset+sizeof(OrderedSet_ElementLength), elemLen);
        if (e < eNOERROR) ERR(handle, e);
#else
        /* get nextDocIdGap */
        if(nextDocId != NIL)
        {
            e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                (ObjectID*)tid, start+elementOffset, sizeof(encodedData),
                                encodedData, NULL, NULL);        
            if (e < eNOERROR) ERR(handle, e);    
            
            nextDocIdGapSize = 0;                     
            LRDS_VARIABLE_BYTE_DECODING(encodedData, nextDocIdGapSize, nextDocIdGap);        
            
            /* get prevDocId */
            prevDocId = nextDocId - nextDocIdGap;
        }
        else
        {
            prevDocId = orderedSetColHdr.lastDocId;
        }
        
        memcpy(&docId, kval.val, sizeof(docId));
        docIdGap = docId - prevDocId;
        
        LRDS_VARIABLE_BYTE_ENCODING(docIdGap, tempData, encodedData, encodedDataLength);
        
        elemLen = elemLen - sizeof(docId) + encodedDataLength;
        elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementOffset, 0, 
                              encodedData, encodedDataLength, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementOffset+encodedDataLength, 0, 
                              &element[sizeof(OrderedSet_ElementLength)+sizeof(docId)], elemLen-encodedDataLength, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);                                      

        /* update next docid gap */        
        if(nextDocId != NIL)
        {
            nextDocIdGap = nextDocId - docId;
            LRDS_VARIABLE_BYTE_ENCODING(nextDocIdGap, tempData, encodedData, encodedDataLength);
            
            e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementOffset+elemLen, nextDocIdGapSize, 
                              encodedData, encodedDataLength, NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);

            deltaOfUpdate = encodedDataLength - nextDocIdGapSize;
        }
        else
        {
            orderedSetColHdr.lastDocId = docId;
            deltaOfUpdate = 0;
        }        

        e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr,
                                                       start, offset+sizeof(OrderedSet_ElementLength), elemLen);
        if (e < eNOERROR) ERR(handle, e);

        if(deltaOfUpdate != 0)
        {
            e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr,
                                                           start, offset+2*sizeof(OrderedSet_ElementLength), deltaOfUpdate);
            if (e < eNOERROR) ERR(handle, e);
        }
#endif

        /* Insert into the nested index the pointer to the newly inserted element */
        e = lrds_OrderedSet_InsertElementIntoNestedIndex(handle, &orderedSetColHdr.nestedIndexId,
                                                         kdesc, &kval, slotNo);
        if (e < eNOERROR) ERR(handle, e);

        /* Update the tuple header. */
        for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
#ifndef COMPRESSION 
            tupHdr.varColOffset[i] += elemLenWithLengthField;
#else
            tupHdr.varColOffset[i] += elemLenWithLengthField + deltaOfUpdate;
#endif
    }

    /* Write the updated column header. */
    orderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, start, oldOrderedSetColHdrSize,
                          (char*)&orderedSetColHdr, orderedSetColHdrSize,
                          NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Write the updated tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                          tupHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_OrderedSet_InsertElement() */



/*@================================
 * LRDS_OrderedSet_DeleteElement()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_DeleteElement(Four, Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Delete an element.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_DeleteElement(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the posting */
    Four colNo,			/* IN column on which the posting is defined */
    KeyValue *kval,             /* IN key of the deleted element */
    LockParameter *lockupPtr)      /* IN lock parameter */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    KeyDesc *kdesc;             /* pointer to the key descriptor for element */
    unsigned char *nullVector;	/* bit array of null flags */
    OrderedSetColHdr_T orderedSetColHdr; /* ordered set column header */
    OrderedSet_ElementLength elemLen; /* element length */
    OrderedSet_ElementLength elemLenWithLengthField; /* element length including length field size */
    Four oldOrderedSetColHdrSize; /* old header size of ordered set column */
    Four orderedSetColHdrSize;  /* header size of ordered set column */
    OrderedSet_ElementOffset_T offset;
    Four offsetFromColStart;    /* offset of an element from column start */
    Boolean membershipFlag;
    Four slotNo;
    Four i;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    Four	  elementOrderedOffset; 
    Four          elementStartOffset; 
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four        prevDocId; 
    Four        nextDocId, nextDocIdGap, nextDocIdGapSize;
    Four        docId, docIdGap, docIdGapSize;
    char        tempData[sizeof(Four)+2];
    char        encodedData[sizeof(Four)+2];
    Four        encodedDataLength;
    Four        deltaOfUpdate;                          
#endif
    

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_DeleteElement()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (kval == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }


    /* Is there any opened scan on the given ordered set? */
    for (i = 0; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++) {
	if (LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

	if (EQUAL_TUPLEID(LRDS_ORDEREDSETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_ORDEREDSETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    ERR(handle, eORDEREDSET_OPENSCAN_LRDS);
	}
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* save the old ordered set column header length because an ordered set
       can destroy nested index during this operation */
    oldOrderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);

    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);

    if (orderedSetColHdr.nElements >= 0) { /* nested index does not exist */
        e = lrds_OrderedSet_FindElementUsingSearch(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, kval, &offsetFromColStart, &membershipFlag);
#else
            kdesc, kval, &offsetFromColStart, &membershipFlag, &prevDocId, &nextDocId, &nextDocIdGapSize);
#endif
        if (e < eNOERROR) ERR(handle, e);

        if (membershipFlag == FALSE) ERR(handle, eORDEREDSET_ELEMENTNOTFOUND_LRDS);

        /* Get the element length. */
        elementOrderedOffset = offsetFromColStart - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

        e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                         start+offsetFromColStart, sizeof(OrderedSet_ElementLength), (char*)&elemLen,
                                         &orderedSetColHdr, start, length, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

        /* Delete the # of elements. */
        orderedSetColHdr.nElements --;

    } else {                    /* nested index is defined */
        e = lrds_OrderedSet_FindElementUsingNestedIndex(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, kval, &slotNo, &membershipFlag);
#else
            kdesc, kval, &slotNo, &membershipFlag, &nextDocId);
#endif    
        if (e < eNOERROR) ERR(handle, e);

        if (membershipFlag == FALSE) ERR(handle, eORDEREDSET_ELEMENTNOTFOUND_LRDS);

        /* Read the slot value. */
        e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, &offset);
        if (e < eNOERROR) ERR(handle, e);

        /* 'offsetFromColStart' is distance from the begining of column. */
        offsetFromColStart = offset + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
            LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);

        /* Get the element length. */
        elementOrderedOffset = offset;

	e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
					 start+offsetFromColStart, sizeof(OrderedSet_ElementLength), (char*)&elemLen,
					 &orderedSetColHdr, start, length, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

        if (orderedSetColHdr.nElements == -1) { /* When delete the last memember */
            Four deletedBytes;

            /* destroy the nested index */
            e = deletedBytes = lrds_OrderedSet_DestroyNestedIndex(handle,
                ornOrScanId, useScanFlag, tid,
                &tupHdr, start, &orderedSetColHdr, TRUE);
            if (e < eNOERROR) ERR(handle, e);

            /* 'offsetFromColStart' is distance from the begining of column. */
            offsetFromColStart = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX;

            /* elementStartOffset is distance from the begining of column. */
            elementStartOffset = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX + sizeof(OrderedSet_ElementLength); 

            /* Delete the # of elements. */
            orderedSetColHdr.nElements --;

            for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                tupHdr.varColOffset[i] -= deletedBytes + (HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX);

        } else {
            /* Deallocate the slot. */
            e = lrds_OrderedSet_DeallocateSlot(handle, ornOrScanId, useScanFlag, tid, start, &orderedSetColHdr, slotNo);
            if (e < eNOERROR) ERR(handle, e);

            /* Decrease the slot values of the slots which are corresponding */
            /* to elements located after the deleted inserted element. */
            e = lrds_OrderedSet_DecreaseSlotValues(handle, ornOrScanId, useScanFlag, tid, start,
                                                   orderedSetColHdr.nSlots, offset,
                                                   sizeof(OrderedSet_ElementLength));
            if (e < eNOERROR) ERR(handle, e);

            /* Insert into the nested index the pointer to the newly inserted element */
            e = lrds_OrderedSet_DeleteElementFromNestedIndex(handle, &orderedSetColHdr.nestedIndexId,
                                                             kdesc, kval, slotNo);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    /* Delete the element. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
			  start+offsetFromColStart, sizeof(OrderedSet_ElementLength), NULL, 0, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

#ifdef COMPRESSION  
    memcpy(&docId, kval->val, kval->len);
    /* fetch current docid gap */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start+elementStartOffset-sizeof(OrderedSet_ElementLength),
                         sizeof(encodedData), encodedData, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* decode current docid gap */
    docIdGapSize = 0;                     
    LRDS_VARIABLE_BYTE_DECODING(encodedData, docIdGapSize, docIdGap);        
    
    if(docId < orderedSetColHdr.lastDocId)
    {
        /* fetch next docid gap */
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                             (ObjectID*)tid, start+elementStartOffset+elemLen-sizeof(OrderedSet_ElementLength),
                             sizeof(encodedData), encodedData, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* decode next docid gap */
        nextDocIdGapSize = 0;                     
        LRDS_VARIABLE_BYTE_DECODING(encodedData, nextDocIdGapSize, nextDocIdGap);        

        /* update next docid gap */
        nextDocIdGap += docIdGap;

        /* encode next docid gap */
        LRDS_VARIABLE_BYTE_ENCODING(nextDocIdGap, tempData, encodedData, encodedDataLength);

        deltaOfUpdate = encodedDataLength - nextDocIdGapSize;

        /* write next docid gap */
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                              start+elementStartOffset+elemLen-sizeof(OrderedSet_ElementLength), nextDocIdGapSize, 
                              encodedData, encodedDataLength, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
    }
    else
    {
        deltaOfUpdate = 0;
    }
#endif

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
			  start+elementStartOffset-sizeof(OrderedSet_ElementLength), elemLen, NULL, 0, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr, start,
						   elementOrderedOffset, -1*elemLen);
    if (e < eNOERROR) ERR(handle, e);

#ifdef COMPRESSION 
    if(docId < orderedSetColHdr.lastDocId)
    {
        if(deltaOfUpdate != 0)
        {
            e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr, start, 
                                                           elementOrderedOffset+sizeof(OrderedSet_ElementLength), deltaOfUpdate);
            if (e < eNOERROR) ERR(handle, e);
        }
    }
    else if(docId == orderedSetColHdr.lastDocId)
    {
        prevDocId = docId - docIdGap;
        orderedSetColHdr.lastDocId = prevDocId;
    }
    else
    {
        ERR(handle, eINTERNAL);
    }
#endif

    /* Update the tuple header. */
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
#ifndef COMPRESSION 
        tupHdr.varColOffset[i] -= elemLenWithLengthField;
#else
        tupHdr.varColOffset[i] -= elemLenWithLengthField - deltaOfUpdate;
#endif

    /* Write the updated column header. */
    orderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, start, oldOrderedSetColHdrSize,
                          (char*)&orderedSetColHdr, orderedSetColHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Write the updated tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                          tupHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_OrderedSet_DeleteElement() */

/*@================================
 * LRDS_OrderedSet_UpdateElement()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_UpdateElement(Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Delete an element.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_UpdateElement(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the posting */
    Four colNo,			/* IN column on which the posting is defined */
    KeyValue *kval,             /* IN key of the deleted element */
    Four updateStart,		/* IN starting offset within the element to be updated */
    Four updateLength,		/* IN length of the updated part */
    Four updateDataLength,	/* IN length of the data */
    void* updateData,		/* IN data */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    KeyDesc *kdesc;             /* pointer to the key descriptor for element */
    Four keyLen;
    unsigned char *nullVector;	/* bit array of null flags */
    OrderedSetColHdr_T orderedSetColHdr; /* ordered set column header */
    OrderedSet_ElementLength elemLen; /* element length */
    OrderedSet_ElementLength elemLenWithLengthField; /* element length including length field size */
    Four oldOrderedSetColHdrSize; /* old header size of ordered set column */
    Four orderedSetColHdrSize;  /* header size of ordered set column */
    OrderedSet_ElementOffset_T offset;
    Four offsetFromColStart;    /* offset of an element from column start */
    Boolean membershipFlag;
    Four slotNo;
    Four i;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    Four            elementOrderedOffset;
    Four            elementStartOffset;
    Four            newElementOrderdOffset;
    Four            sizeOfOrderedSetElementLength;
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    char    encodedData[sizeof(Four)+2];
    Four    prevDocId;
    Four    docId;
    Four    docIdGapSize = 0;
#endif       


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_UpdateElement()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (kval == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* save the old ordered set column header length because an ordered set
       can destroy nested index during this operation */
    oldOrderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);

    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);

    /* update하고자 하는 부분이 element length 또는 key part인가를 검사한다. 
       만약 element length 또는 key part를 update하고자 한다면, BAD PARAMETER 에러를 반환한다.
       참고로 element를 update하면 updateData의 length에 따라 element length가 자동으로 바뀌므로 
       사용자가 일부러 바꿔줄 필요가 없다.
       ordered set의 key part는 element중에서 앞의 4 byte(=sizeof(OrderedSet_ElementLength))를 제외한 그 뒤 부분 부터,
       key length만큼을 사용한다. 즉, 길이는 가변이고 위치는 고정이다.
    */
    /* Check whether the part to update is key part or not. If try to update key part, return BAD PARAMETER.
       If update the elment, the element length is updated automaticaly according to the length of the updateData.
       So a user need not to update that expliciticly.
       The key part of ordered set start after first 4 bytes(=sizeof(OrderedSet_ElementLength)) of the element, and
       the length of the key part is 'key length'. That is, position of the key is fixed, but the length is variable.
    */
    keyLen = 0;
    for (i = 0; i < kdesc->nparts; i++) keyLen += kdesc->kpart[i].length;
    if(updateStart < sizeof(OrderedSet_ElementLength) + keyLen) ERR(handle, eBADPARAMETER); 

    if (orderedSetColHdr.nElements >= 0) { /* nested index does not exist */
        e = lrds_OrderedSet_FindElementUsingSearch(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, kval, &offsetFromColStart, &membershipFlag);
#else
            kdesc, kval, &offsetFromColStart, &membershipFlag, &prevDocId, &docId, &docIdGapSize);
#endif

        if (e < eNOERROR) ERR(handle, e);

        if (membershipFlag == FALSE) ERR(handle, eORDEREDSET_ELEMENTNOTFOUND_LRDS);

        /* Get the element length. */
        e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                         start+offsetFromColStart, sizeof(OrderedSet_ElementLength), (char*)&elemLen,
                                         &orderedSetColHdr, start, length, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);
        elementOrderedOffset = offsetFromColStart - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

    } else {                    /* nested index is defined */
        e = lrds_OrderedSet_FindElementUsingNestedIndex(handle,
            ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
            kdesc, kval, &slotNo, &membershipFlag);
#else
            kdesc, kval, &slotNo, &membershipFlag, &docId);
#endif            
      
        if (e < eNOERROR) ERR(handle, e);

        if (membershipFlag == FALSE) ERR(handle, eORDEREDSET_ELEMENTNOTFOUND_LRDS);

        /* Read the slot value. */
        e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, &offset);
        if (e < eNOERROR) ERR(handle, e);

        /* 'offsetFromColStart' is distance from the begining of column. */
        offsetFromColStart = offset + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
            LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);

        /* Get the element length. */
        e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                         start+offsetFromColStart, sizeof(OrderedSet_ElementLength), (char*)&elemLen,
                                         &orderedSetColHdr, start, length, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);
        elementOrderedOffset = offset;
    }

    updateStart -= sizeof(OrderedSet_ElementLength); 
#ifdef COMPRESSION 
    if(docIdGapSize == 0)
    {
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, 
                             (ObjectID*)tid, start+elementStartOffset, 
                             sizeof(Four)+2, encodedData, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);        
        LRDS_VARIABLE_BYTE_DECODING(encodedData, docIdGapSize, docId);
    }
    updateStart -= sizeof(docId) - docIdGapSize;
#endif  
    
    if (updateLength != updateDataLength) {
        e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, (ObjectID*)tid, &orderedSetColHdr,
                                                       start, elementOrderedOffset, updateDataLength - updateLength);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* Update the element. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, start+elementStartOffset+updateStart, 
                          updateLength, updateData, updateDataLength, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Update the tuple header. */
    if(updateDataLength != updateLength)
    {
	for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
	    tupHdr.varColOffset[i] += updateDataLength - updateLength;

	/* Write the updated tuple header. */
	/* NOTICE: We can reduce the amount of data overwritten. */
	e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
			      (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
			      tupHdrSize, NULL, NULL);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_OrderedSet_UpdateElement() */

/*@================================
 * deletedElementPosCmpFunc()
 *================================*/
/*
 * Function: Four deletedElementPosCmpFunc(void* x, void* y)
 *
 * Description:
 *  compare function used for sorting elementsPosArray
 *
 * Returns:
 *  error code
 */
static int deletedElementPosCmpFunc(const void* x, const void* y)
{
    Four* xoffset = (Four*)x;
    Four* yoffset = (Four*)y;

    if (*yoffset > *xoffset)
        return 1;
    else if (*yoffset < *xoffset)
        return -1;
    else
        return 0;
}

/*@================================
 * LRDS_OrderedSet_DeleteElement()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_DeleteElementsWithNestedIndex(Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Delete elements using nested index
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_DeleteElementsWithNestedIndex(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the posting */
    Four colNo,			/* IN column on which the posting is defined */
    Four nElementsToDelete,	/* IN number of elements to be deleted */
    KeyValue *kval,             /* IN array of keys of the deleted elements */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    KeyDesc *kdesc;             /* pointer to the key descriptor for element */
    unsigned char *nullVector;	/* bit array of null flags */
    OrderedSetColHdr_T orderedSetColHdr; /* ordered set column header */
    OrderedSet_ElementLength elemLen; /* element length */
    OrderedSet_ElementLength elemLenWithLengthField; /* element length including length field size */
    Four oldOrderedSetColHdrSize; /* old header size of ordered set column */
    Four orderedSetColHdrSize;  /* header size of ordered set column */
    OrderedSet_ElementOffset_T offset;
    Four offsetFromColStart;    /* offset of an element from column start */
    Boolean membershipFlag;
    Four slotNo;
    Four i, j;
    Four totalDeletedElementsLength;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    struct {
	Four offset;
	Four offsetFromColStart;
        Four elementStartOffset; 
	Four elemLenWithLengthField;
	Four slotNo;
	Four kvalIndex;
#ifdef COMPRESSION 
        Four docId;
        Four docIdGap;
        Four deltaOfUpdate;
#endif
    } deletedElementPosArray[DELETED_ELEMENT_POS_ARRAY_SIZE];
    Four slotArraySize;
    Four slotOffset;
    Four bytesToRead;
    OrderedSet_ElementOffset_T buf[PAGESIZE/sizeof(OrderedSet_ElementOffset_T)];
    Boolean updateFlag;         /* TRUE if slot values are updated */
    Four kvalIndex;
    Four            elementStartOffset;
    Four	    elementOrderedOffset;
    Four            elementOrderedArraySize;
    Four            prevFirstElementStartOffset; 
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four        prevDocId; 
    Four        nextDocId, nextDocIdGap, nextDocIdGapSize;
    Four        docId;
    Four        docIdGap, docIdGapSize;
    char        tempData[sizeof(Four)+2];
    char        encodedData[sizeof(Four)+2];
    Four        encodedDataLength;
    Four        deltaOfUpdate;                          
    Four        nOrderedSetElements;
#endif
    

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_DeleteElementsWithNestedIndex()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    if(nElementsToDelete > DELETED_ELEMENT_POS_ARRAY_SIZE) ERR(handle, eBADPARAMETER);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (kval == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /* Is there any opened scan on the given ordered set? */
    for (i = 0; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++) {
	if (LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

	if (EQUAL_TUPLEID(LRDS_ORDEREDSETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_ORDEREDSETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    ERR(handle, eORDEREDSET_OPENSCAN_LRDS);
	}
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (BITTEST(nullVector, colNo)) ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* save the old ordered set column header length because an ordered set
       can destroy nested index during this operation */
    oldOrderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);

    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);

    if (orderedSetColHdr.nElements >= 0)
    {
	/* nested index does not exist */
	ERR(handle, eBADPARAMETER);
    }
    else
    {
	/* nested index is defined */
	/* collects (offset, size) of elements to be deleted and keep them into deletedElementPosArray */
	for(i = 0, j = 0; i < nElementsToDelete; i++)
	{
	    e = lrds_OrderedSet_FindElementUsingNestedIndex(handle,
		ornOrScanId, useScanFlag, tid, start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
		kdesc, &kval[i], &slotNo, &membershipFlag); 
#else
        kdesc, &kval[i], &slotNo, &membershipFlag, &docId);        
#endif  

	    if (e < eNOERROR) ERR(handle, e);

            if (membershipFlag == FALSE)
		continue;

	    /* Read the slot value. */
	    e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, &offset);
	    if (e < eNOERROR) ERR(handle, e);

	    /* 'offsetFromColStart' is distance from the begining of column. */
	    offsetFromColStart = offset + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
		LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);

	    /* Get the element length. */
            e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                             start+offsetFromColStart, sizeof(OrderedSet_ElementLength), (char*)&elemLen,
                                             &orderedSetColHdr, start, length, &elementStartOffset);
	    if (e < eNOERROR) ERR(handle, e);

#ifdef COMPRESSION 
            /* fetch docid gap */
            e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                                start+elementStartOffset, sizeof(encodedData), encodedData, NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);
            
            /* decode docid gap */
            docIdGapSize = 0;                     
            LRDS_VARIABLE_BYTE_DECODING(encodedData, docIdGapSize, docIdGap);        
#endif

    	    elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

	    deletedElementPosArray[j].offset                 = offset;
	    deletedElementPosArray[j].offsetFromColStart     = offsetFromColStart;
	    deletedElementPosArray[j].elementStartOffset     = elementStartOffset;
	    deletedElementPosArray[j].elemLenWithLengthField = elemLenWithLengthField;
	    deletedElementPosArray[j].slotNo                 = slotNo;
	    deletedElementPosArray[j].kvalIndex              = i; 
#ifdef COMPRESSION 
            deletedElementPosArray[j].docId                  = docId;
            deletedElementPosArray[j].docIdGap               = docIdGap;
#endif
	    j++;
	}

	/* re-set nElementsToDeleted */
	nElementsToDelete = j;

	/* sort deletedElementPosArray by elementsPosArray[i].offset with decreasing order */
	qsort(deletedElementPosArray, nElementsToDelete, sizeof(deletedElementPosArray[0]), deletedElementPosCmpFunc);

	/* delete elements */
	if(nElementsToDelete == (orderedSetColHdr.nElements) * (-1))
	{
	    Four deletedBytes;

	    /* When delete the whole memember */
            /* destroy the nested index */
            e = lrds_OrderedSet_DestroyNestedIndex(handle,
                ornOrScanId, useScanFlag, tid,
                &tupHdr, start, &orderedSetColHdr, TRUE);
            if (e < eNOERROR) ERR(handle, e);

            /* 'offsetFromColStart' is distance from the begining of column. */
            offsetFromColStart = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX;

            /* Delete the # of elements. */
            orderedSetColHdr.nElements = 0;

	    /* Delete whole elements. */
	    deletedBytes = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr) - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

	    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
				  (ObjectID*)tid, start+HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX,
				  deletedBytes, NULL, 0, NULL, NULL);
	    if (e < eNOERROR) ERR(handle, e);

   	    /* Write the updated column header. */
	    orderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);
	    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
				  (ObjectID*)tid, start, orderedSetColHdrSize,
				  (char*)&orderedSetColHdr, orderedSetColHdrSize, NULL, NULL);
	    if (e < eNOERROR) ERR(handle, e);

	    /* Update the tuple header. */
            for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                tupHdr.varColOffset[i] -= deletedBytes;
        }
	else
	{
	    for(totalDeletedElementsLength = 0, i = 0; i < nElementsToDelete; i++)
	    {
		offsetFromColStart     = deletedElementPosArray[i].offsetFromColStart;
		elemLenWithLengthField = deletedElementPosArray[i].elemLenWithLengthField;
		slotNo		       = deletedElementPosArray[i].slotNo;
		kvalIndex	       = deletedElementPosArray[i].kvalIndex;
                elementStartOffset     = deletedElementPosArray[i].elementStartOffset; 
#ifdef COMPRESSION 
                docId                  = deletedElementPosArray[i].docId;
                docIdGap               = deletedElementPosArray[i].docIdGap;

                if(docId < orderedSetColHdr.lastDocId)
                {                
                    /* fetch next docid gap */
                    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                                        start+elementStartOffset+elemLenWithLengthField-sizeof(OrderedSet_ElementLength), 
                                        sizeof(encodedData), encodedData, NULL, NULL);
                    if (e < eNOERROR) ERR(handle, e);
            
                    /* decode next docid gap */
                    nextDocIdGapSize = 0;                     
                    LRDS_VARIABLE_BYTE_DECODING(encodedData, nextDocIdGapSize, nextDocIdGap);        
            
                    /* update next docid gap */
                    nextDocIdGap += docIdGap;
            
                    /* encode next docid gap */
                    LRDS_VARIABLE_BYTE_ENCODING(nextDocIdGap, tempData, encodedData, encodedDataLength);
            
                    deltaOfUpdate = encodedDataLength - nextDocIdGapSize;

                    /* write next docid gap */
                    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                                          start+elementStartOffset+elemLenWithLengthField-sizeof(OrderedSet_ElementLength), 
                                          nextDocIdGapSize, encodedData, encodedDataLength, NULL, NULL);
                    if (e < eNOERROR) ERR(handle, e);
                }
                else if(docId == orderedSetColHdr.lastDocId)
                {
                    prevDocId = docId - docIdGap;
                    orderedSetColHdr.lastDocId = prevDocId;
                    deltaOfUpdate = 0;
                }
                else
                {
                    ERR(handle, eINTERNAL);
                }
                deletedElementPosArray[i].deltaOfUpdate = deltaOfUpdate;
#endif                    
                                
		/* Delete the element. */
                e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                                      start+elementStartOffset, elemLenWithLengthField-sizeof(OrderedSet_ElementLength),
                                      NULL, 0, NULL, NULL);
		if (e < eNOERROR) ERR(handle, e);

		/* Deallocate the slot. */
		e = lrds_OrderedSet_DeallocateSlot(handle, ornOrScanId, useScanFlag, tid, start, &orderedSetColHdr, slotNo);
		if (e < eNOERROR) ERR(handle, e);

		/* delete from the nested index  */
		e = lrds_OrderedSet_DeleteElementFromNestedIndex(handle, &orderedSetColHdr.nestedIndexId, kdesc,
		                                                 &kval[kvalIndex], slotNo);
		if (e < eNOERROR) ERR(handle, e);

#ifndef COMPRESSION 
		totalDeletedElementsLength += elemLenWithLengthField;
#else
                totalDeletedElementsLength += (elemLenWithLengthField - deltaOfUpdate);
#endif                    
	    }

            for(i = 0; i < nElementsToDelete; i++)
            {
                offsetFromColStart     = deletedElementPosArray[i].offsetFromColStart;
                elemLenWithLengthField = deletedElementPosArray[i].elemLenWithLengthField;
                slotNo                 = deletedElementPosArray[i].slotNo;
                kvalIndex              = deletedElementPosArray[i].kvalIndex;
                elementStartOffset     = deletedElementPosArray[i].elementStartOffset; 

                /* Delete the element ordered offset. */
                e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                                      start+offsetFromColStart, sizeof(OrderedSet_ElementLength), NULL, 0,
                                      NULL, NULL);
                if (e < eNOERROR) ERR(handle, e);

            }

	    /* Decrease the slot values of the slots which are corresponding */
	    /* to elements located after the deleted element. */
	    slotOffset    = LRDS_ORDEREDSET_ITH_SLOT_OFFSET(start, 0);
	    slotArraySize = LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);

	    while (slotArraySize > 0)
	    {
		Four nElementsToModify;

		bytesToRead = MIN(PAGESIZE,slotArraySize);

		e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
				     (ObjectID*)tid, slotOffset, bytesToRead,
				     (char*)buf, NULL, NULL);
		if (e < eNOERROR) ERR(handle, e);

		updateFlag = FALSE;
		nElementsToModify = bytesToRead/sizeof(OrderedSet_ElementOffset_T);
		for (i = 0; i < nElementsToModify; i++)
		{
		    for(j = 0; j < nElementsToDelete; j++)
		    {
			if (buf[i] >=0 && buf[i] >= deletedElementPosArray[j].offset)
			{
			    buf[i] -= sizeof(OrderedSet_ElementLength); 
	    		    updateFlag = TRUE;
			}
		    }
		}

		/* If there is no update in slot values, we don't have to write. */
		if (updateFlag)
		{
		    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
					  (ObjectID*)tid, slotOffset, bytesToRead, buf,
					  bytesToRead, NULL, NULL);
		    if (e < eNOERROR) ERR(handle, e);
		}

		slotArraySize -= bytesToRead;
		slotOffset    += bytesToRead;
	    }

            /* Decrease the element ordered offset */
            /* to elements located after the deleted element. */
            elementOrderedOffset    = start + LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(orderedSetColHdr);
            elementOrderedArraySize = LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements);
            prevFirstElementStartOffset = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr) + 
					  LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(nElementsToDelete);

            while (elementOrderedArraySize > 0)
            {
                Four nElementsToModify;

                bytesToRead = MIN(PAGESIZE,elementOrderedArraySize);

                e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                                     elementOrderedOffset, bytesToRead, (char*)buf, NULL, NULL);
                if (e < eNOERROR) ERR(handle, e);

                updateFlag = FALSE;
                nElementsToModify = bytesToRead/sizeof(OrderedSet_ElementLength);
                for (i = 0; i < nElementsToModify; i++)
                {
                    for(j = 0; j < nElementsToDelete; j++)
                    {
                        if (buf[i] >= deletedElementPosArray[j].elementStartOffset - prevFirstElementStartOffset)
                        {
                            buf[i] -= (deletedElementPosArray[j].elemLenWithLengthField - sizeof(OrderedSet_ElementLength));
                            updateFlag = TRUE;
                            
#ifdef COMPRESSION 
                            if (deletedElementPosArray[j].deltaOfUpdate != 0)
                            {
                                if (buf[i] > deletedElementPosArray[j].elementStartOffset - prevFirstElementStartOffset)
                                {
                                    buf[i] += deletedElementPosArray[j].deltaOfUpdate;
                                }
                            }
#endif                            
                        }
                    }
                }

                /* If there is no update in slot values, we don't have to write. */
                if (updateFlag)
                {
                    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                          (ObjectID*)tid, elementOrderedOffset, bytesToRead, buf,
                                          bytesToRead, NULL, NULL);
                    if (e < eNOERROR) ERR(handle, e);
                }

                elementOrderedArraySize -= bytesToRead;
                elementOrderedOffset    += bytesToRead;
            }


	    /* Update the tuple header. */
	    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
		tupHdr.varColOffset[i] -= totalDeletedElementsLength;

	    /* Write the updated column header. */
	    orderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);
	    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
				  (ObjectID*)tid, start, oldOrderedSetColHdrSize,
				  (char*)&orderedSetColHdr, orderedSetColHdrSize, NULL, NULL);
	    if (e < eNOERROR) ERR(handle, e);
        }
    }

    /* Write the updated tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                          tupHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_OrderedSet_DeleteElementsWithNestedIndex() */

/*@================================
 * LRDS_OrderedSet_DeleteElements()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_DeleteElements(Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Delete an array of elements.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_DeleteElements(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the posting */
    Four colNo,			/* IN column on which the posting is defined */
    Four nElementsToDelete,	/* IN number of elements to be deleted */
    KeyValue *kval,             /* IN array of keys of the deleted elements */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    KeyDesc *kdesc;             /* pointer to the key descriptor for element */
    unsigned char *nullVector;	/* bit array of null flags */
    OrderedSetColHdr_T orderedSetColHdr; /* ordered set column header */
    OrderedSet_ElementLength elemLen; /* element length */
    OrderedSet_ElementLength elemLenWithLengthField; /* element length including length field size */
    Four oldOrderedSetColHdrSize; /* old header size of ordered set column */
    Four orderedSetColHdrSize;  /* header size of ordered set column */
    OrderedSet_ElementOffset_T offset;
    Four offsetFromColStart;    /* offset of an element from column start */
    Boolean membershipFlag;
    Four slotNo;
    Four i, j, k;
    Four startOffset;
    Four keyLen;
    KeyValue elemKval;          /* key value for the element */
    Four totalDeletedElementsLength;
    Four cmp;
#ifndef COMPRESSION 
    char buf[MAXKEYLEN+sizeof(OrderedSet_ElementLength)];
#else    
    char buf[MAXKEYLEN+2+sizeof(OrderedSet_ElementLength)];
#endif
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    Four orderSetNElements;
    Four            elementStartOffset;
    Four	    tmpLength;
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four        prevDocId; 
    Four        nextDocId, nextDocIdGap, nextDocIdGapSize;
    Four        docId = 0;
    Four        docIdGap, docIdGapSize;
    char        tempData[sizeof(Four)+2];
    char        encodedData[sizeof(Four)+2];
    Four        encodedDataLength;
    Four        deltaOfUpdate;                       
#endif
    

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_DeleteElements()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (kval == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /* Is there any opened scan on the given ordered set? */
    for (i = 0; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++) {
	if (LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

	if (EQUAL_TUPLEID(LRDS_ORDEREDSETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_ORDEREDSETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    ERR(handle, eORDEREDSET_OPENSCAN_LRDS);
	}
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    tmpLength = length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* save the old ordered set column header length because an ordered set
       can destroy nested index during this operation */
    oldOrderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);

    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);

    if (orderedSetColHdr.nElements >= 0)
    {
	/* nested index does not exist */
	startOffset = start + HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

	/* Get the key length */
	keyLen = 0;
	for (k = 0; k < kdesc->nparts; k++) keyLen += kdesc->kpart[k].length;

	totalDeletedElementsLength = 0;
	orderSetNElements = orderedSetColHdr.nElements;
	for (i = 0; i < orderSetNElements; i++)
	{
	    /* read i-th elements's length and key value */
            e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid, 
#ifndef COMPRESSION 
                                             startOffset, keyLen+sizeof(OrderedSet_ElementLength), buf, 
#else                                        
                                             startOffset, keyLen+2+sizeof(OrderedSet_ElementLength), buf,      
#endif            
                                             &orderedSetColHdr, start, tmpLength, &elementStartOffset);
	    if (e < eNOERROR) ERR(handle, e);            

	    /* construct elemLen and elemKval from the content read */
	    memcpy(&elemLen, buf, sizeof(OrderedSet_ElementLength));
	    elemKval.len = keyLen;
            
#ifndef COMPRESSION 
	    memcpy(elemKval.val, &buf[sizeof(OrderedSet_ElementLength)], keyLen);
#else
            /* decode current docid gap */
            docIdGapSize = 0;                     
            LRDS_VARIABLE_BYTE_DECODING(&buf[sizeof(OrderedSet_ElementLength)], docIdGapSize, docIdGap);
            docId += docIdGap;
            memcpy(elemKval.val, &docId, keyLen);
#endif            
	    elemLenWithLengthField = elemLen + sizeof(OrderedSet_ElementLength);

	    for(j = 0; j < nElementsToDelete; j++)
	    {

		/* comapre i-th element's key value and j-th key value */
    		cmp = lrds_OrderedSet_CompareKeyValues(handle, kdesc, &elemKval, &kval[j]);
		if (cmp == EQUAL) {

                    /* Decrease the # of elements. */
                    orderedSetColHdr.nElements --;

                    /* Delete the element. */
                    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                                          startOffset, sizeof(OrderedSet_ElementLength), NULL, 0,
                                          NULL, NULL);
                    if (e < eNOERROR) ERR(handle, e);

                    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                                          start+elementStartOffset-sizeof(OrderedSet_ElementLength), elemLen, NULL, 0,
                                          NULL, NULL);
                    if (e < eNOERROR) ERR(handle, e);

#ifndef COMPRESSION  
                    e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr,
								   start,
                                                                   startOffset-start-HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX,
                                                                   -1*elemLen);
                    if (e < eNOERROR) ERR(handle, e);

		    tmpLength -= (sizeof(OrderedSet_ElementLength) + elemLen);

#else               
                    if(docId < orderedSetColHdr.lastDocId)
                    {                
                        /* fetch next docid gap */
                        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                                            start+elementStartOffset-sizeof(OrderedSet_ElementLength),
                                            sizeof(encodedData), encodedData, NULL, NULL);
                        if (e < eNOERROR) ERR(handle, e);
                
                        /* decode next docid gap */
                        nextDocIdGapSize = 0;                     
                        LRDS_VARIABLE_BYTE_DECODING(encodedData, nextDocIdGapSize, nextDocIdGap);        
                        
                        /* update next docid gap */
                        nextDocIdGap += docIdGap;
                        
                        /* next docid gap을 업데이트 했으면 docId 값은 원래로 되돌려야 됨 */
                        /* If you update the next docid gap, you restores the value of docId */
                        docId -= docIdGap;

                        /* encode next docid gap */
                        LRDS_VARIABLE_BYTE_ENCODING(nextDocIdGap, tempData, encodedData, encodedDataLength);
                
                        deltaOfUpdate = encodedDataLength - nextDocIdGapSize;
                        
                        /* write next docid gap */
                        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                                              start+elementStartOffset-sizeof(OrderedSet_ElementLength), 
                                              nextDocIdGapSize, encodedData, encodedDataLength, NULL, NULL);
                        if (e < eNOERROR) ERR(handle, e);
                    }
                    else if(docId == orderedSetColHdr.lastDocId)
                    {
                        prevDocId = docId - docIdGap;
                        orderedSetColHdr.lastDocId = prevDocId;
                        deltaOfUpdate = 0;
                    }
                    else
                    {
			fprintf(stderr, "docId: %d, lastDocId: %d\n", docId, orderedSetColHdr.lastDocId);
			getchar();
                    	ERR(handle, eINTERNAL);
                    }																                    
                    
                    e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr, start,
                                                                   startOffset-start-HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX,
								   -1*elemLen);
                    if (e < eNOERROR) ERR(handle, e);
                    
                    if(deltaOfUpdate)
                    {
                        e = lrds_OrderedSet_UpdateElementOrderedOffset(handle, relTableEntry, smScanId, useScanFlag, tid, &orderedSetColHdr, start,
                                                                       startOffset-start-HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX+sizeof(OrderedSet_ElementLength),
    								       deltaOfUpdate);
                        if (e < eNOERROR) ERR(handle, e);
                    }
                    
		    tmpLength -= (sizeof(OrderedSet_ElementLength) + elemLen - deltaOfUpdate);	
#endif


                    /* update total deleted length which is used to update tuple header */
#ifndef COMPRESSION 
                    totalDeletedElementsLength += elemLenWithLengthField;
#else
                    totalDeletedElementsLength += (elemLenWithLengthField - deltaOfUpdate);
#endif                    

                    break; 	/* escape loop-j */
                }
            }

	    /* if the elements is not deleted, then skip it */
	    if(j == nElementsToDelete) startOffset += sizeof(OrderedSet_ElementLength); 
	}
	/* Update the tuple header. */
	for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
	    tupHdr.varColOffset[i] -= totalDeletedElementsLength;

	/* Write the updated column header. */
	orderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);
	e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
			      (ObjectID*)tid, start, oldOrderedSetColHdrSize,
			      (char*)&orderedSetColHdr, orderedSetColHdrSize, NULL, NULL);
	if (e < eNOERROR) ERR(handle, e);

	/* Write the updated tuple header. */
	/* NOTICE: We can reduce the amount of data overwritten. */
	e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
			      (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
			      tupHdrSize, NULL, NULL);
	if (e < eNOERROR) ERR(handle, e);	
    }
    else
    {
	/* 문제를 DELETED_ELEMENT_POS_ARRAY_SIZE 단위로 잘라서
	   lrds_OrderedSet_DeleteElementsWithNestedIndex을 호출한다. */
	/* divide the problem into several ones whose size is DELETED_ELEMENT_POS_ARRAY_SIZE, and
	   call lrds_OrderedSet_DeleteElementsWithNestedIndex. */
	for(i = 0; i < nElementsToDelete / DELETED_ELEMENT_POS_ARRAY_SIZE; i++)
	{
	    e = lrds_OrderedSet_DeleteElementsWithNestedIndex(handle, ornOrScanId, useScanFlag, tid, colNo,
		                                              DELETED_ELEMENT_POS_ARRAY_SIZE,
							      &kval[i * DELETED_ELEMENT_POS_ARRAY_SIZE], lockupPtr);
	}
	if(nElementsToDelete % DELETED_ELEMENT_POS_ARRAY_SIZE)
	{
	    e = lrds_OrderedSet_DeleteElementsWithNestedIndex(handle, ornOrScanId, useScanFlag, tid, colNo,
		                                              nElementsToDelete % DELETED_ELEMENT_POS_ARRAY_SIZE,
							      &kval[i * DELETED_ELEMENT_POS_ARRAY_SIZE], lockupPtr);
	}
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_OrderedSet_DeleteElements() */


/*@================================
 * LRDS_OrderedSet_Scan_Open()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_Scan_Open(Four, Four, TupleID*, Four)
 *
 * Description:
 *  Open a scan on the given ordered set.
 *
 * Returns:
 *  1) scan id if the return value is greater than or equal to 0
 *  2) error code if the return value is less than 0
 */
Four LRDS_OrderedSet_Scan_Open(
    Four          handle,         /* IN handle */
    Four          ornOrScanId,	  /* IN open relation no or scan id*/
    Boolean       useScanFlag,    /* IN TRUE if above parameter is scan id */
    TupleID       *tid,		  /* IN tuple containing the set */
    Four          colNo,	  /* IN column on which the set is defined */
    Four          scanDirection,  /* IN direction of scanning (FORWARD,BACKWARD_ORDERING,BACKWARD_NOORDERING) */ 
    LockParameter *lockupPtr)     /* IN lock parameter */
{
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    Four setScanId;		/* scan id of newly opened scan */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    lrds_OrderedSetScanTableEntry *setScanTableEntry;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    OrderedSetColHdr_T		orderedSetColHdr;
    Four                	nElements;
    OrderedSet_ElementLength 	*ptr;
    ColDesc *relTableEntry_cdesc; 

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_Scan_Open()"));


    /*
    ** check parameters
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_S || lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = (lockupPtr->mode == L_S) ? L_IS : L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = lockupPtr->mode;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);

    /* We find also an empty slot for the new scan. */
    for (setScanId = 0; setScanId < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; setScanId++)
	if (LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].ornOrRelScanId == NIL) break;

    /* Open a new scan. */
    if (setScanId == lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries) { /* There is no empty entry. */

	e = Util_doublesizeVarArray(handle, &(lrds_perThreadDSptr->lrdsOrderedSetScanTable), sizeof(lrds_OrderedSetScanTableEntry));
	if (e < eNOERROR) ERR(handle, e);

	/* Initialize the newly allocated entries. */
	for (i = setScanId; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++)
	    LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId = NIL;
    }

    /* Set scanTableEntry for 'fast' access */
    setScanTableEntry = &LRDS_ORDEREDSETSCANTABLE(handle)[setScanId];

    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&setScanTableEntry->orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Fill other fields of the scan table entry
     */
    setScanTableEntry->ornOrRelScanId = ornOrScanId;
    setScanTableEntry->useRelScanFlag = useScanFlag;
    setScanTableEntry->tid = *tid;
    setScanTableEntry->colNo = colNo;
    orderedSetColHdr = setScanTableEntry->orderedSetColHdr;

    if (scanDirection == FORWARD) {
        setScanTableEntry->byteOffsetOfElementOrderedOffset = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(orderedSetColHdr);
        setScanTableEntry->byteOffsetOfElement = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr);
    }
    else if (scanDirection == BACKWARD_ORDERING || scanDirection == BACKWARD_NOORDERING) {
        setScanTableEntry->byteOffsetOfElementOrderedOffset = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr);
        setScanTableEntry->byteOffsetOfElement = length;
    }

    setScanTableEntry->scanDirection = scanDirection;
#ifdef COMPRESSION 
    if(scanDirection == FORWARD)
        setScanTableEntry->lastDocId = 0;
    else
        setScanTableEntry->lastDocId = orderedSetColHdr.lastDocId;
#endif

    /* For each scan, we allocate a system buffer to hold lengths of elements. */
    /* We note that this buffer is separated from a buffer given by a user,
     * i.e., the elementSizeBuffer argument of LRDS_OrderedSet_Scan_NextElements(),
     * because we can not assure that a user buffer is used only for storing lengths of elements. 
     */
    e = Util_getElementFromLocalPool(handle, &LRDS_ORDEREDSET_ELEMENT_LENGTH_POOL(handle), 
				     (void*)&(setScanTableEntry->ptrOfElementLengthArray));
    if (e < eNOERROR) ERR(handle, e);

    setScanTableEntry->firstElementIndexOfElementLengthArray = -1;
    setScanTableEntry->nElementOfElementLengthArray          = 0;

    return(setScanId);

} /* LRDS_OrderedSet_Scan_Open() */



/*@================================
 * LRDS_OrderedSet_Scan_Close()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_Scan_Close(Four, Four)
 *
 * Description:
 *  Close the given scan.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_Scan_Close(
    Four handle,                /* IN handle */
    Four setScanId)		/* IN scan id for a scan on a set */
{
    Four e;


    TR_PRINT(handle, TR_LRDS, TR1,
	     ("LRDS_OrderedSet_Scan_Close(handle, setScanId=%ld)", setScanId));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_ORDEREDSETSCANID(handle, setScanId)) ERR(handle, eBADPARAMETER);

    /* Close the given set scan. */
    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].ornOrRelScanId = NIL; /* not used */

    e = Util_freeElementToLocalPool(handle, &LRDS_ORDEREDSET_ELEMENT_LENGTH_POOL(handle),
		   	            (void*)LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].ptrOfElementLengthArray);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_OrderedSet_Scan_Close() */



/*@================================
 * LRDS_OrderedSet_Scan_NextElements()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*)
 *
 * Description:
 *  Fetch some elements from the given scan.
 *
 * Returns:
 *  1) # of elements actually read if the return value is greater than or equal to 0
 *  2) error code if the return value is less than 0
 */
Four LRDS_OrderedSet_Scan_NextElements(
    Four handle,                /* IN handle */
    Four setScanId,		/* IN scan id for a scan on a set */
    Four elementSizeBufferSize, /* IN element size buffer size */ 
    char *elementSizeBuffer,    /* OUT buffer to return the read element size */ 
    Four elementBufferSize,     /* IN buffer element buffer size */ 
#ifndef COMPRESSION 
    char *elementBuffer         /* OUT buffer to return the read element */ 
#else
    char *elementBuffer,        /* OUT buffer to return the read element */ 
    Four *lastDocId
#endif    
)		
{
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four ornOrScanId;
    Boolean useScanFlag;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of the set column */
    Four length;		/* amount of bytes of the set column */
    Four nElementsRead;		/* number of elements read */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    LockParameter lockup;	/* lockup for SM_Fetch Tuple */
    OrderedSet_ElementLength elemLen; /* length of an element */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSet_ElementLength totalBytesForFullElements; /* length for fully read elements */
    Four nElements;		/* number of elements */
    Four			i;
    OrderedSetColHdr_T		*orderedSetColHdr;
    Four			orderedSetSizeLength;
    Four			byteOffsetOfElementOrderedOffset;
    Four			byteToRead_ElementOrderedOffset;
    Four			byteOffsetOfElement;
    Four			byteToRead_Element;
    Four			offset_ElementOrderedOffset;
    Four			offset_Element;
    Four			remainByte_ElementOrderedOffset;
    Four			remainByte_Element;
    Four			align_elementSizeBuffer;
    Four			align_elementBuffer;
    OrderedSet_ElementLength    *elementSize;
    Four			_elementSize;
    Four			nextElementSize;
    Four			validElementOrderedOffset;
    Four			validElement;
    Four			startOffset;
    Four			offset;
    char			*buffer;
    Four                        i_start;
    Four                        i_end;
    Four                        indexOfElementLengthArray;
    Four                        numOfValidElements;
    ColDesc *relTableEntry_cdesc; 
    OrderedSet_ElementLength	*ptr;
    Four			nElementsToCopy;
    Four			numOfElementLength;
    Four			indexToCopy;
    Four			numToCopy;
    Four			indexToRead;
    Four			numToRead;
    Four                        firstElementIndexOfElementLengthArray;
    Four			nElementOfElementLengthArray;
    Four			startToRead;
    Four			elementSizeBufferOffset;
    Four			tempElementOffset;
    Four			cacheOffset;

#ifdef COMPRESSION 
    Four                        docId;
    Four                        docIdGap;
    Four                        oldDocIdGap;
    Four                        inBufferOffset;
    Four                        encodedDocIdLength;
#endif    


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_Scan_NextElements()"));


    /*
    ** check parameters
    */
    if (!LRDS_VALID_ORDEREDSETSCANID(handle, setScanId)) ERR(handle, eBADPARAMETER);

    if (elementSizeBufferSize <= 0 || elementBufferSize <= 0 || elementSizeBuffer == NULL || elementBuffer == NULL) ERR(handle, eBADPARAMETER);

    if (elementSizeBufferSize % sizeof(OrderedSet_ElementLength) != 0) ERR(handle, eBADPARAMETER); 


    ornOrScanId = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].ornOrRelScanId;
    useScanFlag = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].useRelScanFlag;

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].colNo];


    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid),
                         0, tupHdrSize, (char*)&tupHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /*
    ** Fetch the elements.
    */
    /* Get the starting offset & length of the column. */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    orderedSetColHdr = &LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].orderedSetColHdr;
    orderedSetSizeLength = sizeof(OrderedSet_ElementLength);

    /* NOTE:
     *   We need to know lengths of elements to read elements because elements have variable sizes.
     *   1. Read lengths of elements.
     *      - if lengths of elements have been loaded into the system buffer, use them;
     *      - if not, read lengths of elements and store them into the system buffer;
     *      We read lengths to fill the system buffer whose size is LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE.
     *      If we had read lengths of all elements, the system performance could degrade.
     *   2. Read elements by using the lengths which are stored in the system buffer.
     */

    if (LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].scanDirection == FORWARD) {

        /* Get the starting offset of the first element to read. */
        offset_ElementOrderedOffset = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset;
        offset_Element = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement;

        /* if 'length' is 0 then we have read all elements. */
        /* So we read 0 element now. */
        if (offset_Element == length) return(0);

        remainByte_ElementOrderedOffset = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(*orderedSetColHdr) -  offset_ElementOrderedOffset;
        byteToRead_ElementOrderedOffset = MIN(elementSizeBufferSize, remainByte_ElementOrderedOffset);

        remainByte_Element = length - offset_Element;
        byteToRead_Element = MIN(elementBufferSize, remainByte_Element);

	indexOfElementLengthArray = (offset_ElementOrderedOffset -
				    LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(*orderedSetColHdr)) /
				    orderedSetSizeLength;

	elementSizeBufferOffset = 0;
        ptr = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].ptrOfElementLengthArray;
	nElementsToCopy = byteToRead_ElementOrderedOffset / orderedSetSizeLength;
        numOfElementLength = (orderedSetColHdr->nElements >= 0) ? (orderedSetColHdr->nElements) : (-1 * orderedSetColHdr->nElements);

	while (nElementsToCopy > 0) {

	    firstElementIndexOfElementLengthArray = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].firstElementIndexOfElementLengthArray;
            nElementOfElementLengthArray = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].nElementOfElementLengthArray;

            /*
             * Copy element length from system buffer to user buffer
             */
            if (firstElementIndexOfElementLengthArray <= indexOfElementLengthArray &&
                indexOfElementLengthArray < firstElementIndexOfElementLengthArray + nElementOfElementLengthArray) {

                indexToCopy = indexOfElementLengthArray - firstElementIndexOfElementLengthArray;
                numToCopy   = MIN(nElementsToCopy,
                                  nElementOfElementLengthArray-(indexOfElementLengthArray-firstElementIndexOfElementLengthArray));

	        memcpy(&elementSizeBuffer[elementSizeBufferOffset], &ptr[indexToCopy], numToCopy*orderedSetSizeLength);

                indexOfElementLengthArray += numToCopy;
                nElementsToCopy -= numToCopy;
		elementSizeBufferOffset += numToCopy*orderedSetSizeLength;
 	    }

            /*
             * Read element length to system buffer
             */
            if (nElementsToCopy > 0) {

                indexToRead = indexOfElementLengthArray;
		numToRead   = MIN(LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE/orderedSetSizeLength,
                                  numOfElementLength-indexOfElementLengthArray);
                startToRead = start + LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(*orderedSetColHdr) +
			      (orderedSetSizeLength * indexToRead);

			/*
     		 * Fill element length array
     		 */
    		e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
				     (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid),
				     startToRead,
                                     orderedSetSizeLength*numToRead, (char*)ptr,
                                     NULL, NULL);
                if (e < eNOERROR) ERR(handle, e);

                for (i = 0; i < numToRead-1; i++) {
        	    ptr[i] = ptr[i+1] - ptr[i];
    		}

                if (numToRead == numOfElementLength-indexOfElementLengthArray) {
    		    ptr[i] = (length - LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(*orderedSetColHdr)) - ptr[i];

		    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].firstElementIndexOfElementLengthArray = indexToRead;
		    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].nElementOfElementLengthArray = numToRead;
		}
                else {
		    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].firstElementIndexOfElementLengthArray = indexToRead;
		    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].nElementOfElementLengthArray = numToRead-1;
                }
	    }
	}

        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid),
                             start+offset_Element, byteToRead_Element, elementBuffer, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* calculate element size from element ordered offset */
	i_end = byteToRead_ElementOrderedOffset/orderedSetSizeLength;
        validElement = 0;
        elementSize = (OrderedSet_ElementLength*)elementSizeBuffer;

#ifdef COMPRESSION 
        docId = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId;
        *lastDocId = docId;

        inBufferOffset = 0;
#endif

        for (i=0; i<i_end; i++) {

            if (validElement + elementSize[i] > byteToRead_Element) break;

            validElement += elementSize[i];

#ifdef COMPRESSION 
            encodedDocIdLength = 0;
            LRDS_VARIABLE_BYTE_DECODING(&elementBuffer[inBufferOffset], encodedDocIdLength, docIdGap);
            docId += docIdGap;
            inBufferOffset += elementSize[i];
#endif
        }

        LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset += (i * orderedSetSizeLength);
        LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement += validElement;

#ifdef COMPRESSION 
        LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId = docId;
#endif

	return(i);
    }
    else { /* BACKWARD_NOORDERING & BACKWARD_ORDERING */

        /* Get the starting offset of the first element to read. */
        offset_ElementOrderedOffset = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset;
        offset_Element = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement;

        /* if 'length' is 0 then we have read all elements. */
        /* So we read 0 element now. */
        if (offset_Element == LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(*orderedSetColHdr)) return(0);

        remainByte_ElementOrderedOffset = offset_ElementOrderedOffset
                                          - LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(*orderedSetColHdr);
        byteToRead_ElementOrderedOffset = MIN(elementSizeBufferSize, remainByte_ElementOrderedOffset);
        align_elementSizeBuffer = elementSizeBufferSize - byteToRead_ElementOrderedOffset;

        remainByte_Element = offset_Element - LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(*orderedSetColHdr);
        byteToRead_Element = MIN(elementBufferSize, remainByte_Element);
        align_elementBuffer = elementBufferSize - byteToRead_Element;

        indexOfElementLengthArray = (offset_ElementOrderedOffset - /* byteToRead_ElementOrderedOffset - */
                                    LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(*orderedSetColHdr)) /
                                    orderedSetSizeLength - 1; 


        elementSizeBufferOffset = elementSizeBufferSize;
        ptr = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].ptrOfElementLengthArray;
        nElementsToCopy = byteToRead_ElementOrderedOffset / orderedSetSizeLength;
        numOfElementLength = (orderedSetColHdr->nElements >= 0) ? (orderedSetColHdr->nElements) : (-1 * orderedSetColHdr->nElements);

        while (nElementsToCopy > 0) {

            firstElementIndexOfElementLengthArray = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].firstElementIndexOfElementLengthArray;
            nElementOfElementLengthArray = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].nElementOfElementLengthArray;

            /*
             * Copy element length from system buffer to user buffer
             */
            if (firstElementIndexOfElementLengthArray <= indexOfElementLengthArray &&
                indexOfElementLengthArray < firstElementIndexOfElementLengthArray + nElementOfElementLengthArray) {

                numToCopy   = MIN(nElementsToCopy,
                                  indexOfElementLengthArray-firstElementIndexOfElementLengthArray+1);
		indexToCopy = LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE/orderedSetSizeLength -
			      ((firstElementIndexOfElementLengthArray+nElementOfElementLengthArray-1)-
			       (indexOfElementLengthArray-numToCopy)) - 1;

                memcpy(&elementSizeBuffer[elementSizeBufferOffset-numToCopy*orderedSetSizeLength],
                       &ptr[indexToCopy],
                       numToCopy*orderedSetSizeLength);

                indexOfElementLengthArray -= numToCopy;
                nElementsToCopy -= numToCopy;
                elementSizeBufferOffset -= numToCopy*orderedSetSizeLength;
            }

            /*
             * Read element length to system buffer
             */
            if (nElementsToCopy > 0) {

                /* Note: Buffer의 마지막 Element Length를 구하기 위해서 마지막 Element 다음의 Element Offset을 1개 더 읽음 */
                /* Note: To get the last element length of the buffer, read one more elment offset that is next to the last elment. */
                numToRead   = MIN(LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE/orderedSetSizeLength - 1,
                                  indexOfElementLengthArray+1) + 1;
                indexToRead = indexOfElementLengthArray+1-(numToRead-1);
                startToRead = start + LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(*orderedSetColHdr) +
                              (orderedSetSizeLength * indexToRead);
		cacheOffset = LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE/orderedSetSizeLength - numToRead;

                /*
                 * Fill element length array
                 */
                e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                     (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid),
                                     startToRead,
                                     orderedSetSizeLength*numToRead,
				     (char*)&ptr[cacheOffset],
                                     NULL, NULL);
                if (e < eNOERROR) ERR(handle, e);

                for (i = cacheOffset; i < cacheOffset+(numToRead-1)-1; i++) {
                    ptr[i] = ptr[i+1] - ptr[i];
                }

                if (indexToRead+(numToRead-1) == numOfElementLength) {
                    ptr[i] = (length - LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(*orderedSetColHdr)) - ptr[i];

                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].firstElementIndexOfElementLengthArray = indexToRead;
                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].nElementOfElementLengthArray = numToRead-1;
                }
                else {
                    ptr[i] = ptr[i+1] - ptr[i];

                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].firstElementIndexOfElementLengthArray = indexToRead;
                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].nElementOfElementLengthArray = numToRead-1;
                }
            }
        }


        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid),
                             start+offset_Element-byteToRead_Element, byteToRead_Element,
                             &elementBuffer[align_elementBuffer], NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* calculate element size from element ordered offset */
	i_start = align_elementSizeBuffer/orderedSetSizeLength;
	i_end = elementSizeBufferSize/orderedSetSizeLength;
        validElement= 0;
        elementSize = (OrderedSet_ElementLength*)elementSizeBuffer;

#ifdef COMPRESSION 
        docId = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId;
        *lastDocId = docId;
        oldDocIdGap = 0;

        inBufferOffset = elementBufferSize;
#endif

        for (i=i_end-1; i>=i_start; i--) {

            if (validElement + elementSize[i] > byteToRead_Element) break;

            validElement += elementSize[i];

#ifdef COMPRESSION 
            encodedDocIdLength = 0;
            inBufferOffset -= elementSize[i];
            LRDS_VARIABLE_BYTE_DECODING(&elementBuffer[inBufferOffset], encodedDocIdLength, docIdGap);

            docId -= oldDocIdGap;
            oldDocIdGap = docIdGap;
#endif
        }

        numOfValidElements = i_end - 1 - i;
        LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset -= (numOfValidElements * orderedSetSizeLength);
        LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement -= validElement;

#ifdef COMPRESSION 
        docId -= oldDocIdGap;
        LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId = docId;
#endif

        if (LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].scanDirection == BACKWARD_ORDERING) {

            nElements = elementSizeBufferSize/orderedSetSizeLength;
            for(i=0; i<nElements/2; i++) {
                _elementSize = elementSize[i];
                elementSize[i] = elementSize[nElements-1-i];
                elementSize[nElements-1-i] = _elementSize;
            }

            startOffset = 0;
            offset = elementBufferSize;
            nElements = numOfValidElements;
            buffer = (char*)malloc(byteToRead_Element);
            for(i=0; i<nElements; i++) {
                memcpy(&buffer[startOffset], &elementBuffer[offset-elementSize[i]], elementSize[i]);
                startOffset += elementSize[i];
                offset -= elementSize[i];
            }

            memcpy(elementBuffer, buffer, byteToRead_Element);

            free(buffer);
        }


        return(numOfValidElements);
    }

} /* LRDS_OrderedSet_Scan_NextElements() */



/*@================================
 * LRDS_OrderedSet_CreateNestedIndex()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_CreateNestedIndex()
 *
 * Description:
 *  Create nested index on the ordered set columns which are enough size.
 *  The caller should be sure that the nested index does not exist.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_CreateNestedIndex(
    Four handle,                /* IN handle */
    Four orn,                   /* IN open relation number */
    Four colNo)                 /* IN column on which the nested indexes are created */
{
    Four e;			/* error number */
    Four user_v;                /* index on the LRDS User Mount Table*/
    Four tupHdrSize;		/* size of tuple header */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four start;			/* starting offset of the set column */
    Four length;		/* length of the set column */
    Four smScanId;		/* a scan identifier on SM level */
    Four i, j;			/* index variables */
    RelationInfo relInfo;       /* relation information */
    ColDesc *cdesc;              /* column description */
    LockParameter lockup;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr; /* column header for the ordered set */
    TupleID tid;                /* current tuple id */
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_CreateNestedIndex()"));


    if (!LRDS_VALID_ORN(handle, orn)) ERR(handle, eBADPARAMETER);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* When nested index flag is TRUE, this column has nested indexes already. */
    if (LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc) == TRUE) return(eNOERROR);

    /* Open a sequential scan on the given relation. */
    /* We know that the given relation is not a catalog table. */
    lockup.mode = L_SIX;
    lockup.duration = L_COMMIT;
    e = smScanId = SM_OpenSeqScan(handle, &relTableEntry->ri.fid, FORWARD, &lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* Prepare the lock parameter which will be used for the updated object. */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    e = SM_NextObject(handle, smScanId, (ObjectID*)&tid, NULL, NULL, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    while (e != EOS) {

        /*
         * Fetch the tuple header.
         */
        tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

        /* if the relation is a catalog relation then lockup is NULL */
        /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
        e = SM_FetchObject(handle, smScanId, NULL, 0, tupHdrSize, (char*)&tupHdr, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* Get the real tuple header size. */
        tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


        /* 'nullVector' points to bit array of null flags. */
        nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

        /* If the column has non NULL value, then look at the tuple. */
        if (colNo < tupHdr.nFixedCols + tupHdr.nVarCols && !BITTEST(nullVector, colNo)) { 

            /* Get the starting offset and length of the column. */
            start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
            length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

            if (IS_LONG_ORDEREDSET(length)) {
                Four addedBytes;

                /*
                 * Read the ordered set column header
                 */
                e = SM_FetchObject(handle, smScanId, NULL, start, HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX, (char*)&orderedSetColHdr, &lockup);
                if (e < eNOERROR) ERR(handle, e);

                e = addedBytes = lrds_OrderedSet_CreateNestedIndex(handle, smScanId, TRUE, (ObjectID*)&tid, &tupHdr, start, length,
                    &orderedSetColHdr, LRDS_ORDEREDSET_KEYDESC(cdesc));
                if (e < eNOERROR) ERR(handle, e);

                length += addedBytes;

                for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                    tupHdr.varColOffset[i] += addedBytes + (HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX);

                /* Write the updated column header. */
                e = SM_UpdateObject(handle, smScanId, (ObjectID*)&tid,
                                    start, HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX,
                                    (char*)&orderedSetColHdr,
                                    HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX, NULL);
                if (e < eNOERROR) ERR(handle, e);

                /* Write the updated tuple header. */
                /* NOTICE: We can reduce the amount of data overwritten. */
                e = SM_UpdateObject(handle, smScanId, (ObjectID*)&tid,
                                    0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL);
                if (e < eNOERROR) ERR(handle, e);
            }
        }

        e = SM_NextObject(handle, smScanId, (ObjectID*)&tid, NULL, NULL, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* Close the scan for the given relation. */
    e = SM_CloseScan(handle, smScanId);
    if (e < eNOERROR) ERR(handle, e);

    /* Set the nested index flag to TRUE */
    LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc) = TRUE;

    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag != FALSE) {
        for (user_v = 0; user_v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; user_v++)
            if (LRDS_USERMOUNTTABLE(handle)[user_v].volId == relTableEntry->ri.fid.volNo) break;

        assert(user_v != LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE);

        e = lrds_UpdateAuxColInfoInSysColumns(handle, user_v, &relTableEntry->ri.fid, colNo, 
					      PHYSICAL_PTR(cdesc->auxInfo)); 
        if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_OrderedSet_CreateNestedIndex() */



/*@================================
 * LRDS_OrderedSet_DestroyNestedIndex()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_DestroyNestedIndex()
 *
 * Description:
 *  Destroy nested index on the ordered set columns which have nested index.
 *  The caller should be sure that there is no ordered set scan on the
 *  given column.
 *
 * Returns:
 *  error code
 */
Four LRDS_OrderedSet_DestroyNestedIndex(
    Four handle,                /* IN handle */
    Four orn,                   /* IN open relation number */
    Four colNo)                 /* IN column on which the nested indexes are created */
{
    Four e;			/* error number */
    Four user_v;                /* index on the LRDS User Mount Table*/
    Four tupHdrSize;		/* size of tuple header */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four start;			/* starting offset of the set column */
    Four length;		/* length of the set column */
    Four smScanId;		/* a scan identifier on SM level */
    Four i, j;			/* index variables */
    RelationInfo relInfo;       /* relation information */
    ColDesc *cdesc;              /* column description */
    LockParameter lockup;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr; /* column header for the ordered set */
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_DestroyNestedIndex()"));


    if (!LRDS_VALID_ORN(handle, orn)) ERR(handle, eBADPARAMETER);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* When nested index flag is FALSE, this column don't have nested index. */
    if (LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc) == FALSE) return(eNOERROR);

    /* Open a sequential scan on the given relation. */
    /* We know that the given relation is not a catalog table. */
    lockup.mode = L_SIX;
    lockup.duration = L_COMMIT;
    e = smScanId = SM_OpenSeqScan(handle, &relTableEntry->ri.fid, FORWARD, &lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* Prepare the lock parameter which will be used for the updated object. */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    e = SM_NextObject(handle, smScanId, NULL, NULL, NULL, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    while (e != EOS) {

        /*
         * Fetch the tuple header.
         */
        tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

        /* if the relation is a catalog relation then lockup is NULL */
        /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
        e = SM_FetchObject(handle, smScanId, NULL, 0, tupHdrSize, (char*)&tupHdr, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* Get the real tuple header size. */
        tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


        /* 'nullVector' points to bit array of null flags. */
        nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

        /* If the column has non NULL value, then look at the tuple. */
        if (colNo < tupHdr.nFixedCols + tupHdr.nVarCols && !BITTEST(nullVector, colNo)) { 

            /* Get the starting offset and length of the column. */
            start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
            length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);


            /*
             * If nested index exist, destroy it.
             */
            /*
             * Read the ordered set column header
             */
            e = SM_FetchObject(handle, smScanId, NULL, start, sizeof(OrderedSetColHdr_T), (char*)&orderedSetColHdr, NULL);
            if (e < eNOERROR) ERR(handle, e);

            if (orderedSetColHdr.nElements < 0) {
                Four bytesToUpdate;

                bytesToUpdate = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX + LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);

                /* destroy the nested index */
                e = lrds_OrderedSet_DestroyNestedIndex(handle, smScanId, TRUE, NULL, &tupHdr, start, &orderedSetColHdr, FALSE);
                if (e < eNOERROR) ERR(handle, e);

                /*
                 * Delete the slot array and update the ordered set column header
                 */
                e = SM_UpdateObject(handle, smScanId, NULL, start, bytesToUpdate,
                                    &orderedSetColHdr, HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX, &lockup);
                if (e < eNOERROR) ERR(handle, e);

                /* Update the tuple header. */
                for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
                    tupHdr.varColOffset[i] -= bytesToUpdate - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

                /* Write the updated tuple header. */
                /* NOTICE: We can reduce the amount of data overwritten. */
                e = SM_UpdateObject(handle, smScanId, NULL, 0, tupHdrSize,
                                    (char*)&tupHdr, tupHdrSize, NULL);
                if (e < eNOERROR) ERR(handle, e);
            }
        }

        e = SM_NextObject(handle, smScanId, NULL, NULL, NULL, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* Close the scan for the given relation. */
    e = SM_CloseScan(handle, smScanId);
    if (e < eNOERROR) ERR(handle, e);

    /* Set the nested index flag to FALSE */
    LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc) = FALSE;

    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag != FALSE) {
        for (user_v = 0; user_v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; user_v++)
            if (LRDS_USERMOUNTTABLE(handle)[user_v].volId == relTableEntry->ri.fid.volNo) break;

        assert(user_v != LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE);

        e = lrds_UpdateAuxColInfoInSysColumns(handle, user_v, &relTableEntry->ri.fid, colNo, 
					      PHYSICAL_PTR(cdesc->auxInfo)); 
        if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_OrderedSet_DestroyNestedIndex() */



/*@================================
 * lrds_OrderedSet_FindElementUsingSearch()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_FindElementUsingSearch()
 *
 * Description:
 *  Find a element with the given key using search
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_FindElementUsingSearch(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    Four length,                /* IN column length */
    OrderedSetColHdr_T *orderedSetColHdr, /* IN ordered set column header */
    KeyDesc *kdesc,             /* IN key description */
    KeyValue *kval,             /* IN key value to find */
    Four *offset,               /* IN offset of the found element(EQUAL or GREAT) */
#ifndef COMPRESSION 
    Boolean *membershipFlag)    /* OUT membership */
#else
    Boolean *membershipFlag,    /* OUT membership */
    Four *prevDocId,            /* OUT previous docId */ 
    Four *nextDocId,            /* OUT next docId */
    Four *nextDocIdGapSize)     /* OUT next docId gap size */
#endif    
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    char buf[MAXKEYLEN+sizeof(OrderedSet_ElementLength)];
    Four i;
    OrderedSet_ElementLength elemLen; /* element length */
    Four cmp;
    Four columnStart = start; 
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four            constStart;
    Four            offset2;
#ifdef COMPRESSION 
    Four        docIdGap;
    Four        index;
    
    *nextDocId = 0;
#endif


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_FindElementUsingSearch()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);


    /* Get the key length */
    keyLen = 0;
    for (i = 0; i < kdesc->nparts; i++) keyLen += kdesc->kpart[i].length;

    start += HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    constStart = start + LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr->nElements); 


    for (i = 0; i < orderedSetColHdr->nElements; i++) {

        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                             (ObjectID*)tid, start, sizeof(OrderedSet_ElementLength), 
                             buf, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        memcpy(&offset2, buf, sizeof(OrderedSet_ElementLength));

        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                             constStart+offset2, keyLen, &buf[sizeof(OrderedSet_ElementLength)], NULL, NULL);

        memcpy(&elemLen, buf, sizeof(OrderedSet_ElementLength));
        elemKval.len = keyLen;
#ifndef COMPRESSION 
        memcpy(elemKval.val, &buf[sizeof(OrderedSet_ElementLength)], keyLen);
#else
        index = 0;
        LRDS_VARIABLE_BYTE_DECODING(&buf[sizeof(OrderedSet_ElementLength)], index, docIdGap);
        *prevDocId = *nextDocId;
        *nextDocId += docIdGap;
        *nextDocIdGapSize = index;

        memcpy(elemKval.val, nextDocId, keyLen);
#endif

        cmp = lrds_OrderedSet_CompareKeyValues(handle, kdesc, &elemKval, kval);
        if (cmp != LESS) {
            *offset = start - columnStart; 
            *membershipFlag = (cmp == EQUAL) ? TRUE:FALSE;

            return(eNOERROR);
        }

        start += sizeof(OrderedSet_ElementLength); 
    }

#ifdef COMPRESSION 
    *prevDocId = *nextDocId;
    *nextDocId = NIL;
#endif
        
    *offset = start - columnStart; 
    *membershipFlag = FALSE;

    return(eNOERROR);

} /* lrds_OrderedSet_FindElementUsingSearch() */



/*@================================
 * lrds_OrderedSet_FindElementUsingNestedIndex()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_FindElementUsingNestedIndex()
 *
 * Description:
 *  Find a element with the given key using the nested index
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_FindElementUsingNestedIndex(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    Four length,                /* IN column length */
    OrderedSetColHdr_T *orderedSetColHdr, /* IN ordered set column header */
    KeyDesc *kdesc,             /* IN key description */
    KeyValue *kval,             /* IN key value to find */
    Four *slotNo,               /* IN slot no of the found element(EQUAL or GREAT) */
#ifndef COMPRESSION 
    Boolean *membershipFlag)    /* OUT membership */
#else
    Boolean *membershipFlag,    /* OUT membership */
    Four *nextDocId)            /* OUT next docId */
#endif    
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four smScanIdForNestedIndex; /* scan id for scan manager */
    Four keyLen;                /* key length */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    KeyValue elemKval;          /* key value for the element */
    BoundCond lb;               /* lower bound */
    BoundCond ub;               /* upper bound */
    char buf[MAXKEYLEN+sizeof(Two)];
    Cursor *cursor;
    ObjectID oid;
    Four cmp;


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_FindElementUsingNestedIndex()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    /* Initialize the membership flag. */
    *membershipFlag = FALSE;

    lb.op = SM_GE;
    lb.key.len = kval->len;
    memcpy(lb.key.val, kval->val, kval->len);
    ub.op = SM_EOF;
    e = smScanIdForNestedIndex = SM_OpenIndexScan(handle, &relTableEntry->ri.fid, &orderedSetColHdr->nestedIndexId,
                                    kdesc, &lb, &ub, NULL);
    if (e < eNOERROR) ERR(handle, e);

    e = SM_NextObject(handle, smScanIdForNestedIndex, &oid, NULL, NULL, &cursor, NULL);
    if (e < eNOERROR) ERR(handle, e);


#ifndef COMPRESSION 
    /* Compare the key value. */
    if (e != EOS) { 
        *slotNo = oid.pageNo;

        cmp = lrds_OrderedSet_CompareKeyValues(handle, kdesc, &cursor->btree.key, kval);
        if (cmp == EQUAL) *membershipFlag = TRUE;
    } else
        *slotNo = -1;
#else
    /* Compare the key value. */
    if (e != EOS) 
    {
        *slotNo = oid.pageNo;
        memcpy(nextDocId, &cursor->btree.key.val, kval->len);
        
        cmp = lrds_OrderedSet_CompareKeyValues(handle, kdesc, &cursor->btree.key, kval);
        if (cmp == EQUAL) *membershipFlag = TRUE;
    } 
    else
    {
        *nextDocId = NIL;
        *slotNo = -1;
    }
#endif    

    e = SM_CloseScan(handle, smScanIdForNestedIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_OrderedSet_FindElementUsingNestedIndex() */



/*@================================
 * lrds_OrderedSet_ReadSlotValue()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_ReadSlotValue(Four, TupleID*, Four, Four, Four, Four*)
 *
 * Description:
 *  Read a slot value.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_ReadSlotValue(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    Four slotNo,                /* IN slot no */
    Four *offset)               /* OUT offset of the found element(EQUAL or GREAT) */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    char buf[MAXKEYLEN+sizeof(Two)];
    Four slotOffset;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_ReadSlotValue()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    slotOffset = LRDS_ORDEREDSET_ITH_SLOT_OFFSET(start, slotNo);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, slotOffset, sizeof(Four), (char*)offset, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_OrderedSet_ReadSlotValue() */




/*@================================
 * lrds_OrderedSet_WriteSlotValue()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_WriteSlotValue(Four, TupleID*, Four, Four, Four, Four*)
 *
 * Description:
 *  Write a slot value.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_WriteSlotValue(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    Four slotNo,                /* IN slot no */
    OrderedSet_ElementOffset_T offset) /* IN offset of the element */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    char buf[MAXKEYLEN+sizeof(Two)];
    Four slotOffset;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_WriteSlotValue()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    slotOffset = LRDS_ORDEREDSET_ITH_SLOT_OFFSET(start, slotNo);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, slotOffset, sizeof(Four),
                         (char*)&offset, sizeof(Four), NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_OrderedSet_WriteSlotValue() */



/*@================================
 * lrds_OrderedSet_IncreaseSlotValues()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_IncreaseSlotValues(Four, TupleID*, Four, Four, Four, Four*)
 *
 * Description:
 *  Increase slot values.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_IncreaseSlotValues(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    Four nSlots,                /* IN # of slots */
    OrderedSet_ElementOffset_T offsetBoundary, /* IN increase slot values which is greater than or equal to this value */
    Four delta)                 /* IN value to increase */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    Boolean updateFlag;         /* TRUE if slot values are updated */
    Four slotArraySize;
    Four slotOffset;
    Four bytesToRead;
    OrderedSet_ElementOffset_T buf[PAGESIZE/sizeof(OrderedSet_ElementOffset_T)];
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four updateStartIdx, updateEndIdx; 


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_IncreaseSlotValues()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    slotOffset = LRDS_ORDEREDSET_ITH_SLOT_OFFSET(start, 0);
    slotArraySize = LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(nSlots);

    while (slotArraySize > 0) {
        bytesToRead = MIN(sizeof(buf),slotArraySize);

        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                             (ObjectID*)tid, slotOffset, bytesToRead, (char*)buf, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        updateFlag = FALSE;
        for (i = 0; i < bytesToRead/sizeof(OrderedSet_ElementOffset_T); i++) {
            if (buf[i] >= offsetBoundary) {
		buf[i] += delta;
		if (updateFlag == FALSE) {
        	    updateFlag = TRUE;
		    updateStartIdx = i;
		}
		updateEndIdx = i;
	    }
        }

        /* If there is no update in slot values, we don't have to write. */
        if (updateFlag) {
            e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                  (ObjectID*)tid,
                                  slotOffset + sizeof(OrderedSet_ElementOffset_T)*updateStartIdx,  
				  sizeof(OrderedSet_ElementOffset_T)*(updateEndIdx-updateStartIdx+1),
				  &buf[updateStartIdx],
				  sizeof(OrderedSet_ElementOffset_T)*(updateEndIdx-updateStartIdx+1),
                                  NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);
        }

        slotArraySize -= bytesToRead;
        slotOffset += bytesToRead;
    }

    return(eNOERROR);

} /* lrds_OrderedSet_IncreaseSlotValues() */




/*@================================
 * lrds_OrderedSet_DecreaseSlotValues()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_DecreaseSlotValues(Four, TupleID*, Four, Four, Four, Four*)
 *
 * Description:
 *  Decrease slot values.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_DecreaseSlotValues(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    Four nSlots,                /* IN # of slots */
    OrderedSet_ElementOffset_T offsetBoundary, /* IN decrease slot values which is greater than or equal to this value */
    Four delta)                 /* IN value to decrease */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    Boolean updateFlag;         /* TRUE if slot values are updated */
    Four slotArraySize;
    Four slotOffset;
    Four bytesToRead;
    OrderedSet_ElementOffset_T buf[PAGESIZE/sizeof(OrderedSet_ElementOffset_T)];
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four updateStartIdx, updateEndIdx; 


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_DecreaseSlotValues()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    slotOffset = LRDS_ORDEREDSET_ITH_SLOT_OFFSET(start, 0);
    slotArraySize = LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(nSlots);

    while (slotArraySize > 0) {
        bytesToRead = MIN(PAGESIZE,slotArraySize);

        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                             (ObjectID*)tid, slotOffset, bytesToRead,
                             (char*)buf, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        updateFlag = FALSE;
        for (i = 0; i < bytesToRead/sizeof(OrderedSet_ElementOffset_T); i++) {
            if (buf[i] >= offsetBoundary) {
	        buf[i] -= delta;
                if (updateFlag == FALSE) {
            	    updateFlag = TRUE;
		    updateStartIdx = i;
                }
		updateEndIdx = i;
	    }
        }

        /* If there is no update in slot values, we don't have to write. */
        if (updateFlag) {
            e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                  (ObjectID*)tid,
                                  slotOffset + sizeof(OrderedSet_ElementOffset_T)*updateStartIdx,  
				  sizeof(OrderedSet_ElementOffset_T)*(updateEndIdx-updateStartIdx+1),
				  &buf[updateStartIdx],
				  sizeof(OrderedSet_ElementOffset_T)*(updateEndIdx-updateStartIdx+1),
                                  NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);
        }

        slotArraySize -= bytesToRead;
        slotOffset += bytesToRead;
    }

    return(eNOERROR);

} /* lrds_OrderedSet_DecreaseSlotValues() */



/*@================================
 * lrds_OrderedSet_DeallocateSlot()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_DeallocateSlot(Four, TupleID*, Four, Four, Four, Four)
 *
 * Description:
 *  Deallocate a slot.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_DeallocateSlot(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    OrderedSetColHdr_T *orderedSetColHdr, /* IN ordered set column header */
    Four slotNo)                /* IN slot no */
{
    Four e;                     /* error code */
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    char buf[MAXKEYLEN+sizeof(Two)];


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_DeallocateSlot()"));


    e = lrds_OrderedSet_WriteSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, orderedSetColHdr->freeSlotListHdr);
    if (e < eNOERROR) ERR(handle, e);


    /* Update the free slot list header */
    orderedSetColHdr->freeSlotListHdr = -slotNo;

    /* decrease the # of elements */
    orderedSetColHdr->nElements += 1;/* nElements is negative number because nexted index exist */

    /* The header is to be written by caller. */

    return(eNOERROR);

} /* lrds_OrderedSet_DeallocateSlot() */




/*@================================
 * lrds_OrderedSet_AllocateSlot()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_AllocateSlot(Four, TupleID*, Four, Four, Four, Four)
 *
 * Description:
 *  Allocate a slot.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_AllocateSlot(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    OrderedSetColHdr_T *orderedSetColHdr, /* IN ordered set column header */
    Four *slotNo)               /* OUT slot no */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    Four nextFreeSlotNo;        /* next free slot no */
    Four slotBuf[LRDS_ORDEREDSET_CHUNK_OF_SLOTS];


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_AllocateSlot()"));


    assert(orderedSetColHdr->freeSlotListHdr != NO_MORE_SLOT);

    *slotNo = -orderedSetColHdr->freeSlotListHdr;

    e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, tid, start, -orderedSetColHdr->freeSlotListHdr, &nextFreeSlotNo);
    if (e < eNOERROR) ERR(handle, e);

    /* Update the free slot list header */
    orderedSetColHdr->freeSlotListHdr = nextFreeSlotNo;

    /* increase the # of elements */
    orderedSetColHdr->nElements += -1;/* nElements is negative number because nested index exists */

    /* The header is to be written by caller. */

    return(eNOERROR);

} /* lrds_OrderedSet_AllocateSlot() */



/*@================================
 * lrds_OrderedSet_AddSlots()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_AddSlots(Four, TupleID*, Four, Four, Four, OrderedSetColHdr_T*)
 *
 * Description:
 *  Add a chunk of slots.
 *
 * Returns:
 *  error code if negative number is returned
 *  size of add slots in bytes otherwise
 */
Four lrds_OrderedSet_AddSlots(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    Four start,                 /* IN column starting offset */
    OrderedSetColHdr_T *orderedSetColHdr) /* IN ordered set column header */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    Four keyLen;                /* key length */
    KeyValue elemKval;          /* key value for the element */
    Four nextFreeSlotNo;        /* next free slot no */
    Four slotBuf[LRDS_ORDEREDSET_CHUNK_OF_SLOTS];
    Four slotOffset;
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_AllocateSlot()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    /*
     * create a chunk of slots
     */

    /* the end position of the slot array */
    slotOffset = start + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX + LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr->nSlots);

    /* initialize the added slots */
    for (i = 0; i < LRDS_ORDEREDSET_CHUNK_OF_SLOTS-1; i++) {
        slotBuf[i] = -(orderedSetColHdr->nSlots+i+1);
    }
    slotBuf[i] = orderedSetColHdr->freeSlotListHdr;
    orderedSetColHdr->freeSlotListHdr = -orderedSetColHdr->nSlots;
    orderedSetColHdr->nSlots += LRDS_ORDEREDSET_CHUNK_OF_SLOTS;

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, slotOffset, 0, slotBuf,
                          sizeof(slotBuf), NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* The header is to be written by caller. */

    return(sizeof(slotBuf));

} /* lrds_OrderedSet_AddSlots() */



/*@================================
 * lrds_OrderedSet_CompareKeyValues()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_CompareKeyValues(KeyDesc*, KeyValue*, KeyValue*)
 *
 * Description:
 *  Compare two key values.
 *
 * Returns:
 *  result of omparison (positive numbers)
 *    EQUAL : key1 and key2 are same
 *    GREAT : key1 is greater than key2
 *    LESS  : key1 is less than key2
 */
Four lrds_OrderedSet_CompareKeyValues(
    Four 			handle,         /* IN handle */
    KeyDesc 			*kdesc,		/* IN key descriptor for key1 and key2 */
    KeyValue 			*key1,		/* IN the first key value */
    KeyValue 			*key2)		/* IN the second key value */
{
    register unsigned char 	*left; 		/* left key value */
    register unsigned char 	*right; 	/* right key value */
    Four 			i;		/* index for # of key parts */
    Four 			j;		/* temporary variable */
    Four 			kpartSize;	/* size of the current kpart */
    Two  			len1, len2;	/* string length */
    Two_Invariable  		s1, s2;		/* short values */
    Four_Invariable    		i1, i2;		/* int values */
    Four_Invariable   		l1, l2;		/* long values */
    Eight_Invariable   		ll1, ll2;	/* long long values */
    float  			f1, f2;		/* float values */
    double 			d1, d2;		/* double values */
    PageID 			pid1, pid2;	/* PageID values */
    OID    			oid1, oid2;	/* OID values */ 
    Four 			result;


    TR_PRINT(handle, TR_BTM, TR1, ("lrds_OrderedSet_CompareKeyValues()"));


    /* Sequentially compare each key parts.
     *  If the first satisfying key part is found return TRUE
     */
    left = (unsigned char*)&(key1->val[0]);
    right = (unsigned char*)&(key2->val[0]);
    for(i = 0;i < kdesc->nparts;i++) {

        result = EQUAL; 

	switch (kdesc->kpart[i].type & SM_TYPE_MASK) {
	  case SM_SHORT:
	    memcpy(&s1, left, sizeof(Two_Invariable)); 
	    memcpy(&s2, right, sizeof(Two_Invariable)); 

	    if (s1 > s2) result = GREAT;
	    else if(s1 < s2) result = LESS;
	    kpartSize = sizeof(Two_Invariable);

	    break;

	  case SM_INT:
	    memcpy(&i1, left, sizeof(Four_Invariable)); 
	    memcpy(&i2, right, sizeof(Four_Invariable)); 

	    if (i1 > i2) result = GREAT;
	    else if (i1 < i2) result = LESS;
	    kpartSize = sizeof(Four_Invariable);

	    break;

	  case SM_LONG:
	    memcpy(&l1, left, sizeof(Four_Invariable)); 
	    memcpy(&l2, right, sizeof(Four_Invariable)); 

	    if (l1 > l2) result = GREAT;
	    else if (l1 < l2) result = LESS;
	    kpartSize = sizeof(Four_Invariable);

	    break;

	  case SM_LONG_LONG:
	    memcpy(&ll1, left, sizeof(Eight_Invariable));
	    memcpy(&ll2, right, sizeof(Eight_Invariable));

	    if (ll1 > ll2) result = GREAT;
	    else if (ll1 < ll2) result = LESS;
	    kpartSize = sizeof(Eight_Invariable);

	    break;

	  case SM_FLOAT:
	    memcpy(&f1, left, sizeof(float)); 
	    memcpy(&f2, right, sizeof(float)); 

	    if (f1 > f2) result = GREAT;
	    else if (f1 < f2) result = LESS;
	    kpartSize = sizeof(float);

	    break;

	  case SM_DOUBLE:
	    memcpy(&d1, left, sizeof(double)); 
	    memcpy(&d2, right, sizeof(double)); 

	    if (d1 > d2) result = GREAT;
	    else if (d1 < d2) result = LESS;

	    kpartSize = sizeof(double);

	    break;

	  case SM_STRING:	/* fixed length string */
	    for (j = 0; j < kdesc->kpart[i].length; j++, left++, right++) {
		if (*left > *right) {
                    result = GREAT;
                    break;
                } else if (*left < *right) {
                    result = LESS;
                    break;
		}
	    }

	    kpartSize = 0;	/* we already changed the pointers */

	    break;

	  case SM_VARSTRING:	/* variable length string */
	    /* get the string length */
	    memcpy(&len1, left, sizeof(Two)); 
	    memcpy(&len2, right, sizeof(Two)); 

	    left += sizeof(Two);
	    right += sizeof(Two);

	    for (j = MIN(len1, len2); j > 0; j--, left++, right++) {
		if (*left > *right) {
                    result = GREAT;
                    break;
		}
		else if (*left < *right) {
                    result = LESS;
                    break;
		}
	    }

	    /* left and right strings are same in MIN(len1, len2) bytes */
	    if (result == EQUAL && len1 > len2) result = GREAT;
	    else if (result == EQUAL && len1 < len2) result = LESS;

	    kpartSize = 0;	/* we already changed the pointers */

	    break;

	  case SM_PAGEID:
	  case SM_FILEID:
	  case SM_INDEXID:
	    memcpy(&pid1, left, SM_PAGEID_SIZE); 
	    memcpy(&pid2, right, SM_PAGEID_SIZE); 

	    if (pid1.volNo > pid2.volNo) result = GREAT;
	    else if (pid1.volNo < pid2.volNo) result = LESS;
	    else if (pid1.pageNo > pid2.pageNo) result = GREAT;
	    else if (pid1.pageNo < pid2.pageNo) result = LESS;

	    kpartSize = sizeof(PageID);

	    break;

	  case SM_OID:
	    memcpy(&oid1, left, SM_OID_SIZE); 
	    memcpy(&oid2, right, SM_OID_SIZE); 

	    if (oid1.volNo > oid2.volNo) result = GREAT;
	    else if (oid1.volNo < oid2.volNo) result = LESS;
	    else if (oid1.pageNo > oid2.pageNo) result = GREAT;
	    else if (oid1.pageNo < oid2.pageNo) result = LESS;
	    else if (oid1.slotNo > oid2.slotNo) result = GREAT;
	    else if (oid1.slotNo < oid2.slotNo) result = LESS;
	    else if (oid1.slotNo > oid2.slotNo) result = GREAT;
	    else if (oid1.slotNo < oid2.slotNo) result = LESS;

	    /* If the three fields have the same values, then two OIDs must be same. */
	    /* That is, the other two fields unique and classID should be same. */

	    kpartSize = sizeof(OID);

	    break;
	}

        if (result != EQUAL) {
            if (kdesc->kpart[i].type & SM_DESC) result = -result;
            return (result);
        }

	left += kpartSize;
	right += kpartSize;
    }

    return(EQUAL);

} /* lrds_OrderedSet_CompareKeyValues() */



/*@================================
 * lrds_OrderedSet_CreateNestedIndex()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_CreateNestedIndex()
 *
 * Description:
 *  create nested index on an ordered set column.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_CreateNestedIndex(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    TupleHdr *tupHdrPtr,        /* INOUT tuple header */
    Four start,                 /* IN column starting offset */
    Four colLength,             /* IN column length */ 
    OrderedSetColHdr_T *orderedSetColHdr, /* INOUT ordered set column header */
    KeyDesc *kdesc)             /* IN key description */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    ObjectID oid;               /* object id */
    Four slotBuf[LRDS_ORDEREDSET_CHUNK_OF_SLOTS];
    Four keyLen;
    Four i;
    Four slotNo;
    OrderedSet_ElementOffset_T offset;
    Four slotBufIdx; 
#ifndef COMPRESSION 
    char buf[sizeof(OrderedSet_ElementLength)+MAXKEYLEN];
#else
    char buf[sizeof(OrderedSet_ElementLength)+MAXKEYLEN+2];
#endif
    OrderedSet_ElementLength elemLen;
    KeyValue kval;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four            elementStartOffset;
    Four            constStart;
    Four            nSlots = 0; 
#ifdef COMPRESSION 
    Four            docId = 0, docIdGap, index;
#endif


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_CreateNestedIndex()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    if (orderedSetColHdr->nElements == 0) return(0);

    /* Create a nested index. */
    e = SM_AddIndex(handle, &relTableEntry->ri.fid, &orderedSetColHdr->nestedIndexId, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the key length */
    keyLen = 0;
    for (i = 0; i < kdesc->nparts; i++) keyLen += kdesc->kpart[i].length;

    constStart = start;
    start += HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    offset = 0;

    /*
     * The first slot(0-th slot) is not used to avoid confusion of two zeros
     * in the slot array: one represents offset 0, and the other 0-th slot in
     * the free slot list.
     */
    slotBufIdx = 0;
    slotBuf[slotBufIdx++] = UNUSED_SLOT;

    for (slotNo = 0; slotNo < orderedSetColHdr->nElements; slotNo++) {

        /* Read an element size and its key value. */
        e = lrds_OrderedSet_FetchElementFromOrderedSetWithSlotArrayWithoutNestedIndex(
					 handle, relTableEntry, smScanId, useScanFlag, (ObjectID*)tid, 
#ifndef COMPRESSION 
                                         start+offset, keyLen+sizeof(OrderedSet_ElementLength), buf, 
#else
                                         start+offset, keyLen+sizeof(OrderedSet_ElementLength)+2, buf, 
#endif
                                         orderedSetColHdr, constStart, colLength, nSlots, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        /* Set the slot value. */
        slotBuf[slotBufIdx] = offset;

        /* Insert a pair of the element and its pointer into the nested index. */
        memcpy(&elemLen, buf, sizeof(OrderedSet_ElementLength));
        kval.len = keyLen;
#ifndef COMPRESSION 
        memcpy(kval.val, &buf[sizeof(OrderedSet_ElementLength)], keyLen);
#else   
        index = 0;
        LRDS_VARIABLE_BYTE_DECODING(&buf[sizeof(OrderedSet_ElementLength)], index, docIdGap);
        docId += docIdGap;
        
        memcpy(kval.val, &docId, keyLen);
#endif

        e = lrds_OrderedSet_InsertElementIntoNestedIndex(handle, &orderedSetColHdr->nestedIndexId,
                                                         kdesc, &kval, slotNo+1);
        if (e < eNOERROR) ERR(handle, e);

        /* points to the next element */
        offset += sizeof(OrderedSet_ElementLength); 

        if (slotBufIdx == LRDS_ORDEREDSET_CHUNK_OF_SLOTS - 1) {
            /* Store the chunk of slots. */

            e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                  (ObjectID*)tid, start, 0, slotBuf, sizeof(slotBuf), NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);

            start      += sizeof(Four)*LRDS_ORDEREDSET_CHUNK_OF_SLOTS;
            nSlots     += LRDS_ORDEREDSET_CHUNK_OF_SLOTS; 
            colLength  += sizeof(Four)*LRDS_ORDEREDSET_CHUNK_OF_SLOTS; 

            slotBufIdx = 0;     /* reset slotBufIdx */

        } else
            slotBufIdx++;       /* point to the next slot */
    }

    /* Write the slot array when the last chunk of slots is not written. */
    if (slotBufIdx != 0) { 
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, start, 0, slotBuf,
                              sizeof(OrderedSet_ElementOffset_T)*slotBufIdx, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
    }

    orderedSetColHdr->freeSlotListHdr = NO_MORE_SLOT;
    orderedSetColHdr->nSlots = orderedSetColHdr->nElements + 1; /* 0-th slot is not used. */
    orderedSetColHdr->nElements *= -1;

    /* Return the slot array size. */
    return(LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr->nSlots));

} /* lrds_OrderedSet_CreateNestedIndex() */



/*@================================
 * lrds_OrderedSet_DestroyNestedIndex()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_DestroyNestedIndex()
 *
 * Description:
 *  destroy nested index on an ordered set column.
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_DestroyNestedIndex(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the given column */
    TupleHdr *tupHdrPtr,        /* INOUT tuple header */
    Four start,                 /* IN column starting offset */
    OrderedSetColHdr_T *orderedSetColHdr, /* INOUT ordered set column header */
    Boolean deleteFlag)         /* IN delete the slots if TRUE */
{
    Four e;                     /* error code */
    Four orn;
    Four smScanId;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_DestroyNestedIndex()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

    assert(orderedSetColHdr->nElements < 0);

    e = SM_DropIndex(handle, &orderedSetColHdr->nestedIndexId, NULL);
    if (e < eNOERROR) ERR(handle, e);

    if (deleteFlag) {
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, start+HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX,
                              LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr->nSlots),
                              NULL, 0, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
    }

    orderedSetColHdr->nElements *= -1;

    /* Return the slot array size. */
    return(LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr->nSlots)); 

} /* lrds_OrderedSet_DestroyNestedIndex() */



/*@================================
 * lrds_OrderedSet_InsertElementIntoNestedIndex()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_InsertElementIntoNestedIndex()
 *
 * Description:
 *  Insert an element with the given key into the nested index
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_InsertElementIntoNestedIndex(
    Four handle,                /* IN handle */
    IndexID *iid,               /* IN index id of the nested index */
    KeyDesc *kdesc,             /* IN key description */
    KeyValue *kval,             /* IN key value to find */
    Four slotNo)                /* IN slot no of the inserted element  */
{
    Four e;                     /* error code */
    ObjectID oid;               /* object id */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_InsertElementIntoNestedIndex()"));


    /* 'slotNo' is stored pageNo field. */
    /* The others are have NIL values; it is necessary to setting the others as known values to delete in future. */
    /* ObjectID's slotNo is not appropriate for the slot of ordered set because
       the sizes of the types are different. */
    oid.volNo = NIL;
    oid.pageNo = slotNo;
    oid.slotNo = NIL;
    oid.unique = 0;

    e = SM_InsertIndexEntry(handle, iid, kdesc, kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_OrderedSet_InsertElementIntoNestedIndex() */



/*@================================
 * lrds_OrderedSet_DeleteElementFromNestedIndex()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_DeleteElementFromNestedIndex()
 *
 * Description:
 *  Delete an element with the given key from the nested index
 *
 * Returns:
 *  error code
 */
Four lrds_OrderedSet_DeleteElementFromNestedIndex(
    Four handle,                /* IN handle */
    IndexID *iid,               /* IN index id of the nested index */
    KeyDesc *kdesc,             /* IN key description */
    KeyValue *kval,             /* IN key value to find */
    Four slotNo)                /* IN slot no of the deleted element  */
{
    Four e;                     /* error code */
    ObjectID oid;               /* object id */


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_DeleteElementFromNestedIndex()"));


    /* 'slotNo' is stored pageNo field. */
    /* ObjectID's slotNo is not appropriate for the slot of ordered set because
       the sizes of the types are different. */
    oid.volNo = NIL;
    oid.pageNo = slotNo;
    oid.slotNo = NIL;
    oid.unique = 0;

    e = SM_DeleteIndexEntry(handle, iid, kdesc, kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_OrderedSet_DelementElementFromNestedIndex() */



/*@================================
 * LRDS_OrderedSet_HasNestedIndex()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_HasNestedIndex(Four, Four, TupleID*, Four, Four, KeyValue*)
 *
 * Description:
 *  Check if the ordered set has a nested index
 *
 * Returns:
 *  existence if the return value is greater than or equal to 0
 *     TRUE if it has nested index
 *     FALSE otherwise
 *  error code if the return value is less than 0
 */
Four LRDS_OrderedSet_HasNestedIndex(
    Four handle,                /* IN handle */
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the set */
    Four colNo,			/* IN column on which the set is defined */
    LockParameter *lockupPtr)   /* IN lock parameter */
{
    Two  s;			/* for setting of key length */
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four tupHdrSize;		/* size of tuple header */
    Four start;			/* starting offset of fetch */
    Four length;		/* length of the set column */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    OrderedSetColHdr_T orderedSetColHdr;
    Boolean existenceFlag;
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_IsMember()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (lockupPtr != NULL && !((lockupPtr->mode == L_S || lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = (lockupPtr->mode == L_S) ? L_IS:L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = lockupPtr->mode;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }


    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    existenceFlag = (orderedSetColHdr.nElements >= 0) ? FALSE:TRUE;

    return(existenceFlag);

} /* LRDS_OrderedSet_HasNestedIndex() */


/*@========================================
 *  LRDS_OrderedSetAppendBulkLoad()
 * =======================================*/

/*
 * Function : Four LRDS_OrderedSetAppendBulkLoad()
 *
 * Description :
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 */
Four LRDS_OrderedSetAppendBulkLoad(
    Four             handle,              /* IN handle */
    Four             tmpVolId,            /* IN temporary volume in which sort stream is created */ 
    Four             ornOrScanId,         /* IN open relation no or scan id*/
    Boolean          useScanFlag,         /* IN TRUE if above parameter is scan id */
    TupleID*         tid,                 /* IN bulkload ID */
    Four             colNo,               /* IN column number of ordered set type */
    Four             nElements,           /* IN # of elements to append */
    Four             elementsBufSize,     /* IN buffer size */
    char*            elementsBuf,         /* IN elements to append */
    LockParameter*   lockupPtr)           /* IN lock parameter */
{
    Four             e;                   /* error code */
    Four             i, j;
    Four             orn;
    Four             smScanId;
    Four             slotNo;
    char*            ptr;                 /* pointer which points element in elementBuf */
    Four             length;              /* length of element including length field */
    Four             nestedIndexBlkLdId;
    Four             tupHdrSize;          /* size of tuple header */
    Four             colStart;            /* starting offset of fetch */
    Four             colLength;           /* length of the set column */
    ColDesc*         cdesc;               /* pointer to the current column descriptor */
    TupleHdr         tupHdr;              /* a tuple header */
    ObjectID         oid;                 /* object id */
    KeyValue         kval;                /* key value of an index entry */
    Four             keyLen;              /* length of element key */
    KeyDesc*         kdesc;               /* pointer to the key descriptor for element */
    unsigned char*   nullVector;          /* bit array of null flags */
    Four             orderedSetColHdrSize;/* header size of ordered set column */
    Boolean          nestedIndexFlag;     /* TRUE if nested index is allowed */
    Boolean          needNestedIndex;     /* TRUE if new nested index must be created */
    Four             oldOrderedSetColHdrSize; /* old header size of ordered set column */
    LockParameter    fileLockup;          /* lockup for SM_Fetch Tuple */
    LockParameter    objLockup;           /* lockup for SM_Fetch Tuple */
    LockParameter*   fileLockupPtr;       /* pointer to the lockup value */
    LockParameter*   objLockupPtr;        /* pointer to the lockup value */
    lrds_RelTableEntry* relTableEntry;    /* pointer to an entry of relation table */
    OrderedSetColHdr_T  orderedSetColHdr; /* ordered set column header */
    OrderedSet_ElementOffset_T offset;
    OrderedSet_ElementLength   elementLen; /* element length */
    Four                       offset1, offset2;
    Four                       elementOrderedOffset;
    Four		       start;
    Four		       constLength;
    Four                       elementLength;
    char                       *buffer;
    OrderedSet_ElementLength   *ptrOfElementOrderedOffset;
    OrderedSet_ElementLength   prevElementLength;
    ColDesc *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four                docId, docIdGap, oldDocId;
    char                tempData[sizeof(Four)+2];
    char                encodedData[sizeof(Four)+2];
    Four                encodedDataLength;
    Four                encodedElementLen;
    Four                encodedLength;
#endif
    

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    /*
     *  Check parameters.
     */

    /* check scanId */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    /* check orn */
    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    /* check input buffer */
    if (elementsBufSize <= 0 || elementsBuf == NULL || nElements <= 0) ERR(handle, eBADPARAMETER);


    /*
     *  set orn & smScanId
     */
    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }


    /*
     *  Get relTableEntry, cdesc, and kdesc
     */

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (!LRDS_VALID_COLNO(colNo, relTableEntry->ri.nColumns)) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);


    /*
     *  Get the current tuple if the tid is NULL.
     */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;


    /*
     *  Prepare lock parameter
     */

    /* error check */
    if (lockupPtr != NULL && !((lockupPtr->mode == L_X) && (lockupPtr->duration == L_COMMIT))) ERR(handle, eBADPARAMETER);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (lockupPtr == NULL || LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {

        fileLockupPtr = objLockupPtr = NULL;
    } else {

        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }


    /*
     *  Is there any opened scan on the given ordered set?
     */
    for (i = 0; i < lrds_perThreadDSptr->lrdsOrderedSetScanTable.nEntries; i++) {
        if (LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_ORDEREDSETSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_ORDEREDSETSCANTABLE(handle)[i].colNo == colNo) { /* found */
            ERR(handle, eORDEREDSET_OPENSCAN_LRDS);
        }
    }


    /*
     * Fetch the tuple header.
     */

    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* If the column has NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eORDEREDSET_NOTEXIST_LRDS);


    /*
     * Read the ordered set column header
     */

    start = colStart = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    constLength = colLength = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, colStart, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, colStart, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* save the old ordered set column header length because an ordered set can have nested index during this operation */
    oldOrderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);


    /*
     *  For each elements in input buffer, append it into ordered set column
     */

    if (orderedSetColHdr.nElements < 0) {

#ifndef NDEBUG
printf("Nested Index exists!!\n");
#endif

        /* calculate 'keyLen' */
        for (i = 0, keyLen = 0; i < kdesc->nparts; i++ ) {
            keyLen += kdesc->kpart[i].length;
        }

        /* initialize index bulkload */
        nestedIndexBlkLdId = SM_InitSortedIndexBulkLoad(handle, &orderedSetColHdr.nestedIndexId, kdesc, 100, 100, lockupPtr); 
        if (nestedIndexBlkLdId < 0) ERR(handle, nestedIndexBlkLdId);
    }


    /* STEP1. APPEND ELEMENT ORDERED OFFSET & ELEMENT ... */
    buffer = (char*)malloc(elementsBufSize);
    if(buffer == NULL) ERR(handle, eMEMORYALLOCERR);

    /* Write the element ordered offset. */
    ptrOfElementOrderedOffset = (OrderedSet_ElementLength*)buffer;

    if (orderedSetColHdr.nElements >= 0) {
        offset = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX +
                 LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements);
    }
    else {
        offset = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
                 LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots) +
                 LRDS_ORDEREDSET_ELEMENT_ORDERED_OFFSET_SIZE(orderedSetColHdr.nElements);
    }

#ifndef COMPRESSION 
    offset1 = 0;
    ptrOfElementOrderedOffset[0] = constLength - offset;
    memcpy(&prevElementLength, &elementsBuf[0], sizeof(OrderedSet_ElementLength));

    for(i = 1; i < nElements; i++) {
        ptrOfElementOrderedOffset[i] = ptrOfElementOrderedOffset[i-1] + prevElementLength;

        offset1 += (sizeof(OrderedSet_ElementLength) + prevElementLength);
        memcpy(&prevElementLength, &elementsBuf[offset1], sizeof(OrderedSet_ElementLength));
    }

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          start+offset, 0, buffer, sizeof(OrderedSet_ElementLength) * nElements, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Write the element. */
    offset1 = 0;
    offset2 = 0;

    for(i = 0; i < nElements; i++) {
        memcpy(&elementLength, &elementsBuf[offset1], sizeof(OrderedSet_ElementLength));
        memcpy(&buffer[offset2], &elementsBuf[offset1+sizeof(OrderedSet_ElementLength)], elementLength);

        offset1 += (sizeof(OrderedSet_ElementLength) + elementLength);
        offset2 += elementLength;
    }

    offset = constLength + sizeof(OrderedSet_ElementLength) * nElements;

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          start+offset, 0, buffer, offset2, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

#else

    ptrOfElementOrderedOffset[0] = constLength - offset;
    offset2 = sizeof(OrderedSet_ElementLength) * nElements;

    ptr = elementsBuf;
    oldDocId = 0;
    
    for (i = 0; i < nElements; i++ ) 
    {
        memcpy(&elementLength, ptr, sizeof(OrderedSet_ElementLength));
        memcpy(&docId, &ptr[sizeof(OrderedSet_ElementLength)], sizeof(docId));
        
        docIdGap = docId - oldDocId;
        oldDocId = docId;
        
        LRDS_VARIABLE_BYTE_ENCODING(docIdGap, tempData, encodedData, encodedDataLength);
        
        encodedElementLen = elementLength - sizeof(docId) + encodedDataLength;

        /* is not last element */
        if(i < nElements -1)
        {
           ptrOfElementOrderedOffset[i+1] = ptrOfElementOrderedOffset[i] + encodedElementLen;
        }
        else
        {
            memcpy(&orderedSetColHdr.lastDocId, &docId, sizeof(docId));
        }
        
        if(offset2 + encodedDataLength > elementsBufSize)
        {
           if(buffer != NULL) free(buffer);
           ERR(handle, eINTERNAL);
        }
        memcpy(&buffer[offset2], encodedData, encodedDataLength);
        offset2 += encodedDataLength;
        
        if(offset2 + elementLength - sizeof(docId) > elementsBufSize)
        {
           if(buffer != NULL) free(buffer);
           ERR(handle, eINTERNAL);
        }
        memcpy(&buffer[offset2], &ptr[sizeof(OrderedSet_ElementLength)+sizeof(docId)], elementLength - sizeof(docId));
        offset2 += elementLength - sizeof(docId);
        
        ptr += elementLength + sizeof(OrderedSet_ElementLength);
    }

    /* Write the element ordered offset. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, 
                          start+offset, 0, buffer, sizeof(OrderedSet_ElementLength) * nElements, NULL, NULL);
    if (e < eNOERROR) 
    {
        if(buffer != NULL) free(buffer);
        ERR(handle, e);
    }

    /* Write the element. */
    offset = constLength + sizeof(OrderedSet_ElementLength) * nElements; 

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid, start+offset, 0, 
                          &buffer[sizeof(OrderedSet_ElementLength) * nElements], 
                          offset2 - sizeof(OrderedSet_ElementLength) * nElements, NULL, NULL);
    if (e < eNOERROR) 
    {
        if(buffer != NULL) free(buffer);
        ERR(handle, e);
    }
#endif

    free(buffer);

#ifdef COMPRESSION 
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++)
        tupHdr.varColOffset[i] += offset2;
#endif        

    /* ... STEP1. APPEND ELEMENT ORDERED OFFSET & ELEMENT */

    /* STEP2. UPDATE HEADER & CREATE NESTED INDEX ... */

    /* initialize offset */
    offset = (orderedSetColHdr.nElements > 0) ? (orderedSetColHdr.nElements * sizeof(OrderedSet_ElementLength))
                                              : (-1 * orderedSetColHdr.nElements * sizeof(OrderedSet_ElementLength));

    /* initialize ptr */
    ptr = elementsBuf;

    /* for each element in sort stream, append into ordered set */
    for (i = 0; i < nElements; i++ ) {

        /*
         *  get length of ith element
         */

        /* get elementLen */
        memcpy(&elementLen, ptr, sizeof(OrderedSet_ElementLength));

        /* calculate length */
        length = elementLen + sizeof(OrderedSet_ElementLength);

#ifndef COMPRESSION 
        /* Update the tuple header. */
        for (j = cdesc->varColNo; j < tupHdr.nVarCols; j++) { 
            tupHdr.varColOffset[j] += length;
        }
#endif        

        /* Update number of elements in ordered set column header. */
        if (orderedSetColHdr.nElements >= 0) {
            orderedSetColHdr.nElements ++;
        }


        /*
         *  insert corresponding entry into nested index & slot array
         */
        if (orderedSetColHdr.nElements < 0) {

            /* allocate a slot */
            if (orderedSetColHdr.freeSlotListHdr == NO_MORE_SLOT) {
                Four addedBytes;

                e = addedBytes = lrds_OrderedSet_AddSlots(handle, ornOrScanId, useScanFlag, tid, colStart, &orderedSetColHdr);
                if (e < eNOERROR) ERR(handle, e);

                /* Update the tuple header. */
                for (j = cdesc->varColNo; j < tupHdr.nVarCols; j++) { 
                    tupHdr.varColOffset[j] += addedBytes;
                }

            }

            e = lrds_OrderedSet_AllocateSlot(handle, ornOrScanId, useScanFlag, tid, colStart, &orderedSetColHdr, &slotNo);
            if (e < eNOERROR) ERR(handle, e);

            /* Write the slot value. */
            e = lrds_OrderedSet_WriteSlotValue(handle, ornOrScanId, useScanFlag, tid, colStart, slotNo, offset);
            if (e < eNOERROR) ERR(handle, e);

            /* Get the key value from the element. */
            kval.len = keyLen;
            memcpy(kval.val, ptr+sizeof(OrderedSet_ElementLength), keyLen);

            /* construct oid */
            oid.volNo = NIL;
            oid.pageNo = slotNo;
            oid.slotNo = NIL;
            oid.unique = 0;

            /* insert into sort stream for nested index bulkload */
            e = SM_NextSortedIndexBulkLoad(handle, nestedIndexBlkLdId, &kval, &oid);
            if (e < eNOERROR) ERR(handle, e);

        } /* if */


        /*
         *  update ptr & offset
         */
        ptr     += length;
        offset  += sizeof(OrderedSet_ElementLength); 

    } /* for */

    /* ... STEP2. UPDATE HEADER & CREATE NESTED INDEX */

    /* finalize index bulkload */
    if (orderedSetColHdr.nElements < 0) {

        e = SM_FinalSortedIndexBulkLoad(handle, nestedIndexBlkLdId);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
     *  If nested index need to be created, create and prepare bulkload
     */

    /* set 'needNestedIndex' */
    nestedIndexFlag = LRDS_ORDEREDSET_NESTED_INDEX_FLAG(cdesc);
    colLength = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, colStart, tupHdr);
    assert(colLength == offset-colStart);
    needNestedIndex = IS_LONG_ORDEREDSET(colLength) && nestedIndexFlag && orderedSetColHdr.nElements >= 0;

    if (needNestedIndex) {

        Four addedBytes;

        e = addedBytes = lrds_CreateNestedIndexByBulkLoad(handle, tmpVolId, ornOrScanId, useScanFlag, tid, &tupHdr, colStart, colLength,
                                                          &orderedSetColHdr, kdesc, lockupPtr); 
        if (e < eNOERROR) ERR(handle, e);

        /* update tuple header */
        for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) { 
            tupHdr.varColOffset[i] += addedBytes + (HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX - HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX);
        }

    } /* if */


    /*
     *  Write the updated column header.
     */
    orderedSetColHdrSize = LRDS_ORDEREDSET_COLUMN_HEADER_SIZE(orderedSetColHdr.nElements);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, colStart, oldOrderedSetColHdrSize,
                          (char*)&orderedSetColHdr, orderedSetColHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Write the updated tuple header.
     *  NOTICE: We can reduce the amount of data overwritten.
     */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_OrderedSetAppendBulkLoad() */




Four lrds_CreateNestedIndexByBulkLoad(
    Four                handle,             /* IN handle */
    Four                tmpVolId,           /* IN temporary volume in which sort stream is created */ 
    Four                ornOrScanId,        /* IN open relation no or scan id*/
    Boolean             useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                /* IN tuple containing the given column */
    TupleHdr*           tupHdrPtr,          /* INOUT tuple header */
    Four                start,              /* IN column starting offset */
    Four                colLength,          /* IN column length */ 
    OrderedSetColHdr_T* orderedSetColHdr,   /* INOUT ordered set column header */
    KeyDesc*            kdesc,              /* IN key description */
    LockParameter*      lockupPtr)          /* IN lock parameter */
{
    Four                e;                  /* error code */
    Four                orn;
    Four                smScanId;
    ObjectID            oid;                /* object id */
    KeyValue            kval;
    Four                slotBuf[LRDS_ORDEREDSET_CHUNK_OF_SLOTS];
    Four                keyLen;
    Four                i;
    Four                slotNo;
    Four                slotBufIdx;
    Four                nestedIndexBlkLdId;
    lrds_RelTableEntry* relTableEntry;      /* pointer to an entry of relation table */
#ifndef COMPRESSION 
    char                buf[sizeof(OrderedSet_ElementLength)+MAXKEYLEN];
#else
    char                buf[sizeof(OrderedSet_ElementLength)+MAXKEYLEN+2];
#endif
    OrderedSet_ElementLength   elemLen;
    OrderedSet_ElementOffset_T offset;
    Four                elementStartOffset; 
    Four		constStart; 
    Four                nSlots = 0; 
#ifdef COMPRESSION 
    Four                docId = 0, docIdGap, index;
#endif


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_CreateNestedIndexByBulkLoad()"));


    /*
     *  if there is no element, return
     */
    if (orderedSetColHdr->nElements == 0) return(0);


    /*
     *  set orn & smScanId
     */
    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }


    /*
     *  Get relTableEntry, cdesc, and kdesc
     */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

#ifndef NDEBUG
printf("Nested Index is created!!\n");
#endif

    /*
     *  Create nested index
     */
    e = SM_AddIndex(handle, &relTableEntry->ri.fid, &orderedSetColHdr->nestedIndexId, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Initialize nested index bulkload
     */
    nestedIndexBlkLdId = SM_InitIndexBulkLoad(handle, tmpVolId, kdesc);
    if (nestedIndexBlkLdId < 0) ERR(handle, nestedIndexBlkLdId);


    /*
     *  Insert elements of ordered set into nested index
     */

    /* Get the key length */
    keyLen = 0;
    for (i = 0; i < kdesc->nparts; i++) keyLen += kdesc->kpart[i].length;

    constStart = start; 
    start += HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    offset = 0;


    /*
     * The first slot(0-th slot) is not used to avoid confusion of two zeros
     * in the slot array: one represents offset 0, and the other 0-th slot in
     * the free slot list.
     */
    slotBufIdx = 0;
    slotBuf[slotBufIdx++] = UNUSED_SLOT;

    for (slotNo = 0; slotNo < orderedSetColHdr->nElements; slotNo++) {

        /* Read an element size and its key value. */
        e = lrds_OrderedSet_FetchElementFromOrderedSetWithSlotArrayWithoutNestedIndex(
					 handle, relTableEntry, smScanId, useScanFlag, (ObjectID*)tid, 
#ifndef COMPRESSION 
                                         start+offset, keyLen+sizeof(OrderedSet_ElementLength), buf, 
#else
                                         start+offset, keyLen+sizeof(OrderedSet_ElementLength)+2, buf, 
#endif
                                         orderedSetColHdr, constStart, colLength, nSlots, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        /* Set the slot value. */
        slotBuf[slotBufIdx] = offset;

        /* Insert a pair of the element and its pointer into the nested index. */
        memcpy(&elemLen, buf, sizeof(OrderedSet_ElementLength));
        kval.len = keyLen;
#ifndef COMPRESSION 
        memcpy(kval.val, &buf[sizeof(OrderedSet_ElementLength)], keyLen);
#else   
        index = 0;
        LRDS_VARIABLE_BYTE_DECODING(&buf[sizeof(OrderedSet_ElementLength)], index, docIdGap);
        docId += docIdGap;
        
        memcpy(kval.val, &docId, keyLen);
#endif

        /* construct oid */
        oid.volNo = NIL;
        oid.pageNo = slotNo+1;
        oid.slotNo = NIL;
        oid.unique = 0;

        /* insert into nested index */
        e = SM_NextIndexBulkLoad(handle, nestedIndexBlkLdId, &kval, &oid);
        if (e < eNOERROR) ERR(handle, e);

        /* points to the next element */
        offset += sizeof(OrderedSet_ElementLength); 

        if (slotBufIdx == LRDS_ORDEREDSET_CHUNK_OF_SLOTS - 1) {
            /* Store the chunk of slots. */

            e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                  (ObjectID*)tid, start, 0, slotBuf, sizeof(slotBuf), NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);

            start      += sizeof(Four)*LRDS_ORDEREDSET_CHUNK_OF_SLOTS;
            nSlots     += LRDS_ORDEREDSET_CHUNK_OF_SLOTS; 
            colLength  += sizeof(Four)*LRDS_ORDEREDSET_CHUNK_OF_SLOTS; 

            /* reset slotBufIdx */
            slotBufIdx = 0;

        } else {

            /* point to the next slot */
            slotBufIdx++;
        }

    } /* for */

    /*
     * finalize nested index bulkload
     */
    e = SM_FinalIndexBulkLoad(handle, nestedIndexBlkLdId, &orderedSetColHdr->nestedIndexId, 100, 100, lockupPtr);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Write the slot array when the last chunk of slots is not written.
     */
    if (slotBufIdx != 0) {
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, start, 0, slotBuf,
                              sizeof(OrderedSet_ElementOffset_T)*slotBufIdx, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
    }

    /*
     *  Update orderedSetColHdr
     */
    orderedSetColHdr->freeSlotListHdr = NO_MORE_SLOT;
    orderedSetColHdr->nSlots = orderedSetColHdr->nElements + 1; /* 0-th slot is not used. */
    orderedSetColHdr->nElements *= -1;

    return(LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr->nSlots));
}

/*@================================
 * LRDS_OrderedSet_Scan_SkipElementsUntilGivenKeyValue()
 *================================*/
/*
 * Function: Four LRDS_OrderedSet_Scan_SkipElementsUntilGivenKeyValue(Four, Four, char*)
 *
 * Description:
 *  move scan cursor to the point that makes user read elements whose key value is greater than given key value
 *
 * Returns:
 *  1) error code if the return value is less than 0
 *  2) number of elements to be skipped
 */
Four LRDS_OrderedSet_Scan_SkipElementsUntilGivenKeyValue(
    Four handle,                /* IN handle */
    Four setScanId,		/* IN scan id for a scan on a set */
    Four keyLength,		/* IN length of key value */
    char* keyValue)		/* IN pointer to key value */
{
    Four		    e;			/* error code */
    Four		    orn;
    Four		    smScanId;
    Four		    tupHdrSize;		/* size of tuple header */
    Four		    ornOrScanId;
    Boolean		    useScanFlag;
    LockParameter	    lockup;		/* lockup for SM_Fetch Tuple */
    OrderedSetColHdr_T	    orderedSetColHdr;	/* column header for the ordered set */
    lrds_RelTableEntry*	    relTableEntry;	/* pointer to an entry of relation table */
    ColDesc*		    cdesc;		/* pointer to the current column descriptor */
    TupleHdr		    tupHdr;		/* a tuple header */
    KeyDesc*		    kdesc;              /* pointer to the key descriptor for element */
    KeyValue		    kval;               /* key value of an index entry */
    Four		    start;		/* starting offset of the set column */
    Four		    length;		/* length of the set column */
    Four		    offsetFromColStart; /* offset of an element from column start */
    Four		    offset;
    Four		    slotNo;
    Boolean		    membershipFlag;
    Four	            elementStartOffset; 
    char	            buffer[sizeof(OrderedSet_ElementLength)]; 
    Four                    elementLength; 
    ColDesc                 *relTableEntry_cdesc; 
#ifdef COMPRESSION 
    Four                prevDocId;
    Four                nextDocId;
    Four                nextDocIdGap;
    Four                nextDocIdGapSize;
    char                encodedData[sizeof(Four)+2];    
#endif    
    

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_Scan_SkipElementsUntilGivenKeyValue()"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_ORDEREDSETSCANID(handle, setScanId)) ERR(handle, eBADPARAMETER);

    ornOrScanId = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].ornOrRelScanId;
    useScanFlag = LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].useRelScanFlag;

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].colNo];

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid),
			 0, tupHdrSize, (char*)&tupHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Get the key descriptor for the ordered set element
     */
    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);

    /* Get the key value from the element. */
    kval.len = keyLength;
    memcpy(kval.val, keyValue, keyLength);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                     (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), start, sizeof(OrderedSetColHdr_T),
                     (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    if (orderedSetColHdr.nElements >= 0)
    {	/* nested index does not exist */
	e = lrds_OrderedSet_FindElementUsingSearch(handle,
		ornOrScanId, useScanFlag, &(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
		kdesc, &kval, &offsetFromColStart, &membershipFlag);
#else
                kdesc, &kval, &offsetFromColStart, &membershipFlag, &prevDocId, &nextDocId, &nextDocIdGapSize);
#endif
        if (e < eNOERROR) ERR(handle, e);

    }
    else
    {
	e = lrds_OrderedSet_FindElementUsingNestedIndex(handle,
			ornOrScanId, useScanFlag, &(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid),
			start, length, &orderedSetColHdr,
#ifndef COMPRESSION 
			kdesc, &kval, &slotNo, &membershipFlag);
#else
                        kdesc, &kval, &slotNo, &membershipFlag, &nextDocId);
#endif  
	if (e < eNOERROR) ERR(handle, e);

	if(slotNo >= 0)
	{
	    e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, &(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), start, slotNo, &offset);
	    if (e < eNOERROR) ERR(handle, e);

	    /* Now 'offsetFromColStart' is distance from the begining of column. */
            offsetFromColStart = offset + HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX +
		LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);
	}
        else {
            offsetFromColStart = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr); 
        }
    }

    if (LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].scanDirection == FORWARD) {

	if (LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset < offsetFromColStart) { 

            if (offsetFromColStart == LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr)) {
                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset = offsetFromColStart;
                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement = length;
#ifdef COMPRESSION 
                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId = orderedSetColHdr.lastDocId;
#endif
            }
            else {
                e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, 
                                                 (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), 
                                                 start+offsetFromColStart, sizeof(OrderedSet_ElementLength), buffer, 
                                                 &orderedSetColHdr, start, length, &elementStartOffset);
                if (e < eNOERROR) ERR(handle, e);

                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset = offsetFromColStart;
                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement = elementStartOffset;
#ifdef COMPRESSION 
                e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                    (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), 
                                    start + elementStartOffset, sizeof(encodedData),
                                    encodedData, NULL, NULL);        
                if (e < eNOERROR) ERR(handle, e);    
                
                nextDocIdGapSize = 0;                     
                LRDS_VARIABLE_BYTE_DECODING(encodedData, nextDocIdGapSize, nextDocIdGap);        
                
                /* get prevDocId */
                prevDocId = nextDocId - nextDocIdGap;
            
                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId = prevDocId;
#endif                

            }
        }
    }
    else {

	if (membershipFlag == TRUE) {

	    if (LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset > offsetFromColStart + sizeof(OrderedSet_ElementLength)) {

                e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, 
                                                 (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), 
                                                 start+offsetFromColStart, sizeof(OrderedSet_ElementLength), buffer, 
                                                 &orderedSetColHdr, start, length, &elementStartOffset);
                if (e < eNOERROR) ERR(handle, e);

	        memcpy(&elementLength, buffer, sizeof(OrderedSet_ElementLength));

                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset = offsetFromColStart + 
										   sizeof(OrderedSet_ElementLength);
                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement = elementStartOffset + elementLength; 
#ifdef COMPRESSION 
                LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId = nextDocId;
#endif            
	    }
        }
	else { /* membershipFlag == FALSE */

	    if (LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset > offsetFromColStart) { 

  	        if (offsetFromColStart == LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT(orderedSetColHdr)) {
                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset = offsetFromColStart;
                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement = length;
#ifdef COMPRESSION 
                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId = orderedSetColHdr.lastDocId;                
#endif          
                }
                else {
                    e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, 
                                                     (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), 
                                                     start+offsetFromColStart, sizeof(OrderedSet_ElementLength), buffer, 
                                                     &orderedSetColHdr, start, length, &elementStartOffset);
                    if (e < eNOERROR) ERR(handle, e);

                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElementOrderedOffset = offsetFromColStart;
                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].byteOffsetOfElement = elementStartOffset; 
#ifdef COMPRESSION 
                    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                    (ObjectID*)&(LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].tid), 
                                    start + elementStartOffset, sizeof(encodedData),
                                    encodedData, NULL, NULL);        
                    if (e < eNOERROR) ERR(handle, e);    
                    
                    nextDocIdGapSize = 0;                     
                    LRDS_VARIABLE_BYTE_DECODING(encodedData, nextDocIdGapSize, nextDocIdGap);        
                    
                    /* get prevDocId */
                    prevDocId = nextDocId - nextDocIdGap;
                
                    LRDS_ORDEREDSETSCANTABLE(handle)[setScanId].lastDocId = prevDocId;
#endif   
                }
            }
	}

    }


    return(eNOERROR);

} /* LRDS_OrderedSet_Scan_SkipElementsUntilGivenKeyValue() */

/*@================================
 * lrds_OrderedSet_DumpSlotValues()
 *================================*/
/*
 * Function: Four lrds_OrderedSet_DumpSlotValues(Four, Boolean, TupleID*, Four)
 *
 * Description:
 *  Debugging을 위해, subindex의 slot 값들을 전부 dump한다.
 *  For the debugging, dump all values of the slot array in the subindex
 *
 * Returns:
 */
Four lrds_OrderedSet_DumpSlotValues(
    Four         handle,            /* IN handle */
    Four	ornOrScanId,	    /* IN open relation no or scan id*/
    Boolean	useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID*	tid,		    /* IN tuple containing the given column */
    Four	colNo)		    /* IN column no */
{
    Four		e;			/* error code */
    Four		orn;
    Four		smScanId;
    Four		slotArraySize;
    Four		slotOffset;
    Four		bytesToRead;
    Four		i;
    lrds_RelTableEntry*	relTableEntry;		/* pointer to an entry of relation table */
    Four		index;
    Four		start;
    Four		length;
    ColDesc*		cdesc;			/* pointer to the current column descriptor */
    TupleHdr		tupHdr;			/* a tuple header */
    Four		tupHdrSize;		/* size of tuple header */
    Four		slotNo;
    Four		offset;
    OrderedSetColHdr_T	orderedSetColHdr;
    unsigned char*	nullVector;		/* bit array of null flags */
    OrderedSet_ElementOffset_T buf[PAGESIZE/sizeof(OrderedSet_ElementOffset_T)];
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_DumpSlotValues()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry and column description. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc = &relTableEntry_cdesc[colNo];

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* If the column has non NULL value, then return error. */
    if (BITTEST(nullVector, colNo)) ERR(handle, eORDEREDSET_NOTEXIST_LRDS);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    slotOffset = LRDS_ORDEREDSET_ITH_SLOT_OFFSET(start, 0);
    slotArraySize = LRDS_ORDEREDSET_SLOT_ARRAY_SIZE(orderedSetColHdr.nSlots);

    if(orderedSetColHdr.nElements < 0)
    {
	printf("nSlots = %ld, freeSlotListHdr = %ld, Slot Values : ", orderedSetColHdr.nSlots, orderedSetColHdr.freeSlotListHdr);
	index = 0;
	while (slotArraySize > 0) {
	    bytesToRead = MIN(sizeof(buf),slotArraySize);

	    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
				 (ObjectID*)tid, slotOffset, bytesToRead, (char*)buf, NULL, NULL);
	    if (e < eNOERROR) ERR(handle, e);

	    for (i = 0; i < bytesToRead/sizeof(OrderedSet_ElementOffset_T); i++) {
		if(buf[i] == NO_MORE_SLOT)
		    printf("%ld:NO_MORE_SLOT ", index);
		else if(buf[i] == UNUSED_SLOT)
		    printf("%ld:UNUSED_SLOT ", index);
		else
		    printf("%ld:%ld ", index, buf[i]);
		index ++;
	    }

	    slotArraySize -= bytesToRead;
	    slotOffset += bytesToRead;
	}
	printf("\n");

	printf("Dump free slot list\n");
	slotNo = orderedSetColHdr.freeSlotListHdr * (-1);
	printf("%ld -> ", slotNo);
	while(1)
	{
	    e = lrds_OrderedSet_ReadSlotValue(handle, ornOrScanId, useScanFlag, tid, start, slotNo, &offset);
	    if (e < eNOERROR) ERR(handle, e);

	    slotNo = offset * (-1);
	    if(offset == NO_MORE_SLOT)
	    {
		printf("NO_MORE_SLOT\n");
		break;
	    }
	    else if(offset < 0)
		printf("%ld -> ", slotNo);
	    else
	    {
		printf("%ld <- error\n", offset);
		break;
	    }
	}
    }
    else
    {
	printf("subindex is not exist on this ordered set\n");
    }

    return(eNOERROR);

} /* lrds_OrderedSet_DumpSlotValues() */

Four lrds_OrderedSet_DumpAllElement(
    Four        handle,             /* IN handle */
    Four        ornOrScanId,        /* IN open relation no or scan id*/
    Boolean     useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID*    tid,                /* IN tuple containing the given column */
    Four        colNo)              /* IN column no */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                i;
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    Four                start;
    Four                length;
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    OrderedSetColHdr_T  orderedSetColHdr;
    Four		_buffer;
    char                *buffer;
    Four                keyLen;
    Four		size;
    Four		key;
    char		*content;
    Four   		elementStartOffset;
    Four                startOffset;
    KeyDesc 		*kdesc;
    Four 		elementLength;
    Four                nElements;
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_OrderedSet_DumpSlotValues()"));


    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry and column description. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc = &relTableEntry_cdesc[colNo];

    /*
     * Fetch the tuple header.
     */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /*
     * Read the ordered set column header
     */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, start, sizeof(OrderedSetColHdr_T),
                         (char*)&orderedSetColHdr, NULL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    kdesc = LRDS_ORDEREDSET_KEYDESC(cdesc);

    keyLen = 0;
    for (i = 0; i < kdesc->nparts; i++)
        keyLen += kdesc->kpart[i].length;

    printf("-----------------------------------------------------------------------------------------------------\n");
	printf("tid:(pageNo=%ld,volNo=%ld,slotNo=%ld,unique=%ld), colNo:%ld, nElements:%ld\n",
			tid->pageNo, tid->volNo, tid->slotNo, tid->unique, colNo, orderedSetColHdr.nElements);
    printf("-----------------------------------------------------------------------------------------------------\n");

    if(orderedSetColHdr.nElements >= 0)
    {
        nElements = orderedSetColHdr.nElements;
        startOffset = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    }
    else
    {
        nElements = -1 * orderedSetColHdr.nElements;
        startOffset = LRDS_ORDEREDSET_OFFSET_OF_FIRST_ELEMENT_ORDERED_OFFSET(orderedSetColHdr);

        printf("Slot Array = [ ");
        for(i=1; i<orderedSetColHdr.nSlots; i++) {
            e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                                 start+HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX+i*sizeof(Four), sizeof(Four), (char*)&_buffer,
                                 NULL, NULL);
            if (e < eNOERROR) ERR(handle, e);

            if (_buffer == NO_MORE_SLOT) break;
            if (_buffer >= 0) printf("%ld ", _buffer);
        }
        printf("]\n");
    }


    for(i=0; i<nElements; i++) {
        e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                         start+startOffset, sizeof(OrderedSet_ElementLength), (char*)&elementLength,
                                         &orderedSetColHdr, start, length, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        buffer = (char*)malloc(elementLength+sizeof(OrderedSet_ElementLength)+1);

        e = lrds_OrderedSet_FetchElement(handle, relTableEntry, smScanId, useScanFlag, tid,
                                         start+startOffset, sizeof(OrderedSet_ElementLength)+elementLength, buffer,
                                         &orderedSetColHdr, start, length, &elementStartOffset);
        if (e < eNOERROR) ERR(handle, e);

        memcpy(&size, &buffer[0], sizeof(OrderedSet_ElementLength));
        content = &buffer[sizeof(OrderedSet_ElementLength)+sizeof(Four)];
        content[size-sizeof(Four)] = '\0';

        printf("size=%21ld, content=%s\n", size, content);
        free(buffer);

        startOffset += sizeof(OrderedSet_ElementLength);
    }
    printf("-----------------------------------------------------------------------------------------------------\n");

    return(eNOERROR);

}

/*
 * Function: Four LRDS_OrderedSet_IsNull(Four, Four, Boolean, TupleID*, Four)
 *
 * Description:
 *  Check where the given colNo is null or not.
 *
 * Retuns:
 *  error code
 */
Four LRDS_OrderedSet_IsNull(
    Four                handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Four                colNo                   /* IN column on which the set is created */
)
{
    Four                e;                      /* error code */
    Boolean             flag;                   /* flag that indicates the given colNo is null or not */
    Four                orn;
    Four                smScanId;
    Four                tupHdrSize;             /* size of tuple header */
    TupleHdr            tupHdr;                 /* a tuple header */
    unsigned char       *nullVector;            /* bit array of null flags */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OrderedSet_IsNull()"));


    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IS;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_S;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Fetch the tuple header. */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* check where the given colNo is null or not */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) flag = TRUE;
    else flag = FALSE;


    return(flag);
}
#endif
