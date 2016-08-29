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
/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
 * Module: LRDS_CollectionList.c
 *
 * Description:
 *  Implements a List.
 *
 * Imports:
 *
 *
 * Exports:
 *  Four LRDS_CollectionList_Create(Four, Boolean, TupleID*, Two)
 *  Four LRDS_CollectionList_Destroy(Four, Boolean, TupleID*, Two)
 *  Four LRDS_CollectionList_GetN_Elements(Four, Boolean, TupleID*, Two, Four)
 *  Four LRDS_CollectionList_InsertElements(Four, Boolean, TupleID*, Two, Four, Four,
 *                                          ElementLength*, void*)
 *  Four LRDS_CollectionList_DeleteElements(Four, Boolean, TupleID*, Two, Four, Four)
 *  Four LRDS_CollectionList_AppendElements(Four, Boolean, TupleID*, Two, Four, Four*, void*)
 *  Four LRDS_CollectionList_RetrieveElements(Four, Boolean, TupleID*, Two, Four, Four,
 *                                            ElementLength*, Four, void*)
 *  Four LRDS_CollectionList_UpdateElements(Four, Boolean, TupleID*, Two, Four, Four,
 *                                          ElementLength*, void*)
 *  Four LRDS_CollectionList_Concatenate(Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two)
 *  Four LRDS_CollectionList_Resize(Four, Boolean, TupleID*, Two, Four)
 *  Four LRDS_CollectionList_IsMember(Four, Boolean, TupleID*, Two, Four, void*, Four*)
 *  Four LRDS_CollectionList_IsEqual(Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two)
 *  Four LRDS_CollectionList_Scan_Open(Four, Boolean, TupleID*, Two)
 *  Four LRDS_CollectionList_Scan_Close(Four)
 *  Four LRDS_CollectionList_Scan_NextElements(Four, Four, Four*, Four, void*)
 *  Four LRDS_CollectionList_Scan_InsertElements(Four, Four, Four*, void*)
 *  Four LRDS_CollectionList_Scan_DeleteElements(Four)
 *
 * Notice:
 *  The input parameter relScanId is used to identify the relation which
 *  contains the given list. We don't use a scan identified by the relScanId.
 *  I suggest that the relScanId be replaced with the open relation number.
 *  When this change is made, the input parameter `tuple id' should not be
 *  NULL.
 */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM_Internal.h"	
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

Four LRDS_CollectionList_Create(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Two                 colNo)                  /* IN column on which the set is created */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Create(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* create collection */
    e = lrds_Collection_Create(handle, ornOrScanId, useScanFlag, tid, colNo, ALL_VALUE);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionList_Destroy(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Two                 colNo)                  /* IN column on which we destroy the set  */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Destroy(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Get nElementsOfCol */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);


    /* delete all the elements from the column */
    if (relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable-length elements */
        e = lrds_Collection_VarElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                             0, nElementsOfCol);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                               0, nElementsOfCol);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* Set tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    e = lrds_Collection_Destroy(handle, ornOrScanId, useScanFlag, tid, colNo);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionList_GetN_Elements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                *nElements)             /* OUT nElements in the column */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Destroy(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

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

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, nElements);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionList_AssignElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements,              /* IN # of elements to insert */
    Four                *elementSizes,          /* IN sizes of elements, if element size is fixed, then pass NULL */
    void                *elements)              /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_AssignElement()"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* Delete all contents */
    e = LRDS_CollectionList_DeleteAll(handle, ornOrScanId, useScanFlag, tid, colNo);
    if(e < eNOERROR) ERR(handle, e);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, 0,
                                             nElements, elementSizes, elements);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, 0,
                                               nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* Set tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionList_Assign(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the List */
    Two                 colNo,                  /* IN column on which the List is defined */
    Four                assignedOrnOrScanId,    /* IN assigned relation containing the List*/
    Boolean             assignedUseScanFlag,    /* IN TRUE if above parameter is scan id */
    TupleID             *assignedTid,           /* IN assigned tuple containing the List */
    Two                 assignedColNo)          /* IN assigned column on which the List is defined */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Assign(handle)"));

    return lrds_Collection_Assign(handle, ornOrScanId, useScanFlag, tid, colNo,
                                  assignedOrnOrScanId, assignedUseScanFlag, assignedTid, assignedColNo);
}

Four      LRDS_CollectionList_InsertElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes,           /* IN sizes of elements, if element size is fixed, then pass NULL */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_InsertElements(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                             nElements, elementSizes, elements);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                               nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* Set tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionList_DeleteElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements)              /* IN # of elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_DeleteElements(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith, nElements);
        if(e < eNOERROR) ERR(handle, e);
        nElements = e;
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith, nElements);
        if(e < eNOERROR) ERR(handle, e);
        nElements = e;
    }

    /* Set tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return nElements;
}

Four LRDS_CollectionList_DeleteAll(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo)                  /* IN column on which the set is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    Four                nElementsOfCol;
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_DeleteElements(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Get # of elements of column */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                             0, nElementsOfCol);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                               0, nElementsOfCol);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* Set tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionList_AppendElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes,           /* IN sizes of elements, if element size is fixed, then pass NULL */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_DeleteElements(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Get nElementsOfCol */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, nElementsOfCol,
                                             nElements, elementSizes, elements);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, nElementsOfCol,
                                               nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* Set tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionList_GetSizeOfElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementsSize)           /* OUT sizeof elements to read */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    Four                nElementsOfCol;         /* # of elements in the column */
    char                elementSizesBuf[PAGESIZE];
    Four                i;
    Four                nReads;
    Four                toReadIndex;
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_GetSizeOfElements(handle)"));

    /* Check parameters*/
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
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

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

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        Four    nElementsToRead;

        nElementsToRead = nElements;
        toReadIndex     = ith;
        *elementsSize   = 0;
        while(nElementsToRead > 0)
        {
            nReads = sizeof(elementSizesBuf) / sizeof(Four);
            if(nReads > nElementsToRead)
                nReads = nElementsToRead;

            e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                     toReadIndex,
                                                     nReads, (Four*)elementSizesBuf);
            if(e < eNOERROR) ERR(handle, e);

            for(i = 0; i < nReads; i++)
                *elementsSize += ((Four*)elementSizesBuf)[i];
            nElementsToRead -= nReads;
            toReadIndex     += nReads;
        }
    }
    else
    {   /* fixed size element */
        *elementsSize = nElements * cdesc->length;
    }

    return nElements;
}

Four LRDS_CollectionList_RetrieveElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes,           /* INOUT sizes of elements, if element size is fixed, then pass NULL */
    Four                sizeOfElements,         /* IN size of elements */
    void*               elements)               /* INOUT elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    Four                nElementsOfCol;         /* # of elements in the column */
    Four                size;
    Four                i;
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_DeleteElements(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

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

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                                 nElements, (Four*)elementSizes);
        if(e < eNOERROR) ERR(handle, e);

        for(size = 0, i = 0; i < nElements; i++)
        {
            if(sizeOfElements < (size + elementSizes[i]))
                break;
            size += elementSizes[i];
        }
        nElements = i;

        e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                            nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        if((nElements * relTableEntry_cdesc[colNo].length) > sizeOfElements)
            nElements = sizeOfElements / relTableEntry_cdesc[colNo].length;

        if(elementSizes != NULL)
            for(i = 0; i < nElements; i++)
                elementSizes[i] = relTableEntry_cdesc[colNo].length;

        e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                              nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }

    return nElements;
}

Four LRDS_CollectionList_UpdateElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes,           /* IN sizes of elements, if element size is fixed, then pass NULL */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_UpdateElements(handle)"));

    /* Check parameters*/
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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

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

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementUpdate(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                             nElements, elementSizes, elements);
        if(e < eNOERROR) ERR(handle, e);
        nElements = e;
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementUpdate(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                               nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
        nElements = e;
    }

    /* Set tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return nElements;
}

Four LRDS_CollectionList_Concatenate(
    Four handle,
    Four                ornOrScanId,                    /* IN open relation no or scan id*/
    Boolean             useScanFlag,                    /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                            /* IN tuple containing the set */
    Two                 colNo,                          /* IN column on which the set is defined */
    Four                concatnatedOrnOrScanId,         /* IN open relation no or scan id*/
    Boolean             concatnatedUseScanFlag,         /* IN TRUE if above parameter is scan id */
    TupleID*            concatnatedTid,                 /* IN tuple containing the set */
    Two                 concatnatedColNo)               /* IN concatnated column on which the set is defined */
{
    Four                e;                              /* error code */
    Four                orn;
    Four                smScanId;
    Four                concatnatedOrn;
    Four                concatnatedSmScanId;

    lrds_RelTableEntry* relTableEntry;                  /* pointer to an entry of relation table */

    TupleHdr            concatnatedTupHdr;              /* a tuple header */
    Four                concatnatedTupHdrSize;          /* size of tuple header */
    Four                concatnatedNElementsOfCol;      /* # of elements of the column */
    ColDesc*            concatnatedCdesc;               /* pointer to the current column descriptor */
    lrds_RelTableEntry* concatnatedRelTableEntry;       /* pointer to an entry of relation table */
    Four                ith;
    Four                nReads;
    Four                elementsSize;
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                i;
    LockParameter       fileLockup;                     /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;                      /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;                 /* pointer to the lockup value */
    LockParameter       *objLockupPtr;                  /* pointer to the lockup value */

    LockParameter       concatnatedFileLockup;          /* lockup for SM_Fetch Tuple */
    LockParameter       concatnatedObjLockup;           /* lockup for SM_Fetch Tuple */
    LockParameter       *concatnatedFileLockupPtr;      /* pointer to the lockup value */
    LockParameter       *concatnatedObjLockupPtr;       /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;           
    ColDesc             *concatnatedRelTableEntry_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Concatenate(handle)"));

    /* Check parameters*/
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    if (concatnatedUseScanFlag == TRUE && !LRDS_VALID_SCANID(handle, concatnatedOrnOrScanId)) ERR(handle, eBADPARAMETER);

    if (concatnatedUseScanFlag == FALSE && !LRDS_VALID_ORN(handle, concatnatedOrnOrScanId)) ERR(handle, eBADPARAMETER);

    if (concatnatedUseScanFlag) {
        concatnatedOrn = LRDS_SCANTABLE(handle)[concatnatedOrnOrScanId].orn;
        concatnatedSmScanId = LRDS_SCANTABLE(handle)[concatnatedOrnOrScanId].smScanId;
    } else {
        concatnatedOrn = concatnatedOrnOrScanId;
        concatnatedSmScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    concatnatedRelTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, concatnatedOrn);
    concatnatedRelTableEntry_cdesc = PHYSICAL_PTR(concatnatedRelTableEntry->cdesc); 
    concatnatedCdesc         = &concatnatedRelTableEntry_cdesc[concatnatedColNo];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    if (concatnatedColNo >= concatnatedRelTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (concatnatedRelTableEntry_cdesc[concatnatedColNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;
    if (concatnatedUseScanFlag && concatnatedTid == NULL) concatnatedTid = &LRDS_SCANTABLE(handle)[concatnatedOrnOrScanId].tid;

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    if (LRDS_USEROPENRELTABLE(handle)[concatnatedOrn].tmpRelationFlag) {
        concatnatedFileLockupPtr = concatnatedObjLockupPtr = NULL;

    } else {
        concatnatedFileLockup.mode = L_IS;
        concatnatedFileLockup.duration = L_COMMIT;
        concatnatedFileLockupPtr = &concatnatedFileLockup;

        concatnatedObjLockup.mode = L_S;
        concatnatedObjLockup.duration = L_COMMIT;
        concatnatedObjLockupPtr = &concatnatedObjLockup;
    }

    /* Get tupHdr */
    concatnatedTupHdrSize = TUPLE_HEADER_SIZE(concatnatedRelTableEntry->ri.nColumns, concatnatedRelTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &concatnatedRelTableEntry->ri.fid, concatnatedSmScanId, concatnatedUseScanFlag,
                         (ObjectID*)concatnatedTid, 0, concatnatedTupHdrSize, (char*)&concatnatedTupHdr,
                         (concatnatedRelTableEntry->isCatalog) ? NULL : concatnatedFileLockupPtr,
                         (concatnatedRelTableEntry->isCatalog) ? NULL : concatnatedObjLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    concatnatedTupHdrSize = TUPLE_HEADER_SIZE(concatnatedTupHdr.nVarCols + concatnatedTupHdr.nFixedCols, concatnatedTupHdr.nVarCols);

    /* Get # of elements of column */
    e = lrds_Collection_GetN_Elements(handle, concatnatedOrnOrScanId, concatnatedUseScanFlag, concatnatedTid, &concatnatedTupHdr,
                                      concatnatedColNo, &concatnatedNElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if(concatnatedRelTableEntry_cdesc[concatnatedColNo].type == SM_VARSTRING)
    {   /* variable size element */
        ith = 0;
        while(concatnatedNElementsOfCol > 0)
        {
            /* read elementSizes */
            nReads = sizeof(elementsBuf) / sizeof(Four);
            if(nReads > concatnatedNElementsOfCol)
                nReads = concatnatedNElementsOfCol;

            e = lrds_Collection_VarElementFetchSizes(handle, concatnatedOrnOrScanId, concatnatedUseScanFlag, concatnatedTid, &concatnatedTupHdr,
                                                     concatnatedColNo,ith, nReads,
                                                     (Four*)elementSizesBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* calculate nReads for elementsBuf*/
            for(i = 0, elementsSize = 0; i < nReads; i++)
            {
                if((elementsSize + ((Four*)elementSizesBuf)[i]) > sizeof(elementsBuf))
                    break;
                elementsSize += ((Four*)elementSizesBuf)[i];
            }
            nReads = i;

            /* read elements */
            e = lrds_Collection_VarElementFetch(handle, concatnatedOrnOrScanId, concatnatedUseScanFlag, concatnatedTid, &concatnatedTupHdr,
                                                concatnatedColNo, ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* append elements */
            e = LRDS_CollectionList_AppendElements(handle, ornOrScanId, useScanFlag, tid, colNo, nReads,
                                                   (Four*)elementSizesBuf, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            concatnatedNElementsOfCol -= nReads;
            ith            += nReads;
        }
    }
    else
    {   /* fixed size element */
        ith = 0;
        while(concatnatedNElementsOfCol > 0)
        {
            nReads = sizeof(elementsBuf) / concatnatedCdesc->length;
            if(nReads > concatnatedNElementsOfCol)
                nReads = concatnatedNElementsOfCol;

            /* read elements */
            e = lrds_Collection_FixedElementFetch(handle, concatnatedOrnOrScanId, concatnatedUseScanFlag, concatnatedTid, &concatnatedTupHdr,
                                                  concatnatedColNo, ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* append elements */
            e = LRDS_CollectionList_AppendElements(handle, ornOrScanId, useScanFlag, tid, colNo, nReads, NULL, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            concatnatedNElementsOfCol -= nReads;
            ith                       += nReads;
        }
    }

    return eNOERROR;
}

Four LRDS_CollectionList_Resize(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                size)                   /* IN new size of list */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                nToAppends;
    Four                nAppends;
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_InsertElements(handle)"));

    /* Check parameters*/
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
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        fileLockupPtr = objLockupPtr = NULL;

    } else {
        fileLockup.mode = L_IX;
        fileLockup.duration = L_COMMIT;
        fileLockupPtr = &fileLockup;

        objLockup.mode = L_X;
        objLockup.duration = L_COMMIT;
        objLockupPtr = &objLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Get nElementsOfCol */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if(size > nElementsOfCol)
    {
        memset(elementsBuf, 0, sizeof(elementsBuf));
        memset(elementSizesBuf, 0, sizeof(elementSizesBuf));

        if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
        {   /* variable size element */
            nToAppends = size - nElementsOfCol;
            while(nToAppends > 0)
            {
                nAppends = sizeof(elementsBuf) / sizeof(Four);
                if(nAppends > nToAppends)
                    nAppends = nToAppends;

                e = LRDS_CollectionList_AppendElements(handle, ornOrScanId, useScanFlag, tid, colNo, nAppends,
                                                       (Four*)elementSizesBuf, elementsBuf);
                if(e < eNOERROR) ERR(handle, e);

                nToAppends -= nAppends;
            }
        }
        else
        {   /* fixed size element */
            nToAppends = size - nElementsOfCol;
            while(nToAppends > 0)
            {
                nAppends = sizeof(elementsBuf) / cdesc->length;
                if(nAppends > nToAppends)
                    nAppends = nToAppends;

                e = LRDS_CollectionList_AppendElements(handle, ornOrScanId, useScanFlag, tid, colNo, nAppends, NULL, elementsBuf);
                if(e < eNOERROR) ERR(handle, e);

                nToAppends -= nAppends;
            }
        }
    }
    else if(size < nElementsOfCol)
    {
        if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
        {   /* variable size element */
            e = lrds_Collection_VarElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                                 colNo, size, nElementsOfCol - size);
            if(e < eNOERROR) ERR(handle, e);
        }
        else
        {   /* fixed size element */
            e = lrds_Collection_FixedElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                                   colNo, size, nElementsOfCol - size);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* Set tupHdr */
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                              0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
        if (e < 0) ERR(handle, e);

    }

    return eNOERROR;
}

Four LRDS_CollectionList_IsMember(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                elementSize,            /* IN size of element */
    void*               element,                /* IN element */
    Four*               pos)                    /* OUT element position in list */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    Four                nReads;                 /* # of reads */
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    Four                ith;
    Four                i;
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                elementsSize;
    char*               comparedElement;
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_InsertElements(handle)"));

    /* Check parameters*/
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
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

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

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        ith = 0;
        while(nElementsOfCol > 0)
        {
            /* read elementSizes */
            nReads = sizeof(elementsBuf) / sizeof(Four);
            if(nReads > nElementsOfCol)
                nReads = nElementsOfCol;

            e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                     ith, nReads,
                                                     (Four*)elementSizesBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* calculate nReads for elementsBuf*/
            for(i = 0, elementsSize = 0; i < nReads; i++)
            {
                if((elementsSize + ((Four*)elementSizesBuf)[i]) > sizeof(elementsBuf))
                    break;
                elementsSize += ((Four*)elementSizesBuf)[i];
            }
            nReads = i;

            /* read elements */
            e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* compare each elements in elementsBuf with element */
            comparedElement = elementsBuf;

            for(i = 0; i < nReads; i++)
            {
                if(elementSize == ((Four*)elementSizesBuf)[i])
                    if(!memcmp(element, comparedElement, elementSize))
                    {
                        *pos = ith;
                        return TRUE;
                    }

                comparedElement += elementSize;
            }

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }
    else
    {   /* fixed size element */
        ith = 0;
        while(nElementsOfCol > 0)
        {
            nReads = sizeof(elementsBuf) / cdesc->length;
            if(nReads > nElementsOfCol)
                nReads = nElementsOfCol;

            /* read elements */
            e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                  ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* compare each elements in elementsBuf with element */
            comparedElement = elementsBuf;
            elementSize     = cdesc->length;
            for(i = 0; i < nReads; i++)
            {
                if(!memcmp(element, comparedElement, elementSize))
                {
                    *pos = ith;
                    return TRUE;
                }

                comparedElement += elementSize;
            }

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }

    return FALSE;
}

Four LRDS_CollectionList_IsEqual(
    Four handle,
    Four                ornOrScanId,                    /* IN open relation no or scan id*/
    Boolean             useScanFlag,                    /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                            /* IN tuple containing the set */
    Two                 colNo,                          /* IN column on which the set is defined */
    Four                comparedOrnOrScanId,            /* IN open relation no or scan id*/
    Boolean             comparedUseScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            comparedTid,                    /* IN tuple containing the set */
    Two                 comparedColNo)                  /* IN compared column on which the set is defined */
{
    Four                e;                              /* error code */
    Four                orn;
    Four                smScanId;
    Four                comparedOrn;
    Four                comparedSmScanId;
    TupleHdr            tupHdr;                         /* a tuple header */
    Four                tupHdrSize;                     /* size of tuple header */
    Four                nElementsOfCol;                 /* # of elements of the column */
    ColDesc*            cdesc;                          /* pointer to the current column descriptor */
    lrds_RelTableEntry* relTableEntry;                  /* pointer to an entry of relation table */

    TupleHdr            comparedTupHdr;                 /* a tuple header */
    Four                comparedTupHdrSize;             /* size of tuple header */
    Four                comparedNElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* comparedRelTableEntry;          /* pointer to an entry of relation table */

    Four                nReads;
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    char                comparedElementsBuf[PAGESIZE];
    char                comparedElementSizesBuf[PAGESIZE];
    Four                elementsSize;
    char*               element;
    Four                elementSize;
    char*               comparedElement;
    Four                i;
    Four                ith;
    LockParameter       fileLockup;                     /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;                      /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;                 /* pointer to the lockup value */
    LockParameter       *objLockupPtr;                  /* pointer to the lockup value */

    LockParameter       comparedFileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       comparedObjLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *comparedFileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *comparedObjLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;           
    ColDesc             *comparedRelTableEntry_cdesc;  

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_IsEqual(handle)"));

    /* Check parameters*/
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    if (comparedUseScanFlag == TRUE && !LRDS_VALID_SCANID(handle, comparedOrnOrScanId)) ERR(handle, eBADPARAMETER);

    if (comparedUseScanFlag == FALSE && !LRDS_VALID_ORN(handle, comparedOrnOrScanId)) ERR(handle, eBADPARAMETER);

    if (comparedUseScanFlag) {
        comparedOrn = LRDS_SCANTABLE(handle)[comparedOrnOrScanId].orn;
        comparedSmScanId = LRDS_SCANTABLE(handle)[comparedOrnOrScanId].smScanId;
    } else {
        comparedOrn = comparedOrnOrScanId;
        comparedSmScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry         = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc                 = &relTableEntry_cdesc[colNo];
    comparedRelTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, comparedOrn);
    comparedRelTableEntry_cdesc = PHYSICAL_PTR(comparedRelTableEntry->cdesc); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    if (comparedColNo >= comparedRelTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (comparedRelTableEntry_cdesc[comparedColNo].complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;
    if (comparedUseScanFlag && comparedTid == NULL) comparedTid = &LRDS_SCANTABLE(handle)[comparedOrnOrScanId].tid;

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

    if (LRDS_USEROPENRELTABLE(handle)[comparedOrn].tmpRelationFlag) {
        comparedFileLockupPtr = comparedObjLockupPtr = NULL;

    } else {
        comparedFileLockup.mode = L_IS;
        comparedFileLockup.duration = L_COMMIT;
        comparedFileLockupPtr = &comparedFileLockup;

        comparedObjLockup.mode = L_S;
        comparedObjLockup.duration = L_COMMIT;
        comparedObjLockupPtr = &comparedObjLockup;
    }

    /* Get tupHdr */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    comparedTupHdrSize = TUPLE_HEADER_SIZE(comparedRelTableEntry->ri.nColumns, comparedRelTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &comparedRelTableEntry->ri.fid, comparedSmScanId, comparedUseScanFlag,
                         (ObjectID*)comparedTid, 0, comparedTupHdrSize, (char*)&comparedTupHdr,
                         (comparedRelTableEntry->isCatalog) ? NULL : comparedFileLockupPtr,
                         (comparedRelTableEntry->isCatalog) ? NULL : comparedObjLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    comparedTupHdrSize = TUPLE_HEADER_SIZE(comparedTupHdr.nVarCols + comparedTupHdr.nFixedCols, comparedTupHdr.nVarCols);

    /* Get # of elements of column */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid, &comparedTupHdr,
                                      comparedColNo, &comparedNElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);


    /* check size */
    if(nElementsOfCol != comparedNElementsOfCol)
        return FALSE;

    /* check elements type */
    if(relTableEntry_cdesc[colNo].type != comparedRelTableEntry_cdesc[comparedColNo].type)
        return FALSE;

    /* check contents */
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        ith = 0;
        while(nElementsOfCol > 0)
        {
            /* read elementSizes */
            nReads = sizeof(elementsBuf) / sizeof(Four);
            if(nReads > nElementsOfCol)
                nReads = nElementsOfCol;

            e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                     ith, nReads,
                                                     (Four*)elementSizesBuf);
            if(e < eNOERROR) ERR(handle, e);

            e = lrds_Collection_VarElementFetchSizes(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid, &comparedTupHdr,
                                                     comparedColNo, ith, nReads,
                                                     (Four*)comparedElementSizesBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* check elements sizes */
            for(i = 0; i < nReads; i++)
                if(((Four*)elementSizesBuf)[i] != ((Four*)comparedElementSizesBuf)[i])
                    return FALSE;

            /* calculate nReads for elementsBuf*/
            for(i = 0, elementsSize = 0; i < nReads; i++)
            {
                if((elementsSize + ((Four*)elementSizesBuf)[i]) > sizeof(elementsBuf))
                    break;
                elementsSize += ((Four*)elementSizesBuf)[i];
            }
            nReads = i;

            /* read elements */
            e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            e = lrds_Collection_VarElementFetch(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid, &comparedTupHdr,
                                                comparedColNo, ith, nReads,
                                                comparedElementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* remove elements from index */
            element         = elementsBuf;
            comparedElement = comparedElementsBuf;
            for(i = 0; i < nReads; i++)
            {
                elementSize = ((Four*)elementSizesBuf)[i];
                if(memcmp(element, comparedElement, elementSize))
                    return FALSE;

                element         += elementSize;
                comparedElement += elementSize;
            }

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }
    else
    {   /* fixed size element */
        ith = 0;
        while(nElementsOfCol > 0)
        {
            nReads = sizeof(elementsBuf) / cdesc->length;
            if(nReads > nElementsOfCol)
                nReads = nElementsOfCol;

            /* read elements */
            e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                  ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            e = lrds_Collection_FixedElementFetch(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid, &comparedTupHdr,
                                                  comparedColNo, ith, nReads, comparedElementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* compare elements */
            element         = elementsBuf;
            comparedElement = comparedElementsBuf;
            elementSize     = cdesc->length;
            for(i = 0; i < nReads; i++)
            {
                if(memcmp(element, comparedElement, elementSize))
                    return FALSE;

                element         += elementSize;
                comparedElement += elementSize;
            }

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }

    return TRUE;
}

Four LRDS_CollectionList_Scan_Open(
    Four handle,
    Four        ornOrScanId,        /* IN open relation no or scan id*/
    Boolean     useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID*    tid,                /* IN tuple containing the set */
    Two         colNo)              /* IN column on which the set is defined */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Scan_Open(handle)"));

    return lrds_Collection_Scan_Open(handle, ornOrScanId, useScanFlag, tid, colNo);
}

Four LRDS_CollectionList_Scan_Close(
    Four handle,
    Four        collectionScanId)   /* IN scan id for a scan on a set */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Scan_Close(handle)"));

    return lrds_Collection_Scan_Close(handle, collectionScanId);
}

Four LRDS_CollectionList_Scan_NextElements(
    Four handle,
    Four        collectionScanId,   /* IN scan id for a scan on a set */
    Four        nElements,          /* IN # of elements to read */
    Four*       elementSizes,       /* OUT buffer to return the element sizes for variable elements */
    Four        sizeOfElements,     /* IN size of elements */
    void*       elements)           /* OUT buffer to return the read elements */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Scan_NextElements(handle)"));

    return lrds_Collection_Scan_NextElements(handle, collectionScanId, nElements, elementSizes, sizeOfElements, elements);
}

Four LRDS_CollectionList_Scan_GetSizeOfNextElements(
    Four handle,
    Four        collectionScanId,   /* IN scan id for a scan on a list */
    Four        nElements,          /* IN # of elements to read */
    Four*       elementsSize)       /* OUT sizeof elements to read */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Scan_GetSizeOfNextElements(handle)"));

    return lrds_Collection_Scan_GetSizeOfNextElements(handle, collectionScanId, nElements, elementsSize);
}

Four LRDS_CollectionList_Scan_InsertElements(
    Four handle,
    Four        collectionScanId,   /* IN set where to insert the elements */
    Four        nElements,          /* IN # of elements to insert */
    Four*       elementSizes,       /* IN element sizes of elements to insert */
    void*       elements)           /* IN elements to insert */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Scan_InsertElements(handle)"));

    return lrds_Collection_Scan_InsertElements(handle, collectionScanId, nElements, elementSizes, elements);
}

Four LRDS_CollectionList_Scan_DeleteElements(
    Four handle,
    Four        collectionScanId)   /* IN set from which to delete the element */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_Scan_DeleteElements(handle)"));

    return lrds_Collection_Scan_DeleteElements(handle, collectionScanId);
}

/*
 * Function: Four LRDS_CollectionList_IsNull(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Check where the given colNo is null or not.
 *
 * Retuns:
 *  error code
 */
Four LRDS_CollectionList_IsNull(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Two                 colNo                   /* IN column on which the set is created */
)
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionList_IsNull(handle)"));


    return lrds_Collection_IsNull(handle, ornOrScanId, useScanFlag, tid, colNo);
}
