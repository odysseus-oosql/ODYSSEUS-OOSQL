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
 * Module: LRDS_CollectionBag.c
 *
 * Description:
 *  Implements a Bag.
 *
 * Imports:
 *
 *
 * Exports:
 *  Four LRDS_CollectionBag_Create(Four, Boolean, TupleID*, Two, Four)
 *  Four LRDS_CollectionBag_Destroy(Four, Boolean, TupleID*, Two)
 *  Four LRDS_CollectionBag_InsertElements(Four, Boolean, TupleID*, Two, Four, Four*, void*)
 *  Four LRDS_CollectionBag_DeleteElements(Four, Boolean, TupleID*, Two, Four, Four*, void*)
 *  Four LRDS_CollectionBag_IsMember(Four, Boolean, TupleID*, Two, Four, void*)
 *  Four LRDS_CollectionBag_Scan_Open(Four, Boolean, TupleID*, Two)
 *  Four LRDS_CollectionBag_Scan_Close(Four)
 *  Four LRDS_CollectionBag_Scan_NextElements(Four, Four, Four*, Four, void*)
 *  Four LRDS_CollectionBag_Scan_InsertElements(Four, Four, Four*, void*)
 *  Four LRDS_CollectionBag_Scan_DeleteElements(Four)
 *
 * Notice:
 *  The input parameter relScanId is used to identify the relation which
 *  contains the given bag. We don't use a scan identified by the relScanId.
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

Four lrds_CollectionBag_FixedElementSearch(Four, Four, Boolean, TupleID*, TupleHdr*, Two, void*, Four*);
Four lrds_CollectionBag_VarElementSearch(Four, Four, Boolean, TupleID*, TupleHdr*, Two, Four, void*, Four*);

Four LRDS_CollectionBag_Create(
    Four handle,
    Four                        ornOrScanId,            /* IN open relation no or scan id*/
    Boolean                     useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*                    tid,                    /* IN tuple containing the given column */
    Two                         colNo,                  /* IN column on which the bag is created */
    Four                        keySize)                /* IN keysize in varstring, string */
{
    Four                        e;                      /* error code */
    Four                        orn;
    Four                        smScanId;
    lrds_RelTableEntry          *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc                     *relTableEntry_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Create(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* create collection */
    e = lrds_Collection_Create(handle, ornOrScanId, useScanFlag, tid, colNo, keySize);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_Destroy(
    Four handle,
    Four                        ornOrScanId,            /* IN open relation no or scan id*/
    Boolean                     useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*                    tid,                    /* IN tuple containing the given column */
    Two                         colNo)                  /* IN column on which we destroy the bag  */
{
    Four                        e;                      /* error code */
    Four                        orn;
    Four                        smScanId;
    TupleHdr                    tupHdr;                 /* a tuple header */
    Four                        tupHdrSize;             /* size of tuple header */
    Four                        nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry*         relTableEntry;          /* pointer to an entry of relation table */
    LockParameter               fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter               objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter               *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter               *objLockupPtr;          /* pointer to the lockup value */
    ColDesc                     *relTableEntry_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Destroy(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    e = lrds_Collection_Destroy(handle, ornOrScanId, useScanFlag, tid, colNo);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_AssignElements(
    Four handle,
    Four                        ornOrScanId,            /* IN open relation no or scan id*/
    Boolean                     useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*                    tid,                    /* IN tuple containing the bag */
    Two                         colNo,                  /* IN column on which the bag is defined */
    Four                        nElements,              /* IN # of elements to insert */
    Four*                       elementSizes,           /* IN sizes of elements, if element size is fixed, then pass NULL */
    void*                       elements)               /* IN elements to insert */
{
    Four                        e;                      /* error code */
    Four                        orn;
    Four                        smScanId;
    TupleHdr                    tupHdr;                 /* a tuple header */
    Four                        tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry*         relTableEntry;          /* pointer to an entry of relation table */
    LockParameter               fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter               objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter               *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter               *objLockupPtr;          /* pointer to the lockup value */
    ColDesc                     *relTableEntry_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_AssignElement()"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* Delete all contents */
    e = LRDS_CollectionBag_DeleteAll(handle, ornOrScanId, useScanFlag, tid, colNo);
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

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_Assign(
    Four handle,
    Four                        ornOrScanId,            /* IN open relation no or scan id*/
    Boolean                     useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*                    tid,                    /* IN tuple containing the bag */
    Two                         colNo,                  /* IN column on which the bag is defined */
    Four                        assignedOrnOrScanId,    /* IN assigned relation containing the bag*/
    Boolean                     assignedUseScanFlag,    /* IN TRUE if above parameter is scan id */
    TupleID*                    assignedTid,            /* IN assigned tuple containing the bag */
    Two                         assignedColNo)          /* IN assigned column on which the bag is defined */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Assign(handle)"));

    return lrds_Collection_Assign(handle, ornOrScanId, useScanFlag, tid, colNo,
                                  assignedOrnOrScanId, assignedUseScanFlag, assignedTid,
                                  assignedColNo);
}

Four      LRDS_CollectionBag_GetN_Elements(
    Four handle,
    Four                        ornOrScanId,            /* IN open relation no or scan id*/
    Boolean                     useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*                    tid,                    /* IN tuple containing the bag */
    Two                         colNo,                  /* IN column on which the bag is defined */
    Four*                       nElements)              /* OUT nElements in the column */
{
    Four                        e;                      /* error code */
    Four                        orn;
    Four                        smScanId;
    TupleHdr                    tupHdr;                 /* a tuple header */
    Four                        tupHdrSize;             /* size of tuple header */
    lrds_RelTableEntry*         relTableEntry;          /* pointer to an entry of relation table */
    LockParameter               fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter               objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter               *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter               *objLockupPtr;          /* pointer to the lockup value */
    ColDesc                     *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_GetN_Elements(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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

    /* Get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, nElements);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_InsertElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo,                  /* IN column on which the bag is defined */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes,           /* IN sizes of elements, if element size is fixed, then pass NULL */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    char*               element;
    Four                elementSize;
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    Four                i;
    Four                ith;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_InsertElements(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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
        element = elements;
        for(i = 0; i < nElements; i++)
        {
            elementSize = ((Four*)elementSizes)[i];

            /* check if element already inserted */
            /* or find position of the element through variable ith */
            e = lrds_CollectionBag_VarElementSearch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                    elementSize, element, &ith);
            if(e < eNOERROR) ERR(handle, e);

            /* insert element into ith position */
            e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                                 1, &elementSize, element);
            if(e < eNOERROR) ERR(handle, e);

            element += elementSize;
        }
    }
    else
    {   /* fixed size element */
        element = elements;
        for(i = 0; i < nElements; i++)
        {
            /* check if element already inserted */
            /* or find position of the element through variable ith */
            e = lrds_CollectionBag_FixedElementSearch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                      element, &ith);
            if(e < eNOERROR) ERR(handle, e);

            /* insert element into ith position */
            e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith,
                                                   1, element);
            if(e < eNOERROR) ERR(handle, e);

            element += cdesc->length;
        }
    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_DeleteElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo,                  /* IN column on which the bag is defined */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes,           /* IN sizes of elements, if element size is fixed, then pass NULL */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    char*               element;
    Four                elementSize;
    Four                i;
    Four                ith;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_DeleteElements(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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
        element = elements;
        for(i = 0; i < nElements; i++)
        {
            elementSize = ((Four*)elementSizes)[i];

            /* check if element already inserted */
            /* or find position of the element through variable ith */
            e = lrds_CollectionBag_VarElementSearch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                    elementSize, element, &ith);
            if(e < eNOERROR) ERR(handle, e);
            if(e == FALSE)   ERR(handle, eCOLLECTIONBAG_ELEMENTNOTFOUND_LRDS);

            /* delete element from ith position */
            e = lrds_Collection_VarElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith, 1);
            if(e < eNOERROR) ERR(handle, e);

            element += elementSize;
        }
    }
    else
    {   /* fixed size element */
        element = elements;
        for(i = 0; i < nElements; i++)
        {
            /* check if element already inserted */
            /* or find position of the element through variable ith */
            e = lrds_CollectionBag_FixedElementSearch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                      element, &ith);
            if(e < eNOERROR) ERR(handle, e);
            if(e == FALSE)    ERR(handle, eCOLLECTIONBAG_ELEMENTNOTFOUND_LRDS);

            /* delete element from ith position */
            e = lrds_Collection_FixedElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, ith, 1);
            if(e < eNOERROR) ERR(handle, e);

            element += cdesc->length;
        }
    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_DeleteAll(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo)                  /* IN column on which the bag is defined */
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
    Four                nElementsOfCol;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_DeleteAll(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_IsMember(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo,                  /* IN column on which the bag is defined */
    Four                elementSize,            /* IN size of element */
    void*               element)                /* IN element */
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
    Four                ith;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_IsMember(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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
        /* check if element already inserted */
        e = lrds_CollectionBag_VarElementSearch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                elementSize, element, &ith);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        /* check if element already inserted */
        e = lrds_CollectionBag_FixedElementSearch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                  element, &ith);
        if(e < eNOERROR) ERR(handle, e);

    }

    if(e == TRUE)
        return TRUE;
    else
        return FALSE;
}

Four LRDS_CollectionBag_IsEqual(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo,                  /* IN column on which the bag is defined */
    Four                comparedOrnOrScanId,    /* IN compared relation containing the bag*/
    Boolean             comparedUseScanFlag,    /* IN TRUE if above parameter is scan id */
    TupleID*            comparedTid,            /* IN compared tuple containing the bag */
    Two                 comparedColNo)          /* IN compared column on which the bag is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    Four                nElementsOfCol;         /* # of elements of the column */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */

    Four                comparedOrn;
    Four                comparedSmScanId;
    TupleHdr            comparedTupHdr;         /* a tuple header */
    Four                comparedTupHdrSize;     /* size of tuple header */
    Four                comparedNElementsOfCol; /* # of elements of the column */
    lrds_RelTableEntry* comparedRelTableEntry;  /* pointer to an entry of relation table */
    Four                nReads;
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    char                comparedElementsBuf[PAGESIZE];
    char                comparedElementSizesBuf[PAGESIZE];
    Four                elementsSize;
    char*               element;
    Four                elementSize;
    Four                 keySize;
    char*               comparedElement;
    Four                 comparedKeySize;
    Four                i;
    Four                ith;

    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */

    LockParameter       comparedFileLockup;     /* lockup for SM_Fetch Tuple */
    LockParameter       comparedObjLockup;      /* lockup for SM_Fetch Tuple */
    LockParameter       *comparedFileLockupPtr; /* pointer to the lockup value */
    LockParameter       *comparedObjLockupPtr;  /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   
    ColDesc             *comparedRelTableEntry_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_IsEqual(handle)"));

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
    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (comparedColNo >= comparedRelTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (comparedRelTableEntry_cdesc[comparedColNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &keySize);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid, &comparedTupHdr, comparedColNo, &comparedKeySize);
    if(e < eNOERROR) ERR(handle, e);
    if(keySize != comparedKeySize)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

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

            /* calculate nReads for elementsBuf */
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

            /* compare elements */
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

Four LRDS_CollectionBag_IsSubset(
    Four handle,
    Four                ornOrScanId,                    /* IN open relation no or scan id*/
    Boolean             useScanFlag,                    /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                            /* IN tuple containing the bag */
    Two                 colNo,                          /* IN column on which the bag is defined */
    Four                comparedOrnOrScanId,            /* IN compared relation containing the bag*/
    Boolean             comparedUseScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            comparedTid,                    /* IN compared tuple containing the bag */
    Two                 comparedColNo)                  /* IN compared column on which the bag is defined */

{
    Four                e;                              /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                         /* a tuple header */
    Four                tupHdrSize;                     /* size of tuple header */
    ColDesc*            cdesc;                          /* pointer to the current column descriptor */
    Four                nElementsOfCol;                 /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;                  /* pointer to an entry of relation table */

    Four                comparedOrn;
    Four                comparedSmScanId;
    TupleHdr            comparedTupHdr;                 /* a tuple header */
    Four                comparedTupHdrSize;             /* size of tuple header */
    ColDesc*            comparedCdesc;                  /* pointer to the current column descriptor */
    Four                comparedNElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* comparedRelTableEntry;          /* pointer to an entry of relation table */

    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                startNoBuf;
    Four                nElementsInBuf;
    char*               element;
    Four                elementSize;
    Four                 keySize;

    Four                comparedStartNoBuf;
    Four                comparedNElementsInBuf;
    char                comparedElementsBuf[PAGESIZE];
    char                comparedElementSizesBuf[PAGESIZE];
    char*               comparedElement;
    Four                comparedElementSize;
    Four                 comparedKeySize;

    Four                i, j;
    Four                index;
    Four 		compareResult;

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

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_IsSubset(handle)"));

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
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc          = &relTableEntry_cdesc[colNo];
    comparedRelTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, comparedOrn);
    comparedRelTableEntry_cdesc = PHYSICAL_PTR(comparedRelTableEntry->cdesc); 
    comparedCdesc         = &comparedRelTableEntry_cdesc[comparedColNo];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (comparedColNo >= comparedRelTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (comparedRelTableEntry_cdesc[comparedColNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;
    if (comparedUseScanFlag && comparedTid == NULL) comparedTid = &LRDS_SCANTABLE(handle)[comparedOrnOrScanId].tid;

    /* test bag compatibility */
    if(!(relTableEntry_cdesc[colNo].type == comparedRelTableEntry_cdesc[comparedColNo].type))
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

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

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &keySize);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid, &comparedTupHdr, comparedColNo, &comparedKeySize);
    if(e < eNOERROR) ERR(handle, e);
    if(keySize != comparedKeySize)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid, &comparedTupHdr,
                                      comparedColNo, &comparedNElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    /* shortcuts */
    if(nElementsOfCol < comparedNElementsOfCol)
        return FALSE;

    /* initialize read buffers */
    startNoBuf      = 0;
    nElementsInBuf  = 0;

    comparedStartNoBuf     = 0;
    comparedNElementsInBuf = 0;

    i = 0; j = 0;
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        while(i < nElementsOfCol || j < comparedNElementsOfCol)
        {
            if(i < nElementsOfCol &&
               (nElementsInBuf == 0 ||  (startNoBuf + nElementsInBuf) <= i))
            {
                Four nReads;
                Four elementsSize;

                startNoBuf += nElementsInBuf;

                /* read elementSizes */
                nReads = sizeof(elementSizesBuf) / sizeof(Four);
                if(nReads > nElementsOfCol)
                    nReads = nElementsOfCol;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                                         colNo, startNoBuf, nReads,
                                                         (Four*)elementSizesBuf);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf*/
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBuf)[index]) > sizeof(elementsBuf))
                        break;
                    elementsSize += ((Four*)elementSizesBuf)[index];
                }
                nElementsInBuf = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                    startNoBuf, nElementsInBuf, elementsBuf);
                if(e < eNOERROR) ERR(handle, e);

                element     = elementsBuf;
                elementSize = ((Four*)elementSizesBuf)[0];
            }

            if(j < comparedNElementsOfCol &&
               (comparedNElementsInBuf == 0 || (comparedStartNoBuf + comparedNElementsInBuf) <= j))
            {
                Four nReads;
                Four elementsSize;

                comparedStartNoBuf += comparedNElementsInBuf;

                /* read elementSizes */
                nReads = sizeof(comparedElementSizesBuf) / sizeof(Four);
                if(nReads > comparedNElementsOfCol)
                    nReads = comparedNElementsOfCol;

                e = lrds_Collection_VarElementFetchSizes(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid,
                                                         &comparedTupHdr,
                                                         comparedColNo, comparedStartNoBuf,
                                                         nReads,
                                                         (Four*)comparedElementSizesBuf);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf */
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)comparedElementSizesBuf)[index]) > sizeof(comparedElementsBuf))
                        break;
                    elementsSize += ((Four*)comparedElementSizesBuf)[index];
                }
                comparedNElementsInBuf = index;

                e = lrds_Collection_VarElementFetch(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid,
                                                    &comparedTupHdr, comparedColNo,
                                                    comparedStartNoBuf,
                                                    comparedNElementsInBuf,
                                                    comparedElementsBuf);
                if(e < eNOERROR) ERR(handle, e);

                comparedElement     = comparedElementsBuf;
                comparedElementSize = ((Four*)comparedElementSizesBuf)[0];
            }

            if(i >= nElementsOfCol)
            {
                if(j >= comparedNElementsOfCol)
                    return TRUE;
                else
                    return FALSE;
            }
            else if(j >= comparedNElementsOfCol)
            {
                return TRUE;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdesc->type, elementSize, element, comparedElementSize, comparedElement, keySize);

                if(compareResult == 0)                        /* element == comparedElement */
                {
                    i++;
                    j++;
                    element    += elementSize;
                    elementSize = ((Four*)elementSizesBuf)[i - startNoBuf];
                    comparedElement    += comparedElementSize;
                    comparedElementSize = ((Four*)comparedElementSizesBuf)[j - comparedStartNoBuf];
                }
                else if(compareResult < 0)                    /* element < comparedElement */
                {
                    i++;
                    element    += elementSize;
                    elementSize = ((Four*)elementSizesBuf)[i - startNoBuf];
                }
                else if(compareResult > 0)                    /* element > comparedElement */
                {
                    return FALSE;
                }
            }
        }
    }
    else
    {   /* fixed size element */
        while(i < nElementsOfCol || j < comparedNElementsOfCol)
        {
            if(i < nElementsOfCol &&
               (nElementsInBuf == 0 || (startNoBuf + nElementsInBuf) <= i))
            {
                Four temp;

                temp = nElementsInBuf;
                nElementsInBuf = (sizeof(elementsBuf) / cdesc->length);
                if((nElementsOfCol - startNoBuf) < nElementsInBuf)
                    nElementsInBuf = nElementsOfCol - startNoBuf;
                startNoBuf += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                      startNoBuf, nElementsInBuf,
                                                      elementsBuf);
                if(e < eNOERROR) ERR(handle, e);

                element = elementsBuf;
            }

            if(j < comparedNElementsOfCol &&
               (comparedNElementsInBuf == 0 || (comparedStartNoBuf + comparedNElementsInBuf) <= j))
            {
                Four temp;

                temp = comparedNElementsInBuf;
                comparedNElementsInBuf = (sizeof(comparedElementsBuf) / comparedCdesc->length);
                if((comparedNElementsOfCol - comparedStartNoBuf) < comparedNElementsInBuf)
                    comparedNElementsInBuf = comparedNElementsOfCol - comparedStartNoBuf;
                comparedStartNoBuf += temp;

                e = lrds_Collection_FixedElementFetch(handle, comparedOrnOrScanId, comparedUseScanFlag, comparedTid,
                                                      &comparedTupHdr, comparedColNo,
                                                      comparedStartNoBuf,
                                                      comparedNElementsInBuf,
                                                      comparedElementsBuf);
                if(e < eNOERROR) ERR(handle, e);

                comparedElement = comparedElementsBuf;
            }

            if(i >= nElementsOfCol)
            {
                if(j >= comparedNElementsOfCol)
                    return TRUE;
                else
                    return FALSE;
            }
            else if(j >= comparedNElementsOfCol)
            {
                return TRUE;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdesc->type, cdesc->length, element, cdesc->length, comparedElement, keySize);

                if(compareResult == 0)          /* elementA == elementB */
                {
                    element         += cdesc->length;
                    comparedElement += cdesc->length;
                    i++;
                    j++;
                }
                else if(compareResult < 0)      /* elementA < elementB */
                {
                    element += cdesc->length;
                    i++;
                }
                else if(compareResult > 0)      /* elementA > elementB */
                {
                    return FALSE;
                }
            }
        }
    }

    return TRUE;
}

Four LRDS_CollectionBag_GetSizeOfElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo,                  /* IN column on which the bag is defined */
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

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_GetSizeOfElements(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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
            nElementsToRead   -= nReads;
            toReadIndex       += nReads;
        }
    }
    else
    {   /* fixed size element */
        *elementsSize = nElements * cdesc->length;
    }

    return nElements;
}

Four LRDS_CollectionBag_RetrieveElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo,                  /* IN column on which the bag is defined */
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

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_DeleteElements(handle)"));

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

    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
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

Four LRDS_CollectionBag_Union(
    Four handle,
    Four                ornOrScanIdA,           /* IN relation containing the bag A */
    Boolean             useScanFlagA,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidA,                   /* IN tuple containing the bag A */
    Two                 colNoA,                 /* IN column on which the bag A is defined */
    Four                ornOrScanIdB,           /* IN relation containing the bag B */
    Boolean             useScanFlagB,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidB,                   /* IN tuple containing the bag B */
    Two                 colNoB,                 /* IN column on which the bag B is defined */
    Four                ornOrScanId,            /* IN relation containing the bag */
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo)                  /* IN column on which the bag is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    Four                ornA;
    Four                smScanIdA;
    TupleHdr            tupHdrA;                /* a tuple header */
    Four                tupHdrSizeA;            /* size of tuple header */
    ColDesc*            cdescA;                 /* pointer to the current column descriptor */
    Four                nElementsOfColA;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryA;         /* pointer to an entry of relation table */
    Four                ornB;
    Four                smScanIdB;
    TupleHdr            tupHdrB;                /* a tuple header */
    Four                tupHdrSizeB;            /* size of tuple header */
    ColDesc*            cdescB;                 /* pointer to the current column descriptor */
    Four                nElementsOfColB;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryB;         /* pointer to an entry of relation table */

    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                startNoBuf;
    Four                elementsBufTop;
    Four                  keySize;

    Four                startNoBufA;
    Four                nElementsInBufA;
    char                elementsBufA[PAGESIZE];
    char                elementSizesBufA[PAGESIZE];
    char*               elementA;
    Four                elementSizeA;
    Four                 keySizeA;

    Four                startNoBufB;
    Four                nElementsInBufB;
    char                elementsBufB[PAGESIZE];
    char                elementSizesBufB[PAGESIZE];
    char*               elementB;
    Four                elementSizeB;
    Four                 keySizeB;

    Four                i, j, k;
    Four                index;
    Boolean             writeFlag;
    Four                compareResult;
    char*               writeElement;
    Four                writeSize;

    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */

    LockParameter       fileLockupA;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupA;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrA;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrA;         /* pointer to the lockup value */

    LockParameter       fileLockupB;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupB;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrB;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrB;         /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;  
    ColDesc             *relTableEntryA_cdesc;  
    ColDesc             *relTableEntryB_cdesc;  

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Intersect(handle)"));

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

    if (useScanFlagA == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA) {
        ornA = LRDS_SCANTABLE(handle)[ornOrScanIdA].orn;
        smScanIdA = LRDS_SCANTABLE(handle)[ornOrScanIdA].smScanId;
    } else {
        ornA = ornOrScanIdA;
        smScanIdA = NIL;
    }

    if (useScanFlagB == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB) {
        ornB = LRDS_SCANTABLE(handle)[ornOrScanIdB].orn;
        smScanIdB = LRDS_SCANTABLE(handle)[ornOrScanIdB].smScanId;
    } else {
        ornB = ornOrScanIdB;
        smScanIdB = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry  = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc          = &relTableEntry_cdesc[colNo];
    relTableEntryA = LRDS_GET_RELTABLE_ENTRY(handle, ornA);
    relTableEntryA_cdesc = PHYSICAL_PTR(relTableEntryA->cdesc); 
    cdescA         = &relTableEntryA_cdesc[colNoA];
    relTableEntryB = LRDS_GET_RELTABLE_ENTRY(handle, ornB);
    relTableEntryB_cdesc = PHYSICAL_PTR(relTableEntryB->cdesc); 
    cdescB         = &relTableEntryB_cdesc[colNoB];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoA >= relTableEntryA->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryA_cdesc[colNoA].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoB >= relTableEntryB->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryB_cdesc[colNoB].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;
    if (useScanFlagA && tidA == NULL) tidA = &LRDS_SCANTABLE(handle)[ornOrScanIdA].tid;
    if (useScanFlagB && tidB == NULL) tidB = &LRDS_SCANTABLE(handle)[ornOrScanIdB].tid;

    /* test bag compatibility */
    if(!(relTableEntry_cdesc[colNo].type == relTableEntryA_cdesc[colNoA].type &&
         relTableEntry_cdesc[colNo].type == relTableEntryB_cdesc[colNoB].type))
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

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

    if (LRDS_USEROPENRELTABLE(handle)[ornA].tmpRelationFlag) {
        fileLockupPtrA = objLockupPtrA = NULL;

    } else {
        fileLockupA.mode = L_IS;
        fileLockupA.duration = L_COMMIT;
        fileLockupPtrA = &fileLockupA;

        objLockupA.mode = L_S;
        objLockupA.duration = L_COMMIT;
        objLockupPtrA = &objLockupA;
    }

    if (LRDS_USEROPENRELTABLE(handle)[ornB].tmpRelationFlag) {
        fileLockupPtrB = objLockupPtrB = NULL;

    } else {
        fileLockupB.mode = L_IS;
        fileLockupB.duration = L_COMMIT;
        fileLockupPtrB = &fileLockupB;

        objLockupB.mode = L_S;
        objLockupB.duration = L_COMMIT;
        objLockupPtrB = &objLockupB;
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

    tupHdrSizeA = TUPLE_HEADER_SIZE(relTableEntryA->ri.nColumns, relTableEntryA->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA,
                         (ObjectID*)tidA, 0, tupHdrSizeA, (char*)&tupHdrA,
                         (relTableEntryA->isCatalog) ? NULL : fileLockupPtrA,
                         (relTableEntryA->isCatalog) ? NULL : objLockupPtrA);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeA = TUPLE_HEADER_SIZE(tupHdrA.nVarCols + tupHdrA.nFixedCols, tupHdrA.nVarCols);

    tupHdrSizeB = TUPLE_HEADER_SIZE(relTableEntryB->ri.nColumns, relTableEntryB->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryB->ri.fid, smScanIdB, useScanFlagB,
                         (ObjectID*)tidB, 0, tupHdrSizeB, (char*)&tupHdrB,
                         (relTableEntryB->isCatalog) ? NULL : fileLockupPtrB,
                         (relTableEntryB->isCatalog) ? NULL : objLockupPtrB);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeB = TUPLE_HEADER_SIZE(tupHdrB.nVarCols + tupHdrB.nFixedCols, tupHdrB.nVarCols);

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &keySize);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, &keySizeA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB, &keySizeB);
    if(e < eNOERROR) ERR(handle, e);

    if(keySize != keySizeA || keySize != keySizeB)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                      colNoA, &nElementsOfColA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                      colNoB, &nElementsOfColB);
    if(e < eNOERROR) ERR(handle, e);

    /* initialize read buffer and write buffer */
    startNoBuf      = 0;
    elementsBufTop  = 0;

    startNoBufA     = 0;
    nElementsInBufA = 0;

    startNoBufB     = 0;
    nElementsInBufB = 0;

    i = 0; j = 0; k = 0;
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 ||  (startNoBufA + nElementsInBufA) <= i))
            {
                Four nReads;
                Four elementsSize;

                startNoBufA += nElementsInBufA;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufA) / sizeof(Four);
                if(nReads > nElementsOfColA)
                    nReads = nElementsOfColA;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                                         colNoA, startNoBufA, nReads,
                                                         (Four*)elementSizesBufA);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf*/
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufA)[index]) > sizeof(elementsBufA))
                        break;
                    elementsSize += ((Four*)elementSizesBufA)[index];
                }
                nElementsInBufA = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                    startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA     = elementsBufA;
                elementSizeA = ((Four*)elementSizesBufA)[0];
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four nReads;
                Four elementsSize;

                startNoBufB += nElementsInBufB;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufB) / sizeof(Four);
                if(nReads > nElementsOfColB)
                    nReads = nElementsOfColB;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                                         colNoB, startNoBufB, nReads,
                                                         (Four*)elementSizesBufB);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf */
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufB)[index]) > sizeof(elementsBufB))
                        break;
                    elementsSize += ((Four*)elementSizesBufB)[index];
                }
                nElementsInBufB = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                    startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB     = elementsBufB;
                elementSizeB = ((Four*)elementSizesBufB)[0];
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                writeFlag    = TRUE;
                writeElement = elementB;
                writeSize    = elementSizeB;
                j++;
                elementB    += elementSizeB;
                elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                writeSize    = elementSizeA;
                i++;
                elementA    += elementSizeA;
                elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, elementSizeA, elementA, elementSizeB, elementB, keySizeA);

                if(compareResult <= 0)                    /* elementA <= elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    writeSize    = elementSizeA;
                    i++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                }
                else if(compareResult > 0)                    /* elementA > elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementB;
                    writeSize    = elementSizeB;
                    j++;
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                         startNoBuf, k - startNoBuf,
                                                         (Four*)elementSizesBuf,
                                                         elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                ((Four*)elementSizesBuf)[k - startNoBuf] = writeSize;
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                 startNoBuf, k - startNoBuf,
                                                 (Four*)elementSizesBuf,
                                                 elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, k);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        writeSize = cdesc->length;
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 || (startNoBufA + nElementsInBufA) <= i))
            {
                Four temp;

                temp = nElementsInBufA;
                nElementsInBufA = (sizeof(elementsBufA) / cdescA->length);
                if((nElementsOfColA - startNoBufA) < nElementsInBufA)
                    nElementsInBufA = nElementsOfColA - startNoBufA;
                startNoBufA += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                      startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA = elementsBufA;
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four temp;

                temp = nElementsInBufB;
                nElementsInBufB = (sizeof(elementsBufB) / cdescB->length);
                if((nElementsOfColB - startNoBufB) < nElementsInBufB)
                    nElementsInBufB = nElementsOfColB - startNoBufB;
                startNoBufB += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                      startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB = elementsBufB;
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                writeFlag    = TRUE;
                writeElement = elementB;
                elementB    += writeSize;
                j++;
            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                elementA    += writeSize;
                i++;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, cdescA->length, elementA, cdescB->length, elementB, keySizeA);

                if(compareResult <= 0)         /* elementA <= elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    elementA    += writeSize;
                    i++;
                }
                else if(compareResult > 0)      /* elementA > elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementB;
                    elementB    += writeSize;
                    j++;
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                           startNoBuf, k - startNoBuf, elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                   startNoBuf, k - startNoBuf, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, k);
        if(e < eNOERROR) ERR(handle, e);

    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_Intersect(
    Four handle,
    Four                ornOrScanIdA,           /* IN relation containing the bag A */
    Boolean             useScanFlagA,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidA,                   /* IN tuple containing the bag A */
    Two                 colNoA,                 /* IN column on which the bag A is defined */
    Four                ornOrScanIdB,           /* IN relation containing the bag B */
    Boolean             useScanFlagB,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidB,                   /* IN tuple containing the bag B */
    Two                 colNoB,                 /* IN column on which the bag B is defined */
    Four                ornOrScanId,            /* IN relation containing the bag */
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo)                  /* IN column on which the bag is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    Four                ornA;
    Four                smScanIdA;
    TupleHdr            tupHdrA;                /* a tuple header */
    Four                tupHdrSizeA;            /* size of tuple header */
    ColDesc*            cdescA;                 /* pointer to the current column descriptor */
    Four                nElementsOfColA;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryA;         /* pointer to an entry of relation table */
    Four                ornB;
    Four                smScanIdB;
    TupleHdr            tupHdrB;                /* a tuple header */
    Four                tupHdrSizeB;            /* size of tuple header */
    ColDesc*            cdescB;                 /* pointer to the current column descriptor */
    Four                nElementsOfColB;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryB;         /* pointer to an entry of relation table */

    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                startNoBuf;
    Four                elementsBufTop;
    Four                 keySize;

    Four                startNoBufA;
    Four                nElementsInBufA;
    char                elementsBufA[PAGESIZE];
    char                elementSizesBufA[PAGESIZE];
    char*               elementA;
    Four                elementSizeA;
    Four                 keySizeA;

    Four                startNoBufB;
    Four                nElementsInBufB;
    char                elementsBufB[PAGESIZE];
    char                elementSizesBufB[PAGESIZE];
    char*               elementB;
    Four                elementSizeB;
    Four                 keySizeB;

    Four                i, j, k;
    Four                index;
    Boolean             writeFlag;
    Four                compareResult;
    char*               writeElement;
    Four                writeSize;

    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */

    LockParameter       fileLockupA;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupA;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrA;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrA;         /* pointer to the lockup value */

    LockParameter       fileLockupB;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupB;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrB;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrB;         /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   
    ColDesc             *relTableEntryA_cdesc;  
    ColDesc             *relTableEntryB_cdesc;  

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Intersect(handle)"));

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

    if (useScanFlagA == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA) {
        ornA = LRDS_SCANTABLE(handle)[ornOrScanIdA].orn;
        smScanIdA = LRDS_SCANTABLE(handle)[ornOrScanIdA].smScanId;
    } else {
        ornA = ornOrScanIdA;
        smScanIdA = NIL;
    }

    if (useScanFlagB == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB) {
        ornB = LRDS_SCANTABLE(handle)[ornOrScanIdB].orn;
        smScanIdB = LRDS_SCANTABLE(handle)[ornOrScanIdB].smScanId;
    } else {
        ornB = ornOrScanIdB;
        smScanIdB = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry  = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc          = &relTableEntry_cdesc[colNo];
    relTableEntryA = LRDS_GET_RELTABLE_ENTRY(handle, ornA);
    relTableEntryA_cdesc = PHYSICAL_PTR(relTableEntryA->cdesc); 
    cdescA         = &relTableEntryA_cdesc[colNoA];
    relTableEntryB = LRDS_GET_RELTABLE_ENTRY(handle, ornB);
    relTableEntryB_cdesc = PHYSICAL_PTR(relTableEntryB->cdesc); 
    cdescB         = &relTableEntryB_cdesc[colNoB];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoA >= relTableEntryA->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryA_cdesc[colNoA].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoB >= relTableEntryB->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryB_cdesc[colNoB].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;
    if (useScanFlagA && tidA == NULL) tidA = &LRDS_SCANTABLE(handle)[ornOrScanIdA].tid;
    if (useScanFlagB && tidB == NULL) tidB = &LRDS_SCANTABLE(handle)[ornOrScanIdB].tid;

    /* test bag compatibility */
    if(!(relTableEntry_cdesc[colNo].type == relTableEntryA_cdesc[colNoA].type &&
         relTableEntry_cdesc[colNo].type == relTableEntryB_cdesc[colNoB].type))
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

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

    if (LRDS_USEROPENRELTABLE(handle)[ornA].tmpRelationFlag) {
        fileLockupPtrA = objLockupPtrA = NULL;

    } else {
        fileLockupA.mode = L_IS;
        fileLockupA.duration = L_COMMIT;
        fileLockupPtrA = &fileLockupA;

        objLockupA.mode = L_S;
        objLockupA.duration = L_COMMIT;
        objLockupPtrA = &objLockupA;
    }

    if (LRDS_USEROPENRELTABLE(handle)[ornB].tmpRelationFlag) {
        fileLockupPtrB = objLockupPtrB = NULL;

    } else {
        fileLockupB.mode = L_IS;
        fileLockupB.duration = L_COMMIT;
        fileLockupPtrB = &fileLockupB;

        objLockupB.mode = L_S;
        objLockupB.duration = L_COMMIT;
        objLockupPtrB = &objLockupB;
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

    tupHdrSizeA = TUPLE_HEADER_SIZE(relTableEntryA->ri.nColumns, relTableEntryA->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA,
                         (ObjectID*)tidA, 0, tupHdrSizeA, (char*)&tupHdrA,
                         (relTableEntryA->isCatalog) ? NULL : fileLockupPtrA,
                         (relTableEntryA->isCatalog) ? NULL : objLockupPtrA);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeA = TUPLE_HEADER_SIZE(tupHdrA.nVarCols + tupHdrA.nFixedCols, tupHdrA.nVarCols);

    tupHdrSizeB = TUPLE_HEADER_SIZE(relTableEntryB->ri.nColumns, relTableEntryB->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryB->ri.fid, smScanIdB, useScanFlagB,
                         (ObjectID*)tidB, 0, tupHdrSizeB, (char*)&tupHdrB,
                         (relTableEntryB->isCatalog) ? NULL : fileLockupPtrB,
                         (relTableEntryB->isCatalog) ? NULL : objLockupPtrB);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeB = TUPLE_HEADER_SIZE(tupHdrB.nVarCols + tupHdrB.nFixedCols, tupHdrB.nVarCols);

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &keySize);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, &keySizeA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB, &keySizeB);
    if(e < eNOERROR) ERR(handle, e);

    if(keySize != keySizeA || keySize != keySizeB)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                      colNoA, &nElementsOfColA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                      colNoB, &nElementsOfColB);
    if(e < eNOERROR) ERR(handle, e);

    /* initialize read buffer and write buffer */
    startNoBuf      = 0;
    elementsBufTop  = 0;

    startNoBufA     = 0;
    nElementsInBufA = 0;

    startNoBufB     = 0;
    nElementsInBufB = 0;

    i = 0; j = 0; k = 0;
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 ||  (startNoBufA + nElementsInBufA) <= i))
            {
                Four nReads;
                Four elementsSize;

                startNoBufA += nElementsInBufA;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufA) / sizeof(Four);
                if(nReads > nElementsOfColA)
                    nReads = nElementsOfColA;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                                         colNoA, startNoBufA, nReads,
                                                         (Four*)elementSizesBufA);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf*/
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufA)[index]) > sizeof(elementsBufA))
                        break;
                    elementsSize += ((Four*)elementSizesBufA)[index];
                }
                nElementsInBufA = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                    startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA     = elementsBufA;
                elementSizeA = ((Four*)elementSizesBufA)[0];
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four nReads;
                Four elementsSize;

                startNoBufB += nElementsInBufB;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufB) / sizeof(Four);
                if(nReads > nElementsOfColB)
                    nReads = nElementsOfColB;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                                         colNoB, startNoBufB, nReads,
                                                         (Four*)elementSizesBufB);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf */
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufB)[index]) > sizeof(elementsBufB))
                        break;
                    elementsSize += ((Four*)elementSizesBufB)[index];
                }
                nElementsInBufB = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                    startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB     = elementsBufB;
                elementSizeB = ((Four*)elementSizesBufB)[0];
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break; /* exit while loop */
            }
            else if(j >= nElementsOfColB)
            {
                break; /* exit while loop */
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, elementSizeA, elementA, elementSizeB, elementB, keySizeA);

                if(compareResult == 0)                        /* elementA == elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    writeSize    = elementSizeA;
                    i++;
                    j++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
                else if(compareResult < 0)                    /* elementA < elementB */
                {
                    i++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                }
                else if(compareResult > 0)                    /* elementA > elementB */
                {
                    j++;
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                         startNoBuf, k - startNoBuf,
                                                         (Four*)elementSizesBuf,
                                                         elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                ((Four*)elementSizesBuf)[k - startNoBuf] = writeSize;
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                 startNoBuf, k - startNoBuf,
                                                 (Four*)elementSizesBuf,
                                                 elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, k);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        writeSize = cdesc->length;
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 || (startNoBufA + nElementsInBufA) <= i))
            {
                Four temp;

                temp = nElementsInBufA;
                nElementsInBufA = (sizeof(elementsBufA) / cdescA->length);
                if((nElementsOfColA - startNoBufA) < nElementsInBufA)
                    nElementsInBufA = nElementsOfColA - startNoBufA;
                startNoBufA += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                      startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA = elementsBufA;
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four temp;

                temp = nElementsInBufB;
                nElementsInBufB = (sizeof(elementsBufB) / cdescB->length);
                if((nElementsOfColB - startNoBufB) < nElementsInBufB)
                    nElementsInBufB = nElementsOfColB - startNoBufB;
                startNoBufB += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                      startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB = elementsBufB;
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break;
            }
            else if(j >= nElementsOfColB)
            {
                break;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, cdescA->length, elementA, cdescB->length, elementB, keySizeA);

                if(compareResult == 0)          /* elementA == elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    elementA    += writeSize;
                    elementB    += writeSize;
                    i++;
                    j++;
                }
                else if(compareResult < 0)      /* elementA < elementB */
                {
                    elementA    += writeSize;
                    i++;
                }
                else if(compareResult > 0)      /* elementA > elementB */
                {
                    elementB    += writeSize;
                    j++;
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                           startNoBuf, k - startNoBuf, elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                   startNoBuf, k - startNoBuf, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, k);
        if(e < eNOERROR) ERR(handle, e);

    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_Difference(
    Four handle,
    Four                ornOrScanIdA,           /* IN relation containing the bag A */
    Boolean             useScanFlagA,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidA,                   /* IN tuple containing the bag A */
    Two                 colNoA,                 /* IN column on which the bag A is defined */
    Four                ornOrScanIdB,           /* IN relation containing the bag B */
    Boolean             useScanFlagB,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidB,                   /* IN tuple containing the bag B */
    Two                 colNoB,                 /* IN column on which the bag B is defined */
    Four                ornOrScanId,            /* IN relation containing the bag */
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    Two                 colNo)                  /* IN column on which the bag is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    Four                ornA;
    Four                smScanIdA;
    TupleHdr            tupHdrA;                /* a tuple header */
    Four                tupHdrSizeA;            /* size of tuple header */
    ColDesc*            cdescA;                 /* pointer to the current column descriptor */
    Four                nElementsOfColA;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryA;         /* pointer to an entry of relation table */
    Four                ornB;
    Four                smScanIdB;
    TupleHdr            tupHdrB;                /* a tuple header */
    Four                tupHdrSizeB;            /* size of tuple header */
    ColDesc*            cdescB;                 /* pointer to the current column descriptor */
    Four                nElementsOfColB;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryB;         /* pointer to an entry of relation table */

    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                startNoBuf;
    Four                elementsBufTop;
    Four                 keySize;

    Four                startNoBufA;
    Four                nElementsInBufA;
    char                elementsBufA[PAGESIZE];
    char                elementSizesBufA[PAGESIZE];
    char*               elementA;
    Four                elementSizeA;
    Four                 keySizeA;

    Four                startNoBufB;
    Four                nElementsInBufB;
    char                elementsBufB[PAGESIZE];
    char                elementSizesBufB[PAGESIZE];
    char*               elementB;
    Four                elementSizeB;
    Four                 keySizeB;

    Four                i, j, k;
    Four                index;
    Boolean             writeFlag;
    Four                compareResult;
    char*               writeElement;
    Four                writeSize;

    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */

    LockParameter       fileLockupA;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupA;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrA;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrA;         /* pointer to the lockup value */

    LockParameter       fileLockupB;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupB;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrB;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrB;         /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   
    ColDesc             *relTableEntryA_cdesc;  
    ColDesc             *relTableEntryB_cdesc;  

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Intersect(handle)"));

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

    if (useScanFlagA == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA) {
        ornA = LRDS_SCANTABLE(handle)[ornOrScanIdA].orn;
        smScanIdA = LRDS_SCANTABLE(handle)[ornOrScanIdA].smScanId;
    } else {
        ornA = ornOrScanIdA;
        smScanIdA = NIL;
    }

    if (useScanFlagB == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB) {
        ornB = LRDS_SCANTABLE(handle)[ornOrScanIdB].orn;
        smScanIdB = LRDS_SCANTABLE(handle)[ornOrScanIdB].smScanId;
    } else {
        ornB = ornOrScanIdB;
        smScanIdB = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry  = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc          = &relTableEntry_cdesc[colNo];
    relTableEntryA = LRDS_GET_RELTABLE_ENTRY(handle, ornA);
    relTableEntryA_cdesc = PHYSICAL_PTR(relTableEntryA->cdesc); 
    cdescA         = &relTableEntryA_cdesc[colNoA];
    relTableEntryB = LRDS_GET_RELTABLE_ENTRY(handle, ornB);
    relTableEntryB_cdesc = PHYSICAL_PTR(relTableEntryB->cdesc); 
    cdescB         = &relTableEntryB_cdesc[colNoB];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntry_cdesc[colNo].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoA >= relTableEntryA->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryA_cdesc[colNoA].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoB >= relTableEntryB->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryB_cdesc[colNoB].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;
    if (useScanFlagA && tidA == NULL) tidA = &LRDS_SCANTABLE(handle)[ornOrScanIdA].tid;
    if (useScanFlagB && tidB == NULL) tidB = &LRDS_SCANTABLE(handle)[ornOrScanIdB].tid;

    /* test bag compatibility */
    if(!(relTableEntry_cdesc[colNo].type == relTableEntryA_cdesc[colNoA].type &&
         relTableEntry_cdesc[colNo].type == relTableEntryB_cdesc[colNoB].type))
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

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

    if (LRDS_USEROPENRELTABLE(handle)[ornA].tmpRelationFlag) {
        fileLockupPtrA = objLockupPtrA = NULL;

    } else {
        fileLockupA.mode = L_IS;
        fileLockupA.duration = L_COMMIT;
        fileLockupPtrA = &fileLockupA;

        objLockupA.mode = L_S;
        objLockupA.duration = L_COMMIT;
        objLockupPtrA = &objLockupA;
    }

    if (LRDS_USEROPENRELTABLE(handle)[ornB].tmpRelationFlag) {
        fileLockupPtrB = objLockupPtrB = NULL;

    } else {
        fileLockupB.mode = L_IS;
        fileLockupB.duration = L_COMMIT;
        fileLockupPtrB = &fileLockupB;

        objLockupB.mode = L_S;
        objLockupB.duration = L_COMMIT;
        objLockupPtrB = &objLockupB;
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

    tupHdrSizeA = TUPLE_HEADER_SIZE(relTableEntryA->ri.nColumns, relTableEntryA->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA,
                         (ObjectID*)tidA, 0, tupHdrSizeA, (char*)&tupHdrA,
                         (relTableEntryA->isCatalog) ? NULL : fileLockupPtrA,
                         (relTableEntryA->isCatalog) ? NULL : objLockupPtrA);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeA = TUPLE_HEADER_SIZE(tupHdrA.nVarCols + tupHdrA.nFixedCols, tupHdrA.nVarCols);

    tupHdrSizeB = TUPLE_HEADER_SIZE(relTableEntryB->ri.nColumns, relTableEntryB->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryB->ri.fid, smScanIdB, useScanFlagB,
                         (ObjectID*)tidB, 0, tupHdrSizeB, (char*)&tupHdrB,
                         (relTableEntryB->isCatalog) ? NULL : fileLockupPtrB,
                         (relTableEntryB->isCatalog) ? NULL : objLockupPtrB);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeB = TUPLE_HEADER_SIZE(tupHdrB.nVarCols + tupHdrB.nFixedCols, tupHdrB.nVarCols);

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &keySize);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, &keySizeA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB, &keySizeB);
    if(e < eNOERROR) ERR(handle, e);

    if(keySize != keySizeA || keySize != keySizeB)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                      colNoA, &nElementsOfColA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                      colNoB, &nElementsOfColB);
    if(e < eNOERROR) ERR(handle, e);

    /* initialize read buffer and write buffer */
    startNoBuf      = 0;
    elementsBufTop  = 0;

    startNoBufA     = 0;
    nElementsInBufA = 0;

    startNoBufB     = 0;
    nElementsInBufB = 0;

    i = 0; j = 0; k = 0;
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable size element */
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 ||  (startNoBufA + nElementsInBufA) <= i))
            {
                Four nReads;
                Four elementsSize;

                startNoBufA += nElementsInBufA;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufA) / sizeof(Four);
                if(nReads > nElementsOfColA)
                    nReads = nElementsOfColA;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                                         colNoA, startNoBufA, nReads,
                                                         (Four*)elementSizesBufA);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf*/
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufA)[index]) > sizeof(elementsBufA))
                        break;
                    elementsSize += ((Four*)elementSizesBufA)[index];
                }
                nElementsInBufA = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                    startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA     = elementsBufA;
                elementSizeA = ((Four*)elementSizesBufA)[0];
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four nReads;
                Four elementsSize;

                startNoBufB += nElementsInBufB;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufB) / sizeof(Four);
                if(nReads > nElementsOfColB)
                    nReads = nElementsOfColB;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                                         colNoB, startNoBufB, nReads,
                                                         (Four*)elementSizesBufB);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf */
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufB)[index]) > sizeof(elementsBufB))
                        break;
                    elementsSize += ((Four*)elementSizesBufB)[index];
                }
                nElementsInBufB = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                    startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB     = elementsBufB;
                elementSizeB = ((Four*)elementSizesBufB)[0];
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break;
            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                writeSize    = elementSizeA;
                i++;
                elementA    += elementSizeA;
                elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, elementSizeA, elementA, elementSizeB, elementB, keySizeA);

                if(compareResult <= 0)                        /* elementA < elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    writeSize    = elementSizeA;
                    i++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                }
                else if(compareResult > 0)                    /* elementA > elementB */
                {
                    j++;
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                         startNoBuf, k - startNoBuf,
                                                         (Four*)elementSizesBuf,
                                                         elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                ((Four*)elementSizesBuf)[k - startNoBuf] = writeSize;
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                 startNoBuf, k - startNoBuf,
                                                 (Four*)elementSizesBuf,
                                                 elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, k);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        writeSize = cdesc->length;
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 || (startNoBufA + nElementsInBufA) <= i))
            {
                Four temp;

                temp = nElementsInBufA;
                nElementsInBufA = (sizeof(elementsBufA) / cdescA->length);
                if((nElementsOfColA - startNoBufA) < nElementsInBufA)
                    nElementsInBufA = nElementsOfColA - startNoBufA;
                startNoBufA += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                      startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA = elementsBufA;
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four temp;

                temp = nElementsInBufB;
                nElementsInBufB = (sizeof(elementsBufB) / cdescB->length);
                if((nElementsOfColB - startNoBufB) < nElementsInBufB)
                    nElementsInBufB = nElementsOfColB - startNoBufB;
                startNoBufB += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                      startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB = elementsBufB;
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break;
            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                elementA    += writeSize;
                i++;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, cdescA->length, elementA, cdescB->length, elementB, keySizeA);

                if(compareResult <= 0)          /* elementA <= elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    elementA    += writeSize;
                    i++;
                }
                else if(compareResult > 0)      /* elementA > elementB */
                {
                    elementB    += writeSize;
                    j++;
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                           startNoBuf, k - startNoBuf, elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                   startNoBuf, k - startNoBuf, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, k);
        if(e < eNOERROR) ERR(handle, e);

    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_UnionWith(
    Four handle,
    Four                ornOrScanIdA,           /* IN relation containing the bag A */
    Boolean             useScanFlagA,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidA,                   /* IN tuple containing the bag A */
    Two                 colNoA,                 /* IN column on which the bag A is defined */
    Four                ornOrScanIdB,           /* IN relation containing the bag A */
    Boolean             useScanFlagB,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidB,                   /* IN tuple containing the bag B */
    Two                 colNoB)                 /* IN column on which the bag B is defined */
{
    Four                e;                      /* error code */
    Four                ornA;
    Four                smScanIdA;
    TupleHdr            tupHdrA;                /* a tuple header */
    Four                tupHdrSizeA;            /* size of tuple header */
    ColDesc*            cdescA;                 /* pointer to the current column descriptor */
    Four                nElementsOfColA;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryA;         /* pointer to an entry of relation table */
    Four                ornB;
    Four                smScanIdB;
    TupleHdr            tupHdrB;                /* a tuple header */
    Four                tupHdrSizeB;            /* size of tuple header */
    ColDesc*            cdescB;                 /* pointer to the current column descriptor */
    Four                nElementsOfColB;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryB;         /* pointer to an entry of relation table */

    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                startNoBuf;
    Four                elementsBufTop;

    Four                startNoBufA;
    Four                nElementsInBufA;
    char                elementsBufA[PAGESIZE];
    char                elementSizesBufA[PAGESIZE];
    char*               elementA;
    Four                elementSizeA;
    Four                 keySizeA;

    Four                startNoBufB;
    Four                nElementsInBufB;
    char                elementsBufB[PAGESIZE];
    char                elementSizesBufB[PAGESIZE];
    char*               elementB;
    Four                elementSizeB;
    Four                 keySizeB;

    Four                i, j, k;
    Four                index;
    Boolean             writeFlag;
    Four             compareResult;
    char*               writeElement;
    Four                writeSize;

    LockParameter       fileLockupA;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupA;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrA;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrA;         /* pointer to the lockup value */

    LockParameter       fileLockupB;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupB;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrB;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrB;         /* pointer to the lockup value */
    ColDesc             *relTableEntryA_cdesc;  
    ColDesc             *relTableEntryB_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_UnionWith(handle)"));

    /* Check parameters*/
    if (useScanFlagA == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA) {
        ornA = LRDS_SCANTABLE(handle)[ornOrScanIdA].orn;
        smScanIdA = LRDS_SCANTABLE(handle)[ornOrScanIdA].smScanId;
    } else {
        ornA = ornOrScanIdA;
        smScanIdA = NIL;
    }

    if (useScanFlagB == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB) {
        ornB = LRDS_SCANTABLE(handle)[ornOrScanIdB].orn;
        smScanIdB = LRDS_SCANTABLE(handle)[ornOrScanIdB].smScanId;
    } else {
        ornB = ornOrScanIdB;
        smScanIdB = NIL;
    }

    /* Get the relation table entry. */
    relTableEntryA = LRDS_GET_RELTABLE_ENTRY(handle, ornA);
    relTableEntryA_cdesc = PHYSICAL_PTR(relTableEntryA->cdesc); 
    cdescA         = &relTableEntryA_cdesc[colNoA];
    relTableEntryB = LRDS_GET_RELTABLE_ENTRY(handle, ornB);
    relTableEntryB_cdesc = PHYSICAL_PTR(relTableEntryB->cdesc); 
    cdescB         = &relTableEntryB_cdesc[colNoB];

    /* Is the column valid? */
    if (colNoA >= relTableEntryA->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryA_cdesc[colNoA].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoB >= relTableEntryB->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryB_cdesc[colNoB].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlagA && tidA == NULL) tidA = &LRDS_SCANTABLE(handle)[ornOrScanIdA].tid;
    if (useScanFlagB && tidB == NULL) tidB = &LRDS_SCANTABLE(handle)[ornOrScanIdB].tid;

    /* test bag compatibility */
    if(!(relTableEntryA_cdesc[colNoA].type == relTableEntryB_cdesc[colNoB].type))
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[ornA].tmpRelationFlag) {
        fileLockupPtrA = objLockupPtrA = NULL;

    } else {
        fileLockupA.mode = L_IX;
        fileLockupA.duration = L_COMMIT;
        fileLockupPtrA = &fileLockupA;

        objLockupA.mode = L_X;
        objLockupA.duration = L_COMMIT;
        objLockupPtrA = &objLockupA;
    }

    if (LRDS_USEROPENRELTABLE(handle)[ornB].tmpRelationFlag) {
        fileLockupPtrB = objLockupPtrB = NULL;

    } else {
        fileLockupB.mode = L_IS;
        fileLockupB.duration = L_COMMIT;
        fileLockupPtrB = &fileLockupB;

        objLockupB.mode = L_S;
        objLockupB.duration = L_COMMIT;
        objLockupPtrB = &objLockupB;
    }

    /* Get tupHdr */
    tupHdrSizeA = TUPLE_HEADER_SIZE(relTableEntryA->ri.nColumns, relTableEntryA->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA,
                         (ObjectID*)tidA, 0, tupHdrSizeA, (char*)&tupHdrA,
                         (relTableEntryA->isCatalog) ? NULL : fileLockupPtrA,
                         (relTableEntryA->isCatalog) ? NULL : objLockupPtrA);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeA = TUPLE_HEADER_SIZE(tupHdrA.nVarCols + tupHdrA.nFixedCols, tupHdrA.nVarCols);

    tupHdrSizeB = TUPLE_HEADER_SIZE(relTableEntryB->ri.nColumns, relTableEntryB->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryB->ri.fid, smScanIdB, useScanFlagB,
                         (ObjectID*)tidB, 0, tupHdrSizeB, (char*)&tupHdrB,
                         (relTableEntryB->isCatalog) ? NULL : fileLockupPtrB,
                         (relTableEntryB->isCatalog) ? NULL : objLockupPtrB);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeB = TUPLE_HEADER_SIZE(tupHdrB.nVarCols + tupHdrB.nFixedCols, tupHdrB.nVarCols);

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, &keySizeA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB, &keySizeB);
    if(e < eNOERROR) ERR(handle, e);

    if(keySizeA != keySizeB)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                      colNoA, &nElementsOfColA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                      colNoB, &nElementsOfColB);
    if(e < eNOERROR) ERR(handle, e);

    /* initialize read buffer and write buffer */
    startNoBuf      = 0;
    elementsBufTop  = 0;

    startNoBufA     = 0;
    nElementsInBufA = 0;

    startNoBufB     = 0;
    nElementsInBufB = 0;

    i = 0; j = 0; k = 0;
    if(relTableEntryA_cdesc[colNoA].type == SM_VARSTRING)
    {   /* variable size element */
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 ||  (startNoBufA + nElementsInBufA) <= i))
            {
                Four nReads;
                Four elementsSize;

                startNoBufA += nElementsInBufA;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufA) / sizeof(Four);
                if(nReads > nElementsOfColA)
                    nReads = nElementsOfColA;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                                         colNoA, startNoBufA, nReads,
                                                         (Four*)elementSizesBufA);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf*/
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufA)[index]) > sizeof(elementsBufA))
                        break;
                    elementsSize += ((Four*)elementSizesBufA)[index];
                }
                nElementsInBufA = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                    startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA     = elementsBufA;
                elementSizeA = ((Four*)elementSizesBufA)[0];
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four nReads;
                Four elementsSize;

                startNoBufB += nElementsInBufB;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufB) / sizeof(Four);
                if(nReads > nElementsOfColB)
                    nReads = nElementsOfColB;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                                         colNoB, startNoBufB, nReads,
                                                         (Four*)elementSizesBufB);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf */
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufB)[index]) > sizeof(elementsBufB))
                        break;
                    elementsSize += ((Four*)elementSizesBufB)[index];
                }
                nElementsInBufB = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                    startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB     = elementsBufB;
                elementSizeB = ((Four*)elementSizesBufB)[0];
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                writeFlag    = TRUE;
                writeElement = elementB;
                writeSize    = elementSizeB;
                j++;
                elementB    += elementSizeB;
                elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];

            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                writeSize    = elementSizeA;
                i++;
                elementA    += elementSizeA;
                elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, elementSizeA, elementA, elementSizeB, elementB, keySizeA);

                if(compareResult <= 0)                        /* elementA < elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    writeSize    = elementSizeA;
                    i++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                }
                else if(compareResult > 0)                    /* elementA > elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementB;
                    writeSize    = elementSizeB;
                    j++;
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_VarElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                         startNoBuf + nElementsOfColA,
                                                         k - startNoBuf,
                                                         (Four*)elementSizesBuf,
                                                         elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                ((Four*)elementSizesBuf)[k - startNoBuf] = writeSize;
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_VarElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                 startNoBuf + nElementsOfColA, k - startNoBuf,
                                                 (Four*)elementSizesBuf,
                                                 elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* remove previous elements */
        e = lrds_Collection_VarElementDelete(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                             0, nElementsOfColA);
        if(e < eNOERROR) ERR(handle, e);

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, k);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        writeSize = cdescA->length;
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 || (startNoBufA + nElementsInBufA) <= i))
            {
                Four temp;

                temp = nElementsInBufA;
                nElementsInBufA = (sizeof(elementsBufA) / cdescA->length);
                if((nElementsOfColA - startNoBufA) < nElementsInBufA)
                    nElementsInBufA = nElementsOfColA - startNoBufA;
                startNoBufA += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                      startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA = elementsBufA;
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four temp;

                temp = nElementsInBufB;
                nElementsInBufB = (sizeof(elementsBufB) / cdescB->length);
                if((nElementsOfColB - startNoBufB) < nElementsInBufB)
                    nElementsInBufB = nElementsOfColB - startNoBufB;
                startNoBufB += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                      startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB = elementsBufB;
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                writeFlag    = TRUE;
                writeElement = elementB;
                elementB    += writeSize;
                j++;
            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                elementA    += writeSize;
                i++;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, cdescA->length, elementA, cdescB->length, elementB, keySizeA);

                if(compareResult == 0)          /* elementA == elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    elementA    += writeSize;
                    elementB    += writeSize;
                    i++;
                    j++;
                }
                else if(compareResult < 0)      /* elementA < elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    elementA    += writeSize;
                    i++;
                }
                else if(compareResult > 0)      /* elementA > elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementB;
                    elementB    += writeSize;
                    j++;
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_FixedElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                           startNoBuf + nElementsOfColA,
                                                           k - startNoBuf, elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_FixedElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                   startNoBuf + nElementsOfColA,
                                                   k - startNoBuf, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* remove previous elements */
        e = lrds_Collection_FixedElementDelete(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                               0, nElementsOfColA);
        if(e < eNOERROR) ERR(handle, e);

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, k);
        if(e < eNOERROR) ERR(handle, e);

    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA, (ObjectID*)tidA,
                          0, tupHdrSizeA, (char*)&tupHdrA, tupHdrSizeA, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_IntersectWith(
    Four handle,
    Four                ornOrScanIdA,           /* IN relation containing the bag A */
    Boolean             useScanFlagA,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidA,                   /* IN tuple containing the bag A */
    Two                 colNoA,                 /* IN column on which the bag A is defined */
    Four                ornOrScanIdB,           /* IN relation containing the bag A */
    Boolean             useScanFlagB,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidB,                   /* IN tuple containing the bag B */
    Two                 colNoB)                 /* IN column on which the bag B is defined */
{
    Four                e;                      /* error code */
    Four                ornA;
    Four                smScanIdA;
    TupleHdr            tupHdrA;                /* a tuple header */
    Four                tupHdrSizeA;            /* size of tuple header */
    ColDesc*            cdescA;                 /* pointer to the current column descriptor */
    Four                nElementsOfColA;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryA;         /* pointer to an entry of relation table */
    Four                ornB;
    Four                smScanIdB;
    TupleHdr            tupHdrB;                /* a tuple header */
    Four                tupHdrSizeB;            /* size of tuple header */
    ColDesc*            cdescB;                 /* pointer to the current column descriptor */
    Four                nElementsOfColB;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryB;         /* pointer to an entry of relation table */

    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                startNoBuf;
    Four                elementsBufTop;

    Four                startNoBufA;
    Four                nElementsInBufA;
    char                elementsBufA[PAGESIZE];
    char                elementSizesBufA[PAGESIZE];
    char*               elementA;
    Four                elementSizeA;
    Four                 keySizeA;

    Four                startNoBufB;
    Four                nElementsInBufB;
    char                elementsBufB[PAGESIZE];
    char                elementSizesBufB[PAGESIZE];
    char*               elementB;
    Four                elementSizeB;
    Four                 keySizeB;

    Four                i, j, k;
    Four                index;
    Boolean             writeFlag;
    Four                compareResult;
    char*               writeElement;
    Four                writeSize;

    LockParameter       fileLockupA;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupA;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrA;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrA;         /* pointer to the lockup value */

    LockParameter       fileLockupB;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupB;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrB;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrB;         /* pointer to the lockup value */
    ColDesc             *relTableEntryA_cdesc;  
    ColDesc             *relTableEntryB_cdesc;  


    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_IntersectWith(handle)"));

    /* Check parameters*/
    if (useScanFlagA == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA) {
        ornA = LRDS_SCANTABLE(handle)[ornOrScanIdA].orn;
        smScanIdA = LRDS_SCANTABLE(handle)[ornOrScanIdA].smScanId;
    } else {
        ornA = ornOrScanIdA;
        smScanIdA = NIL;
    }

    if (useScanFlagB == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB) {
        ornB = LRDS_SCANTABLE(handle)[ornOrScanIdB].orn;
        smScanIdB = LRDS_SCANTABLE(handle)[ornOrScanIdB].smScanId;
    } else {
        ornB = ornOrScanIdB;
        smScanIdB = NIL;
    }


    /* Get the relation table entry. */
    relTableEntryA = LRDS_GET_RELTABLE_ENTRY(handle, ornA);
    relTableEntryA_cdesc = PHYSICAL_PTR(relTableEntryA->cdesc); 
    cdescA         = &relTableEntryA_cdesc[colNoA];
    relTableEntryB = LRDS_GET_RELTABLE_ENTRY(handle, ornB);
    relTableEntryB_cdesc = PHYSICAL_PTR(relTableEntryB->cdesc); 
    cdescB         = &relTableEntryB_cdesc[colNoB];


    /* Is the column valid? */
    if (colNoA >= relTableEntryA->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryA_cdesc[colNoA].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoB >= relTableEntryB->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryB_cdesc[colNoB].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlagA && tidA == NULL) tidA = &LRDS_SCANTABLE(handle)[ornOrScanIdA].tid;
    if (useScanFlagB && tidB == NULL) tidB = &LRDS_SCANTABLE(handle)[ornOrScanIdB].tid;

    /* test bag compatibility */
    if(!(relTableEntryA_cdesc[colNoA].type == relTableEntryB_cdesc[colNoB].type))
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[ornA].tmpRelationFlag) {
        fileLockupPtrA = objLockupPtrA = NULL;

    } else {
        fileLockupA.mode = L_IX;
        fileLockupA.duration = L_COMMIT;
        fileLockupPtrA = &fileLockupA;

        objLockupA.mode = L_X;
        objLockupA.duration = L_COMMIT;
        objLockupPtrA = &objLockupA;
    }

    if (LRDS_USEROPENRELTABLE(handle)[ornB].tmpRelationFlag) {
        fileLockupPtrB = objLockupPtrB = NULL;

    } else {
        fileLockupB.mode = L_IS;
        fileLockupB.duration = L_COMMIT;
        fileLockupPtrB = &fileLockupB;

        objLockupB.mode = L_S;
        objLockupB.duration = L_COMMIT;
        objLockupPtrB = &objLockupB;
    }

    /* Get tupHdr */
    tupHdrSizeA = TUPLE_HEADER_SIZE(relTableEntryA->ri.nColumns, relTableEntryA->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA,
                         (ObjectID*)tidA, 0, tupHdrSizeA, (char*)&tupHdrA,
                         (relTableEntryA->isCatalog) ? NULL : fileLockupPtrA,
                         (relTableEntryA->isCatalog) ? NULL : objLockupPtrA);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeA = TUPLE_HEADER_SIZE(tupHdrA.nVarCols + tupHdrA.nFixedCols, tupHdrA.nVarCols);

    tupHdrSizeB = TUPLE_HEADER_SIZE(relTableEntryB->ri.nColumns, relTableEntryB->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryB->ri.fid, smScanIdB, useScanFlagB,
                         (ObjectID*)tidB, 0, tupHdrSizeB, (char*)&tupHdrB,
                         (relTableEntryB->isCatalog) ? NULL : fileLockupPtrB,
                         (relTableEntryB->isCatalog) ? NULL : objLockupPtrB);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeB = TUPLE_HEADER_SIZE(tupHdrB.nVarCols + tupHdrB.nFixedCols, tupHdrB.nVarCols);

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, &keySizeA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB, &keySizeB);
    if(e < eNOERROR) ERR(handle, e);

    if(keySizeA != keySizeB)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                      colNoA, &nElementsOfColA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                      colNoB, &nElementsOfColB);
    if(e < eNOERROR) ERR(handle, e);

    /* initialize read buffer and write buffer */
    startNoBuf      = 0;
    elementsBufTop  = 0;

    startNoBufA     = 0;
    nElementsInBufA = 0;

    startNoBufB     = 0;
    nElementsInBufB = 0;

    i = 0; j = 0; k = 0;
    if(relTableEntryA_cdesc[colNoA].type == SM_VARSTRING)
    {   /* variable size element */
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 ||  (startNoBufA + nElementsInBufA) <= i))
            {
                Four nReads;
                Four elementsSize;

                startNoBufA += nElementsInBufA;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufA) / sizeof(Four);
                if(nReads > nElementsOfColA)
                    nReads = nElementsOfColA;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                                         colNoA, startNoBufA, nReads,
                                                         (Four*)elementSizesBufA);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf*/
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufA)[index]) > sizeof(elementsBufA))
                        break;
                    elementsSize += ((Four*)elementSizesBufA)[index];
                }
                nElementsInBufA = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                    startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA     = elementsBufA;
                elementSizeA = ((Four*)elementSizesBufA)[0];
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four nReads;
                Four elementsSize;

                startNoBufB += nElementsInBufB;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufB) / sizeof(Four);
                if(nReads > nElementsOfColB)
                    nReads = nElementsOfColB;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                                         colNoB, startNoBufB, nReads,
                                                         (Four*)elementSizesBufB);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf */
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufB)[index]) > sizeof(elementsBufB))
                        break;
                    elementsSize += ((Four*)elementSizesBufB)[index];
                }
                nElementsInBufB = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                    startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB     = elementsBufB;
                elementSizeB = ((Four*)elementSizesBufB)[0];
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break; /* exit while loop */
            }
            else if(j >= nElementsOfColB)
            {
                break; /* exit while loop */
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, elementSizeA, elementA, elementSizeB, elementB, keySizeA);

                if(compareResult == 0)                        /* elementA == elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    writeSize    = elementSizeA;
                    i++;
                    j++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
                else if(compareResult < 0)                    /* elementA < elementB */
                {
                    i++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                }
                else if(compareResult > 0)                    /* elementA > elementB */
                {
                    j++;
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_VarElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                         startNoBuf + nElementsOfColA,
                                                         k - startNoBuf,
                                                         (Four*)elementSizesBuf,
                                                         elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                ((Four*)elementSizesBuf)[k - startNoBuf] = writeSize;
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_VarElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                 startNoBuf + nElementsOfColA,
                                                 k - startNoBuf,
                                                 (Four*)elementSizesBuf,
                                                 elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* remove previous elements */
        e = lrds_Collection_VarElementDelete(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                             0, nElementsOfColA);
        if(e < eNOERROR) ERR(handle, e);

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, k);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        writeSize = cdescA->length;
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 || (startNoBufA + nElementsInBufA) <= i))
            {
                Four temp;

                temp = nElementsInBufA;
                nElementsInBufA = (sizeof(elementsBufA) / cdescA->length);
                if((nElementsOfColA - startNoBufA) < nElementsInBufA)
                    nElementsInBufA = nElementsOfColA - startNoBufA;
                startNoBufA += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                      startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA = elementsBufA;
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four temp;

                temp = nElementsInBufB;
                nElementsInBufB = (sizeof(elementsBufB) / cdescB->length);
                if((nElementsOfColB - startNoBufB) < nElementsInBufB)
                    nElementsInBufB = nElementsOfColB - startNoBufB;
                startNoBufB += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                      startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB = elementsBufB;
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break;
            }
            else if(j >= nElementsOfColB)
            {
                break;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, cdescA->length, elementA, cdescB->length, elementB, keySizeA);

                if(compareResult == 0)          /* elementA == elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    elementA    += writeSize;
                    elementB    += writeSize;
                    i++;
                    j++;
                }
                else if(compareResult < 0)      /* elementA < elementB */
                {
                    elementA    += writeSize;
                    i++;
                }
                else if(compareResult > 0)      /* elementA > elementB */
                {
                    elementB    += writeSize;
                    j++;
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_FixedElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                           startNoBuf + nElementsOfColA,
                                                           k - startNoBuf, elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_FixedElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                   startNoBuf + nElementsOfColA,
                                                   k - startNoBuf, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* remove previous elements */
        e = lrds_Collection_FixedElementDelete(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                               0, nElementsOfColA);
        if(e < eNOERROR) ERR(handle, e);

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, k);
        if(e < eNOERROR) ERR(handle, e);

    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA, (ObjectID*)tidA,
                          0, tupHdrSizeA, (char*)&tupHdrA, tupHdrSizeA, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_DifferenceWith(
    Four handle,
    Four                ornOrScanIdA,           /* IN relation containing the bag A */
    Boolean             useScanFlagA,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidA,                   /* IN tuple containing the bag A */
    Two                 colNoA,                 /* IN column on which the bag A is defined */
    Four                ornOrScanIdB,           /* IN relation containing the bag A */
    Boolean             useScanFlagB,           /* IN TRUE if above parameter is scan id */
    TupleID*            tidB,                   /* IN tuple containing the bag B */
    Two                 colNoB)                 /* IN column on which the bag B is defined */
{
    Four                e;                      /* error code */
    Four                ornA;
    Four                smScanIdA;
    TupleHdr            tupHdrA;                /* a tuple header */
    Four                tupHdrSizeA;            /* size of tuple header */
    ColDesc*            cdescA;                 /* pointer to the current column descriptor */
    Four                nElementsOfColA;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryA;         /* pointer to an entry of relation table */
    Four                ornB;
    Four                smScanIdB;
    TupleHdr            tupHdrB;                /* a tuple header */
    Four                tupHdrSizeB;            /* size of tuple header */
    ColDesc*            cdescB;                 /* pointer to the current column descriptor */
    Four                nElementsOfColB;        /* # of elements of the column */
    lrds_RelTableEntry* relTableEntryB;         /* pointer to an entry of relation table */

    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                startNoBuf;
    Four                elementsBufTop;

    Four                startNoBufA;
    Four                nElementsInBufA;
    char                elementsBufA[PAGESIZE];
    char                elementSizesBufA[PAGESIZE];
    char*               elementA;
    Four                elementSizeA;
    Four                 keySizeA;

    Four                startNoBufB;
    Four                nElementsInBufB;
    char                elementsBufB[PAGESIZE];
    char                elementSizesBufB[PAGESIZE];
    char*               elementB;
    Four                elementSizeB;
    Four                 keySizeB;

    Four                i, j, k;
    Four                index;
    Boolean             writeFlag;
    Four                compareResult;
    char*               writeElement;
    Four                writeSize;

    LockParameter       fileLockupA;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupA;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrA;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrA;         /* pointer to the lockup value */

    LockParameter       fileLockupB;            /* lockup for SM_Fetch Tuple */
    LockParameter       objLockupB;             /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtrB;        /* pointer to the lockup value */
    LockParameter       *objLockupPtrB;         /* pointer to the lockup value */
    ColDesc             *relTableEntryA_cdesc;  
    ColDesc             *relTableEntryB_cdesc;  


    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_DifferenceWith(handle)"));

    /* Check parameters*/
    if (useScanFlagA == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdA)) ERR(handle, eBADPARAMETER);

    if (useScanFlagA) {
        ornA = LRDS_SCANTABLE(handle)[ornOrScanIdA].orn;
        smScanIdA = LRDS_SCANTABLE(handle)[ornOrScanIdA].smScanId;
    } else {
        ornA = ornOrScanIdA;
        smScanIdA = NIL;
    }

    if (useScanFlagB == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB == FALSE && !LRDS_VALID_ORN(handle, ornOrScanIdB)) ERR(handle, eBADPARAMETER);

    if (useScanFlagB) {
        ornB = LRDS_SCANTABLE(handle)[ornOrScanIdB].orn;
        smScanIdB = LRDS_SCANTABLE(handle)[ornOrScanIdB].smScanId;
    } else {
        ornB = ornOrScanIdB;
        smScanIdB = NIL;
    }


    /* Get the relation table entry. */
    relTableEntryA = LRDS_GET_RELTABLE_ENTRY(handle, ornA);
    relTableEntryA_cdesc = PHYSICAL_PTR(relTableEntryA->cdesc); 
    cdescA         = &relTableEntryA_cdesc[colNoA];
    relTableEntryB = LRDS_GET_RELTABLE_ENTRY(handle, ornB);
    relTableEntryB_cdesc = PHYSICAL_PTR(relTableEntryB->cdesc); 
    cdescB         = &relTableEntryB_cdesc[colNoB];


    /* Is the column valid? */
    if (colNoA >= relTableEntryA->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryA_cdesc[colNoA].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    if (colNoB >= relTableEntryB->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (relTableEntryB_cdesc[colNoB].complexType != SM_COMPLEXTYPE_COLLECTIONBAG)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlagA && tidA == NULL) tidA = &LRDS_SCANTABLE(handle)[ornOrScanIdA].tid;
    if (useScanFlagB && tidB == NULL) tidB = &LRDS_SCANTABLE(handle)[ornOrScanIdB].tid;

    /* test bag compatibility */
    if(!(relTableEntryA_cdesc[colNoA].type == relTableEntryB_cdesc[colNoB].type))
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (LRDS_USEROPENRELTABLE(handle)[ornA].tmpRelationFlag) {
        fileLockupPtrA = objLockupPtrA = NULL;

    } else {
        fileLockupA.mode = L_IX;
        fileLockupA.duration = L_COMMIT;
        fileLockupPtrA = &fileLockupA;

        objLockupA.mode = L_X;
        objLockupA.duration = L_COMMIT;
        objLockupPtrA = &objLockupA;
    }

    if (LRDS_USEROPENRELTABLE(handle)[ornB].tmpRelationFlag) {
        fileLockupPtrB = objLockupPtrB = NULL;

    } else {
        fileLockupB.mode = L_IS;
        fileLockupB.duration = L_COMMIT;
        fileLockupPtrB = &fileLockupB;

        objLockupB.mode = L_S;
        objLockupB.duration = L_COMMIT;
        objLockupPtrB = &objLockupB;
    }

    /* Get tupHdr */
    tupHdrSizeA = TUPLE_HEADER_SIZE(relTableEntryA->ri.nColumns, relTableEntryA->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA,
                         (ObjectID*)tidA, 0, tupHdrSizeA, (char*)&tupHdrA,
                         (relTableEntryA->isCatalog) ? NULL : fileLockupPtrA,
                         (relTableEntryA->isCatalog) ? NULL : objLockupPtrA);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeA = TUPLE_HEADER_SIZE(tupHdrA.nVarCols + tupHdrA.nFixedCols, tupHdrA.nVarCols);

    tupHdrSizeB = TUPLE_HEADER_SIZE(relTableEntryB->ri.nColumns, relTableEntryB->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &relTableEntryB->ri.fid, smScanIdB, useScanFlagB,
                         (ObjectID*)tidB, 0, tupHdrSizeB, (char*)&tupHdrB,
                         (relTableEntryB->isCatalog) ? NULL : fileLockupPtrB,
                         (relTableEntryB->isCatalog) ? NULL : objLockupPtrB);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    tupHdrSizeB = TUPLE_HEADER_SIZE(tupHdrB.nVarCols + tupHdrB.nFixedCols, tupHdrB.nVarCols);

    /* get keysize and check it's compatibility in SM_STRING, SM_VARSTRING */
    e = lrds_Collection_GetKeySize(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, &keySizeA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetKeySize(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB, &keySizeB);
    if(e < eNOERROR) ERR(handle, e);

    if(keySizeA != keySizeB)
        ERR(handle, eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                      colNoA, &nElementsOfColA);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                      colNoB, &nElementsOfColB);
    if(e < eNOERROR) ERR(handle, e);

    /* initialize read buffer and write buffer */
    startNoBuf      = 0;
    elementsBufTop  = 0;

    startNoBufA     = 0;
    nElementsInBufA = 0;

    startNoBufB     = 0;
    nElementsInBufB = 0;

    i = 0; j = 0; k = 0;
    if(relTableEntryA_cdesc[colNoA].type == SM_VARSTRING)
    {   /* variable size element */
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 ||  (startNoBufA + nElementsInBufA) <= i))
            {
                Four nReads;
                Four elementsSize;

                startNoBufA += nElementsInBufA;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufA) / sizeof(Four);
                if(nReads > nElementsOfColA)
                    nReads = nElementsOfColA;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA,
                                                         colNoA, startNoBufA, nReads,
                                                         (Four*)elementSizesBufA);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf*/
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufA)[index]) > sizeof(elementsBufA))
                        break;
                    elementsSize += ((Four*)elementSizesBufA)[index];
                }
                nElementsInBufA = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                    startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA     = elementsBufA;
                elementSizeA = ((Four*)elementSizesBufA)[0];
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four nReads;
                Four elementsSize;

                startNoBufB += nElementsInBufB;

                /* read elementSizes */
                nReads = sizeof(elementSizesBufB) / sizeof(Four);
                if(nReads > nElementsOfColB)
                    nReads = nElementsOfColB;

                e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB,
                                                         colNoB, startNoBufB, nReads,
                                                         (Four*)elementSizesBufB);
                if(e < eNOERROR) ERR(handle, e);

                /* calculate nReads for elementsBuf */
                for(index = 0, elementsSize = 0; index < nReads; index++)
                {
                    if((elementsSize + ((Four*)elementSizesBufB)[index]) > sizeof(elementsBufB))
                        break;
                    elementsSize += ((Four*)elementSizesBufB)[index];
                }
                nElementsInBufB = index;

                e = lrds_Collection_VarElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                    startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB     = elementsBufB;
                elementSizeB = ((Four*)elementSizesBufB)[0];
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break;
            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                writeSize    = elementSizeA;
                i++;
                elementA    += elementSizeA;
                elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, elementSizeA, elementA, elementSizeB, elementB, keySizeA);

                if(compareResult == 0)                        /* elementA == elementB */
                {
                    i++;
                    j++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
                else if(compareResult < 0)                    /* elementA < elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    writeSize    = elementSizeA;
                    i++;
                    elementA    += elementSizeA;
                    elementSizeA = ((Four*)elementSizesBufA)[i - startNoBufA];
                }
                else if(compareResult > 0)                    /* elementA > elementB */
                {
                    j++;
                    elementB    += elementSizeB;
                    elementSizeB = ((Four*)elementSizesBufB)[j - startNoBufB];
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_VarElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                         startNoBuf + nElementsOfColA,
                                                         k - startNoBuf,
                                                         (Four*)elementSizesBuf,
                                                         elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                ((Four*)elementSizesBuf)[k - startNoBuf] = writeSize;
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_VarElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                 startNoBuf + nElementsOfColA,
                                                 k - startNoBuf,
                                                 (Four*)elementSizesBuf,
                                                 elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* remove previous elements */
        e = lrds_Collection_VarElementDelete(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                             0, nElementsOfColA);
        if(e < eNOERROR) ERR(handle, e);

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, k);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {   /* fixed size element */
        writeSize = cdescA->length;
        while(i < nElementsOfColA || j < nElementsOfColB)
        {
            if(i < nElementsOfColA &&
               (nElementsInBufA == 0 || (startNoBufA + nElementsInBufA) <= i))
            {
                Four temp;

                temp = nElementsInBufA;
                nElementsInBufA = (sizeof(elementsBufA) / cdescA->length);
                if((nElementsOfColA - startNoBufA) < nElementsInBufA)
                    nElementsInBufA = nElementsOfColA - startNoBufA;
                startNoBufA += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                      startNoBufA, nElementsInBufA, elementsBufA);
                if(e < eNOERROR) ERR(handle, e);

                elementA = elementsBufA;
            }

            if(j < nElementsOfColB &&
               (nElementsInBufB == 0 || (startNoBufB + nElementsInBufB) <= j))
            {
                Four temp;

                temp = nElementsInBufB;
                nElementsInBufB = (sizeof(elementsBufB) / cdescB->length);
                if((nElementsOfColB - startNoBufB) < nElementsInBufB)
                    nElementsInBufB = nElementsOfColB - startNoBufB;
                startNoBufB += temp;

                e = lrds_Collection_FixedElementFetch(handle, ornOrScanIdB, useScanFlagB, tidB, &tupHdrB, colNoB,
                                                      startNoBufB, nElementsInBufB, elementsBufB);
                if(e < eNOERROR) ERR(handle, e);

                elementB = elementsBufB;
            }

            writeFlag = FALSE;

            if(i >= nElementsOfColA)
            {
                break;
            }
            else if(j >= nElementsOfColB)
            {
                writeFlag    = TRUE;
                writeElement = elementA;
                elementA    += writeSize;
                i++;
            }
            else
            {
                compareResult = lrds_Collection_CompareData(handle, cdescA->type, cdescA->length, elementA, cdescB->length, elementB, keySizeA);

                if(compareResult == 0)          /* elementA == elementB */
                {
                    elementA    += writeSize;
                    elementB    += writeSize;
                    i++;
                    j++;
                }
                else if(compareResult < 0)      /* elementA < elementB */
                {
                    writeFlag    = TRUE;
                    writeElement = elementA;
                    elementA    += writeSize;
                    i++;
                }
                else if(compareResult > 0)      /* elementA > elementB */
                {
                    elementB    += writeSize;
                    j++;
                }
            }

            if(writeFlag == TRUE)
            {
                if((elementsBufTop + writeSize) > sizeof(elementsBuf))
                {
                    /* flush write buffer */
                    /* insert elementsBuf which contains elements from startNoBuf to k */
                    e = lrds_Collection_FixedElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                           startNoBuf + nElementsOfColA,
                                                           k - startNoBuf, elementsBuf);
                    if(e < eNOERROR) ERR(handle, e);

                    /* reset write buffer */
                    startNoBuf     = k;
                    elementsBufTop = 0;
                }

                memcpy(elementsBuf + elementsBufTop, writeElement, writeSize);
                elementsBufTop += writeSize;
                k ++;
            }
        }

        /* flush write buffer */
        if(startNoBuf < k)
        {
            e = lrds_Collection_FixedElementInsert(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                                   startNoBuf + nElementsOfColA,
                                                   k - startNoBuf, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* remove previous elements */
        e = lrds_Collection_FixedElementDelete(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA,
                                               0, nElementsOfColA);
        if(e < eNOERROR) ERR(handle, e);

        /* set n elements */
        e = lrds_Collection_SetN_Elements(handle, ornOrScanIdA, useScanFlagA, tidA, &tupHdrA, colNoA, k);
        if(e < eNOERROR) ERR(handle, e);

    }

    /* Bag tupHdr */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntryA->ri.fid, smScanIdA, useScanFlagA, (ObjectID*)tidA,
                          0, tupHdrSizeA, (char*)&tupHdrA, tupHdrSizeA, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

Four LRDS_CollectionBag_Scan_Open(
    Four handle,
    Four        ornOrScanId,        /* IN relation containing the bag A */
    Boolean     useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID*    tid,                /* IN tuple containing the bag */
    Two         colNo)              /* IN column on which the bag is defined */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Scan_Open(handle)"));
    return lrds_Collection_Scan_Open(handle, ornOrScanId, useScanFlag, tid, colNo);
}

Four LRDS_CollectionBag_Scan_Close(
    Four handle,
    Four        collectionScanId)   /* IN scan id for a scan on a bag */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Scan_Close(handle)"));
    return lrds_Collection_Scan_Close(handle, collectionScanId);
}

Four LRDS_CollectionBag_Scan_NextElements(
    Four handle,
    Four        collectionScanId,   /* IN scan id for a scan on a bag */
    Four        nElements,          /* IN # of elements to read */
    Four*       elementSizes,       /* OUT buffer to return the element sizes for variable elements */
    Four        sizeOfElements,     /* IN size of elements */
    void*       elements)           /* OUT buffer to return the read elements */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Scan_NextElements(handle)"));

    return lrds_Collection_Scan_NextElements(handle, collectionScanId, nElements, elementSizes, sizeOfElements, elements);
}

Four LRDS_CollectionBag_Scan_GetSizeOfNextElements(
    Four handle,
    Four        collectionScanId,   /* IN scan id for a scan on a bag */
    Four        nElements,          /* IN # of elements to read */
    Four*       elementsSize)       /* OUT sizeof elements to read */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Scan_GetSizeOfNextElements(handle)"));

    return lrds_Collection_Scan_GetSizeOfNextElements(handle, collectionScanId, nElements, elementsSize);
}

Four LRDS_CollectionBag_Scan_InsertElements(
    Four handle,
    Four        collectionScanId,   /* IN bag where to insert the elements */
    Four        nElements,          /* IN # of elements to insert */
    Four*       elementSizes,       /* IN element sizes of elements to insert */
    void*       elements)           /* IN elements to insert */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Scan_InsertElements(handle)"));

    return lrds_Collection_Scan_InsertElements(handle, collectionScanId, nElements, elementSizes, elements);
}

Four LRDS_CollectionBag_Scan_DeleteElements(
    Four handle,
    Four        collectionScanId)   /* IN bag from which to delete the element */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_Scan_DeleteElements(handle)"));

    return lrds_Collection_Scan_DeleteElements(handle, collectionScanId);
}

Four lrds_CollectionBag_FixedElementSearch(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the bag is defined */
    void*               element,                /* IN element to find */
    Four*               ith)                    /* OUT index of element found */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                startIndex;             /* variable for binary search */
    Four                endIndex;               /* variable for binary search */
    Four                midIndex;               /* variable for binary search */
    Four                nElementsOfCol;         /* # of elements in the column */
    Four                nReads;                 /* # of elements read */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    char                elementsBuf[PAGESIZE];  /* buffer for holding the elements */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                i;
    Four                compareResult;
    char*               elementInBuf;
    Four                 keySize;
    ColDesc             *relTableEntry_cdesc;  

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementInsert(handle)"));

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* fetch n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    /* if empty bag, then return FALSE */
    if(nElementsOfCol == 0)
    {
        *ith = 0;
        return FALSE;
    }

    /* bag up variables for binary search */
    startIndex = 0;
    endIndex   = nElementsOfCol - 1;
    nReads     = 1;

    /* get keysize  */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo, &keySize);
    if(e < eNOERROR) ERR(handle, e);

    /* check element of startIndex */
    e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                          startIndex, 1, elementsBuf);
    if(e < eNOERROR) ERR(handle, e);

    compareResult = lrds_Collection_CompareData(handle, cdesc->type, cdesc->length, element, cdesc->length, elementsBuf, keySize);

    if(compareResult < 0)         /* element < elementsBuf */
    {
        *ith = startIndex;
        return FALSE;
    }
    else if(compareResult == 0)   /* element == elementsBuf */
    {
        *ith = startIndex;
        return TRUE;
    }

    /* check element of endIndex */
    e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                          endIndex, 1, elementsBuf);
    if(e < eNOERROR) ERR(handle, e);

    compareResult = lrds_Collection_CompareData(handle, cdesc->type, cdesc->length, element, cdesc->length, elementsBuf, keySize);

    if(compareResult > 0)         /* element > elementsBuf */
    {
        *ith = endIndex + 1;
        return FALSE;
    }
    else if(compareResult == 0)
    {
        *ith = endIndex;
        return TRUE;
    }

    while(startIndex < endIndex)
    {
        /* change search scheme from binary search to sequential search */
        if((endIndex - startIndex + 1) <= ((Four)sizeof(elementsBuf) / cdesc->length))
        {
            nReads = endIndex - startIndex + 1;
            break;
        }

        midIndex = startIndex + (endIndex - startIndex) / 2;

        /* fetch element of midIndex */
        e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                              midIndex, 1, elementsBuf);
        if(e < eNOERROR) ERR(handle, e);

        compareResult = lrds_Collection_CompareData(handle, cdesc->type, cdesc->length, element, cdesc->length, elementsBuf, keySize);

        if(compareResult < 0)         /* element < elementsBuf */
            endIndex = midIndex;
        else if(compareResult > 0)    /* element > elementsBuf */
            startIndex = midIndex;
        else
        {
            /* found */
            *ith = midIndex;
            return TRUE;
        }
    }

    /* do sequential search */
    e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                          startIndex, nReads, elementsBuf);
    elementInBuf = elementsBuf;
    for(i = 0; i < nReads; i++)
    {
        compareResult = lrds_Collection_CompareData(handle, cdesc->type, cdesc->length, element, cdesc->length, elementInBuf, keySize);

        if(compareResult == 0)     /* element == elementsBuf[i] */
        {
            /* found */
            *ith = i + startIndex;
            return TRUE;
        }
        else if(compareResult < 0) /* element < elementsBuf[i] */
        {
            *ith = i + startIndex;
            return FALSE;
        }

        elementInBuf += cdesc->length;
    }

    *ith = endIndex + 1;
    return FALSE;
}

Four lrds_CollectionBag_VarElementSearch(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the bag */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the bag is defined */
    Four                elementSize,            /* IN size of element to find */
    void*               element,                /* IN element to find */
    Four*               ith)                    /* OUT index of element found */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                startIndex;             /* variable for binary search */
    Four                endIndex;               /* variable for binary search */
    Four                midIndex;               /* variable for binary search */
    Four                nElementsOfCol;         /* # of elements in the column */
    Four                nReads;                 /* # of elements read */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */
    char                elementSizesBuf[PAGESIZE]; /* buffer for holding the size of the elements */
    char                elementsBuf[PAGESIZE];  /* buffer for holding the elements */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                i;
    Four                elementsSize;
    Four                compareResult;
    char*               elementInBuf;
    Four                 keySize;
    ColDesc             *relTableEntry_cdesc; 

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementInsert(handle)"));

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* fetch n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* fetch keysize */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo, &keySize);
    if(e < 0) ERR(handle, e);

    /* if empty bag, then return FALSE */
    if(nElementsOfCol == 0)
    {
        *ith = 0;
        return FALSE;
    }

    /* set up variables for binary search */
    startIndex = 0;
    endIndex   = nElementsOfCol - 1;
    nReads     = 1;

    /* check element of startIndex */
    /* fetch element size of startIndex */
    e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                             startIndex, 1, (Four*)elementSizesBuf);
    if(e < eNOERROR) ERR(handle, e);

    /* get keysize  */
    e = lrds_Collection_GetKeySize(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo, &keySize);
    if(e < eNOERROR) ERR(handle, e);

    /* fetch element of startIndex */
    e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                        startIndex, 1, elementsBuf);
    if(e < eNOERROR) ERR(handle, e);

    compareResult = lrds_Collection_CompareData(handle, cdesc->type, elementSize, element, ((Four*)elementSizesBuf)[0], elementsBuf, keySize);

    if(compareResult < 0)         /* element < elementsBuf */
    {
        *ith = startIndex;
        return FALSE;
    }
    else if(compareResult == 0)   /* element == elementsBuf */
    {
        *ith = startIndex;
        return TRUE;
    }

    /* check element of endIndex */
    /* fetch element size of endIndex */
    e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                             endIndex, 1, (Four*)elementSizesBuf);
    if(e < eNOERROR) ERR(handle, e);

    /* fetch element of endIndex */
    e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                        endIndex, 1, elementsBuf);
    if(e < eNOERROR) ERR(handle, e);

    compareResult = lrds_Collection_CompareData(handle, cdesc->type, elementSize, element, ((Four*)elementSizesBuf)[0], elementsBuf, keySize);

    if(compareResult > 0)         /* element > elementsBuf */
    {
        *ith = endIndex + 1;
        return FALSE;
    }
    else if(compareResult == 0)   /* element == elementsBuf */
    {
        *ith = endIndex;
        return TRUE;
    }

    while(startIndex < endIndex)
    {
        /* change search scheme from binary search to sequential search */
        if((endIndex - startIndex + 1) <= (sizeof(elementSizesBuf) / sizeof(Four)))
        {
            e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                    startIndex, (endIndex - startIndex + 1),
                                                    (Four*)elementSizesBuf);
            for(i = 0, elementsSize = 0; i < (endIndex - startIndex + 1); i++)
                elementsSize += ((Four*)elementSizesBuf)[i];

            if(elementsSize <= sizeof(elementsBuf))
            {
                nReads = endIndex - startIndex + 1;
                break;
            }
        }

        if((endIndex - startIndex + 1) > nReads)
            break;

        midIndex = startIndex + (endIndex - startIndex) / 2;

        /* fetch element size of midIndex */
        e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                 midIndex, 1, (Four*)elementSizesBuf);
        if(e < eNOERROR) ERR(handle, e);

        /* fetch element of midIndex */
        e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                            midIndex, 1, elementsBuf);
        if(e < eNOERROR) ERR(handle, e);

        compareResult = lrds_Collection_CompareData(handle, cdesc->type, elementSize, element, ((Four*)elementSizesBuf)[0], elementsBuf, keySize);

        if(compareResult < 0)           /* element < elementsBuf */
            endIndex = midIndex;
        else if(compareResult > 0)      /* element > elementsBuf */
            startIndex = midIndex;
        else
        {
            /* found */
            *ith = midIndex;
            return TRUE;
        }
    }

    /* do sequential search */
    e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                             startIndex, nReads, (Four*)elementSizesBuf);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                        startIndex, nReads, elementsBuf);
    if(e < eNOERROR) ERR(handle, e);
    elementInBuf = elementsBuf;
    for(i = 0; i < nReads; i++)
    {
        compareResult = lrds_Collection_CompareData(handle, cdesc->type, elementSize, element, ((Four*)elementSizesBuf)[i], elementInBuf, keySize);

        if(compareResult == 0)     /* element == elementsBuf[i] */
        {
            /* found */
            *ith = i + startIndex;
            return TRUE;
        }
        else if(compareResult < 0) /* element < elementsBuf[i] */
        {
            *ith = i + startIndex;
            return FALSE;
        }

        elementInBuf += ((Four*)elementSizesBuf)[i];
    }

    *ith = endIndex + 1;
    return FALSE;
}

/*
 * Function: Four LRDS_CollectionBag_IsNull(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Check where the given colNo is null or not.
 *
 * Retuns:
 *  error code
 */
Four LRDS_CollectionBag_IsNull(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Two                 colNo                   /* IN column on which the set is created */
)
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_CollectionBag_IsNull(handle)"));


    return lrds_Collection_IsNull(handle, ornOrScanId, useScanFlag, tid, colNo);
}

