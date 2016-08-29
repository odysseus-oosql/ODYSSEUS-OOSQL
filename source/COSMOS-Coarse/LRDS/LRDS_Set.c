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
 * Module: LRDS_Set.c
 *
 * Description:
 *  Implements a set.
 *
 * Exports:
 *  Four LRDS_Set_Create(Four, Boolean, TupleID*, Two)
 *  Four LRDS_Set_Destroy(Four, Boolean, TupleID*, Two)
 *  Four LRDS_Set_InsertElements(Four, Boolean, TupleID*, Two, Four, void*)
 *  Four LRDS_Set_DeleteElements(Four, Boolean, TupleID*, Two, Four, void*)
 *  Four LRDS_Set_IsMember(Four, Boolean, TupleID*, Two, void*)
 *  Four LRDS_Set_Scan_Open(Four, Boolean, TupleID*, Two)
 *  Four LRDS_Set_Scan_Close(Four)
 *  Four LRDS_Set_Scan_NextElements(Four, Four, void*)
 *  Four LRDS_Set_Scan_InsertElements(Four, Four, void*)
 *  Four LRDS_Set_Scan_DeleteElements(Four)
 *  Four lrds_Set_RemoveFromIndex(Four, Boolean, Four, TupleID*, Two,
 *                                Four, Four, LockParameter*)
 *  Four lrds_Set_InsertIntoIndex(Four, IndexInfo*, TupleID*, LockParameter*)
 *
 * Notice:
 *  The input parameter relScanId is used to identify the relation which
 *  contains the given set. We don't use a scan identified by the relScanId.
 *  I suggest that the relScanId be replaced with the open relation number.
 *  When this change is made, the input parameter `tuple id' should not be NULL.
 */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM_Internal.h"
#include "LRDS.h"
#include "LM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*
 * Function: Four LRDS_Set_Create(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Create a set on the given column of the given tuple of the given relation.
 *  Before creating a set the column has NULL value.
 *
 * Retuns:
 *  error code
 */
Four LRDS_Set_Create(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Two                 colNo)                  /* IN column on which the set is created */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                oldTupHdrSize;          /* old size of tuple header */
    Four                newTupHdrSize;          /* new size of tuple header */
    TupleHdr            tupHdr;                 /* a tuple header */
    unsigned char       *nullVector;            /* bit array of null flags */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_Create(handle)"));


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

    if (cdesc->complexType != SM_COMPLEXTYPE_SET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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

    /* Fetch the tuple header. */
    oldTupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, oldTupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    newTupHdrSize = oldTupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols) {
        e = lrds_AddColumns(handle, ornOrScanId, useScanFlag, tid, &tupHdr);
        if (e < 0) ERR(handle, e);

        newTupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);
    }

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (!BITTEST(nullVector, colNo)) ERR(handle, eSET_EXIST_LRDS);

    /* If # of elements should be represented in a set, its code should be placed here. */

    /* Reset the corresponding bit of array of null flags. */
    BITRESET(nullVector, colNo);

    /* Update the tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, oldTupHdrSize, (char*)&tupHdr,
                          newTupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_Set_Create() */



/*
 * Function: Four LRDS_Set_Destroy(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Destroy the set on the given column of the given tuple of the given relation.
 *  The column will be set to NULL value after destroying it.
 *
 * Returns:
 *  error code
 */
Four LRDS_Set_Destroy(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Two                 colNo)                  /* IN column on which we destroy the set  */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                tupHdrSize;             /* size of tuple header */
    Four                start;                  /* starting offset of the set column */
    Four                length;                 /* length of the set column */
    TupleHdr            tupHdr;                 /* a tuple header */
    unsigned char       *nullVector;            /* bit array of null flags */
    Four                i, j;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc; 
    IndexInfo           *relTableEntry_ii;  
    LockReply           lockReply;              /* lock reply */
    LockMode            oldMode;                /* lock mode */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_Destroy(handle)"));

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
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo]; 

    if (cdesc->complexType != SM_COMPLEXTYPE_SET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;


    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsSetScanTable.nEntries; i++) {
	if (LRDS_SETSCANTABLE(handle)[i].ornOrRelScanId == NIL) continue; /* not used */

	if (EQUAL_TUPLEID(LRDS_SETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_SETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    return(eSET_OPENSCAN_LRDS);
	}
    }

    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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

    /* Fetch the tuple header. */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eSET_NOTEXIST_LRDS); 


    /*
    ** Destroy a set.
    */
    /* Get the starting offset and length of the column. */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    /*
    ** Automatic Index Support:
    ** Remove elements from the index if an index is on the set.
    */
    /* lock on the data file that the automatic index is built on to support the automatic index */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag && !relTableEntry->isCatalog) {

        /* check lock parameter */
        if (fileLockup.duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        if (fileLockup.mode != L_X && fileLockup.mode != L_IX && fileLockup.mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);

        /* lock on the data file */
        e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid, fileLockup.mode, fileLockup.duration,
                           L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    /* Check if an index is on the set. */
    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {
	if (relTableEntry_ii[i].colNo[0] == colNo) break; 
    }

    if (i != relTableEntry->ri.nIndexes) { /* index is on the set. */
	e = lrds_Set_RemoveFromIndex(handle, ornOrScanId, useScanFlag, i, tid, colNo,
                                     start, length, fileLockupPtr, objLockupPtr);
	if (e < 0) ERR(handle, e);
    }

    /* Delete all data in the given column. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, start, length, NULL, 0, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /* Set the corresponding bit of array of null flags. */
    BITSET(nullVector, colNo);

    /* Adjust the ending offset of the variable-length columns. */
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
	tupHdr.varColOffset[i] -= length;

    /* Update the tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_Set_Destroy() */



/*
 * Function: Four LRDS_Set_InsertElements(Four, Boolean, TupleID*, Two, Four, void*)
 *
 * Description:
 *  Insert some elements into the given set.
 *  'elements' has the following structure:
 *    case 1) element is fixed-length
 *
 *    case 2) element is variable-length
 *
 * Returns:
 *  error code
 */
Four LRDS_Set_InsertElements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements,              /* IN # of elements to insert */
    void                *elements)              /* IN elements to insert */
{
    Two                 s;                      /* for setting of key length */
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                indexIdx;               /* index on array of index informations */
    Four                tupHdrSize;             /* size of tuple header */
    Four                nInsertedBytes;         /* amount of bytes inserted into the column */
    TupleHdr            tupHdr;                 /* a tuple header */
    KeyValue            kval;                   /* key value of an index entry */
    unsigned char       *nullVector;            /* bit array of null flags */
    Set_ElementLength   elementLength;          /* length of an element */
    char                *ptr;
    Four                i, j;
    LRDS_MBR            mbr;                    /* mbr */
    MLGF_HashValue      mlgfKval[MLGF_MAXNUM_KEYS]; /* mlgf hash values */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;  
    LockReply           lockReply;              /* lock reply */
    LockMode            oldMode;                /* lock mode */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_InsertElements(handle)"));


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
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo]; 

    if (cdesc->complexType != SM_COMPLEXTYPE_SET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (nElements < 0 || (nElements > 0 && elements == NULL)) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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

    /* Fetch the tuple header. */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eSET_NOTEXIST_LRDS);


    /*
    ** Insert the given elements.
    */
    if (cdesc->type == SM_VARSTRING) {
	/* variable-length element */

	/* check the length of each element */
	nInsertedBytes = 0;
	ptr = elements;
	for (i = 0; i < nElements; i++) {
	    memcpy(&elementLength, ptr, sizeof(Set_ElementLength));

	    if (elementLength > cdesc->length) ERR(handle, eBADPARAMETER);
	    ptr += sizeof(Set_ElementLength) + elementLength;
	    nInsertedBytes += sizeof(Set_ElementLength) + elementLength;
	}

	e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, tupHdr.varColOffset[cdesc->varColNo],
                              0, elements, nInsertedBytes, NULL, NULL);
	if (e < 0) ERR(handle, e);
    } else {
	/* fixed-length element */
	nInsertedBytes = cdesc->length * nElements;

	e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, tupHdr.varColOffset[cdesc->varColNo],
                              0, elements, nInsertedBytes, NULL, NULL);
	if (e < 0) ERR(handle, e);
    }

    /* Update the tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
	tupHdr.varColOffset[i] += nInsertedBytes;

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                          tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*
    ** Automatic Index Support:
    ** Insert elements into the index if an index is on the set.
    */
    /* lock on the data file that the automatic index is built on to support the automatic index */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag && !relTableEntry->isCatalog) {

        /* check lock parameter */
        if (fileLockup.duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        if (fileLockup.mode != L_X && fileLockup.mode != L_IX && fileLockup.mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);

        /* lock on the data file */
        e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid, fileLockup.mode, fileLockup.duration,
                           L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    /* Check if an index is on the set. */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++) {
	if (relTableEntry_ii[indexIdx].colNo[0] == colNo) break; 
    }

    if (indexIdx != relTableEntry->ri.nIndexes) { /* index is on the set. */
	switch (relTableEntry_ii[indexIdx].indexType) { 
	  case SM_INDEXTYPE_BTREE:
	    if (cdesc->type == SM_VARSTRING) {
		/* variable-length element */

		ptr = elements;
		for (i = 0; i < nElements; i++) {
		    memcpy(&elementLength, ptr, sizeof(Set_ElementLength));
		    ptr += sizeof(Set_ElementLength);

		    kval.len = sizeof(Two) + elementLength;
		    /* copy 'elementLength' to the temporary variable of data type Two. */
		    s = elementLength;
		    memcpy(&(kval.val[0]), &s, sizeof(Two));
		    memcpy(&(kval.val[sizeof(Two)]), ptr, elementLength);
		    ptr += elementLength;

		    /* Insert element into the index. */
		    e = SM_InsertIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
					    &(relTableEntry_ii[indexIdx].kdesc.btree),
					    &kval, (ObjectID*)tid, objLockupPtr); 
		    if (e < 0) ERR(handle, e);
		}

	    } else {
		/* fixed-length element */
		kval.len = cdesc->length;

		ptr = elements;
		for (i = 0; i < nElements; i++) {

		    /* copy the element value. */
		    memcpy(&kval.val[0], ptr, cdesc->length);
		    ptr += cdesc->length;

		    /* Insert element into the index. */
		    e = SM_InsertIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
					    &(relTableEntry_ii[indexIdx].kdesc.btree),
					    &kval, (ObjectID*)tid, objLockupPtr); 
		    if (e < 0) ERR(handle, e);
		}
	    } /* end of if */
	    break;

	  case SM_INDEXTYPE_MLGF:
	    /* assert that the column's type is SM_MBR */
	    ptr = elements;
	    for (i = 0; i < nElements; i++) {

		/* copy the element value. */
		memcpy(&mbr, ptr, cdesc->length);
		ptr += cdesc->length;

		for (j = 0; j < MBR_NUM_PARTS; j++) mlgfKval[i] = mbr.values[j];

		/* Insert element into the index. */
		e = SM_MLGF_InsertIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
					     &(relTableEntry_ii[indexIdx].kdesc.mlgf),
					     mlgfKval, (ObjectID*)tid, NULL, objLockupPtr); 
		if (e < 0) ERR(handle, e);
	    }
	    break;

	  default:
	    ERR(handle, eINTERNAL);
	} /* end of switch */
    } /* end of if */

    return(eNOERROR);

} /* LRDS_Set_InsertElements() */



/*
 * Function: Four LRDS_Set_DeleteElements(Four, Boolean, TupleID*, Two, Four, void*)
 *
 * Description:
 *  Delete the given elements from the given set.
 *
 * Returns:
 *  error code
 */
Four LRDS_Set_DeleteElements(
    Four handle,
    Four                        ornOrScanId,            /* IN open relation no or scan id*/
    Boolean                     useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID                     *tid,                   /* IN tuple containing the set */
    Two                         colNo,                  /* IN column on which the set is defined */
    Four                        nElements,              /* IN # of elements to delete */
    void                        *elements)              /* IN elements to delete */
{
    Two                         s;                      /* for setting of key length */
    Four                        e;                      /* error code */
    Four                        orn;
    Four                        smScanId;
    Four                        indexIdx;               /* index on array of index informations */
    Four                        tupHdrSize;             /* size of tuple header */
    Four                        start;                  /* starting offset of fetch */
    Four                        length;                 /* length of the set column */
    Four                        totalDeletedBytes;      /* amount of bytes deleted from the column */
    TupleHdr                    tupHdr;                 /* a tuple header */
    KeyValue                    kval;                   /* key value of an index entry */
    unsigned char               *nullVector;            /* bit array of null flags */
    Four                        nBytesToRead;           /* amount of bytes to read from disk */
    Four                        size;                   /* amount of data in the buffer */
    Four                        updateStart;            /* starting offset of update within a buffer */
    Four                        nBytesDeleted;          /* amount of deleted bytes within a buffer */
    char                        buf[PAGESIZE];          /* buffer for holding the elements */
    Four                        current;                /* points to the current element */
    char                        *ptr;                   /* points to an element among the given elements */
    Set_ElementLength           elementLength;          /* length of an element */
    Set_ElementLength           elementLength2;         /* length of an element among the given element */
    LRDS_MBR                    mbr;                    /* mbr */
    MLGF_HashValue              mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    Four      i;
    lrds_RelTableEntry          *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc                     *cdesc;                 /* pointer to the current column descriptor */
    LockParameter               fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter               objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter               *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter               *objLockupPtr;          /* pointer to the lockup value */
    IndexInfo                   *relTableEntry_ii;      
    ColDesc                     *relTableEntry_cdesc;   
    LockReply                   lockReply;              /* lock reply */
    LockMode                    oldMode;                /* lock mode */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_DeleteElements(handle)"));


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
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo]; 

    if (cdesc->complexType != SM_COMPLEXTYPE_SET) ERR(handle, eBADPARAMETER);

    /* Are the elements valid? */
    if (nElements < 0 || (nElements > 0 && elements == NULL)) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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

    /* Fetch the tuple header. */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eSET_NOTEXIST_LRDS);


    /* Get the starting offset and length of the column. */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    /* Check if an index is on the set. */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++) {
	if (relTableEntry_ii[indexIdx].colNo[0] == colNo) break; 
    }

    /* There is no index defined on the set column */
    /* if indexIdx is equal to relTableEntry->ri.nIndexes. */
    /* We change indexIdx so that the set column has no index if it has value -1. */
    if (indexIdx == relTableEntry->ri.nIndexes) indexIdx = -1;


    /*
    ** Delete the given elements.
    */
    /* initialize the the number of bytes deleted */
    totalDeletedBytes = 0;

    /* elementLength is the [maximum] length of an element */
    elementLength = cdesc->length;;

    /* lock on the data file that the automatic index is built on to support the automatic index */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag && !relTableEntry->isCatalog) {

        /* check lock parameter */
        if (fileLockup.duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        if (fileLockup.mode != L_X && fileLockup.mode != L_IX && fileLockup.mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);

        /* lock on the data file */
        e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid, fileLockup.mode, fileLockup.duration,
                           L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    if (cdesc->type == SM_VARSTRING) { /* variable-length element */

	/* initialize 'size', which is amount of bytes in the buf */
	size = 0;

	while (length > 0) {

	    /* Get the length of bytes to read per each call. */
	    nBytesToRead = MIN(length, PAGESIZE-size);

	    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                 (ObjectID*)tid, start, nBytesToRead, &buf[size], NULL, NULL);
	    if (e < 0) ERR(handle, e);
	    size += nBytesToRead;


	    updateStart = -1;	/* starting offset of update */
	    nBytesDeleted = 0;	/* amount of bytes deleted */

	    /* for each element in the buffer */
	    for (current = 0; ; ) {

		/* extract the element length of the current element */
		memcpy(&elementLength, &buf[current], sizeof(Set_ElementLength));
		if (size < current + sizeof(Set_ElementLength) + elementLength) break;
		current += sizeof(Set_ElementLength);

		/* for each given element */
		for (ptr = elements, i = 0; i < nElements; i++, ptr += elementLength2) {
		    /* extract the element length */
		    memcpy(&elementLength2, ptr, sizeof(Set_ElementLength));
		    ptr += sizeof(Set_ElementLength);

		    /* exit the loop if found */
		    if (elementLength != elementLength2) continue;
		    if (memcmp(&buf[current], ptr, elementLength) == 0) break;
		}

		if (i == nElements) { /* not found */
		    current += elementLength;
		} else {	      /* found */
		    /*
		    ** Automatic Index Support:
		    **     Delete the element from the index if an index
		    **     is defined on this column.
		    */
		    if (indexIdx != -1) {
			switch (relTableEntry_ii[indexIdx].indexType) { 
			  case SM_INDEXTYPE_BTREE:
			    kval.len = sizeof(Two) + elementLength;
			    /* copy 'elementLength' to the temporary variable of data type Two. */
			    s = elementLength;
			    memcpy(&(kval.val[0]), &s, sizeof(Two));
			    memcpy(&kval.val[sizeof(Two)], &buf[current], elementLength);
			    e = SM_DeleteIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
						    &(relTableEntry_ii[indexIdx].kdesc.btree),
						    &kval, (ObjectID*)tid, objLockupPtr); 
			    if (e < 0) ERR(handle, e);
			    break;

			  case SM_INDEXTYPE_MLGF:
			    /* cannot happen */
			  default:
			    ERR(handle, eINTERNAL);
			}
		    }

		    /* Delete the current element from the buffer. */
		    if (updateStart == -1) updateStart = current - sizeof(Set_ElementLength);

		    memmove(&buf[current-sizeof(Set_ElementLength)],
                            &buf[current+elementLength],
                            size-(current+elementLength));
		    size -= sizeof(Set_ElementLength) + elementLength;
		    nBytesDeleted += sizeof(Set_ElementLength) + elementLength;
		}
	    } /* buffer scan loop */


	    /* Update the disk. */
	    if (nBytesDeleted != 0) {
		e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                      (ObjectID*)tid, start + updateStart,
                                      size - updateStart + nBytesDeleted,
                                      &buf[updateStart], size - updateStart,
                                      NULL, NULL);
		if (e < 0) ERR(handle, e);
	    }


	    /*
	    ** prepare the next loop
	    */
	    start += nBytesToRead - nBytesDeleted; /* adjust next read point */
	    length -= nBytesToRead; /* adjust the remained bytes in this column */

	    /* Now 'size' represents the unused bytes in the buf. */
	    size -= current;

	    /* move the unused bytes */
	    memmove(&buf[0], &buf[current], size);

	    totalDeletedBytes += nBytesDeleted;
	} /* end of while */

    } else {			/* fixed-length element */
	/* setup for the automatic index support */
	if (indexIdx != -1) kval.len = elementLength;

	/* initialize 'size', which is amount of bytes in the buf */
	size = 0;

	while (length > 0) {

	    /* Get the length of bytes to read per each call. */
	    nBytesToRead = MIN(length, PAGESIZE-size);

	    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                 (ObjectID*)tid, start, nBytesToRead, &buf[size],
                                 NULL, NULL);
	    if (e < 0) ERR(handle, e);
	    size += nBytesToRead;


	    updateStart = -1;	/* starting offset of update */
	    nBytesDeleted = 0;	/* amount of bytes deleted */

	    /* for each element in the buffer */
	    for (current = 0; current + elementLength <= size; ) {

		/* for each given element */
		for (ptr = elements, i = 0; i < nElements; i++, ptr += elementLength) {
		    /* exit the loop if found */
		    if (memcmp(&buf[current], ptr, elementLength) == 0) break;
		}

		if (i == nElements) { /* not found */
		    current += elementLength;
		} else {	      /* found */
		    /*
		    ** Automatic Index Support:
		    **     Delete the element from the index if an index
		    **     is defined on this column.
		    */
		    if (indexIdx != -1) {
			switch (relTableEntry_ii[indexIdx].indexType) { 
			  case SM_INDEXTYPE_BTREE:
			    memcpy(&kval.val[0], &buf[current], elementLength);
			    e = SM_DeleteIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
						    &(relTableEntry_ii[indexIdx].kdesc.btree),
						    &kval, (ObjectID*)tid, objLockupPtr); 
			    if (e < 0) ERR(handle, e);
			    break;

			  case SM_INDEXTYPE_MLGF:
			    /* Element Type is SM_MBR. */
			    memcpy(&mbr, &buf[current], elementLength);
			    for (i = 0; i < MBR_NUM_PARTS; i++) mlgfKval[i] = mbr.values[i];
			    e = SM_MLGF_DeleteIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
							 &(relTableEntry_ii[indexIdx].kdesc.mlgf),
							 mlgfKval, (ObjectID*)tid, fileLockupPtr); 
			    if (e < 0) ERR(handle, e);
			    break;

			  default:
			    ERR(handle, eINTERNAL);
			}
		    }

		    /* Delete the current element from the buffer. */
		    if (updateStart == -1) updateStart = current;

		    memmove(&buf[current], &buf[current+elementLength],
                            size-(current+elementLength));
		    size -= elementLength;
		    nBytesDeleted += elementLength;
		}
	    } /* buffer scan loop */

	    /* Update the disk. */
	    if (nBytesDeleted == elementLength) {
		/* Only one element is deleted. */
		e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                      (ObjectID*)tid, start + updateStart,
                                      elementLength, NULL, 0, NULL, NULL);
		if (e < 0) ERR(handle, e);
	    } else if (nBytesDeleted != 0) {
		/* At least two elements are deleted. */
		e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                      (ObjectID*)tid, start + updateStart,
                                      size - updateStart + nBytesDeleted,
                                      &buf[updateStart], size - updateStart,
                                      NULL, NULL);
		if (e < 0) ERR(handle, e);
	    }

	    /*
	    ** prepare the next loop
	    */
	    start += nBytesToRead - nBytesDeleted; /* adjust next read point */
	    length -= nBytesToRead; /* adjust the remained bytes in this column */

	    /* Now 'size' represents the unused bytes in the buf. */
	    size -= current;

	    /* move the unused bytes */
	    memmove(&buf[0], &buf[current], size);

	    totalDeletedBytes += nBytesDeleted;
	} /* end of while */

    } /* end of if */


    /* Update the tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
	tupHdr.varColOffset[i] -= totalDeletedBytes;

    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr, tupHdrSize,
                          NULL, NULL);
    if (e < 0) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_Set_DeleteElements() */



/*
 * Function: Four LRDS_Set_IsMember(Four, Boolean, TupleID*, Two, void*)
 *
 * Description:
 *  Check if the given element is a member of the given set.
 *
 * Returns:
 *  Membership if the return value is greater than or equal to 0
 *     TRUE if it is a member
 *     FALSE otherwise
 *  error code if the return value is less than 0
 */
Four LRDS_Set_IsMember(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    void                *element)               /* IN elements to search */
{
    Two                 s;                      /* for setting of key length */
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                indexIdx;               /* index on array of index informations */
    Four                tupHdrSize;             /* size of tuple header */
    Four                start;                  /* starting offset of fetch */
    Four                length;                 /* length of the set column */
    Four                totalDeletedBytes;      /* amount of bytes deleted from the column */
    TupleHdr            tupHdr;                 /* a tuple header */
    KeyValue            kval;                   /* key value of an index entry */
    unsigned char       *nullVector;            /* bit array of null flags */
    Four                nBytesToRead;           /* amount of bytes to read from disk */
    Four                size;                   /* amount of data in the buffer */
    Four                updateStart;            /* starting offset of update within a buffer */
    Four                nBytesDeleted;          /* amount of deleted bytes within a buffer */
    char                buf[PAGESIZE];          /* buffer for holding the elements */
    Four                current;                /* points to the current element */
    char                *ptr;                   /* points to an element among the given elements */
    Set_ElementLength   elementLength;          /* length of an element */
    Set_ElementLength   elementLength2;         /* length of an element among the given element */
    LRDS_MBR            mbr;                    /* mbr */
    MLGF_HashValue      mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_IsMember(handle)"));


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

    if (cdesc->complexType != SM_COMPLEXTYPE_SET) ERR(handle, eBADPARAMETER);

    /* Is the element valid? */
    if (element == NULL) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eSET_NOTEXIST_LRDS);


    /* Get the starting offset and length of the column. */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);


    /*
    ** Search the given element.
    */
    /* initialize the the number of bytes deleted */
    totalDeletedBytes = 0;

    /* elementLength is the [maximum] length of an element */
    elementLength = cdesc->length;;

    if (cdesc->type == SM_VARSTRING) { /* variable-length element */

	/* extract the element length from the search-for element */
	memcpy(&elementLength2, element, sizeof(Set_ElementLength));
	ptr = (char*)element + sizeof(Set_ElementLength);

	/* initialize 'size', which is amount of bytes in the buf */
	size = 0;

	while (length > 0) {

	    /* Get the length of bytes to read per each call. */
	    nBytesToRead = MIN(length, PAGESIZE-size);

	    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                 (ObjectID*)tid, start, nBytesToRead, &buf[size],
                                 NULL, NULL);
	    if (e < 0) ERR(handle, e);
	    size += nBytesToRead;

	    /* for each element in the buffer */
	    for (current = 0; ; ) {

		/* extract the element length of the current element */
		memcpy(&elementLength, &buf[current], sizeof(Set_ElementLength));
		if (size < current + sizeof(Set_ElementLength) + elementLength) break;
		current += sizeof(Set_ElementLength);

		/* return TRUE if found */
		if (elementLength == elementLength2 &&
		    memcmp(&buf[current], ptr, elementLength) == 0) {
		    return(TRUE);
		}

		current += elementLength;
	    } /* buffer scan loop */

	    /*
	    ** prepare the next loop
	    */
	    start += nBytesToRead; /* adjust next read point */
	    length -= nBytesToRead; /* adjust the remained bytes in this column */

	    /* Now 'size' represents the unused bytes in the buf. */
	    size -= current;

	    /* move the unused bytes */
	    memmove(&buf[0], &buf[current], size);

	} /* end of while */

    } else {			/* fixed-length element */

	/* initialize 'size', which is amount of bytes in the buf */
	size = 0;
	ptr = (char*)element;

	while (length > 0) {

	    /* Get the length of bytes to read per each call. */
	    nBytesToRead = MIN(length, PAGESIZE-size);

	    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                 (ObjectID*)tid, start, nBytesToRead, &buf[size],
                                 NULL, NULL);
	    if (e < 0) ERR(handle, e);
	    size += nBytesToRead;


	    /* for each element in the buffer */
	    for (current = 0; current + elementLength <= size; ) {

		/* return TRUE if found */
		if (memcmp(&buf[current], ptr, elementLength) == 0) {
		    return(TRUE);
		}

		current += elementLength;
	    } /* buffer scan loop */

	    /*
	    ** prepare the next loop
	    */
	    start += nBytesToRead; /* adjust next read point */
	    length -= nBytesToRead; /* adjust the remained bytes in this column */

	    /* Now 'size' represents the unused bytes in the buf. */
	    size -= current;

	    /* move the unused bytes */
	    memmove(&buf[0], &buf[current], size);
	} /* end of while */

    } /* end of if */


    return(FALSE);

} /* LRDS_Set_IsMember() */



/*
 * Function: Four LRDS_Set_Scan_Open(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Open a scan on the given set.
 *
 * Returns:
 *  1) scan id if the return value is greater than or equal to 0
 *  2) error code if the return value is less than 0
 */
Four LRDS_Set_Scan_Open(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the set */
    Two                 colNo)                  /* IN column on which the set is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                tupHdrSize;             /* size of tuple header */
    Four                setScanId;              /* scan id of newly opened scan */
    TupleHdr            tupHdr;                 /* a tuple header */
    unsigned char       *nullVector;            /* bit array of null flags */
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_Scan_Open(handle)"));

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
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo]; 

    if (cdesc->complexType != SM_COMPLEXTYPE_SET) ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
	ERR(handle, eSET_NOTEXIST_LRDS);

    /* Is the scan already opened? Then return an error. */
    /* Currently we support only one scan per each set. */
    /* We find also an empty slot for the new scan. */
    setScanId = NIL;		/* initialize the setScanId */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsSetScanTable.nEntries; i++) {
	if (LRDS_SETSCANTABLE(handle)[i].ornOrRelScanId == NIL) { /* not used */
	    setScanId = i;
	    continue;
	}

	if (EQUAL_TUPLEID(LRDS_SETSCANTABLE(handle)[i].tid, *tid) &&
	    LRDS_SETSCANTABLE(handle)[i].colNo == colNo) { /* found */
	    return(eSET_OPENSCAN_LRDS);
	}
    }

    /* Open a new scan. */
    if (setScanId == NIL) { /* There is no empty entry. */

	e = Util_doublesizeVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsSetScanTable, sizeof(lrds_SetScanTableEntry));
	if (e < 0) ERR(handle, e);

	setScanId = i;		/* use the first empty slot */

	/* Initialize the newly allocated entries. */
	for (; i < LRDS_PER_THREAD_DS(handle).lrdsScanTable.nEntries; i++)
	    LRDS_SETSCANTABLE(handle)[i].ornOrRelScanId = NIL;
    }

    LRDS_SETSCANTABLE(handle)[setScanId].ornOrRelScanId = ornOrScanId;
    LRDS_SETSCANTABLE(handle)[setScanId].useRelScanFlag = useScanFlag;
    LRDS_SETSCANTABLE(handle)[setScanId].tid = *tid;
    LRDS_SETSCANTABLE(handle)[setScanId].colNo = colNo;
    LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead = 0;
    LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetPrevRead = 0;

    return(setScanId);

} /* LRDS_Set_Scan_Open() */



/*
 * Function: Four LRDS_Set_Scan_Close(Four)
 *
 * Description:
 *  Close the given scan.
 *
 * Returns:
 *  error code
 */
Four LRDS_Set_Scan_Close(
    Four handle,
    Four setScanId)		/* IN scan id for a scan on a set */
{
    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_Scan_Close(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_SETSCANID(handle, setScanId)) ERR(handle, eBADPARAMETER);

    /* Close the given set scan. */
    LRDS_SETSCANTABLE(handle)[setScanId].ornOrRelScanId = NIL; /* not used */

    return(eNOERROR);

} /* LRDS_Set_Scan_Close() */



/*
 * Function: Four LRDS_Set_Scan_NextElements(Four, Four, void*)
 *
 * Description:
 *  Fetch some elements from the given scan.
 *
 * Returns:
 *  1) # of elements actually read if the return value is greater than or equal to 0
 *  2) error code if the return value is less than 0
 *
 * Notice:
 *  We assume that 'elements' has enough space for returning the elements.
 *  When the element type is SM_VARSTRING, the 'elements' should have
 *  (nElements * maximum_length_of_element) bytes.
 */
Four LRDS_Set_Scan_NextElements(
    Four handle,
    Four setScanId,		/* IN scan id for a scan on a set */
    Four nElements,		/* IN # of elements to read */
    void *elements)		/* OUT buffer to return the read elements */
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
    TupleHdr tupHdr;		/* a tuple header */
    Set_ElementLength elementLen; /* length of an element */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_Scan_NextElements(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_SETSCANID(handle, setScanId)) ERR(handle, eBADPARAMETER);

    if (nElements < 0 || (nElements > 0 && elements == NULL)) ERR(handle, eBADPARAMETER);


    ornOrScanId = LRDS_SETSCANTABLE(handle)[setScanId].ornOrRelScanId;
    useScanFlag = LRDS_SETSCANTABLE(handle)[setScanId].useRelScanFlag;

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

    /* 'cdesc' points to the column description. */
    cdesc = &relTableEntry_cdesc[LRDS_SETSCANTABLE(handle)[setScanId].colNo]; 


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, ornOrScanId, useScanFlag,
                         (ObjectID*)&(LRDS_SETSCANTABLE(handle)[setScanId].tid),
                         0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /*
    ** Fetch the elements.
    */
    /* Get the starting offset of the column. */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);

    /* Get the number of bytes remained in the column. */
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr) - LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead;

    /* Get the starting offset of the first element to read. */
    start += LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead;

    /* if 'length' is 0 then we have read all elements. */
    /* So we read 0 element now. */
    if (length == 0) return(0);

    /* Get the length of bytes to read. */
    if (cdesc->type == SM_VARSTRING) {
	/* variable-length elements */

	if (nElements*cdesc->length < length) length = nElements*cdesc->length;

	/* There are two approaches to read the elements. */
	/* 1) Read the element one by one. This requests sevral calls. */
	/* 2) Read the maximum bytes. This may be read useless bytes. */
	/* Here method 1) was adopted. */
	e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                             (ObjectID*)&(LRDS_SETSCANTABLE(handle)[setScanId].tid),
                             start, length, elements, NULL, NULL);
	if (e < 0) ERR(handle, e);

	/* Get the number of elements actually read. */
	if (length == nElements*cdesc->length) {
	    nElementsRead = nElements;
	} else {
	    char *ptr = elements;

	    for (nElementsRead = 0;
		 nElementsRead < nElements && ((ptr-(char*)elements) < length);
		 nElements++) {
		memcpy(&elementLen, ptr, sizeof(Set_ElementLength));
		ptr += sizeof(Set_ElementLength) + elementLen;
	    }
	}
    } else {
	/* fixed-length elements */

	if (nElements*cdesc->length < length) length = nElements*cdesc->length;
	nElementsRead = length / cdesc->length;

	e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                             (ObjectID*)&(LRDS_SETSCANTABLE(handle)[setScanId].tid),
                             start, length, elements, NULL, NULL);
	if (e < 0) ERR(handle, e);
    }

    LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetPrevRead = LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead;
    LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead += length;

    return(nElementsRead);

} /* LRDS_Set_Scan_NextElements() */



/*
 * Function: LRDS_Set_Scan_InsertElements(Four, Four, void*)
 *
 * Description:
 *  Insert some elements into the given set.
 *  The set is specified by the scan id opened on it.
 *
 * Returns:
 *  error code
 */
Four LRDS_Set_Scan_InsertElements(
    Four handle,
    Four setScanId,		/* IN set where to insert the elements */
    Four nElements,		/* IN # of elements to insert */
    void *elements)		/* IN elements to insert */
{
    Four e;			/* error code */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_Scan_InsertElements(handle)"));


    /*
    ** check parameters
    */
    if (!LRDS_VALID_SETSCANID(handle, setScanId)) ERR(handle, eBADPARAMETER);

    if (nElements < 0 || (nElements > 0 && elements == NULL)) ERR(handle, eBADPARAMETER);


    /* Insert the elements using LRDS_Set_InsertElements(). */
    e = LRDS_Set_InsertElements(handle, LRDS_SETSCANTABLE(handle)[setScanId].ornOrRelScanId,
                                LRDS_SETSCANTABLE(handle)[setScanId].useRelScanFlag,
				&LRDS_SETSCANTABLE(handle)[setScanId].tid,
				LRDS_SETSCANTABLE(handle)[setScanId].colNo,
				nElements, elements);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_Set_Scan_InsertElements() */



/*
 * Function: Four LRDS_Set_Scan_DeleteElements(Four)
 *
 * Description:
 *  Delete from the given set some elements which are read previously via this scan.
 *  The set is specifed by the scan id opend on it.
 *
 * Returns:
 *  error code
 */
Four LRDS_Set_Scan_DeleteElements(
    Four handle,
    Four                setScanId)              /* IN set from which to delete the element */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                ornOrScanId;
    Boolean             useScanFlag;
    Four                tupHdrSize;             /* size of tuple header */
    Four                start;                  /* starting offset of the set column */
    Four                length;                 /* amount of bytes in the set column */
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                i, j;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc; 
    LockReply           lockReply;              /* lock reply */
    LockMode            oldMode;                /* lock mode */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_Scan_DeleteElements(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_SETSCANID(handle, setScanId)) ERR(handle, eBADPARAMETER);

    if (LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetPrevRead ==
	LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead)
	return(eNOERROR);

    ornOrScanId = LRDS_SETSCANTABLE(handle)[setScanId].ornOrRelScanId;
    useScanFlag = LRDS_SETSCANTABLE(handle)[setScanId].useRelScanFlag;

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
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* 'cdesc' points to the column description. */
    cdesc = &relTableEntry_cdesc[LRDS_SETSCANTABLE(handle)[setScanId].colNo];


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
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

    /* Fetch the tuple header. */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)&(LRDS_SETSCANTABLE(handle)[setScanId].tid),
                         0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /*
    ** Delete the elements from the set.
    */

    /*
    ** Automatic Index Support:
    ** Remove elements from the index if an index is on the set.
    */
    /* lock on the data file that the automatic index is built on to support the automatic index */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag && !relTableEntry->isCatalog) {

        /* check lock parameter */
        if (fileLockup.duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        if (fileLockup.mode != L_X && fileLockup.mode != L_IX && fileLockup.mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);

        /* lock on the data file */
        e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid, fileLockup.mode, fileLockup.duration,
                           L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr) + LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetPrevRead;
    length = LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead - LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetPrevRead;

    /* Check if an index is on the set. */
    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {
	if (relTableEntry_ii[i].colNo[0] == LRDS_SETSCANTABLE(handle)[setScanId].colNo) break; 
    }

    if (i != relTableEntry->ri.nIndexes) { /* index is on the set. */
	e = lrds_Set_RemoveFromIndex(handle, ornOrScanId, useScanFlag, i, &LRDS_SETSCANTABLE(handle)[setScanId].tid,
				     LRDS_SETSCANTABLE(handle)[setScanId].colNo,
				     start, length, fileLockupPtr, objLockupPtr);
	if (e < 0) ERR(handle, e);
    }


    /*
    ** Delete the elements from the data file.
    */

    /* Delete all data in the given column. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          &LRDS_SETSCANTABLE(handle)[setScanId].tid,
                          start, length, NULL, 0, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /* Adjust the ending offset of the variable-length columns. */
    for (i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
	tupHdr.varColOffset[i] -= length;

    /* Update the tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)&(LRDS_SETSCANTABLE(handle)[setScanId].tid),
                          0, tupHdrSize, (char*)&tupHdr, tupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /* Adjust the cursor of the set scan. */
    LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetToRead = LRDS_SETSCANTABLE(handle)[setScanId].byteOffsetPrevRead;

    return(eNOERROR);

} /* LRDS_Set_Scan_DeleteElements() */



/*
 * Function: Four lrds_Set_RemoveFromIndex(Four, Boolean, Four, TupleID*, Two, Four,
 *                                         Four, LockParameter*)
 *
 * Description:
 *  Deletes from the given index all the elements in the given set.
 *
 * Returns:
 *  error code
 *    eINTERNAL : internal error
 *    some errors caused by function calls
 */
Four lrds_Set_RemoveFromIndex(
    Four handle,
    Four                        ornOrScanId,            /* IN open relation no or scan id*/
    Boolean                     useScanFlag,            /* IN TRUE if above parameter is scan id */
    Four                        indexIdx,               /* IN index on indexInfo table */
    TupleID                     *tid,                   /* IN tuple containing the set */
    Two                         colNo,                  /* IN column on which set is defined */
    Four                        start,                  /* IN starting offset of the column */
    Four                        length,                 /* IN length of the column */
    LockParameter               *fileLockupPtr,         /* lockup for SM_DeleteIndexEntry */
    LockParameter               *objLockupPtr)          /* lockup for SM_DeleteIndexEntry */
{
    Two                         s;                      /* for setting of key length */
    Four                        e;                      /* error code */
    Four                        orn;
    Four                        smScanId;
    Four                        size;                   /* amount of bytes to read per each call */
    char                        buf[PAGESIZE];          /* holds the data in a set */
    KeyValue                    kval;                   /* key value of an index entry */
    char                        *ptr;                   /* points to an element */
    Set_ElementLength           elementLength;          /* length of an element */
    LRDS_MBR                    mbr;                    /* mbr */
    MLGF_HashValue              mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    Four                        i;
    lrds_RelTableEntry          *relTableEntry;         /* pointer to an entry of relation table */
    IndexInfo                   *relTableEntry_ii;      
    ColDesc                     *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("lrds_Set_RemoveFromIndex(handle)"));


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
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* Copy the elemnet length to the local variable. */
    elementLength = relTableEntry_cdesc[colNo].length; 

    switch (relTableEntry_ii[indexIdx].indexType) { 
      case SM_INDEXTYPE_BTREE:
	if (relTableEntry_cdesc[colNo].type == SM_VARSTRING) {	/* variable-length elements */ 

	    size = 0;		/* initialize 'size', which is amount of bytes in the buf */
	    while (length > 0 || size > 0) {

		if (size >= sizeof(Set_ElementLength)) {
		    /* Get the length of an element. */
		    memcpy(&elementLength, ptr, sizeof(Set_ElementLength));
		    ptr += sizeof(Set_ElementLength);
		    size -= sizeof(Set_ElementLength);
		} else {
		    Four partialLength = size;

		    if (length == 0) {	/* end of set */
			if (partialLength != 0) ERR(handle, eINTERNAL);
			break;
		    }

		    memcpy(&elementLength, ptr, partialLength); /* move forward the remained bytes */

		    /* Get the length of bytes to read per each call. */
		    size = MIN(length, PAGESIZE);

		    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                         (ObjectID*)tid, start, size, &buf[0], fileLockupPtr, objLockupPtr);
		    if (e < 0) ERR(handle, e);

		    start += size;	/* adjust next read point */
		    length -= size;	/* adjust the remained bytes in this column */

		    memcpy((char*)&elementLength + partialLength, &buf[0],
                           sizeof(Set_ElementLength)-partialLength);
		    ptr = &buf[sizeof(Set_ElementLength)-partialLength];
		    size -= sizeof(Set_ElementLength)-partialLength;
		}

		/*
		** Construct Key Value.
		*/
		/* key field length is represented by Two byte integer. */
		kval.len = sizeof(Two) + elementLength;

		s = elementLength;
		memcpy(&(kval.val[0]), &s, sizeof(Two));

		if (size >= elementLength) {
		    memcpy(&(kval.val[sizeof(Two)]), ptr, elementLength);
		    ptr += elementLength;
		    size -= elementLength;
		} else {
		    Four partialLength = size; /* # of bytes in the previous buffer */

		    if (length == 0) ERR(handle, eINTERNAL);

		    memcpy(&(kval.val[sizeof(Two)]), ptr, partialLength);

		    /* Get the length of bytes to read per each call. */
		    size = MIN(length, PAGESIZE);

		    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                         (ObjectID*)tid, start, size, &buf[0], fileLockupPtr, objLockupPtr);
		    if (e < 0) ERR(handle, e);

		    start += size;	/* adjust next read point */
		    length -= size;	/* adjust the remained bytes in the column */

		    memcpy(&(kval.val[sizeof(Two)+partialLength]), ptr, elementLength-partialLength);
		    ptr = &buf[elementLength-partialLength];
		    size -= elementLength - partialLength;
		}

		e = SM_DeleteIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
					&(relTableEntry_ii[indexIdx].kdesc.btree),
					&kval, (ObjectID*)tid, objLockupPtr); 
		if (e < 0) ERR(handle, e);

	    } /* end of while */

	} else {			/* fixed-length elements */

	    kval.len = elementLength;
	    size = 0;		/* initialize 'size', which is amount of bytes in the buf */
	    while (length > 0 || size > 0) {

		if (size < elementLength) {
		    Four partialLength = size;

		    /* copy the partial value. */
		    memcpy(&kval.val[0], ptr, partialLength);

		    /* Get the length of bytes to read per each call. */
		    size = MIN(length, PAGESIZE);

		    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                         (ObjectID*)tid, start, size, &buf[0],
                                         fileLockupPtr, objLockupPtr);
		    if (e < 0) ERR(handle, e);

		    start += size;	/* adjust next read point */
		    length -= size;	/* adjust the remained bytes in this column */

		    memcpy(&kval.val[partialLength], &buf[0], elementLength-partialLength);
		    ptr = &buf[elementLength-partialLength];
		    size -= elementLength-partialLength;
		} else {
		    memcpy(&kval.val[0], ptr, elementLength);
		    ptr += elementLength;
		    size -= elementLength;
		}

		/* Delete elements from the index. */
		e = SM_DeleteIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
					&(relTableEntry_ii[indexIdx].kdesc.btree),
					&kval, (ObjectID*)tid, objLockupPtr); 
		if (e < 0) ERR(handle, e);

	    } /* end of while */
	}
	break;

      case SM_INDEXTYPE_MLGF:
	/* The element type is SM_MBR. */
	size = 0;		/* initialize 'size', which is amount of bytes in the buf */
	while (length > 0 || size > 0) {

	    if (size < elementLength) {
		Four partialLength = size;

		/* copy the partial value. */
		memcpy(&mbr, ptr, partialLength);

		/* Get the length of bytes to read per each call. */
		size = MIN(length, PAGESIZE);

		e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                     (ObjectID*)tid, start, size, &buf[0],
                                     fileLockupPtr, objLockupPtr);
		if (e < 0) ERR(handle, e);

		start += size;	/* adjust next read point */
		length -= size;	/* adjust the remained bytes in this column */

		memcpy((char*)&mbr + partialLength, &buf[0], elementLength-partialLength);
		ptr = &buf[elementLength-partialLength];
		size -= elementLength-partialLength;
	    } else {
		memcpy(&mbr, ptr, elementLength);
		ptr += elementLength;
		size -= elementLength;
	    }

	    for (i = 0; i < MBR_NUM_PARTS; i++) mlgfKval[i] = mbr.values[i];
	    /* Delete elements from the index. */
	    e = SM_MLGF_DeleteIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
					 &(relTableEntry_ii[indexIdx].kdesc.mlgf),
					 mlgfKval, (ObjectID*)tid, objLockupPtr);
	    if (e < 0) ERR(handle, e);

	} /* end of while */
	break;

      default:
	ERR(handle, eINTERNAL);
    }

    return(eNOERROR);

} /* lrds_Set_RemoveFromIndex() */



/*
 * Function: Four lrds_Set_InsertIntoIndex(Four, Boolean, IndexInfo*, TupleID*, LockParameter*, LockParameter*)
 *
 * Description:
 *  Inserts into the given index all the elements in the given set.
 *
 * Returns:
 *  error code
 *    eINTERNAL : internal error
 *    some errors caused by function calls
 */
Four lrds_Set_InsertIntoIndex(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    IndexInfo           *indexInfo,             /* IN index information */
    TupleID             *tid,                   /* IN tuple containing the set */
    LockParameter       *fileLockupPtr,         /* lockup for SM_DeleteIndexEntry */
    LockParameter       *objLockupPtr)          /* lockup for SM_DeleteIndexEntry */
{
    Two                 s;                      /* for setting of key length */
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                size;                   /* amount of bytes to read per each call */
    Four                start;                  /* starting offset of the set column */
    Four                length;                 /* length of the set column */
    Two                 colNo;                  /* column on which the given index is defined */
    Four                tupHdrSize;             /* size of tuple header */
    char                buf[PAGESIZE];          /* holds the data in a set */
    TupleHdr            tupHdr;                 /* a tuple header */
    KeyValue            kval;                   /* key value of an index entry */
    char                *ptr;                   /* points to an element */
    Set_ElementLength   elementLength;          /* length of an element */
    LRDS_MBR            mbr;                    /* mbr */
    MLGF_HashValue      mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    IndexInfo *relTableEntry_ii;                
    ColDesc *relTableEntry_cdesc;              


    TR_PRINT(TR_LRDS, TR1, ("lrds_Set_InsertIntoIndex(handle)"));


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
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* 'colNo' points to the column on which the given index is defined. */
    colNo = indexInfo->colNo[0];

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo]; 


    /* Fetch the tuple header. */
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         fileLockupPtr, objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* Get the starting offset and length of the column. */
    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

    /* Copy the elemnet length to the local variable. */
    elementLength = cdesc->length;

    switch (indexInfo->indexType) {
      case SM_INDEXTYPE_BTREE:
	if (cdesc->type == SM_VARSTRING) {	/* variable-length elements */

	    size = 0;		/* initialize 'size', which is amount of bytes in the buf */
	    while (length > 0 || size > 0) {

		if (size >= sizeof(Set_ElementLength)) {
		    /* Get the length of an element. */
		    memcpy(&elementLength, ptr, sizeof(Set_ElementLength));
		    ptr += sizeof(Set_ElementLength);
		    size -= sizeof(Set_ElementLength);
		} else {
		    Four partialLength = size;

		    if (length == 0) {	/* end of set */
			if (partialLength != 0) ERR(handle, eINTERNAL);
			break;
		    }

		    memcpy(&elementLength, ptr, partialLength); /* move forward the remained bytes */

		    /* Get the length of bytes to read per each call. */
		    size = MIN(length, PAGESIZE);

		    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                         (ObjectID*)tid, start, size, &buf[0],
                                         fileLockupPtr, objLockupPtr);
		    if (e < 0) ERR(handle, e);

		    start += size;	/* adjust next read point */
		    length -= size;	/* adjust the remained bytes in this column */

		    memcpy((char*)&elementLength + partialLength, &buf[0],
                           sizeof(Set_ElementLength)-partialLength);
		    ptr = &buf[sizeof(Set_ElementLength)-partialLength];
		    size -= sizeof(Set_ElementLength)-partialLength;
		}

		/*
		** Construct Key Value.
		*/
		/* key field length is represented by Two byte integer. */
		kval.len = sizeof(Two) + elementLength;

		s = elementLength;
		memcpy(&(kval.val[0]), &s, sizeof(Two));

		if (size >= elementLength) {
		    memcpy(&(kval.val[sizeof(Two)]), ptr, elementLength);
		    ptr += elementLength;
		    size -= elementLength;
		} else {
		    Four partialLength = size; /* # of bytes in the previous buffer */

		    if (length == 0) ERR(handle, eINTERNAL);

		    memcpy(&(kval.val[sizeof(Two)]), ptr, partialLength);

		    /* Get the length of bytes to read per each call. */
		    size = MIN(length, PAGESIZE);

		    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                         (ObjectID*)tid, start, size, &buf[0],
                                         fileLockupPtr, objLockupPtr);
		    if (e < 0) ERR(handle, e);

		    start += size;	/* adjust next read point */
		    length -= size;	/* adjust the remained bytes in the column */

		    memcpy(&(kval.val[sizeof(Two)+partialLength]), ptr, elementLength-partialLength);
		    ptr = &buf[elementLength-partialLength];
		    size -= elementLength - partialLength;
		}

		e = SM_InsertIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.btree,
					&kval, (ObjectID*)tid, objLockupPtr);
		if (e < 0) ERR(handle, e);

	    } /* end of while */

	} else {			/* fixed-length elements */

	    kval.len = elementLength;
	    size = 0;		/* initialize 'size', which is amount of bytes in the buf */
	    while (length > 0 || size > 0) {

		if (size < elementLength) {
		    Four partialLength = size;

		    /* copy the partial value. */
		    memcpy(&kval.val[0], ptr, partialLength);

		    /* Get the length of bytes to read per each call. */
		    size = MIN(length, PAGESIZE);

		    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                         (ObjectID*)tid, start, size, &buf[0],
                                         fileLockupPtr, objLockupPtr);
		    if (e < 0) ERR(handle, e);

		    start += size;	/* adjust next read point */
		    length -= size;	/* adjust the remained bytes in this column */

		    memcpy(&kval.val[partialLength], &buf[0], elementLength-partialLength);
		    ptr = &buf[elementLength-partialLength];
		    size -= elementLength-partialLength;
		} else {
		    memcpy(&kval.val[0], ptr, elementLength);
		    ptr += elementLength;
		    size -= elementLength;
		}

		/* Insert elements into the index. */
		e = SM_InsertIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.btree,
					&kval, (ObjectID*)tid, objLockupPtr);
		if (e < 0) ERR(handle, e);

	    } /* end of while */
	}
	break;

      case SM_INDEXTYPE_MLGF:
	/* The element type is SM_MBR. */
	size = 0;		/* initialize 'size', which is amount of bytes in the buf */
	while (length > 0 || size > 0) {

	    if (size < elementLength) {
		Four partialLength = size;

		/* copy the partial value. */
		memcpy(&mbr, ptr, partialLength);

		/* Get the length of bytes to read per each call. */
		size = MIN(length, PAGESIZE);

		e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                     (ObjectID*)tid, start, size, &buf[0],
                                     fileLockupPtr, objLockupPtr);
		if (e < 0) ERR(handle, e);

		start += size;	/* adjust next read point */
		length -= size;	/* adjust the remained bytes in this column */

		memcpy((char*)&mbr + partialLength, &buf[0], elementLength-partialLength);
		ptr = &buf[elementLength-partialLength];
		size -= elementLength-partialLength;
	    } else {
		memcpy(&mbr, ptr, elementLength);
		ptr += elementLength;
		size -= elementLength;
	    }

	    for (i = 0; i < MBR_NUM_PARTS; i++) mlgfKval[i] = mbr.values[i];
	    /* Delete elements from the index. */
	    e = SM_MLGF_InsertIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.mlgf,
				    mlgfKval, (ObjectID*)tid, NULL, objLockupPtr);
	    if (e < 0) ERR(handle, e);

	} /* end of while */
	break;

      default:
	ERR(handle, eINTERNAL);
    }

    return(eNOERROR);

} /* lrds_Set_InsertIntoIndex() */

/*
 * Function: Four LRDS_Set_IsNull(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Check where the given colNo is null or not.
 *
 * Retuns:
 *  error code
 */
Four LRDS_Set_IsNull(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple containing the given column */
    Two                 colNo                   /* IN column on which the set is created */
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

 
    TR_PRINT(TR_LRDS, TR1, ("LRDS_Set_IsNull(handle)"));
 
 
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
 
    if (cdesc->complexType != SM_COMPLEXTYPE_SET) ERR(handle, eBADPARAMETER);
 
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
