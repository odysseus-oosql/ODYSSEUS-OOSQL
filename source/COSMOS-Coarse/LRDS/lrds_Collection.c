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
 * Module: lrds_Collection.c
 *
 * Description:
 *  Implements a base operation of CollectionSet, CollectionBag,
 *                                 CollectionList.
 *
 * Imports:
 *  Four lrds_Collection_Create(Four, Boolean, TupleID*, Two, Four)
 *  Four lrds_Collection_Destroy(Four, Boolean, TupleID*, Two)
 *
 *  Four lrds_Collection_GetN_Elements(Four, Boolean, TupleID*, TupleHdr*, Two, Four*)
 *  Four lrds_Collection_SetN_Elements(Four, Boolean, TupleID*, TupleHdr*, Two, Four)
 *
 *  Four lrds_Collection_FixedElementInsert(Four, Boolean, TupleID*, TupleHdr*, Two, Four,
 *                                          Four, void*)
 *  Four lrds_Collection_FixedElementDelete(Four, Boolean, TupleID*, TupleHdr*, Two, Four, Four)
 *  Four lrds_Collection_FixedElementFetch(Four, Boolean, TupleID*, TupleHdr*, Two, Four,
 *                                         Four, void*)
 *  Four lrds_Collection_FixedElementUpdate(Four, Boolean, TupleID*, TupleHdr*, Two, Four,
 *                                          Four, void*)
 *
 *  Four lrds_Collection_VarElementInsert(Four, Boolean, TupleID*, TupleHdr*, Two, Four, Four,
 *                                        Four*, void*)
 *  Four lrds_Collection_VarElementDelete(Four, Boolean, TupleID*, TupleHdr*, Two, Four, Four)
 *  Four lrds_Collection_VarElementFetchSizes(Four, Boolean, TupleID*, TupleHdr*, Two, Four,
 *                                            Four, Four*)
 *  Four lrds_Collection_VarElementFetch(Four, Boolean, TupleID*, TupleHdr*, Two, Four, Four,
 *                                       Four*)
 *  Four lrds_Collection_VarElementUpdate(Four, Boolean, TupleID*, TupleHdr*, Two, Four, Four,
 *                                        Four*, void*)
 *
 *  Four lrds_Collection_FixedElementRemoveFromIndex(Four, Boolean, IndexInfo*, TupleID*, Two, Four,
 *                                                   void*, LockParameter*)
 *  Four lrds_Collection_FixedElementInsertIntoIndex(Four, Boolean, IndexInfo*, TupleID*, Two, Four, 
 *                                                   void*, LockParameter*)
 *
 *  Four lrds_Collection_VarElementRemoveFromIndex(Four, Boolean, IndexInfo*, TupleID*, Two, Four, 
 *                                                 Four*, void*, LockParameter*)
 *  Four lrds_Collection_VarElementInsertIntoIndex(Four, Boolean, IndexInfo*, TupleID*, Two, Four, 
 *                                                 Four*, void*, LockParameter*)
 *
 *  Four lrds_Collection_Scan_Open(Four, Boolean, TupleID*, Two)
 *  Four lrds_Collection_Scan_Close(Four)
 *  Four lrds_Collection_Scan_NextElements(Four, Four, Four*, Four, void*)
 *  Four lrds_Collection_Scan_InsertElements(Four, Four, Four*, void*)
 *  Four lrds_Collection_Scan_DeleteElements(Four)
 *
 *
 * Exports:
 *
 * Notice:
 *  The input parameter relScanId is used to identify the relation which
 *  contains the given array. We don't use a scan identified by the relScanId.
 *  I suggest that the relScanId be replaced with the open relation number.
 *  When this change is made, the input parameter `tuple id' should not be
 *  NULL.
 */

#include <string.h>
#include <stdlib.h> 	 /* for malloc & free */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM_Internal.h" 
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*
 *  Collection Tuple Encoding
 *  FixedElement
 *
 *     +-----------+---------+----------+----------+---------+
 *     | nElements | keySize | element1 | element2 | ....... |
 *     +-----------+---------+----------+----------+---------+
 *
 *  VarElement
 *
 *     +-----------+---------+----------+----------+---------+----------+----------+---------+
 *     | nElements | keySize | offset 1 | offset 2 | ....... | element1 | element2 | ....... |
 *     +-----------+---------+----------+----------+---------+----------+----------+---------+
 *
 */

#define ENCODING_NELEMENTS_TYPE                         Four
#define ENCODING_KEYSIZE_TYPE                           Four
#define VARENCODING_OFFSET_TYPE                         Four
#define ENCODING_NELEMENTS_SIZE                         sizeof(Four)
#define ENCODING_KEYSIZE_SIZE                           sizeof(Four)
#define VARENCODING_OFFSET_SIZE                         sizeof(Four)
#define FIXEDENCODING_ITHELEMENT_OFFSET(cdesc, ith)     (ENCODING_NELEMENTS_SIZE + cdesc->length * (ith))
#define VARENCODING_ITHOFFSET_OFFSET(ith)               (ENCODING_NELEMENTS_SIZE + ENCODING_KEYSIZE_SIZE + VARENCODING_OFFSET_SIZE * (ith))
#define VARENCODING_FIRSTELEMENT_OFFSET(nElementsOfCol) (ENCODING_NELEMENTS_SIZE + ENCODING_KEYSIZE_SIZE + VARENCODING_OFFSET_SIZE * nElementsOfCol)

/*
 * Function: Four lrds_Collection_Create(Four, Boolean, TupleID*, Two, Four)
 *
 * Description:
 *  Create a collection on the given column of the given tuple of the
 *  given relation.
 *  Before creating a set the column has NULL value.
 *
 * Retuns:
 *  error code
 */
Four lrds_Collection_Create(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the given column */
    Two                 colNo,                  /* IN column on which the set is created */
    Four                 keySize)               /* IN keysize in varstring, string */
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
    ENCODING_NELEMENTS_TYPE nElementsOfCol;
    Four                i;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Create(handle)"));

    /*
    ** check parameters.
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (keySize > ((Four)MAXKEYWORDLEN)) ERR(handle, eBADPARAMETER); 

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

    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* Prepare lock parameter */

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
    if (!BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_EXIST_LRDS);

    /* If # of elements should be represented in a set, its code should be placed here. */

    /* Reset the corresponding bit of array of null flags. */
    BITRESET(nullVector, colNo);

    /* Insert nElementsOfCol and keySize */
    nElementsOfCol = 0;
    if(relTableEntry_cdesc[colNo].type != SM_VARSTRING)
    {
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                              (cdesc->varColNo == 0) ?tupHdr.firstVarColOffset:tupHdr.varColOffset[cdesc->varColNo-1],
                              0, (char*)&nElementsOfCol, ENCODING_NELEMENTS_SIZE, NULL, NULL);
        if(e < 0) ERR(handle, e);

        /* Update varColOffset in tupHdr */
        for(i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
            tupHdr.varColOffset[i] += ENCODING_NELEMENTS_SIZE;
    }
    else
    {
        char headerData[ENCODING_NELEMENTS_SIZE + ENCODING_KEYSIZE_SIZE];

        memcpy(&headerData[0]                      , &nElementsOfCol, ENCODING_NELEMENTS_SIZE);
        memcpy(&headerData[ENCODING_NELEMENTS_SIZE], &keySize, ENCODING_KEYSIZE_SIZE);

        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                              (cdesc->varColNo == 0) ?tupHdr.firstVarColOffset:tupHdr.varColOffset[cdesc->varColNo-1],
                              0, headerData, ENCODING_NELEMENTS_SIZE + ENCODING_KEYSIZE_SIZE, NULL, NULL);
        if(e < 0) ERR(handle, e);

        /* Update varColOffset in tupHdr */
        for(i = cdesc->varColNo; i < tupHdr.nVarCols; i++) 
            tupHdr.varColOffset[i] += (ENCODING_NELEMENTS_SIZE + ENCODING_KEYSIZE_SIZE);
    }

    /* Update the tuple header. */
    /* NOTICE: We can reduce the amount of data overwritten. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, oldTupHdrSize, (char*)&tupHdr,
                          newTupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* lrds_Collection_Create() */



/*
 * Function: Four lrds_Collection_Destroy(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Destroy the set on the given column of the given tuple of the given relation.
 *  The column will be set to NULL value after destroying it.
 *  NOTE : for support automatic indexing, all the elements in this tuple must be
 *         extracted index before calling this function.
 *
 * Returns:
 *  error code
 */
Four lrds_Collection_Destroy(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the given column */
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
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Destroy(handle)"));

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

    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* Prepare lock parameter */

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
	ERR(handle, eCOLLECTION_NOTEXIST_LRDS);


    /*
    ** Destroy a set.
    */
    /* Get the starting offset and length of the column. */
    start  = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
    length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

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

} /* lrds_Collection_Destroy() */


Four lrds_Collection_GetN_Elements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four*               nElements)              /* OUT nElements in the column */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ENCODING_NELEMENTS_TYPE temp_nElements;     /* temporary varables for reading nElements of column */
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_GetN_Elements(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* make lockup parameter */
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* fetch n elements */
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                         (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1],
                         ENCODING_NELEMENTS_SIZE, (char*)&temp_nElements,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if(e < 0) ERR(handle, e);

    *nElements = temp_nElements;    /* type conversion ENCODING_NELEMENTS_TYPE -> Four */

    return eNOERROR;
} /* lrds_Collection_nElements */

Four lrds_Collection_GetKeySize(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                 *keySize)              /* OUT key size of this column */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ENCODING_NELEMENTS_TYPE temp_keySize;       /* temporary varables for reading keysize of column */
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */


    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_GetKeySize(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* make lockup parameter */
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* fetch keySize */
    if(cdesc->type == SM_STRING || cdesc->type == SM_VARSTRING)
    {
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                             ((cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1]) + ENCODING_NELEMENTS_SIZE,
                             ENCODING_KEYSIZE_SIZE, (char*)&temp_keySize, 
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if(e < 0) ERR(handle, e);
    }
    else
        temp_keySize = ALL_VALUE;

    *keySize = temp_keySize;    /* type conversion ENCODING_KEYSIZE_TYPE -> Four */

    return eNOERROR;
} /* lrds_Collection_GetKeySize */

Four lrds_Collection_SetN_Elements(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements)              /* IN nElements in the column */
{
    Four                e;                      /* error code */
    Four                i;
    Four                orn;
    Four                smScanId;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    ENCODING_NELEMENTS_TYPE temp_nElements;     /* temporary varables for writing nElements of column */
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_GetN_Elements(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    /* 'cdesc' points to the column descriptor for the fast and simple acces. */
    cdesc = &relTableEntry_cdesc[colNo];

    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* update n elements */
    temp_nElements = nElements; /* type conversion Four -> ENCODING_NELEMENTS_TYPE */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1],
                          ENCODING_NELEMENTS_SIZE,
                          (char*)&temp_nElements, ENCODING_NELEMENTS_SIZE, NULL, NULL);
    if(e < 0) ERR(handle, e);

    return eNOERROR;
} /* lrds_Collection_nElements */

Four lrds_Collection_Assign(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                assignedOrnOrScanId,    /* IN assigned relation containing the set*/
    Boolean             assignedUseScanFlag,    /* IN TRUE if above parameter is scan id */
    TupleID*            assignedTid,            /* IN assigned tuple containing the set */
    Two                 assignedColNo)          /* IN assigned column on which the set is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                nElementsOfCol;         /* # of elements of the column */
    lrds_RelTableEntry* relTableEntry;          /* pointer to an entry of relation table */
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc*            cdesc;                  /* pointer to the current column descriptor */

    Four                assignedOrn;
    Four                assignedSmScanId;
    TupleHdr            assignedTupHdr;         /* a tuple header */
    Four                assignedTupHdrSize;     /* size of tuple header */
    Four                assignedNElementsOfCol; /* # of elements of the column */
    ColDesc*            assignedCdesc;          /* pointer to the current column descriptor */
    lrds_RelTableEntry* assignedRelTableEntry;  /* pointer to an entry of relation table */

    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */

    LockParameter       assignedFileLockup;     /* lockup for SM_Fetch Tuple */
    LockParameter       assignedObjLockup;      /* lockup for SM_Fetch Tuple */
    LockParameter       *assignedFileLockupPtr; /* pointer to the lockup value */
    LockParameter       *assignedObjLockupPtr;  /* pointer to the lockup value */

    Four                ith;
    Four                nReads;
    Four                elementsSize;
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                i;
    ColDesc             *relTableEntry_cdesc;   
    ColDesc             *assignedRelTableEntry_cdesc;
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Assign(handle)"));

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

    if (assignedUseScanFlag == TRUE && !LRDS_VALID_SCANID(handle, assignedOrnOrScanId)) ERR(handle, eBADPARAMETER);

    if (assignedUseScanFlag == FALSE && !LRDS_VALID_ORN(handle, assignedOrnOrScanId)) ERR(handle, eBADPARAMETER);

    if (assignedUseScanFlag) {
        assignedOrn = LRDS_SCANTABLE(handle)[assignedOrnOrScanId].orn;
        assignedSmScanId = LRDS_SCANTABLE(handle)[assignedOrnOrScanId].smScanId;
    } else {
        assignedOrn = assignedOrnOrScanId;
        assignedSmScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry         = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc                 = &relTableEntry_cdesc[colNo];
    assignedRelTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, assignedOrn);
    assignedRelTableEntry_cdesc = PHYSICAL_PTR(assignedRelTableEntry->cdesc); 
    assignedCdesc         = &assignedRelTableEntry_cdesc[assignedColNo];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

    if (assignedColNo >= assignedRelTableEntry->ri.nColumns)
        ERR(handle, eBADPARAMETER);
    if (assignedCdesc->complexType != cdesc->complexType)
        ERR(handle, eBADPARAMETER);

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;
    if (assignedUseScanFlag && assignedTid == NULL) assignedTid = &LRDS_SCANTABLE(handle)[assignedOrnOrScanId].tid;

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

    if (LRDS_USEROPENRELTABLE(handle)[assignedOrn].tmpRelationFlag) {
        assignedFileLockupPtr = assignedObjLockupPtr = NULL;

    } else {
        assignedFileLockup.mode = L_IX;
        assignedFileLockup.duration = L_COMMIT;
        assignedFileLockupPtr = &assignedFileLockup;

        assignedObjLockup.mode = L_X;
        assignedObjLockup.duration = L_COMMIT;
        assignedObjLockupPtr = &assignedObjLockup;
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    assignedTupHdrSize = TUPLE_HEADER_SIZE(assignedRelTableEntry->ri.nColumns, assignedRelTableEntry->ri.nVarColumns);
    e = LRDS_FETCHOBJECT(handle, &assignedRelTableEntry->ri.fid, assignedSmScanId, assignedUseScanFlag,
                         (ObjectID*)assignedTid, 0, assignedTupHdrSize, (char*)&assignedTupHdr,
                         (assignedRelTableEntry->isCatalog) ? NULL : assignedFileLockupPtr,
                         (assignedRelTableEntry->isCatalog) ? NULL : assignedObjLockupPtr);
    if (e < 0) ERR(handle, e);
    /* Get the real tuple header size. */
    assignedTupHdrSize = TUPLE_HEADER_SIZE(assignedTupHdr.nVarCols + assignedTupHdr.nFixedCols, assignedTupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(assignedTupHdr, assignedTupHdr.nVarCols);

    /* If the column has non NULL value, then return error. */
    if (assignedColNo >= assignedTupHdr.nFixedCols + assignedTupHdr.nVarCols || BITTEST(nullVector, colNo)) 
        ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* Get # of elements of column */
    e = lrds_Collection_GetN_Elements(handle, assignedOrnOrScanId, assignedUseScanFlag,
                                      assignedTid, &assignedTupHdr,
                                      assignedColNo, &assignedNElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if(assignedCdesc->type == SM_VARSTRING)
    {   /* variable size element */
        e = lrds_Collection_VarElementDelete(handle, assignedOrnOrScanId, assignedUseScanFlag,
                                             assignedTid, &assignedTupHdr,
                                             assignedColNo, 0, assignedNElementsOfCol);
        if(e < eNOERROR) ERR(handle, e);

        ith = 0;
        while(nElementsOfCol > 0)
        {
            /* read elementSizes */
            nReads = sizeof(elementsBuf) / sizeof(Four);
            if(nReads > nElementsOfCol)
                nReads = nElementsOfCol;

            e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag,
                                                     tid, &tupHdr,
                                                     colNo,ith, nReads,
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
            e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag,
                                                tid, &tupHdr,
                                                colNo, ith, nReads,
                                                elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* append elements */
            e = lrds_Collection_VarElementInsert(handle, assignedOrnOrScanId, assignedUseScanFlag,
                                                 assignedTid, &assignedTupHdr,
                                                 assignedColNo, ith, nReads,
                                                 (Four*)elementSizesBuf,
                                                 elementsBuf);

            if(e < eNOERROR) ERR(handle, e);

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }
    else
    {   /* fixed size element */
        e = lrds_Collection_FixedElementDelete(handle, assignedOrnOrScanId, assignedUseScanFlag,
                                               assignedTid, &assignedTupHdr,
                                               assignedColNo, 0, assignedNElementsOfCol);
        if(e < eNOERROR) ERR(handle, e);

        ith = 0;
        while(nElementsOfCol > 0)
        {
            nReads = sizeof(elementsBuf) / cdesc->length;
            if(nReads > nElementsOfCol)
                nReads = nElementsOfCol;

            /* read elements */
            e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                                  colNo, ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* append elements */
            e = lrds_Collection_FixedElementInsert(handle, assignedOrnOrScanId, assignedUseScanFlag,
                                                   assignedTid, &assignedTupHdr,
                                                   assignedColNo, ith, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }

    return eNOERROR;
}

Four lrds_Collection_FixedElementInsert(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                insertStartOffset;      /* start offset of inserting in the column */
    Four                nInsertedBytes;         /* amount of bytes inserted into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    Four                indexIdx;               /* index on array of index informations */
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   
    IndexInfo           *relTableEntry_ii;      
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementInsert(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* update n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    nElementsOfCol += nElements;

    e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* insert elements */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    insertStartOffset = colStartOffset + FIXEDENCODING_ITHELEMENT_OFFSET(cdesc, ith);
    nInsertedBytes    = cdesc->length * nElements;
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          insertStartOffset, 0, elements,
                          nInsertedBytes, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* update index - insert elements into index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;
    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        /* make lock parameter */
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

        e = lrds_Collection_FixedElementInsertIntoIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                        tid, colNo,
                                                        nElements, elements, objLockupPtr);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* update tuple header */
    for(i = cdesc->varColNo; i < tupHdr->nVarCols; i++) 
        tupHdr->varColOffset[i] += nInsertedBytes;

    return eNOERROR;
} /* lrds_Collection_FixedElementInsert */

Four lrds_Collection_FixedElementDelete(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN delete from ith element */
    Four                nElements)              /* IN # of elements to delete */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                deleteStartOffset;      /* start offset of deleting in the column */
    Four                nDeletedBytes;          /* amount of bytes deleted into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    Four                indexIdx;               /* index on array of index informations */
    Four                ithToDelete;            /* ith element to delete */
    Four                nElementsToDelete;
    Four                nReads;
    char                elementsBuf[PAGESIZE];
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   
    IndexInfo           *relTableEntry_ii;     
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementDelete(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* 만약 이미 empty set이라면, 아무것도 지울것이 없으므로, 그냥 돌아간다. */
    /* if it's already empty set, there is nothing to delete. just return. */ 
    if(nElementsOfCol == 0)
        return eNOERROR;

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    /* make lockup parameter */
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

    /* update index - remove elements from index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;

    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        ithToDelete = ith;
        nElementsToDelete = nElements;
        while(nElementsToDelete > 0)
        {
            nReads = sizeof(elementsBuf) / cdesc->length;
            if(nReads > nElementsToDelete)
                nReads = nElementsToDelete;

            /* read elements */
            e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                  ithToDelete, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* remove elements from index */
            e = lrds_Collection_FixedElementRemoveFromIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                            tid, colNo,
                                                            nReads, elementsBuf, objLockupPtr );
            if(e < eNOERROR) ERR(handle, e);

            nElementsToDelete -= nReads;
            ithToDelete       += nReads;
        }
    }

    /* update n elements */
    nElementsOfCol -= nElements;

    e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* delete elements */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    deleteStartOffset = colStartOffset + FIXEDENCODING_ITHELEMENT_OFFSET(cdesc, ith);
    nDeletedBytes     = cdesc->length * nElements;
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          deleteStartOffset, nDeletedBytes, NULL,
                          0, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* update tuple header */
    for(i = cdesc->varColNo; i < tupHdr->nVarCols; i++) 
        tupHdr->varColOffset[i] -= nDeletedBytes;

    return nElements;
} /* lrds_Collection_FixedElementDelete */

Four lrds_Collection_FixedElementFetch(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                fetchStartOffset;       /* start offset of fetching in the column */
    Four                nFetchedBytes;          /* amount of bytes fetched into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementFetch(handle)"));

    if(nElements <= 0)
        return eNOERROR;

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    /* make lockup parameter */
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

    /* fetch elements */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    fetchStartOffset  = colStartOffset + FIXEDENCODING_ITHELEMENT_OFFSET(cdesc, ith);
    nFetchedBytes     = cdesc->length * nElements;
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                         fetchStartOffset, nFetchedBytes, elements,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if(e < 0) ERR(handle, e);

    return nElements;
}

Four lrds_Collection_FixedElementUpdate(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                updateStartOffset;      /* start offset of updating in the column */
    Four                nUpdatedBytes;          /* amount of bytes updated into the column */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    Four                indexIdx;               /* index on array of index informations */
    Four                nElementsOfCol;         /* # of elements in the column */
    Four                ithToDelete;
    Four                nElementsToDelete;
    Four                nReads;
    char                elementsBuf[PAGESIZE];
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    Four                i;
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;  
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementUpdate(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    /* make lockup parameter */
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

    /* update index - remove elements from index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;
    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        ithToDelete = ith;
        nElementsToDelete = nElements;
        while(nElementsToDelete > 0)
        {
            nReads = sizeof(elementsBuf) / cdesc->length;
            if(nReads > nElementsToDelete)
                nReads = nElementsToDelete;

            /* read elements */
            e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                  ithToDelete, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* remove elements from index */
            e = lrds_Collection_FixedElementRemoveFromIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                            tid, colNo,
                                                            nReads, elementsBuf, objLockupPtr);
            if(e < eNOERROR) ERR(handle, e);

            nElementsToDelete -= nReads;
            ithToDelete       += nReads;
        }
    }

    /* insert elements */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    updateStartOffset = colStartOffset + FIXEDENCODING_ITHELEMENT_OFFSET(cdesc, ith);
    nUpdatedBytes     = cdesc->length * nElements;
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          updateStartOffset, nUpdatedBytes, elements,
                          nUpdatedBytes, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* update index - insert elements into index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;
    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        e = lrds_Collection_FixedElementInsertIntoIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                        tid, colNo,
                                                        nElements, elements, objLockupPtr);
        if(e < eNOERROR) ERR(handle, e);
    }

    return nElements;
} /* lrds_Collection_FixedElementUpdate */

Four lrds_Collection_VarElementInsert(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes,           /* IN sizes of elements to insert */
    void*               elements)               /* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                insertStartOffset;      /* start offset of inserting in the column */
    Four                updateStartOffset;      /* start offset of updating in the column */
    Four                nInsertedBytes;         /* amount of bytes inserted into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    Four                prev_nElementsOfCol;    /* # of elements in the column before insert */
    Four                nUpdatedElements;       /* # of element sizes updated */
    Four                nReadElements;          /* # of element sizes read */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    char                buf[PAGESIZE];          /* buffer for holding the elementSizes */
    Four                offset;
    Four                elementSize;
    Four                inthOffset;
    Four                i;
    Four                indexIdx;               /* index on array of index informations */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    VARENCODING_OFFSET_TYPE ithOffset;          /* offset of ith element */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_VarElementInsert(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* make lockup parameter */
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* update n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    prev_nElementsOfCol = nElementsOfCol;
    nElementsOfCol += nElements;

    e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* calculate total inserted element size */
    for(i = 0, nInsertedBytes = 0; i < nElements; i++)
        nInsertedBytes += elementSizes[i];

    /* update element sizes from ith to end */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    updateStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    nUpdatedElements  = prev_nElementsOfCol - ith;

    /* initialize ithOffset */
    if(nUpdatedElements == 0) /* all the elements are appended at the end of collection */
    {
        ithOffset = tupHdr->varColOffset[cdesc->varColNo] -
            (colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(prev_nElementsOfCol));
    }
    else
        ithOffset = -1; /* ithOffset is initialized in while loop */

    while(nUpdatedElements > 0)
    {
        if(nUpdatedElements > (sizeof(buf) / VARENCODING_OFFSET_SIZE))
            nReadElements = sizeof(buf) / VARENCODING_OFFSET_SIZE;
        else
            nReadElements = nUpdatedElements;

        /* fetch element sizes */
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                             updateStartOffset, nReadElements * VARENCODING_OFFSET_SIZE, (char*)buf,
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if (e < 0) ERR(handle, e);

        /* save ith offset for later use */
        if(ithOffset == -1)
            ithOffset = ((Four*)buf)[0];

        /* update element offsets */
        for(i = 0; i < nReadElements; i++)
            ((Four*)buf)[i] += nInsertedBytes;

        /* update element offsets */
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                              updateStartOffset, nReadElements * VARENCODING_OFFSET_SIZE,
                              (char*)buf,
                              nReadElements * VARENCODING_OFFSET_SIZE,
                              NULL, NULL);
        if (e < 0) ERR(handle, e);

        /* proceed next element sizes */
        updateStartOffset += nReadElements * VARENCODING_OFFSET_SIZE;
        nUpdatedElements  -= nReadElements;
    }

    /* initialize inthOffset */
    inthOffset = ithOffset + nInsertedBytes;

    /* convert element size to offset */
    offset = ithOffset;
    for(i = 0; i < nElements; i++)
    {
        elementSize = elementSizes[i];
        elementSizes[i] = offset;
        offset += elementSize;
    }

    /* insert element sizes */
    insertStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          insertStartOffset, 0, elementSizes,
                          nElements * VARENCODING_OFFSET_SIZE, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* insert elements */
    insertStartOffset = colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(nElementsOfCol) + ithOffset;
    /* nInsertedBytes is calculated previously */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          insertStartOffset, 0, elements,
                          nInsertedBytes, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* restore element sizes given by user, for avoiding functional side effects */
    for(i = 0; i < (nElements - 1); i++)
        elementSizes[i] = elementSizes[i + 1] - elementSizes[i];
    elementSizes[i] = inthOffset - elementSizes[i];

    /* update index - insert elements into index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;
    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        e = lrds_Collection_VarElementInsertIntoIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                      tid, colNo,
                                                      nElements, elementSizes, elements, objLockupPtr);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* update tuple header */
    for(i = cdesc->varColNo; i < tupHdr->nVarCols; i++) 
        tupHdr->varColOffset[i] += (nInsertedBytes + nElements * VARENCODING_OFFSET_SIZE);

    return eNOERROR;
} /* lrds_Collection_VarElementInsert */

Four lrds_Collection_VarElementDelete(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements)              /* IN # of elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                deleteStartOffset;      /* start offset of deleting in the column */
    Four                updateStartOffset;      /* start offset of updating in the column */
    Four                nDeletedBytes;          /* amount of bytes deleted into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    Four                prev_nElementsOfCol;    /* # of elements in the column before delete */
    Four                nUpdatedElements;       /* # of element sizes updated */
    Four                nReadElements;          /* # of element sizes read */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    char                elementSizesBuf[PAGESIZE];
    char                elementsBuf[PAGESIZE];
    Four                elementsSize;
    Four                ithToDelete;
    Four                nElementsToDelete;
    Four                nReads;
    Four                i;
    Four                indexIdx;               /* index on array of index informations */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    VARENCODING_OFFSET_TYPE ithOffset;          /* offset of ith element */
    VARENCODING_OFFSET_TYPE inthOffset;         /* offset of (i+nElements)th deleted element */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_VarElementDelete(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* make lockup parameter */
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* 만약 이미 empty set이라면, 아무것도 지울것이 없으므로, 그냥 돌아간다. */
    /* if it's already empty set, there is nothing to delete. just return. */ 
    if(nElementsOfCol == 0)
        return eNOERROR;

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    /* update index - remove elements from index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;
    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        ithToDelete = ith;
        nElementsToDelete = nElements;
        while(nElementsToDelete > 0)
        {
            /* read elementSizes */
            nReads = sizeof(elementsBuf) / sizeof(Four);
            if(nReads > nElementsToDelete)
                nReads = nElementsToDelete;

            e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                     ithToDelete, nReads,
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
            e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                ithToDelete, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* remove elements from index */
            e = lrds_Collection_VarElementRemoveFromIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                          tid, colNo,
                                                          nReads, (Four*)elementSizesBuf, elementsBuf, objLockupPtr);
            if(e < eNOERROR) ERR(handle, e);

            nElementsToDelete -= nReads;
            ithToDelete       += nReads;
        }
    }

    /* update n elements */
    prev_nElementsOfCol = nElementsOfCol;
    nElementsOfCol     -= nElements;

    e = lrds_Collection_SetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* calculate total deleted element size */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];

    /* read ith element offset */
    updateStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                         updateStartOffset, VARENCODING_OFFSET_SIZE, (char*)&ithOffset,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* fetch (i+nElements)th element offset */
    if((ith + nElements) >= prev_nElementsOfCol)
    {
        inthOffset = tupHdr->varColOffset[cdesc->varColNo] -
            (colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(prev_nElementsOfCol));
    }
    else
    {
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                             colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith + nElements),
                             VARENCODING_OFFSET_SIZE, (char*)&inthOffset,
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if (e < 0) ERR(handle, e);
    }

    nDeletedBytes = inthOffset - ithOffset;

    /* update element sizes from ith to end */
    updateStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    nUpdatedElements  = prev_nElementsOfCol - ith;
    while(nUpdatedElements > 0)
    {
        if(nUpdatedElements > (sizeof(elementSizesBuf) / VARENCODING_OFFSET_SIZE))
            nReadElements = sizeof(elementSizesBuf) / VARENCODING_OFFSET_SIZE;
        else
            nReadElements = nUpdatedElements;

        /* fetch element sizes */
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                             updateStartOffset, nReadElements * VARENCODING_OFFSET_SIZE, (char*)elementSizesBuf,
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if (e < 0) ERR(handle, e);

        /* update element sizes */
        for(i = 0; i < nReadElements; i++)
            ((Four*)elementSizesBuf)[i] -= nDeletedBytes;

        /* update element sizes */
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                              updateStartOffset, nReadElements * VARENCODING_OFFSET_SIZE, (char*)elementSizesBuf,
                              nReadElements * VARENCODING_OFFSET_SIZE, NULL, NULL);
        if (e < 0) ERR(handle, e);

        /* proceed next element sizes */
        updateStartOffset += nReadElements * VARENCODING_OFFSET_SIZE;
        nUpdatedElements  -= nReadElements;
    }

    /* delete elements */
    deleteStartOffset = colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(prev_nElementsOfCol) + ithOffset;
    /* nDeletedBytes is calculated previously */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          deleteStartOffset, nDeletedBytes, NULL,
                          0, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* delete element sizes */
    deleteStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          deleteStartOffset, VARENCODING_OFFSET_SIZE * nElements, NULL,
                          0, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* update tuple header */
    for(i = cdesc->varColNo; i < tupHdr->nVarCols; i++) 
        tupHdr->varColOffset[i] -= (nDeletedBytes + nElements * VARENCODING_OFFSET_SIZE);

    return nElements;
} /* lrds_Collection_VarElementDelete */

Four lrds_Collection_VarElementFetchSizes(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    Four*               elementSizes)           /* OUT sizes of elements */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                fetchStartOffset;       /* start offset of fetching in the column */
    Four                nFetchedBytes;          /* amount of bytes fetched into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    VARENCODING_OFFSET_TYPE inthOffset;         /* offset of (i+nElements)th fetched element */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    Four                i;
    ColDesc             *relTableEntry_cdesc;  
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_VarElementFetchSizes(handle)"));

    if(nElements <= 0)
        return eNOERROR;

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* make lockup parameter */
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* fetch n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    /* fetch element offset */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    fetchStartOffset  = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    nFetchedBytes     = VARENCODING_OFFSET_SIZE * nElements;
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                         fetchStartOffset, nFetchedBytes, (char*)elementSizes,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if(e < 0) ERR(handle, e);

    /* fetch (i+nElements)th element offset */
    if((ith + nElements) >= nElementsOfCol)
    {
        inthOffset = tupHdr->varColOffset[cdesc->varColNo] -
                     (colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(nElementsOfCol));
    }
    else
    {
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                             colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith + nElements),
                             VARENCODING_OFFSET_SIZE, (char*)&inthOffset,
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if (e < 0) ERR(handle, e);
    }

    /* convert element offset to element size */
    for(i = 0; i < (nElements - 1); i++)
        elementSizes[i] = elementSizes[i + 1] - elementSizes[i];
    elementSizes[i] = inthOffset - elementSizes[i];

    return nElements;
} /* lrds_Collection_VarElementFetchSizes */

Four lrds_Collection_VarElementFetch(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to insert */
    void*               elements)               /* IN elements to fetch */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                fetchStartOffset;       /* start offset of fetching in the column */
    Four                nFetchedBytes;          /* amount of bytes fetched into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    VARENCODING_OFFSET_TYPE ithOffset;          /* offset of ith element */
    VARENCODING_OFFSET_TYPE inthOffset;         /* offset of (i+nElements)th fetched element */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_VarElementFetch(handle)"));

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

    if(nElements <= 0)
        return eNOERROR;

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* make lockup parameter */
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* read n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    /* read ith element offset */
    colStartOffset   = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    fetchStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                         fetchStartOffset, VARENCODING_OFFSET_SIZE, (char*)&ithOffset,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* fetch (i+nElements)th element offset */
    if((ith + nElements) >= nElementsOfCol)
    {
        inthOffset = tupHdr->varColOffset[cdesc->varColNo] -
                     (colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(nElementsOfCol));
    }
    else
    {
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                             colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith + nElements),
                             VARENCODING_OFFSET_SIZE, (char*)&inthOffset,
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if (e < 0) ERR(handle, e);
    }

    /* fetch elements */
    fetchStartOffset  = colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(nElementsOfCol) +
        ithOffset;
    nFetchedBytes     = inthOffset - ithOffset;
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                         fetchStartOffset, nFetchedBytes, elements,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if(e < 0) ERR(handle, e);

    return nElements;
} /* lrds_Collection_VarElementFetchSizes */

Four lrds_Collection_VarElementUpdate(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    TupleHdr*           tupHdr,                 /* IN tuple header of the tuple */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                ith,                    /* IN insert into ith element */
    Four                nElements,              /* IN # of elements to update */
    Four*               elementSizes,           /* IN sizes of elements to update */
    void*               elements)               /* IN elements to update */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                colStartOffset;         /* start offset of the column */
    Four                updateStartOffset;      /* start offset of updating in the column */
    Four                nUpdatedBytes;          /* amount of bytes updated into the column */
    Four                nElementsOfCol;         /* # of elements in the column */
    Four                prev_nElementsOfCol;
    Four                nUpdatedElements;       /* # of element sizes updated */
    Four                nReadElements;          /* # of element sizes read */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    char                elementsBuf[PAGESIZE];  /* buffer for holding the elementSizes */
    char                elementSizesBuf[PAGESIZE];
    Four                elementsSize;
    Four                offset;
    Four                elementSize;
    Four                ithToDelete;
    Four                nElementsToDelete;
    Four                nReads;
    Four                indexIdx;               /* index on array of index informations */
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    VARENCODING_OFFSET_TYPE ithOffset;          /* offset of ith element */
    VARENCODING_OFFSET_TYPE inthOffset;         /* offset of (i+nElements)th updated element */
    VARENCODING_OFFSET_TYPE new_inthOffset;     /* new offset of last updated element */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_VarElementUpdate(handle)"));

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

    /* get column information */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is there any opened scan on the given set? */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) continue; /* not used */

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* make lockup parameter */
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

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(*tupHdr, tupHdr->nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr->nFixedCols + tupHdr->nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* read n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* adjust nElements */
    if((ith + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - ith;

    /* update index - remove elements from index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;
    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        ithToDelete = ith;
        nElementsToDelete = nElements;
        while(nElementsToDelete > 0)
        {
            /* read elementSizes */
            nReads = sizeof(elementsBuf) / sizeof(Four);
            if(nReads > nElementsToDelete)
                nReads = nElementsToDelete;

            e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                     ithToDelete, nReads,
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
            e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, tupHdr, colNo,
                                                ithToDelete, nReads, elementsBuf);
            if(e < eNOERROR) ERR(handle, e);

            /* remove elements from index */
            e = lrds_Collection_VarElementRemoveFromIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                          tid, colNo,
                                                          nReads, (Four*)elementSizesBuf, elementsBuf, objLockupPtr);
            if(e < eNOERROR) ERR(handle, e);

            nElementsToDelete -= nReads;
            ithToDelete       += nReads;
        }
    }

    /* read ith element offset */
    updateStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                         updateStartOffset, VARENCODING_OFFSET_SIZE, (char*)&ithOffset,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* fetch (i+nElements)th element offset */
    if((ith + nElements) >= nElementsOfCol)
    {
        inthOffset = tupHdr->varColOffset[cdesc->varColNo] -
                     (colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(nElementsOfCol));
    }
    else
    {
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                             colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith + nElements),
                             VARENCODING_OFFSET_SIZE, (char*)&inthOffset,
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if (e < 0) ERR(handle, e);
    }

    /* calculate new_inthOffset */
    /* calculate total updated element size */
    for(i = 0, nUpdatedBytes = 0; i < nElements; i++)
        nUpdatedBytes += elementSizes[i];
    new_inthOffset = nUpdatedBytes + ithOffset;

    /* update element sizes from (ith + nElements) to end */
    colStartOffset    = (cdesc->varColNo == 0) ? tupHdr->firstVarColOffset:tupHdr->varColOffset[cdesc->varColNo-1];
    updateStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    nUpdatedElements  = nElementsOfCol - (ith + nElements);
    while(nUpdatedElements > 0)
    {
        if(nUpdatedElements > (sizeof(elementSizesBuf) / VARENCODING_OFFSET_SIZE))
            nReadElements = sizeof(elementSizesBuf) / VARENCODING_OFFSET_SIZE;
        else
            nReadElements = nUpdatedElements;

        /* fetch element sizes */
        e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,(ObjectID*)tid,
                             updateStartOffset, nReadElements * VARENCODING_OFFSET_SIZE, (char*)elementSizesBuf,
                             (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                             (relTableEntry->isCatalog) ? NULL : objLockupPtr);
        if (e < 0) ERR(handle, e);

        /* update element sizes */
        for(i = 0; i < nReadElements; i++)
            ((Four*)elementSizesBuf)[i] += (new_inthOffset - inthOffset);

        /* update element sizes */
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                              updateStartOffset, nReadElements * VARENCODING_OFFSET_SIZE, (char*)elementSizesBuf,
                              nReadElements * VARENCODING_OFFSET_SIZE, NULL, NULL);
        if (e < 0) ERR(handle, e);

        /* proceed next element sizes */
        updateStartOffset += nReadElements * VARENCODING_OFFSET_SIZE;
        nUpdatedElements  -= nReadElements;
    }

    /* convert element size to offset */
    offset = ithOffset;
    for(i = 0; i < nElements; i++)
    {
        elementSize = elementSizes[i];
        elementSizes[i] = offset;
        offset += elementSize;
    }

    /* update element sizes */
    updateStartOffset = colStartOffset + VARENCODING_ITHOFFSET_OFFSET(ith);
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          updateStartOffset, VARENCODING_OFFSET_SIZE * nElements, NULL,
                          0, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* update elements */
    updateStartOffset = colStartOffset + VARENCODING_FIRSTELEMENT_OFFSET(prev_nElementsOfCol) + ithOffset;
    nUpdatedBytes     = inthOffset - ithOffset;
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, (ObjectID*)tid,
                          updateStartOffset, nUpdatedBytes, NULL,
                          0, NULL, NULL);
    if(e < 0) ERR(handle, e);

    /* restore element sizes given by user, for avoiding functional side effects */
    for(i = 0; i < (nElements - 1); i++)
        elementSizes[i] = elementSizes[i + 1] - elementSizes[i];
    elementSizes[i] = new_inthOffset - elementSizes[i];

    /* update index - insert elements into index */
    /* Check if an index is on the collection */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
        if (relTableEntry_ii[indexIdx].colNo[0] == colNo)
            break;
    if (indexIdx != relTableEntry->ri.nIndexes)
    {   /* index is on the set. */
        e = lrds_Collection_VarElementInsertIntoIndex(handle, ornOrScanId, useScanFlag, &relTableEntry_ii[indexIdx],
                                                      tid, colNo,
                                                      nElements, elementSizes, elements, objLockupPtr);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* update tuple header */
    for(i = cdesc->varColNo; i < tupHdr->nVarCols; i++) 
        tupHdr->varColOffset[i] += ((new_inthOffset - inthOffset) +
                                      (prev_nElementsOfCol - nElementsOfCol) *
                                      VARENCODING_OFFSET_SIZE);

    return nElements;
} /* lrds_Collection_VarElementFetchSizes */

Four lrds_Collection_FixedElementRemoveFromIndex(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    IndexInfo*          indexInfo,              /* IN index information */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements,              /* IN # of elements to update */
    void*               elements,               /* IN elements to update */
    LockParameter*      lockup)                 /* lockup for SM_DeleteIndexEntry */
{
    Four                orn;
    Four                smScanId;
    char                *ptr;                   /* points to an element */
    MLGF_HashValue      mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    KeyValue            kval;                   /* key value of an index entry */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                e;                      /* error code */
    Four                i,j;
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementRemoveFromIndex(handle)"));

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
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 
    cdesc         = &relTableEntry_cdesc[colNo];

    switch (indexInfo->indexType)
    {
        case SM_INDEXTYPE_BTREE:
            ptr = elements;
            for(i = 0; i < nElements; i++)
            {
                /* Construct Key Value */
                /* key field length is represented by Two byte integer. */
                kval.len = (Two)cdesc->length;
                memcpy(&kval.val[0], ptr, cdesc->length);
                ptr += cdesc->length;

                /* Delete elements from the index. */
                e = SM_DeleteIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.btree,
                                        &kval, (ObjectID*)tid, lockup);
                if (e < 0) ERR(handle, e);
            }
            break;

        case SM_INDEXTYPE_MLGF:
            /* The element type is SM_MBR. */
            for(i = 0; i < nElements; i++)
            {
                for (j = 0; j < MBR_NUM_PARTS; j++)
                    mlgfKval[j] = (((LRDS_MBR*)elements)[i]).values[j];

                /* Delete elements from the index. */
                e = SM_MLGF_DeleteIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.mlgf,
                                             mlgfKval, (ObjectID*)tid, lockup);
                if (e < 0) ERR(handle, e);
            }
            break;

        default:
            ERR(handle, eINTERNAL);
    }

    return eNOERROR;
} /* lrds_Collection_FixedElementRemoveFromIndex */

Four lrds_Collection_FixedElementInsertIntoIndex(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    IndexInfo*          indexInfo,              /* IN index information */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements,              /* IN # of elements to update */
    void*               elements,               /* IN elements to update */
    LockParameter       *lockup)                /* lockup for SM_InsertIndexEntry */
{
    Four                orn;
    Four                smScanId;
    char                *ptr;                   /* points to an element */
    MLGF_HashValue      mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    KeyValue            kval;                   /* key value of an index entry */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                e;                      /* error code */
    Four                i, j;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_FixedElementInsertIntoIndex(handle)"));


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

    switch (indexInfo->indexType)
    {
        case SM_INDEXTYPE_BTREE:
            ptr = elements;
            for(i = 0; i < nElements; i++)
            {
                /* Construct Key Value */
                /* key field length is represented by Two byte integer. */
                kval.len = (Two)cdesc->length;
                memcpy(&kval.val[0], ptr, cdesc->length);
                ptr += cdesc->length;

                /* Insert elements into the index. */
                e = SM_InsertIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.btree,
                                        &kval, (ObjectID*)tid, lockup);
                if (e < 0) ERR(handle, e);
            }
            break;

        case SM_INDEXTYPE_MLGF:
            /* The element type is SM_MBR. */
            for(i = 0; i < nElements; i++)
            {
                for (j = 0; j < MBR_NUM_PARTS; j++)
                    mlgfKval[j] = (((LRDS_MBR*)elements)[i]).values[j];

                /* Insert elements into the index. */
                e = SM_MLGF_InsertIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.mlgf,
                                             mlgfKval, (ObjectID*)tid, NULL, lockup);
                if (e < 0) ERR(handle, e);
            }
            break;

        default:
            ERR(handle, eINTERNAL);
    }

    return eNOERROR;

} /* lrds_Collection_FixedElementInsertIntoIndex */

Four lrds_Collection_VarElementRemoveFromIndex(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    IndexInfo*          indexInfo,              /* IN index information */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements,              /* IN # of elements to update */
    Four*               elementSizes,           /* IN sizes of elements to update */
    void*               elements,               /* IN elements to update */
    LockParameter       *lockup)                /* lockup for SM_DeleteIndexEntry */
{
    Four                orn;
    Four                smScanId;
    char*               ptrElement;             /* points to an element */
    Four*               ptrElementSize;         /* points to an element size */
    Two                 s;                      /* for setting of key length */
    KeyValue            kval;                   /* key value of an index entry */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                e;                      /* error code */
    Four                i;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_VarElementRemoveFromIndex(handle)"));

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

    switch (indexInfo->indexType)
    {
        case SM_INDEXTYPE_BTREE:
            ptrElement     = elements;
            ptrElementSize = elementSizes;

            for(i = 0; i < nElements; i++)
            {
                /* Construct Key Value */
                /* key field length is represented by Two byte integer. */
                kval.len = sizeof(Two) + *(ptrElementSize);
                s = (Two)*(ptrElementSize);
                memcpy(&kval.val[0], &s, sizeof(Two));
                memcpy(&kval.val[sizeof(Two)], ptrElement, s);

                ptrElement     += cdesc->length;
                ptrElementSize ++;

                /* Delete elements from the index. */
                e = SM_DeleteIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.btree,
                                        &kval, (ObjectID*)tid, lockup);
                if (e < 0) ERR(handle, e);
            }
            break;

        default:
            ERR(handle, eINTERNAL);
    }

    return eNOERROR;
} /* lrds_Collection_VarElementRemoveFromIndex */

Four lrds_Collection_VarElementInsertIntoIndex(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    IndexInfo*          indexInfo,              /* IN index information */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    Four                nElements,              /* IN # of elements to update */
    Four*               elementSizes,           /* IN sizes of elements to update */
    void*               elements,               /* IN elements to update */
    LockParameter       *lockup)                /* lockup for SM_DeleteIndexEntry */
{
    Four                orn;
    Four                smScanId;
    char*               ptrElement;             /* points to an element */
    Four*               ptrElementSize;         /* points to an element size */
    Two                 s;                      /* for setting of key length */
    KeyValue            kval;                   /* key value of an index entry */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                e;                      /* error code */
    Four                i;
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_VarElementInsertIntoIndex(handle)"));

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

    switch (indexInfo->indexType)
    {
        case SM_INDEXTYPE_BTREE:
            ptrElement     = elements;
            ptrElementSize = elementSizes;

            for(i = 0; i < nElements; i++)
            {
                /* Construct Key Value */
                /* key field length is represented by Two byte integer. */
                kval.len = sizeof(Two) + *(ptrElementSize);
                s = (Two)*(ptrElementSize);
                memcpy(&kval.val[0], &s, sizeof(Two));
                memcpy(&kval.val[sizeof(Two)], ptrElement, s);

                ptrElement     += cdesc->length;
                ptrElementSize ++;

                /* Insert elements into the index. */
                e = SM_InsertIndexEntry(handle, &indexInfo->iid, &indexInfo->kdesc.btree,
                                        &kval, (ObjectID*)tid, lockup);
                if (e < 0) ERR(handle, e);
            }
            break;

        default:
            ERR(handle, eINTERNAL);
    }

    return eNOERROR;
} /* lrds_Collection_VarElementInsertIntoIndex */

Four lrds_Collection_Scan_Open(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo)                  /* IN column on which the collection is defined */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                tupHdrSize;             /* size of tuple header */
    Four                collectionScanId;       /* scan id of newly opened scan */
    TupleHdr            tupHdr;                 /* a tuple header */
    unsigned char       *nullVector;            /* bit array of null flags */
    Four                i;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    Four                nElementsOfCol;         /* # of elements of given column */
    ColDesc             *relTableEntry_cdesc;  

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Scan_Open(handle)"));

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
    cdesc         = &relTableEntry_cdesc[colNo];

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

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
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols); 

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) 
        ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* Is the scan already opened? Then return an error. */
    /* Currently we support only one scan per each collection. */
    /* We find also an empty slot for the new scan. */
    collectionScanId = NIL;            /* initialize the collectionScanId */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++) {
        if (LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId == NIL) { /* not used */
            collectionScanId = i;
            continue;
        }

        if (EQUAL_TUPLEID(LRDS_COLLECTIONSCANTABLE(handle)[i].tid, *tid) &&
            LRDS_COLLECTIONSCANTABLE(handle)[i].colNo == colNo) { /* found */
            return(eCOLLECTION_OPENSCAN_LRDS);
        }
    }

    /* Open a new scan. */
    if (collectionScanId == NIL) { /* There is no empty entry. */

        e = Util_doublesizeVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable, sizeof(lrds_CollectionScanTableEntry));
        if (e < 0) ERR(handle, e);

        collectionScanId = i;          /* use the first empty slot */

        /* Initialize the newly allocated entries. */
        for (; i < LRDS_PER_THREAD_DS(handle).lrdsScanTable.nEntries; i++)
            LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId = NIL;
    }

    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ornOrScanId          = (Four)ornOrScanId;
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].useScanFlag          = useScanFlag;
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].tid                  = *tid;
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].colNo                = (Two)colNo;
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead     = 0;
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementPrevRead   = 0;

    return(collectionScanId);
}

Four lrds_Collection_Scan_Close(
    Four handle,
    Four        collectionScanId)          /* IN scan id for a scan on a set */
{
    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Scan_Close(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_COLLECTIONSCANID(handle, collectionScanId)) ERR(handle, eBADPARAMETER);

    /* Close the given set scan. */
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ornOrScanId = NIL; /* not used */

    return(eNOERROR);
}

Four lrds_Collection_Scan_NextElements(
    Four handle,
    Four                collectionScanId,       /* IN scan id for a scan on a set */
    Four                nElements,              /* IN # of elements to read */
    Four*               elementSizes,           /* OUT buffer to return the element sizes for variable elements */
    Four                sizeOfElements,         /* IN size of elements buffer */
    void*               elements)               /* OUT buffer to return the read elements */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                ornOrScanId;
    Boolean             useScanFlag;
    TupleID*            tid;
    Two                 colNo;
    Four                tupHdrSize;             /* size of tuple header */
    Four                nElementsOfCol;
    TupleHdr            tupHdr;                 /* a tuple header */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                size;
    Four                i;
    ColDesc             *relTableEntry_cdesc;  
    Four                idx;                    

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Scan_NextElements(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_COLLECTIONSCANID(handle, collectionScanId)) ERR(handle, eBADPARAMETER);

    if (nElements < 0 || (nElements > 0 && elements == NULL)) ERR(handle, eBADPARAMETER);

    ornOrScanId = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ornOrScanId;
    useScanFlag = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].useScanFlag;

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
    tid          = &LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].tid;
    colNo        = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].colNo;
    tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, ornOrScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Get nElementsOfCol */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if((LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead;

    /* Fetch the elements */
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {
        e = lrds_Collection_VarElementFetchSizes(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                                 LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead,
                                                 nElements, elementSizes);
        if(e < eNOERROR) ERR(handle, e);

        for(size = 0, i = 0; i < nElements; i++)
        {
            if(sizeOfElements < (size + elementSizes[i]))
                break;
            size += elementSizes[i];
        }
        nElements = i;

        e = lrds_Collection_VarElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                            LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead,
                                            nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {
        if((nElements * relTableEntry_cdesc[colNo].length) > sizeOfElements)
            nElements = sizeOfElements / relTableEntry_cdesc[colNo].length;

        e = lrds_Collection_FixedElementFetch(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                              LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead,
                                              nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }

    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementPrevRead = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead;
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead  += nElements;

    return nElements;
}

Four lrds_Collection_Scan_GetSizeOfNextElements(
    Four handle,
    Four                collectionScanId,       /* IN scan id for a scan on a collection */
    Four                nElements,              /* IN # of elements to read */
    Four*               elementsSize)           /* OUT sizeof elements to read */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                ornOrScanId;
    Boolean             useScanFlag;
    TupleID*            tid;
    Two                 colNo;
    Four                tupHdrSize;             /* size of tuple header */
    Four                nElementsOfCol;
    TupleHdr            tupHdr;                 /* a tuple header */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    char                elementSizesBuf[PAGESIZE];
    Four                i;
    Four                nReads;
    Four                toReadIndex;
    ColDesc             *relTableEntry_cdesc;  

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Scan_GetSizeOfNextElements(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_COLLECTIONSCANID(handle, collectionScanId)) ERR(handle, eBADPARAMETER);

    if (nElements < 0 || (nElements > 0 && elementsSize == NULL)) ERR(handle, eBADPARAMETER);

    ornOrScanId = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ornOrScanId;
    useScanFlag = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].useScanFlag;

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    colNo         = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].colNo;
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    cdesc         = &relTableEntry_cdesc[colNo];

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
    tid          = &LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].tid;
    colNo        = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].colNo;
    tupHdrSize   = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, ornOrScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Get nElementsOfCol */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr,
                                      colNo, &nElementsOfCol);
    if(e < eNOERROR) ERR(handle, e);

    if((LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead + nElements) > nElementsOfCol)
        nElements = nElementsOfCol - LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead;

    /* Fetch the elements */
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {
        Four nElementsToRead;

        nElementsToRead = nElements;
        toReadIndex     = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead;
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
    {
        *elementsSize = nElements * cdesc->length;
    }

    return nElements;
}

Four lrds_Collection_Scan_InsertElements(
    Four handle,
    Four        	collectionScanId,       /* IN set where to insert the elements */
    Four        	nElements,          	/* IN # of elements to insert */
    Four*       	elementSizes,       	/* IN element sizes of elements to insert */
    void*       	elements)           	/* IN elements to insert */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                ornOrScanId;
    Boolean             useScanFlag;
    TupleID*            tid;
    Two                 colNo;
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    Four                tupHdrSize;             /* size of tuple header */
    TupleHdr            tupHdr;                 /* a tuple header */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *relTableEntry_cdesc;   

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Scan_InsertElements(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_COLLECTIONSCANID(handle, collectionScanId)) ERR(handle, eBADPARAMETER);

    if (nElements < 0 || (nElements > 0 && elements == NULL)) ERR(handle, eBADPARAMETER);

    ornOrScanId = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ornOrScanId;
    useScanFlag = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].useScanFlag;

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

    /* Prepare lock parameter */

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

    /* Fetch the tuple header. */
    tid          = &LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].tid;
    colNo        = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].colNo;
    tupHdrSize   = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, ornOrScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Insert the elements */
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {
        e = lrds_Collection_VarElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                             LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead,
                                             nElements, elementSizes, elements);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {
        e = lrds_Collection_FixedElementInsert(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                               LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead,
                                               nElements, elements);
        if(e < eNOERROR) ERR(handle, e);
    }

    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementPrevRead = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead;
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead  += nElements;

    return(eNOERROR);
}

Four lrds_Collection_Scan_DeleteElements(
    Four handle,
    Four                collectionScanId)       /* IN set from which to delete the element */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    Four                ornOrScanId;
    Boolean             useScanFlag;
    Four                tupHdrSize;             /* size of tuple header */
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    TupleHdr            tupHdr;                 /* a tuple header */
    TupleID*            tid;
    Two                 colNo;
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    ColDesc             *relTableEntry_cdesc;  


    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_Scan_DeleteElements(handle)"));

    /*
    ** check parameters
    */
    if (!LRDS_VALID_COLLECTIONSCANID(handle, collectionScanId)) ERR(handle, eBADPARAMETER);

    if (LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementPrevRead ==
        LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead)
        return(eNOERROR);

    ornOrScanId = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ornOrScanId;
    useScanFlag = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].useScanFlag;

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
    cdesc = &relTableEntry_cdesc[LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].colNo];

    /* Prepare lock parameter */

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

    /* Fetch the tuple header. */
    tid          = &LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].tid;
    colNo        = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].colNo;
    tupHdrSize   = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, ornOrScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* Delete the elements from the set */
    if(relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {
        e = lrds_Collection_VarElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                            LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementPrevRead, 1);
        if(e < eNOERROR) ERR(handle, e);
    }
    else
    {
        e = lrds_Collection_FixedElementDelete(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                               LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementPrevRead, 1);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* Adjust the cursor of the set scan. */
    LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementToRead = LRDS_COLLECTIONSCANTABLE(handle)[collectionScanId].ithElementPrevRead;

    return(eNOERROR);
}

Four lrds_Collection_InsertIntoIndex(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    IndexInfo*          indexInfo,              /* IN index information */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    LockParameter       *fileLockupPtr,         /* lockup for SM_DeleteIndexEntry */
    LockParameter       *objLockupPtr)          /* lockup for SM_DeleteIndexEntry */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                nElementsOfCol;
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                ith;
    Four                nReads;
    Four                elementsSize;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    Four                i;
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_InsertIntoIndex(handle)"));

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

    /* Get tupHdr */
    tupHdrSize   = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, ornOrScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         fileLockupPtr, objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* insert all the elements into the index */
    if (relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable-length elements */
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

            /* inserts elements into index */
            e = lrds_Collection_VarElementInsertIntoIndex(handle, ornOrScanId, useScanFlag, indexInfo, tid,
                                                          colNo, nReads,
                                                          (Four*)elementSizesBuf,
                                                          elementsBuf, objLockupPtr);
            if(e < eNOERROR) ERR(handle, e);

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

            /* inserts elements into index */
            e = lrds_Collection_FixedElementInsertIntoIndex(handle, ornOrScanId, useScanFlag, indexInfo, tid,
                                                            colNo, nReads, elementsBuf,
                                                            objLockupPtr);
            if(e < eNOERROR) ERR(handle, e);

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }

    return eNOERROR;
}

Four lrds_Collection_RemoveFromIndex(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    IndexInfo*          indexInfo,              /* IN index information */
    TupleID*            tid,                    /* IN tuple containing the set */
    Two                 colNo,                  /* IN column on which the set is defined */
    LockParameter       *fileLockupPtr,         /* lockup for SM_DeleteIndexEntry */
    LockParameter       *objLockupPtr)          /* lockup for SM_DeleteIndexEntry */
{
    Four                e;                      /* error code */
    Four                orn;
    Four                smScanId;
    char                elementsBuf[PAGESIZE];
    char                elementSizesBuf[PAGESIZE];
    Four                nElementsOfCol;
    ColDesc             *cdesc;                 /* pointer to the current column descriptor */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Four                ith;
    Four                nReads;
    Four                elementsSize;
    TupleHdr            tupHdr;                 /* a tuple header */
    Four                tupHdrSize;             /* size of tuple header */
    Four                i;
    ColDesc             *relTableEntry_cdesc;   
    unsigned char       *nullVector;            /* bit array of null flags */

    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_InsertIntoIndex(handle)"));

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

    /* Get tupHdr */
    tupHdrSize   = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, ornOrScanId, useScanFlag,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                         fileLockupPtr, objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    tupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* If the column has non NULL value, then return error. */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) ERR(handle, eCOLLECTION_NOTEXIST_LRDS);

    /* get n elements */
    e = lrds_Collection_GetN_Elements(handle, ornOrScanId, useScanFlag, tid, &tupHdr, colNo,
                                      &nElementsOfCol);
    if(e < 0) ERR(handle, e);

    /* insert all the elements into the index */
    if (relTableEntry_cdesc[colNo].type == SM_VARSTRING)
    {   /* variable-length elements */
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

            /* removes elements from index */
            e = lrds_Collection_VarElementRemoveFromIndex(handle, ornOrScanId, useScanFlag, indexInfo, tid, colNo,
                                                          nReads,
                                                          (Four*)elementSizesBuf,
                                                          elementsBuf, objLockupPtr);
            if(e < eNOERROR) ERR(handle, e);

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

            /* removes elements from index */
            e = lrds_Collection_FixedElementRemoveFromIndex(handle, ornOrScanId, useScanFlag, indexInfo, tid, colNo,
                                                            nReads, elementsBuf,
                                                            objLockupPtr);
            if(e < eNOERROR) ERR(handle, e);

            nElementsOfCol -= nReads;
            ith            += nReads;
        }
    }

    return eNOERROR;
}

Four lrds_Collection_CompareData(
    Four handle,
    Two                 type,           /* IN: type of the element */
    Four                element1size,   /* IN: size of element1 */
    void*               element1,       /* IN: element1 */
    Four                element2size,   /* IN: size of element2 */
    void*               element2,       /* IN: element2 */
    Four                 keySize)        /* IN: size of key compared */
{
    Four                result;
    Four                size;
    Two_Invariable      s1, s2;                 /* variables for 'Short' type */
    Four_Invariable     i1, i2;                 /* variables for 'Int' type */
    Four_Invariable     l1, l2;                 /* variables for 'Long' type */
    Eight_Invariable    ll1, ll2;               /* variables for 'Long Long' type */
    float               f1, f2;                 /* variables for 'float' type */
    double              d1, d2;                 /* variables for 'double' type */

    size = MIN(element1size, element2size);

    switch(type)
    {
    case SM_SHORT:
        memcpy(&s1, element1, sizeof(Two_Invariable));
        memcpy(&s2, element2, sizeof(Two_Invariable));
        result = ((s1 == s2) ? 0 : ((s1 > s2) ? 1 : -1)); 
        break;
    case SM_INT:
        memcpy(&i1, element1, sizeof(Four_Invariable));
        memcpy(&i2, element2, sizeof(Four_Invariable));
        result = ((i1 == i2) ? 0 : ((i1 > i2) ? 1 : -1)); 
        break;
    case SM_LONG:
        memcpy(&l1, element1, sizeof(Four_Invariable));
        memcpy(&l2, element2, sizeof(Four_Invariable));
        result = ((l1 == l2) ? 0 : ((l1 > l2) ? 1 : -1)); 
        break;
    case SM_LONG_LONG:
        memcpy(&ll1, element1, sizeof(Eight_Invariable));
        memcpy(&ll2, element2, sizeof(Eight_Invariable));
        result =((ll1 == ll2) ? 0 : ((ll1 > ll2) ? 1 : -1)); 
        break;        
    case SM_FLOAT:
        memcpy(&f1, element1, sizeof(float));
        memcpy(&f2, element2, sizeof(float));
        result = ((f1 == f2) ? 0 : ((f1 > f2) ? 1 : -1));
        break;
    case SM_DOUBLE:
        memcpy(&d1, element1, sizeof(double));
        memcpy(&d2, element2, sizeof(double));
        result = ((d1 == d2) ? 0 : ((d1 > d2) ? 1 : -1)); 
        break;
    case SM_STRING:
    case SM_VARSTRING:
        if(keySize == ALL_VALUE) {
            result = memcmp(element1, element2, size);
            if(result == 0) 
                result = ((element1size == element2size) ? 0 : ((element1size > element2size) ? 1 : -1));
        }
        else {
            result = memcmp(element1, element2, MIN(size, keySize));
            if(result == 0 && size < keySize)
                result = ((element1size == element2size) ? 0 : ((element1size > element2size) ? 1 : -1)); 
        }
        break;
    default:
        result = memcmp(element1, element2, size);
        if(result == 0)
            result = ((element1size == element2size) ? 0 : ((element1size > element2size) ? 1 : -1));
        break;

    }

    return result;
}

/*
 * Function: Four lrds_Collection_IsNull(Four, Boolean, TupleID*, Two)
 *
 * Description:
 *  Check where the given colNo is null or not.
 *
 * Retuns:
 *  error code
 */
Four lrds_Collection_IsNull(
    Four handle,
    Four     		ornOrScanId,       	/* IN open relation no or scan id*/
    Boolean  		useScanFlag,       	/* IN TRUE if above parameter is scan id */
    TupleID		*tid,               	/* IN tuple containing the given column */
    Two      		colNo              	/* IN column on which the set is created */
)
{
    Four     		e;                      /* error code */
    Boolean             flag;                   /* flag that indicates the given colNo is null or not */
    Four 		orn;
    Four 		smScanId;
    Four 		tupHdrSize;             /* size of tuple header */
    TupleHdr 		tupHdr;                 /* a tuple header */
    unsigned char 	*nullVector;          	/* bit array of null flags */
    lrds_RelTableEntry 	*relTableEntry;  	/* pointer to an entry of relation table */
    ColDesc 		*cdesc;                 /* pointer to the current column descriptor */
    LockParameter 	fileLockup;           	/* lockup for SM_Fetch Tuple */
    LockParameter 	objLockup;            	/* lockup for SM_Fetch Tuple */
    LockParameter 	*fileLockupPtr;       	/* pointer to the lockup value */
    LockParameter 	*objLockupPtr;        	/* pointer to the lockup value */
    ColDesc 		*relTableEntry_cdesc;


    TR_PRINT(TR_LRDS, TR1, ("lrds_Collection_IsNull(handle)"));


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

    if (cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        cdesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST)
        ERR(handle, eBADPARAMETER);

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
    if (e < 0) ERR(handle, e);

    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

    /* check where the given colNo is null or not */
    if (colNo >= tupHdr.nFixedCols + tupHdr.nVarCols || BITTEST(nullVector, colNo)) flag = TRUE;
    else flag = FALSE;


    return (flag);
}
