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
 * Module: LRDS_NextRelationBulkLoad_OrderedSet.c
 *
 * Description:
 *  Process the data file bulk load.
 *
 * Exports:
 *  Four LRDS_NextRelationBulkLoad_Collection(VolID, Four, Four, Boolean, Boolean, TupleID*, Four, Four)
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM.h"
#include "LRDS.h"
#include "BL_LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"
#include "RDsM.h"


#define ENCODING_NELEMENTS_TYPE                         Four
#define ENCODING_KEYSIZE_TYPE                           Four
#define VARENCODING_OFFSET_TYPE                         Four
#define ENCODING_NELEMENTS_SIZE                         sizeof(Four)
#define ENCODING_KEYSIZE_SIZE                           sizeof(Four)
#define VARENCODING_OFFSET_SIZE                         sizeof(Four)

/* internal function prototype */
Four lrds_NextRelationBulkLoad_FlushTuple(Four, LRDS_BlkLdTableEntry*, TupleID*);
Four lrds_InsertIndexEntriesForCollection(Four, Four, Four, Boolean, TupleID*);
Four lrds_InsertIndexEntriesForCollectionByBulkLoad(Four, LRDS_BlkLdTableEntry*, Four, Four, TupleID*, Four);


/*@================================================
 *  LRDS_NextRelationBulkLoad_Collection()
 * ===============================================*/

/*
 * Function : Four LRDS_NextRelationBulkLoad_Collection()
 *
 * Description :
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 */
Four LRDS_NextRelationBulkLoad_Collection(
    Four	       handle,
    VolID              tmpVolId,            /* IN temporary volume ID */
    Four               blkLdId,             /* IN bulkload ID */
    Four               streamId,            /* IN ID of stream in which elements of ordered set is saved */
    Boolean            isSortStream,        /* IN flag which indicates input stream is sort stream or not */
    Boolean            endOfTuple,          /* IN flag which indicates this ordered set is end of tuple or not */
    TupleID*           tid,                 /* OUT tuple ID */
    Four               colNo,               /* IN column number of ordered set type */
    Four               keySize)             /* IN key size in varstring, string */
{
    Four               e;                   /* error code */
    Four               i;                   /* index variable */
    Four               collectionHdrLen;    /* size of collection header */
    Four               varColumnNo;         /* column number of variable column */
    Boolean            done = FALSE;        /* flag which indicates sort stream is empty or not */
    SortStreamTuple    sortTuple;           /* tuple for input sort stream */
    Four               numSortTuple;        /* # of sort tuple from sort stream. In this function, always 1 */
    char               elementBuf[PAGESIZE];/* buffer for element in sort tuple */
    Four               cmp;                 /* comparison result */
    Two                kvalLen;             /* key length */
    KeyValue           prevKval;            /* buffer for previous element */
    Four               remainedLen;         /* remained length of slotArray */
    Four               nowInBufLen;         /* length of the slotArray in tuple buffer */
    Four               elementLen;          /* length of element in sortTuple */
    char*              elementPtr;          /* pointer which points element in sortTuple */
    ColDesc*           complexTypeColDesc;  /* complex type column descriptor */
    Four               collectionColLen;    /* size of collection column */
    Four               offsetArraySize;     /* size of offset array */
    Four               offsetArrayOffset;   /* offset of offset array */
    Four               offsetArrayIdx;      /* index in offset array */
    VARENCODING_OFFSET_TYPE* offsetArray;   /* offset array */
    VARENCODING_OFFSET_TYPE  elementOffset; /* offset array */
    ENCODING_NELEMENTS_TYPE  nElements;     /* # of elements */
    LRDS_BlkLdTableEntry*    blkLdEntry;    /* bulkload table entry pointer */
    lrds_RelTableEntry*      relTableEntry; /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;


    /*
     *  O. Set entry for fast access
     */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];
    relTableEntry = blkLdEntry->lrdsBlkLdRelTableEntry;
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);


    /*
     *  I. Update global variables for data file bulkload
     */

    /* error check */
    /* Note!! this function must be called only once */
    if (colNo <= blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols) ERR(handle, eINTERNAL);

    /* update 'lrdsBlkLdRemainFlagArrayIdxForVarCols' */
    blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols = colNo;

    /* set corresponding entry of 'lrdsBlkLdRemainFlagArray' as FALSE */
    blkLdEntry->lrdsBlkLdRemainFlagArray[colNo] = FALSE;

    /* reset the corresponding bit of null vector. */
    BITRESET(blkLdEntry->lrdsBlkLdNullVector, colNo);


    /*
     *  II. Set complex type column description for fast access
     */

    /* set complexTypeColDesc */
    complexTypeColDesc = &relTableEntry_cdesc[colNo];

    /* error check */
    if (complexTypeColDesc->complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
        complexTypeColDesc->complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
        complexTypeColDesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST) ERR(handle, eBADPARAMETER);

    /* assertion check */
    assert(complexTypeColDesc->varColNo != NIL);


    /*
     *  III. Prepare to insert elements into collection
     */

    /* calculate 'collectionHdrLen' */
    collectionHdrLen = (complexTypeColDesc->type == SM_VARSTRING || complexTypeColDesc->type == SM_STRING) ?
                       ENCODING_NELEMENTS_SIZE + ENCODING_KEYSIZE_SIZE : ENCODING_NELEMENTS_SIZE;

    /* set 'nElements' */
    nElements = (!isSortStream) ?  SM_GetNumTuplesInStream(handle, streamId) : SM_GetNumTuplesInSortStream(handle, streamId); 

    /* prepare offset array if needed */
    if (complexTypeColDesc->type == SM_VARSTRING) {

        /* calculate offsetArraySize */
        offsetArraySize = VARENCODING_OFFSET_SIZE*nElements;

        /* allocate memory for offsetArray */
        offsetArray = (VARENCODING_OFFSET_TYPE *) malloc(offsetArraySize);
        if (offsetArray == NULL) ERR(handle, eMEMORYALLOCERR);

        /* initialize offsetArrayIdx & elementOffset */
        offsetArrayIdx = 0;
        elementOffset = 0;
    }

    /* check index is exist on this collection column */
    if (blkLdEntry->clusteringFlag == FALSE) {

        for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

            /* skip other index */
            if (relTableEntry_ii[i].colNo[0] != colNo) continue;

            /* open stream for automatic indexing of collection */
            blkLdEntry->kvalStreamIdArray[i] = SM_OpenStream(handle, tmpVolId);
            if (blkLdEntry->kvalStreamIdArray[i] < 0) ERR(handle, blkLdEntry->kvalStreamIdArray[i]);
        }
    }


    /*
     *  IV. Append header into collection column
     */

    /* if tuple buffer is overflowed, flush out tuple buffer */
    if (blkLdEntry->lrdsBlkLdTupBufOffset + collectionHdrLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

        /*** Flush ***/
        e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                    blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /*** Reset ***/
        blkLdEntry->lrdsBlkLdTupBufOffset = 0;
        blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
    }

    /* append nElements to tuple */
    memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], &nElements, ENCODING_NELEMENTS_SIZE);
    blkLdEntry->lrdsBlkLdTupBufOffset += ENCODING_NELEMENTS_SIZE;

    /* append 'keySize' if needed */
    if (complexTypeColDesc->type == SM_VARSTRING || complexTypeColDesc->type == SM_STRING) {
        memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], &keySize, ENCODING_KEYSIZE_SIZE);
        blkLdEntry->lrdsBlkLdTupBufOffset += ENCODING_KEYSIZE_SIZE;
    }


    /*
     *  V. Append dummy offset array into tuple if needed
     *     Note!! slot array size can be exceed to buffer size
     */

    if (complexTypeColDesc->type == SM_VARSTRING) {

        /* set 'offsetArrayOffset' */
        offsetArrayOffset = blkLdEntry->lrdsBlkLdTupBufOffset;

        /* initialize 'remaindedLen' */
        remainedLen = offsetArraySize;

        while (blkLdEntry->lrdsBlkLdTupBufOffset + remainedLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

            /* calculate available space */
            nowInBufLen = SIZE_OF_LRDS_TUPLE_BUFFER - blkLdEntry->lrdsBlkLdTupBufOffset;

            /* update 'remainedLen' & 'lrdsBlkLdTupBufOffset' */
            remainedLen -= nowInBufLen;
            blkLdEntry->lrdsBlkLdTupBufOffset += nowInBufLen;

            /*** Flush ***/
            e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                        blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);

            /*** Reset ***/
            blkLdEntry->lrdsBlkLdTupBufOffset = 0;
            blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
        }

        /* update 'lrdsBlkLdTupBufOffset' */
        blkLdEntry->lrdsBlkLdTupBufOffset += remainedLen;
    }


    /*
     *  VI. For each elements in (sort) stream, append it into ordered set column
     */

    /* initialize numSortTuple */
    numSortTuple = 1;

    /* set tuple buffer pointer */
    sortTuple.data = elementBuf;

    /* initialize prevKval */
    prevKval.len = NIL;

    /* for each element in (sort) stream, append into ordered set */
    while (1) {

        /*
         *  Get element from stream
         */

        /* initialize length of sort tuple */
        sortTuple.len = PAGESIZE;

        /* get element */
        if (isSortStream) {
            e = SM_GetTuplesFromSortStream(handle, streamId, &numSortTuple, &sortTuple, &done);
            if (e < eNOERROR) ERR(handle, e);
        }
        else {
            e = SM_GetTuplesFromStream(handle, streamId, &numSortTuple, &sortTuple, &done);
            if (e < eNOERROR) ERR(handle, e);
        }

        /* check end of stream */
        if (done) break;

        /* assertion check */
        assert (numSortTuple == 1);


        /*
         *  Append element into collection
         */

        /* get elementLen & elementPtr */
        /* Note!! in case of varstring, we must remove length field */
        if (complexTypeColDesc->type == SM_VARSTRING) {
            elementLen = sortTuple.len - sizeof(Two);
            elementPtr = sortTuple.data + sizeof(Two);
        }
        else {
            elementLen = sortTuple.len;
            elementPtr = sortTuple.data;

            assert(elementLen == complexTypeColDesc->length);
        }

        /* Check current element is correct */
        if (prevKval.len != NIL && complexTypeColDesc->complexType != SM_COMPLEXTYPE_COLLECTIONLIST) {

            /* compare with previos kval */
            cmp = lrds_Collection_CompareData(handle, complexTypeColDesc->type,
                                              prevKval.len, prevKval.val, elementLen, elementPtr, keySize);

            /* check complex type property */
            if (complexTypeColDesc->complexType == SM_COMPLEXTYPE_COLLECTIONSET) {
                if (cmp >= 0) ERR(handle, eBADPARAMETER);
            }
            else if (complexTypeColDesc->complexType == SM_COMPLEXTYPE_COLLECTIONBAG) {
                if (cmp > 0) ERR(handle, eBADPARAMETER);
            }
        }

        /* set prevKval */
        prevKval.len = (keySize == ALL_VALUE) ? elementLen : MIN(keySize, elementLen);
        memcpy(prevKval.val, elementPtr, prevKval.len);

        /* if tuple buffer is overflowed, flush out tuple buffer */
        if (blkLdEntry->lrdsBlkLdTupBufOffset + elementLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

            /*** Flush ***/
            e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                        blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);

            /*** Reset ***/
            blkLdEntry->lrdsBlkLdTupBufOffset = 0;
            blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;

            /* error check */
            if (elementLen > SIZE_OF_LRDS_TUPLE_BUFFER) ERR(handle, eINTERNAL);
        }

        /* append the element of the ordered set */
        memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], elementPtr, elementLen);

        /* update 'lrdsBlkLdTupBufOffset' */
        blkLdEntry->lrdsBlkLdTupBufOffset += elementLen;


        /*
         *  Insert key value of the current element into kval stream if needed
         */

        if (blkLdEntry->clusteringFlag == FALSE) {

            for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

                /* skip not available kvalStream */
                if (blkLdEntry->kvalStreamIdArray[i] == NIL) continue;

                /* update sortTuple if keySize isn't ALL_VALUE */
                if (keySize != ALL_VALUE) {

                    /* calculate new kvalLen */
                    kvalLen = MIN(keySize, elementLen);

                    /* update sortTuple for new kvalLen */
                    if (complexTypeColDesc->type == SM_VARSTRING) {
                        sortTuple.len = kvalLen + sizeof(Two);
                        memcpy(sortTuple.data, &kvalLen, sizeof(Two));
                    }
                    else if (complexTypeColDesc->type == SM_STRING) {
                        sortTuple.len = kvalLen;
                    }
                }

                /* Note!! element in input stream must be the same format as kval */
                e = SM_PutTuplesIntoStream(handle, blkLdEntry->kvalStreamIdArray[i], 1, &sortTuple);
                if (e < eNOERROR) ERR(handle, e);

            } /* for 'i' */

        } /* if */


        /*
         *  Construct offset array if needed
         */

        if (complexTypeColDesc->type == SM_VARSTRING) {

            /* insert corresponding entry into offset array for input element */
            offsetArray[offsetArrayIdx] = elementOffset;

            /* update 'offsetArrayIdx' & 'elementOffset' */
            offsetArrayIdx ++;
            elementOffset += elementLen;

        } /* if */

    } /* while */


    /*
     *  VII. insert offset array into tuple
     */

    if (complexTypeColDesc->type == SM_VARSTRING) {

        /* assertion check */
        assert(offsetArrayIdx == nElements);

        /* VII-1. if tuple including collection is small */
        if (blkLdEntry->lrdsBlkLdIsFirstTupBuf == TRUE) {

            /* insert slot array to tuple */
            memcpy(&blkLdEntry->lrdsBlkLdTupBuf[offsetArrayOffset], (char *) offsetArray, offsetArraySize);
        }
        /* VII-2. if tuple including collection is large */
        else {

            /* set varColumnNo for fast access */
            varColumnNo = complexTypeColDesc->varColNo;

            /* re-calculate 'offsetArrayOffset' */
            offsetArrayOffset = (varColumnNo == 0) ?
                              blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset + collectionHdrLen :
                              blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1] + collectionHdrLen;

            /* insert slot array to tuple */
            e = SM_NextDataFileBulkLoadWriteLOT(handle, blkLdEntry->smDataFileBlkLdId,
                                                offsetArrayOffset, offsetArraySize, (char*) offsetArray, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);
        }

        /* finalize slot array for ordered set bulkload */
        free(offsetArray);

    } /* if */


    /*
     *  VIII. set varColOffset
     */

    /* set varColumnNo for fast access */
    varColumnNo = complexTypeColDesc->varColNo;

    /* calculate 'collectionColLen' */
    if (complexTypeColDesc->type == SM_VARSTRING) {
        collectionColLen = collectionHdrLen + offsetArraySize + elementOffset;
    }
    else {
        collectionColLen = collectionHdrLen + complexTypeColDesc->length*nElements;
    }

    /* set 'varColOffset' */
    blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo] = (varColumnNo == 0) ?
                                                blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset + collectionColLen :
                                                blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1] + collectionColLen;


    /*
     *  IX. if end of tuple, flush out tuple header
     */

    if (endOfTuple == TRUE) {
        e = lrds_NextRelationBulkLoad_FlushTuple(handle, blkLdEntry, tid);
        if (e < eNOERROR) ERR(handle, e);
    }


    return(eNOERROR);

} /* LRDS_NextRelationBulkLoad_Collection() */


Four lrds_InsertIndexEntriesForCollection(
    Four	       handle,
    Four               kvalStreamId,
    Four               smIndexBlkLdId,
    Boolean            indexBlkLdFlag,
    TupleID*           tid)
{
    Four               e;                   /* error code */
    Boolean            done = FALSE;        /* flag which indicates sort stream is empty or not */
    SortStreamTuple    sortTuple;           /* tuple for input sort stream */
    Four               numSortTuple;        /* # of sort tuple from sort stream. In this function, always 1 */
    KeyValue           kval;                /* key value of each element */


    /*
     *  error check
     */

    if (kvalStreamId == NIL) ERR(handle, eBADPARAMETER);


    /*
     *  Prepare to retrieve kval from stream
     */

    e = SM_ChangePhaseStream(handle, kvalStreamId);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Insert retrieved kval into index
     */

    /* initialize numSortTuple */
    numSortTuple = 1;

    /* set tuple buffer pointer */
    sortTuple.data = &kval.val[0];

    while(1) {

        /* initialize length of sort tuple */
        sortTuple.len = MAXKEYLEN;

        /* get kval.data */
        e = SM_GetTuplesFromStream(handle, kvalStreamId, &numSortTuple, &sortTuple, &done);
        if (e < eNOERROR) ERR(handle, e);

        /* set kval.len */
        kval.len = sortTuple.len;

        /* check end of stream */
        if (done) break;

        /* assertion check */
        assert (numSortTuple == 1);

        /* insert into index */
        if (indexBlkLdFlag == TRUE) {
            e = SM_NextIndexBulkLoad(handle, smIndexBlkLdId, &kval, tid);
            if (e < eNOERROR) ERR(handle, e);
        }
        else {
            e = SM_NextIndexBulkInsert(handle, smIndexBlkLdId, &kval, tid);
            if (e < eNOERROR) ERR(handle, e);
        }
    }


    /*
     *  Close kval stream
     */

    e = SM_CloseStream(handle, kvalStreamId);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* lrds_InsertIndexEntriesForCollection() */



Four lrds_InsertIndexEntriesForCollectionByBulkLoad(
    Four		  handle,
    LRDS_BlkLdTableEntry* blkLdEntry,       /* IN  bulkload table entry pointer */
    Four               smIndexBlkLdId,      /* IN  index bulkload ID */
    Four               scanId,              /* IN  scan ID */
    TupleID*           tid,                 /* IN  tuple ID */
    Four               colNo)               /* IN  column number */
{
    Four               e;                   /* error code */
    Four               i;                   /* index variable */
    Four               collectionScanId;    /* collection scan ID */
    Four               keySize;             /* size of key */
    Four               nElements;           /* # of elements */
    Four               elementSize;         /* element size */
    Four               elementSizes[100];   /* array of element size */
    char               elements[PAGESIZE];  /* buffer for elements */
    char*              elementPtr;          /* pointer of element */
    Two                kvalLen;             /* length of key value */
    KeyValue           kval;                /* key value of each element */

    TupleHdr           tupHdr;              /* a tuple header */
    Four               tupHdrSize;          /* size of tuple header */

    LockParameter      fileLockup;          /* lockup for SM_Fetch Tuple */
    LockParameter      objLockup;           /* lockup for SM_Fetch Tuple */
    LockParameter*     fileLockupPtr;       /* pointer to the lockup value */
    LockParameter*     objLockupPtr;        /* pointer to the lockup value */

    lrds_RelTableEntry* relTableEntry;      /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;


    /*
     *  Open collection scan
     */

    collectionScanId = lrds_Collection_Scan_Open(handle, scanId, TRUE, tid, colNo);
    if (collectionScanId < 0) ERR(handle, collectionScanId);


    /*
     *  Set relTableEntry for fast access
     */

    relTableEntry = blkLdEntry->lrdsBlkLdRelTableEntry;
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);


    /*
     *  Get keySize
     */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    if (relTableEntry->isCatalog) {
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
    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, LRDS_SCANTABLE(handle)[scanId].smScanId, TRUE,
                         (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr, fileLockupPtr, objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);

    /* get keySize */
    e = lrds_Collection_GetKeySize(handle, scanId, TRUE, tid, &tupHdr, colNo, &keySize);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Insert retrieved kval into index
     */

    while(1) {

        /* get elements */
        nElements = lrds_Collection_Scan_NextElements(handle, collectionScanId, 100, elementSizes, PAGESIZE, elements);
        if(nElements < 0) ERR(handle, nElements);

        /* check end of stream */
        if (nElements == 0) break;

        for (i = 0, elementPtr = elements; i < nElements; i++) {

            /* get elementSize */
            elementSize = (relTableEntry_cdesc[colNo].type == SM_VARSTRING) ? elementSizes[i] : relTableEntry_cdesc[colNo].length;

            /* calculate kvalLen */
            kvalLen = (keySize == ALL_VALUE) ? elementSize : MIN(keySize, elementSize);

            /* get kval */
            if (relTableEntry_cdesc[colNo].type == SM_VARSTRING) {
                kval.len = kvalLen + sizeof(Two);
                memcpy(&kval.val[0], &kvalLen, sizeof(Two));
                memcpy(&kval.val[sizeof(Two)], elementPtr, kvalLen);
            }
            else {
                kval.len = kvalLen;
                memcpy(&kval.val[0], elementPtr, kvalLen);
            }

            /* insert into index */
            if (blkLdEntry->indexBlkLdFlag == TRUE) {
                e = SM_NextIndexBulkLoad(handle, smIndexBlkLdId, &kval, tid);
                if (e < eNOERROR) ERR(handle, e);
            }
            else {
                e = SM_NextIndexBulkInsert(handle, smIndexBlkLdId, &kval, tid);
                if (e < eNOERROR) ERR(handle, e);
            }

            /* update elementPtr */
            elementPtr += elementSize;

        } /* for */

    } /* while */


    /*
     *  Close collection scan
     */

    e = lrds_Collection_Scan_Close(handle, collectionScanId);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* lrds_InsertIndexEntriesForCollectionByBulkLoad() */
