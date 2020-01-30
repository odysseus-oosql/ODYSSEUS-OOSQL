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
 * Module: BtM_AppendBulkLoad.c
 *
 * Description:
 *  Make new B+ tree index using given sorted <key, OID> list and old B+ tree index.
 *
 * Exports:
 *  Four BtM_AppendBulkLoad(ObjectID*, PageID *, Four, Four, Two, Pool*, DeallocListElem*)
 *
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Sort.h"
#include "BtM.h"
#include "BL_BtM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


#define MAXNUMOFTUPLES  100




/*@===========================
 * BtM_AppendBulkLoad()
 *===========================*/
/*
 * Function: Four BtM_AppendBulkLoad(ObjectID*, PageID*, Four, Two, Two, Pool*, DeallocListElem*)
 *
 * Description:
 *  Make new B+ tree index using given sorted <key, OID> list and old B+ tree index.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four BtM_AppendBulkLoad (
    Four	   	    handle,
    XactTableEntry_T        *xactEntry,                             /* IN transaction table entry */
    BtreeIndexInfo          *iinfo,                                 /* IN B tree information */
    FileID                  *fid,                                   /* IN FileID */ 
    PageID                  *root,                                  /* IN root PageID of index to be created */
    Four                    sortStreamId,                           /* IN IDentifier of SortStream */
    Two                     eff,                                    /* IN extent fill factor */
    Two                     pff,                                    /* IN page fill factor */
    LogParameter_T          *logParam)                              /* IN log parameter */
{
    Four                    e;                                      /* error number */
    Four                    i;                                      /* a loop index */

    Four                    numSortTuples;                          /* # of tuples from sortStream */
    SortStreamTuple         sortTuples[MAXNUMOFTUPLES];             /* tuple for sort stream */
    char                    tuples[MAXNUMOFTUPLES][2*MAXKEYLEN];    /* buffer for temporary object */

    KeyValue                key;                                    /* a key value */
    ObjectID                oid;                                    /* a Object ID */

    SortStreamTableEntry    *sortTableEntry;                        /* entry in which information about opened sort stream is saved */
    SortTupleDesc           *sortKeyDesc;                           /* sort key descriptor of Btree index */
    KeyDesc                 kdesc;                                  /* key descriptor of B+ tree index */

    Boolean                 done = FALSE;                           /* flag which indicates sort stream is empty or not */

    Four                    compResult;                             /* compare result of key values */
    Boolean                 againFlag = FALSE;                      /* flag to determin looping or not */

    Four                    btmBlkLdId;                             /* BtM bulkload ID */
    BtM_BlkLdTableEntry*    blkLdEntry;                             /* entry in which information about bulkload is saved */


    TR_PRINT(handle, TR_BTM, TR1,
             ("BtM_AppendBulkLoad(xactEntry=%P, iinfo=%P, root=%P, sortStreamId=%P, eff=%ld, pff=%ld, logParam=%P)",
              xactEntry, iinfo, root, sortStreamId, eff, pff, logParam));


    /*
    ** O. Check parameters
    */

    if (xactEntry == NULL)              ERR(handle, eBADPARAMETER);

    if (iinfo == NULL)                  ERR(handle, eBADPARAMETER);

    if (root == NULL)                   ERR(handle, eBADPARAMETER);

    if (sortStreamId < 0)               ERR(handle, eBADPARAMETER);

    if (eff < 0 || eff > 100)           ERR(handle, eBADPARAMETER);

    if (pff < MINPFF || pff > MAXPFF)   ERR(handle, eBADPARAMETER);

    if (logParam == NULL)               ERR(handle, eBADPARAMETER);



    /*
    ** I. Find empty entry from BtM bulkload table
    */

    for (btmBlkLdId = 0; btmBlkLdId < BTM_BLKLD_TABLE_SIZE; btmBlkLdId++ ) {
	if (BTM_BLKLD_TABLE(handle)[btmBlkLdId].isUsed == FALSE) break;
    }
    if (btmBlkLdId == BTM_BLKLD_TABLE_SIZE) ERR(handle, eBLKLDTABLEFULL);

    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId];

    /* set isUsed flag */
    blkLdEntry->isUsed = TRUE;



    /*
    ** II. Get index information
    */

    /* 1. get sort key descriptor */
    sortTableEntry = &SORT_STREAM_TABLE(handle)[sortStreamId];
    sortKeyDesc = &(sortTableEntry->sortTupleDesc);

    /* 2. get key descriptor from sort key descriptor */
    kdesc.nparts = sortKeyDesc->nparts - 1;
    for (i = 0; i < sortKeyDesc->nparts - 1; i++) {
        kdesc.kpart[i].type     = sortKeyDesc->parts[i].type;
        kdesc.kpart[i].length   = sortKeyDesc->parts[i].length;
    }

    /* 3. get information about B+ tree index to be bulkloaded */
    e = btm_BlkLdGetInfo(handle, btmBlkLdId, iinfo, root, &kdesc, eff, pff);
    if (e < 0)  ERR(handle, e);



    /*
    ** III. Initialize buffer
    */

    /* 1. initialize internal node buffer which will be used in index bulkload */
    e = btm_BlkLdInitInternalBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 2. initialize leaf node buffer which will be used in index bulkload */
    e = btm_BlkLdInitLeafBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 3. initialize overflow node buffer which will be used in index bulkload */
    e = btm_BlkLdInitOverflowBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 4. initialize write buffer */
    e = btm_BlkLdInitWriteBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 5. initialize oid array buffer for duplicate key */
    e = btm_BlkLdInitLeaf(handle, btmBlkLdId, &blkLdEntry->btmBlkLdoidBuffer, blkLdEntry->btmBlkLdblkldInfo.isTmp);
    if (e < 0)  ERR(handle, e);

    /* 6. initialize overflow flag */
    blkLdEntry->btmBlkLdoverflow = FALSE;

    /* 7. initialize append flag */
    blkLdEntry->btmBlkLdisAppend = TRUE;


    /*
    ** IV. Prepare scan of B+ tree index
    */

    blkLdEntry->btmBlkLdscanInfo.startKval.len   = 0;
    blkLdEntry->btmBlkLdscanInfo.startCompOp     = SM_BOF;
    blkLdEntry->btmBlkLdscanInfo.stopKval.len    = 0;
    blkLdEntry->btmBlkLdscanInfo.stopCompOp      = SM_EOF;


    e = BtM_Fetch(handle, xactEntry, &blkLdEntry->btmBlkLdblkldInfo.iinfo,
                  fid, 
                  &blkLdEntry->btmBlkLdblkldInfo.kdesc,
                  &blkLdEntry->btmBlkLdscanInfo.startKval, blkLdEntry->btmBlkLdscanInfo.startCompOp,
                  &blkLdEntry->btmBlkLdscanInfo.stopKval, blkLdEntry->btmBlkLdscanInfo.stopCompOp,
                  &blkLdEntry->btmBlkLdscanInfo.currCursor, NULL, NULL);
    if (e < 0)  ERR(handle, e);



    /*
    ** V. Get sorted <key,OID> list from the given SortedStream and the B+ tree index leaf,
    **    Insert <key,OID> list into B+ tree
    */

    while (1) {

        /* 1. initialize numSortTuples */
        numSortTuples = MAXNUMOFTUPLES;

        /* 2. initialize sortTuples */
        for (i = 0; i < numSortTuples; i++) {
            sortTuples[i].len = 2*MAXKEYLEN;
            sortTuples[i].data = &tuples[i][0];
        }

        /* 3. get tuples from sort stream */
        e = Util_GetTuplesFromSortStream(handle, sortStreamId, &numSortTuples, sortTuples, &done);
        if (e < 0)  ERR(handle, e);

        if (done) break;

        /* 4. insert tuples into index */
        for (i = 0; i < numSortTuples; i++) {

            /* 4-1. separate key and oid from sortTuples[i].data */
            key.len = sortTuples[i].len - OBJECTID_SIZE;
            memcpy(key.val, &sortTuples[i].data[0], key.len);
            memcpy(&oid, &sortTuples[i].data[key.len], OBJECTID_SIZE);

            /* 4-2. compare B+ tree's <key,oid> with sortstream's <key,oid> and insert smaller one */
            do {
                /* reset again flag */
                againFlag = FALSE;

                /* IF : B+ tree index leaf's scan isn't Finished */
                if (blkLdEntry->btmBlkLdscanInfo.currCursor.flag != CURSOR_EOS) {

                    /* compare B+ tree's key with sortstream's key */
                    compResult = btm_KeyCompare(handle, &blkLdEntry->btmBlkLdblkldInfo.kdesc, &blkLdEntry->btmBlkLdscanInfo.currCursor.key, &key);

                    /* if B+ tree's key == sortstream's key */
                    if (compResult == EQUAL) {
                        /* compare B+ tree's oid with sortstream's oid */
                        compResult = btm_ObjectIdComp(handle, &blkLdEntry->btmBlkLdscanInfo.currCursor.oid, &oid);
                    }

                    /* CASE1 : B+ tree's key > sortstream's key */
                    if (compResult == GREAT) {
                        /* insert sortstream's <key,oid> into leaf node of index */
                        e = btm_BlkLdInsertLeaf(handle, xactEntry, btmBlkLdId, &key, &oid, logParam);
                        if (e < 0)  ERR(handle, e);

                        /* set again flag with FALSE */
                        againFlag = FALSE;
                    }

                    /* CASE2 : B+ tree's key < sortstream's key */
                    else {
                        /* insert B+ tree's <key,oid> into leaf node of index */
                        e = btm_BlkLdInsertLeaf(handle, xactEntry, btmBlkLdId, &blkLdEntry->btmBlkLdscanInfo.currCursor.key,
                                                &blkLdEntry->btmBlkLdscanInfo.currCursor.oid, logParam);
                        if (e < 0)  ERR(handle, e);

                        /* find next cursor in B+ tree index */
                        e = BtM_FetchNext(handle, xactEntry, &blkLdEntry->btmBlkLdblkldInfo.iinfo,
                                          fid, 
                                          &blkLdEntry->btmBlkLdblkldInfo.kdesc,
                                          &blkLdEntry->btmBlkLdscanInfo.stopKval, blkLdEntry->btmBlkLdscanInfo.stopCompOp,
                                          &blkLdEntry->btmBlkLdscanInfo.currCursor, &blkLdEntry->btmBlkLdscanInfo.nextCursor,
                                          NULL);
                        if (e < 0)  ERR(handle, e);
                        blkLdEntry->btmBlkLdscanInfo.currCursor = blkLdEntry->btmBlkLdscanInfo.nextCursor;

                        /* set again flag with TRUE */
                        againFlag = TRUE;
                    }
                }

                /* IF : B+ tree index leaf's scan is Finished */
                else {
                    /* insert sortstream's <key,oid> into leaf node of index */
                    e = btm_BlkLdInsertLeaf(handle, xactEntry, btmBlkLdId, &key, &oid, logParam);
                    if (e < 0)  ERR(handle, e);
                }

            } while (againFlag == TRUE);
        }
    }



    /*
    ** VI. insert remain <key,oid>s of B+ tree index if B+ tree index leaf's scan isn't Finished
    */
    while (blkLdEntry->btmBlkLdscanInfo.currCursor.flag != CURSOR_EOS) {

        /* 1. insert B+ tree's <key,oid> into leaf node of index */
        e = btm_BlkLdInsertLeaf(handle, xactEntry, btmBlkLdId, &blkLdEntry->btmBlkLdscanInfo.currCursor.key,
                                &blkLdEntry->btmBlkLdscanInfo.currCursor.oid, logParam);
        if (e < 0)  ERR(handle, e);

        /* 2. find next cursor in B+ tree index */
        e = BtM_FetchNext(handle, xactEntry, &blkLdEntry->btmBlkLdblkldInfo.iinfo,
                          fid, 
                          &blkLdEntry->btmBlkLdblkldInfo.kdesc,
                          &blkLdEntry->btmBlkLdscanInfo.stopKval, blkLdEntry->btmBlkLdscanInfo.stopCompOp,
                          &blkLdEntry->btmBlkLdscanInfo.currCursor, &blkLdEntry->btmBlkLdscanInfo.nextCursor,
                          NULL);
        if (e < 0)  ERR(handle, e);

        /* 3. set currCursor with nextCursor */
        blkLdEntry->btmBlkLdscanInfo.currCursor = blkLdEntry->btmBlkLdscanInfo.nextCursor;
    }



    /*
    ** VII. End of bulkload
    */

    /* 0. free all pages of B+ tree index */
    e = btm_BlkLdFreeIndex(handle, xactEntry, btmBlkLdId, logParam);
    if (e < 0)  ERR(handle, e);

    /* 1. case : end of duplicate key occurrence */
    if (blkLdEntry->btmBlkLdoidBuffer.hdr.nSlots != 0) {
        e = btm_BlkLdEndOidBuffer(handle, xactEntry, btmBlkLdId, logParam);
        if (e < 0)  ERR(handle, e);
    }

    /* 2. case : end of bulkload in overflow page */
    if (blkLdEntry->btmBlkLdoverflow == TRUE) {
        e = btm_BlkLdEndOverflow(handle, xactEntry, btmBlkLdId, logParam);
        if (e < 0)  ERR(handle, e);
    }

    /* 3. end of bulkload in leaf page */
    e = btm_BlkLdEndLeaf(handle, xactEntry, btmBlkLdId, logParam);
    if (e < 0)  ERR(handle, e);

    /* 4. end of bulkload in internal page */
    e = btm_BlkLdEndInternal(handle, xactEntry, btmBlkLdId, logParam);
    if (e < 0)  ERR(handle, e);

    /* 5. end of bulkload in write buffer */
    e = btm_BlkLdEndWriteBuffer(handle, xactEntry, btmBlkLdId, logParam);
    if (e < 0)  ERR(handle, e);



    /*
    ** VIII. Finalize buffer
    */

    /* 1. finalize internal node buffer */
    e = btm_BlkLdFinalInternalBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 2. finalize write buffer */
    e = btm_BlkLdFinalWriteBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);



    /*
    ** VII. Empty entry of SORT_STREAM_TABLE(handle)
    */

    blkLdEntry->isUsed = FALSE;



    return eNOERROR;

}   /* BtM_AppendBulkLoad() */
