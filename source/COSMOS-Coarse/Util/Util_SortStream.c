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
 * Module : Util_Sort.c
 *
 * Description :
 *  Sort a data file.
 *
 * Exports :
 *  Four Util_OpenSortStream(VolID, SortTupleDesc*)
 */

#include <stdlib.h> /* for malloc & free */
#include <string.h> /* for memcpy */
#include <assert.h> /* for assertion check */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h" 
#include "Util_Sort.h"
#include "Util.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*
 * Global variable
 */

Four Util_Sort_Init(Four handle)
{
    Four i;

    for (i = 0; i < SORT_STREAM_TABLE_SIZE; i++ ) {
        SORT_STREAM_TABLE(handle)[i].volId = NIL; 
    }

    return eNOERROR;

} /* Util_Sort_Init() */



/* ========================================
 *  Util_OpenSortStream()
 * =======================================*/

/*
 * Function Four Util_OpenSortStream(VolID, SortTupleDesc*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four Util_OpenSortStream(
    Four handle,
    VolID                 volId,                   /* IN  volume ID in which temporary files are allocated */
    SortTupleDesc*        sortTupleDesc)           /* IN  sort key */
{
    Four                  e;                       /* error number */
    Four                  sortStreamId;            /* ID of opened sort stream */
    Four                  firstExtNo;              /* first extent number of the new segment */
    PageID                nearPid;                 /* near page id */
    SortStreamTableEntry* entry;                   /* entry in which information about opened sort stream is saved */


#ifdef DBLOCK
    /*
     *  O. Acquire lock for temporary volume
     */

    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif


    /*
     *  I. Find empty entry from openSortStreamTable
     */

    for (sortStreamId = 0; sortStreamId < SORT_STREAM_TABLE_SIZE; sortStreamId++ ) {
        if (SORT_STREAM_TABLE(handle)[sortStreamId].volId == NIL) break; 
    }
    if (sortStreamId == SORT_STREAM_TABLE_SIZE) ERR(handle, eSORTSTREAMTABLEFULL_UTIL);

    /* set entry for fast access */
    entry = &SORT_STREAM_TABLE(handle)[sortStreamId]; 


    /*
     *  II. Set 'volId', 'sortTupleDesc' & initialize 'externalSortFlag'
     */

    /* set volId */
    entry->volId = volId;

    /* set sortTupleDesc */
    entry->sortTupleDesc = *sortTupleDesc;

    /* initialize externalSortFlag */
    entry->externalSortFlag = FALSE;


    /*
     *  III. Create temporary files and set 'tmpFile1' & 'tmpFile2'
     */

    /* create the segment for the temporary file 1 */
    e = RDsM_CreateSegment(handle, volId, &firstExtNo);
    if (e < 0) ERR(handle, e);

    e = RDsM_ExtNoToPageId(handle, volId, firstExtNo, &nearPid);
    if (e < 0) ERR(handle, e);

    /* allocate page for the temporary file 1 */
    e = RDsM_AllocTrains(handle, volId, firstExtNo, &nearPid, 100, 1, PAGESIZE2, &entry->tmpFile1);
    if (e < 0) ERR(handle, e);


    /* create the segment for the temporary file 2 */
    e = RDsM_CreateSegment(handle, volId, &firstExtNo);
    if (e < 0) ERR(handle, e);

    e = RDsM_ExtNoToPageId(handle, volId, firstExtNo, &nearPid);
    if (e < 0) ERR(handle, e);

    /* allocate page for the temporary file 2 */
    e = RDsM_AllocTrains(handle, volId, firstExtNo, &nearPid, 100, 1, PAGESIZE2, &entry->tmpFile2);
    if (e < 0) ERR(handle, e);


    /*
     *  IV. Allocate memory for In/Out Buffer and initialize them
     */

    /* allocate memory for 'Out buffer' */
    entry->sortOutBufferArray = (SlottedPageForSort *) malloc(PAGESIZE*SIZE_OF_SORT_OUT_BUFFER);
    if (entry->sortOutBufferArray == NULL) ERR(handle, eMEMORYALLOCERR);

    /* initialize 'Out buffer' */
    entry->sortOutBufferArrayIdx = 0;
    entry->sortOutBufferArray[0].header.nSlots = 0;
    entry->sortOutBufferArray[0].header.free = 0;

    /* allocate memory for 'In buffer' */
    entry->sortInBufferArray = (SlottedPageForSort *) malloc(PAGESIZE*SIZE_OF_SORT_IN_BUFFER);
    if (entry->sortInBufferArray == NULL) ERR(handle, eMEMORYALLOCERR);

    /* initialize 'In buffer' */
    entry->sortInBufferArrayIdx = 0;
    entry->sortInBufferArray[0].header.nSlots = 0;
    entry->sortInBufferArray[0].header.free = 0;


    /*
     *  V. Allocate memory for PnoArray them
     */

    e = Util_initVarArray(handle, &entry->srcPnoArray, sizeof(PageNo), SIZE_OF_PNO_ARRAY);
    if (e < 0) ERR(handle, e);
    e = Util_initVarArray(handle, &entry->dstPnoArray, sizeof(PageNo), SIZE_OF_PNO_ARRAY);
    if (e < 0) ERR(handle, e);


    /*
     *  VI. Allocate memory for Run array
     */

    e = Util_initVarArray(handle, &entry->srcRunArray, sizeof(Partition), SIZE_OF_RUN_ARRAY);
    if (e < 0) ERR(handle, e);
    e = Util_initVarArray(handle, &entry->dstRunArray, sizeof(Partition), SIZE_OF_RUN_ARRAY);
    if (e < 0) ERR(handle, e);


    /*
     *  VII. Initialize destination page number array and destination run array
     */

    /* When run is created, it is written to temp file 1 through the Out Buffer        */
    /* so you must initialize desitination page number array and destination run array */

    /* set 'dstPnoArray' */
    PNO_ARRAY(entry->dstPnoArray)[0] = entry->tmpFile1.pageNo;
    entry->numDstPno = 1;

    /* set 'dstRunArray' */
    RUN_ARRAY(entry->dstRunArray)[0].start = 0;
    RUN_ARRAY(entry->dstRunArray)[0].end = -1;
    entry->numDstRun = 1;

    /* Note!! in case of internal sort, numSrcPno isn't initialized. */
    /* But, it is used in Util_CloseSortStream(). So, you must initialize it at this poit!! */

    /* set 'srcPnoArray' */
    PNO_ARRAY(entry->srcPnoArray)[0] = entry->tmpFile2.pageNo;
    entry->numSrcPno = 1;


    /*
     *  VIII. Allocate memory for tuples array
     *        Note!! minimum size of tuples is MAX_NUM_RUN
     */

    /* initialize numTuples */
    entry->numTuples = 0;

    /* allocate memory */
    e = Util_initVarArray(handle, &entry->tuples, sizeof(void *), MAX_NUM_RUN);
    if (e < 0) ERR(handle, e);


    /*
     *  IX. Initialize total number of tuples in sort stream
     */
    entry->totalNumTuples = 0;
    entry->totalSizeOfTuples = 0;


    return sortStreamId;

} /* Util_OpenSortStream() */


/* ========================================
 *  Util_CloseSortStream()
 * =======================================*/

/*
 * Function Four Util_CloseSortStream(Four)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four Util_CloseSortStream(
    Four handle,
    Four                  sortStreamId)            /* IN */
{
    Four                  e;
    Four                  i;
    Four                  firstExtNo;
    PageID                pid;
    SortStreamTableEntry* entry;                   /* entry in which information about opened sort stream is saved */


    /*
     *  I. Set entry for fast access
     */
    entry = &SORT_STREAM_TABLE(handle)[sortStreamId]; 


    /*
     *  II. Deallocate temporary files
     */

    /* set pid's volNo */
    pid.volNo = entry->volId;

    /* disallocate pages of tmpFile1 & tmpFile2 */
    for (i = 0; i < entry->numSrcPno; i++ ) {
        pid.pageNo = PNO_ARRAY(entry->srcPnoArray)[i];
        e = RDsM_FreeTrain(handle, &pid, PAGESIZE2);
        if(e <0) ERR(handle, e);
    }

    /* drop segment of tmpFile1 & tmpFile2 */

    /* set pid to first page's ID of dropped file */
    pid = entry->tmpFile1;

    /* get first extent number of dropped file */
    e = RDsM_PageIdToExtNo(handle, &pid, &firstExtNo);
    if(e <0) ERR(handle, e);

    /* drop segment */
    e = RDsM_DropSegment(handle, entry->volId, firstExtNo);
    if(e <0) ERR(handle, e);

    /* set pid to first page's ID of dropped file */
    pid = entry->tmpFile2;

    /* get first extent number of dropped file */
    e = RDsM_PageIdToExtNo(handle, &pid, &firstExtNo);
    if(e <0) ERR(handle, e);

    /* drop segment */
    e = RDsM_DropSegment(handle, entry->volId, firstExtNo);
    if(e <0) ERR(handle, e);


    /*
     *  III. Fee memory for PnoArray
     */
    e = Util_finalVarArray(handle, &entry->srcPnoArray);
    if (e < 0) ERR(handle, e);
    e = Util_finalVarArray(handle, &entry->dstPnoArray);
    if (e < 0) ERR(handle, e);


    /*
     *  IV. Free memory for RunArray
     */
    e = Util_finalVarArray(handle, &entry->srcRunArray);
    if (e < 0) ERR(handle, e);
    e = Util_finalVarArray(handle, &entry->dstRunArray);
    if (e < 0) ERR(handle, e);


    /*
     *  V. Free memory for tuples
     */
    e = Util_finalVarArray(handle, &entry->tuples);
    if (e < 0) ERR(handle, e);


    /*
     *  VI. Free memory for In/Out Buffer
     */
    free(entry->sortOutBufferArray);
    free(entry->sortInBufferArray);


    /*
     *  VII. Empty entry of SORT_STREAM_TABLE
     */
    entry->volId = NIL;


    return(eNOERROR);

} /* Util_CloseSortStream() */


Four Util_PutTuplesIntoSortStream(
    Four handle,
    Four                  sortStreamId,    /* IN */
    Four                  numTuples,       /* IN */
    SortStreamTuple*      tuples)          /* IN */
{
    Four                  e;
    Four                  i;
    SortStreamTableEntry* entry;           /* entry in which information about opened sort stream is saved */


    /* set entry for fast access */
    entry = &SORT_STREAM_TABLE(handle)[sortStreamId]; 

    for (i = 0; i < numTuples; i++ ) {

        /* insert added object into 'In Buffer' */
        e = util_PutTupleIntoSortInBufferArray(handle, entry, tuples[i].len, tuples[i].data);
        if (e < 0) ERR(handle, e);

        /* update total number of tuples in sort stream */
        entry->totalNumTuples += 1;

        /* update total number of tuples in sort stream */
        entry->totalSizeOfTuples += tuples[i].len;

        /* if 'In Buffer' isn't full, i.e. insertion is succeeded */
        if (e == SUCCESS) {

            /* count the number of tuples in In Buffer */
            entry->numTuples++;
        }
        /* if 'In Buffer' is full */
        else {

            /* set externalSortFlag */
            entry->externalSortFlag = TRUE;

            e = util_CreateRun(handle, entry);
            if (e < 0) ERR(handle, e);

            /* Important!!! you must re-insert tuple which was failed */
            e = util_PutTupleIntoSortInBufferArray(handle, entry, tuples[i].len, tuples[i].data);
            if (e < 0) ERR(handle, e);

            /* in this case, failure is never occured */
            assert(e == SUCCESS);
            entry->numTuples = 1;

        } /* else */
    }


    return eNOERROR;

} /* Util_PutTuplesIntoSortStream() */



Four util_CreateRun(
    Four handle,
    SortStreamTableEntry* entry)       /* IN entry in which information about opened sort stream is saved */
{
    Four                  e;
    Four                  bound;
    Four                  i;       
    Two                   j;       
    Four                  k;       
    Two                   offset;  


    /* assertion check */
    assert(entry->numTuples > 0);

    /* if tuples array is full, tuples array is doubled */
    while (entry->numTuples >= entry->tuples.nEntries) {
        /* doubling it */
        e = Util_doublesizeVarArray(handle, &entry->tuples, sizeof(void *));
        if (e < 0) ERR(handle, e);
    }

    /* set bound */
    bound = (entry->sortInBufferArrayIdx >= SIZE_OF_SORT_IN_BUFFER) ? SIZE_OF_SORT_IN_BUFFER : entry->sortInBufferArrayIdx + 1;

    /* assign each pointer which points tuple in sortInBuffer */
    for (i = 0, k = 0; i < bound; i++ ) {
        for (j = 0; j < entry->sortInBufferArray[i].header.nSlots; j++ ) {
            /* Note!! EMPTY SLOT never exists */
            offset = entry->sortInBufferArray[i].slot[-j].offset;
            TUPLE_ARRAY(entry->tuples)[k++] = &(entry->sortInBufferArray[i].data[offset]);
        }
    }

    /* assertion check */
    assert(k == entry->numTuples);

    /* sort tuples */
    e = util_QuickSort(handle, &entry->sortTupleDesc, TUPLE_ARRAY(entry->tuples), entry->numTuples);
    if (e < 0) ERR(handle, e);

    /* if internal sort is used, return!! */
    if (entry->externalSortFlag == FALSE) return eNOERROR;

    /* insert tuples in the sorted order through the Out buffer */
    for (k = 0; k < entry->numTuples; k++ ) {
        e = util_PutTupleIntoSortOutBufferArray(handle, entry,
                                                ((SlottedPageForSortTuple *)TUPLE_ARRAY(entry->tuples)[k])->len,
                                                ((SlottedPageForSortTuple *)TUPLE_ARRAY(entry->tuples)[k])->data);
        if (e < 0) ERR(handle, e);
    }

    /* it is possible that some tuples remain in Out Buffer!! */
    e = util_FlushOutBuffer(handle, entry);
    if (e < 0) ERR(handle, e);

    /* empty 'In Buffer' */
    entry->sortInBufferArrayIdx = 0;
    entry->sortInBufferArray[0].header.nSlots = 0;
    entry->sortInBufferArray[0].header.free = 0;

    /* initialize run array entry for next run */
    if (entry->numDstRun >= entry->dstRunArray.nEntries) {
        /* if needed, allocate more space for run array */
        e = Util_doublesizeVarArray(handle, &entry->dstRunArray, sizeof(Partition));
        if (e < 0) ERR(handle, e);
    }
    RUN_ARRAY(entry->dstRunArray)[entry->numDstRun].start = RUN_ARRAY(entry->dstRunArray)[entry->numDstRun-1].end + 1;
    RUN_ARRAY(entry->dstRunArray)[entry->numDstRun].end = RUN_ARRAY(entry->dstRunArray)[entry->numDstRun-1].end;
    entry->numDstRun++;


    return eNOERROR;

} /* util_CreateRun() */




/* ========================================
 *  Util_SortingSortStream()
 * =======================================*/

/*
 * Function Util_SortingSortStream(Four)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 *
 */
Four Util_SortingSortStream(
    Four handle,
    Four                  sortStreamId)    /* IN */
{
    Four                  e;
    Four                  i;                    /* index variable */
    Four                  j, k;                 /* index variable */
    Four                  idx;
    Two                   offset;               
    Four                  mergeNumPass;
    Four                  numCreateRun;
    Four                  accumNumRun;
    Four                  numTmpPno;
    SortStreamTableEntry* entry;           /* entry in which information about opened sort stream is saved */
    PageID                pid;             
    Four                  index;                


    /*
     *  set entry for fast access
     */
    entry = &SORT_STREAM_TABLE(handle)[sortStreamId];


    /*
     *  Check tuples remains in 'In Buffer'
     *  Note!! it is possible that tuples remain in 'In Buffer'. you must sort these tuples, too
     */

    if (entry->sortInBufferArray[0].header.nSlots != 0) {
        e = util_CreateRun(handle, entry);
        if (e < 0) ERR(handle, e);
    }


    /*
     *  Set pnoArray & runArray
     */

    /* set 'srcPnoArray' - now, temp file 1 which contains created runs become source file */
    SWAP_PNO_ARRAY(entry->srcPnoArray, entry->dstPnoArray);
    entry->numSrcPno = entry->numDstPno;

    /* set 'srcRunArray' */
    /* Note!! 'numDstRun' is one bigger than real value because 'numDstRun' points last empty run */
    SWAP_RUN_ARRAY(entry->srcRunArray, entry->dstRunArray);
    entry->numSrcRun = entry->numDstRun - 1;

    /* set 'dstPnoArray' */
    PNO_ARRAY(entry->dstPnoArray)[0] = entry->tmpFile2.pageNo;
    entry->numDstPno = 1;

    /* set 'dstRunArray' */
    RUN_ARRAY(entry->dstRunArray)[0].start = 0;
    RUN_ARRAY(entry->dstRunArray)[0].end = -1;
    entry->numDstRun = 1;


    /*
     *  Calculate the number of merge pass
     */

    for (mergeNumPass = 0, index = SIZE_OF_SORT_IN_BUFFER; index < entry->numSrcPno; index *= MAX_NUM_RUN, mergeNumPass++ );

#ifndef NDEBUG
    printf("Number of Merge Pass = %ld\n",mergeNumPass);
#endif


    /*
     *  Merge
     */

    /* 'tuples' array will be used as loser tree's leaf node and contain pointers which points each tuple */
    /* 'loserTree' array will be used as loser tree's leaf node and contain index of tuples array         */

    /* for each merge pass */
    for (i = 0; i < mergeNumPass; i++ ) {

        /* calculate number of runs which will be created in destination file */
        /* Note!! this equation calculates ceiling of 'numSrcRun/MAX_NUM_RUN' */
        numCreateRun = (entry->numSrcRun-1)/MAX_NUM_RUN + 1;

#ifndef NDEBUG
        printf("%ld'th merge pass!! %ld -> %ld\n", i+1, entry->numSrcRun, numCreateRun);
#endif
        /* until all runs are created. 'accumNumRun' represents number of manipulated source run */
        for (j = 0, accumNumRun = 0; j < numCreateRun; j++, accumNumRun += MAX_NUM_RUN ) {

            /*
             *  Prepare i'th merge
             */

            /* calculate number of page which will be readed into In Buffer */
            entry->numRunInBuffer = (j == numCreateRun-1) ? entry->numSrcRun-accumNumRun : MAX_NUM_RUN;

            /* read runs into In Buffer & initialze leaf of loser tree */
            for (k = 0; k < entry->numRunInBuffer; k++ ) {

                /* read pages of k'th run */
                e = util_ReadIthRunIntoInBuffer(handle, entry, accumNumRun+k, k);
                if (e < 0) ERR(handle, e);

                /* insert first tuple of each run's first page into tree of loser's leaf */
                TUPLE_ARRAY(entry->tuples)[k] = entry->sortInBufferArray[k*READ_UNIT].data;

                /* set tuple counter */
                entry->sortInBufferArray[k*READ_UNIT].header.free = 1;

            } /* for 'k' */

            /* set 'loserTreeSize' */
            entry->loserTreeSize = entry->numRunInBuffer;

            /* create Tree of loser */
            if (entry->loserTreeSize == 1)
                entry->loserTree[0] = 0;
            else {
                e = util_CreateLoserTree(handle, &entry->sortTupleDesc, TUPLE_ARRAY(entry->tuples), entry->loserTree, entry->loserTreeSize);
                if (e < 0) ERR(handle, e);
            }


            /*
             *  if last merge pass, break!!
             */
            if (numCreateRun == 1) goto lastMergeStart;


            /*
             *  Start i'th merge
             */

            /* until all pages of runs in 'InBuffer' are merged */
            for (k = entry->loserTree[0]; entry->numRunInBuffer > 0; k = util_FixLoserTree(handle, &entry->sortTupleDesc, TUPLE_ARRAY(entry->tuples), entry->loserTree, entry->loserTreeSize, k) ) {

                /* write tree of loser's top tuple to destination file through the Out Buffer */
                e = util_PutTupleIntoSortOutBufferArray(handle, entry,
                                                        ((SlottedPageForSortTuple *)TUPLE_ARRAY(entry->tuples)[k])->len,
                                                        ((SlottedPageForSortTuple *)TUPLE_ARRAY(entry->tuples)[k])->data);
                if (e < 0) ERR(handle, e);

                /* set 'idx' for fast access */
                idx = k*READ_UNIT + entry->pageIdxInRun[k];

                /* when all tuples of page are exausted in k'th run */
                if (entry->sortInBufferArray[idx].header.free == entry->sortInBufferArray[idx].header.nSlots) {

                    /* move to next page in k'th run */
                    entry->pageIdxInRun[k]++;  idx++;

                    /* if more page does not exist in sortInBuffer, read next pages from disk */
                    if (entry->pageIdxInRun[k] >= READ_UNIT || entry->sortInBufferArray[idx].header.free == NIL) {

                        /* when all pages of run are exausted */
                        if (RUN_ARRAY(entry->srcRunArray)[accumNumRun+k].start == RUN_ARRAY(entry->srcRunArray)[accumNumRun+k].end+1) {
                            TUPLE_ARRAY(entry->tuples)[k] = NULL;
                            entry->numRunInBuffer--;
                            continue;
                        }

                        /* read pages of k'th run */
                        e = util_ReadIthRunIntoInBuffer(handle, entry, accumNumRun+k, k);
                        if (e < 0) ERR(handle, e);

                        /* update 'idx' */
                        idx = k*READ_UNIT;

                    } /* if */

                } /* if */

                /* suplement tuple into tree of loser */
                offset = entry->sortInBufferArray[idx].slot[-(entry->sortInBufferArray[idx].header.free++)].offset;
                TUPLE_ARRAY(entry->tuples)[k] = &(entry->sortInBufferArray[idx].data[offset]);

            } /* for 'k' */

            /* it is possible that pages remains in Out Buffer */
            e = util_FlushOutBuffer(handle, entry);
            if (e < 0) ERR(handle, e);

            /* initialize run array for next run */
            if (entry->numDstRun >= entry->dstRunArray.nEntries ) {
                /* if needed, allocate more space for run array */
                e = Util_doublesizeVarArray(handle, &entry->dstRunArray, sizeof(Partition));
                if (e < 0) ERR(handle, e);
            }
            RUN_ARRAY(entry->dstRunArray)[entry->numDstRun].start = RUN_ARRAY(entry->dstRunArray)[entry->numDstRun-1].end + 1;
            RUN_ARRAY(entry->dstRunArray)[entry->numDstRun].end = RUN_ARRAY(entry->dstRunArray)[entry->numDstRun-1].end;
            entry->numDstRun++;

        } /* for - one run is created in destination file */

        /* switch the source/destination Pno array */
        SWAP_PNO_ARRAY(entry->srcPnoArray, entry->dstPnoArray);
        numTmpPno = entry->numSrcPno; entry->numSrcPno = entry->numDstPno; entry->numDstPno = numTmpPno;

        /* switch source/destination run array & set number of source run */
        /* Note!! 'numDstRun' is one bigger than real value because 'numDstRun' points last empty run */
        SWAP_RUN_ARRAY(entry->srcRunArray, entry->dstRunArray);
        entry->numSrcRun = entry->numDstRun - 1;

        /* initialize destination Run array */
        RUN_ARRAY(entry->dstRunArray)[0].start = 0;
        RUN_ARRAY(entry->dstRunArray)[0].end = -1;
        entry->numDstRun = 1;

    } /* for - one pass */


    /*
     *  Prepare last merge pass
     */

lastMergeStart :

    /* assertion check */
    assert (entry->numRunInBuffer <= MAX_NUM_RUN);

    /* set 'winner' */
    /* Note!! in case of internal sort, winner is used as cursor */
    entry->winner = (entry->externalSortFlag) ? entry->loserTree[0] : 0;

    /* set pid's volNo */
    pid.volNo = entry->volId;

    /* drop temporary file */
    for (index = 0; index < entry->numDstPno; index++ ) {
        pid.pageNo = PNO_ARRAY(entry->dstPnoArray)[index];
        e = RDsM_FreeTrain(handle, &pid, PAGESIZE2); 
        if(e <0) ERR(handle, e);
    }
#ifndef NDEBUG
    printf("# of pages in sort stream = %ld\n", entry->numSrcPno);
#endif


    return eNOERROR;

} /* Util_SortingSortStream() */



Four Util_GetTuplesFromSortStream(
    Four handle,
    Four                     sortStreamId,       /* IN */
    Four*                    numTuples,          /* INOUT */
    SortStreamTuple*         tuples,             /* OUT */
    Boolean*                 eof)                /* OUT */
{
    Four                     e;
    Four                     i;
    Four                     count = 0;
    Four                     idx;
    Two                      offset;            
    Two                      tupleLen;        
    SlottedPageForSortTuple* tuplePtr;
    SortStreamTableEntry*    entry;           /* entry in which information about opened sort stream is saved */


    /*
     *  Set entry for fast access
     */
    entry = &SORT_STREAM_TABLE(handle)[sortStreamId]; 


    /*
     *  Case of internal sort
     *  Note!! in case of internal sort, winner is used as cursor
     */
    if (entry->externalSortFlag == FALSE) {

        while (count < *numTuples && entry->winner < entry->numTuples) {

            /* get 'tuplePtr' */
            tuplePtr = (SlottedPageForSortTuple *) TUPLE_ARRAY(entry->tuples)[entry->winner];

            /* get tupleLen */
            /* Note!! length field in tuplePtr isn't aligned */
            memcpy(&tupleLen, &tuplePtr->len, sizeof(Two));

            /* error check */
            if (tuples[count].len < tupleLen) ERR(handle, eNOTENOUGHSORTTUPLEBUF_UTIL);

            /* get tuple's len & data */
            tuples[count].len = tupleLen;
            memcpy(tuples[count].data, tuplePtr->data, tupleLen);

            /* update total number of tuples in sort stream */
            entry->totalNumTuples -= 1;

            /* update total size of tuples in sort stream */
            entry->totalSizeOfTuples -= tuples[count].len;

            /* update count & entry->winner */
            count ++; entry->winner ++;
        }

        /* set 'numTuples' */
        *numTuples = count;

        /* set 'eof' */
        if (count == 0) *eof = TRUE;
        else            *eof = FALSE;

        return eNOERROR;
    }


    /*
     *  From this line, in case of external sort
     */

    /*
     *  Get tuples
     */

    /* until all runs are merged */
    for ( ; entry->numRunInBuffer > 0 && count < *numTuples
          ; entry->winner = util_FixLoserTree(handle, &entry->sortTupleDesc, TUPLE_ARRAY(entry->tuples), entry->loserTree, entry->loserTreeSize, i) ) {

        /*
         *  Get new winner
         *  Note!! i'th run is winner
         */

        /* get winner */
        i = entry->winner;


        /*
         *  Get count's tuple
         */

        /* get 'tuplePtr' */
        tuplePtr = (SlottedPageForSortTuple *) TUPLE_ARRAY(entry->tuples)[i];

        /* get tupleLen */
        /* Note!! length field in tuplePtr isn't aligned */
        memcpy(&tupleLen, &tuplePtr->len, sizeof(Two));

        /* error check */
        if (tuples[count].len < tupleLen) ERR(handle, eNOTENOUGHSORTTUPLEBUF_UTIL);

        /* get tuple's len & data */
        tuples[count].len = tupleLen;
        memcpy(tuples[count].data, tuplePtr->data, tupleLen);

        /* update total number of tuples in sort stream */
        entry->totalNumTuples -= 1;
        entry->totalSizeOfTuples -= tuples[count].len;

        /* update count */
        count ++;


        /*
         *  Suplement tuple into tree of loser
         */

        /* set 'idx' for fast access */
        idx = i*READ_UNIT + entry->pageIdxInRun[i];

        /* when all tuples of page are exausted */
        if (entry->sortInBufferArray[idx].header.free == entry->sortInBufferArray[idx].header.nSlots) {

            /* move to next page in i'th run */
            entry->pageIdxInRun[i]++;  idx++;

            /* if more page does not exist in sortInBuffer, read next pages from disk */
            if (entry->pageIdxInRun[i] >= READ_UNIT || entry->sortInBufferArray[idx].header.free == NIL) {

                /* when all pages of run are exausted */
                if (RUN_ARRAY(entry->srcRunArray)[i].start == RUN_ARRAY(entry->srcRunArray)[i].end+1) {
                    TUPLE_ARRAY(entry->tuples)[i] = NULL;
                    entry->numRunInBuffer--;
                    continue;
                }

                /* read pages of i'th run */
                e = util_ReadIthRunIntoInBuffer(handle, entry, i, i);
                if (e < 0) ERR(handle, e);

                /* update 'idx' */
                idx = i*READ_UNIT;

            } /* if */

        } /* if */

        /* suplement tuple into tree of loser */
        offset = entry->sortInBufferArray[idx].slot[-(entry->sortInBufferArray[idx].header.free++)].offset;
        TUPLE_ARRAY(entry->tuples)[i] = &(entry->sortInBufferArray[idx].data[offset]);

    } /* for */




    /*
     *  Set return variable
     */

    /* set 'numTuples' */
    *numTuples = count;

    /* set 'eof' */
    if (count == 0) *eof = TRUE;
    else            *eof = FALSE;


    return eNOERROR;

} /* Util_GetTuplesFromSortStream() */


Four Util_GetNumTuplesInSortStream(
    Four 		  handle,
    Four                  sortStreamId)            /* IN */
{
    return                SORT_STREAM_TABLE(handle)[sortStreamId].totalNumTuples; 
}

Four Util_GetSizeOfSortStream(
    Four 		  handle,
    Four                  sortStreamId)            /* IN */
{
    return                SORT_STREAM_TABLE(handle)[sortStreamId].totalSizeOfTuples; 
}
