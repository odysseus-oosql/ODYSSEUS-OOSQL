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
 * Module : Util_Stream.c
 *
 * Description :
 *  Sort a data file.
 *
 * Exports :
 * Four Util_OpenStream(XactTableEntry_T*, VolID, LogParameter_T*);
 * Four Util_CloseStream(XactTableEntry_T*, Four, LogParameter_T*);
 * Four Util_PutTuplesIntoStream(XactTableEntry_T*, Four, Four, SortStreamTuple*, LogParameter_T*);
 * Four Util_ChangePhaseStream(XactTableEntry_T*, Four, LogParameter_T*);
 * Four Util_GetTuplesFromStream(Four, Four*, SortStreamTuple*, Boolean*);
 * Four Util_GetNumTuplesInStream(Four);
 * Four Util_GetSizeOfStream(Four);
 */

#include <stdlib.h> /* for malloc & free */
#include <string.h> /* for memcpy */
#include <assert.h> /* for assertion check */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM.h"
#include "BfM.h"
#include "Util_Sort.h"
#include "Util.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* internal function prototype */
Four util_WriteStream(Four, XactTableEntry_T*, SortStreamTableEntry*, LogParameter_T*);
Four util_ReadStream(Four, SortStreamTableEntry*);



/* ========================================
 *  Util_OpenStream()
 * =======================================*/

/*
 * Function Util_OpenStream(XactTableEntry_T*, VolID, LogParameter_T*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four Util_OpenStream(
    Four	          handle,
    XactTableEntry_T*     xactEntry,               /* IN  entry of transaction table */
    VolID                 volId,                   /* IN  volume ID in which temporary files are allocated */
    LogParameter_T*       logParam)                /* IN  log parameter */
{
    Four                  e;                       /* error number */
    Four                  streamId;                /* ID of opened sort stream */
    Four                  firstExtNo;              /* first extent number of the new segment */
    PageID                nearPid;                 /* near page id */
    SortStreamTableEntry* entry;                   /* entry in which information about opened sort stream is saved */
    Boolean               flag;                    /* temporary volume flag */


    /*
     *  I. Find empty entry from openSortStreamTable
     */

    for (streamId = 0; streamId < SORT_STREAM_TABLE_SIZE; streamId++ ) {
	if (SORT_STREAM_TABLE(handle)[streamId].volId == NIL) break;
    }
    if (streamId == SORT_STREAM_TABLE_SIZE) ERR(handle, eSORTSTREAMTABLEFULL_UTIL);

    /* This login don't check boundary condition. So this is incorrect.
     * for (streamId = 0; SORT_STREAM_TABLE(handle)[streamId].volId != NIL; streamId++ ) {
     *   if (streamId >= SORT_STREAM_TABLE_SIZE) ERR(handle, eSORTSTREAMTABLEFULL_UTIL);
     * }
     */
    
    /* set entry for fast access */
    entry = &SORT_STREAM_TABLE(handle)[streamId];


    /*
     *  II. Set 'volId' & initialize 'externalSortFlag'
     */

    /* set volId */
    entry->volId = volId;

    /* initialize externalSortFlag */
    entry->externalSortFlag = FALSE;


    /*
     *  III. Create temporary files and set 'tmpFile1'
     */

    /* allocate page for the temporary file 1 */
    e = RDsM_CreateSegment(handle, xactEntry, volId, &entry->pageSegmentIDOfTmpFile1, PAGESIZE2, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, volId, &entry->pageSegmentIDOfTmpFile1,
			 NULL, 1, PAGESIZE2, TRUE, &entry->tmpFile1, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_FlushTrain(handle, &entry->tmpFile1, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

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
     *  V. Allocate memory for PnoArray & RunArray
     */

    e = Util_initVarArray(handle, &entry->dstPnoArray, sizeof(PageNo), SIZE_OF_PNO_ARRAY);
    if (e < eNOERROR) ERR(handle, e);

    e = Util_initVarArray(handle, &entry->dstRunArray, sizeof(Partition), SIZE_OF_RUN_ARRAY);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  VI. Initialize destination page number array and destination run array
     */

    /* When stream is flushed, it is written to temp file 1 through the Out Buffer */
    /* so you must initialize desitination page number array */

    /* set 'dstPnoArray' */
    PNO_ARRAY(entry->dstPnoArray)[0] = entry->tmpFile1.pageNo;
    entry->numDstPno = 1;

    /* set 'dstRunArray' */
    RUN_ARRAY(entry->dstRunArray)[0].start = 0;
    RUN_ARRAY(entry->dstRunArray)[0].end = -1;
    entry->numDstRun = 1;


    /*
     *  VII. Allocate memory for tuples array
     *       Note!! minimum size of tuples is MAX_NUM_RUN
     */

    /* initialize numTuples */
    entry->numTuples = 0;

    /* allocate memory */
    e = Util_initVarArray(handle, &entry->tuples, sizeof(void *), MAX_NUM_RUN);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  VIII. Initialize total number of tuples in sort stream
     */
    entry->totalNumTuples = 0;
    entry->totalSizeOfTuples = 0;


    return streamId;

} /* Util_OpenStream() */


/* ========================================
 *  Util_CloseStream()
 * =======================================*/

/*
 * Function Four Util_CloseStream(XactTableEntry_T*, Four, LogParameter_T*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four Util_CloseStream(
    Four		  handle,
    XactTableEntry_T*     xactEntry,               /* IN  entry of transaction table */
    Four                  streamId,                /* IN */
    LogParameter_T*       logParam)                /* IN  log parameter */
{
    Four                  e;
    Four                  i;
    Four                  firstExtNo;
    PageID                pid;
    SortStreamTableEntry* entry;                   /* entry in which information about opened sort stream is saved */


    /*
     *  I. Set entry for fast access
     */
    entry = &SORT_STREAM_TABLE(handle)[streamId];


    /*
     *  II. Deallocate temporary files
     */

    /* set pid's volNo */
    pid.volNo = entry->volId;

    /* disallocate pages of tmpFile1 */
    for (i = 0; i < entry->numDstPno; i++ ) {
        pid.pageNo = PNO_ARRAY(entry->dstPnoArray)[i];

        e = BfM_RemoveTrain(handle, &pid, PAGE_BUF, FALSE);
        if (e < eNOERROR) ERR(handle, e);

    }

    e = RDsM_DropSegment(handle, xactEntry, entry->volId, &entry->pageSegmentIDOfTmpFile1, PAGESIZE2, TRUE, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *  III. Fee memory for PnoArray & RunArray
     */
    e = Util_finalVarArray(handle, &entry->dstPnoArray);
    if (e < eNOERROR) ERR(handle, e);
    e = Util_finalVarArray(handle, &entry->dstRunArray);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  IV. Free memory for tuples
     */
    e = Util_finalVarArray(handle, &entry->tuples);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  V. Free memory for In/Out Buffer
     */
    free(entry->sortOutBufferArray);
    free(entry->sortInBufferArray);


    /*
     *  VI. Empty entry of SORT_STREAM_TABLE(handle)
     */
    entry->volId = NIL;


    return(eNOERROR);

} /* Util_CloseStream() */


/* ========================================
 *  Util_PutTuplesIntoStream()
 * =======================================*/

/*
 * Function Four Util_PutTuplesIntoStream(XactTableEntry_T*, Four, Four, SortStreamTuple*, LogParameter_T*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four Util_PutTuplesIntoStream(
    Four		  handle,
    XactTableEntry_T*     xactEntry,       /* IN  entry of transaction table */
    Four                  streamId,        /* IN */
    Four                  numTuples,       /* IN */
    SortStreamTuple*      tuples,          /* IN */
    LogParameter_T*       logParam)        /* IN  log parameter */
{
    Four                  e;
    Four                  i;
    SortStreamTableEntry* entry;           /* entry in which information about opened sort stream is saved */


    /*
     *  Set entry for fast access
     */
    entry = &SORT_STREAM_TABLE(handle)[streamId];


    /* for each given tuple, insert into stream */
    for (i = 0; i < numTuples; i++ ) {

        /* update total number of tuples in sort stream */
        entry->totalNumTuples += 1;

        /* update total size of tuples in sort stream */
        entry->totalSizeOfTuples += tuples[i].len;

        /* insert added object into 'In Buffer' */
        e = util_PutTupleIntoSortInBufferArray(handle, entry, tuples[i].len, tuples[i].data);
        if (e < eNOERROR) ERR(handle, e);

        /* if 'In Buffer' isn't full, i.e. insertion is succeeded */
        if (e == SUCCESS) {

            /* count the number of tuples in In Buffer */
            entry->numTuples++;
        }
        /* if 'In Buffer' is full */
        else {

            /* set externalSortFlag */
            entry->externalSortFlag = TRUE;

            /* flush tuples in 'In Buffer' into disk */
            e = util_WriteStream(handle, xactEntry, entry, logParam);
            if (e < eNOERROR) ERR(handle, e);

            /* Important!!! you must re-insert tuple which was failed */
            e = util_PutTupleIntoSortInBufferArray(handle, entry, tuples[i].len, tuples[i].data);
            if (e < eNOERROR) ERR(handle, e);

            /* in this case, failure is never occured */
            assert(e == SUCCESS);
            entry->numTuples = 1;

        } /* else */
    }


    return eNOERROR;

} /* Util_PutTuplesIntoStream() */


/* ========================================
 *  Util_ChangePhaseStream()
 * =======================================*/

/*
 * Function Four Util_ChangePhaseStream(XactTableEntry_T*, Four, LogParameter_T*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four Util_ChangePhaseStream(
    Four		  handle,
    XactTableEntry_T*     xactEntry,       /* IN  entry of transaction table */
    Four                  streamId,        /* IN */
    LogParameter_T*       logParam)        /* IN  log parameter */
{
    Four                  e;
    SortStreamTableEntry* entry;           /* entry in which information about opened stream */


    /* set entry for fast access */
    entry = &SORT_STREAM_TABLE(handle)[streamId];

    /* check tuples remains in 'In Buffer' */
    /* Note!! it is possible that tuples remain in 'In Buffer'. you must flush them */
    if (entry->sortInBufferArray[0].header.nSlots != 0) {
        e = util_WriteStream(handle, xactEntry, entry, logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* read pages from disk and initialize tuples array */
    e = util_ReadStream(handle, entry);
    if (e < eNOERROR) ERR(handle, e);

    /* initialize 'winner' */
    entry->winner = 0;


    return eNOERROR;

} /* Util_ChangePhaseStream */


/* ========================================
 *  Util_GetTuplesIntoStream()
 * =======================================*/

/*
 * Function Four Util_GetTuplesFromStream(Four, Four*, SortStreamTuple*, Boolean*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four Util_GetTuplesFromStream(
    Four		     handle,
    Four                     streamId,           /* IN */
    Four*                    numTuples,          /* INOUT */
    SortStreamTuple*         tuples,             /* OUT */
    Boolean*                 eof)                /* OUT */
{
    Four                     e;
    Four                     i;
    Four                     count = 0;
    Four                     idx;
    Four                     offset;
    Two                      tupleLen;
    SlottedPageForSortTuple* tuplePtr;
    SortStreamTableEntry*    entry;              /* entry in which information about opened sort stream is saved */


    /*
     *  Set entry for fast access
     */
    entry = &SORT_STREAM_TABLE(handle)[streamId];


    /*
     *  Get tuples from stream
     */

    while (count < *numTuples) {

        /* if all tuples in 'In Buffer' is exhausted, read from disk */
        if (entry->winner == entry->numTuples) {

            /* read pages from disk and initialize tuples array */
            e = util_ReadStream(handle, entry);
            if (e < eNOERROR) ERR(handle, e);

            /* initialize 'winner' */
            entry->winner = 0;

            /* if there is no more tuples, break */
            if (entry->numTuples == 0) break;
        }

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


    /*
     *  Set return variable
     */

    /* set 'numTuples' */
    *numTuples = count;

    /* set 'eof' */
    if (count == 0) *eof = TRUE;
    else            *eof = FALSE;


    return eNOERROR;

} /* Util_GetTuplesFromStream() */


Four util_WriteStream(
    Four	          handle,
    XactTableEntry_T*     xactEntry,       /* IN  entry of transaction table */
    SortStreamTableEntry* entry,       /* IN entry in which information about opened sort stream is saved */
    LogParameter_T*       logParam)        /* IN  log parameter */
{
    Four                  e;
    Four                  i, j, k;
    Four                  bound;
    Four                  offset;
    Two                   tupleLen;
    SlottedPageForSortTuple* tuplePtr;


    /* assertion check */
    assert(entry->numTuples > 0);

    /* if internal sort is used, return!! */
    if (entry->externalSortFlag == FALSE) return eNOERROR;

    /* set bound */
    bound = (entry->sortInBufferArrayIdx >= SIZE_OF_SORT_IN_BUFFER) ? SIZE_OF_SORT_IN_BUFFER : entry->sortInBufferArrayIdx + 1;

    /* insert tuples into Out buffer */
    for (i = 0, k = 0; i < bound; i++ ) {
        for (j = 0; j < entry->sortInBufferArray[i].header.nSlots; j++, k++ ) {

            offset = entry->sortInBufferArray[i].slot[-j].offset;
            tuplePtr = (SlottedPageForSortTuple*) &(entry->sortInBufferArray[i].data[offset]);

            /* get tupleLen */
            /* Note!! length field in tuplePtr isn't aligned */
            memcpy(&tupleLen, &tuplePtr->len, sizeof(Two));

            e = util_PutTupleIntoSortOutBufferArray(handle, xactEntry, entry, tupleLen, tuplePtr->data, logParam);
            if (e < eNOERROR) ERR(handle, e);
        }
    }
    assert(k == entry->numTuples);

    /* it is possible that some tuples remain in Out Buffer!! */
    e = util_FlushOutBuffer(handle, xactEntry, entry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* empty 'In Buffer' */
    entry->sortInBufferArrayIdx = 0;
    entry->sortInBufferArray[0].header.nSlots = 0;
    entry->sortInBufferArray[0].header.free = 0;


    return eNOERROR;

} /* util_ReadStream() */


Four util_ReadStream(
    Four		  handle,
    SortStreamTableEntry* entry)       /* IN entry in which information about opened sort stream is saved */
{
    Four                  e;
    Four                  i, j;
    Four                  offset;
    Four                  numReadPages;
    Partition*            runPtr;
    PageID                readPageIdArray[SIZE_OF_SORT_IN_BUFFER];


    /*
     *  Read pages from tmpFile
     */

    /* if internal sort is used, skip it!! */
    if (entry->externalSortFlag == TRUE) {

        /* set 'runPtr' for fast access */
        runPtr = &(RUN_ARRAY(entry->dstRunArray)[0]);

        for (i = 0; i < SIZE_OF_SORT_IN_BUFFER && runPtr->start <= runPtr->end; i++, runPtr->start++) {

            /* set 'readPageIdArray' */
            readPageIdArray[i].volNo = entry->volId;
            readPageIdArray[i].pageNo = PNO_ARRAY(entry->dstPnoArray)[runPtr->start];
        }

        /* set numReadPages */
        numReadPages = i;

        e = util_ReadTrains(handle, readPageIdArray, (char *)&entry->sortInBufferArray[0], numReadPages, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);
    }
    else {

        /* set numReadPages */
        numReadPages = (entry->sortInBufferArrayIdx >= SIZE_OF_SORT_IN_BUFFER) ? SIZE_OF_SORT_IN_BUFFER : entry->sortInBufferArrayIdx + 1;

        /* reset sortInBufferArrayIdx */
        entry->sortInBufferArrayIdx = -1;
    }


    /*
     *  Set tuples array
     */
    for (i = 0, entry->numTuples = 0; i < numReadPages; i++ ) {
        for (j = 0; j < entry->sortInBufferArray[i].header.nSlots; j++ ) {

            /* tuples array if needed */
            if (entry->numTuples >= entry->tuples.nEntries) {
                e = Util_doublesizeVarArray(handle, &entry->tuples, sizeof(void *));
                if (e < eNOERROR) ERR(handle, e);
            }

            /* set tuples array */
            offset = entry->sortInBufferArray[i].slot[-j].offset;
            TUPLE_ARRAY(entry->tuples)[entry->numTuples++] = &(entry->sortInBufferArray[i].data[offset]);
        }
    }


    return eNOERROR;

} /* util_ReadStream() */


Four Util_GetNumTuplesInStream(
    Four	  handle,
    Four          streamId)            /* IN */
{
    return        SORT_STREAM_TABLE(handle)[streamId].totalNumTuples;
}


Four Util_GetSizeOfStream(
    Four	  handle,
    Four          streamId)            /* IN */
{
    return        SORT_STREAM_TABLE(handle)[streamId].totalSizeOfTuples;
}
