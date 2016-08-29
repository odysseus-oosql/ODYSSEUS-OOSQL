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
 * Module : util_Buffer.c
 *
 * Description :
 *  Manage a buffer for sorting
 *
 * Exports :
 *  Four util_PutTupleIntoSortInBufferArray(SortStreamTableEntry*, Two, char*)
 *  Four util_PutTupleIntoSortOutBufferArray(SortStreamTableEntry*, Two, char*)
 */


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


/*=============================================
 * util_PutTupleIntoSortInBufferArray()
 *============================================*/

/*
 * Function : Four util_PutTupleIntoSortInBufferArray(SortStreamTableEntry*, Two, char*)
 *
 * Description :
 *  Insert tuple into 'sortInBufferArray'.
 *
 * Return Value :
 *  SUCCESS, if insert succeeds
 *  FAIL, if insert fails
 *
 * Side effets :
 *  None.
 */
Four util_PutTupleIntoSortInBufferArray(
    Four handle,
    SortStreamTableEntry	*entry,              /* IN pointer to entry of sortStreamTable */
    Two                   	len,
    char			*data)               /* IN */
{
    Four                  	e;                  /* error code */
    Four                  	neededSpace;        /* space needed to put new tuple [+ slot] */
    Two                   	tupleOffset;        /* offset of added tuple */
    Two                   	slotIdx;            /* slot index */
    SlottedPageForSortTuple 	*tuplePtr;


    /*
     *  Check given tuple can be inserted
     */

    /* caculate needed space for added tuple */
    neededSpace = ALIGNED_LENGTH(sizeof(Two)+len) + sizeof(SlottedPageForSortSlot);

    /* If current 'sortInBuffer' is full, switch to next 'sortInBuffer' */
    /* Note!! free space in 'sortInBuffer' is always contiguous */
    if(SP_FOR_SORT_FREE(&entry->sortInBufferArray[entry->sortInBufferArrayIdx]) < neededSpace) {

        entry->sortInBufferArrayIdx++;

        if(entry->sortInBufferArrayIdx >= SIZE_OF_SORT_IN_BUFFER) return(FAIL);

        /* initialize next 'sortInBuffer' which will be used next time */
        entry->sortInBufferArray[entry->sortInBufferArrayIdx].header.nSlots = 0;
        entry->sortInBufferArray[entry->sortInBufferArrayIdx].header.free = 0;
    }


    /*
     *  Insert tuple
     */

    /* where to put tuple */
    tupleOffset = entry->sortInBufferArray[entry->sortInBufferArrayIdx].header.free;

    /* set 'tuplePtr' */
    tuplePtr = (SlottedPageForSortTuple *) &(entry->sortInBufferArray[entry->sortInBufferArrayIdx].data[tupleOffset]);

    /* copy the length into the buffer */
    /* Note!! length field in tuplePtr isn't aligned. so, normal assignment can't be used */
    memcpy(&tuplePtr->len, &len, sizeof(Two));

    /* copy the tuple into the buffer */
    memcpy(tuplePtr->data, data, len);

    /* update slot info  */
    slotIdx = entry->sortInBufferArray[entry->sortInBufferArrayIdx].header.nSlots++;
    entry->sortInBufferArray[entry->sortInBufferArrayIdx].slot[-slotIdx].offset = tupleOffset;

    /* update free pointer */
    entry->sortInBufferArray[entry->sortInBufferArrayIdx].header.free += ALIGNED_LENGTH(sizeof(Two)+len);


    return(SUCCESS);

} /* util_PutTupleIntoSortInBufferArray() */




/*=============================================
 * util_PutTupleIntoSortOutBufferArray()
 *============================================*/

/*
 * Function : util_PutTupleIntoSortOutBufferArray(SortStreamTableEntry*, Two, char*)
 *
 * Description :
 *  Insert object into 'Out buffer'.
 *
 * Return Value :
 *  Error Code
 *
 * Side effets :
 *  None.
 */
Four util_PutTupleIntoSortOutBufferArray(
    Four handle,
    SortStreamTableEntry	*entry,              /* IN pointer to entry of sortStreamTable */
    Two                   	len,
    char			*data)               /* IN */
{
    Four                  	e;             /* error code */
    Four                  	neededSpace;   /* space needed to put new object [+ header + slot] */
    Two                   	tupleOffset;
    Two                   	slotIdx;       /* slot index */
    SlottedPageForSortTuple	*tuplePtr;


    /*
     *  Check given tuple can be inserted
     */

    /* caculate needed space for added tuple */
    neededSpace = ALIGNED_LENGTH(sizeof(Two)+len) + sizeof(SlottedPageForSortSlot);

    /* Note!! free space of 'sortOutBuffer' is always contiguous!! */
    if (SP_FOR_SORT_FREE(&entry->sortOutBufferArray[entry->sortOutBufferArrayIdx]) < neededSpace) {

        /* move to next buffer */
        entry->sortOutBufferArrayIdx++;

        /* if 'sortOutBufferArray' is full, write contents to disk */
        if (entry->sortOutBufferArrayIdx >= SIZE_OF_SORT_OUT_BUFFER) {
            e = util_FlushOutBuffer(handle, entry);
            if (e < 0) ERR(handle, e);
        }
        /* initialize next buffer */
        else {
            entry->sortOutBufferArray[entry->sortOutBufferArrayIdx].header.nSlots = 0;
            entry->sortOutBufferArray[entry->sortOutBufferArrayIdx].header.free = 0;
        }
    }


    /*
     *  Insert tuple
     */

    /* set 'tupleOffset' */
    tupleOffset = entry->sortOutBufferArray[entry->sortOutBufferArrayIdx].header.free;

    /* set 'tuplePtr' */
    tuplePtr = (SlottedPageForSortTuple *) &(entry->sortOutBufferArray[entry->sortOutBufferArrayIdx].data[tupleOffset]);

    /* copy the length into the buffer */
    /* Note!! length field in tuplePtr isn't aligned. so, normal assignment can't be used */
    memcpy(&tuplePtr->len, &len, sizeof(Two));

    /* copy the tuple into the buffer */
    memcpy(tuplePtr->data, data, len);

    /* update slot info */
    slotIdx = entry->sortOutBufferArray[entry->sortOutBufferArrayIdx].header.nSlots++;
    entry->sortOutBufferArray[entry->sortOutBufferArrayIdx].slot[-slotIdx].offset = tupleOffset;

    /* update free pointer */
    entry->sortOutBufferArray[entry->sortOutBufferArrayIdx].header.free += ALIGNED_LENGTH(sizeof(Two)+len);


    return(eNOERROR);

} /* util_PutTupleIntoSortOutBufferArray() */



/*=============================================
 * util_FlushOutBuffer()
 *============================================*/

/*
 * Function : util_FlushOutBuffer(SortStreamTableEntry*)
 *
 * Description :
 *  Forced write 'Out Buffer' to disk
 *
 * Return Value :
 *  Error Code
 *
 * Side effets :
 *  Disk pages' content are altered to 'Out Buffer'
 */
Four util_FlushOutBuffer(
    Four handle,
    SortStreamTableEntry* entry)              /* IN pointer to entry of sortStreamTable */
{
    Four                  e;                  /* error code */
    Four                  i;                  /* index variable */
    Four                  bound;
    Four                  numFlushPage;
    Four                  pnoIdx;             /* index of 'Pno Array' */
    Four                  firstExtNo;         /* index of 'Pno Array' */
    PageID                firstPid;           /* ID of first page */
    PageID                pid;                /* page id */
    PageID                curRunLastPID;      /* page id of current run's last page */
    PageID                flushPageIdArray[SIZE_OF_SORT_OUT_BUFFER];


    /*
     *  Get flushPageIdArray & bufPtrArray
     */

    /* set bound */
    bound = (entry->sortOutBufferArrayIdx >= SIZE_OF_SORT_OUT_BUFFER) ? SIZE_OF_SORT_OUT_BUFFER : entry->sortOutBufferArrayIdx+1;

    /* for each page in 'Out Buffer' */
    for (i = 0, numFlushPage = 0; i < bound; i++ ) {

        /* if empty, you don't need to flush */
        if(entry->sortOutBufferArray[i].header.nSlots == 0) continue;

        /* flush always occur in dstRun */
        pnoIdx = ++RUN_ARRAY(entry->dstRunArray)[entry->numDstRun-1].end;

        /* if pages of file are exausted */
        if(pnoIdx >= entry->numDstPno) {

            /* get first extent number */
            MAKE_PAGEID(firstPid, entry->volId, PNO_ARRAY(entry->dstPnoArray)[0]);
            e = RDsM_PageIdToExtNo(handle, &firstPid, &firstExtNo);
            if (e < 0) ERR(handle, e);

            /* allocate new page */
            MAKE_PAGEID(curRunLastPID, entry->volId, PNO_ARRAY(entry->dstPnoArray)[pnoIdx-1]);
            e = RDsM_AllocTrains(handle, entry->volId, firstExtNo, &curRunLastPID, 100, 1, PAGESIZE2, &pid);
            if (e < 0) ERR(handle, e);

            /* if Pno array is full, Pno array is doubled */
            if(entry->numDstPno >= entry->dstPnoArray.nEntries) {
                /* doubling it */
                e = Util_doublesizeVarArray(handle, &entry->dstPnoArray, sizeof(PageNo));
                if (e < 0) ERR(handle, e);
            }

            /* inset pid value */
            PNO_ARRAY(entry->dstPnoArray)[pnoIdx] = pid.pageNo;

            /* update 'numDstPno' value */
            entry->numDstPno++;
        }
        else {
            MAKE_PAGEID(pid, entry->volId, PNO_ARRAY(entry->dstPnoArray)[pnoIdx]);
        }

        /* set 'flushPageIdArray' */
        flushPageIdArray[numFlushPage] = pid;

        /* update 'numFlushPage' */
        numFlushPage++;

    } /* for 'i' */


    /*
     *  If 'Out buffer' is empty, return
     */
    if (numFlushPage == 0) return(eNOERROR);


    /*
     *  Now, write 'Out Buffer' to disk
     */
    e = util_WriteTrains(handle, (char *) &entry->sortOutBufferArray[0], flushPageIdArray, numFlushPage, PAGESIZE2);
    if (e < 0) ERR(handle, e);


    /*
     *  Empty 'Out Buffer
     */
    entry->sortOutBufferArrayIdx = 0;
    entry->sortOutBufferArray[0].header.nSlots = 0;
    entry->sortOutBufferArray[0].header.free = 0;


    return(eNOERROR);

} /* util_FlushOutBuffer() */


Four util_ReadIthRunIntoInBuffer(
    Four handle,
    SortStreamTableEntry* entry,              /* IN pointer to entry of sortStreamTable */
    Four                  ithRunInSrcFile,
    Four                  ithRunInBuffer)
{
    Four                  e;
    Four                  i;
    Four                  numReadPage;
    PageID                pid;
    PageID                readPageIdArray[READ_UNIT];


    /* set pid's volNo */
    pid.volNo = entry->volId;

    /* read ithRunInSrcFile into ithRunInBuffer */
    for (i = 0; i < READ_UNIT; i++ ) {

        /* if source's run is exhausted */
        /* Note!! use slotted page's 'free' field as tuple counter */
        if (RUN_ARRAY(entry->srcRunArray)[ithRunInSrcFile].start == RUN_ARRAY(entry->srcRunArray)[ithRunInSrcFile].end+1) {

            /* assertion check */
            assert (i > 0);

            entry->sortInBufferArray[ithRunInBuffer*READ_UNIT+i].header.free = NIL;
            break;
        }

        /* get PID of first READ_UNIT pages of remaining source's run */
        pid.pageNo = PNO_ARRAY(entry->srcPnoArray)[RUN_ARRAY(entry->srcRunArray)[ithRunInSrcFile].start++];

        /* set 'readPageIdArray' & 'readPageBufPtrArray' */
        readPageIdArray[i] = pid;

    } /* for 'i' */

    /* set 'numReadPage' */
    numReadPage = i;

    /* read pages into buffer */
    e = util_ReadTrains(handle, readPageIdArray, (char *) &entry->sortInBufferArray[ithRunInBuffer*READ_UNIT], numReadPage, PAGESIZE2);
    if (e < 0) ERR(handle, e);

    /* initialize slotted page's 'free' field as tuple counter */
    /* Note!! this operation must be executed after read trains from disk */
    for (i = 0; i < numReadPage; i++ ) entry->sortInBufferArray[ithRunInBuffer*READ_UNIT+i].header.free = 0;

    /* set 'pageIdxInRun' */
    /* Note!! each entry of pageIdxInRun[] have value between 0 and READ_UNIT-1 */
    entry->pageIdxInRun[ithRunInBuffer] = 0;


    return eNOERROR;

} /* util_ReadIthRunIntoInBuffer() */
