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
 * Module: btm_BlkLdAlloc.c
 *
 * Description :
 *  Allocate a new contiguous Btree page in the segment
 *
 * Exports:
 *  Four btm_BlkLdAllocPage(PageID*)
 *  Four btm_BlkLdAllocPages()
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "BtM_Internal.h"
#include "OM_Internal.h"
#include "BL_BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* PageID  allocPrev; */ 

/*@===========================
 * btm_BlkLdAllocPage()
 *===========================*/
/*
 * Function: Four btm_BlkLdAllocPage(PageID*)
 *
 * Description: 
 *  Allocate a new Btree page in the segment
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *  parameter pid is filled with the PageID of the newly allocated page
 */
Four btm_BlkLdAllocPage (
    Four handle,
    Four                    btmBlkLdId,             /* IN    BtM bulkload ID */ 
    PageID                  *pid)                   /* OUT   page ID which will be alloced */
{
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    Four                    idx;                    /* a index */

    Four                    firstExtNo;             /* first extent number of the file */
    Four                    numOfAllocTrains;       /* number of allocated trains */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdFlushPage(handle, pid=%P)", pid));


    /* 0. set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* 1. Find index of bulkload buffer matching alloc-counter */
    idx = (blkLdEntry->btmBlkLdwBuffer.allocCount + blkLdEntry->btmBlkLdwBuffer.startPageIdx) % blkLdEntry->btmBlkLdwBuffer.bufSize;


    /* 2. If there are no allocated page, allocate contiguous pages */
    if (idx == 0) {

        /* 2-1. write old page array buffer to alloc new page id array */
        if (blkLdEntry->btmBlkLdwBuffer.allocedBufCount >= NUMOFWRITEBUFFER &&
            (blkLdEntry->btmBlkLdwBuffer.allocedBufCount - NUMOFWRITEBUFFER) % (NUMOFWRITEBUFFER - 1) == 0) {
            for (i = 0; i < NUMOFWRITEBUFFER-1; i++) {
                e = btm_BlkLdFlushPages(handle, btmBlkLdId, blkLdEntry->btmBlkLdwBuffer.flushedBufCount % NUMOFWRITEBUFFER);
                if (e < 0) ERR(handle, e);
            }
        }

        /* 2-2. alloc new page id array */
        e = btm_BlkLdAllocPages(handle, btmBlkLdId);
        if (e < 0) ERR(handle, e);
        blkLdEntry->btmBlkLdwBuffer.allocedBufCount++;
    }


    /* 3. Assign a newly allocated pageId to pid */
    idx = (blkLdEntry->btmBlkLdwBuffer.allocCount + blkLdEntry->btmBlkLdwBuffer.startPageIdx) % blkLdEntry->btmBlkLdwBuffer.bufSize;
    *pid = blkLdEntry->btmBlkLdwBuffer.allocPageIdArray[idx];


    /* 4. Increase bulkload buffer alloc-counter */
    blkLdEntry->btmBlkLdwBuffer.allocCount++;
    

    return eNOERROR;

}   /* btm_BlkLdAllocPage() */




/*@===========================
 * btm_BlkLdAllocPages()
 *===========================*/
/*
 * Function: Four btm_BlkLdAllocPages()
 *
 * Description: 
 *  Allocate pages in the segment by extent size.
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdAllocPages (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                      /* error number */
    Four                    bufIdx;                 /* a buffer index */
    Four                    firstExtNo;             /* first extent number of the file */
    Four                    numOfAllocTrains;       /* number of allocated trains */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdAllocPages(handle)"));


    /* 0. set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* 1. Get first extent number of this data file */
    e = RDsM_PageIdToExtNo(handle, (PageID *)&blkLdEntry->btmBlkLdblkldInfo.root, &firstExtNo);
    if (e < 0) ERR(handle, e);


    /* 2. Allocate trains by extent size */

    /* case 1 : there exist root page in extent */
    if (blkLdEntry->btmBlkLdwBuffer.allocCount == 0 && blkLdEntry->btmBlkLdisAppend == FALSE) {

        /* 1. number of pages which will be allocated is an extent size */
        numOfAllocTrains = blkLdEntry->btmBlkLdwBuffer.bufSize;

        /* 2. allocate contiguous pages in extent */ 
        e = RDsM_AllocContigTrainsInExt(handle, blkLdEntry->btmBlkLdblkldInfo.pFid.volNo, firstExtNo, &blkLdEntry->btmBlkLdblkldInfo.root, 
                                        blkLdEntry->btmBlkLdblkldInfo.eff, &numOfAllocTrains, PAGESIZE2, 
                                        blkLdEntry->btmBlkLdwBuffer.allocPageIdArray);
        if (e < 0) ERR(handle, e);

        /* 3. if number of allocated page is 0, ie root page is last page of extent */ 
        if (numOfAllocTrains == 0) {

            numOfAllocTrains = blkLdEntry->btmBlkLdwBuffer.bufSize;

            /* reallocate contiguous pages in extent */
            e = RDsM_AllocContigTrainsInExt(handle, blkLdEntry->btmBlkLdblkldInfo.pFid.volNo, firstExtNo, NULL, 
                                            blkLdEntry->btmBlkLdblkldInfo.eff, &numOfAllocTrains, PAGESIZE2, 
                                            blkLdEntry->btmBlkLdwBuffer.allocPageIdArray);
            if (e < 0) ERR(handle, e);
        }

        /* 4. calculate start page index in newly allocated page array */
        blkLdEntry->btmBlkLdwBuffer.startPageIdx = blkLdEntry->btmBlkLdwBuffer.bufSize - numOfAllocTrains;
        memmove(&blkLdEntry->btmBlkLdwBuffer.allocPageIdArray[blkLdEntry->btmBlkLdwBuffer.startPageIdx], 
                &blkLdEntry->btmBlkLdwBuffer.allocPageIdArray[0], 
                sizeof(PageID) * numOfAllocTrains);

        /* 5. copy allocPageIdArray to flushPageIdArray */
        bufIdx = ((blkLdEntry->btmBlkLdwBuffer.allocCount + blkLdEntry->btmBlkLdwBuffer.startPageIdx) / blkLdEntry->btmBlkLdwBuffer.bufSize) 
                 % NUMOFWRITEBUFFER;
        memcpy(&blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][blkLdEntry->btmBlkLdwBuffer.startPageIdx], 
               &blkLdEntry->btmBlkLdwBuffer.allocPageIdArray[blkLdEntry->btmBlkLdwBuffer.startPageIdx], 
               sizeof(PageID) * numOfAllocTrains);
        blkLdEntry->btmBlkLdwBuffer.flushCount[bufIdx] = numOfAllocTrains;
    } 

    /* case 2 : there is no root page in extent */
    else {

        /* 1. number of pages which will be allocated is an extent size */
        numOfAllocTrains = blkLdEntry->btmBlkLdwBuffer.bufSize;

        /* 2. allocate contiguous pages in extent */ 
        e = RDsM_AllocContigTrainsInExt(handle, blkLdEntry->btmBlkLdblkldInfo.pFid.volNo, firstExtNo, NULL, blkLdEntry->btmBlkLdblkldInfo.eff, 
                                        &numOfAllocTrains, PAGESIZE2, blkLdEntry->btmBlkLdwBuffer.allocPageIdArray);
        if (e < 0)  ERR(handle, e);

        /* 3. copy allocPageIdArray to flushPageIdArray */
        bufIdx = ((blkLdEntry->btmBlkLdwBuffer.allocCount + blkLdEntry->btmBlkLdwBuffer.startPageIdx) / blkLdEntry->btmBlkLdwBuffer.bufSize) 
                 % NUMOFWRITEBUFFER;
        memcpy(&blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][0], 
               &blkLdEntry->btmBlkLdwBuffer.allocPageIdArray[0], 
               sizeof(PageID) * numOfAllocTrains);
        blkLdEntry->btmBlkLdwBuffer.flushCount[bufIdx] = numOfAllocTrains;

    }


    return eNOERROR;

}   /* btm_BlkLdAllocPages() */



