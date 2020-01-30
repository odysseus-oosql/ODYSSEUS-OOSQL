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
 * Module: btm_BlkLdFlush.c
 *
 * Description :
 *  Flush a buffer page into the given segment.
 *
 * Exports:
 *  Four btm_BlkLdFlushPage(BtreePage*)
 *  Four btm_BlkLdFlushPages(Four)
 */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM.h"
#include "LOG.h"
#include "BfM.h"
#include "BtM.h"
#include "BL_BtM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"




/*@===========================
 * btm_BlkLdFlushPage()
 *===========================*/
/*
 * Function: Four btm_BlkLdFlushPage(BtreePage*)
 *
 * Description:
 *  Flush a buffer page into the given segment.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdFlushPage (
    Four		    handle,
    XactTableEntry_T        *xactEntry,             /* IN  transaction table entry */
    Four                    btmBlkLdId,             /* IN  BtM bulkload ID */
    BtreePage               *page,                  /* IN  page which will be flushed */
    LogParameter_T          *logParam)              /* IN  log parameter */
{
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    Four                    j;                      /* a loop index */
    Four                    pageIdx;                /* a page index */
    Four                    bufIdx;                 /* a buffer index */
    Boolean                 found;
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */
    BtreePage               *rpage;                 /* a root page pointer */
    Buffer_ACC_CB           *aPage_BCBP;            /* buffer access control block holding catalog data */

    Lsn_T                   lsn;                    /* lsn of the newly written log record */
    Four                    logRecLen;              /* log record length */
    LOG_LogRecInfo_T        logRecInfo;             /* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,
            ("btm_BlkLdFlushPage(xactEntry=%P, btmBlkLdId=%ld, page=%P, logParam=%P)",
             xactEntry, btmBlkLdId, page, logParam));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId];


    /* Case 1 : given page is root of B+ tree index */
    if (EQUAL_PAGEID(page->any.hdr.pid, blkLdEntry->btmBlkLdblkldInfo.root) == TRUE) {

        e = BfM_getAndFixBuffer(handle, &blkLdEntry->btmBlkLdblkldInfo.root, M_FREE, &aPage_BCBP, PAGE_BUF);
        if (e < 0)  ERR(handle, e);

        rpage = (BtreePage*)aPage_BCBP->bufPagePtr;

        /* logging */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                LOG_ACTION_BTM_COPY_PAGE, LOG_REDO_UNDO,
                                rpage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                sizeof(BtreePage), page,
                                sizeof(BtreePage), rpage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR)   ERR(handle, e);

            memcpy(rpage, page, PAGESIZE);

            rpage->any.hdr.lsn = lsn;
            rpage->any.hdr.logRecLen = logRecLen;

        }
        else {
            memcpy(rpage, page, PAGESIZE);
            INCREASE_LSN_BY_ONE(rpage->any.hdr.lsn); 
        }


        /* set dirty flag */
        aPage_BCBP->dirtyFlag = 1;


        e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* Case 2 : given page is not root of B+ tree index */
    else {

        /* 1. Find current buffer index and current page index of bulkload write buffer */
        found = FALSE;
        for (i = 0; i < NUMOFWRITEBUFFER; i++) {
            for (j = 0; j < blkLdEntry->btmBlkLdwBuffer.bufSize; j++) {
                if (EQUAL_PAGEID(page->any.hdr.pid, blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[i][j]) == TRUE) {
                    bufIdx = i;     pageIdx = j;
                    found = TRUE;
                    break;
                }
            }
            if (found)  break;
        }

        /* 2. Flush given buffer page into bulkload buffer or disk directly */
        if (found == TRUE) {

            /* flush given buffer page into bulkload buffer */
            memcpy(&blkLdEntry->btmBlkLdwBuffer.bufArray[bufIdx][pageIdx], page, PAGESIZE);

            /* increase bulkload buffer flush-counter */
            blkLdEntry->btmBlkLdwBuffer.flushCount[bufIdx]--;
        }
        else {

            /* given one page is written to disk */
            e = BfM_FlushTrain(handle, &page->any.hdr.pid, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            e = RDsM_WriteTrain(handle, (char*)page, &page->any.hdr.pid, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

            e = BfM_RemoveTrain(handle, &page->any.hdr.pid, PAGE_BUF, FALSE);
            if (e < eNOERROR) ERR(handle, e);
        }
    }


    return eNOERROR;

}   /* btm_BlkLdFlushPage() */





/*@===========================
 * btm_BlkLdFlushPages()
 *===========================*/
/*
 * Function: Four btm_BlkLdFlushPages(Four)
 *
 * Description:
 *  Flush buffer pages into the given segment by extent size.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdFlushPages (
    Four		    handle,
    XactTableEntry_T        *xactEntry,             /* IN  transaction table entry */
    Four                    btmBlkLdId,             /* IN  BtM bulkload ID */
    Four                    bufIdx,                 /* IN  flush buffer index */
    LogParameter_T          *logParam)              /* IN  log parameter */
{
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    Four                    pageIdx;                /* a page index */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */



    TR_PRINT(handle, TR_BTM, TR1,
            ("btm_BlkLdFlushPages(btmBlkLdId=%ld, bufIdx=%ld)",
             btmBlkLdId, bufIdx));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId];


    /* case 1 : there exist root page in extent */
    if (blkLdEntry->btmBlkLdwBuffer.flushedBufCount == 0) {


        for (i = 0; i < blkLdEntry->btmBlkLdwBuffer.bufSize - blkLdEntry->btmBlkLdwBuffer.startPageIdx; i++) {

            e = BfM_FlushTrain(handle, &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][blkLdEntry->btmBlkLdwBuffer.startPageIdx+i], PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
        }

        e = RDsM_WriteTrains(handle,
			     (char*)&blkLdEntry->btmBlkLdwBuffer.bufArray[bufIdx][blkLdEntry->btmBlkLdwBuffer.startPageIdx],
                             &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][blkLdEntry->btmBlkLdwBuffer.startPageIdx],
                             blkLdEntry->btmBlkLdwBuffer.bufSize - blkLdEntry->btmBlkLdwBuffer.startPageIdx, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        for (i = 0; i < blkLdEntry->btmBlkLdwBuffer.bufSize - blkLdEntry->btmBlkLdwBuffer.startPageIdx; i++) {

            e = BfM_RemoveTrain(handle, &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][blkLdEntry->btmBlkLdwBuffer.startPageIdx+i], PAGE_BUF, FALSE);
            if (e < eNOERROR) ERR(handle, e);
        }

        for (i = 0; i < blkLdEntry->btmBlkLdwBuffer.bufSize; i++) {
            blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i].volNo   = NIL;
            blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i].pageNo  = NIL;
        }

        blkLdEntry->btmBlkLdwBuffer.flushedBufCount++;
    }

    /* case 2 : there is no root page in extent */
    else {
        for (i = 0; i < blkLdEntry->btmBlkLdwBuffer.bufSize; i++) {

            e = BfM_FlushTrain(handle, &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i], PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
        }

        e = RDsM_WriteTrains(handle, (char*)&blkLdEntry->btmBlkLdwBuffer.bufArray[bufIdx][0],
                             &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][0],
                             blkLdEntry->btmBlkLdwBuffer.bufSize, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        for (i = 0; i < blkLdEntry->btmBlkLdwBuffer.bufSize; i++) {

            e = BfM_RemoveTrain(handle, &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i], PAGE_BUF, FALSE);
            if (e < eNOERROR) ERR(handle, e);
        }

        for (i = 0; i < blkLdEntry->btmBlkLdwBuffer.bufSize; i++) {
            blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i].volNo   = NIL;
            blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i].pageNo  = NIL;
        }

        blkLdEntry->btmBlkLdwBuffer.flushedBufCount++;
    }


    return eNOERROR;

}   /* btm_BlkLdFlushPages() */

