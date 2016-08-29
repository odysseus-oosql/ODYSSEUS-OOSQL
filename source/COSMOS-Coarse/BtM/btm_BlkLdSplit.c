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
 * Module: btm_BlkLdSplit.c
 *
 * Description :
 *  Split current page buffer, i.e previous page buffer is flushed and page buffer counter is incremented.
 *
 * Exports:
 *  Four btm_BlkLdSplitInternal() 
 *  Four btm_BlkLdSplitLeaf()
 *  Four btm_BlkLdSplitOverflow()
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "OM_Internal.h"
#include "BL_BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"




/*@===========================
 * btm_BlkLdSplitInternal()
 *===========================*/
/*
 * Function: Four btm_BlkLdSplitInternal(BlkLdBtreeInfo*, BlkLdBtreeInternals*, BlkLdBtreeWriteBuffer*, Four) 
 *
 * Description: 
 *  Split current internal page buffer for given level because that page is filled up. 
 *  If current internal page buffer is first page for given level, just increment page counter.
 *  Otherwise previous internal page buffer is alloced & flushed and increment page counter. 
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdSplitInternal (
    Four handle,
    Four                    btmBlkLdId,             /* IN    BtM bulkload ID */ 
    Four                    level)                  /* IN    internal node's level */
{
    Four                    e;                      /* error number */

    BtreeInternal           *ipage;                 /* current internal window buffer pointer */
    BtreeInternal           *ppage;                 /* previous internal window buffer pointer */
    KeyValue                *ikey;                  /* first pointer's key value in internal page */

    PageID                  pid;                    /* a page id */
    InternalItem            iitem;                  /* internal item which will be inserted */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1,
            ("btm_BlkLdSplitInternal(handle, level=%ld)", level));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* 
    **  CASE I : internal page buffer for given level is the first page buffer
    */
    if (IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount == 0) {

        /* only increment internal buffer page counter */
        IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount++;
    }

    /* 
    **  CASE II : internal page buffer for given level is NOT the first page buffer
    */ 
    else {

        /* 1. find previous internal buffer page matching given level */
        ppage = &(IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iPage[(IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount - 1) % NUMOFWINDOWPAGE]);

        /* 2. find first pointer's key value of current buffer page matching given level */
        ikey = &((IARRAY(blkLdEntry->btmBlkLdiBuffers)[level]).iKey[(IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount - 1) % NUMOFWINDOWPAGE]);

        /* 3. alloc train */
        e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
        if (e < 0)  ERR(handle, e);

        /* 4. set buffer page's pid */
        ppage->hdr.pid = pid;

        /* 5. make internal item to be inserted */
        iitem.spid = ppage->hdr.pid.pageNo;
        iitem.klen = ikey->len;
        memcpy(&(iitem.kval[0]), &(ikey->val[0]), ikey->len);

        /* 6. insert item into parent page */
        if (level+1 < blkLdEntry->btmBlkLdiBuffers.nLevel) {

            e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
            if (e < 0)  ERR(handle, e);
        }
        else {

            /* if B+ tree depth exceed initial internal buffer level, double internal buffer level */ 
            e = btm_BlkLdDoubleSizeInternal(handle, btmBlkLdId);
            if (e < 0)  ERR(handle, e);

            e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
            if (e < 0)  ERR(handle, e);
        }

        /* 7. flush previous page */
        e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
        if (e < 0)  ERR(handle, e);

        /* 8. initialize previous page */
        e = btm_BlkLdInitInternal(handle, btmBlkLdId, ppage, blkLdEntry->btmBlkLdblkldInfo.isTmp);
        if (e < 0)  ERR(handle, e);

        /* 9. increment internal buffer page counter */
        IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount++;
    }


    return eNOERROR;

}   /* btm_BlkLdSplitInternal() */



/*@===========================
 * btm_BlkLdSplitLeaf()
 *===========================*/
/*
 * Function: Four btm_BlkLdSplitLeaf(BlkLdBtreeInfo*, BlkLdBtreeInternals*, BlkLdBtreeLeaf*, BlkLdBtreeWriteBuffer*) 
 *
 * Description: 
 *  Split current leaf page buffer because of that page's being filled up.
 *  Alloc page, internal item is put into internal buffer, set link between previous page and current page,
 *  flush previous page, initialize previous page buffer and increment leaf buffer counter.
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdSplitLeaf (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                      /* error number */

    BtreeLeaf               *lpage;                 /* current leaf window buffer pointer */
    BtreeLeaf               *ppage;                 /* previous leaf window buffer pointer */

    Two                     entryOffset;            /* starting offset of the entry */
    btm_LeafEntry           *entry;                 /* a leaf entry */
    InternalItem            item;                   /* internal item which will be inserted */

    PageID                  pid;                    /* a page id */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdSplitLeaf(handle)"));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* 
    **  CASE I : leaf page buffer for given level is the first page buffer
    */
    if (blkLdEntry->btmBlkLdlBuffer.lCount == 0) {

        /* only increment leaf buffer page counter */
        blkLdEntry->btmBlkLdlBuffer.lCount++;
    }

    /* 
    **  CASE II : leaf page buffer for given level is NOT the first page buffer
    */ 
    else {

        /* 1. get current leaf window buffer page */
        lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount%NUMOFWINDOWPAGE]);

        /* CASE : when leaf buffer count is 1, alloc prev page & current page and flush prev page */
        if (blkLdEntry->btmBlkLdlBuffer.lCount == 1) {

            /* 1. get previous leaf window buffer page */
            ppage = &(blkLdEntry->btmBlkLdlBuffer.lPage[(blkLdEntry->btmBlkLdlBuffer.lCount - 1) % NUMOFWINDOWPAGE]);

            /* 2. alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);

            /* 3. set buffer page's pid */
            ppage->hdr.pid = pid;

            /* 4. make internal item to be inserted */
            entryOffset = ppage->slot[0];
            entry = (btm_LeafEntry*)&(ppage->data[entryOffset]);

            item.spid = ppage->hdr.pid.pageNo;
            item.klen = entry->klen;
            memcpy(&(item.kval[0]), &(entry->kval[0]), entry->klen);

            /* 5. insert item into parent page */
            e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &item, 0);
            if (e < 0)  ERR(handle, e);
        }

        /* 2. alloc train */
        e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
        if (e < 0)  ERR(handle, e);

        /* 3. set buffer page's pid */
        lpage->hdr.pid = pid;

        /* 4. make internal item to be inserted */
        entryOffset = lpage->slot[0];
        entry = (btm_LeafEntry*)&(lpage->data[entryOffset]);

        item.spid = lpage->hdr.pid.pageNo;
        item.klen = entry->klen;
        memcpy(&(item.kval[0]), &(entry->kval[0]), entry->klen);

        /* 5. insert item into parent page */
        e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &item, 0);
        if (e < 0)  ERR(handle, e);

        /* 6-1. set links */
        ppage = &(blkLdEntry->btmBlkLdlBuffer.lPage[(blkLdEntry->btmBlkLdlBuffer.lCount - 1) % NUMOFWINDOWPAGE]);
        ppage->hdr.nextPage = lpage->hdr.pid.pageNo;
        lpage->hdr.prevPage = ppage->hdr.pid.pageNo;

        /* 6-2. flush previous page */
        e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
        if (e < 0)  ERR(handle, e);

        /* 6-3. initialize previous page */
        e = btm_BlkLdInitLeaf(handle, btmBlkLdId, ppage, blkLdEntry->btmBlkLdblkldInfo.isTmp);
        if (e < 0)  ERR(handle, e);

        /* 7. increment leaf buffer page counter */
        blkLdEntry->btmBlkLdlBuffer.lCount++;
    }



    return eNOERROR;

}   /* btm_BlkLdSplitLeaf() */




/*@===========================
 * btm_BlkLdSplitOverflow()
 *===========================*/
/*
 * Function: Four btm_BlkLdSplitOverflow()
 *
 * Description: 
 *  Split current overflow page buffer because of that page's being filled up.
 *  Alloc page, set link between previous page and current page, flush previous page, 
 *  initialize previous page buffer and increment overflow buffer counter.
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdSplitOverflow (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                      /* error number */

    BtreeOverflow           *opage;                 /* current overflow window buffer pointer */
    BtreeOverflow           *ppage;                 /* previous overflow window buffer pointer */
    BtreeLeaf               *lpage;                 /* current leaf window buffer pointer */

    Two                     idx;                    /* slot index */
    Two                     entryLen;               /* length of the entry */
    Two                     alignedKlen;            /* aligned size of the key length */
    Two                     entryOffset;            /* starting offset of the entry */
    btm_LeafEntry           *entry;                 /* an internal entry of an internal page */

    PageID                  pid;                    /* a page id */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdSplitOverflow(handle)"));


    /* 0. set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */

    /* 1. get current overflow window buffer page */
    opage = &(blkLdEntry->btmBlkLdoBuffer.oPage[blkLdEntry->btmBlkLdoBuffer.oCount % NUMOFWINDOWPAGE]);

    /* 2. alloc train */
    e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
    if (e < 0)  ERR(handle, e);

    /* 3. set buffer page's pid */
    opage->hdr.pid = pid;


    /* CASE 4-1 : opage is first overflow page */
    if (blkLdEntry->btmBlkLdoBuffer.oCount == 0) {

        /* get current leaf window buffer page */
        lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);

        /* set links */
        idx = lpage->hdr.nSlots - 1;
        entryOffset = lpage->slot[-idx];
        entry = (btm_LeafEntry*)&(lpage->data[entryOffset]);
        alignedKlen = ALIGNED_LENGTH(entry->klen);

        *((ShortPageID*)&(entry->kval[alignedKlen])) = opage->hdr.pid.pageNo;
    }

    /* CASE 4-2 : opage is NOT first overflow page */
    else {

        /* get previous overflow window buffer page */
        ppage = &(blkLdEntry->btmBlkLdoBuffer.oPage[(blkLdEntry->btmBlkLdoBuffer.oCount - 1) % NUMOFWINDOWPAGE]);

        /* set links */
        ppage->hdr.nextPage = opage->hdr.pid.pageNo;
        opage->hdr.prevPage = ppage->hdr.pid.pageNo;

        /* flush previous page */
        e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
        if (e < 0)  ERR(handle, e);

        /* initialize previous page */
        e = btm_BlkLdInitOverflow(handle, btmBlkLdId, ppage, blkLdEntry->btmBlkLdblkldInfo.isTmp);
        if (e < 0)  ERR(handle, e);
    }

    /* 5. increment leaf buffer page counter */
    blkLdEntry->btmBlkLdoBuffer.oCount++;


    return eNOERROR;

}   /* btm_BlkLdSplitOverflow() */
