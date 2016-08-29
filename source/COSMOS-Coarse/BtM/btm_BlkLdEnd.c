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
 * Module: btm_BlkLdEnd.c
 *
 * Description :
 *  These functions handle the end of bulkloading. When the bulkloading is finished,
 *  last two window pages in leaf, overflow or each internal level should be merge, redistributed,
 *  or only flushed to balance last two pages so that satisfy HALF page fill factor.
 *
 * Exports:
 *  Four btm_BlkLdEndInternal()
 *  Four btm_BlkLdEndLeaf()
 *  Four btm_BlkLdEndOverflow()
 *  Four btm_BlkLdEndOidBuffer()
 *  Four btm_BlkLdEndWriteBuffer()
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



/*@================================
 * btm_BlkLdEndInternal()
 *================================*/
/*
 * Function: Four btm_BlkLdEndInternal()
 *
 * Description:
 *  When the bulkloading is finished, last two window pages in each internal level will be
 *  merge, redistributed or only flushed.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdEndInternal (
    Four handle,
    Four                    btmBlkLdId)             /* IN  BtM bulkload ID */ 
{
    Four                    e;                      /* error number */
    Two                     i;                      /* a loop index */
    Four                    level;                  /* a index of B+ tree level */

    BlkLdBtreeInternal      *iBuffer;               /* internal buffer pointer */
    BtreeInternal           *ipage;                 /* current internal window buffer pointer */
    BtreeInternal           *ppage;                 /* previous internal window buffer pointer */
    KeyValue                *ikey;                  /* first pointer's key value in current internal page */
    KeyValue                *pkey;                  /* first pointer's key value in previous internal page */

    InternalItem            item;                   /* a iternal item */
    InternalItem            iitem;                  /* a iternal item which is inserted parent internal page */
    Two                     itemLen;                /* length of the 0th internal item */

    Two                     idx;                    /* slot index */
    Two                     entryLen;               /* length of the entry */
    Two                     alignedKlen;            /* aligned size of the key length */
    Two                     entryOffset;            /* starting offset of the entry */
    btm_InternalEntry       *entry;                 /* a leaf entry */
    PageID                  pid;                    /* a page id */

    Two                     s_num;                  /* number of entry to be redistributed (source) */
    Two                     d_num;                  /* number of entry to be redistributed (destination) */
    Two                     s_offset;               /* starting offset of redistribution (source) */
    Two                     d_offset;               /* starting offset of redistribution (destination) */
    Two                     s_size;                 /* size of memory to be redistributed (source) */
    Two                     d_size;                 /* size of memory to be redistributed (destination) */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */



    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdEndInternal(handle)"));


    /* 
    **  O. set entry for fast access 
    */

    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /*
    **  I. For each level of B+ tree except root level, handle the end of bulkloading. 
    */

    /* At first B+ tree level is 0 (from bottom) */
    level = 0;
    iBuffer = &IARRAY(blkLdEntry->btmBlkLdiBuffers)[level];

    /* At least two pages of internal buffer in any level are filled */
    while (iBuffer->iCount > 0) {

        /* find current internal window buffer page */
        ipage = &(iBuffer->iPage[iBuffer->iCount % NUMOFWINDOWPAGE]);

        /* find first pointer's key value of current buffer page matching given level */
        ikey = &(iBuffer->iKey[iBuffer->iCount % NUMOFWINDOWPAGE]);    

        /* find previous internal window buffer page */
        ppage = &(iBuffer->iPage[(iBuffer->iCount - 1) % NUMOFWINDOWPAGE]);

        /* find first pointer's key value of current buffer page matching given level */
        pkey = &(iBuffer->iKey[(iBuffer->iCount - 1) % NUMOFWINDOWPAGE]);    


        /* find 0th internal item by using p0 pointer */
        item.spid = ipage->hdr.p0;
        item.klen = ikey->len;
        memcpy(&(item.kval[0]), &(ikey->val[0]), ikey->len);
        itemLen = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two) + item.klen);


        /* Case 1 : merge */
        if (BI_USED(ipage) + sizeof(Two) + itemLen + sizeof(Two) < BI_CFREE(ppage)) {  /* include slot[0] size & 0th entry size */
            
            /* Copy all entries in the right page to the left page. */
            idx = ppage->hdr.nSlots;
            entryOffset = ppage->slot[-idx] = ppage->hdr.free;
            entry = (btm_InternalEntry*)&(ppage->data[entryOffset]);
            memcpy(entry, &item, itemLen);
            ppage->hdr.nSlots++;
            ppage->hdr.free += itemLen;

            for (i = 0; i < ipage->hdr.nSlots; i++) {     
                entryOffset = ipage->slot[-i];
                entry       = (btm_InternalEntry*)&(ipage->data[entryOffset]);
                entryLen    = ALIGNED_LENGTH(sizeof(Two) + entry->klen) + sizeof(ShortPageID);
        
                ppage->slot[-ppage->hdr.nSlots] = ppage->hdr.free;
                memcpy(&(ppage->data[ppage->hdr.free]), (char*)entry, entryLen);
                ppage->hdr.nSlots++;
                ppage->hdr.free += entryLen;
            }

            if (iBuffer->iCount == 1) {
                ppage->hdr.type |= ROOT;
                ppage->hdr.pid = blkLdEntry->btmBlkLdblkldInfo.root;
            }
            else {
            	/* alloc train */
            	e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            	if (e < 0)  ERR(handle, e);

            	/* set buffer page's pid */
            	ppage->hdr.pid = pid;
            }

            /* make internal item to be inserted */
            iitem.spid = ppage->hdr.pid.pageNo;
            iitem.klen = pkey->len;
            memcpy(&(iitem.kval[0]), &(pkey->val[0]), pkey->len);

            /* insert item into parent page */
            if (level+1 < blkLdEntry->btmBlkLdiBuffers.nLevel) {
                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }
            else {
                /* if B+ tree level exceed internal buffer level, double internal buffer */
                e = btm_BlkLdDoubleSizeInternal(handle, btmBlkLdId);
                if (e < 0)  ERR(handle, e);

                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }

            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);
        }

        /* Case 2 : redistribution */
        else if (BI_USED(ipage) < BI_HALF) {            

            for (i = 0; i < ppage->hdr.nSlots; i++)
                if (ppage->slot[-i] >= BI_HALF)     break;

            idx         = i;
            entryOffset = ppage->slot[-idx];
            entry       = (btm_InternalEntry*)&(ppage->data[entryOffset]);
            entryLen    = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two) + entry->klen);

            s_num       = ppage->hdr.nSlots - idx;
            s_offset    = ppage->slot[-(idx + 1)];
            s_size      = ppage->hdr.free - s_offset;

            d_num       = ipage->hdr.nSlots;
            d_offset    = s_size + itemLen;
            d_size      = ipage->hdr.free;

            memmove(&(ipage->data[d_offset]), &(ipage->data[0]), d_size);
            for (i = ipage->hdr.nSlots - 1; i >= 0; i--) {
                ipage->slot[-(i+s_num)] = ipage->slot[-i] + d_offset;
            }

            memcpy(&(ipage->data[s_size]), &item, itemLen);
            ipage->slot[-(s_num-1)] = s_size;

            memcpy(&(ipage->data[0]), &(ppage->data[s_offset]), s_size);
            for (i = 0; i < s_num-1; i++) {     
                ipage->slot[-i] = ppage->slot[-(idx+1+i)] - s_offset;
            }

            ikey->len = entry->klen;
            memcpy(&(ikey->val[0]), &(entry->kval[0]), entry->klen);
            ipage->hdr.p0 = entry->spid; 


            ppage->hdr.nSlots   -= s_num;
            ppage->hdr.free     -= s_size + entryLen;
            ipage->hdr.nSlots   += s_num;
            ipage->hdr.free     += s_size + itemLen;


            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);

            /* set buffer page's pid */
            ppage->hdr.pid = pid;

            /* make internal item to be inserted */
            iitem.spid = ppage->hdr.pid.pageNo;
            iitem.klen = pkey->len;
            memcpy(&(iitem.kval[0]), &(pkey->val[0]), pkey->len);

            /* insert item into parent page */
            if (level+1 < blkLdEntry->btmBlkLdiBuffers.nLevel) {
                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }
            else {
                e = btm_BlkLdDoubleSizeInternal(handle, btmBlkLdId);
                if (e < 0)  ERR(handle, e);

                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }

            /* flush previous page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);


            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);

            /* set buffer page's pid */
            ipage->hdr.pid = pid;

            /* insert item into parent page */
            iitem.spid = ipage->hdr.pid.pageNo;
            iitem.klen = ikey->len;
            memcpy(&(iitem.kval[0]), &(ikey->val[0]), ikey->len);


            if (level+1 < blkLdEntry->btmBlkLdiBuffers.nLevel) {
                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }
            else {
                e = btm_BlkLdDoubleSizeInternal(handle, btmBlkLdId);
                if (e < 0)  ERR(handle, e);

                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }
    
            /* flush current page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ipage);
            if (e < 0)  ERR(handle, e);
        }

        /* Case 3 : only flush */
        else {
            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);

            /* set buffer page's pid */
            ppage->hdr.pid = pid;

            /* make internal item to be inserted */
            iitem.spid = ppage->hdr.pid.pageNo;
            iitem.klen = pkey->len;
            memcpy(&(iitem.kval[0]), &(pkey->val[0]), pkey->len);

            /* insert item into parent page */
            if (level+1 < blkLdEntry->btmBlkLdiBuffers.nLevel) {
                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }
            else {
                e = btm_BlkLdDoubleSizeInternal(handle, btmBlkLdId);
                if (e < 0)  ERR(handle, e);

                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }

            /* flush previous page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);


            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);

            /* set buffer page's pid */
            ipage->hdr.pid = pid;

            /* insert item into parent page */
            iitem.spid = ipage->hdr.pid.pageNo;
            iitem.klen = item.klen;
            memcpy(&(iitem.kval[0]), &(item.kval[0]), item.klen);

            if (level+1 < blkLdEntry->btmBlkLdiBuffers.nLevel) {
                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }
            else {
                e = btm_BlkLdDoubleSizeInternal(handle, btmBlkLdId);
                if (e < 0)  ERR(handle, e);

                e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &iitem, level+1);
                if (e < 0)  ERR(handle, e);
            }
    
            /* flush current page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ipage);
            if (e < 0)  ERR(handle, e);
        }

        level++;
        iBuffer = &IARRAY(blkLdEntry->btmBlkLdiBuffers)[level];
    }


    /*
    **  II. For root level of B+ tree, handle the end of bulkloading. 
    */

    if (iBuffer->iCount == 0) {

        /* Find current internal window buffer page */
        ipage = &(iBuffer->iPage[iBuffer->iCount%NUMOFWINDOWPAGE]);

        /* root page */
        if (ipage->hdr.nSlots != 0) {
            ipage->hdr.type |= ROOT;
            ipage->hdr.pid = blkLdEntry->btmBlkLdblkldInfo.root;

            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ipage);
            if (e < 0)  ERR(handle, e);
        }
    }


    return eNOERROR;

}  /* btm_BlkLdEndInternal() */




/*@================================
 * btm_BlkLdEndLeaf()
 *================================*/
/*
 * Function: Four btm_BlkLdEndLeaf()
 *
 * Description:
 *  When the bulkloading is finished, last two window pages in leaf will be merge, redistributed 
 *  or only flushed.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdEndLeaf (
    Four handle,
    Four                    btmBlkLdId)             /* IN  BtM bulkload ID */ 
{
    Four                    e;                      /* error number */
    Two                     i;                      /* a loop index */

    BtreeLeaf               *lpage;                 /* current leaf window buffer pointer */
    BtreeLeaf               *ppage;                 /* previous leaf window buffer pointer */

    Two                     idx;                    /* slot index */
    Two                     entryLen;               /* length of the entry */
    Two                     alignedKlen;            /* aligned size of the key length */
    Two                     entryOffset;            /* starting offset of the entry */
    btm_LeafEntry           *entry;                 /* a leaf entry */

    Two                     s_num;                  /* number of entry to be redistributed (source) */
    Two                     d_num;                  /* number of entry to be redistributed (destination) */
    Two                     s_offset;               /* starting offset of redistribution (source) */
    Two                     d_offset;               /* starting offset of redistribution (destination) */
    Two                     s_size;                 /* size of memory to be redistributed (source) */
    Two                     d_size;                 /* size of memory to be redistributed (destination) */

    PageID                  pid;                    /* a page id */
    InternalItem            item;                   /* internal item which will be inserted */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdEndLeaf(handle)")); 


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* Find current leaf window buffer page */
    lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);


    /* case 1 : # of leaf page is 1 */
    if (blkLdEntry->btmBlkLdlBuffer.lCount == 0) {

        lpage->hdr.type |= ROOT;
        lpage->hdr.pid = blkLdEntry->btmBlkLdblkldInfo.root;

        e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)lpage);
        if (e < 0)  ERR(handle, e);
    }

    /* case 2 : # of leaf page is NOT 1 */
    else {

        /* Find previous leaf window buffer page */
        ppage = &(blkLdEntry->btmBlkLdlBuffer.lPage[(blkLdEntry->btmBlkLdlBuffer.lCount - 1) % NUMOFWINDOWPAGE]);

        /* Case 2-1 : merge */
        if (BL_USED(lpage) + sizeof(Two) < BL_CFREE(ppage)) {       /* include slot[0] size */
            
            /* Copy all entries in the right page to the left page. */
            for (i = 0; i < lpage->hdr.nSlots; i++) {     
                entryOffset = lpage->slot[-i];
                entry       = (btm_LeafEntry*)&(lpage->data[entryOffset]);
                alignedKlen = ALIGNED_LENGTH(entry->klen);
        
                if(entry->nObjects < 0)     /* overflow page */
                    entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + sizeof(ShortPageID);
                else
                    entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + (entry->nObjects * OBJECTID_SIZE);
        
                ppage->slot[-ppage->hdr.nSlots] = ppage->hdr.free;
                memcpy(&(ppage->data[ppage->hdr.free]), (char*)entry, entryLen);
                ppage->hdr.nSlots++;
                ppage->hdr.free += entryLen;
            }

            if (blkLdEntry->btmBlkLdlBuffer.lCount == 1) {
                ppage->hdr.type |= ROOT;
                ppage->hdr.pid = blkLdEntry->btmBlkLdblkldInfo.root;


		blkLdEntry->btmBlkLdlBuffer.lCount--;
            }

            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);
        }

        /* Case 2-2 : redistribution */
        else if (BL_USED(lpage) < BL_HALF) {
            
            for (i = 0; i < ppage->hdr.nSlots; i++)
                if (ppage->slot[-i] >= BL_HALF)     break;

            idx         = i;

            s_num       = ppage->hdr.nSlots - idx;
            s_offset    = ppage->slot[-idx];
            s_size      = ppage->hdr.free - s_offset;

            d_num       = lpage->hdr.nSlots;
            d_offset    = s_size;
            d_size      = lpage->hdr.free;

            memmove(&(lpage->data[d_offset]), &(lpage->data[0]), d_size);
            for (i = lpage->hdr.nSlots - 1; i >= 0; i--) {
                lpage->slot[-(i+s_num)] = lpage->slot[-i] + d_offset;
            }

            memcpy(&(lpage->data[0]), &(ppage->data[s_offset]), s_size);
            for (i = 0; i < s_num; i++) {     
                lpage->slot[-i] = ppage->slot[-(idx+i)] - s_offset;
            }

            ppage->hdr.nSlots   -= s_num;
            ppage->hdr.free     -= s_size;
            lpage->hdr.nSlots   += s_num;
            lpage->hdr.free     += s_size;


            /* Case : when # of leaf page is only two */
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

            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);

            /* set buffer page's pid */
            lpage->hdr.pid = pid;

            /* insert item into parent page */
            entryOffset = lpage->slot[0];
            entry = (btm_LeafEntry*)&(lpage->data[entryOffset]);

            item.spid = lpage->hdr.pid.pageNo;
            item.klen = entry->klen;
            memcpy(&(item.kval[0]), &(entry->kval[0]), entry->klen);

            e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &item, 0);
            if (e < 0)  ERR(handle, e);
    

            /* set links */
            ppage->hdr.nextPage = lpage->hdr.pid.pageNo;
            lpage->hdr.prevPage = ppage->hdr.pid.pageNo;

            /* flush previous page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);

            /* flush current page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)lpage);
            if (e < 0)  ERR(handle, e);
        }

        /* Case 2-3 : only flush */
        else {

            /* Case : when # of leaf page is only two */
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

            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);


            /* set buffer page's pid */
            lpage->hdr.pid = pid;


            /* insert item into parent page */
            entryOffset = lpage->slot[0];
            entry = (btm_LeafEntry*)&(lpage->data[entryOffset]);

            item.spid = lpage->hdr.pid.pageNo;
            item.klen = entry->klen;
            memcpy(&(item.kval[0]), &(entry->kval[0]), entry->klen);

            e = btm_BlkLdInsertInternal(handle, btmBlkLdId, &item, 0);
            if (e < 0)  ERR(handle, e);

        
            /* set links */
            ppage->hdr.nextPage = lpage->hdr.pid.pageNo;
            lpage->hdr.prevPage = ppage->hdr.pid.pageNo;

            /* flush previous page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);

            /* flush current page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)lpage);
            if (e < 0)  ERR(handle, e);
        }
    }


    return eNOERROR;

}  /* btm_BlkLdEndLeaf() */




/*@================================
 * btm_BlkLdEndOverflow()
 *================================*/
/*
 * Function: Four btm_BlkLdEndOverflow()
 *
 * Description:
 *  When the bulkloading is finished, last two window pages in overflow will be merge, redistributed 
 *  or only flushed.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdEndOverflow (
    Four handle,
    Four                    btmBlkLdId)             /* IN  BtM bulkload ID */ 
{
    Four                    e;                      /* error number */

    SlotNo                  slotNo;
    Two                     idx;
    BtreeLeaf               *lpage;                 /* current leaf window buffer pointer */
    BtreeOverflow           *opage;                 /* current overflow window buffer pointer */
    BtreeOverflow           *ppage;                 /* previous overflow window buffer pointer */

    Two                     entryOffset;            /* starting offset of a leaf entry */
    Two                     alignedKlen;            /* aligen length of the key length */
    btm_LeafEntry           *entry;                 /* a leaf entry */

    PageID                  pid;                    /* a page id */
    Two                     how;                    /* how may bytes are moved to the new page */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdEndOverflow(handle)"));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* get current leaf & overflow window buffer page */
    lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);
    opage = &(blkLdEntry->btmBlkLdoBuffer.oPage[blkLdEntry->btmBlkLdoBuffer.oCount % NUMOFWINDOWPAGE]);


    /* case 1 : # of overflow page is 1 */
    if (blkLdEntry->btmBlkLdoBuffer.oCount == 0) {

        /* alloc train */
        e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
        if (e < 0)  ERR(handle, e);

        /* set buffer page's pid */
        opage->hdr.pid = pid;

        /* set links */
        slotNo      = lpage->hdr.nSlots - 1;
        entryOffset = lpage->slot[-slotNo];
        entry       = (btm_LeafEntry*)&(lpage->data[entryOffset]);
        alignedKlen = ALIGNED_LENGTH(entry->klen);
        *((ShortPageID*)&(entry->kval[alignedKlen])) = opage->hdr.pid.pageNo;

        /* flush current page */
        e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)opage);
        if (e < 0)  ERR(handle, e);
    }

    /* case 2 : # of overflow page is NOT 1 */
    else {

        /* Find previous overflow window buffer page */
        ppage = &(blkLdEntry->btmBlkLdoBuffer.oPage[(blkLdEntry->btmBlkLdoBuffer.oCount-1) % NUMOFWINDOWPAGE]);

        /* case 2-1 : merge */
        if (BO_USED(opage) + BO_USED(ppage) < NO_OF_OBJECTS*OBJECTID_SIZE) {

            idx = ppage->hdr.nObjects;
            how = opage->hdr.nObjects;
            memcpy(&(ppage->oid[idx]), &(opage->oid[0]), how*OBJECTID_SIZE);

            ppage->hdr.nObjects += how;

            /* flush previous page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);
        }
        /* case 2-2 : redistribution */
        else if (BO_USED(opage) < HALF_OF_OBJECTS*OBJECTID_SIZE) {

            /* shift oids of opage */
            idx = ppage->hdr.nObjects - HALF_OF_OBJECTS;
            how = opage->hdr.nObjects;
            memmove(&(opage->oid[idx]), &(opage->oid[0]), how*OBJECTID_SIZE);

            /* move oids of ppage to opage */
            idx = HALF_OF_OBJECTS;
            how = ppage->hdr.nObjects - HALF_OF_OBJECTS;
            memcpy(&(opage->oid[0]), &(ppage->oid[idx]), how*OBJECTID_SIZE);

            ppage->hdr.nObjects -= how;
            opage->hdr.nObjects += how;


            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);


            /* set buffer page's pid */
            opage->hdr.pid = pid;
        
            /* set links */
            ppage->hdr.nextPage = opage->hdr.pid.pageNo;
            opage->hdr.prevPage = ppage->hdr.pid.pageNo;

            /* flush previous page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);

            /* flush current page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)opage);
            if (e < 0)  ERR(handle, e);
        }
        /* case 2-3 : only flush */
        else {

            /* alloc train */
            e = btm_BlkLdAllocPage(handle, btmBlkLdId, &pid);
            if (e < 0)  ERR(handle, e);

            /* set buffer page's pid */
            opage->hdr.pid = pid;
        
            /* set links */
            ppage->hdr.nextPage = opage->hdr.pid.pageNo;
            opage->hdr.prevPage = ppage->hdr.pid.pageNo;

            /* flush previous page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)ppage);
            if (e < 0)  ERR(handle, e);

            /* flush current page */
            e = btm_BlkLdFlushPage(handle, btmBlkLdId, (BtreePage*)opage);
            if (e < 0)  ERR(handle, e);
        }
    }


    return eNOERROR;

}  /* btm_BlkLdEndOverflow() */



/*@================================
 * btm_BlkLdEndOidBuffer()
 *================================*/
/*
 * Function: Four btm_BlkLdEndOidBuffer()
 *
 * Description:
 *  When the bulkloading is finished or oid array buffer is filled by threshold,
 *  contents of oid array buffer is flushed.
 *
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdEndOidBuffer (
    Four handle,
    Four                    btmBlkLdId)             /* IN  BtM bulkload ID */ 
{
    Four                    e;                      /* error number */

    BtreeLeaf               *lpage;                 /* current leaf window buffer pointer */

    Two                     idx;                    /* the position to be inserted */
    Two                     alignedKlen;            /* aligned size of the key length */
    Two                     entryLen;               /* length of the entry */
    Two                     entryOffset;            /* starting offset of the entry */
    btm_LeafEntry           *lEntry;                /* a leaf entry in leaf page */
    btm_LeafEntry           *oEntry;                /* a leaf entry in oid buffer */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdEndOidBuffer(handle)"));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* get current leaf window buffer page */
    lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);

    /* find first entry of oid buffer */
    entryOffset = blkLdEntry->btmBlkLdoidBuffer.slot[0];
    oEntry = (btm_LeafEntry*)&(blkLdEntry->btmBlkLdoidBuffer.data[entryOffset]);

    /* find overflowed leaf entry */
    idx = lpage->hdr.nSlots - 1;
    entryOffset = lpage->slot[-idx];
    lEntry = (btm_LeafEntry*)&(lpage->data[entryOffset]);


    /* CASE 1 : page is Not filled to the pff percentage. */
    if (BL_USED(lpage) + (oEntry->nObjects-1)*OBJECTID_SIZE < BL_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff)) {

        /* copy the oid array to leaf page buffer */
        alignedKlen = ALIGNED_LENGTH(lEntry->klen);
        memcpy(&(lEntry->kval[alignedKlen+OBJECTID_SIZE]), 
               &(oEntry->kval[alignedKlen+OBJECTID_SIZE]), 
               (oEntry->nObjects - 1) * OBJECTID_SIZE);
        lEntry->nObjects += (oEntry->nObjects - 1);
        lpage->hdr.free += (oEntry->nObjects - 1) * OBJECTID_SIZE;
    }

    /* CASE 2 : page's fill factor satisfy the pff percentage. */
    else {

        /* CASE 2-1 : oid array can be copied to current leaf page buffer */ 
        if (BL_USED(lpage) + (oEntry->nObjects-1)*OBJECTID_SIZE < PAGESIZE-BL_FIXED) {

            /* copy the oid array */
            alignedKlen = ALIGNED_LENGTH(lEntry->klen);
            memcpy(&(lEntry->kval[alignedKlen+OBJECTID_SIZE]), 
                   &(oEntry->kval[alignedKlen+OBJECTID_SIZE]), 
                   (oEntry->nObjects - 1) * OBJECTID_SIZE);
            lEntry->nObjects += (oEntry->nObjects - 1);
            lpage->hdr.free += (oEntry->nObjects - 1) * OBJECTID_SIZE;

            /* split leaf page */
            e = btm_BlkLdSplitLeaf(handle, btmBlkLdId);
            if (e < 0)  ERR(handle, e);
        }

        /* CASE 2-2 : oid array CANN'T be copied to current leaf page buffer */ 
        else {

            /* remove duplicate key in current leaf page */
            alignedKlen = ALIGNED_LENGTH(lEntry->klen);
            entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + OBJECTID_SIZE;

            lpage->hdr.free -= entryLen;
            lpage->hdr.nSlots--;
            
            /* split leaf page */
            e = btm_BlkLdSplitLeaf(handle, btmBlkLdId);
            if (e < 0)  ERR(handle, e);

            /* get new leaf window buffer page */
            lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);

            /* store the entry in oid buffer */
            entryOffset = lpage->slot[0] = lpage->hdr.free;
            lEntry = (btm_LeafEntry*)&(lpage->data[entryOffset]);

            alignedKlen = ALIGNED_LENGTH(oEntry->klen);
            entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + oEntry->nObjects * OBJECTID_SIZE;

            lEntry->nObjects = oEntry->nObjects;
            lEntry->klen = oEntry->klen;
            memcpy(&(lEntry->kval[0]), &(oEntry->kval[0]), oEntry->klen);
            memcpy(&(lEntry->kval[alignedKlen]), &(oEntry->kval[alignedKlen]), oEntry->nObjects * OBJECTID_SIZE);

            lpage->hdr.free += entryLen;     /* slot size is not included */
            lpage->hdr.nSlots++;
        }
    }


    return eNOERROR;

}  /* btm_BlkLdEndOidBuffer() */




/*@===========================
 * btm_BlkLdEndWriteBuffer()
 *===========================*/
/*
 * Function: Four btm_BlkLdEndWriteBuffer()
 *
 * Description: 
 *  Free pages which is not used in last extent
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdEndWriteBuffer (
    Four handle,
    Four                    btmBlkLdId)             /* IN  BtM bulkload ID */ 
{
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    Four                    bufIdx;                 /* a buffer index */
    Four                    pageIdx;                /* a page index */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdEndWriteBuffer(handle)"));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* 0. write old page array buffer that didn't flushed yet */
    for (i = blkLdEntry->btmBlkLdwBuffer.flushedBufCount; i < blkLdEntry->btmBlkLdwBuffer.allocedBufCount - 1; i++) {
        e = btm_BlkLdFlushPages(handle, btmBlkLdId, blkLdEntry->btmBlkLdwBuffer.flushedBufCount % NUMOFWRITEBUFFER);
        if (e < 0) ERR(handle, e);
    }

    /* 1. set buffer index and page index */
    bufIdx = blkLdEntry->btmBlkLdwBuffer.flushedBufCount % NUMOFWRITEBUFFER;
    pageIdx = (blkLdEntry->btmBlkLdwBuffer.allocCount + blkLdEntry->btmBlkLdwBuffer.startPageIdx) % blkLdEntry->btmBlkLdwBuffer.bufSize;

    /* if all pages in last write train buffer are flushed, flush all pages in that train buffer */
    if (blkLdEntry->btmBlkLdwBuffer.flushCount[bufIdx] == 0 && pageIdx == 0 && 
	(blkLdEntry->btmBlkLdlBuffer.lCount != 0 || blkLdEntry->btmBlkLdoBuffer.oCount != 0)) { 
        e = btm_BlkLdFlushPages(handle, btmBlkLdId, blkLdEntry->btmBlkLdwBuffer.flushedBufCount % NUMOFWRITEBUFFER);
        if (e < 0) ERR(handle, e);
    }

    /* 2. all pages in write buffer are not yet flushed */
    if (blkLdEntry->btmBlkLdwBuffer.flushCount[bufIdx] != 0) {

        /* Case 2-1 : current write buffer is first write buffer */ 
        if (blkLdEntry->btmBlkLdwBuffer.flushedBufCount == 0) {

            /* flush pages which are written to write buffer */ 
            for (i = blkLdEntry->btmBlkLdwBuffer.startPageIdx; i < pageIdx; i++) {

                e = RDsM_WriteTrains(handle, (char*)&blkLdEntry->btmBlkLdwBuffer.bufArray[bufIdx][i], 
                                     &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i], 1, PAGESIZE2); 
                if (e < 0) ERR(handle, e);
            }       

            /* free pages which are not yet written to write buffer */ 
            for ( ; i < blkLdEntry->btmBlkLdwBuffer.bufSize; i++) {

                e = RDsM_FreeTrain(handle, &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i], PAGESIZE2);
                if (e < 0) ERR(handle, e);
            }
        }

        /* Case 2-2 : current write buffer is NOT first write buffer */ 
        else {

            /* flush pages which are written to write buffer */ 
            for (i = 0; i < pageIdx; i++) {

                e = RDsM_WriteTrains(handle, (char*)&blkLdEntry->btmBlkLdwBuffer.bufArray[bufIdx][i], 
                                     &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i], 1, PAGESIZE2); 
                if (e < 0) ERR(handle, e);

            }       

            /* free pages which are not yet written to write buffer */ 
            for ( ; i < blkLdEntry->btmBlkLdwBuffer.bufSize; i++) {

                e = RDsM_FreeTrain(handle, &blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[bufIdx][i], PAGESIZE2);
                if (e < 0) ERR(handle, e);

            }
        }
    }


    return eNOERROR;

}   /* btm_BlkLdEndWriteBuffer() */

