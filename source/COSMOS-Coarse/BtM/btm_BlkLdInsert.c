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
 * Module: btm_BlkLdInsert.c
 *
 * Description :
 *  These functions btm_BlkLdInsert... build B+ tree index in bottom-up method.
 *  The function btm_BlkLdInsertLeaf insert given <key,oid> into leaf or overflow page
 *  and calls btm_BlkLdInsertInternal() to insert internal item, if one leaf page full.
 *  The function btm_BlkLdInsertInternal recursively calls itself using a proper internal item.
 *  These functions use two page window buffer to satisfy BL_HALF page fill condition 
 *  for last two leaf and each level internal pages.
 *
 * Exports:
 *  Four btm_BlkLdInsertLeaf(KeyValue*, ObjectID*) 
 *  Four btm_BlkLdInsertInternal(InternalItem*, Four) 
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
 * btm_BlkLdInsertLeaf()
 *===========================*/
/*
 * Function: Four btm_BlkLdInsertLeaf(KeyValue*, ObjectID*, Boolean*) 
 *
 * Description: 
 *  Insert given <key,oid> into leaf or overflow page and calls btm_BlkLdInsertInternal() 
 *  to insert internal item when one leaf page full. When index bulkloading is finished, 
 *  calls btm_BlkLdEndLeaf() to satisfy BL_HALF page fill condition for last two leaf and 
 *  each level internal pages.
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInsertLeaf (
    Four handle,
    Four                    btmBlkLdId,             /* IN    BtM bulkload ID */ 
    KeyValue                *key,                   /* IN    key value to insert */
    ObjectID                *oid)                   /* IN    ObjectID which will be inserted */
{
    Four                    e;                      /* error number */
    Four                    j;                      /* a loop index */
    Four                    k;                      /* a loop index */
    char                    touch;                  /* byte for data touch */

    BtreeLeaf               *lpage;                 /* current leaf window buffer pointer */
    BtreeLeaf               *ppage;                 /* previous leaf window buffer pointer */

    Four                    compResult;             /* compare result of key values */
    Boolean                 found;                  /* flag whether same key value exist */

    Two                     idx;                    /* slot index */
    Two                     alignedKlen;            /* aligned size of the key length */
    Two                     entryLen;               /* length of the entry */
    Two                     entryOffset;            /* starting offset of the entry */
    btm_LeafEntry           *lEntry;                /* a leaf entry in leaf page */
    btm_LeafEntry           *oEntry;                /* a leaf entry in oid buffer */

    ObjectID                *oidArray;              /* an array of ObjectIDs */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1,
            ("btm_BlkLdInsertLeaf(handle, key=%P, oid=%P)", key, oid));


    /* 
    ** O. set entry for fast access 
    */

    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /*
    ** I. Find last entry in current leaf window buffer page
    */

    /* 1. get current leaf window buffer page */
    lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);

    /* 2. find last entry and compare entry's key and given key */
    if (lpage->hdr.nSlots == 0) {
        idx = -1;
        found = FALSE;
    }
    else {
        idx = lpage->hdr.nSlots - 1;
        entryOffset = lpage->slot[-idx];
        lEntry = (btm_LeafEntry*)&(lpage->data[entryOffset]);

        /* compare last entry's key with newly inserted key */
        compResult = btm_KeyCompare(handle, &blkLdEntry->btmBlkLdblkldInfo.kdesc, (KeyValue*)&lEntry->klen, key);

        /* set found flag by compare result */
        if (compResult == EQUAL)    found = TRUE;
        else                        found = FALSE; 
    }



    /*
    ** II. Insert <key,oid> into leaf page or overflow page.
    **     When page is full, insert internal item into internal page.
    */

    /* II-1. found : insert only oid */
    if (found) {

        /* case 1 : insert oid into overflow page */
        if (lEntry->nObjects < 0) {
            
            e = btm_BlkLdInsertOverflow(handle, btmBlkLdId, oid);
            if (e < 0)  ERR(handle, e);
        }

        /* case 2 : insert oid into leaf page */
        else {

            /* case : when first duplicate key occur */
            if (blkLdEntry->btmBlkLdoidBuffer.hdr.nSlots == 0) {

                /* store the last entry in oid buffer */
                alignedKlen = ALIGNED_LENGTH(lEntry->klen);
                entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + OBJECTID_SIZE;

                entryOffset = blkLdEntry->btmBlkLdoidBuffer.slot[0] = blkLdEntry->btmBlkLdoidBuffer.hdr.free;
                oEntry = (btm_LeafEntry*)&(blkLdEntry->btmBlkLdoidBuffer.data[entryOffset]);

                oEntry->nObjects = 1;
                oEntry->klen = lEntry->klen;
                memcpy(&(oEntry->kval[0]), &(lEntry->kval[0]), lEntry->klen);
                *((ObjectID*)&(oEntry->kval[alignedKlen])) = *((ObjectID*)&(lEntry->kval[alignedKlen]));
    
                blkLdEntry->btmBlkLdoidBuffer.hdr.free += entryLen;     /* slot size is not included */
                blkLdEntry->btmBlkLdoidBuffer.hdr.nSlots++;
            }

            /* 1. append only oid to present entry in oid buffer */
            entryOffset = blkLdEntry->btmBlkLdoidBuffer.slot[0];
            oEntry = (btm_LeafEntry*)&(blkLdEntry->btmBlkLdoidBuffer.data[entryOffset]);
            alignedKlen = ALIGNED_LENGTH(oEntry->klen);
            oidArray = (ObjectID*)&(oEntry->kval[alignedKlen]);

            oidArray[oEntry->nObjects] = *oid;
            oEntry->nObjects++;
            blkLdEntry->btmBlkLdoidBuffer.hdr.free += OBJECTID_SIZE;

            /* 2. calculate the total length of the entry which includes the new ObjectID */
            entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + (oEntry->nObjects)*OBJECTID_SIZE;

            /* 3. create overflow page */
            if (entryLen + sizeof(Two) > OVERFLOW_SPLIT) {      /* include slot size */
                
                e = btm_BlkLdCreateOverflow(handle, btmBlkLdId);
                if (e < 0)  ERR(handle, e);

                /* initialize oid buffer for duplicate key */
                e = btm_BlkLdInitLeaf(handle, btmBlkLdId, &blkLdEntry->btmBlkLdoidBuffer, blkLdEntry->btmBlkLdblkldInfo.isTmp);
                if (e < 0)  ERR(handle, e);
                
                /* overflow flag is setted */
                blkLdEntry->btmBlkLdoverflow = TRUE;
            }
        }
    }

    /* II-2. not found : insert key & oid */
    else {      

        /* case : end of duplicate key occurrence */
        if (blkLdEntry->btmBlkLdoidBuffer.hdr.nSlots != 0) {

            e = btm_BlkLdEndOidBuffer(handle, btmBlkLdId);
            if (e < 0)  ERR(handle, e);

            /* initialize oid buffer for duplicate key */
            e = btm_BlkLdInitLeaf(handle, btmBlkLdId, &blkLdEntry->btmBlkLdoidBuffer, blkLdEntry->btmBlkLdblkldInfo.isTmp);
            if (e < 0)  ERR(handle, e);
        }

        /* case : end of bulkload in overflow page */
        if (blkLdEntry->btmBlkLdoverflow == TRUE) {

            e = btm_BlkLdEndOverflow(handle, btmBlkLdId);
            if (e < 0)  ERR(handle, e);

            /* initialize overflow window buffer */
            e = btm_BlkLdInitOverflowBuffer(handle, btmBlkLdId);
            if (e < 0)  ERR(handle, e);

            blkLdEntry->btmBlkLdoverflow = FALSE;
        }

        /* get current leaf window buffer page */
        lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);

        if (lpage->hdr.nSlots == 0)
            idx = -1;
        else 
            idx = lpage->hdr.nSlots - 1;

        /* calculate the length of a new entry */
        alignedKlen = ALIGNED_LENGTH(key->len);
        entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + OBJECTID_SIZE;

        /* case 1 : page is Not filled to the pff percentage. */
        if ((BL_USED(lpage) + entryLen + sizeof(Two) < BL_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff)) || 
            (BL_USED(lpage) < BL_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff) &&
             BL_USED(lpage) + entryLen + sizeof(Two) >= BL_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff) &&
             BL_USED(lpage) + entryLen + sizeof(Two) < PAGESIZE-BL_FIXED)) {      /* include slot size */

            /* store the new entry */
            entryOffset = lpage->slot[-(idx+1)] = lpage->hdr.free;
            lEntry = (btm_LeafEntry*)&(lpage->data[entryOffset]);

            lEntry->nObjects = 1;
            lEntry->klen = key->len;
            memcpy(&(lEntry->kval[0]), &(key->val[0]), lEntry->klen);
            *((ObjectID*)&(lEntry->kval[alignedKlen])) = *oid;
    
            lpage->hdr.free += entryLen;     /* slot size is not included */
            lpage->hdr.nSlots++;

        }
        /* case 2 : page's fill factor satisfy the pff percentage. */
        else {

            /* split leaf page */
            e = btm_BlkLdSplitLeaf(handle, btmBlkLdId);
            if (e < 0)  ERR(handle, e);

            /* touch internal page buffer for not permitting virtual memory swapping */
            for (j = 0; j < blkLdEntry->btmBlkLdiBuffers.nLevel; j++) {
                for (k = 0; k < NUMOFWINDOWPAGE; k++) {
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+0);
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+512);
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+1024);
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+1536);
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+2048);
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+2560);
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+3072);
                    touch = *(IARRAY(blkLdEntry->btmBlkLdiBuffers)[j].iPage[k].data+3584);
                }
            }

            /* get new leaf window buffer page */
            lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);

            /* store the new entry */
            entryOffset = lpage->slot[0] = lpage->hdr.free;
            lEntry = (btm_LeafEntry*)&(lpage->data[entryOffset]);

            lEntry->nObjects = 1;
            lEntry->klen = key->len;
            memcpy(&(lEntry->kval[0]), &(key->val[0]), lEntry->klen);
            *((ObjectID*)&(lEntry->kval[alignedKlen])) = *oid;
    
            lpage->hdr.free += entryLen;     /* slot size is not included */
            lpage->hdr.nSlots++;
        }
    }


    return eNOERROR;

}   /* btm_BlkLdInsertLeaf() */




/*@===========================
 * btm_BlkLdInsertInternal()
 *===========================*/
/*
 * Function: Four btm_BlkLdInsertInternal(InternalItem*, Four) 
 *
 * Description: 
 *  Insert into the given internal page an InternalItem.
 *  
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInsertInternal (
    Four handle,
    Four                    btmBlkLdId,             /* IN    BtM bulkload ID */ 
    InternalItem            *item,                  /* IN    Iternal item which is inserted */
    Four                    level)                  /* IN    internal node's level */
{
    Four                    e;                      /* error number */

    BtreeInternal           *ipage;                 /* current internal window buffer pointer */
    BtreeInternal           *ppage;                 /* previous internal window buffer pointer */
    KeyValue                *ikey;                  /* first pointer's key value in internal page */

    Two                     idx;                    /* slot index */
    Two                     entryLen;               /* length of the entry */
    Two                     alignedKlen;            /* aligned size of the key length */
    Two                     entryOffset;            /* starting offset of the entry */
    btm_InternalEntry       *entry;                 /* an internal entry of an internal page */

    PageID                  pid;                    /* a page id */
    InternalItem            iitem;                  /* internal item which will be inserted */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1,
            ("btm_BlkLdInsertInternal(handle, item=%P, level=%ld)", item, level));


    /* 
    ** O. set entry for fast access 
    */

    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; 


    /*
    ** I. Find current internal window buffer page and set some variable.
    */

    /* find current internal buffer page matching given level */
    ipage = &((IARRAY(blkLdEntry->btmBlkLdiBuffers)[level]).iPage[IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount % NUMOFWINDOWPAGE]);

    /* find first pointer's key value of current buffer page matching given level */
    ikey = &((IARRAY(blkLdEntry->btmBlkLdiBuffers)[level]).iKey[IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount % NUMOFWINDOWPAGE]);

    /* the length of a entry */
    entryLen = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two) + item->klen);


    /*
    ** II. Insert internal item into internal page.
    **     When page is full, insert internal item into its parent internal page.
    */

    /* case 1 : Page is Not filled to the pff percentage. */
    if ((BI_USED(ipage) + entryLen + sizeof(Two) < BI_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff)) ||
        (BI_USED(ipage) < BI_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff) &&
         BI_USED(ipage) + entryLen + sizeof(Two) >= BI_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff) &&
         BI_USED(ipage) + entryLen + sizeof(Two) < PAGESIZE-BL_FIXED)) {    /* include slot size */

        /* there is no entry in buffer page */
        if (ipage->hdr.p0 == (ShortPageID)NIL) {
            ikey->len = item->klen;
            memcpy(&(ikey->val[0]), &(item->kval[0]), item->klen);
            ipage->hdr.p0 = item->spid;   
        }
        /* store the new entry and update some informations */
        else {
            idx = ipage->hdr.nSlots - 1;
            entryOffset = ipage->slot[-(idx+1)] = ipage->hdr.free;
            entry = (btm_InternalEntry*)&(ipage->data[entryOffset]);
            memcpy((char*)entry, (char*)item, entryLen);
            ipage->hdr.nSlots++;
            ipage->hdr.free += entryLen;
        }
    }
    /* case 2 : page's fill factor satisfy the pff percentage. */
    else {

        /* split internal page */
        e = btm_BlkLdSplitInternal(handle, btmBlkLdId, level);
        if (e < 0)  ERR(handle, e);

        /* find new internal buffer page matching given level */
        ipage = &(IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iPage[IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount % NUMOFWINDOWPAGE]);

        /* find first pointer's key value of new buffer page matching given level */
        ikey = &(IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iKey[IARRAY(blkLdEntry->btmBlkLdiBuffers)[level].iCount % NUMOFWINDOWPAGE]);

        /* store the new entry */
        ikey->len = item->klen;
        memcpy(&(ikey->val[0]), &(item->kval[0]), item->klen);
        ipage->hdr.p0 = item->spid;   
    }


    return eNOERROR;

}   /* btm_BlkLdInsertInternal() */
