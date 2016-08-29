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
 * Module: btm_BlkLdOverflow.c
 *
 * Description:
 *  This file has two functions which are concerned with maintaining overflow
 *  pages. A new overflow page is created when the size of a leaf item becomes 
 *  greater than (a page size * given page fill factor).  If the member of 
 *  ObjectIDs having the same key value grows more than one page limit, a new page 
 *  should be allocated and they are connected by doubly linked list. If last overflow
 *  page is not filled by HALF, last two overflow pages may be merged or redistributed.
 *
 * Exports:
 *  Four btm_BlkLdInsertOverflow(ObjectID*) 
 *  Four btm_BlkLdCreateOverflow() 
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




/*@================================
 * btm_BlkLdInsertOverflow()
 *================================*/
/*
 * Function: Four btm_BlkLdInsertOverflow(ObjectID*) 
 *
 * Description:
 *  After finding the appropriate page and slot, we insert the new object into the
 *  overflow page if there is enough space. If there is not enough space
 *  in the page, allocate new page and then insert the object into that page.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInsertOverflow (
    Four handle,
    Four                    btmBlkLdId,             /* IN    BtM bulkload ID */ 
    ObjectID                *oid)                   /* IN    ObjectID which will be inserted */
{
    Four                    e;                      /* error number */
    Two                     idx;                    /* the position to be inserted */
    BtreeOverflow           *opage;                 /* current overflow window buffer pointer */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1,
            ("btm_BlkLdInsertOverflow(handle, oid=%P)", oid));


    /*
    ** O. set entry for fast access 
    */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /*
    ** I. Find current overflow buffer page
    */
    opage = &(blkLdEntry->btmBlkLdoBuffer.oPage[blkLdEntry->btmBlkLdoBuffer.oCount % NUMOFWINDOWPAGE]);


    /*
    ** II. Insert ObjectID into overflow page.
    **     When page is full, insert oid into newly allocated page.
    */

    /* II-1. Page is Not filled to the pff percentage. */
    if ((BO_USED(opage) + OBJECTID_SIZE < BO_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff)) ||
        (BO_USED(opage) < BO_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff) &&
         BO_USED(opage) + OBJECTID_SIZE >= BO_LIMIT(blkLdEntry->btmBlkLdblkldInfo.pff) &&
         BO_USED(opage) + OBJECTID_SIZE < PAGESIZE-BO_FIXED-BO_DUMMY)) {

        /* store the new ObjectID */
        idx = opage->hdr.nObjects;
        opage->oid[idx] = *oid;
        opage->hdr.nObjects++;
    }
    /* II-2. page's fill factor satisfy the pff percentage. */
    else {

        /* split overflow page */
        e = btm_BlkLdSplitOverflow(handle, btmBlkLdId);
        if (e < 0)  ERR(handle, e);

        /* get new overflow window buffer page */
        opage = &(blkLdEntry->btmBlkLdoBuffer.oPage[blkLdEntry->btmBlkLdoBuffer.oCount % NUMOFWINDOWPAGE]);

        /* store the new oid */
        idx = opage->hdr.nObjects;
        opage->oid[idx] = *oid;
        opage->hdr.nObjects++;
    }
    

    return eNOERROR;

}  /* btm_BlkLdInsertOverflow() */




/*@================================
 * btm_BlkLdCreateOverflow()
 *================================*/
/*
 * Function: Four btm_BlkLdCreateOverflow()
 *
 * Description:
 *  This function created a new overflow page. Ar first, that is, ObjectIDs
 *  having the same key value are moved from the leaf page to the newly
 *  allocated overflow page.
 *  After the new page was made, the given ObjectID is inserted using the insert
 *  routine.
 *  Sinve it is necessary to mark that over flow pages are used, the item in
 *  the leaf should be updated.
 *  ( The ordinary leaf item :
 *       <key length, key value, # of ObjectIDs, ObjectID list>
 *    The leaf item using overflow page :
 *       <key length, key value, ShortPageID of Overflow page> )
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdCreateOverflow (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                      /* error number */

    BtreeLeaf               *lpage;                 /* current leaf window buffer pointer */
    BtreeOverflow           *opage;                 /* current overflow window buffer pointer */

    Two                     idx;                    /* the position to be inserted */
    Two                     entryOffset;            /* starting offset of a leaf entry */
    Two                     alignedKlen;            /* aligen length of the key length */
    btm_LeafEntry           *lEntry;                /* a leaf entry in leaf page */
    btm_LeafEntry           *oEntry;                /* a leaf entry in oid buffer */

    Two                     how;                    /* how many ObjectID are moved to the new page */
    ObjectID                *oidArray;              /* array of ObjectIDs */

    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdCreateOverflow(handle)"));


    /* 0. set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */

    /* 1. get current leaf & overflow window buffer page */
    lpage = &(blkLdEntry->btmBlkLdlBuffer.lPage[blkLdEntry->btmBlkLdlBuffer.lCount % NUMOFWINDOWPAGE]);
    opage = &(blkLdEntry->btmBlkLdoBuffer.oPage[blkLdEntry->btmBlkLdoBuffer.oCount % NUMOFWINDOWPAGE]);

    /* 2. find overflowed leaf entry */
    idx = lpage->hdr.nSlots - 1;
    entryOffset = lpage->slot[-idx];
    lEntry = (btm_LeafEntry*)&(lpage->data[entryOffset]);
            
    /* 3. find oid array in oid buffer */
    entryOffset = blkLdEntry->btmBlkLdoidBuffer.slot[0];
    oEntry = (btm_LeafEntry*)&(blkLdEntry->btmBlkLdoidBuffer.data[entryOffset]);
    alignedKlen = ALIGNED_LENGTH(oEntry->klen);    

    /* 4. copy ObjectIDs from the oid buffer to the new overflow page */
    oidArray = (ObjectID*)&(oEntry->kval[alignedKlen]);
    how = oEntry->nObjects;
    memcpy(&(opage->oid[0]), &(oidArray[0]), how * OBJECTID_SIZE);
    opage->hdr.nObjects = oEntry->nObjects;

    /* 5. Re-calculate lpage's free space */
    lpage->hdr.free -= OBJECTID_SIZE - sizeof(ShortPageID);

    /* 6. The leaf page has an overflow PageID instead of ObjectID list */
    lEntry->nObjects = NIL;
    

    return eNOERROR;

}  /* btm_BlkLdCreateOverflow() */
