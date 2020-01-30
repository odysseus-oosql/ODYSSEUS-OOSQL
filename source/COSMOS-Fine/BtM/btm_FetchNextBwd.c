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
 * Module: btm_FetchNextBwd.c
 *
 * Description:
 *  Fetch the previous ObjectID satisfying the given condition.
 *  By the B+ tree structure modification resulted from the splitting or merging
 *  the current cursor may point to the invalid position. So we should adjust
 *  the B+ tree cursor before using the cursor.
 *
 * Exports:
 *  Four btm_FetchNextBwd(Four, PageID*, LATCH_TYPE*, KeyDesc*, KeyValue*,
 *                        Four, BtreeCursor*, BtreeCursor*)
 *
 * Returns:
 *  Error code
 *    eBADPAGE_BTM
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "LM.h"
#include "BfM.h"
#include "OM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


#define FIRST_LEAF  1
#define SECOND_LEAF 2
#define NOTFOUND    1
#define FOUND       2


/*
 * Function: Four btm_FetchNextBwd(Four, PageID*, LATCH_TYPE*, KeyDesc*, KeyValue*,
 *                            Four, LockParameter*, BtreeCursor*, BtreeCursor*)
 *
 * Description:
 *  Fetch the next ObjectID satisfying the given condition.
 * By the B+ tree structure modification resulted from the splitting or merging
 * the current cursor may point to the invalid position. So we should adjust
 * the B+ tree cursor before using the cursor.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_FetchNextBwd(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    IndexID    *iid,            /* IN index ID */ 
    FileID     *fid,		/* FileID of a file containing the fetched ObjectID */ 
    PageID     *root,		/* IN root page's PageID */
    LATCH_TYPE *treeLatchPtr,	/* IN pointer to the treeLatch */
    KeyDesc    *kdesc,		/* IN key descriptor */
    BtreeCursor *current,	/* IN current B+ tree cursor */
    BtreeCursor *next,		/* OUT next B+ tree cursor */
    LockParameter *lockup)	/* IN request lock or not */
{
    Four e;			/* error number */
    Four whichLeaf;		/* indicates leaf page holding satisfying key */
    Four slotNo;		/* slot no. of a leaf page */
    Four elemNo;		/* element no. of the array of ObjectIDs */
    Four offset;		/* starting offset of ObjectID array or PageID of overflow page */
    PageID ovPid;               /* overflow page id */
    Boolean  found;		/* search result */
    Boolean  usePrevSlotFlag=FALSE;
    ObjectID *oidArray;		/* array of ObjectIDs */
    LockReply lockResult;	/* result of the lock request */
    BtreeLeaf     *apage;	/* pointer to a buffer holding a leaf page */
    btm_LeafEntry *entry;	/* pointer to a leaf entry */
    Buffer_ACC_CB *leaf_BCB;	/* buffer control block holding satisfying key */
    Buffer_ACC_CB *leaf1_BCB;	/* buffer control block for 1st leaf */
    Buffer_ACC_CB *leaf2_BCB;	/* buffer control block for 2nd leaf */
    LockMode oldMode;


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BTM, TR1,
	     ("BtM_FetchNextBwd(root=%P, treeLatchPtr=%P, kdesc=%P, lockup=%P, current=%P, next=%P",
	      root, treeLatchPtr, kdesc, lockup, current, next));

  restartFromFirst:

    /* Copy the previous cursor to the next one. */
    *next = *current;


    /* Get and fix a buffer for the leaf which contained the old key. */
    e = BfM_getAndFixBuffer(handle, &next->leaf, M_SHARED, &leaf1_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* In most case, we need only one leaf page. */
    whichLeaf = FIRST_LEAF;
    leaf_BCB = leaf1_BCB;

    apage = (BtreeLeaf*)leaf1_BCB->bufPagePtr;

    if (!LSN_CMP_EQ(apage->hdr.lsn, next->leafLsn)) { /* leaf page has been changed */ 

        if (btm_IsCorrectLeaf(handle, apage, kdesc, &next->key, iid, &next->leafLsn)) { 

	    found = btm_BinarySearchLeaf(handle, apage, kdesc, &next->key, &slotNo);

	    if (!found) usePrevSlotFlag = TRUE;

            next->slotNo = slotNo;
            next->leafLsn = apage->hdr.lsn;

	} else {

	    /* We should retraverse from the root of the tree. */

	    /* Releasing the holding resources. */
	    e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1(handle, e, leaf1_BCB, PAGE_BUF);

	    e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);


	    /* Retraverse from the root. */
            /* this function returns without releasing latches of leaf1_BCB.
             * leaf2_BCB is valid only if the return value is NOTFOUND and the slot is the first one.
             */
	    e = btm_SearchLeafHavingCursor(handle, next, treeLatchPtr, iid, root, kdesc, 
                                           &whichLeaf, &leaf1_BCB, &leaf2_BCB);
	    if (e < eNOERROR) ERR(handle, e);

	    if (e == NOTFOUND)  usePrevSlotFlag = TRUE;

	    leaf_BCB = (whichLeaf == FIRST_LEAF) ? leaf1_BCB:leaf2_BCB;

	    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

	}
    }

    if ( !usePrevSlotFlag ) {
        entry = (btm_LeafEntry*)&apage->data[apage->slot[-next->slotNo]];
        offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) - BTM_LEAFENTRY_FIXED;

        if (entry->nObjects > 0) {	/* normal entry */

            oidArray = (ObjectID*)&entry->kval[offset];

            /* Has the leaf page been changed since fetching the previous key? */
            if (LSN_CMP_EQ(apage->hdr.lsn, current->leafLsn)) /* no change */
                elemNo = next->oidArrayElemNo;
            else			/* changed */
                (Boolean)btm_BinarySearchOidArray(handle, oidArray, &current->oid, entry->nObjects, &elemNo);
            elemNo--;

            if (elemNo < 0) usePrevSlotFlag = TRUE;
            else {

                /* Set the cursor information. */
                MAKE_PAGEID(next->overflow, apage->hdr.pid.volNo, NIL);
                next->oid = oidArray[elemNo];
                next->oidArrayElemNo = elemNo;
                next->flag = CURSOR_ON;
            }

        } else {			/* overflow page */

            MAKE_PAGEID(ovPid, apage->hdr.pid.volNo, *((ShortPageID*)&entry->kval[offset]));

            /* We search from the overflow page */
            /* this function returns without releasing the latch of ov_BCB EXCEPT the NOTFOUND case */
            e = btm_SearchOverflowHavingCursor(handle, iid, root, current, next, TRUE, &ovPid); 
            if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

            if (e == NOTFOUND) usePrevSlotFlag = TRUE;

        }

    }

    /*
     * Previous objectID is located in the previous slot.
     *
     * When the slot is the first one in the current page
     *      the previous slot is located in the previous page.
     *      (CAUTION :: apage must be FIRST_LEAF).
     */

    if (usePrevSlotFlag) {

        next->slotNo--;

        if (next->slotNo < 0 ) { /* we passed the last slot of the leaf */
            if (apage->hdr.prevPage == NIL) {

                /* Assert that whichLeaf cannot be SECOND_LEAF. */
                if (whichLeaf == SECOND_LEAF) {
                    TR_PRINT(handle, TR_BTM, TR1, ("some internal error"));
                }

                next->flag = CURSOR_EOS;
                next->oid.volNo = apage->hdr.iid.volNo;
                next->oid.pageNo = apage->hdr.iid.serial; 
                next->oid.slotNo = BOI_SLOTNO;
                next->oid.unique = 0;
                MAKE_PAGEID(next->overflow, apage->hdr.pid.volNo, NIL);

                /* no lock on BOI */
                e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

                return(eNOERROR);
            }

            if (whichLeaf == SECOND_LEAF) {
                /* the previous leaf of the current was already fixed and latched */

                apage = (BtreeLeaf*)leaf1_BCB->bufPagePtr;

            }
            else {
                /* Use the next leaf page. */
                MAKE_PAGEID(next->leaf, apage->hdr.pid.volNo, apage->hdr.prevPage);

                e = btm_OpenPreviousLeaf(handle, &next->leaf, &next->overflow, leaf_BCB, &leaf2_BCB, treeLatchPtr, lockup);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                if (e == BTM_RETRAVERSE) {

                    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
                    if (e < eNOERROR) ERR(handle, e);

                    goto restartFromFirst;
                }

                whichLeaf = SECOND_LEAF;
                leaf_BCB = leaf2_BCB;

                apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

            } /* if whichLeaf != SECOND_LEAF */

            next->slotNo = apage->hdr.nSlots - 1; 

        } /* if slotNo < 0  */

        /* Construct Btree cursor.*/
        e = btm_GetCursorForObjectInSlot(handle, apage, next->slotNo, TRUE, next);
        if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

    } /* if usePrevSlotFlag */


    /*
     * Release latches, unfix lock and request lock unconditionally
     * since next entry lock is alreay granted
     */

    if (whichLeaf == SECOND_LEAF) {
	e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
	if (e < eNOERROR) ERRB1BL1(handle, e, leaf1_BCB, PAGE_BUF, leaf_BCB, PAGE_BUF);

	e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
	if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
    }

    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    if (lockup) {

	e = LM_getObjectLock(handle, &xactEntry->xactId, &next->oid, fid, L_S, L_COMMIT, 
			     L_UNCONDITIONAL, &lockResult, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

        if (lockResult == LR_NOTOK) ERR(handle, eDEADLOCK);
    }


    return(eNOERROR);



    ErrorHandle :
        if (whichLeaf == SECOND_LEAF)
            ERRBL2(handle, e, leaf1_BCB, PAGE_BUF, leaf2_BCB, PAGE_BUF);
        else
            ERRBL1(handle, e, leaf_BCB, PAGE_BUF);


} /* btm_FetchNextBwd() */



/*
 * Function: Four btm_OpenPreviousLeaf( )
 *
 * Description:
 *
 * Returns:
 */
Four btm_OpenPreviousLeaf(
    Four handle,
    PageID *prevPid,		/* INOUT PageID of the previous page */
    PageID *ovPid,              /* IN overflow page id */
    Buffer_ACC_CB *leaf1_BCB,   /* IN pointer to buffer control block for 1st leaf */
    Buffer_ACC_CB **leaf2_BCB,	/* OUT pointer to buffer control block for 2nd leaf if applicable */
    LATCH_TYPE *treeLatchPtr,	/* IN pointer to the treeLatch */
    LockParameter *lockup)	/* IN request lock or not */
{
    Four e;			/* error number */
    BtreeLeaf *apage;		/* a Btree Leaf Page */
    Lsn_T     oldLsn;           /* save LSN */

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_OpenPreviousLeaf(handle, prevPid=%P, ovPid=%P, leaf1_BCB=%P, leaf2_BCB=%P, treelatchPtr=%P, lockup=%P)",
              prevPid, ovPid, leaf1_BCB, leaf2_BCB, treeLatchPtr, lockup));


    apage = (BtreeLeaf*)leaf1_BCB->bufPagePtr;

    /* Get and fix the buffer of the 2nd leaf. */
    e = BfM_getAndFixBuffer(handle, prevPid, M_FREE, leaf2_BCB, PAGE_BUF);
    if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

    e = SHM_getLatch(handle, (*leaf2_BCB)->latchPtr, procIndex, M_SHARED, M_CONDITIONAL, NULL);
    if (e < eNOERROR) ERRB1L1(handle, e, (*leaf2_BCB), PAGE_BUF, leaf1_BCB->latchPtr);

    while (e == SHM_BUSYLATCH) { /* Maybe the previous page is envolved in SMO operation */

        /* unfix the previous page */
        e = BfM_unfixBuffer(handle, *leaf2_BCB, PAGE_BUF);
        if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

        oldLsn = apage->hdr.lsn; /* save the lsn of the 1st page */

        /* release all latches */
        e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);

        /* Request an instant duration S latch on the tree. */
        e = SHM_getLatch(handle, treeLatchPtr, procIndex, M_SHARED, M_UNCONDITIONAL|M_INSTANT, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* Relatch the first leaf page */
        e = SHM_getLatch(handle, leaf1_BCB->latchPtr, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* If lockup != NULL
         * other can't insert any object into the first leaf.
         * Because next object lock is already granted
         */
        if (!lockup &&
            (!LSN_CMP_EQ(oldLsn, apage->hdr.lsn)||ovPid->pageNo != NIL)) {

            /* release all latches */
            e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERR(handle, e);

            /* If there is any changes, re-search from the first leaf */
            return(BTM_RETRAVERSE);
        }


        /* Use the previous leaf page. */
        MAKE_PAGEID(*prevPid, apage->hdr.pid.volNo, apage->hdr.prevPage);

        e = BfM_getAndFixBuffer(handle, prevPid, M_FREE, leaf2_BCB, PAGE_BUF);
        if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

        e = SHM_getLatch(handle, (*leaf2_BCB)->latchPtr, procIndex, M_SHARED, M_CONDITIONAL, NULL);
        if (e < eNOERROR) ERRB1L1(handle, e, (*leaf2_BCB), PAGE_BUF, leaf1_BCB->latchPtr);
    }

    apage = (BtreeLeaf*)(*leaf2_BCB)->bufPagePtr;

    if (apage->hdr.nSlots == 0) { /* empty page */
        e = SHM_releaseLatch(handle, (*leaf2_BCB)->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1L1(handle, e, (*leaf2_BCB), PAGE_BUF, leaf1_BCB->latchPtr);

        e = BfM_unfixBuffer(handle, (*leaf2_BCB), PAGE_BUF);
        if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

        e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);

        /* Request an instant duration S latch on the tree. */
        e = SHM_getLatch(handle, treeLatchPtr, procIndex, M_SHARED, M_UNCONDITIONAL|M_INSTANT, NULL);
        if (e < eNOERROR) ERR(handle, e);

        return(BTM_RETRAVERSE);

    }

    return(eNOERROR);

}

