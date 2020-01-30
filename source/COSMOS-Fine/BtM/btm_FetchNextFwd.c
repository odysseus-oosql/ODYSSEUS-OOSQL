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
 * Module: btm_FetchNextFwd.c
 *
 * Description:
 *  Fetch the next ObjectID satisfying the given condition.
 *  By the B+ tree structure modification resulted from the splitting or merging
 *  the current cursor may point to the invalid position. So we should adjust
 *  the B+ tree cursor before using the cursor.
 *
 * Exports:
 *  Four btm_FetchNextFwd(Four, PageID*, LATCH_TYPE*, KeyDesc*, KeyValue*,
 *                        Four, BtreeCursor*, BtreeCursor*)
 *
 * Returns:
 *  Error code
 *    eBADPAGE_BTM
 *    some errors caused by function calls
 */


#include <string.h>
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


/* Internal Function Prototypes */
Four btm_BuildCursorUsingNextSlot(Four, BtreeLeaf *, BtreeCursor *);
Four btm_OpenSecondLeaf(Four, PageID *, Buffer_ACC_CB **);

#ifdef WIN32
#pragma optimize("g", off)
#endif

/*
 * Function: Four btm_FetchNextFwd(Four, PageID*, LATCH_TYPE*, KeyDesc*, KeyValue*,
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
Four btm_FetchNextFwd(
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
    Four cmp;			/* comparison result */
    Four cmp1;			/* comparison result */
    Four cmp2;			/* comparison result */
    Four whichLeaf;		/* indicates leaf page holding satisfying key */
    Four slotNo;		/* slot no. of a leaf page */
    Four elemNo;		/* element no. of the array of ObjectIDs */
    Four offset;		/* starting offset of ObjectID array or PageID of overflow page */
    Four oldCursorFlag;		/* save the flag value of the cursor for the locked ObjectID */
    PageID ovPid;               /* overflow page id */
    Boolean  found;		/* search result */
    Boolean  useNextSlotFlag=FALSE; /* Is next oid is located at the next slot of current ? */
    ObjectID *oidArray;		/* array of ObjectIDs */
    ObjectID oldOid;		/* save the old locked ObjectID */
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
	     ("BtM_FetchNextFwd(root=%P, treeLatchPtr=%P, kdesc=%P, lockup=%P, current=%P, next=%P",
	      root, treeLatchPtr, kdesc, lockup, current, next));


    /* Copy the previous cursor to the next one. */
    *next = *current;



    /* Get and fix a buffer for the leaf which contained the old key. */
    e = BfM_getAndFixBuffer(handle, &next->leaf, M_SHARED, &leaf1_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* In most case, we need only one leaf page. */
    whichLeaf = FIRST_LEAF;
    leaf_BCB = leaf1_BCB;

    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

    if (!LSN_CMP_EQ(apage->hdr.lsn, next->leafLsn)) { /* leaf page has been changed */ 

	if (btm_IsCorrectLeaf(handle, apage, kdesc, &next->key, iid, &next->leafLsn)) { 

	    found = btm_BinarySearchLeaf(handle, apage, kdesc, &next->key, &slotNo);
	    if (!found)
                useNextSlotFlag = TRUE;
            else {
                next->slotNo = slotNo;
                next->leafLsn = apage->hdr.lsn;
            }

	} else {

	    /* We should retraverse from the root of the tree. */

	    /* Releasing the holding resources. */
	    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

	    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);


	    /* Retraverse from the root. */
	    e = btm_SearchLeafHavingCursor(handle, next, treeLatchPtr, iid, root, kdesc, 
                                           &whichLeaf, &leaf1_BCB, &leaf2_BCB);
	    if (e < eNOERROR) ERR(handle, e);

	    leaf_BCB = (whichLeaf == FIRST_LEAF) ? leaf1_BCB:leaf2_BCB;

	    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

	    if (e == NOTFOUND) useNextSlotFlag = TRUE;
	}
    }

    if (!useNextSlotFlag) {
        entry = (btm_LeafEntry*)&apage->data[apage->slot[-next->slotNo]];
        offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) - BTM_LEAFENTRY_FIXED;

        if (entry->nObjects > 0) {	/* normal entry */

            oidArray = (ObjectID*)&entry->kval[offset];

            /* Has the leaf page been changed since fetching the previous key? */
            if (LSN_CMP_EQ(apage->hdr.lsn, current->leafLsn)) /* no change */
                elemNo = next->oidArrayElemNo;
            else			/* changed */
                (Boolean)btm_BinarySearchOidArray(handle, oidArray, &current->oid, entry->nObjects, &elemNo);
            elemNo++;

            if (elemNo >= entry->nObjects) useNextSlotFlag = TRUE;
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
            e = btm_SearchOverflowHavingCursor(handle, iid, root, current, next, FALSE, &ovPid); 
            if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

            if (e == NOTFOUND) useNextSlotFlag = TRUE;
        }
    } /* !useNextSlotFlag */



    /*
     * Next objectID is located in the next slot.
     *
     * When the slot is the last one in the current page
     *      the next slot is located in the next page.
     *      (CAUTION :: apage must be FIRST_LEAF).
     */

    if (useNextSlotFlag) {
        next->slotNo++;

        if (next->slotNo >= apage->hdr.nSlots) { /* we passed the last slot of the leaf */
            if (apage->hdr.nextPage == NIL) {
                next->flag = CURSOR_EOS;

                goto lockRequest;
            }

            /* Assert that whichLeaf cannot be SECOND_LEAF. */
            if (whichLeaf == SECOND_LEAF) {
                TR_PRINT(handle, TR_BTM, TR1, ("some internal error"));
            }

            /* Use the next leaf page. */
            MAKE_PAGEID(next->leaf, apage->hdr.pid.volNo, apage->hdr.nextPage);

            e = btm_OpenSecondLeaf(handle, &next->leaf, &leaf2_BCB);
            if (e == BTM_RETRAVERSE) {

                e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1(handle, e, leaf1_BCB, PAGE_BUF);

                e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

                e = SHM_getLatch(handle, treeLatchPtr, procIndex, M_SHARED, M_UNCONDITIONAL|M_INSTANT, NULL);
                if (e < eNOERROR) ERR(handle, e);

                return(BTM_RETRAVERSE);
            }

            apage = (BtreeLeaf*)leaf2_BCB->bufPagePtr;
            whichLeaf = SECOND_LEAF;
            leaf_BCB = leaf2_BCB; 

            next->slotNo = 0;
            next->leafLsn = apage->hdr.lsn;

        }

        e = btm_BuildCursorUsingNextSlot(handle, apage, next);
        if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

    } /* if useNextSlotFlag */

    /*
     * At this point, 'next' points to the next-satisfying key
     *                and latch(es) are holded.
     *
     * Description :
     *             If locking is required
     *                request the conditional lock for next->oid
     *             Release hoding latch(es) and unfix buffer page(s)
     *             If lock is not required or lock grant is successful
     *                goto oidFoud
     *             else
     *                request unconditional lock
     *                relatch the corresponding page and check the next->oid
     *                if it is not correct
     *                   retraverse after release the lock (retraverseAfterLockGrant)
     *                if it had been changed
     *                   save next->oid and then search new next->oid
     *                   if old and new are different
     *                      release old lock
     *                      goto lockRequest
     *                   else
     *                      goto ExtendLockDuration
     */

  lockRequest:
    while (1) {
        e = eNOERROR;		/* clear return value when lockup == NULL */
        /* Request a conditional commit duration S lock on the found ObjectID. */

        if (next->flag == CURSOR_EOS) {
            next->oid.volNo = apage->hdr.iid.volNo;
            next->oid.pageNo = apage->hdr.iid.serial; 
            next->oid.slotNo = -1;
            next->oid.unique = 0;
            MAKE_PAGEID(next->overflow, apage->hdr.pid.volNo, NIL);

            if ( lockup )
                e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &next->oid, L_S,
                                         L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
        } else {

            if ( lockup ) {

                e = LM_getObjectLock(handle, &xactEntry->xactId, &next->oid, fid, L_S, 
                                     L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
            }
        }

        if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);


        if (whichLeaf == SECOND_LEAF) {
            e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1BL1(handle, e, leaf1_BCB, PAGE_BUF, leaf2_BCB, PAGE_BUF);

            e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRBL1(handle, e, leaf2_BCB, PAGE_BUF);
        }



        if ( !lockup || lockResult != LR_NOTOK ) goto ObjectIdFound;


        /* now, lockResult is L_NOTOK */
        e = btm_RequestUnconditionalLock(handle, xactEntry, &next->oid, fid, kdesc, &next->key, 
                                         (next->flag==CURSOR_EOS)?TRUE:FALSE, whichLeaf, leaf_BCB);
        if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

        if (e == BTM_RETRAVERSE) {

            goto retraverseAfterLockGrant;
        }

        /* Save the cursor information for the locked ObjectID. */
        oldOid = next->oid;
        oldCursorFlag= next->flag;

        /* The leaf page has been updated. */
        if (!LSN_CMP_EQ(next->leafLsn, apage->hdr.lsn)) { 
            /* Compare the searched-for key with the first key value of the leaf page. */
            entry = (btm_LeafEntry*)&(apage->data[apage->slot[0]]);
            cmp1 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, &next->key);

            /* Compare the searched-for key with the last key value of the leaf page. */
            entry = (btm_LeafEntry*)&(apage->data[apage->slot[-(apage->hdr.nSlots-1)]]);
            cmp2 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, &next->key);

            if (cmp1 == GREAT || cmp2 == LESS) {     /* cannot find the searched-for key */

                if (apage->hdr.nextPage == NIL) { /* last leaf page */

                    if (next->flag == CURSOR_EOS) { /* found */

                        break;  /* goto ExtendLockDuration; */

                    } else { /* try with EOF */
                        /* Release the lock on the old ObjectID. */
                        e = LM_releaseObjectLock(handle, &xactEntry->xactId, &next->oid, L_MANUAL);
                        if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                        next->flag = CURSOR_EOS;

                        continue; /* goto lockRequest; */
                    }
                }

                /* Release the holding latches. */
                e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                /* Retraverse after freeing the acquired resources. */
                goto retraverseAfterLockGrant;
            }

            found = btm_BinarySearchLeaf(handle, apage, kdesc, &current->key, &slotNo);
            next->slotNo = slotNo;
            next->leafLsn = apage->hdr.lsn;
            if (!found) goto useNextSlotAfterLockGrant;
        }

        /* cursor->key is equal to the key located at the current position. */
        if (entry->nObjects > 0) {	/* normal entry */

            oidArray = (ObjectID*)&entry->kval[offset];

            (Boolean)btm_BinarySearchOidArray(handle, oidArray, &current->oid, entry->nObjects, &elemNo);
            elemNo++;

            if (elemNo >= entry->nObjects) goto useNextSlotAfterLockGrant;


            next->oid = oidArray[elemNo];
            MAKE_PAGEID(next->overflow, apage->hdr.pid.volNo, NIL);
            next->oidArrayElemNo = elemNo;
            next->flag = CURSOR_ON;

        } else {			/* overflow page */

            MAKE_PAGEID(ovPid, apage->hdr.pid.volNo, *((ShortPageID*)&entry->kval[offset]));

            /* We search from the overflow page */
            e = btm_SearchOverflowHavingCursor(handle, iid, root, current, next, FALSE, &ovPid); 
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

            if (e == NOTFOUND)  goto useNextSlotAfterLockGrant;
        }


        /*
         * Compare old and new ObjectIds
         */

      compareObjectIDs:

        /* Compare the new ObjectID with the old ObjectID. */
        cmp = btm_ObjectIdComp(handle, &oldOid, &next->oid);

        if (cmp == EQUAL)  break; /* goto ExtendLockDuration; */
	else {
            /* Release the lock on the old ObjectID. */
            if (oldCursorFlag == CURSOR_EOS)
                e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &oldOid, L_MANUAL);
            else
                e = LM_releaseObjectLock(handle, &xactEntry->xactId, &oldOid, L_MANUAL);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

            continue;               /* goto lockRequest */
        }

        /*
         * Next objectID is located in the next slot.
         *
         * Description :
         *      If the slot is the last one in the current page and
         *      the next slot is located in the next page
         *          retraverse after releasing old lock.
         *      else
         *          search new next->oid in the next slot
         */

      useNextSlotAfterLockGrant:
        next->slotNo++;


        if (next->slotNo >= apage->hdr.nSlots) { /* we passed the last slot of the leaf */
            if (apage->hdr.nextPage == NIL) {	 /* last page */
                if (oldCursorFlag == CURSOR_EOS) { /* EOS is not changed */

                    break; /* goto ObjectIdFound; */
                }

                /* release old lock */
                e = LM_releaseObjectLock(handle, &xactEntry->xactId, &oldOid, L_MANUAL);
                if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                next->flag = CURSOR_EOS;

                continue; /* goto lockRequest; request new lock for EOS */
            }

            /* release holding latch */
            e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

            /* search new next->oid is located in another page */
            goto retraverseAfterLockGrant;
        }

        /* search new next->oid in apage */
        e = btm_BuildCursorUsingNextSlot(handle, apage, next);
        if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

        goto compareObjectIDs;

    } /* lockRequest */

    /* ExtendLockDuration: */

    /* Change the manual duration to commit duration. */
    e = LM_getObjectLock(handle, &xactEntry->xactId, &next->oid, fid, 
                         L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

  ObjectIdFound:

    /* release holding latch */
    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

    /*
     * While the unconditional lock is granted,
     * the leaf page has been changed and (maybe) next->oid become invalid target.
     *
     * Description :
     *             Release the lock for (maybe) wrong object
     *             Unfix the buffer pages.
     *             (for a page which oid is belong to and if exist an overflow page)
     *
     * CAUTION :: All latches are released before.
     */
  retraverseAfterLockGrant:

    if (next->flag == CURSOR_EOS)
	e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &next->oid, L_MANUAL);
    else
	e = LM_releaseObjectLock(handle, &xactEntry->xactId, &next->oid, L_MANUAL);
    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);


    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(BTM_RETRAVERSE);


    ErrorHandle :
        if (whichLeaf == SECOND_LEAF)
            ERRBL2(handle, e, leaf1_BCB, PAGE_BUF, leaf2_BCB, PAGE_BUF);
        else
            ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

} /* btm_FetchNextFwd() */

#ifdef WIN32
#pragma optimize("", on)
#endif


/*
 * Function: Four btm_SearchLeafHavingCursor( )
 *
 * Description:
 *
 * Returns:
 */
Four btm_SearchLeafHavingCursor(
    Four handle,
    BtreeCursor *next,		/* INOUT holds the previous position at entry time */
    LATCH_TYPE *treeLatchPtr,	/* IN pointer to the tree latch */
    IndexID *iid,		/* IN index ID */ 
    PageID *root,		/* IN PageID of the root page of the tree */
    KeyDesc *kdesc,		/* IN Btree key descriptor */
    Four *whichLeaf,		/* OUT indicates the leaf page holding satisfying key */
    Buffer_ACC_CB **bcb1,	/* OUT pointer to buffer control block for 1st leaf */
    Buffer_ACC_CB **bcb2)	/* OUT pointer to buffer control block for 2nd leaf if applicable */
{
    Four e;			/* error number */
    Four slotNo;		/* slot no of the leaf page */
    Lsn_T parent_LSN;		/* LSN of the parent */
    Lsn_T leaf1_LSN;		/* LSN of the 1st leaf page */
    Boolean found;		/* result of search */
    BtreeLeaf *apage;		/* a Btree Leaf Page */
    Buffer_ACC_CB *parent_BCB;	/* buffer control block for the parent */
    Buffer_ACC_CB *leaf1_BCB;	/* buffer control block for 1st leaf page */
    Buffer_ACC_CB *leaf2_BCB;	/* buffer control block for 2nd leaf page */
    btm_TraversePath path;	/* Btree traverse path stack */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_SearchLeafHavingCursor(handle, next=%P, treeLatchPtr=%P, root=%P, kdesc=%P, whichLeaf=%P, bcb1=%P, bcb2=%P)",
	      next, treeLatchPtr, root, kdesc, whichLeaf, bcb1, bcb2));


    /* Initialize the traverse path stack. */
    btm_InitPath(handle, &path, treeLatchPtr);

    /* In most case, we need only one leaf page. */
    *whichLeaf = FIRST_LEAF;

    for (;;) {

        /* Retraversal: Retraverse the tree. */
	e = btm_Search(handle, iid, root, kdesc, &next->key, BTM_FETCH, 0, &path); 
	if (e < eNOERROR) ERR(handle, e);


	/* Get the buffer control block for the 1st leaf. */
	e = btm_PopElemFromPath(handle, &path, &leaf1_BCB, &leaf1_LSN);
	if (e < eNOERROR) ERR(handle, e);

        parent_BCB = NULL;
	/* Get the buffer control block for the parent. */
	if (!btm_IsEmptyPath(handle, &path)) {
	    e = btm_ReadTopElemFromPath(handle, &path, &parent_BCB, &parent_LSN);
	    if (e < eNOERROR) ERRBL1(handle, e, leaf1_BCB, PAGE_BUF);
	}

        /*
         * check the status of Leaf
         */
        /* this func release the latch of parent_BCB */
        e = btm_CheckStatusOfLeaf(handle, leaf1_BCB, (parent_BCB != NULL)?parent_BCB->latchPtr:NULL);
        if (e < eNOERROR) ERRB1(handle, e, leaf1_BCB, PAGE_BUF);

        if (e == BTM_RETRAVERSE) {

            e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            /* Request an instant duration S latch on tree.
             * Wait until an SMO completes if it exists
             */
            e = btm_GetTreeLatchInPath(handle, &path, M_SHARED, M_UNCONDITIONAL|M_INSTANT);
            if (e < eNOERROR) ERR(handle, e);

            /* Restart the search from the parent. */
            continue;
        }


	apage = (BtreeLeaf*)leaf1_BCB->bufPagePtr;

	/* Search the slot with the key value, next->key. */
	found = btm_BinarySearchLeaf(handle, apage, kdesc, &next->key, &slotNo);

	if (!found) slotNo++;

	/* We reached the previous state. */
	if (slotNo < apage->hdr.nSlots || apage->hdr.nextPage == NIL) {

	    /* return the leaf page information. */
	    *bcb1 = leaf1_BCB;

            break;
	}

	/* Get PageID of the 2nd leaf. */
	MAKE_PAGEID(next->leaf, apage->hdr.pid.volNo, apage->hdr.nextPage);

        e = btm_SearchNextLeaf(handle, &next->leaf, kdesc, &next->key, 0, FALSE, &leaf2_BCB, &found, &slotNo);
        if (e < eNOERROR) ERRBL1(handle, e, leaf1_BCB, PAGE_BUF);


	if ( e == BTM_RETRAVERSE) {
	    /* Unlatch and unfix the lst leaf and the 2nd leaf. */
	    e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1(handle, e, leaf1_BCB, PAGE_BUF);

	    e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    /* Requests an instant duration tree latch in S mode. */
	    e = btm_GetTreeLatchInPath(handle, &path, M_SHARED, M_UNCONDITIONAL|M_INSTANT);
	    if (e < eNOERROR) ERR(handle, e);

	    /* Restarts the search from the parent. */
            continue;

	}

        /* satisfying key is found on the 2nd leaf. */
        if (found || slotNo > 0) {

            /* We don't have to hold the 1st page. */
            e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1BL1(handle, e, leaf1_BCB, PAGE_BUF, leaf2_BCB, PAGE_BUF);

            e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRBL1(handle, e, leaf2_BCB, PAGE_BUF);

            /* Return the leaf page information. */
            *bcb1 = leaf2_BCB;


        } else {
            /* We should hold the 1st leaf. */
            *whichLeaf = SECOND_LEAF;

            /* Return the leaf page information. */
            *bcb1 = leaf1_BCB;
            *bcb2 = leaf2_BCB;
        }

        break;

    }	/* for loop */

    /* Finalize the data structure for the traverse path stack. */
    e = btm_FinalPath(handle, &path);
    if (e < eNOERROR) ERR(handle, e);

    /* Set the cursor information. */
    next->leaf = (*whichLeaf == FIRST_LEAF) ? (*bcb1)->key : (*bcb2)->key;
    next->leafLsn = apage->hdr.lsn;
    next->slotNo = (found) ? slotNo : (slotNo - 1);

    if (found) return(FOUND);

    return(NOTFOUND);


} /* btm_SearchLeafHavingCursor() */



/*
 * Function: Four btm_SearchOverflowHavingCursor( )
 *
 * Description:
 *
 * Returns:
 */
Four btm_SearchOverflowHavingCursor(
    Four handle,
    IndexID    *iid,		/* IN index ID */ 
    PageID     *root,           /* IN IndexId */
    BtreeCursor *current,       /* IN holds the previous position */
    BtreeCursor *next,		/* INOUT may hold the next position  */
    Boolean    isBwd,           /* IN is this backward scan ? */
    PageID     *ovPid)          /* IN overflow page */
{
    Four e;			/* error number */
    Four cmp;			/* comparison result */
    Four elemNo;		/* element no. of the array of ObjectIDs */
    Boolean found;		/* result of search */
    BtreeOverflow *opage;       /* a Btree Leaf Page */
    Buffer_ACC_CB *ov_BCB;	/* OUT pointer to buffer control block for overflow page */
    Buffer_ACC_CB *tmp_BCB;	/* buffer control block for the temporary overflow */
    Boolean searchFromFirstOverflow; /* TRUE if we search the overflow chain from the first page */

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_SearchOverflowHavingCursor(handle, root=%P, current=%P, next=%P, isBwd=%ld, ovPid=%P, bcb=%P)", root, current, next, isBwd, ovPid, ov_BCB));

    /* We search from the first page of the overflow chain except some */
    /* optimizable cases. */
    searchFromFirstOverflow = TRUE;

    /* If possible use the overflow page of the previous cursor. */
    /* At this point, next->overflow is equal to current->overflow. */
    if (!IS_NILPAGEID(next->overflow)) {
        e = BfM_getAndFixBuffer(handle, &next->overflow, M_SHARED, &ov_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

        /* The page was deallocated? */
        if (opage->hdr.iid.serial != iid->serial || !(opage->hdr.type & OVERFLOW)) { /* ignore this next->overflow */ 
            e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

            e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

        } else {
            searchFromFirstOverflow = FALSE;

            /* After running the next 'for-statement' we garantee that */
            /* a new ObjectID cannot exist in the front part of the overflow chain. */
            for (;;) {
                cmp = btm_ObjectIdComp(handle, &opage->oid[0], &current->oid);

                /* Exit this for-statement. */
                if (cmp != GREAT || opage->hdr.prevPage == NIL) break;

                /* Try with the previous overflow page. */
                MAKE_PAGEID(next->overflow, next->overflow.volNo, opage->hdr.prevPage);

                /* This backward tree protocol is just allowed to overflow chain */
                /* Because latch of the leaf node (holding overflow chain) is already granted   */
                e = BfM_getAndFixBuffer(handle, &next->overflow, M_SHARED, &tmp_BCB, PAGE_BUF);
                if (e < eNOERROR) ERRBL1(handle, e, ov_BCB, PAGE_BUF);

                e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1BL1(handle, e, ov_BCB, PAGE_BUF, tmp_BCB, PAGE_BUF);

                e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
                if (e < eNOERROR) ERRBL1(handle, e, tmp_BCB, PAGE_BUF);

                ov_BCB = tmp_BCB;
                opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

            } /* for(;;) */
        } /* else */
    } /* if !NIL_PAGE... */


    /* Get and fix a buffer for the first overflow page. */
    if (searchFromFirstOverflow) {

        e = BfM_getAndFixBuffer(handle, ovPid, M_SHARED, &ov_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        opage = (BtreeOverflow*)ov_BCB->bufPagePtr;
    }

    /* We search the overflow page for the new ObjectID. */
    for (;;) {

        found = (Boolean)btm_BinarySearchOidArray(handle, opage->oid, &current->oid, opage->hdr.nObjects, &elemNo);
        if (!isBwd || !found) elemNo++;

        if (elemNo < opage->hdr.nObjects) break;

        if (opage->hdr.nextPage == NIL) { /* Last page of the overflow chain. */

            if (isBwd) break;

            e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

            e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            return(NOTFOUND);

        }

        /* Try with the next overflow page. */
        MAKE_PAGEID(next->overflow, next->overflow.volNo, opage->hdr.nextPage);
        e = BfM_getAndFixBuffer(handle, &next->overflow, M_SHARED, &tmp_BCB, PAGE_BUF);
        if (e < eNOERROR) ERRBL1(handle, e, ov_BCB, PAGE_BUF );

        e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1BL1(handle, e, ov_BCB, PAGE_BUF, tmp_BCB, PAGE_BUF);

        e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
        if (e < eNOERROR) ERRBL1(handle, e, tmp_BCB, PAGE_BUF);

        ov_BCB = tmp_BCB;
        opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

    } /* for (;;) */


    if (isBwd) {
        elemNo--;

        if (elemNo < 0) {
            if (opage->hdr.prevPage == NIL) { /* Last page of the overflow chain. */
                e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

                e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);


                return(NOTFOUND);
            }

            /* Try with the next overflow page. */
            MAKE_PAGEID(next->overflow, next->overflow.volNo, opage->hdr.prevPage);
            e = BfM_getAndFixBuffer(handle, &next->overflow, M_SHARED, &tmp_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRBL1(handle, e, ov_BCB, PAGE_BUF);

            e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1BL1(handle, e, ov_BCB, PAGE_BUF, tmp_BCB, PAGE_BUF);

            e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRBL1(handle, e, tmp_BCB, PAGE_BUF);

            ov_BCB = tmp_BCB;
            opage = (BtreeOverflow*)ov_BCB->bufPagePtr;


            elemNo = opage->hdr.nObjects - 1;


        }
    }

    next->oid = opage->oid[elemNo];
    next->overflowLsn = opage->hdr.lsn;
    next->oidArrayElemNo = elemNo;
    next->flag = CURSOR_ON;

    e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}


/*
 * Function: Four btm_OpenSecondLeaf()
 *
 * Description:
 *
 * Returns:
 */
Four btm_OpenSecondLeaf(
    Four    handle,
    PageID *leaf,		/* IN PageID of the root page of the tree */
    Buffer_ACC_CB **leaf_BCB)	/* OUT pointer to buffer control block for 2nd leaf if applicable */
{
    Four e;			/* error number */
    BtreeLeaf *apage;		/* a Btree Leaf Page */

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_OpenSecondLeaf(leaf=%P, bcb=%P)", leaf, leaf_BCB));



    e = BfM_getAndFixBuffer(handle, leaf, M_SHARED, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (BtreeLeaf*)(*leaf_BCB)->bufPagePtr;

    if (apage->hdr.nSlots == 0) { /* empty page */
        e = SHM_releaseLatch(handle, (*leaf_BCB)->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, (*leaf_BCB), PAGE_BUF);

        e = BfM_unfixBuffer(handle, (*leaf_BCB), PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        return(BTM_RETRAVERSE);
    }

    return(eNOERROR);
}


/*
 * Function: Four btm_BuildCursorUsingNextSlot()
 *
 * Description:
 *
 * Returns:
 */
Four btm_BuildCursorUsingNextSlot(
    Four        handle,
    BtreeLeaf   *apage,         /* IN pointer to a buffer holding a leaf page */
    BtreeCursor *next)		/* INOUT holds the previous position at entry time */
{
    Four e;			/* error number */
    Four offset;		/* starting offset of ObjectID array or PageID of overflow page */
    btm_LeafEntry *entry;	/* pointer to a leaf entry */

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_BuildCursorUsingNextSlot(apage=%P, next=%P, bcb=%P)", apage, next));

    /* search new next->oid in apage */

    entry = (btm_LeafEntry*)&apage->data[apage->slot[-next->slotNo]];
    offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) - BTM_LEAFENTRY_FIXED;

    next->flag = CURSOR_ON;
    next->key.len = entry->klen;
    memcpy(next->key.val, entry->kval, entry->klen);
    next->oidArrayElemNo = 0;

    if (entry->nObjects > 0) {	/* normal entry */
        MAKE_PAGEID(next->overflow, apage->hdr.pid.volNo, NIL);
        next->oid = *((ObjectID*)&entry->kval[offset]);

    } else {			/* overflow page */

	MAKE_PAGEID(next->overflow, apage->hdr.pid.volNo, *((ShortPageID*)&(entry->kval[offset])));
        e = btm_FirstObjectIdOfOverflow(handle, &next->overflow, &next->oid);
        if (e < eNOERROR) ERR(handle, e);

    }

    return(eNOERROR);

}



