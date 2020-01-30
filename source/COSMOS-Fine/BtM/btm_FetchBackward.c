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
 * Module: btm_FetchBackward.c
 *
 * Description:
 *  btm_FetchBackward() is called by BtM_Fetch() when the scan is backward one.
 *
 * Exports:
 *  Four btm_FetchBackward(Four, btm_TraversePath*, KeyDesc*, KeyValue*, LockParameter*, BtreeCursor*)
 *
 * Returns:
 *  Error code
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


#define FIRST_LEAF 		1
#define SECOND_LEAF 		2
#define PREV_SECOND_LEAF 	3


Four btm_FetchBackward(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    FileID           *fid,	/* FileID of the fetched object */ 
    btm_TraversePath *path,	/* IN Btree traverse path stack */
    KeyDesc  *kdesc,		/* IN key descriptor */
    KeyValue *startKval,	/* IN key value of start condition */
    Four     startCompOp,	/* IN comparison operator of start condition */
    BtreeCursor *cursor,	/* OUT Btree Cursor */
    LockParameter *lockup)	/* IN request lock or not */
{
    Four e;			/* error number */
    Four slotNo;		/* slot no of the wanted slot of a page */
    Four whichLeaf;		/* which leaf contains the satisfying key? */
    Boolean eofFlag=FALSE;	/* TRUE if the cursor reach the end of file */
    Buffer_ACC_CB *leaf_BCB;	/* pointer to BCB for buffer holding the key */
    Buffer_ACC_CB *leaf1_BCB;	/* buffer control block for first leaf page */
    Buffer_ACC_CB *leaf2_BCB;	/* buffer control block for 2nd leaf page */
    Buffer_ACC_CB *parent_BCB;	/* buffer control block for parent page */
    Lsn_T leaf1_LSN;		/* LSN of the first page */
    Lsn_T parent_LSN;		/* LSN of the parent page */
    BtreeLeaf *apage;		/* a Btree Leaf Page */
    Boolean found;		/* result of binary search */
    Boolean latchRelease=FALSE; /* Flag of releasing latch of leaf_BCB  */
    PageID leaf2;		/* the 2nd page's PageID */
    btm_LeafEntry *entry;	/* an entry in Btree Leaf Page */
    LockReply lockResult;	/* result of the lock request */
    Four cmp1;			/* result of key comparison */
    Four cmp2;			/* result of key comparison */
    Four cmp;			/* result of the comparison */
    ObjectID oldOid;		/* save the old ObjectID */
    LockMode oldMode;


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_FetchBackward(handle, path=%P, kdesc=%P, startKval=%P, startCompOp=%ld, lockup=%P, cursor=%P)",
	      path, kdesc, startKval, startCompOp, lockup, cursor));


    /* Get the buffer control block for the leaf. */
    e = btm_PopElemFromPath(handle, path, &leaf1_BCB, &leaf1_LSN);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the buffer control block for the parent. */
    if (btm_IsEmptyPath(handle, path)) {
	parent_BCB = NULL;
    } else {
	e = btm_ReadTopElemFromPath(handle, path, &parent_BCB, &parent_LSN);
        if (e < eNOERROR) ERRBL1(handle, e, leaf1_BCB, PAGE_BUF);
    }

    /*
     * check the SM_Bit
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
	e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_UNCONDITIONAL|M_INSTANT);
	if (e < eNOERROR) ERR(handle, e);

	/* Restart the search from the parent. */
	return(BTM_RETRAVERSE);
    }


    /*
    ** Find a satisfying key.
    */

    apage = (BtreeLeaf*)leaf1_BCB->bufPagePtr;

    whichLeaf = FIRST_LEAF;
    leaf_BCB = leaf1_BCB;

    /* Search the given key in the leaf page. */
    found = btm_BinarySearchLeaf(handle, apage, kdesc, startKval, &slotNo);

    /* insert the comparison of startCompOp */
    if (!found || startCompOp == SM_LE) slotNo++;

    if (slotNo >= apage->hdr.nSlots) {
	/* The 1st leaf is not empty and a satisfying key is not found. */

	if (apage->hdr.nextPage == NIL)
	    eofFlag = TRUE;

	else {		/* no last leaf page */

	    /* Get PageID of the 2nd leaf. */
	    MAKE_PAGEID(leaf2, apage->hdr.pid.volNo, apage->hdr.nextPage);

            e = btm_SearchNextLeaf(handle, &leaf2, kdesc, startKval, startCompOp, TRUE, &leaf2_BCB, &found, &slotNo); 
            if (e < eNOERROR) ERRBL1(handle, e, leaf1_BCB, PAGE_BUF);

            if ( e == BTM_RETRAVERSE ) {

		e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
		if (e < eNOERROR) ERRB1(handle, e, leaf1_BCB, PAGE_BUF);

		e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
		if (e < eNOERROR) ERR(handle, e);

		/* Requests an instant duration tree latch in S mode. */
		e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_UNCONDITIONAL|M_INSTANT);
		if (e < eNOERROR) ERR(handle, e);

		/* Restarts the search from the parent. */
		return(BTM_RETRAVERSE);
            }

            /* satisfying key is found on the 2nd leaf. */

	    if (slotNo == 0 && !(found && startCompOp == SM_LE)) {

		whichLeaf = SECOND_LEAF;
                leaf_BCB = leaf2_BCB;

	    } else {

		/* Unlatch and unfix the lst leaf. */
		e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
		if (e < eNOERROR) ERRB1BL1(handle, e, leaf1_BCB, PAGE_BUF, leaf2_BCB, PAGE_BUF);

		e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
		if (e < eNOERROR) ERRBL1(handle, e, leaf2_BCB, PAGE_BUF);

		/*  The 2nd leaf becomes the first leaf. */
		leaf_BCB = leaf2_BCB;
	    }

	}

    } 		/* slotNo >= apage->nSlot */



    /*
     * Request Next ObjectId Lock:
     */

    if (lockup) {

	/*
	 * Construct the cursor and request lock
	 */
	if (eofFlag) {
	    /* Construct the special ObjectID. */
	    cursor->oid.volNo = apage->hdr.iid.volNo;
	    cursor->oid.pageNo = apage->hdr.iid.serial; 
	    cursor->oid.slotNo = EOI_SLOTNO;
	    cursor->oid.unique = 0;

	    cursor->flag = CURSOR_EOS;

	    e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
	    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

	} else {

	    /* Construct Btree cursor.*/
	    e = btm_GetCursorForObjectInSlot(handle, apage, slotNo, FALSE, cursor);
	    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);


	    /* While holding the page latches, request conditional S lock on the found ObjectID. */
	    e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid, 
				 L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
	    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
	}


	if (lockResult == LR_NOTOK ) {

            latchRelease = TRUE;

            if ( whichLeaf == SECOND_LEAF ) {
                /* release the first leaf before while loop */
                e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1BL1(handle, e, leaf1_BCB, PAGE_BUF, leaf2_BCB, PAGE_BUF);

                e = BfM_unfixBuffer(handle, leaf1_BCB, PAGE_BUF);
                if (e < eNOERROR) ERRBL1(handle, e, leaf2_BCB, PAGE_BUF);
            }


            while ( lockResult == LR_NOTOK ) {

                e = btm_RequestUnconditionalLock(handle, xactEntry, &cursor->oid, fid, 
                                                 kdesc, startKval, eofFlag, whichLeaf, leaf_BCB);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                if (e == BTM_RETRAVERSE) {

                    /* Return after freeing the acquired resources. */
                    goto retraverseAfterLockGrant;
                }

                if ( LSN_CMP_EQ(cursor->leafLsn, apage->hdr.lsn)) {
                    /*  the page's LSN is still the same. */

                    if (eofFlag == FALSE && cursor->overflow.pageNo != NIL) {

                        oldOid = cursor->oid;

                        /* find the first overflow page */
                        cursor->oidArrayElemNo = 0;
                        e = btm_FirstObjectIdOfOverflow(handle, &cursor->overflow, &cursor->oid);
                        if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                        cmp = btm_ObjectIdComp(handle, &oldOid, &cursor->oid);
                        if (cmp != EQUAL) {
                            e = LM_releaseObjectLock(handle, &xactEntry->xactId,
                                                     &cursor->oid, L_MANUAL);
                            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                            /* Request the conditional commit duration lock on the new ObjectID. */
                            e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid, 
                                                 L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
                            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                            continue;
                        }
                    }

                    break; /* goto ExtendLockDuration; */
                }

                /* Compare the searched-for key with the first key value of the leaf page. */
                entry = (btm_LeafEntry*)&(apage->data[apage->slot[0]]);
                cmp1 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, startKval);

                /* Compare the searched-for key with the last key value of the leaf page. */
                entry = (btm_LeafEntry*)&(apage->data[apage->slot[-(apage->hdr.nSlots-1)]]);
                cmp2 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, startKval);

                if (cmp1 == GREAT || cmp2 == LESS
                    || cmp2 == EQUAL && startCompOp == SM_LE) {

                    if (eofFlag && apage->hdr.nextPage == NIL) {

                        break; /* goto ExtendLockDuration;  */
                    }

                    /* Retraverse after freeing the acquired resources. */
                    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
                    if (e < eNOERROR) ERR(handle, e);

                    goto retraverseAfterLockGrant;
                }


                found = btm_BinarySearchLeaf(handle, apage, kdesc, startKval, &slotNo);
                if (!found || (found && startCompOp == SM_LE)) slotNo++;

                /* Save the remembered ObjectID. */
                oldOid = cursor->oid;

                /* Construct Btree cursor.*/
                e = btm_GetCursorForObjectInSlot(handle, apage, slotNo, FALSE, cursor);
                if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                cmp = btm_ObjectIdComp(handle, &oldOid, &cursor->oid);

                if (cmp == EQUAL) break; /* goto ExtendLockDuration */
                else {
                    /* Release the lock on the old ObjectID. */
                    if (eofFlag)
                        e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &oldOid, L_MANUAL);
                    else
                        e = LM_releaseObjectLock(handle, &xactEntry->xactId, &oldOid, L_MANUAL);
                    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                    /* The new satisfying key is an ordinary ObjectID. */
                    eofFlag = FALSE;

                    /* Request the conditional commit duration lock on the new ObjectID. */
                    e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid, 
                                         L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
                    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                    continue;
                }

            } /* while (lockResult == L_NOTOK) */


            /* ExtendLockDuration: */

            /* extend the manual duration lock into commit duration lock */
            if (eofFlag)
                e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid,
                                         L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
            else
                e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid, 
                                     L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

        } /* if lockResult == L_NOTOK */


    } /* if lockup */


  findPrevObject:
    /*
     * Now the next entry is locked
     */

    /* Make slotNo of the previous object */
    slotNo--;

    if ( slotNo < 0 || apage->hdr.nSlots == 0) {
                                /* apage->hdr.nSlots == 0 when empty root page */

	if ( !latchRelease && whichLeaf == SECOND_LEAF ) {

	    /* Unlatch and unfix the 2nd leaf */
	    e = SHM_releaseLatch(handle, leaf2_BCB->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1BL1(handle, e, leaf2_BCB, PAGE_BUF, leaf1_BCB, PAGE_BUF);

	    e = BfM_unfixBuffer(handle, leaf2_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERRBL1(handle, e, leaf1_BCB, PAGE_BUF);

	    whichLeaf = FIRST_LEAF;
	    leaf_BCB = leaf1_BCB;

	}
	else {

	    if ( apage->hdr.prevPage == NIL ) {

		/* Construct the special ObjectID. */
		cursor->oid.volNo = apage->hdr.iid.volNo;
		cursor->oid.pageNo = apage->hdr.iid.serial;
		cursor->oid.slotNo = BOI_SLOTNO;
		cursor->oid.unique = 0;

		cursor->flag = CURSOR_EOS;

                /*
                ** We reached the end of the tree.
                */

                e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

                return(eNOERROR);

	    }

	    /* satisfying key is found on the previous page of the first leaf. */

	    /* Get PageID of the previous 2nd leaf. */
	    MAKE_PAGEID(cursor->leaf, apage->hdr.pid.volNo, apage->hdr.prevPage);

            e = btm_OpenPreviousLeaf(handle, &cursor->leaf, &cursor->overflow, leaf_BCB, &leaf2_BCB, path->treeLatchPtr, lockup);
            if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

            if (e == BTM_RETRAVERSE) {

                e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                return(BTM_RETRAVERSE);
            }

	    /* Unlatch and unfix the first leaf */
	    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1BL1(handle, e, leaf_BCB, PAGE_BUF, leaf2_BCB, PAGE_BUF);

	    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERRBL1(handle, e, leaf2_BCB, PAGE_BUF);

	    leaf_BCB = leaf2_BCB;

	}			/* whichLeaf == FIRST_LEAF */

	apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;
	slotNo = apage->hdr.nSlots -1;

    }				/*  if (slotNo < 0) */

    /* Construct Btree cursor.*/
    e = btm_GetCursorForObjectInSlot(handle, apage, slotNo, TRUE, cursor);
    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

    /* Unlatch and unfix the leaf */
    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (!lockup) return(eNOERROR);

    /* While holding the page latches, request conditional S lock on the found ObjectID. */
    e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid, 
                         L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
    if (e < eNOERROR) ERR(handle, e);

    if ( lockResult == LR_DEADLOCK ) ERR(handle, eDEADLOCK);

    return(eNOERROR);


    /*
    ** We acquired the lock on the found object.
    ** But we should retraverse the tree.
    */
  retraverseAfterLockGrant:

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (eofFlag)
	e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_MANUAL);
    else
	e = LM_releaseObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_MANUAL);
    if (e < eNOERROR) ERR(handle, e);

    return(BTM_RETRAVERSE);

} /* btm_FetchBackward() */

