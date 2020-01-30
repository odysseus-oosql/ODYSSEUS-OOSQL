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
 * Module: btm_GetNextObjectLock.c
 *
 * Description:
 *  Acquire the lock on the next object of the given object.
 *
 *  CAUTION :: Calling routine must acquire the latch of child_BCB.
 *             And this routine keeps the latch of child_BCB.
 *
 * Exports:
 *  Four btm_GetNextObjectLock(Four, btm_TraversePath *, Buffer_ACC_CB *, KeyDesc *,
 *                             KeyValue *, ObjectID *, Four, PageID *)
 *
 * Returns:
 *  eNOERROR
 *  BTM_RELATCHED
 *  BTM_RETRAVERSE
 *  Error code
 *    some errors caused by function calls
 *
 */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "LM.h"
#include "OM.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Constant Definition */
#define FIRST_LEAF  0
#define SECOND_LEAF 1


Four btm_GetNextObjectLock(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    FileID           *fid,	/* FileID of file containing the next object */ 
    btm_TraversePath *path,	/* IN Btree Traverse Path stack */
    Buffer_ACC_CB *leaf1_BCB,	/* IN leaf page for `oid' */
    KeyDesc *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value */
    ObjectID *oid,		/* IN object to delete or insert */
    Four op,			/* IN type of operation (BTM_INSERT/BTM_DELETE) */
    PageID *oid_ovPid)		/* OUT overflow page for 'oid' */
{
    Four e;			/* error number */
    Four cmp;			/* result of comparison */
    Four state;			/* 0: we don't have the old next object */
				/* 1: we have lock on the old next object */
    Four slotNo;		/* slot no of the next object */
    Four elemNo;		/* element no of the array of ObjectIDs */
    Four offset;		/* starting offset of an array of ObjectIDs or an overflow PageID  */
    Four whichLeaf;		/* leaf page containing the next object */
    Boolean done;		/* TRUE if we find the next object */
    Boolean found;		/* result of search */
    Boolean eofFlag;		/* end-of-file flag */
    Boolean oldEofFlag;		/* end-of-file flag for the old next object */
    Boolean latchRelease;	/* TRUE if latch on leaf is released */
    Boolean oidFound;		/* TRUE if the given object is found */
    FileID oldFid;		/* FileID of file containing the old next objec */
    IndexID iid;		/* IndexID of this tree */
    PageID nextPid;		/* PageID of the 2nd leaf page */
    PageID ovPid;		/* PageID of the overflow page */
    PageID next_ovPid;          /* PageID of the next overflow page */
    ObjectID nextOid;		/* ObjectID of the next object */
    ObjectID oldNextOid;	/* ObjectID of the old next object */
    ObjectID *oidArray;		/* array of ObjectIDs */
    BtreeLeaf *apage1, *apage2;	/* pointers to Btree leaf pages */
    BtreeOverflow *opage;	/* pointer to a Btree overflow page */
    btm_LeafEntry *entry;	/* a leaf entry of Btree */
    LockDuration duration;	/* requested lock duration */
    LockReply lockResult;	/* result of lock request */
    Lsn_T leaf1_LSN;		/* remember LSN of the first leaf page */
    Buffer_ACC_CB *ov_BCB;	/* buffer control block for overflow page */
    Buffer_ACC_CB *leaf2_BCB;	/* buffer control block for the 2nd leaf */
    Buffer_ACC_CB *next_ov_BCB;	/* temporary pointer to next overflow page buffer control block */
    LockMode oldMode;


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_lockNextObjectLock(path=%P, leaf1_BCB=%P, kdesc=%P, kval=%P, oid=%P, op=%ld, oid_ovPid=%P)",
	      path, leaf1_BCB, kdesc, kval, oid, op, oid_ovPid));


    /* Initialize the variable latchRelease to FALSE. */
    latchRelease = FALSE;

    apage1 = (BtreeLeaf*)leaf1_BCB->bufPagePtr;

    for (state = 0; ; ) {

	/* Initialize the variable oidFound to FALSE. */
	oidFound = FALSE;

	eofFlag = FALSE;
	whichLeaf = FIRST_LEAF;

	done = FALSE;		/* TRUE if the next object is found */

	/* Search the first leaf page for the given key value. */
	found = btm_BinarySearchLeaf(handle, apage1, kdesc, kval, &slotNo);

	if (found) {

	    /* Points to the entry with given key value. */
	    entry = (btm_LeafEntry*)&apage1->data[apage1->slot[-slotNo]];
	    offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED + entry->klen) - BTM_LEAFENTRY_FIXED;

	    if (entry->nObjects > 0) { /* normal entry */
		oidArray = (ObjectID*)&entry->kval[offset];

		oidFound = btm_BinarySearchOidArray(handle, oidArray, oid, entry->nObjects, &elemNo);

		elemNo++;	/* Points to next ObjectID */

		if (elemNo < entry->nObjects) {
		    nextOid = oidArray[elemNo];

		    done = TRUE; /* we found the next object */
		}

	    } else {		   /* overflow page */

		/* Get the PageID of the overflow page. */
		MAKE_PAGEID(ovPid, apage1->hdr.pid.volNo, *((ShortPageID*)&entry->kval[offset]));
		e = BfM_getAndFixBuffer(handle, &ovPid, M_SHARED, &ov_BCB, PAGE_BUF);
                if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

		for ( ; ; ) {

		    opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

		    cmp = btm_ObjectIdComp(handle, &opage->oid[opage->hdr.nObjects-1], oid);

		    if (cmp == EQUAL) {
			oidFound = TRUE;
			*oid_ovPid = ovPid;

		    } else if (cmp == GREAT) {
			found = btm_BinarySearchOidArray(handle, opage->oid, oid, opage->hdr.nObjects, &elemNo);

			if (found) {
			    oidFound = TRUE;
			    *oid_ovPid = ovPid;
			}

			elemNo ++; /* Use the next object. */

			/* Assert that elemNo < opage->nObjects. */
			nextOid = opage->oid[elemNo];

			done = TRUE; /* we found the next object */
		    }

                    MAKE_PAGEID(next_ovPid, ovPid.volNo, opage->hdr.nextPage);

		    if (cmp != GREAT && next_ovPid.pageNo != NIL) {

			e = BfM_getAndFixBuffer(handle, &next_ovPid, M_SHARED, &next_ov_BCB, PAGE_BUF);
			if (e < eNOERROR) ERRBL1L1(handle, e, ov_BCB, PAGE_BUF, leaf1_BCB->latchPtr);
		    }

		    e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
		    if (e < eNOERROR) ERRB1L1(handle, e, ov_BCB, PAGE_BUF, leaf1_BCB->latchPtr);

		    e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
                    if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

		    if (cmp == GREAT || next_ovPid.pageNo == NIL) {
			if (oidFound == FALSE) *oid_ovPid = ovPid;
			break;  /* exit from inner for loop */
		    }

		    ov_BCB = next_ov_BCB;
                    ovPid = next_ovPid;

		} /* inner for loop */
	    } /* else */
	} /* if found */

	/* return error state right after detecting the error */
        if (op == BTM_INSERT && oidFound == TRUE || op == BTM_DELETE && oidFound == FALSE) {
            e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERR(handle, e);

            if (oidFound) return(BTM_FOUND);
            else  return(BTM_NOTFOUND);

        }

	if (!done) {            /* if we not found the next object from the current slot */
	    slotNo ++;		/* go to the next slot */

	    if (slotNo < apage1->hdr.nSlots) {

                /* get the first oid of the next slot */
		e = btm_GetObjectId(handle, apage1, slotNo, &nextOid, &ovPid);
                if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

	    } else {

		if (apage1->hdr.nextPage == NIL) { /* last page of Btree */
		    nextOid.volNo = apage1->hdr.iid.volNo;
		    nextOid.pageNo = apage1->hdr.iid.serial;
		    nextOid.slotNo = -1;
		    nextOid.unique = 0;
		    eofFlag = TRUE;

		    done = TRUE;

		} else {	/* nextOid is in the next page */

		    whichLeaf = SECOND_LEAF;

		    MAKE_PAGEID(nextPid, apage1->hdr.pid.volNo, apage1->hdr.nextPage);

		    e = BfM_getAndFixBuffer(handle, &nextPid, M_SHARED, &leaf2_BCB, PAGE_BUF);
                    if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

		    apage2 = (BtreeLeaf*)leaf2_BCB->bufPagePtr;

		    if (apage2->hdr.nSlots == 0) {
			e = SHM_releaseLatch(handle, leaf2_BCB->latchPtr, procIndex);
                        if (e < eNOERROR) ERRB1L1(handle, e, leaf2_BCB, PAGE_BUF, leaf1_BCB->latchPtr);

			e = BfM_unfixBuffer(handle, leaf2_BCB, PAGE_BUF);
                        if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

			/* Remember the LSN of the first leaf page. */
			leaf1_LSN = apage1->hdr.lsn;
			iid = apage1->hdr.iid;

			e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
			if (e < eNOERROR) ERR(handle, e);

			/* The latch has been released. */
			latchRelease = TRUE;

			/* Request instant duration S latch on the tree. */
			e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_UNCONDITIONAL|M_INSTANT);
			if (e < eNOERROR) ERR(handle, e);

			/* Relatch the first leaf page. */
			e = SHM_getLatch(handle, leaf1_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
			if (e < eNOERROR) ERR(handle, e);


			if (!btm_IsCorrectLeaf(handle, apage1, kdesc, kval, &iid, &leaf1_LSN)) {
			    if (state == 1) {
				/* Release the lock on the next object. */
				if (oldEofFlag)
				    e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &oldNextOid, L_MANUAL);
				else
				    e = LM_releaseObjectLock(handle, &xactEntry->xactId, &oldNextOid, L_MANUAL);
                                if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);
			    }

                            e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
                            if (e < eNOERROR) ERR(handle, e);

			    return(BTM_RETRAVERSE);
			}

			/* Search again from the first leaf page. */
			continue;

		    } /* if page2.hdr.nslots == 0 */

		    slotNo = 0;

		    e = btm_GetObjectId(handle, apage2, slotNo, &nextOid, &ovPid);
                    if (e < eNOERROR) ERRBL1L1(handle, e, leaf2_BCB, PAGE_BUF, leaf1_BCB->latchPtr);
		}
	    }
	}


	if (state != 0) {
	    /* We have the manual duration X lock on the old next object. */
	    if (btm_ObjectIdComp(handle, &nextOid, &oldNextOid) == EQUAL) {
		/* In this case current variables are same as old ones */
		if (op == BTM_INSERT) {
		    /* Release the lock on the next object. */
			break;
		    if (eofFlag)
			e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &nextOid, L_MANUAL);
		    else
			e = LM_releaseObjectLock(handle, &xactEntry->xactId, &nextOid, L_MANUAL);
                    if (e < 0)  ERRGOTO(handle, e, ErrorHandling);

		} else {	/* op == BTM_DELETE */
		    /* Request the commit duration lock on the next object. */
		    if (eofFlag)
			e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &nextOid,
						 L_X, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
		    else
			e = LM_getObjectLock(handle, &xactEntry->xactId, &nextOid, &oldFid,
					     L_X, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
                    if (e < 0)  ERRGOTO(handle, e, ErrorHandling);
		}

		break;		/* exit the loop */
	    }

	    /* Release the lock on the old next object. */
	    if (oldEofFlag)
		e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &oldNextOid, L_MANUAL);
	    else
		e = LM_releaseObjectLock(handle, &xactEntry->xactId, &oldNextOid, L_MANUAL);
            if (e < 0)  ERRGOTO(handle, e, ErrorHandling);
	}

	/* If the operation is insert, request instant duration X lock. */
	/* Otherwise (delete operation) request commit duration X lock. */
	duration = (op == BTM_INSERT) ? L_INSTANT : L_COMMIT;

	if (eofFlag)
	    e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &nextOid, L_X, duration, L_CONDITIONAL, &lockResult, &oldMode);
	else
	    e = LM_getObjectLock(handle, &xactEntry->xactId, &nextOid, fid, L_X, duration, L_CONDITIONAL, &lockResult, &oldMode); 
        if (e < 0)  ERRGOTO(handle, e, ErrorHandling);

	if (lockResult != LR_NOTOK) break; /* exit the loop */

	if (whichLeaf == SECOND_LEAF) {
	    e = SHM_releaseLatch(handle, leaf2_BCB->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1L1(handle, e, leaf2_BCB, PAGE_BUF, leaf1_BCB->latchPtr);

	    e = BfM_unfixBuffer(handle, leaf2_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);
	}

	/* Remember the LSN of the first leaf page. */
	leaf1_LSN = apage1->hdr.lsn;
	iid = apage1->hdr.iid;

	/* Release the holding latches. */
	e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);


	/* The latch on leaf has been released. */
	latchRelease = TRUE;

	/* Requests an unconditional, manual duration, X lock on the next key's ObjectID. */
	if (eofFlag)
	    e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &nextOid,
				     L_X, L_MANUAL, L_UNCONDITIONAL, &lockResult, &oldMode);
	else
	    e = LM_getObjectLock(handle, &xactEntry->xactId, &nextOid, fid,
				 L_X, L_MANUAL, L_UNCONDITIONAL, &lockResult, &oldMode);
        if (e < eNOERROR) ERR(handle, e);


	if ( lockResult == LR_DEADLOCK ) ERR(handle, eDEADLOCK);


	/* Relatch the original leaf. */
	e = SHM_getLatch(handle, leaf1_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e);

	if (!btm_IsCorrectLeaf(handle, apage1, kdesc, kval, &iid, &leaf1_LSN)) {

	    /* Release the lock on the next object. */
	    if (eofFlag)
		e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &nextOid, L_MANUAL);
	    else
		e = LM_releaseObjectLock(handle, &xactEntry->xactId, &nextOid, L_MANUAL);
            if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);

            e = SHM_releaseLatch(handle, leaf1_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERR(handle, e);

	    return(BTM_RETRAVERSE);
	}

	/* Remember the information for the next object. */
	oldEofFlag = eofFlag;
	oldNextOid = nextOid;
	oldFid = *fid; 

	/* Change the state to check if the original leaf remains a correct one. */
	state = 1;              /* exit from outer for loop */
    }

    if (whichLeaf == SECOND_LEAF) {
	e = SHM_releaseLatch(handle, leaf2_BCB->latchPtr, procIndex);
	if (e < eNOERROR) ERRB1L1(handle, e, leaf2_BCB, PAGE_BUF, leaf1_BCB->latchPtr);

	e = BfM_unfixBuffer(handle, leaf2_BCB, PAGE_BUF);
        if (e < eNOERROR) ERRL1(handle, e, leaf1_BCB->latchPtr);
    }

    return((latchRelease == TRUE) ? BTM_RELATCHED:eNOERROR);


  ErrorHandling:

    if (whichLeaf == SECOND_LEAF)
        ERRBL1L1(handle, e, leaf2_BCB, PAGE_BUF, leaf1_BCB->latchPtr);
    else
        ERRL1(handle, e, leaf1_BCB->latchPtr);


} /* btm_GetNextObjectLock() */


