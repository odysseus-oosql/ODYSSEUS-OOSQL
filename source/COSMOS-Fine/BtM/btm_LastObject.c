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
 * Module: btm_LastObject.c
 *
 * Description :
 *  Find the last ObjectID of the given Btree. The 'cursor' will indicate
 *  the last ObjectID in the Btree, and it will be used as successive access
 *  by using the Btree.
 *
 * Exports:
 *  Four btm_LastObject(Four, PageID*, btm_TraversePath*, BtreeCursor*)
 *
 * Returns:
 *  Error code
 *    eBADPAGE_BTM
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 *  cursor : A position in the Btree which indicates the last ObjectID.
 *             The last object's object identifier is also returned via this.
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LM.h"
#include "BfM.h"
#include "BtM.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four btm_LastObject(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, 		/* IN transaction table entry */
    IndexID 		*iid,			/* IN index ID */ 
    FileID 		*fid,			/* FileID of file containing last object */
    PageID  		*root,			/* IN The root of Btree(or subtree) */
    btm_TraversePath 	*path,			/* INOUT traverse path stack */
    BtreeCursor 	*cursor,		/* OUT The last ObjectID in the Btree */
    LockParameter 	*lockup)		/* IN request lock or not */
{
    Four 		e;			/* error code */
    Boolean 		eofFlag=FALSE;      	/* flag for end-of-file */
    Lsn_T 		LeafLsn;		/* LSN of the leaf */
    Buffer_ACC_CB 	*leaf_BCB;		/* pointer to buffer control block for 2nd leaf */
    BtreeLeaf 		*apage;			/* pointer to buffer holding child page */
    LockReply 		lockResult;		/* result of lock request */
    LockMode 		oldMode;


    TR_PRINT(handle, TR_BTM, TR1, ("btm_LastObject(root=%P, path=%P, cursor=%P)", root, path, cursor));


    /* Prevent from appending new elements or deleting the last element */
    if (lockup) {
	/* Construct the special ObjectID. */
	cursor->oid.volNo = root->volNo;
	cursor->oid.pageNo = root->pageNo;
	cursor->oid.slotNo = EOI_SLOTNO;
	cursor->oid.unique = 0;

	cursor->flag = CURSOR_EOS;

	e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

    }

    /*
     * Search the last leaf page
     * the latch of the top entry(leaf_BCB) is holding
     */
    e = btm_SearchFirstOrLastPage(handle, iid, root, 1, path);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the buffer control block for the leaf. */
    e = btm_PopElemFromPath(handle, path, &leaf_BCB, &LeafLsn);
    if (e < eNOERROR) ERR(handle, e);

    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;


    if (apage->hdr.nSlots == 0) {
        /* already have the Btree cursor information */
        eofFlag = TRUE;
    }
    else{
        /* Construct Btree cursor.*/
        e = btm_GetCursorForObjectInSlot(handle, apage, apage->hdr.nSlots-1, TRUE, cursor);
        if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
    }


    /* Release the latch and unfix the buffer of the leaf. */
    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (!lockup || eofFlag) return(eNOERROR);

    /* While holding the page latch, request conditional S lock on the found ObjectID. */
    e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid, 
                         L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
    if (e < eNOERROR) ERR(handle, e);

    if (lockResult == LR_DEADLOCK) ERR(handle, eDEADLOCK);

    /* We found and locked the last object. */
    return(eNOERROR);


} /* btm_LastObject() */


Four btm_FirstObjectIdOfOverflow(
    Four		handle,
    PageID 		*overflow,		/* INOUT PageID of the last overflow page */
    ObjectID 		*oid)			/* INOUT ObjectID of the last objectID of the last overflow page */
{

    Four 		e;			/* error returnresizeed */
    Buffer_ACC_CB 	*ov_BCB;		/* buffer control block for overflow page */
    BtreeOverflow 	*opage;			/* a Btree Overflow Page */



    e = BfM_getAndFixBuffer(handle, overflow, M_SHARED, &ov_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

    *oid = opage->oid[0];

    e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
} /* btm_FirstObjectIdOfOverflow( ) */


Four btm_LastObjectIdOfOverflow(
    Four		handle,
    PageID 		*overflow,		/* INOUT PageID of the last overflow page */
    ObjectID 		*oid,			/* INOUT ObjectID of the last objectID of the last overflow page */
    Two 		*oIndex			/* OUT   oid's index in oidArray */
)
{

    Four 		e;			/* error returnresizeed */
    Buffer_ACC_CB 	*ov_BCB;		/* buffer control block for overflow page */
    Buffer_ACC_CB 	*tmp_BCB;		/* buffer control block for overflow page */
    BtreeOverflow 	*opage;			/* a Btree Overflow Page */



    e = BfM_getAndFixBuffer(handle, overflow, M_SHARED, &ov_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

    while (opage->hdr.nextPage != NIL) {

	/* Try with the next overflow page. */
	MAKE_PAGEID(*overflow, overflow->volNo, opage->hdr.nextPage);

	e = BfM_getAndFixBuffer(handle, overflow, M_SHARED, &tmp_BCB, PAGE_BUF);
	if (e < eNOERROR) ERRBL1(handle, e, ov_BCB, PAGE_BUF);

	e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
	if (e < eNOERROR) ERRB1BL1(handle, e, ov_BCB, PAGE_BUF, tmp_BCB, PAGE_BUF);

	e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
	if (e < eNOERROR) ERRBL1(handle, e, tmp_BCB, PAGE_BUF);

	ov_BCB = tmp_BCB;
	opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

    }

    *oIndex = opage->hdr.nObjects - 1;
    *oid = opage->oid[*oIndex];

    e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_LastObjectIdOfOverflow() */



Four btm_GetCursorForObjectInSlot(
    Four		handle,
    BtreeLeaf 		*apage,			/* IN a Btree Leaf Page */
    Four 		slotNo,			/* IN slot no of the wanted slot of a page */
    Boolean 		isLastElemFlag,		/* IN TRUE if the last element is requested */
    BtreeCursor 	*cursor)		/* OUT Btree Cursor */
{

    Four 		e;			/* error code */
    Four 		offset;			/* starting offset of some field in an entry */
    btm_LeafEntry 	*entry;			/* a Btree leaf entry */

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_getCurosrForLastObjectInSlot(apage=%P, slotNo=%ld, cursor=%P)",
	      apage, slotNo, cursor));

    /*
    ** Construct Btree cursor.
    */
    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-slotNo]]);
    offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED + entry->klen) - BTM_LEAFENTRY_FIXED;

    cursor->flag = CURSOR_ON;
    cursor->leaf = apage->hdr.pid;
    cursor->slotNo = slotNo;
    cursor->key.len = entry->klen;
    memcpy(cursor->key.val, entry->kval, cursor->key.len);
    cursor->leafLsn = apage->hdr.lsn;

    if (entry->nObjects > 0) { /* normal entry */

	cursor->oidArrayElemNo = (isLastElemFlag) ? entry->nObjects - 1: 0;
	MAKE_PAGEID(cursor->overflow, apage->hdr.pid.volNo, NIL);
	cursor->oid = ((ObjectID*)&(entry->kval[offset]))[cursor->oidArrayElemNo];

    } else {		/* overflow page */

	MAKE_PAGEID(cursor->overflow, apage->hdr.pid.volNo,
		    *((ShortPageID*)&(entry->kval[offset])));

	if (isLastElemFlag)
	    /* find the last overflow page */
	    e = btm_LastObjectIdOfOverflow(handle, &cursor->overflow, &cursor->oid, &(cursor->oidArrayElemNo));
	else {
	    cursor->oidArrayElemNo = 0;
	    e = btm_FirstObjectIdOfOverflow(handle, &cursor->overflow, &cursor->oid);
	}
	if (e < eNOERROR) ERR(handle, e);

    }

    return(eNOERROR);

} /* btm_GetCursorForObjectInSlot() */



