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
 * Module: btm_Search.c
 *
 * Description:
 *  btm_search() traverses the tree from the root page to the target page
 *  for operations like key fetch, delete and insert. The traverse can be
 *  optimized by starting from the page in the old traverse path which are
 *  passed by an input parameter. btm_search() returns the traverse path
 *  from the root page to the target page via the output parameter `path'.
 *
 * Exports:
 *  Four btm_Search(Four, IndexID*, KeyDesc*, KeyValue*, Four, Four, btm_TraversePath*)
 *
 * Returns:
 *  Error code
 *
 * Note:
 *  We assume that the top page's height of `path' is greater than the target
 *  height when the `path' is not empty. So the top page is always an internal
 *  node.
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four btm_Search(
    Four handle,
    IndexID  *iid,		/* IN index ID */ 
    PageID   *root,		/* IN PageID of root page of the Btree */
    KeyDesc  *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value */
    Four     op,		/* IN action routine */
    Four     targetHeight,	/* IN height of the target node */
    btm_TraversePath *path)	/* INOUT traverse path stack */

{
    Four e;			/* error code */
    Four cmp;			/* result of comparison */
    Four slotNo;		/* slot no of the wanted entry in a page */
    Four iEntryOffset;		/* starting offset of an internal entry */
    PageID child;		/* PageID of the child page */
    PageID parent;		/* PageID of the parent */
    Lsn_T parentLsn;		/* LSN of the parent */
    Lsn_T childLsn;		/* LSN of the child */
    Buffer_ACC_CB *child_BCB;	/* pointer to buffer control block for child */
    Buffer_ACC_CB *parent_BCB;	/* pointer to buffer control block for parent */
    BtreePage *apage;		/* pointer to buffer holding child page */
    btm_InternalEntry *iEntry;	/* an internal entry */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_Search(handle, root=%P, kdesc=%P, kval=%P, op=%ld, targetHeight=%P, path=%P)",
	      root, kdesc, kval, op, targetHeight, path));


  StartFromParent:

    /* If the path is empty then traverse from the root. Otherwise traverse */
    /* from the top page of the traverse path stack. */
    for (; !btm_IsEmptyPath(handle, path); ) {

        e = btm_ReadTopElemFromPath(handle, path, &parent_BCB, &parentLsn);
        if (e < eNOERROR) ERR(handle, e);

	e = SHM_getLatch(handle, parent_BCB->latchPtr, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
	if (e < eNOERROR) ERR(handle, e);

	apage = (BtreePage*)parent_BCB->bufPagePtr;

	/* The page isn't touched since we got the parent. */
	if (btm_IsCorrectInternal(handle, &apage->bi, kdesc, kval, iid, &parentLsn)) { 

	    parent = parent_BCB->key;
	    goto Decend;
	}

	e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
	if (e < eNOERROR) ERRB1(handle, e, parent_BCB, PAGE_BUF);

	e = BfM_unfixBuffer(handle, parent_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	e = btm_PopElemFromPath(handle, path, &parent_BCB, &parentLsn);
	if (e < eNOERROR) ERR(handle, e);

    }

  StartFromRoot:
    child = *root;
    MAKE_PAGEID(parent, root->volNo, NIL);
    parent_BCB = NULL;


    do {

	/* Get a buffer. */
	e = BfM_getAndFixBuffer(handle, &child, M_SHARED, &child_BCB, PAGE_BUF);
	if (e < 0) {
            if (parent_BCB != NULL)
                ERRBL1(handle, e, parent_BCB, PAGE_BUF);
            else
                ERR(handle, e);

        }

	apage = (BtreePage*)child_BCB->bufPagePtr;

        if (apage->any.hdr.height > targetHeight) {
	    /* 'child' is a nonleaf node. */

	    if ( apage->bi.hdr.statusBits & BTM_SM_BIT ) {

		cmp = LESS;
		if (apage->bi.hdr.nSlots != 0) {

		    /* Compare the input key with the highest key in child. */
		    iEntryOffset = apage->bi.slot[-(apage->bi.hdr.nSlots-1)];
		    iEntry = (btm_InternalEntry*)&apage->bi.data[iEntryOffset];

		    cmp = btm_KeyCompare(handle, kdesc, kval, (KeyValue*)&iEntry->klen);
		}
	    }

	    if (!(apage->bi.hdr.statusBits & BTM_SM_BIT) ||
		 (apage->bi.hdr.statusBits & BTM_SM_BIT) && cmp != GREAT  ) {

		/* Not an ambiguous case */

		if (parent_BCB != NULL) { /* Unlatch parent */
		    e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
		    if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);
		}

		/* Push the current child into the traverse path stack. */
		e = btm_PushElemIntoPath(handle, path, child_BCB, &apage->any.hdr.lsn);
		if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);

		parent = child;
		parent_BCB = child_BCB;

	      Decend:
		(Boolean) btm_BinarySearchInternal(handle, &apage->bi, kdesc, kval, &slotNo);
		if (slotNo < 0) {

		    MAKE_PAGEID(child, parent.volNo, apage->bi.hdr.p0);

		} else {
		    iEntryOffset = apage->bi.slot[-slotNo];
		    iEntry = (btm_InternalEntry*)&apage->bi.data[iEntryOffset];

		    MAKE_PAGEID(child, parent.volNo, iEntry->spid);
		}

	    } else {		/* BTM_SM_BIT is 1.
				   Unfinished SMO causing ambiguity */

		if (parent_BCB != NULL) { /* Unlatch parent */
		    e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
		    if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);
		}

		/* Unlatch the child. */
		e = SHM_releaseLatch(handle, child_BCB->latchPtr, procIndex);
		if (e < eNOERROR) ERRB1(handle, e, child_BCB, PAGE_BUF);

		/* Request S latch on the tree. */
		e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_UNCONDITIONAL);
		if (e < eNOERROR) ERRB1(handle, e, child_BCB, PAGE_BUF);

		/* X latch the child to update the SM_Bit. */
		e = SHM_getLatch(handle, child_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
		if (e < eNOERROR) ERRB1TL(handle, e, child_BCB, PAGE_BUF, path);

		/* Confirm again */
		if (apage->bi.hdr.statusBits & BTM_SM_BIT) {
		    child_BCB->dirtyFlag = 1; /* Set the dirty flag to 1. */
		    apage->bi.hdr.statusBits = 0; /* Reset the SM_Bit to 0. */
		}

		e = SHM_releaseLatch(handle, child_BCB->latchPtr, procIndex);
		if (e < eNOERROR) ERRB1TL(handle, e, child_BCB, PAGE_BUF, path);


		/* Unfix the buffer for the child. */
		e = BfM_unfixBuffer(handle, child_BCB, PAGE_BUF);
		if (e < eNOERROR) ERRTL(handle, e, path);

		/* Release the tree latch. */
		e = btm_ReleaseTreeLatchInPath(handle, path);
		if (e < eNOERROR) ERR(handle, e);

		/* Retraverse from the root. */
		goto StartFromParent;
	    }			/* Unfinished SMO causing ambiguity */

	}
	else {		/* if apage->any.hdr.height == targetHeight */

	    if (op == BTM_INSERT || op == BTM_DELETE) {

                /* X latch if the action routine is INSERT or DELETE. */
                e = SHM_getLatch(handle, child_BCB->latchPtr, procIndex, M_EXCLUSIVE,
                                 M_UNCONDITIONAL, child_BCB->latchPtr);
                if (e < 0) {
                    if (parent_BCB != NULL)
                        ERRB1L1(handle, e, child_BCB, PAGE_BUF, parent_BCB->latchPtr);
                    else
                        ERRB1(handle, e, child_BCB, PAGE_BUF);
                }

		if (parent_BCB == NULL && apage->any.hdr.height != targetHeight) {
		    /* This is the case that the child was root node and */
		    /* the tree's height was changed. */

		    /* Release X latch and unfix the buffer. */
		    e = SHM_releaseLatch(handle, child_BCB->latchPtr, procIndex);
		    if (e < eNOERROR) ERRB1(handle, e, child_BCB, PAGE_BUF);

		    e = BfM_unfixBuffer(handle, child_BCB, PAGE_BUF);
		    if (e < eNOERROR) ERR(handle, e);

		    /* Retraverse from the start. */
		    goto StartFromRoot;
		}
	    }

             /* when the target page is an internal page, release latch of parent page. */
            if (apage->any.hdr.type & INTERNAL && parent_BCB != NULL) {
                e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);
            }

	    /* Push the current node information */
	    e = btm_PushElemIntoPath(handle, path, child_BCB, &childLsn);
	    if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);

	}

    } while (apage->any.hdr.height > targetHeight);


    return(eNOERROR);


} /* btm_Search() */
