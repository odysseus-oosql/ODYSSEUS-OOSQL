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
 * Module: btm_FirstObject.c
 *
 * Description : 
 *  Find the first ObjectID of the given Btree. 
 *
 * Exports:
 *  Four btm_FirstObject(PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_FirstObject()
 *================================*/
/*
 * Function: Four btm_FirstObject(PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*)
 *
 * Description : 
 *  Find the first ObjectID of the given Btree. The 'cursor' will indicate
 *  the first ObjectID in the Btree, and it will be used as successive access
 *  by using the Btree.
 *
 * Returns:
 *  error code
 *    eBADPAGE_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 *  cursor : A position in the Btree which indicates the first ObjectID.
 *             The first object's object identifier is also returned via this.
 */
Four btm_FirstObject(
    Four handle,
    PageID  		*root,		/* IN The root of Btree */
    KeyDesc 		*kdesc,		/* IN Btree key descriptor */
    KeyValue 		*stopKval,	/* IN key value of stop condition */
    Four     		stopCompOp,	/* IN comparison operator of stop condition */
    BtreeCursor 	*cursor)	/* OUT The first ObjectID in the Btree */
{
    Four 		e;				/* error */
    Four 		cmp;			/* result of comparison */
    PageID 		curPid;			/* PageID of the current page */
    PageID 		child;			/* PageID of the child page */
    BtreePage 		*apage;			/* a page pointer */
    Two             lEntryOffset;   /* starting offset of a leaf entry */
    btm_LeafEntry 	*lEntry;		/* a leaf entry */
    Two             alignedKlen;    /* aligned length of the key length */
    
    
    TR_PRINT(TR_BTM, TR1,
             ("btm_FirstObject(handle, root=%P, kdesc=%P, stopKval=%P, stopCompOp=%ld, cursor=%P)",
	      root, kdesc, stopKval, stopCompOp, cursor));

    
    if (root == NULL) ERR(handle, eBADPAGE_BTM);

    curPid = *root;

    e = BfM_GetTrain(handle, &curPid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ Traverse the B+ tree. */
    /* Traverse the B+ tree via p0 pointers from the root to leaf. */
    while (apage->any.hdr.type & INTERNAL) {
	
	/* Get the first(left most) child of the given root page */
	MAKE_PAGEID(child, curPid.volNo, apage->bi.hdr.p0);

	/*@ Free the current page. */
	e = BfM_FreeTrain(handle, &curPid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	curPid = child;
	
	e = BfM_GetTrain(handle, &curPid, (char **)&apage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    /* From now, the curPid is the PageID of a leaf page. */
    
    if (apage->bl.hdr.nSlots == 0) {
	/* Assert that this page is the root page. */
	
	cursor->flag = CURSOR_EOS;
	
    } else {
    
	/* Get the first entry */
	lEntryOffset = apage->bl.slot[0];
	lEntry = (btm_LeafEntry*)&(apage->bl.data[lEntryOffset]);
	
	/*@ Construct the 'cursor' */
	cursor->key.len = lEntry->klen;
	memcpy(&(cursor->key.val[0]), &(lEntry->kval[0]), cursor->key.len);

	if (stopCompOp != SM_EOF) { /* stopCompOp is one of SM_LE and SM_LT. */
	    
	    cmp = btm_KeyCompare(handle, kdesc, &cursor->key, stopKval);

	    if (cmp == GREAT || (cmp == EQUAL && stopCompOp == SM_LT)) {
		cursor->flag = CURSOR_EOS;
		    
		e = BfM_FreeTrain(handle, &curPid, PAGE_BUF);
		if (e < 0) ERR(handle, e);

		return(eNOERROR);
	    }
	}

	cursor->flag = CURSOR_ON;
	cursor->leaf = curPid;
	cursor->slotNo = 0;
	cursor->oidArrayElemNo = 0;
	
	if (lEntry->nObjects < 0) {	/* Overflow page */
	    alignedKlen = ALIGNED_LENGTH(cursor->key.len);
	    
	    MAKE_PAGEID(cursor->overflow,
			curPid.volNo,
			*((ShortPageID*)&(lEntry->kval[alignedKlen])));
	    btm_get_objectid_from_overflow(handle, cursor);
	    
	} else {	/* an ordinary leaf item */
	    MAKE_PAGEID(cursor->overflow, curPid.volNo, NIL);
	    btm_get_objectid_from_leaf(handle, cursor);
	}
    }
    
    e = BfM_FreeTrain(handle, &curPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_FirstObject() */
