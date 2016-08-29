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
 * Module: BtM_FetchNext.c
 *
 * Description:
 *  Find the next ObjectID satisfying the given condition. The current ObjectID
 *  is specified by the 'current'.
 *
 * Exports:
 *  Four BtM_FetchNext(PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*, BtreeCursor*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ Internal Function Prototypes */
Four btm_FetchRetraverse(Four, PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*, BtreeCursor*);
Four btm_FetchNext(Four, KeyDesc*, KeyValue*, Four, BtreeCursor*, BtreeCursor*);
Four btm_FetchObjectIdInOverflow(Four, BtreeCursor*, Four, Boolean*);



/*@================================
 * BtM_FetchNext()
 *================================*/
/*
 * Function: Four BtM_FetchNext(PageID*, KeyDesc*, KeyValue*,
 *                              Four, BtreeCursor*, BtreeCursor*)
 *
 * Description:
 *  Fetch the next ObjectID satisfying the given condition.
 * By the B+ tree structure modification resulted from the splitting or merging
 * the current cursor may point to the invalid position. So we should adjust
 * the B+ tree cursor before using the cursor.
 *
 * Returns:
 *  error code
 *    eBADCURSOR
 *    some errors caused by function calls
 */
Four BtM_FetchNext(
    Four handle,
    PageID                      *root,          /* IN root page's PageID */
    KeyDesc                     *kdesc,         /* IN key descriptor */
    KeyValue                    *kval,          /* IN key value of stop condition */
    Four                        compOp,         /* IN comparison operator of stop condition */
    BtreeCursor                 *current,       /* IN current B+ tree cursor */
    BtreeCursor                 *next)          /* OUT next B+ tree cursor */
{
    Four                        e;              /* error number */
    Four                        cmp;            /* comparison result */
    Two                         slotNo;         /* slot no. of a leaf page */
    Two                         oidArrayElemNo; /* element no. of the array of ObjectIDs */
    Two                         alignedKlen;    /* aligned length of key length */
    PageID                      overflow;       /* temporary PageID of an overflow page */
    Boolean                     found;          /* search result */
    ObjectID                    *oidArray;      /* array of ObjectIDs */
    BtreeLeaf                   *apage;         /* pointer to a buffer holding a leaf page */
    BtreeOverflow               *opage;         /* pointer to a buffer holding an overflow page */
    btm_LeafEntry               *entry;         /* pointer to a leaf entry */
    BtreeCursor                 tCursor;        /* a temporary Btree cursor */
  
    
    TR_PRINT(TR_BTM, TR1,
             ("BtM_FetchNext(root=%P, kdesc=%P, kval=%P, compOp=%ld, current=%P, next=%P",
	      root, kdesc, kval, compOp, current, next));
    
    /*@ check parameter */
    if (root == NULL || kdesc == NULL || kval == NULL || current == NULL || next == NULL)
	ERR(handle, eBADPARAMETER_BTM);
    
    /* Is the current cursor valid? */
    if (current->flag != CURSOR_ON && current->flag != CURSOR_EOS)
	ERR(handle, eBADCURSOR);
    
    if (current->flag == CURSOR_EOS) return(eNOERROR);
    
    /*@ Copy the current cursor to the next cursor. */
    *next = *current;
    
    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    if (!(apage->hdr.type & LEAF)) goto retraverse;
    
    /*@ Adjust the slot no. */
    found = FALSE;
    if (next->slotNo >= 0 && next->slotNo < apage->hdr.nSlots) {
	entry = (btm_LeafEntry*)&apage->data[apage->slot[-next->slotNo]];
		
	if (btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, &current->key) == EQUAL)
	    found = TRUE;
    }

    if (!found) {
	found = btm_BinarySearchLeaf(handle, apage, kdesc, &current->key, &slotNo);
	if (!found && (slotNo < 0 || slotNo >= apage->hdr.nSlots-1)) goto retraverse;
	next->slotNo = slotNo;	
    }
        
    if (!found) {		/*@ cannot find the current cursor's key */
	/* The current cursor's key has been deleted. */
	/* 'next->slotNo' points to the slot with a key less than the current cursor's key. */
	
	switch(compOp) {
	  case SM_EQ:
	    next->flag = CURSOR_EOS;
	    break;
	    
	  case SM_LT:		/* forward scan */
	  case SM_LE:
	  case SM_EOF:
	    next->slotNo++;
	    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
	    alignedKlen = ALIGNED_LENGTH(entry->klen);
	    
	    if (compOp != SM_EOF) { 
		/* Check the boundary condition. */
		cmp = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);
		if (cmp == GREAT || (cmp == EQUAL && compOp == SM_LT)) {
		    next->flag = CURSOR_EOS;
		    break;
		}
	    }
	    
	    if (entry->nObjects < 0) { /* overflow page */
		MAKE_PAGEID(next->overflow, next->leaf.volNo,
			    *((ShortPageID*)&entry->kval[alignedKlen]));
		
		e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
		next->oidArrayElemNo = 0; /*@ set oidArrayElemNo to 0.  */
		next->oid = opage->oid[next->oidArrayElemNo];
		
		e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
	    } else {		       /* normal entry */
		oidArray = (ObjectID*)&(entry->kval[alignedKlen]);
		
		next->oidArrayElemNo = 0; /*@ set oidArrayElemNo to 0. */
		next->oid = oidArray[0];
		MAKE_PAGEID(next->overflow, next->leaf.volNo, NIL);
	    }
	    
	    next->flag = CURSOR_ON;
	    break;
	    
	  case SM_GT:		/* backward scan */
	  case SM_GE:
	  case SM_BOF:
	    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
	    alignedKlen = ALIGNED_LENGTH(entry->klen);
	    
	    if (compOp != SM_BOF) {
		/* Check the boundary condition. */
		cmp = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);
		if (cmp == LESS || (cmp == EQUAL && compOp == SM_GT)) {
		    next->flag = CURSOR_EOS;
		    break;
		}
	    }
	    
	    if (entry->nObjects < 0) { /* overflow page */
		/*@ get the page id. */
		MAKE_PAGEID(next->overflow, next->leaf.volNo,
			    *((ShortPageID*)&entry->kval[alignedKlen]));

		/*@ read the page */
		e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
		while (opage->hdr.nextPage != NIL) {
		    MAKE_PAGEID(overflow, next->overflow.volNo, opage->hdr.nextPage);
		    
		    e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		    
		    next->overflow = overflow;
		    
		    e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		}
		
		next->oidArrayElemNo = opage->hdr.nObjects - 1;
		next->oid = opage->oid[next->oidArrayElemNo];

		/*@ free the page */
		e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
	    } else {		       /* normal entry */
		oidArray = (ObjectID*)&(entry->kval[alignedKlen]);
		
		next->oidArrayElemNo = entry->nObjects - 1;
		next->oid = oidArray[next->oidArrayElemNo];
		MAKE_PAGEID(next->overflow, next->leaf.volNo, NIL);
	    }
	    
	    next->flag = CURSOR_ON;
	    break;
	} /* end of switch */

	if (next->flag == CURSOR_ON)
	    memcpy((char*)&next->key, (char*)&entry->klen, entry->klen+sizeof(Two));
	
	e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	return(eNOERROR);
    }
    
    /* At this point, the slot no. is correct. */
    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
    alignedKlen = ALIGNED_LENGTH(entry->klen);
    
    if (entry->nObjects < 0) {	/* overflow page */
	if (IS_NILPAGEID(next->overflow)) {
	    /* The current cursor's ObjectID was not in the overflow page. */
	    MAKE_PAGEID(next->overflow, next->leaf.volNo, *((ShortPageID*)(&entry->kval[alignedKlen])));
	    next->oidArrayElemNo = 0;
	} else {
	    Boolean freePageFlag; /* TRUE if the page has been freed. */

	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);

	    freePageFlag = (opage->hdr.type == FREEPAGE) ? TRUE:FALSE;

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->overflow,PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);

	    if (freePageFlag) {
		MAKE_PAGEID(next->overflow, next->leaf.volNo,
			    *((ShortPageID*)(&entry->kval[alignedKlen])));
		next->oidArrayElemNo = 0;		
	    }	    
	}

	/*@ free the page */
	e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	e = btm_FetchObjectIdInOverflow(handle, next, compOp, &found);
	if (e < 0) ERR(handle, e);
	
	if (!found) {		/* cannot locate to the next ObjectID */
	    tCursor = *next;
	    e = btm_FetchNext(handle, kdesc, kval, compOp, &tCursor, next);
	    if (e < 0) ERR(handle, e);
	}
	
    } else {			/* normal entry */
	oidArray = (ObjectID*)&entry->kval[alignedKlen];
	found = btm_BinarySearchOidArray(handle, oidArray, &current->oid, entry->nObjects, &oidArrayElemNo);
	
	if (compOp == SM_EQ || compOp == SM_LT || compOp == SM_LE || compOp == SM_EOF) /* forward scan */
	    next->oidArrayElemNo = oidArrayElemNo;
	else			/* SM_GT or SM_GE: backward scan */
	    next->oidArrayElemNo = (found) ? oidArrayElemNo:(oidArrayElemNo+1);
	
	MAKE_PAGEID(next->overflow, next->leaf.volNo, NIL);

	/*@ free the page */
	/* Notice: next->leaf may be changed in btm_FetchNext(). */
	e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	tCursor = *next;
	e = btm_FetchNext(handle, kdesc, kval, compOp, &tCursor, next);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);
    
  retraverse:

    /*@ free the page */
    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    e = btm_FetchRetraverse(handle, root, kdesc, kval, compOp, current, next);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* BtM_FetchNext() */



/*@================================
 * btm_FetchRetraverse()
 *================================*/
/*
 * Function: Four btm_FetchRetraverse(PageID*, KeyDesc*, KeyValue*,
 *                                    Four, BtreeCursor*, BtreeCursor*)
 *
 * Description:
 *  Retraverse the tree from the root node to find the next ObjectID.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_FetchRetraverse(
    Four handle,
    PageID   		*root,		/* IN root page's PageID */
    KeyDesc  		*kdesc,		/* IN key descriptor */
    KeyValue 		*kval,		/* IN key value of stop condition */
    Four     		compOp,		/* IN comparison operator of stop condition */
    BtreeCursor 	*current,	/* IN current B+ tree cursor */
    BtreeCursor 	*next)		/* OUT next B+ tree cursor */
{
    Four 		e;		/* error number */
    Four 		cmp;		/* comparison result */
    Two 		oidArrayElemNo;	/* element no. of the array of ObjectIDs */
    Four 		fetchCompOp;	/* comparison operator to use on call of BtM_Fetch() */
    Two 		alignedKlen;	/* aligned length of key length */
    Boolean 		found;		/* search result */
    ObjectID 		*oidArray;	/* array of ObjectIDs */
    BtreeLeaf   	*apage;		/* pointer to a buffer holding a leaf page */
    BtreeCursor 	tCursor;	/* a temporary Btree cursor */
    btm_LeafEntry 	*entry;		/* pointer to a leaf entry */
    
    
    TR_PRINT(TR_BTM, TR1,
             ("btm_FetchRetraverse(handle, root=%P, kdesc=%P, kval=%P, compOp=%ld, current=%P, next=%P)",
	      root, kdesc, kval, compOp, current, next));
    
    
    if (compOp == SM_EQ)
	fetchCompOp = SM_EQ;
    else if (compOp == SM_LT || compOp == SM_LE || compOp == SM_EOF)
	fetchCompOp = SM_GE;
    else			/* SM_GT, SM_GE, or SM_BOF */
	fetchCompOp = SM_LE;
    
    e = BtM_Fetch(handle, root, kdesc, &current->key, fetchCompOp, kval, compOp, next);
    if (e < 0) ERR(handle, e);
    
    /*
    ** Check the boundary condition.
    */
    if (next->flag == CURSOR_EOS) return(eNOERROR);

    cmp = btm_KeyCompare(handle, kdesc, &current->key, &next->key);
    if (cmp != EQUAL) return(eNOERROR);

    /*@ save the cursor. */
    tCursor = *next;
    cmp = btm_ObjectIdComp(handle, &tCursor.oid, &current->oid);
    if (cmp == EQUAL) {
	e = btm_FetchNext(handle, kdesc, kval, compOp, &tCursor, next);
	if (e < 0) ERR(handle, e);
	
	return(eNOERROR);	
    }
    
    if (compOp == SM_EQ || compOp == SM_LT || compOp == SM_LE || compOp == SM_EOF) { /* forward scan */
	
	if (cmp == GREAT) return(eNOERROR);
	if (IS_NILPAGEID(next->overflow)) { /* normal entry */
	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
	    alignedKlen = ALIGNED_LENGTH(entry->klen);
	    oidArray = (ObjectID*)&entry->kval[alignedKlen];
	    
	    found = btm_BinarySearchOidArray(handle, oidArray, &current->oid, entry->nObjects, &oidArrayElemNo);
	    
	    if (oidArrayElemNo+1 < entry->nObjects) {
		/* Go to the right ObjectID. */
		next->oidArrayElemNo = oidArrayElemNo + 1;
		next->flag = CURSOR_ON;
	    } else {
		next->oidArrayElemNo = oidArrayElemNo; /* last ObjectID */
		next->flag = CURSOR_INVALID;
	    }
	    next->oid = oidArray[next->oidArrayElemNo];
	    
	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    if (next->flag == CURSOR_ON) return(eNOERROR);
	    
	} else {		/* overflow page */
	    
	    e = btm_FetchObjectIdInOverflow(handle, next, compOp, &found);
	    if (e < 0) ERR(handle, e);
	    
	    if (found) return(eNOERROR);
	}
	
    } else {			/* SM_GT, SM_GE, or SM_BOF : backward scan */
	
	if (cmp == LESS) return(eNOERROR);
	if (IS_NILPAGEID(next->overflow)) { /* normal entry */
	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
	    alignedKlen = ALIGNED_LENGTH(entry->klen);
	    oidArray = (ObjectID*)&entry->kval[alignedKlen];
	    
	    found = btm_BinarySearchOidArray(handle, oidArray, &current->oid, entry->nObjects, &oidArrayElemNo);
	    
	    if (found) oidArrayElemNo--;
	    
	    if (oidArrayElemNo >= 0) {
		next->oidArrayElemNo = oidArrayElemNo;
		next->flag = CURSOR_ON;
	    } else {
		next->oidArrayElemNo = 0; /* the first ObjectID */
		next->flag = CURSOR_INVALID;
	    }
	    next->oid = oidArray[next->oidArrayElemNo];

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    if (next->flag == CURSOR_ON) return(eNOERROR);
	    
	} else {		/* overflow page */
	    e = btm_FetchObjectIdInOverflow(handle, next, compOp, &found);
	    if (e < 0) ERR(handle, e);
	    
	    if (found) return(eNOERROR);
	}
    }
    
    tCursor = *next;
    e = btm_FetchNext(handle, kdesc, kval, compOp, &tCursor, next);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_FetchRetraverse() */



/*@================================
 * btm_FetchNext()
 *================================*/
/*
 * Function: Four btm_FetchNext(KeyDesc*, KeyValue*, Four,
 *                              BtreeCursor*, BtreeCursor*)
 *
 * Description:
 *  Get the next item. We assume that the current cursor is valid; that is.
 *  'current' rightly points to an existing ObjectID.
 *
 * Returns:
 *  Error code
 *    eBADCOMPOP_BTM
 *    some errors caused by function calls
 */
Four btm_FetchNext(
    Four handle,
    KeyDesc  		*kdesc,		/* IN key descriptor */
    KeyValue 		*kval,		/* IN key value of stop condition */
    Four     		compOp,		/* IN comparison operator of stop condition */
    BtreeCursor 	*current,	/* IN current cursor */
    BtreeCursor 	*next)		/* OUT next cursor */
{
    Four 		e;		/* error number */
    Four 		cmp;		/* comparison result */
    Two 		alignedKlen;	/* aligned length of a key length */
    PageID 		leaf;		/* temporary PageID of a leaf page */
    PageID 		overflow;	/* temporary PageID of an overflow page */
    ObjectID 		*oidArray;	/* array of ObjectIDs */
    BtreeLeaf 		*apage;		/* pointer to a buffer holding a leaf page */
    BtreeOverflow 	*opage;		/* pointer to a buffer holding an overflow page */
    btm_LeafEntry 	*entry;		/* pointer to a leaf entry */    
    
    
    TR_PRINT(TR_BTM, TR1,
             ("btm_FetchNext(handle, kdesc=%P, kval=%P, compOp=%ld, current=%P, next=%P)",
	      kdesc, kval, compOp, current, next));
    
    
    /*@ Copy the current cursor to the next cursor. */
    *next = *current;
    
    if (compOp == SM_EQ || compOp == SM_LT || compOp == SM_LE || compOp == SM_EOF) { /* forward scan */
	
	if (IS_NILPAGEID(next->overflow)) { /* normal entry */
	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
	    
	    /* Go to the right ObjectID. */
	    next->oidArrayElemNo ++;
	    
	    if (next->oidArrayElemNo < entry->nObjects) {
		oidArray = (ObjectID*)&entry->kval[ALIGNED_LENGTH(entry->klen)];
		next->oid = oidArray[next->oidArrayElemNo];
		next->flag = CURSOR_ON;

		/*@ free the page */
		e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		return(eNOERROR);
	    }
	    
	} else {		/* overflow page */

	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    /* Go to the right item. */
	    next->oidArrayElemNo++;
	    
	    if (next->oidArrayElemNo >= opage->hdr.nObjects && opage->hdr.nextPage != NIL) {
		/* Go to the right overflow page. */
		MAKE_PAGEID(overflow, next->overflow.volNo, opage->hdr.nextPage);

		/*@ free the page */
		e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		next->overflow = overflow;

		/* get the page */
		e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		next->oidArrayElemNo = 0;
	    }
	    
	    if (next->oidArrayElemNo < opage->hdr.nObjects) {
		next->oid = opage->oid[next->oidArrayElemNo];
		next->flag = CURSOR_ON;

	    } else
		next->flag = CURSOR_INVALID;

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    if (next->flag == CURSOR_ON) return(eNOERROR);

	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);	    
	}
	
	/* At this point, notice that we should move to the right slot. */
	
	if (compOp == SM_EQ) {
	    next->flag = CURSOR_EOS;

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    return(eNOERROR);
	}
	
	/* Go to the right leaf entry. */
	next->slotNo++;
	
	if (next->slotNo >= apage->hdr.nSlots && apage->hdr.nextPage != NIL) {
	    /* Go to the right leaf page. */
	    MAKE_PAGEID(leaf, next->leaf.volNo, apage->hdr.nextPage);

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    next->leaf = leaf;

	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    next->slotNo = 0;	    
	}
	
	if (next->slotNo < apage->hdr.nSlots) {
	    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
	    alignedKlen = ALIGNED_LENGTH(entry->klen);
	    
	    if (compOp != SM_EOF) {
		/* Check the boundary condition. */
		cmp = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);
		if (cmp == GREAT || (compOp == SM_LT && cmp == EQUAL)) {
		    /*@ free the page */
		    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
		    if (e < 0) ERR(handle, e);
		    
		    next->flag = CURSOR_EOS;
		    return(eNOERROR);
		}
	    }
	    
	    if (entry->nObjects < 0) { /* overflow page */
		MAKE_PAGEID(next->overflow, next->leaf.volNo,
			    *((ShortPageID*)&entry->kval[alignedKlen]));

		/*@ get the page */
		e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
		next->oidArrayElemNo = 0;
		next->oid = opage->oid[next->oidArrayElemNo];

		/*@ free the page */
		e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
	    } else {		/* normal entry */
		MAKE_PAGEID(next->overflow, next->leaf.volNo, NIL);
		next->oidArrayElemNo = 0;
		next->oid = *((ObjectID*)&entry->kval[alignedKlen]);
	    }
	    
	    memcpy((char*)&next->key, (char*)&entry->klen, entry->klen+sizeof(Two));
	    next->flag = CURSOR_ON;
	    
	} else
	    next->flag = CURSOR_EOS;

	/*@ free the page */
	e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
    } else {			/* SM_GT, SM_GE, or SM_BOF : backward scan */
	
	if (IS_NILPAGEID(next->overflow)) { /* normal entry */
	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    entry = (btm_LeafEntry*)&apage->data[apage->slot[-next->slotNo]];
	    
	    /* Go to the left ObjectID. */
	    next->oidArrayElemNo --;
	    
	    if (next->oidArrayElemNo >= 0) {
		oidArray = (ObjectID*)&entry->kval[ALIGNED_LENGTH(entry->klen)];
		next->oid = oidArray[next->oidArrayElemNo];
		next->flag = CURSOR_ON;

		/*@ free the page */
		e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		return(eNOERROR);
	    }
	    
	} else {
	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    /* Go to the left item. */
	    next->oidArrayElemNo--;
	    
	    if (next->oidArrayElemNo < 0 && opage->hdr.prevPage != NIL) {
		/* Go to the left overflow page. */
		MAKE_PAGEID(overflow, next->overflow.volNo, opage->hdr.prevPage);

		/*@ free the page */
		e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		next->overflow = overflow;

		/*@ get the page */
		e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		next->oidArrayElemNo = opage->hdr.nObjects - 1; /* last element */
	    }
	    
	    if (next->oidArrayElemNo >= 0) {
		next->oid = opage->oid[next->oidArrayElemNo];
		next->flag = CURSOR_ON;
	    } else
		next->flag = CURSOR_INVALID;

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    if (next->flag == CURSOR_ON) return(eNOERROR);
	    
	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	}
	
	/* At this point, notice that we should move to the left slot. */
	
	/* Go to the left leaf entry. */
	next->slotNo--;
	
	if (next->slotNo < 0 && apage->hdr.prevPage != NIL) {
	    /* Go to the right leaf page. */
	    MAKE_PAGEID(leaf, next->leaf.volNo, apage->hdr.prevPage);

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    next->leaf = leaf;

	    /*@ get the page */
	    e = BfM_GetTrain(handle, &next->leaf, (char**)&apage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    next->slotNo = apage->hdr.nSlots - 1; /* last slot */
	}
	
	if (next->slotNo >= 0) {
	    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-next->slotNo]]);
	    alignedKlen = ALIGNED_LENGTH(entry->klen);
	    
	    if (compOp != SM_BOF) {
		/* Check the boundary condition. */
		cmp = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);
		if (cmp == LESS || (compOp == SM_GT && cmp == EQUAL)) {
		    /*@ free the page */
		    e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
		    if (e < 0) ERR(handle, e);
		    
		    next->flag = CURSOR_EOS;
		    return(eNOERROR);
		}
	    }
	    
	    if (entry->nObjects < 0) { /* overflow page */		
		MAKE_PAGEID(next->overflow, next->leaf.volNo,
			    *((ShortPageID*)&entry->kval[alignedKlen]));

		/*@ get the page */
		e = BfM_GetTrain(handle, &next->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
		while (opage->hdr.nextPage != NIL) {
		    MAKE_PAGEID(overflow, next->overflow.volNo, opage->hdr.nextPage );

		    /*@ free the page */
		    e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		    
		    next->overflow = overflow;
		    
		    e = BfM_GetTrain(handle, &next->overflow, (char**)&apage, PAGE_BUF);
		    if (e  < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		}
		
		next->oidArrayElemNo = opage->hdr.nObjects - 1;
		next->oid = opage->oid[next->oidArrayElemNo];

		/*@ free the page */
		e = BfM_FreeTrain(handle, &next->overflow, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &next->leaf, PAGE_BUF);
		
	    } else {		/* normal entry */
		MAKE_PAGEID(next->overflow, next->leaf.volNo, NIL);
		next->oidArrayElemNo = entry->nObjects - 1;
		oidArray =(ObjectID*)&entry->kval[alignedKlen];
		next->oid = oidArray[next->oidArrayElemNo];
	    }
	    
	    memcpy((char*)&next->key, (char*)&entry->klen, entry->klen+sizeof(Two));	    
	    next->flag = CURSOR_ON;
	    
	} else
	    next->flag = CURSOR_EOS;

	/*@ free the page */
	e = BfM_FreeTrain(handle, &next->leaf, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);
    
} /* btm_FetchNext() */



/*@================================
 * btm_FetchObjectIdOverflow()
 *================================*/
/*
 * Function: Four btm_FetchObjectIdInOverflow(BtreeCursor*, Four, Boolean*)
 *
 * Description:
 *  Fetch the next ObjectID from the overflow page list. If the next ObjectID
 *  is found, then 'nextFound' will be set to TRUE. Otherwise 'nextFound' will
 *  be set to FALSE, and the cursor will point to the last ObjectID of the
 *  overflow page list in the scan order; that is, the first ObjectID of the
 *  first overflow page if backward scan, the last ObjectID of the last
 *  overflow page if forward scan.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter cursor: points to the next ObjectID if possible
 *                       points to the last ObjectID in the scan order otherwise
 *  2) parameter nextFound: TRUE if the next ObjectID is fetched
 *                          FALSE otherwise
 */
Four btm_FetchObjectIdInOverflow(
    Four handle,
    BtreeCursor 	*cursor,	/* INOUT btree cursor */
    Four     		compOp,		/* IN comparison operator of stop condition */
    Boolean  		*nextFound)	/* OUT TRUE if the next ObjectID is fetched */
{
    Four 		e;		/* error number */
    Four 		cmp;		/* comparison result */
    Two 		oidArrayElemNo;	/* element no. of array of ObjectIDs */
    PageID 		overflow;	/* temporary PageID of an overflow page */
    Boolean 		found;		/* search result */
    BtreeOverflow 	*opage;		/* pointer to a buffer holding an overflow page */


    TR_PRINT(TR_BTM, TR1,
             ("btm_FetchObjectIdInOverflow(handle, cursor=%P, compOp=%P, found=%P)",
	      cursor, compOp, found));

    /*@ get the page */
    e = BfM_GetTrain(handle, &cursor->overflow, (char**)&opage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (cursor->oidArrayElemNo >= opage->hdr.nObjects)
	cursor->oidArrayElemNo = opage->hdr.nObjects - 1;
    
    cmp = btm_ObjectIdComp(handle, &opage->oid[cursor->oidArrayElemNo], &cursor->oid);
    
    if (compOp == SM_EQ || compOp == SM_LT || compOp == SM_LE || compOp == SM_EOF) { /* forward scan */

	if (cmp == EQUAL || cmp == LESS) { /* we should move forward. */
	    for (;;) {
		cmp = btm_ObjectIdComp(handle, &opage->oid[opage->hdr.nObjects-1], &cursor->oid);
		if (cmp == GREAT) break;

		if (opage->hdr.nextPage == NIL) break;

		MAKE_PAGEID(overflow, cursor->overflow.volNo, opage->hdr.nextPage);

		/*@ free the page */
		e = BfM_FreeTrain(handle, &cursor->overflow, PAGE_BUF);
		if (e < 0) ERR(handle, e);

		cursor->overflow = overflow;

		/*@ get the page */
		e = BfM_GetTrain(handle, &cursor->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
	    }
	} else {		/* cmp == GREAT; we should move backward. */
	    for (;;) {
		cmp = btm_ObjectIdComp(handle, &opage->oid[0], &cursor->oid);
		if (cmp != GREAT) break;

		if (opage->hdr.prevPage == NIL) break;

		MAKE_PAGEID(overflow, cursor->overflow.volNo, opage->hdr.prevPage);

		/*@ free the page */
		e = BfM_FreeTrain(handle, &cursor->overflow, PAGE_BUF);
		if (e < 0) ERR(handle, e);

		cursor->overflow = overflow;

		/*@ get the page */
		e = BfM_GetTrain(handle, &cursor->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
	    }
	}
	
	(Boolean)btm_BinarySearchOidArray(handle, &opage->oid[0], &cursor->oid,
					  opage->hdr.nObjects, &oidArrayElemNo);
	cursor->oidArrayElemNo = oidArrayElemNo + 1;
	    
	if (cursor->oidArrayElemNo >= opage->hdr.nObjects) {
	    cursor->oidArrayElemNo = opage->hdr.nObjects-1;
	    *nextFound = FALSE;
	} else
	    *nextFound = TRUE;
	
    } else {			/* SM_GT, SM_GE, or SM_BOF: backward scan */

	if (cmp == EQUAL || cmp == GREAT) { /* we should move backward. */
	    for (;;) {
		cmp = btm_ObjectIdComp(handle, &opage->oid[0], &cursor->oid);
		if (cmp == LESS) break;

		if (opage->hdr.prevPage == NIL) break;

		MAKE_PAGEID(overflow, cursor->overflow.volNo, opage->hdr.prevPage);

		/*@ free the page */
		e = BfM_FreeTrain(handle, &cursor->overflow, PAGE_BUF);
		if (e < 0) ERR(handle, e);

		cursor->overflow = overflow;

		/*@ get the page */
		e = BfM_GetTrain(handle, &cursor->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
	    }
	} else {		/* cmp == LESS; we should move forward. */
	    for (;;) {
		cmp = btm_ObjectIdComp(handle, &opage->oid[opage->hdr.nObjects-1], &cursor->oid);
		if (cmp != LESS) break;

		if (opage->hdr.nextPage == NIL) break;

		MAKE_PAGEID(overflow, cursor->overflow.volNo, opage->hdr.nextPage);

		/*@ free the page */
		e = BfM_FreeTrain(handle, &cursor->overflow, PAGE_BUF);
		if (e < 0) ERR(handle, e);

		cursor->overflow = overflow;

		/*@ get the page */
		e = BfM_GetTrain(handle, &cursor->overflow, (char**)&opage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
	    }
	}
	
	found = btm_BinarySearchOidArray(handle, &opage->oid[0], &cursor->oid,
					  opage->hdr.nObjects, &oidArrayElemNo);
	if (found) cursor->oidArrayElemNo = oidArrayElemNo - 1;
	else cursor->oidArrayElemNo = oidArrayElemNo;
	    
	if (cursor->oidArrayElemNo < 0) {
	    cursor->oidArrayElemNo = 0;
	    *nextFound = FALSE;
	} else
	    *nextFound = TRUE;
    }
	
    cursor->oid = opage->oid[cursor->oidArrayElemNo];
    cursor->flag = CURSOR_ON;

    /*@ free the page */
    e = BfM_FreeTrain(handle, &cursor->overflow, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* btm_FetchObjectId() */
    
    
    
