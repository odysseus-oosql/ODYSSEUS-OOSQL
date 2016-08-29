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
 * Module: BtM_Fetch.c
 *
 * Description :
 *  Find the first object satisfying the given condition.
 *  If there is no such object, then return with 'flag' field of cursor set
 *  to CURSOR_EOS. If there is an object satisfying the condition, then cursor
 *  points to the object position in the B+ tree and the object identifier
 *  is returned via 'cursor' parameter.
 *  The condition is given with a key value and a comparison operator;
 *  the comparison operator is one among SM_BOF, SM_EOF, SM_EQ, SM_LT, SM_LE, SM_GT, SM_GE.
 *
 * Exports:
 *  Four BtM_Fetch(PageID*, KeyDesc*, KeyValue*, Four, KeyValue*, Four, BtreeCursor*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ Internal Function Prototypes */
Four btm_Fetch(Four, PageID*, KeyDesc*, KeyValue*, Four, KeyValue*, Four, BtreeCursor*);



/*@================================
 * BtM_Fetch()
 *================================*/
/*
 * Function: Four BtM_Fetch(PageID*, KeyDesc*, KeyVlaue*, Four, KeyValue*, Four, BtreeCursor*)
 *
 * Description:
 *  Find the first object satisfying the given condition. See above for detail.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_BTM
 *    eBADCOMPOP_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 *  cursor  : The found ObjectID and its position in the Btree Leaf
 *            (it may indicate a ObjectID in an  overflow page).
 */
Four BtM_Fetch(
    Four handle,
    PageID   *root,			/* IN The current root of the subtree */
    KeyDesc  *kdesc,		/* IN Btree key descriptor */
    KeyValue *startKval,	/* IN key value of start condition */
    Four     startCompOp,	/* IN comparison operator of start condition */
    KeyValue *stopKval,		/* IN key value of stop condition */
    Four     stopCompOp,	/* IN comparison operator of stop condition */
    BtreeCursor *cursor)	/* OUT Btree Cursor */
{
    Four e;		   /* error number */

    
    TR_PRINT(TR_BTM, TR1,
             ("BtM_Fetch(handle, root=%P, kdesc=%P, startKval=%P, startCompOp=%ld, stopKval=%P, stopCompOp=%ld, cursor=%P)",
	      root, kdesc, startKval, startCompOp, stopKval, stopCompOp, cursor));

    
    if (root == NULL) ERR(handle, eBADPARAMETER_BTM);

    if (startCompOp == SM_BOF) {
	/* Return the first object of the B$^+$ tree. */
	e = btm_FirstObject(handle, root, kdesc, stopKval, stopCompOp, cursor);
	if (e < 0) ERR(handle, e);
	
    } else if (startCompOp == SM_EOF) {
	/* Return the last object of the B$^+$ tree. */
	e = btm_LastObject(handle, root, kdesc, stopKval, stopCompOp, cursor);
	if (e < 0) ERR(handle, e);
	
    } else { /* SM_EQ, SM_LT, SM_LE, SM_GT, SM_GE */
	
	e = btm_Fetch(handle, root, kdesc, startKval, startCompOp, stopKval, stopCompOp, cursor);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);

} /* BtM_Fetch() */



/*@================================
 * btm_Fetch()
 *================================*/
/*
 * Function: Four btm_Fetch(PageID*, KeyDesc*, KeyVlaue*, Four, KeyValue*, Four, BtreeCursor*)
 *
 * Description:
 *  Find the first object satisfying the given condition.
 *  This function handles only the following conditions:
 *  SM_EQ, SM_LT, SM_LE, SM_GT, SM_GE.
 *
 * Returns:
 *  Error code *   
 *    eBADstartCompOp_BTM
 *    eBADPARAMETER_BTM
 *    some errors caused by function calls
 */
Four btm_Fetch(
    Four handle,
    PageID              *root,          /* IN The current root of the subtree */
    KeyDesc             *kdesc,         /* IN Btree key descriptor */
    KeyValue            *startKval,     /* IN key value of start condition */
    Four                startCompOp,    /* IN comparison operator of start condition */
    KeyValue            *stopKval,      /* IN key value of stop condition */
    Four                stopCompOp,     /* IN comparison operator of stop condition */
    BtreeCursor         *cursor)        /* OUT Btree Cursor */
{
    Four                e;              /* error number */
    Four                cmp;            /* result of comparison */
    Two                 idx;            /* index */
    PageID              child;          /* child page when the root is an internla page */
    Two                 alignedKlen;    /* aligned size of the key length */
    BtreePage           *apage;         /* a Page Pointer to the given root */
    BtreeOverflow       *opage;         /* a page pointer if it necessary to access an overflow page */
    Boolean             found;          /* search result */
    PageID              *leafPid;       /* leaf page pointed by the cursor */
    Two                 slotNo;         /* slot pointed by the slot */
    PageID              ovPid;          /* PageID of the overflow page */
    PageNo              ovPageNo;       /* PageNo of the overflow page */
    PageID              prevPid;        /* PageID of the previous page */
    PageID              nextPid;        /* PageID of the next page */
    ObjectID            *oidArray;      /* array of the ObjectIDs */
    Two                 iEntryOffset;   /* starting offset of an internal entry */
    btm_InternalEntry   *iEntry;        /* an internal entry */
    Two                 lEntryOffset;   /* starting offset of a leaf entry */
    btm_LeafEntry       *lEntry;        /* a leaf entry */


    TR_PRINT(TR_BTM, TR1,
             ("btm_Fetch(handle, root=%P, kdesc=%P, startKval=%P, startCompOp=%ld, stopKval=%P, stopCompOp=%ld, cursor=%P)",
	      root, kdesc, startKval, startCompOp, stopKval, stopCompOp, cursor));


    /*@ get the page */
    e = BfM_GetTrain(handle, root, (char **)&apage, PAGE_BUF);
    if (e < 0)  ERR(handle, e);

    /* bluechry test ... */
    if (root->volNo == 1000 && root->pageNo == 165) {
    	printf("### [pID=%d, tID=%d] Get TreePage: [%d, %d] at %x\n", procIndex, handle, root->volNo, root->pageNo, apage);
     	printf("### [pID=%d, tID=%d] TreePage Type: %x, %x\n", procIndex, handle, ((Page*)apage)->header.flags, apage->any.hdr.type);
    fflush(stdout);
    }
    /* ... bluechry test */

   
    if (apage->any.hdr.type & INTERNAL) {
	/*@ Find the child page by using binary search routine. */
	(Boolean) btm_BinarySearchInternal(handle, &(apage->bi), kdesc, startKval, &idx);
	
	/*@ Get the child page */
	if (idx >= 0) {
	    iEntryOffset = apage->bi.slot[-idx];
	    iEntry = (btm_InternalEntry*)&(apage->bi.data[iEntryOffset]);
	    MAKE_PAGEID(child, root->volNo, iEntry->spid);
	} else
	    MAKE_PAGEID(child, root->volNo, apage->bi.hdr.p0);

	/*@ recursive call */
	/* Recursively call itself by using the child */
	e = BtM_Fetch(handle, &child, kdesc, startKval, startCompOp, stopKval, stopCompOp, cursor);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	/*@ free the page */
	e = BfM_FreeTrain(handle, root, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
    } else if (apage->any.hdr.type & LEAF) {
	/* Search the leaf item which has the given key value. */
	found = btm_BinarySearchLeaf(handle, &(apage->bl), kdesc, startKval, &idx);

	leafPid = root;		/* set the current leaf page to root */
	slotNo = idx;		/* set the current slotNo to idx */
	
	switch(startCompOp) {
	  case SM_EQ:
	    if (!found) {
		e = BfM_FreeTrain(handle, root, PAGE_BUF);
		if (e < 0) ERR(handle, e);

		cursor->flag = CURSOR_EOS;
		return(eNOERROR);
	    }
	    break;

	  case SM_LT:
	  case SM_LE:
	    if (startCompOp == SM_LT && found) /* use the left slot */
		slotNo--;
	    
	    if (slotNo < 0) {
		/* use the left page */
		/*@ get the page id */
		MAKE_PAGEID(prevPid, root->volNo, apage->bl.hdr.prevPage);

		/*@ free the page */
		e = BfM_FreeTrain(handle, root, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		if (prevPid.pageNo == NIL) {
		    cursor->flag = CURSOR_EOS;
		    return(eNOERROR);
		}
		
		/* read the left page */
		/*@ get the page */
		e = BfM_GetTrain(handle, &prevPid, (char **)&apage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		slotNo = apage->bl.hdr.nSlots - 1;
		leafPid = &prevPid;
	    }

	    break;

	  case SM_GT:
	  case SM_GE:
	    if (startCompOp == SM_GT || !found) {
		/* use the right slot */
		slotNo++;

		if (slotNo >= apage->bl.hdr.nSlots) {		
		    /* use the right page */
		    
		    MAKE_PAGEID(nextPid, root->volNo, apage->bl.hdr.nextPage);
		    
		    e = BfM_FreeTrain(handle, root, PAGE_BUF);
		    if (e < 0) ERR(handle, e);
		    
		    if (nextPid.pageNo == NIL) {
			cursor->flag = CURSOR_EOS;
			return(eNOERROR);
		    }
		    
		    /* read the right page */
		    /*@ get the page */
		    e = BfM_GetTrain(handle, &nextPid, (char**)&apage, PAGE_BUF);
		    if (e < 0) ERR(handle, e);
		    
		    slotNo = 0;
		    leafPid = &nextPid;
		}
	    }
	    
	    break;

	  default:
	    ERRB1(handle, eBADCOMPOP_BTM, root, PAGE_BUF);
	}


	/* Construct a cursor for successive access */
	cursor->leaf = *leafPid;
	cursor->slotNo = slotNo;

	lEntryOffset = apage->bl.slot[-(cursor->slotNo)];
	lEntry = (btm_LeafEntry*)&(apage->bl.data[lEntryOffset]);
	alignedKlen = ALIGNED_LENGTH(lEntry->klen);

	cursor->key.len = lEntry->klen;
	memcpy(&(cursor->key.val[0]), &(lEntry->kval[0]), cursor->key.len);

	if (stopCompOp != SM_BOF && stopCompOp != SM_EOF) {
	    cmp = btm_KeyCompare(handle, kdesc, &cursor->key, stopKval);

	    if (cmp == EQUAL && (stopCompOp == SM_LT || stopCompOp == SM_GT) ||
		cmp == LESS && (stopCompOp == SM_GT || stopCompOp == SM_GE) ||
		cmp == GREAT && (stopCompOp == SM_LT || stopCompOp == SM_LE)) {

		cursor->flag = CURSOR_EOS;

		e = BfM_FreeTrain(handle, leafPid, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		return(eNOERROR);
	    }	    
	}
	
	if (lEntry->nObjects < 0) { /* entry having overflow page */
	    ovPageNo =  *((ShortPageID*)&(lEntry->kval[alignedKlen]));
	    MAKE_PAGEID(ovPid, root->volNo, ovPageNo);

	    /*@ get the page */
	    e = BfM_GetTrain(handle, &ovPid, (char **)&opage, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);

	    if (startCompOp == SM_LT || startCompOp == SM_LE) {
		/* use the last ObjectID of this entry */
		
		while (opage->hdr.nextPage != NIL) {
		    ovPageNo = opage->hdr.nextPage;

		    /*@ free the page */
		    e = BfM_FreeTrain(handle, &ovPid, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);

		    MAKE_PAGEID(ovPid, root->volNo, ovPageNo);

		    /*@ get the page */
		    e = BfM_GetTrain(handle, &ovPid, (char **)&opage, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);
		}

		cursor->oidArrayElemNo = opage->hdr.nObjects - 1;
		
	    } else {
		cursor->oidArrayElemNo = 0;
	    }
	    
	    cursor->overflow = ovPid;
	    cursor->oid = opage->oid[cursor->oidArrayElemNo];

	    /*@ free the page */
	    e = BfM_FreeTrain(handle, &ovPid, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);

	} else {		/* a normal entry */
	    oidArray = (ObjectID*)&(lEntry->kval[alignedKlen]);

	    if (startCompOp == SM_LT || startCompOp == SM_LE)
		cursor->oidArrayElemNo = lEntry->nObjects - 1;
	    else
		cursor->oidArrayElemNo = 0;

	    MAKE_PAGEID(cursor->overflow, root->volNo, NIL);
	    cursor->oid = oidArray[cursor->oidArrayElemNo];
	}
        
	/*@ free the page */
	e = BfM_FreeTrain(handle, leafPid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* The B+ tree cursor is valid. */
	cursor->flag = CURSOR_ON;
    } else {
        /* bluechry test ... */
        printf("### [pID=%d, tID=%d] Error BtreePID: [%d, %d] at %x\n", procIndex, handle, root->volNo, root->pageNo, apage);
    	printf("### [pID=%d, tID=%d] Error PageType: %x, %x\n", procIndex, handle, ((Page*)apage)->header.flags, apage->any.hdr.type);
        fflush(stdout);
        /* ... bluechry test */

	ERRB1(handle, eBADBTREEPAGE_BTM, root, PAGE_BUF); 
    }
    	    
    return(eNOERROR);
    
} /* btm_Fetch() */

