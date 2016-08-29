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
 * Module: btm_Split.c
 *
 * Description : 
 *  This file has three functions about 'split'.
 *  'btm_SplitInternal(...) and btm_SplitLeaf(...) insert the given item
 *  after spliting, and return 'ritem' which should be inserted into the
 *  parent page.
 *
 * Exports:
 *  Four btm_SplitInternal(ObjectID*, BtreeInternal*, Two, InternalItem*, InternalItem*)
 *  Four btm_SplitLeaf(ObjectID*, PageID*, BtreeLeaf*, Two, LeafItem*, InternalItem*)
 *  Four btm_SplitOverflow(ObjectID*, PageID*, BtreeOverflow*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_SplitInternal()
 *================================*/
/*
 * Function: Four btm_SplitInternal(ObjectID*, BtreeInternal*,Two, InternalItem*, InternalItem*)
 *
 * Description:
 *  At first, the function btm_SplitInternal(...) allocates a new internal page
 *  and initialize it.  Secondly, all items in the given page and the given
 *  'item' are divided by halves and stored to the two pages.  By spliting,
 *  the new internal item should be inserted into their parent and the item will
 *  be returned by 'ritem'.
 *
 *  A temporary page is used because it is difficult to use the given page
 *  directly and the temporary page will be copied to the given page later.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Note:
 *  The caller should call BfM_SetDirty() for 'fpage'.
 */
Four btm_SplitInternal(
    Four handle,
    ObjectID                    *catObjForFile,         /* IN catalog object of B+ tree file */
    BtreeInternal               *fpage,                 /* INOUT the page which will be splitted */
    Two                         high,                   /* IN slot No. for the given 'item' */
    InternalItem                *item,                  /* IN the item which will be inserted */
    InternalItem                *ritem)                 /* OUT the item which will be returned by spliting */
{
    Four                        e;                      /* error number */
    Two                         i;                      /* slot No. in the given page, fpage */
    Two                         j;                      /* slot No. in the splitted pages */
    Two                         k;                      /* slot No. in the new page */
    Two                         maxLoop;                /* # of max loops; # of slots in fpage + 1 */
    Four                        sum;                    /* the size of a filled area */
    Boolean                     flag=FALSE;             /* TRUE if 'item' become a member of fpage */
    PageID                      newPid;                 /* for a New Allocated Page */
    BtreeInternal               *npage;                 /* a page pointer for the new allocated page */
    Two                         fEntryOffset;           /* starting offset of an entry in fpage */
    Two                         nEntryOffset;           /* starting offset of an entry in npage */
    Two                         entryLen;               /* length of an entry */
    btm_InternalEntry           *fEntry;                /* internal entry in the given page, fpage */
    btm_InternalEntry           *nEntry;                /* internal entry in the new page, npage*/
    Boolean                     isTmp;                  


    TR_PRINT(TR_BTM, TR1,
             ("btm_SplitInternal(handle, catObjForFile=%P, fpage=%P, high=%ld, item=%P, ritem=%P)",
	      catObjForFile, fpage, high, item, ritem));
    
    /*@ Allocate a new page and initialize it as an internal page */
    e = btm_AllocPage(handle, catObjForFile, (PageID *)&fpage->hdr.pid, &newPid); 
    if (e < 0) ERR(handle, e);

    /* check this B-tree is temporary */
    e = btm_IsTemporary(handle, catObjForFile, &isTmp); 
    if (e < 0) ERR(handle, e);
    
    e = btm_InitInternal(handle, &newPid, FALSE, isTmp);
    if (e < 0) ERR(handle, e);
    
    e = BfM_GetNewTrain(handle,  &newPid, (char **)&npage, PAGE_BUF );
    if (e < 0) ERR(handle, e);

    /* loop until 'sum' becomes greater than BI_HALF */
    /* j : loop counter, maximum loop count = # of old Slots and a new slot */
    /* i : slot No. variable of fpage */
    maxLoop = fpage->hdr.nSlots+1;
    for (sum = 0, i = 0, j = 0; j < maxLoop && sum < BI_HALF; j++) {
	if (j == high+1) {	/* use the given 'item' */
	    entryLen = sizeof(ShortPageID) +
		ALIGNED_LENGTH(sizeof(Two)+item->klen);

	    flag = TRUE;

	} else {
	    fEntryOffset = fpage->slot[-i];
	    fEntry = (btm_InternalEntry*)&(fpage->data[fEntryOffset]);
	    entryLen = sizeof(ShortPageID) +
		ALIGNED_LENGTH(sizeof(Two)+fEntry->klen);
	    
	    i++;		/* increment the slot no. */
	}

	sum += entryLen + sizeof(Two);
    }

    /* i-th old entries are to be remained in 'fpage' */
    fpage->hdr.nSlots = i;

    /*@ fill the new page */
    /* i : slot no. of fpage, continued from the above loop */
    /* j : loop counter, continued from the above loop */
    /* k : slot No. of new page, npage */
    /* (k == -1; special case, constructing 'ritem' */
    for (k = -1; j < maxLoop; j++, k++) {

	if (k == -1) {		/* Construct 'ritem' */
	    /* We handle the InternalEntry as btm_InternalEntry. */
	    /* So two their data structures should consider this situation. */
	    nEntry = (btm_InternalEntry*)ritem;	    
	} else {
	    nEntryOffset = npage->slot[-k] = npage->hdr.free;
	    nEntry = (btm_InternalEntry*)&(npage->data[nEntryOffset]);
	}
	
	if (j == high+1) { /* use the given 'item' */
	    nEntry->spid = item->spid;
	    nEntry->klen = item->klen;
	    memcpy(&(nEntry->kval[0]), &(item->kval[0]), nEntry->klen);
	    entryLen = sizeof(ShortPageID) +
		ALIGNED_LENGTH(sizeof(Two)+nEntry->klen);
	} else {
	    fEntryOffset = fpage->slot[-i];
	    fEntry = (btm_InternalEntry*)&(fpage->data[fEntryOffset]);
	    entryLen = sizeof(ShortPageID) +
		ALIGNED_LENGTH(sizeof(Two)+fEntry->klen);

	    memcpy((char*)nEntry, (char*)fEntry, entryLen);

	    /* free the space from the fpage */
	    if (fEntryOffset + entryLen == fpage->hdr.free)
		fpage->hdr.free -= entryLen;
	    else
		fpage->hdr.unused += entryLen;

	    i++;		/* increment the slot No. */
	}

	if (k == -1) {		/* Construct 'ritem' */
	    /* In this case nEntry points to ritem. */
	    npage->hdr.p0 = nEntry->spid;
	    ritem->spid = newPid.pageNo;
	} else {
	    npage->hdr.free += entryLen;
	}
    }

    npage->hdr.nSlots = k;

    /*@ adjust 'fpage' */
    /* if the given item is to be stored in 'fpage', place it in 'fpage' */
    if (flag) {

	entryLen = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two)+item->klen);
	
	if (BI_CFREE(fpage) < entryLen + sizeof(Two))
	    btm_CompactInternalPage(handle, fpage, NIL);

	/* Empty the (high+1)-th slot */
	for (i = fpage->hdr.nSlots-1; i >= high+1; i--)
	    fpage->slot[-(i+1)] = fpage->slot[-i];
		
	fEntryOffset = fpage->slot[-(high+1)] = fpage->hdr.free;
	fEntry = (btm_InternalEntry*)&(fpage->data[fEntryOffset]);
	
	fEntry->spid = item->spid;
	fEntry->klen = item->klen;
	memcpy(&(fEntry->kval[0]), &(item->kval[0]), fEntry->klen);

	fpage->hdr.free += entryLen;
	fpage->hdr.nSlots++;
    }
    
    /* If the given page was a root, it is not a root any more */
    if (fpage->hdr.type & ROOT) fpage->hdr.type = INTERNAL;

    /* bluechry test ... */
    if (fpage->hdr.type & ROOT) { 
	if (fpage->hdr.pid.volNo == 1000 && fpage->hdr.pid.pageNo == 165) {
            printf("### [pID=%d, tID=%d] Set Internal Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, fpage);
            printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
            fflush(stdout);
	}
    }
    /* ... bluechry test */
    
    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &newPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_SplitInternal() */



/*@================================
 * btm_SplitLeaf()
 *================================*/
/*
 * Function: Four btm_SplitLeaf(ObjectID*, PageID*, BtreeLeaf*, Two, LeafItem*, InternalItem*)
 *
 * Description: 
 *  The function btm_SplitLeaf(...) is similar to btm_SplitInternal(...) except
 *  that the entry of a leaf differs from the entry of an internal and the first
 *  key value of a new page is used to make an internal item of their parent.
 *  Internal pages do not maintain the linked list, but leaves do it, so links
 *  are properly updated.
 *
 * Returns:
 *  Error code
 *    eDUPLICATEDKEY_BTM
 *    some errors caused by function calls
 *
 * Note:
 *  The caller should call BfM_SetDirty() for 'fpage'.
 */
Four btm_SplitLeaf(
    Four handle,
    ObjectID                    *catObjForFile, /* IN catalog object of B+ tree file */
    PageID                      *root,          /* IN PageID for the given page, 'fpage' */
    BtreeLeaf                   *fpage,         /* INOUT the page which will be splitted */
    Two                         high,           /* IN slotNo for the given 'item' */
    LeafItem                    *item,          /* IN the item which will be inserted */
    InternalItem                *ritem)         /* OUT the item which will be returned by spliting */
{
    Four                        e;              /* error number */
    Two                         i;              /* slot No. in the given page, fpage */
    Two                         j;              /* slot No. in the splitted pages */
    Two                         k;              /* slot No. in the new page */
    Two                         maxLoop;        /* # of max loops; # of slots in fpage + 1 */
    Four                        sum;            /* the size of a filled area */
    PageID                      newPid;         /* for a New Allocated Page */
    PageID                      nextPid;        /* for maintaining doubly linked list */
    BtreeLeaf                   tpage;          /* a temporary page for the given page */
    BtreeLeaf                   *npage;         /* a page pointer for the new page */
    BtreeLeaf                   *mpage;         /* for doubly linked list */
    btm_LeafEntry               *itemEntry;     /* entry for the given 'item' */
    btm_LeafEntry               *fEntry;        /* an entry in the given page, 'fpage' */
    btm_LeafEntry               *nEntry;        /* an entry in the new page, 'npage' */
    ObjectID                    *iOidArray;     /* ObjectID array of 'itemEntry' */
    ObjectID                    *fOidArray;     /* ObjectID array of 'fEntry' */
    Two                         fEntryOffset;   /* starting offset of 'fEntry' */
    Two                         nEntryOffset;   /* starting offset of 'nEntry' */
    Two                         oidArrayNo;     /* element No in an ObjectID array */
    Two                         alignedKlen;    /* aligned length of the key length */
    Two                         itemEntryLen;   /* length of entry for item */
    Two                         entryLen;       /* entry length */
    Boolean                     flag;
    Boolean                     isTmp;          
 
    
    TR_PRINT(TR_BTM, TR1,
             ("btm_SplitLeaf(handle, catObjForFile=%P, root=%P, fpage=%P, high=%ld, item=%P, ritem=%P)",
	      catObjForFile, root, fpage, high, item, ritem));

    /*
    ** To handle 'item' uniformly without considering whether 'item' is a new
    ** entry or an ObjectID is inserted, we copy the entry for 'item' into
    ** 'tpage'. If an ObjectID is inserted, the corresponding entry is moved
    *  from the fpage into the 'tpage' and it is deleted from the 'fpage'.
    */
    itemEntry = (btm_LeafEntry*)&(tpage.data[0]);
    if (item->nObjects == 0) {	/* a new entry */

	alignedKlen = ALIGNED_LENGTH(item->klen);
	itemEntry->nObjects = 1;
	itemEntry->klen = item->klen;	
	memcpy(&(itemEntry->kval[0]), &(item->kval[0]), itemEntry->klen);
	memcpy(&(itemEntry->kval[alignedKlen]), (char*)&(item->oid), OBJECTID_SIZE);
	
	itemEntryLen = BTM_LEAFENTRY_FIXED + alignedKlen + OBJECTID_SIZE;
	
    } else if (item->nObjects > 0) { /* an ObjectID is inserted */
	
	fEntryOffset = fpage->slot[-(high+1)];
	fEntry = (btm_LeafEntry*)&(fpage->data[fEntryOffset]);
	alignedKlen = ALIGNED_LENGTH(fEntry->klen);
	fOidArray = (ObjectID*)&(fEntry->kval[alignedKlen]);
	
	e = btm_BinarySearchOidArray(handle, fOidArray, &(item->oid),
				     fEntry->nObjects, &oidArrayNo);
	if (e == TRUE) ERR(handle, eDUPLICATEDOBJECTID_BTM);

	itemEntry->nObjects = fEntry->nObjects + 1;
	itemEntry->klen = fEntry->klen;
	memcpy(&(itemEntry->kval[0]), &(fEntry->kval[0]), itemEntry->klen);
	iOidArray = (ObjectID*)&(itemEntry->kval[alignedKlen]);
	memcpy((char*)&(iOidArray[0]), (char*)&(fOidArray[0]), OBJECTID_SIZE*(oidArrayNo+1));
	iOidArray[oidArrayNo+1] = item->oid;
	memcpy((char*)&(iOidArray[oidArrayNo+2]), (char*)&(fOidArray[oidArrayNo+1]),
	       OBJECTID_SIZE*(fEntry->nObjects-(oidArrayNo+1)));

	itemEntryLen = BTM_LEAFENTRY_FIXED +
	    alignedKlen + OBJECTID_SIZE*(itemEntry->nObjects);
	
	/* delete the fEntry from the fpage */
	for (i = high+1; i < fpage->hdr.nSlots; i++)
	    fpage->slot[-i] = fpage->slot[-(i+1)];
	fpage->hdr.nSlots--;
	if (fEntryOffset + itemEntryLen - sizeof(ObjectID) == fpage->hdr.free)
	    fpage->hdr.free -= itemEntryLen - sizeof(ObjectID);
	else
	    fpage->hdr.unused += itemEntryLen - sizeof(ObjectID);
	
    } else {
	/* cannot happen; thus do nothing */
    }

    
    /*@ Allocate a new page and initialize it as a leaf page */
    e = btm_AllocPage(handle, catObjForFile, (PageID *)&fpage->hdr.pid, &newPid); 
    if (e < 0) ERR(handle, e);

    /* check this B-tree is temporary */
    e = btm_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0) ERR(handle, e);

    /* Initialize the new page to the leaf page. */
    e = btm_InitLeaf(handle, &newPid, FALSE, isTmp);
    if (e < 0) ERR(handle, e);
    
    e = BfM_GetNewTrain(handle, &newPid, (char **)&npage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* loop until 'sum' becomes greater than BL_HALF */
    /* j : loop counter, maximum loop count = # of old Slots and a new slot */
    /* i : slot No variable of fpage */
    maxLoop = fpage->hdr.nSlots + 1;
    flag = FALSE;		/* itemEntry is to be placed on new page. */
    for (sum = 0, i = 0, j = 0; j < maxLoop && sum < BL_HALF; j++) {

	if (j == high+1) {	/* use itemEntry */	    
	    sum += itemEntryLen;
	    flag = TRUE;	/* itemEntry is to be placed on fpage. */
	    
	} else {	    
	    
	    fEntryOffset = fpage->slot[-i];
	    fEntry = (btm_LeafEntry*)&(fpage->data[fEntryOffset]);
	    sum += BTM_LEAFENTRY_FIXED + ALIGNED_LENGTH(fEntry->klen) +
		((fEntry->nObjects > 0) ? OBJECTID_SIZE*fEntry->nObjects : sizeof(ShortPageID));

	    i++;		/* increment the slot No. */
	}		
	sum += sizeof(Two);	/* slot space */
    }

    /* i-th old entries will be remained in 'fpage' */
    fpage->hdr.nSlots = i;
    
    /*@ fill the new page */
    /* i : slot no. of fpage, continued from above loop */
    /* j : loop counter, continued from above loop */
    /* k : slot no. of new page, npage */
    for (k = 0; j < maxLoop; j++, k++) {
	
	nEntryOffset = npage->slot[-k] = npage->hdr.free;
	nEntry = (btm_LeafEntry*)&(npage->data[nEntryOffset]);
	
	if (j == high+1) {	/* use 'itemEntry' */
	    entryLen = itemEntryLen;
	    memcpy((char*)nEntry, (char*)itemEntry, entryLen);
	    
	} else {
	    fEntryOffset = fpage->slot[-i];
	    fEntry = (btm_LeafEntry*)&(fpage->data[fEntryOffset]);
	    
	    if (fEntry->nObjects < 0) {	/* overflow page */
		entryLen = BTM_LEAFENTRY_FIXED + ALIGNED_LENGTH(fEntry->klen)
		    + sizeof(ShortPageID);
	    } else {
		entryLen = BTM_LEAFENTRY_FIXED + ALIGNED_LENGTH(fEntry->klen)
		    + fEntry->nObjects*OBJECTID_SIZE;
	    }
	    
	    memcpy((char*)nEntry, (char*)fEntry, entryLen);
	    
	    /* free the space from the fpage */
	    if (fEntryOffset + entryLen == fpage->hdr.free)
		fpage->hdr.free -= entryLen;
	    else
		fpage->hdr.unused += entryLen;

	    i++;		/* increment the slot No. */
	}
	
	npage->hdr.free += entryLen;
    }
    
    npage->hdr.nSlots = k;
    
    /*@ adjust 'fpage' */
    /* if the given item is to be stored in 'fpage', place it in 'fpage' */
    if (flag) {
	
	if (BL_CFREE(fpage) < itemEntryLen + sizeof(Two))
	    btm_CompactLeafPage(handle, fpage, NIL);

	/* Empty the (high+1)-th slot of 'fpage' */
	for (i = fpage->hdr.nSlots-1; i >= high+1; i--)
	    fpage->slot[-(i+1)] = fpage->slot[-i];
	
	fEntryOffset = fpage->slot[-(high+1)] = fpage->hdr.free;
	fEntry = (btm_LeafEntry*)&(fpage->data[fEntryOffset]);

	memcpy((char*)fEntry, (char*)itemEntry, itemEntryLen);
	
	fpage->hdr.free += itemEntryLen;
	fpage->hdr.nSlots++;
    }

    /* Comstruct 'ritem' which will be inserted into its parent */
    /* The key of ritem is that of the 0-th slot of npage. */
    nEntryOffset = npage->slot[0];
    nEntry = (btm_LeafEntry*)&(npage->data[nEntryOffset]);	
    ritem->spid = newPid.pageNo;
    ritem->klen = nEntry->klen;
    memcpy(&(ritem->kval[0]), &(nEntry->kval[0]), ritem->klen);
	
	
    /* If the given page was a root, it is not a root any more. */
    if (fpage->hdr.type & ROOT) fpage->hdr.type = LEAF;

    /* bluechry test ... */
    if (fpage->hdr.type & ROOT) { 
	if (fpage->hdr.pid.volNo == 1000 && fpage->hdr.pid.pageNo == 165) {
            printf("### [pID=%d, tID=%d] Set Leaf Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, fpage);
            printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
            fflush(stdout);
	}
    }
    /* ... bluechry test */


    /* Leaves are connected by doubly linked list, so it should update the links. */
    MAKE_PAGEID(nextPid, root->volNo, fpage->hdr.nextPage);
    npage->hdr.nextPage = nextPid.pageNo;
    npage->hdr.prevPage = root->pageNo;
    fpage->hdr.nextPage = newPid.pageNo;
    if (nextPid.pageNo != NIL) {
	e = BfM_GetTrain(handle, &nextPid, (char **)&mpage, PAGE_BUF);
	if (e < 0)  ERRB1(handle, e, &newPid, PAGE_BUF);

	mpage->hdr.prevPage = newPid.pageNo;
	
	e = BfM_SetDirty(handle, &nextPid, PAGE_BUF);
	if (e < 0)  ERRB1(handle, e, &newPid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
	if (e < 0)  ERRB1(handle, e, &newPid, PAGE_BUF);
    }
    
    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < 0)  ERRB1(handle, e, &newPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_SplitLeaf() */



/*@================================
 * btm_SplitOverflow()
 *================================*/
/*
 * Function: Four btm_SplitOverflow(ObjectID*, PageID*, BtreeOverflow*)
 *
 * Description:
 *  This functions simply splits the given overflow page. At first, it allocates
 *  a new overflow page, and then half of the ObjectIDs are moved to the new
 *  page.
 *  Secondly, it should update the links for maintaining doubly linked list.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Note:
 *  The caller should call BfM_SetDirty() for 'fpage'.
 */
Four btm_SplitOverflow(
    Four handle,
    ObjectID            *catObjForFile,         /* IN catalog object of B+ tree file */
    PageID              *fpid,                  /* IN Page IDentifier of the given page */
    BtreeOverflow       *fpage)                 /* INOUT the page which will be splitted */
{
    Four                e;                      /* error number */
    Two                 how;                    /* how may bytes are moved to the new page */
    Two                 from;                   /* the starting byte to be copied */
    PageID              newPid;                 /* a new allocated page */
    PageID              nextPid;                /* for maintaining doubly linked list */
    BtreeOverflow       *npage;                 /* a page pointer to the new page */
    Boolean             isTmp;                  
   

    TR_PRINT(TR_BTM, TR1,
             ("btm_SplitOverflow(handle, catObjForFile=%P, fpid=%P, fpage=%P)",
	      catObjForFile, fpid, fpage));
    
    /*@ Allocate a new page and initialize it as an overflow page. */
    e = btm_AllocPage(handle, catObjForFile, (PageID *)&fpage->hdr.pid, &newPid); 
    if (e < 0)  ERR(handle, e);

    /* check this B-tree is temporary */
    e = btm_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0)  ERR(handle, e);

    /* Initialize the new page to the overflow page. */
    e = btm_InitOverflow(handle, &newPid, isTmp);
    if (e < 0)  ERR(handle, e);
    
    e = BfM_GetNewTrain(handle, &newPid, (char **)&npage, PAGE_BUF);
    if (e < 0)  ERR(handle, e);
    
    
    /* Half of the ObjectIDs are remained in the original page and the rest of
     * the ObjectIDs are moved to the new allocated overflow page.
     */
    from = HALF_OF_OBJECTS;
    how = NO_OF_OBJECTS - HALF_OF_OBJECTS;
        
    /* Move half of the ObjectIDs and update the variables of two pages */
    memcpy((char*)&(npage->oid[0]), (char*)&(fpage->oid[from]), how*OBJECTID_SIZE);
    fpage->hdr.nObjects = from;
    npage->hdr.nObjects = how;

    /*@ Page ID of the next page */
    MAKE_PAGEID(nextPid, fpid->volNo, fpage->hdr.nextPage);
    
    /* Update links to maintain doubly linked list */
    npage->hdr.nextPage = fpage->hdr.nextPage;
    fpage->hdr.nextPage = newPid.pageNo;
    npage->hdr.prevPage = fpid->pageNo;
    
    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &newPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    e = BfM_SetDirty(handle, fpid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (nextPid.pageNo != NIL) {
	e = BfM_GetTrain(handle, &nextPid, (char **)&npage, PAGE_BUF);
	if(e < 0) ERR(handle, e);

	npage->hdr.prevPage = newPid.pageNo;
	
	e = BfM_SetDirty(handle, &nextPid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &nextPid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);
    
} /* btm_SplitOverflow() */

