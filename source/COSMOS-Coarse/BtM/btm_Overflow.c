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
 * Module: btm_Overflow.c
 *
 * Description : 
 *  This file has five functions which are concerned with maintaining overflow
 *  pages. A new overflow page is created when the size of a leaf item becomes 
 *  greater than a third of a page size.  If the member of ObjectIDs having the
 *  same key value grows more than one page limit, the page should be splitted
 *  by two pages and they are connected by doubly linked list. The deleting a
 *  ObjectID may occur an underflow of an overflow page. If it occurs, overflow
 *  pages may be merged or redistributed.
 *
 * Exports:
 *  Four btm_CreateOverflow(ObjectID*, BtreeLeaf*, Two, ObjectID*)
 *  Four btm_InsertOverflow(ObjectID*, PageID*, ObjectID*)
 *  Four btm_DeleteOverflow(PhysicalFileID*, PageID*, ObjectID*, Two*
 *                          Pool*, DeallocListElem*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ Internal Function Prototypes */
Four btm_OverflowMerge(Four, PhysicalFileID*, PageID*, BtreeOverflow*, BtreeOverflow*,
		       Pool*, DeallocListElem*);
void btm_OverflowDistribute(Four, BtreeOverflow*, BtreeOverflow*);



/*@================================
 * btm_CreateOverflow()
 *================================*/
/*
 * Function: Four btm_CreateOverflow(ObjectID*, BtreeLeaf*, Two, ObjectID*)
 *
 * Description:
 *  This function created a new overflow page. Ar first, that is, ObjectIDs
 *  having the same key value are moved from the leaf page to the newly
 *  allocated overflow page.
 *  After the new page was made, the given ObjectID is inserted using the insert
 *  routine.
 *  Sinve it is necessary to mark that over flow pages are used, the item in
 *  the leaf should be updated.
 *  ( The ordinary leaf item :
 *       <key length, key value, # of ObjectIDs, ObjectID list>
 *    The leaf item using overflow page :
 *       <key length, key value, ShortPageID of Overflow page> )
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four btm_CreateOverflow(
    Four handle,
    ObjectID            *catObjForFile, /* IN catalog object of B+ tree file */
    BtreeLeaf           *rpage,         /* IN leaf page including the leaf item */
    Two                 slotNo,         /* IN slot to be converted into overflow page */
    ObjectID            *oid)           /* IN ObjectID to be inserted */
{
    Four                e;              /* error number */
    PageID              newPid;         /* a New PageID for a new overflow page */
    BtreeOverflow       *npage;         /* Page Pointer to the new page */
    btm_LeafEntry       *lEntry;        /* a leaf entry */
    Two                 lEntryOffset;   /* starting offset of a leaf entry */
    Two                 alignedKlen;    /* aligen length of the key length */
    ObjectID            *oidArray;      /* array of ObjectIDs */
    Two                 entryLen;       /* length of an entry */
    Boolean             isTmp;          


    TR_PRINT(TR_BTM, TR1,
             ("btm_CreateOverflow(handle, catObjForFile=%P, rpage=%P, slotNo=%ld, oid=%P)",
	      catObjForFile, rpage, slotNo, oid));
    
    /*@ Allocat a new page and initialize it as an overflow page. */
    e = btm_AllocPage(handle, catObjForFile, &rpage->hdr.pid, &newPid); 
    if (e < 0) ERR(handle, e);

    /* check this B-tree is temporary */
    e = btm_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0) ERR(handle, e);

    e = btm_InitOverflow(handle, &newPid, isTmp);
    if (e < 0) ERR(handle, e);
    
    e = BfM_GetNewTrain(handle, &newPid, (char **)&npage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    lEntryOffset = rpage->slot[-slotNo];
    lEntry = (btm_LeafEntry*)&(rpage->data[lEntryOffset]);
    alignedKlen = ALIGNED_LENGTH(lEntry->klen);
    
    /* Copy ObjectIDs from the leaf to the new overflow page */
    oidArray = (ObjectID*)&(lEntry->kval[alignedKlen]);
    memcpy((char*)&(npage->oid[0]), (char*)&(oidArray[0]), lEntry->nObjects*OBJECTID_SIZE);
    npage->hdr.nObjects = lEntry->nObjects;
    
    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &newPid, PAGE_BUF);
	
    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* Insert the given ObjectID into the new overflow page */
    e = btm_InsertOverflow(handle, catObjForFile, &newPid, oid);
    if (e < 0) ERR(handle, e);

    /*@ change the 'unused' or 'free' field of rpage */
    entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + lEntry->nObjects*OBJECTID_SIZE;

    if (lEntryOffset + entryLen == rpage->hdr.free)
	rpage->hdr.free -= lEntry->nObjects*OBJECTID_SIZE - sizeof(ShortPageID);
    else
	rpage->hdr.unused += lEntry->nObjects*OBJECTID_SIZE - sizeof(ShortPageID);

    
    /* The leaf page has an overflow PageID instead of ObjectID list */
    lEntry->nObjects = NIL;
    *((ShortPageID*)&(lEntry->kval[alignedKlen])) = newPid.pageNo;
    
    return(eNOERROR);
    
} /* btm_CreateOverflow() */



/*@================================
 * btm_InsertOverflow()
 *================================*/
/*
 * Function: Four btm_InsertOverflow(ObjectID*, PageID*, ObjectID*)
 *
 * Description:
 *  Search the overflow page where the new object will be put.
 *  The ObjectID's in the overflow chain are sorted by their values so
 *  we can search the overflow page efficiently.
 *  After finding the appropriate page we insert the new object into the
 *  overflow page if there is enough space. If there is not enough space
 *  in the page, split it and then recursively call btm_InsertOverflow().
 *
 * Returns:
 *  Error code
 *    eDUPLICATEDOBJECTID_BTM
 *    some errors caused by function calls
 */
Four btm_InsertOverflow(
    Four handle,
    ObjectID                    *catObjForFile, /* IN catalog object of B+ tree file */
    PageID                      *overPid,       /* IN where the ObjectID to be inserted */
    ObjectID                    *oid)           /* IN ObjectID to be inserted */
{
    Four                        e;              /* error number */
    Two                         idx;            /* the position to be inserted */
    ObjectID                    curOid;         /* The current ObjectID which is examined */
    PageID                      curPid;         /* current overflow page */
    PageID                      nextPid;        /* next overflow page */
    BtreeOverflow               *apage;         /* Page Pointer to the overflow page */
    Boolean                     found;          /* search result */
    

    TR_PRINT(TR_BTM, TR1,
             ("btm_InsertOverflow(handle, catObjForFile=%P, overPid=%P, oid=%P)",
	      catObjForFile, overPid, oid));


    curPid = *overPid;
    
    e = BfM_GetTrain(handle, &curPid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* The last ObjectID in the given overflow page */
    curOid = apage->oid[apage->hdr.nObjects-1];
    
    while (btm_ObjectIdComp(handle, oid, &curOid) == GREAT && apage->hdr.nextPage != NIL) {

	MAKE_PAGEID(nextPid, curPid.volNo, apage->hdr.nextPage);

	/*@ Free the buffer for the current page. */
	e = BfM_FreeTrain(handle, &curPid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	curPid = nextPid;
	
	e = BfM_GetTrain(handle, &curPid, (char **)&apage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* The last ObjectID in the given overflow page */
	curOid = apage->oid[apage->hdr.nObjects-1];
    }

    
    /* The ObjectID should be inserted in the current page */
	
    if (apage->hdr.nObjects < BO_MAXOBJECTIDS) {
	/* Search the correct postion to be inserted using the binary search */
	found = btm_BinarySearchOidArray(handle, &(apage->oid[0]), oid, apage->hdr.nObjects, &idx);
	if (found) ERRB1(handle, eDUPLICATEDOBJECTID_BTM, &curPid, PAGE_BUF);
	
	/* Make room for the ObjectID */
	memmove(&(apage->oid[idx+2]), &(apage->oid[idx+1]),
		(apage->hdr.nObjects-idx-1)*OBJECTID_SIZE);
	
	/*@ Save the ObjectID and decrease free area */
	apage->oid[idx+1] = *oid;
	(apage->hdr.nObjects)++;
	
	e = BfM_SetDirty(handle, &curPid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
	
    } else {	/* not enough */
	/* Insert it after split */
	e = btm_SplitOverflow(handle, catObjForFile, &curPid, apage);
	if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
	
	e = btm_InsertOverflow(handle, catObjForFile, &curPid, oid);
	if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
    }
    
    e = BfM_FreeTrain(handle, &curPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_InsertOverflow() */



/*@================================
 * btm_DeleteOverflow()
 *================================*/
/*
 * Function: Four btm_DeleteOverflow(PhysicalFileID* PageID*, ObjectID*, Two*,
 *                                   Pool*, DeallocListElem*)
 *
 * Description:
 *  This function deletes the given ObjectID from the overflow pages. If the
 *  ObjectID is in the given overflow page, simply delete it. If not, get the
 *  next page and recursively call itself using the next page. If the page after
 *  deleting is not half full, the page and the neighboring page are merged or
 *  redistributed. After deleting, if there is only one overflow page and its
 *  size is less than a fourth of a page, then 'f' is assigned to the # of
 *  ObjectIDs, otherwise it becomes FALSE.
 *
 * Returns:
 *  Error code
 *    eNOTFOUND_BTM
 *    some errors caused by function calls
 */
Four btm_DeleteOverflow(
    Four handle,
    PhysicalFileID              *pFid,          /* IN PhysicalFileID of the Btree file */
    PageID                      *overPid,       /* IN The given ObjectID will be inserted into this page */
    ObjectID                    *oid,           /* IN ObjectID to be deleted */
    Two                         *f,             /* OUT # of ObjectIDs if total ObjectIDs < 1/4 of a page */
    Pool                        *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)        /* INOUT head of the dealloc list */
{
    Four                        e;              /* error number */
    Two                         idx;            /* nth ObjectID in the overflow page */
    PageID                      curPid;         /* current overflow page */
    PageID                      prevPid;        /* previous overflow page */
    PageID                      nextPid;        /* next overflow page */
    ObjectID                    curOid;         /* The current ObjectID which is examined */
    BtreeOverflow               *mpage;         /* Page Pointer to the prevPid or nextPage */
    BtreeOverflow               *opage;         /* Page Pointer to the given overflow page */
    Boolean                     found;          /* search result */
    

    TR_PRINT(TR_BTM, TR1,
             ("btm_DeleteOverflow(handle, pFid=%P, overPid=%P, oid=%P, f=%P, dlPool=%P, dlHead=%P)",
	      pFid, overPid, oid, f, dlPool, dlHead));


    curPid = *overPid;
    
    e = BfM_GetTrain(handle,  &curPid, (char **)&opage, PAGE_BUF );
    if( e < 0 ) ERR(handle,  e );
        
    /* curOid is the last ObjectID of the given overflow page */
    curOid = opage->oid[opage->hdr.nObjects - 1];
    
    while (btm_ObjectIdComp(handle, oid, &curOid) == GREAT) {
	
	MAKE_PAGEID(nextPid, curPid.volNo, opage->hdr.nextPage);

	e = BfM_FreeTrain(handle, &curPid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	curPid = nextPid;
	
	if( curPid.pageNo == NIL ) ERR(handle,  eNOTFOUND_BTM );

	e = BfM_GetTrain(handle,  &curPid, (char **)&opage, PAGE_BUF );
	if( e < 0 ) ERR(handle,  e );
        
	/* curOid is the last ObjectID of the given overflow page */
	curOid = opage->oid[opage->hdr.nObjects - 1];
    }
	
    /* exist in the current page if exist */
    /*@ Search the ObjectID */
    found = btm_BinarySearchOidArray(handle, &(opage->oid[0]), oid, opage->hdr.nObjects, &idx);

    if (!found)	{
        printf("oid = (%ld, %ld)\n", oid->volNo, oid->pageNo);
        ERRB1(handle, eNOTFOUND_BTM, &curPid, PAGE_BUF);
    } 
    
    /* Delete the ObjectID */
    memmove(&(opage->oid[idx]), &(opage->oid[idx+1]),
	    (opage->hdr.nObjects-idx-1)*OBJECTID_SIZE);
    
    /*@ update the variables of the page */
    (opage->hdr.nObjects)--;
    
    *f = FALSE;	/* initially FALSE */
    
    if (opage->hdr.nObjects < HALF_OF_OBJECTS) {	/* Underflow */
	if (opage->hdr.nextPage == NIL) {
	    if (opage->hdr.prevPage == NIL) {
		if (opage->hdr.nObjects < A_FOURTH_OF_OBJECTS)
		    *f = opage->hdr.nObjects;
	    } else {
		
		MAKE_PAGEID(prevPid, curPid.volNo, opage->hdr.prevPage);
		
		/* The previous page is used by the neighboring page */
		e = BfM_GetTrain(handle,  &prevPid, (char **)&mpage, PAGE_BUF );
		if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
		
		if((mpage->hdr.nObjects + opage->hdr.nObjects) <= BO_MAXOBJECTIDS) {
		    /* The sum of the two pages is less than a page size */
		    e = btm_OverflowMerge(handle, pFid, &curPid, mpage,
					  opage, dlPool, dlHead);
		    if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
		} else { /* more than one page size */
		    btm_OverflowDistribute(handle, mpage, opage);		    
		}

		/* The previous page was modified in both cases of merge and distribute */
		if (BfM_SetDirty(handle,  &prevPid, PAGE_BUF) < 0) {
		    BfM_FreeTrain(handle, &prevPid, PAGE_BUF);
		    BfM_FreeTrain(handle, &curPid, PAGE_BUF);
		    ERR(handle, e);
		}
		
		e = BfM_FreeTrain(handle, &prevPid, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
	    }
	} else {
	    MAKE_PAGEID(nextPid, curPid.volNo, opage->hdr.nextPage);
	    
	    /* The next page is used by the neighboring page */
	    e = BfM_GetTrain(handle, &nextPid, (char **)&mpage, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
	    
	    if ((opage->hdr.nObjects + mpage->hdr.nObjects) <= BO_MAXOBJECTIDS) {
		/* The sum of the two pages is less than a page size */
		e = btm_OverflowMerge(handle, pFid, &nextPid, opage, mpage, dlPool, dlHead); 
		if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
	    } else { /* more than one page size */
		btm_OverflowDistribute(handle, opage, mpage);
		
		if (BfM_SetDirty(handle, &nextPid, PAGE_BUF) < 0) {
		    BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
		    BfM_FreeTrain(handle, &curPid, PAGE_BUF);
		    ERR(handle, e);
		}
	    }
	    
	    e = BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
	}
    }

    e = BfM_SetDirty(handle, &curPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &curPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &curPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
} /* btm_DeleteOverflow() */



/*@================================
 * btm_OverflowMerge()
 *================================*/
/*
 * Function: Four btm_OverflowMerge(PhysicalFileID*, PageID*, BtreeOverflow*,
 *                                  BtreeOverflow*, Pool*, DeallocListElem*)
 *
 * Description:
 *  All ObjectIDs in the second page(page2) are copied to the first page(page1).
 *  After the copy is completed, 'nextPage' and 'prevPage' are updated to
 *  maintain doubly linked list. Finally, deallocate the the second page.
 *
 * Returns:
 *  Error number
 *    some errors caused by function calls
 */
Four btm_OverflowMerge(
    Four handle,
    PhysicalFileID *pFid,	/* IN PhysicalFileID of the Btree file */
    PageID        *pid2,	/* IN PageID of the 2nd page */
    BtreeOverflow *page1,	/* INOUT The page to be copied to */
    BtreeOverflow *page2,	/* INOUT The page to be copied from */
    Pool          *dlPool,	/* INOUT pool of dealloc list elements */
    DeallocListElem *dlHead) /* INOUT head of the dealloc list */
{
    Four e;			/* error number */
    PageID  nextPid;		/* The next page of page2 */
    BtreeOverflow *npage;	/* Page Pointer to the overflow page */
    DeallocListElem *dlElem; /* an element of dealloc list */
    

    TR_PRINT(TR_BTM, TR1,
             ("btm_OverflowMerge(handle, pFid=%P, pid2=%P, page1=%P, page2=%P, dlPool=%P, dlHead=%P)",
	      pFid, pid2, page1, page2, dlPool, dlHead));

    
    /* Copy the contents of the page2 to the page1 */
    memcpy((char*)&(page1->oid[page1->hdr.nObjects]), (char*)&(page2->oid[0]),
	       page2->hdr.nObjects*OBJECTID_SIZE);
    
    page1->hdr.nObjects += page2->hdr.nObjects;
    
    /* Update links */
    MAKE_PAGEID(nextPid, pid2->volNo, page2->hdr.nextPage);
    page1->hdr.nextPage = nextPid.pageNo;
    if (nextPid.pageNo != NIL) {
	e = BfM_GetTrain(handle, &nextPid, (char **)&npage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	npage->hdr.prevPage = page2->hdr.prevPage;
	
	e = BfM_SetDirty(handle, &nextPid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &nextPid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    page2->hdr.type = FREEPAGE;

    /* bluechry test ... */
    if (page2->hdr.pid.volNo == 1000 && page2->hdr.pid.pageNo == 165) {
        printf("### [pID=%d, tID=%d] Set FreePage: [%d, %d] at %x\n", procIndex, handle, page2->hdr.pid.volNo, page2->hdr.pid.pageNo, page2);
	printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
        fflush(stdout);
    }
    /* ... bluechry test */
    
    /*
    ** Insert a new node for the dropped file.
    */    
    e = Util_getElementFromPool(handle, dlPool, &dlElem);
    if (e < 0) ERR(handle, e);

    dlElem->type = DL_PAGE;
    dlElem->elem.pid = *pid2;	/* store NILPAGEID */
    dlElem->next = dlHead->next; /* insert to the list */
    dlHead->next = dlElem;       /* new first element of the list */

    return(eNOERROR);
    
} /* btm_OverflowMerge() */



/*@================================
 * btm_OverflowDistribute()
 *================================*/
/*
 * Function: void btm_OverflowDistribute(BtreeOverflow*, BtreeOverflow*)
 *
 * Description:
 *  If the size of the first page is larger than the second page, ObjectIDs are
 *  moved from the first page to the second page, otherwise from the second
 *  page to the first page.
 *
 * Returns:
 *  None
 */
void btm_OverflowDistribute(
    Four			handle,
    BtreeOverflow               *page1,                 /* INOUT The first page to be redistributed */
    BtreeOverflow               *page2)                 /* INOUT The second page to be redistributed */
{
    Two                         nMovedObjects;          /* # of objects to move */


    TR_PRINT(TR_BTM, TR1,
             ("btm_OverflowDistribute(handle, page1=%P, page2=%P)", page1, page2));
        
    if( page1->hdr.nObjects > page2->hdr.nObjects ) {	/* page1 -> page2 */
	/*@ number of objects to move */
	nMovedObjects = (page1->hdr.nObjects - page2->hdr.nObjects)/2;

	/* reserve space in page2 */
	memmove(&(page2->oid[nMovedObjects]), &(page2->oid[0]),
		page2->hdr.nObjects*OBJECTID_SIZE);

	/* move the objects from page1 to page2 */
	memcpy((char*)&(page2->oid[0]), (char*)&(page1->oid[page1->hdr.nObjects-nMovedObjects]),
	       nMovedObjects*OBJECTID_SIZE);

	/* update the number of objects in each page */
	page1->hdr.nObjects -= nMovedObjects;
	page2->hdr.nObjects += nMovedObjects;
	
    } else {	/* page2 => page1 */
	/*@ number of objects to move */
	nMovedObjects = (page2->hdr.nObjects - page1->hdr.nObjects)/2;

	/* move the objects from page2 to page1 */
	memcpy((char*)&(page1->oid[page1->hdr.nObjects]), (char*)&(page2->oid[0]),
	       nMovedObjects*OBJECTID_SIZE);
	
	/* fill the moved space in page2 */
	memmove(&(page2->oid[0]), &(page2->oid[nMovedObjects]),
		(page2->hdr.nObjects-nMovedObjects)*OBJECTID_SIZE);

	/* update the number of objects in each page */
	page1->hdr.nObjects += nMovedObjects;
	page2->hdr.nObjects -= nMovedObjects;
    }

} /* btm_OverflowDistribute() */
