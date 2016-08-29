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
 * Module: btm_Underflow.c
 *
 * Description : 
 *   This file has three functions, but the interface function of the external
 *   routines is only one routine "btm_Underflow(....)".
 *   These underflow routines have two major functions, one is merging and
 *   another is redistributing.
 *
 * Exports:
 *  Four btm_Underflow(PhysicalFileID*, BtreePage*, PageID*, Two, Boolean*, Boolean*,
 *                     InternalItem*, Pool*, DeallocListElem*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ function prototypes used only in this file */
Four btm_UnderflowInternal(Four, PhysicalFileID*, BtreeInternal*, PageID*, BtreeInternal*, Two,
	                           Boolean*, Boolean*, InternalItem*, Pool*, DeallocListElem*);
Four btm_UnderflowLeaf(Four, PhysicalFileID*, BtreeInternal*, PageID*, BtreeLeaf*, Two,
	                       Boolean*, Boolean*, InternalItem*, Pool*, DeallocListElem*);


/*@================================
 * btm_Underflow()
 *================================*/
/*
 * Function: Four btm_Underflow(PhysicalFileID*, BtreePage*, PageID*, Two, Boolean*,
 *                              Boolean*, InternalItem*, Pool*, DeallocListElem*)
 *
 * Description:
 *  This function merges or redistributes between the given 'child' page and
 *  the 'sibling' which is gotten by using the given root page.  If the child
 *  page is an internal page, 'btm_UnderflowInternal(...)' may be used,
 *  otherwise 'btm_UnderflowLeaf(...)' may be used.
 *  The flag 'f' will be set TRUE if the root is not half full.
 *
 *  Redistributing pages may replace the key value of their parent and
 *  the parent page may be splitted by the result of this replacing.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Note:
 *  The caller should call BfM_SetDirty() for 'rpage'.
 */
Four btm_Underflow(
    Four handle,
    PhysicalFileID      *pFid,          /* IN PhysicalFileID of the Btree file */ 
    BtreePage           *rpage,         /* INOUT a page pointer of a given root */
    PageID              *child,         /* IN child Page IDentifier */
    Two                 slotNo,         /* IN slot correspoding to the child page */
    Boolean             *f,             /* OUT whether the root is half full */
    Boolean             *h,             /* OUT whether the root is splitted */
    InternalItem        *item,          /* OUT The internal item to be returned */
    Pool                *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)        /* INOUT head of the dealloc list */
{
    Four                e;              /* error number */
    BtreePage           *cpage;         /* for child */


    TR_PRINT(TR_BTM, TR1,
             ("btm_Underflow(handle, pFid=%P, rpage=%P, child=%P, slotNo=%ld, f=%P, h=%P, item=%P, dlPool=%P, dlHead=%P)",
	      pFid, rpage, child, slotNo, f, h, item, dlPool, dlHead));

    
    /*@ get page */
    e = BfM_GetTrain(handle, child, (char **)&cpage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    if( cpage->any.hdr.type & INTERNAL )
	e = btm_UnderflowInternal(handle, pFid, &(rpage->bi), child, &(cpage->bi),
				  slotNo, f, h, item, dlPool, dlHead);
    else
	e = btm_UnderflowLeaf(handle, pFid, &(rpage->bi), child, &(cpage->bl),
			      slotNo, f, h, item, dlPool, dlHead);
    
    if (e < 0) ERRB1(handle, e, child, PAGE_BUF); 

    /*@ set dirty flag */
    e = BfM_SetDirty(handle, child, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, child, PAGE_BUF); 

    /*@ free page */
    e = BfM_FreeTrain(handle, child, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, child, PAGE_BUF); 
    
    return(eNOERROR);

} /* btm_Underflow() */



/*@================================
 * btm_UnderflowInternal()
 *================================*/
/*
 * Function: Four btm_UnderflowInternal(PhysicalFileID*, BtreeInternal*, PageID*,
 *                                      BtreeInternal*, Two, Boolean*, Boolean*,
 *                                      InternalItem*, Pool*, DeallocListElem*)
 *
 * Description:
 *  At first, it select a sibling page of the given child page.  The sibling
 *  page is the right page (or the next page) of the given child page except
 *  that the child page is not the last page of the root page (in this case,
 *  the sibling page is the left page of the child page.). 
 *  If the size of two pages (child and sibling) is less than or equal to
 *  a page size, the two pages are merged into one page, which is always the
 *  left page, and the right page entry should be deleted from the root page.
 *  If the size is greater than a page size, the two pages are redistributed and
 *  an entry of the root page should be replaced by new one. This replacing may
 *  occur splitting the given root page in spite of deleting not inserting.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four btm_UnderflowInternal(
    Four handle,
    PhysicalFileID      *pFid,                  /* IN PhysicalFileID of the Btree file */ 
    BtreeInternal       *rpage,                 /* INOUT page pointer to the root */
    PageID              *child,                 /* IN Page ID for a child */
    BtreeInternal       *cpage,                 /* INOUT page pointer to the child */
    Two                 slotNo,                 /* IN slot in a root for a child */
    Boolean             *f,                     /* OUT whether the root is half full */
    Boolean             *h,                     /* OUT whether the root is splitted */
    InternalItem        *item,                  /* OUT Internal item */
    Pool                *dlPool,                /* INOUT pool of dealloc list element */
    DeallocListElem     *dlHead)                /* INOUT head of the dealloc list */
{
    Four                e;                      /* error number */
    Two                 i;                      /* index */
    Four                volNo;                  /* volume where the B+tree is placed */
    Two                 diff;                   /* difference the size of the two pages */
    Boolean             flag;                   /* TRUE if the sibling is a left page of a given child page */
    Two                 length;                 /* length of the entry */
    Two                 c_size;                 /* 'size' for child page */
    Two                 s_size;                 /* 'slot' for sibling page */
    PageID              sibling;                /* Page IDentifier for a sibling */
    BtreeInternal       *spage;                 /* page pointer to the sibling */
    BtreeInternal       *tpage;                 /* temporary page pointer */
    Two                 rightEntryOffset;       /* starting offset of the entry for the right child */
    Two                 leftEntryOffset;        /* starting offset of the entry for the left child */
    Two                 iEntryOffset;           /* starting offset of an internal entry */
    Two                 rightEntryLen;          /* length of the entry for right child in root */
    btm_InternalEntry   *rightEntry;            /* entry for the right child in root */
    btm_InternalEntry   *leftEntry;             /* entry for the left child in root */
    btm_InternalEntry   *oldEntry;              /* points to the old right entry */
    btm_InternalEntry   *iEntry;                /* an internal entry */
    DeallocListElem     *dlElem;                /* an element of dealloc list */
    Two                 rightSlotNo;            /* slot No of the right child entry in root */
    btm_InternalEntry   *firstEntry;            /* first entry of the right child */
    Two                 firstEntryOffset;       /* starting offset of right child */
    Two                 firstEntryLen;          /* length of the first entry */
    Two                 iEntryLen;              /* length of an internal entry */
        

    TR_PRINT(TR_BTM, TR1,
             ("btm_UnderflowInternal(handle, pFid=%P, rpage=%P, child=%P, cpage=%P, slotNo=%ld, f=%P, h=%P, item=%P, dlPool=%P, dlHead=%P)",
	      pFid, rpage, child, cpage, slotNo, f, h, item, dlPool, dlHead));
    
    *f = *h = FALSE;
    
    /* get the volume number; Btree index cannot span across two volumes */
    volNo = child->volNo;
    
    /* Get the sibling page using the given index which indicates the child
     * page. 'rightEntry' is the right entry in the root of the two child pages.
     */
    if (slotNo < (rpage->hdr.nSlots-1)) {	/* sibling is a right page */
	flag = FALSE;
	rightSlotNo = slotNo + 1;
	rightEntryOffset = rpage->slot[-rightSlotNo];
	rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);
	MAKE_PAGEID(sibling, volNo, rightEntry->spid);
	
    } else {	/* sibling is a left page */
	flag = TRUE;
	rightSlotNo = slotNo;
	rightEntryOffset = rpage->slot[-rightSlotNo];
	rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);
	
	if(slotNo != 0) {
	    leftEntryOffset = rpage->slot[-(slotNo-1)];
	    leftEntry = (btm_InternalEntry*)&(rpage->data[leftEntryOffset]);
	    MAKE_PAGEID(sibling, volNo, leftEntry->spid);
	} else
	    MAKE_PAGEID(sibling, volNo, rpage->hdr.p0);
    }

    rightEntryLen = sizeof(ShortPageID) +
	ALIGNED_LENGTH(sizeof(Two)+rightEntry->klen);
    
    
    /*@ Get the sibling page and load some useful variables */
    e = BfM_GetTrain(handle, &sibling, (char **)&spage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* Calculate the size of the used portion in each page */
    c_size = cpage->hdr.free - cpage->hdr.unused + cpage->hdr.nSlots*sizeof(Two);
    s_size = spage->hdr.free - spage->hdr.unused + spage->hdr.nSlots*sizeof(Two);
    
    
    /* If the size of two pages + the size of an entry in the root <= the size
     * of an internal page, then merge the two pages into left page.
     * BI_FIXED includes a slot size and this is consolidated with a new slot
     * for the new entry.
     *
     * original comparison expression of the next 'if' statement
     *  c_size+s_size-sizeof(Two)+(rightEntryLen+sizeof(Two)) <= PAGESIZE-BI_FIXED
     *  (BI_FIXED includes a slot.)
     */    
    if ((c_size+s_size+rightEntryLen) <= (PAGESIZE-BI_FIXED)) {
	
	/* Change the child and sibling page for simplicity
	 * if sibling is left page.
	 */
	if (flag) {
	    e = s_size; s_size = c_size; c_size = e;
	    tpage = spage; spage = cpage; cpage = tpage;
	}

	/*@ make room for the merged entries */
	if (BI_CFREE(cpage) < s_size + rightEntryLen + sizeof(Two))
	    btm_CompactInternalPage(handle, cpage, NIL);
	
	/* Copy the entry for the right page in the root to the left page. */
	iEntryOffset = cpage->slot[-cpage->hdr.nSlots] = cpage->hdr.free;
	iEntry = (btm_InternalEntry*)&(cpage->data[iEntryOffset]);

	iEntry->spid = spage->hdr.p0;
	iEntry->klen = rightEntry->klen;
	memcpy(&(iEntry->kval[0]), &(rightEntry->kval[0]), iEntry->klen);
	
	cpage->hdr.nSlots++, cpage->hdr.free += rightEntryLen;
	
	/* Copy all entries in the right page to the left page. */
	for (i = 0;i < spage->hdr.nSlots;i++ ) {
	    iEntryOffset = spage->slot[-i];
	    iEntry = (btm_InternalEntry*)&(spage->data[iEntryOffset]);

	    length = sizeof(ShortPageID) +
		ALIGNED_LENGTH(sizeof(Two)+iEntry->klen);
	    
	    cpage->slot[-cpage->hdr.nSlots] = cpage->hdr.free;
	    memcpy(&(cpage->data[cpage->hdr.free]), (char*)iEntry, length);
	    cpage->hdr.nSlots++, cpage->hdr.free += length;
	}	
	
	/*@ Delete the entry, which is copied to the left page, from the root page */
	for(i = rightSlotNo+1;i < rpage->hdr.nSlots; i++) 
	    rpage->slot[-(i-1)] = rpage->slot[-i];
	
	if (rightEntryOffset + rightEntryLen == rpage->hdr.free)
	    rpage->hdr.free -= rightEntryLen;
	else
	    rpage->hdr.unused += rightEntryLen;
	rpage->hdr.nSlots--;
	
	/*
	** Insert a new node for the dropped file.
	*/
	spage->hdr.type = FREEPAGE;
	
	e = Util_getElementFromPool(handle, dlPool, &dlElem);
	if (e < 0) ERR(handle, e);

        dlElem->type = DL_PAGE;
	dlElem->elem.pid = flag ? *child:sibling; /* save the PageID */
	dlElem->next = dlHead->next; /* insert to the list */
	dlHead->next = dlElem;       /* new first element of the list */
	
        e = BfM_SetDirty(handle, &sibling, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, &sibling, PAGE_BUF);

        /* bluechry test ... */
        if (sibling.volNo == 1000 && sibling.pageNo == 165) {
            printf("### [pID=%d, tID=%d] Set FreePage: [%d, %d] at %x\n", procIndex, handle, sibling.volNo, sibling.pageNo, spage);
	    printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
            fflush(stdout);
        }
        /* ... bluechry test */

    } else if (s_size > BI_HALF) { /* redistribute if the sibling page is not underflow */
        Two  nMovedEntries;     	/* # of entries to move */
        Two  c_size2, s_size2;  	/* sizes after redistribution */
        Two  len_entryMovedIntoChild; 	/* length of the entry moved into child */
        Two  len_entryMovedFromSibling; /* length of the entry moved from sibling */
        
        /* Find the # of entries to move to the underflowed page. */
        /* At first, an entry in the parent page is moved. */
        len_entryMovedIntoChild = rightEntryLen;
        c_size2 = c_size; s_size2 = s_size; /* initialization */
        for (nMovedEntries = 0; nMovedEntries < spage->hdr.nSlots; nMovedEntries++) {
            /* When entering this loop, s_size2 is equal or greater than BI_HALF,
             * and s_size2 is also greater than c_size2. */
            Two c_size2_tmp, s_size2_tmp;
            
            if (flag)
                iEntry = (btm_InternalEntry*)&spage->data[spage->slot[-(spage->hdr.nSlots-1-nMovedEntries)]];
            else
                iEntry = (btm_InternalEntry*)&spage->data[spage->slot[-nMovedEntries]];
            len_entryMovedFromSibling = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two)+iEntry->klen);

            c_size2_tmp = c_size2 + len_entryMovedIntoChild + sizeof(Two);
            s_size2_tmp = s_size2 - (len_entryMovedFromSibling + sizeof(Two));

            if (s_size2_tmp < BI_HALF) break;
            if (c_size2_tmp > s_size2_tmp) {
                /* If the current move cause the difference between page sizes,
                 * move the current entry too.
                 */
                if ((c_size2_tmp - s_size2_tmp) < (s_size - c_size)) {
                    c_size2 = c_size2_tmp;
                    s_size2 = s_size2_tmp;
                    nMovedEntries++; /* move this entry too */
                }
                break;
            }

            c_size2 = c_size2_tmp;
            s_size2 = s_size2_tmp;
            len_entryMovedIntoChild = len_entryMovedFromSibling;
        }

        /* If there is no entry to move, return immediately. */
        if (nMovedEntries == 0) {    
            e = BfM_FreeTrain(handle, &sibling, PAGE_BUF);
            if (e < 0) ERR(handle, e);

            return(eNOERROR);
        }

	if (BI_CFREE(cpage) < (c_size2 - c_size)) 
	    btm_CompactInternalPage(handle, cpage, NIL);
	
	/* If the left page is a sibling, entries are inserted in front
	 * of a child page, else appended to the child page.
	 */
	if (flag) { /* inserted */
	    for(i = cpage->hdr.nSlots - 1;i >= 0;i--)
		cpage->slot[-(i+1)] = cpage->slot[-i];
	    cpage->slot[0] = cpage->hdr.free;
	} else /* appended */
	    cpage->slot[-cpage->hdr.nSlots] = cpage->hdr.free;
	
	/*@ copy the entry in the root to the child page */
	iEntryOffset = cpage->hdr.free;
	iEntry = (btm_InternalEntry*)&(cpage->data[iEntryOffset]);
	iEntry->spid = (flag) ? cpage->hdr.p0 : spage->hdr.p0;
	iEntry->klen = rightEntry->klen;
	memcpy(&(iEntry->kval[0]), &(rightEntry->kval[0]), iEntry->klen);
	cpage->hdr.nSlots++, cpage->hdr.free += rightEntryLen;

        nMovedEntries --;  
	
	/* Move entries in the sibling page to the child page. */
        for ( ; nMovedEntries > 0; nMovedEntries--) { 
	    if (flag)		/* move the last entry */
		iEntryOffset = spage->slot[-(spage->hdr.nSlots-1)];
	    else 		/* move the first entry */
		iEntryOffset = spage->slot[0];

	    iEntry = (btm_InternalEntry*)&(spage->data[iEntryOffset]);
	    iEntryLen = sizeof(ShortPageID) +
		ALIGNED_LENGTH(sizeof(Two) + iEntry->klen);

	    
	    /* Copy an entry to the child page */
	    if (flag) {
		for(i = cpage->hdr.nSlots - 1;i >= 0;i--)
		    cpage->slot[-(i+1)] = cpage->slot[-i];
		cpage->slot[0] = cpage->hdr.free;
	    } else 
		cpage->slot[-cpage->hdr.nSlots] = cpage->hdr.free;

	    memcpy(&(cpage->data[cpage->hdr.free]), (char*)iEntry, iEntryLen);
	    
	    /*@ Delete the entry from the sibling page */
	    if (iEntryOffset + iEntryLen == spage->hdr.free)
		spage->hdr.free -= iEntryLen;
	    else
		spage->hdr.unused += iEntryLen;
	    
	    if (!flag)	/* Delete slot */
		for (i = 1; i < spage->hdr.nSlots; i++)
		    spage->slot[-(i-1)] = spage->slot[-i];
	    spage->hdr.nSlots--;
	    
	    /* Update 'free' and 'nEntries' of  cpage */
	    cpage->hdr.nSlots++, cpage->hdr.free += iEntryLen;
	}
	
	/* Get the entry which will be replaced by the old entry in the root. */	
	/* rightEntry: old delimeter for the right child in rpage */
	/* firstEntry: new delimeter for the right child in rpage */	
	if (flag)
	    firstEntryOffset = spage->slot[-(spage->hdr.nSlots-1)];
	else
	    firstEntryOffset = spage->slot[0];

	firstEntry = (btm_InternalEntry*)&(spage->data[firstEntryOffset]);
	firstEntryLen = sizeof(ShortPageID) +
	    ALIGNED_LENGTH(sizeof(Two) + firstEntry->klen);

	diff = firstEntryLen - rightEntryLen;
	
	if (diff <= BI_FREE(rpage)) {

	    if (rightEntryOffset + rightEntryLen == rpage->hdr.free &&
		diff <= BI_CFREE(rpage)) {

		rpage->hdr.free += diff;

	    } else if (diff <= 0) {

		rpage->hdr.unused += -diff;
		
	    } else if (firstEntryLen <= BI_CFREE(rpage)) {

		/*@ move the entry to the free space */
		rightEntryOffset = rpage->slot[-rightSlotNo] = rpage->hdr.free;
		oldEntry = rightEntry;
		rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);

		rightEntry->spid = oldEntry->spid;
		
		rpage->hdr.unused += rightEntryLen;
		rpage->hdr.free += firstEntryLen;
		
	    } else { /* no enough contiguous space */

		btm_CompactInternalPage(handle, rpage, rightSlotNo);
		/* 'rightEntry' has moved by btm_CompactInternalPage() */
		rightEntryOffset = rpage->slot[-rightSlotNo];
		rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);
		
		rpage->hdr.free += diff;		
	    }

	    rightEntry->klen = firstEntry->klen;
	    memcpy(&(rightEntry->kval[0]), &(firstEntry->kval[0]), rightEntry->klen);
	} else {
	    /* Insert after delete */
	    if (rightEntryOffset + rightEntryLen == rpage->hdr.free)
		rpage->hdr.free -= rightEntryLen;
	    else
		rpage->hdr.unused += rightEntryLen;

	    for(i = rightSlotNo+1; i < rpage->hdr.nSlots; i++) 
		rpage->slot[-(i-1)] = rpage->slot[-i];
	    rpage->hdr.nSlots--;

	    /* construct the internal item for the insertion in rpage */
	    item->spid = rightEntry->spid;
	    item->klen = firstEntry->klen; 
	    memcpy(&(item->kval[0]), &(firstEntry->kval[0]), item->klen);
	    *h = TRUE;
	}
	
	/* Fill the p0 pointer using the pointer of the new entry in the root. */
	if (flag)
	    cpage->hdr.p0 = firstEntry->spid;
	else
	    spage->hdr.p0 = firstEntry->spid;
	
	/* Delete the entry from the sibling page */
	if (firstEntryOffset + firstEntryLen == spage->hdr.free)
	    spage->hdr.free -= firstEntryLen;
	else
	    spage->hdr.unused += firstEntryLen;
	
	if (!flag)		/* delete the slot */
	    for(i = 1;i < spage->hdr.nSlots; i++)
		spage->slot[-(i-1)] = spage->slot[-i];
	
	spage->hdr.nSlots--;

        e = BfM_SetDirty(handle, &sibling, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, &sibling, PAGE_BUF);
    }
    
    /* 'f' is TRUE if the size of the root is not half full. */
    if(BI_FREE(rpage) > BI_HALF)	*f = TRUE;
    
    e = BfM_FreeTrain(handle, &sibling, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
}   /* btm_UnderflowInternal() */



/*@================================
 * btm_UnderflowLeaf()
 *================================*/
/*
 * Function: Four btm_UnderflowLeaf(PhysicalFileID*, BtreeInternal*, PageID*, BtreeLeaf*,
 *                                  Two, Boolean*, Boolean*, InternalItem*,
 *                                  Pool*, DeallocListElem*)
 *
 * Description:
 *  This function is similar to the internal case except that the child is not
 *  an internal page but a leaf page and the first entry of the leaf should be
 *  copied to the parent. Since the doubly linked list is always maintained in
 *  leaf pages for sequential searching using Btree index, two field 'nextPage'
 *  and 'prevPage' should be modified.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_UnderflowLeaf(
    Four handle,
    PhysicalFileID      *pFid,                  /* IN PhysicalFileID of the Btree file */ 
    BtreeInternal       *rpage,                 /* INOUT page pointer to the root */
    PageID              *child,                 /* IN PageID for a child */
    BtreeLeaf           *cpage,                 /* INOUT page pointer to the child */
    Two                 slotNo,                 /* IN slot No in a root for a child */
    Boolean             *f,                     /* OUT whether the root is half full */
    Boolean             *h,                     /* OUT whether the root is splitted */
    InternalItem        *item,                  /* OUT An Internal Item */
    Pool                *dlPool,                /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)                /* INOUT head of the dealloc list */
{
    Four                e;                      /* error number */
    Two                 i;                      /* loop index */
    Four                volNo;                  /* volume where the Btree is placed */
    Two                 diff;                   /* difference the size of the two pages */
    Two                 length;                 /* length of the entry */
    Two                 c_size;                 /* 'size' for the child */
    Two                 s_size;                 /* 'size' for the sibling */
    Two                 t_size;
    PageID              sibling;                /* Page IDentifier for a sibling */
    PageID              nextPage;               /* Page IDentifier for the next page */
    BtreeLeaf           *spage;                 /* page pointer to the sibling */
    BtreeLeaf           *tpage;                 /* temporary page pointer */
    Boolean             flag;                   /* TRUE if the sibling is a left page of a given child page */
    Two                 rightSlotNo;            /* slot No. of the right entry in the root */
    Two                 leftEntryOffset;        /* starting offset of the left entry in root */
    Two                 rightEntryOffset;       /* starting offset of the right entry in the root */
    Two                 firstEntryOffset;       /* starting offset of the first entry in the right child */
    Two                 rightEntryLen;          /* length of the right entry */
    Two                 firstEntryLen;          /* length of the first entry */
                                                /* This is not the leaf entry's length */
                                                /* but the length when moved into the parent */
    btm_InternalEntry   *leftEntry;             /* left entry of the two children in root */
    btm_InternalEntry   *rightEntry;            /* right entry of the two children in root */
    btm_LeafEntry       *firstEntry;            /* the first entry of the right child */
    Two                 lEntryOffset;           /* starting offset of a leaf entry */
    btm_LeafEntry       *lEntry;                /* a leaf entry */
    DeallocListElem     *dlElem;                /* an element of the dealloc list */
    Two                 alignedKlen;            /* aligned length of the key length */
    btm_InternalEntry   *oldEntry;              /* save the old rightEntry */


    TR_PRINT(TR_BTM, TR1,
             ("btm_UnderflowLeaf(handle, pFid=%P, rpage=%P, child=%P, cpage=%P, slotNo=%ld, f=%P, h=%P, item=%P, dlPool=%P, dlHead=%P)",
	      pFid, rpage, child, cpage, slotNo, f, h, item, dlPool, dlHead));
	      
    *f = *h = FALSE;
    
    /* get the volume number; Btree index cannot span across two volumes */
    volNo = child->volNo;
    
    /*
    ** Get the sibling page using the given index indicating the child page.
    ** 'rightEntry' is the right item in the root of the two pages.
    */
    if (slotNo < (rpage->hdr.nSlots-1)) {	/* sibling is a right page */
	flag = FALSE;
	rightSlotNo = slotNo+1;
	rightEntryOffset = rpage->slot[-rightSlotNo];
	rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);
	MAKE_PAGEID(sibling, volNo, rightEntry->spid);
    } else {	/* sibling is a left page */
	flag = TRUE;
	rightSlotNo = slotNo;
	rightEntryOffset = rpage->slot[-rightSlotNo];	
	rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);
	
	if(slotNo != 0) {
	    leftEntryOffset = rpage->slot[-(slotNo-1)];
	    leftEntry = (btm_InternalEntry*)&(rpage->data[leftEntryOffset]);
	    MAKE_PAGEID(sibling, volNo, leftEntry->spid);
	} else
	    MAKE_PAGEID(sibling, volNo, rpage->hdr.p0);
    }
    
    /* Note:
     * At this point rightEntry points to the right entry in root.
     * This information will be used some time after.
     */    
    
    /*@ Get the sibling page and load some useful variables */
    e = BfM_GetTrain(handle, &sibling, (char **)&spage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* calculate the size of the used portion in each page */
    c_size = cpage->hdr.free - cpage->hdr.unused + cpage->hdr.nSlots*sizeof(Two);    
    s_size = spage->hdr.free - spage->hdr.unused + spage->hdr.nSlots*sizeof(Two);
    
    /* If the size of two pages <= the size of a leaf page, then merge
       the two pages into the left page. BL_FIXED includes a slot size. */
    if ((c_size+s_size-sizeof(Two)) <= (PAGESIZE-BL_FIXED)) {	/* merge */
	
	/* Change the child and sibling page for simplicity */
	if (flag) {
            t_size = s_size; s_size = c_size; c_size = t_size;
	    tpage = spage; spage = cpage; cpage = tpage;
	}

	/*@ make room for the merged entries */
	if (BL_CFREE(cpage) < s_size)
	    btm_CompactLeafPage(handle, cpage, NIL);
	
	/* Copy all entries in the right page to the left page. */
	for (i = 0;i < spage->hdr.nSlots;i++) {	    
	    lEntryOffset = spage->slot[-i];
	    lEntry = (btm_LeafEntry*)&(spage->data[lEntryOffset]);
	    alignedKlen = ALIGNED_LENGTH(lEntry->klen);
	    
	    if(lEntry->nObjects < 0)	/* overflow page */
		length = BTM_LEAFENTRY_FIXED + alignedKlen + sizeof(ShortPageID);
	    else
		length = BTM_LEAFENTRY_FIXED + alignedKlen +
		    lEntry->nObjects * OBJECTID_SIZE;
	    
	    cpage->slot[-cpage->hdr.nSlots] = cpage->hdr.free;
	    memcpy(&(cpage->data[cpage->hdr.free]), (char*)lEntry, length);
	    cpage->hdr.nSlots++, cpage->hdr.free += length;
	}
	
	/* 'nextPage' and 'prevPage' field should be updated. */
	MAKE_PAGEID(nextPage, volNo, spage->hdr.nextPage);
	cpage->hdr.nextPage = nextPage.pageNo;
	if (nextPage.pageNo != NIL) {
	    e = BfM_GetTrain(handle, &nextPage, (char **)&tpage, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &sibling, PAGE_BUF);

	    tpage->hdr.prevPage = spage->hdr.prevPage;
	    
	    e = BfM_SetDirty(handle, &nextPage, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &sibling, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, &nextPage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	}
	
	/*@ Delete the entry, which is copied to the left page, from the root page */
	length = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two)+rightEntry->klen);
	for(i = rightSlotNo+1;i < rpage->hdr.nSlots;i++)
	    rpage->slot[-(i-1)] = rpage->slot[-i];
	if (rightEntryOffset + length == rpage->hdr.free)
	    rpage->hdr.free -= length;
	else
	    rpage->hdr.unused += length;
	rpage->hdr.nSlots--;
	
	/* the right page is not used any more. */
	/* Deallocate this page */
	/*
	** Insert a new node for the dropped file.
	*/
	spage->hdr.type = FREEPAGE;
	
	/* bluechry test ... */
        if (sibling.volNo == 1000 && sibling.pageNo == 165) {
            printf("### [pID=%d, tID=%d] Set FreePage: [%d, %d] at %x\n", procIndex, handle, sibling.volNo, sibling.pageNo, spage);
	    printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
            fflush(stdout);
        }
        /* ... bluechry test */

	e = Util_getElementFromPool(handle, dlPool, &dlElem);
	if (e < 0) ERR(handle, e);

        dlElem->type = DL_PAGE;
	dlElem->elem.pid = flag ? *child:sibling; /* Save the PageID. */
	/* dlElem->pFid = *pFid; */  /* save the file identifier */
	dlElem->next = dlHead->next; /* insert to the list */
	dlHead->next = dlElem;       /* new first element of the list */
	
    } else {	/* redistribute */
	
        diff = (CONSTANT_CASTING_TYPE)((c_size + s_size)/2) - c_size;   /* difference */

	/*@ make room for the moved entries */
	if (BL_CFREE(cpage) < diff)
	    btm_CompactLeafPage(handle, cpage, NIL);
	
	/* If the left page is a sibling, entries are inserted in front of
	 * a child page, else appended to the child page.
	 */
	while (diff > 0) {
	    if (flag)		/* move the last entry */
		lEntryOffset = spage->slot[-(spage->hdr.nSlots-1)];
	    else		/* move the first entry */
		lEntryOffset = spage->slot[0];
	    
	    lEntry = (btm_LeafEntry*)&(spage->data[lEntryOffset]);
	    alignedKlen = ALIGNED_LENGTH(lEntry->klen);
	    
	    if (lEntry->nObjects < 0)	/* overflow page */
		length = BTM_LEAFENTRY_FIXED + alignedKlen + sizeof(ShortPageID);
	    else
		length = BTM_LEAFENTRY_FIXED + alignedKlen +
		    lEntry->nObjects*OBJECTID_SIZE;

	    if (length + sizeof(Two) > diff) break; 
	    
	    /* Copy an entry to the child page */	    
	    memcpy(&(cpage->data[cpage->hdr.free]), (char*)lEntry, length);
	    	
	    /*@ Delete the entry from the sibling */
	    if (lEntryOffset + length == spage->hdr.free)
		spage->hdr.free -= length;
	    else
		spage->hdr.unused += length;
    
	    if (!flag)	/* delete the slot from the sibling */
		for(i = 1;i < spage->hdr.nSlots;i++)
		    spage->slot[-(i-1)] = spage->slot[-i];

	    /* update the 'nSlots' of spage */
	    spage->hdr.nSlots--;
	    
	    if (flag) {
		for(i = cpage->hdr.nSlots - 1;i >= 0;i--)
		    cpage->slot[-(i+1)] = cpage->slot[-i];
		cpage->slot[0] = cpage->hdr.free;
	    } else
		cpage->slot[-cpage->hdr.nSlots] = cpage->hdr.free;
	    
	    /* Update 'free' and 'nSlots' of cpages */
	    cpage->hdr.nSlots++, cpage->hdr.free += length;
	    
	    diff -= (length + sizeof(Two));
	}
	
	/* Get the entry which will be replaced by the old entry in the root */
	/* rightEntry: old delimeter for the right child in rpage */
	/* firstEntry: new delimeter for the right child in rpage */
	if (flag) {
	    firstEntryOffset = cpage->slot[0];
	    firstEntry = (btm_LeafEntry*)&(cpage->data[firstEntryOffset]);
	} else {
	    firstEntryOffset = spage->slot[0];
	    firstEntry = (btm_LeafEntry*)&(spage->data[firstEntryOffset]);
	}
	
	/* Adjust */
	rightEntryLen = sizeof(ShortPageID) +
	    ALIGNED_LENGTH(sizeof(Two)+rightEntry->klen);
	firstEntryLen = sizeof(ShortPageID) + 
	    ALIGNED_LENGTH(sizeof(Two)+firstEntry->klen);
	diff = firstEntryLen - rightEntryLen;
	
	if (diff <= BI_FREE(rpage)) {
	    
	    if (rightEntryOffset + rightEntryLen == rpage->hdr.free &&
		diff <= BI_CFREE(rpage)) {

		rpage->hdr.free += diff;
		
	    } else if (diff <= 0) {

		rpage->hdr.unused += -diff;
		
	    } else if (firstEntryLen <= BI_CFREE(rpage)) {

		/*@ move the entry to the free space */		
		rightEntryOffset = rpage->slot[-rightSlotNo] = rpage->hdr.free;
		oldEntry = rightEntry;		
		rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);
		
		rightEntry->spid = oldEntry->spid;
		
		rpage->hdr.unused += rightEntryLen;
		rpage->hdr.free += firstEntryLen;
		
	    } else {		/* no enough contiguous space */
		
		btm_CompactInternalPage(handle, rpage, rightSlotNo);
		/* rightEntry have moved in btm_CompactInternalPage(). */
		rightEntryOffset = rpage->slot[-rightSlotNo];
		rightEntry = (btm_InternalEntry*)&(rpage->data[rightEntryOffset]);		
		rpage->hdr.free += diff;		
	    }
	    
	    rightEntry->klen = firstEntry->klen;
	    memcpy(&(rightEntry->kval[0]), &(firstEntry->kval[0]), rightEntry->klen);
	    
	} else {
	    /* Insert after delete */
	    if (rightEntryOffset + rightEntryLen == rpage->hdr.free)
		rpage->hdr.free -= rightEntryLen;
	    else
		rpage->hdr.unused += rightEntryLen;
	    
	    for(i = rightSlotNo+1;i < rpage->hdr.nSlots;i++)
		rpage->slot[-(i-1)] = rpage->slot[-i];
	    rpage->hdr.nSlots--;

	    /* construct the internal item for the insertion in rpage */
	    item->spid = rightEntry->spid;
	    item->klen = firstEntry->klen;
	    memcpy(&(item->kval[0]), &(firstEntry->kval[0]), item->klen);
	    *h = TRUE;
	}
    }
    
    /* 'f' is TRUE if the size of the root is not half full. */
    if (BI_FREE(rpage) > BI_HALF) *f = TRUE;
    
    e = BfM_SetDirty(handle, &sibling, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &sibling, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &sibling, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
	
} /* btm_UnderflowLeaf() */
