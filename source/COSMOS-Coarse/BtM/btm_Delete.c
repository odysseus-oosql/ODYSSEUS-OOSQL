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
 * Module: btm_Delete.c
 *
 * Description : 
 *  This function btm_Delete(...) recursively calls itself until the type
 *  of root page becomes LEAF.  If the root page is an internal page, it
 *  may get the proper child page using the binary search routine and then
 *  recursively calls itself using the child as a root page. If the filled
 *  area of the child page is less than half of the page, it should merge
 *  or redistribute using the given root, and set the flag 'f' according to
 *  the result status of the given root page.
 *
 *  If the root page is a leaf page , it find out the correct node (entry)
 *  using the binary search routine.  If the entry is normal,  it simply
 *  delete the ObjectID or the entry when the # of ObjectIDs becomes zero.
 *  The entry, however, is not normal, that is, if the overflow page is used,
 *  the special routine btm_DeleteOverflow(...) should be called. The # of
 *  ObjectIDs will be returned by the result of the btm_DeleteOverflow(...),
 *  if the total # of ObjectIDs is less than 1/4 of the page and the ObjectIDs
 *  in the overflow page should be moved to the leaf page. (This process may
 *  has a complicate problem which the leaf page may be splitted in spite of
 *  deleteing not inserting an ObjectID.)
 *
 *  Deleting an ObjectID may cause redistribute pages and by this reason, the
 *  page may be splitted.
 *
 * Exports:
 *  Four btm_Delete(ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*,
 *                  Boolean*, Boolean*, InternalItem*, Pool*, DeallocListElem*)
 *
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "OM_Internal.h"	/* for "SlottedPage" including catalog object */
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ Internal Function Prototypes */
Four btm_DeleteLeaf(Four, PhysicalFileID*, PageID*, BtreeLeaf*, KeyDesc*, KeyValue*, ObjectID*,
		    Boolean*, Boolean*, InternalItem*, Pool*, DeallocListElem*);



/*@================================
 * btm_Delete()
 *================================*/
/*
 * Function: Four btm_Delete(ObjectID*, PageID*, KeyDesc*, KeyValue*,
 *                           ObjectID*, Boolean*, Boolean*, InternalItem*)
 *
 * Description:
 *
 * Returns:
 *  error code
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 *  f    : TRUE if the given root page is not half full.
 *  h    : TRUE if the given page is splitted.
 *  item : The internal item to be inserted into the parent if 'h' is TRUE.
 */
Four btm_Delete(
    Four handle,
    ObjectID                    *catObjForFile, /* IN catalog object of B+ tree file */
    PageID                      *root,          /* IN root page */
    KeyDesc                     *kdesc,         /* IN a key descriptor */
    KeyValue                    *kval,          /* IN key value */
    ObjectID                    *oid,           /* IN Object IDentifier which will be deleted */
    Boolean                     *f,             /* OUT whether the root page is half full */
    Boolean                     *h,             /* OUT TRUE if it is spiltted. */
    InternalItem                *item,          /* OUT The internal item to be returned */
    Pool                        *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)        /* INOUT head of the dealloc list */
{
    Four                        e;              /* error number */
    Boolean                     lf;             /* TRUE if a page is not half full */
    Boolean                     lh;             /* TRUE if a page is splitted */
    Two                         idx;            /* the index by the binary search */
    PageID                      child;          /* a child page when the root is an internal page */
    KeyValue                    tKey;           /* a temporary key */
    BtreePage                   *rpage;         /* for a root page */
    InternalItem                litem;          /* local internal item */
    btm_InternalEntry           *iEntry;        /* an internal entry */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForBtree       *catEntry;      /* pointer to Btree file catalog information */
    PhysicalFileID              pFid;           /* B+-tree file's FileID */ 
  

    TR_PRINT(TR_BTM, TR1,
             ("btm_Delete(handle, catObjForFile=%P, root=%P, kdesc=%P, kval=%P, oid=%P, f=%P, h=%P, item=%P, dlPool=%P, dlHead=%P)",
	      catObjForFile, root, kdesc, kval, oid, f, h, item, dlPool, dlHead));
        
    *h = *f = FALSE;
    
    /* Get the B+ tree file's FileID from the catalog object */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    GET_PTR_TO_CATENTRY_FOR_BTREE(catObjForFile, catPage, catEntry);
    
    MAKE_PHYSICALFILEID(pFid, catEntry->fid.volNo, catEntry->firstPage); 
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    e = BfM_GetTrain(handle, root, (char **)&rpage, PAGE_BUF);	/*@ Disk -> Buffer */
    if (e < 0) ERR(handle, e);
    
    if (rpage->any.hdr.type & INTERNAL) {
	
	/*@ Search the correct child page */
	(Boolean) btm_BinarySearchInternal(handle, &(rpage->bi), kdesc, kval, &idx);
	    
	if (idx >= 0) {
	    iEntry = (btm_InternalEntry*)&(rpage->bi.data[rpage->bi.slot[-idx]]);
	    MAKE_PAGEID(child, root->volNo, iEntry->spid);
	} else
	    MAKE_PAGEID(child, root->volNo, rpage->bi.hdr.p0);
	
	/*@ recursively call */
	e = btm_Delete(handle, catObjForFile, &child, kdesc, kval, oid, &lf, &lh,
		       &litem, dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	
	/* if 'lf' is TRUE, the child page should be merged or redistributed
	 * since it is not half full. The flag 'f' will be set to TRUE
	 * if the given root is not half full.
	 */
	if (lh) {
	    /*@ find the correct position */
	    tKey.len = litem.klen;
	    memcpy(&(tKey.val[0]), &(litem.kval[0]), tKey.len);
	    (Boolean) btm_BinarySearchInternal(handle, &(rpage->bi), kdesc, &tKey, &idx);
	    
	    e = btm_InsertInternal(handle, catObjForFile, &(rpage->bi), &litem, idx, h, item);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    
	    e = BfM_SetDirty(handle, root, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    
	} else if (lf) {
	    e = btm_Underflow(handle, &pFid, rpage, &child, idx, f, &lh, &litem, dlPool, dlHead); 
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    
	    if(lh) {
		/*@ find the correct position */
		tKey.len = litem.klen;
		memcpy(&(tKey.val[0]), &(litem.kval[0]), tKey.len);
		(Boolean) btm_BinarySearchInternal(handle, &(rpage->bi), kdesc, &tKey, &idx);
		
		e = btm_InsertInternal(handle, catObjForFile, &(rpage->bi), &litem, idx, h, item);
		if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    }
	    
	    e = BfM_SetDirty(handle, root, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	}
	
    } else if( rpage->any.hdr.type & LEAF ) {
	e = btm_DeleteLeaf(handle, &pFid, root, &(rpage->bl), 
			   kdesc, kval, oid, f, h, item, dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
    } else 
	ERRB1(handle, eBADBTREEPAGE_BTM, root, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
}   /* btm_Delete() */



/*@================================
 * btm_DeleteLeaf()
 *================================*/
/*
 * Function: Four btm_DeleteLeaf(PhysicalFileID*, PageID*, BtreeLeaf*, KeyDesc*,
 *                               KeyValue*, ObjectID*, Boolean*, Boolean*,
 *                               InternalItem*, Pool*, DeallocListElem*)
 *
 * Description:
 *
 * Returns:
 *  Error code
 *    eNOTFOUND_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 *  f    : TRUE if the given root page is not half full.
 *  h    : TRUE if the given page is splitted.
 *  item : The internal item to be inserted into the parent if 'h' is TRUE.
 */ 
Four btm_DeleteLeaf(
    Four 			handle,
    PhysicalFileID              *pFid,          /* IN FileID of the Btree file */
    PageID                      *pid,           /* IN PageID of the leaf page */
    BtreeLeaf                   *apage,         /* INOUT buffer for the Leaf Page */
    KeyDesc                     *kdesc,         /* IN a key descriptor */
    KeyValue                    *kval,          /* IN key value */
    ObjectID                    *oid,           /* IN ObjectID which will be deleted */
    Boolean                     *f,             /* OUT whether the root page is half full */
    Boolean                     *h,             /* OUT TRUE if it is spiltted. */
    InternalItem                *item,          /* OUT The internal item to be returned */
    Pool                        *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)        /* INOUT head of a dealloc list */
{
    Four                        e;              /* error number */
    Two                         i;              /* index */
    Two                         of;             /* # of ObjectIDs of an overflow page when less than 1/4 */
    Two                         idx;            /* the index by the binary search */
    ObjectID                    tOid;           /* a Object IDentifier */
    BtreeOverflow               *opage;         /* for a overflow page */
    Boolean                     found;          /* Search Result */
    Two                         lEntryOffset;   /* starting offset of a leaf entry */
    btm_LeafEntry               *lEntry;        /* an entry in leaf page */
    ObjectID                    *oidArray;      /* start position of the ObjectID array */
    Two                         oidArrayElemNo; /* element number in the ObjectIDs array */
    Two                         entryLen;       /* length of the old leaf entry */
    Two                         newLen;         /* length of the new leaf entry */
    Two                         alignedKlen;    /* aligned length of the key length */
    PageID                      ovPid;          /* overflow page's PageID */
    DeallocListElem             *dlElem;        /* an element of the dealloc list */


    TR_PRINT(TR_BTM, TR1,
             ("btm_DeleteLeaf(handle, pFid=%P, pid=%P, apage=%P, kdesc=%P, kval=%P, oid=%P, f=%P, h=%P, item=%P, dlPool=%P, dlHead=%P)",
	      pFid, pid, apage, kdesc, kval, oid, f, h, item, dlPool, dlHead));
	      
    /*@ Search the entry */
    found = btm_BinarySearchLeaf(handle, apage, kdesc, kval, &idx);
    
    if (!found) return(eNOTFOUND_BTM);
    
    lEntryOffset = apage->slot[-idx];
    lEntry = (btm_LeafEntry*)&(apage->data[lEntryOffset]);
    
    /* aligned length of the key length */
    alignedKlen = ALIGNED_LENGTH(lEntry->klen);
    
    if (lEntry->nObjects < 0) {		/* Overflow Page */
	
	MAKE_PAGEID(ovPid, pid->volNo, *((ShortPageID*)&(lEntry->kval[alignedKlen])));
	
	e = btm_DeleteOverflow(handle, pFid, &ovPid, oid, &of, dlPool, dlHead);
	if (e < 0) ERR(handle, e);
	
	/* of = # of ObjectIDs when less than 1/4 */
	if (of) {
	    if (of*OBJECTID_SIZE <= (BL_FREE(apage)+sizeof(ShortPageID))) {
		/*@ Get the page. */
		e = BfM_GetTrain(handle, &ovPid, (char **)&opage, PAGE_BUF);
		if (e < 0) ERR(handle, e);
		
		/* length of the leaf entry */
		entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + sizeof(ShortPageID);
		
		/* length of the leaf entry after adding ObjectID array */
		newLen = BTM_LEAFENTRY_FIXED + alignedKlen + of*OBJECTID_SIZE;
		
		if (lEntryOffset + entryLen == apage->hdr.free &&
		    of*OBJECTID_SIZE <= BL_CFREE(apage)+sizeof(ShortPageID)) {
		    /* Case 1: entry is on the boundary of the free space */
		    /* Case 1 should proceed the case 2. */
		    apage->hdr.free += of*OBJECTID_SIZE - sizeof(ShortPageID);
		    
		} else if (newLen <= BL_CFREE(apage)) {
		    /* Case 2: There is enough contigous space. */
		    
		    /* move the leaf entry to another position */
		    apage->slot[-idx] = apage->hdr.free;
		    
		    /* ShortPageID of overflow page is no longer used */
		    memcpy(&(apage->data[apage->hdr.free]), (char*)lEntry,
			       entryLen-sizeof(ShortPageID));
		    
		    /* 'entry' points to the moved entry */
		    lEntry = (btm_LeafEntry*)&(apage->data[apage->hdr.free]);
		    
		    apage->hdr.free += newLen;
		    apage->hdr.unused += entryLen;
		    
		} else {
		    /* Case 3: we should compact the leaf page. */
		    
		    /* compact the leaf page to make the contigous space */
		    btm_CompactLeafPage(handle, apage, idx);
		    
		    lEntryOffset = apage->slot[-idx];
		    lEntry = (btm_LeafEntry*)&(apage->data[lEntryOffset]);
		    
		    /* At this point, the situation is same with case 1 */
		    apage->hdr.free += of*OBJECTID_SIZE - sizeof(ShortPageID);
		}
		
		/* the original leaf entry is on free space boundary */
		oidArray = (ObjectID*)&(lEntry->kval[alignedKlen]);
		memcpy((char*)&(oidArray[0]), (char*)&(opage->oid[0]), of*OBJECTID_SIZE);
		
		lEntry->nObjects = of;
		
		opage->hdr.type = FREEPAGE;
		e = BfM_SetDirty(handle, &ovPid, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, &ovPid, PAGE_BUF);
		
		e = BfM_FreeTrain(handle, &ovPid, PAGE_BUF);
		if (e < 0) ERR(handle, e);

                /* bluechry test ... */
                if (ovPid.volNo == 1000 && ovPid.pageNo == 165) {
                    printf("### [pID=%d, tID=%d] Set FreePage: [%d, %d] at %x\n", procIndex, handle, ovPid.volNo, ovPid.pageNo, opage);	
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
		dlElem->elem.pid = ovPid; /* save the deallcoated PageID. */
		dlElem->next = dlHead->next; /* insert to the list */
		dlHead->next = dlElem;       /* new first element of the list */
		
	    } else {	/* Split it and redistribute */
	    }
	}
	
	/* If there are more than one ObjectID, simply delete the ObjectID. */
    } else if (lEntry->nObjects > 1) {
	
	/* find out the given ObjectID by the bianry search */
	oidArray = (ObjectID*)&(lEntry->kval[alignedKlen]);
	found = btm_BinarySearchOidArray(handle, oidArray, oid,
					 lEntry->nObjects, &oidArrayElemNo);
	
	/* Delete it, and decrement the number of ObjectIDs */
	if (found) {
	    
	    /* total length of the leaf entry */
	    entryLen = BTM_LEAFENTRY_FIXED + alignedKlen +
		lEntry->nObjects*OBJECTID_SIZE;
	    
	    /* free the space */
	    memmove((char*)&(oidArray[oidArrayElemNo]), (char*)&(oidArray[oidArrayElemNo+1]),
		        (lEntry->nObjects-oidArrayElemNo-1)*OBJECTID_SIZE);

	    /*@ decreae the number of objects */
	    lEntry->nObjects --;
	    
	    if (lEntryOffset + entryLen == apage->hdr.free)
		apage->hdr.free -= OBJECTID_SIZE;
	    else
		apage->hdr.unused += OBJECTID_SIZE;
	    
	} else 
	    return(eNOTFOUND_BTM);
	
    } else {	/* leaf.nObjects == 1 */	    
	/* If there is only one ObjectID, delete the entry itself. */
	
	tOid = *((ObjectID*)&(lEntry->kval[alignedKlen]));
	
	if(btm_ObjectIdComp(handle, oid, &tOid) == EQUAL) {
	    
	    /* Move slots to delete the slot */
	    for(i = idx+1; i < apage->hdr.nSlots; i++)
		apage->slot[-(i-1)] = apage->slot[-i];
	    
	    /* total length of the leaf entry */
	    entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + OBJECTID_SIZE;
	    
	    /* free the space and decrement the # of entries by 1 */
	    if (lEntryOffset + entryLen == apage->hdr.free)
		apage->hdr.free -= entryLen;
	    else
		apage->hdr.unused += entryLen;
	    
	    apage->hdr.nSlots--;
	    
	} else
	    return(eNOTFOUND_BTM);
    }
    
    /* Set 'f' to TRUE if the page is not half full. */
    if (BL_FREE(apage) > BL_HALF)	*f = TRUE;
    
    e = BfM_SetDirty(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_DeleteLeaf() */
