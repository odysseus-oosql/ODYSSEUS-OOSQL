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
 * Module: btm_Insert.c
 *
 * Description : 
 *  This function btm_Insert(...) recursively calls itself until the type
 *  of a root page becomes LEAF.  If the given root page is an internal,
 *  it recursively calls itself using a proper child.  If the result of
 *  the call occur spliting, merging, or redistributing the children, it
 *  may insert, delete, or replace its own internal item, and if the given
 *  root page may be merged, splitted, or redistributed, it affects the
 *  return values.
 *
 * Exports:
 *  Four btm_Insert(ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*,
 *                  Boolean*, Boolean*, InternalItem*, Pool*, DeallocListElem*)
 *  Four btm_InsertLeaf(ObjectID*, PageID*, BtreeLeaf*, KeyDesc*, KeyValue*,
 *                      ObjectID*, Boolean*, Boolean*, InternalItem*)
 *  Four btm_InsertInternal(ObjectID*, BtreeInternal*, InternalItem*,
 *                          Two, Boolean*, InternalItem*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "OM_Internal.h"	/* for SlottedPage containing catalog object */
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_Insert()
 *================================*/
/*
 * Function: Four btm_Insert(ObjectID*, PageID*, KeyDesc*, KeyValue*,
 *                           ObjectID*, Boolean*, Boolean*, InternalItem*,
 *                           Pool*, DeallocListElem*)
 *
 * Description:
 *  If the given root is a leaf page, it should get the correct entry in the
 *  leaf. If the entry is already in the leaf, it simply insert it into the
 *  entry and increment the number of ObjectIDs.  If it is not in the leaf it
 *  makes a new entry and insert it into the leaf.
 *  If there is not enough spage in the leaf, the page should be splitted.  The
 *  overflow page may be used or created by this routine. It is created when
 *  the size of the entry is greater than a third of a page.
 * 
 *  'h' is TRUE if the given root page is splitted and the entry item will be
 *  inserted into the parent page.  'f' is TRUE if the given page is not half
 *  full because of creating a new overflow page.
 *
 * Returns:
 *  Error code
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 */
Four btm_Insert(
    Four handle,
    ObjectID                    *catObjForFile,         /* IN catalog object of B+-tree file */
    PageID                      *root,                  /* IN the root of a Btree */
    KeyDesc                     *kdesc,                 /* IN Btree key descriptor */
    KeyValue                    *kval,                  /* IN key value */
    ObjectID                    *oid,                   /* IN ObjectID which will be inserted */
    Boolean                     *f,                     /* OUT whether it is merged by creating a new overflow page */
    Boolean                     *h,                     /* OUT whether it is splitted */
    InternalItem                *item,                  /* OUT Internal Item which will be inserted */
                                                        /*     into its parent when 'h' is TRUE */
    Pool                        *dlPool,                /* INOUT pool of dealloc list */
    DeallocListElem             *dlHead)                /* INOUT head of the dealloc list */
{
    Four                        e;                      /* error number */
    Boolean                     lh;                     /* local 'h' */
    Boolean                     lf;                     /* local 'f' */
    Two                         idx;                    /* index for the given key value */
    PageID                      newPid;                 /* a new PageID */
    KeyValue                    tKey;                   /* a temporary key */
    InternalItem                litem;                  /* a local internal item */
    BtreePage                   *apage;                 /* a pointer to the root page */
    btm_InternalEntry           *iEntry;                /* an internal entry */
    Two                         iEntryOffset;           /* starting offset of an internal entry */
    SlottedPage                 *catPage;               /* buffer page containing the catalog object */
    sm_CatOverlayForBtree       *catEntry;              /* pointer to Btree file catalog information */
    PhysicalFileID              pFid;                   /* B+-tree file's FileID */ 


    TR_PRINT(TR_BTM, TR1,
             ("btm_Insert(handle, catObjForFile=%P, root=%P, kdesc=%P, kval=%P, oid=%P, f=%P, h=%P, item=%P)",
	      catObjForFile, root, kdesc, kval, oid, f, h, item));
    
    /* Get the B+ tree file's FileID from the catalog object */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    GET_PTR_TO_CATENTRY_FOR_BTREE(catObjForFile, catPage, catEntry);
    
    MAKE_PHYSICALFILEID(pFid, catEntry->fid.volNo, catEntry->firstPage); 
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /*@ Initially the flags are FALSE */
    *h = *f = FALSE;
    
    e = BfM_GetTrain(handle, root, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (apage->any.hdr.type & INTERNAL) {	/* Internal */
	
	/*@ Search the correct position */
	(Boolean) btm_BinarySearchInternal(handle, &(apage->bi), kdesc, kval, &idx);

	/*@ Get the correct child page */
	if(idx >= 0) {
	    iEntryOffset = apage->bi.slot[-idx];
	    iEntry = (btm_InternalEntry*)&(apage->bi.data[iEntryOffset]);
	    MAKE_PAGEID(newPid, root->volNo, iEntry->spid);
	} else
	    MAKE_PAGEID(newPid, root->volNo, apage->bi.hdr.p0);
	
	/* Recursively call using the child */
	e = btm_Insert(handle, catObjForFile, &newPid, kdesc, kval, oid, &lf, &lh,
		       &litem, dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	
	if (lh) {		/* the child was splitted */
	    /*@ find the correct position */
	    tKey.len = litem.klen;
	    memcpy(&(tKey.val[0]), &(litem.kval[0]), tKey.len);
	    (Boolean) btm_BinarySearchInternal(handle,  &(apage->bi), kdesc, &tKey, &idx);
	    
	    /* Insert the returned internal item into the given root page */
	    e = btm_InsertInternal(handle, catObjForFile, &(apage->bi), &litem, idx, h, item);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    
	    e = BfM_SetDirty(handle, root, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    
	} else if( lf ) {	/* the child is not half full */
	    e = btm_Underflow(handle, &pFid, apage, &newPid, idx, f, &lh, &litem, 
			      dlPool, dlHead);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    
	    if (lh) {
		/*@ find the correct position */
		tKey.len = litem.klen;
		memcpy(&(tKey.val[0]), &(litem.kval[0]), tKey.len);
		(Boolean) btm_BinarySearchInternal(handle, &(apage->bi), kdesc, &tKey, &idx);
		
		e = btm_InsertInternal(handle, catObjForFile, &(apage->bi),
				       &litem, idx, h, item);
		if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	    }
	    
	    e = BfM_SetDirty(handle, root, PAGE_BUF);
	    if( e < 0 ) ERRB1(handle,  e, root, PAGE_BUF );
	}
	
    } else if( apage->any.hdr.type & LEAF ) {
	
	e = btm_InsertLeaf(handle, catObjForFile, root, &(apage->bl),
			    kdesc, kval, oid, f, h, item);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	
	e = BfM_SetDirty(handle, root, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	
    } else
	ERRB1(handle, eBADBTREEPAGE_BTM, root, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
}   /* btm_Insert() */



/*@================================
 * btm_InsertLeaf()
 *================================*/
/*
 * Function: Four btm_InsertLeaf(ObjectID*, PageID*, BtreeLeaf*, KeyDesc*,
 *                               KeyValue*, ObjectID*, Boolean*, Boolean*,
 *                               InternalItem*)
 *
 * Description:
 *  Insert into the given leaf page an ObjectID with the given key.
 *
 * Returns:
 *  Error code
 *    eDUPLICATEDOBJECTID_BTM
 *    some errors causd by function calls
 *
 * Side effects:
 *  1) f : TRUE if the leaf page is underflowed by creating an overflow page
 *  2) h : TRUE if the leaf page is splitted by inserting the given ObjectID
 *  3) item : item to be inserted into the parent
 */
Four btm_InsertLeaf(
    Four handle,
    ObjectID                    *catObjForFile, /* IN catalog object of B+-tree file */
    PageID                      *pid,           /* IN PageID of Leag Page */
    BtreeLeaf                   *page,          /* INOUT pointer to buffer page of Leaf page */
    KeyDesc                     *kdesc,         /* IN Btree key descriptor */
    KeyValue                    *kval,          /* IN key value */
    ObjectID                    *oid,           /* IN ObjectID which will be inserted */
    Boolean                     *f,             /* OUT whether it is merged by creating */
                                                /*     a new overflow page */
    Boolean                     *h,             /* OUT whether it is splitted */
    InternalItem                *item)          /* OUT Internal Item which will be inserted */
                                                /*     into its parent when 'h' is TRUE */
{
    Four                        e;              /* error number */
    Two                         i;
    Two                         idx;            /* index for the given key value */
    LeafItem                    leaf;           /* a Leaf Item */
    Boolean                     found;          /* search result */
    btm_LeafEntry               *entry;         /* an entry in a leaf page */
    Two                         entryOffset;    /* start position of an entry */
    Two                         alignedKlen;    /* aligned length of the key length */
    PageID                      ovPid;          /* PageID of an overflow page */
    Two                         entryLen;       /* length of an entry */
    ObjectID                    *oidArray;      /* an array of ObjectIDs */
    Two                         oidArrayElemNo; /* an index for the ObjectID array */


    TR_PRINT(TR_BTM, TR1,
             ("btm_InsertLeaf(handle, catObjForFile=%P, pid=%P, page=%P, kdesc=%P, kval=%P, oid=%P, f=%P, h=%P, item=%P)",
	      catObjForFile, pid, page, kdesc, kval, oid, f, h, item));
    
    /*@ Initially the flags are FALSE */
    *h = *f = FALSE;
    
    /*@ Search the leaf entry */
    found = btm_BinarySearchLeaf(handle, page, kdesc, kval, &idx);
    
    /* check duplicated key */
    if ((found) && (kdesc->flag & KEYFLAG_UNIQUE))
        return (eDUPLICATEDKEY_BTM);

    if (found) {	/* found */
	entryOffset = page->slot[-idx];
	entry = (btm_LeafEntry*)&(page->data[entryOffset]);
	alignedKlen = ALIGNED_LENGTH(entry->klen);
	
	if (entry->nObjects < 0) {	/* overflow page */
	    MAKE_PAGEID(ovPid, pid->volNo, *((ShortPageID*)&entry->kval[alignedKlen]));
	    /* Use a special routine for inserting into an overflow page */
	    e = btm_InsertOverflow(handle, catObjForFile, &ovPid, oid);
	    if (e < 0) ERR(handle, e);
	} else {
	    /* the total length of the entry which includes the given ObjectID */
	    entryLen = BTM_LEAFENTRY_FIXED + alignedKlen +
		(entry->nObjects+1)*OBJECTID_SIZE;
	    	    
	    /* make a new overflow page */
	    if (entryLen > OVERFLOW_SPLIT) {
		e = btm_CreateOverflow(handle, catObjForFile, page, idx, oid);
		if (e < 0) ERR(handle, e);
				
		/* Creating a new overflow page may
		   make the leaf page be less than half of a page */
		if (BL_FREE(page) > BL_HALF) *f = TRUE;
		
	    } else if(OBJECTID_SIZE <= BL_FREE(page)) { /* enough space */
		
		/*@ Find the correct position which the given object is inserted */
		oidArray = (ObjectID*)&(entry->kval[alignedKlen]);

		e = btm_BinarySearchOidArray(handle, oidArray, oid,
					     entry->nObjects, &oidArrayElemNo);
		if (e) ERR(handle, eDUPLICATEDOBJECTID_BTM);

		if (entryOffset + entryLen - OBJECTID_SIZE == page->hdr.free &&
		    BL_CFREE(page) >= OBJECTID_SIZE) {		

		    page->hdr.free += OBJECTID_SIZE;
		    
		} else if (BL_CFREE(page) >= entryLen) {

		    entryOffset = page->slot[-idx] = page->hdr.free;
		    memcpy(&(page->data[entryOffset]), (char*)entry, entryLen-OBJECTID_SIZE);
		    entry = (btm_LeafEntry*)&(page->data[entryOffset]);
		    oidArray = (ObjectID*)&(entry->kval[alignedKlen]);
		    
		    page->hdr.free += entryLen;
		    page->hdr.unused += entryLen - OBJECTID_SIZE;
		    
		} else {
		    btm_CompactLeafPage(handle, page, idx);

		    entryOffset = page->slot[-idx];
		    entry = (btm_LeafEntry*)&(page->data[entryOffset]);
		    oidArray = (ObjectID*)&(entry->kval[alignedKlen]);

		    page->hdr.free += OBJECTID_SIZE;
		}
		
		/*
		** empty (oidArrayElemNo+1)-th element by moving
		** the ObjectIDs.
		*/
		memmove((char*)&(oidArray[oidArrayElemNo+2]), (char*)&(oidArray[oidArrayElemNo+1]),
			OBJECTID_SIZE*(entry->nObjects-(oidArrayElemNo+1)));
		oidArray[oidArrayElemNo+1] = *oid;

		entry->nObjects++;
		
	    } else {	/* split */
		
		/*
		** There is no enough spage, split the leaf page and insert
		** the ObjectID. The key value is extracted directly from the
		** idx-th entry of the leaf page in btm_SplitLeaf().
		*/
		leaf.nObjects = entry->nObjects;
		leaf.oid = *oid;
		
		e = btm_SplitLeaf(handle, catObjForFile, pid, page, idx-1, &leaf, item);
		if (e < 0) ERR(handle, e);
		
		*h = TRUE;	/* Mark */
	    }
	}
	
    } else {	/* not found */
	
	/* Make a new entry and insert it into the leaf page */
	
	/* the length of a new entry */
	alignedKlen = ALIGNED_LENGTH(kval->len);
	entryLen = BTM_LEAFENTRY_FIXED + alignedKlen + sizeof(ObjectID);

	/* There is enough space? We should count the slot space. */
	if (entryLen + sizeof(Two) <= BL_FREE(page)) { 	/*enough space */

	    if (BL_CFREE(page) < entryLen+sizeof(Two))
		btm_CompactLeafPage(handle, page, NIL);
	    
	    /* Move slots for a new slot */
	    for(i = page->hdr.nSlots-1; i >= idx+1; i--)
		page->slot[-(i+1)] = page->slot[-i];
	    
	    /*@ Store the new entry */
	    entryOffset = page->slot[-(idx+1)] = page->hdr.free;
	    entry = (btm_LeafEntry*)&(page->data[entryOffset]);

	    entry->nObjects = 1;
	    entry->klen = kval->len;
	    memcpy(&(entry->kval[0]), &(kval->val[0]), entry->klen);
	    *((ObjectID*)&(entry->kval[alignedKlen])) = *oid;
	    
	    page->hdr.free += entryLen;	/* slot size is not included */
	    page->hdr.nSlots++;
	    
	} else {	/* There is not enough space */
	    
	    /* Construct the new entry, leaf item */
	    leaf.nObjects = 0;
	    leaf.klen = kval->len;
	    memcpy(&(leaf.kval[0]), &(kval->val[0]), leaf.klen);
	    leaf.oid = *oid;
	    
	    e = btm_SplitLeaf(handle, catObjForFile, pid, page, idx, &leaf, item);
	    if (e < 0) ERR(handle, e);
	    
	    *h = TRUE;	/* mark */
	}	
    }
    
    return(eNOERROR);
    
} /* btm_InsertLeaf() */



/*@================================
 * btm_InsertInternal()
 *================================*/
/*
 * Function: Four btm_InsertInternal(ObjectID*, BtreeInternal*, InternalItem*, Two, Boolean*, InternalItem*)
 *
 * Description:
 *  This routine insert the given internal item into the given page. If there
 *  is not enough space in the page, it should split the page and the new
 *  internal item should be returned for inserting into the parent.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side effects:
 *  h:	TRUE if the page is splitted
 *  ritem: an internal item which will be inserted into parent
 *          if spliting occurs.
 */
Four btm_InsertInternal(
    Four handle,
    ObjectID            *catObjForFile, /* IN catalog object of B+-tree file */
    BtreeInternal       *page,          /* INOUT Page Pointer */
    InternalItem        *item,          /* IN Iternal item which is inserted */
    Two                 high,           /* IN index in the given page */
    Boolean             *h,             /* OUT whether the given page is splitted */
    InternalItem        *ritem)         /* OUT if the given page is splitted, the internal item may be returned by 'ritem'. */
{
    Four                e;              /* error number */
    Two                 i;              /* index */
    Two                 entryOffset;    /* starting offset of an internal entry */
    Two                 entryLen;       /* length of the new entry */
    btm_InternalEntry   *entry;         /* an internal entry of an internal page */


    TR_PRINT(TR_BTM, TR1,
             ("btm_InsertInternal(handle, catObjForFile=%P, page=%P, item=%P, high=%ld, h=%P, ritem=%P)",
	      catObjForFile, page, item, high, h, ritem));

    
    /*@ Initially the flag are FALSE */
    *h = FALSE;
    
    /* the length of a entry */
    entryLen = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two)+item->klen);

    /* There is enough space? We should count the slot space. */
    if(BI_FREE(page) < entryLen+sizeof(Two)) {   /* not enough */
	
	/* Insert the item after spliting the given internal page */
	e = btm_SplitInternal(handle, catObjForFile, page, high, item, ritem);
	if (e < 0) ERR(handle, e);
	
	*h = TRUE;	/* mark */
	
    } else {	/* enough space */

	if (BI_CFREE(page) < entryLen + sizeof(Two))
	    btm_CompactInternalPage(handle, page, NIL);
	
	/* Move slots for a new slot */
	for(i = page->hdr.nSlots-1; i > high; i--)
	    page->slot[-(i+1)] = page->slot[-i];
	
	/*@ store the new entry and update some informations */
	entryOffset = page->slot[-(high+1)] = page->hdr.free;
	entry = (btm_InternalEntry*)&(page->data[entryOffset]);
	memcpy((char*)entry, (char*)item, entryLen);
	page->hdr.nSlots++;
	page->hdr.free += entryLen;
	
	*h = FALSE;		/* it is not splitted */
    }
    
    return(eNOERROR);
    
} /* btm_InsertInternal() */

