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
 * Module: btm_root.c
 *
 * Description : 
 *  This file has two routines which are concerned with the changing the
 *  current root node. When an overflow or a underflow occurs in the root page
 *  the root node should be changed. But we don't change the root node to
 *  the new page. The old root page is used as the new root node; thus the
 *  root page is fixed always.
 *
 * Exports:
 *  Four btm_root_delete(PhysicalFileID*, PageID*, Pool*, DeallocListElem*)
 *  Four btm_root_insert(ObjectID*, PageID*, InternalItem*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_root_delete()
 *================================*/
/*
 * Function: Four btm_root_delete(PhysicalFileID*, PageID*, Pool*, DeallocListElem*)
 *
 * Description:
 *  This function is called when an entry of the root was deleted and the root
 *  is not half full. If the # of entries of the root is zero, the root page 
 *  should be replaced a new one. The new root page will be the page pointed by
 *  'p0'. But we make it a rule to fix the root page. So the contents of the
 *  page pointed by 'p0' are copied to the root node and the child is released.
 *  When the root is a leaf node, we just return; the root node isn't released.
 *
 * Returns:
 *  Error code
 *    eBADPAGE_BUF
 *    some errors caused by function calls
 */
Four btm_root_delete(
    Four handle,
    PhysicalFileID *pFid,	/* IN catalog object of B+ tree file */ 
    PageID   *root,		/* IN Page IDentifier for the root */
    Pool     *dlPool,		/* INOUT pool of dealloc list elements */
    DeallocListElem *dlHead) /* INOUT head of the dealloc list */
{
    Four      e;		/* error number */
    PageID    child;		/* child page */
    BtreePage *childPage;	/* pointer to buffer holding the child */
    BtreePage *rootPage;	/* pointer to buffer holding the root */
    DeallocListElem *dlElem; /* element of dealloc list */
    

    TR_PRINT(TR_BTM, TR1,
             ("btm_root_delete(handle, pFid=%P, root=%P, dlPool=%P, dlHead=%P)",
	      pFid, root, dlPool, dlHead));

    
    /*@ Get the root page */
    e = BfM_GetTrain(handle, root, (char **)&rootPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    if (rootPage->any.hdr.type & INTERNAL) {	/* Internal page */
		
	/* If the # of entries becomes zero, move the entries from the child
	   to the root and release the child. */
	if(rootPage->bi.hdr.nSlots == 0) {
	    
	    /* get the child's PageID */
	    MAKE_PAGEID(child, root->volNo, rootPage->bi.hdr.p0);

	    /*@ read the child page */
	    e = BfM_GetTrain(handle,  &child, (char **)&childPage, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    /* copy entries from the child to the root */
	    memcpy((char*)rootPage, (char*)childPage, sizeof(BtreePage));

	    rootPage->any.hdr.pid = *root;   
     
	    childPage->any.hdr.type = FREEPAGE;
	    e = BfM_SetDirty(handle, &child, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &child, PAGE_BUF);
	    
	    /*@ free the buffer for child */
	    e = BfM_FreeTrain(handle,  &child, PAGE_BUF );
	    if (e < 0) ERR(handle, e);
	   
            /* bluechry test ... */
            if (child.volNo == 1000 && child.pageNo == 165) {
                printf("### [pID=%d, tID=%d] Set FreePage: [%d, %d] at %x\n", procIndex, handle, child.volNo, child.pageNo, childPage);
		printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
                fflush(stdout);
            }
            /* ... bluechry test */

 
	    /* deallocate the child page */
	    /*
	    ** Insert a new node for the dropped file.
	    */    
	    e = Util_getElementFromPool(handle, dlPool, &dlElem);
	    if (e < 0) ERR(handle, e);

            dlElem->type = DL_PAGE;
	    dlElem->elem.pid = child; /* save the PageID. */
	    dlElem->next = dlHead->next; /* insert to the list */
	    dlHead->next = dlElem;       /* new first element of the list */
	    	    
	    rootPage->any.hdr.type |= ROOT; /* mark the page as a new root */
	    
	    e = BfM_SetDirty(handle, root, PAGE_BUF);
	    if (e < 0) ERR(handle, e);

            /* bluechry test ... */
            if (root->volNo == 1000 && root->pageNo == 165) {
                printf("### [pID=%d, tID=%d] Set Root Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, rootPage);
		printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
                fflush(stdout);
            }
            /* ... bluechry test */
	}
	
    } else if (rootPage->any.hdr.type & LEAF) {	/* Leaf Page */
		
	if (rootPage->bl.hdr.nSlots == 0) {
	    /* do nothing */
	}
    } else {
	ERRB1(handle, eBADPAGE_BTM, root, PAGE_BUF);
    }

    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_root_delete() */



/*@================================
 * btm_root_insert()
 *================================*/
/*
 * Function: Four btm_root_insert(ObjectID*, PageID*, InternalItem*)
 *
 * Description:
 *  This routine is called when a new entry was inserted into the root page and
 *  it was splitted two pages, 'root' and 'item->pid'. The new root should be
 *  made by the given root Page IDentifier and the sibling entry 'item'.
 *  We make it a rule to fix the root page; so a new page is allocated and
 *  the root node is copied into the newly allocated page. The root node
 *  is changed so that it points to the newly allocated node and the 'item->pid'.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_root_insert(
    Four handle,
    ObjectID     *catObjForFile, /* IN catalog object of B+ tree file */
    PageID       *root,		 /* IN root Page IDentifier */
    InternalItem *item)		 /* IN Internal item which will be the unique entry of the new root */
{
    Four      e;		/* error number */
    PageID    newPid;		/* newly allocated page */
    PageID    nextPid;		/* PageID of the next page of root if root is leaf */
    BtreePage *rootPage;	/* pointer to a buffer holding the root page */
    BtreePage *newPage;		/* pointer to a buffer holding the new page */
    BtreeLeaf *nextPage;	/* pointer to a buffer holding next page of root */
    btm_InternalEntry *entry;	/* an internal entry */
    Boolean   isTmp;            


    TR_PRINT(TR_BTM, TR1,
             ("btm_root_insert(handle, catObjForFile=%P, root=%P, item=%P)",
	      catObjForFile, root, item));
    
    /* Get the new root */
    e = btm_AllocPage(handle, catObjForFile, (PageID *)root, &newPid);
    if (e < 0) ERR(handle, e);

    /*@ read the root page */
    e = BfM_GetTrain(handle, root, (char **)&rootPage, PAGE_BUF); 
    if (e < 0) ERR(handle, e);

    if (rootPage->any.hdr.type & LEAF) {
	/* Change the prevPage pointer of the next page of the root. */
	
	MAKE_PAGEID(nextPid, root->volNo, rootPage->bl.hdr.nextPage);	
	e = BfM_GetTrain(handle, &nextPid, (char **)&nextPage, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	nextPage->hdr.prevPage = newPid.pageNo;

	e = BfM_SetDirty(handle, &nextPid, PAGE_BUF);
	if (e < 0) ERRB2(handle, e, root, PAGE_BUF, &newPid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
    }
    
    /*@ read the newly allocated page */
    e = BfM_GetNewTrain(handle, &newPid, (char **)&newPage, PAGE_BUF);		
    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

    /* copy the root page to the newly allocated page */
    memcpy((char*)newPage, (char*)rootPage, sizeof(BtreePage));

    newPage->bl.hdr.pid = newPid; 
    
    /*@ set dirty flag and free the buffer holding the new page */
    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &newPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* check this B-tree is temporary */
    e = btm_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0) ERR(handle, e);

    /* The old root page becomes the new root. */
    e = btm_InitInternal(handle, root, TRUE, isTmp);
    if (e < 0) ERR(handle, e);
        
    /* 'p0' points to the newly allocated page. */
    rootPage->bi.hdr.p0 = newPid.pageNo;
    
    /*@ Store the unique entry using the given 'item' */
    rootPage->bi.slot[0] = 0;
    entry = (btm_InternalEntry*)&(rootPage->bi.data[0]);
    entry->spid = item->spid;
    entry->klen = item->klen;
    memcpy(entry->kval, item->kval, entry->klen);
    
    /* There is only one entry in the new root */
    rootPage->bi.hdr.nSlots = 1;
    rootPage->bi.hdr.free = sizeof(ShortPageID) +
	ALIGNED_LENGTH(sizeof(Two)+entry->klen);
    
    e = BfM_SetDirty(handle, root, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* btm_root_insert() */
