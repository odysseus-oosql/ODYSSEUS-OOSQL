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
 * Module: btm_FreePages.c
 *
 * Description :
 *  Free all pages which were related with the given page. 
 *
 * Exports:
 *  Four btm_FreePages(FileID*, PageID*, Pool*, DeallocListElem*)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_FreePages()
 *================================*/
/*
 * Function: Four btm_FreePages(FileID*, PageID*, Pool*, DeallocListElem*)
 *
 * Description :
 *  Free all pages which were related with the given page. If the given page
 *  is an internal page, recursively free all child pages before it is freed.
 *  In a leaf page, examine all leaf items whether it has an overflow page list
 *  before it is freed. If it has, recursively call itself by using the first
 *  overflow page. In an overflow page, it recursively calls itself if the
 *  'nextPage' exist.
 *
 * Returns:
 *  error code
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 */
Four btm_FreePages(
    Four handle,
    PhysicalFileID      *pFid,          /* IN FileID of the Btree file */
    PageID              *curPid,        /* IN The PageID to be freed */
    Pool                *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)        /* INOUT head of the dealloc list */
{
    Four                e;              /* error number */
    Two                 i;              /* index */
    Two                 alignedKlen;    /* aligned length of the key length */
    PageID              tPid;           /* a temporary PageID */
    PageID              ovPid;          /* a temporary PageID of an overflow page */
    BtreePage           *apage;         /* a page pointer */
    BtreeOverflow       *opage;         /* page pointer to a buffer holding an overflow page */
    Two                 iEntryOffset;   /* starting offset of an internal entry */
    Two                 lEntryOffset;   /* starting offset of a leaf entry */
    btm_InternalEntry   *iEntry;        /* an internal entry */
    btm_LeafEntry       *lEntry;        /* a leaf entry */
    DeallocListElem     *dlElem;        /* an element of dealloc list */


    TR_PRINT(TR_BTM, TR1,
             ("btm_FreePages(handle, pFid=%P, curPid=%P, dlPool=%P, dlHead=%P)",
	      pFid, curPid, dlPool, dlHead));
    
    
    e = BfM_GetTrain(handle, curPid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    if (apage->any.hdr.type & INTERNAL) {	/* Internal Page */
	
	/*@ Recursively call itself for all children of this page */

        MAKE_PAGEID(tPid, curPid->volNo, apage->bi.hdr.p0);
        e = btm_FreePages(handle, pFid, &tPid, dlPool, dlHead);
        if (e < 0) ERRB1(handle, e, curPid, PAGE_BUF);

	for (i = 0; i < apage->bi.hdr.nSlots; i++) {
	    iEntryOffset = apage->bi.slot[-i];
	    iEntry = (btm_InternalEntry*)&(apage->bi.data[iEntryOffset]);

	    MAKE_PAGEID(tPid, curPid->volNo, iEntry->spid);
	    e = btm_FreePages(handle, pFid, &tPid, dlPool, dlHead);
	    if (e < 0) ERRB1(handle, e, curPid, PAGE_BUF);
	}
	
    } else if (apage->any.hdr.type & LEAF)  {	/* Leaf Page */
	
	/* For all leaf items, examine whether it has an overflow page */
	for (i = 0; i < apage->bl.hdr.nSlots; i++) {
	    lEntryOffset = apage->bl.slot[-i];
	    lEntry = (btm_LeafEntry*)&(apage->bl.data[lEntryOffset]);

	    /*@ recursive call */
	    /* If the item has an overflow page, recursively call itself */
	    if(lEntry->nObjects < 0)  {
		alignedKlen = ALIGNED_LENGTH(lEntry->klen);
		MAKE_PAGEID(ovPid, curPid->volNo,
			    *((ShortPageID*)&(lEntry->kval[alignedKlen])));

		do {
		    e = BfM_GetTrain(handle, &ovPid, (char**)&opage, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, curPid, PAGE_BUF);

		    MAKE_PAGEID(tPid, curPid->volNo, opage->hdr.nextPage);

		    opage->hdr.type = FREEPAGE;

		    e = BfM_SetDirty(handle, &ovPid, PAGE_BUF);
		    if (e < 0) ERRB2(handle, e, &ovPid, PAGE_BUF, curPid, PAGE_BUF);

		    e = BfM_FreeTrain(handle, &ovPid, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, curPid, PAGE_BUF);

                    /* bluechry test ... */
                    if (ovPid.volNo == 1000 && ovPid.pageNo == 165) {
                        printf("### [pID=%d, tID=%d] Set FreePage: [%d, %d] at %x\n", procIndex, handle, ovPid.volNo, ovPid.pageNo, opage);
			printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
                        fflush(stdout);
                    }
                    /* ... bluechry test */

		    
		    /*
		     * Insert the deallocated page into the dealloc list.
		     */
		    e = Util_getElementFromPool(handle, dlPool, &dlElem);
		    if (e < 0) ERR(handle, e);

                    dlElem->type = DL_PAGE;
		    dlElem->elem.pid = ovPid; /* save the page identifier */
		    dlElem->next = dlHead->next; /* insert into the list */
		    dlHead->next = dlElem;	   /* new first element of the list */

		    ovPid = tPid;
		} while (ovPid.pageNo != NIL);		
	    } 
	}
	
    } else
	ERRB1(handle, eBADBTREEPAGE_BTM, curPid, PAGE_BUF);

    apage->any.hdr.type = FREEPAGE;
    e = BfM_SetDirty(handle, curPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, curPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, curPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* bluechry test ... */
    if (curPid->volNo == 1000 && curPid->pageNo == 165) {
        printf("### [pID=%d, tID=%d] Set FreePage: [%d, %d] at %x\n", procIndex, handle, curPid->volNo, curPid->pageNo, apage);
	printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__); 
        fflush(stdout);
    }
    /* ... bluechry test */

    /*
     * Insert the deallocated page into the dealloc list.
     */
    e = Util_getElementFromPool(handle, dlPool, &dlElem);
    if (e < 0) ERR(handle, e);

    dlElem->type = DL_PAGE;
    dlElem->elem.pid = *curPid;	/* save the page identifier */
    dlElem->next = dlHead->next; /* insert into the list */
    dlHead->next = dlElem;	   /* new first element of the list */
    
    return(eNOERROR);
    
}   /* btm_FreePages() */
