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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: MLGF_DropIndex.c
 *
 * Description:
 *  Drop the given MLGF index.
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* Internal Function Prototype */
Four mlgf_FreePages(Four, PhysicalFileID*, PageID*, Pool*, DeallocListElem*);


/*
 * Funciton: Four MLGF_DropIndex(PhysicalFileID*, PageID*, Pool*, DeallocListElem*)
 *
 * Description:
 *  Drop the given MLGF index. It uses the auxiary fucntion mlgf_FreePages().
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 */
Four MLGF_DropIndex(
    Four handle,
    PhysicalFileID *pFid,	/* IN FileID of the index file */ 
    PageID *rootPid,		/* IN root page of the dropped MLGF index */ 
    Pool   *dlPool,		/* INOUT pool of the dealloc list elements */
    DeallocListElem *dlHead) /* INOUT head of the dealloc list */
{
    Four e;			/* error code */


    TR_PRINT(TR_MLGF, TR1, ("MLGF_DropIndex(handle)"));


    /* check parameters */
    if (rootPid == NULL) ERR(handle, eBADPARAMETER); 


    /* Free all pages concerned with the index. */
    /* IndexID is the root page id of the index. */
    e = mlgf_FreePages(handle, pFid, rootPid, dlPool, dlHead);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* MLGF_DropIndex() */



/*
 * Function: Four mlgf_FreePages(PageID*, MLGF_KeyDesc*, LocalPool*, DeallocListElem*)
 *
 * Description:
 *  Deallocate the pages of a subtree rooted at the given page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_FreePages(
    Four handle,
    PhysicalFileID      *pFid,          /* IN FileID of the index file */
    PageID              *root,          /* IN root page of a subtree */
    Pool                *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)        /* INOUT head of the dealloc list */
{
    Four                e;              /* error code */
    Two                 i;              /* temporary variable */
    Two                 entryLen;       /* length of a directory entry */
    PageID              tPid;           /* a temporary PageID */
    PageID              child;          /* PageID of the child */
    PageID              ovPid;          /* Overflow Page's ID */
    mlgf_Page           *apage;         /* a page of the MLGF index */
    mlgf_OverflowPage   *opage;         /* page pointer to a buffer holding an overflow page */
    mlgf_LeafEntry      *leafEntry;     /* a leaf entry */
    mlgf_DirectoryEntry *dirEntry;      /* a directory entry */
    DeallocListElem     *dlElem;        /* an element of dealloc list */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_FreePages(handle)"));


    e = BfM_GetTrain(handle, root, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (apage->any.hdr.type & MLGF_DIRECTORYPAGE) {

	/* calculate the directory entry size */
	entryLen = MLGF_DIRENTRY_LENGTH(apage->directory.hdr.nKeys);

	/* for all entries */
	dirEntry = MLGF_ITH_DIRENTRY(&apage->directory, 0, entryLen);
	for (i = 0; i < apage->directory.hdr.nEntries; i++) {
	    MAKE_PAGEID(child, root->volNo, dirEntry->spid);

	    /* Call the mlgf_FreePage() recursively. */
	    e = mlgf_FreePages(handle, pFid, &child, dlPool, dlHead);
	    if (e < 0) {
		ERRB1(handle, e, root, PAGE_BUF);
	    }

	    dirEntry = MLGF_NEXT_DIRENTRY(dirEntry, entryLen);
	}

    } else {			/* MLGF_LEAFPAGE */

	/* for all entries */
	for (i = 0; i < apage->leaf.hdr.nEntries; i++) {

	    leafEntry = (mlgf_LeafEntry*)&apage->leaf.data[apage->leaf.slot[-i]];

	    if (leafEntry->nObjects < 0) { /* overflow page chain */

		MAKE_PAGEID(ovPid, root->volNo, MLGF_LEAFENTRY_FIRST_OVERFLOW(apage->leaf.hdr.nKeys, leafEntry));

		do {
		    e = BfM_GetTrain(handle, &ovPid, (char**)&opage, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

		    MAKE_PAGEID(tPid, root->volNo, opage->hdr.nextPage);

		    opage->hdr.type = MLGF_FREEPAGE;

		    e = BfM_SetDirty(handle, &ovPid, PAGE_BUF);
		    if (e < 0) ERRB2(handle, e, &ovPid, PAGE_BUF, root, PAGE_BUF);

		    e = BfM_FreeTrain(handle, &ovPid, PAGE_BUF);
		    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
		    
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
    }

    apage->any.hdr.type = MLGF_FREEPAGE;
    
    e = BfM_SetDirty(handle, root, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*
     * Insert the deallocated page into the dealloc list.
     */
    e = Util_getElementFromPool(handle, dlPool, &dlElem);
    if (e < 0) ERR(handle, e);

    dlElem->type = DL_PAGE;
    dlElem->elem.pid = *root;	/* save the page identifier */
    dlElem->next = dlHead->next; /* insert into the list */
    dlHead->next = dlElem;	   /* new first element of the list */

    return(eNOERROR);

} /* mlgf_FreePages() */
