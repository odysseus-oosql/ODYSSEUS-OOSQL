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
/*    Fine-Granule Locking Version                                            */
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
 * Module: BtM_GetStatistics.c
 *
 * Description :
 *  Get the statistics about the given index.
 *
 * Exports:
 *  Four BtM_GetStatistics(Four, PageID*, Four*)
 *
 * Notes:
 *  This is an expedient so that we support the query language; distinct count(*)
 *  There is no concurrency control; we should consider later.
 *  We count only the number of different key values; we should consider later.
 */



#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BtM_GetStatistics( )
 *================================*/
/*
 * Function: Four BtM_GetStatistics(Four, PageID*, KeyDesc*, Four*)
 *
 * Description :
 *  Get statistics about the given index.
 *
 * Returns:
 *  Error code
 */
Four BtM_GetStatistics(
    Four 		handle,
    PageID  		*root,		/* IN The root of Btree(or subtree) */
    Four    		*nKvals)	/* OUT number of different key values */
{
    Four 		e;		/* error */
    PageID 		child;		/* the child page */
    PageID 		leafPid;	/* page id of leaf pages */
    BtreePage 		*apage;		/* a page pointer */
    Buffer_ACC_CB 	*aPage_BCBP;	/* buffer access control block containing data */
    Four 		count;		/* number of different key values */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("BtM_GetStatistics(handle, root=%P, nKvals=%ld)", root, nKvals));


    /*
     * Search for the left-most leaf page.
     */
    child = *root;
    for ( ; ; ) {
	/*@ read the page into the buffer */
	e = BfM_getAndFixBuffer(handle, &child, M_FREE, &aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	apage = (BtreePage *)aPage_BCBP->bufPagePtr;

	/* If leaf page, exit the loop. */
	if (apage->any.hdr.type & LEAF) break;

	/* Get the first(left most) child of teh given root page */
	child.pageNo = apage->bi.hdr.p0;

	/*@ unfix the buffer */
	e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }


    /*
     * Count the number of different key values.
     * It is equal to number of leaf entries.
     */
    leafPid = child;

    /* initialize the counter. */
    count = 0;

    for ( ; ; ) {
	/* add the number of entries in this leaf page. */
	count += apage->bl.hdr.nSlots;

	/* Get the next page id. */
	leafPid.pageNo = apage->bl.hdr.nextPage;

	/*@ unfix the buffer */
	e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	/* exit the loop if there is no more leaf page. */
	if (leafPid.pageNo == NIL) break;

	/*@ read the next page into the buffer */
	e = BfM_getAndFixBuffer(handle, &leafPid, M_FREE, &aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	apage = (BtreePage *)aPage_BCBP->bufPagePtr;
    }

    *nKvals = count;

    return(eNOERROR);

} /* BtM_GetStatistics() */




/*@================================
 * BtM_GetStatistics_BtreePageInfo( )
 *================================*/
/*
 * Function: Four BtM_GetStatistics_BtreePageInfo(Four, PageID*, Four, Four*, sm_PageInfo*)
 *
 * Description:
 *
 * Returns:
 *  Error code
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 */
Four BtM_GetStatistics_BtreePageInfo(
    Four 	       handle,
    PageID*            root,	                /* IN the root of a Btree */
    Four               numPinfoArray,           /* IN */
    Four*              pinfoArrayIdx,           /* INOUT */
    sm_PageInfo*       pinfoArray,              /* OUT */
    LockParameter*     lockup)                  /* IN */
{
    Four               e;                       /* error number */
    Four               idx;                     /* index for the given key value */
    PageID             childPid;                /* a child PageID */
    PageID             ovPid;                   /* a overflow PageID */
    Buffer_ACC_CB*     apage_BCBP;              /* buffer access control block containing data */
    BtreePage*         apage;                   /* a pointer to the root page */
    btm_InternalEntry* iEntry;                  /* an internal entry */
    Four               iEntryOffset;            /* starting offset of an internal entry */
    btm_LeafEntry*     lEntry;                  /* an internal entry */
    Four               lEntryOffset;            /* starting offset of an internal entry */
    Four               alignedKlen;


    TR_PRINT(handle, TR_BTM, TR1,
	     ("BtM_GetStatistics_BtreePageInfo(handle, root=%P, numPinfoArray=%lD, pinfoArrayIdx=%P, pinfoArray=%P, lockup=%P)",
	      root, numPinfoArray, pinfoArrayIdx, pinfoArray, lockup));


    /* check parameter */
    if(*pinfoArrayIdx >= numPinfoArray) ERR(handle, eBADPARAMETER);


    /* get page into the buffer */
    e = BfM_getAndFixBuffer(handle, root, M_FREE, &apage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (BtreePage *)apage_BCBP->bufPagePtr;


    /* error check */
    if (apage->any.hdr.pageFlags & PAGE_TYPE_VECTOR_MASK != BTREE_PAGE_TYPE) ERR(handle, eBADPAGETYPE_BTM);


    if (apage->any.hdr.type & INTERNAL || apage->any.hdr.type & ROOT) {	/* Internal */

        /* set pinfoArray */
        pinfoArray[*pinfoArrayIdx].type = Btree_I_NodeType;
        pinfoArray[*pinfoArrayIdx].nSlots = apage->bi.hdr.nSlots;
        pinfoArray[*pinfoArrayIdx].free = apage->bi.hdr.free;
        pinfoArray[*pinfoArrayIdx].unused = apage->bi.hdr.unused;

        /* update 'pinfoArrayIdx' */
        (*pinfoArrayIdx)++;

	/* for each child page */

         /* the p0 page also must be countered */
        MAKE_PAGEID(childPid, root->volNo, apage->bi.hdr.p0);
        e = BtM_GetStatistics_BtreePageInfo(handle, &childPid, numPinfoArray, pinfoArrayIdx, pinfoArray, lockup);

	for(idx = 0; idx < apage->bi.hdr.nSlots; idx++ ) {

            iEntryOffset = apage->bi.slot[-idx];
  	    iEntry = (btm_InternalEntry*)&(apage->bi.data[iEntryOffset]);
	    MAKE_PAGEID(childPid, root->volNo, iEntry->spid);

	    /* Recursively call using the child */
	    e = BtM_GetStatistics_BtreePageInfo(handle, &childPid, numPinfoArray, pinfoArrayIdx, pinfoArray, lockup);
	    if (e < eNOERROR) ERRB1(handle, e, apage_BCBP, PAGE_BUF);
        }

    } else if( apage->any.hdr.type & LEAF ) {

        /* set pinfoArray */
        pinfoArray[*pinfoArrayIdx].type = Btree_L_NodeType;
        pinfoArray[*pinfoArrayIdx].nSlots = apage->bl.hdr.nSlots;
        pinfoArray[*pinfoArrayIdx].free = apage->bl.hdr.free;
        pinfoArray[*pinfoArrayIdx].unused = apage->bl.hdr.unused;

        /* update 'pinfoArrayIdx' */
        (*pinfoArrayIdx)++;

	/* for each data entry */
	for(idx = 0; idx < apage->bl.hdr.nSlots; idx++ ) {

            lEntryOffset = apage->bl.slot[-idx];
  	    lEntry = (btm_LeafEntry*)&(apage->bl.data[lEntryOffset]);
            alignedKlen = ALIGNED_LENGTH(lEntry->klen);

            if (lEntry->nObjects < 0) {      /* overflow page */

                MAKE_PAGEID(ovPid, root->volNo, *((ShortPageID*)&lEntry->kval[alignedKlen]));

 	        /* Recursively call using the child */
	        e = BtM_GetStatistics_BtreePageInfo(handle, &ovPid, numPinfoArray, pinfoArrayIdx, pinfoArray, lockup);
	        if (e < eNOERROR) ERRB1(handle, e, apage_BCBP, PAGE_BUF);
            }
        }

    } else if( apage->any.hdr.type & OVERFLOW ) {

        /* set pinfoArray */
        pinfoArray[*pinfoArrayIdx].type = Btree_O_NodeType;
        pinfoArray[*pinfoArrayIdx].nSlots = apage->bo.hdr.nObjects;
        pinfoArray[*pinfoArrayIdx].free = apage->bo.hdr.nObjects * OBJECTID_SIZE;
        pinfoArray[*pinfoArrayIdx].unused = 0;

        /* update 'pinfoArrayIdx' */
        (*pinfoArrayIdx)++;

        /* if there is more overflow pages */
        if(apage->bo.hdr.nextPage != NIL) {

            MAKE_PAGEID(ovPid, root->volNo, apage->bo.hdr.nextPage);

            /* Recursively call using the child */
	    e = BtM_GetStatistics_BtreePageInfo(handle, &ovPid, numPinfoArray, pinfoArrayIdx, pinfoArray, lockup);
	    if (e < eNOERROR) ERRB1(handle, e, apage_BCBP, PAGE_BUF);
        }

    } else
	ERRB1(handle, eBADBTREEPAGE_BTM, apage_BCBP, PAGE_BUF);

    /* free buffer */
    e = BfM_unfixBuffer(handle, apage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

}   /* BtM_GetStatistics_BtreePageInfo() */

