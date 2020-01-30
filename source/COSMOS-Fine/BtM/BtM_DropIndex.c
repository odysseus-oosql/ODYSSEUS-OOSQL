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
 * Module:	BtM_DropIndex.c
 *
 * Description :
 *  Drop the B+ tree Index specified by 'rootPid', a root PageID of the B+tree.
 *  If an deallocated list is given by the caller, then the index pages are
 *  added into the list and the deallocation of the pages are deffered; the
 *  caller is responsible for deallcating the pages.
 *  If the dallcated list is not given, then the index pages are deallocated
 *  immediately.
 *
 * Exports:
 *  Four BtM_DropIndex(Four, XactTableEntry_T*, PhysicalIndexID*, SegmentID*, Boolean, LogParameter_T*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "RDsM.h"
#include "Util.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*@ Internal Function Prototypes */
Four btm_FreePages(Four, XactTableEntry_T*, PageID*, Boolean, LogParameter_T*);



/*@================================
 * BtM_DropIndex( )
 *================================*/
/*
 * Function: Four BtM_DropIndex(Four, XactTableEntry_T*, PhysicalIndexID*, SegmentID*, Boolean, LogParameter_T*)
 *
 * Description:
 *  Refer to the module description.
 *
 * Returns:
 *  Error code
 *    some errors : by other function calls
 */
Four BtM_DropIndex(
    Four                        handle,         /* IN handle */
    XactTableEntry_T 		*xactEntry, 	/* IN transaction table entry */
    IndexID			*iid, 		/* IN logical index id */ 
    PhysicalIndexID 		*pIid,      	/* IN B tree index ID */ 
    SegmentID_T			*pageSegmentID,	/* IN page segment ID concerned with the index */
    Boolean 			immediateFlag,  /* IN TRUE if drop immediately */
    LogParameter_T 		*logParam)   	/* IN log parameter */
{
    Four 			e;		/* for the error number */


    TR_PRINT(handle, TR_BTM, TR1, ("BtM_DropIndex(xactEntry=%P, pIid=%P, pageSegmentID=%P, immediateFlag=%ld, logParam=%P)",
	     xactEntry, pIid, pageSegmentID, immediateFlag, logParam));


    /* check parameters */
    if (pIid == NULL) ERR(handle, eBADPARAMETER); 

    e = btm_IdMapping_DeleteEntry(handle, iid);
    if (e < eNOERROR) ERR(handle, e);

    if (immediateFlag == TRUE) {
        /* Drop the page segment concerned with the index. */
        e = RDsM_DropSegment(handle, xactEntry, pIid->volNo, pageSegmentID, pageSegmentID->sizeOfTrain, immediateFlag, logParam);
        if (e < eNOERROR) ERR(handle, e);
    }
    else {
	/* Add the segment identification to dealloc list */
	e = TM_XT_AddToDeallocList(handle, xactEntry, pIid, pageSegmentID, NULL, DL_BTREEINDEX);
	if (e < eNOERROR) ERR(handle, e);
    }


    return(eNOERROR);

} /* BtM_DropIndex() */



/*@================================
 * btm_FreePages()
 *================================*/
/*
 * Function: Four btm_FreePages(PageID*, Boolean)
 *
 * Description :
 *  Free all pages which were related with the given page. If the given page
 *  is an internal page, recursively free all child pages before it is freed.
 *  In a leaf page, examine all leaf items whether it has an overflow page list
 *  before it is freed. If it has, recursively call itself by using the first
 *  overflow page. In an overflow page, it recursively calls itself if the
 *  'nextPage' exist.
 *
 * Assumption
 *  This function is used to drop an index.
 *  We assume that the file has been locked by x-mode, and this function has
 *  no locking mechanism.
 *
 * Returns:
 *  Error code
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 */
Four btm_FreePages(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    PageID *curPid,		/* IN The PageID to be freed */
    Boolean immediateFlag,      /* IN TRUE if drop immediately */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four i;			/* index */
    Four alignedKlen;		/* aligned length of the key length */
    PageID tPid;		/* a temporary PageID */
    PageID ovPid;		/* a temporary PageID of an overflow page */
    BtreePage *apage;		/* a page pointer */
    Buffer_ACC_CB *aPage_BCBP;
    BtreeOverflow *opage;	/* page pointer to a buffer holding an overflow page */
    Buffer_ACC_CB *oPage_BCBP;
    Four iEntryOffset;		/* starting offset of an internal entry */
    Four lEntryOffset;		/* starting offset of a leaf entry */
    btm_InternalEntry *iEntry;	/* an internal entry */
    btm_LeafEntry *lEntry;	/* a leaf entry */


    TR_PRINT(handle, TR_BTM, TR1, ("btm_FreePages(curPid=%P,immediateFlag=%ld)", curPid, immediateFlag));


    /*@ read the page */
    e = BfM_getAndFixBuffer(handle, curPid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (BtreePage *)aPage_BCBP->bufPagePtr;

    if (apage->any.hdr.type & INTERNAL) {	/* Internal Page */

	/*@ Recursively call itself for all children of this page */

        MAKE_PAGEID(tPid, curPid->volNo, apage->bi.hdr.p0);
        e = btm_FreePages(handle, xactEntry, &tPid, immediateFlag, logParam);

	for (i = 0; i < apage->bi.hdr.nSlots; i++) {
	    iEntryOffset = apage->bi.slot[-i];
	    iEntry = (btm_InternalEntry*)&(apage->bi.data[iEntryOffset]);

	    MAKE_PAGEID(tPid, curPid->volNo, iEntry->spid);
	    e = btm_FreePages(handle, xactEntry, &tPid, immediateFlag, logParam);
	    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
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
		    e = BfM_getAndFixBuffer(handle, &ovPid, M_FREE, &oPage_BCBP, PAGE_BUF);
		    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

		    opage = (BtreeOverflow *)oPage_BCBP->bufPagePtr;

		    MAKE_PAGEID(tPid, curPid->volNo, opage->hdr.nextPage);

		    e = BfM_unfixBuffer(handle, oPage_BCBP, PAGE_BUF);
		    if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

                    e = RDsM_FreeTrain(handle, xactEntry, &ovPid, PAGESIZE2, immediateFlag, logParam);
                    if (e < eNOERROR) ERR(handle, e);

		    ovPid = tPid;
		} while (ovPid.pageNo != NIL);
	    }
	}

    } else
	ERRB1(handle, eBADBTREEPAGE_BTM, aPage_BCBP, PAGE_BUF);

    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_FreeTrain(handle, xactEntry, curPid, PAGESIZE2, immediateFlag, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

}   /* btm_FreePages() */

