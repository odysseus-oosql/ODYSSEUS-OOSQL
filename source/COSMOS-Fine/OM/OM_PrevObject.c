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
 * Module: OM_PrevObject.c
 *
 * Description:
 *  Return the previous object of the given current object. Find the object in
 *  the same page which has the current object and  if there  is no previous
 *  object in the same page, find it from the previous page.
 *  If the current object is NULL, return the last object of the file.
 *
 * Assume :
 *  This function is used for sequential sacn.
 *  When the sequential scan is opened, the file must be locked.
 *  So, we don't need to lock the page.
 *
 * Exports:
 *  Four OM_PrevObject(Four, DataFileInfo*, ObjectID*, ObjectID*, ObjectHdr*, LockParameter*)
 *
 * Return value:
 *  Error codes
 *    eBADCATOBJ_OM
 *    eBADOBJECTID_OM
 *    eBADFILEID_OM
 *    eNOPREVOBJECT_OM
 *    some errors caused by function calls
 *
 * Side effect:
 *  1) parameter prevOID
 *     prevOID is filled with the previous object's identifier
 *  2) parameter objHdr
 *     objHdr is filled with the previous object's header
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BfM.h"
#include "OM.h"
#include "LM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four OM_PrevObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    ObjectID *curOID,		/* IN a ObjectID of the current object */
    ObjectID *prevOID,		/* OUT the previous object of a current object */
    ObjectHdr*objHdr,		/* OUT the object header of previous object */
    LockParameter *lockup)	/* IN request lock or not */ /* in case CCPL, lockup parameter is reserved */
{
    Four e;			/* error */
    Four i;			/* index */
    Four offset;		/* starting offset of object within a page */
    PageID pid;			/* a page identifier */
    PageNo pageNo;		/* a temporary var for previous page's PageNo */
    SlottedPage *apage;		/* a pointer to the data page */
    Buffer_ACC_CB *aPage_BCBP;  /* buffer access control block holding data */
    Object *obj;		/* a pointer to the Object */
    SlottedPage *catPage;  	/* pointer to buffer containing the catalog */
    sm_CatOverlayForData *catEntry;/* pointer to data file catalog information */
    Buffer_ACC_CB *catPage_BCBP;  /* buffer access control block holding catalog data */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;


    TR_PRINT(handle, TR_OM, TR1,
	     ("OM_PrevObject(handle, finfo=%P, curOID=%P, prevOID=%P)",
	      finfo, curOID, prevOID));


    /* parameter checking */
    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (prevOID == NULL) ERR(handle, eBADOBJECTID_OM);


    /* Get the previous object. */
    if (curOID == NULL) {
	/*
	 * Get the last page's page No of the data file.
	 */

	/* use the last page */
	if (finfo->tmpFileFlag) {
	    /* temporary file */
	    MAKE_PAGEID(pid, finfo->fid.volNo, finfo->catalog.entry->data.lastPage);
	} else {
	    /* ordinary file; get the last page from the catalog table */

#ifdef CCPL
	    /* Because the current transaction has the lock on this file */
	    /* we don't have to check that the last page is correct. */
	    /* acquire lock on the catalog entry for accessing last page */
	    e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid,
				   L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode); 
	    if (e < eNOERROR) ERR(handle, e);

	    if (lockReply == LR_DEADLOCK) {
		ERR(handle, eDEADLOCK); /* deadlock */
	    }
#endif /* CCPL */

#ifdef CCPL
	    e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_FREE, &catPage_BCBP, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	    e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_SHARED, &catPage_BCBP, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	    catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
	    GET_PTR_TO_CATENTRY_FOR_DATA(finfo->catalog.oid.slotNo, catPage, catEntry);

	    /* get the last page */
	    MAKE_PAGEID(pid, finfo->fid.volNo, catEntry->lastPage); 

#ifdef CCRL
            e = SHM_releaseLatch(handle, catPage_BCBP->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	    e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
	    if(e < eNOERROR) ERR(handle, e);

#ifdef CCPL
	    /* Release the lock on the catalog page. */
	    e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid, L_MANUAL);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */
	}

    } else {			/* curOID != NULL */

	/* At first, look at the curOID page.
	 * If no previous Object, see the previous page.
	 */
        /* Read the page into the buffer page */

#ifdef CCPL
        e = BfM_getAndFixBuffer(handle, (PageID *)curOID, M_FREE, &aPage_BCBP, PAGE_BUF);
        if( e < 0 )  ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
        e = BfM_getAndFixBuffer(handle, (PageID *)curOID, M_SHARED, &aPage_BCBP, PAGE_BUF);
        if( e < 0 )  ERR(handle, e);
#endif /* CCRL */

	apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

	/* Find the Object within the page which has the current Object */
	for( i = curOID->slotNo - 1; i >= 0; i-- ) {
	    offset = apage->slot[-i].offset;

	    if( offset != EMPTYSLOT )  {  /* if a Object is found, return it */
		obj = (Object *)&(apage->data[offset]);

		if(!(obj->header.properties & P_FORWARDED)) {
		    MAKE_OBJECTID(*prevOID, curOID->volNo, curOID->pageNo,
				  i, apage->slot[-i].unique);

		    /* if objHdr is not NULL, return the object header */
		    if (objHdr != NULL) *objHdr = obj->header;

#ifdef CCRL
		    /* Request lock before release buffer latch */
		    if (lockup) {
			e = LM_getObjectLock(handle, &xactEntry->xactId, prevOID,
					     &finfo->fid, lockup->mode,
					     lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
			if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			if ( lockReply == LR_NOTOK ) {
			    /* release latch to avoid deadlock situation */
			    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
			    if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			    e = LM_getObjectLock(handle, &xactEntry->xactId, prevOID, &finfo->fid,
						 lockup->mode, lockup->duration,
						 L_UNCONDITIONAL, &lockReply, &oldMode);
			    if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			    if ( lockReply == LR_DEADLOCK ) ERRBL1(handle, eDEADLOCK, aPage_BCBP, PAGE_BUF);

			    /* relatch the page */
			    e = SHM_getLatch(handle, aPage_BCBP->latchPtr, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
			    if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			    /* re-validation the objectID */
			    if(!IS_VALID_OBJECTID(prevOID, apage)) ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
			}
		    }
#endif /* CCRL */

#ifdef CCRL
                    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex); 
                    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

		    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
		    if(e < 0)  ERR(handle, e);

		    return( eNOERROR );
		}
	    }
	}

	/* If there is no previous Object within the page,
	 * find it from the next pages
	 */
	MAKE_PAGEID(pid, curOID->volNo, apage->header.prevPage);

#ifdef CCRL
        e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
	if( e < 0 )  ERR(handle, e);
    }

    while (pid.pageNo != NIL) {
#ifdef CCPL
	e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
	if( e < 0 )  ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	e = BfM_getAndFixBuffer(handle, &pid, M_SHARED, &aPage_BCBP, PAGE_BUF);
	if( e < 0 )  ERR(handle, e);
#endif /* CCRL */

	apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

	for( i = apage->header.nSlots-1; i >= 0; i-- )  {
	    offset = apage->slot[-i].offset;

	    if( offset != EMPTYSLOT )  {  /* if a Object is found, return it */
		obj = (Object *)&(apage->data[offset]);

		if(!(obj->header.properties & P_FORWARDED)) {
		    MAKE_OBJECTID(*prevOID, pid.volNo, pid.pageNo,
				  i, apage->slot[-i].unique);

		    /* if objHdr is not NULL, return the object header */
		    if (objHdr != NULL) *objHdr = obj->header;

#ifdef CCRL
		    /* Request lock before release buffer latch */
		    if (lockup) {
			e = LM_getObjectLock(handle, &xactEntry->xactId, prevOID,
					     &finfo->fid, lockup->mode,
					     lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
			if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			if ( lockReply == LR_NOTOK ) {
			    /* release latch to avoid deadlock situation */
			    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
			    if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			    e = LM_getObjectLock(handle, &xactEntry->xactId, prevOID, &finfo->fid,
						 lockup->mode, lockup->duration,
						 L_UNCONDITIONAL, &lockReply, &oldMode);
			    if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			    if ( lockReply == LR_DEADLOCK ) ERRBL1(handle, eDEADLOCK, aPage_BCBP, PAGE_BUF);

			    /* relatch the page */
			    e = SHM_getLatch(handle, aPage_BCBP->latchPtr, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
			    if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

			    /* re-validation the objectID */
			    if(!IS_VALID_OBJECTID(prevOID, apage)) ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);

			}	/* if lockReply == NOTOK */
		    }		/* if lockup  */
#endif /* CCRL */

#ifdef CCRL
                    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
                    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

		    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
		    if (e < eNOERROR) ERR(handle,  e );


		    return( eNOERROR );
		}
	    }
	}

	/* save the nextPage's PageNo & free the current page */
	pageNo = apage->header.prevPage;

#ifdef CCRL
        e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle,  e );

	MAKE_PAGEID(pid, pid.volNo, pageNo);
    }

    return( EOS );

} /* OM_PrevObject() */
