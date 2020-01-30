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
 * Module : om_DeletePageFromFile.c
 *
 * Description :
 *  Delete the given page from the double linked list of data pages.
 *
 * Assume:
 *  The lock on the delPage has been gotten.
 *
 * Exports:
 *  Four om_DeletePageFromFile(Four, DataFileInfo*, SlottedPage*, LockParameter*)
 *
 * Return Value:
 *  Error code
 *    eBADCATOBJ_OM
 *    eBADPAGEID_OM
 *    eDEADLOCK_OM
 *    some errors caused by function calls
 *
 * Side effect:
 *  If the given page is the first page, then ignore and return immediately.
 *  If the given page is the last page, catalog entry's lastPage field is
 *  updated to new last page.
 */

/* This file is the modified version of om_FileMapAddPage.c */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LOG.h"
#include "BfM.h"
#include "OM.h"
#include "LM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four om_DeletePageFromFile(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    SlottedPage  *delPage,      /* INOUT page to be deleted */
    LockParameter *lockup,      /* IN request lock or not */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error */
    PageID prevPid;		/* PageID of previous(relative to new page) page */
    PageID nextPid;		/* PageID of next(relative to deleted page) page */
    SlottedPage *prevPage;	/* point to a previous(relative to deleted page) page */
    SlottedPage *nextPage;	/* point to a next(relative to the deleted page) page */
    SlottedPage *catPage;       /* pointer to the catalog page */
    sm_CatOverlayForData *catEntry; /* pointer to data file catalog information */
    Buffer_ACC_CB *prevPage_BCBP;/* pointer to the buffer access control block holding previous page */
    Buffer_ACC_CB *nextPage_BCBP;/* pointer to the buffer access control block holding next page */
    Buffer_ACC_CB *catPage_BCBP;/* pointer to the buffer access control block holding catalog */
    LockReply lockReply;        /* lock reply */
    LockMode oldMode;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    PhysicalFileID  pFid;	/* physical file ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1,
	     ("om_DeletePageFromFile(handle, finfo=%P, delPage=%P, lockup=%P)",
	      finfo, delPage, lockup));


    /* check parameters */

    if (delPage == NULL) ERR(handle, eBADPAGEID);

    /* get physical file ID */
    e = om_GetPhysicalFileID(handle, xactEntry, finfo, &pFid, lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* if the first page, ignore this and return successfully */
    if (EQUAL_PAGEID(pFid, delPage->header.pid))
	return(eNOERROR);

    /* get the pageID of the previous page */
    MAKE_PAGEID(prevPid, finfo->fid.volNo, delPage->header.prevPage);

    /* get the correct previous, next pages */
    if (lockup){
	/* get lock on the prevPage */
	e = LM_getPageLock(handle, &xactEntry->xactId, &prevPid, &finfo->fid,
			   lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){ /* deadlock */
	    ERR(handle, eDEADLOCK);
	}
    }

    /* get the previous page */
    e = BfM_getAndFixBuffer(handle, &prevPid, M_FREE, &prevPage_BCBP,  PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    prevPage = (SlottedPage *)prevPage_BCBP->bufPagePtr;


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_MODIFY_PAGE_LIST_NEXT_LINK, LOG_REDO_UNDO,
                              prevPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(ShortPageID), &(delPage->header.nextPage),
                              sizeof(ShortPageID), &(prevPage->header.nextPage));

	e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	if (e < eNOERROR) ERRB1(handle, e, prevPage_BCBP, PAGE_BUF);

	/* mark the lsn in the page */
	prevPage->header.lsn = lsn;
	prevPage->header.logRecLen = logRecLen;
    }

    /* get the next page id */
    MAKE_PAGEID(nextPid, finfo->fid.volNo, delPage->header.nextPage);

    /* Change the previous page */
    prevPage->header.nextPage = nextPid.pageNo;

    prevPage_BCBP->dirtyFlag = 1;

    /* unfix the previous page */
    e = BfM_unfixBuffer(handle, prevPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** Update the next page or catalog
    */
    if(nextPid.pageNo == NIL){	/* last page */
	/*
	** Update the last page of the file.
	*/

	if (finfo->tmpFileFlag) { /* temporary file */
	    /* Update the catalog entry in the memory. */
	    /* update the last page */
	    finfo->catalog.entry->data.lastPage = prevPid.pageNo;

	} else {		/* ordinary file */

	    /* Update the catalog entry in the disk. */
	    /*
	    ** Request X lock on the page.
	    ** We use the flat page lock because 1) we don't know the file id of
	    ** the catalog table and 2) the check of file lock is an overhead.
	    */
            if (lockup != NULL) {
                e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid,
                                       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
                if (e < eNOERROR) ERR(handle, e);

                if (lockReply == LR_DEADLOCK) {
                    ERR(handle, eDEADLOCK); /* deadlock */
                }
            }

	    /* update the last page in catalog */
	    e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_FREE, &catPage_BCBP, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
	    GET_PTR_TO_CATENTRY_FOR_DATA(finfo->catalog.oid.slotNo, catPage, catEntry);

	    /*
	     * Write log record.
	     */
	    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_OM_MODIFY_LAST_PAGE_IN_CATALOG_ENTRY, LOG_REDO_UNDO,
                                      catPage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(finfo->catalog.oid.slotNo), &(finfo->catalog.oid.slotNo),
                                      sizeof(ShortPageID), &(prevPid.pageNo),
                                      sizeof(ShortPageID), &(catEntry->lastPage));

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
		if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

		/* mark the lsn in the page */
		catPage->header.lsn = lsn;
		catPage->header.logRecLen = logRecLen;
	    }

	    /* Assert that the deleted page is the last page; it was locked */
	    /* in the caller and it shouldn't be changed. */
	    /* So we don't have to check the lastPage value in catalog. */
	    catEntry->lastPage = prevPid.pageNo;

	    catPage_BCBP->dirtyFlag = 1;

	    e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
	    if(e < eNOERROR) ERR(handle, e);

	    /* Release the lock on the catalog page. */
            if (lockup != NULL) {
                e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid, L_MANUAL);
                if (e < eNOERROR) ERR(handle, e);
            }
	}

    } else {			/* no last page */

	if (lockup) {
	    /* get lock on the nextPage */
	    e = LM_getPageLock(handle, &xactEntry->xactId, &nextPid,
			       &finfo->fid, lockup->mode,
			       lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){ /* deadlock */
		ERR(handle, eDEADLOCK);
	    }
	}

	/* get the next page */
	e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &nextPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	nextPage = (SlottedPage *)nextPage_BCBP->bufPagePtr;

	/*
	 * Write log record.
	 */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	    LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_MODIFY_PAGE_LIST_PREV_LINK, LOG_REDO_UNDO,
                                  nextPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &(prevPid.pageNo),
                                  sizeof(ShortPageID), &(nextPage->header.prevPage));

	    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	    if (e < eNOERROR) ERRB1(handle, e, nextPage_BCBP, PAGE_BUF);

	    /* mark the lsn in the page */
	    nextPage->header.lsn = lsn;
	    nextPage->header.logRecLen = logRecLen;
	}

	nextPage->header.prevPage = prevPid.pageNo;

	nextPage_BCBP->dirtyFlag = 1;

	/* unfix the next page */
	e = BfM_unfixBuffer(handle, nextPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* om_DeletePageFromFile() */
