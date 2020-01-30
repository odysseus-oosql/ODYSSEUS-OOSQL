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
 * Module : om_AddPageToFile.c
 *
 * Description :
 *  Add the new page into the double linked list of data pages.
 *  Insert the new page after the prevPid page, which is the previous page.
 *  The previous page and new page  must be locked or latched by caller.
 *  So, now other transaction can't append a page on the previous page,
 *  even though the previous page is the last page.
 *
 * Assume :
 *  We have the lock on the previous page and the new page.
 *  We will not release the new page's buffer latch.
 *  If (lockup == NULL) the prevPage is not locked.
 *  else the prevPage is locked.
 *
 * Exports:
 *  Four om_AddPageToFile(Four, DataFileInfo*, PageID*, SlottedPage*, LockParameter*)
 *
 * Return value:
 *  Error code
 *    eBADCATOBJ_OM
 *    eBADPAGEID_OM
 *    eLOCKREQUESTFAIL_OM
 *    some errors caused by function calls
 *
 * Side effect:
 *  If newPID is appended after the last page,
 *  catalog entry's lastPage field is updated with the new page.
 */

/* This file is the modified version of om_FileMapAddPage.c */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LOG.h"
#include "LM.h"
#include "BfM.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four om_AddPageToFile(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    PageID   *prevPid,		/* IN after which newPID is added */
    SlottedPage *newPage,       /* INOUT new page to be added */
    LockParameter *lockup,      /* IN request lock or not */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four   e;		   	/* error */
    PageID nextPid;	   	/* PageID of next(relative to the new page) page */
    SlottedPage *prevPage; 	/* pointer to a previous(relative to new page) page */
    SlottedPage *nextPage; 	/* pointer to a next(relative to new page) page */
    SlottedPage *catPage;  	/* pointer to buffer containing the catalog */
    sm_CatOverlayForData *catEntry;/* pointer to data file catalog information */
    Buffer_ACC_CB *catPage_BCBP;  /* buffer access control block holding catalog data */
    Buffer_ACC_CB *prevPage_BCBP; /* buffer access control block holding data of the previous page*/
    Buffer_ACC_CB *nextPage_BCBP; /* buffer access control block holding data of the next page */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1,
	     ("om_AddPageToFile(handle, finfo=%P, prevPid=%P, newPage=%P, lockup=%P)",
	      finfo, prevPid, newPage, lockup));


    /* check parameters */

    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (prevPid == NULL) ERR(handle, eBADPAGEID);

    if (newPage == NULL) ERR(handle, eBADPAGEID);


    /*
     * Change the previous page
     */

    /* get the previous page */
    e = BfM_getAndFixBuffer(handle, prevPid, M_FREE, &prevPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    prevPage = (SlottedPage *)prevPage_BCBP->bufPagePtr;


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_MODIFY_PAGE_LIST_NEXT_LINK, LOG_REDO_UNDO,
                              *prevPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(ShortPageID), &(newPage->header.pid),
                              sizeof(ShortPageID), &(prevPage->header.nextPage));

	e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	if (e < eNOERROR) ERRB1(handle, e, prevPage_BCBP, PAGE_BUF);

        /* mark the lsn in the page */
	prevPage->header.lsn = lsn;
	prevPage->header.logRecLen = logRecLen;
    }

    /* get the previous page's next page id */
    MAKE_PAGEID(nextPid, finfo->fid.volNo, prevPage->header.nextPage);

    /* change previous page's nextPage into the new page */
    prevPage->header.nextPage = newPage->header.pid.pageNo;

    /* set dirty flag of the previous page */
    prevPage_BCBP->dirtyFlag = 1;

    /* unfix the previous page's buffer page */
    e = BfM_unfixBuffer(handle, prevPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Change the new page
     */
    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	/*
	 * log record for updating the previous link
	 */
        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_MODIFY_PAGE_LIST_PREV_LINK, LOG_REDO_ONLY,
                              newPage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(ShortPageID), &(prevPid->pageNo));

	e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	if (e < eNOERROR) ERR(handle, e);

	/*
	 * log record for updating the next link
	 */
        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_MODIFY_PAGE_LIST_NEXT_LINK, LOG_REDO_ONLY,
                              newPage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(ShortPageID), &(nextPid.pageNo));

	e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	if (e < eNOERROR) ERR(handle, e);

	/* mark the lsn in the page */
	newPage->header.lsn = lsn;
	newPage->header.logRecLen = logRecLen;
    }

    /* insert the new page into the double linked list of file map */
    newPage->header.nextPage = nextPid.pageNo;
    newPage->header.prevPage = prevPid->pageNo;


    /*
     * Change the next page
     */
    if (nextPid.pageNo == NIL) { /* last page */
	/*
	** Update the last page of the file.
	*/

	if (finfo->tmpFileFlag) { /* temporary file */
	    /* Update the catalog entry in the memory. */
	    /* update the last page */
	    finfo->catalog.entry->data.lastPage = newPage->header.pid.pageNo;

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
                                      sizeof(ShortPageID), &(newPage->header.pid.pageNo),
                                      sizeof(ShortPageID), &(catEntry->lastPage));

		e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
		if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

		/* mark the lsn in the page */
		catPage->header.lsn = lsn;
		catPage->header.logRecLen = logRecLen;
	    }

	    /* Assert that the previous page was the last page; it was checked */
	    /* in the caller and it shouldn't be changed. */
	    /* So we don't have to check the lastPage value in catalog. */
	    catEntry->lastPage = newPage->header.pid.pageNo;

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
	/* get lock on the next page */
	if(lockup){
	    e = LM_getPageLock(handle, &xactEntry->xactId, &nextPid, &finfo->fid,
			       lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERR(handle, e);

	    if (lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);     /* deadlock */
	    }
	}

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
                                  sizeof(ShortPageID), &(newPage->header.pid.pageNo),
                                  sizeof(ShortPageID), &(nextPage->header.prevPage));

	    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, nextPage_BCBP, PAGE_BUF);

	    /* mark the lsn in the page */
	    nextPage->header.lsn = lsn;
	    nextPage->header.logRecLen = logRecLen;
	}

	/* insert the new page into the double linked list of file map */
	nextPage->header.prevPage = newPage->header.pid.pageNo;

	nextPage_BCBP->dirtyFlag = 1;

	/* unfix the next page's buffer page */
	e = BfM_unfixBuffer(handle, nextPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* om_AddPageToFile() */
