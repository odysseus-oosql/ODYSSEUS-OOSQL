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
 * Module: rm_Redo.c
 *
 * Description:
 *  start the redo pass for restart recovery
 *
 * Exports:
 *  Four rm_Redo(Four, Lsn_T*, RM_DirtyPageTable_T*)
 */


#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "dirtyPageTable.h"
#include "TM.h"
#include "BfM.h"
#include "SM.h"
#include "LOG.h"
#include "RM.h"
#include "LM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four rm_Redo(Four, Lsn_T*, DirtyPageTable_T*)
 *
 * Description:
 *  start the redo pass for restart recovery
 *
 * Returns:
 *  error code
 */
Four rm_Redo(
    Four 		handle,
    Lsn_T 		*redoLsn,		/* IN starting point of the redo pass */
    DirtyPageTable_T 	*dpt)			/* IN dirty page table */
{
    Four 		e;                     	/* returned error code */
    Lsn_T 		lsn;                  	/* lsn of the current log record */
    Lsn_T 		recLsn;			/* recovery lsn of the dirty page */
    Lsn_T 		tmpLsn;			/* temporary variable */
    Four 		logRecLen;             	/* log record length */
    Buffer_ACC_CB 	*aPage_BCBP;		/* buffer access control block */
    PageHdr_T 		*pageHdr;         	/* a page header */
    XactTableEntry_T 	*xactEntryPtr; 		/* ptr to an entry in transaction table */
    LOG_LogRecInfo_T 	logRecInfo; 		/* log record information */
    Four 		i;                     /* loop index */
    Four 		type;                  /* page type */
    DeallocListElem 	*dlHead;
    DeallocListElem 	*dlElem;
    DeallocListElem 	*lastElem;
    Four 		size;			/* size of log image data */
    PageID 		*pids;			/* page identifications */
    SegmentID_T 	*segmentID;     	/* segment identifications */
    Four 		image[LOG_MAX_NUM_IMAGES][LOG_MAX_IMAGE_SIZE/sizeof(Four)]; /* space for images */

    /* pointer for LOG Data Structure of perThreadTable */
    LOG_PerThreadDS_T *log_perThreadDSptr = LOG_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_RM, TR1, ("rm_Redo(redoLsn=%P, dpt=%P)", redoLsn, dpt));


    /*
     *  allocate enough memory for log record images
     */
    for (i = 0; i < LOG_MAX_NUM_IMAGES; i++)
	logRecInfo.imageData[i] = image[i];


    /*
     * Open log scan and position at restart point
     */
    e = LOG_OpenScan(handle, redoLsn);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * read log record at restart redo point
     */
    e = LOG_NextRecord(handle, &lsn, &logRecInfo, &logRecLen);
    if (e != eENDOFLOG_LOG && e < eNOERROR) ERR(handle, e);


    /*
     *	do the following sequence until the last log record is encountered
     */
    while (e == eNOERROR) {

	switch(logRecInfo.type) {

	  case LOG_TYPE_UPDATE:
	  case LOG_TYPE_COMPENSATION:
	    /*
	     * Is the log record is a redoable page update? The updated page
	     * might not have made it to disk before sys failure. need to
	     * access page and check its LSN.
	     */
	    if ((logRecInfo.redoUndo == LOG_REDO_ONLY || logRecInfo.redoUndo == LOG_REDO_UNDO) &&
		RM_DPT_GetEntry(handle, dpt, &logRecInfo.pid, log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType, &recLsn) && LSN_CMP_GE(lsn, recLsn)) { 

                assert(!IS_NILPAGEID(logRecInfo.pid)); 

		/* fix the page */
		/*
		 * We don't have to latch the page because the restart redo is
		 * done by only one process.
		 */
		e = BfM_getAndFixBuffer(handle, &(logRecInfo.pid), M_FREE, &aPage_BCBP,
                                        log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType);
		if (e < eNOERROR) ERR(handle, e);

		pageHdr = (PageHdr_T*)aPage_BCBP->bufPagePtr;

		/* if the update is not present, then redo it. */
		if (LSN_CMP_LT(pageHdr->lsn, lsn)) {
		    /* redo update */
		    e = (*(log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].redoFnPtr))(handle, aPage_BCBP->bufPagePtr, &logRecInfo);
		    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType);

		    /* Set the page lsn. */
                    pageHdr->lsn = lsn;
                    pageHdr->logRecLen = logRecLen;

		    /* Set the dirty flag. */
		    aPage_BCBP->dirtyFlag = 1;

		} else { /* update already on page */
		    /* update dirty page list with correct info. */
		    /*
		     * this will happen if this page was written to disk after
		     * the checkpt but before sys failure
		     */
		    tmpLsn = pageHdr->lsn;
		    LOG_INCREASE_LSN(tmpLsn, 1);

		    e = RM_DPT_InsertEntry(handle, dpt, &(logRecInfo.pid), log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType, &tmpLsn); 
		    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
		}

		/* unfix the page */
		e = BfM_unfixBuffer(handle, aPage_BCBP, log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType);
		if (e < eNOERROR) ERR(handle, e);
	    }

            /*
             * If the log record is to free the page/train after commit,
             * remove the page/train from the deallocated page/train list.
             * Note: At commit we at first free the last page/train in the deallocated list.
             */
            if (logRecInfo.action == LOG_ACTION_RDSM_FREE_TRAINS &&
                TM_XT_GetEntryPtr(handle, &logRecInfo.xactId, &xactEntryPtr) &&
                !IS_NIL_LSN(xactEntryPtr->deallocLsn) &&
                LSN_CMP_GT(lsn, xactEntryPtr->deallocLsn)) {
                LOG_Image_RDsM_UpdateBitmap_T *updateBitmap = (LOG_Image_RDsM_UpdateBitmap_T*)logRecInfo.imageData[0];

                assert(xactEntryPtr->status == X_COMMIT);
                assert(LSN_CMP_GT(lsn, common_perThreadDSptr->nilLsn));

                if (updateBitmap->nBits == PAGESIZE2)
                    xactEntryPtr->nestedTopActions[0].idxOnDeallocPageArray --;
                else            /* TRAINSIZE2 */
                    xactEntryPtr->nestedTopActions[0].idxOnDeallocTrainArray --;

                assert(xactEntryPtr->nestedTopActions[0].idxOnDeallocPageArray >= -1);
                assert(xactEntryPtr->nestedTopActions[0].idxOnDeallocTrainArray >= -1);
            }
	    else if (logRecInfo.action == LOG_ACTION_XACT_PREPARE_LOCKS &&
		     TM_XT_GetEntryPtr(handle, &logRecInfo.xactId, &xactEntryPtr) &&
		     xactEntryPtr->status == X_PREPARE) {

		     e = LM_recoverLocksOfPreparedXact(handle, &logRecInfo);
		     if (e < eNOERROR) ERR(handle, e);
	    }

	    break;

	  case LOG_TYPE_TRANSACTION:
	    if (logRecInfo.action == LOG_ACTION_XACT_DEALLOC_LIST &&
                TM_XT_GetEntryPtr(handle, &logRecInfo.xactId, &xactEntryPtr) &&
                xactEntryPtr->status == X_COMMIT) {

                /* append the new elements to the list */
                type = *((One*)logRecInfo.imageData[0]);
		if (type == DL_PAGE || type == DL_TRAIN) {
		    pids = logRecInfo.imageData[1];
		    size = sizeof(PageID);
		}
		else if (type == DL_PAGE_SEGMENT || type == DL_TRAIN_SEGMENT) {
		    segmentID = logRecInfo.imageData[1];
		    size = sizeof(SegmentID_T);
		}
		else {
		    ERR(handle, eINTERNAL);
		}

                for (i = 0; i < logRecInfo.imageSize[1]/size; i++) { 
		    switch (type) {
		        case DL_PAGE:
                            e = TM_XT_AddToDeallocPageList(handle, xactEntryPtr, &pids[i]);
                            if (e < eNOERROR) ERR(handle, e);
			    break;
		        case DL_TRAIN:
                            e = TM_XT_AddToDeallocTrainList(handle, xactEntryPtr, &pids[i]);
                            if (e < eNOERROR) ERR(handle, e);
			    break;
		        case DL_PAGE_SEGMENT: 
                            e = TM_XT_AddToDeallocPageSegmentList(handle, xactEntryPtr, &segmentID[i]);
                            if (e < eNOERROR) ERR(handle, e);
			    break;
		        case DL_TRAIN_SEGMENT:
                            e = TM_XT_AddToDeallocTrainSegmentList(handle, xactEntryPtr, &segmentID[i]);
                            if (e < eNOERROR) ERR(handle, e);
			    break;
			default:
			    ERR(handle, eINTERNAL);
		    } /* switch */
                } /* for */
	    } /* if */

	    if (logRecInfo.action == LOG_ACTION_XACT_DELETE_ENTRY_FROM_DEALLOC_LIST &&
                TM_XT_GetEntryPtr(handle, &logRecInfo.xactId, &xactEntryPtr) &&
                xactEntryPtr->status == X_COMMIT) {

                /* delete last element from dealloc page/train segment list */
		segmentID = (SegmentID_T*)logRecInfo.imageData[0];

		if (segmentID->sizeOfTrain == PAGESIZE2) {
		    e = TM_XT_DeleteLastElemFromDeallocPageSegmentList(handle, xactEntryPtr, segmentID);
		    if (e < eNOERROR) ERR(handle, e);
    		}
    		else if (segmentID->sizeOfTrain == TRAINSIZE2) {
		    e = TM_XT_DeleteLastElemFromDeallocTrainSegmentList(handle, xactEntryPtr, segmentID);
		    if (e < eNOERROR) ERR(handle, e);
    		}
    		else ERR(handle, eINTERNAL);
	    }

	    break;
	}


	/*
	 * read the next log record
	 */
	e = LOG_NextRecord(handle, &lsn, &logRecInfo, &logRecLen);
	if (e != eENDOFLOG_LOG && e < eNOERROR) ERR(handle, e);

    } /* end while */


    /*
     * Close the log scan.
     */
    e = LOG_CloseScan(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * If a failure were to occur after the logging of the end record of a
     * transaction, but before the execution of all the pending actions of
     * that transaction, the remaining pending actions are redone during
     * this pass.
     */
    e = TM_XT_DoPendingActionsOfCommittedTransactions(handle);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* rm_Redo( ) */
