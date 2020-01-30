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
 * Module: rm_Undo.c
 *
 * Description:
 *  start the undo pass for restart recovery
 *
 * Exports:
 *  Four rm_Undo(Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "dirtyPageTable.h"
#include "xactTable.h"
#include "LOG.h"
#include "TM.h"
#include "BfM.h"
#include "RM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four rm_Undo(Four)
 *
 * Description:
 *  start the undo pass for restart recovery
 *
 * Returns:
 *  error code
 */
Four rm_Undo(
    Four 		handle
)
{
    Four 		e;			/* error code */
    Four 		i;			/* loop index */
    Four 		logRecLen;		/* length of a log record */
    Lsn_T 		lsn;			/* log sequence number */
    LOG_LogRecInfo_T 	logRecInfo; 		/* log record information */
    Buffer_ACC_CB 	*aPage_BCBP;		/* buffer access control block */
    PageHdr_T 		*pageHdr;         	/* a page header */
    XactTableEntry_T 	*xactEntryPtr; 		/* ptr to an entry in transaction table */
    SegmentID_T 	*segmentID;     	/* segment identification */
    LOG_LogRecInfo_T 	tmpLogRecInfo; 		/* log record information */
    Four 		image[LOG_MAX_NUM_IMAGES][LOG_MAX_IMAGE_SIZE/sizeof(Four)]; /* space for images */

    /* pointer for LOG Data Structure of perThreadTable */
    LOG_PerThreadDS_T *log_perThreadDSptr = LOG_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    /*
     * allocate enough memory for log record images
     */
    for (i = 0; i < LOG_MAX_NUM_IMAGES; i++)
	logRecInfo.imageData[i] = (char *) image[i];


    /*
     * do the following sequence until the all the imcomplete transactions are rolled back
     */
    for ( ; ; ) {

	/*
	 * get the maximum undoNextLsn from the TM
	 */
	(void) TM_XT_GetMaxUndoNextLsn(handle, &lsn);


	/*
	 * there is no transaction in the transaction table
	 */
	if (IS_NIL_LSN(lsn)) break;


	/*
	 * read the log record pointed by lsn into logRecInfo
	 */
	e = LOG_ReadLogRecord(handle, &lsn, &logRecInfo, &logRecLen);
	if (e < eNOERROR) ERR(handle, e);


	/*
	 * Get the transaction table entry pointer.
	 */
	if (logRecInfo.type == LOG_TYPE_UPDATE || logRecInfo.type == LOG_TYPE_COMPENSATION || logRecInfo.type == LOG_TYPE_TRANSACTION)
	    (Boolean) TM_XT_GetEntryPtr(handle, &(logRecInfo.xactId), &xactEntryPtr);


	switch (logRecInfo.type) {

	  case LOG_TYPE_UPDATE:
	    /*
	     * if log reocrd is undoable then record needs undoing
	     * (not redo-only record)
	     */
	    if (logRecInfo.redoUndo == LOG_UNDO_ONLY || logRecInfo.redoUndo == LOG_REDO_UNDO) {
		e = BfM_getAndFixBuffer(handle, &(logRecInfo.pid), M_FREE, &aPage_BCBP,
                                        log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType);
		if (e < eNOERROR) ERR(handle, e);

		pageHdr = (PageHdr_T*)aPage_BCBP->bufPagePtr;

		/* if the update is present, then undo it. */
		if (LSN_CMP_GE(pageHdr->lsn, lsn)) {
		    /* undo update */
		    e = (*(log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].undoFnPtr))(handle, xactEntryPtr, aPage_BCBP, &lsn, &logRecInfo);
		    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

		    /* set the dirty flag */
		    aPage_BCBP->dirtyFlag = 1;
		}

		e = BfM_unfixBuffer(handle, aPage_BCBP, log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType);
		if (e < eNOERROR) ERR(handle, e);

	    } else {
		/* record cannot be undone -ignore it */
	    }

	    /*
	     * next record to process is the one preceeding this record
	     * in its backward chain.
	     */
	    xactEntryPtr->undoNextLsn = logRecInfo.prevLsn;


	    /*
	     * have undone completely? - write end
	     */
	    if (IS_NIL_LSN(logRecInfo.prevLsn)) {

		/*
		 * notify the transaction is completely rollbacked by logging
		 */
                LOG_FILL_LOGRECINFO_0(tmpLogRecInfo, xactEntryPtr->xactId, LOG_TYPE_TRANSACTION,
                                      LOG_ACTION_XACT_END_TRANSACTION, LOG_REDO_ONLY,
                                      common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn);

		e = LOG_WriteLogRecord(handle, xactEntryPtr, &tmpLogRecInfo, &lsn, &logRecLen);
		if (e < eNOERROR) ERR(handle, e);

		/*
		 * delete the corresponding entry from the transaction table using the TM
		 */
		TM_XT_DeleteEntry(handle, &(logRecInfo.xactId));
	    }
	    break;

	  case LOG_TYPE_COMPENSATION:
	    /*
	     * pick up address of the next record to examine
	     */
	    xactEntryPtr->undoNextLsn = logRecInfo.undoNextLsn;
	    break;

	  case LOG_TYPE_TRANSACTION:
	    if (logRecInfo.action == LOG_ACTION_XACT_DELETE_ENTRY_FROM_DEALLOC_LIST && xactEntryPtr->status == X_COMMIT) {

                /* add the page segment id to dealloc page/train segment list */
                segmentID = (SegmentID_T*)logRecInfo.imageData[0];

    		if (segmentID->sizeOfTrain == PAGESIZE2) {
        	    e = TM_XT_AddToDeallocPageSegmentList(handle, xactEntryPtr, segmentID);
        	    if (e < eNOERROR) ERR(handle, e);
    		}
    		else if (segmentID->sizeOfTrain == TRAINSIZE2) {
        	    e = TM_XT_AddToDeallocTrainSegmentList(handle, xactEntryPtr, segmentID);
        	    if (e < eNOERROR) ERR(handle, e);
    		}
		else ERR(handle, eINTERNAL);

    	        /*
     	         *  make the compensation log record
     	         */
    	        LOG_FILL_LOGRECINFO_1(tmpLogRecInfo, xactEntryPtr->xactId, LOG_TYPE_COMPENSATION,
                                      LOG_ACTION_XACT_DELETE_ENTRY_FROM_DEALLOC_LIST, LOG_REDO_ONLY,
                                      common_perThreadDSptr->nilPid, xactEntryPtr->lastLsn, logRecInfo.prevLsn,
                                      logRecInfo.imageSize[0], logRecInfo.imageData[0]);

    	        e = LOG_WriteLogRecord(handle, xactEntryPtr, &tmpLogRecInfo, &lsn, &logRecLen);
    	        if (e < eNOERROR) ERR(handle, e);
    	    }

	    /*
	     * pick up address of the next record to examine
	     */
	    xactEntryPtr->undoNextLsn = logRecInfo.prevLsn;
	    break;
	}

    } /* end while */

    return(eNOERROR);

} /* rm_Undo() */
