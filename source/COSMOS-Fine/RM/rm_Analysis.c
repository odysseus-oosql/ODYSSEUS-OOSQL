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
 * Module: rm_Analysis.c
 *
 * Description:
 *  Start the analysis pass for restart recovery.
 *
 * Exports:
 *  Four rm_Analysis(Four, Lsn_T*)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "latch.h"
#include "dirtyPageTable.h"
#include "xactTable.h"
#include "LOG.h"
#include "RDsM.h"
#include "TM.h"
#include "RM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four rm_Analysis(Four, DirtyPageTable_T*, Lsn_T*)
 *
 * Description:
 *  start the analysis pass for restart recovery.
 *  assume that the log file is already is opened.
 *
 * Returns:
 *  error code
 */
Four rm_Analysis(
    Four 			handle,
    DirtyPageTable_T 		*dpt,			/* OUT restart dirty page table */
    Lsn_T 			*redoLsn)		/* OUT starting point of the redo pass */
{
    Four 			e;			/* error code */
    Four 			volNo;                 	/* mounted volume number */
    Lsn_T 			lsn;                  	/* log sequence number */
    Lsn_T 			minRecLsn;            	/* the minimum recovery LSN */
    Lsn_T 			minDeallocLsn;		/* the minimum deallocate pages LSN */
    Lsn_T 			dummyLsn;             	/* dummy variable */
    XactID 			xactIdCounter;		/* next transaction id to alloc  */
    Boolean 			deleteXactEntryFlag; 	/* delete transaction table entry if TRUE */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    ActiveXactRec_T 		activeXact; 		/* information for an active transaction */
    ActiveXactRec_T 		*activeXactPtr; 	/* ptr to active transaction information */
    XactTableEntry_T 		*xactEntryPtr; 		/* ptr to an entry in transaction table */
    DirtyPage_T 		*dirtyPage;     	/* dirty page information */
    Boolean 			chkptProcessingFlag; 	/* TRUE if we should process the chkpt */
    Four 			logRecLen;             	/* log record length */
    Four 			i;                     	/* loop index */
    char 			*ptr;
    Four 			nVolumes;
    LOG_Image_RDsM_MountedVol_T *mountedVol; 		/* mounted/dismounted volume information */
    Four 			image[LOG_MAX_NUM_IMAGES][LOG_MAX_IMAGE_SIZE/sizeof(Four)]; /* space for images, declaration for allignment */

    /* pointer for LOG Data Structure of perThreadTable */
    LOG_PerThreadDS_T *log_perThreadDSptr = LOG_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_RM, TR1, ("rm_Analysis(dpt=%P, redoLsn=%P)", dpt, redoLsn));


    /*
     *  allocate enough memory for log record images
     */
    for (i = 0; i < LOG_MAX_NUM_IMAGES; i++)
	logRecInfo.imageData[i] = image[i];


    /*
     *	get the lsn of the checkpoint log record.
     */
    e = LOG_GetCheckpointLsn(handle, &lsn);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *	Does the checkpoint log record exist?
     */
    if (IS_NIL_LSN(lsn)) { /* if checkpointing have not been performed yet */
	/* Set the lsn to the starting lsn. */
	SET_STARTING_LSN(lsn);

	chkptProcessingFlag = FALSE;
    } else
	chkptProcessingFlag = TRUE;


    /*
     * Open the log scan
     */
    e = LOG_OpenScan(handle, &lsn);
    if (e < eNOERROR) ERR(handle, e);


    /* Initialize the transaction id counter */
    SET_MIN_XACTID(xactIdCounter);


    /*
     * read the log record following CHKPT_BEGIN
     */
    e = LOG_NextRecord(handle, &lsn, &logRecInfo, &logRecLen);
    if (e != eENDOFLOG_LOG && e < eNOERROR) ERR(handle, e);


    /*
     *	do the following sequence until the last complete log record is encountered
     */

    while (e == eNOERROR) {

	/*
	 *	if the log record is transaction-related
	 */
	if (logRecInfo.type == LOG_TYPE_UPDATE ||
	    logRecInfo.type == LOG_TYPE_COMPENSATION ||
	    logRecInfo.type == LOG_TYPE_TRANSACTION) {

	    /*
	     *	check whether this is the recently initiated transaction
	     */
	    if (XACTID_CMP_GT(logRecInfo.xactId, xactIdCounter))
		xactIdCounter = logRecInfo.xactId;


	    /*
	     *	check whether the corresponding entry exists in the transaction table
	     */
	    if (!TM_XT_GetEntryPtr(handle, &(logRecInfo.xactId), &xactEntryPtr)) {

		/*
		 *	construct transaction table entry information
		 */
		activeXact.xactId = logRecInfo.xactId;
		activeXact.status = X_NORMAL;
        activeXact.firstLsn = lsn; 
		activeXact.lastLsn = lsn;
		activeXact.undoNextLsn = logRecInfo.prevLsn;
		activeXact.deallocLsn = common_perThreadDSptr->nilLsn;


		/*
		 *	insert corresponding entry into the table
		 */
		e = TM_XT_InsertEntry(handle, &activeXact, &xactEntryPtr);
		if (e < eNOERROR) ERR(handle, e);

	    } /* end if */

	} /* end if */


	/*
	 * do the respective job according to the type of the log record
	 */
	switch (logRecInfo.type) {

	  case LOG_TYPE_UPDATE:
	  case LOG_TYPE_COMPENSATION:

#ifndef NDEBUG
            if (logRecInfo.redoUndo != LOG_NO_REDO_UNDO)
                assert(!IS_NILPAGEID(logRecInfo.pid));
#endif /* NDEBUG */

	    xactEntryPtr->lastLsn = lsn;

	    if (logRecInfo.type == LOG_TYPE_UPDATE) {
		/* if the log record is undoable */
		if (logRecInfo.redoUndo == LOG_REDO_UNDO ||
		    logRecInfo.redoUndo == LOG_UNDO_ONLY)
		    xactEntryPtr->undoNextLsn = lsn;

	    } else /* LOG_TYPE_COMPENSATION */
		xactEntryPtr->undoNextLsn = logRecInfo.undoNextLsn;

	    /*
	     * if the log record is redoable and the updated page is not
	     * in the dirty page, then insert it into the dirty page table.
	     */
	    if (logRecInfo.redoUndo == LOG_REDO_UNDO ||
		logRecInfo.redoUndo == LOG_REDO_ONLY ) {
		if (!RM_DPT_GetEntry(handle, dpt, &(logRecInfo.pid), log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType, &dummyLsn)) {
		    e = RM_DPT_InsertEntry(handle, dpt, &(logRecInfo.pid), log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType, &lsn);
		    if (e < eNOERROR) ERR(handle, e);
		}
	    }

	    break;


	  case LOG_TYPE_TRANSACTION:

	    /* initialize the deleteXactEntryFlag */
	    deleteXactEntryFlag = FALSE;

	    /* LOG_ACTION_BEGIN_TRANSACTION does not exist. */
	    switch (logRecInfo.action) {

	      case LOG_ACTION_XACT_BEGIN_LOGGING_DEALLOC_LIST:
		/* save the lsn */
		xactEntryPtr->deallocLsn = lsn;
		break;

	      case LOG_ACTION_XACT_PREPARE_TRANSACTION:
                xactEntryPtr->status = X_PREPARE;
                break;

	      case LOG_ACTION_XACT_COMMIT_TRANSACTION:
                xactEntryPtr->status = X_COMMIT;
                break;

	      case LOG_ACTION_XACT_ABORT_TRANSACTION:
                xactEntryPtr->status = X_ABORT;
		break;

	      case LOG_ACTION_XACT_END_TRANSACTION:
		deleteXactEntryFlag = TRUE;
		break;
	    }

	    if (deleteXactEntryFlag) {
		/* delete the transaction table entry */
		TM_XT_DeleteEntry(handle, &(logRecInfo.xactId));
	    }

	    break;


	  case LOG_TYPE_CHECKPOINT:

	    /*
	     * If we have already processed a chkpt record,
	     * we ignore the other chkpt records.
	     */
	    if (!chkptProcessingFlag) break;

	    switch (logRecInfo.action) {

	      case LOG_ACTION_CHKPT_BEGIN:

		/* ignore this log record */
		break;

	      case LOG_ACTION_CHKPT_END:
		/* the following chkpt records are not processed. */
		chkptProcessingFlag = FALSE;
		break;

	      case LOG_ACTION_CHKPT_ACTIVE_XACTS:

		activeXactPtr = (ActiveXactRec_T*)logRecInfo.imageData[0];

		/*
		 * for each active transaction, insert the corresponding entry
		 * into the transaction table
		 */
		for (i = 0; i < logRecInfo.imageSize[0]/sizeof(ActiveXactRec_T); i++) {

		    /*
		     *	check whether the corresponding entry exists
		     *  in the transaction table
		     */
		    if (TM_XT_GetEntryPtr(handle, &(activeXactPtr->xactId), &xactEntryPtr)) {
			/*
			 * Set the dealloc lsn using the active xact record.
			 * If the transaction entry's dealloc lsn was not NIL,
			 * then the value should be same as the active xact
			 * record's value.
			 */
                        if (!IS_NIL_LSN(activeXactPtr->deallocLsn))
                            xactEntryPtr->deallocLsn = activeXactPtr->deallocLsn;

                        if (!IS_NIL_LSN(activeXactPtr->firstLsn))
                            xactEntryPtr->firstLsn = activeXactPtr->firstLsn;

		    } else {
			e = TM_XT_InsertEntry(handle, activeXactPtr, &xactEntryPtr);
			if (e < eNOERROR) ERR(handle, e);
		    }

		    activeXactPtr++;

		} /* end for */

		break;

	      case LOG_ACTION_CHKPT_DIRTY_PAGES:

		dirtyPage = (DirtyPage_T*)logRecInfo.imageData[1];

		/*
		 * for each dirty page, insert the corresponding entry
		 * into the dirty pages table
		 */
		for (i = 0; i < logRecInfo.imageSize[1]/sizeof(DirtyPage_T); i++) {
		    e = RM_DPT_InsertEntry(handle, dpt, &(dirtyPage->pid), *((One*)logRecInfo.imageData[0]), &(dirtyPage->recLsn));
		    if (e < eNOERROR) ERR(handle, e);

		    dirtyPage++;

		} /* end for */

		break;

	      case LOG_ACTION_CHKPT_MOUNTED_VOLS:

		/*
		 * Mount the volumes.
		 */
		nVolumes = *((Four*)logRecInfo.imageData[0]);
                ptr = logRecInfo.imageData[1];
		for (i = 0; i < nVolumes; i++) {

                    assert(ptr != NULL);

		    e = RDsM_MountWithDeviceListString(handle, ptr, &volNo, FALSE);
		    if (e < eNOERROR) ERR(handle, e);

                    ptr = strchr(ptr, '\0');
                    ptr++;
		}

		break;

	    } /* end switch */

	    break;


	  case LOG_TYPE_VOLUME:

	    if (logRecInfo.action == LOG_ACTION_VOL_MOUNT_VOLUME) {

		/*
		 * Mount the volume.
		 */
		mountedVol = (LOG_Image_RDsM_MountedVol_T*)logRecInfo.imageData[0];
		e = RDsM_MountWithDeviceListString(handle, logRecInfo.imageData[1], &volNo, FALSE);
		if (e < eNOERROR) ERR(handle, e);

	    } else if (logRecInfo.action == LOG_ACTION_VOL_DISMOUNT_VOLUME) {

		volNo = *((VolNo*)logRecInfo.imageData[0]);

		/*
		 * delete all the entries corresponding to the dirty pages included in
		 * this volume from the dirty pages table
		 */
		e = RM_DPT_DeleteEntries(handle, dpt, volNo);
		if (e < eNOERROR) ERR(handle, e);

		/*
		 * Dismount the volume.
		 */
		e = RDsM_Dismount(handle, volNo, FALSE);
		if (e < eNOERROR) ERR(handle, e);

	    } /* end else if */

	    break;

	} /* end switch */


	/*
	 * read the next log record
	 */
	e = LOG_NextRecord(handle, &lsn, &logRecInfo, &logRecLen);
	if (e != eENDOFLOG_LOG && e < eNOERROR) ERR(handle, e);

    } /* end while */


    /*
     * Close the scan.
     */
    e = LOG_CloseScan(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Remove all the log page in the buffer manager buffer pool,
     * Some log pages in the buffer pool may be allocated to the log buffer.
     * In this case log pages are located in two places => duplicate!!!.
     * In the long run, buffer pool version will have obsolte data.
     */
    e = BfM_RemoveLogPages(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *	set the next lsn in the logMaster
     */
    e = LOG_SetNextLogRecordLsn(handle, &lsn);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Set the xactId counter
     */
    if (IS_MIN_XACTID(xactIdCounter))
        SET_STARTING_XACTID(xactIdCounter);
    else
        INCREASE_XACTID(xactIdCounter);
    (void) TM_SetXactIdCounter(handle, &xactIdCounter);


    /*
     *	find the minimum recLSN in the dirty pages table
     */
    e = RM_DPT_GetMinRecLsn(handle, dpt, &minRecLsn);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *	find the minimum deallocLsn in the transactions table
     */
    (void) TM_XT_GetMinDeallocLsn(handle, &minDeallocLsn);


    /*
     *	determine the redoLsn
     */
    *redoLsn = (LSN_CMP_LT(minDeallocLsn, minRecLsn)) ? minDeallocLsn : minRecLsn;


    /*
     *  Delete rollbacked transactions with missing end record
     */
    (void) TM_XT_DeleteRollbackedXactEntries(handle);

    return(eNOERROR);

} /* rm_Analysis() */
