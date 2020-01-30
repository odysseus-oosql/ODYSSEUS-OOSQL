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
 * Module: RM_Rollback.c
 *
 * Description:
 *  the individual rollback of a transaction is performed
 *
 * Exports:
 *  Four RM_Rollback(Four, XactTableEntry_T*)
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
 * Function: Four RM_Rollback(Four, XactTableEntry_T*)
 *
 * Description:
 *  the individual rollback of a transaction is performed
 *
 * Returns:
 *  error code
 *    eNOERROR
 *    eNULLPTR_RM - pointer has a null value
 */
Four RM_Rollback(
    Four 		handle,
    XactTableEntry_T	*xactEntry, 		/* INOUT transaction table entry */
    Lsn_T 		*saveLsn)             	/* IN savepoint to rollback */
{
    Four 		e;			/* error code */
    LOG_LogRecInfo_T 	logRecInfo; 		/* information for a log record */
    Lsn_T 		undoNextLsn;		/* log record to undo next time */
    Buffer_ACC_CB 	*aPage_BCBP;		/* buffer access control block */
    PageHdr_T 		*pageHdr;         	/* page header */
    Four 		logRecLen;             	/* log record length */
    Four 		i;			/* temporary variable */
    Four 		image[LOG_MAX_NUM_IMAGES][LOG_MAX_IMAGE_SIZE/sizeof(Four)]; /* space for images */

    /* pointer for LOG Data Structure of perThreadTable */
    LOG_PerThreadDS_T *log_perThreadDSptr = LOG_PER_THREAD_DS_PTR(handle);


    /*
     * allocate enough memory for log record images
     */
    for (i = 0; i < LOG_MAX_NUM_IMAGES; i++)
	logRecInfo.imageData[i] = (char *) image[i];


    /*
     * get the lsn of the log record from the transaction table
     */
    undoNextLsn = xactEntry->undoNextLsn;


    /*
     * do the following sequence until the first log record of the transaction
     * is encountered
     */
    while (LSN_CMP_LT(*saveLsn, undoNextLsn)) { 

	/*
	 * read the log record pointed by lsn into logRecInfo
	 */
	e = LOG_ReadLogRecord(handle, &undoNextLsn, &logRecInfo, &logRecLen);
        if (e == eENDOFLOG_LOG) return(eNOERROR);
	if (e < eNOERROR) ERR(handle, e);


	switch (logRecInfo.type) {

	  case LOG_TYPE_UPDATE:
	    /*
	     * if log reocrd is undoable then record needs undoing
	     * (not redo-only record)
	     */
	    if (logRecInfo.redoUndo == LOG_UNDO_ONLY || logRecInfo.redoUndo == LOG_REDO_UNDO) {
#ifdef CCPL
                /* We have the lock on the page. */
		e = BfM_getAndFixBuffer(handle, &logRecInfo.pid, M_FREE, &aPage_BCBP,
                                        log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType); 
#endif /* CCPL */
#ifdef CCRL
		e = BfM_getAndFixBuffer(handle, &logRecInfo.pid, M_EXCLUSIVE, &aPage_BCBP,
                                        log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType); 
#endif /* CCRL */
		if (e < eNOERROR) ERR(handle, e);

		pageHdr = (PageHdr_T*)aPage_BCBP->bufPagePtr;

		/* if the update is present, then undo it. */
		if (LSN_CMP_GE(pageHdr->lsn, undoNextLsn)) {
		    /* undo update */
                    e = (*(log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].undoFnPtr))(handle, xactEntry, aPage_BCBP, &undoNextLsn, &logRecInfo);
                    if (e < eNOERROR) ERR(handle, e);

		    /* set the dirty flag */
		    /* we set the dirty flag in the undo functions */
		}

#ifdef CCRL
		if (aPage_BCBP->latchPtr != NULL) { 
		    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
		    if (e < eNOERROR) ERR(handle, e);
		}
#endif /* CCRL */

		e = BfM_unfixBuffer(handle, aPage_BCBP, log_perThreadDSptr->LOG_logRecTbl[logRecInfo.action].bufType);
		if (e < eNOERROR) ERR(handle, e);

	    }

	    /*
	     * get next record to process
	     */
	    undoNextLsn = logRecInfo.prevLsn;

	    break;

	  case LOG_TYPE_COMPENSATION:
	    /*
	     * pick up address of the next record to examine
	     */
	    undoNextLsn = logRecInfo.undoNextLsn;
	    break;

	  default:
	    /*
	     * pick up address of the next record to examine
	     */
	    undoNextLsn = logRecInfo.prevLsn;
	    break;
	}

	/*
	 * Update the undo next lsn of the transaction table.
	 */
	e = SHM_getLatch(handle, &xactEntry->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	if (e < eNOERROR) ERR(handle, e);

	MY_UNDO_NEXT_LSN(handle) = undoNextLsn;

	e = SHM_releaseLatch(handle, &xactEntry->latch, procIndex);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* RM_Rollback( ) */
