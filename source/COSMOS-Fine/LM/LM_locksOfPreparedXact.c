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
 * Module: LM_LogLocksOfPreparedXact.c
 *
 * Description:
 *  	log & recover locks of lock table entries of prepared transaction
 *
 * Exports:
 * 	Four LM_logLocksOfPreparedXact(XactID*)
 *	Four LM_recoverLocksOfPreparedXact(LOG_LogRecInfo_T*)
 */


#include <stdio.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "TM.h"
#include "LM.h"
#include "LM_macro.h"
#include "LM_LockMatrix.h"
#include "SHM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*================================
 * LM_logLocksOfPreparedXact()
 *================================*/
/*
 * Function: Four LM_logLocksOfPreparedXact(XactID*)
 *
 * Description:
 *  	log lock images of lock table entries of prepared transaction.
 *
 * Returns:
 *  	error code
 */
Four LM_logLocksOfPreparedXact(
    Four				handle, 		/* IN handle */
    XactID 				*xactID			/* IN transaction ID */
)
{
    Four				nLocks = 0; 		/* # of LockTableInfo */
    LOG_Image_LM_LocksOfPreparedXact_T	bufOfLockTableInfo[LOG_MAX_LOCKS_OF_PREPARED_XACT_PER_LOG_RECORD]; /* buffer of LockTableInfo */
    XactTableEntry_T 			*xactEntry;		/* transaction table entry */
    Four				e;    			/* return value */
    LockLevel           		lockLevel;		/* for loop */
    RequestNode_Type    		*aRequest;		/* point to request node */
    LockHashEntry       		*lockHashEntryPtr;	/* point to lock hash entry */
    XactBucket_Type     		*xBucket;		/* point to transaction bucket */
    LockBucket_Type     		*lBucket;		/* point to lock bucket */
    RequestNode_Type    		*_rnode;		/* point to request node */
    LockBucket_Type     		*_lbucket;		/* point to lock bucket */
    Lsn_T 				lsn;            	/* lsn of the newly written log record */
    Four 				logRecLen;      	/* log record length */
    LOG_LogRecInfo_T 			logRecInfo; 		/* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_LM, TR1, ("LM_logLocksOfPreparedXact(xactID=%P)", xactID));


    /* get my transaction table entry */
    xactEntry = MY_XACT_TABLE_ENTRY(handle);

    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, L_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* if no xactbucket of this xactID, invalid transaction identifier error */
    e = find_xactBucket(handle, xactID, &xBucket);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    /* search the lockBucket from xBucket->grantedList[L_FILE] to xBucket->grantedList[L_OBJECT] */
    /* NOTE: we must get file locks first later for lock hierachy */
    for (lockLevel=L_FILE; lockLevel>=L_OBJECT; lockLevel--) {

        aRequest = PHYSICAL_PTR(xBucket->grantedList[lockLevel]);

        while ( aRequest != NULL) {

            lBucket = PHYSICAL_PTR(aRequest->lockHDR);

            /* request latch for access the lockBucket */
            e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, L_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	    /* fill the bufOfLockTableInfo */
	    bufOfLockTableInfo[nLocks].target  = lBucket->target;
	    bufOfLockTableInfo[nLocks].level   = lBucket->level;
	    bufOfLockTableInfo[nLocks].mode    = aRequest->mode;
	    bufOfLockTableInfo[nLocks].duration = aRequest->duration;

	    /* find the file ID to which the target belongs, if the target's level is L_OBJECT or L_PAGE*/
	    if (lockLevel == L_OBJECT || lockLevel == L_PAGE) {

	    	_rnode = PHYSICAL_PTR(aRequest->higherRequestNode);
	    	if (_rnode == NULL) ERR(handle, eINTERNAL);

	    	_lbucket = PHYSICAL_PTR(_rnode->lockHDR);
	    	if (_lbucket == NULL) ERR(handle, eINTERNAL);
	    	if (_lbucket->level != L_FILE ) ERR(handle, eINTERNAL);	/* NOTE: level must be a L_FILE */

	        bufOfLockTableInfo[nLocks].fileID = _lbucket->target.fileID;
	    }

	    nLocks++;

	    /* release latch for the lockBucket */
            e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
            if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

            /* For access next granted request node */
            aRequest = PHYSICAL_PTR(aRequest->nextGrantedEntry);

            /* Write Log Image */
            if (nLocks == LOG_MAX_LOCKS_OF_PREPARED_XACT_PER_LOG_RECORD) {

                LOG_FILL_LOGRECINFO_1(logRecInfo, *xactID, LOG_TYPE_TRANSACTION,
                                      LOG_ACTION_XACT_PREPARE_LOCKS, LOG_REDO_ONLY,
                                      common_perThreadDSptr->nilPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      nLocks*sizeof(LOG_Image_LM_LocksOfPreparedXact_T), bufOfLockTableInfo);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

                nLocks = 0;
            } /* if */
        } /* while */
    } /* for */

    /* Write Log Image */
    if (nLocks > 0) {
        LOG_FILL_LOGRECINFO_1(logRecInfo, *xactID, LOG_TYPE_TRANSACTION,
                              LOG_ACTION_XACT_PREPARE_LOCKS, LOG_REDO_ONLY,
                              common_perThreadDSptr->nilPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              nLocks*sizeof(LOG_Image_LM_LocksOfPreparedXact_T), bufOfLockTableInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);
    }

    /* release lmLatch */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);
}



/*================================
 * LM_recoverLocksOfPreparedXact()
 *================================*/
/*
 * Function: Four LM_recoverLocksOfPreparedXact(LOG_LogRecInfo_T*)
 *
 * Description:
 *  	recover the locks of lock table entries from log image of locks of prepared transaction.
 *
 * Returns:
 *  	error code
 */
Four LM_recoverLocksOfPreparedXact(
    Four				handle, 			/* IN handle */
    LOG_LogRecInfo_T 			*logRecInfo			/* IN log record info */
)
{
    Four				i;				/* for loop */
    XactID				*xactID;			/* point to transaction ID */
    Four				nLocks;				/* # of lock table entries */
    LOG_Image_LM_LocksOfPreparedXact_T	*bufOfLockTableInfo;		/* buffer of lock table entries */
    LOG_Image_LM_LocksOfPreparedXact_T	*ptr;				/* point to bufferOfLockTableInfo */
    Four				e;				/* return value */
    LockReply 				lockReply;			/* lock reply */
    LockMode				oldMode;			/* old lock mode */


    TR_PRINT(handle, TR_LM, TR1, ("LM_recoverLocksOfPreparedXact(logRecInfo=%P)", logRecInfo));


    /* get the information from log record info */
    xactID 	       = &logRecInfo->xactId;
    nLocks  	       = logRecInfo->imageSize[0] / sizeof(LOG_Image_LM_LocksOfPreparedXact_T);
    bufOfLockTableInfo = (LOG_Image_LM_LocksOfPreparedXact_T*)logRecInfo->imageData[0];

    /* get locks */
    for (i=0; i<nLocks; i++) {

    	ptr = &bufOfLockTableInfo[i];

    	switch(ptr->level) {
    	    case L_OBJECT:
    	        e = LM_getObjectLock(handle, xactID, &ptr->target.objectID, &ptr->fileID,
    	       	                     ptr->mode, ptr->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
		if (e < eNOERROR) ERR(handle, e);

    	    case L_PAGE:
    	    	e = LM_getPageLock(handle, xactID, &ptr->target.pageID, &ptr->fileID,
    	    	                   ptr->mode, ptr->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
		if (e < eNOERROR) ERR(handle, e);

    	    case L_KEYVALUE:
    	    	e = LM_getKeyRangeLock(handle, xactID, &ptr->target.keyValue,
    	    	                       ptr->mode, ptr->duration, L_UNCONDITIONAL, &lockReply);
		if (e < eNOERROR) ERR(handle, e);

    	    case L_FLAT_OBJECT:
    	    	e = LM_getFlatObjectLock(handle, xactID, &ptr->target.objectID,
    	    	                         ptr->mode, ptr->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
		if (e < eNOERROR) ERR(handle, e);

    	    case L_FLAT_PAGE:
    	    	e = LM_getFlatPageLock(handle, xactID, &ptr->target.pageID,
    	    	                       ptr->mode, ptr->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
		if (e < eNOERROR) ERR(handle, e);

    	    case L_FILE:
    	    	e = LM_getFileLock(handle, xactID, &ptr->target.fileID,
    	    	                   ptr->mode, ptr->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
    	    	if (e < eNOERROR) ERR(handle, e);

    	    default:
    	    	return (eINTERNAL);
    	}
    }


    return (eNOERROR);
}
