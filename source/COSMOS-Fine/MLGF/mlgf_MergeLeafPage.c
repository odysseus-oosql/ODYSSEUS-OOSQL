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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: mlgf_MergeLeafPage.c
 *
 * Description:
 *  Merge two leaf pages into one.
 *
 * Exports:
 *  Four mlgf_MergeLeafPage(Four, Buffer_ACC_CB*, MLGF_KeyDesc*, Four, Two*,
 *                          LocalPool*, DeallocListElem*)
 *
 * Returns:
 *  eNOERROR
 *  MLGF_STATUS_DELETE
 *  MLGF_STATUS_UNDERFLOW
 *  MLGF_STATUS_EMPTYPAGE
 *  Error code
 *    some errors caused by function calls
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "RDsM.h"
#include "MLGF.h"
#include "LM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

Four mlgf_MergeLeafPage(
    Four 			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    Buffer_ACC_CB 		*dirPage_BCB,		/* INOUT buffer access control block for directory page */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of MLGF index */
    Four 			mergedEntryNo,		/* IN entry to be merged */
    LockParameter 		*lockup,      		/* IN request lock or not */
    LogParameter_T 		*logParam)   		/* IN log parameter */
{
    Four 			e;			/* error code */
    Four 			i;			/* index variable */
    Four 			dirEntryLen;		/* length of a directory entry */
    Four 			leafEntryLen;		/* length of a leaf entry */
    Four 			buddyEntryNo;		/* entry no of buddy entry of merged entry */
    Four 			buddyKey;		/* attribute no of buddy key */
    PageID 			pid;			/* a temporary PageID */
    Boolean 			found;			/* TRUE if buddy entry is found */
    Boolean 			mergeFlag;		/* TRUE if a merge occurs */
    Buffer_ACC_CB 		*mergedPage_BCB;    	/* buffer access control block for merged page */
    Buffer_ACC_CB 		*buddyPage_BCB;     	/* buffer access control block for buddy page */
    mlgf_DirectoryPage 		*dirPage;      		/* directory page containing 'mergedEntry' and 'buddyEntry' */
    mlgf_LeafPage 		*mergedPage;	      	/* merged directory page */
    mlgf_LeafPage 		*buddyPage;	      	/* buddy page of merged directory page */
    mlgf_LeafEntry 		*entry;	      		/* a leaf entry */
    mlgf_DirectoryEntry 	*mergedEntry; 		/* entry for merged page */
    mlgf_DirectoryEntry 	*buddyEntry;  		/* entry for buddy page of merged page */
    mlgf_DirectoryEntry 	buddyEntry_old; 	/* old version of buddy entry */
    LockReply 			lockReply;	
    LockMode 			oldMode;
    PageID 			mergePid; 
    MLGF_HashValue 		*extremeHashValues_x, *extremeHashValues_y; /* points to array of extreme(= min or max) hash values */
    Four 			k; 
    Lsn_T 			lsn;                  	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    LockMode  			mergedPageLockMode; 
    LOG_Image_MLGF_SpecifyEntries_T entriesInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_MergeLeafPage()"));


    /* Calculate the length of a directory entry. */
    dirEntryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    dirPage = (mlgf_DirectoryPage*)dirPage_BCB->bufPagePtr;

    mergedEntry = MLGF_ITH_DIRENTRY(dirPage, mergedEntryNo, dirEntryLen);

    mergeFlag = FALSE;		/* Intialize mergeFlag to FALSE. */

    /* Repeat until the merge is impossible. */
    while (mergedEntry->theta < MLGF_LP_THRESHOLD) {

	/* Find the buddy entry. */
	found = FALSE;
	for (buddyEntryNo = mergedEntryNo-1; buddyEntryNo <= mergedEntryNo+1; buddyEntryNo+=2) {

	    if (buddyEntryNo < 0 || buddyEntryNo >= dirPage->hdr.nEntries) continue;

	    buddyEntry = MLGF_ITH_DIRENTRY(dirPage, buddyEntryNo, dirEntryLen);

	    if (mlgf_BuddyTest(handle, kdesc->nKeys, mergedEntry, buddyEntry, &buddyKey)) {
		found = TRUE;
		break;
	    }
	}

	/* if there is not buddy region, exit the loop. */
	if (!found) break;

	/* there is not enough space to merge two pages */
	if (mergedEntry->theta + buddyEntry->theta + sizeof(Two) > PAGESIZE - MLGF_LP_FIXED) 
	    break;

	/* From now, merge proceeds. */
	mergeFlag = TRUE;

	if (buddyEntryNo > mergedEntryNo) { /* swap two entries */
	    buddyEntryNo = mergedEntryNo;
	    mergedEntryNo = buddyEntryNo + 1;

	    buddyEntry = MLGF_ITH_DIRENTRY(dirPage, buddyEntryNo, dirEntryLen);
	    mergedEntry = MLGF_ITH_DIRENTRY(dirPage, mergedEntryNo, dirEntryLen);
	}


	/*
	 * Merge the page pointed by 'mergedEntry'
	 * into the page pointed by 'buddyEntry'.
	 */

	/* get the PageIDs of buddy page and merged page */
	MAKE_PAGEID(pid, dirPage->hdr.pid.volNo, buddyEntry->spid);
	MAKE_PAGEID(mergePid, dirPage->hdr.pid.volNo, mergedEntry->spid);

	/* get lock on pid and mergePid*/
	if(lockup) {
	    /* get the lock mode of merged page */
	    e = LM_getKeyRangeLock(handle, &xactEntry->xactId, &mergePid, L_IX, L_MANUAL,
				   L_UNCONDITIONAL, &lockReply);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);
	    }
	    mergedPageLockMode = (LockMode)lockReply;

	    /* get keyRangeLock on buddy page */
	    e = LM_getKeyRangeLock(handle, &xactEntry->xactId, &pid, mergedPageLockMode, L_MANUAL,
				   L_UNCONDITIONAL, &lockReply);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);
	    }

	    e = LM_getFlatPageLock(handle, &xactEntry->xactId, &pid, L_X, L_MANUAL,
				   L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);
	    }


	    e = LM_getFlatPageLock(handle, &xactEntry->xactId, &mergePid, L_X, L_MANUAL,
				   L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);
	    }
	}

	/* Read the buddy page into the buffer. */
	e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &buddyPage_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	buddyPage = (mlgf_LeafPage*)buddyPage_BCB->bufPagePtr;

	/* Read the merged page into the buffer. */
	e = BfM_getAndFixBuffer(handle, &mergePid, M_FREE, &mergedPage_BCB, PAGE_BUF);
	if (e < 0) {
	    ERRB1(handle, e, buddyPage_BCB, PAGE_BUF);
	}

	mergedPage = (mlgf_LeafPage*)mergedPage_BCB->bufPagePtr;

	if (MLGF_LP_CFREE(buddyPage) < mergedEntry->theta + sizeof(Two))
	    mlgf_CompactLeafPage(handle, buddyPage, kdesc->nKeys, kdesc->extraDataLen, NIL);

	/* move entries in 'mergedPage' into the 'buddyPage' */
	for (i = 0; i < mergedPage->hdr.nEntries; i++) {
	    /* 'entry' points to a current leaf entry. */
	    entry = MLGF_ITH_LEAFENTRY(mergedPage, i);

	    leafEntryLen = MLGF_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen, entry->nObjects);

	    memcpy(&buddyPage->data[buddyPage->hdr.free], (char*)entry, leafEntryLen);

	    buddyPage->slot[-buddyPage->hdr.nEntries] = buddyPage->hdr.free;
	    buddyPage->hdr.nEntries++;
	    buddyPage->hdr.free += leafEntryLen;
	}

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            /*
             * merged page
             */
            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_INDEXID_OF_MLGF_PAGE, LOG_REDO_UNDO,
                                  mergedPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(IndexID), &(common_perThreadDSptr->nilIid),
                                  sizeof(IndexID), &mergedPage->hdr.iid);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            mergedPage->hdr.lsn = lsn;
            mergedPage->hdr.logRecLen = logRecLen;


            /*
             * buddy page
             */
            entriesInfo.startEntryNo = buddyPage->hdr.nEntries - mergedPage->hdr.nEntries;
            entriesInfo.nEntries = mergedPage->hdr.nEntries;

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_INSERT_LEAF_ENTRIES, LOG_REDO_UNDO,
                                  buddyPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_MLGF_SpecifyEntries_T), &entriesInfo,
                                  buddyPage->hdr.free - buddyPage->slot[-entriesInfo.startEntryNo], MLGF_ITH_LEAFENTRY(buddyPage, entriesInfo.startEntryNo));

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            buddyPage->hdr.lsn = lsn;
            buddyPage->hdr.logRecLen = logRecLen;
        }

	SET_NILINDEXID(mergedPage->hdr.iid); /* any more not used in this index */
	mergedPage_BCB->dirtyFlag = 1;

	/* Release latch and unfix buffer for mergedPage. */
	e = BfM_unfixBuffer(handle, mergedPage_BCB, PAGE_BUF);
	if (e < eNOERROR) ERRB1(handle, e, buddyPage_BCB, PAGE_BUF);

        e = RDsM_FreeTrain(handle, xactEntry, &mergePid, PAGESIZE2, FALSE, logParam);
        if (e < eNOERROR) ERRB1(handle, e, buddyPage_BCB, PAGE_BUF);

	/* Update the buddyEntry. */
        memcpy(&buddyEntry_old, buddyEntry, dirEntryLen);
	buddyEntry->nValidBits[buddyKey] --;
	buddyEntry->theta = MLGF_LP_THETA(buddyPage);

        /*
         * Set the MBR of the buddy page
         */
        extremeHashValues_x = MLGF_DIRENTRY_HASHVALUEPTR(buddyEntry, kdesc->nKeys);
        extremeHashValues_y = MLGF_DIRENTRY_HASHVALUEPTR(mergedEntry, kdesc->nKeys);
        for (k = 0; k < kdesc->nKeys; k++) {

	    if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, k)) {
                if (extremeHashValues_x[k] > extremeHashValues_y[k]) extremeHashValues_x[k] = extremeHashValues_y[k];
            } else { /* MAXTYPE */
                if (extremeHashValues_x[k] < extremeHashValues_y[k]) extremeHashValues_x[k] = extremeHashValues_y[k];
            }
        }

	/* Release latch and unfix buffer for buddyPage. */
	buddyPage_BCB->dirtyFlag = 1;

	e = BfM_unfixBuffer(handle, buddyPage_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            Two tmpEntryNo = buddyEntryNo;

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_DIRECTORY_ENTRY, LOG_REDO_UNDO,
                                  dirPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Two), &tmpEntryNo,
                                  dirEntryLen, buddyEntry,
                                  dirEntryLen, &buddyEntry_old);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            dirPage->hdr.lsn = lsn;
            dirPage->hdr.logRecLen = logRecLen;
        }

	/* Delete 'mergedEntry'. */
        e = mlgf_DeleteFromDirectory(handle, xactEntry, dirPage, kdesc, mergedEntryNo, logParam);
        if (e < eNOERROR) ERR(handle, e);

	mergedEntry = buddyEntry;
	mergedEntryNo = buddyEntryNo;

	/* release locks on pid and mergePid */
	if(lockup){
	    e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &pid, L_MANUAL);
	    if (e < eNOERROR) ERR(handle, e);

	    e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &mergePid, L_MANUAL);
	    if (e < eNOERROR) ERR(handle, e);
	}
    }

    if (mergeFlag) dirPage_BCB->dirtyFlag = 1;

    assert(dirPage->hdr.nEntries != 0);

    return(eNOERROR);

} /* mlgf_MergeLeafPage() */


