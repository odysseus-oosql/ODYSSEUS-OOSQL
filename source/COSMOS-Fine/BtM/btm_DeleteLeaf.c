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
 * Module: btm_DeleteLeaf.c
 *
 * Exports
 *   Four btm_DeleteLeaf(Four, btm_TraversePath*, PageID*, KeyDesc*, KeyValue*,
 *                       ObjectID*, LockParameter*, LocalPool*, DeallocListElem*);
 * Returns:
 *   Error code
 *     some errors caused by function calls
 */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LOG.h"
#include "TM.h"
#include "RDsM.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Function Prototypes */
Four btm_DeleteObjectIdFromLeaf(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, PageID*, KeyDesc*,
				KeyValue*, ObjectID*, LogParameter_T*);


/*
 * Function: Four btm_DeleteLeaf( )
 *
 * Description:
 *   Delete from the searched Leaf an ObjectID oid whose key value is given by "kval".
 *
 *   CAUTION :: Calling routine must acquire the latch(es) of child_BCB
 *              (and parent_BCB, if exists).
 *
 *            When this routine returns ....
 *            CASE eNOERROR : after releasing these latches.
 *            CASE BTM_RETRAVERSE : after release these latches.
 *                                 and with the maybe correct half path.
 *            CASE error : without release any latch or any lock.
 *
 * Returns:
 *    some errors caused by fucntion calls
 */
Four btm_DeleteLeaf(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo *iinfo,      /* IN index information */ 
    FileID     *fid,            /* IN FileID */ 
    btm_TraversePath *path,	/* INOUT Btree traverse path stack */
    PageID *root,		/* IN root page of Btree */
    KeyDesc *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value of the deleted object */
    ObjectID *oid,		/* IN ObjectID of the deleted object */
    LockParameter *lockup,	/* IN request lock or not */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four retErrCode;		/* error code to return */
    PageID prevPid;		/* previous leaf page */
    PageID nextPid;		/* next leaf page */
    PageID ovPid;		/* PageID of overflow page */
    BtreeLeaf *apage;		/* pointer to buffer holding a leaf page */
    btm_LeafEntry *entry;	/* a Btree leaf entry */
    Lsn_T leaf_LSN;		/* log sequence number of a leaf page */
    Lsn_T parent_LSN;		/* log sequence number of a parent page */
    Buffer_ACC_CB *parent_BCB;	/* buffer control block for parent page */
    Buffer_ACC_CB *leaf_BCB;	/* buffer control block for leaf page */
    Buffer_ACC_CB *prev_BCB;	/* buffer control block for previous leaf page */
    Buffer_ACC_CB *next_BCB;	/* buffer control block for next leaf page */
    Boolean treeFlag = FALSE;   /* for error handling for tree latch */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_DeleteLeaf(handle, path=%P, root=%P, kdesc=%P, kval=%P, oid=%P, lockup=%P)",
	      path, root, kdesc, kval, oid, lockup));


    /* Get the buffer control block for the leaf. */
    e = btm_PopElemFromPath(handle, path, &leaf_BCB, &leaf_LSN);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the buffer control block for the parent. */
    if (btm_IsEmptyPath(handle, path)) {
	parent_BCB = NULL;
    } else {
	e = btm_ReadTopElemFromPath(handle, path, &parent_BCB, &parent_LSN);
  	if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
    }

    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

    if (apage->hdr.statusBits & BTM_SM_BIT) {
        /* this routine release the parent latch */
	e = btm_ResetStatusBitsLeaf(handle, path, kdesc, kval, leaf_BCB,
				    (parent_BCB ==  NULL) ? NULL:parent_BCB->latchPtr);
	if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

	if (e == BTM_RETRAVERSE) {
	    /* Release the holding resource and restart the search from the parent. */

	    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);


	    return(BTM_RETRAVERSE);
	}
    } else {
	/* Release the parent latch. */
	if (parent_BCB != NULL) {
	    e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
	}
    }

    /* lock the next object */

    if ( lockup ) {
	/* Get an commit duration X lock on the next object. */
	e = btm_GetNextObjectLock(handle, xactEntry, fid, path, leaf_BCB, kdesc, kval, oid, BTM_DELETE, &ovPid); 
	if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

	if (e == BTM_RETRAVERSE || e == BTM_NOTFOUND) {

	    retErrCode = e;

	    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);


	    return(retErrCode);
	}

	if (e == BTM_RELATCHED && apage->hdr.statusBits & BTM_SM_BIT) {
	    /* reset the status bits of leaf page */
	    e = btm_ResetStatusBitsLeaf(handle, path, kdesc, kval, leaf_BCB, NULL);
	    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

	    if (e == BTM_RETRAVERSE) {
		/* Release the holding resources and restart the search from the parent. */

		e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
		if (e < eNOERROR) ERR(handle, e);


		return(BTM_RETRAVERSE);
	    }
	}
    }				/* if lockup */

    entry = (btm_LeafEntry*)&apage->data[apage->slot[0]];

    /* Initialize these pointers for error handling */
    treeFlag = FALSE;
    prev_BCB = NULL;
    next_BCB = NULL;

    if (!(apage->hdr.type & ROOT) && apage->hdr.nSlots == 1 && entry->nObjects == 1) { /* the last object */

	/* Fix the sibling pages of the leaf page. */
	if (apage->hdr.prevPage != NIL) {
	    MAKE_PAGEID(prevPid, apage->hdr.pid.volNo, apage->hdr.prevPage);

	    e = BfM_getAndFixBuffer(handle, &prevPid, M_FREE, &prev_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);
	}

	if (apage->hdr.nextPage != NIL) {
	    MAKE_PAGEID(nextPid, apage->hdr.pid.volNo, apage->hdr.nextPage);

	    e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &next_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);
	}

	/* Request conditional X latch on the tree. */
	e = btm_GetTreeLatchInPath(handle, path, M_EXCLUSIVE, M_CONDITIONAL);
        if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

	if (e != eNOERROR) {
	    /* e == SHM_BUSYLATCH */

	    /* Unfix the sibling pages of the leaf page. */
	    if (next_BCB != NULL) {
		e = BfM_unfixBuffer(handle, next_BCB, PAGE_BUF);
                next_BCB = NULL;
                if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

	    }

	    if (prev_BCB != NULL) {
		e = BfM_unfixBuffer(handle, prev_BCB, PAGE_BUF);
                prev_BCB = NULL;
		if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
	    }

	    /* Remember the LSN of the leaf.*/
	    leaf_LSN = apage->hdr.lsn;

	    /* Release the holding latch. */
	    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

	    /* Request unconditional X latch on the tree. */
	    e = btm_GetTreeLatchInPath(handle, path, M_EXCLUSIVE, M_UNCONDITIONAL);
	    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);
            treeFlag = TRUE;

	    /* Relatch the leaf page. */
	    e = SHM_getLatch(handle, leaf_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	    if (e < eNOERROR) ERRB1TL(handle, e, leaf_BCB, PAGE_BUF, path);

	    if (!btm_IsCorrectLeaf(handle, apage, kdesc, kval, &iinfo->iid, &leaf_LSN)) {
		/* Release the latch and unfix the buffer. */
		e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1TL(handle, e, leaf_BCB, PAGE_BUF, path);

		e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
		if (e < eNOERROR) ERRTL(handle, e, path);

		e = btm_Search(handle, &iinfo->iid, root, kdesc, kval, BTM_DELETE, 0, path); 
		if (e < eNOERROR) ERRTL(handle, e, path);

		/* Get the buffer control block for the leaf. */
		e = btm_PopElemFromPath(handle, path, &leaf_BCB, &leaf_LSN);
		if (e < eNOERROR) ERRTL(handle, e, path);

		apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

		/* Get the buffer control block for the parent. */
		if (!btm_IsEmptyPath(handle, path)) {
		    e = btm_ReadTopElemFromPath(handle, path, &parent_BCB, &parent_LSN);
		    if (e < eNOERROR) ERRBL1TL(handle, e, leaf_BCB, PAGE_BUF, path);

		    e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
		    if (e < eNOERROR) ERRBL1TL(handle, e, leaf_BCB, PAGE_BUF, path);
		}
	    }

	    entry = (btm_LeafEntry*)&apage->data[apage->slot[0]];

	    if (apage->hdr.nSlots > 1 || entry->nObjects != 1) {
		/* There is at least two objects. */

		e = btm_ReleaseTreeLatchInPath(handle, path);
		if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

	    } else {
		/* Fix the sibling pages of the leaf page. */
		if (apage->hdr.prevPage != NIL) {
		    MAKE_PAGEID(prevPid, apage->hdr.pid.volNo, apage->hdr.prevPage);

		    e = BfM_getAndFixBuffer(handle, &prevPid, M_FREE, &prev_BCB, PAGE_BUF);
		    if (e < eNOERROR) ERRBL1TL(handle, e, leaf_BCB, PAGE_BUF, path);
		} else
		    prev_BCB = NULL;

		if (apage->hdr.nextPage != NIL) {
		    MAKE_PAGEID(nextPid, apage->hdr.pid.volNo, apage->hdr.nextPage);

		    e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &next_BCB, PAGE_BUF);
                    if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);
		} else
		    next_BCB = NULL;
	    }
	}

        treeFlag = TRUE;        /* for error handling */
    }


    /* Delete the object from the leaf page. */
    e = btm_DeleteObjectIdFromLeaf(handle, xactEntry, iinfo, leaf_BCB, (lockup) ? &ovPid:NULL, 
				   kdesc, kval, oid, logParam);
    if (e < 0 ) {
        if ( e == eNOTFOUND)
            ERRGOTO(handle, e, ErrorHandle);
        ERR(handle, e);                 /* Do Not Release Any Resource */
    }

    if ((apage->hdr.type & ROOT) || apage->hdr.nSlots != 0) { /* nonempty page */

	/* Release the holding latches. */
	e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e); 

	e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	/* Returns with success status. */
	return(eNOERROR);
    }
    /*
     * Begin new nested top action
     */
    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

    /* Delete the leaf page from the B+ tree. */
    /* This routine releases the latch of leaf_BCB */
    e = btm_DeleteLeafPage(handle, xactEntry, iinfo, leaf_BCB, prev_BCB, next_BCB, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Unfix the buffers for leaf pages. */
    if (next_BCB != NULL) {
	e = BfM_unfixBuffer(handle, next_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
    }

    if (prev_BCB != NULL) {
	e = BfM_unfixBuffer(handle, prev_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
    }

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /* Modify the parent page to remove the reference to the deleted page. */
    e = btm_DeletePage(handle, xactEntry, iinfo, path, root, kdesc, kval, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = btm_ReleaseTreeLatchInPath(handle, path);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * End the nested top action.
     */
    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);


  ErrorHandle:
    if ( prev_BCB != NULL )
        BfM_unfixBuffer(handle, prev_BCB, PAGE_BUF);
    if ( next_BCB != NULL )
        BfM_unfixBuffer(handle, next_BCB, PAGE_BUF);
    if ( treeFlag )
        btm_ReleaseTreeLatchInPath(handle, path);
    ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

}


/*
 * Function: btm_DeleteObjectIdFromLeaf()
 *
 * Description:
 *  Delete a given ObjectID from the leaf page.
 *
 * Returns:
 *    some errors caused by fucntion calls
 *
 */
Four btm_DeleteObjectIdFromLeaf(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo *iinfo,      /* IN index information */ 
    Buffer_ACC_CB *leaf_BCB,	/* INOUT buffer control block for leaf page */
    PageID *oid_ovPid,		/* INOUT PageID of  overflow page */
    KeyDesc *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value of the deleted object */
    ObjectID *oid,		/* IN ObjectID to be deleted */
    LogParameter_T *logParam)   /* IN log parameter */

{
    Four e;			/* error code */
    Four of;			/* # of ObjectIDs of an overflow page when less than 1/4 */
    Four slotNo;		/* slot no for the given key value */
    Four offset;		/* starting offset of array of ObjectIDs or overflow chain */
    Four elemNo;		/* element no for the given ObjectID in array of ObjectIDs */
    Four entryLen;		/* length of the entry */
    Four newEntryLen;           /* length of the new entry */
    Boolean found;		/* result of search */
    PageID ovPid;		/* PageID of overflow page */
    ObjectID *oidArray;		/* array of ObjectIDs */
    BtreeLeaf *apage;		/* a Btree leaf page */
    BtreeOverflow *opage;	/* a Btree overflow page */
    btm_LeafEntry *entry;	/* a leaf entry */
    Buffer_ACC_CB *ov_BCB;	/* buffer access control block for overflow page */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* information for a log record */
    LOG_Image_BtM_OidInLeafEntry_T deleteOidInfo;
    Two deletedSlotNo;          /* slot containing the deleted object for logging */
    LOG_Image_BtM_ChangeLeafEntry_T changeLeafEntryInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_DeleteObjectIdFromLeaf(leaf_BCB=%P, oid_ovPid=%P, kdesc=%P, kval=%P, oid=%P)",
	      leaf_BCB, oid_ovPid, kdesc, kval, oid));


    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

    /* Search the leaf page for the key value. */
    found = btm_BinarySearchLeaf(handle, apage, kdesc, kval, &slotNo);

    /*
     * Notice: We have not yet checked whether the key exists.
     * Yes. With locking, we had checked the existence.
     */
    if (!found) ERR(handle, eNOTFOUND);

    entry = (btm_LeafEntry*)&apage->data[apage->slot[-slotNo]];
    entryLen = BTM_LEAF_ENTRY_LENGTH(entry->klen, entry->nObjects);

    offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED + entry->klen) - BTM_LEAFENTRY_FIXED;

    if (entry->nObjects > 0) {	/* normal entry */
	oidArray = (ObjectID*)&entry->kval[offset];

	found = btm_BinarySearchOidArray(handle, oidArray, oid, entry->nObjects, &elemNo);

        /*
         * Notice: We have not yet checked whether the object exists.
         * Yes. With locking, we had checked its existence.
         */
	if (!found) ERR(handle, eNOTFOUND);

	if (entry->nObjects == 1) {
            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                Two deletedSlotNo = slotNo;

                if (logParam->logFlag & LOG_FLAG_UNDO) {
                    LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
                                          LOG_ACTION_BTM_DELETE_OID_WITH_LEAF_ENTRY, LOG_REDO_ONLY,
                                          apage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                          sizeof(Two), &deletedSlotNo);
                } else {
		    LOG_Image_BtM_IndexInfo_T logImage;

		    /* set logImage */
		    logImage.iid = iinfo->iid;
		    logImage.catEntry = iinfo->catalog.oid;

                    LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_BTM_DELETE_OID_WITH_LEAF_ENTRY, LOG_REDO_UNDO,
                                          apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(Two), &deletedSlotNo,
                                          entryLen, entry,
                                          sizeof(LOG_Image_BtM_IndexInfo_T), &logImage,
                                          KEYDESC_USED_SIZE(kdesc), kdesc);
                }

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERR(handle, e);

                apage->hdr.lsn = lsn;
                apage->hdr.logRecLen = logRecLen;
            } else {
                INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
            }

	    /* Delete the slot. */
            BTM_DELETE_SLOTS_IN_BTREE_PAGE(apage, slotNo, 1);

	    apage->hdr.nSlots--;	/* decrement the number of slots */

	    /* Free the space: entry space */
	    apage->hdr.unused += entryLen;

	} else {

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                deleteOidInfo.slotNo = slotNo;
                deleteOidInfo.oidArrayElemNo = elemNo;
                deleteOidInfo.oid = *oid;

                if (logParam->logFlag & LOG_FLAG_UNDO) {
                    LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
                                          LOG_ACTION_BTM_DELETE_OID_FROM_LEAF_ENTRY, LOG_REDO_ONLY,
                                          apage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                          sizeof(LOG_Image_BtM_OidInLeafEntry_T), &deleteOidInfo);
                } else {
                    LOG_Image_BtM_IndexInfo_T logImage;

                    /* set logImage */
                    logImage.iid = iinfo->iid;
                    logImage.catEntry = iinfo->catalog.oid;

                    LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_BTM_DELETE_OID_FROM_LEAF_ENTRY, LOG_REDO_UNDO,
                                          apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(LOG_Image_BtM_OidInLeafEntry_T), &deleteOidInfo,
                                          sizeof(LOG_Image_BtM_IndexInfo_T), &logImage,
                                          KEYDESC_USED_SIZE(kdesc), kdesc,
                                          kval->len, kval->val);
                }

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERR(handle, e);

                apage->hdr.lsn = lsn;
                apage->hdr.logRecLen = logRecLen;
            } else {
                INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
            }

	    /* Delete the ObjectID. */
            BTM_DELETE_OIDS_SPACE_FROM_OID_ARRAY(oidArray, entry->nObjects, elemNo, 1);

	    entry->nObjects--;
	    apage->hdr.unused += OBJECTID_SIZE;
	}

        /* Set SM_Delete Bit only if the tree latch is not held */
        if ((apage->hdr.type & ROOT) || apage->hdr.nSlots != 0)
            apage->hdr.statusBits |= BTM_DELETE_BIT;

	/* Set the dirty flag of the given page. */
	leaf_BCB->dirtyFlag = 1;

    } else {			/* overflow page */

	if (oid_ovPid == NULL) {
	    offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) - BTM_LEAFENTRY_FIXED;

	    /* Get the PageID of the overflow page. */
	    MAKE_PAGEID(ovPid, apage->hdr.pid.volNo, *((ShortPageID*)&entry->kval[offset]));

	    oid_ovPid = &ovPid;
	}

	e = btm_DeleteOverflow(handle, xactEntry, iinfo, oid_ovPid, kdesc, kval, oid, &of, logParam); 
	if (e < eNOERROR) ERR(handle, e);

        newEntryLen = BTM_LEAF_ENTRY_LENGTH(entry->klen, of);
        if (newEntryLen < OVERFLOW_MERGE) { /* overflow page underflow was occured */
            if (newEntryLen - entryLen <= BL_FREE(apage)) {

                /*
                 * Begin new nested top action
                 */
                e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
                if (e < eNOERROR) ERR(handle, e);

                e = BfM_getAndFixBuffer(handle, oid_ovPid, M_EXCLUSIVE, &ov_BCB, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

                opage = (BtreeOverflow*)ov_BCB->bufPagePtr;

                if (of == 0) { /* Delete the leaf entry. */
                    /* Move slots to delete the slot */
                    BTM_DELETE_SLOTS_IN_BTREE_PAGE(apage, slotNo, 1);

                    /* free the space and decrement the # of entries by 1 */
                    apage->hdr.unused += entryLen;
                    apage->hdr.nSlots --;

                    /*
                     * Write log record.
                     */
                    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                        /*
                         * Because this operation is a part of SMO,
                         * the logical undo cannot be happened.
                         * So we are not to log the logical undo information.
                         */
                        deletedSlotNo = slotNo;

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_BTM_DELETE_LEAF_ENTRY, LOG_REDO_UNDO,
                                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(Two), &deletedSlotNo,
                                              entryLen, entry);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) ERRBL1(handle, e, ov_BCB, PAGE_BUF);

                        apage->hdr.lsn = lsn;
                        apage->hdr.logRecLen = logRecLen;
                    } else {
                        INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
                    }

                } else {    /* of > 0 */
                    btm_ChangeLeafEntrySize(handle, apage, slotNo, newEntryLen);

                    entry = (btm_LeafEntry*)&apage->data[apage->slot[-slotNo]];

                    /*
                     * logging for the leaf page
                     */
                    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                        changeLeafEntryInfo.slotNo = slotNo;
                        changeLeafEntryInfo.deltaOfObjectArrayAreaSize = of * OBJECTID_SIZE - sizeof(PageNo);
                        changeLeafEntryInfo.deltaInNumOfObjects = of - entry->nObjects;

                        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_BTM_CHANGE_LEAF_ENTRY, LOG_REDO_UNDO,
                                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_BtM_ChangeLeafEntry_T), &changeLeafEntryInfo,
                                              of*OBJECTID_SIZE, opage->oid,
                                              sizeof(PageNo), &opage->hdr.pid.pageNo);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

                        apage->hdr.lsn = lsn;
                        apage->hdr.logRecLen = logRecLen;
                    } else {
                        INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
                    }

                    /* Copy the ObjectIDs of the overflow page to leaf page. */
                    memcpy(&entry->kval[offset], opage->oid, OBJECTID_SIZE*of);

                    entry->nObjects = of;
                }


                /*
                 * logging for the overflow page
                 */
                if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                    LOG_Image_BtM_IndexInfo_T logImage;

                    /* set logImage */
                    logImage.iid = iinfo->iid;
                    logImage.catEntry = iinfo->catalog.oid;

                    LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_BTM_MODIFY_INDEXID_OF_BTREE_PAGE, LOG_REDO_UNDO,
                                          opage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(IndexID), &(common_perThreadDSptr->nilIid),
                                          sizeof(LOG_Image_BtM_IndexInfo_T), &logImage);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

                    opage->hdr.lsn = lsn;
                    opage->hdr.logRecLen = logRecLen;
                } else {
                    INCREASE_LSN_BY_ONE(opage->hdr.lsn);
                }

                SET_NILINDEXID(opage->hdr.iid);
                ov_BCB->dirtyFlag = 1;


                /* Release latch and unfix buffer. */
                e = SHM_releaseLatch(handle, ov_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRB1(handle, e, ov_BCB, PAGE_BUF);

                e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

                e = RDsM_FreeTrain(handle, xactEntry, oid_ovPid, PAGESIZE2, FALSE, logParam);
                if (e < eNOERROR) ERR(handle, e);

                e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
                if (e < eNOERROR) ERR(handle, e);

                /* Set the dirty flag of the given page. */
                leaf_BCB->dirtyFlag = 1;
            }
	}
    }

    return(eNOERROR);

} /* btm_DeleteObjectIdFromLeaf() */
