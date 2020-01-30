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
 * Module: btm_DeletePage.c
 *
 * Description:
 *  When keys are deleted from a page, the page may become empty at which
 *  point the page is deleted from the tree and the parent is modified to
 *  remove the reference to the deleted page. A result of such a change may
 *  be that the parent page becomes a empty. Hence, the page deletion operation
 *  may have to be propagated recursively up the tree.
 *
 * Exports:
 *   Four btm_DeletePage(Four, btm_TraversePath*, PageID*, KeyDesc*,
 *                       KeyValue*, LocalPool*, DeallocListElem*)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four btm_DeleteRootPage(Four, XactTableEntry_T*, Buffer_ACC_CB*, LogParameter_T*);
Four btm_DeleteInternalPage(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, LogParameter_T*); 
Four btm_DeleteInternal(Four, XactTableEntry_T*, Buffer_ACC_CB*, Four, LogParameter_T*);



/*
 * Function: Four btm_DeletePage(Four, btm_TraversePath*, PageID*, KeyDesc*,
 *                               KeyValue*, LocalPool*, DeallocListElem*)
 *
 * Description:
 *  Delete an entry from the internal on the top of the traverse path stack.
 *  If the page becomes empty page, then update its parent page. The update
 *  may be propagated up to the root page.
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 */
Four btm_DeletePage(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo *iinfo,	/* IN index information */ 
    btm_TraversePath *path,	/* INOUT the traverse path stack */
    PageID *root,		/* IN root page of Btree */
    KeyDesc *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value of the deleted key */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four slotNo;		/* slot no of an entry in Btree node */
    Four height;		/* height of a node */
    Boolean done;		/* flag to indicate whether SMO ends or not */
    BtreeInternal *origPage;	/* original Btree Page */
    Lsn_T orig_LSN;		/* LSN of the original page */
    Buffer_ACC_CB *orig_BCB;	/* buffer control block of original leaf page */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_DeletePage(handle, path=%P, root=%P, kdesc=%P, kval=%P)",
	      path, root, kdesc, kval));


    for (done = FALSE, height = 1; !done; ) {

	e = btm_PopElemFromPath(handle, path, &orig_BCB, &orig_LSN);
	if (e < eNOERROR) ERR(handle, e);

	/* Request an unconditional X latch on the original page. */
	e = SHM_getLatch(handle, orig_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	if (e < eNOERROR) ERRB1(handle, e, orig_BCB, PAGE_BUF);

	origPage = (BtreeInternal*)orig_BCB->bufPagePtr;

	if (!btm_IsCorrectInternal(handle, origPage, kdesc, kval, &iinfo->iid, &orig_LSN)) { 

            /* Release the holding latches and restart the search from the parent. */
            e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, orig_BCB, PAGE_BUF);

            e = BfM_unfixBuffer(handle, orig_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            e = btm_Search(handle, &iinfo->iid, root, kdesc, kval, BTM_DELETE, height, path); 
            if (e < eNOERROR) ERR(handle, e);

            e = btm_PopElemFromPath(handle, path, &orig_BCB, &orig_LSN);
            if (e < eNOERROR) ERR(handle, e);

            origPage = (BtreeInternal*)orig_BCB->bufPagePtr; 
	}

	btm_BinarySearchInternal(handle, origPage, kdesc, kval, &slotNo);
        if (e < eNOERROR) ERRBL1(handle, e, orig_BCB, PAGE_BUF);

	e = btm_DeleteInternal(handle, xactEntry, orig_BCB, slotNo, logParam);
        if (e < eNOERROR) ERR(handle, e); 

	if (e == eNOERROR) {	/* The page is nonempty page. */

	    done = TRUE;

	} else {		/* e == BTM_EMPTYPAGE */
	    if (origPage->hdr.type & ROOT) { /* root page is empty */
		e = btm_DeleteRootPage(handle, xactEntry, orig_BCB, logParam);
		if (e < eNOERROR) ERR(handle, e);

		done = TRUE;

	    } else { /* The normal internal page is empty. */

		e = btm_DeleteInternalPage(handle, xactEntry, iinfo, orig_BCB, logParam);
		if (e < eNOERROR) ERR(handle, e);

		height++;
	    }
	}

	e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);

	e = BfM_unfixBuffer(handle, orig_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* btm_DeletePage() */



/*
 * Function: Four btm_DeleteRootPage(Buffer_ACC_CB*)
 *
 * Description:
 *  Reinitialize the root page.
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 */
Four btm_DeleteRootPage(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Buffer_ACC_CB *root_BCB,	/* INOUT buffer control block of root page */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error code */
    BtreePage *rootPage;	/* Btree root page */
    IndexID iid;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* information for a log record */
    LOG_Image_BtM_InitLeafPage_T initLeafPageInfo;
    LOG_Image_BtM_InitInternalPage_T initInternalPageInfo;
    LOG_Image_BtM_InitLeafPage_UpdateType_T update_type;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1, ("btm_DeleteRootPage(root_BCB=%P)", root_BCB));


    rootPage = (BtreePage*)root_BCB->bufPagePtr;
    iid = rootPage->any.hdr.iid;

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        initLeafPageInfo.iid = iid;
        initLeafPageInfo.rootFlag = TRUE;
        initLeafPageInfo.prevPage = NIL;
        initLeafPageInfo.nextPage = NIL;

        update_type = BTREE_INIT_LEAF_UPDATE_TYPE_ROOT_DELETE;

        initInternalPageInfo.iid = iid;
        initInternalPageInfo.rootFlag = FALSE;
        initInternalPageInfo.p0 = rootPage->bi.hdr.p0;
        initInternalPageInfo.height = rootPage->bi.hdr.height;

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INIT_LEAF_PAGE, LOG_REDO_UNDO,
                              rootPage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_InitLeafPage_T), &initLeafPageInfo,
                              sizeof(LOG_Image_BtM_InitLeafPage_UpdateType_T), &update_type,
                              sizeof(LOG_Image_BtM_InitInternalPage_T), &initInternalPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        rootPage->any.hdr.lsn = lsn;
        rootPage->any.hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(rootPage->any.hdr.lsn); 
    }

    BTM_INIT_LEAF_PAGE(&rootPage->bl, iid, root_BCB->key, NIL, NIL, TRUE);

    /* The SM_Bit is set to '1' on the deleted page. */
    rootPage->any.hdr.statusBits |= BTM_SM_BIT;

    /* Set the dirty flag to 1. */
    root_BCB->dirtyFlag = 1;

    return(eNOERROR);

} /* btm_DeleteRootPage() */



/*
 * Module: Four btm_DeleteInternalPage(Four, Buffer_ACC_CB*, LocalPool*, DeallocListElem*)
 *
 * Description:
 *  Delete an internal page from a B+ tree. The page deallocation should be
 *  deffered until the SMO is completed. We insert the page into the deallocation
 *  list so that the page is deallocated when the transaction commits.
 *
 * Returns:
 *  Error code
 *    some erros caused by function calls
 */
Four btm_DeleteInternalPage(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo* iinfo,	/* IN index information */ 
    Buffer_ACC_CB *orig_BCB,	/* INOUT buffer control block of deleted page */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error code */
    BtreeInternal *origPage;	/* internal page to be deleted */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* information for a log record */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1, ("btm_DeleteInternalPage(orig_BCB=%P)", orig_BCB));


    origPage = (BtreeInternal*)orig_BCB->bufPagePtr;

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        LOG_Image_BtM_IndexInfo_T logImage;

        /* set logImage */
        logImage.iid = iinfo->iid;
        logImage.catEntry = iinfo->catalog.oid;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_MODIFY_INDEXID_OF_BTREE_PAGE, LOG_REDO_UNDO,
                              origPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(IndexID), &(common_perThreadDSptr->nilIid),
                              sizeof(LOG_Image_BtM_IndexInfo_T), &logImage);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        origPage->hdr.lsn = lsn;
        origPage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(origPage->hdr.lsn); 
    }

    /* Set IndexID to NIL IndexID. */
    origPage->hdr.iid.serial = NIL; 

    /* Set the SM_Bit to 1. */
    origPage->hdr.statusBits |= BTM_SM_BIT;

    /* Set the dirty flag to 1. */
    orig_BCB->dirtyFlag = 1;

    e = RDsM_FreeTrain(handle, xactEntry, &orig_BCB->key, PAGESIZE2, FALSE, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_DeleteInternalPage() */



/*
 * Module: Four btm_DeleteLeafPage(Four, Buffer_ACC_CB*, BUffer_ACC_CB*,
 *                                 Buffer_ACC_CB*, LocalPool*, DeallocListElem*)
 *
 * Description:
 *  Delete a leaf page from a B+ tree. The page deallocation should be deffered
 *  until the transaction commits. So the deallocated page is inseted into the
 *  deallocated page list.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_DeleteLeafPage(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo* iinfo,	/* IN index information */ 
    Buffer_ACC_CB *orig_BCB,	/* INOUT buffer control block of deleted page */
    Buffer_ACC_CB *prev_BCB,	/* INOUT buffer control block of previous page */
    Buffer_ACC_CB *next_BCB,	/* INOUT buffer control block of next page */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    BtreeLeaf *origPage;	/* leaf page to be deleted */
    BtreeLeaf *prevPage;	/* previous page of original leaf page */
    BtreeLeaf *nextPage;	/* next page of original leaf page */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* information for a log record */
    ShortPageID nil = NIL;      /* temporary variable for logging NIL value */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_DeleteLeafPage(handle, orig_BCB=%P, prev_BCB=%P, next_BCB=%P)",
	      orig_BCB, prev_BCB, next_BCB));


    origPage = (BtreeLeaf*)orig_BCB->bufPagePtr;

    /* root page will be deleted by btm_DeletePage */
    if (origPage->hdr.type & ROOT) { /* root page */
	e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        LOG_Image_BtM_IndexInfo_T logImage;

        /* set logImage */
        logImage.iid = iinfo->iid;
        logImage.catEntry = iinfo->catalog.oid;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_MODIFY_INDEXID_OF_BTREE_PAGE, LOG_REDO_UNDO,
                              origPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(IndexID), &(common_perThreadDSptr->nilIid),
                              sizeof(LOG_Image_BtM_IndexInfo_T), &logImage);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        origPage->hdr.lsn = lsn;
        origPage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(origPage->hdr.lsn); 
    }

    /* Set the IndexID to NIL IndexID. */
    origPage->hdr.iid.serial = NIL; 

    /* The SM_Bit is set to '1' on the deleted page. */
    origPage->hdr.statusBits |= BTM_SM_BIT;

    /* Set the dirty flag of the original page. */
    orig_BCB->dirtyFlag = 1;

    /* The page is deallocated. */
    e = RDsM_FreeTrain(handle, xactEntry, &orig_BCB->key, PAGESIZE2, FALSE, logParam);
    if (e < eNOERROR) ERR(handle, e);       

    /* The successor leaf page is X latched and its PrevPage pointer is */
    /* updated to point to the predecessor of the deleted page. */
    if (next_BCB != NULL) {

	e = SHM_getLatch(handle, next_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e); 

	nextPage = (BtreeLeaf*)next_BCB->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_BTM_MODIFY_LEAF_CHAIN_PREV_LINK, LOG_REDO_UNDO,
                                  nextPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &origPage->hdr.prevPage,
                                  sizeof(ShortPageID), &nextPage->hdr.prevPage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e); 

            nextPage->hdr.lsn = lsn;
            nextPage->hdr.logRecLen = logRecLen;
        } else {
            INCREASE_LSN_BY_ONE(nextPage->hdr.lsn);
        }

	nextPage->hdr.prevPage = origPage->hdr.prevPage;

	next_BCB->dirtyFlag = 1;

	e = SHM_releaseLatch(handle, next_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e); 
    }

    /* Release the original page. */
    e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e); 

    if (prev_BCB != NULL) {
	e = SHM_getLatch(handle, prev_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e); 

	prevPage = (BtreeLeaf*)prev_BCB->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_BTM_MODIFY_LEAF_CHAIN_NEXT_LINK, LOG_REDO_UNDO,
                                  prevPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), ((next_BCB == NULL) ? &nil : &next_BCB->key.pageNo),
                                  sizeof(ShortPageID), &prevPage->hdr.nextPage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            prevPage->hdr.lsn = lsn;
            prevPage->hdr.logRecLen = logRecLen;
        } else {
            INCREASE_LSN_BY_ONE(prevPage->hdr.lsn); 
        }

	prevPage->hdr.nextPage = (next_BCB == NULL) ? NIL:next_BCB->key.pageNo;

	prev_BCB->dirtyFlag = 1;

	e = SHM_releaseLatch(handle, prev_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e); 
    }

    return(eNOERROR);

} /* btm_DeleteLeafPage() */



/*
 * Function: Four btm_DeleteInternal(Four, Buffer_ACC_CB*, Four)
 *
 * Description:
 *  Delete an entry from an internal page.
 *
 * Returns:
 *  Error codes if return value is less than 0
 *    some erros caused by function calls
 *  Status otherwise
 *    BTM_EMPTYROOTPAGE
 *    BTM_EMPTYPAGE
 */
Four btm_DeleteInternal(
    Four		handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Buffer_ACC_CB *bcb,		/* IN buffer control block for updated page */
    Four slotNo,		/* IN slot to be deleted */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four entryLen;		/* length of the new entry */
    BtreeInternal *apage;	/* a Btree internal page */
    btm_InternalEntry *entry;	/* an internal entry */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* information for a log record */
    Four e;                     /* returned error code */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BTM, TR1, ("btm_DeleteInternal(bcb=%P, slotNo=%ld)", bcb, slotNo));


    apage = (BtreeInternal*)bcb->bufPagePtr;

    assert(apage->hdr.nSlots >= 0);

    if (apage->hdr.nSlots == 0) return(BTM_EMPTYPAGE);

    /* When the first child is deleted. */
    if (slotNo == -1) {

	entry = (btm_InternalEntry*)&apage->data[apage->slot[0]];

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            Two tmpSlotNo = slotNo; /* for type Two variable */

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_BTM_MODIFY_FIRST_CHILD_OF_INTERNAL_PAGE, LOG_REDO_UNDO,
                                  apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &entry->spid,
                                  sizeof(ShortPageID), &apage->hdr.p0);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e); 
        }

	apage->hdr.p0 = entry->spid;

	slotNo = 0;
    }

    /* length of the deleted entry */
    entry = (btm_InternalEntry*)&apage->data[apage->slot[-slotNo]];
    entryLen = BTM_INTERNAL_ENTRY_LENGTH(entry->klen);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        Two tmpSlotNo = slotNo; /* for type Two variable */

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_DELETE_INTERNAL_ENTRY, LOG_REDO_UNDO,
                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Two), &tmpSlotNo,
                              entryLen, entry);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        apage->hdr.lsn = lsn;
        apage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
    }

    apage->hdr.unused += entryLen;

    /* Move slots to delete the slot */
    BTM_DELETE_SLOTS_IN_BTREE_PAGE(apage, slotNo, 1);

    /* Decrease the number of slots by 1. */
    apage->hdr.nSlots--;

    /* Set the dirty flag of the updated page. */
    bcb->dirtyFlag = 1;

    return(eNOERROR);

} /* btm_DeleteInternal() */

