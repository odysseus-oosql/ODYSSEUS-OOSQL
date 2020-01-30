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
 * Module: btm_Split.c
 *
 * Description:
 *  Split the leaf page on the top of the traverse path stack 'path'.
 *  As new keys are added to a page, at some point, the page will become full.
 *  At that time, a page split operation is performed which results in some of
 *  the entries of that page being moved to a new page and the parent of the
 *  original page getting modified to include information about the keys in
 *  the original and information about the new page. Such changes to the parent
 *  page itself may cause the parent page to be split. This process may have
 *  to be repeated recursively.
 *
 * Exports:
 *  Four btm_Split(Four, btm_TraversePath*, PageID*, KeyDesc*, KeyValue*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Function Prototypes */
Four btm_SplitInternal(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, Four, btm_InternalEntry*, btm_InternalEntry*, LogParameter_T*); 
Four btm_SplitRoot(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, KeyDesc*, btm_InternalEntry*, LogParameter_T*); 
Four btm_SplitLeaf(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, Buffer_ACC_CB*, btm_InternalEntry*, LogParameter_T*);
Four btm_InsertInternal(Four, XactTableEntry_T*, Buffer_ACC_CB*, Four, btm_InternalEntry*, LogParameter_T*);
Four btm_RequestExclusiveTreeLatch(Four, btm_TraversePath *,Buffer_ACC_CB*);


/*
 * Function: Four btm_Split(Four, btm_TraversePath*, PageID*, KeyDesc*, KeyValue*)
 *
 * Description:
 *  Split a leaf page on the top of traverse path stack 'path' and reflect
 *  the child's overflow to the parent page. Propagate recursively the split
 *  to the root until the parent has enough space.
 *
 * Returns:
 *  1) error codes if return value is less than 0.
 *    some errors caused by function calls
 *  2) execution status otherwise
 *    BTM_RETRAVERSE
 *
 * Notice:
 *  1) We assume that we acquired only a latch on the leaf page.
 *  2) ErrorHandle labels are used for error handling of
 *     next leaf page(next_BCB)
 */
Four btm_Split(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    btm_TraversePath *path,	/* INOUT Btree traverse path stack */
    BtreeIndexInfo  *iinfo,	/* IN Btree index info */ 
    PageID   *root,		/* IN Root Page of Btree */
    KeyDesc  *kdesc,		/* IN Btree Key Descriptor */
    KeyValue *kval,		/* IN key value to be inseted */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error code */
    Four height;		/* height of Btree nodes */
    Four slotNo;		/* slot no of a Btree entry */
    PageID nextPid;		/* PageID of the next leaf page */
    Boolean done;		/* a variable */
    BtreePage *origPage;	/* original btree leaf page */
    Lsn_T orig_LSN;		/* LSN of the original leaf page */
    Buffer_ACC_CB *orig_BCB;	/* buffer control block for original leaf page */
    Buffer_ACC_CB *next_BCB;	/* a buffer control block of next leaf page */

    btm_InternalEntry overflowedEntry; /* a Btree internal entry resulting from overflow */
    btm_InternalEntry insertedEntry;   /* a Btree internal entry to insert */
    IndexID  *iid;		/* index ID */ 


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_Split(handle, path=%P, root=%P, kdesc=%P, kval=%P)",
	      path, root, kdesc, kval));


    /* set iid from Btree Index Info */
    iid = &iinfo->iid;

    /* Get the original page. */
    e = btm_PopElemFromPath(handle, path, &orig_BCB, &orig_LSN);
    if (e < eNOERROR) ERR(handle, e);

    origPage = (BtreePage*)orig_BCB->bufPagePtr;

    /* Get and fix the next page of the leaf page. */
    /* Minimize the time during holding which the X latch is held on the */
    /* tree in order to reduce the interference with other transactions that */
    /* might need the tree latch in the S or X mode. */
    if (origPage->bl.hdr.nextPage != NIL) {
	MAKE_PAGEID(nextPid, origPage->bl.hdr.pid.volNo, origPage->bl.hdr.nextPage);

	e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &next_BCB, PAGE_BUF);
	if (e < eNOERROR) ERRBL1(handle, e, orig_BCB, PAGE_BUF);
    } else {
	next_BCB = NULL;
    }

    e = btm_RequestExclusiveTreeLatch(handle, path, orig_BCB);
    if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandleB1);

    if (e == BTM_RETRAVERSE) {

        if (next_BCB != NULL) {
            e = BfM_unfixBuffer(handle, next_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRB1(handle, e, orig_BCB, PAGE_BUF);
        }

        e = BfM_unfixBuffer(handle, orig_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);


        return(BTM_RETRAVERSE);
    }

    /* The original leaf has not changed or a new leaf is located and */
    /* a split is still necessary. */

    /*
     * Begin new nested top action
     */
    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandleBL1TL);

    for (done = FALSE, height = 0; !done; ) {

	if (height == 0) {	/* leaf page */
	    /* Is the current page is root page? */
	    if (origPage->any.hdr.type & ROOT) {
		e = btm_SplitRoot(handle, xactEntry, iinfo, orig_BCB, kdesc, &overflowedEntry, logParam); 
		if (e < eNOERROR) ERR(handle, e); 

		done = TRUE;

	    } else {
		e = btm_SplitLeaf(handle, xactEntry, iinfo, orig_BCB, next_BCB, &overflowedEntry, logParam);
		if (e < eNOERROR) ERR(handle, e); 
	    }

	    /* Unfix the buffer for the next leaf page of the original page. */
	    if (next_BCB != NULL) {
		e = BfM_unfixBuffer(handle, next_BCB, PAGE_BUF);
		if (e < eNOERROR) ERR(handle, e); 
	    }

	} else {		/* internal page */
	    btm_BinarySearchInternal(handle, &origPage->bi, kdesc,
                                     (KeyValue*)&overflowedEntry.klen, &slotNo);

	    e = btm_InsertInternal(handle, xactEntry, orig_BCB, slotNo+1, &overflowedEntry, logParam);
            if (e < eNOERROR) ERR(handle, e); 

	    if (e == eNOERROR) {

		done = TRUE;

	    } else if (e == BTM_NOSPACE) {	/* 'split' is required. */

		/* Is the current page is root page? */
		if (origPage->any.hdr.type & ROOT) {
		    e = btm_SplitRoot(handle, xactEntry, iinfo, orig_BCB, kdesc, &overflowedEntry, logParam); 
                    if (e < eNOERROR) ERR(handle, e); 

		    done =TRUE;
		} else {
		    insertedEntry = overflowedEntry;

		    e = btm_SplitInternal(handle, xactEntry, iinfo, orig_BCB, slotNo+1, &insertedEntry, &overflowedEntry, logParam); 
                    if (e < eNOERROR) ERR(handle, e); 
		}
	    }
	}

	/* Release the latch and unfix the buffer. */
	e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e); 

	e = BfM_unfixBuffer(handle, orig_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e); 

	if (done) {

            /*
             * End the nested top action.
             */
            e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
            if (e < eNOERROR) ERR(handle, e); 

	    e = btm_ReleaseTreeLatchInPath(handle, path);
	    if (e < eNOERROR) ERR(handle, e);

	    return(eNOERROR);
	}

	height++;

	if (!btm_IsEmptyPath(handle, path)) {
	    e = btm_PopElemFromPath(handle, path, &orig_BCB, &orig_LSN);
            if (e < eNOERROR) ERR(handle, e); 

	    /* Request an unconditional X latch on the original page. */
	    e = SHM_getLatch(handle, orig_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERR(handle, e); 

	    origPage = (BtreePage*)orig_BCB->bufPagePtr;

	    if (LSN_CMP_EQ(origPage->any.hdr.lsn, orig_LSN)) continue;

	    e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERR(handle, e); 

	    e = BfM_unfixBuffer(handle, orig_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e); 
	}
        else
            ERRTL(handle, eTRAVERSEPATH_BTM, path);

	/* LSN has been changed. */
        /* the latch of the top element of path is acquired from btm_Search */
	e = btm_Search(handle, iid, root, kdesc, kval, BTM_INSERT, height, path); 
        if (e < eNOERROR) ERR(handle, e); 

        /* orig_BCB->latch is already acquired from btm_Search */
	e = btm_PopElemFromPath(handle, path, &orig_BCB, &orig_LSN);
        if (e < eNOERROR) ERR(handle, e); 
    }

    return(eNOERROR);

  ErrorHandleB1:
    if (next_BCB != NULL)
        ERRB2(handle, e, next_BCB, PAGE_BUF, orig_BCB, PAGE_BUF);
    else
        ERRB1(handle, e, orig_BCB, PAGE_BUF);

  ErrorHandleBL1TL:
    if (next_BCB != NULL)
        ERRB1BL1TL(handle, e, next_BCB, PAGE_BUF, orig_BCB, PAGE_BUF, path);
    else
        ERRBL1TL(handle, e, orig_BCB, PAGE_BUF, path);


} /* btm_Split() */



/*
 * Function: Four btm_SplitRoot(Four, Buffer_ACC_CB*, KeyDesc*, btm_InternalEntry*)
 *
 * Description:
 *
 * Returns:
 */
Four btm_SplitRoot(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo   *iinfo,	 /* IN index info */ 
    Buffer_ACC_CB *root_BCB,	/* INOUT buffer control block for root page */
    KeyDesc *kdesc,		/* IN key descriptor */
    btm_InternalEntry *entry,	/* IN entry to be inserted */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error code */
    Four slotNo;		/* slot where 'entry' to be placed */
    PageID newPid;		/* PageID of the newly allocated page */
    BtreePage *rootPage;	/* a Btree root page */
    BtreePage *newPage;		/* newly allocated page */
    Buffer_ACC_CB *new_BCB;	/* buffer control block of the newly allocated page */
    btm_InternalEntry overflowedEntry; /* entry resulting from overflow */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_InitInternalPage_T initInternalPageInfo;
    SegmentID_T pageSegmentID;  /* page segment ID */
    IndexID     *iid;           /* index id */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_SplitRoot(root_BCB=%P, kdesc=%P, entry=%P)", root_BCB, kdesc, entry));


    /* set iid from index info */
    iid = &iinfo->iid;

    rootPage = (BtreePage*)root_BCB->bufPagePtr;

    /* Allocate the new leaf page. */
    e = btm_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, rootPage->any.hdr.pid.volNo, &pageSegmentID,
			  &rootPage->any.hdr.pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* The new page is fixed in the buffer pool and is X latched. */
    e = BfM_fixNewBuffer(handle, &newPid, M_EXCLUSIVE, &new_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    newPage = (BtreePage*)new_BCB->bufPagePtr;

    /* Copy the root page to new page */
    *newPage = *rootPage;
    newPage->any.hdr.pid.pageNo = newPid.pageNo;
    newPage->any.hdr.type ^= ROOT;  /* Reset the ROOT bit flag of the new page. */

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_COPY_PAGE, LOG_REDO_ONLY,
                              newPage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(BtreePage), newPage);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        newPage->any.hdr.lsn = lsn;
        newPage->any.hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(newPage->any.hdr.lsn); 
    }

    /* Split the new page. */
    if (newPage->any.hdr.type & LEAF)
        e = btm_SplitLeaf(handle, xactEntry, iinfo, new_BCB, NULL, &overflowedEntry, logParam);
    else {
        btm_BinarySearchInternal(handle, &newPage->bi, kdesc, (KeyValue*)&entry->klen, &slotNo);

        e = btm_SplitInternal(handle, xactEntry, iinfo, new_BCB, slotNo+1, entry, &overflowedEntry, logParam); 
    }
    if (e < eNOERROR) ERR(handle, e); 

    /*@ set dirty flag and free the buffer holding the new page */
    new_BCB->dirtyFlag = 1;

    e = SHM_releaseLatch(handle, new_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e); 

    e = BfM_unfixBuffer(handle, new_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_COPY_PAGE, LOG_UNDO_ONLY,
                              rootPage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              0, NULL,
                              sizeof(BtreePage), rootPage);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        rootPage->any.hdr.lsn = lsn;
        rootPage->any.hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(rootPage->any.hdr.lsn); 
    }

    /* Initialize the root page. */
    BTM_INIT_INTERNAL_PAGE(&rootPage->bi, *iid, root_BCB->key, newPid.pageNo, rootPage->any.hdr.height+1, TRUE);

    /* Set the SM_Bit to 1 */
    rootPage->any.hdr.statusBits |= BTM_SM_BIT;

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        initInternalPageInfo.iid = *iid; 
        initInternalPageInfo.rootFlag = TRUE;
        initInternalPageInfo.p0 = rootPage->bi.hdr.p0;
        initInternalPageInfo.height = rootPage->bi.hdr.height; 

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INIT_INTERNAL_PAGE, LOG_REDO_ONLY,
                              rootPage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_InitInternalPage_T), &initInternalPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        rootPage->any.hdr.lsn = lsn;
        rootPage->any.hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(rootPage->any.hdr.lsn); 
    }

    /* Insert the overflowed entry into the root page. */
    e = btm_InsertInternal(handle, xactEntry, root_BCB, 0, &overflowedEntry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    root_BCB->dirtyFlag = 1;

    return(eNOERROR);

} /* btm_SplitRoot() */


/*
 * Function: Four btm_SplitInternal(Four, Buffer_ACC_CB*, Four,
 *                                  btm_InternalEntry*, btm_InternalEntry*)
 *
 * Description:
 *  Split the internal page and insert an overflowed entry resulted from the
 *  split of the child. Return a new overflow entry resulted from the
 *  split of the parent.
 *
 * Returns:
 *  Error codes
 *   some errors caused by function calls
 */
Four btm_SplitInternal(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo   *iinfo,	 /* IN index info */ 
    Buffer_ACC_CB *orig_BCB,	      /* IN buffer control block for original page */
    Four high,                  /* IN slot where new entry to place */
    btm_InternalEntry *insertedEntry, /* IN entry to be inserted */
    btm_InternalEntry *overflowEntry,  /* OUT entry to be returned */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four i;			/* slot no index including 'item' */
    Four j;			/* points to the slot from which the entries are moved into new page */
    Four k;			/* slot no index in new page */
    Four sum;			/* the size of a filled area */
    PageID newPid;		/* for a New Allocated Page */
    BtreeInternal *fpage;       /* the page which will be splitted */
    BtreeInternal *npage;	/* a page pointer for the new allocated page */
    Buffer_ACC_CB *nPage_BCBP;	/* buffer access control block containing data */
    Four insertedEntryLen;      /* length of the 'item' when it is inserted into internal page */
    btm_InternalEntry *parentEntry; /* an entry which will be propagated to the parent */
    Four nRemainedSlotsInOrigPage; /* # of slots remained in the original page */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_SpecifyEntries_T entriesInfo;
    LOG_Image_BtM_InitInternalPage_T initInternalPageInfo;
    IndexID* iid;		/* index ID */
    SegmentID_T pageSegmentID;  /* page segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1, ("btm_SplitInternal()"));


    /* set iid from index info */
    iid = &iinfo->iid;

    fpage = (BtreeInternal*)orig_BCB->bufPagePtr;

    /* Set the SM_Bit to 1. */
    fpage->hdr.statusBits |= BTM_SM_BIT;

    /*@ Allocate a new page and initialize it as an internal page */
    e = btm_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, fpage->hdr.pid.volNo, &pageSegmentID,
			 &fpage->hdr.pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* compact the internal page */
    /* The compaction will arrange the entries in order. */
    btm_CompactInternalPage(handle, fpage, NIL);

    /* Calculate the length of the newly inserted entry. */
    insertedEntryLen = BTM_INTERNAL_ENTRY_LENGTH(insertedEntry->klen);

    /* Get the split point. */
    /* At least two entries should be reserved for 'overflowEntry' and entries of new page. */
    for (sum = 0, i = 0; (i < fpage->hdr.nSlots - 1) && (sum < BI_HALF); i++) {

        if (i < high) {
            sum = fpage->slot[-(i+1)];
        } else {
            sum = fpage->slot[-i] + insertedEntryLen;
        }
        sum += sizeof(Two) * (i+1);
    }

    /*
     * Setting control variables parentEntry and nRemaindSlotsInOrigPage.
     * nRemainedSlotsInOrigPage: # of slots remained in original page after split (not including 'item')
     * parentEntry points to an entry which will be propagated to the parent.
     */
    if (i == high) {
        /* We handle the InternalItem as btm_InternalEntry. */
        /* So two their data structures should consider this situation. */
        nRemainedSlotsInOrigPage = i;
        parentEntry = insertedEntry;
    } else {
        nRemainedSlotsInOrigPage = (i > high) ? (i-1):i;
        parentEntry = (btm_InternalEntry*)&fpage->data[fpage->slot[-nRemainedSlotsInOrigPage]];
    }

    /*
     * construct 'overflowEntry'
     */
    overflowEntry->spid = newPid.pageNo;
    overflowEntry->klen = parentEntry->klen;
    memcpy(overflowEntry->kval, parentEntry->kval, overflowEntry->klen);


    e = BfM_fixNewBuffer(handle,  &newPid, M_EXCLUSIVE, &nPage_BCBP, PAGE_BUF );
    if (e < eNOERROR) ERR(handle, e);

    npage = (BtreeInternal *)nPage_BCBP->bufPagePtr;

    BTM_INIT_INTERNAL_PAGE(npage, fpage->hdr.iid, newPid, parentEntry->spid, fpage->hdr.height, FALSE);

    /* Set the SM_Bit to 1. */
    npage->hdr.statusBits |= BTM_SM_BIT;


    /*
     * [j,(fpage->nSlots-1)] entries of fpage are moved into npage.
     * j is the slot no from which the entries are moved into new page
     */
    j = (i == high) ? nRemainedSlotsInOrigPage:(nRemainedSlotsInOrigPage+1);

    npage->hdr.nSlots = fpage->hdr.nSlots - j;
    npage->hdr.free = fpage->hdr.free - fpage->slot[-j];

    memcpy(npage->data, &fpage->data[fpage->slot[-j]], npage->hdr.free);

    for (k = 0; k < npage->hdr.nSlots; k++)
        npage->slot[-k] = fpage->slot[-(j+k)] - fpage->slot[-j];


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        Four moveStartOffset = fpage->slot[-nRemainedSlotsInOrigPage];


        /*
         * original page
         */
        entriesInfo.startSlotNo = nRemainedSlotsInOrigPage;
        entriesInfo.nEntries = fpage->hdr.nSlots - nRemainedSlotsInOrigPage;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                              fpage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_SpecifyEntries_T), &entriesInfo,
                              fpage->hdr.free - moveStartOffset, &fpage->data[moveStartOffset]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        fpage->hdr.lsn = lsn;
        fpage->hdr.logRecLen = logRecLen;


        /*
         * new page
         */
        initInternalPageInfo.iid = *iid; 
        initInternalPageInfo.rootFlag = FALSE;
        initInternalPageInfo.p0 = npage->hdr.p0;
        initInternalPageInfo.height = npage->hdr.height; 

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INIT_INTERNAL_PAGE, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_InitInternalPage_T), &initInternalPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        entriesInfo.startSlotNo = 0;
        entriesInfo.nEntries = npage->hdr.nSlots;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INSERT_INTERNAL_ENTRIES, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_SpecifyEntries_T), &entriesInfo,
                              npage->hdr.free, npage->data);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        npage->hdr.lsn = lsn;
        npage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(fpage->hdr.lsn);
        INCREASE_LSN_BY_ONE(npage->hdr.lsn); 
    }


    fpage->hdr.nSlots = nRemainedSlotsInOrigPage;
    fpage->hdr.free = fpage->slot[-fpage->hdr.nSlots];


    /*
     * Insert the 'insertedEntry' entry if it was not propagated to the parent.
     */
    if (i > high) {
        /* insert the 'insertedEntry' into the original page. */
        e = btm_InsertInternal(handle, xactEntry, orig_BCB, high, insertedEntry, logParam);
    } else if (i < high) {
        /* insert the 'item' into the new page. */
        e = btm_InsertInternal(handle, xactEntry, nPage_BCBP, high-j, insertedEntry, logParam);
    }
    if (e < eNOERROR) ERR(handle, e); 

    nPage_BCBP->dirtyFlag = 1;

    e = SHM_releaseLatch(handle, nPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e); 

    e = BfM_unfixBuffer(handle, nPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    orig_BCB->dirtyFlag = 1; 

    return(eNOERROR);

} /* btm_SplitInternal() */



/*
 * Function: Four btm_SplitLeaf(Four, Buffer_ACC_CB*, btm_InternalEntry*)
 *
 * Description:
 *  Split a leaf page. Return an overflow entry resulting from the leaf page
 *  split.
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls.
 */
Four btm_SplitLeaf(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo   *iinfo,          /* IN    btree index info */
    Buffer_ACC_CB *orig_BCB,	      /* INOUT buffer control block for original page */
    Buffer_ACC_CB *next_BCB,	      /* INOUT buffer control block of next leaf page  */
    btm_InternalEntry *overflowEntry, /* OUT entry to be returned */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four i;			/* slot No. in the given page, fpage */
    Four j;			/* slot No. in the new page */
    Four maxLoop;		/* # of max loops; # of slots in fpage */
    Four sum;			/* the size of a filled area */
    PageID newPid;		/* for a New Allocated Page */
    PageID nextPid;		/* for maintaining doubly linked list */
    BtreeLeaf *fpage;		/* the page which will be splitted */
    BtreeLeaf *npage;		/* a page pointer for the new page */
    Buffer_ACC_CB *nPage_BCBP;	/* buffer access control block containing data */
    BtreeLeaf *mpage;		/* for doubly linked list */
    btm_LeafEntry *fEntry;	/* an entry in the given page, 'fpage' */
    btm_LeafEntry *nEntry;	/* an entry in the new page, 'npage' */
    Four nEntryOffset;		/* starting offset of 'nEntry' */
    Four entryLen;		/* entry length */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_InitLeafPage_T initLeafPageInfo;
    LOG_Image_BtM_SpecifyEntries_T entriesInfo;
    SegmentID_T pageSegmentID;  /* page segment ID */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1, ("btm_SplitLeaf()"));


    fpage = (BtreeLeaf*)orig_BCB->bufPagePtr;

    /*@ Allocate a new page and initialize it as a leaf page */
    e = btm_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, fpage->hdr.pid.volNo, &pageSegmentID,
			 &fpage->hdr.pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_fixNewBuffer(handle, &newPid, M_EXCLUSIVE, &nPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    npage = (BtreeLeaf *)nPage_BCBP->bufPagePtr;

    /* Initialize the new page to the leaf page. */
    BTM_INIT_LEAF_PAGE(npage, fpage->hdr.iid, newPid, fpage->hdr.pid.pageNo, fpage->hdr.nextPage, FALSE);

    /* Set the SM_Bit to 1. */
    fpage->hdr.statusBits |= BTM_SM_BIT;
    npage->hdr.statusBits |= BTM_SM_BIT;

    /* loop until 'sum' becomes greater than BL_HALF */
    /* i : slot no of fpage */
    maxLoop = fpage->hdr.nSlots;
    for (sum = 0, i = 0; i < maxLoop && sum < BL_HALF; i++) {

        fEntry = (btm_LeafEntry*)&(fpage->data[fpage->slot[-i]]);
        sum += BTM_LEAF_ENTRY_LENGTH(fEntry->klen, fEntry->nObjects);
	sum += sizeof(Two);	/* slot space */
    }

    /* i-th old entries will be remained in 'fpage' */
    fpage->hdr.nSlots = i;

    /*@ fill the new page */
    /* i : slot no. of fpage, continued from above loop */
    /* j : slot no. of new page, npage */
    for (j = 0; i < maxLoop; i++, j++) {

	nEntryOffset = npage->slot[-j] = npage->hdr.free;
	nEntry = (btm_LeafEntry*)&(npage->data[nEntryOffset]);

        fEntry = (btm_LeafEntry*)&(fpage->data[fpage->slot[-i]]);

        entryLen = BTM_LEAF_ENTRY_LENGTH(fEntry->klen, fEntry->nObjects);

        memcpy(nEntry, fEntry, entryLen);

	/* In new page, adjust the pointer to the free space */
	npage->hdr.free += entryLen;
    }

    /* free the space from the original page */
    fpage->hdr.unused += npage->hdr.free;

    npage->hdr.nSlots = j;

    /* Comstruct 'overflowEntry' which will be inserted into its parent */
    /* The key of ritem is that of the 0-th slot of npage. */
    nEntryOffset = npage->slot[0];
    nEntry = (btm_LeafEntry*)&(npage->data[nEntryOffset]);
    overflowEntry->spid = newPid.pageNo;
    overflowEntry->klen = nEntry->klen;
    memcpy(&(overflowEntry->kval[0]), &(nEntry->kval[0]), overflowEntry->klen);

    /* Leaves are connected by doubly linked list, so it should update the links. */
    MAKE_PAGEID(nextPid, fpage->hdr.pid.volNo, fpage->hdr.nextPage);
    fpage->hdr.nextPage = newPid.pageNo;


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        /*
         * original page
         */
        LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_SPLIT_LEAF_PAGE, LOG_REDO_UNDO,
                              fpage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Two), &npage->hdr.nSlots,
                              sizeof(Two), &npage->hdr.free,
                              sizeof(ShortPageID), &fpage->hdr.nextPage,
                              sizeof(ShortPageID), &nextPid.pageNo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        fpage->hdr.lsn = lsn;
        fpage->hdr.logRecLen = logRecLen;

        /*
         * new page
         */
        initLeafPageInfo.iid = npage->hdr.iid;
        initLeafPageInfo.rootFlag = FALSE;
        initLeafPageInfo.prevPage = npage->hdr.prevPage;
        initLeafPageInfo.nextPage = npage->hdr.nextPage;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INIT_LEAF_PAGE, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_InitLeafPage_T), &initLeafPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        entriesInfo.startSlotNo = 0;
        entriesInfo.nEntries = npage->hdr.nSlots;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INSERT_LEAF_ENTRIES, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_SpecifyEntries_T), &entriesInfo,
                              npage->hdr.free, npage->data);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        npage->hdr.lsn = lsn;
        npage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(fpage->hdr.lsn); 
        INCREASE_LSN_BY_ONE(npage->hdr.lsn); 
    }

    if (nextPid.pageNo != NIL) { /* next_BCB != NULL */
	/* Latch the next page of the new page. */
	e = SHM_getLatch(handle, next_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e); 

	mpage = (BtreeLeaf *)next_BCB->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_BTM_MODIFY_LEAF_CHAIN_PREV_LINK, LOG_REDO_UNDO,
                                  mpage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &newPid.pageNo,
                                  sizeof(ShortPageID), &mpage->hdr.prevPage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e); 

            mpage->hdr.lsn = lsn;
            mpage->hdr.logRecLen = logRecLen;
        } else {
            INCREASE_LSN_BY_ONE(mpage->hdr.lsn); 
        }

	mpage->hdr.prevPage = newPid.pageNo;

	next_BCB->dirtyFlag = 1;

	e = SHM_releaseLatch(handle, next_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e); 
    }

    nPage_BCBP->dirtyFlag = 1;

    e = SHM_releaseLatch(handle, nPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e); 

    e = BfM_unfixBuffer(handle, nPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    orig_BCB->dirtyFlag = 1; 

    return(eNOERROR);

} /* btm_SplitLeaf() */


/*
 * Function: Four btm_InsertInternal(Four, Buffer_ACC_CB*, Four, btm_InternalEntry*)
 *
 * Description:
 *
 * Returns:
 */
Four btm_InsertInternal(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Buffer_ACC_CB *bcb,		      /* IN buffer control block for updated page */
    Four insertSlotNo,		      /* IN slot where the new entry to place */
    btm_InternalEntry *insertedEntry, /* IN entry to be inserted */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four entryLen;		/* length of the new entry */
    BtreeInternal *apage;	/* a Btree internal page */
    btm_InternalEntry *entry;	/* an internal entry */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    Two insertPosition;         /* insert position variable for logging */
    Four e;                     /* returned error code */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_InsertInternal(bcb=%P, insertSlotNo=%ld, insertedEntry=%P)",
	      bcb, insertSlotNo, insertedEntry));


    apage = (BtreeInternal*)bcb->bufPagePtr;

    /* length of the inserted entry */
    entryLen = BTM_INTERNAL_ENTRY_LENGTH(insertedEntry->klen);

    /* There is enough space? We should count the slot space. */
    if (BI_FREE(apage) < entryLen+sizeof(Two)) /* not enough */
	return(BTM_NOSPACE); /* split should be occur */

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        insertPosition = insertSlotNo;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INSERT_INTERNAL_ENTRY, LOG_REDO_UNDO,
                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Two), &insertPosition,
                              entryLen, insertedEntry);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        apage->hdr.lsn = lsn;
        apage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(apage->hdr.lsn);
    }

    if (BI_CFREE(apage) < entryLen+sizeof(Two))
	btm_CompactInternalPage(handle, apage, NIL);

    /* Move slots for a new slot */
    BTM_INSERT_SLOTS_IN_BTREE_PAGE(apage, insertSlotNo, 1);

    /* store the new entry and update some information */
    apage->slot[-insertSlotNo] = apage->hdr.free;
    entry = (btm_InternalEntry*)&apage->data[apage->slot[-insertSlotNo]];
    memcpy(entry, insertedEntry, entryLen);

    apage->hdr.nSlots++;
    apage->hdr.free += entryLen;

    /* Set the dirty flag of the updated page. */
    bcb->dirtyFlag = 1;


    return(eNOERROR);

} /* btm_InsertInternal() */


Four btm_RequestExclusiveTreeLatch(
    Four 		handle,
    btm_TraversePath 	*path,     	/* IN btree traverse path stack */
    Buffer_ACC_CB    	*orig_BCB) 	/* IN pointer to buffer control block for orignal leaf */
{
    Four 		e;		/* error code */
    BtreePage 		*origPage;	/* original btree leaf page */
    Lsn_T 		orig_LSN;	/* LSN of the original leaf page */

    /* Request a conditional X latch on the tree. */
    e = btm_GetTreeLatchInPath(handle, path, M_EXCLUSIVE, M_CONDITIONAL);
    if (e < eNOERROR) ERRL1(handle, e, orig_BCB->latchPtr);

    if (e == SHM_BUSYLATCH) {

        origPage = (BtreePage*)orig_BCB->bufPagePtr;
        orig_LSN = origPage->bl.hdr.lsn;

	/* Release the holding latches. */
	e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	/* Request an unconditional X latch on the tree. */
	e = btm_GetTreeLatchInPath(handle, path, M_EXCLUSIVE, M_UNCONDITIONAL);
	if (e < eNOERROR) ERR(handle, e);

	/* Relatch the leaf page. */
	e = SHM_getLatch(handle, orig_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	if (e < eNOERROR) ERRTL(handle, e, path);

	if (!LSN_CMP_EQ(origPage->bl.hdr.lsn, orig_LSN)) {
	    /* Release the latch and unfix the buffer. */
	    e = SHM_releaseLatch(handle, orig_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRTL(handle, e, path);

	    /* Release the tree latch. */
	    e = btm_ReleaseTreeLatchInPath(handle, path);
	    if (e < eNOERROR) ERR(handle, e);

	    return(BTM_RETRAVERSE);
	}
    }

    return(eNOERROR);

}

