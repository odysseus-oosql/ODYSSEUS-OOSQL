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
 * Module : lot_InsertInternal.c
 *
 * Description:
 *  Insert the L_O_T_ItemList 'itemList' into the internal node.
 *
 * Export:
 *  Four lot_InsertInternal(Four, DataFileInfo*, PageID*, Four, Four,
 *                          L_O_T_ItemList*, L_O_T_ItemList*, Boolean*)
 */

#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "LOT.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_InsertInternal( )
 *================================*/
/*
 * Function: Four lot_InsertInternal(Four, DataFileInfo*, PageID*, Four, Four,
 *                          L_O_T_ItemList*, L_O_T_ItemList*, Boolean*)
 *
 * Description:
 *  Insert the L_O_T_ItemList 'itemList' into the internal node.
 *
 * Returns:
 *  Error codes
 *    eMEMORYALLOCERR
 *    some errors caused by function calls
 */
Four lot_InsertInternal(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    PageID *nearPidForRoot,     /* IN near page for root */ 
    L_O_T_INodePage *apage,     /* INOUT root page */
    L_O_T_INode *anode,         /* INOUT root node */
    Four     idx,		/* IN start position of insert */
    L_O_T_ItemList *list,	/* IN item list to be inserted */
    L_O_T_ItemList *newList,	/* OUT item list to be inserted into the parent node */
    Boolean  *overflow,		/* OUT overflow flag */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four	e;		/* error number */
    Four	movedEntries;	/* moved entries for current node at that time */
    Four	totalEntries;	/* # of entries considered */
    Four	nEntries;	/* # of entries for current node to hold */
    Four	listEntries;	/* remained entries in the 'list' */
    Four	nNodes;		/* # of nodes to hold the total entries */
    Four	nNewPages;      /* # of newly allocated pages */
    Four	entriesPerNode;	/* # of entries for 1 node to hold */
    Four	remains;	/* # of entries remaind after balancing */
    PageID	*newPids;	/* array of PageIDs to be newly allocated */
    L_O_T_INodePage *currentPage;
    Buffer_ACC_CB *currentPage_BCBP;
    L_O_T_INode	tmpNode;	/* save the original node */
    L_O_T_INodeEntry *entryPtr;	/* pointer to entry array of temporary node */
    Four src;			/* where the entries comes for current node? */
    Four toIdx;			/* yet unused entries before idx location */
    Four i, j, n;
    Four old_nEntries;          /* # of entries before update */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_ReplaceInternalEntries_T replaceInternalEntriesInfo;
    LOG_Image_LOT_DeleteInternalEntries_T deleteInternalEntriesInfo;
    PageID nearPid;
    SegmentID_T pageSegmentID; /* page segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    /* pointer for LOT Data Structure of perThreadTable */
    LOT_PerThreadDS_T *lot_perThreadDSptr = LOT_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1, ("lot_InsertInternal()"));


    *overflow = FALSE;

    old_nEntries = anode->header.nEntries; /* save the # of old entries */
    totalEntries = anode->header.nEntries + list->nEntries - list->nReplaces;


    /* change the count to subtree count instead of accumulated count */
    for (j = anode->header.nEntries - 1; j > 0; j--)
	anode->entry[j].count -= anode->entry[j-1].count;


    if (totalEntries <= LOT_MAXENTRIES) {
	/* There is enough space to accomadate the new item(s) */

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && apage != NULL) {

            replaceInternalEntriesInfo.start = idx;
            replaceInternalEntriesInfo.nNewEntries = list->nEntries;
            replaceInternalEntriesInfo.nOldEntries = list->nReplaces;

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                  apage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                  sizeof(L_O_T_INodeEntry)*list->nEntries, list->entry,
                                  sizeof(L_O_T_INodeEntry)*list->nReplaces, &anode->entry[idx]);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            apage->header.lsn = lsn;
            apage->header.logRecLen = logRecLen;
        }

	/* prepare the space moving the entries */
	movedEntries = anode->header.nEntries - idx - list->nReplaces;
	memmove(&anode->entry[idx+list->nEntries], &anode->entry[idx+list->nReplaces],
		movedEntries*sizeof(L_O_T_INodeEntry));

	/*@ copy new entries into the system buffer */
	memcpy(&anode->entry[idx], list->entry, list->nEntries*sizeof(L_O_T_INodeEntry));

	anode->header.nEntries += list->nEntries - list->nReplaces;

	/*@ adjust the count fields */
	for (j = 1; j < anode->header.nEntries; j++)
	    anode->entry[j].count += anode->entry[j-1].count;

    } else {
        /* save the original node */
        tmpNode = *anode;

	nNodes = CEIL_AFTER_DIVIDE(totalEntries, LOT_MAXENTRIES);

	entriesPerNode = totalEntries / nNodes;

	/* The remain entries 'remains' are distributed evenly from the first node */
	remains = totalEntries % nNodes;

	/* get memory for saving the PageIDs of nodes (both old and new) */
        /* newPids[0] hold the original page id */
        e = Util_reallocVarArray(handle, &(lot_perThreadDSptr->lot_pageidArray), sizeof(PageID), nNodes);
        if (e < eNOERROR) ERR(handle, e);

	newPids = (PageID *)(lot_perThreadDSptr->lot_pageidArray.ptr);

	/* allocat the needed nodes (# of Nodes = nNodes - 1) */
        if (apage != NULL) {
            newPids[0] = apage->header.pid;
            nNewPages = nNodes - 1;
            nearPid = apage->header.pid;
        } else {
            nNewPages = nNodes;
            nearPid = *nearPidForRoot;
        }
	e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &pageSegmentID, PAGESIZE2);
	if (e < eNOERROR) ERR(handle, e);

	e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &pageSegmentID,
			     &nearPid, nNewPages, PAGESIZE2, FALSE, newPids+(nNodes-nNewPages), logParam);
	if (e < 0) {
            ERR(handle, e);
        }

	/*@ make the newList */
	newList->nEntries = nNodes;
	newList->nReplaces = 1;
        newList->entryArrayPtr = LOT_GET_ENTRY_ARRAY(handle);
        e = Util_reallocVarArray(handle, newList->entryArrayPtr, sizeof(L_O_T_INodeEntry), nNodes);
        if (e < eNOERROR) ERR(handle, e);

	newList->entry = (L_O_T_INodeEntry *)newList->entryArrayPtr->ptr;

	/* At first, take the entries from the original entries */
	/* If reach idx postition, use the new entries. */
	/* After the new entreis are used, use the entries after idx pos. */
	src = 0;
	entryPtr = tmpNode.entry;
	toIdx = idx;		/* # of unused entries before idx position */
	for (i = 0; i < nNodes; i++) {
            if (i == 0 && apage != NULL) {       /* original page */
                currentPage = apage;
            } else {            /* newly allocated page */
                e = BfM_fixNewBuffer(handle, &newPids[i], M_FREE, &currentPage_BCBP, PAGE_BUF);
                if (e < 0) {
                    ERR(handle, e);
                }

                currentPage = (L_O_T_INodePage *)currentPage_BCBP->bufPagePtr;
                LOT_INIT_INODE_PAGE_HDR(currentPage, finfo->fid, newPids[i]);
            }

            anode = &currentPage->node;

	    /* # of entries for this node to hold */
	    nEntries = entriesPerNode + ((remains > i) ? 1:0);

	    movedEntries = 0;
	    while (movedEntries < nEntries) {

		switch(src) {
		  case 0:	/* take the entries from the original entries */
		    n = MIN(nEntries - movedEntries, toIdx);
		    memcpy((char*)&anode->entry[movedEntries], (const char*)entryPtr, n*sizeof(L_O_T_INodeEntry));
		    entryPtr += n;
		    toIdx -= n;
		    movedEntries += n;

		    if (toIdx == 0) {
			src = 1; /* From next, use the new entries */
			listEntries = list->nEntries;
			entryPtr = list->entry;
		    }

		    break;

		  case 1:	/* take the entries from the new entries */
		    n = MIN(nEntries - movedEntries, listEntries);
		    memcpy((char*)&anode->entry[movedEntries], (const char*)entryPtr, n*sizeof(L_O_T_INodeEntry));
		    entryPtr += n;
		    listEntries -= n;
		    movedEntries += n;

		    if (listEntries == 0) {
			src = 2;	/* From now, use the original entries again */
			entryPtr = &tmpNode.entry[idx+list->nReplaces];
		    }

		    break;

		  case 2:	/* take the entries from the original entries */
		    n = nEntries - movedEntries;
		    memcpy((char*)&anode->entry[movedEntries], (const char*)entryPtr, n*sizeof(L_O_T_INodeEntry));
		    entryPtr += n;
		    movedEntries += n;
		    break;

		} /* end of switch */
	    }

	    /*@ adjust the count fields */
	    for (j = 1; j < nEntries; j++)
		anode->entry[j].count += anode->entry[j-1].count;

	    anode->header.nEntries = nEntries;
	    anode->header.height = tmpNode.header.height; 

	    newList->entry[i].spid = newPids[i].pageNo;
	    newList->entry[i].count = anode->entry[nEntries-1].count;

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                if (i == 0 && apage != NULL) {
                    if (idx >= nEntries) {
                        assert(old_nEntries > nEntries);

                        deleteInternalEntriesInfo.start = nEntries;
                        deleteInternalEntriesInfo.nEntries = old_nEntries - nEntries;
                        deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                        deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                              apage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                              deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &tmpNode.entry[deleteInternalEntriesInfo.start]);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) {
                            ERR(handle, e);
                        }

                    } else {    /* idx < nEntries */
                        Four nInsertedEntries = MIN(nEntries - idx, list->nEntries - list->nReplaces);
                        Four nDeletedEntries = old_nEntries - nEntries + nInsertedEntries; /* # of original entries moved to next node: old_nEntries - idx - (nEntries - idx - nInsertedEntries) */

                        if (nDeletedEntries > 0) {
                            deleteInternalEntriesInfo.start = old_nEntries - nDeletedEntries;
                            deleteInternalEntriesInfo.nEntries = nDeletedEntries;
                            deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                            deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                                  LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                                  apage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                                  sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                                  deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &tmpNode.entry[deleteInternalEntriesInfo.start]);

                            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                            if (e < eNOERROR) {
                                ERR(handle, e);
                            }
                        }

                        assert(nInsertedEntries > 0);
                        replaceInternalEntriesInfo.start = idx;
                        replaceInternalEntriesInfo.nOldEntries = MIN(nEntries - idx, list->nReplaces);
                        replaceInternalEntriesInfo.nNewEntries = nInsertedEntries + replaceInternalEntriesInfo.nOldEntries;

                        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                              apage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                              sizeof(L_O_T_INodeEntry)*replaceInternalEntriesInfo.nNewEntries, list->entry,
                                              sizeof(L_O_T_INodeEntry)*replaceInternalEntriesInfo.nOldEntries, &tmpNode.entry[idx]);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) {
                            ERR(handle, e);
                        }
                    }
                } else {
                    LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_LOT_INIT_INTERNAL_NODE_PAGE, LOG_REDO_ONLY,
                                          newPids[i], xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(FileID), &finfo->fid,
                                          LOT_INODE_USED_SIZE(anode), anode);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) {
                        ERRB1(handle, e, currentPage_BCBP, PAGE_BUF);
                    }
                }

                /* mark the lsn in the page */
                currentPage->header.lsn = lsn;
                currentPage->header.logRecLen = logRecLen;
            }

	    if (i != 0 || apage == NULL) {
                currentPage_BCBP->dirtyFlag = 1;

                e = BfM_unfixBuffer(handle, currentPage_BCBP, PAGE_BUF);
                if (e < 0) {
                    ERR(handle, e);
                }
            }

	} /* end of for */

	*overflow = TRUE;
    }

    return(eNOERROR);

} /* lot_InsertInternal( ) */

