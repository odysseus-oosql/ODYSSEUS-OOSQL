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
 * Module: lot_MakeRoot.c
 *
 * Description:
 *  make the root from the items
 *
 * Exports:
 *  Four lot_MakeRoot(Four, DataFileInfo*, PageID*, Four, Four, L_O_T_ItemList*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "LOT.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_MakeRoot( )
 *================================*/
/*
 * Function: Four lot_MakeRoot(Four, DataFileInfo*, PageID*, Four, Four, L_O_T_ItemList*)
 *
 * Description:
 *  make the root from the items
 *
 * Returns:
 *  Error codes
 *    eMEMORYALLOCERR
 *    some errors caused by function calls
 */
Four lot_MakeRoot(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    PageID *nearPidForRoot,     /* IN near page for root */ 
    char *anodeOrRootPageNo,    /* INOUT anode or root page no */
    Boolean *rootWithHdr_Flag,  /* INOUT TRUE if root is with header */
    Four maxNodeLen,            /* IN node can grow at most to this value */
    Four     height,		/* IN height of the original tree */
    L_O_T_ItemList *itemList,	/* IN entries from which internal tree is constructed */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    Four totalEntries;		/* total # of entries considered */
    Four nEntries;		/* # of entries fot the current node to hold */
    Four nNodes;		/* # of nodes needed to hold the items */
    Four entriesPerNode;	/* # of entries for one page to hold */
    Four remains;		/* small fraction after balancing */
    PageID *newPids;		/* array of PageIDs to be newly allocated */
    PageID currentPid;		/* PageID being currently processed */
    L_O_T_INodePage *apage;     /* pointer to buffer holding slotted page */
    Buffer_ACC_CB *apage_BCBP;
    L_O_T_INode *anode;
    L_O_T_INodeEntry *entryPtr;	/* start of next copy */
    L_O_T_ItemList localList;	/* list construected at this stage */
    PageID root;		/* the newly created root page's PageID */
    Four i, j;
    Four inodeUsedSize;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    SegmentID_T pageSegmentID;  /* page segment ID */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    /* pointer for LOT Data Structure of perThreadTable */
    LOT_PerThreadDS_T *lot_perThreadDSptr = LOT_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_LOT, TR1, ("lot_MakeRoot()"));


    if (itemList->nEntries <= LOT_MAXENTRIES) {
        /* the slots can be put in one node */

        anode = (L_O_T_INode*)anodeOrRootPageNo;
        anode->header.height = height;
        anode->header.nEntries = itemList->nEntries;
        memcpy(anode->entry, itemList->entry, itemList->nEntries*sizeof(L_O_T_INodeEntry));

        /*@ adjust the count fields */
        for (i = 1; i < anode->header.nEntries; i++)
            anode->entry[i].count += anode->entry[i-1].count;

        inodeUsedSize = LOT_INODE_USED_SIZE(anode);

        if (inodeUsedSize <= maxNodeLen) {
            *rootWithHdr_Flag = TRUE;

        } else {
	    /* Allocate one page for the root node */
            e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &pageSegmentID, PAGESIZE2);
	    if (e < eNOERROR) ERR(handle, e);

	    e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &pageSegmentID,
				 nearPidForRoot, 1, PAGESIZE2, FALSE, &root, logParam);
	    if (e < eNOERROR) ERR(handle, e);

	    e = BfM_getAndFixBuffer(handle, &root, M_FREE, &apage_BCBP, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    apage = (L_O_T_INodePage *)apage_BCBP->bufPagePtr;

            LOT_INIT_INODE_PAGE_HDR(apage, finfo->fid, root);

	    memcpy(&apage->node, anode, inodeUsedSize);

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_INIT_INTERNAL_NODE_PAGE, LOG_REDO_ONLY,
                                      root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(FileID), &finfo->fid,
                                      inodeUsedSize, anode);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB1(handle, e, apage_BCBP, PAGE_BUF);

                /* mark the lsn in the page */
                apage->header.lsn = lsn;
                apage->header.logRecLen = logRecLen;
            }

	    apage_BCBP->dirtyFlag = 1;

	    e = BfM_unfixBuffer(handle, apage_BCBP, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    /* store the root PageNo */
	    *((ShortPageID *)anodeOrRootPageNo) = root.pageNo;

            *rootWithHdr_Flag = FALSE;
	}

    } else {

	/* The slots cannot be put in one node */
	/* We recursively call this function. */

	totalEntries = itemList->nEntries;
	nNodes = CEIL_AFTER_DIVIDE(totalEntries, LOT_MAXENTRIES);

	entriesPerNode = totalEntries / nNodes;

	/* The remain slots 'remains' are distributed evenly from the first node */
	remains = totalEntries % nNodes;

        e = Util_reallocVarArray(handle, &(lot_perThreadDSptr->lot_pageidArray), sizeof(PageID), nNodes);
        if (e < eNOERROR) ERR(handle, e);

	newPids = (PageID *)(lot_perThreadDSptr->lot_pageidArray.ptr);

	/* allocate the needed nodes */
        e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &pageSegmentID, PAGESIZE2);
	if (e < eNOERROR) ERR(handle, e);

	e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &pageSegmentID,
			     nearPidForRoot, nNodes, PAGESIZE2, FALSE, newPids, logParam);
	if (e < 0) {
            ERR(handle, e);
        }

	localList.nEntries = nNodes;
        localList.entryArrayPtr = LOT_GET_ENTRY_ARRAY(handle);
        e = Util_reallocVarArray(handle, localList.entryArrayPtr, sizeof(L_O_T_INodeEntry), localList.nEntries);
        if (e < 0) {
            ERR(handle, e);
        }

	localList.entry = (L_O_T_INodeEntry *)localList.entryArrayPtr->ptr;

	/* save the ShortPageID in the itemList */
        for (i = 0; i < nNodes; i++) localList.entry[i].spid = newPids[i].pageNo;

	/* Evenly distribute the items in itemList */
	entryPtr = itemList->entry;
	for (i = 0; i < nNodes; i++) {
	    MAKE_PAGEID(currentPid, finfo->fid.volNo, localList.entry[i].spid);

	    e = BfM_fixNewBuffer(handle, &currentPid, M_FREE, &apage_BCBP, PAGE_BUF);
	    if (e < 0) {
                ERR(handle, e);
            }

	    apage = (L_O_T_INodePage *)apage_BCBP->bufPagePtr;
            anode = &apage->node;

            LOT_INIT_INODE_PAGE_HDR(apage, finfo->fid, currentPid);

	    nEntries = entriesPerNode + ((remains > i) ? 1:0);

	    memcpy(anode->entry, entryPtr, nEntries*sizeof(L_O_T_INodeEntry));
	    entryPtr += nEntries;

            anode->header.height = height; 
	    anode->header.nEntries = nEntries;

	    /* adjust the count fields */
	    for(j = 1; j < nEntries; j++)
		anode->entry[j].count += anode->entry[j-1].count;

	    /* Construct the L_O_T_ItemList */
	    localList.entry[i].count = anode->entry[nEntries-1].count;

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_INIT_INTERNAL_NODE_PAGE, LOG_REDO_ONLY,
                                      currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(FileID), &finfo->fid,
                                      LOT_INODE_USED_SIZE(&apage->node), &apage->node);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB1(handle, e, apage_BCBP, PAGE_BUF);

                /* mark the lsn in the page */
                apage->header.lsn = lsn;
                apage->header.logRecLen = logRecLen;
            }

	    apage_BCBP->dirtyFlag = 1;

	    e = BfM_unfixBuffer(handle, apage_BCBP, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);
	}

	e = lot_MakeRoot(handle, xactEntry, finfo, nearPidForRoot, anodeOrRootPageNo, rootWithHdr_Flag,
                         maxNodeLen, height+1, &localList, logParam); 

	if (e < eNOERROR) ERR(handle, e);
    }

    return (eNOERROR);

} /* lot_MakeRoot( ) */
