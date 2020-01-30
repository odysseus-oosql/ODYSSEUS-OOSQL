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
 * Module: lot_RebalanceTree.c
 *
 * Description:
 *  Rebalnce the large object tree. The tree may not be unbalanced.
 *
 * Exports:
 *  Four lot_RebalanceTree(Four, DataFileInfo*, L_O_T_Path*, PageID*, Boolean*, Boolean*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_RebalanceTree( )
 *================================*/
/*
 * Function: Four lot_RebalanceTree(Four, DataFileInfo*, L_O_T_Path*, PageID*, Boolean*, Boolean*)
 *
 * Description:
 *  Rebalnce the large object tree. The tree may not be unbalanced.
 *
 * Returns:
 *  Error codes
 *    eMEMORYALLOCERR
 *    some errors caused by function calls
 */
Four lot_RebalanceTree(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    L_O_T_Path *path,		/* IN information for the cut path */
    char *anodeOrRootPageNo,    /* INOUT anode or root page no */
    Boolean *rootWithHdr_Flag,  /* INOUT TRUE if root is with header */
    Four maxNodeLen,            /* IN node can grow at most to this value */
    Boolean *uf,		/* OUT indicate the underflow of child node */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    PageID p_pid;		/* PageID of the current node */
    L_O_T_INodePage *p_page;    /* pointer to the current page */
    L_O_T_INode *p_node;	/* pointer to the current node */
    Buffer_ACC_CB *p_page_BCBP;
    L_O_T_INode *anode;         /* a node */
    Four p_c_idx;		/* entry index of the child in cut path */
    Boolean p_c_uf;		/* child underflow flag from the path */
    Boolean c_uf;		/* child underflow flag from the recursive call */
    Boolean c_mf;		/* merge flag of merge/distribution */
    Four c_l_idx;		/* left entry index of merge/distribution */
    L_O_T_ItemList c_list;	/* for merge/distribution of two children */
    Boolean rootChange;		/* root change flag */
    Four movedEntries;		/* # of entries moved from left to right */
    PageID root;
    L_O_T_INodePage *rootPage;
    Buffer_ACC_CB *rootPage_BCBP;
    Four dummy;			/* dummy parameter */
    PageID pid;
    Four n;
    L_O_T_INodeEntry tmpEntries[2]; /* temporary entries */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_ReplaceInternalEntries_T replaceInternalEntriesInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1, ("lot_RevalanceTree()"));

    if (lot_EmptyPath(handle, path)) {
	/* one basis of recursive call */
	/* In deleteion phase we didn't reach the node at height 1 */
	*uf = FALSE;

	return(eNOERROR);
    }

    /*@ Pop from the path the information for the current node */
    lot_PopPath(handle, path, &p_page_BCBP, &anode, &p_c_idx, &p_c_uf); 

    if (p_page_BCBP != NULL) {
        p_pid = p_page_BCBP->key;
        p_page = (L_O_T_INodePage *)p_page_BCBP->bufPagePtr;
        p_node = &p_page->node;

    } else {
        p_page = NULL;
        p_node = anode;
    }

    /* Set to the FALSE */
    rootChange = FALSE;

    if (p_c_uf && p_node->header.nEntries > 1) {
	/* merge or redistributes with the sibling node */

	c_list.nEntries = 2;
	c_list.entry = tmpEntries;

	/*@ find the node to merge/distribute */
	/* we prefer the right sibling */
	if (p_c_idx == p_node->header.nEntries-1)
	    /* merge with the left node */
	    c_l_idx = p_c_idx-1;
	else
	    /* merge with the right node */
	    c_l_idx = p_c_idx;

	c_list.entry[0].spid = p_node->entry[c_l_idx].spid;
	c_list.entry[0].count = lot_GetCount(handle, p_node, c_l_idx);
	c_list.entry[1].spid = p_node->entry[c_l_idx+1].spid;
	c_list.entry[1].count = lot_GetCount(handle, p_node, c_l_idx+1);

	if (p_node->header.height == 1) {
	    /* Children are leaf nodes */

	    e = lot_MergeOrDistributeLeaf(handle, xactEntry, finfo, &c_list, &c_uf, &c_mf, logParam);
	    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	} else {
	    /* Children are internal nodes */

	    e = lot_MergeOrDistribute(handle, xactEntry, finfo, &c_list, &movedEntries, &c_uf, &c_mf, logParam);
	    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
	}

	/* At this point, assert that c_uf is FALSE. */
	/* adjust the count fields */
	if (c_mf) {
	    /* merge has been occured. */

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && p_page != NULL) {
                L_O_T_INodeEntry tmp_oldEntries[2];

                replaceInternalEntriesInfo.start = c_l_idx;
                replaceInternalEntriesInfo.nNewEntries = 1;
                replaceInternalEntriesInfo.nOldEntries = 2;

                tmp_oldEntries[0].spid = p_node->entry[c_l_idx].spid;
                tmp_oldEntries[0].count = lot_GetCount(handle, p_node, c_l_idx);
                tmp_oldEntries[1].spid = p_node->entry[c_l_idx+1].spid;
                tmp_oldEntries[1].count = lot_GetCount(handle, p_node, c_l_idx+1);

                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      p_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                      replaceInternalEntriesInfo.nNewEntries*sizeof(L_O_T_INodeEntry), c_list.entry,
                                      replaceInternalEntriesInfo.nOldEntries*sizeof(L_O_T_INodeEntry), tmp_oldEntries);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                /* mark the lsn in the page */
                p_page->header.lsn = lsn;
                p_page->header.logRecLen = logRecLen;
            }

	    /*@ adjust the count field */
	    p_node->entry[c_l_idx].count = ((c_l_idx == 0) ? 0:p_node->entry[c_l_idx-1].count) + c_list.entry[0].count;

	    /* fill the erased part by moving the entries after it */
	    n = p_node->header.nEntries - (c_l_idx+2);
	    memmove(&p_node->entry[c_l_idx+1], &p_node->entry[c_l_idx+2],
		    n*sizeof(L_O_T_INodeEntry));
	    p_node->header.nEntries --;

	    if (p_c_idx != c_l_idx) {
		/* The node was the rightmost node */
		p_c_idx --;

		if (p_node->header.height > 1) {

		    MAKE_PAGEID(pid, finfo->fid.volNo, p_node->entry[c_l_idx].spid);
		    e = lot_ReplaceTop(handle, path, &pid, movedEntries);
		    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
		}
	    }

	} else {
	    /* distribution has been occurred */

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && p_page != NULL) {
                L_O_T_INodeEntry tmp_oldEntries[2];

                replaceInternalEntriesInfo.start = c_l_idx;
                replaceInternalEntriesInfo.nNewEntries = 2;
                replaceInternalEntriesInfo.nOldEntries = 2;

                tmp_oldEntries[0].spid = p_node->entry[c_l_idx].spid;
                tmp_oldEntries[0].count = lot_GetCount(handle, p_node, c_l_idx);
                tmp_oldEntries[1].spid = p_node->entry[c_l_idx+1].spid;
                tmp_oldEntries[1].count = lot_GetCount(handle, p_node, c_l_idx+1);

                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      p_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                      replaceInternalEntriesInfo.nNewEntries*sizeof(L_O_T_INodeEntry), c_list.entry,
                                      replaceInternalEntriesInfo.nOldEntries*sizeof(L_O_T_INodeEntry), tmp_oldEntries);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                /* mark the lsn in the page */
                p_page->header.lsn = lsn;
                p_page->header.logRecLen = logRecLen;
            }

	    /* adjust the count field */
	    p_node->entry[c_l_idx].count =
		p_node->entry[c_l_idx+1].count - c_list.entry[1].count;

	    if (p_c_idx != c_l_idx && p_node->header.height > 1) {
		/* the node was the rightmost node */
		e = lot_ReplaceTop(handle, path, (PageID *)NULL, movedEntries);
		if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
	    }
	}
    }

    if (p_node->header.height > 1) {
	/* children are internal nodes */

	if (anodeOrRootPageNo != NULL && p_node->header.nEntries == 1) {
	    /* Make the child the new root node */

            rootChange = TRUE;
	    e = lot_RebalanceTree(handle, xactEntry, finfo, path, anodeOrRootPageNo,
                                  rootWithHdr_Flag, maxNodeLen, &c_uf,logParam);
	    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	} else {

	    /*@ recursive call with root NULL */
	    e = lot_RebalanceTree(handle, xactEntry, finfo, path, NULL, NULL, 0, &c_uf, logParam);
	    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	    if (c_uf) {
		/* Underflow has occurred in the child node */

		c_list.nEntries = 2;
		c_list.entry = tmpEntries;

		/*@ find the node to merge/distribute */
		/* we prefer the right sibling */
		if (p_c_idx == p_node->header.nEntries-1)
		    /* merge with the left node */
		    c_l_idx = p_c_idx-1;
		else
		    /* merge with the right node */
		    c_l_idx = p_c_idx;

		c_list.entry[0].spid = p_node->entry[c_l_idx].spid;
		c_list.entry[0].count = lot_GetCount(handle, p_node, c_l_idx);
		c_list.entry[1].spid = p_node->entry[c_l_idx+1].spid;
		c_list.entry[1].count = lot_GetCount(handle, p_node, c_l_idx+1);

		/* Children are internal nodes */
		e = lot_MergeOrDistribute(handle, xactEntry, finfo, &c_list, &dummy, &c_uf, &c_mf, logParam);
		if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

		/* At this point, assert that c_uf is FALSE. */
		if (c_mf) {
		    /* merge has been occured. */

                    /*
                     * Write log record.
                     */
                    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && p_page != NULL) {
                        L_O_T_INodeEntry tmp_oldEntries[2];

                        replaceInternalEntriesInfo.start = c_l_idx;
                        replaceInternalEntriesInfo.nNewEntries = 1;
                        replaceInternalEntriesInfo.nOldEntries = 2;

                        tmp_oldEntries[0].spid = p_node->entry[c_l_idx].spid;
                        tmp_oldEntries[0].count = lot_GetCount(handle, p_node, c_l_idx);
                        tmp_oldEntries[1].spid = p_node->entry[c_l_idx+1].spid;
                        tmp_oldEntries[1].count = lot_GetCount(handle, p_node, c_l_idx+1);

                        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                              p_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                              replaceInternalEntriesInfo.nNewEntries*sizeof(L_O_T_INodeEntry), c_list.entry,
                                              replaceInternalEntriesInfo.nOldEntries*sizeof(L_O_T_INodeEntry), tmp_oldEntries);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                        /* mark the lsn in the page */
                        p_page->header.lsn = lsn;
                        p_page->header.logRecLen = logRecLen;
                    }

		    /*@ adjust the count field */
   	            p_node->entry[c_l_idx].count = ((c_l_idx == 0) ? 0:p_node->entry[c_l_idx-1].count) + c_list.entry[0].count;

		    /* fill the erased part by moving the entries after it */
		    n = p_node->header.nEntries - (c_l_idx+2);
		    memmove(&p_node->entry[c_l_idx+1], &p_node->entry[c_l_idx+2],
			    n*sizeof(L_O_T_INodeEntry));
		    p_node->header.nEntries --;

		} else {
		    /* distribution has been occurred */

                    /*
                     * Write log record.
                     */
                    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && p_page != NULL) {
                        L_O_T_INodeEntry tmp_oldEntries[2];

                        replaceInternalEntriesInfo.start = c_l_idx;
                        replaceInternalEntriesInfo.nNewEntries = 2;
                        replaceInternalEntriesInfo.nOldEntries = 2;

                        tmp_oldEntries[0].spid = p_node->entry[c_l_idx].spid;
                        tmp_oldEntries[0].count = lot_GetCount(handle, p_node, c_l_idx);
                        tmp_oldEntries[1].spid = p_node->entry[c_l_idx+1].spid;
                        tmp_oldEntries[1].count = lot_GetCount(handle, p_node, c_l_idx+1);

                        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                              p_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                              replaceInternalEntriesInfo.nNewEntries*sizeof(L_O_T_INodeEntry), c_list.entry,
                                              replaceInternalEntriesInfo.nOldEntries*sizeof(L_O_T_INodeEntry), tmp_oldEntries);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                        /* mark the lsn in the page */
                        p_page->header.lsn = lsn;
                        p_page->header.logRecLen = logRecLen;
                    }

		    /*@ adjust the count field */
		    p_node->entry[c_l_idx].count =
			p_node->entry[c_l_idx+1].count - c_list.entry[1].count;
		}
	    }

	    if (anodeOrRootPageNo != NULL && p_node->header.nEntries == 1) {
		/* Make the child the new root */

		MAKE_PAGEID(root, finfo->fid.volNo, p_node->entry[0].spid);

                e = BfM_getAndFixBuffer(handle, &root, M_FREE, &rootPage_BCBP, PAGE_BUF);
                if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

                rootPage = (L_O_T_INodePage*)rootPage_BCBP->bufPagePtr;

                if (LOT_INODE_USED_SIZE(&rootPage->node) <= maxNodeLen) {
                    rootChange = TRUE;
                    memcpy(anodeOrRootPageNo, &rootPage->node, LOT_INODE_USED_SIZE(&rootPage->node));
                    *rootWithHdr_Flag = TRUE;

                } else {
                    memcpy(anodeOrRootPageNo, &root.pageNo, sizeof(ShortPageID));
                    *rootWithHdr_Flag = FALSE;
                }

                e = BfM_unfixBuffer(handle, rootPage_BCBP, PAGE_BUF);
                if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

                if (rootChange) {
                    e = RDsM_FreeTrain(handle, xactEntry, &p_pid, PAGESIZE2, finfo->tmpFileFlag, logParam);
                    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
                }
	    }
	}
    }

    *uf = (p_node->header.nEntries < LOT_HALFENTRIES) ? TRUE:FALSE; 

    if (p_page_BCBP != NULL) {
        if (anodeOrRootPageNo != NULL && rootChange == FALSE && LOT_INODE_USED_SIZE(p_node) <= maxNodeLen) {
            rootChange = TRUE;
            memcpy(anodeOrRootPageNo, p_node, LOT_INODE_USED_SIZE(p_node));
            *rootWithHdr_Flag = TRUE;
        }

        p_page_BCBP->dirtyFlag = 1;
        e = BfM_unfixBuffer(handle, p_page_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        if (rootChange) {
            e = RDsM_FreeTrain(handle, xactEntry, &p_pid, PAGESIZE2, finfo->tmpFileFlag, logParam);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    return(eNOERROR);

  LABEL_Error:
    if (p_page != NULL) (Four)BfM_unfixBuffer(handle, p_page_BCBP, PAGE_BUF);
    return(e);

} /* lot_RebalanceTree() */

