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
 * Module: lot_DeleteFromObject.c
 *
 * Description:
 *
 * Exports:
 *  Four lot_DeleteFromObject(Four, DataFileInfo*, L_O_T_ItemList*, Four, Four
 *                            Four, Four, Boolean*, Boolean*, L_O_T_Path*)
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
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_DeleteFromObject( )
 *================================*/
/*
 * Function: Four lot_DeleteFromObject(Four, DataFileInfo*, L_O_T_ItemList*, Four, Four
 *                            Four, Four, Boolean*, Boolean*, L_O_T_Path*)
 *
 * Description:
 *
 * Exports:
 *  Four lot_DeleteFromObject(Four, DataFileInfo*, L_O_T_ItemList*, Four, Four
 *                            Four, Four, Boolean*, Boolean*, L_O_T_Path*)
 */
Four lot_DeleteFromObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    L_O_T_ItemList *list,	/* INOUT information for two nodes (count fields are OUT variables) */
    L_O_T_INode *anode,         /* INOUT root node when root is with header */
    Four     from,		/* IN starting offset of deletion */
    Four     to,		/* IN ending offset of deletion */
    Four     *which,		/* OUT which node have the underflowed child? */
    Boolean  *uf,		/* OUT underflow flag */
    Boolean  *mf,		/* OUT merge flag */
    L_O_T_Path *path,		/* OUT cut-path registration */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    L_O_T_INodePage *l_page;    /* pointer to the left page */
    L_O_T_INodePage *r_page;    /* pointer to the right page */
    Buffer_ACC_CB *l_page_BCBP;
    PageID l_root;		/* root of left cut-path subtree */
    PageID r_root;		/* root of right cut-path subtree */
    L_O_T_INode *l_node;	/* pointer to the left node */
    L_O_T_INode *r_node;	/* pointer to the right node */
    Buffer_ACC_CB *r_page_BCBP;
    PageID c_l_root;		/* PageID of the root of left subtree */
    PageID c_r_root;		/* PageID of the root of right subtree */
    PageID *c_l_root_ptr;	/* pointer to PageID of left root */
    PageID *c_r_root_ptr;	/* Pointer to PageID of right root */
    Four c_l_idx;		/* entry index of left cut path */
    Four c_r_idx;		/* entry index of right cut path */
    Four c_l_count;		/* # of original bytes in left subtree */
    Four c_r_count;		/* # of original bytes in right subtree */
    L_O_T_ItemList c_list;	/* ItemList for the subtree */
    Four c_from;		/* starting offset of delete in subtree */
    Four c_to;			/* ending offset of delete in subtree */
    Four height;		/* height of current node */
    Four c_which;		/* which value of child node */
    Four p_c_idx;		/* entry index of possibly underflowed child node */
    Boolean c_uf;		/* underflow flag of subtree */
    Boolean c_mf;		/* merge flag of subtree */
    Boolean md_uf;		/* underflow flag in lot_MergeOrDistribute( ) */
    Boolean md_mf;		/* merge flag in lot_MergeOrDistribute( ) */
    Boolean l_uf;		/* underflow flag of left node */
    Boolean r_uf;		/* underflow flag of right node */
    Four l_begin;		/* begin index of subtrees to drop in left node */
    Four l_end;			/* end index of subtrees to drop in left node */
    Four r_end;			/* end index of subtrees to drop in right node */
    Four movedEntries;		/* # of entries moved from left to right */
    PageID pid;
    Four deletedBytes;
    Four movedBytes;
    Four n;
    Four i;
    L_O_T_INodeEntry tmpEntries[2]; /* temporary entries */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_DeleteInternalEntries_T deleteInternalEntriesInfo;
    LOG_Image_LOT_UpdateCountFields_T updateCountFieldsInfo; /* count fields update info */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1, ("lot_DeleteFromObject()"));


    /* initialize l_page and r_page */
    l_page = r_page = NULL;

    /* initialize l_node and r_node */
    l_node = r_node = NULL;

    /*@ Cache the left node information in local variables */
    MAKE_PAGEID(l_root, finfo->fid.volNo, list->entry[0].spid);
    if (l_root.pageNo != NIL) { 
	e = BfM_getAndFixBuffer(handle, &l_root, M_FREE, &l_page_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

        l_page = (L_O_T_INodePage*)l_page_BCBP->bufPagePtr;
        l_node = &l_page->node;

    } else if (anode != NULL) {
        l_node = anode;         /* root is with header */
    }

    /*@ Cache the right node information in local variables */
    MAKE_PAGEID(r_root, finfo->fid.volNo, list->entry[1].spid);
    if (r_root.pageNo != NIL) { 
	e = BfM_getAndFixBuffer(handle, &r_root, M_FREE, &r_page_BCBP, PAGE_BUF);
	if (e < 0) {
	    if (l_page != NULL) (Four)BfM_unfixBuffer(handle, l_page_BCBP, PAGE_BUF); 
	    ERR(handle, e);
	}

	r_page = (L_O_T_INodePage *)r_page_BCBP->bufPagePtr;
        r_node = &r_page->node;
    }

    /*
     * When the root of the left subtree is not NIL.
     */
    if (l_node != NULL) { 

	/*@ height of current node */
	height = l_node->header.height;

	/* get the index of the child node containing 'from' byte */
	c_l_idx = lot_SearchInNode(handle, l_node, from);

	c_l_count = lot_GetCount(handle, l_node, c_l_idx);
	c_from = from - ((c_l_idx == 0) ? 0:l_node->entry[c_l_idx-1].count);

	if (c_from == 0) {
	    /* the whole subtree is to be deleted */
	    c_l_root_ptr = (PageID *)NULL;
	} else {
	    MAKE_PAGEID(c_l_root, finfo->fid.volNo, l_node->entry[c_l_idx].spid);
	    c_l_root_ptr = &c_l_root;
	}

	if (r_node == NULL) { 

	    /* The right subtree root is NIL */

	    if (to == TO_END) {
		/* The whole right subtree was deleted at the ancestor. */
		/* So the right subtree root is NIL */
		c_to = TO_END;
		c_r_root_ptr = (PageID *)NULL;

	    } else {
		/* We don't yet pass the least common ancstor */

		/* get the index of the child node containing 'to' byte */
		c_r_idx = lot_SearchInNode(handle, l_node, to);

		c_r_count = lot_GetCount(handle, l_node, c_r_idx);
		c_to = to - ((c_r_idx == 0) ? 0:l_node->entry[c_r_idx-1].count);

		if (c_to == lot_GetCount(handle, l_node, c_r_idx) - 1) {

		    c_to = TO_END;
		    c_r_root_ptr = (PageID*)NULL;

		} else {
		    if (c_l_idx == c_r_idx && c_l_root_ptr != NULL) {
			c_r_root_ptr = (PageID*)NULL; /* c_from != 0 */

		    } else {
			MAKE_PAGEID(c_r_root, finfo->fid.volNo, l_node->entry[c_r_idx].spid);
			c_r_root_ptr = &c_r_root;
		    }
		}
	    }
	}
    } else
	c_l_root_ptr = (PageID *)NULL;

    /*@
     * When the root of the right subtree is not NIL.
     */
    if (r_node != NULL) { 

	/* height of current node: replicated line for l_root is NIL */
	height = r_node->header.height;

	c_r_idx = lot_SearchInNode(handle, r_node, to);

	c_r_count = lot_GetCount(handle, r_node, c_r_idx);
	c_to = to - ((c_r_idx == 0) ? 0:r_node->entry[c_r_idx-1].count);

	if (c_to == lot_GetCount(handle, r_node, c_r_idx) - 1) {
	    /* the whole subtree is to be deleted */
	    c_r_root_ptr = (PageID *)NULL;
	    c_to = TO_END;

	} else {
	    MAKE_PAGEID(c_r_root, finfo->fid.volNo, r_node->entry[c_r_idx].spid);
	    c_r_root_ptr = &c_r_root;

	}
    }

    /*@
     * Here is the delete operation.
     */
    /* drop all subtrees whose all bytes to be deleted */
    if (l_node != NULL) { 
        /* change the count to subtree count instead of accumulated count */
        for (i = l_node->header.nEntries - 1; i > 0; i--)
            l_node->entry[i].count -= l_node->entry[i-1].count;

	l_begin = (c_l_root_ptr == NULL) ? c_l_idx:(c_l_idx+1);

	if (r_node != NULL || to == TO_END) 
	    l_end = l_node->header.nEntries - 1;
	else
	    l_end = (c_to == TO_END) ? c_r_idx:c_r_idx-1;

	for (i =l_begin; i <= l_end; i++){
            assert(i < l_node->header.nEntries);

	    MAKE_PAGEID(pid, finfo->fid.volNo, l_node->entry[i].spid);

	    if (height > 1) {
                if (finfo->tmpFileFlag)
                    e = lot_DropTree(handle, xactEntry, &pid, TRUE, logParam);
                else
                    e = TM_XT_AddToDeallocList(handle, xactEntry, &pid, NULL, NULL, DL_LRGOBJ); 
	    } else {
                e = RDsM_FreeTrain(handle, xactEntry, &pid, TRAINSIZE2, finfo->tmpFileFlag, logParam);
            }
	    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
	}
    }

    if (r_node != NULL) { 

        /* change the count to subtree count instead of accumulated count */
        for (i = r_node->header.nEntries - 1; i > 0; i--)
            r_node->entry[i].count -= r_node->entry[i-1].count;

	/* if c_to == TO_END, the [c_r_idx]-th subtree is also droped */
	r_end = (c_to == TO_END) ? c_r_idx:c_r_idx-1;
	for (i = 0; i <= r_end; i++) {
	    MAKE_PAGEID(pid, finfo->fid.volNo, r_node->entry[i].spid);
	    if (height > 1) {
                if (finfo->tmpFileFlag)
                    e = lot_DropTree(handle, xactEntry, &pid, TRUE, logParam);
                else
                    e = TM_XT_AddToDeallocList(handle, xactEntry, &pid, NULL, NULL, DL_LRGOBJ); 
	    } else {
                e = RDsM_FreeTrain(handle, xactEntry, &pid, TRAINSIZE2, finfo->tmpFileFlag, logParam);
            }
	    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
	}
    }

    /* set the initial which value:
       If there isn't a underflowed node, we may choose LEFT node */
    if (l_node == NULL) 
	*which = RIGHT;
    else
	*which = LEFT;

    /* delete the entries in the nodes at the current level. */
    /* If the subtree's part is to be deleted, delete it first */
    if (c_l_root_ptr == NULL && c_r_root_ptr == NULL) {
	/* Whole subtrees are deleted in both left and right cut paths */

	if(l_node != NULL) { 
            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && l_page != NULL) {
                deleteInternalEntriesInfo.start = c_l_idx;
                deleteInternalEntriesInfo.nEntries = ((r_node != NULL || to == TO_END) ? (l_node->header.nEntries - c_l_idx) : (c_r_idx - c_l_idx + 1));
                deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                      deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &l_node->entry[c_l_idx]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                /* mark the lsn in the page */
                l_page->header.lsn = lsn;
                l_page->header.logRecLen = logRecLen;
            }

	    if (r_node != NULL || to == TO_END) { 
		/* all entries in [c_l_idx, ~) are deleted. */
		l_node->header.nEntries = c_l_idx;

	    } else {
		/* delete the entries in [c_l_idx, c_r_idx] */
		n = l_node->header.nEntries - (c_r_idx+1);
		memmove(&l_node->entry[c_l_idx], &l_node->entry[c_r_idx+1],
		      n*sizeof(L_O_T_INodeEntry));
		l_node->header.nEntries -= (c_r_idx - c_l_idx + 1);
	    }
	}

	if (r_node != NULL) { 
	    /* delete the entries in [0, c_r_idx] */

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                deleteInternalEntriesInfo.start = 0;
                deleteInternalEntriesInfo.nEntries = c_r_idx + 1;
                deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      r_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                      (c_r_idx+1)*sizeof(L_O_T_INodeEntry), &r_node->entry[0]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                /* mark the lsn in the page */
                r_page->header.lsn = lsn;
                r_page->header.logRecLen = logRecLen;
            }

	    n = r_node->header.nEntries - (c_r_idx+1);
	    memmove(&r_node->entry[0], &r_node->entry[c_r_idx+1], n*sizeof(L_O_T_INodeEntry));
	    r_node->header.nEntries -= (c_r_idx+1);
	}

	c_uf = FALSE;
	c_mf = FALSE;

    } else {

	/* construt L_O_T_ItemList */
	c_list.nEntries = 2;
	c_list.entry = tmpEntries;
	c_list.entry[0].spid = (c_l_root_ptr == NULL) ? NIL:c_l_root_ptr->pageNo;
	c_list.entry[1].spid = (c_r_root_ptr == NULL) ? NIL:c_r_root_ptr->pageNo;

	if (height > 1) {
	    /* recursive call */

	    e = lot_DeleteFromObject(handle, xactEntry, finfo, &c_list, NULL, c_from,
				     c_to, &c_which, &c_uf, &c_mf, path, logParam);

	} else {
	    /* The children are leaf nodes */

            c_list.entry[0].count = c_l_count;
            c_list.entry[1].count = c_r_count;

	    c_which = (c_l_root_ptr == NULL) ? RIGHT:LEFT;
	    e = lot_DeleteFromLeaf(handle, xactEntry, finfo, &c_list,
				   c_from, c_to, &c_uf, &c_mf, logParam);
	}

	if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	if (c_mf) {
	    /*@ two child nodes are merged */
	    /* Assert that l_root is not NIL PageID */

	    if (r_node != NULL) { 
                /*
                 * Write log record.
                 */
                if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                    /* left page */
                    if (l_page != NULL) {
                        deleteInternalEntriesInfo.start = c_l_idx + 1;
                        deleteInternalEntriesInfo.nEntries = l_node->header.nEntries - (c_l_idx+1);
                        deleteInternalEntriesInfo.nDeletedBytesBefore = l_node->entry[c_l_idx].count - c_list.entry[0].count;
                        deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                              l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                              deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &l_node->entry[c_l_idx+1]);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                        /* mark the lsn in the page */
                        l_page->header.lsn = lsn;
                        l_page->header.logRecLen = logRecLen;
                    }

                    /* right page */
                    deleteInternalEntriesInfo.start = 0;
                    deleteInternalEntriesInfo.nEntries = c_r_idx + 1;
                    deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                    deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                    LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                          r_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                          deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &r_node->entry[0]);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                    /* mark the lsn in the page */
                    r_page->header.lsn = lsn;
                    r_page->header.logRecLen = logRecLen;
                }

		l_node->header.nEntries = c_l_idx + 1;
		l_node->entry[c_l_idx].count = c_list.entry[0].count;

		/* delete the entries [0,c_r_idx] in r_node */
		r_node->header.nEntries -= (c_r_idx+1);
		n = r_node->header.nEntries;
		memmove(&r_node->entry[0], &r_node->entry[c_r_idx+1], n*sizeof(L_O_T_INodeEntry));

	    } else {
		/* Both root nodes of subtrees are in the same node, l_node */

                /*
                 * Write log record.
                 */
                if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && l_page != NULL) {
                    deleteInternalEntriesInfo.start = c_l_idx + 1;
                    deleteInternalEntriesInfo.nEntries = c_r_idx - c_l_idx;
                    deleteInternalEntriesInfo.nDeletedBytesBefore = l_node->entry[c_l_idx].count - c_list.entry[0].count;
                    deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                    LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                          l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                          deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &l_node->entry[c_l_idx+1]);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                    /* mark the lsn in the page */
                    l_page->header.lsn = lsn;
                    l_page->header.logRecLen = logRecLen;
                }

		/* delete the entries [c_l_idx+1, c_r_idx] in l_node */
		n = l_node->header.nEntries - (c_r_idx+1);
		memmove(&l_node->entry[c_l_idx+1], &l_node->entry[c_r_idx+1], n*sizeof(L_O_T_INodeEntry));
		l_node->header.nEntries -= (c_r_idx - c_l_idx);

		/* adjust the count fields in l_node */
		l_node->entry[c_l_idx].count = c_list.entry[0].count;
	    }

	} else {
	    if (l_node != NULL) { 
		if (r_node != NULL || to == TO_END) { 
                    /*
                     * Write log record.
                     */
                    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && l_page != NULL) {
                        deleteInternalEntriesInfo.start = l_begin;
                        deleteInternalEntriesInfo.nEntries = l_node->header.nEntries - l_begin;
                        deleteInternalEntriesInfo.nDeletedBytesBefore = l_node->entry[c_l_idx].count - c_list.entry[0].count;
                        deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                              l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                              deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &l_node->entry[c_l_idx+1]);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                        /* mark the lsn in the page */
                        l_page->header.lsn = lsn;
                        l_page->header.logRecLen = logRecLen;
                    }

		    /* all entries [l_begin, ~) are deleted in l_node */
		    l_node->header.nEntries = l_begin;

		    /* adjust the count fields */
		    /* if l_begin is 0, c_l_root_ptr has NULL. And
		     * c_r_root_ptr is NULL from above condition.
		     * It cannot be occurred
		     */
		    if (l_begin != c_l_idx)
                        l_node->entry[c_l_idx].count = c_list.entry[0].count;

		} else {
                    if (c_l_idx == c_r_idx) {
                        assert(l_begin > l_end);
                        /*
                         * Write log record.
                         */
                        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && l_page != NULL) {
                            updateCountFieldsInfo.start = c_l_idx;
                            updateCountFieldsInfo.delta = -(to - from + 1);

                            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                                  LOG_ACTION_LOT_UPDATE_COUNT_FIELDS, LOG_REDO_UNDO,
                                                  l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                                  sizeof(LOG_Image_LOT_UpdateCountFields_T), &updateCountFieldsInfo);

                            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                            if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                            /* mark the lsn in the page */
                            l_page->header.lsn = lsn;
                            l_page->header.logRecLen = logRecLen;
                        }

			/* common cut-path on the child node */
                        l_node->entry[c_l_idx].count -= (to - from + 1);

                    } else {
                        /*
                         * Write log record.
                         */
                        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && l_page != NULL) {
                            deleteInternalEntriesInfo.start = l_begin;
                            deleteInternalEntriesInfo.nEntries = l_end - l_begin + 1;
                            deleteInternalEntriesInfo.nDeletedBytesBefore = ((c_l_idx == l_begin) ? 0:(l_node->entry[c_l_idx].count - c_list.entry[0].count));
                            deleteInternalEntriesInfo.nDeletedBytesAfter = ((c_r_idx == l_end) ? 0:(l_node->entry[c_r_idx].count - c_list.entry[1].count));

                            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                                  LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                                  l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                                  sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                                  deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &l_node->entry[c_l_idx+1]);

                            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                            if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                            /* mark the lsn in the page */
                            l_page->header.lsn = lsn;
                            l_page->header.logRecLen = logRecLen;
                        }


			/* adjust the count fields */
			if (c_l_idx != l_begin) l_node->entry[c_l_idx].count = c_list.entry[0].count;
                        if (c_r_idx != l_end) l_node->entry[c_r_idx].count = c_list.entry[1].count;

                        if (l_begin <= l_end) {
                            /* delete the entries [l_begin, l_end] in l_node */
                            n = l_node->header.nEntries - (l_end+1);
                            memmove(&l_node->entry[l_begin], &l_node->entry[l_end+1],
                                    n*sizeof(L_O_T_INodeEntry));
                            l_node->header.nEntries -= (l_end - l_begin +1);
                        }

                    }
		}
	    }

	    if (r_node != NULL) { 
                /* # of deleted bytes in partially deleted subtree */
		deletedBytes = r_node->entry[c_r_idx].count - c_list.entry[1].count;

                /*
                 * Write log record.
                 */
                if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                    deleteInternalEntriesInfo.start = 0;
                    deleteInternalEntriesInfo.nEntries = c_r_idx;
                    deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                    deleteInternalEntriesInfo.nDeletedBytesAfter = deletedBytes;

                    LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                          r_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                          c_r_idx*sizeof(L_O_T_INodeEntry), &r_node->entry[0]);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                    /* mark the lsn in the page */
                    r_page->header.lsn = lsn;
                    r_page->header.logRecLen = logRecLen;
                }

		/* delete the entries [0,c_r_idx-1] */
		r_node->header.nEntries -= c_r_idx;
		memmove(&r_node->entry[0], &r_node->entry[c_r_idx],
		      (r_node->header.nEntries)*sizeof(L_O_T_INodeEntry));

		/* adjust the count fields */
                r_node->entry[0].count -= deletedBytes;

		/* change the which value */
		if (c_l_root_ptr == NULL)
		    *which = RIGHT;
		else
		    *which = c_which;
	    }
	}
    }

    /* Set the child node index:
       Here must come the setting. The value may be changed after this. */
    if (*which == LEFT)
	p_c_idx = c_l_idx;
    else
	p_c_idx = 0;

    l_uf = r_uf = FALSE;

    if (l_node != NULL) { 
        /*@ adjust the count fields */
        for (i = 1; i < l_node->header.nEntries; i++)
            l_node->entry[i].count += l_node->entry[i-1].count;

	list->entry[0].count = (l_node->header.nEntries == 0) ?
	    0:l_node->entry[l_node->header.nEntries-1].count;
	l_uf = (l_node->header.nEntries < LOT_HALFENTRIES) ? TRUE:FALSE;

    }

    if (r_node != NULL) { 
        /*@ adjust the count fields */
        for (i = 1; i < r_node->header.nEntries; i++)
            r_node->entry[i].count += r_node->entry[i-1].count;

	list->entry[1].count = (r_node->header.nEntries == 0) ?
	    0:r_node->entry[r_node->header.nEntries-1].count;
	r_uf = (r_node->header.nEntries < LOT_HALFENTRIES) ? TRUE:FALSE;
    }

    if (l_uf || r_uf) {
	if (l_node != NULL && r_node != NULL) { 
	    e = lot_MergeOrDistribute(handle, xactEntry, finfo, list, &movedEntries, uf, mf, logParam);
	    if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	    if(*mf) {
		/* merge was occured. */

		if(*which == RIGHT) {
		    *which = LEFT;
		    p_c_idx++;
		}

	    } else {
		/* distribution was occured. */

		if((*which == RIGHT) && l_uf) {
		    *which = LEFT;
		    p_c_idx++;

		} else if (*which == LEFT && r_uf) {
		    *which = RIGHT;
		    p_c_idx = movedEntries - 1;
		}
	    }

	} else {
	    *uf = TRUE;
	    *mf = FALSE;
	}

    } else {
	*uf = FALSE;
	*mf = FALSE;
    }

    /*
     * To free the buffer is responsibility of the caller.
     * Actually the buffer will be freed in lot_RebalanceTree( ).
     */
    if (l_node != NULL && *which != LEFT) { 

        if (l_page != NULL) {
            l_page_BCBP->dirtyFlag = 1;

            e = BfM_unfixBuffer(handle, l_page_BCBP, PAGE_BUF);
            l_page = NULL;
            if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
        }
    }

    if (r_node != NULL && *which != RIGHT) { 
        assert(r_page != NULL);

        r_page_BCBP->dirtyFlag = 1;

        e = BfM_unfixBuffer(handle, r_page_BCBP, PAGE_BUF);
        r_page = NULL;
        if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
    }

    /* register the current node in path */
    if (*which == LEFT) {
        if (l_page != NULL)
            e = lot_PushPath(handle, path, l_page_BCBP, NULL, p_c_idx, c_uf);
        else
            e = lot_PushPath(handle, path, NULL, anode, p_c_idx, c_uf);
	if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

    } else {	/* r_root is not NIL PageID */
	e = lot_PushPath(handle, path, r_page_BCBP, NULL, p_c_idx, c_uf);
	if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
    }

    return(eNOERROR);

  LABEL_Error:
    if (l_page != NULL) (Four)BfM_unfixBuffer(handle, l_page_BCBP, PAGE_BUF);
    if (r_page != NULL) (Four)BfM_unfixBuffer(handle, r_page_BCBP, PAGE_BUF);

    return(e);


} /* lot_DeleteFromObject() */
