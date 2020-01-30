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
 * Module: lot_MergeOrDistribute.c
 *
 * Description:
 *  Merge or distribute the entries in the two internal nodes. If the entries
 * can be merged into one node, the entries are merged in the left node.
 * Otherwise the entries are evenly distributed into two nodes. If the number
 * of entries are odd, the left node has one more entry.
 *
 * Exports:
 *  Four lot_MergeOrDistribute(Four, DataFileInfo*, L_O_T_ItemList*,
 *                             Four*, Boolean*, Boolean*)
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
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_MergeOrDistribute( )
 *================================*/
/*
 * Function: Four lot_MergeOrDistribute(Four, DataFileInfo*, L_O_T_ItemList*,
 *                             Four*, Boolean*, Boolean*)
 *
 * Description:
 *  Merge or distribute the entries in the two internal nodes. If the entries
 * can be merged into one node, the entries are merged in the left node.
 * Otherwise the entries are evenly distributed into two nodes. If the number
 * of entries are odd, the left node has one more entry.
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 *
 * Note:
 *  The parameter 'finfo' is required for passing the volume number.
 *  The volume number is necessary because the large object tree uses ShortPageID.
 */
Four lot_MergeOrDistribute(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    L_O_T_ItemList *list,	/* INOUT information two nodes */
    Four       *movedEntries,	/* OUT # of entries moved from left to right */
    Boolean        *uf,		/* OUT underflow flag */
    Boolean        *mf,		/* OUT merge flag */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    PageID l_pid;		/* PageID of the left node */
    PageID r_pid;		/* PageID of the right node */
    L_O_T_INodePage *l_page;    /* pointer to the left page */
    L_O_T_INode *l_node;	/* pointer to the left node */
    Buffer_ACC_CB *l_page_BCBP;
    L_O_T_INodePage *r_page;	/* pointer to the right page */
    L_O_T_INode *r_node;	/* pointer to the right node */
    Buffer_ACC_CB *r_page_BCBP;
    Four totalEntries;		/* sum of # of entries in two node */
    Four l_nEntries;		/* # of original entries in left node */
    Four r_nEntries;		/* # of original entries in right node */
    Four i;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_ReplaceInternalEntries_T replaceInternalEntriesInfo;
    LOG_Image_LOT_DeleteInternalEntries_T deleteInternalEntriesInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1,
	     ("lot_MergeOrDistribute(handle, finfo=%P, list=%P, uf=%P, mf=%P)",
	      finfo, list, uf, mf));


    MAKE_PAGEID(l_pid, finfo->fid.volNo, list->entry[0].spid);
    e = BfM_getAndFixBuffer(handle, &l_pid, M_FREE, &l_page_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    l_page = (L_O_T_INodePage *)l_page_BCBP->bufPagePtr;
    l_node = &l_page->node;

    MAKE_PAGEID(r_pid, finfo->fid.volNo, list->entry[1].spid);
    e = BfM_getAndFixBuffer(handle, &r_pid, M_FREE, &r_page_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    r_page = (L_O_T_INodePage *)r_page_BCBP->bufPagePtr;
    r_node = &r_page->node;

    l_nEntries = l_node->header.nEntries;
    r_nEntries = r_node->header.nEntries;

    /*@ merge or distribute */
    if (l_nEntries + r_nEntries <= LOT_MAXENTRIES) {
	/* merge two nodes into the left node */

	memcpy((char*)&l_node->entry[l_nEntries],
	       (const char*)&r_node->entry[0],
	       r_nEntries*sizeof(L_O_T_INodeEntry));
	l_node->header.nEntries += r_nEntries;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            /* change the count to subtree count instead of accumulated count */
            /* this change applys to only moved entries */
            for (i = l_node->header.nEntries - 1; i > l_nEntries; i--)
                l_node->entry[i].count -= l_node->entry[i-1].count;

	    replaceInternalEntriesInfo.start = l_nEntries;
	    replaceInternalEntriesInfo.nNewEntries = r_nEntries;
	    replaceInternalEntriesInfo.nOldEntries = 0;

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                  l_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                  r_nEntries*sizeof(L_O_T_INodeEntry), &l_node->entry[l_nEntries],
                                  0, NULL);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB2(handle, e, l_page_BCBP, PAGE_BUF, r_page_BCBP, PAGE_BUF);

            /* mark the lsn in the page */
            l_page->header.lsn = lsn;
            l_page->header.logRecLen = logRecLen;

            /* restore the count fields */
            for (i = l_nEntries+1; i < l_node->header.nEntries; i++)
                l_node->entry[i].count += l_node->entry[i-1].count;
        }

	/*@ adjust the count fields */
	for (i = l_nEntries; i < l_nEntries+r_nEntries; i++)
	    l_node->entry[i].count += l_node->entry[l_nEntries-1].count;

	list->entry[0].count += list->entry[1].count;
	list->entry[1].count = 0;

	*mf = TRUE;
	*uf = (l_node->header.nEntries < LOT_HALFENTRIES) ? TRUE:FALSE;
	*movedEntries = -r_nEntries;

    } else {
	/* distribute the entries evenly */

	totalEntries = l_nEntries + r_nEntries;
	/* movedEntries is the # of entries moved from the left node
	 * to the right node */
	*movedEntries = l_nEntries - (totalEntries/2 + totalEntries%2);

	if (*movedEntries > 0) {
	    /* move the entries form the left node to the right node */

	    memmove(&r_node->entry[*movedEntries], &r_node->entry[0],
		    r_nEntries*sizeof(L_O_T_INodeEntry));
	    memcpy((char*)&r_node->entry[0],
		   (const char*)&l_node->entry[l_nEntries-(*movedEntries)],
		   (*movedEntries)*sizeof(L_O_T_INodeEntry));

	    /*@ adjust the count fields */
	    for (i = 0; i < *movedEntries; i++)
		r_node->entry[i].count -= l_node->entry[l_nEntries-(*movedEntries)-1].count;

	    for (i = *movedEntries; i < r_nEntries+(*movedEntries); i++)
		r_node->entry[i].count += r_node->entry[(*movedEntries)-1].count;

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                /* change the count to subtree count instead of accumulated count */
                /* this change applys to only moved entries */
                for (i = *movedEntries - 1; i > 0; i--)
                    r_node->entry[i].count -= r_node->entry[i-1].count;

                /* left page */
                deleteInternalEntriesInfo.start = l_nEntries - *movedEntries;
                deleteInternalEntriesInfo.nEntries = *movedEntries;
                deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      l_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                      (*movedEntries)*sizeof(L_O_T_INodeEntry), &r_node->entry[0]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_page_BCBP, PAGE_BUF, r_page_BCBP, PAGE_BUF);

                /* mark the lsn in the page */
                l_page->header.lsn = lsn;
                l_page->header.logRecLen = logRecLen;

                /* right page */
                replaceInternalEntriesInfo.start = 0;
                replaceInternalEntriesInfo.nNewEntries = *movedEntries;
                replaceInternalEntriesInfo.nOldEntries = 0;

                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      r_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                      (*movedEntries)*sizeof(L_O_T_INodeEntry), &r_node->entry[0],
                                      0, NULL);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_page_BCBP, PAGE_BUF, r_page_BCBP, PAGE_BUF);

                /* mark the lsn in the page */
                r_page->header.lsn = lsn;
                r_page->header.logRecLen = logRecLen;

                /* restore the count */
                for (i = 1; i < *movedEntries; i++)
                    r_node->entry[i].count += r_node->entry[i-1].count;
            }

	} else {
	    /* move the entries form the right node to the left node */

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                /* change the count to subtree count instead of accumulated count */
                /* this change applys to only moved entries */
                for (i = -(*movedEntries) - 1; i > 0; i--)
                    r_node->entry[i].count -= r_node->entry[i-1].count;

                /* left page */
                replaceInternalEntriesInfo.start = l_nEntries;
                replaceInternalEntriesInfo.nNewEntries = -(*movedEntries);
                replaceInternalEntriesInfo.nOldEntries = 0;

                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      l_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_ReplaceInternalEntries_T), &replaceInternalEntriesInfo,
                                      replaceInternalEntriesInfo.nNewEntries*sizeof(L_O_T_INodeEntry), &r_node->entry[0],
                                      0, NULL);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_page_BCBP, PAGE_BUF, r_page_BCBP, PAGE_BUF);

                /* mark the lsn in the page */
                l_page->header.lsn = lsn;
                l_page->header.logRecLen = logRecLen;

                /* right page */
                deleteInternalEntriesInfo.start = 0;
                deleteInternalEntriesInfo.nEntries = -(*movedEntries);
                deleteInternalEntriesInfo.nDeletedBytesBefore = 0;
                deleteInternalEntriesInfo.nDeletedBytesAfter = 0;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES, LOG_REDO_UNDO,
                                      r_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_DeleteInternalEntries_T), &deleteInternalEntriesInfo,
                                      deleteInternalEntriesInfo.nEntries*sizeof(L_O_T_INodeEntry), &r_node->entry[0]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_page_BCBP, PAGE_BUF, r_page_BCBP, PAGE_BUF);

                /* mark the lsn in the page */
                r_page->header.lsn = lsn;
                r_page->header.logRecLen = logRecLen;

                /* restore the count */
                for (i = 1; i < -(*movedEntries); i++)
                    r_node->entry[i].count += r_node->entry[i-1].count;
            }

	    memcpy((char*)&l_node->entry[l_nEntries],
		   (const char*)&r_node->entry[0],
		   -(*movedEntries)*sizeof(L_O_T_INodeEntry));
	    memcpy((char*)&r_node->entry[0],
		   (const char*)&r_node->entry[-(*movedEntries)],
		   (r_nEntries+(*movedEntries))*sizeof(L_O_T_INodeEntry));

	    /* adjust the count fields */
	    for (i = 0; i < r_nEntries+(*movedEntries); i++)
		r_node->entry[i].count -= l_node->entry[l_nEntries-(*movedEntries)-1].count;
	    for (i = l_nEntries; i < l_nEntries-(*movedEntries); i++)
		l_node->entry[i].count += l_node->entry[l_nEntries-1].count;

	}

	list->entry[0].count = l_node->entry[l_nEntries-(*movedEntries)-1].count;
	list->entry[1].count = r_node->entry[r_nEntries+(*movedEntries)-1].count;
	l_node->header.nEntries -= *movedEntries;
	r_node->header.nEntries += *movedEntries;

	*mf = FALSE;
	*uf = FALSE;
    }

    l_page_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, l_page_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERRB1(handle, e, r_page_BCBP, PAGE_BUF);

    r_page_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, r_page_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (*mf) {
        /*
         * r_root was merged with l_root at lot_MergeOrDistribute
         */
        e = RDsM_FreeTrain(handle, xactEntry, &r_pid, PAGESIZE2, finfo->tmpFileFlag, logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* lot_MergeOrDistribute( ) */
