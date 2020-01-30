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
 * Module: lot_MergeOrDistributeLeaf.c
 *
 * Description:
 *  Merge or distributes bytes in two nodes.
 *
 * Export:
 *  Four lot_MergeOrDistributeLeaf(Four, DataFileInfo*, L_O_T_ItemList*,
 *                                 Boolean*, Boolean*)
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
 * lot_MergeOrDistributeLeaf( )
 *================================*/
/*
 * Function: Four lot_MergeOrDistributeLeaf(Four, ObjectID*, L_O_T_ItemList*,
 *                                          Boolean*, Boolean*)
 *
 * Description:
 *  Merge or distributes bytes in two nodes. If the bytes can be entered
 * into one leaf node then merge occur. Otherwise the redistribution will
 * be occurred.
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 *
 * Note:
 *  The parameter 'finfo' is required for passing the volume number.
 *  The volume number is necessary because the large object tree uses ShortPageID.
 */
Four lot_MergeOrDistributeLeaf(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    L_O_T_ItemList *list,	/* INOUT information of two leaf nodes */
    Boolean        *uf,		/* OUT underflow flag */
    Boolean        *mf,		/* OUT merge flag */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four   e;			/* error number */
    PageID l_pid;		/* PageID of the left node */
    PageID r_pid;		/* PageID of the right node */
    L_O_T_LNode *l_node;	/* pointer to the left node */
    Buffer_ACC_CB *l_node_BCBP;
    L_O_T_LNode *r_node;	/* pointer to the right node */
    Buffer_ACC_CB *r_node_BCBP;
    Four totalBytes;		/* sum of bytes in two node */
    Four l_count;		/* # of original bytes in left node */
    Four r_count;		/* # of original bytes in right node */
    Four movedBytes;		/* # of bytes moved from left to right */
    Four i;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_ModifyLeafData_T modifyLeafDataInfo; /* train data info */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1,
	     ("lot_MergeOrDistributeLeaf(handle, finfo=%P, list=%P, uf=%P, mf=%P)",
	      finfo, list, uf, mf));


    MAKE_PAGEID(l_pid, finfo->fid.volNo, list->entry[0].spid);

    /*@ read the page into the buffer */
    e = BfM_getAndFixBuffer(handle, &l_pid, M_FREE, &l_node_BCBP, TRAIN_BUF);
    if (e < eNOERROR) ERR(handle, e);

    l_node = (L_O_T_LNode *)l_node_BCBP->bufPagePtr;

    MAKE_PAGEID(r_pid, finfo->fid.volNo, list->entry[1].spid);

    e = BfM_getAndFixBuffer(handle, &r_pid, M_FREE, &r_node_BCBP, TRAIN_BUF);
    if (e < eNOERROR) ERRB1(handle, e, l_node_BCBP, TRAIN_BUF);

    r_node = (L_O_T_LNode *)r_node_BCBP->bufPagePtr;

    l_count = list->entry[0].count;
    r_count = list->entry[1].count;

    /*@ merge or distribute */
    if (l_count + r_count<= LOT_LNODE_MAXFREE) {
	/* merge two nodes into the left node */

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                  l_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Four), &l_count,
                                  r_count, &r_node->data[0]);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB2(handle, e, l_node_BCBP, TRAIN_BUF, r_node_BCBP, TRAIN_BUF);

            /* mark the lsn in the page */
            l_node->header.lsn = lsn;
            l_node->header.logRecLen = logRecLen;
        }

	memcpy(&l_node->data[l_count], &r_node->data[0], r_count);

	list->entry[0].count += r_count;
	list->entry[1].count = 0;

	*mf = TRUE;
	*uf = (list->entry[0].count < LOT_LNODE_HALFFREE) ? TRUE:FALSE;

    } else {
	/* distribute the bytes evenly */

	totalBytes = l_count + r_count;

	/* movedBytes is the # of bytes moved from the left node
	   to the right node */
	movedBytes = l_count - (totalBytes/2 + totalBytes%2);

	if (movedBytes > 0) {
	    /* move the bytes form the left node to the right node */

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                Four start;

                /* left page */
                start = l_count - movedBytes;
                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_WRITE_DATA, LOG_UNDO_ONLY,
                                      l_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(Four), &start,
                                      0, NULL,
                                      movedBytes, &l_node->data[start]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_node_BCBP, TRAIN_BUF, r_node_BCBP, TRAIN_BUF);

                /* mark the lsn in the page */
                l_node->header.lsn = lsn;
                l_node->header.logRecLen = logRecLen;

                /* right page */
                modifyLeafDataInfo.start = 0;
                modifyLeafDataInfo.length = movedBytes;
                modifyLeafDataInfo.oldTotalLength = r_count;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_INSERT_DATA, LOG_REDO_UNDO,
                                      r_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                      movedBytes, &l_node->data[start]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_node_BCBP, TRAIN_BUF, r_node_BCBP, TRAIN_BUF);

                /* mark the lsn in the page */
                r_node->header.lsn = lsn;
                r_node->header.logRecLen = logRecLen;
            }

	    memmove(&r_node->data[movedBytes], &r_node->data[0], r_count);
	    memcpy(&r_node->data[0], &l_node->data[l_count-movedBytes], movedBytes);

	} else {
	    /* move the bytes form the right node to the left node */

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                /* left page */
                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                      l_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(Four), &l_count,
                                      -movedBytes, &r_node->data[0]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_node_BCBP, TRAIN_BUF, r_node_BCBP, TRAIN_BUF);

                /* mark the lsn in the page */
                l_node->header.lsn = lsn;
                l_node->header.logRecLen = logRecLen;

                /* right page */
                modifyLeafDataInfo.start = 0;
                modifyLeafDataInfo.length = -movedBytes;
                modifyLeafDataInfo.oldTotalLength = r_count;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_DELETE_DATA, LOG_REDO_UNDO,
                                      r_pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                      -movedBytes, &r_node->data[0]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, l_node_BCBP, TRAIN_BUF, r_node_BCBP, TRAIN_BUF);

                /* mark the lsn in the page */
                r_node->header.lsn = lsn;
                r_node->header.logRecLen = logRecLen;
            }

	    memcpy(&l_node->data[l_count], &r_node->data[0], -movedBytes);
	    memmove(&r_node->data[0], &r_node->data[-movedBytes], r_count+movedBytes);
	}

	list->entry[0].count -= movedBytes;
	list->entry[1].count += movedBytes;

	*mf = FALSE;
	*uf = FALSE;
    }

    l_node_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, l_node_BCBP, TRAIN_BUF);
    if (e < eNOERROR) ERRB1(handle, e, r_node_BCBP, TRAIN_BUF);

    r_node_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, r_node_BCBP, TRAIN_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (*mf) {
        e = RDsM_FreeTrain(handle, xactEntry, &r_pid, TRAINSIZE2, finfo->tmpFileFlag, logParam);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* lot_MergeOrDistributeLeaf( ) */
