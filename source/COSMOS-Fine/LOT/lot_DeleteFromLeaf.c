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
 * Module: lot_DeleteFromLeaf.c
 *
 * Description:
 *
 * Exports:
 *  Four lot_DeleteFromLeaf(Four, DataFileInfo*, L_O_T_ItemList*, Four,
 *                          Four, Boolean*, Boolean*)
 *
 * Note:
 *  The parameters from and to are offsets in the leaf node.
 *  They are different from the length. So that to is 0 means 1 byte is
 *  to be deleted.
 */


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


/*
 * Internal Function Prototypes
 */
Four lot_DistributeLeaf(Four, XactTableEntry_T*, L_O_T_LNode*, Four*, L_O_T_LNode*, Four*, LogParameter_T*);


/*@================================
 * lot_DeleteFromLeaf( )
 *================================*/
/*
 * Function: Four lot_DeleteFromLeaf(Four, DataFileInfo*, L_O_T_ItemList*, Four,
 *                                   Four, Boolean*, Boolean*)
 *
 * Description:
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 *
 * Note:
 *  The parameter 'finfo' is required for passing the volume number.
 *  The volume number is necessary because the large object tree uses ShortPageID.
 */
Four lot_DeleteFromLeaf(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    L_O_T_ItemList *list,	/* INOUT list of two leaf nodes */
    Four     from,		/* IN starting offset of deletion */
    Four     to,		/* IN ending offset of deletion */
    Boolean  *uf,		/* OUT underflow flag */
    Boolean  *mf,		/* OUT TRUE if merge occur */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    PageID l_root;		/* root of left cut-path subtree */
    PageID r_root;		/* root of right cut-path subtree */
    L_O_T_LNode *l_node;	/* pointer to the left leaf node */
    Buffer_ACC_CB *l_node_BCBP;
    L_O_T_LNode *r_node;	/* pointer to the right leaf node */
    Buffer_ACC_CB *r_node_BCBP;
    Four l_count;		/* # of bytes in left leaf node */
    Four r_count;		/* # of bytes in right leaf node */
    Boolean l_f;		/* underflow flag of left leaf node */
    Boolean r_f;		/* underflow flag of right leaf node */
    Four n;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_ModifyLeafData_T modifyLeafDataInfo; /* train data info */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1,
	     ("lot_DeleteFromLeaf(handle, finfo, list, from, to, uf, mf)",
	      finfo, list, from, to, uf, mf));


    l_f = r_f = FALSE;

    l_node = r_node = NULL;     /* initialize for error handling */

    /*@ Cache the left leaf node information in local variables */
    MAKE_PAGEID(l_root, finfo->fid.volNo, list->entry[0].spid);

    /*@ Cache the right leaf node information in local variables */
    MAKE_PAGEID(r_root, finfo->fid.volNo, list->entry[1].spid);

    /* read the left leaf node */
    if (l_root.pageNo != NIL) { 
	l_count = list->entry[0].count;
	e = BfM_getAndFixBuffer(handle, &l_root, M_FREE, &l_node_BCBP, TRAIN_BUF);
	if (e < eNOERROR) ERR(handle, e);

	l_node = (L_O_T_LNode *)l_node_BCBP->bufPagePtr;
    }

    /* read the right leaf node */
    if (r_root.pageNo != NIL) { 
	r_count = list->entry[1].count;
	e = BfM_getAndFixBuffer(handle, &r_root, M_FREE, &r_node_BCBP, TRAIN_BUF);
	if (e < 0) {
	    if (l_root.pageNo == NIL) ERR(handle, e); 
	    else ERRB1(handle, e, l_node_BCBP, TRAIN_BUF);
	}
	r_node = (L_O_T_LNode *)r_node_BCBP->bufPagePtr;
    }

    /*@ delete data from the left leaf node */
    if (l_root.pageNo != NIL) { 

	if (r_root.pageNo == NIL && to != TO_END) { 
	    /* In this case, 'to' indicates the ending offset of deletion
	       in left leaf node */

            /* n: # of bytes after the deleted portion */
	    n = l_count - (to+1);

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                modifyLeafDataInfo.start = from;
                modifyLeafDataInfo.length = to - from + 1;
                modifyLeafDataInfo.oldTotalLength = l_count;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_DELETE_DATA, LOG_REDO_UNDO,
                                      l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                      modifyLeafDataInfo.length, &l_node->data[from]); 

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                /* mark the lsn in the page */
                l_node->header.lsn = lsn;
                l_node->header.logRecLen = logRecLen;
            }

	    /* fill the erased part by moving the upper part forward */
	    memmove(&l_node->data[from], &l_node->data[to+1], n);

	    l_count -= (to - from + 1);

	} else {
            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_WRITE_DATA, LOG_UNDO_ONLY,
                                      l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(Four), &from,
                                      0, NULL,
                                      l_count-from, &l_node->data[from]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                /* mark the lsn in the page */
                l_node->header.lsn = lsn;
                l_node->header.logRecLen = logRecLen;
            }

	    /* We don't have to read the leaf node */
	    /* So we need to refine not to read in this case */
	    /* Notice, if merge occur then we should read this node. */
	    l_count = from;
	}

	/* Set the underflow flag of left leaf node */
	if (l_count < LOT_LNODE_HALFFREE) l_f = TRUE;
    }

    /*@ delete data from the right leaf node */
    if (r_root.pageNo != NIL) { 
	/* fill the erased part moving the others forward */
	n = r_count - (to + 1);

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            modifyLeafDataInfo.start = 0;
            modifyLeafDataInfo.length = to + 1;
            modifyLeafDataInfo.oldTotalLength = r_count;

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_DELETE_DATA, LOG_REDO_UNDO,
                                  r_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                  modifyLeafDataInfo.length, &r_node->data[0]); 

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

            /* mark the lsn in the page */
            r_node->header.lsn = lsn; 
            r_node->header.logRecLen = logRecLen; 
        }

	memmove(&r_node->data[0], &r_node->data[to+1], n);

	/* Decrement the # of bytes in the right leaf node */
	r_count -= (to + 1);

	/* set the underflow flag of right leaf node */
	if (r_count < LOT_LNODE_HALFFREE) r_f = TRUE;
    }

    if (l_root.pageNo == NIL) { 
	*uf = r_f;		/* underflow flag */
	*mf = FALSE;		/* no merge */
    } else if (r_root.pageNo == NIL) {
	*uf = l_f;		/* underflow flag */
	*mf = FALSE;		/* no merge */
    } else if (!l_f && !r_f) {
	*uf = FALSE;		/* no underflow */
	*mf = FALSE;		/* no merge */
    } else {
	/* Either left or right leaf node is underflow */

	if (l_count + r_count <= LOT_LNODE_MAXFREE) {
	    /* merge two leaf nodes into left leaf node */

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                /* left node */
                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                      l_root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(Four), &l_count,
                                      r_count, &r_node->data[0]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) { PRTERR(handle, e); goto LABEL_Error; }

                /* mark the lsn in the page */
                l_node->header.lsn = lsn;
                l_node->header.logRecLen = logRecLen;

                /* right node : no need to log becaue it will be deallocated */
            }

	    /* copy the data in right node into the leaf node */
	    memcpy(&l_node->data[l_count], &r_node->data[0], r_count);

	    l_count += r_count;
	    r_count = 0;

	    /* set the flags indicating the status */
	    *mf = TRUE;		/* two nodes were merged */
	    if (l_count < LOT_LNODE_HALFFREE)
		*uf = TRUE;	/* underflow occurs */
	    else
		*uf = FALSE;

	} else {
	    /* distribute the original data evenly into two nodes */

	    e = lot_DistributeLeaf(handle, xactEntry, l_node, &l_count, r_node, &r_count, logParam);
            if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	    /* set the flags indicating the status */
	    *mf = FALSE;
	    *uf = FALSE;
	}

    }

    /*@ write the local variables to the return parameters */
    list->entry[0].count = l_count;
    list->entry[1].count = r_count;

    if (l_root.pageNo != NIL) { 
	l_node_BCBP->dirtyFlag = 1;

	e = BfM_unfixBuffer(handle, l_node_BCBP, TRAIN_BUF);
	if (e < 0) { PRTERR(handle, e); l_node = NULL; goto LABEL_Error; }
    }

    if (r_root.pageNo != NIL) { 
        r_node_BCBP->dirtyFlag = 1;

        e = BfM_unfixBuffer(handle, r_node_BCBP, TRAIN_BUF);
        if (e < eNOERROR) ERR(handle, e);

	if (*mf) {
	    /* free the right node to disk manager */
            e = RDsM_FreeTrain(handle, xactEntry, &r_root, TRAINSIZE2, finfo->tmpFileFlag, logParam);
	    if (e < eNOERROR) ERR(handle, e);
	}
    }

    return(eNOERROR);

  LABEL_Error:
    if (l_node != NULL) (Four)BfM_unfixBuffer(handle, l_node_BCBP, TRAIN_BUF);
    if (r_node != NULL) (Four)BfM_unfixBuffer(handle, r_node_BCBP, TRAIN_BUF);
    return(e);

} /* lot_DeleteFromLeaf() */



/*@================================
 * lot_DistributeLeaf()
 *================================*/
/*
 * Function: Four lot_DistributeLeaf(PageID*, Four*, PageID*, Four*)
 *
 * Description:
 *  Distributes the data in two leaf nodes into the original nodes.
 *  We assume that the parameters are correct.
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 *
 * Side effects:
 */
Four lot_DistributeLeaf(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    L_O_T_LNode *l_node,        /* INOUT left leaf node */
    Four   *l_count,		/* INOUT # of bytes in left leaf node */
    L_O_T_LNode *r_node,        /* INOUT right leaf node */
    Four   *r_count,		/* INOUT # of bytes in right leaf node */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four totalBytes;		/* sum of bytes in two leaf nodes */
    Four bytesPerNode;		/* # of bytes distributed evenly */
    Four remainedBytes;		/* # of bytes after balancing */
    Four movedBytes;		/* # of bytes moved from left to right */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_ModifyLeafData_T modifyLeafDataInfo; /* train data info */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1, ("lot_DistributeLeaf()"));

    totalBytes = *l_count + *r_count;
    bytesPerNode = totalBytes/2;
    remainedBytes = totalBytes % 2;
    /* movedBytes is the # of bytes to be moved from the left to the right */
    /* If movedBytes is less than 0, it means that move is from right to left */
    movedBytes = *l_count - (bytesPerNode + ((remainedBytes > 0) ? 1:0));

    if (movedBytes > 0) {
	/* go from the left to the right */

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            Four start;

            /* left node */
            start = *l_count - movedBytes;
            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_WRITE_DATA, LOG_UNDO_ONLY,
                                  l_node->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Four), &start,
                                  0, NULL,
                                  movedBytes, &l_node->data[start]);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            l_node->header.lsn = lsn;
            l_node->header.logRecLen = logRecLen;

            /* right page */
            modifyLeafDataInfo.start = 0;
            modifyLeafDataInfo.length = movedBytes;
            modifyLeafDataInfo.oldTotalLength = *r_count;

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_INSERT_DATA, LOG_REDO_UNDO,
                                  r_node->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                  movedBytes, &l_node->data[start]);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            r_node->header.lsn = lsn;
            r_node->header.logRecLen = logRecLen;
        }

	/* prepare space in the right node */
	memmove(&r_node->data[movedBytes], &r_node->data[0], *r_count);
	/* copy data from the left to the right */
	memcpy(&r_node->data[0], &l_node->data[*l_count - movedBytes], movedBytes);
    } else {
	/* go from the right to the left */

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            /* left node */
            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                  l_node->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Four), l_count,
                                  -movedBytes, &r_node->data[0]);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            l_node->header.lsn = lsn;
            l_node->header.logRecLen = logRecLen;

            /* right node */
            modifyLeafDataInfo.start = 0;
            modifyLeafDataInfo.length = -movedBytes;
            modifyLeafDataInfo.oldTotalLength = *r_count;

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_DELETE_DATA, LOG_REDO_UNDO,
                                  r_node->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                  -movedBytes, &r_node->data[0]);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            r_node->header.lsn = lsn;
            r_node->header.logRecLen = logRecLen;
        }

	/* copy data from the right to the left */
	memcpy(&l_node->data[*l_count], &r_node->data[0], -movedBytes);
	/* zap the right node by moving the uncopied part */
	memmove(&r_node->data[0], &r_node->data[-movedBytes], *r_count+movedBytes);
    }

    *l_count -= movedBytes;
    *r_count += movedBytes;

    return(eNOERROR);

} /* lot_DistributeLeaf() */
