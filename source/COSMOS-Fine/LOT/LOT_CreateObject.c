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
 * Module: LOT_CreateObject.c
 *
 * Description:
 *  LOT_CreateObject( ) create a large object with given data.
 *
 * Exports:
 *  Four LOT_CreateObject(Four, DataFileInfo*, char*, Boolean*, Four, Four, char*, LogParameter_T*)
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
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * LOT_CreateObject( )
 *================================*/
/*
 * Function: Four LOT_CreateObject(Four, DataFileInfo*, char*, Boolean*, Four, Four, char*, LogParameter_T*)
 *
 * Description:
 *  LOT_CreateObject( ) creates a large object with given data.
 *
 * Returns:
 *  Error codes
 *    eBADOBJECTID_OM
 *    some errors caused by function calls
 */
Four LOT_CreateObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    PageID *nearPidForRoot,     /* IN where the root page is located */ 
    char *anodeOrRootPageNo,    /* OUT anode or root page no */
    Boolean *rootWithHdr_Flag,  /* OUT TRUE if root is with header */
    Four maxNodeLen,            /* IN node can grow at most to this value */
    Four length,                /* IN object data length */
    char *data,                 /* IN object data */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    PageID root;		/* new root page */
    PageID leafPid;		/* the newly created leaf node */
    L_O_T_INode *anode;		/* pointer to buffer holding the root node */
    L_O_T_INodePage *rootPage;  /* root page */
    Buffer_ACC_CB *rootPage_BCBP;
    L_O_T_LNode *leafNode;	/* pointer to buffer holding the leaf node */
    Buffer_ACC_CB *leafNode_BCBP;
    Four inodeUsedSize;         /* used size of internal node */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    SegmentID_T pageSegmentID;  /* page segment ID */
    SegmentID_T trainSegmentID; /* train segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1, ("LOT_CreateObject()"));


    /*@ allocate the new data page */
    e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &trainSegmentID, TRAINSIZE2);
    if (e < eNOERROR) ERR(handle, e);

    if (IS_NIL_SEGMENT_ID(&trainSegmentID) == TRUE) {
	e = RDsM_CreateSegment(handle, xactEntry, finfo->fid.volNo, &trainSegmentID, TRAINSIZE2, logParam);
	if (e < eNOERROR) ERR(handle, e);

	e = om_SetSegmentIDToCatOverlayForDataUsingDataFileInfo(handle, xactEntry, finfo, &trainSegmentID, TRAINSIZE2, logParam);
	if (e < eNOERROR) ERR(handle, e);
    }
    e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &trainSegmentID,
			 (PageID *)NULL, 1, TRAINSIZE2, FALSE, &leafPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*@ Read the new page into the buffer. */
    e = BfM_fixNewBuffer(handle, &leafPid, M_FREE, &leafNode_BCBP, TRAIN_BUF); 
    if (e < eNOERROR) ERR(handle, e);

    leafNode = (L_O_T_LNode *)leafNode_BCBP->bufPagePtr;

    LOT_INIT_LEAF_NODE(leafNode, finfo->fid, leafPid);

    memcpy(leafNode->data, data, length);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        Four start = 0;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_LOT_INIT_LEAF_NODE, LOG_REDO_ONLY,
                              leafPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(FileID), &finfo->fid);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, leafNode_BCBP, TRAIN_BUF);

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                              leafPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Four), &start,
                              length, leafNode->data);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, leafNode_BCBP, TRAIN_BUF);

        /* mark the lsn in the page */
        leafNode->header.lsn = lsn;
        leafNode->header.logRecLen = logRecLen;
    }

    leafNode_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, leafNode_BCBP, TRAIN_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * make the root node
     */
    anode = (L_O_T_INode*)anodeOrRootPageNo;

    /* set the node values */
    anode->header.height = 1;
    anode->header.nEntries = 1;
    anode->entry[0].spid = leafPid.pageNo;
    anode->entry[0].count = length;

    inodeUsedSize = LOT_INODE_USED_SIZE(anode);
    if (inodeUsedSize <= maxNodeLen) { /* root will be with header */
        *rootWithHdr_Flag = TRUE;

    } else { /* root will be allocated */
        *rootWithHdr_Flag = FALSE;

        e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &pageSegmentID, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

	e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &pageSegmentID,
			     nearPidForRoot, 1, PAGESIZE2, FALSE, &root, logParam);
	if (e < eNOERROR) ERR(handle, e);

	/* initialize the root node */
	e = BfM_getAndFixBuffer(handle, &root, M_FREE, &rootPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	rootPage = (L_O_T_INodePage *)rootPage_BCBP->bufPagePtr;

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
            if (e < eNOERROR) ERRB1(handle, e, rootPage_BCBP, PAGE_BUF);

            /* mark the lsn in the page */
            rootPage->header.lsn = lsn;
            rootPage->header.logRecLen = logRecLen;
        }

        LOT_INIT_INODE_PAGE_HDR(rootPage, finfo->fid, root);

        memcpy(&rootPage->node, anode, inodeUsedSize);

	rootPage_BCBP->dirtyFlag = 1;

	e = BfM_unfixBuffer(handle, rootPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	/*@ store the root PageNo */
	*((ShortPageID *)anodeOrRootPageNo) = root.pageNo;
    }

    return(eNOERROR);

} /* LOT_CreateObject() */
