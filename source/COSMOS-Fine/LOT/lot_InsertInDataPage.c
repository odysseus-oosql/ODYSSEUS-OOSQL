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
 * Module: lot_InsertInDataPage.c
 *
 * Description:
 *  Append data to data page.
 *
 * Exports:
 *  Four lot_InsertInDataPage(Four, DataFileInfo*, Four, Four, char *,
 *                            L_O_T_ItemList*, Boolean*)
 */


#include <assert.h>
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
 * lot_InsertInDataPage( )
 *================================*/
/*
 * Function: Four lot_InsertInDataPage(Four, DataFileInfo*, Four, Four, char *,
 *                            L_O_T_ItemList*, Boolean*)
 *
 * Description:
 *  Append data to data page.
 *
 * Returns:
 *  Error codes
 *    eMEMORYALLOCERR
 *    some errors caused by function calls
 */
Four lot_InsertInDataPage(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    Four     start,		/* IN starting offset of insert */
    Four     length,		/* IN amount of data to insert */
    char     *newData,		/* IN data to be inserted */
    L_O_T_ItemList *list,	/* INOUT overflowed items to be inserted into parent node */
    Boolean  *overflow,		/* OUT flag indicating data page has been split */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    Four eff;			/* data file's extent fill factor */
    Four firstExt;		/* first Extent No of the file */
    Four origBytes;		/* # of bytes in original data page */
    Four totalBytes;		/* total # of bytes in original data and new data */
    Four nTrains;		/* # of trains to be needed to accomated total Bytes */
    Four bytesPerTrain;		/* # of bytes balanced in all the trains */
    Four remainBytes;		/* # of bytes after balancing */
    Four nBytes;		/* # of bytes to copy to current data page */
    Four movedBytes;		/* # of bytes copied for current node at that time */
    Four toStart;		/* yet unused bytes befor start location */
    PageID *newPids;		/* PageIDs to be newly allocated */
    PageID origPid;		/* Original PageID */
    PageID currentPid;		/* PageID being processed currently */
    L_O_T_LNode *origPage;	/* pointer to the DataPage buffer */
    Buffer_ACC_CB *oPage_BCBP;
    L_O_T_LNode *currentPage;	/* pointer to the current page's buffer */
    Buffer_ACC_CB *curPage_BCBP;
    char tmpData[LOT_LNODE_MAXFREE]; /* save the original bytes */
    char *ptr;			   /* point to bytes to be copied next time */
    Four src;			   /* where the bytes comes ? */
    Four i, j, n;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_ModifyLeafData_T modifyLeafDataInfo; /* train data info */
    SegmentID_T trainSegmentID;  /* train segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    /* pointer for LOT Data Structure of perThreadTable */
    LOT_PerThreadDS_T *lot_perThreadDSptr = LOT_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1,
	     ("lot_InsertInDataPage(handle, finfo=%P, start=%ld, length=%ld, newData=%P, list=%P, overflow=%ld)",
	      finfo, start, length, newData, list, overflow));


    *overflow = FALSE;

    MAKE_PAGEID(origPid, finfo->fid.volNo, list->entry[list->nEntries-1].spid);

    if (list->entry[list->nEntries-1].count + length <= LOT_LNODE_MAXFREE) {
        /* the new data can be put into the current leaf node */

        /*@ Read the original Page */
        e = BfM_getAndFixBuffer(handle, &origPid, M_FREE, &oPage_BCBP, TRAIN_BUF);
        if (e < eNOERROR) ERR(handle, e);

        origPage = (L_O_T_LNode *)oPage_BCBP->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            modifyLeafDataInfo.start = start;
            modifyLeafDataInfo.length = length;
            modifyLeafDataInfo.oldTotalLength = list->entry[list->nEntries-1].count;

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_INSERT_DATA, LOG_REDO_UNDO,
                                  origPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                  length, newData);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, oPage_BCBP, TRAIN_BUF);

            /* mark the lsn in the page */
            origPage->header.lsn = lsn;
            origPage->header.logRecLen = logRecLen;
        }

	/* prepare space moving bytes after start */
	n = list->entry[list->nEntries-1].count - start;
	memmove(&origPage->data[start+length], &origPage->data[start], n);

	/*@ Insert the new data */
	memcpy(&origPage->data[start], newData, length);

	oPage_BCBP->dirtyFlag = 1;

        e = BfM_unfixBuffer(handle, oPage_BCBP, TRAIN_BUF);
        if (e < eNOERROR) ERR(handle, e);

    } else {

	/* origDataSize is total # of bytes in original data page(s) */
	origBytes = list->entry[0].count;
	totalBytes = origBytes + length;
	nTrains = CEIL_AFTER_DIVIDE(totalBytes,LOT_LNODE_MAXFREE);

	/*@ allocate the needed trains */
	/* that is, (nTrains - itemList->nEntries) */
        e = Util_reallocVarArray(handle, &(lot_perThreadDSptr->lot_pageidArray), sizeof(PageID), nTrains-list->nEntries);
        if (e < eNOERROR) ERR(handle, e);

	newPids = (PageID *)(lot_perThreadDSptr->lot_pageidArray.ptr);

	e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &trainSegmentID, TRAINSIZE2);
	if (e < eNOERROR) ERR(handle, e);

	e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &trainSegmentID,
			     &origPid, nTrains - list->nEntries, TRAINSIZE2, FALSE, newPids, logParam);
	if (e < 0) {
            ERR(handle, e);
        }

	/* save the ShortPageID in the itemList */
	for (i = list->nEntries, j = 0; i < nTrains; i++, j++)
	    list->entry[i].spid = newPids[j].pageNo;
	list->nEntries = nTrains;

	bytesPerTrain = totalBytes / nTrains;
	remainBytes = totalBytes % nTrains;

        /*@ Read the original Page */
        e = BfM_getAndFixBuffer(handle, &origPid, M_FREE, &oPage_BCBP, TRAIN_BUF);
        if (e < eNOERROR) ERR(handle, e);

        origPage = (L_O_T_LNode *)oPage_BCBP->bufPagePtr;

	/* save the original data into tmpData */
	memcpy(tmpData, &origPage->data, origBytes);


	/* From now the variable 'i' denotes the entry index in the itemList */
	/* At first, take the data from the original data */
	/* If reach start position, use the new data */
	/* After all the new data are used, use the data after start pos. */
	src = 0;
	ptr = &tmpData[0];
	toStart = start;	/* # of bytes unused before start position */
	for (i = 0; i < nTrains; i++) {

	    MAKE_PAGEID(currentPid, finfo->fid.volNo, list->entry[i].spid);

            if (i == 0) {       /* this is original page */
                curPage_BCBP = oPage_BCBP;
                currentPage = (L_O_T_LNode *)curPage_BCBP->bufPagePtr;

            } else {            /* this is the newly allocated page */
                e = BfM_fixNewBuffer(handle, &currentPid, M_FREE, &curPage_BCBP, TRAIN_BUF);
                if (e < eNOERROR) ERR(handle, e);

                currentPage = (L_O_T_LNode *)curPage_BCBP->bufPagePtr;
                LOT_INIT_LEAF_NODE(currentPage, finfo->fid, currentPid);
            }

	    /* get # of bytes for the current page to hold */
	    nBytes = bytesPerTrain + ((remainBytes > i) ? 1:0);

	    movedBytes = 0;
	    while (movedBytes < nBytes) {

		switch(src) {
		  case 0:	/* take the entries from the original entries */
		    n = MIN(nBytes - movedBytes, toStart);
		    memcpy(&currentPage->data[movedBytes], ptr, n);
		    ptr += n;
		    toStart -= n;
		    movedBytes += n;

		    if (toStart == 0) {
			src = 1; /* From next, use the new entries */
			ptr = newData;
		    }

		    break;

		  case 1:	/* take the entries from the new entries */
		    n = MIN(nBytes - movedBytes, length);
		    memcpy(&currentPage->data[movedBytes], ptr, n);
		    ptr += n;
		    length -= n;
		    movedBytes += n;

		    if (length == 0) {
			src = 2;	/* From now, use the original entries again */
			ptr = &tmpData[start];
		    }

		    break;

		  case 2:	/* take the entries from the original entries */
		    n = nBytes - movedBytes;
		    memcpy(&currentPage->data[movedBytes], ptr, n);
		    ptr += n;
		    movedBytes += n;

		    break;

		} /* end of switch */
	    }

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                if (i == 0) { /* this is orginal page */
                    Four updatePosition = MIN(start, nBytes);
                    Four nOverwrittenBytes = origBytes - updatePosition;

                    assert(nOverwrittenBytes > 0);

                    LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_LOT_WRITE_DATA, LOG_UNDO_ONLY,
                                          currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(Four), &updatePosition,
                                          0, NULL,
                                          nOverwrittenBytes, &tmpData[updatePosition]);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERRB1(handle, e, curPage_BCBP, TRAIN_BUF);

                    if (nBytes > start) {
                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                              currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(Four), &updatePosition,
                                              nBytes-updatePosition, &currentPage->data[updatePosition]);

                        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                        if (e < eNOERROR) ERRB1(handle, e, curPage_BCBP, TRAIN_BUF);
                    }

                } else { /* this is new page */
                    Four start = 0;

                    LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_LOT_INIT_LEAF_NODE, LOG_REDO_ONLY,
                                          currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(FileID), &finfo->fid);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERRB1(handle, e, curPage_BCBP, TRAIN_BUF);

                    LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                          currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(Four), &start,
                                          nBytes, &currentPage->data[0]);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERRB1(handle, e, curPage_BCBP, TRAIN_BUF);
                }

                /* mark the lsn in the page */
                currentPage->header.lsn = lsn;
                currentPage->header.logRecLen = logRecLen;
            }

	    curPage_BCBP->dirtyFlag = 1;

	    e = BfM_unfixBuffer(handle, curPage_BCBP, TRAIN_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    /*@ construct the itemList */
	    list->entry[i].count = nBytes;
	}

	*overflow = TRUE;
    }

    return(eNOERROR);

} /* lot_InsertInDataPage( ) */
