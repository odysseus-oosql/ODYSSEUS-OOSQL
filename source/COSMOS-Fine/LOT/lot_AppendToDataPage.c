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
 * Module: lot_AppendToDataPage.c
 *
 * Description:
 *  Append data to data page.
 *
 * Exports:
 *  Four lot_AppendToDataPage(Four, DataFileInfo*, Four, char*,
 *                            L_O_T_ItemList*, Boolean*)
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
 * lot_AppendToDataPage( )
 *================================*/
/*
 * Function: Four lot_AppendToDataPage(Four, DataFileInfo*, Four, char*,
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
Four lot_AppendToDataPage(
    Four           	handle,
    XactTableEntry_T 	*xactEntry, 	/* IN transaction table entry */
    DataFileInfo   	*finfo,		/* IN file information */
    Four           	length,		/* IN amount of data to append */
    char           	*newData,	/* IN data to be appended */
    L_O_T_ItemList 	*list,		/* INOUT overflowed items to be inserted into parent node */
    Boolean        	*overflow,	/* OUT flag indicating data page has been split */
    LogParameter_T 	*logParam) 	/* IN log parameter */
{
    Four e;			/* error number */
    Four eff;			/* data file's extent fill factor */
    Four firstExt;		/* first Extent No of the file */
    Four origBytes;		/* # of bytes in original data page */
    Four totalBytes;		/* total # of bytes in original data and new data */
    Four nOrigTrains;		/* # of original trains participating in this operation */
    Four nTrains;		/* # of trains to be needed to accomated total Bytes */
    Four nPartialTrains;	/* # of trains filled partially */
    Four remainBytes;		/* # of bytes in partially filled trains */
    Four remain;		/* bytes remained after balancing the remainBytes */
    Four bytesPerTrain;		/* # of bytes balanced in the partially filled trains */
    Four nBytes;		/* # of bytes to copy to current data page */
    PageID *newPids;		/* PageIDs to be newly allocated */
    PageID origPid;		/* Original PageID */
    PageID currentPid;		/* PageID being processed currently */
    L_O_T_LNode *origPage;	/* pointer to the DataPage buffer */
    Buffer_ACC_CB *oPage_BCBP;
    L_O_T_LNode *currentPage;	/* pointer to the current page's buffer */
    Buffer_ACC_CB *curPage_BCBP;
    char *currentDataPtr;
    char tmpData1[LOT_LNODE_MAXFREE];
    char tmpData2[LOT_LNODE_MAXFREE];
    char *tmpData1Ptr, *tmpData2Ptr;
    Four tmpData1Bytes, tmpData2Bytes;
    Four i, j;
    Four firstLeafNodeOrigBytes;
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
	     ("lot_AppendToDataPage(handle, finfo=%P, length=%ld, newData=%P, list=%P, overflow=%ld)",
	      finfo, length, newData, list, overflow));


    *overflow = FALSE;

    if (list->entry[list->nEntries-1].count + length <= LOT_LNODE_MAXFREE) {

	/* the new data can be put into the last page */
	MAKE_PAGEID(origPid, finfo->fid.volNo, list->entry[list->nEntries-1].spid);
	e = BfM_getAndFixBuffer(handle, &origPid, M_FREE, &oPage_BCBP, TRAIN_BUF);
	if (e < eNOERROR) ERR(handle, e);

	origPage = (L_O_T_LNode *)oPage_BCBP->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            modifyLeafDataInfo.start = list->entry[list->nEntries-1].count;
            modifyLeafDataInfo.length = length;
            modifyLeafDataInfo.oldTotalLength = list->entry[list->nEntries-1].count;

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_INSERT_DATA, LOG_REDO_ONLY,
                                  origPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                  length, newData);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, oPage_BCBP, TRAIN_BUF);

            /* mark the lsn in the page */
            origPage->header.lsn = lsn;
            origPage->header.logRecLen = logRecLen;
        }

	memcpy(origPage->data + list->entry[list->nEntries-1].count, newData, length);

	oPage_BCBP->dirtyFlag = 1;

	e = BfM_unfixBuffer(handle, oPage_BCBP, TRAIN_BUF);
	if (e < eNOERROR) ERR(handle, e);

    } else {

	/* origDataSize is total # of bytes in original data page(s) */
	origBytes = list->entry[0].count +
	    ((list->nEntries == 2) ? list->entry[1].count:0);
	nOrigTrains = list->nEntries;
	totalBytes = origBytes + length;
	nTrains = CEIL_AFTER_DIVIDE(totalBytes, LOT_LNODE_MAXFREE);

	/*@
	 * allocate the needed trains, nTrains
	 */
        e = Util_reallocVarArray(handle, &(lot_perThreadDSptr->lot_pageidArray), sizeof(PageID), nTrains-nOrigTrains);
        if (e < eNOERROR) ERR(handle, e);

	newPids = (PageID *)(lot_perThreadDSptr->lot_pageidArray.ptr);

	if(nTrains > nOrigTrains) {
	    /* newly allocated trains: (nTrains - nOrigTrains) */

            MAKE_PAGEID(origPid, finfo->fid.volNo, list->entry[nOrigTrains-1].spid);

	    e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &trainSegmentID, TRAINSIZE2);
	    if (e < eNOERROR) ERR(handle, e);

	    e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &trainSegmentID,
				 &origPid, nTrains - nOrigTrains, TRAINSIZE2, FALSE, newPids, logParam);
	    if (e < 0) {
                ERR(handle, e);
            }
	}

	/* save the ShortPageID in the itemList */
	for (i = list->nEntries, j = 0; i < nTrains; i++, j++)
	    list->entry[i].spid = newPids[j].pageNo;
	list->nEntries = nTrains;

	if ((totalBytes % LOT_LNODE_MAXFREE) > 0) {

	    /* There is a partially filled leaf node(s). */
	    if ((totalBytes % LOT_LNODE_MAXFREE) >= LOT_LNODE_HALFFREE)
		nPartialTrains = 1;
	    else
		nPartialTrains = 2;

	    remainBytes = totalBytes - (nTrains - nPartialTrains)*LOT_LNODE_MAXFREE;
	    bytesPerTrain = remainBytes / nPartialTrains;

	    /* The 'remain' are 1 or 0. When nPartialTrains is 1,
	       the 2nd last page has 1 more byte than the last page
	       if remainsBytes is odd number. */
	    remain = remainBytes % nPartialTrains;

	} else {

	    /* all nodes are filled completely */
	    nPartialTrains = 0;
	}

	/* When two original pages participate,
	   copy the data in the last page into the temporary buffer */
	if (nOrigTrains == 2) {
	    MAKE_PAGEID(origPid, finfo->fid.volNo, list->entry[1].spid);

	    e = BfM_getAndFixBuffer(handle, &origPid, M_FREE, &oPage_BCBP, TRAIN_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    origPage = (L_O_T_LNode *)oPage_BCBP->bufPagePtr;

	    memcpy(tmpData2, origPage->data, list->entry[1].count);
	    tmpData2Bytes = list->entry[1].count;

	    e = BfM_unfixBuffer(handle, oPage_BCBP, TRAIN_BUF);
	    if (e < eNOERROR) ERR(handle, e);
	} else
	    tmpData2Bytes = 0;

	/* save the # of bytes in first leaf node */
        firstLeafNodeOrigBytes = list->entry[0].count;

	/* From now the variable 'i' denotes the entry index in the list */
	tmpData1Bytes = 0;
	for (i = 0; i < nTrains; i++) {

	    MAKE_PAGEID(currentPid, finfo->fid.volNo, list->entry[i].spid);

            if (i < nOrigTrains) {
                e = BfM_getAndFixBuffer(handle, &currentPid, M_FREE, &curPage_BCBP, TRAIN_BUF);
                if (e < eNOERROR) ERR(handle, e);
                currentPage = (L_O_T_LNode *)curPage_BCBP->bufPagePtr;

            } else {
                e = BfM_fixNewBuffer(handle, &currentPid, M_FREE, &curPage_BCBP, TRAIN_BUF);
                if (e < eNOERROR) ERR(handle, e);

                currentPage = (L_O_T_LNode *)curPage_BCBP->bufPagePtr;
                LOT_INIT_LEAF_NODE(currentPage, finfo->fid, currentPid);
            }


	    /* get # of bytes for the current page to hold */
	    nBytes = (i < nTrains - nPartialTrains) ?
		LOT_LNODE_MAXFREE : (bytesPerTrain + ((remain-- > 0) ? 1:0));

	    if (origBytes > 0) { /* use the original data */

		if (i == 0) {
		    if (nBytes <= list->entry[0].count) {
			/* move others to the next train */
			tmpData1Bytes = list->entry[0].count - nBytes;
			memcpy(tmpData1, &currentPage->data[nBytes], tmpData1Bytes);
		    } else if (nBytes > list->entry[0].count && nOrigTrains == 2) {
			/* borrow from the next page */
			/* Notice that it is sufficient to borrow from the next page */
			Four borrow = nBytes - list->entry[0].count;

			memcpy(&currentPage->data[list->entry[0].count], tmpData2, borrow);
			tmpData2Ptr = tmpData2 + borrow;
			tmpData2Bytes -= borrow;
		    }
		} else {
		    currentDataPtr = currentPage->data;

		    if (tmpData1Bytes != 0) {
			memcpy(currentDataPtr, tmpData1, tmpData1Bytes);
			currentDataPtr += tmpData1Bytes;
			tmpData1Bytes = 0;
		    }

		    if (tmpData2Bytes != 0) {
			Four bytes = MIN(nBytes - (currentDataPtr - currentPage->data), tmpData2Bytes);

			memcpy(currentDataPtr, tmpData2Ptr, bytes);
			tmpData2Ptr += bytes;
			tmpData2Bytes -= bytes;
		    }
		}

		origBytes -= nBytes; /* decrement the origBytes */
		if (origBytes < 0) {
		    /* attach the new data to the end */
		    memcpy(&currentPage->data[nBytes+origBytes], newData, -origBytes);
		    newData += -origBytes;
		}

	    } else {		/* use the new Data */
		memcpy(currentPage->data, newData, nBytes);
		newData += nBytes;

	    }

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                if (i == 0) { /* this is the first orginal page */
                    Four nAppendedBytes = nBytes - list->entry[0].count;

                    if (nAppendedBytes < 0) {
                        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_WRITE_DATA, LOG_UNDO_ONLY,
                                              currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(Four), &nBytes,
                                              0, NULL,
                                              tmpData1Bytes, &tmpData1[0]);
                        assert(tmpData1Bytes == -nAppendedBytes);

                    } else if (nAppendedBytes > 0) {

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                              currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(Four), &list->entry[0].count,
                                              nAppendedBytes, &currentPage->data[list->entry[0].count]);
                    }

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERRB1(handle, e, curPage_BCBP, TRAIN_BUF);

                } else if (i == 1 && i < nOrigTrains) {

                    Four nDeletedBytes = list->entry[0].count - firstLeafNodeOrigBytes; /* deleted bytes from the front */ 
                    Four nAppendedBytes = nBytes - (list->entry[1].count - nDeletedBytes); /* appended bytes to the rear */

                    if (nDeletedBytes > 0) {
                        modifyLeafDataInfo.start = 0;
                        modifyLeafDataInfo.length = nDeletedBytes;
                        modifyLeafDataInfo.oldTotalLength = list->entry[1].count;

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_DELETE_DATA, LOG_REDO_UNDO,
                                              currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                              nDeletedBytes, &tmpData2[0]);


                    } else if (nDeletedBytes < 0) {
                        assert(nTrains == 2);

                        modifyLeafDataInfo.start = 0;
                        modifyLeafDataInfo.length = -nDeletedBytes;
                        modifyLeafDataInfo.oldTotalLength = list->entry[1].count;

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_INSERT_DATA, LOG_REDO_UNDO,
                                              currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_LOT_ModifyLeafData_T), &modifyLeafDataInfo,
                                              -nDeletedBytes, &currentPage->data[0]);
                    }

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERRB1(handle, e, curPage_BCBP, TRAIN_BUF);

                    if (nAppendedBytes > 0) {
                        Four start = nBytes - nAppendedBytes;

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_WRITE_DATA, LOG_REDO_ONLY,
                                              currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(Four), &start,
                                              nAppendedBytes, &currentPage->data[start]);

                    } else if (nAppendedBytes < 0) {
                        assert(nTrains > 2);
                        assert(nDeletedBytes >= 0);

                        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_LOT_WRITE_DATA, LOG_UNDO_ONLY,
                                              currentPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(Four), &nBytes,
                                              -nAppendedBytes, &tmpData2[nDeletedBytes+nBytes]);
                    }

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERRB1(handle, e, curPage_BCBP, TRAIN_BUF);

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

	    /*@ construct the L_O_T_ItemList */
	    list->entry[i].count = nBytes;
	}

	*overflow = TRUE;
    }

    return(eNOERROR);

} /* lot_AppendToDataPage( ) */
