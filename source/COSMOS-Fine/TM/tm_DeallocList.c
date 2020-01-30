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
 * Module: tm_DeallocList.c
 *
 * Description:
 *  Manage the dealloc list.
 */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "LM.h"
#include "RDsM.h"
#include "LOT.h"
#include "OM.h"
#include "BtM.h"
#include "MLGF.h"
#include "SM.h"
/* #incldue "perProcessDS.h" */
#include "perThreadDS.h"


/*
 * Function: tm_DL_ConvertIntoSmallUnits(XactTableEntry_T*)
 *
 * Description:
 *  Convert the dealloc elements into the small units (page/train).
 */
Four tm_DL_ConvertIntoSmallUnits(
    Four             	handle,
    XactTableEntry_T 	*xactEntry) 		/* IN transaction table entry */
{
    Four 		e;
    DeallocListElem 	*dlElem, newDLHead;

    /* fake the deallocated page list */
    dlElem = xactEntry->dlHead.next;

    while (dlElem) {		/* until reach end of list */

        switch(dlElem->type) {
          case DL_BTREEINDEX :
            e = TM_XT_AddToDeallocPageSegmentList(handle, xactEntry, &(dlElem->pageSegmentID));
            if (e < eNOERROR) ERR(handle, e); break;

          case DL_FILE :
            e = TM_XT_AddToDeallocPageSegmentList(handle, xactEntry, &(dlElem->pageSegmentID));
            if (e < eNOERROR) ERR(handle, e);
	    if (IS_NIL_SEGMENT_ID(&(dlElem->trainSegmentID)) == FALSE) {
	        e = TM_XT_AddToDeallocTrainSegmentList(handle, xactEntry, &(dlElem->trainSegmentID));
                if (e < eNOERROR) ERR(handle, e);
	    }
	    break;
          case DL_MLGFINDEX :
            e = TM_XT_AddToDeallocPageSegmentList(handle, xactEntry, &(dlElem->pageSegmentID));
            if (e < eNOERROR) ERR(handle, e); break;

          case DL_LRGOBJ:
            e = lot_DropTree(handle, xactEntry, &dlElem->elem.pid, FALSE, NULL); 
            if (e < eNOERROR) ERR(handle, e); break;

          default :
            ERR(handle, eBADDEALLOCLISTTYPE_TM);
        }

        dlElem = dlElem->next;
    }

    return(eNOERROR);

} /* tm_DL_ConvertIntoSmallUnits() */



/*
 * Function: tm_DL_Log(XactTableEntry_T*)
 *
 * Description:
 *  Log the dealloc list.
 */
Four tm_DL_Log(
    Four             	handle,
    XactTableEntry_T 	*xactEntry) 		/* IN transaction table entry */
{
    Four 		e;
    One 		dlElemType;
    Four 		totalDeallocElements;
    Lsn_T 		lsn;                  	/* lsn of the newly written log record */
    Four 		logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 		/* log record information */
    LogParameter_T 	logParam;
    Four 		i;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    /*
     * BUG: logging the deallocated pages/trains
     */
    dlElemType = DL_PAGE;
    totalDeallocElements = xactEntry->nestedTopActions[0].idxOnDeallocPageArray + 1;
    for (i = 0; i < totalDeallocElements; i += LOG_MAX_DEALLOC_ELEMENTS_PER_LOG_RECORD) {
        Four count = MIN(LOG_MAX_DEALLOC_ELEMENTS_PER_LOG_RECORD, totalDeallocElements - i);

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_TRANSACTION,
                              LOG_ACTION_XACT_DEALLOC_LIST, LOG_NO_REDO_UNDO,
                              common_perThreadDSptr->nilPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(One), &dlElemType,
                              sizeof(PageID)*count, &(((PageID*)PHYSICAL_PTR(xactEntry->deallocPageArray))[i]));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        if (IS_NIL_LSN(xactEntry->deallocLsn))
            xactEntry->deallocLsn = lsn;
    }

    dlElemType = DL_TRAIN;
    totalDeallocElements = xactEntry->nestedTopActions[0].idxOnDeallocTrainArray + 1;
    for (i = 0; i < totalDeallocElements; i += LOG_MAX_DEALLOC_ELEMENTS_PER_LOG_RECORD) {
        Four count = MIN(LOG_MAX_DEALLOC_ELEMENTS_PER_LOG_RECORD, totalDeallocElements - i);

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_TRANSACTION,
                              LOG_ACTION_XACT_DEALLOC_LIST, LOG_NO_REDO_UNDO,
                              common_perThreadDSptr->nilPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(One), &dlElemType,
                              sizeof(PageID)*count, &(((PageID*)PHYSICAL_PTR(xactEntry->deallocTrainArray))[i]));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        if (IS_NIL_LSN(xactEntry->deallocLsn))
            xactEntry->deallocLsn = lsn;
    }

    dlElemType = DL_PAGE_SEGMENT;
    totalDeallocElements = xactEntry->idxOnDeallocPageSegmentArray + 1;
    for (i = 0; i < totalDeallocElements; i += LOG_MAX_DEALLOC_SEGMENT_ID_ELEMENTS_PER_LOG_RECORD) {
        Four count = MIN(LOG_MAX_DEALLOC_SEGMENT_ID_ELEMENTS_PER_LOG_RECORD, totalDeallocElements - i);

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_TRANSACTION,
                              LOG_ACTION_XACT_DEALLOC_LIST, LOG_NO_REDO_UNDO,
                              common_perThreadDSptr->nilPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(One), &dlElemType,
                              sizeof(SegmentID_T)*count, &(((SegmentID_T*)PHYSICAL_PTR(xactEntry->deallocPageSegmentArray))[i]));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        if (IS_NIL_LSN(xactEntry->deallocLsn))
            xactEntry->deallocLsn = lsn;
    }

    dlElemType = DL_TRAIN_SEGMENT;
    totalDeallocElements = xactEntry->idxOnDeallocTrainSegmentArray + 1;
    for (i = 0; i < totalDeallocElements; i += LOG_MAX_DEALLOC_SEGMENT_ID_ELEMENTS_PER_LOG_RECORD) {
        Four count = MIN(LOG_MAX_DEALLOC_SEGMENT_ID_ELEMENTS_PER_LOG_RECORD, totalDeallocElements - i);

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_TRANSACTION,
                              LOG_ACTION_XACT_DEALLOC_LIST, LOG_NO_REDO_UNDO,
                              common_perThreadDSptr->nilPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(One), &dlElemType,
                              sizeof(SegmentID_T)*count, &(((SegmentID_T*)PHYSICAL_PTR(xactEntry->deallocTrainSegmentArray))[i]));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        if (IS_NIL_LSN(xactEntry->deallocLsn))
            xactEntry->deallocLsn = lsn;
    }


    return(eNOERROR);

} /* tm_DL_Log() */



/*
 * Function: tm_DL_FreeReally(XactTableEntry_T*)
 *
 * Description:
 *  Free the space in the dealloc list.
 */
Four tm_DL_FreeReally(
    Four                        handle,                 /* IN handle */
    XactTableEntry_T 		*xactEntry) 		/* IN transaction table entry */
{
    Four 			e;                     	/* error code */
    LogParameter_T 		logParam;
    Four 			i;
    SegmentID_T			pageSegmentID;		/* page segment identification */
    SegmentID_T			trainSegmentID;		/* train segment identification */


    /*
     * after commit operation really deallocate page
     */
    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);

    for (i = xactEntry->nestedTopActions[0].idxOnDeallocPageArray; i >= 0; i--) {
        e = RDsM_FreeTrain(handle, xactEntry, &(((PageID*)PHYSICAL_PTR(xactEntry->deallocPageArray))[i]), PAGESIZE2, TRUE, &logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    for (i = xactEntry->nestedTopActions[0].idxOnDeallocTrainArray; i >= 0; i--) {
        e = RDsM_FreeTrain(handle, xactEntry, &(((PageID*)PHYSICAL_PTR(xactEntry->deallocTrainArray))[i]), TRAINSIZE2, TRUE, &logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    for (i = xactEntry->idxOnDeallocPageSegmentArray; i >= 0; i--) {
	pageSegmentID = (((SegmentID_T*)PHYSICAL_PTR(xactEntry->deallocPageSegmentArray))[i]);
	e = RDsM_DropSegment(handle, xactEntry, GET_VOLNO_FROM_SEGMENT_ID(&pageSegmentID), &pageSegmentID, PAGESIZE2, FALSE, &logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    for (i = xactEntry->idxOnDeallocTrainSegmentArray; i >= 0; i--) {
	trainSegmentID = (((SegmentID_T*)PHYSICAL_PTR(xactEntry->deallocTrainSegmentArray))[i]);
	e = RDsM_DropSegment(handle, xactEntry, GET_VOLNO_FROM_SEGMENT_ID(&trainSegmentID), &trainSegmentID, TRAINSIZE2, FALSE, &logParam);
        if (e < eNOERROR) ERR(handle, e);
    }


    return(eNOERROR);

} /* tm_DL_FreeReally() */
