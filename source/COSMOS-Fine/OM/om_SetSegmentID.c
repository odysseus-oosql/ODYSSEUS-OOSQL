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
 * Module: om_SetSegmentID.c
 *
 * Description:
 *  Set the Segment ID to the catalog overlay of the given data file.
 *
 * Exports:
 *  Four om_SetSegmentIDToCatOverlayForDataUsingDataFileInfo(Four, DataFileInfo*, SegmentID_T*, Four)
 *  Four om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData(Four, sm_CatOverlayForData*, SegmentID_T*, Four)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * om_SetSegmentIDToCatOverlayForDataUsingDataFileInfo()
 *================================*/
/*
 * Function: Four om_SetSegmentIDToCatOverlayForDataUsingDataFileInfo(Four, sm_CatOverlayForData*, SegmentID_T*, Four)
 *
 * Description:
 *  Set the SegmentID to the catalog overlay of the given data file.
 *  The data file is specified by 'fid', the FileID of the data file.
 *
 * Returns:
 *  Error code : ePARAMETER
 */
Four om_SetSegmentIDToCatOverlayForDataUsingDataFileInfo(
    Four                        handle,                 /* IN     handle */
    XactTableEntry_T		*xactEntry,		/* IN     transaction table entry */
    DataFileInfo		*finfo,			/* INOUT  catalog overlay for data */
    SegmentID_T			*segmentID,		/* IN     segment ID of data file which indicated by fInfo */
    Four			type,			/* IN     segment ID's type */
    LogParameter_T		*logParam		/* IN     log parameter */
)
{
    Four                        e;                      /* error code */
    Buffer_ACC_CB               *catPage_BCBP;          /* buffer access control block holding catalog data */
    SlottedPage                 *catPage;               /* pointer to buffer containing the catalog */
    sm_CatOverlayForData        *catEntry;              /* pointer to data file catalog information */
    Lsn_T			lsn;			/* lsn of the newly written log record */
    Four			logRecLen;		/* log record length */
    LOG_LogRecInfo_T		logRecInfo;		/* log record information */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    /* get 'catEntry' that is sm_CatOverlayForData */
    if (finfo->tmpFileFlag != TRUE) {   /* it is normal file */

#ifdef CCPL
        /* Request X lock on the page where the catalog entry resides. */
        e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid,
                               L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < 0) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) {
            ERR(handle, eDEADLOCK); /* deadlock */
        }

        /* get and fix the catalog page */
        e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_FREE, &catPage_BCBP, PAGE_BUF);
        if(e < 0) ERR(handle, e);
#endif 

#ifdef CCRL
        /* get and fix the catalog page */
        e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_EXCLUSIVE, &catPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
#endif 

        catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
        GET_PTR_TO_CATENTRY_FOR_DATA(finfo->catalog.oid.slotNo, catPage, catEntry);
    }
    else {                              /* it is temporary file */

        catEntry = &finfo->catalog.entry->data;
    }

    if (finfo->tmpFileFlag != TRUE && type == TRAINSIZE2) {   /* it is normal file */

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_MODIFY_TRAIN_SEGMENT_ID_IN_CATALOG_ENTRY, LOG_REDO_UNDO,
                                  catPage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(finfo->catalog.oid.slotNo), &(finfo->catalog.oid.slotNo),
                                  sizeof(SegmentID_T), &segmentID,
                                  sizeof(SegmentID_T), &(catEntry->trainSegmentID));

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

            /* mark the lsn in the page */
            catPage->header.lsn = lsn;
            catPage->header.logRecLen = logRecLen;
        }
    }

    /* set the segment ID */
    e = om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData(handle, catEntry, segmentID, type);
    if (e < eNOERROR) ERR(handle, e);

    if (finfo->tmpFileFlag != TRUE) {   /* it is normal file */

        /* set dirty flag */
	catPage_BCBP->dirtyFlag = 1;

#ifdef CCPL
        e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
        if(e < eNOERROR) ERR(handle, e);

        /* Release the lock on the catalog page. */
        e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid, L_MANUAL);
        if (e < 0) ERR(handle, e);
#endif

#ifdef CCRL
        /* free the catalog page */
	BFM_FREEBUFFER(handle, catPage_BCBP, PAGE_BUF, e);
        if(e < eNOERROR) ERR(handle, e);
#endif
    }


    return (eNOERROR);
}



/*@================================
 * om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData()
 *================================*/
/*
 * Function: Four om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData(Four, sm_CatOverlayForData*, SegmentID_T*, Four)
 *
 * Description:
 *  Set the SegmentID to the catalog overlay of the given data file.
 *  The data file is specified by 'fid', the FileID of the data file.
 *
 * Returns:
 *  Error code : ePARAMETER
 */
Four om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData(
    Four                        handle,                 /* IN     handle */
    sm_CatOverlayForData	*catEntry,		/* INOUT  catalog overlay for data */
    SegmentID_T			*segmentID,		/* IN     segment ID of data file which indicated by fInfo */
    Four			type			/* IN     segment ID's type */
)
{
    Four			e;			/* error code */


    /* set the segment ID */
    if (type == PAGESIZE2) {

        catEntry->pageSegmentID = *segmentID;
    }
    else if (type == TRAINSIZE2) {

        catEntry->trainSegmentID = *segmentID;
    }
    else {
	return (eBADPARAMETER);
    }


    return (eNOERROR);
}
