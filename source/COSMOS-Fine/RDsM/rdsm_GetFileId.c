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
 * Module: rdsm_GetFileId.c
 *
 * Description:
 *  get a file identifer that is the first page of a file
 *
 * Exports:
 *  Four rdsm_GetFileId(RDsM_VolumeInfo_T*, PageID*)
 */


#include <assert.h> 
#include <math.h>
#include "common.h"
#include "error.h"
#include "latch.h"
#include "SHM.h"
#include "RDsM.h"
#include "BfM.h"
#include "TM.h"
#include "LM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four rdsm_GetFileId(RDsM_VolumeInfo_T*, PageID*)
 *
 * Description:
 *  get a file identifer that is the first page of a file
 *  Note!! first page of a file is always first page of the extent
 *
 * Returns:
 *  Error code
 *    eNOERROR
 */
Four rdsm_GetFileId(
    Four              handle,            /* IN handle */
    XactTableEntry_T  *xactEntry,        /* IN transaction table entry */
    RDsM_VolumeInfo_T *volInfo,          /* IN pointer to an entry in the volume table */
    PageID            *pageId,           /* OUT page identifier */
    LogParameter_T    *logParam)         /* IN log parameter */
{
    Four              e;                 /* returned error code */
    Four              i, j;              /* loop index */
    Boolean           flag;              /* flag indicating whether a bit is set or not */
    Four              extNo;             /* extent number */
    Four              devNo;             /* device number in the volume */
    Four              pageOffset;        /* offset of page in the device (unit = # of page) */
    Four              extOffset;         /* offset of extent in the device (unit = # of extent) */
    Four              pos;               /* corresponding bit position */
    PageID            bmTrainId;         /* pointer to a train identifier for a bit map */
    BitmapTrain_T     *bmTrain;          /* pointer to a bitmap train */
    Buffer_ACC_CB     *bmTrain_BCBP;     /* Buffer Access CB holding a Train */
    Lsn_T             lsn;               /* LSN of the newly written log record */
    Four              logRecLen;         /* log record length */
    LOG_LogRecInfo_T  logRecInfo;        /* log record information */
    LOG_Image_RDsM_UpdateBitmap_T updateBitmap; /* bitmap update image */
    RDsM_DevInfo *devInfo; 
    RDsM_DevInfoForDataVol *devInfoForDataVol; 
    Four lastPageOffsetInSegment; 

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    assert(volInfo->type == VOLUME_TYPE_DATA); 

    devInfo = PHYSICAL_PTR(volInfo->devInfo); 
    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo); 

    /*
     * Set pageId's volNo
     */
    pageId->volNo = volInfo->volNo;


    /*
     *  I. Get pageId's pageNo from existing page segments
     *     Note!! This operation is done by predetermined order : 1/2, 3/4, 1/4, 7/8, 5/8, ....
     *            To take account for expansion of segments, order is started from end of page segments
     */
    for (i = 2; i <= (volInfo->dataVol.numExtsForPages-1); i*=2) {

	for (j = i-1; j > 0; j-=2) {

	    /*
	     *	get the candidate page number
	     *   - Note!! this candidate page is always first page of extent
	     */
	    extNo = (Four)((volInfo->dataVol.numExtsForPages-1) * ((double)j / i)); 
	    pageId->pageNo = extNo * volInfo->extSize;

            /*
             *  Get Physical Information about position of extent's first page
             */
            e = rdsm_GetPhysicalInfo(handle, volInfo, PAGESIZE2, pageId->pageNo, &devNo, &pageOffset, &lastPageOffsetInSegment);
            if (e < eNOERROR) ERR(handle, e);

            extOffset = pageOffset / volInfo->extSize;

	    /*
	     *	calculate the bmTrainId that the bit map of the extent exists
	     */
	    bmTrainId.volNo = devInfoForDataVol[devNo].bitmapTrainId.volNo; 
	    bmTrainId.pageNo = devInfoForDataVol[devNo].bitmapTrainId.pageNo + (extOffset/volInfo->dataVol.numExtMapsInTrain)*TRAINSIZE2; 

	    /*
	     *	get a train buffer for a bitmap train
	     */
	    e = BfM_getAndFixBuffer(handle, &bmTrainId, M_EXCLUSIVE, &bmTrain_BCBP, TRAIN_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    bmTrain = (BitmapTrain_T*)bmTrain_BCBP->bufPagePtr;

	    /*
	     *	calculate page position pos : the 1st page is 0-th position
	     */
	    pos = pageOffset % (volInfo->dataVol.numExtMapsInTrain * volInfo->extSize);

	    /*
	     *	test whether the corresponding bit is set
	     */
	    Util_TestBitSet(handle, bmTrain->bytes, pos, &flag);

	    /* the bit is set; i.e., allocatable */
	    if (flag == TRUE) {

                /*
                 *  Write log record.
                 */
                if (logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) { 
                    updateBitmap.start = pos;
                    updateBitmap.nBits = 1;

                    LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_RDSM_ALLOC_TRAINS, LOG_REDO_UNDO,
                                          bmTrain->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(updateBitmap), &updateBitmap);

                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERR(handle, e);

                    bmTrain->hdr.lsn = lsn;
                    bmTrain->hdr.logRecLen = logRecLen;
                }

		/*
		 *  set the bit
		 */
		Util_ClearBits(handle, bmTrain->bytes, pos, 1);

		/*
		 *  set dirty bit
		 */
		bmTrain_BCBP->dirtyFlag = 1;

		/*
		 *  free this page
		 */
		BFM_FREEBUFFER(handle, bmTrain_BCBP, TRAIN_BUF, e);
		if (e < eNOERROR) ERR(handle, e);

		return(eNOERROR);

	    } /* end if */

	    /*
	     *  free this page
	     */
	    BFM_FREEBUFFER(handle, bmTrain_BCBP, TRAIN_BUF, e);
	    if (e < eNOERROR) ERR(handle, e);

	} /* end for */

    } /* end for */


    /*
     *  II. Get pageId's pageNo from new allocated page segment
     *      Note!! there is no free extent in existing data segments, so allocate new data segment
     */

    /* Allocate new data segment, and get first extent's number of new data segment */
    e = rdsm_AllocSegmentForPage(handle, volInfo, &extNo);
    if (e < eNOERROR) ERR(handle, e);

    pageId->pageNo = extNo * volInfo->extSize;

    /* Get Physical Information about position of extent's first page */
    e = rdsm_GetPhysicalInfo(handle, volInfo, PAGESIZE2, pageId->pageNo, &devNo, &pageOffset, &lastPageOffsetInSegment);
    if (e < eNOERROR) ERR(handle, e);

    extOffset = pageOffset / volInfo->extSize;

    /* calculate the bmTrainId that the bit map of the extent exists */
    bmTrainId.volNo = devInfoForDataVol[devNo].bitmapTrainId.volNo; 
    bmTrainId.pageNo = devInfoForDataVol[devNo].bitmapTrainId.pageNo + (extOffset/volInfo->dataVol.numExtMapsInTrain)*TRAINSIZE2; 

    /* get a train buffer for a bitmap train */
    e = BfM_getAndFixBuffer(handle, &bmTrainId, M_EXCLUSIVE, &bmTrain_BCBP, TRAIN_BUF);
    if (e < eNOERROR) ERR(handle, e);

    bmTrain = (BitmapTrain_T*)bmTrain_BCBP->bufPagePtr;

    /* calculate page position pos : the 1st page is 0-th position */
    pos = pageOffset % (volInfo->dataVol.numExtMapsInTrain * volInfo->extSize);

    /* Note!! first extent of new data segment is always free */

    /* Write log record */
    if (logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) { 
        updateBitmap.start = pos;
        updateBitmap.nBits = 1;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_RDSM_ALLOC_TRAINS, LOG_REDO_UNDO,
                              bmTrain->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(updateBitmap), &updateBitmap);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        bmTrain->hdr.lsn = lsn;
        bmTrain->hdr.logRecLen = logRecLen;
    }

    /* set the bit */
    Util_ClearBits(handle, bmTrain->bytes, pos, 1);

    /* set dirty bit */
    bmTrain_BCBP->dirtyFlag = 1;

    /* free bitmap page */
    BFM_FREEBUFFER(handle, bmTrain_BCBP, TRAIN_BUF, e);
    if (e < eNOERROR) ERR(handle, e);


    return eNOERROR;

} /* rdsm_GetFileId() */
