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
 * Module: RDsM_DropSegment.c
 *
 * Description:
 *  Drop the segment.
 *
 * Exports:
 *  Four RDsM_DropSegment(XactTableEntry_T*, Four, SegmentID_T*, Four, Boolean, LogParameter_T*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "TM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four RDsM_DropSegment(XactTableEntry_T*, Four, SegmentID_T*, Four, Boolean, LogParameter_T*)
 *
 * Description:
 *  Drop the segment which has a given segment id 'segmentID'.
 *
 * Returns:
 *  Error code
 */
Four RDsM_DropSegment(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T  		*xactEntry,    		/* IN transaction table entry */
    Four              		volNo,         		/* IN volume number where the segment is created */
    SegmentID_T        		*segmentID,    		/* OUT segment identifier */
    Four                	sizeOfTrain,            /* IN size of train in a segment to create */
    Boolean                     immediateFlag,          /* IN if TRUE, free the segment immediately */
    LogParameter_T    		*logParam  		/* IN log parameter */
)
{
    Four        		e;             		/* returned error code */
    Four                        i;			/* for loop */
    Four              		entryNo;                /* entry no of volume table entry corresponding to the given volume */
    RDsM_VolumeInfo_T 		*volInfo;               /* volume information in volume table entry */
    AllocAndFreeExtentInfo_T    *firstExtent;           /* ptr of first extent in a segment */
    AllocAndFreeExtentInfo_T    *nextExtent;            /* ptr of first extent in a segment */
    VarArray                    extent;                 /* extents of the segment */
    Four                        nextExt;                /* next extent No. of the extent */
    Four                        count = 0;              /* # of extent of segment */
    Lsn_T                       lsn;        		/* LSN of the newly written log record */
    Four                        logRecLen;              /* log record length */
    LOG_LogRecInfo_T            logRecInfo;             /* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_DropSegment(xactEntry=%P, volNo=%lD, segmentID=%P, sizeOfTrain=%lD, immediateFlag=%lD, logParam=%p)", xactEntry, volNo, segmentID, sizeOfTrain, immediateFlag, logParam));


    /*
     * check parameters
     */
    if (segmentID->firstExtent == NIL) ERR(handle, eBADPARAMETER);
    if (segmentID->sizeOfTrain != sizeOfTrain) ERR(handle, eBADPARAMETER);


    /*
     *  get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * set a pointer to the corresponding entry
     */
    volInfo = &(RDSM_VOLTABLE[entryNo].volInfo);


    /*
     * initialize 'extent' that is a variable array
     */
    e = Util_initVarArray(handle, &extent, sizeof(AllocAndFreeExtentInfo_T), RDSM_INIT_ALLOC_AND_FREE_EXTENT_INFO_SIZE);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * initialize alloc & free extent info
     */
    e = rdsm_InitAllocAndFreeExtentInfo(handle, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), RDSM_INIT_ALLOC_AND_FREE_EXTENT_INFO_SIZE);
    if (e < eNOERROR) ERR(handle, e);

    /* set 'first' & 'next' extent */
    firstExtent = &RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent)[0];
    nextExtent  = &RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent)[1];


    /*
     * ### Critical Section Start ###
     *
     * get the latch of page alloc & dealloc
     */
    e = SHM_getLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Nested Top Action Start.
     * Note: This is for extent map. and This must be called after the lath of page alloc & dealloc is acquired.
     */
    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /*
     * Write log record.
     */
    if ((logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) && !immediateFlag) {

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_TRANSACTION,
                              LOG_ACTION_XACT_DELETE_ENTRY_FROM_DEALLOC_LIST, LOG_REDO_UNDO,
                              common_perThreadDSptr->nilPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(SegmentID_T), segmentID);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* first extent number to be freed */
    firstExtent->extentNo = segmentID->firstExtent;

    /* set 'count' */
    count = 1;

    /* get the extentmap buffer latch */
    e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, firstExtent);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /* get the bitmap buffer latch */
    e = rdsm_getAndFixBitMapBuffer(handle, volInfo, firstExtent);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, firstExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /* get next extent No. of the extent */
    e = rdsm_GetExtentMapInfo(handle, firstExtent, NULL, &nextExtent->extentNo);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, firstExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));


    /*@ find last extnet of segment */
    while (nextExtent->extentNo != NIL) {

        /* increase 'count' */
        count++;

    	/* get the extentmap buffer latch */
    	e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, nextExtent);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries,
					       &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    	/* get the bitmap buffer latch */
    	e = rdsm_getAndFixBitMapBuffer(handle, volInfo, nextExtent);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries,
					       &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /* get next extent No. of the extent */
	e = rdsm_GetExtentMapInfo(handle, nextExtent, NULL, &nextExt);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries,
					       &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

	if (nextExt != NIL) {
    	    /* free the extentmap buffer latch */
    	    RDSM_FREE_EXTENTMAP_BUFFER(handle, nextExtent, e);
    	    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries,
					           &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
        }

        /* if entries is full, dobule the size of array */
	if (count >= extent.nEntries) {
            e = Util_doublesizeVarArray(handle, &extent, sizeof(AllocAndFreeExtentInfo_T));
    	    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries,
					           &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
	}

        nextExtent = &RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent)[count];
        nextExtent->extentNo = nextExt;
    }

    /*
     * Caution: the pointer of 'firstExtent' is not valid, since we use 'Util_doublesizeVarArray(handle, )'
     */

    /* free the extent extnum */
    e = rdsm_InsertExtentToFreeExtentList(handle, xactEntry, volInfo, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), segmentID->sizeOfTrain,
    	                                  count, logParam);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries,
					   &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /* free the extentmap buffer latch of first extent*/
    RDSM_FREE_EXTENTMAP_BUFFER(handle, &RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent)[0], e);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries);

    if (count >= 2) {

        /* free the extentmap buffer latch of last extent */
    	RDSM_FREE_EXTENTMAP_BUFFER(handle, &RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent)[count-1], e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries);
    }

    /* set the bitmap information and then free the bitmap buffer latch */
    for (i=0; i<count; i++) {

        /* set bitmap */
        e = rdsm_SetBitMapInfo(handle, xactEntry, &RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent)[i],
			       0, volInfo->extSize * PAGESIZE2, SET_BITS, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries);

        /* free the bitmap buffer latch */
    	RDSM_FREE_BITMAP_BUFFER(handle, &RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent)[i], e);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries);
    }

    /* finalize variable array 'extent' */
    e = Util_finalVarArray(handle, &extent);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Nested Top Action End.
     * Note: This is for extent map. and This must be called before the lath of page alloc & dealloc is released.
     */
    ENABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(extent), extent.nEntries,
					   &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    DISABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

    /* 
     * ### Critical Section End ###
     *
     * release the latch of page alloc & dealloc
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex);
    if (e < eNOERROR) ERR(handle, e);

    /* assign firstExtent of segmentID to new alloc extent No. */
    segmentID->volNo       = NIL;
    segmentID->firstExtent = NIL;
    segmentID->sizeOfTrain = NIL;


    return (eNOERROR);
}
