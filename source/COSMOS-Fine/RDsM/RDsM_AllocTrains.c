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
 * Module: RDsM_AllocTrains.c
 *
 * Description:
 *  Allocate the trains.
 *
 * Exports:
 *  Four RDsM_AllocTrains(XactTableEntry_T*, Four, SegmentID_T*, PageID*, Four, Four, Boolean, PageID*, LogParameter_T*)
 */

#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four RDsM_AllocTrains(XactTableEntry_T*, Four, SegmentID_T*, PageID*, Four, Four, Boolean, PageID*, LogParameter_T*)
 *
 * Description:
 *  Allocate the trains.
 *
 * Returns:
 *  Error code
 */
Four RDsM_AllocTrains(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T  		*xactEntry,    		/* IN transaction table entry */
    Four              		volNo,         		/* IN volume number where the segment is created */
    SegmentID_T        		*segmentID,    		/* IN segment identifier */
    PageID 			*nearPID,               /* IN allocate as near this pid as possible */
    Four 			numOfTrains,            /* IN number of trains to be allocated */
    Four 			sizeOfTrain,            /* IN size of a train to be allocated */
    Boolean                	effFlag,                /* IN indicates whether EFF should be reserved */
    PageID 			*trainIDs,              /* OUT pointer to train identifiers for the allocated trains */
    LogParameter_T    		*logParam  		/* IN log parameter */
)
{
    Four        		e;             		/* returned error code */
    Four              		entryNo;                /* entry no of volume table entry corresponding to the given volume */
    RDsM_VolumeInfo_T 		*volInfo;               /* volume information in volume table entry */
    Four                	numOfEmptyTrains;       /* number of empty trains in a extent */
    AllocAndFreeExtentInfo_T	extent[3];		/* extent information data structure */
    AllocAndFreeExtentInfo_T	*prevExtent;		/* pointer of previous extent */
    AllocAndFreeExtentInfo_T	*midExtent;		/* pointer of middle extent */
    AllocAndFreeExtentInfo_T	*nextExtent;		/* pointer of next extent */
    AllocAndFreeExtentInfo_T	*_nextExtent;		/* temporary pointer of next extent */
    AllocAndFreeExtentInfo_T	*newExtent;		/* pointer of new extent */
    Four                        remainTrains;		/* # of remain trains to allocate */
    Four                        allocatedTrains;	/* # of allocated trians */
    Four                        numTrains;		/* # of train to require */
    Four                        eff;			/* extent fill factor */
    Four			midExtent_eff;		/* # of empty trains in middle extent */
    Four			prevExtent_eff;		/* # of empty trains in previous extent */
    Four			nextExtent_eff;		/* # of empty trains in next extent */
    Four 			pos;			/* position */
    Four			numOfNewExtent;		/* # of newly allocated extent */
    Four			i;			/* for loop */


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_AllocTrains(xactEntry=%P, volNo=%lD, segmentID=%P, nearPID=%P, numOfTrains=%lD, sizeofTrain=%lD, effFlag=%lD, trainIDs=%P, logParam=%p)", xactEntry, volNo, segmentID, nearPID, numOfTrains, sizeOfTrain, effFlag, trainIDs, logParam));


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


    /* Calculate eff that is an allocatable maximum number of pages in an extent */
    if (effFlag == TRUE)
        eff = volInfo->dataVol.eff;
    else
        eff = volInfo->extSize;

    /*
     * check parameters
     */
    if (segmentID->firstExtent < 0 || segmentID->firstExtent >= volInfo->numExts) ERR(handle, eBADPARAMETER);
    if (eff < 0 || eff > volInfo->extSize) ERR(handle, eINVALIDEFF_RDSM);
    if (numOfTrains < 0) ERR(handle, eBADPARAMETER);
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eINVALIDTRAINSIZE_RDSM);
    if (trainIDs == NULL) ERR(handle, eBADPARAMETER);


    /* if numOfTrains is zero, there is nothing to do */
    if (numOfTrains == 0) return (eNOERROR);

    /*
     * Page Allocation Algorithm
     */

    remainTrains = numOfTrains;
    numTrains = 0;

    /*
     * 0. initialize alloc & free extent info
     */
    e = rdsm_InitAllocAndFreeExtentInfo(handle, extent, 3);
    if (e < eNOERROR) ERR(handle, e);

    midExtent = &extent[0]; prevExtent = &extent[1]; nextExtent = &extent[2];

    /*
     * 1. acquire the latchs of bitmap & extentmap that is used to algorithm.
     */

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

    /* get the extentmap and bitmap buffer latchs of middle extent */
    midExtent->extentNo = (nearPID != NULL) ? nearPID->pageNo / volInfo->extSize : segmentID->firstExtent;

    /* get 'pos' that is offset in a extent */
    pos = (nearPID != NULL) ? nearPID->pageNo - (nearPID->pageNo / volInfo->extSize) * volInfo->extSize + sizeOfTrain
			    : 0;

    /*
     * get and fix extent map buffer page & bit map buffer page
     */
    e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, midExtent);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    e = rdsm_getAndFixBitMapBuffer(handle, volInfo, midExtent);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    e = rdsm_GetExtentMapInfo(handle, midExtent, &(prevExtent->extentNo), &(nextExtent->extentNo));
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    e = rdsm_GetBitMapInfo(handle, midExtent, pos, volInfo->extSize-pos, &midExtent_eff);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    if (prevExtent->extentNo != NIL) {

        /*
         * get and fix extent map buffer page & bit map buffer page
         */
        e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, prevExtent);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        e = rdsm_getAndFixBitMapBuffer(handle, volInfo, prevExtent);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        e = rdsm_GetBitMapInfo(handle, prevExtent, 0, volInfo->extSize, &prevExtent_eff);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
    }
    else {
	prevExtent_eff = 0;
    }

    if (nextExtent->extentNo != NIL) {

        /*
         * get and fix extent map buffer page & bit map buffer page
         */
        e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, nextExtent);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        e = rdsm_getAndFixBitMapBuffer(handle, volInfo, nextExtent);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        e = rdsm_GetBitMapInfo(handle, nextExtent, 0, volInfo->extSize, &nextExtent_eff);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
    }
    else {
	nextExtent_eff = 0;
    }

    numOfEmptyTrains = ( MAX(0, midExtent_eff  + (eff - volInfo->extSize)) +
			 MAX(0, prevExtent_eff + (eff - volInfo->extSize)) +
			 MAX(0, nextExtent_eff + (eff - volInfo->extSize)) ) / sizeOfTrain;

    /* calculate number of extent that will be allocated newly */
    numOfNewExtent = (numOfTrains > numOfEmptyTrains) ? ((numOfTrains - numOfEmptyTrains) * sizeOfTrain - 1) / volInfo->extSize + 1
                                                      : 0;

    /* get array of new extent info */
    newExtent = (AllocAndFreeExtentInfo_T*)malloc(sizeof(AllocAndFreeExtentInfo_T) * numOfNewExtent);
    if (newExtent == NULL) ERR(handle, eMEMORYALLOCERR);

    /* initialize alloc & free extent info */
    e = rdsm_InitAllocAndFreeExtentInfo(handle, newExtent, numOfNewExtent);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /* set '_nextExtent' which points the extent of next to 'midExtent' */
    _nextExtent = nextExtent;

    /* get the extentmap and bitmap buffer latchs of new extent */
    for (i = 0; i < numOfNewExtent; i++) {

 	/* allocate an extent for this segment */
    	e = rdsm_GetExtentFromFreeExtentList(handle, volInfo, &newExtent[i].extentNo, sizeOfTrain);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    	e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, &newExtent[i]);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP2_L1(handle, e, extent, 3, newExtent, numOfNewExtent, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    	e = rdsm_getAndFixBitMapBuffer(handle, volInfo, &newExtent[i]);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP2_L1(handle, e, extent, 3, newExtent, numOfNewExtent, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

	e = rdsm_RemoveExtentFromFreeExtentList(handle, xactEntry, volInfo, &newExtent[i], sizeOfTrain, logParam);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP2_L1(handle, e, extent, 3, newExtent, numOfNewExtent, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

	e = rdsm_InsertExtentToSegment(handle, xactEntry, volInfo, midExtent, _nextExtent, &newExtent[i], logParam);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP2_L1(handle, e, extent, 3, newExtent, numOfNewExtent, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /* update '_nextExtent' */
        _nextExtent = &newExtent[i];
    }

    /*
     * Nested Top Action End.
     * Note: This is for extent map. and This must be called before the lath of page alloc & dealloc is released.
     */
    ENABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP2_L1(handle, e, extent, 3, newExtent, numOfNewExtent, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    DISABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

    /*
     * ### Critical Section End ###
     * release the latch of page alloc & dealloc
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex);
    if (e < eNOERROR) ERR(handle, e);


    if (remainTrains > 0) {

	if (nearPID == NULL || (pos % volInfo->extSize) != 0) {

            /*
             * allocate the trains in a middle extent
             */
	    e = rdsm_AllocTrainsInExtent(handle, xactEntry, volInfo, midExtent, sizeOfTrain, pos,
		                         remainTrains, eff, &trainIDs[numTrains], &allocatedTrains, logParam);
	    if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

    	    remainTrains -= allocatedTrains;
	    numTrains += allocatedTrains;
	}
    }

    RDSM_FREE_EXTENTMAP_BUFFER(handle, midExtent, e);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

    RDSM_FREE_BITMAP_BUFFER(handle, midExtent, e);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

    if (numOfNewExtent > 0) {

        for (i = numOfNewExtent-1; i > 0; i--) {

            /*
             * allocate the trains in extens except last extent among new allocated extents
             */
	    e = rdsm_AllocTrainsInExtent(handle, xactEntry, volInfo, &newExtent[i], sizeOfTrain, 0,
					 remainTrains, eff, &trainIDs[numTrains], &allocatedTrains, logParam);
	    if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

	    remainTrains -= allocatedTrains;
	    numTrains += allocatedTrains;

            RDSM_FREE_EXTENTMAP_BUFFER(handle, &newExtent[i], e);
    	    if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

    	    RDSM_FREE_BITMAP_BUFFER(handle, &newExtent[i], e);
    	    if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);
	}
    }

    if (remainTrains > 0 && nextExtent->extentNo != NIL) {

        /*
         * allocate the trains in a next extents
         */
	e = rdsm_AllocTrainsInExtent(handle, xactEntry, volInfo, nextExtent, sizeOfTrain, 0,
				     remainTrains, eff, &trainIDs[numTrains], &allocatedTrains, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

        remainTrains -= allocatedTrains;
        numTrains += allocatedTrains;
    }

    if (nextExtent->extentNo != NIL) {

        RDSM_FREE_EXTENTMAP_BUFFER(handle, nextExtent, e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

        RDSM_FREE_BITMAP_BUFFER(handle, nextExtent, e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);
    }

    if (remainTrains > 0 && prevExtent->extentNo != NIL) {

        /*
         * allocate the trains in a previous extents
         */
	e = rdsm_AllocTrainsInExtent(handle, xactEntry, volInfo, prevExtent, sizeOfTrain, 0,
				     remainTrains, eff, &trainIDs[numTrains], &allocatedTrains, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

        remainTrains -= allocatedTrains;
        numTrains += allocatedTrains;
    }

    if (prevExtent->extentNo != NIL) {

        RDSM_FREE_EXTENTMAP_BUFFER(handle, prevExtent, e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

        RDSM_FREE_BITMAP_BUFFER(handle, prevExtent, e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);
    }

    if (numOfNewExtent > 0) {

        /*
         * allocate the trains in a last extent among new allocated extents
         */
	e = rdsm_AllocTrainsInExtent(handle, xactEntry, volInfo, &newExtent[0], sizeOfTrain, 0,
				     remainTrains, eff, &trainIDs[numTrains], &allocatedTrains, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

        remainTrains -= allocatedTrains;
        numTrains += allocatedTrains;

        RDSM_FREE_EXTENTMAP_BUFFER(handle, &newExtent[0], e);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);

        RDSM_FREE_BITMAP_BUFFER(handle, &newExtent[0], e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP2(handle, e, extent, 3, newExtent, numOfNewExtent);
    }

    free(newExtent);


    return(eNOERROR);
}


