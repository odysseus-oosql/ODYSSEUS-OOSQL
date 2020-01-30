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
 * Module: rdsm_AllocContigTrainsInExt.c
 *
 * Description:
 *  Allocate the contiguous trains.
 *
 * Exports:
 *  Four rdsm_AllocContigTrainsInExt(XactTableEntry_T*, Four, SegmentID_T*, PageID*, Four*, Four, Four, PageID*, LogParameter_T*)
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
 * Function: Four rdsm_AllocContigTrainsInExt(XactTableEntry_T*, Four, SegmentID_T*, PageID*, Four*, Four, Four, PageID*, LogParameter_T*)
 *
 * Description:
 *  Allocate the contiguous trains in a extent.
 *
 * Returns:
 *  Error code
 */
Four rdsm_AllocContigTrainsInExt(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T  		*xactEntry,         	/* IN    transaction table entry */
    Four              		volNo,              	/* IN    volume number in question */
    SegmentID_T        		*segmentID,    		/* IN    segment identifier */
    PageID            		*startTrainID,      	/* IN    start train ID of contiguous allocated trains in the extent */
    Four              		*numOfTrains,       	/* INOUT number of allocated trains */
    Four              		sizeOfTrain,        	/* IN    size of a train to be allocated */
    Four              		eff,                	/* IN    number of pages in an extent to keep filled */
    PageID            		*trainIDs,          	/* OUT   array for train ID which allocated */
    LogParameter_T    		*logParam          	/* IN    log parameter */
)
{
    Four        		e;             		/* returned error code */
    Four              		entryNo;                /* entry no of volume table entry corresponding to the given volume */
    RDsM_VolumeInfo_T 		*volInfo;               /* volume information in volume table entry */
    AllocAndFreeExtentInfo_T   	*extent;                /* new alloc extent */
    Four                        numOfNewExtent;         /* number of new alloc extent */
    AllocAndFreeExtentInfo_T    *midExtent;             /* pointer of middle extent */
    AllocAndFreeExtentInfo_T    *nextExtent;		/* pointer of next extent */
    AllocAndFreeExtentInfo_T    *newExtent;		/* pointer of new extent */
    AllocAndFreeExtentInfo_T    *ptrOfExtent;		/* pointer of a extent */
    BitmapTrain_T               *bitmapTrain;		/* bitmap train */
    Four                        idx = 0;		/* index */
    Four                        i = 0;			/* index */
    Four                        pos = 0;		/* position */
    Four                        ith;			/* ith in a extent */
    Four                        offset;			/* offset */
    Four                        remainPages;		/* remain pages to allocate */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_AllocContigTrainsInExt(xactEntry=%P, volNo=%lD, segmentID=%P, startTrainID=%lD, numOfTrains=%lD, sizeOfTrain=%lD, eff=%lD, trainIDs=%P, logParam=%p)", xactEntry, volNo, segmentID, startTrainID, numOfTrains, sizeOfTrain, eff, trainIDs, logParam));


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
     * if startTrainID points the last trains in the extent, there is no trains to allocate
     */
    if ( startTrainID != NULL && (startTrainID->pageNo + sizeOfTrain) % volInfo->extSize == 0 ) {
 	*numOfTrains = 0;
        return (eNOERROR);
    }

    /*
     * Note:
     *  conversion of the extent fill factor in the number of pages
     *  the unit of the extent fill factor is the number of pages in RDsM
     *  whicl the unit of the extent fill factor, input parameter is % in the upper level
     */
    eff = (eff * volInfo->extSize) / 100;

    /*
     * count the number of new extent
     */
    numOfNewExtent = (startTrainID == NULL) ? ((*numOfTrains * sizeOfTrain - 1) / volInfo->extSize + 1)
					    : 1;
    /*
     * allocate alloc & free extent info
     */
    extent = (AllocAndFreeExtentInfo_T*)malloc(sizeof(AllocAndFreeExtentInfo_T) * (numOfNewExtent + 2));
    if (extent == NULL) ERR(handle, eMEMORYALLOCERR);

    /*
     * initialize alloc & free extent info
     */
    e = rdsm_InitAllocAndFreeExtentInfo(handle, extent, numOfNewExtent + 2);
    if (e < eNOERROR) ERR(handle, e);

    midExtent = &extent[0]; nextExtent = &extent[1];


    /*
     * ### Critical Section Start ###
     *
     * get the latch of page alloc & dealloc
     */
    e = SHM_getLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* if startPID is given, allocate contiguous trains after startPID in the extent */
    if (startTrainID != NULL){

        midExtent->extentNo = startTrainID->pageNo / volInfo->extSize;

        /* get the extentmap buffer latch */
        e = rdsm_getAndFixBitMapBuffer(handle, volInfo, midExtent);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        ptrOfExtent = midExtent;
    }
    else { /* if startPID is NULL, allocate new extent, then allocate contiguous trains in that extent */

        /*
         * Nested Top Action Start.
         * Note: This is for extent map. and This must be called after the lath of page alloc & dealloc is acquired.
         */
        e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        midExtent->extentNo = segmentID->firstExtent;

        /* get the extentmap buffer latch */
        e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, midExtent);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

	for (idx=0; idx<numOfNewExtent; idx++) {

	    newExtent = &extent[idx+2];

            /* get new extent No. from free extent list */
            e = rdsm_GetExtentFromFreeExtentList(handle, volInfo, &newExtent->extentNo, sizeOfTrain);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, &extent[2], numOfNewExtent, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* get the extentmap buffer latch */
            e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, newExtent);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* get the bitmap buffer latch */
            e = rdsm_getAndFixBitMapBuffer(handle, volInfo, newExtent);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* allocate an extent for this segment */
            e = rdsm_RemoveExtentFromFreeExtentList(handle, xactEntry, volInfo, newExtent, sizeOfTrain, logParam);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* get the next extent No. */
            e = rdsm_GetExtentMapInfo(handle, midExtent, NULL, &(nextExtent->extentNo));
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            if (nextExtent->extentNo != NIL) {

                e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, nextExtent);
                if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
	    }

            e = rdsm_InsertExtentToSegment(handle, xactEntry, volInfo, midExtent, nextExtent, newExtent, logParam);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* free the extentmap buffer latch */
            RDSM_FREE_EXTENTMAP_BUFFER(handle, newExtent, e);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

	    if (nextExtent->extentNo != NIL) {

                /* free the extentmap buffer latch */
                RDSM_FREE_EXTENTMAP_BUFFER(handle, nextExtent, e);
                if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
	    }
        }

        /* free the extentmap buffer latch */
        RDSM_FREE_EXTENTMAP_BUFFER(handle, midExtent, e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, numOfNewExtent+2, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /*
         * Nested Top Action End.
         * Note: This is for extent map. and This must be called before the lath of page alloc & dealloc is released.
         */
        ENABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

        e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        DISABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

        ptrOfExtent = &extent[2];
    }

    /*
     * ### Critical Section End ###
     *
     * release the latch of page alloc & dealloc
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex);
    if (e < eNOERROR) ERR(handle, e);


    for (idx=0; idx<numOfNewExtent; idx++) {

        /* allocate trains in extent that is pointed by 'ptrOfExtent' */
        bitmapTrain = (BitmapTrain_T*)RDSM_BITMAP_BUFFER_ACC_CB(&ptrOfExtent[idx])->bufPagePtr;
        offset = RDSM_BITMAP_OFFSET(&ptrOfExtent[idx]);
        pos = (startTrainID == NULL) ? 0
				     : startTrainID->pageNo + sizeOfTrain
							    - ( (startTrainID->pageNo / volInfo->extSize) * volInfo->extSize);

        /* set 'remainPages' */
        Util_CountBitsSet(handle, bitmapTrain->bytes, offset, volInfo->extSize, &remainPages);

        while (volInfo->extSize - remainPages < eff) {

            /* find the empty trains */
            Util_FindBits(handle, bitmapTrain->bytes, offset+pos, volInfo->extSize-pos, sizeOfTrain, &ith);
            if (ith == NIL || ith != 0) break;

            /* update bitmap */
            e = rdsm_SetBitMapInfo(handle, xactEntry, &ptrOfExtent[idx], pos+ith, sizeOfTrain, CLEAR_BITS, logParam);
            if (e < eNOERROR) ERR(handle, e);

            trainIDs[i].volNo = volInfo->volNo;
            trainIDs[i].pageNo = (&ptrOfExtent[idx])->extentNo * volInfo->extSize + pos + ith;

            i++;
            pos += sizeOfTrain;
            remainPages -= sizeOfTrain;

            if (pos == volInfo->extSize) break;
        }

        /* free bitmap buffer page */
        RDSM_FREE_BITMAP_BUFFER(handle, &ptrOfExtent[idx], e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, ptrOfExtent, 1);
    }

    /* set 'numOfTrains' that is a number of extent to be allocated */
    *numOfTrains = i;

    /*
     * free alloc & free extent info
     */
    free(extent);


    return (eNOERROR);
}


