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
 * Module: RDsM_FreeTrain.c
 *
 * Description:
 *  Free the train pointed by 'trainId'.
 *
 * Exports:
 *  Four RDsM_FreeTrain(XactTableEntry_T*, PageID*, Four, Boolean, LogParameter_T*)
 */


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
 * Function: Four RDsM_FreeTrain(XactTableEntry_T*, PageID*, Four, Boolean, LogParameter_T*)
 *
 * Description:
 *  Free the train pointed by 'trainId'.
 *
 * Returns:
 *  Error code
 */

Four RDsM_FreeTrain(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T  		*xactEntry,    		/* IN transaction table entry */
    PageID 			*trainId,               /* IN deallocate as near this pid as possible */
    Four 			sizeOfTrain,            /* IN size of a train to be allocated */
    Boolean           		immediateFlag,          /* IN deallocate immediate if TRUE */
    LogParameter_T    		*logParam  		/* IN log parameter */
)
{
    Four        		e;             		/* returned error code */
    Four              		entryNo;                /* entry no of volume table entry corresponding to the given volume */
    RDsM_VolumeInfo_T 		*volInfo;               /* volume information in volume table entry */
    Four                	numOfEmptyTrains;       /* number of empty trains in a extent */
    PageID            		bmTrainId;              /* bitmap train identifier */
    BitmapTrain_T     		*bitmapTrain;           /* pointer to a train buffer */
    Buffer_ACC_CB     		*bitmapTrain_BCBP;      /* buffer page access control block */
    PageID            		extentmapPageId;        /* extenetmap page identifier */
    ExtentMapPage_T    		*extentmapPage;         /* pointer to a page buffer */
    Buffer_ACC_CB     		*extentmapPage_BCBP;    /* buffer page access control block */
    Four                        offsetInExtent;         /* offset in the extent */
    Four 			midExtent_eff;          /* # of empty pages in middle extent */
    AllocAndFreeExtentInfo_T	extent[3];              /* extent info data structure */
    AllocAndFreeExtentInfo_T	*prevExtent;            /* pointer of previous extent */
    AllocAndFreeExtentInfo_T	*midExtent;             /* pointer of middle extent */
    AllocAndFreeExtentInfo_T	*nextExtent;            /* pointer of next extent */


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_FreeTrain(xactEntry=%P, trainId=%P, sizeofTrain=%lD, immediateFlag=%lD, logParam=%p)", xactEntry, trainId, sizeOfTrain, immediateFlag, logParam));


    /*
     * check parameters
     */
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eBADPARAMETER);


    /* check 'immediateFlag'.
     * Then if 'immediateFlag' is false, put the train to 'DeallocPageList'
     */
    if (!immediateFlag) {
        if (sizeOfTrain == PAGESIZE2)
            e = TM_XT_AddToDeallocPageList(handle, xactEntry, trainId);
        else
            e = TM_XT_AddToDeallocTrainList(handle, xactEntry, trainId);
        if (e < eNOERROR) ERR(handle, e);

        return(eNOERROR);
    }


    /*
     *  get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, trainId->volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * set a pointer to the corresponding entry
     */
    volInfo = &(RDSM_VOLTABLE[entryNo].volInfo);


    /*
     * initialize alloc & free extent info
     */
    e = rdsm_InitAllocAndFreeExtentInfo(handle, extent, 3);
    if (e < eNOERROR) ERR(handle, e);

    midExtent = &extent[0]; prevExtent = &extent[1]; nextExtent = &extent[2];

    /* set 'midExtent' */
    midExtent->extentNo = trainId->pageNo / volInfo->extSize;


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

    /* get the extentmap buffer latch */
    e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, midExtent);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /* get the bitmap buffer latch */
    e = rdsm_getAndFixBitMapBuffer(handle, volInfo, midExtent);
    if (e < eNOERROR) ERRBL1L1(handle, e, RDSM_EXTENTMAP_BUFFER_ACC_CB(midExtent), PAGE_BUF, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /* get extent map info from the extent */
    e = rdsm_GetExtentMapInfo(handle, midExtent, &(prevExtent->extentNo), &(nextExtent->extentNo));
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    e = rdsm_GetBitMapInfo(handle, midExtent, 0, volInfo->extSize, &midExtent_eff);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    /* check where this extent must be inserted to free extent list */
    if (midExtent_eff + sizeOfTrain == volInfo->extSize && prevExtent->extentNo != NIL) {

    	/* get the extentmap buffer latch of the previous extent */
        /* NOTE: prevExtent must not be a NIL */
        e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, prevExtent);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        if (nextExtent->extentNo != NIL) {
    	    /* get the extentmap buffer latch of the next extent */
    	    e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, nextExtent);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
        }

        e = rdsm_RemoveExtentFromSegment(handle, xactEntry, volInfo, prevExtent, midExtent, nextExtent, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        e = rdsm_InsertExtentToFreeExtentList(handle, xactEntry, volInfo, midExtent, sizeOfTrain, 1, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
    }

    /*
     * Nested Top Action End.
     * Note: This is for extent map. and This must be called before the lath of page alloc & dealloc is released.
     */
    ENABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, extent, 3, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    DISABLE_EXTENT_MAP_LOGGING_FLAG(logParam); 

    /*
     * ### Critical Section End ###
     *
     * release the latch of page alloc & dealloc
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * set the bit map
     */
    offsetInExtent = trainId->pageNo - midExtent->extentNo * volInfo->extSize;

    e = rdsm_SetBitMapInfo(handle, xactEntry, midExtent, offsetInExtent, sizeOfTrain, SET_BITS, logParam);
    if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, extent, 3);

    /* free prevExtent & nextExtent extent buffer */
    if (midExtent_eff + sizeOfTrain == volInfo->extSize && prevExtent->extentNo != NIL) {

        /* free the extentmap buffer latch */
        RDSM_FREE_EXTENTMAP_BUFFER(handle, prevExtent, e);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, extent, 3);

        if (nextExtent->extentNo != NIL) {

            /* free the extentmap buffer latch */
            RDSM_FREE_EXTENTMAP_BUFFER(handle, nextExtent, e);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP(handle, e, extent, 3);
        }
    }

    /* free the extentmap buffer latch */
    RDSM_FREE_EXTENTMAP_BUFFER(handle, midExtent, e);
    if (e < eNOERROR) ERRBL1(handle, e, RDSM_BITMAP_BUFFER_ACC_CB(midExtent), PAGE_BUF);

    /* free the bitmap buffer latch */
    RDSM_FREE_BITMAP_BUFFER(handle, midExtent, e);
    if (e < eNOERROR) ERR(handle, e);


    return (eNOERROR);
}


