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
 * Module: RDsM_ExtentReorganization.c
 *
 * Description:
 *  Reorganize the extent.
 *    In particular, Insert the extent that is not 1st extent of the segment and has only empty pages to free extent list.
 *
 * Exports:
 *  Four RDsM_ExtentReorganization(XactTableEntry_T*, Four)
 */


#include <stdlib.h>
#include <assert.h>
#include <string.h>
#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif /* WIN32 */
#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "SHM.h"
#include "RDsM.h"
#include "TM.h"
#include "LOG.h"
#include "Util_varArray.h"
#include "Util_heap.h"
#include "perProcessDS.h"
#include "perThreadDS.h"





/*
 * Function: Four RDsM_ExtentReorganization(XactTableEntry_T*, Four)
 *
 * Description:
 *  Reorganize the extent.
 *    In particular, Insert the extent that is not 1st extent of the segment and has only empty pages to free extent list.
 *
 * Returns:
 *  Error code
 */
Four RDsM_ExtentReorganization(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T            *xactEntry,             /* IN transaction table entry */
    Four			volNo                   /* IN volume ID */
)
{
    Four			e;			/* return error code */
    Four                        entryNo;                /* entry no of volume table entry */
    RDsM_VolumeInfo_T           *volInfo;               /* volume information in volume table entry */
    Four			i;			/* loop index */
    Boolean			*map;			/* virtual map */
    AllocAndFreeExtentInfo_T    *extent;
    AllocAndFreeExtentInfo_T    *prevExtent;
    AllocAndFreeExtentInfo_T    *midExtent;
    AllocAndFreeExtentInfo_T    *nextExtent;
    Four			prevExtent;
    Four			nextExtent;
    Four			numOfEmptyPages;



    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_ExtentReorganization(volNo=%lD)", volNo));


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
     * initialize the virtual map that has a extent information to free
     */
    map = (Boolean*)malloc(sizeof(Boolean) * volInfo->numExts);

    for (i=0; i<volInfo->numExts; i++) map[i] = TRUE;


    /*
     * initialize the extent info
     */
    e = rdsm_InitAllocAndFreeExtentInfo(handle, extent, 3);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

    prevExtent = &extent[0]; midExtent = &extent[1]; nextExtent = &extent[2];


    /*
     * ### Critical Section Start ###
     *
     * get the latch of page alloc & dealloc
     */
    e = SHM_getLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * find a free extent in the free extent list;
     */
    for (i=0; i<3; i++) {


	switch (i) {

	    case 0: midExtent->extentNo = volInfo->dataVol.freeExtent;
                    break;

	    case 1: midExtent->extentNo = volInfo->dataVol.freePageExtent;
                    break;

	    case 2: midExtent->extentNo = volInfo->dataVol.freeTrainExtent;
                    break;
	}

        while (midExtent->extentNo != NIL) {

            /* get and fix the extent map buffer */
       	    e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, midExtent);
    	    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* get the extent map info */
            e = rdsm_GetExtentMapInfo(handle, midExtent, &prevExt, &nextExt);
    	    if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* free the extent map buffer */
            RDSM_FREE_EXTENTMAP_BUFFER(handle, midExtent, e);
    	    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* set the extent informaion */
            map[midExtent->extentNo] = FALSE;

            /* for next loop */
            midExtent->extentNo = nextExt;
        }
    }


    /*
     * find the extent that has only empty pages and is not 1st extent of the segment;
     */
    for (i=0; i<volInfo->numExts; i++) {

        /* it is not necessary about the extent that is in the free extent list */
	if (map[i] == FALSE) continue;

        /* set extent.extentNo */
	midExtent->extentNo = i;

        /* get and fix the bit map buffer */
        e = rdsm_getAndFixBitMapBuffer(handle, volInfo, midExtent);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /* get the bit map info */
        e = rdsm_GetBitMapInfo(handle, midExtent, 0, volInfo->extSize, &numOfEmptyPages);
    	if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        if (numOfEmptyPages == volInfo->extSize) {

            /* get and fix the extent map buffer */
            e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, midExtent);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* get the extent map info */
            e = rdsm_GetExtentMapInfo(handle, midExtent, &prevExt, &nextExt);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* free the extent map buffer */
            RDSM_FREE_EXTENTMAP_BUFFER(handle, midExtent, e);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

            /* set the extent informaion */
            if (prevExt == NIL) map[midExtent->extentNo] = FALSE;
        }
        else {
            /* set the extent informaion */
            map[midExtent->extentNo] = FALSE;
	}

        /* free the bit map buffer */
        RDSM_FREE_BITMAP_BUFFER(handle, midExtent, e);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
    }


    /*
     * free the extent that is not 1st extent of the segment and has only empty pages.
     */
    for (i=0; i<volInfo->numExts; i++) {

        /* it is not necessary about the extent that is in the free extent list */
	if (map[i] == FALSE) continue;

        /* set extent.extentNo */
	midExtent->extentNo = i;

        /* get and fix the extent map buffer */
        e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, midExtent);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /* get the extent map info */
        e = rdsm_GetExtentMapInfo(handle, midExtent, &(prevExtent->extentNo), &(nextExtent->extentNo));
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /* get the extentmap buffer latch of the prev extent */
        if (prevExtent->extentNo != NIL) {
            e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, prevExtent);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
	}

        /* get the extentmap buffer latch of the next extent */
        if (nextExtent->extentNo != NIL) {
            e = rdsm_getAndFixExtentMapBuffer(handle, volInfo, nextExtent);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
        }

        fprintf(stderr, "Extent Reorganization: Extent No.%ld will be freed\n", midExtent->extentNo);

        /* remove the midExtent from the segment */
        e = rdsm_RemoveExtentFromSegment(handle, xactEntry, volInfo, prevExtent, midExtent, nextExtent, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /* insert the midExtent to the free extent list */
        sizeOfTrain를 주의 할 것.
        /* Be careful of sizeOfTrain */ 
        e = rdsm_InsertExtentToFreeExtentList(handle, xactEntry, volInfo, midExtent, sizeOfTrain, 1, logParam);
        if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));

        /* free the extentmap buffer latch of the prev extent */
        if (prevExtent->extentNo != NIL) {
            RDSM_FREE_EXTENTMAP_BUFFER(handle, prevExtent, e);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
	}

        /* free the extentmap buffer latch of the prev extent */
        if (nextExtent->extentNo != NIL) {
            RDSM_FREE_EXTENTMAP_BUFFER(handle, nextExtent, e);
            if (e < eNOERROR) ERR_EXTMAP_BITMAP_L1(handle, e, midExtent, 1, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
	}

	/* free the extentmap buffer latch */
        RDSM_FREE_EXTENTMAP_BUFFER(handle, midExtent, e);
	if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo));
    }


    /*
     * ### Critical Section End ###
     * release the latch of page alloc & dealloc
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_PAGEALLOCDEALLOC(volInfo), procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * destroy the virtual map
     */
    free(map);


    return (eNOERROR);
}
