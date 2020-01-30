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
 * Module: rdsm_AllocTrainsInExtent.c
 *
 * Description:
 *  Allocate the trains in a extent.
 *
 * Exports:
 *  Four rdsm_AllocTrainsInExtent(rdsm_AllocTrainsInExtent*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, Four, Four, Four, PageID*, Four*, LogParameter_T*)
 */



#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four rdsm_AllocTrainsInExtent(rdsm_AllocTrainsInExtent*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, Four, Four, Four, Page ID*, Four*, LogParameter_T*)
 *
 * Description:
 *  Allocate the trains in a extent.
 *
 * Returns:
 *  Error code
 */
Four rdsm_AllocTrainsInExtent(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T		*xactEntry,		/* IN  transaction table entry */
    RDsM_VolumeInfo_T		*volInfo,		/* IN  volume information */
    AllocAndFreeExtentInfo_T	*extent,		/* IN  extent information data structure */
    Four			sizeOfTrain,		/* IN  size of train */
    Four                        pos,			/* IN  offset in a extent */
    Four                        numOfTrain,		/* IN  # of train to require */
    Four                        eff,			/* IN  extent fill factor */
    PageID			*trainIDs,		/* OUT allocated train IDs */
    Four                        *allocatedTrains,	/* OUT # of train allocated */
    LogParameter_T		*logParam		/* IN  log parameter */
)
{
    BitmapTrain_T		*bitmapTrain;		/* bitmap train */
    Four			i = 0;			/* index */
    Four 			ith;			/* ith in a extent */
    Four                        offset;			/* offset */
    Four			e;			/* returned error value */
    Four                        remainPages;		/* # of empty trains */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_AllocTrainsInExtent(xactEntry=%P, volInfo=%P, extent=%P, sizeOfTrain=%lD, pos=%lD, numOfTrain=%lD, eff=%lD, trainIDs=%P, allocatedTrains=%P, logParam=%p)", xactEntry, volInfo, extent, sizeOfTrain, pos, numOfTrain, eff, trainIDs, allocatedTrains, logParam));


    bitmapTrain = (BitmapTrain_T*)RDSM_BITMAP_BUFFER_ACC_CB(extent)->bufPagePtr;
    offset = RDSM_BITMAP_OFFSET(extent);

    /* set 'remainPages' */
    Util_CountBitsSet(handle, bitmapTrain->bytes, offset, volInfo->extSize, &remainPages);

    while (volInfo->extSize - remainPages < eff) {

        /*
         * find a empty trains in a extent
         */
	if (pos == volInfo->extSize || i == numOfTrain) break;

  	Util_FindBits(handle, bitmapTrain->bytes, offset+pos, volInfo->extSize-pos, sizeOfTrain, &ith);
	if (ith == NIL) break;

        /* update bit map */
	e = rdsm_SetBitMapInfo(handle, xactEntry, extent, pos+ith, sizeOfTrain, CLEAR_BITS, logParam);
	if (e < eNOERROR) ERR(handle, e);

	trainIDs[i].volNo = volInfo->volNo;
	trainIDs[i].pageNo = extent->extentNo * volInfo->extSize + pos + ith;

	i++;
	pos += ith + sizeOfTrain;
	remainPages -= sizeOfTrain;
    }

    *allocatedTrains = i;


    return (eNOERROR);
}
