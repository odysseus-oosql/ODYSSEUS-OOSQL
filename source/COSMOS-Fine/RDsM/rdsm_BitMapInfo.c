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
 * Module: rdsm_BitMapInfo.c
 *
 * Description:
 *  Manipulation bitmap page
 *
 * Exports:
 *  Four  rdsm_GetBitMapInfo(AllocAndFreeExtentInfo_T*, Four, Four, Four*)
 */



#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four rdsm_GetBitMapInfo(AllocAndFreeExtentInfo_T*, Four, Four, Four*)
 *
 * Description:
 *  Get information of bitmap in bitmap page
 *
 * Returns:
 *  Error code
 */
Four rdsm_GetBitMapInfo(
    Four                        handle,                 /* IN    handle */
    AllocAndFreeExtentInfo_T   	*extent, 		/* IN  extent information data structure */
    Four			offset, 		/* IN  offset in a extent */
    Four			size,			/* IN  # of trains to inspect */
    Four			*midExtent_eff		/* OUT # of empty trains */
)
{
    BitmapTrain_T               *bitmapTrain;		/* bitmap train */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_GetBitMapInfo(extent=%P, offset=%lD, size=%lD, midExtent_eff=%P)", extent, offset, size, midExtent_eff));


    /* get the bit map train */
    bitmapTrain = (BitmapTrain_T*)RDSM_BITMAP_BUFFER_ACC_CB(extent)->bufPagePtr;

    /* count the bit map */
    Util_CountBitsSet(handle, bitmapTrain->bytes, RDSM_BITMAP_OFFSET(extent) + offset, size, midExtent_eff);


    return (eNOERROR);
}

/*
 * Function: Four rdsm_SetBitMapInfo(XactTableEntry_T*, AllocAndFreeExtentInfo_T*, Four, Four, Four, Four*)
 *
 * Description:
 *  Update bitmap info in bitmap page
 *
 * Returns:
 *  Error code
 */
Four rdsm_SetBitMapInfo(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T            *xactEntry, 		/* IN  transaction table entry */
    AllocAndFreeExtentInfo_T    *extent, 		/* IN  extent information data structure */
    Four                        offset, 		/* IN  offset in a extent */
    Four                        number,			/* IN  # of trains to update */
    Four                        operation,		/* IN  operation */
    LogParameter_T		*logParam		/* IN  log parameter */
)
{

    BitmapTrain_T		*bitmapTrain;		/* bitmap train */
    Lsn_T             		lsn,_lsn1,_lsn2;        /* LSN of the newly written log record */
    Four              		logRecLen;              /* log record length */
    LOG_LogRecInfo_T  		logRecInfo;             /* log record information */
    LOG_Image_RDsM_UpdateBitmap_T updateBitmap; 	/* bitmap update image */
    Four			e;			/* returned error value */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_SetBitMapInfo(xactEntry=%P, extent=%P, offset=%lD, number=%lD, operation=%lD, logParam=%P)", xactEntry, extent, offset, number, operation, logParam));


    /* get the bit map train */
    bitmapTrain = (BitmapTrain_T*)RDSM_BITMAP_BUFFER_ACC_CB(extent)->bufPagePtr;

    if (operation == SET_BITS) {

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) {

            updateBitmap.start = RDSM_BITMAP_OFFSET(extent) + offset;
            updateBitmap.nBits = number;

            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_RDSM_FREE_TRAINS, LOG_REDO_ONLY,
                                  bitmapTrain->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(updateBitmap), &updateBitmap);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            bitmapTrain->hdr.lsn = lsn;
            bitmapTrain->hdr.logRecLen = logRecLen;
        }

    	/* set the bit map */
    	Util_SetBits(handle, bitmapTrain->bytes, RDSM_BITMAP_OFFSET(extent) + offset, number);
    }
    else {

        /*
         *  Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) {
            updateBitmap.start = RDSM_BITMAP_OFFSET(extent) + offset;
            updateBitmap.nBits = number;

            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_RDSM_ALLOC_TRAINS, LOG_REDO_UNDO,
                                  bitmapTrain->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(updateBitmap), &updateBitmap);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            bitmapTrain->hdr.lsn = lsn;
            bitmapTrain->hdr.logRecLen = logRecLen;
        }

    	/* clear the bit map */
    	Util_ClearBits(handle, bitmapTrain->bytes, RDSM_BITMAP_OFFSET(extent) + offset, number);
    }

    /* set dirty bit if needed */
    RDSM_SETDIRTYBIT_BITMAP_BUFFER(extent);


    return (eNOERROR);
}

