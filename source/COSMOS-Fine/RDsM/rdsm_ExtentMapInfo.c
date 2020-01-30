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
 * Module: rdsm_ExtentMapInfo.c
 *
 * Description:
 *  Manipulation extent map
 *
 * Exports:
 *  Four rdsm_InitAllocAndFreeExtentInfo(AllocAndFreeExtentInfo_T*, Four)
 *  Four rdsm_GetExtentMapInfo(AllocAndFreeExtentInfo_T*, Four*, Four*)
 *  Four rdsm_SetExtentMapInfo(XactTableEntry_T*, AllocAndFreeExtentInfo_T*, Four, Four, LogParameter_T*)
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
 * Function: Four rdsm_InitAllocAndFreeExtentInfo(AllocAndFreeExtentInfo_T*, Four)
 *
 * Description:
 *  Initialize extent information data structure
 *
 * Returns:
 *  Error code
 */
Four rdsm_InitAllocAndFreeExtentInfo(
    Four                        handle,         /* IN  handle */
    AllocAndFreeExtentInfo_T	*extent, 	/* IN  pointer of extent information data structures */
    Four			num		/* IN  # of extent information data structure */
)
{
    Four 			i;		/* for loop */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_InitAllocAndFreeExtentInfo(AllocAndFreeExtentInfo_T=X, num=%lD)", extent, num));


    for (i=0; i<num; i++) {

        /* initialize 'extentNo' */
	extent[i].extentNo = NIL;

        /* initialize 'extentmapPage_BCBP' */
	extent[i].extentmapPage_BCBP = NULL;

        /* initialize 'bitmapTrain_BCBP' */
	extent[i].bitmapTrain_BCBP = NULL;
    }


    return (eNOERROR);
}


/*
 * Function: Four rdsm_GetExtentMapInfo(AllocAndFreeExtentInfo_T*, Four*, Four*)
 *
 * Description:
 *  Get extent info from extent map page
 *
 * Returns:
 *  Error code
 */
Four rdsm_GetExtentMapInfo(
    Four                        handle,                 /* IN    handle */
    AllocAndFreeExtentInfo_T	*extent,		/* IN  extent information data structure */
    Four			*prevExt,		/* OUT previous extent of extent pointed by 'extent' */
    Four			*nextExt		/* OUT next extent of extent pointed by 'extent' */
)
{
    ExtentMapEntry_T		*extentMapEntry;	/* extent map entry */
    ExtentMapPage_T             *extentMapPage;		/* extent map page */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_GetExtentMapInfo(extent=%P, prevExt=%P, nextExt=%P)", extent, prevExt, nextExt));

    /* get extent map entry */
    extentMapPage = (ExtentMapPage_T*)(RDSM_EXTENTMAP_BUFFER_ACC_CB(extent) -> bufPagePtr);
    extentMapEntry = &((extentMapPage->entry)[RDSM_EXTENTMAP_OFFSET(extent)]);

    /* set 'prevExt' & 'nextExt' */
    if (prevExt != NULL) *prevExt = extentMapEntry -> prevExt;
    if (nextExt != NULL) *nextExt = extentMapEntry -> nextExt;


    return (eNOERROR);
}


/*
 * Function: Four rdsm_SetExtentMapInfo(XactTableEntry_T*, AllocAndFreeExtentInfo_T*, Four, Four, LogParameter_T*)
 *
 * Description:
 *  Set extent info to extent map page
 *
 * Returns:
 *  Error code
 */
Four rdsm_SetExtentMapInfo(
    Four                        handle,                		/* IN handle */
    XactTableEntry_T            *xactEntry,             	/* IN transaction table entry */
    AllocAndFreeExtentInfo_T	*extent,			/* IN extent information data structure */
    Four			prevExt,			/* IN previous extent No. */
    Four			nextExt,			/* IN next extent No. */
    LogParameter_T              *logParam               	/* IN log parameter */
)
{
    Four			e;				/* returned error value */
    ExtentMapEntry_T		*extentMapEntry;		/* extent map entry */
    ExtentMapPage_T             *extentMapPage;			/* extent map page */
    Lsn_T                       lsn;                    	/* LSN of the newly written log record */
    Lsn_T                       _lsn1;                    	/* LSN of the newly written log record */
    Lsn_T                       _lsn2;                    	/* LSN of the newly written log record */
    Four                        logRecLen;              	/* log record length */
    LOG_LogRecInfo_T            logRecInfo;         		/* log record information */
    Four                        updateExtentMapIndex;   	/* log record about extent map index */
    Four		  	sizeOfLOG_Image_RDsM_Modify_ExtentLink_T; 	/* size of 'LOG_Image_RDsM_Modify_ExtentLink_T' */
    LOG_Image_RDsM_Modify_ExtentLink_T updateExtentLink[2]; 	/* log image of RDsM_Modify_ExtentLink_T */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_SetExtentMapInfo(xactEntry=X, extent=%P, prevExt=%lD, nextExt=%lD, logParam=%P)", xactEntry, extent, prevExt, nextExt, logParam));


    /* get extent map entry */
    extentMapPage = (ExtentMapPage_T*)(RDSM_EXTENTMAP_BUFFER_ACC_CB(extent) -> bufPagePtr);
    extentMapEntry = &((extentMapPage->entry)[RDSM_EXTENTMAP_OFFSET(extent)]);


    /*
     *  Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) {

        /*
         *  set pointers pointing to respective images
         *
         *  logRecInfo->imageData[0] : index of extent map array in the page
         *  logRecInfo->imageData[1] : updateExtentLink[0] : new extent map entry info
         *  logRecInfo->imageData[2] : updateExtentLink[1] : old extent map entry info
         */

        updateExtentMapIndex                                = RDSM_EXTENTMAP_OFFSET(extent);
        updateExtentLink[0].prevExt                         = prevExt;
        updateExtentLink[0].nextExt                         = nextExt;
        updateExtentLink[1].prevExt                         = extentMapEntry->prevExt;
        updateExtentLink[1].nextExt                         = extentMapEntry->nextExt;
        sizeOfLOG_Image_RDsM_Modify_ExtentLink_T            = sizeof(LOG_Image_RDsM_Modify_ExtentLink_T);

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_RDSM_MODIFY_EXTENT_LINK, LOG_REDO_UNDO,
                              extentMapPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Four), &updateExtentMapIndex,
                              sizeOfLOG_Image_RDsM_Modify_ExtentLink_T, &updateExtentLink[0],
                              sizeOfLOG_Image_RDsM_Modify_ExtentLink_T, &updateExtentLink[1]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        extentMapPage->hdr.lsn = lsn;
        extentMapPage->hdr.logRecLen = logRecLen;
    }

    /* update extent map info */
    if (prevExt != NO_OP) extentMapEntry -> prevExt = prevExt;
    if (nextExt != NO_OP) extentMapEntry -> nextExt = nextExt;

    RDSM_SETDIRTYBIT_EXTENTMAP_BUFFER(extent);


    return (eNOERROR);
}
