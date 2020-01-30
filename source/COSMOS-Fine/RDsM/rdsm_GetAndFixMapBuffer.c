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
 * Module: rdsm_GetAndFixMapBuffer.c
 *
 * Description:
 *  Get and Fix buffer page of bitmap & extentmap
 *
 * Exports:
 *  Four rdsm_getAndFixBitMapBuffer(RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*)
 *  rdsm_getAndFixExtentMapBuffer(RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*)
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
 * Function: Four rdsm_getAndFixExtentMapBuffer(RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*)
 *
 * Description:
 *  Get and Fix extent map buffer page
 *
 * Returns:
 *  Error code
 */
Four rdsm_getAndFixExtentMapBuffer(
    Four                        handle,                 /* IN    handle */
    RDsM_VolumeInfo_T   	*volInfo,		/* IN     volume information */
    AllocAndFreeExtentInfo_T    *extent			/* INOUT  extent information to getAndFix */
)
{
    Four			e;			/* returned error value */
    Four			devNo;			/* device no */
    Four			trainOffset;		/* train offset */
    Four			extentOffset;		/* extent offset */
    Four                        extentmapOffset;	/* extent map offset */
    RDsM_DevInfoForDataVol      *devInfoForDataVol;	/* device info for data volume */
    PageID			extentmapPageId;	/* extent map page ID */
    Buffer_ACC_CB		*extentmapPage_BCBP;	/* BCBP of extent map Page */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_getAndFixExtentMapBuffer(volInfo=%P, extent=%P)", volInfo, extent));


    /* get physical infor from extent */
    e = rdsm_GetPhysicalInfo(handle, volInfo, extent->extentNo * volInfo->extSize, &devNo, &trainOffset);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate extent No. */
    extentOffset = trainOffset / volInfo->extSize;

    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo);

    /* get extentmap page ID */
    extentmapPageId.volNo = devInfoForDataVol[devNo].extentMapPageId.volNo;
    extentmapPageId.pageNo = devInfoForDataVol[devNo].extentMapPageId.pageNo +
			   (extentOffset/volInfo->dataVol.numExtentMapEntryInPage)*PAGESIZE2;

    /* get and fix extentmap buffer page */
    e = BfM_getAndFixBuffer(handle, &extentmapPageId, M_EXCLUSIVE, &extentmapPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate offset in a extentmap page */
    extentmapOffset = extentOffset % (volInfo->dataVol.numExtentMapEntryInPage);


    /* set 'extentmapPage_BCBP' & 'extentmapOffset' */
    extent->extentmapPage_BCBP = extentmapPage_BCBP;
    extent->extentmapOffset = extentmapOffset;


    return (eNOERROR);
}



/*
 * Function: Four rdsm_getAndFixBitMapBuffer(RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*)
 *
 * Description:
 *  Get and Fix bitmap map buffer page
 *
 * Returns:
 *  Error code
 */
Four rdsm_getAndFixBitMapBuffer(
    Four                        handle,                 /* IN    handle */
    RDsM_VolumeInfo_T   	*volInfo,		/* IN    volume information */
    AllocAndFreeExtentInfo_T    *extent			/* INOUT extent information data structure */
)
{
    Four			e;			/* returned error value */
    Four			devNo;			/* device no */
    Four			trainOffset;		/* train offset */
    Four			extentOffset;		/* extent offset */
    Four                        bitmapOffset;		/* bitmap offset */
    RDsM_DevInfoForDataVol      *devInfoForDataVol;	/* device info for data volume */
    PageID			bitmapTrainId;		/* bitmap train ID */
    Buffer_ACC_CB		*bitmapTrain_BCBP;	/* BCBP of bitmap train */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_getAndFixBitMapBuffer(volInfo=%P, extent=%P)", volInfo, extent));


    /* get physical info from extent */
    e = rdsm_GetPhysicalInfo(handle, volInfo, extent->extentNo * volInfo->extSize, &devNo, &trainOffset);
    if (e < eNOERROR) ERR(handle, e);

    /* get extent No. */
    extentOffset = trainOffset / volInfo->extSize;

    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo);

    /* get bitmap train ID */
    bitmapTrainId.volNo = devInfoForDataVol[devNo].bitmapTrainId.volNo;
    bitmapTrainId.pageNo = devInfoForDataVol[devNo].bitmapTrainId.pageNo +
			   (extentOffset/volInfo->dataVol.numExtMapsInTrain)*TRAINSIZE2;

    /* get and fix bitmap buffer page */
    e = BfM_getAndFixBuffer(handle, &bitmapTrainId, M_EXCLUSIVE, &bitmapTrain_BCBP, TRAIN_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate offset in a bitmap page */
    bitmapOffset = trainOffset % (volInfo->dataVol.numExtMapsInTrain * volInfo->extSize);


    /* set 'bitmapTrain_BCBP' & 'bitmapOffset' */
    extent->bitmapTrain_BCBP = bitmapTrain_BCBP;
    extent->bitmapOffset = bitmapOffset;


    return (eNOERROR);
}
