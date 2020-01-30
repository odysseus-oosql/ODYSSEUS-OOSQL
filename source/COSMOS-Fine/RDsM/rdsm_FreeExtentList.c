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
 * Module: rdsm_FreeExtentList.c
 *
 * Description:
 *  Manipulation free extent list.
 *
 * Exports:
 *  Four rdsm_GetExtentFromFreeExtentList(RDsM_VolumeInfo_T*, Four*, Four)
 *  Four rdsm_RemoveExtentFromFreeExtentList(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, LogParameter_T*)
 *  Four rdsm_InsertExtentToFreeExtentList(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, LogParameter_T*)
 *
 */



#include <stdlib.h>
#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif /* WIN32 */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/* Internal Function */
Four rdsm_UpdateFreeExtentListHeaderOfVolumeInfoPage(Four, RDsM_FreeExtentListHeader_T, VolInfoPage_T*, Four, Four);


/*
 * Function: Four rdsm_GetExtentFromFreeExtentList(RDsM_VolumeInfo_T*, Four*, Four)
 *
 * Description:
 *  Get a first free extent No. in free extent list.
 *
 * Returns:
 *  Error code
 */
Four rdsm_GetExtentFromFreeExtentList(
    Four                handle,                 /* IN handle */
    RDsM_VolumeInfo_T   *volInfo,               /* IN volume information in volume table entry */
    Four                *extentNo,              /* OUT extent No. to be allocated */
    Four                sizeOfTrain             /* IN size of train in a segment to allocate */
)
{

    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_GetExtentFromFreeExtentList(volInfo=%P, extentNo=%P, sizeOfTrain=%lD)",
	     volInfo, extentNo, sizeOfTrain));


    /*
     * check parameters
     */
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eBADPARAMETER);


    /*
     * get an extent from page or train free extent list
     */
    if (sizeOfTrain == PAGESIZE2 && volInfo->dataVol.numOfFreePageExtent > 0) {

	*extentNo = volInfo->dataVol.freePageExtent;

    }
    else if (sizeOfTrain == TRAINSIZE2 && volInfo->dataVol.numOfFreeTrainExtent > 0) {

	*extentNo = volInfo->dataVol.freeTrainExtent;

    }
    else if (volInfo->dataVol.numOfFreeExtent > 0) {

	*extentNo = volInfo->dataVol.freeExtent;

    }
    else {

	return (eNODISKSPACE_RDSM);
    }


    /*
     * check output parameters
     */
    if (*extentNo < 0 || *extentNo >= volInfo->numExts) ERR(handle, eINTERNAL);


    return (eNOERROR);
}


/*
 * Function: Four rdsm_RemoveExtentFromFreeExtentList(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, LogParameter_T*)
 *
 * Description:
 *  Remove the first extent from free extent list.
 *
 * Returns:
 *  Error code
 */
Four rdsm_RemoveExtentFromFreeExtentList(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T    	*xactEntry,             /* IN transaction table entry */
    RDsM_VolumeInfo_T   	*volInfo,               /* IN volume information in volume table entry */
    AllocAndFreeExtentInfo_T    *extent,                /* IN extent to remove */
    Four                	sizeOfTrain,            /* IN size of train in the extent */
    LogParameter_T      	*logParam               /* IN log parameter */
)
{
    Four			e;			/* returned error value */
    Four			*ptrOfExtentNo;		/* pointer of free extent */
    Four 			*ptrOfExtentNum;	/* pointer of a number of free extent */
    Four                	nextExt;		/* next extent */
    Lsn_T                       lsn;                    /* LSN of the newly written log record */
    Four                        logRecLen;              /* log record length */
    LOG_LogRecInfo_T            logRecInfo;             /* log record information */
    Buffer_ACC_CB		*volInfoPage_BCBP;	/* BCBP of volume info page */
    VolInfoPage_T		*volInfoPage;		/* volume info page */
    Four			sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T; /* log image size */
    RDsM_FreeExtentListHeader_T freeExtentListHeaderType;   /* free extent header type */
    LOG_Image_RDsM_Modify_FreeExtentListHeader_T     updateFreeExtentListHeader[2]; /* log image of RDsM_Modify_ExtentMapHeader_T */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_RemoveExtentFromFreeExtentList(xactEntry=%P, volInfo=%P, extent=%P, sizeOfTrain=%lD, logParam=%P)",
	     xactEntry, volInfo, extent, sizeOfTrain, logParam));


    /*
     * check parameters
     */
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eBADPARAMETER);
    if (extent->extentNo < 0 || extent->extentNo >= volInfo->numExts) ERR(handle, eBADPARAMETER);


    /*
     * get first extent No. & number of free extent of the free extent list that the extent belong to
     */
    if (sizeOfTrain == PAGESIZE2 && extent->extentNo == volInfo->dataVol.freePageExtent) {

        /* this extent is in a free page extent list */
        ptrOfExtentNo = &volInfo->dataVol.freePageExtent;
        ptrOfExtentNum = &volInfo->dataVol.numOfFreePageExtent;

        freeExtentListHeaderType = RDSM_FREE_PAGE_EXTENT_HEADER;
    }
    else if (sizeOfTrain == TRAINSIZE2 && extent->extentNo == volInfo->dataVol.freeTrainExtent) {

        /* this extent is in a free train extent list */
        ptrOfExtentNo = &volInfo->dataVol.freeTrainExtent;
        ptrOfExtentNum = &volInfo->dataVol.numOfFreeTrainExtent;

        freeExtentListHeaderType = RDSM_FREE_TRAIN_EXTENT_HEADER;
    }
    else if (extent->extentNo == volInfo->dataVol.freeExtent) {

        /* this extent is in a free extent list */
        ptrOfExtentNo = &volInfo->dataVol.freeExtent;
        ptrOfExtentNum = &volInfo->dataVol.numOfFreeExtent;

        freeExtentListHeaderType = RDSM_FREE_EXTENT_HEADER;
    }
    else {
    	return (eBADPARAMETER);
    }

    /*
     * remove the extent from free extent list
     */

    /* get the next extent No. of the first extent */
    e = rdsm_GetExtentMapInfo(handle, extent, NULL, &nextExt);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * update free extent header of volume info page
     */
    e = BfM_getAndFixBuffer(handle, &volInfo->volInfoPageId, M_EXCLUSIVE, &volInfoPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* set 'volInfoPage' */
    volInfoPage = (VolInfoPage_T*)volInfoPage_BCBP->bufPagePtr;

    /*
     *  Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) {

        /*
         *  set pointers pointing to respective images
         *
	 *  logRecInfo->imageData[0] : freeExtentListHeaderType      : free extent list header type
         *  logRecInfo->imageData[1] : updateFreeExtentListHeader[0] : new FreeExtentListHeader
         *  logRecInfo->imageData[2] : updateFreeExtentListHeader[1] : old FreeExtentListHeader
         */

        updateFreeExtentListHeader[0].freeExtentNo             		= nextExt;
        updateFreeExtentListHeader[0].differenceOfNumOfFreeExtent   	= -1;
        updateFreeExtentListHeader[1].freeExtentNo             		= *ptrOfExtentNo;
        updateFreeExtentListHeader[1].differenceOfNumOfFreeExtent   	= 1;
        sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T       	= sizeof(LOG_Image_RDsM_Modify_FreeExtentListHeader_T);

        /*
         * write log record
         */
        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_RDSM_MODIFY_FREE_EXTENT_LIST_HEADER, LOG_REDO_UNDO,
                              volInfoPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
			      sizeof(RDsM_FreeExtentListHeader_T), &freeExtentListHeaderType,
                              sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T, &updateFreeExtentListHeader[0],
                              sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T, &updateFreeExtentListHeader[1]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRBL1(handle, e, volInfoPage_BCBP, PAGE_BUF);

        volInfoPage->hdr.lsn = lsn;
        volInfoPage->hdr.logRecLen = logRecLen;
    }

    /* update volInfoPage */
    e = rdsm_UpdateFreeExtentListHeaderOfVolumeInfoPage(handle, freeExtentListHeaderType, volInfoPage, nextExt, -1);
    if (e < eNOERROR) ERRBL1(handle, e, volInfoPage_BCBP, PAGE_BUF);

    /* set dirty bit */
    volInfoPage_BCBP->dirtyFlag = 1;

    /* free volume info page */
    BFM_FREEBUFFER(handle, volInfoPage_BCBP, PAGE_BUF, e);

    /* set the free extent No. & the number of free extent */
    *ptrOfExtentNo = nextExt;
    *ptrOfExtentNum = *ptrOfExtentNum - 1;


    return (eNOERROR);
}

/*
 * Function: Four rdsm_InsertExtentToFreeExtentList(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, LogParameter_T*)
 *
 * Description:
 *  Insert an extent to the free extent list
 *
 * Returns:
 *  Error code
 */
Four rdsm_InsertExtentToFreeExtentList(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T    	*xactEntry,             /* IN transaction table entry */
    RDsM_VolumeInfo_T   	*volInfo,               /* IN volume information in volume table entry */
    AllocAndFreeExtentInfo_T    *extent,                /* IN extent to remove */
    Four                	sizeOfTrain,            /* IN size of train in the extent */
    Four                        number,                 /* IN number of extent */
    LogParameter_T      	*logParam               /* IN log parameter */
)
{
    Four			e;			/* returned error value */
    Four                        i;                      /* loop index */
    Four			*ptrOfExtentNo;		/* pointer of free extent */
    Four 			*ptrOfExtentNum;	/* pointer of a number of free extent */
    Four                	nextExt;		/* next extent */
    BitmapTrain_T       	*bitmapTrain;		/* bitmap train */
    Lsn_T                       lsn;                    /* LSN of the newly written log record */
    Four                        logRecLen;              /* log record length */
    LOG_LogRecInfo_T            logRecInfo;             /* log record information */
    Buffer_ACC_CB		*volInfoPage_BCBP;	/* BCBP of volume info page */
    VolInfoPage_T		*volInfoPage;		/* volume info page */
    Four			sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T; /* log image size */
    RDsM_FreeExtentListHeader_T freeExtentListHeaderType;   /* free extent header type */
    LOG_Image_RDsM_Modify_FreeExtentListHeader_T     updateFreeExtentListHeader[2]; /* log image of RDsM_Modify_ExtentMapHeader_T */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_InsertExtentToFreeExtentList(xactEntry=%P, volInfo=%P, extent=%P, sizeOfTrain=%lD, logParam=%P)", xactEntry, volInfo, extent, sizeOfTrain, logParam));

    /*
     * check parameters
     */
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eBADPARAMETER);
    if (number < 1) ERR(handle, eBADPARAMETER);
    for (i=0; i<number; i++)
	if (extent[i].extentNo < 0 || extent[i].extentNo >= volInfo->numExts) ERR(handle, eBADPARAMETER);


    /*
     * get first extent No. & number of free extent of the free extent list that the extent belong to
     */
    if (sizeOfTrain == PAGESIZE2) {

        /* this extent is in a free page extent list */
        ptrOfExtentNo = &volInfo->dataVol.freePageExtent;
        ptrOfExtentNum = &volInfo->dataVol.numOfFreePageExtent;

        freeExtentListHeaderType = RDSM_FREE_PAGE_EXTENT_HEADER;
    }
    else if (sizeOfTrain == TRAINSIZE2) {

        /* this extent is in a free train extent list */
        ptrOfExtentNo = &volInfo->dataVol.freeTrainExtent;
        ptrOfExtentNum = &volInfo->dataVol.numOfFreeTrainExtent;

        freeExtentListHeaderType = RDSM_FREE_TRAIN_EXTENT_HEADER;
    }

    /*
     * insert the extent to free extent list
     */

    /* set the extent map information */
    e = rdsm_SetExtentMapInfo(handle, xactEntry, &extent[number-1], NO_OP, *ptrOfExtentNo, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * update free extent header of volume info page
     */
    e = BfM_getAndFixBuffer(handle, &volInfo->volInfoPageId, M_EXCLUSIVE, &volInfoPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* set 'volInfoPage' */
    volInfoPage = (VolInfoPage_T*)volInfoPage_BCBP->bufPagePtr;

    /*
     *  Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING) {

        /*
         *  set pointers pointing to respective images
         *
	 *  logRecInfo->imageData[0] : freeExtentListHeaderType      : free extent list header type
         *  logRecInfo->imageData[0] : updateFreeExtentListHeader[0] : new FreeExtentListHeader
         *  logRecInfo->imageData[1] : updateFreeExtentListHeader[1] : old FreeExtentListHeader
         */

        updateFreeExtentListHeader[0].freeExtentNo             		= (&extent[0])->extentNo;
        updateFreeExtentListHeader[0].differenceOfNumOfFreeExtent       = number;
        updateFreeExtentListHeader[1].freeExtentNo             		= *ptrOfExtentNo;
        updateFreeExtentListHeader[1].differenceOfNumOfFreeExtent       = -1 * number;
        sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T       	= sizeof(LOG_Image_RDsM_Modify_FreeExtentListHeader_T);

        /*
         * write log record
         */
        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_RDSM_MODIFY_FREE_EXTENT_LIST_HEADER, LOG_REDO_UNDO,
                              volInfoPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
			      sizeof(RDsM_FreeExtentListHeader_T), &freeExtentListHeaderType,
                              sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T, &updateFreeExtentListHeader[0],
                              sizeOfLOG_Image_RDsM_Modify_FreeExtentListHeader_T, &updateFreeExtentListHeader[1]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        volInfoPage->hdr.lsn = lsn;
        volInfoPage->hdr.logRecLen = logRecLen;

    }

    /* update volInfoPage */
    e = rdsm_UpdateFreeExtentListHeaderOfVolumeInfoPage(handle, freeExtentListHeaderType, volInfoPage, (&extent[0])->extentNo, number);
    if (e < eNOERROR) ERRBL1(handle, e, volInfoPage_BCBP, PAGE_BUF);

    /* set dirty big */
    volInfoPage_BCBP->dirtyFlag = 1;

    /* free the volume info page */
    BFM_FREEBUFFER(handle, volInfoPage_BCBP, PAGE_BUF, e);

    /* set the free extent No. & the number of free extent */
    *ptrOfExtentNo = (&extent[0])->extentNo;
    *ptrOfExtentNum = *ptrOfExtentNum + number;


    return (eNOERROR);
}


/*
 * Function: Four rdsm_UpdateFreeExtentListHeaderOfVolumeInfoPage(RDSM_FREE_EXTENT_HEADER_T, VolInfoPage_T*, LOG_Image_RDsM_Modify_FreeExtentListHeader_T*)
 *
 * Description:
 *  Update volume info page
 *
 * Returns:
 *  Error code
 */
Four rdsm_UpdateFreeExtentListHeaderOfVolumeInfoPage(
    Four                        	handle,                 		/* IN    handle */
    RDsM_FreeExtentListHeader_T		freeExtentListHeaderType,             	/* IN  free extent header type */
    VolInfoPage_T			*volInfoPage,				/* IN  volume info page */
    Four				extentNo,		                /* IN  first extent of free extent list */
    Four				differenceOfNumOfFreeExtent		/* IN  difference of # of free extent */
)
{

    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_UpdateFreeExtentListHeaderOfVolumeInfoPage(freeExtentListHeaderType=%lD, volInfoPage=%P, extentNo=%lD, differenceOfNumOfFreeExtent=%lD)", freeExtentListHeaderType, volInfoPage, extentNo, differenceOfNumOfFreeExtent));


    /*
     * update volume info page according to log action
     */
    if (freeExtentListHeaderType == RDSM_FREE_EXTENT_HEADER) {

	volInfoPage->dataVol.freeExtent       = extentNo;
	volInfoPage->dataVol.numOfFreeExtent += differenceOfNumOfFreeExtent;
    }
    else if (freeExtentListHeaderType == RDSM_FREE_PAGE_EXTENT_HEADER) {

        volInfoPage->dataVol.freePageExtent       = extentNo;
        volInfoPage->dataVol.numOfFreePageExtent += differenceOfNumOfFreeExtent;
    }
    else if (freeExtentListHeaderType == RDSM_FREE_TRAIN_EXTENT_HEADER) {

        volInfoPage->dataVol.freeTrainExtent       = extentNo;
        volInfoPage->dataVol.numOfFreeTrainExtent += differenceOfNumOfFreeExtent;
    }
    else {

	return (eBADPARAMETER);
    }


    return (eNOERROR);
}
