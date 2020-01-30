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
 * Module: RDsM_GetStatistics.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_GetStatistics_numExtents(Four, Four*, Four*, Four*, Boolean)
 */

#include "common.h"
#include "trace.h"
#include "error.h"
#include "SHM.h"
#include "RDsM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * RDsM_GetStatistics()
 *================================*/
/*
 * Function: Four RDsM_GetStatistics(Four, Four*, Four*, Four*, Boolean)
 *
 * Description:
 *  Given the ID of a volume, get # of extents
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  None
 */
Four RDsM_GetStatistics(
    Four               handle,                /* IN    handle */
    Four               volId,                 /* IN  volume id */
    Four*              extentSize,            /* OUT extent size */
    Four*              numPages,              /* OUT # of total pages */
    Four*              numUsedPages,          /* OUT # of used pages */
    Boolean            printBitmap)           /* IN  flag which indicates print bitmaps */
{
    Four               e;                     /* error code */
    Four               i, j, k;               /* loop variable */
    Four               ithExt;                /* variable which counts extent number */
    Four               bound;                 /* variable which indicates bound of for loop */
    Four	       count;                 /* number of free pages in an extent */
    Four	       entryNo;               /* entry number */
    RDsM_VolumeInfo_T* volInfo;               /* volume information in volume table */
    PageID             bmTrainId;             /* bitmap train identifier */
    BitmapTrain_T*     bmTrain;               /* pointer to a train buffer */
    Buffer_ACC_CB*     bmTrain_BCBP;          /* buffer page access control block */
    RDsM_DevInfo *devInfo; 
    RDsM_DevInfoForDataVol *devInfoForDataVol; 
    RDsM_SegmentInfo *pageSegInfo, *trainSegInfo; 


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_GetStatistics_ExtentInfo()"));


    /*
     *  get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volId, &entryNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  set a pointer to the corresponding entry
     */
    volInfo = &(RDSM_VOLTABLE[entryNo].volInfo);


    /*
     *  get statistics value about extent
     */
    *extentSize = volInfo->extSize;
    *numPages = volInfo->numExts*volInfo->extSize;


    /*
     *  calculate number of used extents
     */

    /* initialize number of used extents */
    *numUsedPages = 0;
    ithExt = 0;

    devInfo = PHYSICAL_PTR(volInfo->devInfo); 
    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo); 

    /* for each device in volume */
    for (i = 0; i < volInfo->numDevices; i++ ) {

        /* print out device number */
        if (printBitmap == TRUE) {
            printf("\n###### %2ldth Device #####\n", i);
        }

        /* initialize bmTrainId */
        bmTrainId.volNo = devInfoForDataVol[i].bitmapTrainId.volNo;
        bmTrainId.pageNo = devInfoForDataVol[i].bitmapTrainId.pageNo;

        /* for each bitmap train */
        for (j = 0; j < devInfoForDataVol[i].bitmapSize/TRAINSIZE2; j++ ) { 

            /* get a train buffer for a bitmap train */
            e = BfM_getAndFixBuffer(handle, &bmTrainId, M_EXCLUSIVE, &bmTrain_BCBP, TRAIN_BUF);
            if (e < eNOERROR) ERR(handle, e);

            bmTrain = (BitmapTrain_T*)bmTrain_BCBP->bufPagePtr;

            /* calculate bound */
            bound = (j < devInfoForDataVol[i].bitmapSize/TRAINSIZE2 - 1) ?
                    volInfo->dataVol.numExtMapsInTrain : devInfo[i].numExtsInDevice % volInfo->dataVol.numExtMapsInTrain; 

            /* for each extent map in bitmap train */
            for (k = 0; k < bound; k++ ) {

                /* count the number of bits set in the extent map */
                Util_CountBitsSet(handle, bmTrain->bytes, k*volInfo->extSize, volInfo->extSize, &count);

                /* update numUsedExts */
                *numUsedPages += (volInfo->extSize - count);

                /* print out bitmap */
                if (printBitmap == TRUE) {
                    printf("%5ldth extent : ", ithExt++);
                    e = Util_PrintBits(handle, bmTrain->bytes, k*volInfo->extSize, volInfo->extSize);
                    if (e < eNOERROR) ERR(handle, e);
                }
            }

            /* free this bitmap train */
            BFM_FREEBUFFER(handle, bmTrain_BCBP, TRAIN_BUF, e);
            if (e < eNOERROR) ERR(handle, e);

            /* update bmTrainId */
            bmTrainId.pageNo += TRAINSIZE2;
        }
    }


    return(eNOERROR);

} /* RDsM_GetStatistics() */


/*@================================
 * RDsM_GetStatistics_numExtents()
 *================================*/
/*
 * Function: Four RDsM_GetStatistics_numExtents(Four, Four*, Four*, Four*)
 *
 * Description:
 *  Given the ID of a volume, get # of extents
 *
 * Returns:
 *  error code
 *    eVOLNOTMOUNTED_RDSM - volume not mounted
 *
 * Side Effects:
 *  None
 */
Four RDsM_GetStatistics_numExtents(
    Four         handle,                   /* IN    handle */
    Four         volId,                    /* IN  volume id */
    Four*        extentSize,               /* OUT extent size */
    Four*        nTotalExtents,            /* OUT # of total extents */
    Four*        nUsedExtents)             /* OUT # of used extents */
{
    Four               			e;                     /* error code */
    Four               			i, j, k;               /* loop variable */
    Four               			ithExt;                /* variable which counts extent number */
    Four               			bound;                 /* variable which indicates bound of for loop */
    Four	  			count;                 /* number of free pages in an extent */
    Four	       			entryNo;               /* entry number */
    RDsM_VolumeInfo_T* 			volInfo;               /* volume information in volume table */
    PageID             			bmTrainId;             /* bitmap train identifier */
    BitmapTrain_T*     			bmTrain;               /* pointer to a train buffer */
    Buffer_ACC_CB*     			bmTrain_BCBP;          /* buffer page access control block */
    RDsM_DevInfo*			devInfo; 
    RDsM_DevInfoForDataVol*		devInfoForDataVol; 
    RDsM_SegmentInfo*			pageSegInfo, *trainSegInfo; 


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_GetStatistics_ExtentInfo()"));


    /*
     *  get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volId, &entryNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  set a pointer to the corresponding entry
     */
    volInfo = &(RDSM_VOLTABLE[entryNo].volInfo);


    /*
     *  get statistics value about extent
     */
    *extentSize = volInfo->extSize;


    /*
     *  calculate number of used extents
     */

    /* initialize number of used extents */
    *nUsedExtents 	= 0;
    *nTotalExtents 	= 0;
    ithExt = 0;

    devInfo = PHYSICAL_PTR(volInfo->devInfo); 
    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo); 

    /* for each device in volume */
    for (i = 0; i < volInfo->numDevices; i++ ) {

        /* initialize bmTrainId */
        bmTrainId.volNo = devInfoForDataVol[i].bitmapTrainId.volNo;
        bmTrainId.pageNo = devInfoForDataVol[i].bitmapTrainId.pageNo;

        /* for each bitmap train */
        for (j = 0; j < devInfoForDataVol[i].bitmapSize/TRAINSIZE2; j++ ) { 

            /* get a train buffer for a bitmap train */
            e = BfM_getAndFixBuffer(handle, &bmTrainId, M_EXCLUSIVE, &bmTrain_BCBP, TRAIN_BUF);
            if (e < eNOERROR) ERR(handle, e);

            bmTrain = (BitmapTrain_T*)bmTrain_BCBP->bufPagePtr;

            /* calculate bound */
            bound = (j < devInfoForDataVol[i].bitmapSize/TRAINSIZE2 - 1) ?
                    volInfo->dataVol.numExtMapsInTrain :
					devInfo[i].numExtsInDevice % volInfo->dataVol.numExtMapsInTrain; 

            /* for each extent map in bitmap train */
            for (k = 0; k < bound; k++ ) {

                /* count the number of bits set in the extent map */
                Util_CountBitsSet(handle, bmTrain->bytes, k*volInfo->extSize, volInfo->extSize, &count);

                /* update numUsedExts */
		(*nTotalExtents)++;
		if (count != *extentSize)
		    (*nUsedExtents)++;

            }

            /* free this bitmap train */
            BFM_FREEBUFFER(handle, bmTrain_BCBP, TRAIN_BUF, e);
            if (e < eNOERROR) ERR(handle, e);

            /* update bmTrainId */
            bmTrainId.pageNo += TRAINSIZE2;
        }
    }

    return(eNOERROR);

} /* RDsM_GetStatistics_numExtents() */



/*@================================
 * RDsM_GetStatistics_numPages()
 *================================*/
/*
 * Function: Four RDsM_GetStatistics_numPages(Four, Four*, Four*, Boolean)
 *
 * Description:
 *  Given the ID of a train, read it from a disk into a main memory buffer.
 *
 * Returns:
 *  error code
 *    eVOLNOTMOUNTED_RDSM - volume not mounted
 *    eMEMORYALLOCERR - no more free memory
 *
 * Side Effects:
 *  None
 */
Four RDsM_GetStatistics_numPages(
    Four        handle,                /* IN    handle */
    Four      	volId,                 /* IN  volume id */
    sm_NumPages	*numPages,             /* OUT # of pages */
    Boolean   	getKindFlag,           /* IN  get kind of page if TRUE */ 
    Boolean   	bitmapPrintFlag)       /* IN  print bitmap if TRUE */
{
    Four               		e;                     /* error code */
    Four               		i, j, k;               /* loop variable */
    Four               		ithExt;                /* variable which counts extent number */
    Four               		bound;                 /* variable which indicates bound of for loop */
    Four	     		count;                 /* number of free pages in an extent */
    Four	       		entryNo;               /* entry number */
    RDsM_VolumeInfo_T		*volInfo;               /* volume information in volume table */
    PageID             		bmTrainId;             /* bitmap train identifier */
    BitmapTrain_T		*bmTrain;               /* pointer to a train buffer */
    Buffer_ACC_CB		*bmTrain_BCBP;          /* buffer page access control block */
    RDsM_DevInfo 		*devInfo; 
    RDsM_DevInfoForDataVol	*devInfoForDataVol; 
    RDsM_SegmentInfo 		*pageSegInfo, *trainSegInfo; 


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_GetStatistics_ExtentInfo()"));

    if (getKindFlag) {
	fprintf(stderr,"getKindFlag option is not implemented yet.\n");
	return (eNOERROR);
    }


    /*
     *  get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volId, &entryNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  set a pointer to the corresponding entry
     */
    volInfo = &(RDSM_VOLTABLE[entryNo].volInfo);


    /*
     *  calculate number of used extents
     */

    /* initialize number of used extents */
    numPages->numTotalPages = 0;
    ithExt = 0;

    devInfo = PHYSICAL_PTR(volInfo->devInfo); 
    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo); 

    /* for each device in volume */
    for (i = 0; i < volInfo->numDevices; i++ ) {

        /* print out device number */
        if (bitmapPrintFlag == TRUE) {
            printf("\n###### %2ldth Device #####\n", i);
        }

        /* initialize bmTrainId */
        bmTrainId.volNo = devInfoForDataVol[i].bitmapTrainId.volNo;
        bmTrainId.pageNo = devInfoForDataVol[i].bitmapTrainId.pageNo;

        /* for each bitmap train */
        for (j = 0; j < devInfoForDataVol[i].bitmapSize/TRAINSIZE2; j++ ) { 

            /* get a train buffer for a bitmap train */
            e = BfM_getAndFixBuffer(handle, &bmTrainId, M_EXCLUSIVE, &bmTrain_BCBP, TRAIN_BUF);
            if (e < eNOERROR) ERR(handle, e);

            bmTrain = (BitmapTrain_T*)bmTrain_BCBP->bufPagePtr;

            /* calculate bound */
            bound = (j < devInfoForDataVol[i].bitmapSize/TRAINSIZE2 - 1) ?
                    volInfo->dataVol.numExtMapsInTrain : devInfo[i].numExtsInDevice % volInfo->dataVol.numExtMapsInTrain; 

            /* for each extent map in bitmap train */
            for (k = 0; k < bound; k++ ) {

                /* count the number of bits set in the extent map */
                Util_CountBitsSet(handle, bmTrain->bytes, k*volInfo->extSize, volInfo->extSize, &count);

                /* update numUsedExts */
		numPages->numTotalPages += (volInfo->extSize - count);

                /* print out bitmap */
                if (bitmapPrintFlag == TRUE) {
                    printf("%5ldth extent : ", ithExt++);
                    e = Util_PrintBits(handle, bmTrain->bytes, k*volInfo->extSize, volInfo->extSize);
                    if (e < eNOERROR) ERR(handle, e);
                }
            }

            /* free this bitmap train */
            BFM_FREEBUFFER(handle, bmTrain_BCBP, TRAIN_BUF, e);
            if (e < eNOERROR) ERR(handle, e);

            /* update bmTrainId */
            bmTrainId.pageNo += TRAINSIZE2;
        }
    }

    return(eNOERROR);

} /* RDsM_GetStatistics_numPages() */


