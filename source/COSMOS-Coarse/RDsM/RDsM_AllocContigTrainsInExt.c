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
/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
 * Module: RDsM_AllocContigTrainsInExt.c
 *
 * Description:
 *  allocate contiguous trains in an extent and return their train identifiers in "TrainIds".
 *  If startTrainID is specified, allocate trains after it.
 *
 * Exports:
 *  Four RDsM_AllocContigTrainsInExt(Four, Four, PageID*, Two, Four*, Two, PageID*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "BfM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@================================
 * RDsM_AllocContigTrainsInExt()
 *================================*/
/*
 * Function: Four RDsM_AllocContigTrainsInExt(Four, Four, PageID*, Two, Four*, Two, PageID*)
 *
 * Description:
 *  allocate contiguous trains in an extent and return their train identifiers in "TrainIds".
 *  If startTrainID is specified, allocate trains after it.
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  additional extent is allocated if startTrainID is NULL
 */
Four RDsM_AllocContigTrainsInExt(
    Four handle,
    Four        volNo,              /* IN    volume number in question */
    Four        firstExt,           /* IN    first extent number of the segment */
    PageID      *startTrainID,      /* IN    start train ID of contiguous allocated trains in the extent */
    Two         eff,                /* IN    number of pages in an extent to keep filled */
    Four        *numOfTrains,       /* INOUT number of allocated trains */
    Two         sizeOfTrain,        /* IN    size of a train to be allocated */
    PageID      *trainIDs)          /* OUT   array for train ID which allocated */
{
    Four        e;                  /* returned error code */
    VolumeTable	*v;                 /* pointer to an entry in shmPtr->volTable */
    Two         i;                  /* loop index */
    Two         curEff;             /* extent fill factor of the current extent */
    Four        extNum;             /* extent number */
    Four        numAllocs = 0;      /* number of trains allocated */
    Four        maxNumAllocs;       /* maximum number of trains allocated */
    Four        devNo;              /* number of device in which given extent is located */
    Four        pageOffset;         /* offset of the extent (unit = # of page) */
    Four        extOffset;          /* offset of the extent (unit = # of extent) */
    Two         allocOffset;        /* offset from which train allocation starts (unit = # of page) */
    PageID      bmPageId;           /* page identifier */
    PageType    *bmPage;            /* pointer to a page */
    Four        pageNo;             /* page number to be allocated */
    Two         ith;                /* i-th bit from the position start */
    Four        start;              /* start bit position to be checked in a page */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_AllocContigTrainsInExt(handle, volNo=%lD, firstExt=%lD, startTrainID=%P, eff=%lD, numOfTrains=%lD, sizeOfTrain=%lD, trainIDs=%P)",
	      volNo, firstExt, startTrainID, eff, numOfTrains, sizeOfTrain, trainIDs));


    /*
     *  Get the corresponding volume table entry via searching the shmPtr->volTable
     */
    for (i = 0; i < MAXNUMOFVOLS; i++) {
	if (RDSM_PER_THREAD_DS(handle).volTable[i].volNo == volNo) break; 
    }
    if (i >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);

    v = &(RDSM_PER_THREAD_DS(handle).volTable[i]); 


    /*
     *	NOTE:
     *	conversion of the extent fill factor in the number of pages
     *	the unit of the extent fill factor is the number of pages in RDsM
     *	while the unit of the eff, input parameter is % in the upper layer
     */
    eff = (eff * v->sizeOfExt) / 100;

    /*@ check input parameters */
    if (firstExt < NIL ||firstExt >= v->numOfExts) ERR(handle, eINVALIDFIRSTEXT_RDSM);
    if (trainIDs == NULL) ERR(handle, eBADPARAMETER);
    if (eff < 0 || eff > v->sizeOfExt) ERR(handle, eINVALIDEFF_RDSM);
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eINVALIDTRAINSIZE_RDSM);


    /*
     *  Get extNum & allocOffset
     */

    /* if startPID is given, allocate contiguous trains after startPID in the extent */
    if (startTrainID != NULL) {

        /* validate NearPID */
        if (startTrainID->volNo != volNo || startTrainID->pageNo < 0 || startTrainID->pageNo >= v->numOfExts*v->sizeOfExt)
            ERR(handle, eINVALIDPID_RDSM);

        /* get extent number to which given startTrainID belongs */
        extNum = startTrainID->pageNo / v->sizeOfExt;

        /* get allocOffset */
        allocOffset = startTrainID->pageNo - extNum*v->sizeOfExt + sizeOfTrain;

        /* check allocOffset is out of bound */
        /* Note!! if startTrainID is end of extent, this situation occurs */
        if (allocOffset >= v->sizeOfExt) {
            *numOfTrains = 0;
            return(eNOERROR);
        }
    }
    /* if startPID is NULL, allocate new extent, then allocate contiguous trains in that extent */
    else {

        /* allocate new extent */
        e = RDsM_alloc_ext(handle, v, firstExt, &extNum);
        if (e < eNOERROR) ERR(handle, e); 

        /* get allocOffset */
        allocOffset = 0;
    }


    /*
     *  Find the PageMap page of the extent
     */

    /* Get physical information about given extNum */
    e = rdsm_GetPhysicalInfo(handle, v, extNum*v->sizeOfExt, &devNo, &pageOffset);
    if (e < eNOERROR) ERR(handle, e);

    extOffset = pageOffset / v->sizeOfExt;

    /* get bmPageId of page map page */
    bmPageId.volNo = DEVINFO_ARRAY(v->devInfo)[devNo].bitMapPageId.volNo;
    bmPageId.pageNo = DEVINFO_ARRAY(v->devInfo)[devNo].bitMapPageId.pageNo + extOffset / ((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/v->sizeOfExt);

    /*@ get a page for the bit map page */
    e = BfM_GetTrain(handle, &bmPageId, (char**)&bmPage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 


    /*
     *  Get 'maxNumAllocs'
     */

    /* check the extent fill factor of the extent */
    e = RDsM_check_eff(handle, v, extNum, &curEff);
    if (e < eNOERROR) ERR(handle, e); 

    /* calculate 'maxNumAllocs' */
    maxNumAllocs = (eff - curEff) / sizeOfTrain;


    /*
     *  Set the first bit position start and initialize 'pageNo'
     */
    start = (extOffset % ((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/v->sizeOfExt)) * v->sizeOfExt + allocOffset;
    pageNo = extNum * v->sizeOfExt + allocOffset;


    /*
     *  Allocate contiguous trains
     */
    do {

        ith = RDsM_find_bits(handle, bmPage, start, sizeOfTrain, sizeOfTrain);
        if (ith != 0) break;

        RDsM_clear_bits(handle, bmPage, start, sizeOfTrain);

        trainIDs->volNo = v->volNo;
        trainIDs->pageNo = pageNo;

        start += sizeOfTrain;
        pageNo += sizeOfTrain;
        numAllocs ++;
        trainIDs ++;

    } while (numAllocs < *numOfTrains && numAllocs < maxNumAllocs);

    /*@
     *  Set dirty bit if needed
     */
    if (numAllocs > 0) {
        e = BfM_SetDirty(handle, &bmPageId, PAGE_BUF);
        if (e < eNOERROR) ERRB1(handle, e, &bmPageId, PAGE_BUF);
    }

    /*
     *  Free page map
     */
    e = BfM_FreeTrain(handle, &bmPageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 


    /*
     *  Updates a new extent fill factor in the corresponding extent entry
     */
    e = RDsM_change_eff(handle, v, extNum, curEff + numAllocs*sizeOfTrain);
    if (e < eNOERROR) ERR(handle, e); 

    /*
     *  Set return parameter
     */
    *numOfTrains = numAllocs;


    return(eNOERROR);

} /* RDsM_AllocContigTrainsInExt() */
