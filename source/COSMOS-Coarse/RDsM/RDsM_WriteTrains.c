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
 * Module: RDsM_WriteTrains.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_WriteTrains(PageType*, PageID*, Four, Two)
 */


#include <assert.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_WriteTrains()
 *================================*/
/*
 * Function: Four RDsM_WriteTrains(PageType*, PageID*, Four, Two)
 *
 * Description:
 *  Given the ID of the train, read it from a disk into a main memory buffer
 *
 * Returns:
 *  error code
 *    eNULLPIDPTRRDsM - null pageid pointer
 *    eNULLBUFPTRRDsM - null buffer pointer
 *    eVOLNOTMOUNTEDRDsM - volume not mounted
 *    eINVALIDPIDRDsM - invalid page identifier
 *    eLSEEKFAILRDsM - lseek operation is failed
 *    eWRITEFAILRDsM - write operation is failed
 *
 * Side Effects:
 *  a train is brought into memory
 */
Four RDsM_WriteTrains(
    Four 			handle,
    PageType   	  	*bufPtr,           /* IN a pointer for a buffer of trains */
    PageID        	*startTrainId,     /* IN identifier of the train */
    Four         	numTrains,         /* IN number of trains to write */
    Two           	sizeOfTrain)       /* IN the size of a train in pages */
{
    Four          	e;                   /* error code */
    Two           	i;                   /* loop index */
    Four          	devNo;               /* device number in which given train is located */
    Four          	startTrainOffset;    /* physical offset of given train in the device */
    Four          	numWritePages;       /* size of write trains (in # of pages) */
    Four          	remain;              /* size of remain trains (in # of pages) */
    Four         	numPagesInDevice;    /* size of device (in # of pages) */
    VolumeTable   	*v;                  /* pointer for an volume table entry */


    TR_PRINT(TR_RDSM, TR1,
         ("RDsM_WriteTrains(handle, bufPtr=%P, startTrainId=%P, sizeOfTrain=%lD)", bufPtr, startTrainId, sizeOfTrain));


    /*
     *  if numTrains is zero, just return
     */
    if (numTrains == 0) return(eNOERROR);


    /*
     *  Check input parameters
     */

    if (startTrainId == NULL) ERR(handle, eBADPARAMETER);
    if (bufPtr == NULL)  ERR(handle, eBADPARAMETER);
    if (numTrains < 0)  ERR(handle, eBADPARAMETER);
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eINVALIDTRAINSIZE_RDSM);


    /*
     *  Get volum table entry
     */

    /* get the corresponding volume table entry via searching the shmPtr->volTable */
    for (i = 0; i < MAXNUMOFVOLS; i++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[i].volNo == startTrainId->volNo) break; 
    }
    if (i >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);


    /*@ set v to point to the corresponding entry */
    v = &(RDSM_PER_THREAD_DS(handle).volTable[i]); 


    /*
     *  Validate the TrainId
     */
    if (startTrainId->volNo < 0 || startTrainId->pageNo < 0 || startTrainId->pageNo >= v->numOfExts*v->sizeOfExt)
        ERR(handle, eINVALIDPID_RDSM);


    /*
     *  Write trains to disk
     */

    /* get physical information about given train */
    e = rdsm_GetPhysicalInfo(handle, v, startTrainId->pageNo, &devNo, &startTrainOffset);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate 'numTrainsInDevice' */
    numPagesInDevice = NUM_PAGES_IN_DEVICE(v, devNo);

    /* calculate 'numWritePages' */
    numWritePages = (numTrains*sizeOfTrain <= numPagesInDevice - startTrainOffset) ?
                    numTrains*sizeOfTrain :
                    numPagesInDevice - startTrainOffset;

    /* write the train into the buffer */
    e = rdsm_WriteTrain(handle, DEVINFO_ARRAY(v->devInfo)[devNo].devAddr, startTrainOffset, bufPtr, numWritePages);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate 'remain' and update 'bufPtr' */
    remain = numTrains*sizeOfTrain - numWritePages;
    bufPtr += numWritePages;

    while (remain > 0) {

        /* increase 'devNo' */
        devNo += 1;

        /* calculate numTrainsInDevice */
        numPagesInDevice = NUM_PAGES_IN_DEVICE(v, devNo);

        /* calculate 'numWritePages' */
        numWritePages = (remain <= numPagesInDevice) ?  remain : numPagesInDevice;

        /* write remain train */
        e = rdsm_WriteTrain(handle, DEVINFO_ARRAY(v->devInfo)[devNo].devAddr, 0, bufPtr, numWritePages);
        if (e < eNOERROR) ERR(handle, e);

        /* update 'remain' & 'bufPtr' */
        remain -= numWritePages;
        bufPtr += numWritePages;
    }

    /* assertion check */
    assert (remain == 0);


    return(eNOERROR);

} /* RDsM_WriteTrains() */
