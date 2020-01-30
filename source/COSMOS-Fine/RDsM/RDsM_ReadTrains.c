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
 * Module: RDsM_ReadTrains.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_ReadTrains(PageID*, PageType*, Four)
 */


#include <assert.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "SHM.h"
#include "RDsM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * RDsM_ReadTrains()
 *================================*/
/*
 * Function: Four RDsM_ReadTrains(Four, PageID*, char**, Four, Four)
 *
 * Description:
 *  Given the ID of the train, read it from a disk into a main memory buffer
 *
 * Returns:
 *  error code
 *    eNULLPIDPTRRDsM - null pageid pointer
 *    eNULLBUFPTRRDsM - null buffer pointer
 *    eVOLNOTMOUNTED_RDSMRDsM - volume not mounted
 *    eINVALIDPIDRDsM - invalid page identifier
 *    eLSEEKFAIL_RDSMRDsM - lseek operation is failed
 *    eWRITEFAIL_RDSMRDsM - write operation is failed
 *
 * Side Effects:
 *  a train is brought into memory
 */
Four RDsM_ReadTrains(
    Four          handle,            /* IN    handle */
    PageID        *startTrainId,     /* IN identifier of the train */
    char       	  *bufPtr,           /* IN a pointer for a buffer of trains */
    Four          numTrains,         /* IN number of trains to write */
    Four          sizeOfTrain)       /* IN the size of a train in pages */
{
    Four          e;                   /* error code */
    Four          i;                   /* loop index */
    Four          devNo;               /* device number in which given train is located */
    Four          startTrainOffset;    /* physical offset of given train in the device */
    Four          numReadPages;        /* size of write trains (in # of pages) */
    Four          entryNo;             /* entry number of volume table entry */
    RDsM_VolumeInfo_T *volInfo;        /* volume information in volume table entry */
    RDsM_DevInfo  *devInfo; 
    Four          numTotalReadPages; 
    Four          startPageNo; 
    Four 	  numPagesInDevice;



    TR_PRINT(handle, TR_RDSM, TR1,
         ("RDsM_ReadTrains(startTrainId=%P, bufPtr=%P, sizeOfTrain=%lD)", startTrainId, bufPtr, sizeOfTrain));


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
     *  get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, startTrainId->volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * points to the volume information
     */
    volInfo = &RDSM_VOLTABLE[entryNo].volInfo;


    /*
     *  Validate the TrainId
     */
    if (startTrainId->volNo < 0 || startTrainId->pageNo < 0 || startTrainId->pageNo >= volInfo->numExts*volInfo->extSize)
        ERR(handle, eBADPAGEID);


    /*
     *  Read trains to disk
     */
    numTotalReadPages = numTrains * sizeOfTrain;
    startPageNo = startTrainId->pageNo;
    while (numTotalReadPages > 0) {
        /* get physical information about given train */
        e = rdsm_GetPhysicalInfo(handle, volInfo, startPageNo, &devNo, &startTrainOffset);
        if (e < eNOERROR) ERR(handle, e);

        devInfo = PHYSICAL_PTR(volInfo->devInfo); 

	numPagesInDevice = devInfo[devNo].numExtsInDevice * volInfo->extSize; 

        /* calculate 'numReadPages' */
        numReadPages = (startTrainOffset + numTotalReadPages  <= numPagesInDevice) ?
                       numTotalReadPages : (numPagesInDevice - startTrainOffset);

        /* write the train into the buffer */
        e = rdsm_ReadTrain(handle, OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[devNo], startTrainOffset, bufPtr, numReadPages);
        if (e < eNOERROR) ERR(handle, e);

        bufPtr += numReadPages*PAGESIZE;
        startPageNo += numReadPages;
        numTotalReadPages -= numReadPages;
    }

    return(eNOERROR);

} /* RDsM_ReadTrains() */
