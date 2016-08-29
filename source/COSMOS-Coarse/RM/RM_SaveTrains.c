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
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "RM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@================================
 * RM_SaveTrains()
 *================================*/
/*
 * Function: Four RM_SaveTrain(PageType*, PageID*, Four, Two)
 *
 * Description:
 *  Save multiple data pages/trains into the log volume.
 *
 * Returns:
 *  error code
 */
Four RM_SaveTrains(
    Four handle,
    char   	  *bufPtr,           /* IN a pointer for a buffer of trains */
    PageID        *trainIds,	     /* IN array of identifier of the trains */
    Four          numTrains,         /* IN number of trains to write */
    Two           sizeOfTrain        /* IN the size of a train in pages */
)
{
    Four	e;
    Four	logPageNo;
    char*	currentTrainPtr;
    PageID	pid;
    Four	i;
    Four	width;

    /* bufPtr에 있는 trian들을 LOG에 쓴다. */
    /* bufPtr에 들어 있는 내용들 중에는, 이미 LOG에 써진 내용이 있을 수가 있다. 이들 내용을 무시한채로,
       두번 쓰게 되면, LOG에 같은 내용이 두개가 존재하게 되므로, 이를 피하기 위해서는 반드시 확인을 해야 한다. */
    /* Write trains pointed by bufPtr to LOG. */
    /* Among trains pointed by bufPtr, there may be trains already written to LOG.
       If we write them to LOG without checking the duplication, there are duplicated ones in LOG.
       So, must check the duplication to prevent the duplication. */

    /* bufPtr에 있는 내용중에 LOG에 이미 있는 내용이라면, 개별적으로 미리 LOG에 쓴다. */
    /* If trains in bufPtr are already in LOG, write them to LOG one by one. */
    currentTrainPtr = bufPtr;
    width  = sizeOfTrain * PAGESIZE;
    for(i = 0; i < numTrains;)
    {
	if(rm_LookUpInLogTable(handle, &trainIds[i], &logPageNo))
	{
	    /* 주어진 내용을 LOG에 쓴다. */
            /* Write the given trains to LOG. */
	    e = RM_SaveTrain(handle, &trainIds[i], currentTrainPtr, sizeOfTrain);
	    if(e < 0) ERR(handle, e);
	    
	    /* 쓴 내용을 trainIds와 bufPtr로 부터 제거한다. 제거 하는 방법은 현재 원소로 부터 바로 뒤에 있는
	       내용을 앞으로 끌어 당김으로써 수행한다. */
	    /* Eliminate train ID and train itself from trainIDs and bufPtr
	       by doing shift one element that is next to elements of trainIDs and bufPtr ahead. */
    	    memcpy(&trainIds[i], &trainIds[i + 1], sizeof(PageID) * (numTrains - i - 1));
	    memcpy(currentTrainPtr, currentTrainPtr + width, width * (numTrains - i - 1));
	    numTrains --;
	}
	else
	{
	    currentTrainPtr += width;
	    i ++;
	}
    }
    
    if(numTrains > 0)
    {
	/* bufPtr에 있는 연속된 내용을 LOG에 새로 붙인다. */
        /* Append contiguous trains pointed by bufPtr to LOG. */
	if (RM_NUM_OF_FREE_LOG_PAGES(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo) < sizeOfTrain * numTrains)
	    ERR(handle, eLOGVOLUMEFULL_RM);

	if (sizeOfTrain == PAGESIZE2)
	    logPageNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage;
	else /* sizeOfTrain is equal to TRAINSIZE2 */
	    logPageNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForTrain - (numTrains - 1) * sizeOfTrain;
    
	pid.volNo  = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo;
	pid.pageNo = logPageNo;
	e = RDsM_WriteTrainsForLogVolume(handle, (PageType*)bufPtr, &pid, numTrains, sizeOfTrain);
	if(e < 0) ERR(handle, e);

	/* LOG관리는 위한 HASHTABLE에 새로 추가된 페이지에 대한 정보를 추가한다. */
        /* Insert an information of trains that is appended newly to HASHTABLE for LOG managing. */ 
	for(i = 0; i < numTrains; i++)
	{
	    e = rm_InsertIntoLogTable(handle, &trainIds[i], logPageNo);
	    if (e < eNOERROR) ERR(handle, e);

	    logPageNo += sizeOfTrain;
	}

	if (sizeOfTrain == PAGESIZE2)
	    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage += numTrains * sizeOfTrain;
	else /* sizeOfTrain is equal to TRAINSIZE2 */
	    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForTrain -= numTrains * sizeOfTrain;
    }

    return eNOERROR;
}

