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
#ifdef USE_COHERENCY_VOLUME
/*
 * Module: BfM_CoherencyVolume.c
 *
 * Description:
 *  Buffer Coherency 문제를 해결하기 위한 공유 데이타 구조인 coherency volume관련 연산을 정의한다.
 * 
 *  Define oprations about coherency volume data structures to solve buffer coherency problem.
 *
 * Exports:
 *  BfM_FormatCoherencyVolume
 *  BfM_SyncBufferUsingCoherencyVolume
 *  BfM_UpdateCoherencyVolume
 *  BfM_MountCoherencyVolume
 *  BfM_DismountCoherencyVolume
 *
 *  bfm_GetCoherencyVolumeLock
 *  bfm_ReleaseCoherencyVolumeLock
 *  bfm_ReadCoherencyVolumeTimestamp
 *  bfm_WriteCoherencyVolumeTimestamp
 *  bfm_AppendCoherencyPageInfos
 *  bfm_FlushCoherencyPageInfos
 *  bfm_InitCoherencyPageInfos
 *  bfm_ReadCoherencyPageInfos
 *
 */

#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "RDsM_Internal.h"
#include "RM.h"
#include <string.h>
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

Four BfM_FormatCoherencyVolume(
    Four handle,
    char*		devName,        /* IN  name of the coherency volume device */
    char*		title,          /* IN  volume title */
    Four    	volNo           /* IN  volume id */
)
{
    Four                            	e;
    char*                           	devNames[1];
    Four                            	numPagesInDevice[1];
    bfm_CoherencyVolumeHeaderPage   	headerPage;
    bfm_CoherencyVolumePageInfosPage    pageInfosPage;
    Four                            	i;
    Two                            		extentSize;

    TR_PRINT(TR_BFM, TR1,("BfM_FormatCoherencyVolume(handle)"));

    /* RDsM_Format을 사용하여 device를 format한다. */ 
    /* Format device using RDsM_Format(). */
    devNames[0]         = devName;
    /* extent size는 8(=BFM_COHERENCY_VOLUME_EXTENTSIZE)로 사용한다. 이유는 최소한의 RDsM의 volume header정보가 
       6 page이상이기 때문이다. 이들 정보가 하나의 extent안에 들어가는 것이 좋기 때문에 8을 사용한다. */
    /* Set extent size as 8(=BFM_COHRENCY_VOLUME_EXTENTSIZE) because the least size of volume header of RDsM is 6 pages.
       Since that can be put into one extent, we set extent size as 8. */
    extentSize          = BFM_COHERENCY_VOLUME_EXTENTSIZE;
    /* format되는 device의 크기를 정한다. device크기는 앞의 header를 위한 하나의 extent와 
       BFM_COHERENCY_VOLUME_HEADER_N_PAGE + BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE 이다. 
       default setting은 각각 8, 1, 1 page로 총 10 page이다. */
    /* decide the size of device to be formated. The size of the device is sum of 
       one extent size for the header, BFM_COHERENCY_VOLUME_HEADER_N_PAGE, and BFM_COHRENCY_VOLUME_PAGEINFOS_N_PAGE.
       default parameters about that size are 8, 1, and 1 page, so we use 10 pages totally. */
    numPagesInDevice[0] = extentSize + BFM_COHERENCY_VOLUME_HEADER_N_PAGE + BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE;

    /* numPagesInDevice[0]를 extentSize의 배수로 만든다. */
    /* Set numPagesInDevice[0] to twice extentSize. */
    if(numPagesInDevice[0] % extentSize)    /* extentSize의 배수가 아닌 경우 */
                                            /* In case numPagesInDevice[0] is not a multiple of extentSize */
        numPagesInDevice[0] = ((numPagesInDevice[0] / extentSize) + 1) * extentSize;

    e = RDsM_Format(handle, 1, devNames, title, volNo, extentSize, numPagesInDevice);
    if(e < eNOERROR) ERR(handle, e);

    /* format된 device를 mount한다, mount된 정보는 bfm_CoherencyVolumeInfo에 기록된다. */
    /* Mount the formatted device. Mount information is saved in bfm_CoherencyVolumeInfo. */
    e = BfM_MountCoherencyVolume(handle, devName);
    if(e < eNOERROR) ERR(handle, e);

    /* header page와 pageInfo page를 각각 작성하여 이를 쓴다 */
    /* Initialize header page and pageInfo page, and write them to coherency volume. */
    headerPage.header.circularListHead = 0;
    headerPage.header.circularListSize = BFM_N_COHERENCY_PAGEINFOS;
    headerPage.header.circularListTail = 0;
    headerPage.header.nPageInfos       = 0;
    headerPage.header.timestamp        = 0;

    for(i = 0; i < headerPage.header.circularListSize; i++)
    {
#ifdef USE_SHARED_MEMORY_BUFFER	
	/* shared memory buffer를 사용시 buffer sync를 위한 hostid 및 shared memory buffer Id */
	pageInfosPage.pageInfos[0].hostId	 = NIL;
	pageInfosPage.pageInfos[0].shmBufferId	 = NIL;
#endif
        pageInfosPage.pageInfos[0].timestamp     = NIL;
        pageInfosPage.pageInfos[0].pageId.pageNo = NIL;
        pageInfosPage.pageInfos[0].pageId.volNo  = NIL;
    }

    e = RDsM_WriteTrain(handle, (char*)&headerPage, &(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId), BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    e = RDsM_WriteTrain(handle, (char*)&pageInfosPage, &(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId), BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);
    
    /* fomat된 device를 unmount한다 */
    /* Unmount the formatted device */
    e = BfM_DismountCoherencyVolume(handle);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four BfM_SyncBufferUsingCoherencyVolume(
    Four handle,
    Four 		volNo
)
{
    Four                    	e;
    bfm_CoherencyPageInfo_t 	pageInfos[BFM_N_COHERENCY_PAGEINFOS];
    Four                    	nReturnedPageInfos;
    Four                    	i, index;
    Four                    	type;
#ifdef USE_SHARED_MEMORY_BUFFER
    Four		    			hostId;
    BfMHashKey		    		localKey;
#endif

    TR_PRINT(TR_BFM, TR1,("BfM_SyncBufferUsingCoherencyVolume(handle)")); 

#ifdef USE_SHARED_MEMORY_BUFFER	
    /* Block signals. */
    SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(handle);
#endif 

    /* Check whether a coherency volume is mounted. If it is not mounted, do nothing. */
    /* Check whether a coherency volume is mounted. 만약 mount가 되어 있지 않다면, 아무것도 하지 않는다. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) { 

#ifdef USE_SHARED_MEMORY_BUFFER		
    	/* Unblock signals. */
    	SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(handle);

    	/* Check received signal and handle it. */
    	SHARED_MEMORY_BUFFER_CHECK_SIGNAL(handle);
#endif 
	return eNOERROR;
    }

    /* coherency volume에 lock을 건다 */
    /* Acquire the lock on coherency volume */
    e = bfm_GetCoherencyVolumeLock(handle, L_S);
    if(e < eNOERROR) ERR(handle, e);

#if defined(VERBOSE)
    printf("Start synchronizing buffer for volume %ld with timestamp %ld\n", volNo, BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp);
#endif

    /* For a given volNo, read pageInfos that is updated after bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimeStamp 
       from coherency volume and invalidate buffer pages correspond to that pageInfos. */
    /* coherency volume을 읽어, 주어진 volNo에 대해 bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp 이후에 
       갱신이 이루어진 pageInfo를 읽어, 이에 해당하는 BfM의 buffer page들을 invalidate시킨다. */

#ifdef USE_SHARED_MEMORY_BUFFER		
    /* get the host id of this machine */
    hostId = gethostid();
#endif

    /* coherency volume에서의 pageInfos를 읽는다 */
    /* Read pageInfos from coherency volume */
    e = bfm_ReadCoherencyPageInfos(handle, BFM_N_COHERENCY_PAGEINFOS, pageInfos, &nReturnedPageInfos);
    if (e < eNOERROR) ERR(handle, e);

    /* 문제 lastCoherencyUpdateTimestamp가 wrap around되면, 순간적으로 큰값에서 작은 값이 되버린다.
       이렇게 되면, 반드시, 모든 buffer page를 invalidate하게 되는 조건으로 가게 된다.
       하지만, 이런 경우는 매우 드물게 (계산상으로는 1년에 1번꼴)로 생기기 때문에 무시해 버린다. */
    /* Problem! - When lastCoherencyUpdateTimeStamp is wrap around, it is changed from large value to 
       small value immediately. In this case, all buffer pages will be invalidate. But, the case will 
       be happen rarely. (By calculation, it happens one time per one year.) So, ignore this case. */
    /* buffer를 synchronize한다. */
    /* Synchronize buffer. */
    if(nReturnedPageInfos > 0 && BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp != NIL)  
    {   /* nReturnedPageInfos이 0이거나, lastCoherencyUpdateTimestamp이 NIL이면, buffer sync를 할 필요가 없다 */
        /* If nReturnedPageInfos is 0 or lastCoherencyUpdateTimestamp is NIL, need not to sync buffer */
        if((pageInfos[0].timestamp >= BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp && 
	    pageInfos[nReturnedPageInfos - 1].timestamp > BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp ) ||
           (BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp - pageInfos[nReturnedPageInfos - 1].timestamp) > 
	   (BFM_COHERENCY_TIMESTAMP_MAX / 2))
        {
            /* pageInfos의 맨 앞을 본다. 만약 맨 앞의 timestamp가 lastCoherencyUpdateTimestamp 보다 큰 값이라면,
               전체, BfM의 Buffer Page들을 invalidate시킨다. */
            /* pageInfos의 맨 뒤를 본다. 만약 맨뒤보다 lastCoherencyUpdateTimestamp가 너무 크다면, wrap around에 의해
               lastCoherencyUpdateTimestamp가 앞에서 일어난 일일 수가 있다. 이때는 BfM의 전체를 invalidate한다. */
            /* Check the first timestamp of pageInfos. If the timestamp is larger than lastCoherencyUpdateTimestamp,
	       	   invalidate all buffer pages. */
		    /* Check the last timestamp of pageInfos. If lastCoherencyUpdateTimestamp is much larger than the timestamp,
	       	   the lastCoherencyUpdateTimestap can be happen previously, So invalidate all buffer pages in this case */

#if defined(VERBOSE)
            printf("Discard all buffers\n");
            printf("coherency's first timestamp %ld\n", pageInfos[0].timestamp);
            printf("coherency's last  timestamp %ld\n", pageInfos[nReturnedPageInfos - 1].timestamp);
            printf("in memory last timestamp %ld\n", BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp); 
#endif
            /* 이때, 추가적으로 pageInfos에서 주어진 volNo에 대한 정보를 걸러서 처리해야 좀더 정확한 시점을
               구해낼 수 있으나, 이 경우, 예외 상황이 간혹 발생하기 때문에 이를 포함하는 조건으로 검사를 한다. */
            /* 주어진 volNo에 대한 buffer invalidataion을 시도한다 */
		    /* In this case, to find more exact buffer sync timing, check more informations of the volume 
		       corresponds to the given volNo. But, because exception can be happend occasionally, ignore it. */
	    	/* Invalidate buffer pages corresponds to the given volNo. */
            for(type = 0; type < NUM_BUF_TYPES; type++) 
            {
                for(index = 0; index < BI_NBUFS(type); index++)  
                {
                    /* Invalidate buffer pages corresponds to the given volNo. */
                    /* 주어진 page에 대한 buffer invalidation을 시도한다 */
#ifdef USE_SHARED_MEMORY_BUFFER		
					localKey = BI_KEY(type, index);
                    if(!IS_NILBFMHASHKEY(localKey) && localKey.volNo == volNo) {
						/* Acquire lock of the buffer table entry. */
						ERROR_PASS(handle, bfm_Lock(handle, (TrainID* )&localKey, type));
						
						if (!EQUALKEY(&BI_KEY(type, i), &localKey)) {
							/* Release lock of the buffer table entry. */
							ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
							continue;
						}
#else
					if(!IS_NILBFMHASHKEY(BI_KEY(type, index)) && BI_KEY(type, index).volNo == volNo) {
#endif
						if(BI_BITS(type, index) & DIRTY) {
							/* This situation must not be happen. It means there were write operations before acquiring 
							   first lock on given volume in first transaction. So, it is internal error. */
							/* 발생하면 안되는 경우가 발생하였다. transaction 첫 시작 이후, 주어진 volume에 대해
							   첫 lock이 걸려, sync 작업을 하는데, 그 이전에 쓰기 연산을 했다는 뜻이 되므로,
							   내부 에러이다. */
							ERR_BfM(handle, eINTERNAL, (TrainID* )&localKey, type);	
						}
						if(BI_NFIXED(type, index)) {
							ERR_BfM(handle, eINTERNAL, (TrainID* )&localKey, type);	
						}
						
						e = bfm_Delete(handle, &BI_KEY(type, index), type);
						if(e < eNOERROR) {
							ERR_BfM(handle, e, (TrainID* )&localKey, type);	
						}
#ifdef USE_SHARED_MEMORY_BUFFER		
						/* 
						 * Set BI_NFIXED(type, i) by 1 not to select ac victim
						 * during the initializing BI_KEY(type, i) to NIL.
						 */
						BI_NFIXED(type, index) = 1;
#endif
                        BI_BITS(type, index) = ALL_0;
                        SET_NILBFMHASHKEY(BI_KEY(type, index));
                        BI_NFIXED(type, index) = 0;

#ifdef USE_SHARED_MEMORY_BUFFER	
						/* Release lock of the buffer table entry. */
						ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
#endif
					}
				}
			}
		}
        else
        {
            /* pageInfos를 뒤에서 부터 차례대로 보면서, 조건을 만족하는 pageInfo에 대해 buffer page invalidation을 시도한다. */
            /* Check a timestamp of pageInfos backward, invalidate buffer pages correspond to pageInfos which satisfies the condition. */ 
            for(i = nReturnedPageInfos - 1; i >= 0; i--)
            {
                if(pageInfos[i].timestamp <= BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp)
                    /* lastCoherencyUpdateTimestamp이전의 pageInfo들은 적용할 필요가 없다. */
                    /* Need not consider pageInfos of which timestamp is earlier than lastCoherencyUpdateTimestamp. */ 
                    break;

#ifdef USE_SHARED_MEMORY_BUFFER
				/* Shared memory buffer를 사용시 다른 machine에서 사용한 page인지, 같은 machine이어도
				 * 다른 share memory buffer에 있던 page인가를 확인하고 sync해야 한다. 
				 */
				else if(pageInfos[i].pageId.volNo == volNo &&
					(pageInfos[i].hostId != hostId || pageInfos[i].shmBufferId != BFM_PER_PROCESS_DS.shmBufferId) &&  
					pageInfos[i].timestamp > BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp)
#else
                else if(pageInfos[i].pageId.volNo == volNo &&
                        pageInfos[i].timestamp > BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp) 
#endif
                {
                    for(type = 0; type < NUM_BUF_TYPES; type++) 
                    {
                        if((index = bfm_LookUp(handle, (BfMHashKey *)&pageInfos[i].pageId, type)) != NOTFOUND_IN_HTABLE) 
                        {
                            /* Invalidate buffer pages correspond to the given volNo. */
                            /* 주어진 page에 대한 buffer invalidation을 시도한다 */

#ifdef USE_SHARED_MEMORY_BUFFER		
			    localKey = BI_KEY(type, index);
			    if (localKey.volNo == volNo) {
				/* Acquire lock of the buffer table entry. */
				ERROR_PASS(handle, bfm_Lock(handle, (TrainID* )&localKey, type));

				if (!EQUALKEY(&BI_KEY(type, index), &localKey)) {
				    /* Release lock of the buffer table entry. */
				    ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
				    continue;
				}

#else
                            if(BI_KEY(type, index).volNo == volNo) {
#endif

#ifdef VERBOSE
                                printf("Discard buffer for page (%ld, %ld)\n", pageInfos[i].pageId.volNo, pageInfos[i].pageId.pageNo);
#endif
                                if(BI_BITS(type, index) & DIRTY) {
                                    /* This situation must not be happen. It means there were write operations before acquiring 
						               first lock on given volume in first transaction. So, it is internal error. */
                                    /* 발생하면 안되는 경우가 발생하였다. transaction 첫 시작 이후, 주어진 volume에 대해
                                       첫 lock이 걸려, sync 작업을 하는데, 그 이전에 쓰기 연산을 했다는 뜻이 되므로,
                                       내부 에러이다. */
									ERR_BfM(handle, eINTERNAL, (TrainID* )&localKey, type);
								}
								if(BI_NFIXED(type, index)) {
									ERR_BfM(handle, eINTERNAL, (TrainID* )&localKey, type);
								}

                                e = bfm_Delete(handle, &BI_KEY(type, index), type);
                                if(e < 0) {
									ERR_BfM(handle, e, (TrainID* )&localKey, type);
								}

#ifdef USE_SHARED_MEMORY_BUFFER	
								/* 
								 * Set BI_NFIXED(type, i) by 1 not to select ac victim
								 * during the initializing BI_KEY(type, i) to NIL.
								 */
								BI_NFIXED(type, index) = 1;
#endif
                                BI_BITS(type, index) = ALL_0;
                                SET_NILBFMHASHKEY(BI_KEY(type, index));
                                BI_NFIXED(type, index) = 0;

#ifdef USE_SHARED_MEMORY_BUFFER
								/* Release lock of the buffer table entry corresponds to pageInfos.pageId. */
								ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
#endif
							}
						}
					}
                }
            }
        }
    }
    /* coherency volume의 lock을 해제한다 */
    /* Release the lock on coherency volume */
    e = bfm_ReleaseCoherencyVolumeLock(handle);
    if(e < eNOERROR) ERR(handle, e);

#ifdef USE_SHARED_MEMORY_BUFFER	 	
    /* Unblock signals. */
    SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(handle);

    /* Check received signal and handle it. */
    SHARED_MEMORY_BUFFER_CHECK_SIGNAL(handle);
#endif 

#ifdef VERBOSE
    printf("Done for synchronizing buffer for volume %ld\n", volNo);
#endif

    return eNOERROR;
}

Four BfM_UpdateCoherencyVolume(Four handle)
{
    Four    	e;
    Four    	timestamp;
    Four    	i;
#ifndef USE_SHARED_MEMORY_BUFFER 
    Four    	nMountedVols;
    Four    	mountedVolNos[MAXNUMOFVOLS];
#endif

    /* bfm_CoherencyVolumeInfo에 기록된 pageInfo들을 disk로 flush out 시킨다. */
    /* Flush out pageInfos of bfm_CoherencyVolumeInfo to disk. */
    TR_PRINT(TR_BFM, TR1,("BfM_UpdateCoherencyVolume(handle)"));  

    /* Check whether a coherency volume is mounted or not. If it is not mounted, do nothing. */
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOERROR; 

    /* coherency volume에 lock을 건다 */
    /* Acquire the lock on coherency volume */
    e = bfm_GetCoherencyVolumeLock(handle, L_X);
    if(e < eNOERROR) ERR(handle, e);

    /* Timestamp를 갱신하기 전에, 아직 buffer sync가 되지 않은 볼륨들을 강제로 sync한다.
       왜냐하면, bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp을 update하고 나면, 모든 buffer들은 sync되었다고
       가정하기 때문이다. */
    /* Before updating timestamp, sync buffers which didn't sync yet forcibly
       because we assume that all buffers are synced after updating bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp. */
    /* Synchronize buffers about all mounted volumes. */
    /* 모든 mount된 volume에 대하여 buffer sync를 시도한다 */

#ifndef USE_SHARED_MEMORY_BUFFER
    e = RDsM_GetAllMountedVolNos(handle, mountedVolNos);
    if (e < eNOERROR) ERR(handle, e);
    nMountedVols = e;
#endif

#ifndef USE_SHARED_MEMORY_BUFFER
    for(i = 0; i < nMountedVols; i++)
#else
    for(i = 0; i < BFM_PER_THREAD_DS(handle).nMountedVols; i++)
#endif
    {
#ifndef USE_SHARED_MEMORY_BUFFER
        if(RDsM_GetVolumeLockMode(handle, mountedVolNos[i]) == L_NL && 
           !IS_TEMP_VOLUME(mountedVolNos[i]) && 
           !RM_IsLogVolume(handle, mountedVolNos[i]) &&
           !BfM_IsCoherencyVolume(handle, mountedVolNos[i]))
#else
        if(RDsM_GetVolumeLockMode(handle, BFM_PER_THREAD_DS(handle).mountedVolNos[i]) == L_NL && 
           !IS_TEMP_VOLUME(BFM_PER_THREAD_DS(handle).mountedVolNos[i]) && 
           !RM_IsLogVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos[i]) &&
           !BfM_IsCoherencyVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos[i]))
#endif
        {
#ifndef USE_SHARED_MEMORY_BUFFER
#ifdef VERBOSE
            printf("Sync volume %ld\n", BFM_PER_THREAD_DS(handle).mountedVolNos[i]);
#endif
            e = BfM_SyncBufferUsingCoherencyVolume(handle, mountedVolNos[i]);
            if (e < eNOERROR) ERR(handle, e);
#else
#ifdef VERBOSE
            printf("Sync volume %ld\n", mountedVolNos[i]);
#endif
            e = BfM_SyncBufferUsingCoherencyVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos[i]);
            if (e < eNOERROR) ERR(handle, e);
#endif
        }
    }

#ifdef VERBOSE
    printf("Start updating coherency volume\n");
#endif

    /* 마지막으로 coherency volume을 update한 시간을 현재 시간으로 재 설정한다. */
    /* Set up last update timestamp of coherency volume to current timestamp. */ 
    e = bfm_ReadCoherencyVolumeTimestamp(handle, &timestamp);
    if (e < eNOERROR) ERR(handle, e);

    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp = timestamp; 
    timestamp = (timestamp + 1) % BFM_COHERENCY_TIMESTAMP_MAX;

    e = bfm_WriteCoherencyVolumeTimestamp(handle, timestamp);
    if (e < eNOERROR) ERR(handle, e);

#ifdef VERBOSE
    printf("Get and set new timestamp %ld\n", timestamp);
#endif

    /* pageInfos를 disk로 flush out시킨다. */
    /* Flush out pageInfos to disk. */
    e = bfm_FlushCoherencyPageInfos(handle, BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp); 
    if (e < eNOERROR) ERR(handle, e);

    e = bfm_InitCoherencyPageInfos(handle);
    if (e < eNOERROR) ERR(handle, e);

    /* coherency volume의 lock을 해제한다 */
    /* Release the lock on coherency volume */
    e = bfm_ReleaseCoherencyVolumeLock(handle);
    if(e < eNOERROR) ERR(handle, e);

#ifdef VERBOSE
    printf("Done for updating coherency volume\n");
#endif

    return eNOERROR;
}

Four BfM_MountCoherencyVolume(
    Four handle,
    char*   		devName             /* IN  device name of the coherency volume */
)
{
    Four            e;
    char*           devNames[1];
    Four            volNo;

    TR_PRINT(TR_BFM, TR1,("BfM_MountCoherencyVolume(handle)"));   

    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo != NO_COHERENCY_VOLUME) return eALREADYMOUNTEDCOHERENCYVOLUME_BFM; 

    if(devName == NULL) return eNOERROR;

    devNames[0] = devName;
    e = RDsM_Mount(handle, 1, devNames, &volNo);
    if (e < eNOERROR) ERR(handle, e);

    /* Initialize bfm_CoherencyVolumeInfo structure */
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo                       = volNo;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp = NIL;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos                  = 0;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId.pageNo         = BFM_COHERENCY_VOLUME_EXTENTSIZE + 0;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId.volNo          = volNo;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId.pageNo      = BFM_COHERENCY_VOLUME_EXTENTSIZE + BFM_COHERENCY_VOLUME_HEADER_N_PAGE; /* It is next to header page */
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId.volNo       = volNo;

    return eNOERROR;
}

Four BfM_DismountCoherencyVolume(Four handle)
{
    Four            e;
    char*           devNames[1];
    Four            volNo;

    TR_PRINT(TR_BFM, TR1,("BfM_DismountCoherencyVolume(handle)"));    

    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOERROR; 

    e = RDsM_Dismount(handle, BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo);		 
    if (e < eNOERROR) ERR(handle, e);

    /* Initialize bfm_CoherencyVolumeInfo structure */
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo                       = NIL;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp = NIL;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos                  = 0;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId.pageNo         = 0;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId.volNo          = NIL;
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId.pageNo      = 0; 
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId.volNo       = NIL;

    return eNOERROR;
}

Boolean BfM_IsCoherencyVolume(
    Four handle,
    Four 		volNo
)
{
    TR_PRINT(TR_BFM, TR1,("BfM_IsCoherencyVolume(handle)"));  

    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NIL)
        return FALSE;

    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == volNo)
        return TRUE;

    return FALSE;
}

Four bfm_GetCoherencyVolumeLock(
    Four handle,
    LockMode 	lockMode
)
{
    Four 			e;

    TR_PRINT(TR_BFM, TR1,("bfm_GetCoherencyVolumeLock(handle)"));

    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOERROR; 
    
    /* get lock for coherency volume */
    e = RDsM_GetVolumeLock(handle, BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo, lockMode, FALSE); 	
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four bfm_ReleaseCoherencyVolumeLock(Four handle)
{
    Four 			e;

    TR_PRINT(TR_BFM, TR1,("bfm_ReleaseCoherencyVolumeLock(handle)"));

    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOERROR; 

    /* release lock for coherency volume */
    e = RDsM_ReleaseVolumeLock(handle, BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo);	
    if(e < eNOERROR) ERR(handle, e);
    
    return eNOERROR;
}

Four bfm_ReadCoherencyVolumeTimestamp(
    Four handle,
    Four*   	timestamp
)
{
    Four                         e;
    bfm_CoherencyVolumeHeaderPage headerPage;
    
    TR_PRINT(TR_BFM, TR1,("bfm_ReadCoherencyVolumeTimestamp(handle)"));

    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOTMOUNTEDCOHERENCYVOLUME_BFM; 

    /* coherency volume header를 읽는다 */
    /* Read the header of a coherency volume */
    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, (char*)&headerPage, BFM_COHERENCY_VOLUME_HEADER_N_PAGE); 
    if(e < eNOERROR) ERR(handle, e);

    /* header로부터 timestamp 값을 읽어 return value로 설정한다 */
    /* Read timestamp from header and return it. */
    *timestamp = headerPage.header.timestamp;

    return eNOERROR;
}

Four bfm_WriteCoherencyVolumeTimestamp(
    Four handle,
    Four    	timestamp
)
{
    Four                         	e;
    bfm_CoherencyVolumeHeaderPage 	headerPage;
    
    TR_PRINT(TR_BFM, TR1,("bfm_WriteCoherencyVolumeTimestamp(handle)"));

    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOTMOUNTEDCOHERENCYVOLUME_BFM; 

    /* coherency volume header를 읽는다 */
    /* Read the header of a coherency volume */
    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, (char*)&headerPage, BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    /* header의 timestamp를 update한다 */
    /* Update timestamp of header */
    headerPage.header.timestamp = (timestamp % BFM_COHERENCY_TIMESTAMP_MAX);

    /* coherency volume header를 쓴다 */
    /* Write the header of a coherency volume to coherency volume */
    e = RDsM_WriteTrain(handle, (char*)&headerPage, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four bfm_AppendCoherencyPageInfos(
    Four handle,
    bfm_CoherencyPageInfo_t*    pageInfo
)
{
    Four e;
    
    TR_PRINT(TR_BFM, TR1,("bfm_AppendCoherencyPageInfos(handle)"));

    /* pageInfo를 bfm_CoherencyVolumeInfo.pageInfos에 추가한다 */
    /* 만약 bfm_CoherencyVolumeInfo.nPageInfos이 BFM_N_COHERENCY_PAGEINFOS보다 큰 경우에는, 
       데이타를 하나 앞당겨서 이를 쓴다 - 가정 memory copy는 그리 큰 연산이 아니라고 가정한다. */
    /* Add pageInfo to bfm_CoherencyVolumeInfo.pageInfos */
    /* If bfm_CoherencyVolumeInfo.nPageInfos is larger than BFM_N_COHERENCY_PAGEINFOS,
       shift pageInfos to one pageInfo ahead. - assume that cost of memory copy is not much. */

    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos >= BFM_N_COHERENCY_PAGEINFOS) 
    {
        /* 첫번째 pageInfo를 지우고 앞으로 땡긴다 */
        /* Delete first pageInfo and shift the other pageInfos to one pageInfo ahead. */
        memcpy(&BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[0], 
               &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[1], 
               sizeof(bfm_CoherencyPageInfo_t) * (BFM_N_COHERENCY_PAGEINFOS - 1));

        memcpy(&BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[BFM_N_COHERENCY_PAGEINFOS - 1],
               pageInfo, sizeof(bfm_CoherencyPageInfo_t));
        BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[BFM_N_COHERENCY_PAGEINFOS - 1].timestamp = NIL;

        BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos = BFM_N_COHERENCY_PAGEINFOS;
    }
    else
    {
        memcpy(&BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos],
               pageInfo, sizeof(bfm_CoherencyPageInfo_t));
        BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos].timestamp = NIL;
        BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos ++;
    }

    return eNOERROR;
}

Four bfm_FlushCoherencyPageInfos(
    Four handle,
    Four 		timestamp                  /* IN : timestamp used on flush */
)
{
    Four                            	e;
    bfm_CoherencyVolumeHeaderPage   	headerPage;
    bfm_CoherencyVolumePageInfosPage    pageInfosPage;
    Four                            	i;

    TR_PRINT(TR_BFM, TR1,("bfm_FlushCoherencyPageInfos(handle)"));
 
    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOTMOUNTEDCOHERENCYVOLUME_BFM;

    /* If there are nothing to update, then return */
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos == 0) return eNOERROR; 

    /* bfm_CoherencyVolumeInfo.pageInfos의 내용을 coherency volume에 flush한다 */
    /* Flush the contents of bfm_CoherencyVolumeInfo.pageInfos to coherency volume */    
    /* coherency volume header와 pageInfos를 읽는다 */
    /* Read a header and pageInfos of coherency volume */

    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, (char*)&headerPage, BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId, (char*)&pageInfosPage, BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

#if defined(VERBOSE)
    printf("Flushing %ld page infos\n", BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos); 
    printf("Circular list (head=%ld, tail=%ld)\n", 
           headerPage.header.circularListHead, headerPage.header.circularListTail);
#endif

    /* disk pageInfos에 pageInfo를 하나씩 추가하면서 header를 수정한다 */
    /* With adding pageInfo to disk pageInfos one by one, update header */
#ifdef VERBOSE
    printf("page list [");
#endif

    for(i = 0; i < BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos; i++) 
    {
#if defined(VERBOSE)
        printf("(%ld,%ld)", BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[i].pageId.volNo, 
                            BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[i].pageId.pageNo);
#endif
        /* bfm_CoherencyVolumeInfo.pageInfos[i]를 pageInfosPage에 추가한다. */
        /* pageInfosPage는 circular buffer의 형태를 하고 있으므로 이를 다룰때 주의할것 */
		/* Add bfm_CoherencyVolumeInfo.pageInfos[i] to pageInfosPage. */
		/* Because pageInfosPage is a circular buffer, pay attention to handle it. */
        if(headerPage.header.nPageInfos >= headerPage.header.circularListSize)
        {
            /* pageInfos에 최대 크기 만큼 데이타가 들어간 경우 header를 하나 증가시키고, tail도 하나 증가시키고, 데이타를 삽입한다 */
		    /* When pageInfos are full, increase header and tail, and insert new pageInfo. */
            headerPage.header.circularListHead = (headerPage.header.circularListHead + 1) % headerPage.header.circularListSize;
            headerPage.header.circularListTail = (headerPage.header.circularListTail + 1) % headerPage.header.circularListSize;
            memcpy(&pageInfosPage.pageInfos[headerPage.header.circularListTail],
                   &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[i], sizeof(bfm_CoherencyPageInfo_t));
            pageInfosPage.pageInfos[headerPage.header.circularListTail].timestamp = timestamp;
        }
        else
        {
            /* tail을 하나 증가시키고, 데이타를 삽입한다. */
            /* Increase tail and insert bfm_CoherencyVolumeInfo.pageInfos to pageInfosPage */
            if(headerPage.header.nPageInfos == 0)
            {
                headerPage.header.circularListHead = 0;
                headerPage.header.circularListTail = 0;
            }
            else
                headerPage.header.circularListTail = (headerPage.header.circularListTail + 1) % headerPage.header.circularListSize;

            memcpy(&pageInfosPage.pageInfos[headerPage.header.circularListTail],
                   &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[i], sizeof(bfm_CoherencyPageInfo_t)); 
            pageInfosPage.pageInfos[headerPage.header.circularListTail].timestamp = timestamp;

            /* 새로운 pageInfo가 들어온 것이므로, headerPage.header.nPageInfos을 증가시킨다 */
            /* Increase headerPage.header.nPageInfos because new pageInfo is inserted. */
            headerPage.header.nPageInfos ++;
        }
    }
#ifdef VERBOSE
    printf("]\n");
#endif

    /* disk에 header와 pageInfos를 쓴다 */
    /* Write header and pageInfos to disk */
    e = RDsM_WriteTrain(handle, (char*)&headerPage, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    e = RDsM_WriteTrain(handle, (char*)&pageInfosPage, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId, BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

#ifdef VERBOSE
    printf("Circular list (head=%ld, tail=%ld)\n", 
           headerPage.header.circularListHead, headerPage.header.circularListTail);
#endif

    return eNOERROR;
}

Four bfm_ReadCoherencyPageInfos(
    Four handle,
    Four                    	sizeOfPageInfos,    	/* IN  the size of pageInfos array.(the size of data to read is fitted to this size) */
    bfm_CoherencyPageInfo_t 	*pageInfos,         	/* OUT location in which read pageInfos are saved */
    Four                    	*nReturnedPageInfos  	/* OUT the number of read pages */
)
{
    Four                            	e;
    bfm_CoherencyVolumeHeaderPage   	headerPage;
    bfm_CoherencyVolumePageInfosPage    pageInfosPage;
    Four                            	nTransferred;
    Four                            	i;

    TR_PRINT(TR_BFM, TR1,("bfm_ReadCoherencyPageInfos(handle)"));

    /* Check whether a coherency volume is mounted. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOTMOUNTEDCOHERENCYVOLUME_BFM; 

    /* coherency volume로 부터 page info를 읽어 이를 사용자가 준 'pageInfos' array로 반환한다 */
    /* Read page infos from coherency volume and convert them to 'pageInfos' array */    
    /* coherency volume header와 pageInfos를 읽는다 */
    /* Read coherency volume header and pageInfos. */

    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, (char*)&headerPage, BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId, (char*)&pageInfosPage, BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    /* disk pageInfos를 읽어 이를 pageInfos에 추가한다. */
    /* Read disk pageInfos and add it to pageInfos. */
    if(headerPage.header.nPageInfos > 0)
    {
        if(headerPage.header.circularListHead <= headerPage.header.circularListTail)
        {
            /* list head와 tail사이에, wrap around가 존재하지 않는다면 바로 copy한다 */
            /* If there is no wrap around between head and tail, just copy. */
            nTransferred = headerPage.header.circularListTail - headerPage.header.circularListHead + 1;
            if(nTransferred > sizeOfPageInfos)  /* 만약 읽어들일 양에, pageInfos array보다 크다면, 그 양을 보정한다. */
                                                /* If the size to read is larger than pageInfos array, adjust the size. */ 
                nTransferred = sizeOfPageInfos;
            if(nTransferred > 0)
            {
                memcpy(&pageInfos[0], &pageInfosPage.pageInfos[headerPage.header.circularListHead],
                       nTransferred * sizeof(bfm_CoherencyPageInfo_t));
            }

            *nReturnedPageInfos = nTransferred;
        }
        else
        {
            /* list head와 tail사이에, wrap around가 존재하는 경우에는 두번에 나누어서 이를 copy한다 */
            /* headerPage.header.circularListHead 부터 headerPage.header.circularListSize - 1 까지를 읽는다 */
		    /* If there is wrap around between head and tail, diveide pageInfos into two parts and copy each part one by one. */
		    /* Read pageInfos which spans from headerPage.header.circularListSize to headerPage.header.circularListSize - 1. */
            nTransferred = headerPage.header.circularListSize - headerPage.header.circularListHead;
            if(nTransferred > sizeOfPageInfos)  /* 만약 읽어들일 양에, pageInfos array보다 크다면, 그 양을 보정한다. */
                                                /* If the size of read is larger than pageInfos array, adjust the size. */
                nTransferred = sizeOfPageInfos;
            if(nTransferred > 0)
            {
                memcpy(&pageInfos[0], &pageInfosPage.pageInfos[headerPage.header.circularListHead],
                       nTransferred * sizeof(bfm_CoherencyPageInfo_t));
            }
            *nReturnedPageInfos =  nTransferred;
            sizeOfPageInfos     -= nTransferred;

            /* 0부터 headerPage.header.circularListTail까지를 읽는다 */
            /* Read pageInfos which spans from 0 to headerPage.header.circularListTail. */
            nTransferred = headerPage.header.circularListTail - 0 + 1;
            if(nTransferred > sizeOfPageInfos)  /* 만약 읽어들일 양에, pageInfos array보다 크다면, 그 양을 보정한다. */
                                                /* If the size of read is larger than pageInfos array, adjust the size. */
                nTransferred = sizeOfPageInfos;
            if(nTransferred > 0)
            {
                memcpy(&pageInfos[*nReturnedPageInfos], &pageInfosPage.pageInfos[0],
                       nTransferred * sizeof(bfm_CoherencyPageInfo_t));
            }
            *nReturnedPageInfos +=  nTransferred;
            sizeOfPageInfos     -= nTransferred;
        }
    }
    else
        *nReturnedPageInfos = 0;

    /* 반환할, pageInfos를 처음부터 읽으면서, timestamp에 대한 wrap around가 있는지 여부를 검사하여, 이를
       수정한다. 만약 존재한다면 BFM_COHERENCY_TIMESTAMP_MAX 만큼을 더해준다 */
    /* While reading whole pageInfos to be returned, check that timestamp is wrapped around. 
       If the timestamp is wrapped around, add BFM_COHERENCY_TIMESTAMP_MAX to it. */
    /* timestamp는 0부터 BFM_COHERENCY_TIMESTAMP_MAX 사이의 값이고, BFM_COHERENCY_TIMESTAMP_MAX은 Four type이 가질수 
       있는 최대 값보다 1/2 만큼 작은 값이다. 때문에 BFM_COHERENCY_TIMESTAMP_MAX을 더한다고, overflow가 발생하지 않는다.
       이 값을 wrap around가 일어난 부분 이후 부터 더해 주면, 일시적으로 wrap around가 없는것 처럼, 표현을 할 수 있다.

        주의 할점은, BfM_SyncBufferUsingCoherencyVolume에서 transaction timestamp를 사용하여, 위치를 찾을때,
        transaction timestamp에서의 wrap around를 처리하는 부분이 문제이다. 
    */
    /* Timestamp is from 0 to BFM_COHERENCY_TIMESTAMP_MAX, and BFM_COHERENCY_TIMESTAMP_MAX is half of maximum value
       of Four type. So, adding BFM_COHERENCY_TIMESTAMP_MAX to timestamp will not occur overflow.
       As adding the value to wrapped around timestamps, we consider the timestamps for a moment as if the timestamps is not wrapped around. 
   
       The point to pay attention is that handling wrap around of transaction timestamp 
       when finding a position using transaction timestap in BfM_SyncBufferUsingCoherencyVolume. 
    */

    for(i = 1; i < *nReturnedPageInfos; i++)
    {
        if(pageInfos[i - 1].timestamp > pageInfos[i].timestamp)
            break;
    }
    for(; i < *nReturnedPageInfos; i++)
    {
        pageInfos[i].timestamp += BFM_COHERENCY_TIMESTAMP_MAX;
    }

    return eNOERROR;
}

Four bfm_InitCoherencyPageInfos(Four handle)
{
    TR_PRINT(TR_BFM, TR1,("bfm_InitCoherencyPageInfos(handle)"));

    /* bfm_CoherencyVolumeInfo.nPageInfos을 0으로 하여 bfm_CoherencyVolumeInfo.pageInfos에 아무 정보도 
       없는 것으로 설정한다. */
    /* As setting bfm_CoherencyVolumeInfo.nPageInfos to 0, we think that there is no pageInfos in bfm_CoherencyVolumeInfo.pageInfos */

    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos = 0; 

    return eNOERROR;
}
#endif
