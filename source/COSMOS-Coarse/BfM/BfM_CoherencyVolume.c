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
 *  Buffer Coherency ������ �ذ��ϱ� ���� ���� ����Ÿ ������ coherency volume���� ������ �����Ѵ�.
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

    /* RDsM_Format�� ����Ͽ� device�� format�Ѵ�. */ 
    /* Format device using RDsM_Format(). */
    devNames[0]         = devName;
    /* extent size�� 8(=BFM_COHERENCY_VOLUME_EXTENTSIZE)�� ����Ѵ�. ������ �ּ����� RDsM�� volume header������ 
       6 page�̻��̱� �����̴�. �̵� ������ �ϳ��� extent�ȿ� ���� ���� ���� ������ 8�� ����Ѵ�. */
    /* Set extent size as 8(=BFM_COHRENCY_VOLUME_EXTENTSIZE) because the least size of volume header of RDsM is 6 pages.
       Since that can be put into one extent, we set extent size as 8. */
    extentSize          = BFM_COHERENCY_VOLUME_EXTENTSIZE;
    /* format�Ǵ� device�� ũ�⸦ ���Ѵ�. deviceũ��� ���� header�� ���� �ϳ��� extent�� 
       BFM_COHERENCY_VOLUME_HEADER_N_PAGE + BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE �̴�. 
       default setting�� ���� 8, 1, 1 page�� �� 10 page�̴�. */
    /* decide the size of device to be formated. The size of the device is sum of 
       one extent size for the header, BFM_COHERENCY_VOLUME_HEADER_N_PAGE, and BFM_COHRENCY_VOLUME_PAGEINFOS_N_PAGE.
       default parameters about that size are 8, 1, and 1 page, so we use 10 pages totally. */
    numPagesInDevice[0] = extentSize + BFM_COHERENCY_VOLUME_HEADER_N_PAGE + BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE;

    /* numPagesInDevice[0]�� extentSize�� ����� �����. */
    /* Set numPagesInDevice[0] to twice extentSize. */
    if(numPagesInDevice[0] % extentSize)    /* extentSize�� ����� �ƴ� ��� */
                                            /* In case numPagesInDevice[0] is not a multiple of extentSize */
        numPagesInDevice[0] = ((numPagesInDevice[0] / extentSize) + 1) * extentSize;

    e = RDsM_Format(handle, 1, devNames, title, volNo, extentSize, numPagesInDevice);
    if(e < eNOERROR) ERR(handle, e);

    /* format�� device�� mount�Ѵ�, mount�� ������ bfm_CoherencyVolumeInfo�� ��ϵȴ�. */
    /* Mount the formatted device. Mount information is saved in bfm_CoherencyVolumeInfo. */
    e = BfM_MountCoherencyVolume(handle, devName);
    if(e < eNOERROR) ERR(handle, e);

    /* header page�� pageInfo page�� ���� �ۼ��Ͽ� �̸� ���� */
    /* Initialize header page and pageInfo page, and write them to coherency volume. */
    headerPage.header.circularListHead = 0;
    headerPage.header.circularListSize = BFM_N_COHERENCY_PAGEINFOS;
    headerPage.header.circularListTail = 0;
    headerPage.header.nPageInfos       = 0;
    headerPage.header.timestamp        = 0;

    for(i = 0; i < headerPage.header.circularListSize; i++)
    {
#ifdef USE_SHARED_MEMORY_BUFFER	
	/* shared memory buffer�� ���� buffer sync�� ���� hostid �� shared memory buffer Id */
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
    
    /* fomat�� device�� unmount�Ѵ� */
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
    /* Check whether a coherency volume is mounted. ���� mount�� �Ǿ� ���� �ʴٸ�, �ƹ��͵� ���� �ʴ´�. */    
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) { 

#ifdef USE_SHARED_MEMORY_BUFFER		
    	/* Unblock signals. */
    	SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(handle);

    	/* Check received signal and handle it. */
    	SHARED_MEMORY_BUFFER_CHECK_SIGNAL(handle);
#endif 
	return eNOERROR;
    }

    /* coherency volume�� lock�� �Ǵ� */
    /* Acquire the lock on coherency volume */
    e = bfm_GetCoherencyVolumeLock(handle, L_S);
    if(e < eNOERROR) ERR(handle, e);

#if defined(VERBOSE)
    printf("Start synchronizing buffer for volume %ld with timestamp %ld\n", volNo, BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp);
#endif

    /* For a given volNo, read pageInfos that is updated after bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimeStamp 
       from coherency volume and invalidate buffer pages correspond to that pageInfos. */
    /* coherency volume�� �о�, �־��� volNo�� ���� bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp ���Ŀ� 
       ������ �̷���� pageInfo�� �о�, �̿� �ش��ϴ� BfM�� buffer page���� invalidate��Ų��. */

#ifdef USE_SHARED_MEMORY_BUFFER		
    /* get the host id of this machine */
    hostId = gethostid();
#endif

    /* coherency volume������ pageInfos�� �д´� */
    /* Read pageInfos from coherency volume */
    e = bfm_ReadCoherencyPageInfos(handle, BFM_N_COHERENCY_PAGEINFOS, pageInfos, &nReturnedPageInfos);
    if (e < eNOERROR) ERR(handle, e);

    /* ���� lastCoherencyUpdateTimestamp�� wrap around�Ǹ�, ���������� ū������ ���� ���� �ǹ�����.
       �̷��� �Ǹ�, �ݵ��, ��� buffer page�� invalidate�ϰ� �Ǵ� �������� ���� �ȴ�.
       ������, �̷� ���� �ſ� �幰�� (�������δ� 1�⿡ 1����)�� ����� ������ ������ ������. */
    /* Problem! - When lastCoherencyUpdateTimeStamp is wrap around, it is changed from large value to 
       small value immediately. In this case, all buffer pages will be invalidate. But, the case will 
       be happen rarely. (By calculation, it happens one time per one year.) So, ignore this case. */
    /* buffer�� synchronize�Ѵ�. */
    /* Synchronize buffer. */
    if(nReturnedPageInfos > 0 && BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp != NIL)  
    {   /* nReturnedPageInfos�� 0�̰ų�, lastCoherencyUpdateTimestamp�� NIL�̸�, buffer sync�� �� �ʿ䰡 ���� */
        /* If nReturnedPageInfos is 0 or lastCoherencyUpdateTimestamp is NIL, need not to sync buffer */
        if((pageInfos[0].timestamp >= BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp && 
	    pageInfos[nReturnedPageInfos - 1].timestamp > BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp ) ||
           (BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp - pageInfos[nReturnedPageInfos - 1].timestamp) > 
	   (BFM_COHERENCY_TIMESTAMP_MAX / 2))
        {
            /* pageInfos�� �� ���� ����. ���� �� ���� timestamp�� lastCoherencyUpdateTimestamp ���� ū ���̶��,
               ��ü, BfM�� Buffer Page���� invalidate��Ų��. */
            /* pageInfos�� �� �ڸ� ����. ���� �ǵں��� lastCoherencyUpdateTimestamp�� �ʹ� ũ�ٸ�, wrap around�� ����
               lastCoherencyUpdateTimestamp�� �տ��� �Ͼ ���� ���� �ִ�. �̶��� BfM�� ��ü�� invalidate�Ѵ�. */
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
            /* �̶�, �߰������� pageInfos���� �־��� volNo�� ���� ������ �ɷ��� ó���ؾ� ���� ��Ȯ�� ������
               ���س� �� ������, �� ���, ���� ��Ȳ�� ��Ȥ �߻��ϱ� ������ �̸� �����ϴ� �������� �˻縦 �Ѵ�. */
            /* �־��� volNo�� ���� buffer invalidataion�� �õ��Ѵ� */
		    /* In this case, to find more exact buffer sync timing, check more informations of the volume 
		       corresponds to the given volNo. But, because exception can be happend occasionally, ignore it. */
	    	/* Invalidate buffer pages corresponds to the given volNo. */
            for(type = 0; type < NUM_BUF_TYPES; type++) 
            {
                for(index = 0; index < BI_NBUFS(type); index++)  
                {
                    /* Invalidate buffer pages corresponds to the given volNo. */
                    /* �־��� page�� ���� buffer invalidation�� �õ��Ѵ� */
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
							/* �߻��ϸ� �ȵǴ� ��찡 �߻��Ͽ���. transaction ù ���� ����, �־��� volume�� ����
							   ù lock�� �ɷ�, sync �۾��� �ϴµ�, �� ������ ���� ������ �ߴٴ� ���� �ǹǷ�,
							   ���� �����̴�. */
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
            /* pageInfos�� �ڿ��� ���� ���ʴ�� ���鼭, ������ �����ϴ� pageInfo�� ���� buffer page invalidation�� �õ��Ѵ�. */
            /* Check a timestamp of pageInfos backward, invalidate buffer pages correspond to pageInfos which satisfies the condition. */ 
            for(i = nReturnedPageInfos - 1; i >= 0; i--)
            {
                if(pageInfos[i].timestamp <= BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp)
                    /* lastCoherencyUpdateTimestamp������ pageInfo���� ������ �ʿ䰡 ����. */
                    /* Need not consider pageInfos of which timestamp is earlier than lastCoherencyUpdateTimestamp. */ 
                    break;

#ifdef USE_SHARED_MEMORY_BUFFER
				/* Shared memory buffer�� ���� �ٸ� machine���� ����� page����, ���� machine�̾
				 * �ٸ� share memory buffer�� �ִ� page�ΰ��� Ȯ���ϰ� sync�ؾ� �Ѵ�. 
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
                            /* �־��� page�� ���� buffer invalidation�� �õ��Ѵ� */

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
                                    /* �߻��ϸ� �ȵǴ� ��찡 �߻��Ͽ���. transaction ù ���� ����, �־��� volume�� ����
                                       ù lock�� �ɷ�, sync �۾��� �ϴµ�, �� ������ ���� ������ �ߴٴ� ���� �ǹǷ�,
                                       ���� �����̴�. */
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
    /* coherency volume�� lock�� �����Ѵ� */
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

    /* bfm_CoherencyVolumeInfo�� ��ϵ� pageInfo���� disk�� flush out ��Ų��. */
    /* Flush out pageInfos of bfm_CoherencyVolumeInfo to disk. */
    TR_PRINT(TR_BFM, TR1,("BfM_UpdateCoherencyVolume(handle)"));  

    /* Check whether a coherency volume is mounted or not. If it is not mounted, do nothing. */
    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo == NO_COHERENCY_VOLUME) return eNOERROR; 

    /* coherency volume�� lock�� �Ǵ� */
    /* Acquire the lock on coherency volume */
    e = bfm_GetCoherencyVolumeLock(handle, L_X);
    if(e < eNOERROR) ERR(handle, e);

    /* Timestamp�� �����ϱ� ����, ���� buffer sync�� ���� ���� �������� ������ sync�Ѵ�.
       �ֳ��ϸ�, bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp�� update�ϰ� ����, ��� buffer���� sync�Ǿ��ٰ�
       �����ϱ� �����̴�. */
    /* Before updating timestamp, sync buffers which didn't sync yet forcibly
       because we assume that all buffers are synced after updating bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp. */
    /* Synchronize buffers about all mounted volumes. */
    /* ��� mount�� volume�� ���Ͽ� buffer sync�� �õ��Ѵ� */

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

    /* ���������� coherency volume�� update�� �ð��� ���� �ð����� �� �����Ѵ�. */
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

    /* pageInfos�� disk�� flush out��Ų��. */
    /* Flush out pageInfos to disk. */
    e = bfm_FlushCoherencyPageInfos(handle, BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp); 
    if (e < eNOERROR) ERR(handle, e);

    e = bfm_InitCoherencyPageInfos(handle);
    if (e < eNOERROR) ERR(handle, e);

    /* coherency volume�� lock�� �����Ѵ� */
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

    /* coherency volume header�� �д´� */
    /* Read the header of a coherency volume */
    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, (char*)&headerPage, BFM_COHERENCY_VOLUME_HEADER_N_PAGE); 
    if(e < eNOERROR) ERR(handle, e);

    /* header�κ��� timestamp ���� �о� return value�� �����Ѵ� */
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

    /* coherency volume header�� �д´� */
    /* Read the header of a coherency volume */
    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, (char*)&headerPage, BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    /* header�� timestamp�� update�Ѵ� */
    /* Update timestamp of header */
    headerPage.header.timestamp = (timestamp % BFM_COHERENCY_TIMESTAMP_MAX);

    /* coherency volume header�� ���� */
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

    /* pageInfo�� bfm_CoherencyVolumeInfo.pageInfos�� �߰��Ѵ� */
    /* ���� bfm_CoherencyVolumeInfo.nPageInfos�� BFM_N_COHERENCY_PAGEINFOS���� ū ��쿡��, 
       ����Ÿ�� �ϳ� �մ�ܼ� �̸� ���� - ���� memory copy�� �׸� ū ������ �ƴ϶�� �����Ѵ�. */
    /* Add pageInfo to bfm_CoherencyVolumeInfo.pageInfos */
    /* If bfm_CoherencyVolumeInfo.nPageInfos is larger than BFM_N_COHERENCY_PAGEINFOS,
       shift pageInfos to one pageInfo ahead. - assume that cost of memory copy is not much. */

    if(BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos >= BFM_N_COHERENCY_PAGEINFOS) 
    {
        /* ù��° pageInfo�� ����� ������ ����� */
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

    /* bfm_CoherencyVolumeInfo.pageInfos�� ������ coherency volume�� flush�Ѵ� */
    /* Flush the contents of bfm_CoherencyVolumeInfo.pageInfos to coherency volume */    
    /* coherency volume header�� pageInfos�� �д´� */
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

    /* disk pageInfos�� pageInfo�� �ϳ��� �߰��ϸ鼭 header�� �����Ѵ� */
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
        /* bfm_CoherencyVolumeInfo.pageInfos[i]�� pageInfosPage�� �߰��Ѵ�. */
        /* pageInfosPage�� circular buffer�� ���¸� �ϰ� �����Ƿ� �̸� �ٷ궧 �����Ұ� */
		/* Add bfm_CoherencyVolumeInfo.pageInfos[i] to pageInfosPage. */
		/* Because pageInfosPage is a circular buffer, pay attention to handle it. */
        if(headerPage.header.nPageInfos >= headerPage.header.circularListSize)
        {
            /* pageInfos�� �ִ� ũ�� ��ŭ ����Ÿ�� �� ��� header�� �ϳ� ������Ű��, tail�� �ϳ� ������Ű��, ����Ÿ�� �����Ѵ� */
		    /* When pageInfos are full, increase header and tail, and insert new pageInfo. */
            headerPage.header.circularListHead = (headerPage.header.circularListHead + 1) % headerPage.header.circularListSize;
            headerPage.header.circularListTail = (headerPage.header.circularListTail + 1) % headerPage.header.circularListSize;
            memcpy(&pageInfosPage.pageInfos[headerPage.header.circularListTail],
                   &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfos[i], sizeof(bfm_CoherencyPageInfo_t));
            pageInfosPage.pageInfos[headerPage.header.circularListTail].timestamp = timestamp;
        }
        else
        {
            /* tail�� �ϳ� ������Ű��, ����Ÿ�� �����Ѵ�. */
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

            /* ���ο� pageInfo�� ���� ���̹Ƿ�, headerPage.header.nPageInfos�� ������Ų�� */
            /* Increase headerPage.header.nPageInfos because new pageInfo is inserted. */
            headerPage.header.nPageInfos ++;
        }
    }
#ifdef VERBOSE
    printf("]\n");
#endif

    /* disk�� header�� pageInfos�� ���� */
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

    /* coherency volume�� ���� page info�� �о� �̸� ����ڰ� �� 'pageInfos' array�� ��ȯ�Ѵ� */
    /* Read page infos from coherency volume and convert them to 'pageInfos' array */    
    /* coherency volume header�� pageInfos�� �д´� */
    /* Read coherency volume header and pageInfos. */

    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.headerPageId, (char*)&headerPage, BFM_COHERENCY_VOLUME_HEADER_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    e = RDsM_ReadTrain(handle, &BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.pageInfosPageId, (char*)&pageInfosPage, BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE);
    if(e < eNOERROR) ERR(handle, e);

    /* disk pageInfos�� �о� �̸� pageInfos�� �߰��Ѵ�. */
    /* Read disk pageInfos and add it to pageInfos. */
    if(headerPage.header.nPageInfos > 0)
    {
        if(headerPage.header.circularListHead <= headerPage.header.circularListTail)
        {
            /* list head�� tail���̿�, wrap around�� �������� �ʴ´ٸ� �ٷ� copy�Ѵ� */
            /* If there is no wrap around between head and tail, just copy. */
            nTransferred = headerPage.header.circularListTail - headerPage.header.circularListHead + 1;
            if(nTransferred > sizeOfPageInfos)  /* ���� �о���� �翡, pageInfos array���� ũ�ٸ�, �� ���� �����Ѵ�. */
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
            /* list head�� tail���̿�, wrap around�� �����ϴ� ��쿡�� �ι��� ����� �̸� copy�Ѵ� */
            /* headerPage.header.circularListHead ���� headerPage.header.circularListSize - 1 ������ �д´� */
		    /* If there is wrap around between head and tail, diveide pageInfos into two parts and copy each part one by one. */
		    /* Read pageInfos which spans from headerPage.header.circularListSize to headerPage.header.circularListSize - 1. */
            nTransferred = headerPage.header.circularListSize - headerPage.header.circularListHead;
            if(nTransferred > sizeOfPageInfos)  /* ���� �о���� �翡, pageInfos array���� ũ�ٸ�, �� ���� �����Ѵ�. */
                                                /* If the size of read is larger than pageInfos array, adjust the size. */
                nTransferred = sizeOfPageInfos;
            if(nTransferred > 0)
            {
                memcpy(&pageInfos[0], &pageInfosPage.pageInfos[headerPage.header.circularListHead],
                       nTransferred * sizeof(bfm_CoherencyPageInfo_t));
            }
            *nReturnedPageInfos =  nTransferred;
            sizeOfPageInfos     -= nTransferred;

            /* 0���� headerPage.header.circularListTail������ �д´� */
            /* Read pageInfos which spans from 0 to headerPage.header.circularListTail. */
            nTransferred = headerPage.header.circularListTail - 0 + 1;
            if(nTransferred > sizeOfPageInfos)  /* ���� �о���� �翡, pageInfos array���� ũ�ٸ�, �� ���� �����Ѵ�. */
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

    /* ��ȯ��, pageInfos�� ó������ �����鼭, timestamp�� ���� wrap around�� �ִ��� ���θ� �˻��Ͽ�, �̸�
       �����Ѵ�. ���� �����Ѵٸ� BFM_COHERENCY_TIMESTAMP_MAX ��ŭ�� �����ش� */
    /* While reading whole pageInfos to be returned, check that timestamp is wrapped around. 
       If the timestamp is wrapped around, add BFM_COHERENCY_TIMESTAMP_MAX to it. */
    /* timestamp�� 0���� BFM_COHERENCY_TIMESTAMP_MAX ������ ���̰�, BFM_COHERENCY_TIMESTAMP_MAX�� Four type�� ������ 
       �ִ� �ִ� ������ 1/2 ��ŭ ���� ���̴�. ������ BFM_COHERENCY_TIMESTAMP_MAX�� ���Ѵٰ�, overflow�� �߻����� �ʴ´�.
       �� ���� wrap around�� �Ͼ �κ� ���� ���� ���� �ָ�, �Ͻ������� wrap around�� ���°� ó��, ǥ���� �� �� �ִ�.

        ���� ������, BfM_SyncBufferUsingCoherencyVolume���� transaction timestamp�� ����Ͽ�, ��ġ�� ã����,
        transaction timestamp������ wrap around�� ó���ϴ� �κ��� �����̴�. 
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

    /* bfm_CoherencyVolumeInfo.nPageInfos�� 0���� �Ͽ� bfm_CoherencyVolumeInfo.pageInfos�� �ƹ� ������ 
       ���� ������ �����Ѵ�. */
    /* As setting bfm_CoherencyVolumeInfo.nPageInfos to 0, we think that there is no pageInfos in bfm_CoherencyVolumeInfo.pageInfos */

    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.nPageInfos = 0; 

    return eNOERROR;
}
#endif
