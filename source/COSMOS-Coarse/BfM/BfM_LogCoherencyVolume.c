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
#ifdef USE_LOG_COHERENCY_VOLUME
/*
 * Module: BfM_LogCoherencyVolume.c
 *
 * Description:
 *
 * Note:
 *
 * Exports:
 *  BfM_SyncBufferUsingCoherencyVolume
 */

#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "RDsM_Internal.h"	
#include "RM.h"
#include <string.h>
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

/* global variable declarations */
bfm_CoherencyVolumeInfo_t bfm_CoherencyVolumeInfo = {NO_COHERENCY_VOLUME};

Four BfM_InitCoherencyInfo(Four handle, Four volNo)
{
    bfm_CoherencyVolumeInfo.volNo = volNo;
    bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp = NIL;
    bfm_CoherencyVolumeInfo.nPageInfos = 0;
    bfm_CoherencyVolumeInfo.headerPageId.pageNo = 1; 
    bfm_CoherencyVolumeInfo.headerPageId.volNo = volNo;
    bfm_CoherencyVolumeInfo.pageInfosPageId.pageNo = 2;
    bfm_CoherencyVolumeInfo.pageInfosPageId.volNo = volNo;

    return (eNOERROR);
}

Four BfM_FinalCoherencyInfo(Four handle)
{
    bfm_CoherencyVolumeInfo.volNo = NIL;
    bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp = NIL;
    bfm_CoherencyVolumeInfo.nPageInfos = 0;
    bfm_CoherencyVolumeInfo.headerPageId.pageNo = 0;
    bfm_CoherencyVolumeInfo.headerPageId.volNo = NIL;
    bfm_CoherencyVolumeInfo.pageInfosPageId.pageNo = 0;
    bfm_CoherencyVolumeInfo.pageInfosPageId.volNo = NIL;

    return eNOERROR;
}

Four BfM_SyncBufferUsingLogVolume(
    Four handle,
    Four  				volNo,
    char				*coherencyHeaderPage,
    char				*coherencyPageInfosPage
)
{
    Four                    		e;
    bfm_CoherencyPageInfo_t 		pageInfos[BFM_N_COHERENCY_PAGEINFOS];
    bfm_CoherencyVolumeHeaderPage* 	headerPage;
    bfm_CoherencyVolumePageInfosPage* 	pageInfosPage;
    Four                    		nReturnedPageInfos;
    Four                    		i, index;
    Four                    		type;
#ifdef USE_SHARED_MEMORY_BUFFER		
    Four		    		hostId;
    BfMHashKey		    		localKey;
#endif

    TR_PRINT(TR_BFM, TR1,("BfM_SyncBufferUsingCoherencyVolume(handle)")); 
   
#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Block signals. */
    SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(handle);
#endif 

#ifdef VERBOSE
    printf("Start synchronizing buffer for volume %d with timestamp %ld\n", volNo, bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp);
#endif

    headerPage = (bfm_CoherencyVolumeHeaderPage* ) coherencyHeaderPage;
    pageInfosPage = (bfm_CoherencyVolumePageInfosPage* ) coherencyPageInfosPage; 

#ifdef USE_SHARED_MEMORY_BUFFER		
    hostId = gethostid();
#endif

    e = bfm_ReadCoherencyPageInfos(handle, BFM_N_COHERENCY_PAGEINFOS, headerPage, pageInfosPage, pageInfos, &nReturnedPageInfos);
    if (e < eNOERROR) ERR(handle, e);

    if(nReturnedPageInfos > 0 && bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp != NIL) 
    {   
        if((pageInfos[0].timestamp >= bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp && pageInfos[nReturnedPageInfos - 1].timestamp > bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp ) ||
           (bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp - pageInfos[nReturnedPageInfos - 1].timestamp) > (BFM_COHERENCY_TIMESTAMP_MAX / 2))
        {
#ifdef VERBOSE
            printf("Discard all buffers\n");
            printf("coherency's first timestamp %ld\n", pageInfos[0].timestamp);
            printf("coherency's last  timestamp %ld\n", pageInfos[nReturnedPageInfos - 1].timestamp);
            printf("in memory last timestamp %ld\n", bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp);
#endif
            for(type = 0; type < NUM_BUF_TYPES; type++) 
            {
                for(index = 0; index < BI_NBUFS(type); index++)  
                {
#ifdef USE_SHARED_MEMORY_BUFFER		
		    localKey = BI_KEY(type, index);
                    if(!IS_NILBFMHASHKEY(localKey) && localKey.volNo == volNo) {
			/* Acquire lock of the buffer table entry. */
			ERROR_PASS(bfm_Lock(handle, (TrainID* )&localKey, type));

			if (!EQUALKEY(&BI_KEY(type, i), &localKey)) {
			    /* Release lock of the buffer table entry. */
			    ERROR_PASS(bfm_Unlock(handle, (TrainID* )&localKey, type));
			    continue;
			}
#else
                    if(!IS_NILBFMHASHKEY(BI_KEY(type, index)) && BI_KEY(type, index).volNo == volNo) {
#endif
                        if(BI_BITS(type, index) & DIRTY) {
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
			ERROR_PASS(bfm_Unlock(handle, (TrainID* )&localKey, type));
#endif
                    }
                }
            }
        }
        else
        {
            for(i = nReturnedPageInfos - 1; i >= 0; i--)
            {
                if(pageInfos[i].timestamp <= bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp)
                    break;

#if defined(USE_SHARED_MEMORY_BUFFER)	
		else if(pageInfos[i].pageId.volNo == volNo &&
			(pageInfos[i].hostId != hostId || pageInfos[i].shmBufferId != BFM_PER_PROCESS_DS.shmBufferId) && 
			pageInfos[i].timestamp > bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp)
#else
                else if(pageInfos[i].pageId.volNo == volNo &&
                        pageInfos[i].timestamp > bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp)
#endif
                {
                    for(type = 0; type < NUM_BUF_TYPES; type++) 
                    {
                        if((index = bfm_LookUp(handle, (BfMHashKey *)&pageInfos[i].pageId, type)) != NOTFOUND_IN_HTABLE) 
                        {

#ifdef USE_SHARED_MEMORY_BUFFER		
			    localKey = BI_KEY(type, index);
			    if (localKey.volNo == volNo) {
				/* Acquire lock of the buffer table entry. */
				ERROR_PASS(bfm_Lock(handle, (TrainID* )&localKey, type));

				if (!EQUALKEY(&BI_KEY(type, index), &localKey)) {
				    /* Release lock of the buffer table entry. */
				    ERROR_PASS(bfm_Unlock(handle, (TrainID* )&localKey, type));
				    continue;
				}

#else
                            if(BI_KEY(type, index).volNo == volNo) {
#endif

#ifdef VERBOSE
                                printf("Discard buffer for page (%d, %d)\n", pageInfos[i].pageId.volNo, pageInfos[i].pageId.pageNo);
#endif
                                if(BI_BITS(type, index) & DIRTY) {
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
				ERROR_PASS(bfm_Unlock(handle, (TrainID* )&localKey, type));
#endif
                            }
                        }
                    }
                }
            }
        }
    }

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

Four BfM_UpdateCoherencyPageOfLogVolume(
    Four handle,
     char				*coherencyHeaderPage, 
     char				*coherencyPageInfosPage)
{
    Four    							e;
    Four    							timestamp;
    Four    							i;
    Four    							nMountedVols;
    Four    							mountedVolNos[MAXNUMOFVOLS];
    bfm_CoherencyVolumeHeaderPage		*headerPage;
    bfm_CoherencyVolumePageInfosPage	*pageInfosPage;

    TR_PRINT(TR_BFM, TR1,("BfM_UpdateCoherencyVolume(handle)"));
   

    e = RDsM_GetAllMountedVolNos(handle, mountedVolNos);
    if (e < eNOERROR) ERR(handle, e);
    nMountedVols = e;

    for(i = 0; i < nMountedVols; i++)
    {
        if(RDsM_GetVolumeLockMode(handle, mountedVolNos[i]) == L_NL &&
           !IS_TEMP_VOLUME(mountedVolNos[i]) &&
           !RM_IsLogVolume(handle, mountedVolNos[i]))
        {
#ifdef VERBOSE
            printf("Sync volume %ld\n", mountedVolNos[i]);
#endif
            e = BfM_SyncBufferUsingLogVolume(handle, mountedVolNos[i], coherencyHeaderPage, coherencyPageInfosPage);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

#ifdef VERBOSE
    printf("Start updating coherency volume\n");
#endif

    headerPage = (bfm_CoherencyVolumeHeaderPage* ) coherencyHeaderPage;
    pageInfosPage = (bfm_CoherencyVolumePageInfosPage* ) coherencyPageInfosPage; 

    timestamp = headerPage->header.timestamp;
    bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp = timestamp;
    timestamp = (timestamp + 1) % BFM_COHERENCY_TIMESTAMP_MAX;
    headerPage->header.timestamp = timestamp;

#ifdef VERBOSE
    printf("Get and set new timestamp %ld\n", timestamp);
#endif

    e = bfm_FlushCoherencyPageInfos(handle, bfm_CoherencyVolumeInfo.lastCoherencyUpdateTimestamp, headerPage, pageInfosPage);
    if (e < eNOERROR) ERR(handle, e);

    e = bfm_InitCoherencyPageInfos(handle);
    if (e < eNOERROR) ERR(handle, e);

#ifdef VERBOSE
    printf("Done for updating coherency volume\n");
#endif

    return eNOERROR;
}

Four bfm_ReadCoherencyPageInfos(
    Four handle,
    Four                    			sizeOfPageInfos,    
    bfm_CoherencyVolumeHeaderPage* 		headerPage,
    bfm_CoherencyVolumePageInfosPage* 	pageInfosPage,
    bfm_CoherencyPageInfo_t*    		pageInfos,          
    Four*                   			nReturnedPageInfos  
)
{
    Four                            	e;
    Four                            	nTransferred;
    Four                            	i;

    TR_PRINT(TR_BFM, TR1,("bfm_ReadCoherencyPageInfos(handle)"));

    if(headerPage->header.nPageInfos > 0)
    {
        if(headerPage->header.circularListHead <= headerPage->header.circularListTail)
        {
            nTransferred = headerPage->header.circularListTail - headerPage->header.circularListHead + 1;
            if(nTransferred > sizeOfPageInfos)  
                nTransferred = sizeOfPageInfos;
            if(nTransferred > 0)
            {
                memcpy(&pageInfos[0], &(pageInfosPage->pageInfos[headerPage->header.circularListHead]),
                       nTransferred * sizeof(bfm_CoherencyPageInfo_t));
            }

            *nReturnedPageInfos = nTransferred;
        }
        else
        {
            nTransferred = headerPage->header.circularListSize - headerPage->header.circularListHead;
            if(nTransferred > sizeOfPageInfos)  
                nTransferred = sizeOfPageInfos;
            if(nTransferred > 0)
            {
                memcpy(&pageInfos[0], &(pageInfosPage->pageInfos[headerPage->header.circularListHead]),
                       nTransferred * sizeof(bfm_CoherencyPageInfo_t));
            }
            *nReturnedPageInfos =  nTransferred;
            sizeOfPageInfos     -= nTransferred;

            nTransferred = headerPage->header.circularListTail - 0 + 1;
            if(nTransferred > sizeOfPageInfos)  
                nTransferred = sizeOfPageInfos;
            if(nTransferred > 0)
            {
                memcpy(&pageInfos[*nReturnedPageInfos], &(pageInfosPage->pageInfos[0]),
                       nTransferred * sizeof(bfm_CoherencyPageInfo_t));
            }
            *nReturnedPageInfos +=  nTransferred;
            sizeOfPageInfos     -= nTransferred;
        }
    }
    else
        *nReturnedPageInfos = 0;

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

Four bfm_AppendCoherencyPageInfos(
    Four handle,
    bfm_CoherencyPageInfo_t*    pageInfo
)
{
    Four e;
    
    TR_PRINT(TR_BFM, TR1,("bfm_AppendCoherencyPageInfos(handle)"));

    if(bfm_CoherencyVolumeInfo.nPageInfos >= BFM_N_COHERENCY_PAGEINFOS)
    {
        memcpy(&bfm_CoherencyVolumeInfo.pageInfos[0], 
               &bfm_CoherencyVolumeInfo.pageInfos[1], 
               sizeof(bfm_CoherencyPageInfo_t) * (BFM_N_COHERENCY_PAGEINFOS - 1));

        memcpy(&bfm_CoherencyVolumeInfo.pageInfos[BFM_N_COHERENCY_PAGEINFOS - 1],
               pageInfo, sizeof(bfm_CoherencyPageInfo_t));
        bfm_CoherencyVolumeInfo.pageInfos[BFM_N_COHERENCY_PAGEINFOS - 1].timestamp = NIL;

        bfm_CoherencyVolumeInfo.nPageInfos = BFM_N_COHERENCY_PAGEINFOS;
    }
    else
    {
        memcpy(&bfm_CoherencyVolumeInfo.pageInfos[bfm_CoherencyVolumeInfo.nPageInfos],
               pageInfo, sizeof(bfm_CoherencyPageInfo_t));
        bfm_CoherencyVolumeInfo.pageInfos[bfm_CoherencyVolumeInfo.nPageInfos].timestamp = NIL;
        bfm_CoherencyVolumeInfo.nPageInfos ++;
    }

    return eNOERROR;
}

Four bfm_FlushCoherencyPageInfos(
    Four handle,
    Four 								timestamp,                  
    bfm_CoherencyVolumeHeaderPage		*headerPage,
    bfm_CoherencyVolumePageInfosPage	*pageInfosPage
)
{
    Four                            	e;
    Four                            	i;

    TR_PRINT(TR_BFM, TR1,("bfm_FlushCoherencyPageInfos(handle)"));

   
    /* If there are nothing to update, then return */
    if(bfm_CoherencyVolumeInfo.nPageInfos == 0) return eNOERROR;

#ifdef VERBOSE
    printf("Flushing %ld page infos\n", bfm_CoherencyVolumeInfo.nPageInfos);
    printf("Circular list (head=%ld, tail=%ld)\n", headerPage.header.circularListHead, headerPage.header.circularListTail);
#endif

#ifdef VERBOSE
    printf("page list [");
#endif
    for(i = 0; i < bfm_CoherencyVolumeInfo.nPageInfos; i++)
    {
#ifdef VERBOSE
        printf("(%ld,%ld)", bfm_CoherencyVolumeInfo.pageInfos[i].pageId.volNo, bfm_CoherencyVolumeInfo.pageInfos[i].pageId.pageNo);
#endif
        if(headerPage->header.nPageInfos >= headerPage->header.circularListSize)
        {
            headerPage->header.circularListHead = (headerPage->header.circularListHead + 1) % headerPage->header.circularListSize;
            headerPage->header.circularListTail = (headerPage->header.circularListTail + 1) % headerPage->header.circularListSize;
            memcpy(&(pageInfosPage->pageInfos[headerPage->header.circularListTail]),
                   &bfm_CoherencyVolumeInfo.pageInfos[i], sizeof(bfm_CoherencyPageInfo_t));
            pageInfosPage->pageInfos[headerPage->header.circularListTail].timestamp = timestamp;
        }
        else
        {
            if(headerPage->header.nPageInfos == 0)
            {
                headerPage->header.circularListHead = 0;
                headerPage->header.circularListTail = 0;
            }
            else
                headerPage->header.circularListTail = (headerPage->header.circularListTail + 1) % headerPage->header.circularListSize;

            memcpy(&(pageInfosPage->pageInfos[headerPage->header.circularListTail]),
                   &bfm_CoherencyVolumeInfo.pageInfos[i], sizeof(bfm_CoherencyPageInfo_t));
            pageInfosPage->pageInfos[headerPage->header.circularListTail].timestamp = timestamp;

            headerPage->header.nPageInfos ++;
        }
    }
#ifdef VERBOSE
    printf("]\n");
#endif

#ifdef VERBOSE
    printf("Circular list (head=%ld, tail=%ld)\n", headerPage->header.circularListHead, headerPage->header.circularListTail);
#endif

    return eNOERROR;
}

Four bfm_InitCoherencyPageInfos(Four handle)
{
    TR_PRINT(TR_BFM, TR1,("bfm_InitCoherencyPageInfos(handle)"));


    bfm_CoherencyVolumeInfo.nPageInfos = 0;

    return eNOERROR;
}

#endif 
