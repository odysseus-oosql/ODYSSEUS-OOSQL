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
 * Module: bfm_FlushTrains.c
 *
 * Description : 
 *  Write multiple trains in the buffer
 *
 * Exports:
 *  Four bfm_FlushTrains(Four)
 */

#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "RM.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

/* move the following definitions to the perThreadDS.h */

/* local functions */
static Four bfm_BulkFlushTrainsToLog(Four, Four, Four*, Four);
static Four bfm_BulkFlushTrainsToRaw(Four, Four, Four*, Four);
static Four bfm_GetWorkingSpaceForBulkFlushTrainsToLog(Four, char**, TrainID**);
static Four bfm_GetWorkingSpaceForBulkFlushTrainsToRaw(Four, char**, bfm_PageIdSortElement**);
static Four bfm_GetWorkingSpaceForBufferIndexes(Four, Four**, Four**);

/*@================================
 * bfm_FlushTrains()
 *================================*/
/*
 * Function: Four bfm_FlushTrains(Four) 
 *
 * Description : 
 *  �־��� type(PAGE_BUF Ȥ�� TRAIN_BUF) �� ���ϴ� ������ �߿���, fixed�� 0�̰� dirty �� �������� 
 *  ��� disk�� flush out��Ų��. �̶�, �ִ��� sequential disk access�� �߻��ϵ��� �Ͽ�
 *  ���� �ð��ȿ�, �־��� �۾��� ��ĥ �� �ֵ��� �Ѵ�. �� �Լ��� append bulkloading�� ���� ����Ÿ���̽���
 *  ���������� �����ϸ鼭 �� ������ �����ϰ��� �Ҷ�, �ſ� ȿ�����̴�.
 *
 *  Flush out all buffer pages which are unfixed(fixed value is 0) and dirty in a given type.
 *  When flush out buffer pages, try to write them to disk sequentially as possible as it is to reduce disk I/O time.
 *  This function is useful to update database sequentially like append bulkloading.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four bfm_FlushTrains(
    Four handle,
    Four    		type			/* IN buffer type */
)
{
    Four		e;						/* error code */
    UFour       i;                      /* loop index */
    Four        *logBulkFlushes;                
    Four        logBulkFlushesSize;             
    Four        nLogBulkFlushes;                
    Four        *rawBulkFlushes;                
    Four        rawBulkFlushesSize;             
    Four        nRawBulkFlushes;                
    Four        victim;                         
    TrainID		*trainId;
#ifdef USE_SHARED_MEMORY_BUFFER         
    LockMode    volumeLockMode;         /* volume lock mode */
#endif

    /* �־��� Ÿ���� ���۸� �Ѳ����� flushout�Ѵ�. */
    /* flush out�Ҷ�, logging�� �Ǵ� �������� ���ؼ��� ��������� ���� �� �ִ�. */
    /* logging�� ���� �ʴ� �������� ���ؼ���, ���� �̵��� ���������� ��ġ�Ǿ� �ִٸ�, ��������� ���� �� �ִ�. */
    /* Flush out buffer in a given type at a time. */
    /* When flush out, the performace is improved to the pages that are logged. */ 
    /* And even though the pages are not logged, the performance is improved if the pages are contiguous. */
    
    TR_PRINT(TR_BFM, TR1, ("bfm_FlushTrains(handle, type=%ld)", type));

    /* �־��� Ÿ���� ���۸� �����鼭, fixed�� 0�̰�, dirty�� �������� ã�´�. */
    /* ���� �־��� �������� logging�� �Ǵ� ���������, �̸� ��� �ξ��ٰ� �Ѳ����� flush out�� �ϰ�, */
    /* logging�� ���� �ʴ� ���������, ���� �̸� ��� �ξ��ٰ� pageid ������ sort�Ͽ� ���ӵǰ� �̸� flush out�Ѵ�. */
    /* While reading buffer in a given type, find out the buffer pages which are unfixed and dirty. */
    /* If the pages have to be logged, gather such pages and then, flush them out to disk at a time. */ 
    /* If the pages don't have to be logged, gather such pages and then, sort them by pageid and flush out them contiguously. */ 
    e = bfm_GetWorkingSpaceForBufferIndexes(handle, &logBulkFlushes, &rawBulkFlushes);
    if (e < 0) ERR(handle, e);

    logBulkFlushesSize = BFM_BULKFLUSH_MAX_BUFFER_SIZE;
    rawBulkFlushesSize = BFM_BULKFLUSH_MAX_BUFFER_SIZE;
    nLogBulkFlushes    = 0;
    nRawBulkFlushes    = 0;
    victim             = BI_NEXTVICTIM(type) % BI_NBUFS(type);	

    for(i = 0; i < BI_NBUFS(type); i++, victim = (victim + 1) % BI_NBUFS(type))
    {
	trainId = (TrainID*)&BI_KEY(type, victim);

#ifdef USE_SHARED_MEMORY_BUFFER 
	/* flush out buffers in mounted volumes */
	/* If use shared memory buffer, must flush buffer page in mounted volume
	 * because pages in not-mounted volume may be in shared memory buffer.
	 */
#ifdef DB_LOCK
	volumeLockMode = RDsM_GetVolumeLockMode(handle, BI_KEY(type, victim).volNo);
	if(BI_NFIXED(type, victim) <= 0 && BI_BITS(type, victim) & DIRTY && 
	   bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, BFM_PER_THREAD_DS(handle).nMountedVols, 
	   &BI_KEY(type, victim)) && (volumeLockMode == L_X || volumeLockMode == L_IX || volumeLockMode == L_SIX))
#else
	if(BI_NFIXED(type, victim) <= 0 && BI_BITS(type, victim) & DIRTY && 
	   bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, BFM_PER_THREAD_DS(handle).nMountedVols, 
	   &BI_KEY(type, victim)))
#endif
#else
	if(BI_NFIXED(type, victim) <= 0 && BI_BITS(type, victim) & DIRTY) 
#endif
	{
	    if (!IS_TEMP_VOLUME(trainId->volNo) && RM_IS_ROLLBACK_REQUIRED(handle) && !IS_TEMP_PAGE(BI_BUFFER(type, victim)) && !(BI_BITS(type, victim) & NEW))
	    {
		/* logging�� �ʿ��� ��� */
	    /* When logging is necessary */
		logBulkFlushes[nLogBulkFlushes] = victim;
		nLogBulkFlushes++;
	    }
	    else
	    {
		/* logging�� �ʿ�ġ ���� ��� */
        /* When logging is not necessary */
		rawBulkFlushes[nRawBulkFlushes] = victim;
		nRawBulkFlushes++;
	    }
	}
    }

    /* ������ flush out�Ѵ�. */
    /* Flush out buffer */
    e = bfm_BulkFlushTrainsToLog(handle, type, logBulkFlushes, nLogBulkFlushes);
    if (e < 0) ERR(handle, e);

    e = bfm_BulkFlushTrainsToRaw(handle, type, rawBulkFlushes, nRawBulkFlushes);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}

/*@================================
 * bfm_BulkFlushTrainsToLog()
 *================================*/
/*
 * Function: Four bfm_BulkFlushTrainsToLog(Four, Two*, Two)
 *
 * Description : 
 *  logBulkFlushes array�� ���� �־��� buffer page���� �Ѳ����� log�� flush out�Ѵ�.
 *  �̶�, �ִ��� sequential disk access�� �߻��ϵ��� �Ͽ�
 *  ���� �ð��ȿ�, �־��� �۾��� ��ĥ �� �ֵ��� �Ѵ�. 
 * 
 *  Flush out buffer in logBulkFlushes array at a time.
 *  At this time, write buffer to disk as sequentially as possible to reduce the disk I/O.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
static Four bfm_BulkFlushTrainsToLog(
    Four handle,
    Four                        type,                   /* IN buffer type */
    Four                        *logBulkFlushes,        /* IN array of buffer indexes to flush */
    Four                        nLogBulkFlushes         /* IN # of index in logBulkFlushes */
)
{
    Four                        e;                      
    Four                        i;                      
    Four                        index;                  
    Four                        width, width2;          
    TrainID                     *trainIds;
    char                        *flushBuffer;
    char                        *flushBufferPtr;
    Four                        nFlushBuffers;          
    Four                        currentFlushTrainIdPos; 

    if(nLogBulkFlushes == 0)
	return eNOERROR;

    if(type == PAGE_BUF)
    {
	width  = PAGESIZE;
	width2 = PAGESIZE2;
    }
    else
    {
	width  = TRAINSIZE;
	width2 = TRAINSIZE2;
    }

    e = bfm_GetWorkingSpaceForBulkFlushTrainsToLog(handle, &flushBuffer, &trainIds);
    if(e < eNOERROR) ERR(handle, e);

    /* �־��� ������ ���ӵ� �޸� ���ۿ� ��ġ�Ѵ�. */
    /* Layout the given contents in contiguous memory buffer. */
    flushBufferPtr         = flushBuffer;
    currentFlushTrainIdPos = 0;
    nFlushBuffers          = 0;
    for(i = 0; i < nLogBulkFlushes; i++)
    {
	index = logBulkFlushes[i];

	memcpy(flushBufferPtr, BI_BUFFER(type, index), width);
	trainIds[i] = *(TrainID*)&BI_KEY(type, index);
	flushBufferPtr += width;
	nFlushBuffers ++;

	if(nFlushBuffers >= BFM_BULKFLUSH_DISKWRITE_SIZE)
	{
	    /* ������� ������ ������� RM_SaveTrains�� ȣ���Ѵ�. */
        /* Call RM_SaveTrains with contiguous memory buffer. */
	    e = RM_SaveTrains(handle, flushBuffer, &trainIds[currentFlushTrainIdPos], nFlushBuffers, width2);
	    if(e < 0) ERR(handle, e);

	    flushBufferPtr         =  flushBuffer;
	    currentFlushTrainIdPos += nFlushBuffers;
	    nFlushBuffers          =  0;
	}
    }

    if(nFlushBuffers > 0)
    {
	/* ������� ������ ������� RM_SaveTrains�� ȣ���Ѵ�. */
    /* Call RM_SaveTrains with contiguous memory buffer. */
	e = RM_SaveTrains(handle, flushBuffer, &trainIds[currentFlushTrainIdPos], nFlushBuffers, width2);
	if(e < 0) ERR(handle, e);
    }

    /* ��� ������ flush out�Ǿ�����, �ش� buffer�� ���¸� �ʱ���·� �ٲ۴�. */
    /* After flush out all buffer pages, initialize status of the buffer. */
    for(i = 0; i < nLogBulkFlushes; i++)
    {
	index = logBulkFlushes[i];

	if(!IS_NILBFMHASHKEY(BI_KEY(type, index))) 
	{
	    e = bfm_Delete(handle, &BI_KEY(type, index), type);
	    if(e < 0) ERR(handle, e);
	}

	BI_BITS(type, index)  = ALL_0;
    BI_NFIXED(type, index) = 0;
	SET_NILBFMHASHKEY(BI_KEY(type, index));
    }

    return eNOERROR;
}

/*@================================
 * bfm_PageIdCompare()
 *================================*/
/*
 * Function: Four bfm_PageIdCompare(void*, void*)
 *
 * Description : 
 *  �ΰ��� �־��� pageid�� �����Ѵ�. bfm_SortFlushPages���� sorting�ÿ� ���ȴ�. 
 *
 *  Compare with two given pageId. It is used in bfm_SortFlushPages for sorting.  
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
static int bfm_PageIdCompare(const void* page1, const void* page2)
{
    PageID  pageId1;
    PageID  pageId2;

    pageId1 = *((PageID*)(page1));
    pageId2 = *((PageID*)(page2));

    if(pageId1.volNo > pageId2.volNo)
	return 1;
    else if(pageId1.volNo < pageId2.volNo)
	return -1;
    else
    {
	if(pageId1.pageNo > pageId2.pageNo)
	    return 1;
	else if(pageId1.pageNo < pageId2.pageNo)
	    return -1;
	else
	    return 0;
    }
}

/*@================================
 * bfm_SequentialWritingToRaw()
 *================================*/
/*
 * Function: Four bfm_SequentialWritingToRaw(char*, Two, Two)
 *
 * Description : 
 *  �־��� ���������� raw disk�� sequential writing�Ѵ�.
 * 
 *  Write given pages to raw disk sequentially.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
static Four bfm_SequentialWritingToRaw(
    Four handle,
    char*		flushBuffer, 
    Four        numTrains,         /* IN number of trains to write */
    Four        sizeOfTrain        /* IN the size of a train in pages */
)
{
    Four	flushPageNo;			/* commitBuffer������ flush�߿� �ִ� ������ ��ȣ�� ��Ī�� */
                                	/* indicate the pageNo of the page that is flushing  in commitBuffer*/
    Four	nSequentialFlushPages;  /* commitBuffer������ ���ӵ� �������� �� */
                                    /* the number of pages which are contiguos in commitBuffer */
    char*	flushPagePtr;			/* commitBuffer������ flush�߿� �ִ� ������ */
                                    /* indicate the page that is flushing in commitBuffer */
    PageID	dataPid;				/* page id of the updated page */
    PageID	nextDataPid;			/* page id of the next updated page */
    Four	width;
    Four	e;
    Four	i;
    Four	numPages;

    if(sizeOfTrain == PAGESIZE2)
	width = PAGESIZE;
    else
	width = TRAINSIZE;

    numPages = numTrains * sizeOfTrain;

    /* �־��� �����߿��� �ִ����� ���ӵ� ������ ���� disk writing�� �õ��Ѵ�. */
    /* write the buffer pages that are contiguous to disk at a time. */
    for(flushPageNo = 0; flushPageNo < numPages; /* loop counter is in the end of loop */)
    {
	flushPagePtr = flushBuffer + PAGESIZE * flushPageNo;
	
	/* All page have the page id on the beginning of the page. */
	dataPid = *((PageID*)flushPagePtr);
    
	/* check this page is nil */
	if (IS_NILPAGEID(dataPid))
	{
	    flushPageNo += sizeOfTrain;
	    continue;
	}

	/* nSequentialFlushPages �� �����Ѵ�. */
    /* Calculate nSequentialFlushPages. */
	for(i = flushPageNo + sizeOfTrain, nSequentialFlushPages = sizeOfTrain; i < numPages; i += sizeOfTrain)
	{
	    nextDataPid = *((PageID*)(flushBuffer + PAGESIZE * i));
	    if(dataPid.volNo == nextDataPid.volNo && 
	       dataPid.pageNo + nSequentialFlushPages == nextDataPid.pageNo)	/* If it is a contiguous page*/
	    {
		/* Increase the nSequentialFlushPages. */
		nSequentialFlushPages += sizeOfTrain;
	    }
	    else
		break;
	}

	/* Write buffers sequentially. */
	e = RDsM_WriteTrains(handle, (PageType *)flushPagePtr, &dataPid, nSequentialFlushPages / sizeOfTrain, sizeOfTrain);
	if (e < eNOERROR) {
	    fprintf(stderr, "dataPid.pageNo: %ld\n", dataPid.pageNo);
	    fprintf(stderr, "dataPid.volNo: %ld\n", dataPid.volNo);
	    ERR(handle, e); 
	}

	flushPageNo += nSequentialFlushPages;
    }

    return eNOERROR;
}

/*@================================
 * bfm_BulkFlushTrainsToRaw()
 *================================*/
/*
 * Function: Four bfm_BulkFlushTrainsToLog(Four, Two*, Two)
 *
 * Description : 
 *  rawBulkFlushes array�� ���� �־��� buffer page���� �Ѳ����� raw disk�� flush out�Ѵ�.
 *  �̶�, �ִ��� sequential disk access�� �߻��ϵ��� �Ͽ�
 *  ���� �ð��ȿ�, �־��� �۾��� ��ĥ �� �ֵ��� �Ѵ�. 
 *
 *  Flush out buffer pages that is given by logBulkFlushes array to the raw disk at a time.
 *  At this time, write buffer pages to disk as sequentially as possible to reduce the disk I/O.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
static Four bfm_BulkFlushTrainsToRaw(
    Four handle,
    Four                    type,               /* IN buffer type */
    Four                    *rawBulkFlushes,    /* IN array of buffer indexes to flush */
    Four                    nRawBulkFlushes     /* IN # of index in rawBulkFlushes */
)
{
    Four                    e;                  
    Four                    i;                  
    Four                    index;              
    Four                    width, width2;      
    Four                    nFlushBuffers;      
    char*                   flushBufferPtr;
    char*                   flushBuffer;
    bfm_PageIdSortElement*  pageIdSortBuffer;

    if(nRawBulkFlushes == 0)
	return eNOERROR;

    if(type == PAGE_BUF)
    {
	width  = PAGESIZE;
	width2 = PAGESIZE2;
    }
    else
    {
	width  = TRAINSIZE;
	width2 = TRAINSIZE2;
    }
    
    e = bfm_GetWorkingSpaceForBulkFlushTrainsToRaw(handle, &flushBuffer, &pageIdSortBuffer);
    if(e < 0) ERR(handle, e);

    /* �־��� ������ ������ �о� ���, �̸� page id������ �����Ѵ�. */
    /* Read buffers and sort them by page id. */
    for(i = 0; i < nRawBulkFlushes; i++)
    {
	index = rawBulkFlushes[i];
	pageIdSortBuffer[i].pageId = *(PageID*)&BI_KEY(type, index);
	pageIdSortBuffer[i].index  = index;
    }

    qsort(pageIdSortBuffer, nRawBulkFlushes, sizeof(bfm_PageIdSortElement), bfm_PageIdCompare);

    /* pageIdSortBuffer�� ��� �ִ� ������ �������, �� ������ �о� ����. */
    /* Read buffers in order of pageIdSortBuffer and write them to disk. */
    flushBufferPtr = flushBuffer;
    nFlushBuffers  = 0;
    for(i = 0; i < nRawBulkFlushes; i++)
    {
	index = pageIdSortBuffer[i].index;

	memcpy(flushBufferPtr, BI_BUFFER(type, index), width);
	flushBufferPtr += width;
	nFlushBuffers ++;

	if(nFlushBuffers >= BFM_BULKFLUSH_DISKWRITE_SIZE)
	{
	    e = bfm_SequentialWritingToRaw(handle, flushBuffer, nFlushBuffers, width2);
	    if(e < 0) ERR(handle, e);

	    flushBufferPtr = flushBuffer;
	    nFlushBuffers  = 0;
	}
    }

    if(nFlushBuffers > 0)
    {
	e = bfm_SequentialWritingToRaw(handle, flushBuffer, nFlushBuffers, width2);
	if(e < 0) ERR(handle, e);
    }

    /* ��� ������ flush out�Ǿ�����, �ش� buffer�� ���¸� �ʱ���·� �ٲ۴�. */
    /* After flush out all buffers, initialize status of the buffers. */
    for(i = 0; i < nRawBulkFlushes; i++)
    {
	index = rawBulkFlushes[i];

	if(!IS_NILBFMHASHKEY(BI_KEY(type, index))) 
	{
	    e = bfm_Delete(handle, &BI_KEY(type, index), type);
	    if(e < 0) ERR(handle, e);
	}

	BI_BITS(type, index)  = ALL_0;
    BI_NFIXED(type, index) = 0;
	SET_NILBFMHASHKEY(BI_KEY(type, index));
    }

    return eNOERROR;
}

Four bfm_GetWorkingSpaceForBufferIndexes(
    Four handle,
    Four	**logBulkFlushes,
    Four	**rawBulkFlushes
)
{
    if(BFM_PER_THREAD_DS(handle).bfm_logBulkFlushesMemory == NULL)
    {
	BFM_PER_THREAD_DS(handle).bfm_logBulkFlushesMemory = (Four*)malloc(sizeof(Four) * BFM_BULKFLUSH_MAX_BUFFER_SIZE);
	if(BFM_PER_THREAD_DS(handle).bfm_logBulkFlushesMemory == NULL) ERR(handle, eMEMORYALLOCERR);
    }
    if(BFM_PER_THREAD_DS(handle).bfm_rawBulkFlushesMemory == NULL)
    {
	BFM_PER_THREAD_DS(handle).bfm_rawBulkFlushesMemory = (Four*)malloc(sizeof(Four) * BFM_BULKFLUSH_MAX_BUFFER_SIZE);
	if(BFM_PER_THREAD_DS(handle).bfm_rawBulkFlushesMemory == NULL) ERR(handle, eMEMORYALLOCERR);
    }

    *logBulkFlushes = BFM_PER_THREAD_DS(handle).bfm_logBulkFlushesMemory;
    *rawBulkFlushes = BFM_PER_THREAD_DS(handle).bfm_rawBulkFlushesMemory;

    return eNOERROR;
}

Four bfm_GetWorkingSpaceForBulkFlushTrainsToLog(
    Four handle,
    char**		flushBuffer, 
    TrainID**	trainIds
)
{
    if(BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory == NULL)
    {
	BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory = (char*)malloc(TRAINSIZE * BFM_BULKFLUSH_DISKWRITE_SIZE);
	if(BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory == NULL) ERR(handle, eMEMORYALLOCERR);
    }
    if(BFM_PER_THREAD_DS(handle).bfm_trainIdsMemory == NULL)
    {
	BFM_PER_THREAD_DS(handle).bfm_trainIdsMemory = (TrainID*)malloc(sizeof(TrainID) * BFM_BULKFLUSH_MAX_BUFFER_SIZE);
	if(BFM_PER_THREAD_DS(handle).bfm_trainIdsMemory == NULL) ERR(handle, eMEMORYALLOCERR);
    }

    *flushBuffer = BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory;
    *trainIds    = BFM_PER_THREAD_DS(handle).bfm_trainIdsMemory;

    return eNOERROR;
}

Four bfm_GetWorkingSpaceForBulkFlushTrainsToRaw(
    Four handle,
    char**						flushBuffer, 
    bfm_PageIdSortElement**		pageIdSortBuffer
)
{
    if(BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory == NULL)
    {
	BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory = (char*)malloc(TRAINSIZE * BFM_BULKFLUSH_DISKWRITE_SIZE);
	if(BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory == NULL) ERR(handle, eMEMORYALLOCERR);
    }
    if(BFM_PER_THREAD_DS(handle).bfm_pageIdSortBufferMemory == NULL)
    {
	BFM_PER_THREAD_DS(handle).bfm_pageIdSortBufferMemory = (bfm_PageIdSortElement*)malloc(sizeof(bfm_PageIdSortElement) * BFM_BULKFLUSH_MAX_BUFFER_SIZE);
	if(BFM_PER_THREAD_DS(handle).bfm_pageIdSortBufferMemory == NULL) ERR(handle, eMEMORYALLOCERR);
    }

    *flushBuffer      = BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory;
    *pageIdSortBuffer = BFM_PER_THREAD_DS(handle).bfm_pageIdSortBufferMemory;

    return eNOERROR;
}
