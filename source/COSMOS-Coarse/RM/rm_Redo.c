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
#include <assert.h>
#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "RM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

static Four rm_RedoWithCommitBuffer(Four, Four, char*, char*, char*, Four);
static Four rm_RedoWithoutCommitBuffer(Four, Four);

typedef struct {
    PageID	pageId;
    Four	index;
} rm_PageIdSortElement;


/*@================================
 * rm_Redo()
 *================================*/
/*
 * Function: Four rm_Redo(void)
 *
 * Description:
 *  Redo the update of the committed transaction. Actually we save the
 *  updated pages.
 *
 * Returns:
 *  error code
 */
Four rm_Redo(Four handle)
{
    Four    e;			    /* error code */
    Four    verboseFlag;	    /* debugging을 위해 commit progress를 화면에 출력할 지 여부를 나타내는 flag, 
                                       COSMOS_VERBOSE 라는 환경변수가 정의되어 있으면 1을 갖는다. */
                                    /* a flag to denote whether print a commit progress or not for debugging. 
                                       If COSMOS_VERBOSE environment variable is set, value of the flag is set to 1. */
    char    *commitBuffer;	    /* buffer to commit */
    char    *tempCommitBuffer;	    /* temporary buffer to rearrange the buffer that is sorted to commit */
    char    *pageIdSortBuffer;	    /* buffer to sort page ID */ 

    TR_PRINT(TR_RM, TR1, ("rm_Redo(handle)"));

    if(getenv("COSMOS_VERBOSE") != NULL)
	verboseFlag = TRUE;
    else
	verboseFlag = FALSE;

    commitBuffer     = (char*)malloc(RM_REDO_BUFFER_SIZE * PAGESIZE * 2 + RM_REDO_BUFFER_SIZE * sizeof(rm_PageIdSortElement));
    tempCommitBuffer = commitBuffer + RM_REDO_BUFFER_SIZE * PAGESIZE;
    pageIdSortBuffer = commitBuffer + RM_REDO_BUFFER_SIZE * PAGESIZE * 2;
    if(commitBuffer)
    {
	e = rm_RedoWithCommitBuffer(handle, verboseFlag, pageIdSortBuffer, commitBuffer, tempCommitBuffer, RM_REDO_BUFFER_SIZE);
	if (e < eNOERROR) 
	{
	    if(commitBuffer) free(commitBuffer);
	    ERR(handle, e);
	}
    }
    else
    {
	e = rm_RedoWithoutCommitBuffer(handle, verboseFlag);
	if (e < eNOERROR) 
	{
	    if(commitBuffer) free(commitBuffer);
	    ERR(handle, e);
	}
    }

    if(commitBuffer) free(commitBuffer);
    
    return(eNOERROR);

} /* rm_Redo() */


/*@================================
 * rm_PageIdCompare()
 *================================*/
/*
 * Function: Four rm_PageIdCompare(void*, void*)
 *
 * Description:
 *  주어진 두 pageid를 비교한다. rm_SortCommitPages에서 주어진 page들을 pageid순으로 sorting하기 위해 사용된다.
 *
 *  Compare with two page IDs. It is used to sort pages in rm_SortCommitPages in order of page ID.
 *
 * Returns:
 *  error code
 */
static int rm_PageIdCompare(const void* page1, const void* page2)
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
 * rm_SortCommitPages()
 *================================*/
/*
 * Function: void rm_SortCommitPages(char*, char*, char*, Four, Two)
 *
 * Description:
 *  Sort given pages in order of pageid.
 *
 * Returns:
 *  error code
 */
static void rm_SortCommitPages(
    Four			handle, 
    char                        *pageIdSortBuffer,      /* page id sort buffer */
    char                        *commitBuffer,          /* commit buffer */
    char                        *tempCommitBuffer,      /* temporary commit buffer for reordering operation */
    Four                        nElements,              /* number of elements */
    Two                         elementWidth            /* element width */
)
{
    rm_PageIdSortElement	*pageIdSortElementPtr;	/* pointer to pageIdSortElement */
    PageID			*commitPageIdPtr;	/* pointer to page ID of commit page */
    char			*commitPagePtr;		/* pointer to commit page */
    char			*tempCommitPagePtr;	/* pointer to temporary commit page */
    Four			i;

    /* 
       commitBuffer내에 들어 있는 내용을 sorting한다. page내용 자체를 sort하여도 되지만, 그럴경우, memcpy되는
       내용이 너무 커지기 때문에, pageid만을 따로 때어 내어 이를 sorting한다. 
       이와 같은 처리를 하면, memcpy가 nlogn걸리는 것이 2n으로 바뀐다. 
       n이 100이라고 가정하는 경우 nlogn = 900, 2n = 200 이기 때문에 차이가 많이 나는 연산이다. 
    */
    /* 
       Sort pages of commitBuffer. Instead of sorting all contents of the pages, gather only page IDs and then, 
       sort only page IDs of the pages for reducing data size to memcpy.
       With the sort method, it can reduce time complexity of memcpy from nlogn to 2n. For example,
       assume that n is 100, reduce 700 times of copy(900(nlogn) - to 200(2n)).
    */
    /* commitBuffer에 들어 있는 page id를 pageIdSortBuffer에 넣는다. */
    /* put page IDs of commit buffer to pageIdSortBuffer. */
    for(i = 0; i < nElements; i++)
    {
	commitPageIdPtr	     = (PageID*)(commitBuffer + i * elementWidth);
	pageIdSortElementPtr = (rm_PageIdSortElement*)(pageIdSortBuffer + i * sizeof(rm_PageIdSortElement));

	pageIdSortElementPtr->pageId = *commitPageIdPtr;
	pageIdSortElementPtr->index  = i;
    }

    /* pageIdSortBuffer에 있는 내용을 sorting 한다. */
    /* Sort page IDs of pageIdSortBuffer. */
    qsort(pageIdSortBuffer, nElements, sizeof(rm_PageIdSortElement), rm_PageIdCompare);

    /* pageIdSortBuffer에 있는 순서대로 commitBuffer에 있는 내용을 재배치한다. */
    /* Rearrange pages in commitBuffer in order of page IDs in pageIsSortBuffer. */
    for(i = 0; i < nElements; i++)
    {
	pageIdSortElementPtr = (rm_PageIdSortElement*)(pageIdSortBuffer + i * sizeof(rm_PageIdSortElement));

	/* i번째, pageIdSortElement의 index번째의 PAGE를 i번째로 만든다. */
        /* Locate the index-th page of i-th pageIdSortElement at i-th page of tempCommitBuffer. */
	pageIdSortElementPtr->index;

	commitPagePtr = commitBuffer + pageIdSortElementPtr->index * elementWidth;
	tempCommitPagePtr = tempCommitBuffer + i * elementWidth;

	memcpy(tempCommitPagePtr, commitPagePtr, elementWidth);
    }

    /* 재배치된 내용을 commitBuffer에 반영한다. */
    /* Copy rearranged buffer to commitBuffer. */
    memcpy(commitBuffer, tempCommitBuffer, elementWidth * nElements);
}

/*@================================
 * rm_RedoWithCommitBuffer()
 *================================*/
/*
 * Function: Four rm_RedoWithCommitBuffer(void)
 *
 * Description:
 *  Redo the update of the committed transaction. Actually we save the
 *  updated pages.
 *  복수의 commit buffer를 사용한 commit 방법으로, 대부분의 연산이 sequential access이기 때문에 매우 빠르다.
 *
 *  Because it uses commit method with two commit buffers, almost operations can write data sequentialy.
 *  So it can achieve better performance.
 *
 * Returns:
 *  error code
 */
static Four rm_RedoWithCommitBuffer(
    Four 	handle,
    Four	verboseFlag,	    /* IN verbose flag : flag to denote whether commit progress is printed or not */
    char*	pageIdSortBuffer,   /* IN page id sort buffer */
    char*	commitBuffer,	    /* IN commit buffer */
    char*	tempCommitBuffer,   /* IN temporary commit buffer */
    Four	commitBufferSize    /* IN size of commit buffer in page */
)
{
    Four    e;			    /* error code */
    Two     trainSize;              /* size of train in pages */
    PageID  dataPid;		    /* page id of the updated page */
    PageID  nextDataPid;	    /* page id of the next updated page */
    PageNo  logPageNo;		    /* log page corresponding to the data page */
    PageID  logPid;		    /* page id of the log page */
    UFour   nPagesToCommit;	    /* the number of pages to commit */
    UFour   nTrainsToCommit;	    /* the number of trains to commit */
    UFour   nTotalPagesToCommit;    /* the number of total pages and trains to commit */
    UFour   nTotalPagesHasBeenCommited; /* the number of total pages which are commited */
    Four    progress;		    /* Commit progress in % */
    Four    prevProgress;	    /* previous progress value printed on screen */
    Four    nCommitPages;	    /* the number of pages of commitBuffer */
    Four    commitPageNo;	    /* indicates the pageNo of commitBuffer on commit */
    Four    nSequentialCommitPages; /* the number of contiguous pages of commitBuffer */
    char*   commitPagePtr;	    /* pages of commitBuffer on commit */
    Four    i;			    /* loop counter */

    TR_PRINT(TR_RM, TR1, ("rm_RedoWithCommitBuffer(handle)"));

#if defined(DBLOCK) && !defined(NDEBUG)
    /* check lock for log volume is acquired */
    /* Note!! Redo operation must be performed by only one process */
    if ( !RDsM_CheckVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, L_X) ) ERR(handle, eINTERNAL);	
#endif

    /* commit해야 할 양을 계산하고 이를 화면에 출력한다. */
    /* Calculate the number of pages to commit and print it. */
    nPagesToCommit      = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage - RM_INITIAL_ALLOC_POSITION_FOR_PAGE;
    nTrainsToCommit     = (RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo) -
	    		   RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForTrain) / TRAINSIZE2;
    nTotalPagesToCommit = nPagesToCommit + nTrainsToCommit * TRAINSIZE2;
    nTotalPagesHasBeenCommited = 0;
    progress            = 0;
    prevProgress        = -1;

    if(verboseFlag)
    {
	printf("Transaction Commit Information\n");
        printf("Total number of pages to commit : %ld", nTotalPagesToCommit);
        printf(" (page = %ld, train = %ld)\n", nPagesToCommit, nTrainsToCommit);
	printf("Progress :     "); fflush(stdout);
    }

    /* page log를 commit 한다 */
    /* Commit page log. */
    for (logPageNo = RM_INITIAL_ALLOC_POSITION_FOR_PAGE; 
         logPageNo < RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage; 
	 /* loop counter is in the end of loop */) 
    {
	/* 최대 commitBufferSize 만큼을 읽는다. 실제 읽은 page의 갯수는 nCommitPages 에 저장된다. */
	/* (logPageNo, logPageNo + commitBufferSize - 1) 까지를 읽기를 시도한다. */
	/* Read pages from log as many as commitBufferSize. The number of read pages are saved in nCommitPages. */ 
	/* Read pages of which page numbers are from logPageNo to logPageNo + commitBufferSize - 1. */
	if(logPageNo + commitBufferSize - 1 < RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage)
	    nCommitPages = commitBufferSize;
	else
	    nCommitPages = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage - logPageNo;

        logPid.volNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo; 
        logPid.pageNo = logPageNo;
	/* 주의 : RDsM_ReadTrainForLogVolume API는 임이의 페이지크기를 디스크에 쓸수 있어야 한다.
	          device boundary에 걸리더래도, 자동으로 두번에 나누어서 처리해야 한다. */
	/* Note : RDsM_ReadTrainForLogVolume API can write an arbitrary size page to disk.
	          Even though page size violates device boundary, automatically divides 
		  pages and write the pages to disk in twice. */
        e = RDsM_ReadTrainsForLogVolume(handle, &logPid, commitBuffer, nCommitPages, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

	/* commitBuffer 내의 내용을 (volNo, pageNo)순으로 sorting한다. */
        /* Sort pages of commitBuffer in order of (volNo, pageNo). */
	rm_SortCommitPages(handle, pageIdSortBuffer, commitBuffer, tempCommitBuffer, nCommitPages, PAGESIZE);

	for(commitPageNo = 0; commitPageNo < nCommitPages; /* loop counter is in the end of loop */)
	{
	    commitPagePtr = commitBuffer + PAGESIZE * commitPageNo;

	    /* All page have the page id on the beginning of the page. */
            dataPid = *((PageID*)(commitPagePtr));
        
	    /* check this page is nil */
	    if ( IS_NILPAGEID(dataPid) ) 
	    {
		commitPageNo               += PAGESIZE2;
		nTotalPagesHasBeenCommited += PAGESIZE2;
		continue;
	    }

	    /* Calculate nSequentialCommitPAges. */
	    for(i = commitPageNo + PAGESIZE2, nSequentialCommitPages = PAGESIZE2; i < nCommitPages; i += PAGESIZE2)
	    {
		nextDataPid = *((PageID*)(commitBuffer + PAGESIZE * i));
		if(dataPid.volNo == nextDataPid.volNo && 
		   dataPid.pageNo + nSequentialCommitPages == nextDataPid.pageNo)	/* If it is a contiguous page */
		{
		    /* Increase the number of contiguous pages. */
		    nSequentialCommitPages += PAGESIZE2;
		}
		else
		    break;
	    }

#if defined(DBLOCK) && !defined(NDEBUG)
            /* check lock for data volume is acquired */
	    if ( !RDsM_CheckVolumeLock(handle, dataPid.volNo, L_X) ) ERR(handle, eINTERNAL);
#endif
	    
	    /* sequential writing을 시도한다. */
            /* Write commit page to disk sequentially. */
	    e = RDsM_WriteTrains(handle, commitPagePtr, &dataPid, nSequentialCommitPages, PAGESIZE2);
	    if (e < eNOERROR) ERR(handle, e);  

	    commitPageNo               += nSequentialCommitPages;
	    nTotalPagesHasBeenCommited += nSequentialCommitPages;
	    if(verboseFlag)
	    {
		progress = ((float)nTotalPagesHasBeenCommited / (float)nTotalPagesToCommit) * 100;
		if(progress != prevProgress)
		{
                    printf("\b\b\b\b%3ld%%", progress); fflush(stdout);
		    prevProgress = progress;
		}
	    }
	}
	logPageNo += nCommitPages;
    }

    /* train log를 commit한다 */
    /* Commit to train log. */
    for (logPageNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForTrain + TRAINSIZE2; 
         logPageNo < RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo) + TRAINSIZE2; 
	 /* loop counter is in the end of loop */) 
    {
	/* 최대 commitBufferSize 만큼을 읽는다. 실제 읽은 page의 갯수는 nCommitPages 에 저장된다. */
	/* (logPageNo, logPageNo + commitBufferSize - 1) 까지를 읽기를 시도한다. */
	/* Read pages from log as many as commitBufferSize. The number of read pages are saved in nCommitPages. */ 
	/* Read pages of which page numbers are from logPageNo to logPageNo + commitBufferSize - 1. */
	if(logPageNo + commitBufferSize - 1 < RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo) + TRAINSIZE2)
	    nCommitPages = commitBufferSize;
	else
	    nCommitPages = RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo) + TRAINSIZE2 - logPageNo;

        logPid.volNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo; 
        logPid.pageNo = logPageNo;
        e = RDsM_ReadTrainsForLogVolume(handle, &logPid, commitBuffer, nCommitPages / TRAINSIZE2, TRAINSIZE2);
        if (e < eNOERROR) ERR(handle, e);

	/* commitBuffer 내의 내용을 (volNo, pageNo)순으로 sorting한다. */
        /* Sort pages of commitBuffer in order of (volNo, pageNo). */
	rm_SortCommitPages(handle, pageIdSortBuffer, commitBuffer, tempCommitBuffer, nCommitPages / TRAINSIZE2, TRAINSIZE);

	for(commitPageNo = 0; commitPageNo < nCommitPages; /* loop counter is in the end of loop */)
	{
	    commitPagePtr = commitBuffer + PAGESIZE * commitPageNo;
	    
	    /* All page have the page id on the beginning of the page. */
	    dataPid = *((PageID*)commitPagePtr);
        
	    /* check this page is nil */
	    if ( IS_NILPAGEID(dataPid) )
	    {
		commitPageNo               += TRAINSIZE2;
		nTotalPagesHasBeenCommited += TRAINSIZE2;
		continue;
	    }

	    /* Calculate nSequentialCommitPages. */
	    for(i = commitPageNo + TRAINSIZE2, nSequentialCommitPages = TRAINSIZE2; i < nCommitPages; i += TRAINSIZE2)
	    {
		nextDataPid = *((PageID*)(commitBuffer + PAGESIZE * i));
		if(dataPid.volNo == nextDataPid.volNo && 
		   dataPid.pageNo + nSequentialCommitPages == nextDataPid.pageNo)	/* If it is a coutigous page */ 
		{
		    /* Increase the number of contiguos pages. */
		    nSequentialCommitPages += TRAINSIZE2;
		}
		else
		    break;
	    }

#if defined(DBLOCK) && !defined(NDEBUG)
	    /* check lock for data volume is acquired */
	    if ( !RDsM_CheckVolumeLock(handle, dataPid.volNo, L_X) ) ERR(handle, eINTERNAL);
#endif

	    /* sequential writing을 시도한다. */
            /* Write commit pages to disk sequentially. */
	    e = RDsM_WriteTrains(handle, commitPagePtr, &dataPid, nSequentialCommitPages / TRAINSIZE2, TRAINSIZE2);
	    if (e < eNOERROR) ERR(handle, e);  

	    commitPageNo               += nSequentialCommitPages;
	    nTotalPagesHasBeenCommited += nSequentialCommitPages;
	    if(verboseFlag)
	    {
		progress = ((float)nTotalPagesHasBeenCommited / (float)nTotalPagesToCommit) * 100;
		if(progress != prevProgress)
		{
                    printf("\b\b\b\b%3ld%%", progress); fflush(stdout);
		    prevProgress = progress;
		}
	    }
	}
	logPageNo += nCommitPages;
    }
    
    if(verboseFlag)
    {
	printf("\b\b\b\bdone\n"); 
	fflush(stdout);
    }

    return(eNOERROR);
}

/*@================================
 * rm_RedoWithoutCommitBuffer()
 *================================*/
/*
 * Function: Four rm_RedoWithoutCommitBuffer(void)
 *
 * Description:
 *  Redo the update of the committed transaction. Actually we save the
 *  updated pages.
 *  한개의 train buffer를 사용한 commit 방법으로, random access를 많이 발생시키기 때문에 좋지 않은 방법이다.
 *  예전 구현과의 성능 비교를 위해 그대로 유지한다.
 *
 *  This function implements commit method with one train buffer. 
 *  Because the method occurs many random access, it is not good.
 *  But, remain this function for comparing performance.
 * Returns:
 *  error code
 */
static Four rm_RedoWithoutCommitBuffer(Four handle, Four verboseFlag)
{
    Four    e;			    /* error code */
    Two     trainSize;              /* size of train in pages */
    char    aTrainBuf[TRAINSIZE];   /* buffer for a train */
    PageID  dataPid;		    /* page id of the updated page */
    PageNo  logPageNo;		    /* log page corresponding to the data page */
    PageID  logPid;		    /* page id of the log page */
    UFour   nPagesToCommit;	    /* the number of pages to commit */
    UFour   nTrainsToCommit;	    /* the number of trains to commit */
    UFour   nTotalPagesToCommit;    /* the number of total pages to commit */
    UFour   nTotalPagesHasBeenCommited; /* the number of total pages that are commited */
    Four    progress;		    /* commit progress in % */
    Four    prevProgress;	    /* previous commit progress printed on screen */ 

    TR_PRINT(TR_RM, TR1, ("rm_RedoWithoutCommitBuffer(handle)"));

#if defined(DBLOCK) && !defined(NDEBUG)
    /* check lock for log volume is acquired */
    /* Note!! Redo operation must be performed by only one process */
    if ( !RDsM_CheckVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, L_X) ) ERR(handle, eINTERNAL);	
#endif

    /* Calculate the number of pages to commit and print it. */
    nPagesToCommit      = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage - RM_INITIAL_ALLOC_POSITION_FOR_PAGE;
    nTrainsToCommit     = (RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo) - 
	    		   RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForTrain) / TRAINSIZE2;
    nTotalPagesToCommit = nPagesToCommit + nTrainsToCommit * TRAINSIZE2;
    nTotalPagesHasBeenCommited = 0;
    progress            = 0;
    prevProgress        = -1; 

    if(verboseFlag)
    {
	printf("Transaction Commit Information\n");
        printf("Total number of pages to commit : %ld", nTotalPagesToCommit);
        printf(" (page = %ld, train = %ld)\n", nPagesToCommit, nTrainsToCommit);
	printf("Progress : ____"); fflush(stdout);
    }

    for (logPageNo = RM_INITIAL_ALLOC_POSITION_FOR_PAGE; logPageNo < RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForPage; logPageNo++) 
    {
        logPid.volNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo; 
        logPid.pageNo = logPageNo;
        e = RDsM_ReadTrainForLogVolume(handle, &logPid, aTrainBuf, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* All page have the page id on the beginning of the page. */
        dataPid = *((PageID*)aTrainBuf);
        
        /* check this page is nil */
        if ( IS_NILPAGEID(dataPid) ) continue;

#if defined(DBLOCK) && !defined(NDEBUG)
        /* check lock for data volume is acquired */
        if ( !RDsM_CheckVolumeLock(handle, dataPid.volNo, L_X) ) ERR(handle, eINTERNAL);
#endif

        e = RDsM_WriteTrain(handle, aTrainBuf, &dataPid, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);  

	nTotalPagesHasBeenCommited += PAGESIZE2;
	if(verboseFlag)
	{
	    progress = ((float)nTotalPagesHasBeenCommited / (float)nTotalPagesToCommit) * 100;
	    if(progress != prevProgress)
	    {
                printf("\b\b\b\b%3ld%%", progress); fflush(stdout);
		prevProgress = progress;
	    }
	}
    }

    for (logPageNo = RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 
	 logPageNo > RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.pageNoToAllocForTrain; 
	 logPageNo -= TRAINSIZE2) 
    {
        logPid.volNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo; 
        logPid.pageNo = logPageNo;
        e = RDsM_ReadTrainForLogVolume(handle, &logPid, aTrainBuf, TRAINSIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* All page have the page id on the beginning of the page. */
        dataPid = *((PageID*)aTrainBuf);
        
        /* check this page is nil */
        if ( IS_NILPAGEID(dataPid) ) continue;

#if defined(DBLOCK) && !defined(NDEBUG)
        /* check lock for data volume is acquired */
        if ( !RDsM_CheckVolumeLock(handle, dataPid.volNo, L_X) ) ERR(handle, eINTERNAL);
#endif

        e = RDsM_WriteTrain(handle, aTrainBuf, &dataPid, TRAINSIZE2);
        if (e < eNOERROR) ERR(handle, e);                        

	nTotalPagesHasBeenCommited += TRAINSIZE2;
	if(verboseFlag)
	{
	    progress = ((float)nTotalPagesHasBeenCommited / (float)nTotalPagesToCommit) * 100;
	    if(progress != prevProgress)
	    {
                printf("\b\b\b\b%3ld%%", progress); fflush(stdout);
		prevProgress = progress;
	    }
	}
    }
    
    if(verboseFlag)
    {
	printf("\b\b\b\bdone\n"); 
	fflush(stdout);
    }

    return(eNOERROR);
}

