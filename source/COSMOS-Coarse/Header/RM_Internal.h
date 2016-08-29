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
#ifndef _RM_INTERNAL_H_
#define _RM_INTERNAL_H_


/*@
 * Constant Definitions
 */
#define NO_LOG_VOLUME    -1     /* log volume is not mounted */

#define RM_TRAINNOTEXIST 1      /* train not exist */

#define RM_NONEXTENTRY   2      /* there is no next entry in the hash table */


/*@
 * Type Definitions
 */
/*
 * Type Definition for the Log Volume Infomation
 */
typedef struct rm_LogVolumeInfo_t_tag {
    Four 	volNo;                 
    Four 	sizeOfExt;             /* size of extent */
    Four 	numOfExts;             /* number of extents */
    Four 	pageNoToAllocForPage;  /* page no to alloc for the new page */
    Four 	pageNoToAllocForTrain; /* page no to alloc for the new train */
    Boolean 	onCommitFlag;   	/* TRUE when we are saving pages of the committed transaction */
    Four 	mountedDataVolumeListSize; /* size of mounted data volume list */
    Four 	firstPageNoForMountedDataVolumeList;

    /* for Global Transaction */
    Boolean         	onPrepareFlag;		/* TRUE if 'Prepare To Commit' is done. */
    Boolean             isReadOnlyFlag;         /* TRUE if read-only transaction is prepared */
    Boolean 		RollbackRequiredFlag; 	/* TRUE if the current transaction requires rollback facility */
    GlobalXactID    	globalXactID;     	/* global transaction ID */
} rm_LogVolumeInfo_t;

typedef struct rm_LogVolumeInfoPage_t_tag {
    rm_LogVolumeInfo_t logVolumeInfo;
    char dummy[PAGESIZE-sizeof(rm_LogVolumeInfo_t)];
} rm_LogVolumeInfoPage_t;

#ifdef USE_LOG_COHERENCY_VOLUME
/*
 * Type Definition for managing log volume to sync buffer pages
 *
 * To replace coherency volume with log volume in multi-sever environment,
 * the structures for coherency volume is defined in log volume.
 * Following structures are same with ones in Header/BfM_Internals.
 * These structures are used to bring informations of coherency volume in BfM to RM.
 */
typedef struct {
    Four        hostId;		/* machine Id */
    Four        shmBufferId;	/* shared memory Id */
    Four        timestamp; 	/* timestamp has the time a page is used or NIL */
    PageID      pageId;
} rm_CoherencyPageInfo_t;

#define RM_COHERENCY_HEADER_N_PAGE      PAGESIZE2               /* header is 1 page */
#define RM_COHERENCY_PAGEINFOS_N_PAGE   PAGESIZE2               /* pageinfos is 1 page */
#define RM_N_COHERENCY_PAGEINFOS        ((RM_COHERENCY_PAGEINFOS_N_PAGE * PAGESIZE) / sizeof(rm_CoherencyPageInfo_t))

typedef struct {
    Four                        volNo;                                  /* volume id of the coherency volume */
    Four                        lastCoherencyUpdateTimestamp;           /* timestamp of last syncing buffer */ 
    PageID                      headerPageId;                           /* header page id which contains header information */
    PageID                      pageInfosPageId;                        /* page id which contains pageInfos list */
    Four                        nPageInfos;                             /* number of page informations in the pageInfos */
    rm_CoherencyPageInfo_t     	pageInfos[RM_N_COHERENCY_PAGEINFOS];    /* page information holder */
} rm_CoherencyInfo_t;

typedef struct {
    Four        timestamp;
    Four        nPageInfos;
    Four        circularListHead;
    Four        circularListTail;
    Four        circularListSize;
} rm_CoherencyHeaderPage_t;

typedef struct {
    rm_CoherencyHeaderPage_t 	header;                                            /* header of the coherency volume header page */
    char                      	data[PAGESIZE - sizeof(rm_CoherencyHeaderPage_t)]; /* data area */
} rm_CoherencyHeaderPage;

typedef struct {
    rm_CoherencyPageInfo_t     	pageInfos[RM_N_COHERENCY_PAGEINFOS];
    char                       	data[(RM_COHERENCY_PAGEINFOS_N_PAGE * PAGESIZE) - (sizeof(rm_CoherencyPageInfo_t) * RM_N_COHERENCY_PAGEINFOS)];
} rm_CoherencyPageInfosPage;
#endif

/*@
 * Macro Definitions
 */
#define RM_LOG_VOLUME_INFO_PAGE_NO 0 

#ifdef USE_LOG_COHERENCY_VOLUME	     
#define RM_LOG_VOLUME_COHERENCY_HEADER_PAGE_NO		1	/* location of coherency header page in log volume */
#define RM_LOG_VOLUME_COHERENCY_PAGEINFOS_PAGE_NO 	2	/* location of coherency pageInfos page in log volume */
#endif

#ifdef USE_LOG_COHERENCY_VOLUME	    
#define RM_INITIAL_ALLOC_POSITION_FOR_PAGE 3	/* location of first log volume page (skip coherency header, pageInfos pages) */
#else
#define RM_INITIAL_ALLOC_POSITION_FOR_PAGE 1
#endif

#define RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(x) ((x).sizeOfExt*(x).numOfExts - TRAINSIZE2)
#define RM_INIT_ALLOC_POSITION_OF_LOG_VOLUME(x) \
((x).pageNoToAllocForPage = RM_INITIAL_ALLOC_POSITION_FOR_PAGE, \
 (x).pageNoToAllocForTrain = RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(x))
#define RM_NOT_EXIST_ANY_LOGGED_PAGE_OR_TRAIN(x) \
((x).pageNoToAllocForPage == RM_INITIAL_ALLOC_POSITION_FOR_PAGE && \
 (x).pageNoToAllocForTrain == RM_INITIAL_ALLOC_POSITION_FOR_TRAIN(x))

#define RM_NUM_OF_FREE_LOG_PAGES(x) \
( (x).pageNoToAllocForTrain - (x).pageNoToAllocForPage + TRAINSIZE2)

#ifdef USE_LOG_COHERENCY_VOLUME		
extern rm_CoherencyInfo_t rm_CoherencyInfo;	/* Coherency Information for RM */
#endif

/*@
 * Function Prototypes
 */
/* Internal Function Prototypes */
Four rm_InitLogTable(Four);
Four rm_FinalLogTable(Four);
Boolean rm_LookUpInLogTable(Four, PageID*, PageNo*);
Four rm_InsertIntoLogTable(Four, PageID*, PageNo);
Four rm_DoubleLogTable(Four);
Four rm_DeleteAllFromLogTable(Four);
Four rm_DeleteFromLogTable(Four, PageID*); 
Four rm_Redo(Four);
Four rm_ReadTrain(PageNo, char*, Two);
Four rm_WriteTrain(PageNo, char*, Two);
Four rm_ReadLogVolumeInfo(Four, Four, rm_LogVolumeInfo_t*);
Four rm_WriteLogVolumeInfo(Four, Four, rm_LogVolumeInfo_t*);
#ifdef USE_LOG_COHERENCY_VOLUME
Four rm_ReadLogVolumeInfoWithCoherencyPage(Four, Four, rm_LogVolumeInfo_t*, rm_CoherencyHeaderPage*, rm_CoherencyPageInfosPage* );
Four rm_WriteLogVolumeInfoWithCoherencyPage(Four, Four, rm_LogVolumeInfo_t*, rm_CoherencyHeaderPage*, rm_CoherencyPageInfosPage* );
Four rm_ReadCoherencyPage(Four, Four, rm_CoherencyHeaderPage*, rm_CoherencyPageInfosPage* );
Four rm_WriteCoherencyPage(Four, Four, rm_CoherencyHeaderPage*, rm_CoherencyPageInfosPage* );
#endif
Four rm_OpenLogTableScan(Four);
Four rm_CloseLogTableScan(Four);
Four rm_GetNextEntryFromLogTable(Four, PageID*, PageNo*);
Four rm_SaveDataVolumeList(Four, char*);
Four rm_LoadDataVolumeList(Four, char*);
    
/* Interface Function Prototypes */
Four RM_AbortTransaction(Four);
Four RM_BeginTransaction(Four, Boolean);
Four RM_CommitTransaction(Four);
char *RM_Err(Four);
Four RM_Final(Four);
Four RM_LoadTrain(Four, PageID*, void*, Two);
Four RM_Restart(Four, char*);
#ifdef USE_LOG_COHERENCY_VOLUME	
Four RM_RestartAndSyncBuffer(Four, char*, Four);
#endif
Four RM_RestartTwoPhaseCommit(Four, Four, Boolean*); 
Four RM_SaveTrain(Four, PageID*, void*, Two);
Four RM_SaveTrains(Four, char*, PageID*, Four, Two);
Four RM_FormatLogVolume(Four, Four);
Four RM_DeleteTrain(Four, PageID*, Two); 
Four RM_GetLogVolumeLock(Four);
Four RM_ReleaseLogVolumeLock(Four);
Four RM_IsDuplicatedGXID(Four, GlobalXactID*, Boolean*);
Four RM_EnterTwoPhaseCommit(Four, XactID*, GlobalXactID*);
Four RM_PrepareTransaction(Four, XactID*);
Four RM_IsReadOnlyTransaction(Four, XactID*, Boolean*);
Four RM_RecoverTwoPhaseCommit(Four, GlobalXactID*, XactID*);
Four RM_GetNumberofPreparedTransactions(Four*);
Four RM_GetPreparedTransactions(Four, Four, GlobalXactID*);

Boolean RM_IsLogVolume(Four, Four volNo);

#endif /* _RM_INTERNAL_H_ */


