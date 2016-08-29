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
#ifndef _BFM_INTERNAL_H_
#define _BFM_INTERNAL_H_

#ifdef USE_SHARED_MEMORY_BUFFER
#include "THM_lock.h"
#include "latch.h"
#endif

/*@
 * Constant Definitions
 */ 
/* number of buffer types : number of buffer pools used */
#define NUM_BUF_TYPES 2

/* Buffer Types */
#define PAGE_BUF     0
#define LOT_LEAF_BUF 1

/* macro to detect a bad buffer type value */
#define IS_BAD_BUFFERTYPE(type) (type < 0 || type >= NUM_BUF_TYPES)

#ifdef USE_SHARED_MEMORY_BUFFER

/* Macros for process table */
#define BFM_PROCESSTABLE processTable 

#endif

/*@
 * Type Definitions
 */

#ifdef USE_SHARED_MEMORY_BUFFER	
/* 
 * This comment and codes are taken from COSMOS-CC BfM.h.
 * The structure of locking mechanism for buffer manager 
 * to avoid deadlock situation in buffer manager. 
 */
typedef struct lock_ctrlBlock 	Lock_ctrlBlock;
typedef struct lock_hash_entry 	Lock_hashEntry;

struct lock_ctrlBlock {
    TrainID 				key;
    Four 				lockCounter;
#ifdef USE_MUTEX
    cosmos_thread_mutex_t		mutex;
#else
    LATCH_TYPE 				latch;
#endif
    LOGICAL_PTR_TYPE(Lock_ctrlBlock* )  nextHashChain; 	
};

struct lock_hash_entry {
#ifdef USE_MUTEX
    cosmos_thread_mutex_t		mutex;
#else
    LATCH_TYPE      			latch;
#endif
    LOGICAL_PTR_TYPE(Lock_ctrlBlock* ) 	blockPtr; 	
    LOGICAL_PTR_TYPE(Lock_ctrlBlock* )	freeBlockPtr; 	
};
#endif

/* The structure of key type used at hashing in buffer manager */
/* same as "typedef BfMHashKey PageID; */
typedef struct {
    PageNo pageNo;		/* a PageNo */
    VolNo volNo;		/* a volumeNo */
} BfMHashKey;

#define SET_NILBFMHASHKEY(key) (SET_NILPAGEID(key)) 
#define IS_NILBFMHASHKEY(key)  (IS_NILPAGEID(key))

/* Taken following codes from bfm_Hash.c */
/* Check the validity of the given key, some restrictions may be added. */
#define CHECKKEY(k)  \
        { if( ((k)->volNo < 0) || ((k)->pageNo < 0) )  \
              return( eBADHASHKEY_BFM ); }

/* If k1 = k2 then true, otherwise false */
#define EQUALKEY(k1, k2) \
          (((k1)->volNo == (k2)->volNo) && ((k1)->pageNo == (k2)->pageNo))

#ifdef USE_SHARED_MEMORY_BUFFER
/* The structure of hash table entry */
typedef struct {
#ifdef USE_MUTEX
    cosmos_thread_mutex_t	mutex;
#else
    LATCH_TYPE			latch;	/* latch for access hash table */
#endif
    Four		bufTableEntry;	/* index of buffer table entry */
} HashTable;
#endif

/* The structure of BufferTable which is used in buffer replacement algo. */
typedef struct {
    BfMHashKey 	key;		/* identify a page */
    Two    	fixed;		/* fixed count */
    One    	bits;		/* bit 1 : DIRTY, bit 2 : VALID, bit 3 : REFER, bit 4 : NEW */ 
#ifdef USE_SHARED_MEMORY_BUFFER 
#ifdef USE_MUTEX
    cosmos_thread_mutex_t mutex;
#else
    LATCH_TYPE	latch;		/* latch for access of buffer table entry & page */
#endif
#endif
    Four   	nextHashEntry;  
} BufferTable;

#define DIRTY  0x01
#define VALID  0x02
#define REFER  0x04
#define NEW    0x08		/* new allocated train */ 
#define ALL_0  0x00
#define ALL_1  ((sizeof(One) == 1) ? (0xff) : (0xffff))

/*
 * change bufTable, bufferPool, lockCBTable, lockHashTable, hashTable to use LOGICAL_PTR_TYPE
 */
/* type definition for buffer pool information */
typedef struct {
    Four                bufSize;        /* size of a buffer in page size */
    Four                nBufs;          /* # of buffers in this buffer pool */
    UFour               nextVictim;     /* starting point for searching a next victim */
	LOGICAL_PTR_TYPE(BufferTable*)      bufTable;       /* for*/ 
	LOGICAL_PTR_TYPE(char*)             bufferPool;     /* a set of buffers */ 
#ifdef USE_SHARED_MEMORY_BUFFER
	LOGICAL_PTR_TYPE(Lock_ctrlBlock*)   lockCBTable;            /* table of lock control block */ 
	LOGICAL_PTR_TYPE(Lock_hashEntry*)   lockHashTable;          /* hash table for lock control block */ 
	LOGICAL_PTR_TYPE(HashTable*)        hashTable;              /* hash table */
    LOGICAL_PTR_TYPE(Lock_ctrlBlock* )	freeLockCBHeader;	/* free lock control block header */
#else
    Four*        	hashTable;	/* hash table */
#endif
} BufferInfo;

#define BI_BUFSIZE(type)	     (bufInfo[type].bufSize)
#define BI_NBUFS(type)		     (bufInfo[type].nBufs)
#define BI_NEXTVICTIM(type)	     (bufInfo[type].nextVictim)
#define BI_BUFTABLE(type)	     (bufInfo[type].bufTable)
#define BI_BUFTABLE_ENTRY(type, idx) (((BufferTable*)PHYSICAL_PTR(bufInfo[type].bufTable))[idx]) 
#define BI_KEY(type, idx)            (((BufferTable*)PHYSICAL_PTR(bufInfo[type].bufTable))[idx].key) 
#define BI_NFIXED(type, idx)          (((BufferTable*)PHYSICAL_PTR(bufInfo[type].bufTable))[idx].fixed) 
#define BI_BITS(type, idx)           (((BufferTable*)PHYSICAL_PTR(bufInfo[type].bufTable))[idx].bits) 
#define BI_NEXTHASHENTRY(type, idx)  (((BufferTable*)PHYSICAL_PTR(bufInfo[type].bufTable))[idx].nextHashEntry) 
#define BI_BUFFERPOOL(type)	     (bufInfo[type].bufferPool)
#define BI_BUFFER(type, idx)         ((char*)PHYSICAL_PTR(BI_BUFFERPOOL(type))+PAGESIZE*BI_BUFSIZE(type)*idx)  

#ifdef USE_SHARED_MEMORY_BUFFER
#define BI_LOCK_CB_TABLE(type)       (bufInfo[type].lockCBTable)
#define BI_LOCK_CB_ENTRY(type, idx)  (((Lock_ctrlBlock*)PHYSICAL_PTR(bufInfo[type].lockCBTable))[idx])  
#define BI_LOCKHASHTABLE(type)       (bufInfo[type].lockHashTable)
#define BI_LOCKHASHENTRY(type, idx)  (((Lock_hashEntry*)PHYSICAL_PTR(bufInfo[type].lockHashTable))[idx])  
#define BI_FREE_LOCKCBHEADER(type)   (bufInfo[type].freeLockCBHeader)	
#define BI_HASHTABLE(type)	     (bufInfo[type].hashTable)
#define BI_HASHTABLEENTRY(type,idx)  (((HashTable*)PHYSICAL_PTR(bufInfo[type].hashTable))[idx].bufTableEntry)  
#else
#define BI_HASHTABLE(type)	     (bufInfo[type].hashTable)
#define BI_HASHTABLEENTRY(type,idx)  (*(BI_HASHTABLE(type) + idx))
#endif

/* The size of the hash table is four times of the size of the buffer pool
 * in order to minimize the rate of collisions.
 * Each entry of the hash table corresponds to an index of the buffer pool
 * as well as the buffer table.
 */
#define HASHTABLESIZE_TO_NBUFS(_x)   	((_x) * 3 - 1) 	
#define LOCKHASHTABLESIZE_TO_NBUFS(_x)	((_x) * 5 + 1)
#define LOCK_CB_TABLESIZE_TO_NBUFS(_x)  ((_x) * 3 + 1)
#define HASHTABLESIZE(type) 	     	(HASHTABLESIZE_TO_NBUFS(BI_NBUFS(type))) 
#define LOCKHASHTABLESIZE(type)  	(LOCKHASHTABLESIZE_TO_NBUFS(BI_NBUFS(type)))
#define LOCK_CB_TABLESIZE(type)  	(LOCK_CB_TABLESIZE_TO_NBUFS(BI_NBUFS(type)))
#define FIXED_BUF_TABLESIZE		8	

/* constant definition: The BfMHashKey don't exist in the hash table. */
#define NOTFOUND_IN_HTABLE  -1

#ifdef USE_SHARED_MEMORY_BUFFER
/* type definition for fixed buffer table */
typedef struct {
    BfMHashKey  key;	/* key of fixed buffer */
    Four	type;	/* type of fixed buffer */
} FixedBufferTable;

/* type definition for fixed buffer information */
typedef struct {
    Four                nFixedBufs;	   /* the number of fixed buffers */
    Four		nUnfixedBufs;	   /* the number of unfixed buffers */
    FixedBufferTable*   fixedBufTable;	   /* fixed buffer table */
    FixedBufferTable*   unfixedBufTable;   /* unfixed buffer table */
    Four		fixedBufTableSize; /* the size of fixe buffer table */
} FixedBufferInfo;

#endif

#if defined(USE_COHERENCY_VOLUME) || defined(USE_LOG_COHERENCY_VOLUME)
/* structure definitions for managing coherency volume */
typedef struct {
#ifdef USE_SHARED_MEMORY_BUFFER		
    Four	hostId;
    Four	shmBufferId;
#endif
    Four	timestamp;	/* timestamp는 NIL이거나 언제 값이 사용된것인지를 나타내는 시각을 나타낸다 */
                                /* timestamp indicates when a page is used or it is NIL. */ 
    PageID	pageId;
} bfm_CoherencyPageInfo_t;

#define BFM_COHERENCY_VOLUME_HEADER_N_PAGE	PAGESIZE2		/* header는 1 page로 구성됨 *//* header is 1 page */
#define BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE	PAGESIZE2		/* pageinfos는 1 page로 구성됨 *//* pageinfos is 1 page */
#define BFM_COHERENCY_VOLUME_EXTENTSIZE         8
#define BFM_N_COHERENCY_PAGEINFOS		((BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE * PAGESIZE) / sizeof(bfm_CoherencyPageInfo_t))
#define BFM_COHERENCY_TIMESTAMP_MAX             (CONSTANT_ONE << (sizeof(Four)*(BITSPERBYTE)-2)) 

typedef struct {
    /* volNo는 BfM_MountCoherencyVolume일때, 값이 부여 되며 모든 coherency volume연산시 사용된다.
       초기값은 NO_COHERENCY_VOLUME 이다. */
    /* nPageInfos와 pageInfos는 bfm_AppendCoherencyPageInfos, bfm_AppendCoherencyPageInfos, bfm_AppendCoherencyPageInfos 에서
       사용하는 값으로 page info를 memory에 보관하고 있다가 후에 BfM_UpdateCoherencyVolume에서 coherency volume으로 
       써지는 값들을 보관한다. */
    /* volNo is set when BfM_MountCoherencyVolume() is called.
       It is used on all operations of coherency volume. And it's initial value is NO_COHERENCY_VOLUME */
    /* pageInfos and pageInfos are used in bfm_AppendCoherencyPageInfos(), bfm_AppendCoherencyPageInfos(), and bfm_AppendCoherencyPageInfos().
       They are used to keep the page info, which is write to coherency volume when BfM_UpdatecoherencyVolume() is called, in the memory.  */
    VolNo			volNo;					/* volume id of the coherency volume */
    Four			lastCoherencyUpdateTimestamp;   	/* timestamp when the last buffer synch is done */
    PageID			headerPageId;				/* header page id which contains header information */
    PageID			pageInfosPageId;			/* page id which contains pageInfos list */
    Four			nPageInfos;				/* number of page informations in the pageInfos */
    bfm_CoherencyPageInfo_t	pageInfos[BFM_N_COHERENCY_PAGEINFOS];	/* page information holder */
} bfm_CoherencyVolumeInfo_t;

typedef struct {
    Four	timestamp;
    Four	nPageInfos;
    Four	circularListHead;
    Four	circularListTail;
    Four	circularListSize;
} bfm_CoherencyVolumeHeader_t;

typedef struct {
    bfm_CoherencyVolumeHeader_t header; 					      /* header of the coherency volume header page */
    char			data[PAGESIZE - sizeof(bfm_CoherencyVolumeHeader_t)]; /* data area */
} bfm_CoherencyVolumeHeaderPage;

typedef struct {
    bfm_CoherencyPageInfo_t	pageInfos[BFM_N_COHERENCY_PAGEINFOS];
    char			data[(BFM_COHERENCY_VOLUME_PAGEINFOS_N_PAGE * PAGESIZE) - (sizeof(bfm_CoherencyPageInfo_t) * BFM_N_COHERENCY_PAGEINFOS)];
} bfm_CoherencyVolumePageInfosPage;

#define NO_COHERENCY_VOLUME	-1
#endif

/*@
 * Macro Definitions
 */


/*@
 * Global Variables
 */
#ifdef USE_SHARED_MEMORY_BUFFER
/* 
 * Global variables used for buffer manager
 * The definitions of these variables are in file BfM_Init.c. and bfm_Lock.c
 */
extern BufferInfo* 		  bufInfo;			/* buffer information */
extern PIB*			  processTable; 		/* pointer of process table in shared memory */ 
extern TCB*			  TCBPool;			/* pointer of thread contol block pool in shared memory */
extern Four			  procIndex;			/* process ID of this process */

#else
/* 
 * Global variable used for buffer manager
 * The definition of this variable is in file BfM_Init.c.
 */
extern BufferInfo bufInfo[];

#endif

/*@
 * Function Prototypes
 */
/* internal function prototypes */
Four bfm_AllocTrain(Four, Four);
Four bfm_Delete(Four, BfMHashKey *, Four);
Four bfm_DeleteAll(Four); 
Four bfm_CreateLocalMemoryBuffer(Four);
Four bfm_DestroyLocalMemoryBuffer(Four);
Four bfm_FinalBufferInfo(Four); 
Four bfm_FlushTrain(Four, TrainID *, Four);
Four bfm_FlushTrains(Four, Four);
Four bfm_InitBufferInfo(Four, Four, Four, Four);
Four bfm_InitHashTable(Four, Four);
Four bfm_Insert(Four, BfMHashKey *, Four, Four);
Four bfm_LookUp(Four, BfMHashKey *, Four);
Four bfm_ReadTrain(Four, TrainID *, char *, Four);
void bfm_dump_buffertable(Four);
void bfm_dump_hashtable(Four);

#ifdef USE_SHARED_MEMORY_BUFFER
Four bfm_CreateSharedMemoryBuffer(Four);
Four bfm_DestroySharedMemoryBuffer(Four);
Four bfm_GetNumAttachedSHMBuffer(Four);

void bfm_SignalHandler(int);
Four bfm_InstallSignalHandler(Four);

Four bfm_CheckBufPageInMountedVolume(Four, Four*, Four, BfMHashKey* );

Four bfm_InitLatch(Four, LATCH_TYPE* latchPtr);
Four bfm_GetLatch(Four, LATCH_TYPE* latchPtr, Four procIndex, LatchMode reqMode, LatchConditional reqCondition, LATCH_TYPE* releasedLatchPtr);
Four bfm_ReleaseLatch(Four, LATCH_TYPE* latchPtr, Four procIndex);

Four bfm_InitFixedBufferInfo(Four);
Four bfm_FinalFixedBufferInfo(Four);
Four bfm_InsertFixedBuffer(Four, BfMHashKey*, Four);
Four bfm_DeleteFixedBuffer(Four, BfMHashKey*, Four);
Four bfm_DoubleFixedBufferTable(Four);
Four bfm_FreeAllFixedBuffers(Four);

Four bfm_InitProcessTable(Four);
Four bfm_AllocProcessTableEntry(Four);
Four bfm_FreeProcessTableEntry(Four);

Four bfm_AllocThreadCtrlBlock(Four);
Four bfm_FreeThreadCtrlBlock(Four);
Four bfm_InitLocalLatchList(Four);
Four bfm_FinalLocalLatchList(Four);

Four bfm_Lock(Four, TrainID*, Four );
Four bfm_Unlock(Four, TrainID*, Four );
Four bfm_InitLockHashTable(Four, Four );
Four bfm_InitLockCBTable(Four, Four );
Four bfm_AllocMyLockCBs(Four, Four );
Four bfm_FreeMyLockCBs(Four, Four );
#endif

#ifdef USE_COHERENCY_VOLUME
Four bfm_GetCoherencyVolumeLock(Four, LockMode lockMode);
Four bfm_ReleaseCoherencyVolumeLock(Four);
Four bfm_ReadCoherencyVolumeTimestamp(Four, Four* timestamp);
Four bfm_WriteCoherencyVolumeTimestamp(Four, Four timestamp);
Four bfm_AppendCoherencyPageInfos(Four, bfm_CoherencyPageInfo_t* pageInfo);
Four bfm_FlushCoherencyPageInfos(Four, Four timestamp);
Four bfm_ReadCoherencyPageInfos(Four, Four sizeOfPageInfos, bfm_CoherencyPageInfo_t* pageInfos, Four* nReturnedPageInfos);
Four bfm_InitCoherencyPageInfos(Four);
#endif

#ifdef USE_LOG_COHERENCY_VOLUME        
Four bfm_ReadCoherencyPageInfos(Four, Four, bfm_CoherencyVolumeHeaderPage*, bfm_CoherencyVolumePageInfosPage* , bfm_CoherencyPageInfo_t*, Four*);
Four bfm_FlushCoherencyPageInfos(Four, Four, bfm_CoherencyVolumeHeaderPage*, bfm_CoherencyVolumePageInfosPage*);
Four bfm_AppendCoherencyPageInfos(Four, bfm_CoherencyPageInfo_t* pageInfo);
Four bfm_InitCoherencyPageInfos(Four);
#endif

/* Interface Function Prototypes */
#ifdef USE_SHARED_MEMORY_BUFFER		
Four BfM_Mount(Four);
#endif
Four BfM_Dismount(Four, Four);
char *BfM_Err(Four);
Four BfM_FinalPerProcessDS(Four);	
Four BfM_FinalPerThreadDS(Four);	
Four BfM_FreeTrain(Four, PageID *, Four);
Four BfM_GetTrain(Four, PageID *, char **, Four);
Four BfM_GetNewTrain(Four, TrainID *, char **, Four);
Four BfM_InitPerProcessDS(Four);
Four BfM_InitPerThreadDS(Four);		
Four BfM_SetDirty(Four, PageID *, Four);
Four BfM_DiscardAll(Four);      	
Four BfM_FlushAll(Four);        	
Four BfM_readTrain(Four, TrainID *, char *, Four);   
Four BfM_RemoveTrain(Four, PageID *, Four, Boolean); 
Four BfM_DiscardAllTrainsInVolume(Four, Four); 
#ifdef USE_COHERENCY_VOLUME
Four BfM_FormatCoherencyVolume(Four, char* devName, char* title, Four volNo);
Four BfM_SyncBufferUsingCoherencyVolume(Four, Four volNo);
Four BfM_UpdateCoherencyVolume(Four);
Four BfM_MountCoherencyVolume(Four, char*	devName);
Four BfM_DismountCoherencyVolume(Four);
Boolean BfM_IsCoherencyVolume(Four, Four volNo);
#endif

#ifdef USE_LOG_COHERENCY_VOLUME
Four BfM_InitCoherencyInfo(Four, Four);
Four BfM_FinalCoherencyInfo(Four);
Four BfM_SyncBufferUsingLogVolume(Four, Four, char*, char* );
Four BfM_UpdateCoherencyPageOfLogVolume(Four, char*, char* );
#endif

#endif /* _BFM_INTERNAL_H_ */
