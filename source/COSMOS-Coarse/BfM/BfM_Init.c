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
 * Module: BfM_Init.c
 *
 * Description :
 *  Initialize the data structure used in buffer manager.
 *  The used data structure is BufferInfo, one per each buffer pool.
 *  For each buffer pool, the required information is buffer size and
 *  the number of buffers in it. With these information, the needed
 *  memory for buffer pool, buffer table, and hash table is allocated.
 *  At last, all data structure is initiated with the intial value.
 *
 *  Note: In addtion, the BufferInfo data structure,called share memory 
 *        buffer can be allocated at shared memory. The buffer pool and buffer 
 *        table, hash table and mutex are allocated at shared memory. Through
 *        this shared memory, processes share the BufferInfo. The mutex
 *        allocated at shared memory is used for access control of the
 *        shared memory buffer. 
 *
 * Exports:
 *  Four BfM_Init(void)
 *   (Internally BfM_Init() use bfm_InitBufferInfo() function.)
 */

#ifdef USE_SHARED_MEMORY_BUFFER
#include <sys/shm.h>
#undef PAGESIZE		/* undef PAGESIZE defined in sys/shm.h	*/
#endif

#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

/*@ global variable */ 
#ifdef USE_SHARED_MEMORY_BUFFER
BufferInfo* 	 	    bufInfo; 		   /* buffer information for all types */
TCB*		    	    TCBPool;		   /* pointer of thread control block pool in shared memory */ 
PIB*		    	    processTable;	   /* pointer of process table in shared memory */

#ifdef COSMOS_MULTITHREAD
VolumeLockTable*	    volLockTable;	   /* volume lock table for multi-threading environment */
#endif

#else	/* USE_SHARED_MEMORY_BUFFER */
BufferInfo 		    bufInfo[NUM_BUF_TYPES];/* buffer information for all types */
#endif  /* USE_SHARED_MEMORY_BUFFER */

/*
 * Function: Four BfM_InitPerProcessDS(void)
 *
 * Description:
 *  Initialize the perProcessDS of buffer manager 
 * 
 * Note:
 *  In the case uses shared memory buffer, create a buffer in shared memory and 
 *  initialize it. The mutex for concurrency control of shared memory buffer is also created.
 *
 *  In the other case, uses local memory buffer. Create buffer in local memory of
 *  process and initialize it. 
 *  
 * Returns:
 *  error code
 *    some errors cased by function calls
 */
Four BfM_InitPerProcessDS(Four handle)
{
    Four      e;                        /* error number */
    Four      i;
    
    TR_PRINT(TR_BFM, TR1, ("BfM_Init(handle)"));

#ifdef USE_SHARED_MEMORY_BUFFER
    
    BFM_PER_PROCESS_DS.receivedSignalNo   = -1;

    /* Create shared memory buffer */
    e = bfm_CreateSharedMemoryBuffer(handle);
    if (e < eNOERROR) ERR(handle, e);
    
    /* The first process initialize the buffer information */
    if ((e = bfm_GetNumAttachedSHMBuffer(handle)) == 1) {
    /* initialize buffer information of PAGE_BUF */
	e = bfm_InitBufferInfo(handle, PAGE_BUF, PAGESIZE2, NUM_PAGE_BUFS);
	if (e < eNOERROR) ERR(handle, e); 

    /* initialize buffer information of LOT_LEAF_BUF */
	e = bfm_InitBufferInfo(handle, LOT_LEAF_BUF, LOT_LEAF_SIZE2, NUM_LOT_LEAF_BUFS);
	if (e < eNOERROR) ERR(handle, e); 

	/* initialize process table */
	e = bfm_InitProcessTable(handle);
	if (e < eNOERROR) ERR(handle, e);

#ifdef COSMOS_MULTITHREAD
    	for (i = 0; i < MAXNUMOFVOLS; i++) {
            RWLOCK_INIT(&(volLockTable[i].rwlock), &cosmos_thread_rwlock_attr, PTHREAD_PROCESS_SHARED);
    	}
#endif
    } 
    else if (e < eNOERROR) ERR(handle, e); 

    /* Allocate and initialize process table entry for this process. */
    e = bfm_AllocProcessTableEntry(handle);
    if (e < eNOERROR) ERR(handle, e);

#else
    /* Create local memory buffer */
    e = bfm_CreateLocalMemoryBuffer(handle);
    if (e < eNOERROR) ERR(handle, e);
    
    /* initialize buffer information of PAGE_BUF */
    e = bfm_InitBufferInfo(handle, PAGE_BUF, PAGESIZE2, NUM_PAGE_BUFS);
    if (e < eNOERROR) {
	free(BI_BUFTABLE(PAGE_BUF));
	free(BI_BUFFERPOOL(PAGE_BUF));
	free(BI_HASHTABLE(PAGE_BUF));
	ERR(handle, e);
    }
   
    /* initialize buffer information of LOT_LEAF_BUF */
    e = bfm_InitBufferInfo(handle, LOT_LEAF_BUF, LOT_LEAF_SIZE2, NUM_LOT_LEAF_BUFS );
    if (e < eNOERROR) {
	free(BI_BUFTABLE(LOT_LEAF_BUF));
	free(BI_BUFFERPOOL(LOT_LEAF_BUF));
	free(BI_HASHTABLE(LOT_LEAF_BUF));
	ERR(handle, e);
    }
#endif

#ifdef USE_COHERENCY_VOLUME
    BFM_PER_PROCESS_DS.bfm_CoherencyVolumeInfo.volNo = NO_COHERENCY_VOLUME;
#endif
 
   return (eNOERROR);
}  /* BfM_InitSharedMemory */

/*
 * Function: Four BfM_InitPerThreadDS(Four handle)
 *
 * Description:
 *  Initialize the perThreadDS of buffer manager 
 * 
 * Note:
 *  
 * Returns:
 *  error code
 *    some errors cased by function calls
 */

Four BfM_InitPerThreadDS(Four handle)
{
    Four e;

    /* for signal block */
    BFM_PER_THREAD_DS(handle).signalBlockCounter = 0;

    /* for bulk flush */
    BFM_PER_THREAD_DS(handle).bfm_flushBufferMemory = NULL;
    BFM_PER_THREAD_DS(handle).bfm_trainIdsMemory = NULL;
    BFM_PER_THREAD_DS(handle).bfm_pageIdSortBufferMemory = NULL;
    BFM_PER_THREAD_DS(handle).bfm_logBulkFlushesMemory = NULL;
    BFM_PER_THREAD_DS(handle).bfm_rawBulkFlushesMemory = NULL;

#ifdef USE_SHARED_MEMORY_BUFFER
    BFM_PER_THREAD_DS(handle).nMountedVols = -1;

#ifndef USE_MUTEX
    /* Allocate thread contol blocks for this thread. */
    e = bfm_AllocThreadCtrlBlock(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    /* Allocate lock control blocks for this thread. */
    e = bfm_AllocMyLockCBs(handle, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
 
    /* Initialize fixed buffer table. */
    e = bfm_InitFixedBufferInfo(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (eNOERROR);
} /* BfM_InitPerThreadDS */

#ifdef USE_SHARED_MEMORY_BUFFER
/*@================================
 * bfm_CreateSharedMemoryBuffer()
 *================================*/
/*
 * Function: Four bfm_CreateSharedMemory()
 * 
 * Description:
 *  Create shared memory buffer. 
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four bfm_CreateSharedMemoryBuffer(Four handle)
{
    Four 	    sharedMemBufSize;	/* size of shared memory */
    key_t	    shmkey;				/* key to access shared memory */ 
    char*	    shmkeyPtr;			/* pointer to environment value of COSMOS_SHARED_MEMORY_KEY */
    Four	    bufOffset;			/* location of buffer at shared memory */	
    Four	    e;					/* error code */
    Four	    shmBufferId;		/* shared memory buffer id */ 
   
    TR_PRINT(TR_BFM, TR1, ("bfm_CreateSharedMemoryBuffer(handle)"));
 
    /* 
     * Initialize bufInfo Structure before create shared memory buffer
     */
    bufInfo = NULL;

    /*@
     * Caculate shared memory buffer size 
     */
    /* Caculate shared memory buffer size */
    sharedMemBufSize = sizeof(BufferInfo) * NUM_BUF_TYPES + 		/* buffer information size of PAGE_BUF and LOT_LEAF_BUF */
					   sizeof(BufferTable) * NUM_PAGE_BUFS + 		/* buffer table size of PAGE_BUF */
					   NUM_PAGE_BUFS * PAGESIZE2 * PAGESIZE +		/* buffer pool size of PAGE_BUF */
					   ALIGNED_LENGTH(sizeof(Lock_ctrlBlock) * LOCK_CB_TABLESIZE_TO_NBUFS(NUM_PAGE_BUFS)) + /* lock control block table size */
					   ALIGNED_LENGTH(sizeof(Lock_hashEntry) * LOCKHASHTABLESIZE_TO_NBUFS(NUM_PAGE_BUFS)) + /* lock hash table size */
					   ALIGNED_LENGTH(sizeof(HashTable) * HASHTABLESIZE_TO_NBUFS(NUM_PAGE_BUFS)) + /* hash table size of PAGE_BUF */
					   
					   sizeof(BufferTable) * NUM_LOT_LEAF_BUFS +	/* buffer table size of LOT_LEAF_BUF */
					   NUM_LOT_LEAF_BUFS * LOT_LEAF_SIZE2 * PAGESIZE +	/* buffer pool size of LOT_LEAF_BUF */
					   ALIGNED_LENGTH(sizeof(Lock_ctrlBlock) * LOCK_CB_TABLESIZE_TO_NBUFS(NUM_LOT_LEAF_BUFS)) + /* lock control block table size */
					   ALIGNED_LENGTH(sizeof(Lock_hashEntry) * LOCKHASHTABLESIZE_TO_NBUFS(NUM_LOT_LEAF_BUFS)) + /* lock hash table size */
					   ALIGNED_LENGTH(sizeof(HashTable) * HASHTABLESIZE_TO_NBUFS(NUM_LOT_LEAF_BUFS)) + /* hash table size of LOT_LEAF_BUF */
					   ALIGNED_LENGTH(sizeof(PIB) * MAXPROCS); 		/* size of process table */

#ifndef USE_MUTEX	
    sharedMemBufSize += ALIGNED_LENGTH(sizeof(TCB) * TOTALTHREADS);	/* sizeo of thread control block pool */
#endif
#ifdef COSMOS_MULTITHREAD
    sharedMemBufSize += ALIGNED_LENGTH(sizeof(VolumeLockTable) * MAXNUMOFVOLS); /* size of volume lock table */
#endif

    /*@ 
     * Create a shared memory and initialize it 
     */
    /* Get a shared memory key */
    if ((shmkeyPtr = (char* )getenv("COSMOS_SHARED_MEMORY_BUFFER_KEY")) == NULL) {
        /* The COSMOS_SHARED_MEMORY_BUFFER_KEY is not in environment variables. */
    	/* Use COSMOS_SHARED_MEMORY_BUFFER_KEY constant */
    	shmkey = COSMOS_SHARED_MEMORY_BUFFER_KEY;
    }
    else {
        /* The COSMOS_SHARED_MEMORY_BUFFER_KEY is in environment variables. */
    	shmkey = atol(shmkeyPtr);
    }

    /* Create the shared memory and get the Id */
    /* If a shared memory does already exist, just reference it by the Id */
    BFM_PER_PROCESS_DS.shmBufferId = shmBufferId = shmget(shmkey, sharedMemBufSize, PERMS | IPC_CREAT); 
    if (shmBufferId < 0) ERR(handle, eSHMGETFAILED_BFM);

    /* Attach the shared memory at local memory of process. */
    /* bufInfo points the address of the attached shared memory. */
#if defined(USE_LOGICAL_PTR)
    bufInfo = (BufferInfo* )shmat(shmBufferId, NULL, 0);
#else
    bufInfo = (BufferInfo* )shmat(shmBufferId, (char *)0xaf64e000, SHM_RND);
#endif
    if (bufInfo == (BufferInfo* ) -1) {
	bufInfo = NULL;
	ERR(handle, eSHMATFAILED_BFM); 
    }

    /*@ 
     * Adjust the addresses of buffer table, buffer pool, hash table, and mutex 
     */
    /* skip buffer information structures */
    bufOffset = sizeof(BufferInfo) * NUM_BUF_TYPES;			
    BI_BUFTABLE(PAGE_BUF) = LOGICAL_PTR((BufferTable* )((char* )&bufInfo[0] + bufOffset));
	
    /* skip buffer table of PAGE_BUF */
    bufOffset += sizeof(BufferTable) * NUM_PAGE_BUFS;			
    BI_BUFFERPOOL(PAGE_BUF) = LOGICAL_PTR((char* )((char* )&bufInfo[0] + bufOffset)); 

    
    /* skip buffer pool of PAGE_BUF */
    bufOffset += NUM_PAGE_BUFS * PAGESIZE2 * PAGESIZE;			
    BI_LOCK_CB_TABLE(PAGE_BUF) = LOGICAL_PTR((Lock_ctrlBlock* )((char* )&bufInfo[0] + bufOffset)); 


    /* skip lock control block table of PAGE_BUF */
    bufOffset += ALIGNED_LENGTH(sizeof(Lock_ctrlBlock) * LOCK_CB_TABLESIZE_TO_NBUFS(NUM_PAGE_BUFS)); 
    BI_LOCKHASHTABLE(PAGE_BUF) = LOGICAL_PTR((Lock_hashEntry* )((char* )&bufInfo[0] + bufOffset)); 
    
    /* skip lock hash table of PAGE_BUF */
    bufOffset += ALIGNED_LENGTH(sizeof(Lock_hashEntry) * LOCKHASHTABLESIZE_TO_NBUFS(NUM_PAGE_BUFS)); 
    BI_HASHTABLE(PAGE_BUF) = LOGICAL_PTR((HashTable* )((char* )&bufInfo[0] + bufOffset)); 


    /* skip hash table of PAGE_BUF */
    bufOffset += ALIGNED_LENGTH(sizeof(HashTable) * HASHTABLESIZE_TO_NBUFS(NUM_PAGE_BUFS));	
    BI_BUFTABLE(LOT_LEAF_BUF) = LOGICAL_PTR((BufferTable* )((char* )&bufInfo[0] + bufOffset));

    /* skip buffer table of LOT_LEAF_BUF */
    bufOffset += sizeof(BufferTable) * NUM_LOT_LEAF_BUFS;		
    BI_BUFFERPOOL(LOT_LEAF_BUF) = LOGICAL_PTR((char* )((char* )&bufInfo[0] + bufOffset)); 

    /* skip buffer pool of LOT_LEAF_BUF */
    bufOffset += NUM_LOT_LEAF_BUFS * LOT_LEAF_SIZE2 * PAGESIZE;		
    BI_LOCK_CB_TABLE(LOT_LEAF_BUF) = LOGICAL_PTR((Lock_ctrlBlock* )((char* )&bufInfo[0] + bufOffset));

    /* skip lock control block table of LOT_LEAF_BUF */  
    bufOffset += ALIGNED_LENGTH(sizeof(Lock_ctrlBlock) * LOCK_CB_TABLESIZE_TO_NBUFS(NUM_LOT_LEAF_BUFS)); 
    BI_LOCKHASHTABLE(LOT_LEAF_BUF) = LOGICAL_PTR((Lock_hashEntry* )((char* )&bufInfo[0] + bufOffset)); 
    
    /* skip lock hash table of LOT_LEAF_BUF */
    bufOffset += ALIGNED_LENGTH(sizeof(Lock_hashEntry) * LOCKHASHTABLESIZE_TO_NBUFS(NUM_LOT_LEAF_BUFS)); 
    BI_HASHTABLE(LOT_LEAF_BUF) = LOGICAL_PTR((HashTable* )((char* )&bufInfo[0] + bufOffset)); 
    
    /* skip hash table of LOT_LEAF_BUF */
    bufOffset += ALIGNED_LENGTH(sizeof(HashTable) * HASHTABLESIZE_TO_NBUFS(NUM_LOT_LEAF_BUFS)); 
    processTable = (PIB* )((char* )&bufInfo[0] + bufOffset);

#ifndef USE_MUTEX
    /* skip process table */
    bufOffset += ALIGNED_LENGTH(sizeof(PIB) * MAXPROCS);
    TCBPool = (TCB* )((char* )&bufInfo[0] + bufOffset); 
#endif

#ifdef COSMOS_MULTITHREAD
#ifndef USE_MUTEX
    /* skip thread control block */
    bufOffset += ALIGNED_LENGTH(sizeof(TCB) * TOTALTHREADS); 
#else
    /* skip process table */
    bufOffset += ALIGNED_LENGTH(sizeof(PIB) * MAXPROCS);
#endif
    volLockTable = (VolumeLockTable* )((char* )&bufInfo[0] + bufOffset);
#endif

    return (eNOERROR);
} /* bfm_CreateSharedMemoryBuffer() */
#else

/*
 * Function: Four bfm_CreateLocalMemoryBuffer()
 *
 * Description:
 *  Create local memory buffer
 *
 * Returns:
 *  error code
 *    some errors cased by function calls
 */
Four bfm_CreateLocalMemoryBuffer(Four handle)
{
    Four e;	/* error code */
    
    TR_PRINT(TR_BFM, TR1, ("bfm_CreateLocalMemoryBuffer(handle)"));

    /* allocate memory for buffer table of PAGE_BUF */
    BI_BUFTABLE(PAGE_BUF) = (BufferTable *)malloc(sizeof(BufferTable) * NUM_PAGE_BUFS); 
    if (BI_BUFTABLE(PAGE_BUF) == NULL) ERR(handle, eMEMORYALLOCERR);
    
    /* allocate memory for buffer table of LOT_LEAF_BUF */
    BI_BUFTABLE(LOT_LEAF_BUF) = (BufferTable *)malloc(sizeof(BufferTable) * NUM_LOT_LEAF_BUFS); 
    if (BI_BUFTABLE(LOT_LEAF_BUF) == NULL) ERR(handle, eMEMORYALLOCERR);

    /* allocate memory for buffer pool of PAGE_BUF */
    BI_BUFFERPOOL(PAGE_BUF) = (char *)malloc(NUM_PAGE_BUFS * PAGESIZE2 * PAGESIZE);
    if (BI_BUFFERPOOL(PAGE_BUF) == NULL) ERR(handle, eMEMORYALLOCERR);
    
    /* allocate memory for buffer pool of LOT_LEAF_BUF */
    BI_BUFFERPOOL(LOT_LEAF_BUF) = (char *)malloc(NUM_LOT_LEAF_BUFS * LOT_LEAF_SIZE2 * PAGESIZE);
    if (BI_BUFFERPOOL(LOT_LEAF_BUF) == NULL) ERR(handle, eMEMORYALLOCERR);

    /* allocate memory for hash table of PAGE_BUF */
    BI_HASHTABLE(PAGE_BUF) = (Four* )malloc(sizeof(Four) * HASHTABLESIZE_TO_NBUFS(NUM_PAGE_BUFS));
    if (BI_HASHTABLE(PAGE_BUF) == NULL) ERR(handle, eMEMORYALLOCERR);
    
    /* allocate memory for hash table of LOT_LEAF_BUF */
    BI_HASHTABLE(LOT_LEAF_BUF) = (Four* )malloc(sizeof(Four) * HASHTABLESIZE_TO_NBUFS(NUM_LOT_LEAF_BUFS));
    if (BI_HASHTABLE(LOT_LEAF_BUF) == NULL) ERR(handle, eMEMORYALLOCERR);

    return (eNOERROR);
} /* bfm_CreateLocalMemoryBuffer() */
#endif

/*@================================
 * bfm_InitBufferInfo()
 *================================*/
/*
 * Function: Four bfm_InitBufferInfo(Four, Two, Two)
 * 
 * Description:
 *  Initialize the BufferInfo data structure.
 *
 * Note: Some codes are moved to bfm_CreateLocalMemoryBuffer(). 
 *
 * Returns:
 *  error code
 *    eNOMEM_BFM - memory allocation failed
 *    some errors caused by function calls
 *
 * Note: eNOMEM_BFM error code is not neccesary. 
 *
 */
Four bfm_InitBufferInfo(
    Four handle,
    Four type,			/* IN buffer type */
    Four bufSize, 		/* IN buffer size */
    Four nBufs)			/* IN number of buffers */
{
    Four e;				/* error code */
    Four  i;			/* index variable */
    
    TR_PRINT(TR_BFM, TR1, ("bfm_InitBufferInfo(handle)"));

    /* set the control values */
    BI_BUFSIZE(type) = bufSize;
    BI_NBUFS(type) = nBufs;
    BI_NEXTVICTIM(type) = 0;

    /*@ initialize the buffer table */
    for (i = 0; i < nBufs; i++) {
	/* reset dirty/refer bit */
	BI_BITS(type, i) = ALL_0;

	/* fixed counter is reset to 0. */
	BI_NFIXED(type, i) = 0;

	/* hash key is initialized to NIL value. */
	SET_NILBFMHASHKEY(BI_KEY(type, i));

#if   defined(USE_SHARED_MEMORY_BUFFER) &&  defined(USE_MUTEX)	
	/* Initialize mutex. */
	MUTEX_INIT(&BI_BUFTABLE_ENTRY(type, i).mutex, &cosmos_thread_mutex_attr, PTHREAD_PROCESS_SHARED);
#elif defined(USE_SHARED_MEMORY_BUFFER) && !defined(USE_MUTEX)
	/* Initialize latch. */
	e = bfm_InitLatch(handle, &BI_BUFTABLE_ENTRY(type, i).latch);
	if (e < eNOERROR) ERR(handle, e);
#endif
    }

    /*@ Initialize the hash table */
    e = bfm_InitHashTable(handle, type);
    if (e < eNOERROR) {
	ERR(handle, e);
    }

#ifdef USE_SHARED_MEMORY_BUFFER	
    /*@ Initialize the lock hash table. */
    e = bfm_InitLockHashTable(handle, type);		
    if (e < eNOERROR) ERR(handle, e);		
#endif

    return(eNOERROR);
} /* bfm_InitBufferInfo() */

#ifdef USE_SHARED_MEMORY_BUFFER
/*@================================
 * bfm_GetNumAttachedSHMBuffer()
 *================================*/
/*
 * Function: Four bfm_GetNumAttachedSHMBuffer()
 * 
 * Description:
 *  Obtain the number of attached shared memory buffer.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four bfm_GetNumAttachedSHMBuffer(Four handle)
{
    struct 		shmid_ds shmInfo;		/* structure to get current shared memory status */
    Four	    e;						/* error code */

    TR_PRINT(TR_BFM, TR1, ("bfm_GetNumAttachedSHMBuffer(handle)"));

    /* Get the number of processes attach the shared memory */
    e = shmctl(BFM_PER_PROCESS_DS.shmBufferId, IPC_STAT, &shmInfo); 
    if (e < 0) ERR(handle, eSHMCTLFAILED_BFM);

    return shmInfo.shm_nattch;
} /* bfm_GetNumAttachedSHMBuffer() */

#endif
