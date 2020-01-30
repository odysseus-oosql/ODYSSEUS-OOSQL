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
#ifndef __BFM_H__
#define __BFM_H__

#include "Util.h"


/* number of buffer types : number of buffer pools used */
#define NUM_BUF_TYPES 	2

/* Buffer Types */
#define PAGE_BUF    	0
#define TRAIN_BUF 	1
#define LOG_BUF 	2

/*
 * Buffer Manager Interface Type
 */
typedef struct _buffer_acc_cb Buffer_ACC_CB;

struct _buffer_acc_cb {

    PageID 	key;		/* id of page */
    char 	*bufPagePtr;	/* base address of page in bufferpool */
    LATCH_TYPE	*latchPtr;	/* pointer to the latch for the page */
    Two		dirtyFlag;	/* flag says caller modified page */
    One		invalidFlag;	/* flag says caller destroyed page */
    Two         type;		/* Buffer type */
    Four    	latchFlag;      /* flag says caller got the latch on this buffer page */
    Buffer_ACC_CB *prev;	/* pointer to the previous block by this process */
    Buffer_ACC_CB *next;	/* pointer to the next block by this process */

};


/* macro to detect a bad buffer type value */
#define IS_BAD_BUFFERTYPE(type) (type < 0 || type >= NUM_BUF_TYPES)

/* The structure of key type used at hashing in buffer manager */
/* same as "typedef BfMHashKey PageID; */
typedef PageID BfMHashKey;

#define SET_NILBFMHASHKEY(key) (SET_NILPAGEID(key))
#define IS_NILBFMHASHKEY(key)  (IS_NILPAGEID(key))

#define R_MOD(type) (HASHTABLESIZE(type)*3)

#define BFM_HASH(k, temp,type)       \
(((k)->volNo + (k)->pageNo) % HASHTABLESIZE(type))

/* If k1 = k2 then true, otherwise false */
#define EQUALKEY(k1, k2) \
	  (((k1)->volNo == (k2)->volNo) && ((k1)->pageNo == (k2)->pageNo))

/* The structure of locking mechanism for buffer manager */
/* to avoid deadlock situation in buffer manager */

typedef struct lock_ctrlBlock Lock_ctrlBlock;
typedef struct lock_hash_entry Lock_hashEntry;

struct lock_ctrlBlock {
    TrainID key;
    Four lockCounter;
    LATCH_TYPE latch;
    LOGICAL_PTR_TYPE(Lock_ctrlBlock *) nextHashChain;
};

struct lock_hash_entry {
    LATCH_TYPE      latch;
    LOGICAL_PTR_TYPE(Lock_ctrlBlock *) blockPtr;
    LOGICAL_PTR_TYPE(Lock_ctrlBlock *) freeBlockPtr;
};

/* The structure of BufferTable which is used in buffer replacement algo. */
typedef struct buftblentry BufTBLEntry;

struct buftblentry {
    BfMHashKey  key;		/* identify a page */
    Lsn_T	recLsn;		/* for recovery */

    Two         fixed;		/* fixed count */
    unsigned	dirtyFlag:1;	/* whether the buffer page is modified */
    unsigned 	invalidFlag:1;	/* whether the buffer page is invalid or not */
    unsigned	referFlag:1;    /* whether the buffer page is refered or not */
    LATCH_TYPE  latch;		/* Mutex for access of buffer table entry & page */

    LOGICAL_PTR_TYPE(BufTBLEntry *) nextHashChain; /* next element of the hash chain */ 

};


/* The structure of HashTable which is used for searching */
typedef struct {
    LATCH_TYPE  latch;		/* latch for access hash chain */
    LOGICAL_PTR_TYPE(BufTBLEntry *) entryPtr; /* pointer to buffer table entry */
} bfmHashEntry;


/* type definition for buffer pool information */
typedef struct {
    Two          bufSize;	/* size of a buffer in page size */
    Two          nBufs;		/* # of buffers in this buffer pool */

    /* locking mechanism in buffer manager */
    Pool         lock_cb_pool; /* pool of lock control block */
    LOGICAL_PTR_TYPE(Lock_hashEntry *) lockHashTable; /* hash table for lock control block */

    LOGICAL_PTR_TYPE(BufTBLEntry *) bufTable; /* for */ 
    LOGICAL_PTR_TYPE(char *) bufferPool; /* a set of buffers */
    LOGICAL_PTR_TYPE(bfmHashEntry *) hashTable;	/* hash table */

    UFour        nextVictim;     /* index of NextVictim in Buffer */
} BufferInfo;


/* constant definition: The BfMHashKey don't exist in the hash table. */
#define NOTFOUND_IN_HTABLE  -1

/*
 * Buffer Manager Interface Type
 */

#define BFM_BACB_POOLSIZE 31

/* see the "RM.h" for the definition */

#define ADD_BACB_INTO_MYFIXEDBACB(_handle, bacb) \
        (bacb)->next = perThreadTable[_handle].bfmDS.MyFixed_BACB.next; \
	(bacb)->next->prev = (bacb); \
        perThreadTable[_handle].bfmDS.MyFixed_BACB.next = (bacb); \
        (bacb)->prev = &(perThreadTable[_handle].bfmDS.MyFixed_BACB);

#define DELETE_BACB_FROM_MYFIXEDBACB(bacb) \
        (bacb)->next->prev = (bacb)->prev; \
	(bacb)->prev->next = (bacb)->next;

#define BFM_FREEBUFFER(_handle, bcbp, type, e)  \
BEGIN_MACRO \
if( (e = SHM_releaseLatch(_handle, (bcbp)->latchPtr, procIndex) ) < eNOERROR) \
    (Four) BfM_unfixBuffer((_handle), (bcbp), (type)); \
else \
    e = BfM_unfixBuffer((_handle), (bcbp), type); \
END_MACRO

/* Check the validity of the given key, some restrictions may be added. */
#define CHECKKEY(k)  \
BEGIN_MACRO \
if( ((k)->volNo < 0) || ((k)->pageNo < 0) ) return( eBADHASHKEY_BFM ); \
END_MACRO


/* Global variable used for buffer manager
 * This variable's definition is in file BfM_Init.c.
 */


/*** BEGIN_OF_SHM_RELATED_AREA ***/
/*
 * Shared Memory Structure
 */
typedef struct {
    BufferInfo	bufInfo[NUM_BUF_TYPES];
} BfM_SHM;

extern BfM_SHM *bfm_shmPtr;
extern Four procIndex;

/* define statement for Buffer Manager */

#define BI_BUFSIZE(type)	 (bfm_shmPtr->bufInfo[type].bufSize)
#define BI_NBUFS(type)		 (bfm_shmPtr->bufInfo[type].nBufs)
#define BI_BUFTABLE(type)	 (bfm_shmPtr->bufInfo[type].bufTable)
#define BI_BTENTRY(type, idx)    (((BufTBLEntry*)PHYSICAL_PTR(bfm_shmPtr->bufInfo[type].bufTable))[idx]) 

#define BI_NEXTVICTIM(type)      (bfm_shmPtr->bufInfo[type].nextVictim)

/* for buffer pool */
#define BI_BUFFERPOOL(type)	 (bfm_shmPtr->bufInfo[type].bufferPool)
#define BI_BUFFER(type, entry)	 ( (char*)PHYSICAL_PTR(BI_BUFFERPOOL(type)) + BI_BUFSIZE(type)*PAGESIZE*(entry-&BI_BTENTRY(type,0))) 

/* for buffer hash table */
#define BI_HASHTABLE(type)	 (bfm_shmPtr->bufInfo[type].hashTable)
#define BI_HASHENTRY(type, idx)  (((bfmHashEntry*)PHYSICAL_PTR(bfm_shmPtr->bufInfo[type].hashTable))[idx]) 

/* for buffer lock hash table --- BfM_lock(), BfM_unlock() */
#define BI_LOCKHASHTABLE(type)	 (bfm_shmPtr->bufInfo[type].lockHashTable)
#define BI_LOCKHASHENTRY(type, idx)  (((Lock_hashEntry*)PHYSICAL_PTR(bfm_shmPtr->bufInfo[type].lockHashTable))[idx]) 
#define BI_LOCK_CB_POOL(type)    (bfm_shmPtr->bufInfo[type].lock_cb_pool)

/* The size of the hash table is four times of the size of the buffer pool
 * in order to minimize the rate of collisions.
 * Each entry of the hash table corresponds to an index of the buffer pool
 * as well as the buffer table.
 */
#define HASHTABLESIZE(type)             (BI_NBUFS(type)*4 + 1)
#define BFM_LOCKHASHTABLESIZE(type)     (BI_NBUFS(type)*5 + 1)
#define BFM_LOCK_CB_POOLSIZE(type)      (BI_NBUFS(type)*3 + 1) 
/*** END_OF_SHM_RELATED_AREA ***/

/*
 * Function Prototypes
 */
Four bfm_allocBuffer(Four, Four, BufTBLEntry **);
Four bfm_delete(Four, BufTBLEntry *, Four);
Four bfm_finalBufferInfo(Four, Four);
Four bfm_flushBuffer(Four, BufTBLEntry *, Four);
Four bfm_initBufferInfo(Four, Four, Four, Four);
Four bfm_initBufferHashTable(Four, Four);
Four bfm_initLockHashTable(Four, Four);
Four bfm_insert(Four, BfMHashKey *, BufTBLEntry *, Four);
Four bfm_lookUp(Four, BfMHashKey *, Four, BufTBLEntry **);
Four bfm_readBuffer(Four, TrainID *, char *, Four);
void bfm_dump_buffertable(Four, Four);
void bfm_dump_hashtable(Four, Four);
Four BfM_dismount(Four, Four);

char *BfM_Err(Four);
Four BfM_finalSharedDS(Four);
Four BfM_finalLocalDS(Four);
Four BfM_FlushTrain(Four, TrainID*, Four); 
Four BfM_unfixBuffer(Four, Buffer_ACC_CB *, Four);
Four BfM_unfixMyBACB(Four);
Four BfM_getAndFixBuffer(Four, TrainID *, Four, Buffer_ACC_CB **, Four);
Four BfM_fixNewBuffer(Four, TrainID *, Four, Buffer_ACC_CB **, Four);
Four BfM_initSharedDS(Four);	
Four BfM_initLocalDS(Four);
Four BfM_UpdateDirtyPageTableEntries(Four); 
Four BfM_LogDirtyPageTableEntries(Four);
Four BfM_readTrain(Four, TrainID *, char *, Four);
Four BfM_RemoveLogPages(Four);
Four BfM_RemoveTrain(Four, TrainID*, Four, Boolean);

/* reduce # of useless function call request in COSMOS-CC/SINGLE */
#ifndef SINGLE_USER
Four bfm_lock(Four, TrainID *, Four);
Four bfm_unlock(Four, TrainID *, Four);
#else
#define bfm_lock(_handle, _x, _y)    eNOERROR
#define bfm_unlock(_handle, _x, _y)  eNOERROR
#endif

#endif /* __BFM_H__ */
