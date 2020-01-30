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
#ifndef __LM_H__
#define __LM_H__

#include "Util_pool.h"
#include "common.h"

#define	MAXLOCKBUCKET		4000
#define	MAXXACTBUCKET		2400
#define	MAXREQUESTNODE		20000
#define	MAXLOCKQUEUEENTRY	2400

#define	MAXLOCKHASHENTRY	(MAXLOCKBUCKET*2)+3
#define MAXXACTHASHENTRY	(MAXXACTBUCKET*2)+3


#define N_LEVEL 		6        /* L_OBJECT/L_FLAT/L_FILE/L_PAGE/L_KEYVALUE */ 

struct keyValueLockID {
    MLGF_HashValue 	value;
    Four 		cipher;
    Four 		radix;
};

typedef union TargetID TargetID;

typedef struct rnode RequestNode_Type;
typedef struct lbucket LockBucket_Type;
typedef struct xbucket  XactBucket_Type;
typedef PageID KeyValueLockID; 

union TargetID {
    PageID  		pageID;
    FileID  		fileID;
    ObjectID 		objectID;
    KeyValueLockID 	keyValue;
};

struct rnode {
    XactID		xactID;		/* transaction identifier */
    Four		counter; 	/* granted lock counter by this transaction */
    LockMode		mode;	 	/* mode requested (and granted) */
    LockMode		oldMode;	/* if convert wait, mode before converting */
    LockDuration	duration; 	/* requested lock duration */
    LockStatus		status;	  	/* GRANTED, WAITING, CONVERTING, DENIED */
    Four                nLowLocks;       /* number of lowe lock */
    LOGICAL_PTR_TYPE(TCB *) xcbPtr; 	/* transaction to wakeup when lock is granted */

    LOGICAL_PTR_TYPE(RequestNode_Type *) prev; /* prev free request node pointer or header pointer */
    LOGICAL_PTR_TYPE(RequestNode_Type *) next; /* next free/nonfree request node pointer */ 

    LOGICAL_PTR_TYPE(LockBucket_Type *)  lockHDR; 
    LOGICAL_PTR_TYPE(RequestNode_Type *) prevGrantedEntry; /* previous RequestNode  pointer */
    LOGICAL_PTR_TYPE(RequestNode_Type *) nextGrantedEntry; /* next RequestNode  pointer */ 

    LOGICAL_PTR_TYPE(RequestNode_Type *) higherRequestNode; /* higher requestNode pointer */ 
    LOGICAL_PTR_TYPE(XactBucket_Type *)  xactBucketPtr;	/* the transaction bucket pointer */

};


struct lbucket {

    LATCH_TYPE 		latch;		/* exclusive semaphore on object information */
    TargetID	        target;		/* lock object identifier */
    LockLevel		level;		/* L_FILE/L_PAGE or L_FLAT */
    LockMode		groupMode; 	/* granted lock type */

    Four		nWaiting; 	/* number of waiting request */
    LockStatus		status;	  	/* L_GRANTED/L_WAITING/L_CONVERTING */
    LOGICAL_PTR_TYPE(RequestNode_Type *) queue; /* request queue */ 


    /* for keeping the hierarchy protocol */

    LOGICAL_PTR_TYPE(LockBucket_Type *) lowerLock; /* pointer to the lower level lock bucket */
    LOGICAL_PTR_TYPE(LockBucket_Type *) higherLock; /* pointer to the higher level lock bucket */ 
    LOGICAL_PTR_TYPE(LockBucket_Type *) prevSetLock; /* doubly linked list of same level lock bucket */ 
    LOGICAL_PTR_TYPE(LockBucket_Type *) nextSetLock; /* doubly linked list of same level lock bucket */

    /* chaining - overflow handling */

    LOGICAL_PTR_TYPE(LockBucket_Type *) prev; /* previous lock bucket pointer */ 
    LOGICAL_PTR_TYPE(LockBucket_Type *) next; /* next lock bucket pointer */ 

};


typedef struct lockhashentry LockHashEntry;

struct lockhashentry {

    LATCH_TYPE 		                latch;		/* exclusive semaphore on chain */
    LOGICAL_PTR_TYPE(LockBucket_Type *) bucketPtr; 	/* LockBucket_Type anchor */ 

};


struct xbucket {

    XactID		xactID;		  /* transaction identifier */
    Four		nLock[N_LEVEL];	  /* number of lock granted */
    Four		nUnlock[N_LEVEL]; /* number of lock released */
    Four                maxLowLocks;      /* max number of low locks on a file */ 
    XactStatus		status;		  /* waiting or not */
    ConcurrencyLevel    ccLevel;          /* concurrency level of this transaction */ 

    LOGICAL_PTR_TYPE(RequestNode_Type *) grantedList[N_LEVEL]; /* anchor of granted request node  */ 
    LOGICAL_PTR_TYPE(RequestNode_Type *) startAction[N_LEVEL]; /* anchor of start action node  */
    LOGICAL_PTR_TYPE(LockBucket_Type *)  waitingLock; /* anchor of waiting lock */ 


    /* chaining - overflow handling */

    LOGICAL_PTR_TYPE(XactBucket_Type *) prev; /* previous transaction bucket pointer */
    LOGICAL_PTR_TYPE(XactBucket_Type *) next; /* next transaction bucket pointer*/

#ifndef WFG
    /* Waits for Graph(WFG) for deadlock detection */
    /* binary tree representation of trees */

    LOGICAL_PTR_TYPE(XactBucket_Type *) waitingfor; /* this transaction is waiting for waitingfor transaction */
    Four                                deadlockVisit;  /* deadlock detection flag */
#endif
};

/* Stack type to detect deadlock */
typedef struct {
    XactBucket_Type 	*xactBucket;   	/* transaction bucket pointer */
    Four            	cost;           /* the cost for aborting this transaction */
} lm_DeadlockStackEntry;

#define MAXDEADLOCKCHAIN 	TOTALTHREADS 
#define INITDEADLOCKSTACKTOP 	0

/* macro for deadlock detection */
#define NUMOFGETLOCK(p) ((p)->nLock[L_FLAT_PAGE] + (p)->nLock[L_FLAT_OBJECT] + (p)->nLock[L_FILE] + (p)->nLock[L_PAGE] + (p)->nLock[L_KEYVALUE]- \
             ((p)->nUnlock[L_FLAT_PAGE] + (p)->nLock[L_FLAT_OBJECT] + (p)->nUnlock[L_FILE] + (p)->nUnlock[L_PAGE] + (p)->nUnlock[L_KEYVALUE]))

typedef enum { DLS_OK, DLS_DEADLOCK } DeadlockStatus; /* deadlock status */


typedef struct xacthashentry XactHashEntry;

struct xacthashentry {

    LATCH_TYPE                       	latch;      	/* exclusive semaphore on chain */
    LOGICAL_PTR_TYPE(XactBucket_Type *) bucketPtr; 	/* xactbucket anchor */ 

};


/*** BEGIN_OF_SHM_RELATED_AREA ***/
/*
 * Shared Memory Structure
 */
typedef struct {

    /* Per lock object information */
    LATCH_TYPE    	lmLatch;

    LATCH_TYPE    	maxLockOnFileLatch;
    Four          	maxLockOnFile;

    Pool      		requestNodePool; 		 /* request node pool */
    Pool      		lockBucketPool; 		 /* lock bucket pool */
    LockHashEntry 	lockHashTable[MAXLOCKHASHENTRY]; /* lock hash table */

    /* Per transaction information */

    Pool      		xactBucketPool; 		 /* transaction bucket pool */
    XactHashEntry 	xactHashTable[MAXXACTHASHENTRY]; /* transaction hash table */

} LM_SHM;

extern LM_SHM *lm_shmPtr;
extern Four procIndex;

#define LM_XACTHASHTABLE    		lm_shmPtr->xactHashTable
#define LM_LOCKHASHTABLE    		lm_shmPtr->lockHashTable
#define LM_LOCKBUCKETPOOL   		lm_shmPtr->lockBucketPool
#define LM_XACTBUCKETPOOL   		lm_shmPtr->xactBucketPool
#define LM_REQUESTNODEPOOL  		lm_shmPtr->requestNodePool
#define LM_LATCH                	lm_shmPtr->lmLatch
#define LM_MAX_LOCKS_ON_FILE_LATCH   	lm_shmPtr->maxLockOnFileLatch 
#define LM_MAX_LOCKS_ON_FILE    	lm_shmPtr->maxLockOnFile

/*** END_OF_SHM_RELATED_AREA ***/


/*
 * Internal Function Prototypes
 */
Four find_xactBucket(Four, XactID*, XactBucket_Type**);
Four lm_deleteLockBucketFromChain(Four, LockHashEntry*, LockBucket_Type*);
Four wakeup_nextRequest(Four, LockBucket_Type*);
Four check_lowerLevelLock(Four, LockBucket_Type*, XactBucket_Type*);
Four disconnect_hierarchy(Four, LockBucket_Type*);

Four find_nextModeAndRequest(Four, LockBucket_Type*,LockMode*,RequestNode_Type**);
void chk_lockStatusWithoutARequest(Four, LockBucket_Type*,RequestNode_Type*,LockMode*, Boolean*);
Four lm_getKeyValueLock(Four, XactID*, KeyValueLockID*, LockMode, LockDuration,
			LockConditional, LockReply*);
Four lm_getLock(Four, XactID*, TargetID*, LockLevel, FileID*, LockMode,
		LockDuration, LockConditional, Boolean, LockReply *,
		XactBucket_Type**, LockBucket_Type**, LockMode*);
Four lm_releaseLock(Four, XactID*, TargetID*, LockLevel, LockDuration);
Four lm_allocAndInitLockBucket(Four, LockLevel, TargetID*, LockHashEntry*, LockBucket_Type**);
Four lm_findLockBucketAndRequestNode(Four, XactID*, TargetID*, LockLevel, LockHashEntry*, LockBucket_Type**, RequestNode_Type**);
Four lm_allocAndInsertIntoQueue(Four, XactID*, XactBucket_Type*, LockBucket_Type*, LockMode, LockDuration, LockStatus, RequestNode_Type**);
Four lm_escalateLockOnFile(Four, XactID*, FileID*, LockMode, LockDuration, LockConditional, LockReply*);
Four lm_lockEscalation(Four, XactBucket_Type*);
Four lm_getMaxLocksOnFile(Four, Four *);
Four lm_resetMaxLocksOnFile(Four);

Boolean lm_needLockEscalation(Four);




/*
 * External Function Prototypes
 */
Four LM_cleanupLockTBL(Four);
Four LM_createXactBucket(Four, XactID*);
Four LM_detectDeadlock();
Four LM_dropXactBucket(Four, XactID*);
Four LM_getFileLock(Four, XactID*, FileID*, LockMode, LockDuration, LockConditional, LockReply *, LockMode *); 
Four LM_getFlatPageLock(Four, XactID*, PageID*, LockMode, LockDuration, LockConditional, LockReply *, LockMode *); 
Four LM_getFlatObjectLock(Four, XactID*, ObjectID*, LockMode, LockDuration, LockConditional, LockReply *, LockMode *); 
Four LM_getKeyRangeLock(Four, XactID*, PageID *, LockMode, LockDuration,
		       LockConditional, LockReply *);
Four LM_getPageLock(Four, XactID*, PageID *, FileID*, LockMode, LockDuration,
		    LockConditional, LockReply *, LockMode *); 
Four LM_getObjectLock(Four, XactID*, ObjectID *, FileID*, LockMode, LockDuration,
		    LockConditional, LockReply *, LockMode *); 
Four LM_initLocalDS(Four);
Four LM_initSharedDS(Four);
Four LM_finalLocalDS(Four);
Four LM_finalSharedDS(Four);
Four LM_initLockTBL(Four);
Four LM_initXactBucket(Four, XactID*, ConcurrencyLevel); 
Four LM_releaseFileLock(Four, XactID*, FileID*, LockDuration);
Four LM_releaseFlatPageLock(Four, XactID*, PageID*, LockDuration);
Four LM_releaseKeyRangeLock(Four, XactID*, PageID*, LockDuration); 
Four LM_releaseFlatObjectLock(Four, XactID*, ObjectID*, LockDuration);
Four LM_releasePageLock(Four, XactID*, PageID*, LockDuration);
Four LM_releaseObjectLock(Four, XactID*, ObjectID*, LockDuration);
Four LM_releaseXactFlatLock(Four, XactID*);
Four LM_releaseXactManualFlatLock(Four, XactID*);
Four LM_releaseXactLock(Four, XactID*);
Four LM_beginAction(Four, XactID*, Four); 
Four LM_endAction(Four, XactID*, Four); 
/* 
 * log & recover locks of lock table entries of prepared transaction for two phase commit
 */
Four LM_logLocksOfPreparedXact(Four, XactID*);
Four LM_recoverLocksOfPreparedXact(Four, LOG_LogRecInfo_T*);


#endif /* _LM_H_ */

