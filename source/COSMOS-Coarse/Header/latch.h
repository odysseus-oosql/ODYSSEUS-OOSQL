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
#ifndef __LATCH_H__
#define __LATCH_H__

#include "common.h"
#include "Util_varArray.h"
#include "THM_lock.h"

#ifndef __LOGICAL_PTR__
#define __LOGICAL_PTR__

#ifdef USE_LOGICAL_PTR

#define SHM_BASE_ADDRESS (bufInfo)

#ifdef CHECK_LOGICAL_PTR
/* This section is for compilation check. Do not use as a binary. */

typedef struct {MEMORY_ALIGN_TYPE i;} LogicalPtr_T;

extern LogicalPtr_T dummyNullLogicalPtr;

#define LOGICAL_PTR_TYPE(_type) LogicalPtr_T
#define LOGICAL_PTR(_p) (dummyNullLogicalPtr)
#define PHYSICAL_PTR(_p) ((void*)((_p.i) + (MEMORY_ALIGN_TYPE)SHM_BASE_ADDRESS))
#define NULL_LOGICAL_PTR (dummyNullLogicalPtr)

#else /* CHECK_LOGICAL_PTR */

typedef MEMORY_ALIGN_TYPE LogicalPtr_T;

#define LOGICAL_PTR_TYPE(_type) LogicalPtr_T
#define LOGICAL_PTR(_p) ((_p == NULL) ? NULL_LOGICAL_PTR : ((LogicalPtr_T)(_p) - (LogicalPtr_T)SHM_BASE_ADDRESS))
#define PHYSICAL_PTR(_p) ((_p == NULL_LOGICAL_PTR) ? (void*)NULL : ((void*)((LogicalPtr_T)(_p) + (LogicalPtr_T)SHM_BASE_ADDRESS)))
#define NULL_LOGICAL_PTR ((LogicalPtr_T)0x0)

#endif /* CHECK_LOGICAL_PTR */

#else /* USE_LOGICAL_PTR */

/* Use physical address. */

#define LOGICAL_PTR_TYPE(_type) _type
#define LOGICAL_PTR(_p) (_p)
#define PHYSICAL_PTR(_p) (_p)
#define NULL_LOGICAL_PTR NULL

#endif /* USE_LOGICAL_PTR */
#endif /* __LOGICAL_PTR__ */


#ifdef NOTTESTANDSET
#define IDLE 		0
#define INCS 		1
#define WAITING 	2
#endif


/*
 * Type Definition for Latch Conditions
 */
#define NUM_LATCH_MODES 3
typedef enum LatchMode_tag {
    M_FREE=0x0,
    M_SHARED=0x1,
    M_EXCLUSIVE=0x2
} LatchMode;

typedef enum LatchConditional_tag {
    M_UNCONDITIONAL=0x1,
    M_CONDITIONAL=0x2,
    M_INSTANT=0x4
} LatchConditional;


#define BFM_BUSYLATCH   5

typedef struct pcell PIB;   /* PIB - process information block structure */

struct pcell {
    Four    procIndex;            /* process ID */
};

typedef struct tcell TCB;   /* TCB - thread control block structure */

struct tcell{
    cosmos_thread_sem_t     	semID;      		/* semaphore ID for this thread */
    cosmos_thread_semName_t    	semName[MAXSEMAPHORENAME]; /* semaphore ID for this thread */

    /* data structure for latch algorithm */
    LatchMode                   mode;                   /* latch request mode. old: Four mode */
    LatchConditional            condition;              /* check whether M_INSTANT or not. old: Four condition */
    LOGICAL_PTR_TYPE(TCB *) 	next;      		/* used for latch->queue structure */
    Four        		nGranted;   		/* number of granted latch */
    VarArray    		*grantedLatchStruct; 	/* keep information for granted latch */
};

typedef struct GlobalHandle_T {
    Four    procIndex;
    Four    threadIndex;
} GlobalHandle;

typedef struct {

#ifndef NOTTESTANDSET
    One_Invariable		sync;		/* testandset target */
    One				dummy;		/* alignment */
#else
    GlobalHandle   		turn;
    GlobalHandle   		globalHandle;
    Four   			flags[MAXPROCS][MAXTHREADS];
#endif
    LatchMode                   mode;           /* M_FREE, M_SHARD or M_EXCLUSIVE. */
    Two    			latchCounter;	/* number of granted */
    GlobalHandle    		grantedGlobalHandle;	/* index of process which owns this latch */
    LOGICAL_PTR_TYPE(TCB *) 	queue; /* waiting queue */ 

    cosmos_thread_mutex_t       mutex; 

} LATCH_TYPE;

typedef struct _latchEntry  LatchEntry;

struct _latchEntry {

    Four       counter;		/* number of granted */
    LATCH_TYPE *latchPtr;	/* granted Latch pointer  */
};

#define MAXLATCHENTRIES 60	/* latch entry maximum (initial value) */

#define IS_BAD_LATCHMODE(mode)  (mode < 0 || mode >= NUM_LATCH_MODES)

#endif /* __LATCH_H__ */

