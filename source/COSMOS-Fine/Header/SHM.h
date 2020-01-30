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
#ifndef __SHM_H__
#define __SHM_H__

#ifndef WIN32
#include <sys/ipc.h>
#else
typedef int     key_t;
#endif
#include "common.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "RDsM.h"
#include "BfM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "Util.h"


#ifdef LRDS_INCLUDED
#include "LRDS.h"
#endif

#ifdef DEMON_PROCESS
#define NUM_DEMONS      1
#else /* DEMON_PROCESS */
#define NUM_DEMONS      0
#endif /* DEMON_PROCESS */

#define	CONFIRMED  	999
#define	NOTCONFIRMED  	444

#define MAXFREESPACE 536870912


#define PERMS   0666            /* shared mememory/semaphore permissions */


typedef struct shm_ArgForInitFn_T_tag {
    CfgParams_T 	*cfgParams;
    ComponentInfo_T 	*componentInfos;
    Four 		nComponents;
} shm_ArgForInitFn_T;

typedef struct shm_ArgForFinalFn_T_tag {
    CfgParams_T 	*cfgParams;
    ComponentInfo_T 	*componentInfos;
    Four nComponents;
} shm_ArgForFinalFn_T;


typedef struct SemStruct_tag {

#ifdef TRACE
Four  sequence;
#endif

/* Common Data Structure -------------------------------------------------------- */

    COMMON_SHM	commonDS;

/* LOG Data Structure -------------------------------------------------------- */

    LOG_SHM	logDS;

/* Process/Transaction Control Informations ------------------------------------- */

    TM_SHM	tmDS;

/* Lock Table Data Structures ------------------------------------------------- */

    LM_SHM	lmDS;

/* RDsM :: Volume Table Data Structure ----------------------------------------- */

    RDsM_SHM	rdsmDS;

/* BfM :: Buffer Table Data Structure ------------------------------------------ */

    BfM_SHM	bfmDS;

/* Btree Manager Data Structure ------------------------------------------------ */

    BtM_SHM	btmDS;

/* Scan Manager Data Structure ------------------------------------------------ */

    SM_SHM	smDS;

/* Low Relational Data System ------------------------------------------------- */

#ifdef LRDS_INCLUDED
    LRDS_SHM	lrdsDS;
#endif

/* Demon Process -------------------------------------------------------------- */
    Four 	demonPId;

    Four 	nTotalThread;

    Four 	nTotalProcess;

/* --------------------------------------------------------------------------- */
    LOGICAL_PTR_TYPE(HeapWord *) sharedHeap;	/* Heap for dynamic allocation */
    LATCH_TYPE  latch_sharedHeap; 		/* latch for alloc/free variable size elements */
    HeapWord	freeSpaceBase[1]; 		/* strings for free space  */

    /* Any data structure can not be located here */
    /* This is free space for dynamic allocation */

/* --------------------------------------------------------------------------- */

} SemStruct;

/* macros for num of threads & num of processes in system */
#define NUM_OF_THREADS_IN_SYSTEM	(shmPtr->nTotalThread)
#define NUM_OF_PROCESS_IN_SYSTEM	(shmPtr->nTotalProcess)

#define LATCH_SHAREDHEAP shmPtr->latch_sharedHeap
#define SHAREDHEAP	 shmPtr->sharedHeap

/* macros for processTable information */
extern Four	procIndex;
extern SemStruct *shmPtr; 

/* This macros is definded at Util_heap.c & Util_localHeap.c */
/* Note: We have casted the (x) with 'Four' which is the alignment type. */
#define USED           CONSTANT_ONE
#define SET_USED(x)    (HeapWord*)((MEMORY_ALIGN_TYPE)(x) | USED)
#define RESET_USED(x)  (HeapWord*)((MEMORY_ALIGN_TYPE)(x) & ~USED)
#define IS_USED(x)     ((MEMORY_ALIGN_TYPE)(x) & USED)


/*
 * Global Variables
 */
extern VarArray shm_grantedLatchStruct[MAXTHREADS];
/* EARLY 위의 변수는 나중에 perThreadTable로 들어가야 한다. 잊지 말것 */
/* EARLY - DON'T FORGET put the above variable into the perThreadTable Data Structure later. */

/* ----------------- BEGIN of per Process Section --------------------- */

/* per Thread Data Structures */

typedef struct SHM_PerProcessDS_T_tag {

	Four demonPId;  /* Demon Process ID */

} SHM_PerProcessDS_T;

/* ----------------- END of per Process Section --------------------- */




/*
 * Internal Function Prototypes
 */
Four shm_dumpMemory(Four, Four);
Four shm_dumpMyLatches(Four);
void shm_assignSharedPtr(Four);
Four shm_initLocalLatchList(Four);
Four shm_initProcessTable(Four);
Four shm_allocAndInitProcessTableEntry(Four, Four*);
Four shm_freeProcessTableEntry(Four, Four);
Four shm_initSharedHeap(Four, Four);


/*
 * Interface Function Prototypes
 */
Four SHM_alloc(Four, Four, Four, char **);
Four SHM_free(Four, char *, Four);
Four SHM_semCreateOrOpen(Four, key_t, Four (*)(Four, void*), void*);
Four SHM_semClose(Four, Four, Four (*)(Four, void*, Boolean), void*);
Four SHM_semInit(Four, Four, Four);
Four SHM_semSignal(Four, cosmos_thread_sem_t*);
Four SHM_semWait(Four, cosmos_thread_sem_t*);
void testandset_init(char*);
int  testandset(char*, int);
void testandset_release(char*);
Four SHM_beginProcess(Four, CfgParams_T*, ComponentInfo_T[], Four);
Four SHM_endProcess(Four, CfgParams_T*, ComponentInfo_T[], Four);
Four SHM_initDemon(Four);
Four SHM_finalDemon(Four);
Four SHM_initSharedDS(Four);
Four SHM_initLocalDS(Four);
Four SHM_finalSharedDS(Four);
Four SHM_finalLocalDS(Four);
Four SHM_InstallSignalHandler(void); 
Four SHM_UninstallSignalHandler(void); 

#endif /* __SHM_H__ */
