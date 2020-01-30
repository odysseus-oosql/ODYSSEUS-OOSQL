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
#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/timeb.h>
#include <unistd.h>

#include "perProcessDS.h"
#include "perThreadDS.h"
#include "THM_cosmosThread.h"


/*
 * Global Data Structure
 */
#include "log_LogRecTbl.i" 


static KeyDesc lrds_SysCountersCounterNameIndexKdesc = { 0, 1, {{SM_VARSTRING, NIL, LRDS_COUNTER_NAME_MAX_LEN+1}} };


Four THM_AllocHandle(Four* handle)
{

    Four	i;
    Four 	e;
    char	semName[MAXSEMAPHORENAME];
    Four	nComponents;


    for (i=0; i<MAXTHREADS; i++) {
  	if (perThreadTable[i].used == FALSE) {
	    *handle = i;
   	    perThreadTable[i].used = TRUE;
	    break;
	}
    }

    /* Alloc TCB */
    perThreadTable[*handle].TCBptr = &tm_shmPtr->TCB_Pool[procIndex * MAXTHREADS + *handle];

    /* initialize TCB */
#if !defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2)
    sprintf(semName,"/%lld_%lld",procIndex, *handle);
#else
    sprintf(semName,"/%ld_%ld",procIndex, *handle);
#endif

    e = cosmos_thread_unnamed_sem_create(&(perThreadTable[*handle].TCBptr->semID), 0, 0);
    if (e < eNOERROR) ERR(*handle, e);
    strcpy(perThreadTable[*handle].TCBptr->semName, semName);
    perThreadTable[*handle].TCBptr->next = LOGICAL_PTR(NULL); 


    /* allocate and initialize its own latchList */
    MY_NUMGRANTED(*handle) = 0;
    MY_GRANTEDLATCHSTRUCT(*handle) = &shm_grantedLatchStruct[*handle];

    /*
     * Initialize all the local data structures of all the components.
     */
    nComponents = sizeof(componentInfos)/sizeof(componentInfos[0]);
    for (i = 0; i < nComponents; i++) {
        if (componentInfos[i].initLocalDS != NULL) {
            e = componentInfos[i].initLocalDS(*handle);
            if (e < eNOERROR) ERR(*handle, e);
        }
    }

    /* Init perThread Table DS */
    e = thm_InitPerThreadDS(*handle);
    if (e < eNOERROR) ERR(*handle, e);

    e = SM_InitializeGlobalData(*handle);
    if (e < eNOERROR) ERR(*handle, e);

    /*
     * Process restart recovery
     */

    if (NUM_OF_THREADS_IN_SYSTEM == 0) {

   	if (strcmp(CFG_LOGVOLUMEDEVICELIST,"") != 0) {
       	    e = RM_Restart(*handle, CFG_LOGVOLUMEDEVICELIST);
            if (e < eNOERROR) ERR(*handle, e);

            common_shmPtr->recoveryFlag = TRUE;
	}

#ifdef DEMON_PROCESS
#ifndef SINGLE_USER
       /*
        * Invoke demons
        */
        e = SHM_initDemon(*handle);
        if (e < eNOERROR) ERR(*handle, e);
#endif /* SINGLE_USER */
#endif /* DEMON_PROCESS */


    }
    else {

    	if (strcmp(CFG_LOGVOLUMEDEVICELIST,"") != 0) {
            if (common_shmPtr->recoveryFlag == TRUE) {
           	Four dummyVolId;

            	e = RDsM_MountWithDeviceListString(*handle, CFG_LOGVOLUMEDEVICELIST, &dummyVolId, FALSE);
            	if (e < eNOERROR) ERR(*handle, e);
            }
	}
    }

    NUM_OF_THREADS_IN_SYSTEM++;
    NUM_OF_THREADS_IN_PROCESS++;


    fprintf(stderr, "[%2ld, %2ld] Thread Created (active threads=%ld) in this process(pid=%d)\n",
	    procIndex, *handle, NUM_OF_THREADS_IN_PROCESS, getpid());


    return (eNOERROR);
}

Four THM_FreeHandle(Four handle)
{

    Four 		i;
    Four 		e;
    char		semName[MAXSEMAPHORENAME];
    Four		nComponents;
    struct timeb    	checkTime;


    if (common_shmPtr->recoveryFlag && NUM_OF_THREADS_IN_SYSTEM == 1) {
        e = RM_Checkpoint(handle);
        if (e < eNOERROR) ERR(handle, e);
    }

    if (NUM_OF_THREADS_IN_PROCESS == 1) {
	previousHandle = handle;
    }
    else {

    	/* finalize perThread Table (local data structure) */
    	nComponents = sizeof(componentInfos)/sizeof(componentInfos[0]);

    	/* finalize all the components */
    	for (i = nComponents-1; i >= 0; i--) {
            if (componentInfos[i].finalComponent != NULL) {
           	e = (*componentInfos[i].finalComponent)(handle);
       	   	if (e < eNOERROR) ERR(handle, e);
       	    }
    	}

    	/* finalize all the local data structure */
    	for (i = nComponents-1; i >= 0; i--) {
            if (componentInfos[i].finalLocalDS != NULL) {
       	   	e = (*componentInfos[i].finalLocalDS)(handle);
            	if (e < eNOERROR) ERR(handle, e);
            }
    	}

	e = thm_FinalPerThreadDS(handle);
	if (e < eNOERROR) ERR(handle, e);
    }

    NUM_OF_THREADS_IN_SYSTEM--;
    NUM_OF_THREADS_IN_PROCESS--;

    fprintf(stderr, "[%2ld, %2ld] Thread Destroyed (active threads=%ld) in this process(pid=%d)\n",
	    procIndex, handle, NUM_OF_THREADS_IN_PROCESS, getpid());


    return (eNOERROR);
}

Four thm_InitPerThreadDS(
    Four        handle
)
{

    /* LRDS_PerThreadDS Initialize */

    strcpy(perThreadTable[handle].lrdsDS.catalogTable[0], "lrdsSysTables");
    strcpy(perThreadTable[handle].lrdsDS.catalogTable[1], "lrdsSysColumns");
    strcpy(perThreadTable[handle].lrdsDS.catalogTable[2], "lrdsSysIndexes");

    perThreadTable[handle].lrdsDS.lrds_SysCountersCounterNameIndexKdesc = lrds_SysCountersCounterNameIndexKdesc;

    /* RDsM_PerThreadDS Initialize */
    perThreadTable[handle].rdsmDS.io_num_of_reads  = 0;
    perThreadTable[handle].rdsmDS.io_num_of_writes = 0;

    /* LOG_PerThreadDS Initialize */
    if (sizeof(perThreadTable[handle].logDS.LOG_logRecTbl) != sizeof(LOG_logRecTbl)) return(eINTERNAL);
    ARRAYCOPY(perThreadTable[handle].logDS.LOG_logRecTbl, LOG_logRecTbl, sizeof(LOG_logRecTbl));
    perThreadTable[handle].logDS.logPage_BCBP   = NULL;

    return (eNOERROR);
}

Four thm_FinalPerThreadDS(
    Four        handle
)
{
    Four        e;
    char	semName[MAXSEMAPHORENAME];


    perThreadTable[handle].used = FALSE;

#if !defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2)
    sprintf(semName,"/%lld_%lld",procIndex, handle);
#else
    sprintf(semName,"/%ld_%ld",procIndex, handle);
#endif

    e = cosmos_thread_unnamed_sem_destroy(&(perThreadTable[handle].TCBptr->semID));
    if (e < eNOERROR) ERR(handle, e);

    perThreadTable[handle].TCBptr->next = LOGICAL_PTR(NULL);

    /* Free TCB */
    perThreadTable[handle].TCBptr = NULL;


    return (eNOERROR);
}

