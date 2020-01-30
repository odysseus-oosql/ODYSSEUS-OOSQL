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
/*
 * Module: SHM_process.c
 *
 * Description:
 *  Process Initialization and finalization
 *
 * Exports:
 *	new_process()
 *	end_process()
 */


#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#undef PAGESIZE			/* undef PAGESIZE defined in sys/shm.h */
#include "perProcessDS.h"
#include "perThreadDS.h"

#include <stdlib.h>

#include <errno.h>
#include <sys/timeb.h>

#include "common.h"
#include "error.h"
#include "trace.h"
#include "RM.h"
#include "SHM.h"


/* Static Variable */
Four_Invariable         shmId;
static Four 		sharedHeapSize;		/* needed shared memory size */


/*@================================
 * SHM_initFn()
 *================================*/
Four shm_initFn(
    Four 		handle,
    void 		*arg)                  /* IN argument */
{
    Four 		e;                     /* error code */
    Four 		i;
    CfgParams_T 	*cfgParams      = ((shm_ArgForInitFn_T*)arg)->cfgParams;
    ComponentInfo_T 	*componentInfos = ((shm_ArgForInitFn_T*)arg)->componentInfos;
    Four 		nComponents     = ((shm_ArgForInitFn_T*)arg)->nComponents;


    TR_PRINT(handle, TR_SHM, TR1, ("shm_initFn()"));

    /*
     * Initialize the area of shared memory
     */
    memset(shmPtr, 0x00, sizeof(SemStruct)+sharedHeapSize);

    /*
     * Initialize all the shared data structures of all the components.
     */
    common_shmPtr->cfgParams = *cfgParams;
    common_shmPtr->recoveryFlag = FALSE;


    if((e = shm_initSharedHeap(handle, sharedHeapSize)) < eNOERROR) ERR(handle, e);

    for (i = 0; i < nComponents; i++) {
        if (componentInfos[i].initSharedDS != NULL) {
            e = componentInfos[i].initSharedDS(-1);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    NUM_OF_THREADS_IN_SYSTEM = 0;


    return(eNOERROR);

} /* shm_initFn() */



/*@================================
 * SHM_finalFn()
 *================================*/
Four shm_finalFn(
    Four 		handle,
    void 		*arg,                  	/* IN argument */
    Boolean 		lastProcessFlag)    	/* IN last process flag */
{
    Four 		e;                     	/* error code */
    Four 		i;
    char    		semName[MAXSEMAPHORENAME];
    CfgParams_T 	*cfgParams      = ((shm_ArgForFinalFn_T*)arg)->cfgParams;
    ComponentInfo_T 	*componentInfos = ((shm_ArgForFinalFn_T*)arg)->componentInfos;
    Four 		nComponents     = ((shm_ArgForFinalFn_T*)arg)->nComponents;


    TR_PRINT(handle, TR_SHM, TR1, ("shm_finalFn()"));


    if (!lastProcessFlag) {

	handle = previousHandle;

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

        if (shmdt((char *) shmPtr) < 0) {
            if (errno != EINVAL) { /* ignore EINVAL */
		ERR(handle, eSHMDTFAILED_SHM);
	    }
        }

    } else {

#ifdef DEMON_PROCESS
        e = SHM_finalDemon(handle);
        if (e < eNOERROR) ERR(handle, e);
#endif /* DEMON_PROCESS */

	handle = previousHandle;

        /*
         * Finalize all the shared data structures of all the components.
         */
        for (i = nComponents-1; i >= 0; i--) {
            if (componentInfos[i].finalComponent != NULL) {
                e = (*componentInfos[i].finalComponent)(handle);
                if (e < eNOERROR) ERR(handle, e);
            }
            if (componentInfos[i].finalLocalDS != NULL) {
                e = (*componentInfos[i].finalLocalDS)(handle);
                if (e < eNOERROR) ERR(handle, e);
            }
            if (componentInfos[i].finalSharedDS != NULL) {
                e = componentInfos[i].finalSharedDS(handle);
                if (e < eNOERROR) ERR(handle, e);
            }
        }

        e = thm_FinalPerThreadDS(handle);
        if (e < eNOERROR) ERR(handle, e);

        if (shmdt((char *) shmPtr) < 0) {
            if (errno != EINVAL) {  /* ignore EINVAL */
                ERR(handle, eSHMDTFAILED_SHM);
            }
        }

        if ( shmctl(shmId, IPC_RMID, (struct shmid_ds *) 0) < 0 ) {
            ERR(handle, eSHMCTLFAILED_SHM);
        }

    }


    return(eNOERROR);

} /* shm_finalFn() */



/*@================================
 * SHM_beginProcess( )
 *================================*/
Four SHM_beginProcess(
    Four 		handle,
    CfgParams_T 	*cfgParams,     /* IN configuration parameters */
    ComponentInfo_T 	componentInfos[], /* IN component informations */
    Four 		nComponents)    /* IN number of components */
{

    Four 		i;		/* loop index */
    Four		e;              /* error number */
    key_t		shmkey;		/* key of the shared memory */
    key_t		semkey;		/* key of the semaphore */
    shm_ArgForInitFn_T 	argForInitFn; 	/* argument for the init function */
    char		*shmkeyPtr;	/* pointer to environment value of KAOSS_SHMKEY */
    char		*semkeyPtr;	/* pointer to environment value of KAOSS_SEMKEY */


    /*@ evaluate the size */
    /* these are defined in BfM_init() */
    sharedHeapSize = PAGESIZE*NUM_PAGE_BUFS+ /* for page size buffer */
	             PAGESIZE*NUM_LOT_LEAF_BUFS*TRAINSIZE2+/* for large object buffer */ 
	             sizeof(bfmHashEntry)*NUM_PAGE_BUFS*7+sizeof(Lock_hashEntry)*NUM_PAGE_BUFS*7+
	             sizeof(Lock_ctrlBlock)*NUM_PAGE_BUFS*7+MAXFREESPACE;

    TR_PRINT(handle, TR_SHM, TR2, ("SemStruct size is %ld\n", sizeof(SemStruct)));
    TR_PRINT(handle, TR_SHM, TR2, ("sharedHeapSize size is %ld\n", sharedHeapSize));
    TR_PRINT(handle, TR_SHM, TR2, ("So, request size is %ld\n", sizeof(SemStruct)+sharedHeapSize));


    /*@
     * get the semaphore key and shared memory key.
     */
    /* get the semaphore key */
    if ((semkeyPtr = getenv("COSMOS_SEMAPHORE_KEY")) == NULL)
        semkey = COSMOS_SEMAPHORE_KEY;
    else
        semkey = atoi(semkeyPtr);

    /* get the shared memory key */
    if ((shmkeyPtr = getenv("COSMOS_SHARED_MEMORY_KEY")) == NULL)
	shmkey = COSMOS_SHARED_MEMORY_KEY;
    else
	shmkey = atoi(shmkeyPtr);


    TR_PRINT(handle, TR_SHM, TR1, ("semkey = %0P\n", semkey));
    TR_PRINT(handle, TR_SHM, TR1, ("shmkey = %0P\n", shmkey));


    /* get shard memeory from system */
    if ( (shmId = shmget(shmkey, sizeof(SemStruct)+sharedHeapSize, PERMS | IPC_CREAT )) < 0) { 
        perror("shmget");
	ERR(handle, eSHMGETFAILED_SHM);
    }

    /* attach shared segment into each process */
#ifdef USE_LOGICAL_PTR
    if ((shmPtr = (SemStruct *)shmat(shmId, (char *)NULL, 0)) == (SemStruct *) -1)
#else
    if ((shmPtr = (SemStruct *)shmat(shmId, (char *)0xe3400000, SHM_RND)) == (SemStruct *) -1)
#endif
    {
        perror("shmat");
	ERR(handle, eSHMATFAILED_SHM);
    }

    printf("Shared memory is attached at shmPtr=%p\n",shmPtr);
    fflush(stdout);


    TR_PRINT(handle, TR_ALL, TR0, ("value of smPtr = %p\n", shmPtr));


    /*
     * Initialize all the local data structures of all the components.
     */
    shm_assignSharedPtr(handle);



    /*
     * Create or open the semaphore.
     * When this process is the first process, it also initialize
     * the shared data structure and do some initial actions.
     */
    argForInitFn.cfgParams = cfgParams;
    argForInitFn.componentInfos = componentInfos;
    argForInitFn.nComponents = nComponents;
    e = SHM_semCreateOrOpen(handle, semkey, shm_initFn, &argForInitFn);
    if (e < eNOERROR) ERR(handle, e);

    TR_PRINT(handle, TR_SHM, TR2, ("### new_process begins"));

    TR_PRINT(handle, TR_SHM, TR1, ("new_process id :: %ld\n", (key_t) getpid() ));
    TR_PRINT(handle, TR_ALL, TR1, ("My procIndex %ld\n", procIndex));


    return(eNOERROR);

} /* SHM_beginProcess() */



/*@================================
 * SHM_endProcess( )
 *================================*/
Four SHM_endProcess(
    Four 		handle,
    CfgParams_T 	*cfgParams,     	/* IN configuration parameters */
    ComponentInfo_T 	componentInfos[], 	/* IN component informations */
    Four 		nComponents)           	/* IN number of components */
{
    Four 		e = eNOERROR;		/* returned error number */
    shm_ArgForFinalFn_T argForFinalFn;
    Four 		nProcsAfterEnding;
    Four 		i;
    struct shmid_ds     shmInfo;


    TR_PRINT(handle, TR_ALL, TR1, ("end_process :: %ld\n", (key_t) getpid() ));
    TR_PRINT(handle, TR_ALL, TR1, ("My procIndex %ld\n", procIndex));


    /*
     * Close the semaphore.
     * If this process is the last user process, it does some finaliation.
     */
    argForFinalFn.cfgParams = cfgParams;
    argForFinalFn.componentInfos = componentInfos;
    argForFinalFn.nComponents = nComponents;
    e = SHM_semClose(handle, NUM_DEMONS, shm_finalFn, &argForFinalFn);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SHM_endProcess() */

