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
 * Module: SHM_demon.c
 *
 * Description:
 *   demon process control
 *
 * Exports:
 *
 */

#include <sys/types.h>
#include <sys/wait.h>	/* for waitpid */
#include <signal.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdlib.h>
#include <unistd.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"
#include <errno.h>


/* shm_initDemon :: initialize Demon Process */
#define ST_DEADLOCKDETECTION 4


void shm_signalHandlerOfDemon();
void shm_signalHandlerOfServer();

Four shm_initInheritedProperty(Four handle); 


/*@================================
 * SHM_initDemon( )
 *================================*/
Four SHM_initDemon(
    Four    	handle
)
{
    Four 	demonPId;			/* index loop */
    Four	e;
    char        semName[MAXSEMAPHORENAME];
    Four        tmp;
    FileDesc    fd_1, fd_2, fd_3; 

    signal(SIGTRAP, SIG_IGN);
    signal(SIGCHLD, shm_signalHandlerOfServer); 

    if ( ( demonPId = fork() ) == -1 )
	ERR(handle, eDEMONFORKFAILED_SHM);

    else if ( demonPId == 0 ) {
	/* child process */
	shm_perProcessDSptr->demonPId = getpid();

        /*
         * Initialize the inherited property from its parent process
         */
        e = shm_initInheritedProperty(handle);
        if ( e < eNOERROR ) ERR(handle, e);

        /*
         * CRITICAL SECTION INIT
         */
	/* INIT_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle); */
	INIT_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_1);

        /*
         * CRITICAL SECTION BEGIN
         */
        /* START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle); */
	START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_2);


 	e = shm_allocAndInitProcessTableEntry(handle, &procIndex);
	if ( e < eNOERROR ) ERR(handle, e);

        /* Alloc TCB */
        perThreadTable[handle].TCBptr = &tm_shmPtr->TCB_Pool[procIndex * MAXTHREADS + handle];

        /* initialize TCB */
#if !defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2)
        sprintf(semName,"/%lld_%lld",procIndex, handle);
#else
        sprintf(semName,"/%ld_%ld",procIndex, handle);
#endif
        e = cosmos_thread_unnamed_sem_create(&(perThreadTable[handle].TCBptr->semID), 0, 0);
        if (e < eNOERROR) ERR(handle, e);

        strcpy(perThreadTable[handle].TCBptr->semName, semName);
        perThreadTable[handle].TCBptr->next = LOGICAL_PTR(NULL); 

        /* allocate and initialize its own latchList */
        MY_NUMGRANTED(handle) = 0;
        MY_GRANTEDLATCHSTRUCT(handle) = &shm_grantedLatchStruct[handle];

	/*@ signal handling */
	/* change signal handler from shm_sigHandle to shm_signalHandlerOfDemon */
	signal(SIGINT, shm_signalHandlerOfDemon);
	signal(SIGQUIT, shm_signalHandlerOfDemon);
	signal(SIGKILL, shm_signalHandlerOfDemon);

	fprintf(stderr, "[%2ld] Demon Process(pid=%ld) Created\n", procIndex, getpid());


        /*
         * CRITICAL SECTION END
         */
	END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_3);

	CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_1);
	CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_2);
	CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_3);

	for(;;) {

	    sleep(ST_DEADLOCKDETECTION);

	    e = LM_detectDeadlock(handle);
	    if (e < eNOERROR) ERR(handle, e);
	}

    }
    else {

	/* parent process */
	shmPtr->demonPId = demonPId;
    }

    return(eNOERROR);
}



/*@================================
 * SHM_finalDemon( )
 *================================*/
Four SHM_finalDemon(
    Four    	handle
)
{
    Four 	e;


    fprintf(stderr, "[%2ld] This process(pid=%d) Send Signal %ld to Demon Process(pid=%ld)\n", 
	    procIndex, getpid(), SIGQUIT, shmPtr->demonPId);


    kill(shmPtr->demonPId, SIGQUIT);

    return(eNOERROR);
}



/*@================================
 * shm_signalHandlerOfDemon
 *================================*/
void shm_signalHandlerOfDemon(Four sigNo) /* change function name from shm_sigHandle */
{
    char        semName[MAXSEMAPHORENAME];


    fprintf(stderr, "[%2ld] Demon Process(pid=%ld) Destroyed by signal %ld\n", procIndex, getpid(), sigNo);


    exit(0); /* DO NOT REMOVE THIS LINE */ 
}

/*@================================
 * shm_signalHandlerOfServer
 *================================*/
void shm_signalHandlerOfServer(Four sigNo)
{
    pid_t	pid;
    int		status;

    /*
     * Wait for the termination of demon and 
     * get the exit status of the demon
     */
    if ((pid = waitpid(-1, &status, 0)) < 0) {
	fprintf(stderr, "waitpid error\n");
    }


    return;
}

/*============================================
 * shm_initInheritedProperty(handle)
 *============================================*/
Four shm_initInheritedProperty(
    Four                handle
)
{
    Four                e;              /* error */
    Four                fd;


    /* We close all opend file descriptors except stdard I/O (stdin, stdout, stderr) */
    for (fd = 0; fd < sysconf(_SC_OPEN_MAX); fd++) {
        if (fd == STDIN_FILENO || fd == STDOUT_FILENO || fd == STDERR_FILENO)
            continue;
        else
            close(fd);
    }

    /* We detach this demon process from the process group in order to prevent signal's effect */
    e = setpgrp();
    if (e == -1) ERR(handle, eINTERNAL);


    return eNOERROR;
}
