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
 * Module: shm_SignalHandler.c
 *
 * Description:
 *  Signal Handler to handle the signal cachted.
 *  It handles all signal.
 *  If catches the signals, finalize itself, and then, call previous signal handler.
 *
 * Exports:
 *  shm_signalHandler(int)
 *  SHM_InstallSignalHandler()
 *  SHM_UninstallSignalHandler()
 */


#include <sys/types.h>
#include <signal.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*@
 * Global static variables
 */
SignalHandler_T signalToHandle[] = {
    {SIGINT, 	NULL},		/* software interrupt signal */
    {SIGQUIT,   NULL},		/* quit signal */
    {SIGTERM, 	NULL},		/* termination signal */
    {SIGSEGV, 	NULL},		/* segmentation violation signal */
    {SIGBUS, 	NULL},		/* bus error signal */
    {SIGFPE, 	NULL},		/* arithmetic exception signal */
    {SIGILL, 	NULL},		/* illegal instruction signal */
    {SIGTSTP, 	NULL}		/* terminal suspend signal */
};

Four shm_finalProcess(void);


/*@================================
 * shm_signalHandler(int)
 *================================*/
/*
 * Function: void shm_signalHandler(int)
 *
 * Description:
 *  signal handler to handle all signals
 *  If catches some signals, abort transaction and finalize.
 *  Besides, call previous signal handler.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
void shm_signalHandler(
    int         sigNo)                	/* signal number */
{
    Four        e;                      /* error code */
    Four        i;                      /* loop index */
    Four        handle = -1;             /* handle */
    sigset_t    originalSigMask;        /* signal mask before block some signals */
    sigset_t    blockedSigMask;         /* signal mask to be blocked */
    sigset_t    pendedSigMask;          /* signal mask received while signal blocking region */


    TR_PRINT(handle, TR_SHM, TR1, ("shm_signalHandler()"));


    /*@
     * Block signal for atomic operation of signal hander.
     * Avoid calling this signal handler recursively while signal handler is executing.
     */
    sigemptyset(&blockedSigMask);

    for (i=0; i<sizeof(signalToHandle)/sizeof(SignalHandler_T); i++) {
        sigaddset(&blockedSigMask, signalToHandle[i].sigNo);
    }

    sigprocmask(SIG_BLOCK, &blockedSigMask, &originalSigMask);

    fprintf(stderr, "[%2ld] Signal %ld is catched (pid=%ld)\n", procIndex, sigNo, getpid());

    /*@
     * If signal above except SIGTSTP is received, we finalize this process
     */
    if (sigNo != SIGTSTP) {

        /* Finalize this process */
        if (NUM_OF_THREADS_IN_PROCESS > 0 || IS_PER_PROCESS_DS_INITIALIZED == 1) {

            fprintf(stderr, "[%2ld] Signal Handler is called (pid=%ld) by signal %ld\n", procIndex, getpid(), sigNo);

            /* we finalize this process */
            e = shm_finalProcess();
            if (e < eNOERROR) PRTERR(handle, e);
        }

	exit(1);
    }
    else {

       /*@
     	* The case received SIGTSTP signal(terminal suspend signal).
     	* This signal means stopping process not terminating.
     	* So, don't terminate. Do pause to wait for another signals.
     	*/
        sigpending(&pendedSigMask);
	if (sigismember(&pendedSigMask, SIGINT)  || sigismember(&pendedSigMask, SIGQUIT) || sigismember(&pendedSigMask, SIGTERM)) {
	    exit(1);
	}

    	sigprocmask(SIG_SETMASK, &originalSigMask, NULL);
    	fprintf(stderr, "This process(pid=%ld) is paused\n", getpid());
    	pause();
    }

}

/*@================================
 * SHM_InstallSignalHandler()
 *================================*/
/*
 * Function: Four SHM_InstallSignalHandler(int)
 *
 * Description:
 *  Install signal hander to handle some signal
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four SHM_InstallSignalHandler(void)
{
    Four			handle = -1;		/* handle */
    Four			i;			/* signal no. */


    TR_PRINT(handle, TR_SHM, TR1, ("SHM_SHM_InstallSignalHandler()"));


    for (i=0; i<sizeof(signalToHandle)/sizeof(SignalHandler_T); i++) {

        signalToHandle[i].handler = signal(signalToHandle[i].sigNo, shm_signalHandler);

        if (signalToHandle[i].handler == SIG_ERR) {

            /* 
	     * If we cannot install new signal handler on the signal, we ignore it 
	     */
            signalToHandle[i].handler = NULL;
        }
    }


    return eNOERROR;
}

/*@================================
 * SHM_UninstallSignalHandler()
 *================================*/
/*
 * Function: Four SHM_UninstallSignalHandler(int)
 *
 * Description:
 *  Uninstall signal hander to handle some signal
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four SHM_UninstallSignalHandler(void)
{
    Four			handle = -1;		/* handle */
    Four			i;			/* signal no. */


    TR_PRINT(handle, TR_SHM, TR1, ("SHM_SHM_UninstallSignalHandler()"));


    for (i=0; i<sizeof(signalToHandle)/sizeof(SignalHandler_T); i++) {

        if (signalToHandle[i].handler == NULL) continue;

        if (signal(signalToHandle[i].sigNo, signalToHandle[i].handler) == SIG_ERR) {
            /* 
	     * If we cannot install previous signal handler on the signal, we ignore it 
	     */
        }

        signalToHandle[i].handler = NULL;
    }


    return eNOERROR;
}

/*@================================
 * shm_exitHandler()
 *================================*/
/*
 * Function: void shm_exitHandler()
 *
 * Description:
 *  exit handler
 *  If exit signal is cated, abort transaction and finalize.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
void shm_exitHandler(void)
{
    Four        e;                      /* error code */
    Four        i;                      /* loop index */
    Four        handle = -1;            /* handle */


    TR_PRINT(handle, TR_SHM, TR1, ("shm_exitHandler()"));

    fprintf(stderr, "[%2ld] Exit is catched (pid=%ld)\n", procIndex, getpid());

    /* check where this process is demon or not */
    if (shm_perProcessDSptr->demonPId == getpid()) return;

    /* Finalize this process */
    if (NUM_OF_THREADS_IN_PROCESS > 0 || IS_PER_PROCESS_DS_INITIALIZED == 1) {

        fprintf(stderr, "[%2ld] Exit Handler is called (pid=%ld)\n", procIndex, getpid());

        /* we finalize this process */
        e = shm_finalProcess();
        if (e < eNOERROR) PRTERR(handle, e);
    }


    return;
}

/*@================================
 * SHM_InstallExitHandler()
 *================================*/
/*
 * Function: Four SHM_InstallExitHandler()
 *
 * Description:
 *  Install exit hander
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four SHM_InstallExitHandler(void)
{
    Four			handle = -1;		/* handle */
    Four			e;			/* error code */


    TR_PRINT(handle, TR_SHM, TR1, ("SHM_SHM_InstallExitHandler()"));


    /* 
     * we install exit signal handler
     */
    e = atexit(shm_exitHandler);
    if (e < eNOERROR) ERR(handle, eHANDLERINSTALLFAILED_SHM);


    return eNOERROR;
} 

/*============================================
 * shm_finalProcess()
 *============================================*/
Four shm_finalProcess(void)
{
    Four                	e;
    Four                	i;


    /*
     * 1. Abort all transaction in this process.
     * 2. Finalize all threads in this process.
     */
    if (NUM_OF_THREADS_IN_PROCESS > 0) {
        for (i=0; i<MAXTHREADS; i++) {
            if (perThreadTable[i].used == TRUE) {

                /* If the transaction is not end, abort it */
                if (MY_XACT_TABLE_ENTRY(i) != NULL) {
                    e = LRDS_AbortTransaction(i, &MY_XACTID(i));
                    if (e < eNOERROR) PRTERR(i, e);
	        }

                /* Free thread handle */
                e = LRDS_FreeHandle(i);
                if (e < eNOERROR) PRTERR(i, e);
 	    }
        }
    }


    /*
     * 3. Finalize this process.
     */
    if (IS_PER_PROCESS_DS_INITIALIZED == 1) {
        e = LRDS_Final();
        if (e < eNOERROR) PRTERR(handle, e);
    }


    return (eNOERROR);
}
