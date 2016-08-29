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
 * Module: bfm_SignalHandler.c	
 *
 * Description:
 *  Signal Handler to handle the signal cachted during BfM operations. 
 *  It handles SIGINT, SIGQUIT. SIGTERM, SIGSEGV, SIGBUS, SIGTSPT.
 *  If catches the signals during BfM operations, record signal and just return,
 *  but, in other case, abort transaction and exit.
 *
 * Exports:
 *  bfm_SignalHandler(int)
 *  bfm_InstallSignalHandler()
 *  bfm_FreeAllFixedBuffers()
 */

#ifdef USE_SHARED_MEMORY_BUFFER

#include <signal.h>
#include <errno.h>
#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

/*@================================
 * bfm_SignalHandler(int)
 *================================*/
/*
 * Function: void bfm_SignalHandler(int)
 *
 * Description:
 *  signal handler to handle some signals
 *  If catches some signals during BfM operations, record signal and just return,
 *  but, in other case, abort transaction and exit.
 *
 *  Signals to be handled
 *      SIGINT          (software interrupt signal)
 *      SIGQUIT         (quit signal)
 *      SIGTERM         (termination signal)
 *      SIGSEGV         (segmentation violation signal)
 *      SIGBUS          (bus error signal)
 *      SIGTSPT         (terminal suspend signal)
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
void bfm_SignalHandler(int sigNo)       /* signal number */
{
    Four        e;                      /* error code */
    sigset_t    originalSigMask;        /* signal mask before block some signals */
    sigset_t    blockedSigMask;         /* signal mask to be blocked */
    sigset_t    pendedSigMask;          /* signal mask received while signal blocking region */
    Four		handle = 0;				/* handle for each thread */	
    FileDesc	fd1;					/* lock file descriptor for starting critical section */ 
    FileDesc	fd2;					/* lock file descriptor for ending critical section */ 


    TR_PRINT(TR_BFM, TR1, ("bfm_SignalHandler(handle)"));

    /*@
     * Block signal for atomic operation of signal hander.
     * Avoid calling this signal handler recursively while signal handler is executing.
     */
    sigemptyset(&blockedSigMask);
    sigaddset(&blockedSigMask, SIGINT);
    sigaddset(&blockedSigMask, SIGQUIT);
    sigaddset(&blockedSigMask, SIGTERM);
    sigaddset(&blockedSigMask, SIGSEGV);
    sigaddset(&blockedSigMask, SIGBUS);
    sigaddset(&blockedSigMask, SIGTSTP);
    sigprocmask(SIG_BLOCK, &blockedSigMask, &originalSigMask);

    /*@
     * ReceivedSignalNo is the first received signal number.
     * If many signals are received sequentially, do signal handler for first received signal.
     * For this reason, first signal is preserved.
     *
     * But there is exceptional case. when SIGSEGV or SIGBUS received, received signal number is
     * changed to SIGSEGV or SIGBUS.
     * It means SIGSEGV and SIGBUS has to be handled first.
     */
    if (BFM_PER_PROCESS_DS.receivedSignalNo == -1 || sigNo == SIGSEGV || sigNo == SIGBUS) {
        BFM_PER_PROCESS_DS.receivedSignalNo = sigNo;
    }

    /*@
     * First Case: signal is received in signal-blocked range.
     *
     * In signal-blocked range, all signals we chosen are ignored except SIGSEGV and SIGBUS.
     * Segmentation fault and bus error in signal-blocked range are considered as fault of
     * shared memory buffer.
     * So, when segmentation fault and bus error are occured, terminate process immediately.
     */
    if (BFM_PER_THREAD_DS(handle).signalBlockCounter != 0) {
        if (BFM_PER_PROCESS_DS.receivedSignalNo == SIGSEGV || BFM_PER_PROCESS_DS.receivedSignalNo == SIGBUS) { 
            /*
             * Print name of received signal.
             */
	    switch (BFM_PER_PROCESS_DS.receivedSignalNo) { 
                case (SIGSEGV):
                    fprintf(stderr, "received SIGSEGV\n");
                    break;
                case (SIGBUS):
                    fprintf(stderr, "received SIGBUS\n");
                    break;
            }

            /*
             * Free all fixed buffers.
             */
            if (FBI_NFIXEDBUFS(handle) != FBI_NUNFIXEDBUFS(handle)) {
                puts("There is fixed buffers. Free all.");
                /*
                 * Reset signal number.
                 *
                 * Unless reset received signal number, BfM_FreeTrain() in bfm_FreeAllFixedBuffers()
                 * checks recieved signal number and call this signal handler again.
                 * So, it occur infinite loop of calling signal handler.
                 */
		BFM_PER_PROCESS_DS.receivedSignalNo = -1; 

                e = bfm_FreeAllFixedBuffers(handle);
                if (e < eNOERROR) PRTERR(handle, e);
            }
            else {
                puts("There is no fixed buffer.");
            }

            /*
             * Terminate process abnormally.
             */
            exit(1);
        }

        /* Unblock signals received in this signal handler. */
        sigprocmask(SIG_SETMASK, &originalSigMask, NULL);
        return;
    }

    /*@
     * Print name of received signal.
     */
    switch (BFM_PER_PROCESS_DS.receivedSignalNo) { 
        case(SIGINT):
            fprintf(stderr, "received SIGINT\n");
            break;
        case(SIGQUIT):
            fprintf(stderr, "received SIGQUIT\n");
            break;
        case(SIGTERM):
            fprintf(stderr, "received SIGTERM\n");
            break;
        case(SIGSEGV):
            fprintf(stderr, "received SIGSEGV\n");
            break;
        case(SIGBUS):
            fprintf(stderr, "received SIGBUS\n");
            break;
        case(SIGTSTP):
            fprintf(stderr, "received SIGTSTP\n");
            break;
        default:
            /*
             *  Never occured this default situation
             *  because this signal handler is installed to above signals.
             */
            fprintf(stderr, "unknown signal\n");
            return;
    }

    /*@
     * Second Case: signal is received in signal-unblocked range
     *
     * In signal-unlocked range, all signals are caught and handled with signal handler.
     * If a signal is received,
     *  First,  discard buffer pages which process uses.
     *  Second, destroy shared memory buffer.
     *  Finally, terminate process.
     */
    if (BFM_PER_PROCESS_DS.receivedSignalNo != SIGTSTP) { 
        if (bufInfo != NULL) {
            /*
             * Reset signal number.
             *
             * Unless reset received signal number, BfM_DiscardAll() checks
             * recieved signal number and call this signal handler again.
             * So, it occur infinite loop of signal handler.
             */
            BFM_PER_PROCESS_DS.receivedSignalNo = -1; 

            /*
             * Discard all buffers which only used by this process.
             */
            puts("Discard buffers");
            e = BfM_DiscardAll(handle);
            if (e < eNOERROR) PRTERR(handle, e);
        }

        puts("Destroy shared memory buffer");
        /* START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle); */
        START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd1); 
        e = bfm_DestroySharedMemoryBuffer(handle);
        if (e < eNOERROR) PRTERR(handle, e);
        /* END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle); */
        END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd2); 

	CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd1);
	CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd2);

        puts("Exit");
        exit(1);        /* abnormal termination */
    }

    /*@
     * The case received SIGTSTP signal(terminal suspend signal).
     * This signal means stopping process not terminating.
     * So, don't terminate. Do pause to wait for another signals.
     *
     * But, If there are pended terminate signals, terminate process.
     * If not, unblock signals and pause process.
     */
    sigpending(&pendedSigMask);
    if (sigismember(&pendedSigMask, SIGINT)  ||
        sigismember(&pendedSigMask, SIGQUIT) ||
        sigismember(&pendedSigMask, SIGTERM)) {
        exit(1);        /* abnormal termination */
    }

    /*
     * Reset signal number.
     */
    BFM_PER_PROCESS_DS.receivedSignalNo = -1; 
    
    sigprocmask(SIG_SETMASK, &originalSigMask, NULL);
    fprintf(stderr, "pause this process.\n");
    pause();
} /* bfm_SignalHander() */

/*@================================
 * bfm_InstallSignalHandler()
 *================================*/
/*
 * Function: Four bfm_InstallSignalHandler(int)
 *
 * Description:
 *  Install signal hander to handle some signal
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four bfm_InstallSignalHandler(Four handle)
{
    TR_PRINT(TR_BFM, TR1, ("bfm_InstallSignalHandler(handle)"));


    if (sigset(SIGINT,  bfm_SignalHandler) == SIG_ERR) {
        printf("%ld, %s\n", __LINE__, strerror(errno));
        ERR(handle, eSIGHANDLERINSTALLFAILED_BFM);
    }
    if (sigset(SIGQUIT, bfm_SignalHandler) == SIG_ERR) {
        printf("%ld, %s\n", __LINE__, strerror(errno));
        ERR(handle, eSIGHANDLERINSTALLFAILED_BFM);
    }
    if (sigset(SIGTERM, bfm_SignalHandler) == SIG_ERR) {
        printf("%ld, %s\n", __LINE__, strerror(errno));
        ERR(handle, eSIGHANDLERINSTALLFAILED_BFM);
    }
    if (sigset(SIGSEGV, bfm_SignalHandler) == SIG_ERR) {
        printf("%ld, %s\n", __LINE__, strerror(errno));
        ERR(handle, eSIGHANDLERINSTALLFAILED_BFM);
    }
    if (sigset(SIGBUS, bfm_SignalHandler) == SIG_ERR) {
        printf("%ld, %s\n", __LINE__, strerror(errno));
        ERR(handle, eSIGHANDLERINSTALLFAILED_BFM);
    }
    if (sigset(SIGTSTP, bfm_SignalHandler) == SIG_ERR) {
        printf("%ld, %s\n", __LINE__, strerror(errno));
        ERR(handle, eSIGHANDLERINSTALLFAILED_BFM);
    }

    return eNOERROR;
} /* bfm_InstallSignalHandle */

/*@================================
 * bfm_FreeAllFixedBuffers()
 *================================*/
/*
 * Function: Four bfm_FreeAllFixedBuffers(void)
 *
 * Description:
 *  Free all fixed buffers of this process.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four bfm_FreeAllFixedBuffers(Four handle)
{
    Four i, j;
    Four e;

    for (i = 0; i < FBI_NFIXEDBUFS(handle); i++) {
	for (j = 0; j < FBI_NUNFIXEDBUFS(handle); j++) {
	    if (FBI_FIXED_BUFTABLE(handle)[i].type = FBI_UNFIXED_BUFTABLE(handle)[j].type &&
		FBI_FIXED_BUFTABLE(handle)[i].key.volNo == FBI_UNFIXED_BUFTABLE(handle)[j].key.volNo &&
		FBI_FIXED_BUFTABLE(handle)[i].key.pageNo == FBI_UNFIXED_BUFTABLE(handle)[j].key.pageNo) {
		
		break;
	    }
	}
	if (j == FBI_NUNFIXEDBUFS(handle)) {
	    e = BfM_FreeTrain(handle, (TrainID* )&FBI_FIXED_BUFTABLE(handle)[i].key, FBI_FIXED_BUFTABLE(handle)[i].type);	
	    if (e < eNOERROR) ERR(handle, e);
	}
    }

    return (eNOERROR);
}
#endif
