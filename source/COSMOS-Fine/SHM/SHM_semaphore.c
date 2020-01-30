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
 * Module: SHM_semaphore.c
 *
 * Description:
 *  Semaphore operation :: wait and signal
 *
 * Exports:
 *	SHM_semWait();
 *	SHM_semSignal();
 *	SHM_semInit();
 */


#include <assert.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <stdio.h>
#include <errno.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "RDsM.h" 
#include "perProcessDS.h"
#include "perThreadDS.h"
#include "THM_cosmosThread.h"

#ifdef __SVR4
union semun {
    int val;
    struct semid_ds *buf;
    ushort *array;
};
#endif /* __SVR4 */

#ifndef LINUX	
extern char *sys_errlist[];
#endif

extern Four_Invariable shmId;


/*@================================
 * SHM_semCreateOrOpen( )
 *================================*/
Four SHM_semCreateOrOpen(
    Four 		handle,
    key_t 		key,                  /* IN semaphore key */
    Four 		(*initFn)(Four, void *), /* IN initialize function */
    void 		*argForInitFn)        /* IN arg for initialize function */
{
    Four 		e;                     /* error code */
    struct shmid_ds     shmInfo;

    e = shmctl(shmId, IPC_STAT, &shmInfo);
    if (e < 0 ) fprintf(stderr, "shmctl error: get infomation\n");

    if (shmInfo.shm_nattch == 1) {

        e = (*initFn)(handle, argForInitFn);
        if (e < eNOERROR) ERR(handle, e);

	NUM_OF_PROCESS_IN_SYSTEM = 0;
    }
    else {

	/* The following code must be called in SHM_finalLocalDS(). */
	/* close the corresponding entry of the processTable */
	/* shm_freeProcessTableEntry(handle, procIndex); */ 

        e = shm_allocAndInitProcessTableEntry(handle, &procIndex);
        if (e < eNOERROR) ERR(handle, e);
    }

    NUM_OF_PROCESS_IN_SYSTEM++;

    fprintf(stderr, "[%2ld] Process(pid=%ld) Created (active processes=%ld)\n", procIndex, getpid(), NUM_OF_PROCESS_IN_SYSTEM);


    return(eNOERROR);

} /* SHM_semCreateOrOpen() */



/*@================================
 * SHM_semClose( )
 *================================*/
Four SHM_semClose(
    Four 		handle,
    Four 		nDemonProcs,           /* IN # of demon processes */
    Four 		(*finalFn)(Four, void*, Boolean), /* IN finalization function */
    void 		*argForFinalFn)        /* IN arg for finalization function */
{
    Four 		e;                     /* error code */
    struct shmid_ds     shmInfo;

    e = shmctl(shmId, IPC_STAT, &shmInfo);
    if (e < 0 ) fprintf(stderr, "shmctl error: get infomation\n");

    NUM_OF_PROCESS_IN_SYSTEM--;

    fprintf(stderr, "[%2ld] Process(pid=%ld) Destroyed (active processes=%ld)\n", procIndex, getpid(), NUM_OF_PROCESS_IN_SYSTEM);

#ifndef DEMON_PROCESS
    if (shmInfo.shm_nattch == 1) {
#else
    if (shmInfo.shm_nattch == 2) {
#endif /* DEMON_PROCESS */

        e = (*finalFn)(handle, argForFinalFn, TRUE);
        if (e < eNOERROR) ERR(handle, e);
    }
    else {

        e = (*finalFn)(handle, argForFinalFn, FALSE);
        if (e < eNOERROR) ERR(handle, e);
    }


    return(eNOERROR);

} /* SHM_semClose() */



/*@================================
 * SHM_semWait( )
 *================================*/
/* By using a seamphore value in a set of semaphores instead of a semaphore for each process, this file was modified. */
Four SHM_semWait(
    Four                    handle,
    cosmos_thread_sem_t     *semID
)
{
    Four 		    e;

    e = cosmos_thread_sem_wait(semID);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

}



/*@================================
 * SHM_semSignal( )
 *================================*/
Four SHM_semSignal(
    Four 			handle,
    cosmos_thread_sem_t 	*semID
)
{

    Four                        e;

    e = cosmos_thread_sem_post(semID);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}



/*@================================
 * SHM_semInit( )
 *================================*/
Four SHM_semInit(
    Four 	handle,
    Four 	semNo,
    Four 	value
)
{

    return(eNOERROR);

}
