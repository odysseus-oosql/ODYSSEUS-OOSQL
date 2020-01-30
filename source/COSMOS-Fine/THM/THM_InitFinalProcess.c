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

#include "perProcessDS.h"
#include "perThreadDS.h"
#include "THM_cosmosThread.h"


/* perProcess Data Structure */
PerProcessDS_T          perProcessDS;

/* perThread Table Structure */
PerThreadTableEntry_T   perThreadTable[MAXTHREADS];
Four                    previousHandle;


/* For fast access */
SM_PerProcessDS_T   *sm_perProcessDSptr     =   SM_PER_PROCESS_DS_PTR;
LM_PerProcessDS_T   *lm_perProcessDSptr     =   LM_PER_PROCESS_DS_PTR;
SHM_PerProcessDS_T  *shm_perProcessDSptr    =   SHM_PER_PROCESS_DS_PTR;


/* cosmos thread attrubute */
cosmos_thread_mutex_attr_t      cosmos_thread_mutex_attr_default;
cosmos_thread_cond_attr_t       cosmos_thread_cond_attr_default;
cosmos_thread_attr_t            cosmos_thread_attr_default;


Four THM_InitPerProcess(void)
{
    Four                handle = -1;
    Four 		e;
    Four 		i;


    /* SM Data Structure */
    perProcessDS.smDS.sm_sysTablesDataFileIdIndexKdesc = sm_sysTablesDataFileIdIndexKdesc;
    perProcessDS.smDS.sm_sysIndexesDataFileIdIndexKdesc = sm_sysIndexesDataFileIdIndexKdesc;
    perProcessDS.smDS.sm_sysIndexesIndexIdIndexKdesc = sm_sysIndexesIndexIdIndexKdesc;


    /* LM_PerProcessDS Initialize */
    ARRAYCOPY(perProcessDS.lmDS.LOCK_conversion, LOCK_conversion, sizeof(LOCK_conversion));
    ARRAYCOPY(perProcessDS.lmDS.LOCK_compatible, LOCK_compatible, sizeof(LOCK_compatible));
    ARRAYCOPY(perProcessDS.lmDS.LOCK_supreme, LOCK_supreme, sizeof(LOCK_supreme));
    ARRAYCOPY(perProcessDS.lmDS.LOCK_super, LOCK_super, sizeof(LOCK_super));
    ARRAYCOPY(perProcessDS.lmDS.LOCK_hierarchy, LOCK_hierarchy, sizeof(LOCK_hierarchy));


    for (i=0; i<MAXTHREADS; i++) {
 	perThreadTable[i].used = FALSE;
    }

    NUM_OF_THREADS_IN_PROCESS = 0;



    /* Initialization for thread. Initialization must be done on every beginning of process. */
    e = pthread_mutexattr_init(&cosmos_thread_mutex_attr_default);
    if (e < eNOERROR) ERR(handle, e);
    e = pthread_mutexattr_setpshared(&cosmos_thread_mutex_attr_default, PTHREAD_PROCESS_SHARED);
    if (e < eNOERROR) ERR(handle, e);

    e = pthread_condattr_init(&cosmos_thread_cond_attr_default);
    if (e < eNOERROR) ERR(handle, e);
    e = pthread_condattr_setpshared(&cosmos_thread_cond_attr_default, PTHREAD_PROCESS_SHARED);
    if (e < eNOERROR) ERR(handle, e);

    e = pthread_attr_init(&cosmos_thread_attr_default);
    if (e < eNOERROR) ERR(handle, e);
    e = pthread_attr_setscope(&cosmos_thread_attr_default, PTHREAD_SCOPE_SYSTEM);
    if (e < eNOERROR) ERR(handle, e);
    e = pthread_attr_setdetachstate(&cosmos_thread_attr_default, PTHREAD_CREATE_JOINABLE);
    if (e < eNOERROR) ERR(handle, e);


    return (eNOERROR);
}


Four THM_FinalPerProcess(void)
{
    Four    handle = -1;
    Four    e;


    /* Finalization for thread. Finalization must be done on every termination of process. */
    e = pthread_mutexattr_destroy(&cosmos_thread_mutex_attr_default);
    if (e < eNOERROR) ERR(handle, e);

    e = pthread_condattr_destroy(&cosmos_thread_cond_attr_default);
    if (e < eNOERROR) ERR(handle, e);

    e = pthread_attr_destroy(&cosmos_thread_attr_default);
    if (e < eNOERROR) ERR(handle, e);


    return (eNOERROR);
}
