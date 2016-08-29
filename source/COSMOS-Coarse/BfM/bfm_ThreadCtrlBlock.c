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
 * Module: bfm_ThreadCtrlBlock.c
 *
 * Description:
 *  Allocate and free thread control block which has latch information. 
 *
 * Exports:
 *  bfm_AllocThreadCtrlBlock()
 *  bfm_FreeThreadCtrlBlock()
 *  bfm_InitLocalLatchList()
 *  bfm_FinalLocalLatchList()
 */ 

#if defined(USE_SHARED_MEMORY_BUFFER) && !defined(USE_MUTEX)

#include "common.h"
#include "BfM_Internal.h"
#include "THM_lock.h"	
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

/*@
 * Global variables
 */ 
extern TCB* TCBPool;

Four bfm_AllocThreadCtrlBlock(Four handle)
{
    Four 	e;								/* error code */
    Four 	i;								/* loop index */
    char 	semName[MAXSEMAPHORENAME];		/* name of semaphore of this process */

    /*@
     * Set pointer of process table entry of this process.
     */
    MY_TCB(handle) = &TCBPool[procIndex * MAXTHREADS + handle];

    /*@
     * Create semaphore of this process table entry.
     */
    sprintf(semName, "/%ld_%ld", procIndex, handle); 
    strcpy(MY_TCB(handle)->semName, semName);
    
    e = THM_CreateUnnamedSemaphore(handle, &(MY_TCB(handle)->semID), 0);
    if (e < eNOERROR) ERR(handle, e);

    /*@
     * Initialize array of granted latch structures. 
     */
    e = bfm_InitLocalLatchList(handle);
    if (e < eNOERROR) ERR(handle, e);
    
    MY_NUMGRANTED(handle) = 0;
    MY_GRANTEDLATCHSTRUCT(handle) = &(BFM_PER_THREAD_DS(handle).grantedLatchStruct);

    /*@
     * Initialize other fields of process table entry.
     */
    MY_TCB(handle)->next = LOGICAL_PTR(NULL);

    return (eNOERROR);
}

/*@================================
 * bfm_FreeProcessTableEntry()
 *================================*/
/*
 * Function: Four bfm_FreeProcessTableEntry(void)
 * 
 * Description:
 *  Free process table entry.
 *
 * Returns:
 *  error codes
 */
Four bfm_FreeThreadCtrlBlock(Four handle)
{
    Four	e;	/* error code */

    /*@
     * Finalize array of granted latch structures.
     */
    e = bfm_FinalLocalLatchList(handle);
    if (e < eNOERROR) ERR(handle, e);

    /*@
     * Destroy semaphore of this process table entry.
     */
    e = THM_DestroyUnnamedSemaphore(handle, &(MY_TCB(handle)->semID));
    if (e < eNOERROR) ERR(handle, e);

    /*@
     * Free process contol block.
     */
    MY_TCB(handle)->next = LOGICAL_PTR(NULL);
    MY_TCB(handle) = NULL;

    return (eNOERROR);
}

/*@================================
 * bfm_InitLocalLatchList()
 *================================*/
/*
 * Function: Four bfm_InitLocalLatchList(void)
 * 
 * Description:
 *  Initialize local latch list.
 *
 * Returns:
 *  error codes
 */
Four bfm_InitLocalLatchList(Four handle)
{
    Four 		e;		/* error code */
    Four 		i;		/* loop index */
    LatchEntry  *latchList;	/* pointer of latch list */	

    e = Util_initVarArray(handle, &(BFM_PER_THREAD_DS(handle).grantedLatchStruct), sizeof(LatchEntry), MAXLATCHENTRIES);
    if (e < eNOERROR) ERR(handle, e);

    latchList = (LatchEntry* )(BFM_PER_THREAD_DS(handle).grantedLatchStruct.ptr);
    for (i = 0; i < BFM_PER_THREAD_DS(handle).grantedLatchStruct.nEntries; i++) {
	latchList[i].latchPtr = NULL;
	latchList[i].counter = 0;
    }

    return (eNOERROR);
}

/*@================================
 * bfm_FinalLocalLatchList()
 *================================*/
/*
 * Function: Four bfm_FinalLocalLatchList(void)
 * 
 * Description:
 *  Finalize local latch list.
 *
 * Returns:
 *  error codes
 */
Four bfm_FinalLocalLatchList(Four handle)
{
    Four	e;	/* error code */

    e = Util_finalVarArray(handle, &(BFM_PER_THREAD_DS(handle).grantedLatchStruct));
    if (e < eNOERROR) ERR(handle, e);

    return (eNOERROR);
}

#endif
