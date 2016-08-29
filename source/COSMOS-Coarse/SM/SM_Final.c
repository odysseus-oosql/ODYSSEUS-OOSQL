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
 * Module: SM_Final.c
 *
 * Description:
 *  Finalize the storage system.
 *
 * Exports:
 *  Four SM_Final(void)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "RM.h"                
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"
#include "THM_Internal.h"



/*@================================
 * SM_Final()
 *================================*/
/*
 * Function: Four SM_Final(void)
 *
 * Description:
 *  Finalize the storage system. At first, dismount the mounted volume. Then
 *  free all dynamically allocated memory. At last, finalize the sub layers;
 *  Buffer Manager and Raw Disk Manager.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four _SM_Final()
{
    Four e;
    Four handle;
    FileDesc	fd1;	/* lock file descriptor for starting critical section */ 
    FileDesc	fd2;	/* lock file descriptor for ending critical section */ 

    TR_PRINT(TR_SM, TR1, ("_SM_Final()"));

    handle = -1;

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* Start of critical section for shared memory */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd1); 
#endif

    if (IS_PER_PROCESS_DS_INITIALIZED == TRUE && NUM_OF_THREADS_IN_PROCESS == 0) {

	IS_PER_PROCESS_DS_INITIALIZED = FALSE;

    	e = THM_FinalProcess();
    	if (e < eNOERROR) ERR(handle, e);
    }

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* End of critical section for shared memory */ 
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd2);

    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd2);
#endif

    return(eNOERROR);

} /* SM_Final() */

Four _SM_FreeHandle(Four handle)
{
    Four e; 
    FileDesc	fd1;	/* lock file descriptor for starting critical section */ 
    FileDesc	fd2;	/* lock file descriptor for ending critical section */ 

    TR_PRINT(TR_SM, TR1, ("_SM_FreeHandle(handle)"));

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* start of critical section for shared memory */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd1); 
#endif

#ifdef LRDS_INCLUDED
    e = LRDS_FinalLocalDS(handle);
    if (e < eNOERROR) ERR(handle, e);

    e = LRDS_FinalSharedDS(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    e = sm_FinalizeGlobalData(handle);
    if (e < eNOERROR) ERR(handle, e);

    e = THM_FreeHandle(handle);
    if (e < eNOERROR) ERR(handle, e);

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* end of critical section for shared memory */
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd2); 

    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd2);
#endif

    return(eNOERROR);
} /* _SM_FreeHandle(handle) */

Four sm_FinalizeGlobalData(Four handle)
{

    Four e;			/* error code */
    Four v;			/* array index on the mount table */

    TR_PRINT(TR_SM, TR1, ("sm_FinalizeGlobalData"));

    if (SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eEXISTACTIVETRANSACTION_SM);
    
    /* Dismount the volumes that has been mounted but not dismounted. */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId != NIL) { /* entry for a mounted volume */
	    e = _SM_Dismount(handle, SM_PER_THREAD_DS(handle).smMountTable[v].volId);
	    if (e < 0) ERR(handle, e);
	}
    
    /* Finalize the main memory data structure used in scan manager */
    e = Util_finalVarArray(handle, &SM_PER_THREAD_DS(handle).smScanTable);
    if (e < 0) ERR(handle, e);

    /* Finalize the main memory data structure used for temporary file ID table */
    e = Util_finalVarArray(handle, &SM_PER_THREAD_DS(handle).smTmpFileIdTable);
    if (e < 0) ERR(handle, e);

    /* Finalize the deallocatedPageList pool. */
    e = Util_finalPool(handle, &SM_PER_THREAD_DS(handle).dlPool);
    if (e < 0) ERR(handle, e);
    
    /* Finalize the Buffer Manager */
#ifdef USE_COHERENCY_VOLUME
	e = BfM_DismountCoherencyVolume(handle);
	if (e < 0) ERR(handle, e);
#endif
        
    e = LOT_Final(handle);
    if (e < 0) ERR(handle, e);

    e = BfM_FinalPerThreadDS(handle);
    if (e < 0) ERR(handle, e);

    /* Finalize the Recovery Manager */
    e = RM_Final(handle);
    if (e < 0) ERR(handle, e);
    
    /* Finalize the Raw Disk Manager */
    e = RDsM_Finalize(handle);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
}

