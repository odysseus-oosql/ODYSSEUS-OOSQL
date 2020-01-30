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
#include <stdlib.h>
#include "perProcessDS.h"
#include "perThreadDS.h"

#include <errno.h>

#include "common.h"
#include "error.h"
#include "trace.h"
#include "RM.h"
#include "SHM.h"


/* Static Variable */
static Four_Invariable  shmId;
static Four 		sharedHeapSize;		/* needed shared memory size */



/*@================================
 * SHM_beginProcess( )
 *================================*/
Four SHM_beginProcess(
    Four 		handle,
    CfgParams_T 	*cfgParams,     	/* IN configuration parameters */
    ComponentInfo_T 	componentInfos[], 	/* IN component informations */
    Four 		nComponents)           	/* IN number of components */
{

    Four 		i;			/* loop index */
    Four		e;              	/* error number */


    /*
     *  Evaluate the size
     */
    /* these are defined in BfM_init() */
    sharedHeapSize = PAGESIZE*NUM_PAGE_BUFS+ /* for page size buffer */
                     PAGESIZE*NUM_LOT_LEAF_BUFS*TRAINSIZE2+/* for large object buffer */
                     sizeof(bfmHashEntry)*NUM_PAGE_BUFS*7+sizeof(Lock_hashEntry)*NUM_PAGE_BUFS*7+
                     sizeof(Lock_ctrlBlock)*NUM_PAGE_BUFS*7+MAXFREESPACE;


    /*
     *  Get shard memeory from system
     */
    shmPtr = (SemStruct *) malloc(sizeof(SemStruct)+sharedHeapSize);
    if (shmPtr == NULL) ERR(handle, eMEMORYALLOCERR);


    /*
     * Initialize all the local data structures of all the components.
     */
    shm_assignSharedPtr(handle);


    /*
     * Initialize all the shared data structures of all the components.
     */

    common_shmPtr->cfgParams = *cfgParams;
    common_shmPtr->recoveryFlag = FALSE;

    e = shm_initSharedHeap(handle, sharedHeapSize);
    if (e < eNOERROR) ERR(handle, e);

    for (i = 0; i < nComponents; i++) {
        if (componentInfos[i].initSharedDS != NULL) {
            e = componentInfos[i].initSharedDS(handle);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    NUM_OF_THREADS_IN_SYSTEM = 1;


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
    Four 		i;



    handle = previousHandle;

    /*
     * Finalize all the shared & local data structures of all the components.
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

    NUM_OF_THREADS_IN_SYSTEM = 0;

    /*
     *  Free shard memeory from system
     */
    free(shmPtr);

    e = thm_FinalPerThreadDS(handle);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SHM_endProcess() */
