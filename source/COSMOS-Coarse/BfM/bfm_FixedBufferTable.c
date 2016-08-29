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
 * Module: bfm_FixedBufferTable.c
 *
 * Description:
 *  Signal Handler to handle the signal cachted during BfM operations.
 *  It handles SIGINT, SIGQUIT. SIGTERM, SIGSEGV, SIGBUS, SIGTSPT.
 *  If catches the signals during BfM operations, record signal and just return,
 *  but, in other case, abort transaction and exit.
 *
 * Exports:
 *  bfm_InitFixedBufferInfo(void)
 *  bfm_FinalFixedBufferInfo(void)
 *  bfm_InsertFixedBuffer(BfMHashKey*, Four)
 *  bfm_DeleteFixedBuffer(BfMHashKey*, Four)
 *  bfm_DoubleFixedBufferTable(void)
 *  bfm_FreeAllFixedBuffers(void)
 */

#ifdef USE_SHARED_MEMORY_BUFFER	 

#include <unistd.h>	/* bluechry debug */
#include <pthread.h>	/* bluechry debug */
#include <stdlib.h>
#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

/*@================================
 * bfm_InitFixedBufferInfo
 *================================*/
/*
 * Function: Four bfm_InitFixedBufferInfo(void)
 *
 * Description:
 *  Allocated fixed buffer table.
 *
 * Returns:
 *  error code
 */
Four bfm_InitFixedBufferInfo(Four handle)
{
    Four e;     /* error code */
    Four i;     /* loop index */

    /*
     * Allocate memory space for fixed buffer table.
     */
    FBI_FIXED_BUFTABLE(handle) = (FixedBufferTable* )malloc(sizeof(FixedBufferTable) * FIXED_BUF_TABLESIZE);
    if (FBI_FIXED_BUFTABLE(handle) == NULL) ERR(handle, eMEMORYALLOCERR);

    FBI_UNFIXED_BUFTABLE(handle) = (FixedBufferTable* )malloc(sizeof(FixedBufferTable) * FIXED_BUF_TABLESIZE);
    if (FBI_UNFIXED_BUFTABLE(handle) == NULL) ERR(handle, eMEMORYALLOCERR);

    /*
     * Initialize the number of fixed buffer size and free fixed buffer header.
     */
    FBI_NFIXEDBUFS(handle) = 0;
    FBI_NUNFIXEDBUFS(handle) = 0;
    FBI_BUFTABLESIZE(handle) = FIXED_BUF_TABLESIZE;

    return (eNOERROR);
} /* bfm_InitFixedBufferInfo() */

/*@================================
 * bfm_FinalFixedBufferInfo
 *================================*/
/*
 * Function: Four bfm_FianlFixedBufferInfo(void)
 *
 * Description:
 *  Free allocated fixed buffer table.
 *
 * Returns:
 *  error code
 */
Four bfm_FinalFixedBufferInfo(Four handle)
{
    Four e;     /* error code */
    pid_t pid;

    pid = getpid();

    if (FBI_NFIXEDBUFS(handle) != FBI_NUNFIXEDBUFS(handle)) {
    	fprintf(stderr, "[pID=%d, tID=%d] handle value: %d\n", pid, pthread_self(), handle);
    	fprintf(stderr, "[pID=%d, tID=%d] FBI_NFIXEDBUFS: %d\n", pid, pthread_self(), FBI_NFIXEDBUFS(handle));
    	fprintf(stderr, "[pID=%d, tID=%d] FBI_NUNFIXEDBUFS: %d\n", pid, pthread_self(), FBI_NUNFIXEDBUFS(handle));

	ERR(handle, eINTERNAL);
    }

    /*
     * Free allocated fixed buffer table and unfixed buffer table.
     */
    free(FBI_FIXED_BUFTABLE(handle));
    free(FBI_UNFIXED_BUFTABLE(handle));

    return (eNOERROR);
} /* bfm_FinalFixedBufferInfo() */

/*@================================
 * bfm_DoubleFixedBufferTable
 *================================*/
/*
 * Function: Four bfm_DoubleFixedBufferTable(void)
 *
 * Description:
 *  Doubling fixed buffer table. 
 *
 * Returns:
 *  error code
 */
Four bfm_DoubleFixedBufferTable(Four handle)
{
    Four                e;                      /* error code */
    Four                i;                      /* loop index */
    Four                oldNFixedBufs;          /* the number of fixed buffer pages before doubling */
    Four                newNFixedBufs;          /* the number of fixed buffer pages after doubling */
    FixedBufferTable*   tempBufferTable;        /* temporary buffer table */

    newNFixedBufs = FBI_BUFTABLESIZE(handle) * 2;

    /*
     * Double the space of fixed buffer table and unfixed buffer table.
     */
    tempBufferTable = (FixedBufferTable* )realloc(FBI_FIXED_BUFTABLE(handle), newNFixedBufs * sizeof(FixedBufferTable));
    if (tempBufferTable == NULL) ERR(handle, eMEMORYALLOCERR);
    FBI_FIXED_BUFTABLE(handle) = tempBufferTable;

    tempBufferTable = (FixedBufferTable* )realloc(FBI_UNFIXED_BUFTABLE(handle), newNFixedBufs * sizeof(FixedBufferTable));
    if (tempBufferTable == NULL) ERR(handle, eMEMORYALLOCERR);
    FBI_UNFIXED_BUFTABLE(handle) = tempBufferTable;

    /*
     * Initialize the number of fixed buffer size and free fixed buffer header.
     */
    FBI_BUFTABLESIZE(handle) = newNFixedBufs;

    return (eNOERROR);
} /* bfm_DoubleFixedBufferTable() */

/*@================================
 * bfm_InsertFixedBuffer
 *================================*/
/*
 * Function: Four bfm_InsertFixedBuffer(BfMHashKey*, Four)
 *
 * Description:
 *  Insert fixed buffer into fixed buffer table. 
 *
 * Returns:
 *  error code
 */
Four bfm_InsertFixedBuffer(Four handle, BfMHashKey* key, Four type)
{
    Four e;

    if (FBI_NFIXEDBUFS(handle) >= FBI_BUFTABLESIZE(handle)) { 
        e = bfm_DoubleFixedBufferTable(handle); 
	if (e < eNOERROR) {
	    ERR(handle, e);
	}
    }

    FBI_FIXED_BUFTABLE(handle)[FBI_NFIXEDBUFS(handle)].key = *key; 
    FBI_FIXED_BUFTABLE(handle)[FBI_NFIXEDBUFS(handle)].type = type; 

    ++FBI_NFIXEDBUFS(handle); 

    return (eNOERROR);
} /* bfm_InsertFixedBufPage() */

/*@================================
 * bfm_DeleteFixedBuffer
 *================================*/
/*
 * Function: Four bfm_DeleteFixedBuffer(BfMHashKey*, Four)
 *
 * Description:
 *  Delete fixed buffer as inserting it into unfixed buffer table. 
 *
 * Returns:
 *  error code
 */
Four bfm_DeleteFixedBuffer(Four handle, BfMHashKey* key, Four type)
{
    FBI_UNFIXED_BUFTABLE(handle)[FBI_NUNFIXEDBUFS(handle)].key = *key; 
    FBI_UNFIXED_BUFTABLE(handle)[FBI_NUNFIXEDBUFS(handle)].type = type; 

    ++FBI_NUNFIXEDBUFS(handle); 

    return (eNOERROR);
} /* bfm_DeleteFixedBuf() */

#endif
