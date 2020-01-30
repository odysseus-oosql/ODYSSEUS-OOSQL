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
 * Module: rdsm_ReadWriteBuffer.c
 *
 * Description:
 *  Manage a system read/write buffer
 *
 * Exports:
 *  Four rdsm_InitReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*)
 *  Four rdsm_reallocReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*, Four)
 *  Four rdsm_FinalReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*)
 */


#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "RDsM.h"


#ifdef READ_WRITE_BUFFER_ALIGN_FOR_LINUX


/*
 * Function: Four rdsm_InitReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*)
 *
 * Description:
 *   Initialize aligned system read/write buffer
 *
 * Returns:
 *  Error code
 */
Four rdsm_InitReadWriteBuffer(
    Four                                handle,                 /* handle */
    RDsM_ReadWriteBuffer_T		*buffer			/* read/write buffer */
)
{
    Four			offset;


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_InitReadWriteBuffer(buffer=%p)", buffer));


    /* Parameter Check */
    if (buffer == NULL) ERR(handle, eBADPARAMETER);

    /* alloc read/write buffer */
    buffer->size = RDSM_READ_WRITE_BUFFER_INIT_SIZE+RDSM_READ_WRITE_BUFFER_ALIGN_MASK;

    buffer->ptr = (void*)malloc(buffer->size);
    if (buffer->ptr == NULL) ERR(handle, eMEMORYALLOCERR);

    /* align allocated read/write buffer */
    if (((MEMORY_ALIGN_TYPE)(buffer->ptr) & RDSM_READ_WRITE_BUFFER_ALIGN_MASK) == 0)
        offset = 0;
    else
        offset = (RDSM_READ_WRITE_BUFFER_ALIGN_MASK+0x00000001) - ((MEMORY_ALIGN_TYPE)(buffer->ptr) & RDSM_READ_WRITE_BUFFER_ALIGN_MASK);

    if (offset > RDSM_READ_WRITE_BUFFER_ALIGN_MASK) ERR(handle, eINTERNAL);

    buffer->alignedPtr  = (void*)((char*)(buffer->ptr) + offset);
    buffer->alignedSize = buffer->size - offset;


    return (eNOERROR);
}

/*
 * Function: Four rdsm_reallocReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*, Four)
 *
 * Description:
 *   Increase aligned system read/write buffer size
 *
 * Returns:
 *  Error code
 */
Four rdsm_reallocReadWriteBuffer(
    Four                        handle,                 /* handle */
    RDsM_ReadWriteBuffer_T	*buffer,		/* read/write buffer */
    Four			needSize		/* need size (Unit = byte) */
)
{
    Four			allocSize;
    Four			offset;
    char			*tempPtr;


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_reallocReadWriteBuffer(buffer=%p, needSize=%ld)", buffer, needSize));


    /* Parameter Check */
    if (buffer == NULL) ERR(handle, eBADPARAMETER);
    if (needSize <= 0) ERR(handle, eBADPARAMETER);

    /* if needSize is less than or equal to RDSM_READ_WRITE_BUFFER_SIZE, do nothing */
    if (needSize <= RDSM_READ_WRITE_BUFFER_SIZE(buffer)) return (eNOERROR);

    /* calculate 'allocSize' */
    allocSize = buffer->size - RDSM_READ_WRITE_BUFFER_ALIGN_MASK;

    do {
    	allocSize *= 2;
    } while (allocSize < needSize);

    /* alloc read/write buffer */
    tempPtr = buffer->ptr;

    buffer->size = allocSize+RDSM_READ_WRITE_BUFFER_ALIGN_MASK;

    buffer->ptr = (void*)realloc(buffer->ptr, buffer->size);
    if (buffer->ptr == NULL) {
    	free(tempPtr);
    	ERR(handle, eMEMORYALLOCERR);
    }

    /* align allocated read/write buffer */
    if (((MEMORY_ALIGN_TYPE)(buffer->ptr) & RDSM_READ_WRITE_BUFFER_ALIGN_MASK) == 0)
        offset = 0;
    else
        offset = (RDSM_READ_WRITE_BUFFER_ALIGN_MASK+0x00000001) - ((MEMORY_ALIGN_TYPE)(buffer->ptr) & RDSM_READ_WRITE_BUFFER_ALIGN_MASK);

    if (offset > RDSM_READ_WRITE_BUFFER_ALIGN_MASK) ERR(handle, eINTERNAL);

    buffer->alignedPtr  = (void*)((char*)(buffer->ptr)  + offset);
    buffer->alignedSize = buffer->size - offset;


    return(eNOERROR);
}

/*
 * Function: Four rdsm_FinalReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*)
 *
 * Description:
 *   Finalize aligned system read/write buffer
 *
 * Returns:
 *  Error code
 */
Four rdsm_FinalReadWriteBuffer(
    Four                                handle,                 /* handle */
    RDsM_ReadWriteBuffer_T		*buffer			/* read/write buffer */
)
{
    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_FinalReadWriteBuffer(buffer=%p)", buffer));


    /* Parameter Check */
    if (buffer == NULL) ERR(handle, eBADPARAMETER);

    /* free read/write buffer */
    free(buffer->ptr);


    return (eNOERROR);
}
#endif /* READ_WRITE_BUFFER_ALIGN_FOR_LINUX */
