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
 * Module: rdsm_ReadTrain.c
 *
 * Description:
 *  Given the ID of a train, read it from a disk into a main memory buffer
 *
 * Exports:
 *  Four rdsm_ReadTrain(FileDesc, Four, void*, Two)
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
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*
 * Function: Four rdsm_ReadTrain(FileDesc, Four, void*, Two)
 *
 * Description:
 *   Write a train from a main memory buffer to a disk.
 *
 * Returns:
 *  Error code
 */

Four rdsm_ReadTrain(
    Four 		handle,
    FileDesc 	fd,                    /* IN  open file descriptor for the device */
    Four     	trainOffset,           /* IN  physical offset of train in given device (unit = # of page) */
    void     	*bufPtr,               /* IN  a pointer for a buffer page */
    Two      	sizeOfTrain)           /* IN  the size of a train in pages */
{
    Four     	e;                     /* returned error code */
    void     	*_bufPtr;              /* pointer of aligned buffer */
#ifdef WIN32
    Four  		readSize;
#endif

    TR_PRINT(TR_RDSM, TR1, ("rdsm_ReadTrain(handle, fd=%ld, trainOffset=%ld, bufPtr=%P, sizeOfTrain=%ld)",
                            fd, trainOffset, bufPtr, sizeOfTrain));

    /*
     * get aligned read/write buffer
     */
#ifdef READ_WRITE_BUFFER_ALIGN_FOR_LINUX
    if (RDSM_IS_ALIGNED_READ_WRITE_BUFFER(bufPtr))
        _bufPtr = bufPtr;
    else {
    	if (PAGESIZE*sizeOfTrain > RDSM_READ_WRITE_BUFFER_SIZE(&RDSM_PER_THREAD_DS(handle).rdsm_ReadWriteBuffer)) {
    	    e = rdsm_reallocReadWriteBuffer(handle, &RDSM_PER_THREAD_DS(handle).rdsm_ReadWriteBuffer, PAGESIZE*sizeOfTrain);
    	    if (e < eNOERROR) ERR(handle, e);	
        }
        _bufPtr = RDSM_READ_WRITE_BUFFER_PTR(&RDSM_PER_THREAD_DS(handle).rdsm_ReadWriteBuffer);
    }
#else
    _bufPtr = bufPtr;
#endif

    /*
     *	locate position on the given train
     */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
    if (lseek(fd, ((devOffset_t)trainOffset)*PAGESIZE, FROM_SET) == -1) ERR(handle, eLSEEKFAIL_RDSM); /* Type Converting */
#else
    if (lseek64(fd, ((devOffset_t)trainOffset)*PAGESIZE, FROM_SET) == -1) ERR(handle, eLSEEKFAIL_RDSM); /* Type Converting */
#endif

#else
    if (SetFilePointer(fd, trainOffset*PAGESIZE, NULL, FILE_BEGIN) == 0xFFFFFFFF) ERR(handle, eLSEEKFAIL_RDSM);
#endif

    /*
     * read the train into the buffer
     */
#ifndef WIN32
    if (read(fd, _bufPtr, PAGESIZE*sizeOfTrain) != PAGESIZE*sizeOfTrain) 
        ERR(handle, eREADFAIL_RDSM);
#else
    if (ReadFile(fd, _bufPtr, PAGESIZE*sizeOfTrain, &readSize, NULL) == 0 || readSize != PAGESIZE*sizeOfTrain) 
        ERR(handle, eREADFAIL_RDSM);
#endif

    /*
     * copy aligned system read/write buffer to unaligned user read/write buffer 
     */
#ifdef READ_WRITE_BUFFER_ALIGN_FOR_LINUX
    if (RDSM_IS_ALIGNED_READ_WRITE_BUFFER(bufPtr)) {
        /* do nothing */
    }
    else {
    	memcpy(bufPtr, _bufPtr, PAGESIZE*sizeOfTrain);
    }
#else
    /* do nothing */
#endif

    /*
     *	for benchmark statistics
     */
    rdsm_updateDiskStatistics(handle, fd, trainOffset, sizeOfTrain, 'R');


    return(eNOERROR);

} /* rdsm_ReadTrain() */
