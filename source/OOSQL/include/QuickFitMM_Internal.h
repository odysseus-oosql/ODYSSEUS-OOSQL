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
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
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

#ifndef _QuickFitMM_INTERNAL_H_
#define _QuickFitMM_INTERNAL_H_

#include "QuickFitMM_common.h"
#include "QuickFitMM_Err.h"

#ifndef eNOERROR
#define eNOERROR        0
#endif
#ifndef BEGIN_MACRO
#define BEGIN_MACRO     do {
#endif
#ifndef END_MACRO
#define END_MACRO       } while(0)
#endif

#define QUICKFITMM_ERR(e) \
BEGIN_MACRO \
printf("File:%s Line:%d\n", __FILE__, __LINE__); \
if (1) return(e);  \
END_MACRO

#define QUICKFITMM_PRTERR(e) \
BEGIN_MACRO \
printf("File:%s Line:%d\n", __FILE__, __LINE__); \
END_MACRO

#define QuickFitMM_ALLOCUNIT            32
#define GUARDSPACE_SIZE                 10

/* Header of block */
typedef struct _quickFitMM_header {
        char         frontGuardSpace[GUARDSPACE_SIZE];  
        struct _quickFitMM_header *prevBlock; /* previous header pointer */
        unsigned long size;
        unsigned int isFree; /* check if this entry is free or not */
        unsigned int isLast; /* check if this block is last or not */
        char         rearGuardSpace[GUARDSPACE_SIZE];
} QuickFitMM_Header;

/* Free block Information */
typedef struct _quickFitMM_freeblockinfo {
        struct _quickFitMM_freeblockinfo *prevFreeBlock; /* previous free block */
        struct _quickFitMM_freeblockinfo *nextFreeBlock; /* next free block */
} QuickFitMM_FreeBlockInfo;

/* data structure of Buffer */
typedef struct _quickFitMM_bufferpool {
        struct _quickFitMM_bufferpool *nextFreeBufferPool; /* next free buffer pool */
        char data[sizeof(long)];
} QuickFitMM_BufferPool;

/* free list tbl entry */
typedef struct {
        struct _quickFitMM_freeblockinfo *nextFreeBlock;
} QuickFitMM_FreeListTbl;

/* handle to memory manager */
typedef struct {
    Four maxBufferSize;
    Four freeListTblSize;
    QuickFitMM_BufferPool *mm_bufferPool;
    QuickFitMM_FreeListTbl *mm_freeListTbl;
} QuickFitMM_Handle;

/* Error Handling */
#define QuickFitMM_ERROR(e) \
if (1) { \
printf("Error Code %d: %s\n", e, QuickFitMM_Err(e)); \
printf("File:%s Line:%d\n", __FILE__, __LINE__); \
fflush(stdout); \
if (1) return(e);  \
} else

#endif _QuickFitMM_INTERNAL_H_ 
