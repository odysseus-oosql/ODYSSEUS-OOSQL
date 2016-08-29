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

#ifndef _QuickFitMM_HXX_
#define _QuickFitMM_HXX_

#include "dblablib.h"

#ifdef INTERNAL
#include "QuickFitMM_Internal.h"
#else
typedef struct quickfitmm_header QuickFitMM_Header;
typedef struct {
    Four dummy[4];
} QuickFitMM_Handle;
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifndef QUICKFITMM_ERR
#define QUICKFITMM_ERR(e) \
{ \
printf("File:%s Line:%ld\n", __FILE__, __LINE__); \
if (1) return(e);  \
}
#endif

/* Memory Manager Initialization/Finalization/Doubling */
Four QuickFitMM_Init(QuickFitMM_Handle *mm_handle, Four );
Four QuickFitMM_ReInit(QuickFitMM_Handle *mm_handle);
Four QuickFitMM_Final(QuickFitMM_Handle *mm_handle);
Four QuickFitMM_DoubleBufferPool(QuickFitMM_Handle *mm_handle);

/* Allocation/Deallocation/Reallocation of memory */
void* QuickFitMM_Alloc(QuickFitMM_Handle *mm_handle, Four);
Four QuickFitMM_ReAlloc(QuickFitMM_Handle *mm_handle, void **p, Four size);
Four QuickFitMM_Free(QuickFitMM_Handle *mm_handle, char *);
Four QuickFitMM_Free_Void_Pointer(QuickFitMM_Handle *mm_handle, void *);


/* Error routine */
Four QuickFitMM_Check(QuickFitMM_Handle*);
char *QuickFitMM_Err(Four);
Four QuickFitMM_PutErr();

#ifdef INTERNAL
/* internal function */
void _quickFitMM_insertFreeBlkToFreeListTbl(QuickFitMM_Handle *mm_handle, QuickFitMM_Header *,Four);
void _quickFitMM_removeFreeBlkFromFreeListTbl(QuickFitMM_Handle *mm_handle, QuickFitMM_Header *, Four);
Four getIndexOfFreeListTbl(Four);
#endif

#ifdef __cplusplus
}
#endif

extern Four _quickFitMM_errno;

#endif _QuickFitMM_HXX_

