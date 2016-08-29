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
#ifndef __ERROR_H__
#define __ERROR_H__


/* include error code definitions */
#include "errorcodes.h"
#include "Util_errorLog.h" 


typedef struct Err_ErrInfo_T_tag {
    Four code;			/* error code */
    char *name;			/* error name */
    char *msg;			/* error message */
} Err_ErrInfo_T;


typedef struct Err_ErrBaseInfo_T_tag {
    char *name;			/* error base name */
    char *msg;			/* error base message */
    Four nErrors;		/* # of error codes in the base */
} Err_ErrBaseInfo_T;


/*
 * Macro Definitions
 */
#define PRTERR(handle, e) \
BEGIN_MACRO \
Util_ErrorLog_Printf("Error : %d(%s) in %s:%d\n", ((Four_Invariable)(e)), Err_GetErrName(e), __FILE__, __LINE__); \
END_MACRO

#define ERR(handle, e) \
BEGIN_MACRO \
    PRTERR(handle, e); if (1) return(e); \
END_MACRO

#define ERRB1(handle, e, pid, t) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    (Four) BfM_FreeTrain((handle), (pid),(t)); \
    if (1) return(e); \
END_MACRO
    
#define ERRB2(handle, e, pid1, t1, pid2, t2) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    (Four) BfM_FreeTrain((handle), (pid1),(t1)); \
    (Four) BfM_FreeTrain((handle), (pid2),(t2)); \
    if (1) return(e); \
END_MACRO

/* For multi thread, insert handle to each macro */

#define ERRL1(handle, e, latch) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    (Four) SHM_releaseLatch((handle), (latch), procIndex); \
    if (1) return(e); \
END_MACRO
    
#define ERRBL1(handle, e, bp, t) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    (Four) SHM_releaseLatch((handle), (bp)->latchPtr, procIndex); \
    (Four) BfM_unfixBuffer((handle), (bp),(t)); \
    if (1) return(e); \
END_MACRO
    
#define ERRBL2(handle, e, bp1, t1, bp2, t2) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    (Four) SHM_releaseLatch((handle), (bp1)->latchPtr, procIndex); \
    (Four) BfM_unfixBuffer((handle), (bp1),(t1)); \
    (Four) SHM_releaseLatch((handle), (bp2)->latchPtr, procIndex); \
    (Four) BfM_unfixBuffer((handle), (bp2),(t2)); \
    if (1) return(e); \
END_MACRO
    
#define ERRBL1L1(handle, e, bp, t, latch) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    (Four) SHM_releaseLatch((handle), (bp)->latchPtr, procIndex); \
    (Four) BfM_unfixBuffer((handle), (bp),(t)); \
    (Four) SHM_releaseLatch((handle), (latch), procIndex); \
    if (1) return(e); \
END_MACRO

#define ERROR_PASS(handle, func) \
BEGIN_MACRO \
  Four _E = func; \
  if ( _E < eNOERROR ) ERR(handle, _E); \
END_MACRO

#define GOTO_ERROR(handle, _e) \
BEGIN_MACRO \
  PRTERR(handle, _e); \
  if (1) goto ERROR_LABEL; \
END_MACRO

#ifdef USE_SHARED_MEMORY_BUFFER

#define ERR_BfM(handle, e, pTrainID, type) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    bfm_Unlock((handle), pTrainID, type); \
    if (1) return(e); \
END_MACRO

#else

#define ERR_BfM(handle, e, pTrainID, type) \
BEGIN_MACRO \
    PRTERR(handle, e); \
    if (1) return(e); \
END_MACRO

#endif

/*
 * Global Variables
 */
extern Err_ErrBaseInfo_T err_errBaseInfo[];
extern Err_ErrInfo_T     *err_allErrInfo[];


/*
 * Function Prototypes
 */
char *Err_GetErrBaseMsg(Four);
char *Err_GetErrBaseName(Four);
char *Err_GetErrMsg(Four, Four);
char *Err_GetErrName(Four);
void Err_Init(Four);


#endif /* __ERROR_H__ */
