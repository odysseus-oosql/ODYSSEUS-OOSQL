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

#ifndef _TOOL_ERROR_H_
#define _TOOL_ERROR_H_

#include <stdio.h>
#include <stdlib.h>
#include "dblablib.h"

/* include error code definitions */
#include "TOOL_errorcodes.h"

#define eNOERROR 0

typedef struct TOOL_Err_ErrInfo_T_tag {
    Four    code;               /* error code */
    char    *name;              /* error name */
    char    *msg;               /* error message */
} TOOL_Err_ErrInfo_T;


typedef struct TOOL_Err_ErrBaseInfo_T_tag {
    char    *name;              /* error base name */
    char    *msg;               /* error base message */
    Four    nErrors;            /* # of error codes in the base */
} TOOL_Err_ErrBaseInfo_T;


/*
 * Macro Definitions
 */
#ifdef __cplusplus
extern "C" {
#endif
void Util_ErrorLog_Printf(char* msg, ...);
#ifdef __cplusplus
}
#endif

#define TOOL_PRTERR(e)                                                                              \
{                                                                                                   \
    Util_ErrorLog_Printf("Error : (%s) in %s:%d\n", TOOL_Err_GetErrName(e), __FILE__, __LINE__);    \
}

#define TOOL_ERR(e)                                                                                 \
{                                                                                                   \
    TOOL_PRTERR(e); if (1) return(e);                                                               \
}

#define TOOL_ERR_WITH_INFO(e, cause, action)                                                        \
{                                                                                                   \
}

#define TOOL_ERROR(handle, e)                                                                       \
{                                                                                                   \
    TOOL_PRTERR(e); if (1) return(e);                                                               \
}

#define TOOL_ERR_EXIT(e)                                                                            \
{                                                                                                   \
    TOOL_PRTERR(e); if (1) exit(1);                                                                 \
}

#define TOOL_CHECK_ERR(e)                                                                           \
    if(e < eNOERROR)    TOOL_ERR(e);

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Global Variables
 */
extern TOOL_Err_ErrBaseInfo_T   tool_err_errBaseInfo[];
extern TOOL_Err_ErrInfo_T*      tool_err_allErrInfo[];

/*
 * Function Prototypes
 */
char* TOOL_Err_GetErrBaseMsg(Four);
char* TOOL_Err_GetErrBaseName(Four);
char* TOOL_Err_GetErrMsg(Four);
char* TOOL_Err_GetErrName(Four);
void  TOOL_Err_Init(void);

#ifdef __cplusplus
}
#endif

#endif /* __ERROR_H__ */
