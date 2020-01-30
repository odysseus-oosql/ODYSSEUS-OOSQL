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
 * Module: LRDS_Counter.c
 *
 * Description:
 *  Implements a counter.
 *
 * Exports:
 *  Four LRDS_CreateCounter(Four, Four, char*, Four, CounterID*)
 *  Four LRDS_DestroyCounter(Four, Four, char*)
 *  Four LRDS_GetCounterId(Four, Four, char*, CounterID*)
 *  Four LRDS_SetCounterId(Four, CounterID*, Four)
 *  Four LRDS_ReadCounter(Four, Four, CounterID*, Four*)
 *  Four LRDS_GetCounterValues(Four, Four, CounterID*, Four, Four*)
 */

#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: Four LRDS_CreateCounter(Four, Four, char*, Four, CounterID*)
 *
 * Description:
 *  Creates a counter as the given name.
 *
 * Returns:
 *  error code
 */
Four LRDS_CreateCounter(
    Four handle,
    Four volId,                 /* IN volume id */
    char *cntrName,             /* IN counter name */
    Four initialValue,          /* IN initialize the counter as this value */
    CounterID *cntrId)          /* OUT counter id */
{
    Four e;                     /* error code */

    e = SM_CreateCounter(handle, volId, cntrName, initialValue, cntrId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}


/*
 * Function: Four LRDS_DestroyCounter(Four, Four, char*)
 *
 * Description:
 *  Destroy the given counter.
 *
 * Returns:
 *  error code
 */
Four LRDS_DestroyCounter(
    Four handle,
    Four volId,                 /* IN volume id */
    char *cntrName)             /* IN counter name */
{
    Four e;                     /* error code */

    e = SM_DestroyCounter(handle, volId, cntrName);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}


/*
 * Function: Four LRDS_GetCounterId(Four, Four, char*, CounterID*)
 *
 * Description:
 *  Returns the internal id of the given counter.
 *
 * Returns:
 *  error code
 */
Four LRDS_GetCounterId(
    Four handle,
    Four volId,                 /* IN volume id */
    char *cntrName,             /* IN counter name */
    CounterID *cntrId)          /* OUT counter id */
{
    Four e;                     /* error code */

    e = SM_GetCounterId(handle, volId, cntrName, cntrId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}


/*
 * Function: Four LRDS_SetCounter(Four, Four, CounterID*, Four)
 *
 * Description:
 *  Set the counter to the given value.
 *
 * Returns:
 *  error code
 */
Four LRDS_SetCounter(
    Four handle,
    Four volId,                 /* IN volume id */
    CounterID *cntrId,          /* IN counter id */
    Four value)                 /* IN set the counter to this value */
{
    Four e;                     /* error code */

    e = SM_SetCounter(handle, volId, cntrId, value);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}


/*
 * Function: Four LRDS_ReadCounter(Four, Four, CounterID*, Four*)
 *
 * Description:
 *  Read the current value from the counter.
 *
 * Returns:
 *  error code
 */
Four LRDS_ReadCounter(
    Four handle,
    Four volId,                 /* IN volume id */
    CounterID *cntrId,          /* IN counter id */
    Four *value)                /* OUT the current counter value */
{
    Four e;                     /* error code */

    e = SM_ReadCounter(handle, volId, cntrId, value);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}


/*
 * Function: Four LRDS_GetCounterValues(Four, Four, CounterID*, Four, Four*)
 *
 * Description:
 *  The counter allocates the given number of numbers to the user.
 *  The numbers start from the startValue and contiguous.
 *
 * Returns:
 *  error code
 */
Four LRDS_GetCounterValues(
    Four handle,
    Four volId,                 /* IN volume id */
    CounterID *cntrId,          /* IN counter id */
    Four nValues,               /* IN number of values to be allocated */
    Four *startValue)           /* OUT allocated numbers start from this value */
{
    Four e;                     /* error code */

    e = SM_GetCounterValues(handle, volId, cntrId, nValues, startValue);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

