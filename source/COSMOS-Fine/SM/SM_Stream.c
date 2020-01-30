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
 * Module : SM_Stream.c
 *
 * Description :
 *  Sort a data file.
 *
 * Exports :
 * Four SM_OpenStream(Four);
 * Four SM_CloseStream(Four);
 * Four SM_PutTuplesIntoStream(Four, Four, SortStreamTuple*);
 * Four SM_ChangePhaseStream(Four);
 * Four SM_GetTuplesFromStream(Four, Four*, SortStreamTuple*, Boolean*);
 * Four SM_GetNumTuplesInStream(Four);
 * Four SM_GetSizeOfStream(Four);
 */

#include <assert.h> /* for assertion check */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Sort.h"
#include "SM.h"
#include "RDsM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/* ========================================
 *  SM_OpenStream()
 * =======================================*/

/*
 * Function SM_OpenStream(VolID)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four SM_OpenStream(
    Four		 handle,
    Four                 volId)                   /* IN  volume ID in which temporary files are allocated */
{
    LogParameter_T       logParam;


    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);
    return (Util_OpenStream(handle, MY_XACT_TABLE_ENTRY(handle), volId, &logParam));

} /* SM_OpenStream() */


/* ========================================
 *  SM_CloseStream()
 * =======================================*/

/*
 * Function Four SM_CloseStream(Four)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four SM_CloseStream(
    Four		  handle,
    Four                  streamId)                /* IN */
{
    LogParameter_T        logParam;

    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);
    return (Util_CloseStream(handle, MY_XACT_TABLE_ENTRY(handle), streamId, &logParam));

} /* SM_CloseStream() */


/* ========================================
 *  SM_PutTuplesIntoStream()
 * =======================================*/

/*
 * Function Four SM_PutTuplesIntoStream(Four, Four, SortStreamTuple*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four SM_PutTuplesIntoStream(
    Four		  handle,
    Four                  streamId,        /* IN */
    Four                  numTuples,       /* IN */
    SortStreamTuple*      tuples)          /* IN */
{
    LogParameter_T        logParam;

    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);
    return (Util_PutTuplesIntoStream(handle, MY_XACT_TABLE_ENTRY(handle), streamId, numTuples, tuples, &logParam));

} /* SM_PutTuplesIntoStream() */


/* ========================================
 *  SM_ChangePhaseStream()
 * =======================================*/

/*
 * Function Four SM_ChangePhaseStream(Four)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four SM_ChangePhaseStream(
    Four		  handle,
    Four                  streamId)        /* IN */
{
    LogParameter_T        logParam;

    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);
    return (Util_ChangePhaseStream(handle, MY_XACT_TABLE_ENTRY(handle), streamId, &logParam));

} /* SM_ChangePhaseStream */


/* ========================================
 *  SM_GetTuplesIntoStream()
 * =======================================*/

/*
 * Function Four SM_GetTuplesFromStream(Four, Four*, SortStreamTuple*, Boolean*)
 *
 * Description :
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 */
Four SM_GetTuplesFromStream(
    Four		     handle,
    Four                     streamId,           /* IN */
    Four*                    numTuples,          /* INOUT */
    SortStreamTuple*         tuples,             /* OUT */
    Boolean*                 eof)                /* OUT */
{
    return (Util_GetTuplesFromStream(handle, streamId, numTuples, tuples, eof));

} /* SM_GetTuplesFromStream() */


/* ========================================
 *  SM_GetNumTuplesInStream()
 * =======================================*/

Four SM_GetNumTuplesInStream(
    Four	  handle,
    Four          streamId)            /* IN */
{
    return (Util_GetNumTuplesInStream(handle, streamId));

} /* SM_GetNumTuplesInStream() */


/* ========================================
 *  SM_GetSizeOfStream()
 * =======================================*/

Four SM_GetSizeOfStream(
    Four	  handle,
    Four          streamId)            /* IN */
{
    return (Util_GetSizeOfStream(handle, streamId));

} /* SM_GetSizeOfStream() */
