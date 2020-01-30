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
 * Module: LM_pageLock.c
 *
 * Description:
 *   lock request/release operation
 *
 * Exports: LM_getPageLock(handle, xactID, pid, fileID, mode, duration, conditional)
 *          LM_releasePageLock(handle, xactID, pid, duration)
 *
*/


#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "latch.h"
#include "Util.h"
#include "TM.h"
#include "LM.h"
#include "LM_macro.h"
#include "LM_LockMatrix.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

/*@================================
 * LM_getObjectLock( )
 *================================*/
/* ------------------------------------------------------------ */
/*								*/
/* LM_getObjectLock ::	 					*/
/*  	request lock of the Obj for this transaction 		*/
/*								*/
/* paprameters 							*/
/*    xactID	IN transaction identifier 			*/
/*    objectID	IN object identifier to be locked 		*/
/*    fileID	IN file identifier to be locked 		*/
/*    mode	IN lock mode 					*/
/*    duration	IN lock duration 				*/
/*    conditional  IN conditional or unconditional lock 	*/
/*    lockReply OUT (L_OK, LR_NOTOK, LR_DEADLOCK)		*/
/*								*/
/* return value							*/
/*    result messages 						*/
/*								*/
/* ------------------------------------------------------------ */

Four LM_getObjectLock(
    Four 		handle,
    XactID 		*xactID,        /* IN transaction identifier */
    ObjectID 		*oid,           /* IN obj id. to be locked */
    FileID 		*fileID,        /* IN file id.to which page belongs */
    LockMode 		mode,           /* IN lock mode */
    LockDuration 	duration,       /* IN lock duration */
    LockConditional 	conditional,  	/* IN conditional or unconditional ? */
    LockReply 		*lockReply,     /* OUT L_OK/L_NOT_OK/LR_DEADLOCK */
    LockMode  		*oldMode) 
{
    XactBucket_Type 	*xBucket;
    LockBucket_Type 	*flBucket;

    Four 		e;


    e = lm_getLock(handle, xactID, (TargetID*)oid, L_OBJECT, fileID, mode, duration, conditional, FALSE, lockReply,
                   &xBucket, &flBucket, oldMode); 
    if(e < eNOERROR) ERR(handle, e);


#ifdef LOCK_ESCALATION
    e = lm_lockEscalation(handle, xBucket);
    if(e < eNOERROR) ERR(handle, e);
#endif

    return(e);
}

/*@================================
 * LM_releaseObjectLock( )
 *================================*/
/* ------------------------------------------------------------ */
/*                                                              */
/* LM_releaseObjectLock ::                                      */
/*      unlock the object by locked this transaction            */
/*                                                              */
/* paprameters                                                  */
/*    xactID    IN transaction identifier                       */
/*    oid       IN oid identifier to be unlocked                */
/*    duration  IN lock duration                                */
/*                                                              */
/* return value                                                 */
/*              error number if error occurs                    */
/*                                                              */
/* ------------------------------------------------------------ */
Four LM_releaseObjectLock(
    Four    		handle,
    XactID 		*xactID,
    ObjectID 		*oid,
    LockDuration 	duration)
{
    Four		e;		/* function return value */

    e = lm_releaseLock(handle, xactID, (TargetID*)oid, L_OBJECT, duration);
    if(e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

}




