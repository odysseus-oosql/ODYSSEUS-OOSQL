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
 * Module: LM_initDS.c
 *
 * Description:
 *  Initialize data structures used in lock manager.
 *
 * Exports:
 *  Four LM_initSharedDS(Four)
 *  Four LM_initLocalDS(Four)
*/


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "LM.h"
#include "LM_macro.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * Four LM_initSharedDS( )
 *================================*/
/*
 * Function: Four LM_initSharedDS(Four)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */
Four LM_initSharedDS(
    Four    	handle
)
{
    Four 	e;                     /* error code */


    TR_PRINT(handle, TR_LM, TR1, ("LM_initSharedDS()"));

    /* initialize the lock table structure */
    e = LM_initLockTBL(handle);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LM_initSharedDS() */



/*@================================
 * Four LM_initLocalDS( )
 *================================*/
/*
 * Function: Four LM_initLocalDS(Four)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */
Four LM_initLocalDS(
    Four    	handle
)
{
    Four 	e;             /* error number */


    TR_PRINT(handle, TR_LM, TR1, ("LM_initLocalDS()"));

    LM_DISABLE_ACTION_FLAG(handle);
    LM_DISABLE_AUTO_ACTION_FLAG(handle); 

    return(eNOERROR);

} /* LM_initLocalDS() */


/*@================================
 * LM_initLockTBL( )
 *================================*/
Four LM_initLockTBL(
    Four    		handle
)
{
    register Four	i;


    Util_initPool(handle, &LM_REQUESTNODEPOOL, sizeof(RequestNode_Type),MAXREQUESTNODE);
    Util_initPool(handle, &LM_LOCKBUCKETPOOL, sizeof(LockBucket_Type), MAXLOCKBUCKET);
    Util_initPool(handle, &LM_XACTBUCKETPOOL, sizeof(XactBucket_Type), MAXXACTBUCKET);

    SHM_initLatch(handle, &LM_LATCH);
    SHM_initLatch(handle, &LM_MAX_LOCKS_ON_FILE_LATCH); 

    LM_MAX_LOCKS_ON_FILE = LM_INIT_MAX_LOCKS_ON_FILE; 

    for( i = 0; i < MAXLOCKHASHENTRY; i++) {

	LM_LOCKHASHTABLE[i].bucketPtr = LOGICAL_PTR(NULL);
	SHM_initLatch(handle, &LM_LOCKHASHTABLE[i].latch);
    }

    for( i = 0; i < MAXXACTHASHENTRY; i++) {

	LM_XACTHASHTABLE[i].bucketPtr = LOGICAL_PTR(NULL);
	SHM_initLatch(handle, &LM_XACTHASHTABLE[i].latch);
    }

    return(eNOERROR);
}



/*@================================
 * LM_cleanupLockTBL( )
 *================================*/
Four LM_cleanupLockTBL(
    Four    	handle
)
{

    Util_finalPool(handle, &LM_REQUESTNODEPOOL);
    Util_finalPool(handle, &LM_LOCKBUCKETPOOL);
    Util_finalPool(handle, &LM_XACTBUCKETPOOL);

    return(eNOERROR);
}
