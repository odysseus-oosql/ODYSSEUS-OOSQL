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
 * Module: LM_xactLock.c
 *
 * Description:
 *   release all locks of the given transaction.
 *
 * Exports: LM_releaseXactLock
 *          LM_releaseXactManualFlatLock
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
 * LM_releaseXactLock( )
 *================================*/
/* ------------------------------------------------------------ */
/*								*/
/* LM_releaseXactLock :: 					*/
/*  	unlock all the lock locked by this transaction 		*/
/*								*/
/* paprameters 							*/
/*    xactID	IN transaction identifier 			*/
/*								*/
/* return value							*/
/*		error number if error occurs			*/
/*								*/
/* ------------------------------------------------------------ */
Four LM_releaseXactLock(
    Four 		handle,
    XactID 		*xactID
)
{
    LockHashEntry	*lockHashEntryPtr;

    XactBucket_Type	*xBucket;
    LockBucket_Type	*lBucket;

    RequestNode_Type	*aRequest;
    Four		e, e2;	/* function return value */
    Boolean		deletedFlag;
    LockLevel           tmpLevel;
    Seed        	tempSeed;   /* temp seed */

    /*@ get latch */
    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* if no xactbucket of this xactID, invalid transaction identifier error */
    e = find_xactBucket(handle, xactID, &xBucket);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    /*
    ** first release Page Lock and then release File Lock
    ** to keep the lock hierarchy protocol
    */

    /*@ release locks */
    /* release the page lockBucket from xBucket->grantedList[L_PAGE] */
    tmpLevel = L_OBJECT;
    while(tmpLevel <= L_FILE){
        aRequest = PHYSICAL_PTR(xBucket->grantedList[tmpLevel]);

        while ( aRequest ) {

            lBucket = PHYSICAL_PTR(aRequest->lockHDR);

            /* request latch for access the lockBucket */
            e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE,
                             M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

            DELETE_FROM_REQUESTNODE_DLIST(lBucket->queue, aRequest);
            DELETE_FROM_REQUESTNODE_DLIST2(xBucket->grantedList[tmpLevel], aRequest);

            /* release Request Node */
            FREE_REQUESTNODE(handle, aRequest);

            e = wakeup_nextRequest(handle, lBucket);
            if (e < 0) {

                e2 = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
                if (e2 < 0) ERRL1(handle, e2, &LM_LATCH);

                /* release lmLatch */
                e2 = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
                if (e2 < 0) ERR(handle, e2);

                ERR(handle, e);
            }

            if (PHYSICAL_PTR(lBucket->queue)) {

                e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
                if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);
            }
            else {			/* delete lBucket */

                /* delete bucket from linked list and free lockbucket */
                lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_LOCKID_HASH(lBucket->target, tempSeed, tmpLevel)];

                e = lm_deleteLockBucketFromChain(handle, lockHashEntryPtr, lBucket);
                if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

            }
            aRequest = PHYSICAL_PTR(xBucket->grantedList[tmpLevel]);

        }

        tmpLevel++;
    }

    /*@ release latch */
    /* release lmLatch */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

/*@================================
 * LM_releaseXactManualFlatLock( )
 *================================*/
/* ------------------------------------------------------------ */
/*								*/
/* LM_releaseXactManualFlatLock :: 				*/
/*  	unlock all the manual duration lock locked              */
/*	by this transaction 		                        */
/*								*/
/* paprameters 							*/
/*    xactID	IN transaction identifier 			*/
/*								*/
/* return value							*/
/*		error number if error occurs			*/
/*								*/
/* ------------------------------------------------------------ */

Four LM_releaseXactManualFlatLock(
    Four 		handle,
    XactID 		*xactID)
{
    XactHashEntry	*xactHashEntryPtr;
    LockHashEntry	*lockHashEntryPtr;

    XactBucket_Type	*xBucket;
    LockBucket_Type	*lBucket;
    LockLevel           level;

    RequestNode_Type	*aRequest;
    Four		e, e2;	/* function return value */
    Boolean		deletedFlag;
    Seed        	tempSeed;  /* temp seed */


    /*@ get latch */
    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*@ find the requested ones */
    /* if no xactbucket of this xactID, invalid transaction identifier error */
    e = find_xactBucket(handle, xactID, &xBucket);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);


    /*@ release locks */
    /* release the page/object lockBucket from xBucket->grantedList[L_FLAT_OBJECT/L_FLAT_PAGE] */
    for(level = L_FLAT_OBJECT; level <= L_FLAT_PAGE; level++){
      aRequest = PHYSICAL_PTR(xBucket->grantedList[level]);
      while ( aRequest ) {

   	if(aRequest->duration != L_MANUAL){
	    aRequest = PHYSICAL_PTR(aRequest->nextGrantedEntry); 
	    continue;
	}

	lBucket = PHYSICAL_PTR(aRequest->lockHDR);

	/* request latch for access the lockBucket */

	e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

        DELETE_FROM_REQUESTNODE_DLIST(lBucket->queue, aRequest);
        DELETE_FROM_REQUESTNODE_DLIST2(xBucket->grantedList[level], aRequest);

	/* release Request Node */
	FREE_REQUESTNODE(handle, aRequest);

	if (( e = wakeup_nextRequest(handle, lBucket)) < eNOERROR) {
	    e2 = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	    if (e2 < 0) ERR(handle, e2);

	    /* release lmLatch */
	    e2 = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	    if (e2 < 0) ERR(handle, e2);

	    ERR(handle, e);
	}

	if (PHYSICAL_PTR(lBucket->queue)) { 

	    e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);
	}
	else {

	    /* delete bucket from linked list and free lockbucket */
            lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_LOCKID_HASH(lBucket->target, tempSeed, level)]; 

	    e = lm_deleteLockBucketFromChain(handle, lockHashEntryPtr, lBucket);
	    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);
	}
	aRequest = PHYSICAL_PTR(xBucket->grantedList[level]);

      } /*end while */
    } /* end for */


#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

    /*@ release latch */
    /* release lmLatch */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}
