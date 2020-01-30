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
 * Module: lm_escalateLockOnFile.c
 *
 * Description:
 *   Escalate the lock for the given file.
 *
 * Exports:
 *  lm_escalateLockOnFile(handle, xactID, fileID, mode, duration, conditional, lockReply)
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


Four lm_escalateLockOnFile(
    Four    		handle,
    XactID 		*xactID,              	/* IN transaction identifier */
    FileID 		*fileID,              	/* IN file id. to be locked */
    LockMode 		mode,               	/* IN lock mode */
    LockDuration 	duration,       	/* IN lock duration */
    LockConditional 	conditional, 		/* IN conditional or unconditional ? */
    LockReply 		*lockReply)        	/* OUT L_OK/LR_NOTOK/LR_DEADLOCK */
{
    Four                e, e2;
    LockHashEntry	*lockHashEntryPtr;

    XactBucket_Type     *xBucket;
    LockBucket_Type     *lBucket;
    LockBucket_Type     *flBucket;

    RequestNode_Type	*aRequest, *nextRequest;
    Four                releaseLockCounter = 0;
    LockMode 		oldMode;           
    Seed        	tempSeed;		/* temp seed */


    /*@ get latch  */
    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*@ get the file lock on the given file */
    e = lm_getLock(handle, xactID, (TargetID*)fileID, L_FILE, NULL, mode, duration, conditional,
                   TRUE, lockReply, &xBucket, &flBucket, &oldMode); 
    if(e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    switch(*lockReply){
      case LR_NOTOK:
        /*
        ** Oh! I didn't escalate the lock on the given file..
        ** Release the LM_LATCH and return
        */
        e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
        if(e < eNOERROR) ERR(handle, e);

		return(LR_NOTOK);
      case LR_DEADLOCK:  ERRL1(handle, eDEADLOCK, &LM_LATCH);
      default: break; /* go ahead */
    }

    /* Now, you already have the lock on the file with 'mode' */
    /* Release all lower level locks */

#ifdef CCRL
    aRequest = PHYSICAL_PTR(xBucket->grantedList[L_OBJECT]);
#elif CCPL
    aRequest = PHYSICAL_PTR(xBucket->grantedList[L_PAGE]);
#endif

	while ( aRequest ) {

	lBucket = PHYSICAL_PTR(aRequest->lockHDR);

        if(PHYSICAL_PTR(lBucket->higherLock) != flBucket) {
		aRequest = PHYSICAL_PTR(aRequest->nextGrantedEntry);
		continue; /* This page is not belonged th the given file */
        }

	/* request latch for access the lockBucket */
	e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	/* save the pointer */
	nextRequest = PHYSICAL_PTR(aRequest->nextGrantedEntry);

#ifdef CCRL
	xBucket->nUnlock[L_OBJECT]++;
#elif CCPL
	xBucket->nUnlock[L_PAGE]++;
#endif

        DELETE_FROM_REQUESTNODE_DLIST(lBucket->queue, aRequest);
#ifdef CCRL
        DELETE_FROM_REQUESTNODE_DLIST2(xBucket->grantedList[L_OBJECT], aRequest);
#elif CCPL
        DELETE_FROM_REQUESTNODE_DLIST2(xBucket->grantedList[L_PAGE], aRequest);
#endif

        ((RequestNode_Type*)PHYSICAL_PTR(aRequest->higherRequestNode))->nLowLocks--;
        releaseLockCounter ++;


	/* release Request Node */
	FREE_REQUESTNODE(handle, aRequest);

	aRequest = nextRequest;

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
	    lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_LOCKID_HASH(lBucket->target, tempSeed, L_OBJECT)];

	    e = lm_deleteLockBucketFromChain(handle, lockHashEntryPtr, lBucket);
	    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	}

    }

    /*@ release latch */
    /* release lmLatch */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

