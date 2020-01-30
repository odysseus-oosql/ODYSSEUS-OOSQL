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
 * Module: lm_lock.c
 *
 * Description:
 *   lock request/release operation
 *
 * Exports: lm_getLock(handle, xactID, fileID, mode, duration, conditional)
 *          lm_releaseLock(handle, xactID, fileID, duration)
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


Four lm_getLock(
    Four	    	handle,
    XactID          	*xactID,	        /* IN transaction identifier */
    TargetID        	*lockID,	        /* IN target lock id. to be locked */
    LockLevel       	level,               	/* IN level of the lock id */
    FileID          	*fileID,             	/* IN fileID when level == L_OBJECT || level == L_PAGE */
    LockMode        	mode,	         	/* IN lock mode */
    LockDuration    	duration,            	/* IN lock duration */
    LockConditional 	conditional,         	/* IN conditional or unconditional ? */
    Boolean         	escalateFlag,        	/* IN TRUE if lock escalation case */
    LockReply       	*lockReply,          	/* OUT L_OK/LR_NOTOK/LR_DEADLOCK */
    XactBucket_Type 	**xactBucket,        	/* OUT xactBucket of this transaction */
    LockBucket_Type 	**outLBucket,       	/* OUT lock bucket of this lockid */
    LockMode        	*oldModeForReturn) 
{
    XactBucket_Type     *xBucket;
    LockBucket_Type	*lBucket;
    LockBucket_Type	*higherLockBucket;
    LockHashEntry	*lockHashEntryPtr;

    RequestNode_Type	*aRequest;
    RequestNode_Type	*higherRequest;
    Four		i;			/* index of for loop */
    Four		e, e2;			/* function return value */

    LockMode 		oldMode; 		/* save old lock mode for instant duration */
    LockDuration	oldDuration; 		/* save old lock duration for lock conversion */
    Boolean		deletedFlag;
    LockMode            groupMode; 		/* groupMode of lBucket without considering a request node */
    Boolean             convertFlag; 		/* Flag for checking L_CONVERTING status */
    Seed        	tempSeed;   		/* temp seed */

#ifdef DEBUG
    static Four counter = 0;

    printf("call file lock: %ld\n", ++counter);
#endif

    /* Initialize the parameter */
    *outLBucket = NULL;
    higherRequest = NULL;

    /*@ get latch  */
    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

    /*@ find the requested ones */

    /* if no xactbucket of this xactID, invalid transaction identifier error */
    e = find_xactBucket(handle, xactID, &xBucket);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    /* set the OUT parameter */
    *xactBucket = xBucket;

    /* check lock hierarchy */
    if((level == L_OBJECT || level == L_PAGE) && fileID != NULL){ 
        /* search the RequestNode from xBucket->grantedList[L_FILE] */
        SEARCH_REQUESTNODEOFFILE(xBucket->grantedList[L_FILE], fileID, higherRequest);

        /* if no RequestNode of this fileID exists, ERROR !! */
	if (higherRequest){

     	    higherLockBucket = PHYSICAL_PTR(higherRequest->lockHDR);

       	    /* request latch for access the lockBucket */
       	    e = SHM_getLatch(handle, &higherLockBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
       	    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

       	    switch ( lm_perProcessDSptr->LOCK_hierarchy[higherRequest->mode][mode] ) {

       	   	case OK	    :	break;

       	   	case NONEED :
       	     	    e = SHM_releaseLatch(handle, &higherLockBucket->latch, procIndex);
       	            if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

       	            /* return OK result */
       	            *lockReply = LR_NL;

       	            /* release lmLatch */
       	            e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
       	            if (e < eNOERROR) ERR(handle, e);

       	            return(eNOERROR);

       	   	case NOTOK  :
       	            e = SHM_releaseLatch(handle, &higherLockBucket->latch, procIndex);
       	            if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

#ifdef TRACE
       	            printf("violate lock hierarchy\n");
#endif
       	            /* return NOTOK result */
       	            *lockReply = LR_NOTOK;

      	            ERRL1(handle, eLOCKHIERARCHYVIOLATE_LM, &LM_LATCH);

       	   	default	    :	/* impossible case */

       	     	    ERRL2(handle, eWRONGLOCKMODE_LM, &higherLockBucket->latch, &LM_LATCH);

       	            /* case OK : keep the lock hierarchy protocol */
       	    }

       	    /* NO LockBucket LATCH EXISTS when find_lockBucket() is executed */
       	    /* deadlock avoidance between latches */
       	    e = SHM_releaseLatch(handle, &higherLockBucket->latch, procIndex);
       	    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);
	}
	else 
	    higherLockBucket = NULL;
    }

    /* find requested lockBucket */

    lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_LOCKID_HASH((*lockID), tempSeed, level)];
    e = lm_findLockBucketAndRequestNode(handle, xactID, lockID, level, lockHashEntryPtr,
                                        &lBucket, &aRequest);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    /* CASE 1 : First request and No wait case */
    if (!(lBucket) ||
        !PHYSICAL_PTR(lBucket->queue) ||
        (!aRequest && lBucket->nWaiting == 0 && lm_perProcessDSptr->LOCK_compatible[lBucket->groupMode][mode])) {

	*oldModeForReturn = L_NL; 

	/* for a INSTANT duration lock, check only that there is no conflicts */
	if (duration == L_INSTANT) {
	    if (lBucket) {
	        INSTANT_DURATION_HANDLING(handle, lBucket->latch, LM_LATCH, 
			                  xBucket->nLock[level], *lockReply);
	    }
	    else {
	        INSTANT_DURATION_HANDLING(handle, lockHashEntryPtr->latch, LM_LATCH, 
			                  xBucket->nLock[level], *lockReply);
	    }
	}

	if(!lBucket){
            /* Some codes are removed */
            /* allocate new lock bucket and initialize it */
            e = lm_allocAndInitLockBucket(handle, level, lockID,
                                          lockHashEntryPtr, &lBucket);
            if(e < eNOERROR) ERRL2(handle, e, &lockHashEntryPtr->latch, &LM_LATCH);

            /* to keep consistent status get the next latch before release prev. one */
            e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL2(handle, e, &lockHashEntryPtr->latch, &LM_LATCH);

            e = SHM_releaseLatch(handle, &lockHashEntryPtr->latch, procIndex);
            if (e < eNOERROR) ERRL2(handle, e, &lBucket->latch, &LM_LATCH);

            /* deadlock prevention between latches */
            /* LATCH sequence :: lowerLock -> higherLock */
            /* add new lower lockBucket into the hierarchy tree of higher one */
            if((level == L_PAGE || level == L_OBJECT) && higherRequest){ 
		ADD_LOCK_HIERARCHY(handle, higherLockBucket, lBucket);
                higherRequest->nLowLocks++;

                /* reset xBucket->maxLowLocks */
                if(higherRequest->nLowLocks > xBucket->maxLowLocks)
                    xBucket->maxLowLocks = higherRequest->nLowLocks;
            }

        }

	/* allocate and fill the new RequestNode  */
	e = lm_allocAndInsertIntoQueue(handle, xactID, xBucket, lBucket,
                                       mode, duration, L_GRANTED, &aRequest);
        if(e < eNOERROR) ERRL2(handle, e, &lBucket->latch, &LM_LATCH);

        if(level == L_PAGE || level == L_OBJECT)
            aRequest->higherRequestNode = LOGICAL_PTR(higherRequest);

	lBucket->groupMode = lm_perProcessDSptr->LOCK_supreme[lBucket->groupMode][mode];

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	/* update XactBucket */
        xBucket->status = X_NORMAL;
        xBucket->waitingLock = LOGICAL_PTR(NULL);

	/* add RequestNode into xbucket->grantedList */
	ADD_INTO_REQUESTNODE_DLIST2(xBucket->grantedList[level], aRequest);

	xBucket->nLock[level]++;

	/* return OK result */
	*lockReply = (LockReply)mode;

        *outLBucket = lBucket; 

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

	/*@ release latch */
	/* release lmLatch */
	e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

    /* Now, lBucket is valid. Set the OUT parameter. */
    *outLBucket = lBucket;


    /* CASE 2 : lock is not free */
    if(!aRequest){

        /* CASE 2-1 :  new request :: must be wait case */

        /* for conditional lock, if it is not granted now,
           without allocating Request Node it return NOTOK */

        if ( conditional == L_CONDITIONAL ) {
            CONDITIONAL_LOCK_HANDLING(handle, lBucket->latch, LM_LATCH, *lockReply);
        }

	*oldModeForReturn = L_NL;

        e = lm_allocAndInsertIntoQueue(handle, xactID, xBucket, lBucket, mode,
                                       duration, L_WAITING, &aRequest);
        if(e < eNOERROR) ERRL2(handle, e, &lBucket->latch, &LM_LATCH);

        if(level == L_PAGE || level == L_OBJECT)
            aRequest->higherRequestNode = LOGICAL_PTR(higherRequest);

        lBucket->nWaiting++;

        /* all latch must be released before waiting for semaphore */
        e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
        if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

        /* make the transaction state waiting for requsted lock */
        xBucket->status = X_WAITING;
        xBucket->waitingLock = LOGICAL_PTR(lBucket);

        /* release lmLatch */
        e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
        if (e < eNOERROR) ERR(handle, e);

       /* process waits until conflicting transactions release this lock */
        e = SHM_semWait(handle, &(((TCB*)PHYSICAL_PTR(aRequest->xcbPtr))->semID));
        if (e < eNOERROR) ERR(handle, e);


        /*@ get latch */
        /* get the lmLatch for deadlock detection */
        e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* if deadlock detection is executed THEN ... */
        if (xBucket->status == X_DEADLOCK) {

            /* return DEADLOCK result */
            *lockReply = LR_DEADLOCK;

#ifdef TRACE
            e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

            /* release lmLatch */
            e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
            if (e < eNOERROR) ERR(handle, e);

            return(eNOERROR);
        }

        if (duration == L_INSTANT) {

            e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

            /* drop RequestNode and update LockBucket info */
            DELETE_FROM_REQUESTNODE_DLIST(lBucket->queue, aRequest);

            /* release Request Node */
            FREE_REQUESTNODE(handle, aRequest);

            if (( e = wakeup_nextRequest(handle, lBucket)) < eNOERROR) {
                e2 = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
                if (e2 < 0) ERRL1(handle, e2, &LM_LATCH);

                /*@ release latch */
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
                if (level == L_FILE && PHYSICAL_PTR(lBucket->lowerLock)) {
                    /* if PageLock in this file exist, it violate the lock hierarchy */
                    e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
                    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

                    /* release lmLatch */
                    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
                    if (e < eNOERROR) ERR(handle, e);

                    ERR(handle, eLOCKHIERARCHYVIOLATE_LM);
                }


                e = lm_deleteLockBucketFromChain(handle, lockHashEntryPtr, lBucket);
                if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

            }

            xBucket->nLock[level]++;

            /* return OK result */
            *lockReply = (LockReply)mode;

#ifdef TRACE
            e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

            /* release lmLatch */
            e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
            if (e < eNOERROR) ERR(handle, e);

            return(eNOERROR);
        }

        /*@ update the status */
        /* update XactBucket */
        xBucket->status = X_NORMAL;
        xBucket->waitingLock = LOGICAL_PTR(NULL);

        /* add RequestNode into xbucket->grantedList */
        ADD_INTO_REQUESTNODE_DLIST2(xBucket->grantedList[level], aRequest);

        xBucket->nLock[level]++;


        /* return OK result */
        *lockReply = (LockReply)mode;

#ifdef TRACE
        e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

        /* release lmLatch */
        e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
        if (e < eNOERROR) ERR(handle, e);

        return(eNOERROR);
    }


    /* CASE 2-2 : lock re-request by this transaction (conversion case) */

    *oldModeForReturn = aRequest->mode; 

    /* CASE 2-2-1 : already enough !! - no stronger lock is needed */
    if ( lm_perProcessDSptr->LOCK_supreme[aRequest->mode][mode] == aRequest->mode) {

	/* if instant duration lock, no need to allocate Request Node */
	if ( duration == L_INSTANT )
	    INSTANT_DURATION_HANDLING(handle, lBucket->latch, LM_LATCH, 
		                      xBucket->nLock[level], *lockReply);

	/*@ update the status */
	/* update LockBucket and RequestNode */
	aRequest->counter++;
	if ( aRequest->duration < duration )
	    aRequest->duration = duration;

	/* return OK result */
	*lockReply = (LockReply)aRequest->mode;

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	/* update XactBucket */
	xBucket->status = X_NORMAL;
	xBucket->waitingLock = LOGICAL_PTR(NULL);

	xBucket->nLock[level]++;


#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

	/*@ release latch */
	/* release lmLatch */
	e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

    /* check the lockBucket status aginst existing request mode */
    chk_lockStatusWithoutARequest(handle, lBucket, aRequest, &groupMode, &convertFlag);

    /* CASE 2-2-2 : new mode is compatible with already_granted_group_mode */
    /*              or this is the only request for this page */

    if ( lm_perProcessDSptr->LOCK_compatible[groupMode][mode] )  { 

	/* do not check "lBucket->status != L_CONVERTING" condition !!
	   Because the converting transaction waits for this one ends.
	   if checking is done then deadlock situation occurs and
	   this transaction will be aborted. */

	/* if instant duration lock, no need to allocate Request Node */
	if ( duration == L_INSTANT )
	    INSTANT_DURATION_HANDLING(handle, lBucket->latch, LM_LATCH, 
		                      xBucket->nLock[level], *lockReply);

	/*@ update the status */
	/* update LockBucket */
	/* the new mode is the lm_perProcessDSptr->LOCK_supreme(groupMode, requested mode) */
	lBucket->groupMode = lm_perProcessDSptr->LOCK_supreme[lBucket->groupMode][mode];

	/* update RequestNode */
	/* new mode is old mode + new mode */
	aRequest->mode = lm_perProcessDSptr->LOCK_conversion[aRequest->mode][mode];
	aRequest->counter++;
	if ( aRequest->duration < duration )
	    aRequest->duration = duration;

	/* return OK result */
	*lockReply = (LockReply)aRequest->mode;

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	/* update XactBucket */
	xBucket->status = X_NORMAL;
	xBucket->waitingLock = LOGICAL_PTR(NULL);

	xBucket->nLock[level]++;


#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

	/* release lmLatch */
	e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);

    }

    /* CASE 2-2-3 : new mode is INCOMPATIBLE with already_granted_group_mode */

    /* if conditional lock, just return NOTOK */
    if ( conditional == L_CONDITIONAL )
        CONDITIONAL_LOCK_HANDLING(handle, lBucket->latch, LM_LATCH, *lockReply);

    /* now, unconditional lock case */

    /* CASE 2-2-3-1 : avoid deadlock situation caused by two conversion */

    if ( convertFlag ) { 

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	/* update XactBucket */
	xBucket->status = X_DEADLOCK;

	/* return DEADLOCK result */
	*lockReply = LR_DEADLOCK;

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

	/* release lmLatch */
	e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

    /* CASE 2-2-3-2 : waiting caused by conversion */

    /*@ update the status */

    /* new mode is old mode + new mode */
    aRequest->status = L_CONVERTING;
    if ( duration == L_INSTANT )
	oldMode = aRequest->mode;
    else
	aRequest->counter++;

    aRequest->oldMode = aRequest->mode;
    oldDuration = aRequest->duration;
    aRequest->mode = lm_perProcessDSptr->LOCK_conversion[aRequest->mode][mode];
    if ( aRequest->duration < duration )
	aRequest->duration = duration;

    /* return OK result */
    *lockReply = (LockReply)aRequest->mode;

    lBucket->nWaiting++;        /* Update lBucket info. */

    /* release all latch before sleep */
    e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    /* update XactBucket info */
    xBucket->status = X_CONVERTING;
    xBucket->waitingLock = LOGICAL_PTR(lBucket);


    /*@ release latch */
    /* release lmLatch */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /* process waits until conflicting transactions release this lock */
    e = SHM_semWait(handle, &(((TCB*)PHYSICAL_PTR(aRequest->xcbPtr))->semID));
    if (e < eNOERROR) ERR(handle, e);


    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* if deadlock detection is executed THEN ... */
    if (xBucket->status == X_DEADLOCK) {

	/* return DEADLOCK result */
	*lockReply = LR_DEADLOCK;

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

	e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL,NULL);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	/*@ update the status */
	aRequest->mode = aRequest->oldMode;
	aRequest->duration = oldDuration;

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	/* release lmLatch */
	e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

    if ( duration == L_INSTANT ) {

	e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL,NULL);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH); 

	aRequest->mode = oldMode;

	/* give chance to another request */
	if (( e = wakeup_nextRequest(handle, lBucket)) < eNOERROR) {
	    e2 = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	    if (e2 < 0) ERRL1(handle, e2, &LM_LATCH);

	    /* release lmLatch */
	    e2 = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	    if (e2 < 0) ERR(handle, e2);

	    ERR(handle, e);

	}
	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

	*lockReply = (LockReply)mode;
    }

    /*@ update the status */
    /* update XactBucket info */
    xBucket->status = X_NORMAL;
    xBucket->waitingLock = LOGICAL_PTR(NULL);
    xBucket->nLock[level]++;

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

    /* release lmLatch */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

    /* end of CASE 2-2 : (conversion case) */

}


/*@================================
 * lm_releaseLock( )
 *================================*/
/* ------------------------------------------------------------ */
/*								*/
/* lm_releaseLock :: 					        */
/*  	unlock                                                  */
/*								*/
/* paprameters 							*/
/*    xactID	IN transaction identifier 			*/
/*    lockID	IN lock identifier to be unlocked 		*/
/*    level     IN lock level                                   */
/*    duration	IN lock duration 				*/
/*								*/
/* return value							*/
/*		error number if error occurs			*/
/*								*/
/* ------------------------------------------------------------ */
Four lm_releaseLock(
    Four 		handle,
    XactID            	*xactID,
    TargetID          	*lockID,
    LockLevel         	level,
    LockDuration      	duration)
{
    LockHashEntry	*lockHashEntryPtr;

    XactBucket_Type	*xBucket;
    LockBucket_Type	*lBucket;

    RequestNode_Type	*aRequest, *tmpRequest;
    Four		e, e2;			/* function return value */
    Boolean		deletedFlag;
    Seed  		tempSeed;  		/* temp seed */


    /*@ get latch */
    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

    /*@ find the requested ones */
    /* if no xactbucket of this xactID, invalid transaction identifier error */
    e = find_xactBucket(handle, xactID, &xBucket);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    /* search the RequestNode from xBucket->grantedList[level] */
    switch(level){
      case L_FILE:
        SEARCH_REQUESTNODEOFFILE(xBucket->grantedList[level], &lockID->fileID, aRequest);
        /*@ error check */
        /* if no RequestNode exists, ERROR */
        if (!aRequest) {
	        /* release lmLatch */
		e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
		if (e < eNOERROR) ERR(handle, e);

		return(eNOERROR);
	}
        break;
      case L_PAGE:
      case L_FLAT_PAGE:
        SEARCH_REQUESTNODEOFPAGE(xBucket->grantedList[level], &lockID->pageID, aRequest);
        /*@ error check */
        /* if no RequestNode exists, ERROR */
        if (!aRequest) {
	        /* release lmLatch */
		e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
		if (e < eNOERROR) ERR(handle, e);

		return(eNOERROR);
	}
        break;
      case L_OBJECT:
      case L_FLAT_OBJECT:
        SEARCH_REQUESTNODEOFOBJECT(xBucket->grantedList[level], &lockID->objectID, aRequest);
        /*@ error check */
        /* if no RequestNode exists, ERROR */
        if (!aRequest) {
	        /* release lmLatch */
		e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
		if (e < eNOERROR) ERR(handle, e);

		return(eNOERROR);
	}
        break;
      case L_KEYVALUE:
        SEARCH_REQUESTNODEOFKEYVALUE(xBucket->grantedList[level], &lockID->keyValue, aRequest);
        /*@ error check */
        /* if no RequestNode exists, ERROR */
        if (!aRequest) {
	        /* release lmLatch */
		e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
		if (e < eNOERROR) ERR(handle, e);

		return(eNOERROR);
	}
        break;
    }

    lBucket = PHYSICAL_PTR(aRequest->lockHDR);

    /* request latch for access the lockBucket */
    e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    /* check the hierarchical lock protocol */
    if (level == L_FILE && PHYSICAL_PTR(lBucket->lowerLock)) {

	if (check_lowerLevelLock(handle, PHYSICAL_PTR(lBucket->lowerLock), xBucket)) {
	    /* if any requests for lower level exist
	       then error violating lock hierarchy */

	    ERRL2(handle, eLOCKHIERARCHYVIOLATE_LM, &lBucket->latch, &LM_LATCH);
	}
    }

    /* if one more requests exist,
       return only after decrease counter(i.e. releaselock is NULL operation) */

    if ( aRequest->counter > 1 ) {

    	aRequest->counter--;

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

	/* release lmLatch */
	e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

    /* check if the requested duration is stronger than granted */
    if ( aRequest->duration < duration ) {

        ERRL2(handle, eWRONGDURATION_LM, &lBucket->latch, &LM_LATCH);
    }

    /* if aRequest->counter was 1(i.e. if it is the last instance of lock) */

    DELETE_FROM_REQUESTNODE_DLIST(lBucket->queue, aRequest);

    if (( e = wakeup_nextRequest(handle, lBucket)) < eNOERROR) {
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

	if (level == L_FILE && PHYSICAL_PTR(lBucket->lowerLock)) {
            /* if PageLock in this file exist, it violate the lock hierarchy */
	    ERRL2(handle, eLOCKHIERARCHYVIOLATE_LM, &lBucket->latch, &LM_LATCH);
	}

	lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_LOCKID_HASH((*lockID), tempSeed, level)];

	e = lm_deleteLockBucketFromChain(handle, lockHashEntryPtr, lBucket);
	if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);
    }

    /*@ update the status */
    /* update xactBucket */
    /* delete RequestNode from grantedList of xactBucket */
    DELETE_FROM_REQUESTNODE_DLIST2(xBucket->grantedList[level], aRequest);

    /* decrease the value of 'nLowLock' in the higherRequest */
    if(level == L_PAGE || level == L_OBJECT){
        if(PHYSICAL_PTR(aRequest->higherRequestNode))
            ((RequestNode_Type*)PHYSICAL_PTR(aRequest->higherRequestNode))->nLowLocks--; 

        /* reset maxLowLocks */
        xBucket->maxLowLocks = 0;

        tmpRequest = PHYSICAL_PTR(xBucket->grantedList[L_FILE]);

        while(tmpRequest){ 
            if(tmpRequest->nLowLocks > xBucket->maxLowLocks)
                xBucket->maxLowLocks = tmpRequest->nLowLocks;
	    tmpRequest = PHYSICAL_PTR(tmpRequest->nextGrantedEntry);
	} 

    }


    /* release Request Node */
    FREE_REQUESTNODE(handle, aRequest);

    xBucket->nUnlock[level]++;

#ifdef TRACE
    e = Util_checkPool(handle, &LM_LOCKBUCKETPOOL);
#endif

    /* release lmLatch */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

}



/*@================================
 * wakeup_nextRequest( )
 *================================*/
/* wakeup_nextRequest( ) :
   wakeup the next transaction to get the lock from the LockBucket queue
   First, get the current group lock mode
   Second, check whether ( loop )
   the the oldest waiting request's mode is compatible
   if compatible, send the message */
Four wakeup_nextRequest(
    Four 		handle,
    LockBucket_Type 	*lBucket) 	/* INOUT */
{

    RequestNode_Type	*nextRequest;
    LockMode		nextMode;
    Boolean             doItAgain;
    Four  		e;

    e = find_nextModeAndRequest(handle, lBucket, &nextMode, &nextRequest);
    if (e < eNOERROR) ERR(handle, e);

    if ( !nextRequest ) {	/* no waiting request */

	lBucket->groupMode = nextMode;
	return (eNOERROR);
    }
    else if ( nextRequest->status != L_WAITING
	      && nextRequest->status != L_CONVERTING )
	ERR(handle, eWRONGLOCKSTATUS_LM);


    do {

	doItAgain = FALSE;

	if ( lm_perProcessDSptr->LOCK_compatible[nextMode][nextRequest->mode] ) {

	    nextRequest->status = L_GRANTED;

	    lBucket->groupMode = lm_perProcessDSptr->LOCK_supreme[nextMode][nextRequest->mode];
	    lBucket->nWaiting--;

            ((XactBucket_Type*)PHYSICAL_PTR(nextRequest->xactBucketPtr))->status = X_NORMAL;
            ((XactBucket_Type*)PHYSICAL_PTR(nextRequest->xactBucketPtr))->waitingLock = LOGICAL_PTR(NULL);

	    e = SHM_semSignal(handle, &(((TCB*)PHYSICAL_PTR(nextRequest->xcbPtr))->semID));
	    if ( e < eNOERROR ) ERR(handle, e);

	    doItAgain = TRUE;

	    nextMode = lBucket->groupMode;
	    while ( nextRequest && nextRequest->status != L_WAITING )
		nextRequest = PHYSICAL_PTR(nextRequest->next);
	}

    } while ( doItAgain && nextRequest );

    return(eNOERROR);
}



/*@================================
 * check_lowerLevelLock( )
 *================================*/
/* check_lowerLevelLock is called under lBucket->latch is granted */
/* mutual exclusion is guranteed for read operation */
Four check_lowerLevelLock(
    Four 		handle,
    LockBucket_Type 	*lBucket,
    XactBucket_Type 	*xBucket
)
{

    RequestNode_Type	*aRequest;

    while (lBucket) {

	/* search the request by xactID transaction */
	SEARCH_REQUESTNODEOFPAGE(xBucket->grantedList[L_PAGE], &lBucket->target.pageID, aRequest);

	if (aRequest) return(TRUE);


	lBucket = PHYSICAL_PTR(lBucket->nextSetLock);
    }

    return(FALSE);

}



/*@================================
 * disconnect_hierarchy( )
 *================================*/
/* disconnect_hierarchy () ::
   disconnect the given LockBucket from its lock hierarchy chain */
Four disconnect_hierarchy(
    Four 		handle,
    LockBucket_Type 	*lBucket
)
{
    Four 		e;
    LockBucket_Type	*higherLockBucket;

    /* find the higher level lock of this lock */
    higherLockBucket = PHYSICAL_PTR(lBucket->higherLock);
    if(!higherLockBucket) return(eNOERROR);

#ifdef TRACE
    if (!higherLockBucket)
	printf("in disconnect_hierarchy :: higherLockBucket is NULL\n");
#endif

    /* for mutual exclusion, get latch for higherLockBucker (headnode) */
    e = SHM_getLatch(handle, &higherLockBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    if ( PHYSICAL_PTR(higherLockBucket->lowerLock) == lBucket )
	    higherLockBucket->lowerLock = lBucket->nextSetLock;

    if (PHYSICAL_PTR(lBucket->prevSetLock))
	((LockBucket_Type*)PHYSICAL_PTR(lBucket->prevSetLock))->nextSetLock = lBucket->nextSetLock;
    if (PHYSICAL_PTR(lBucket->nextSetLock))
	((LockBucket_Type*)PHYSICAL_PTR(lBucket->nextSetLock))->prevSetLock = lBucket->prevSetLock;


    e = SHM_releaseLatch(handle, &higherLockBucket->latch, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}



/*@================================
 * find_nextModeAndRequest( )
 *================================*/
/*
** find next groupMode and next Request to be granted
** if there is converting request then it is the next request
** otherwise the first waiting request is the next request
*/
Four find_nextModeAndRequest(
    Four 		handle,
    LockBucket_Type 	*lBucket, 	/* IN pointer to queue */
    LockMode 		*nextMode,      /* OUT next groupMode */
    RequestNode_Type 	**nextRequest 	/* OUT next request */
)
{
    RequestNode_Type	*convertingRequest;

    (*nextRequest) = PHYSICAL_PTR(lBucket->queue);
    *nextMode = L_NL;
    convertingRequest = NULL;

    while ( (*nextRequest) && (*nextRequest)->status != L_WAITING ) {

	if ( (*nextRequest)->status == L_GRANTED )
	    *nextMode = lm_perProcessDSptr->LOCK_supreme[*nextMode][(*nextRequest)->mode];

	else if ( (*nextRequest)->status == L_CONVERTING )
	    convertingRequest = (*nextRequest);

	(*nextRequest) = PHYSICAL_PTR((*nextRequest)->next);
    }

    if ( convertingRequest )
	(*nextRequest) = convertingRequest;

    return(eNOERROR);
}



/*@================================
 * chk_lockStatusWithoutARequest( )
 *================================*/
/* is_compatible function is replaced with chk_lockStatusWithoutARequest */
/*
** check the lockBucket status aginst new request mode
** First, get the groupMode which do not include the lock mode of the current request node
** Second, check whether covtering requested mode exist.
*/
void chk_lockStatusWithoutARequest(
    Four            	handle,
    LockBucket_Type 	*lBucket,		/* IN pointer to queue */
    RequestNode_Type 	*currentRequest, 	/* IN current request node */
    LockMode 		*groupMode,	      	/* OUT granted mode */
    Boolean 		*convertFlag)	      	/* OUT whether convertiong mode was set */
{
    RequestNode_Type	*aRequest;


    *convertFlag = FALSE; 

    aRequest = PHYSICAL_PTR(lBucket->queue);
    (*groupMode) = L_NL;

    while ( aRequest && aRequest->status != L_WAITING ) {

	if ( aRequest->status == L_CONVERTING )
	    *convertFlag = TRUE;

	if ( aRequest != currentRequest && aRequest->status == L_GRANTED )
	    (*groupMode) = lm_perProcessDSptr->LOCK_supreme[(*groupMode)][aRequest->mode];

	aRequest = PHYSICAL_PTR(aRequest->next);
    }

}



/*@================================
 * lm_findLockBucketAndRequestNode( )
 *================================*/
/* lm_findLockBucketAndRequestNode ::

   	if it is already in the locktable  return the found lockBucket
	else without releasing lockHashTable entry latch return NULL

        if the given transaction already have request node, return the pointer
        else return NULL
*/
Four lm_findLockBucketAndRequestNode(
    Four		handle,
    XactID              *xactID,        /* IN transaction identifier */
    TargetID 		*lockID, 	/* IN search target */
    LockLevel           level,          /* IN level of the lockID */
    LockHashEntry 	*hashEntryPtr,	/* IN start point */
    LockBucket_Type 	**aBucket,	/* OUT result bucket */
    RequestNode_Type    **aRequest)     /* OUT result request node */
{

    Four 		e;

    e = SHM_getLatch(handle, &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    (*aBucket) = PHYSICAL_PTR(hashEntryPtr->bucketPtr);

    while(*aBucket) {
	if ( (*aBucket)->level == level &&
	    EQUAL_TARGETID(((*aBucket)->target), lockID, level) ) {

	    e = SHM_getLatch(handle, &(*aBucket)->latch, procIndex, M_EXCLUSIVE,
                             M_UNCONDITIONAL, NULL);
	    if (e < eNOERROR) ERRL1(handle, e, &hashEntryPtr->latch);

	    e = SHM_releaseLatch(handle, &hashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

            /* find request node */
            (*aRequest) = PHYSICAL_PTR((*aBucket)->queue);
            while((*aRequest)){
                if (EQUAL_XACTID((*aRequest)->xactID, *xactID)) return(eNOERROR);
                else (*aRequest) = PHYSICAL_PTR((*aRequest)->next);
            }

            /* Now, *aRequest is NULL */
            return(eNOERROR);
	}
	*aBucket = PHYSICAL_PTR((*aBucket)->next);
    }


    /* return NULL pointer as OUTPUT */
    return(eNOERROR);
}



/*@================================
 * lm_deleteLockBucketFromChain( )
 *================================*/
Four lm_deleteLockBucketFromChain(
    Four		handle,
    LockHashEntry 	*lockHashEntryPtr,	/* IN start point */
    LockBucket_Type 	*lBucket)		/* INOUT result bucket */
{
    Four 		e;
    Boolean 		found;
    Boolean		deletedFlag;

    LockLevel 		level;			/* lock level */
    TargetID  		tid;			/* target Identifier */

    e = SHM_getLatch(handle, &lockHashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_CONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    if ( e == SHM_BUSYLATCH ) {

	level = lBucket->level;
	tid = lBucket->target;

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	/* re-search and then if exist, delete lockBucket */
	e = SHM_getLatch(handle, &lockHashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	if (e < eNOERROR) ERR(handle, e);

	lBucket = PHYSICAL_PTR(lockHashEntryPtr->bucketPtr);

	found = FALSE;
	while(!found && lBucket) {

	    if (lBucket->level == level)
		switch (level) {
		  case L_FILE : if (EQUAL_FILEID( (lBucket->target.fileID), tid.fileID))
		      found = TRUE; break;
		  case L_PAGE:
		  case L_FLAT_PAGE:
		      if (EQUAL_PAGEID( (lBucket->target.pageID), tid.pageID)) found = TRUE;
		      break;
		  case L_OBJECT:
		  case L_FLAT_OBJECT:
		      if (EQUAL_OBJECTID( (lBucket->target.objectID), tid.objectID)) found = TRUE;
		      break;
		  case L_KEYVALUE:
		      if(EQUAL_KEYVALUELOCKID((lBucket->target.keyValue), tid.keyValue)) found = TRUE;
		      break;
		  }

	    if (!found)
		lBucket = PHYSICAL_PTR(lBucket->next);
	}

	if ( found ) {

	    e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	    if (e < eNOERROR) ERRL1(handle, e, &lockHashEntryPtr->latch);

	    if ( !PHYSICAL_PTR(lBucket->queue)) {
		DELETE_FROM_LOCKBUCKET_DLIST(lockHashEntryPtr->bucketPtr, lBucket);
		deletedFlag = TRUE;
	    }
	    else
		deletedFlag = FALSE;

	    e = SHM_releaseLatch(handle, &lockHashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    if (deletedFlag) {

		if (level == L_PAGE ) {
		    e = disconnect_hierarchy(handle, lBucket);
		    if (e < eNOERROR) ERR(handle, e);
		}

		e = Util_freeElementToPool(handle, &LM_LOCKBUCKETPOOL, lBucket);
		if (e < eNOERROR) ERR(handle, e);
	    }
	}
	else {
	    /* old lBucket is  already deleted and freed */
	    e = SHM_releaseLatch(handle, &lockHashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);
	}

    }
    else {

	DELETE_FROM_LOCKBUCKET_DLIST(lockHashEntryPtr->bucketPtr, lBucket);

	e = SHM_releaseLatch(handle, &lockHashEntryPtr->latch, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	e = SHM_releaseLatch(handle, &lBucket->latch, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	e = Util_freeElementToPool(handle, &LM_LOCKBUCKETPOOL, lBucket);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

}

