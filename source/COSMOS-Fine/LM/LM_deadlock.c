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
 * Module: LM_deadlock.c
 *
 * Description:
 * 	deadlock detection and abort the victim
 *
 * Exports:
 *	LM_detectDeadlock()
 *
*/

#include <stdio.h>
#include "common.h"
#include "error.h"
#include "latch.h"
#include "Util.h"
#include "LM.h"
#include "LM_macro.h"
#include "LM_LockMatrix.h"
#include "SHM.h"
#include "trace.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*@ function prototype */
Four lm_detectCycle(Four, XactBucket_Type*, lm_DeadlockStackEntry*, Four*, DeadlockStatus*, Four*);
Four lm_costEvaluation(Four, XactBucket_Type *, Four *);


/*@================================
 * LM_detectDeadlock( )
 *================================*/
/* ------------------------------------------------------------ */
/*                                                              */
/* LM_detectDeadlock ::                                         */
/*      detect deadlock and abort the victim			*/
/*                                                              */
/* paprameters                                                  */
/*    NONE                                                      */
/*                                                              */
/* return value                                                 */
/*    error code 	                                        */
/*                                                              */
/* ------------------------------------------------------------ */
Four LM_detectDeadlock(
    Four 			handle
)
{
    Four 			e;				/* error code */
    Four 			i;				/* index for loop */
    XactBucket_Type 		*temp;				/* temporary storage of the transaction bucket */
    DeadlockStatus 		deadlockStatus;			/* deadlock status */
    lm_DeadlockStackEntry 	lmDeadlockStack[MAXDEADLOCKCHAIN]; /* path of the deadlock detection */
    Four 			deadlockStack_Top;		/* top of lmDeadlockStack */
    Four 			victim;				/* index of the victim in lmDeadlockStack */

    /* Initialize the deadlock stack top index */
    deadlockStack_Top = INITDEADLOCKSTACKTOP;

    /*@ get LM_LATCH */
    /* get the lockLatch to prevent for another transaction
       to touch the lock table structure */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* initialize for deadlock detection */
    for (i = 0; i < MAXXACTHASHENTRY; i++){
	if(lm_shmPtr  == NULL) ERR(handle, eFATALERROR_LM);
	for (temp = PHYSICAL_PTR(LM_XACTHASHTABLE[i].bucketPtr); temp != NULL; temp = PHYSICAL_PTR(temp->next)){ 
	    temp->waitingfor = LOGICAL_PTR(NULL); 
	    temp->deadlockVisit = 0;
	}
    }

    /* start deadlock detection */
    for(i=0; i < MAXXACTHASHENTRY; i++)
	for(temp = PHYSICAL_PTR(LM_XACTHASHTABLE[i].bucketPtr); temp != NULL; temp = PHYSICAL_PTR(temp->next)){ 
    	    e = lm_detectCycle(handle, temp, lmDeadlockStack,
                               &deadlockStack_Top, &deadlockStatus, &victim);
	    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);
	}

    /*@ release LM_LATCH */
    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}



/*@================================
 * lm_detectCycle()
 *================================*/
/* ------------------------------------------------------------ */
/*                                                              */
/* lm_detectCycle ::                                            */
/*      detect one cycle and select a victim 			*/
/*                                                              */
/* paprameters                                                  */
/*    XactBucket_Type        IN transaction bucket to 		*/
/*				start deadlock detection 	*/
/*    lm_DeadlockStackEntry  INOUT deadlock stack		*/
/*    Four      	     INOUT top of deadlock stack	*/
/*    DeadlockStatus  	     OUT deadlock ? 			*/
/*    Four  		     OUT index of deadlock stack	*/
/*                                                              */
/* return value                                                 */
/*    error code 						*/
/*                                                              */
/* ------------------------------------------------------------ */

Four lm_detectCycle(
    Four			handle,
    XactBucket_Type 		*me,			/* IN transaction bucket to start deadlock detection */
    lm_DeadlockStackEntry 	*deadlockStack,		/* INOUT deadlock stack */
    Four 			*deadlockStack_Top,	/* INOUT top of deadlock stack */
    DeadlockStatus 		*deadlockStatus,	/* OUT deadlock ? */
    Four 			*victimIndex)		/* OUT index of deadlock stack */
{
    Four 			e;			/* error code */
    Four 			i;			/* index for loop */
    XactBucket_Type 		*him;			/* transaction bucket to be tested */
    RequestNode_Type 		*them;			/* request node to be tested */
    RequestNode_Type 		*myRequestNode;		/* my request node */
    Four 			minCost;		/* minimal cost for aborting victim */
    Four 			victimCandidateIndex;	/* index of the candidate of victim */
    XactBucket_Type 		*victim;		/* transaction bucket which is selected victim */
    RequestNode_Type 		*victimWait;		/* request node of the victim */
    LockMode 			grantedMode;		/* lock mode of a granted(or converting) request node */
    cosmos_thread_sem_t 	*semID;			/* semaphore No of the victim */
    LockBucket_Type 		*lBucket; 


    /*@ check status */
    /* check that this transaction bucket has been tested */
    if(me->deadlockVisit)  {
	*deadlockStatus = DLS_OK;
	return(eNOERROR);
    }

    /* if this transaction does not wait any lock then it is not deadlock */
    if(PHYSICAL_PTR(me->waitingLock) == NULL){
	*deadlockStatus = DLS_OK;
	me->deadlockVisit = 1;
	return(eNOERROR);
    }

    /* find my request node */
    for (myRequestNode = PHYSICAL_PTR( ((LockBucket_Type*)PHYSICAL_PTR(me->waitingLock))->queue);
	 myRequestNode != NULL && !EQUAL_XACTID(myRequestNode->xactID, me->xactID);
	 myRequestNode = PHYSICAL_PTR(myRequestNode->next)); 

    /* push this transaction into the deadlock stack */
    deadlockStack[*deadlockStack_Top].xactBucket = me;
    e = lm_costEvaluation(handle, me, &(deadlockStack[*deadlockStack_Top].cost));
    if (e < eNOERROR) ERR(handle, e);

    (*deadlockStack_Top)++;

    /*@ search deadlock */
    /* 'them' is not NULL, because there is a RequestNode (me) at least. */
    /* Consider L_CONVERTING node. */

    for (them = PHYSICAL_PTR( ((LockBucket_Type*)PHYSICAL_PTR(me->waitingLock))->queue);
 	 them != NULL; 
	 them = PHYSICAL_PTR(them->next)) {

	if(myRequestNode->status == L_CONVERTING && them == myRequestNode) continue;
	if(myRequestNode->status == L_CONVERTING && them->status == L_WAITING) break;

	if(them == myRequestNode && myRequestNode->status == L_WAITING &&
	   ((LockBucket_Type*)PHYSICAL_PTR(myRequestNode->lockHDR))->status != L_CONVERTING){
	    if(lm_perProcessDSptr->LOCK_compatible[myRequestNode->mode][((LockBucket_Type*)PHYSICAL_PTR(myRequestNode->lockHDR))->groupMode]){
		e = wakeup_nextRequest(handle, (LockBucket_Type*)PHYSICAL_PTR(myRequestNode->lockHDR));
		if(e < eNOERROR) ERR(handle, e);
	    }
	    break;
	}

	if(lm_perProcessDSptr->LOCK_compatible[myRequestNode->mode][them->mode] && 
	   them->status != L_WAITING && them->status != L_CONVERTING) continue;
	else if((them->status == L_WAITING || them->status == L_CONVERTING) &&  myRequestNode->mode == them->mode) continue;

        if(PHYSICAL_PTR(me->waitingLock) == NULL) break;

	him = PHYSICAL_PTR(them->xactBucketPtr);
	me->waitingfor = LOGICAL_PTR(him);

	if(PHYSICAL_PTR(him->waitingfor) != NULL) {

    	    /* deadlock !!
     	     * It is needed to select a victim to solve the deadlock
     	     */
    	    minCost = deadlockStack[*deadlockStack_Top-1].cost;
    	    victimCandidateIndex = *deadlockStack_Top-1;

    	    /* we should include 'him'. */
    	    for (i = *deadlockStack_Top-2; deadlockStack[i+1].xactBucket != him; i--) {
		if (minCost > deadlockStack[i].cost) {
	    	    minCost = deadlockStack[i].cost;
	    	    victimCandidateIndex = i;
		}
    	    }
#ifdef TRACE
	    for (i = *deadlockStack_Top-1; deadlockStack[i+1].xactBucket != him; i--) {
		printf("Transaction %ld: cost %ld\n", deadlockStack[i].xactBucket->xactID.low, deadlockStack[i].cost);
	    }
#endif /* TRACE */
    	    /* Now, we decide the victim */
    	    victim = deadlockStack[victimCandidateIndex].xactBucket;

            /* check variables */
            if (victimCandidateIndex >= MAXDEADLOCKCHAIN) ERR(handle, eINTERNAL);
            if (victim == NULL) ERR(handle, eINTERNAL);
            if (PHYSICAL_PTR(victim->waitingLock) == NULL) ERR(handle, eINTERNAL);

            /* find the request node */
            victimWait = PHYSICAL_PTR( ((LockBucket_Type*)PHYSICAL_PTR(victim->waitingLock))->queue);
            while (1) {

                if (victimWait == NULL) break;
                if (PHYSICAL_PTR(victimWait->xactBucketPtr) == victim) break;

                victimWait = PHYSICAL_PTR(victimWait->next);
            }

#ifdef DEBUG
   	    if(victimWait == NULL){
		/* error */
		/* This error may not happen */
		printf("Victim is not waiting..\n");
	    }
#endif /* DEBUG */

	    /* It is needed to abort the victim */
#ifdef TRACE
	    printf("\nvictim is %ld\n", victim->xactID.low);
#endif /* TRACE */

 	    /* Save the Semaphore ID of the victim. */
   	    semID = &(((TCB*)PHYSICAL_PTR(victimWait->xcbPtr))->semID);

            /* change lock bucket information */
	    ((LockBucket_Type*)PHYSICAL_PTR(victimWait->lockHDR))->nWaiting--; 

	    /* Disconnect/Update the request node. */
	    if (victimWait->status == L_WAITING) {
		/* disconnect that request node from the corresponding queue */
		DELETE_FROM_REQUESTNODE_DLIST(((LockBucket_Type*)PHYSICAL_PTR(victimWait->lockHDR))->queue, victimWait);

                /* release Request Node */
		FREE_REQUESTNODE(handle, victimWait);

		lBucket = (LockBucket_Type*)PHYSICAL_PTR(victimWait->lockHDR);
            	if (( e = wakeup_nextRequest(handle, lBucket)) < eNOERROR)
			ERR(handle, e);

	    } else { /* L_CONVERTING */

		/* revert to the original granted node. */
		victimWait->mode = victimWait->oldMode;
		victimWait->status = L_GRANTED;
	    }

	    /* Set the victim-selected transaction's 'waitingLock' to NULL */
	    victim->waitingLock = LOGICAL_PTR(NULL);

	    /* Set the victim-selected transaction's status to X_DEADLOCK. */
	    victim->status = X_DEADLOCK;

	    /* Wake up the victim. */
	    e = SHM_semSignal(handle, semID);
	    if (e < eNOERROR) ERR(handle, e);


	    victim->deadlockVisit = 1;

	    *victimIndex = victimCandidateIndex;
	    *deadlockStatus = DLS_DEADLOCK;

	    /* pop the entry from the deadlockStack */
	    (*deadlockStack_Top)--;
	    me->waitingfor = LOGICAL_PTR(NULL);

	    return(eNOERROR);

	}else {
	    /* recursively call */
	    e = lm_detectCycle(handle, him, deadlockStack,
                               deadlockStack_Top,  deadlockStatus, victimIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    /* if deadlock, pop the stack entry until the victim is popped */
	    if ((*deadlockStatus == DLS_DEADLOCK) && (*victimIndex <= *deadlockStack_Top - 1)){
		(*deadlockStack_Top)--;
		me->waitingfor = LOGICAL_PTR(NULL);
		return(eNOERROR);
	    }
	}

	me->waitingfor = LOGICAL_PTR(NULL);

    }

    me->deadlockVisit = 1;
    *deadlockStatus = DLS_OK;

    /* pop the entry from deadlock Stack */
    (*deadlockStack_Top)--; 


    return(eNOERROR);
}



/*@================================
 * lm_costEvaluation()
 *================================*/
/* ------------------------------------------------------------ */
/*                                                              */
/* lm_costEvaluation ::                                         */
/*      caculate the cost for aborting given transaction	*/
/*                                                              */
/* paprameters                                                  */
/*    XactBucket_Type   IN candidate transaction to be aborted  */
/*    Four              OUT cost for aborting given transaction */
/*                                                              */
/* return value                                                 */
/*    error code                                                */
/*                                                              */
/* ------------------------------------------------------------ */

Four lm_costEvaluation(
    Four		handle,
    XactBucket_Type 	*target,	/* IN candidate transaction to be aborted */
    Four 		*cost)		/* OUT cost for aborting given transaction */
{
    /* cost = number of granted lock - number of released lock */
    if (target->status == X_DEADLOCK)
	*cost = (CONSTANT_ALL_BITS_SET(Four) >> 1); /* a maximum number: 0x7FFFFFFF or 0x7FFFFFFFFFFFFFFF */
    else
	*cost = NUMOFGETLOCK(target);

    return(eNOERROR);
}
