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
 * Module: SHM_latch.c
 *
 * Description:
 *  support Mutual Exclusion for a critical section
 *
 * Exports:
 *	SHM_initLatch()
 *	SHM_getLatch()
 *	SHM_releaseLatch()
 *	SHM_releaseMyLatches()
 */

#ifndef SINGLE

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#ifndef LINUX
#include <sys/time_impl.h>
#endif
#include <pthread.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Function Prototypes */
void enter_latch_queue(LATCH_TYPE *, TCB *);
Four shm_getInfoFromSemID(cosmos_thread_semName_t* ,GlobalHandle*);


#if (defined(USETESTANDSET))	/* if use testandset function(assembly code) */

#define MUTEX_BEGIN()    while(testandset(&latchPtr->sync, 1) != 0)
#define MUTEX_END()      testandset_release(&latchPtr->sync)

#elif (defined(USEMUTEX)) 	/* if use mutex function */

/*
 * we use the API name directly to reduce function call overhead
 */
#define MUTEX_BEGIN()    if ( (e = pthread_mutex_lock(&latchPtr->mutex)) != 0) ERR(handle, eMUTEXLOCKUNKNOWN_THM) 
#define MUTEX_END()      if ( (e = pthread_mutex_unlock(&latchPtr->mutex)) != 0) ERR(handle, eMUTEXUNLOCKUNKNOWN_THM) 

#else				/* if use s/w mutex */

#define MUTEX_BEGIN()\
    do {\
	latchPtr->flags[procIndex] = WAITING;\
\
	while ( latchPtr->turn != procIndex) \
	    if (latchPtr->flags[latchPtr->turn] == IDLE)\
		latchPtr->turn = procIndex;\
\
	latchPtr->flags[procIndex] = INCS;\
\
	i = 0; \
	while( (i < MAXPROCS) && ( (i == procIndex) || (latchPtr->flags[i] != INCS))) {\
	    i = i + 1;\
	}\
    }\
    while (i < MAXPROCS)

#define MUTEX_END()     latchPtr->flags[procIndex] = IDLE

#endif



/*@================================
 * SHM_initLatch( )
 *================================*/
Four SHM_initLatch(
    Four 		handle,
    LATCH_TYPE 		*latchPtr
)
{
    register Four 	i;

#if (defined(USETESTANDSET))    /* if use testandset function(assembly code) */

    testandset_init(&latchPtr->sync);

#elif (defined(USEMUTEX))       /* if use mutex function */

    if (pthread_mutex_init(&latchPtr->mutex, &cosmos_thread_mutex_attr_default) != 0)
	return (eMUTEXINITFAILED_THM);

#else                      

    for ( i = 0; i < MAXPROCS; i++) {
	latchPtr->flags[i] = IDLE;
    }
    latchPtr->turn = 0;
    latchPtr->procIndex = -1;

#endif

    latchPtr->mode = M_FREE;
    latchPtr->queue = LOGICAL_PTR(NULL); 


    return(eNOERROR);
}



/*@================================
 * SHM_getLatch( )
 *================================*/
Four SHM_getLatch(
    Four 		handle,
    LATCH_TYPE 		*latchPtr,		/* target latch pointer */
    Four 		procIndex,		/* process index */
    Four 		reqMode,		/* M_FREE, M_SHARED, M_EXCLUSIVE */
    Four 		reqCondition,		/* M_CONDITIONAL, M_UNCONDITIONAL, M_INSTANT */
    LATCH_TYPE 		*releasedLatchPtr) 	/* to be release latch pointer */
{

    Boolean		waitingFlag;
    LatchEntry  	*list;
    TCB	  		*out_latch_queue();
    Four 		nGranted;		/* temporal variable */
    Four 		ii,i;			/* loop index */
    Four 		current;		/* temporary index */
    Four 		e = eNOERROR;		/* error handling */


    if (procIndex < 0 || handle < 0) {
	return(eNOERROR);
    }


    MUTEX_BEGIN();/* ------------------------------------------------------- */


#if 0 
    TR_PRINT(handle, TR_SHM, TR3, ("access sequnce %ld\n", shmPtr->sequence++));
#endif

    /* if other processes exists, enter latch queue */
    waitingFlag = TRUE;

    if ( reqCondition & M_INSTANT )  {

	switch ( latchPtr->mode ) {
	    case M_FREE : waitingFlag = FALSE;  break;
	    case M_EXCLUSIVE :
		if (latchPtr->grantedGlobalHandle.procIndex == procIndex && latchPtr->grantedGlobalHandle.threadIndex == handle)
		    waitingFlag = FALSE; /* already acquired latch */
		    break;
	    case M_SHARED :
		if ( reqMode == M_SHARED )
		    waitingFlag = FALSE;
		else if ( reqMode == M_EXCLUSIVE && latchPtr != releasedLatchPtr ) {
		    /* check previous granted latch if exist then return error */
		    list = MY_GRANTEDLATCHLIST(handle);
		    for ( i = MY_NUMGRANTED(handle)-1; i >= 0; i-- )
			if ( list[i].counter > 0 && list[i].latchPtr == latchPtr ) {
			    e = eBADLATCHCONVERSION_SHM;
			    goto EXIT_MUTEX;
			}	/* if */

		}		/* else if  */
	}			/* switch */
    }				/* if M_INSTANT */

    else {
	switch ( latchPtr->mode ) {
	    case M_FREE :
	    	waitingFlag = FALSE;

	    	latchPtr->mode = reqMode;
	    	latchPtr->latchCounter = 1;
	    	latchPtr->grantedGlobalHandle.procIndex = procIndex;
	    	latchPtr->grantedGlobalHandle.threadIndex = handle;
	    	break;

	    case M_EXCLUSIVE :
            	/* already acquired latch */
	    	if (latchPtr->grantedGlobalHandle.procIndex == procIndex && latchPtr->grantedGlobalHandle.threadIndex == handle) {
		    waitingFlag = FALSE;
		    latchPtr->latchCounter++;
	    	}
    		break;
	    case M_SHARED :
		if ( reqMode == M_SHARED ) {
		    waitingFlag = FALSE;
		    latchPtr->latchCounter++;
	        }
		/* do not allow conversion from M_SHARED to M_EXCLUSIVE */
	  	else if ( reqMode == M_EXCLUSIVE && latchPtr != releasedLatchPtr ) {

		    /* check previous granted latch if exist then return error */
		    list = MY_GRANTEDLATCHLIST(handle);
		    for ( i = MY_NUMGRANTED(handle)-1; i >= 0; i-- )
		    	if ( list[i].counter > 0 && list[i].latchPtr == latchPtr ) {
			    e = eBADLATCHCONVERSION_SHM;
			    goto EXIT_MUTEX;
		    	}
	   	}
	}			/* end of switch  */

	if (!waitingFlag || waitingFlag && reqCondition & M_UNCONDITIONAL) {
	    /* if granted then recording this grant */

	    /* check previous granted latch */
	    list = MY_GRANTEDLATCHLIST(handle);
	    for ( i = MY_NUMGRANTED(handle)-1; i >= 0; i-- )
		if ( list[i].latchPtr == latchPtr ) {
		    /* found case :: just increase counter */
		    list[i].counter++;
		    break;
	    }

	    /* not found case : allocate new entry and record this grant */
	    if ( i  < 0 ) {
		nGranted = MY_NUMGRANTED(handle)++;
		if ( nGranted == MY_GRANTEDLATCHSTRUCT(handle)->nEntries ) {

		    e = Util_doublesizeVarArray(handle, MY_GRANTEDLATCHSTRUCT(handle), sizeof(LatchEntry));
		    if (e < 0) goto EXIT_MUTEX;
		}
		MY_GRANTEDLATCHENTRY(handle, nGranted).counter = 1;
		MY_GRANTEDLATCHENTRY(handle, nGranted).latchPtr = latchPtr;
	    }
	}

    }				/* else  */

    /* if waiting is needed then put TCB into latch->queue */
    if (waitingFlag) {
	if (reqCondition & M_UNCONDITIONAL) {

	    TR_PRINT(handle, TR_SHM, TR1, ("\tpid %ld enter latch queue %ld -", procIndex, TM_PROCESSTABLE(handle)->semID));

	    TM_PROCESSTABLE(handle)->mode = reqMode;
    	    if ( reqCondition & M_INSTANT )
		TM_PROCESSTABLE(handle)->condition = M_INSTANT;
	    else
		TM_PROCESSTABLE(handle)->condition = M_UNCONDITIONAL;
	    enter_latch_queue(latchPtr, TM_PROCESSTABLE(handle));

	}
    }

EXIT_MUTEX :


    MUTEX_END();/* --------------------------------------------------------- */


    if ( e < eNOERROR ) ERR(handle, e);

    /*@ release latch */
    /* for concurrency and for deadlock avoidance */
    if (releasedLatchPtr) {
	e = SHM_releaseLatch(handle, releasedLatchPtr, procIndex);
	if ( e < eNOERROR ) ERR(handle, e);
    }

    if (waitingFlag) {
	if ( reqCondition & M_CONDITIONAL) {
    	    TR_PRINT(handle, TR_SHM, TR1, ("\tpid %ld :: fails a getCondLatch", procIndex));
	    return(SHM_BUSYLATCH);
	}
	else {
	    TR_PRINT(handle, TR_SHM, TR1, ("\tpid %ld :: waitsem %ld\n", procIndex, TM_PROCESSTABLE(handle)->semID));
 
	    e = SHM_semWait(handle, (&(TM_PROCESSTABLE(handle)->semID))); /* semID => semNo */
            if ( e < eNOERROR ) ERR(handle, e);
	}
    }

    return(eNOERROR);
}



/*@================================
 * SHM_releaseLatch( )
 *================================*/
Four SHM_releaseLatch(
    Four 		handle,
    LATCH_TYPE 		*latchPtr,
    Four 		procIndex)
{

    TCB	  		*tcb, *nextTCB = NULL;	/* temporary entry */
    TCB	  		*out_latch_queue();
    Four  		ii, i, current, e = eNOERROR;
    LatchEntry 		*list;


    if (procIndex < 0 || handle < 0) return(eNOERROR);


    MUTEX_BEGIN();/* ------------------------------------------------------- */


#if 0 
    TR_PRINT(handle, TR_SHM, TR3, ("access sequnce %ld\n", shmPtr->sequence++));
#endif

    /* check if this process had this latch */

    /* First, examine the latchPtr */
    if ( latchPtr->latchCounter <= 0) {
        e = eBADPARAMETER;
        PRTERR(handle, e);
    }
	
    switch ( latchPtr->mode ) {
    	case M_FREE:
		 e = eBADPARAMETER;

      	case M_EXCLUSIVE :
	    if ( latchPtr->grantedGlobalHandle.procIndex != procIndex || latchPtr->grantedGlobalHandle.threadIndex != handle) {
		e = eBADPARAMETER;
            }
    }

    if ( e < eNOERROR) {
	PRTERR(handle, e);
	goto EXIT_MUTEX;
    }

    /* Second, examine grantedLatchList */
    list = MY_GRANTEDLATCHLIST(handle);

    for ( current = MY_NUMGRANTED(handle)-1; current >= 0; current-- )
	if ( list[current].latchPtr == latchPtr ) {
	   list[current].counter--;

	   /* decrease MY_NUMGRANTED(handle) if top entry is released */
	   if ( current == MY_NUMGRANTED(handle)-1 )
		for ( i = current; list[i].counter == 0 && i >= 0; i-- ) {
		    MY_NUMGRANTED(handle)--;
	   }
	   break;
	}

    if ( current < 0 ) {
	e = eBADPARAMETER;
	PRTERR(handle, e); goto EXIT_MUTEX;
    }

   latchPtr->latchCounter--;

   if ( latchPtr->latchCounter == 0 ) {
	if ( PHYSICAL_PTR(latchPtr->queue) ) { 

    	    /* extract all granted transaction from queue */
    	    nextTCB = out_latch_queue(latchPtr);

    	    /* skip the leading instant duration request */
    	    for ( tcb = nextTCB; tcb; tcb = PHYSICAL_PTR(tcb->next) ) 
		if ( tcb->condition != M_INSTANT )
	    	    break;

    	    if (!tcb)
		latchPtr->mode = M_FREE;
    	    else {
		latchPtr->mode = tcb->mode;

	        switch ( tcb->mode ) {
	  	    case M_EXCLUSIVE :
	    		latchPtr->latchCounter++;
			shm_getInfoFromSemID(tcb->semName, &(latchPtr->grantedGlobalHandle));
	    		break;
	  	    case M_SHARED :
	    		/* if request mode of the first queued entry is M_SHARED
                           then grant the next M_SHARED requests */
	    		for (; tcb; tcb = PHYSICAL_PTR(tcb->next) ) 
			    /* skip the instant duration request among shared ones */
			    if ( tcb->condition != M_INSTANT )
		    		latchPtr->latchCounter++;

		}		/* switch  */

    	    } /* else */

    	    TR_PRINT(handle, TR_SHM, TR1, ("\tpid %ld :: next owner semID %ld, mode %ld - ",
					procIndex, nextTCB->semID, nextTCB->mode)); /* nextTCB->semID => nextTCB->semID */
	} /* if */
	else
    	    latchPtr->mode = M_FREE;
    } /* if (latchPtr->queue)  */


    EXIT_MUTEX:


    if ( e < eNOERROR ) ERR(handle, e);

    while ( nextTCB ) {
	TR_PRINT(handle, TR_SHM, TR1, ("\tpid %ld :: send signal to semID  %ld\n", procIndex, nextTCB->semID)); /* nextTCB->semID => nextTCB->semID */

	tcb = PHYSICAL_PTR(nextTCB->next); /* save the next link before sending the signal */ 

    	e = SHM_semSignal(handle, &(nextTCB->semID)); /* nextTCB->semID => nextTCB->semID */
    	if ( e < eNOERROR ) ERR(handle, e);

    	nextTCB = tcb;
    }

    MUTEX_END(); /* ------------------------------------------------------- */


    return(eNOERROR);
}



/*@================================
 * enter_latch_queue()
 *================================*/
/* append proc_ptr into latchPtr->queue */
void enter_latch_queue(LATCH_TYPE *latchPtr, TCB *proc_ptr)
{
    TCB		*temp;

    proc_ptr->next = LOGICAL_PTR(NULL); 

    if ( PHYSICAL_PTR(latchPtr->queue) == NULL ) 
	latchPtr->queue = LOGICAL_PTR(proc_ptr); 
    else {

	/* find the last entry of the queue */
	temp = PHYSICAL_PTR(latchPtr->queue); 
	while ( PHYSICAL_PTR(temp->next) != NULL ) temp = PHYSICAL_PTR(temp->next); 
        temp->next = LOGICAL_PTR(proc_ptr); 
    }
}



/*@================================
 * out_latch_queue()
 *================================*/
/* out_latch_queue :: return concurrent TCB list */
TCB * out_latch_queue(LATCH_TYPE *latchPtr)
{
    TCB		*headPtr;		/* pointer to the first entry */
    TCB		*tailPtr;		/* pointer to the last entry to be granted */


    if ( PHYSICAL_PTR(latchPtr->queue) == NULL ) return (NULL); 

    headPtr = PHYSICAL_PTR(latchPtr->queue); /* identify the first entry */ 
    tailPtr = headPtr;		/* initialize last entry */

    /* First, check instant duration request */
    if ( headPtr->condition == M_INSTANT ) {
	tailPtr = PHYSICAL_PTR(headPtr->next); 
	while ( tailPtr && tailPtr->condition == M_INSTANT )
	    tailPtr = PHYSICAL_PTR(tailPtr->next); 
    }

    /* Second, check SHARED mode request */
    if ( tailPtr && tailPtr->mode == M_SHARED )
	while ( PHYSICAL_PTR(tailPtr->next) && ((TCB*)PHYSICAL_PTR(tailPtr->next))->mode == M_SHARED ) 
	    tailPtr = PHYSICAL_PTR(tailPtr->next); 

    /* Third, disconnect granted requests from queue */
    if ( tailPtr ) {
	latchPtr->queue = tailPtr->next;
	tailPtr->next = LOGICAL_PTR(NULL); 
    }
    else
	latchPtr->queue = LOGICAL_PTR(NULL); 

    return(headPtr);

}



/*@================================
 * SHM_releaseMyLatches( )
 *================================*/
Four SHM_releaseMyLatches(
    Four 	handle,
    Four 	procIndex)
{

    LatchEntry  *list;
    Four 	current;		/* temporary index */
    Four 	e;			/* returned error number */

    list = MY_GRANTEDLATCHLIST(handle);
    for ( current = MY_NUMGRANTED(handle)-1; current >= 0; current-- )
	while ( list[current].counter > 0) {
	    e = SHM_releaseLatch(handle, list[current].latchPtr, procIndex);
	    if (e < eNOERROR) ERR(handle, e);
	}

    return(eNOERROR);


}

Four shm_getInfoFromSemID(cosmos_thread_semName_t *semName, GlobalHandle *globalHandle)
{

    char    temp[256];
    char    *pIndex, *tIndex;

    strcpy(temp, semName);

    pIndex = temp+1;
    tIndex = strstr(temp, "_");
    tIndex++;

    globalHandle->procIndex   = (Four)(atol(pIndex));
    globalHandle->threadIndex = (Four)(atol(tIndex));

    return (eNOERROR);
}

#endif

