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
 * Module: LM_action.c
 *
 * Description:
 *     Begin/End action
 *
 * Exports: LM_beginAction()
 *          LM_endAction()
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
 * LM_beginAction( )
 *================================*/
/* ------------------------------------------------------------ */
/*								*/
/* LM_beginAction :: 					        */
/*  	set startAction pointer to point the first request node */
/*								*/
/* paprameters 							*/
/*    XactID                                                    */
/*								*/
/* return value							*/
/*		error number if error occurs			*/
/*								*/
/* ------------------------------------------------------------ */
Four LM_beginAction( 
    Four 		handle,
    XactID 		*xactID,
    Four 		type
)
{
    Four 		e;
    Four 		i;
    XactBucket_Type 	*xBucket;

    if(LM_TEST_ACTION_FLAG_ON(handle)) ERR(handle, eWRONGACTIONSTART_LM); 

    /*@ get latch */
    /* get the lmLatch for deadlock detection */
    e = SHM_getLatch(handle, &LM_LATCH, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* if no xactbucket of this xactID, invalid transaction identifier error */
    e = find_xactBucket(handle, xactID, &xBucket);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    if(xBucket->ccLevel == X_RR_RR) ERRL1(handle, eNONEEDACTION_LM, &LM_LATCH); 

    for(i = 0; i < N_LEVEL; i++)
        xBucket->startAction[i] = xBucket->grantedList[i];

    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if(e < eNOERROR) ERR(handle, e);

    /* enable 'LM_actionFlag' */
    if(type == USER_ACTION){
    	LM_ENABLE_ACTION_FLAG(handle);
	LM_DISABLE_AUTO_ACTION_FLAG(handle);

    }else if(type == AUTO_ACTION)
		LM_ENABLE_AUTO_ACTION_FLAG(handle);

    return(eNOERROR);

}


/*@================================
 * LM_endAction( )
 *================================*/
/* ------------------------------------------------------------ */
/*								*/
/* LM_endAction :: 					        */
/*  	release all L_S locks                                   */
/*								*/
/* paprameters 							*/
/*    XactID                                                    */
/*								*/
/* return value							*/
/*		error number if error occurs			*/
/*								*/
/* ------------------------------------------------------------ */
Four LM_endAction(
    Four 		handle,
    XactID 		*xactID,
    Four 		type)
{
    Four             	e, e2;
    Four             	i;
    LockLevel        	startLevel, stopLevel;
    XactBucket_Type  	*xBucket;
    RequestNode_Type 	*aRequest;
    LockBucket_Type  	*lBucket;
    LockHashEntry    	*lockHashEntryPtr;
    Seed           	tempSeed;    		/* temp seed */

    if(!LM_TEST_ACTION_FLAG_ON(handle) && !LM_TEST_AUTO_ACTION_FLAG_ON(handle)) ERR(handle, eWRONGACTIONEND_LM);

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
    /* release the page lockBucket from xBucket->grantedList */
    switch(xBucket->ccLevel){
      case X_RR_CS:
        startLevel = L_OBJECT;
        stopLevel = L_FLAT_PAGE;
        break;
      case X_CS_CS:
        startLevel = L_OBJECT;
        stopLevel = L_FILE;
        break;
      case X_CS_BROWSE:
        startLevel = L_FILE;
        stopLevel = L_FILE;
        break;
      default: /* no action */
        startLevel = L_FILE;
        stopLevel = L_OBJECT;
        break;
    }

    while(startLevel <= stopLevel){
        aRequest = PHYSICAL_PTR(xBucket->startAction[startLevel]);

        while ( aRequest && aRequest->mode == L_S) {

            lBucket = PHYSICAL_PTR(aRequest->lockHDR);

            /* request latch for access the lockBucket */
            e = SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE,
                             M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

            DELETE_FROM_REQUESTNODE_DLIST(lBucket->queue, aRequest);
            DELETE_FROM_REQUESTNODE_DLIST2(xBucket->grantedList[startLevel], aRequest);

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
                lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_LOCKID_HASH(lBucket->target, tempSeed, startLevel)];

                e = lm_deleteLockBucketFromChain(handle, lockHashEntryPtr, lBucket);
                if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

            }
            aRequest = PHYSICAL_PTR(xBucket->grantedList[startLevel]);

        }

        startLevel++;
    }

    /* disable 'LM_actionFlag */
    if(type == USER_ACTION)
    	LM_DISABLE_ACTION_FLAG(handle);
    else if(type = AUTO_ACTION)
		LM_DISABLE_AUTO_ACTION_FLAG(handle);

    e = SHM_releaseLatch(handle, &LM_LATCH, procIndex);
    if(e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

}




