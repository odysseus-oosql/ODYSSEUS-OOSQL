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
 * Module: LM_dumpLock.c
 *
 * Description:
 *   lock request/release operation
 *
 * Exports: LM_dumpFileLock(fileID)
 *          LM_dumpObjectLock(objectID)
 *          LM_dumpObjectLock(objectID)
 *          LM_dumpXactLock(xactID)
 *
*/


#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "latch.h"
#include "Util.h"
#include "LM.h"
#include "LM_macro.h"
#include "LM_LockMatrix.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



#define PRINT_LOCKBUCKET(lb) \
switch ( (lb)->level ) {\
  case L_PAGE :  PRINT_PAGEID("pid", &(lb)->target.pageID);\
    printf(" :: L_PAGE\n"); break;\
  case L_FILE   :  PRINT_FILEID("fileID", &(lb)->target.fileID);\
    printf(" :: L_FILE\n"); break;\
  case L_FLAT_PAGE   :  PRINT_PAGEID("flat_page_id", &(lb)->target.pageID);\
    printf(" :: L_FLAT_PAGE\n");\
  case L_FLAT_OBJECT   :  PRINT_OBJECTID("flat_obj_id", &(lb)->target.objectID);\
    printf(" :: L_FLAT_OBJECT\n");\
}\
printf("+--------------------------------------------------------------------+\n");\
printf("| groupMode | nWaiting | status | lower | higher | prevSet | nextSet |\n");\
printf("+--------------------------------------------------------------------+\n");\
	 printf("|%6ld      |%5ld     |%5ld   | %5P | %5P | %5P | %5P |\n",\
		(lb)->groupMode, (lb)->nWaiting, (lb)->status, (lb)->lowerLock,\
		(lb)->higherLock, (lb)->prevSetLock, (lb)->nextSetLock);\
printf("+--------------------------------------------------------------------+\n");



#define PRINT_REQUESTNODE(req) \
printf("+---------------------------------------------+\n");\
printf("| counter | mode | duration | status | xactID |\n");\
printf("+---------------------------------------------+\n");\
printf("|%6ld   |%4ld  |%6ld    |%5ld   | %6ld |\n",\
       (req)->counter, (req)->mode, (req)->duration, (req)->status, (req)->xactID.low);\
printf("+---------------------------------------------+\n");



#define PRINT_XACT(xb)\
printf("\n+--------------------------------------+\n");\
printf("| status | waitingLock | waitingfor    |\n");\
printf("+--------------------------------------+\n");\
printf("|%5ld   | %7P    | %9P  |\n", (xb)->status, (xb)->waitingLock, (xb)->waitingfor);\
printf("+--------------------------------------+\n");



/*@================================
 * LM_dumpFileLock()
 *================================*/
Four LM_dumpFileLock(FileID *fileID)
{
    LockBucket_Type	*lBucket;

    XactHashEntry	*xactHashEntryPtr;
    LockHashEntry	*lockHashEntryPtr;

    RequestNode_Type	*aRequest;
    Four		i;		/* index of for loop */
    Four		e;		/* function return value */
    Seed       		tempSeed;   	/* temp seed */

    /* find requested lockBucket */

    lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_FILEID_HASH(fileID, tempSeed)];
    ERROR_PASS(handle, find_fileLockBucket(fileID, lockHashEntryPtr, &lBucket));


    if (!lBucket)  {
	PRINT_FILEID("no lock for this fileID", fileID);
	ERROR_PASS(handle, SHM_releaseLatch(handle, &lockHashEntryPtr->latch, procIndex));
    }
    else {

	if (!lBucket->queue) {
	    PRINT_FILEID("no lock for this fileID", fileID);
	    ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));
	}

	PRINT_LOCKBUCKET(lBucket);
	aRequest = lBucket->queue;

	printf("\n RequestNodes are .....\n");
	while (aRequest) {
	    PRINT_REQUESTNODE(aRequest);

	    aRequest= aRequest->next;
	}

	ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));
    }

    return(eNOERROR);

}



/*@================================
 * LM_dumpPageLock()
 *================================*/
Four LM_dumpPageLock(PageID *pid)
{
    LockBucket_Type	*lBucket, *higherLockBucket;

    LockHashEntry	*lockHashEntryPtr;

    RequestNode_Type	*aRequest;
    Four		e;		/* function return value */
    Seed        	tempSeed;  	/* temp seed */

    /* find requested lockBucket */

    lockHashEntryPtr = &LM_LOCKHASHTABLE[LOCKTABLE_PAGEID_HASH(pid,tempSeed)];
    ERROR_PASS(handle, find_pageLockBucket(pid, lockHashEntryPtr, &lBucket));

    if (!lBucket)  {
	PRINT_PAGEID("no lock for this pid", pid);
	ERROR_PASS(handle, SHM_releaseLatch(handle, &lockHashEntryPtr->latch, procIndex));
    }
    else {

	if (!lBucket->queue) {
	    PRINT_PAGEID("no lock for this pid", pid);
	    ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));
	}

	PRINT_LOCKBUCKET(lBucket);
	aRequest = lBucket->queue;

	printf("\n RequestNodes are .....\n");
	while (aRequest) {
	    PRINT_REQUESTNODE(aRequest);

	    aRequest= aRequest->next;
	}

	ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));
    }

    return(eNOERROR);
}



/*@================================
 * LM_dumpXactLock()
 *================================*/
Four LM_dumpXactLock(XactID *xactID)
{
    XactHashEntry	*xactHashEntryPtr;
    LockHashEntry	*lockHashEntryPtr;

    XactBucket_Type	*xBucket;
    LockBucket_Type	*lBucket;

    RequestNode_Type	*aRequest, *nextRequest;
    Four		e;		/* function return value */
    Boolean		deletedFlag;


    /* if no xactbucket of this xactID, invalid transaction identifier error */
    e = find_xactBucket(handle, xactID, &xBucket);
    if (e < eNOERROR) ERRL1(handle, e, &LM_LATCH);

    printf("\nGranted Lock of Transaction ");PRINT_XACTID(*xactID);
    PRINT_XACT(xBucket);

    /* release the page lockBucket from xBucket->grantedList[L_PAGE] */
    aRequest = xBucket->grantedList[L_PAGE];
    while ( aRequest ) {

	lBucket = aRequest->lockHDR;

	/* request latch for access the lockBucket */
	ERROR_PASS(handle, SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
	PRINT_LOCKBUCKET(lBucket);
	PRINT_REQUESTNODE(aRequest);

	ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));


	aRequest = aRequest->nextGrantedEntry;

    }


    /* release the page lockBucket from xBucket->grantedList[L_FILE] */
    aRequest = xBucket->grantedList[L_FILE];
    while ( aRequest ) {

	lBucket = aRequest->lockHDR;

	/* request latch for access the lockBucket */
	ERROR_PASS(handle, SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
	PRINT_LOCKBUCKET(lBucket);
	PRINT_REQUESTNODE(aRequest);

	ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));

	aRequest = aRequest->nextGrantedEntry;

    }

    /* release the page lockBucket from xBucket->grantedList[L_FLAT_OBJECT] */
    aRequest = xBucket->grantedList[L_FLAT_OBJECT];
    while ( aRequest ) {

	lBucket = aRequest->lockHDR;

	/* request latch for access the lockBucket */
	ERROR_PASS(handle, SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
	PRINT_LOCKBUCKET(lBucket);
	PRINT_REQUESTNODE(aRequest);

	ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));


	aRequest = aRequest->nextGrantedEntry;

    /* release the page lockBucket from xBucket->grantedList[L_FLAT_PAGE] */
    aRequest = xBucket->grantedList[L_FLAT_PAGE];
    while ( aRequest ) {

	lBucket = aRequest->lockHDR;

	/* request latch for access the lockBucket */
	ERROR_PASS(handle, SHM_getLatch(handle, &lBucket->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
	PRINT_LOCKBUCKET(lBucket);
	PRINT_REQUESTNODE(aRequest);

	ERROR_PASS(handle, SHM_releaseLatch(handle, &lBucket->latch, procIndex));


	aRequest = aRequest->nextGrantedEntry;

    }


    return(eNOERROR);
}


