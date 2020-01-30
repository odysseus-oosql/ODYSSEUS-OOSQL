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
 * Module: LM_initXact.c
 *
 * Description:
 *   Allocate xactBucket for given xactID in Transaction Hash Table.
 *
 * Exports: Four LM_initXactBucket(Four, xactID*)
 *
 */


#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "latch.h"
#include "SHM.h"
#include "Util.h"
#include "TM.h"
#include "LM.h"
#include "LM_macro.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * LM_initXact()
 *================================*/
/* ------------------------------------------------------------ */
/*								*/
/* LM_initXact ::	 					*/
/*  	request lock of the file for this transaction 		*/
/*								*/
/* paprameters 							*/
/*    xactID	IN transaction identifier 			*/
/*								*/
/* return value							*/
/*    result messages 						*/
/*								*/
/* ------------------------------------------------------------ */
Four LM_initXactBucket (
    Four		handle,
    XactID 		*xactID,	      	/* IN transaction identifier */
    ConcurrencyLevel 	ccLevel) 		/* IN concurrency level of this transaction */ 
{
    XactBucket_Type	*xBucket;
    XactHashEntry	*xactHashEntryPtr;
    Four		e;			/* function return value */
    Four		i;			/* loop index */

    /* find xactBucket in lock table */

    xactHashEntryPtr = &LM_XACTHASHTABLE[XACTTABLE_HASH(xactID)];

    e = SHM_getLatch(handle, &xactHashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    xBucket = PHYSICAL_PTR(xactHashEntryPtr->bucketPtr);

    while(xBucket){
	if ( EQUAL_XACTID(*xactID, xBucket->xactID) ) {
	    e = SHM_releaseLatch(handle, &xactHashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    return(eDUPXACTID_LM);	/* FATAL ERROR */
	}
	xBucket = PHYSICAL_PTR(xBucket->next);
    }

    /* if no xactbucket of this xactID is exist, insert new one */
    if (!xBucket) {

	/* allocate and initialize the content */
	GET_NEWXACTBUCKET(handle, xBucket, *xactID, ccLevel);

	/* add new LockBucket as first entry of doubly linked list */
	ADD_INTO_XACTBUCKET_DLIST(xactHashEntryPtr->bucketPtr, xBucket);

	/* getLatch call was in find_xactBucket */
	e = SHM_releaseLatch(handle, &xactHashEntryPtr->latch, procIndex);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);
}



/*@================================
 * LM_dropXactBucket( )
 *================================*/
Four LM_dropXactBucket (
    Four		handle,
    XactID 		*xactID)		/* IN transaction identifier */
{
    XactBucket_Type	*xBucket;
    XactHashEntry	*xactHashEntryPtr;
    Four		e;			/* function return value */
    Four		i;			/* loop index */

    /* find xactBucket in lock table */

    xactHashEntryPtr = &LM_XACTHASHTABLE[XACTTABLE_HASH(xactID)];

    e = SHM_getLatch(handle, &xactHashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    xBucket = PHYSICAL_PTR(xactHashEntryPtr->bucketPtr);

    while(xBucket){
	if ( EQUAL_XACTID(*xactID, xBucket->xactID) ) {

	    DELETE_FROM_XACTBUCKET_DLIST(xactHashEntryPtr->bucketPtr, xBucket);

	    e = SHM_releaseLatch(handle, &xactHashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    FREE_XACTBUCKET(handle, xBucket);

	    return(eNOERROR);
	}
	xBucket = PHYSICAL_PTR(xBucket->next);
    }

    return(eBADXACTID_LM);
}



/*@================================
 * find_xactBucket( )
 *================================*/
/* find_xactBucket ::

   	if it is already in the locktable  return the found xactBucket
	else without releasing xactHashTable entry latch return NULL
*/
Four find_xactBucket(
    Four 		handle,
    XactID 		*xactID,		/* IN search target */
    XactBucket_Type	**aBucket)   		/* OUT result bucket */
{

    Four 		e;			/* error returned */
    XactHashEntry 	*xactHashEntryPtr; 	/* starting point */

    xactHashEntryPtr = &LM_XACTHASHTABLE[XACTTABLE_HASH(xactID)];

    e = SHM_getLatch(handle, &xactHashEntryPtr->latch, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    (*aBucket) = PHYSICAL_PTR(xactHashEntryPtr->bucketPtr);

    while(*aBucket){
	if ( EQUAL_XACTID(*xactID, (*aBucket)->xactID) ) {

	    e = SHM_releaseLatch(handle, &xactHashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);
	    return(eNOERROR);
	}
	(*aBucket) = PHYSICAL_PTR((*aBucket)->next);
    }

    e = SHM_releaseLatch(handle, &xactHashEntryPtr->latch, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eBADXACTID_LM);
}

