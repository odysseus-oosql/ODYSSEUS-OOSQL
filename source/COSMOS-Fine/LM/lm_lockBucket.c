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
 * Module: lm_lockBucket.c
 *
 * Description:
 *   allocate/deallocate/initialize lockBucket
 *
 * Exports: lm_allocAndInitLockBucket(handle, xactID, fileID, mode, duration, conditional)
 *          lm_deallocLockBucket(xactID, fileID, duration)
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

/*----------------------------------------------------------------- */
/*                                                                  */
/* lm_allocAndInitLockBucket ::                                     */
/*         allocate and initialize lockBucket.                      */
/*                                                                  */
/* Assumption ::                                                    */
/*         this function is called with LM_LATCH                    */
/*                                                                  */
/* parameters                                                       */
/*    LockLevel       IN lock level                                 */
/*    TargetID        IN target lock identifier                     */
/*    LockHashEntry   IN allocated lBucket will be added into       */
/*    LockBucket_Type OUT allocated lBucket                         */
/*                                                                  */
/* return value                                                     */
/*    result messages                                               */
/*                                                                  */
/*----------------------------------------------------------------- */

Four lm_allocAndInitLockBucket(
    Four		handle,
    LockLevel       	level,
    TargetID        	*lockID,
    LockHashEntry   	*lockHashEntryPtr,
    LockBucket_Type 	**lBucket)
{
    Four  		e;
    Four  		howMuchLBucket;

    /* check parameters */
    if(lockID == NULL) ERR(handle, eBADPARAMETER);
    if(lockHashEntryPtr == NULL) ERR(handle, eBADPARAMETER);

    /* allocate lBucket */
    e = Util_getElementFromPool(handle, &LM_LOCKBUCKETPOOL, lBucket);
    if (e < eNOERROR) ERR(handle, e);

    /* initialize lBucket */
    SHM_initLatch(handle, &(*lBucket)->latch);	  /* initLatch precedes getlatch */

    /*@ update the status */
    (*lBucket)->level = level;
    if(level == L_FILE)   (*lBucket)->target.fileID = lockID->fileID;
    else if(level == L_PAGE) (*lBucket)->target.pageID = lockID->pageID;
    else if(level == L_OBJECT ) (*lBucket)->target.objectID = lockID->objectID;
    else if(level == L_KEYVALUE) (*lBucket)->target.keyValue = lockID->keyValue;
    else if(level == L_FLAT_OBJECT) (*lBucket)->target.objectID = lockID->objectID;
    else if(level == L_FLAT_PAGE) (*lBucket)->target.pageID = lockID->pageID;
    else ERR(handle, eBADPARAMETER);

    (*lBucket)->nWaiting    = 0;
    (*lBucket)->status      = L_GRANTED; 
    (*lBucket)->queue       = LOGICAL_PTR(NULL);
    (*lBucket)->groupMode   = L_NL;
    (*lBucket)->lowerLock   = (*lBucket)->higherLock = LOGICAL_PTR(NULL);
    (*lBucket)->prevSetLock = (*lBucket)->nextSetLock = LOGICAL_PTR(NULL);

    /* add new LockBucket as first entry of doubly linked list */
    ADD_INTO_LOCKBUCKET_DLIST(lockHashEntryPtr->bucketPtr, *lBucket);

    return(e);
}





