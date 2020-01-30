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
 * Module: BtM_GetTreeLatchPtrFromIndexId.c
 *
 * Description : Dynamically allocate and deallocate treeLatch for IndexID.
 *
 * Exports:
 *   BtM_GetTreeLatchPtrFromIndexId(handle, indexID*, LATCH_TYPE*)
 *   BtM_ReleaseTreeLatchPtr(handle, indexID*)
 *
 * Side effects:
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "SHM.h"
#include "Util.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

#define BTM_HASH(k) (((k)->volNo + (k)->serial) % BTM_TREELATCH_HASHSIZE)
#define EQUALKEY_BTM(k1, k2) (((k1)->volNo == (k2)->volNo) && ((k1)->serial == (k2)->serial))


/*
 * Module: BtM_GetTreeLatchPtrFromIndexId.c
 *
 * Description : Dynamically allocate and deallocate treeLatch for IndexID.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_BTM
 *    eNOSUCHTREELATCH_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 */
Four BtM_GetTreeLatchPtrFromIndexId(
    Four handle,
    IndexID   *lookupKey,	/* IN index identifier */
    LATCH_TYPE **treeLatchPtr)	/* OUT pointer to the tree latch */
{
    btmTreeLatchCell *aCellPtr;	/* temporary variable */
    Four newSlot;               /* new slot in the cache for lookupKey */
    Four e;			/* error number */
    Four i;                     /* temporary index */

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,
	     ("BtM_GetTreeLatchFromIndexId(lookupKey=%P, treeLatchPtr=%P)",
	      lookupKey, treeLatchPtr));


    /* Check the parameters. */
    if (lookupKey == NULL ) ERR(handle, eBADPARAMETER);

    /* First, search in the local cache for the tree latch */
    newSlot = -1;
    for (i = 0; i < btm_perThreadDSptr->btmCache4TreeLatch.nEntries; i++) {
        if (IS_NILINDEXID(BTM_CACHE4TREELATCH(handle)[i].iid) && newSlot == -1)
            newSlot = i;         /* to allocate new entry */
        else if (EQUAL_INDEXID(BTM_CACHE4TREELATCH(handle)[i].iid, *lookupKey)) {

            *treeLatchPtr = &(BTM_CACHE4TREELATCH(handle)[i].tLatchCellPtr->latch);
            return(eNOERROR);
        }
    }

    /* Second, check the space for new entry */
    if (newSlot == -1)   {    /* no space for caching */

        newSlot  = btm_perThreadDSptr->btmCache4TreeLatch.nEntries;

        e = Util_doublesizeVarArray(handle, &(btm_perThreadDSptr->btmCache4TreeLatch), sizeof(btmCache4TreeLatchCell));
        if (e < eNOERROR) ERR(handle, e);

        /* Initialize the newly allocated entries */
        for (i = newSlot; i < btm_perThreadDSptr->btmCache4TreeLatch.nEntries; i++) {
            SET_NILINDEXID(BTM_CACHE4TREELATCH(handle)[i].iid);
            BTM_CACHE4TREELATCH(handle)[i].tLatchCellPtr = NULL;
        }
    }

    /* Third, search the shared memory */
    /* Mutex Begin : for accessing BTM_TREELATCHTABLE */
    e = SHM_getLatch(handle, &BTM_LATCH4TREELATCHTABLE, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* find corrsponding hash entry */
    aCellPtr = PHYSICAL_PTR(BTM_TREELATCHPTRTABLE[BTM_HASH(lookupKey)]); 

    /* search corresponding buffer table entry in hash chain */
    while ( aCellPtr && !EQUALKEY_BTM(&aCellPtr->iid, lookupKey) ) 
	    aCellPtr = PHYSICAL_PTR(aCellPtr->next); 

    if ( aCellPtr) {

	aCellPtr->counter++;	/* increase fix counter */
    }
    else  {
        /* allocate and initialize treeLatchCell */
	e = Util_getElementFromPool(handle, &BTM_TREELATCHPOOL, &aCellPtr);
	if ( e < eNOERROR )
	    ERRL1(handle, e, &BTM_LATCH4TREELATCHTABLE);

	aCellPtr->counter = 1;
	aCellPtr->iid = *lookupKey;
	aCellPtr->next = BTM_TREELATCHPTRTABLE[BTM_HASH(lookupKey)];
	BTM_TREELATCHPTRTABLE[BTM_HASH(lookupKey)] = LOGICAL_PTR(aCellPtr); 

	SHM_initLatch(handle, &aCellPtr->latch);
    }

    /* Mutex End : for accessing BTM_TREELATCHTABLE */
    e = SHM_releaseLatch(handle, &BTM_LATCH4TREELATCHTABLE, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    /* Fourth, fill the local cache entry */
    BTM_CACHE4TREELATCH(handle)[newSlot].iid = *lookupKey;
    BTM_CACHE4TREELATCH(handle)[newSlot].tLatchCellPtr = aCellPtr;

    /* Finally, fill the return value */
    *treeLatchPtr = &(aCellPtr->latch);

    return(eNOERROR);

} /* BtM_GetTreeLatchPtrFromIndexId() */


Four BtM_ReleaseAllTreeLatchPtr(
    Four    handle
)
{
    btmTreeLatchCell *aCellPtr, *prevCellPtr;	/* temporary variable */
    Four e;			/* error number */
    IndexID  *lookupKey;        /* temporary variable */
    Four i;

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,("BtM_ReleaseAllTreeLatchPtr()"));


    /* Mutex Begin : for accessing BTM_TREELATCHTABLE */
    e = SHM_getLatch(handle, &BTM_LATCH4TREELATCHTABLE, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    for (i = 0; i < btm_perThreadDSptr->btmCache4TreeLatch.nEntries; i++) {
        if (!IS_NILINDEXID(BTM_CACHE4TREELATCH(handle)[i].iid)) {

            /* ASSERT :: checking the caching information is correct */
            if (!EQUAL_INDEXID(BTM_CACHE4TREELATCH(handle)[i].iid,
                               BTM_CACHE4TREELATCH(handle)[i].tLatchCellPtr->iid)) {

                e = SHM_releaseLatch(handle, &BTM_LATCH4TREELATCHTABLE, procIndex);
                if (e < eNOERROR) ERR(handle, e);

                return(eBADCACHETREELATCHCELLPTR_BTM);
            }

            BTM_CACHE4TREELATCH(handle)[i].tLatchCellPtr->counter--;
            if (BTM_CACHE4TREELATCH(handle)[i].tLatchCellPtr->counter == 0) {

                /* find corrsponding hash entry */
                lookupKey = &(BTM_CACHE4TREELATCH(handle)[i].iid);
                aCellPtr = PHYSICAL_PTR(BTM_TREELATCHPTRTABLE[BTM_HASH(lookupKey)]); 

                /* search corresponding buffer table entry in hash chain */
                prevCellPtr = NULL;
                while ( aCellPtr && !EQUALKEY_BTM(&aCellPtr->iid, lookupKey)) { 
                    prevCellPtr = aCellPtr;
                    aCellPtr = PHYSICAL_PTR(aCellPtr->next); 
                }

                if ( aCellPtr) {
                    /* disconnect hash chain */
                    if ( prevCellPtr )
                        prevCellPtr->next = aCellPtr->next;
                    else
                        BTM_TREELATCHPTRTABLE[BTM_HASH(lookupKey)] = aCellPtr->next;

                    /* deallocate */
                    e = Util_freeElementToPool(handle, &BTM_TREELATCHPOOL, aCellPtr);
                    if ( e < eNOERROR )
                        ERRL1(handle, e, &BTM_LATCH4TREELATCHTABLE);
                }
                else
                    ERRL1(handle, eNOSUCHTREELATCH_BTM, &BTM_LATCH4TREELATCHTABLE);
            }

            SET_NILINDEXID(BTM_CACHE4TREELATCH(handle)[i].iid);

        } /* if IS_NILINDEXID */
    } /* for */

    /* Mutex End : for accessing BTM_TREELATCHTABLE */
    e = SHM_releaseLatch(handle, &BTM_LATCH4TREELATCHTABLE, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* BtM_ReleaseAllTreeLatchPtr() */

