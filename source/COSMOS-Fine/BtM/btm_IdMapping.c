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
 * Module: btm_IdMapping.c
 *
 * Description:
 *  Manages the mapping table used to get Physical Id from Logical Id.
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Functions */
Four btm_IdMapping_GetPhysicalIdFromCatalog(Four, XactTableEntry_T*, BtreeIndexInfo*, PhysicalIndexID*, LockParameter*);


/*
 * Function: Four btm_IdMapping_InitTable(Four)
 *
 * Description:
 *  Initialize the mapping table.
 */
Four btm_IdMapping_InitTable(
    Four			handle
)
{
    Four 			e;			/* error code */
    Four 			i;			/* loop index */

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_TM, TR1, ("btm_IdMapping_InitTable()"));


    /*
     * Initialize the hash table.
     */
    for (i = 0; i < BTM_SIZE_OF_HASH_TABLE_FOR_ID_MAPPING; i++)
        btm_perThreadDSptr->btm_HashTableForIdMapping[i] = NULL;

    e = Util_initLocalPool(handle, &(btm_perThreadDSptr->btm_IdMappingTableEntryPool), sizeof(btm_IdMappingTableEntry_T), BTM_INIT_SIZE_MAPPING_TABLE_ENTRY_POOL);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* btm_IdMapping_InitTable() */



/*
 * Function: btm_IdMapping_FinalTable(Four)
 *
 * Description:
 *  finalize the ID mapping table.
 */
Four btm_IdMapping_FinalTable(
    Four			handle
)
{
    Four 			e;                     /* error code */

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);


    e = Util_finalLocalPool(handle, &(btm_perThreadDSptr->btm_IdMappingTableEntryPool));
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* btm_IdMapping_FinalTable() */



/*
 * Function: Four btm_IdMapping_GetPhysicalId(Four, XactTableEntry_T*, BtreeIndexInfo*, PhysicalIndexID*, LockParameter*)
 *
 * Description:
 *  Get physical index id from the logical index id
 */
Four btm_IdMapping_GetPhysicalId(
    Four			handle,
    XactTableEntry_T		*xactEntry,		/* IN  transaction table entry */
    BtreeIndexInfo		*iinfo,			/* IN  index information */
    PhysicalIndexID		*pIid,      		/* OUT physical index ID */
    LockParameter		*lockup)		/* IN  lockup parameter */
{
    Four 			e;
    Four 			hashValue;		/* hash value */
    btm_IdMappingTableEntry_T 	*entryPtr; 		/* points to an entry of id mapping table */

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);


    /* find the deleted transaction entry. */
    hashValue = BTM_MAPPINGTBL_HASH(iinfo->iid);

    /* search corresponding entry in hash chain */
    entryPtr = btm_perThreadDSptr->btm_HashTableForIdMapping[hashValue];

    while (entryPtr != NULL) {

	if (EQUAL_LOGICALID(entryPtr->logicalId, iinfo->iid)) {
            *pIid = entryPtr->physicalId;
	    return (eNOERROR);
	}

	/* points to the next entry */
	entryPtr = entryPtr->nextEntry;
    }

    /* The id mapping table does not contain the logical id */
    /* So we get the physical id from the catalog and cache the value in the id mapping table */
    e = btm_IdMapping_GetPhysicalIdFromCatalog(handle, xactEntry, iinfo, pIid, lockup);
    if (e < eNOERROR) ERR(handle, e);

    e = btm_IdMapping_InsertEntry(handle, &iinfo->iid, pIid);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* btm_IdMapping_GetPhysicalId() */


/*
 * Function: Four btm_IdMapping_InsertEntry(Four, IndexID*, PhysicalIndexID*)
 *
 * Description:
 *  insert an entry into the id mapping table
 */
Four btm_IdMapping_InsertEntry(
    Four			handle,
    IndexID 			*lIid,       		/* IN logical id */
    PhysicalIndexID 		*pIid)     		/* IN physical id */
{
    Four 			e;                     	/* error code */
    Four 			hashValue;		/* hash value */
    btm_IdMappingTableEntry_T 	*newEntryPtr;

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_TM, TR1, ("btm_IdMapping_InsertEntry(lIid=%P, pIid=%P)", lIid, pIid));


    /*
     *	allocate a free entry
     */
    e = Util_getElementFromLocalPool(handle, &(btm_perThreadDSptr->btm_IdMappingTableEntryPool), &newEntryPtr);
    if (e < eNOERROR) ERR(handle, e);

    newEntryPtr->logicalId = *lIid;
    newEntryPtr->physicalId = *pIid;


    /* find the insertion position. */
    hashValue = BTM_MAPPINGTBL_HASH(*lIid);

    /* insert the new entry */
    newEntryPtr->nextEntry = btm_perThreadDSptr->btm_HashTableForIdMapping[hashValue];
    btm_perThreadDSptr->btm_HashTableForIdMapping[hashValue] = newEntryPtr;


    return(eNOERROR);

} /* btm_IdMapping_InsertEntry() */



/*
 * Function: Four btm_IdMapping_DeleteEntry(Four, LogicalID*)
 *
 * Description:
 *  delete the specified entry from the id mapping table
 */
Four btm_IdMapping_DeleteEntry(
    Four			handle,
    IndexID 			*logicalId)         	/* IN logical index id to delete */
{
    Four 			e;                     	/* error code */
    Four 			hashValue;		/* hash value */
    btm_IdMappingTableEntry_T 	*entryPtr;		/* points to an transaction table entry */
    btm_IdMappingTableEntry_T 	*prevEntryPtr; 		/* points to previous entry of the current entry */

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_TM, TR1, ("btm_IdMapping_DeleteEntry(logicalId=%P)", logicalId));


    /* find the deleted transaction entry. */
    hashValue = BTM_MAPPINGTBL_HASH(*logicalId);

    /* search corresponding entry in hash chain */
    prevEntryPtr = NULL;
    entryPtr = btm_perThreadDSptr->btm_HashTableForIdMapping[hashValue];

    while (entryPtr != NULL) {

	if (EQUAL_LOGICALID(entryPtr->logicalId, *logicalId)) {

	    /* delete this transaction */
            if (prevEntryPtr == NULL)
                btm_perThreadDSptr->btm_HashTableForIdMapping[hashValue] = entryPtr->nextEntry;
            else
                prevEntryPtr->nextEntry = entryPtr->nextEntry;

	    /* return this entry to the free pool. */
	    e = Util_freeElementToLocalPool(handle, &(btm_perThreadDSptr->btm_IdMappingTableEntryPool), entryPtr);
            if (e < eNOERROR) ERR(handle, e);

	    return (eNOERROR);

	} else {

	    /* points to the next entry */
	    prevEntryPtr = entryPtr;
	    entryPtr = prevEntryPtr->nextEntry;
	}
    }

#ifndef NDEBUG
    printf("btm_IdMapping_DeleteEntry(): no such entry\n");
#endif /* NDEBUG */

    return (eNOERROR);

} /* btm_IdMapping_DeleteEntry()*/



/*
 * Function: Four btm_IdMapping_GetPhysicalIdFromCatalog(Four, XactTableEntry_T*, BtreeIndexInfo*, PhysicalIndexID*, LockParameter*);
 *
 * Description:
 *  Get the physical id from the catalog table
 */
Four btm_IdMapping_GetPhysicalIdFromCatalog(
    Four			handle,
    XactTableEntry_T		*xactEntry,			/* IN  transaction table entry */
    BtreeIndexInfo		*iinfo,				/* IN  index information */
    PhysicalIndexID		*physicalId,     		/* OUT physical ID */
    LockParameter		*lockup)			/* IN  lockup parameter */
{
    Four               		e;               		/* error code */
    Boolean            		checkFlag;       		/* TRUE when we should do the check */
    LockReply          		lockReply;       		/* lock reply */
    LockMode           		oldMode;
    PageID             		tmpPid;
    Buffer_ACC_CB		*catPage_BCBP;    		/* buffer access control block holding catalog data */
    SlottedPage			*catPage;         		/* pointer to buffer containing the catalog */
    sm_CatOverlayForSysIndexes	*catEntry; 			/* pointer to data file catalog information */


    /* ordinary file; get the last page from the catalog table */
#ifdef CCPL
    /* Request X lock on the page where the catalog entry resides. */
    if (lockup != NULL) {
        e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)&iinfo->catalog.oid,
                               L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) {
            ERR(handle, eDEADLOCK); /* deadlock */
        }
    }

    /* get the root page in catalog */
    e = BfM_getAndFixBuffer(handle, (TrainID*)&iinfo->catalog.oid, M_FREE, &catPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
    GET_PTR_TO_CATENTRY_FOR_BTREE(iinfo->catalog.oid.slotNo, catPage, catEntry);

    /* get the root page ID */
    MAKE_PAGEID(*physicalId, iinfo->fid.volNo, catEntry->rootPage);

    e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
    if(e < eNOERROR) ERR(handle, e);

    /* Release the lock on the catalog page. */
    if (lockup != NULL) {
        e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)&iinfo->catalog.oid, L_MANUAL);
        if (e < eNOERROR) ERR(handle, e);
    }
#endif /* CCPL */

#ifdef CCRL
    /* get the last page in catalog */
    e = BfM_getAndFixBuffer(handle, (TrainID*)&iinfo->catalog.oid, M_SHARED, &catPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
    GET_PTR_TO_CATENTRY_FOR_BTREE(iinfo->catalog.oid.slotNo, catPage, catEntry);

    /* get the root page ID */
    MAKE_PAGEID(*physicalId, iinfo->iid.volNo, catEntry->rootPage);

    e = SHM_releaseLatch(handle, catPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

    e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
    if(e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    return(eNOERROR);

} /* btm_IdMapping_GetPhysicalIdFromCatalog() */


