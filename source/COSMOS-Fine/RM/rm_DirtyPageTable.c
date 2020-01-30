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
 * Module: RM_DirtyPageTable.c
 *
 * Description:
 *  Manages the restart dirty page table.
 *
 * Exports:
 *  Four RM_DPT_InitTable(Four, DirtyPageTable_T*, Four, Four)
 *  Four RM_DPT_FinalTable(Four, DirtyPageTable_T*)
 *  Four RM_DPT_InsertEntry(Four, DirtyPageTable_T*, PageID*, Lsn_T*)
 *  Four RM_DPT_DeleteEntry(Four, DirtyPageTable_T*, PageID*)
 *  Four RM_DPT_DeleteEntries(Four, DirtyPageTable_T*, Four)
 *  Boolean RM_DPT_GetEntry(Four, DirtyPageTable_T*, PageID*, Lsn_T*)
 *  Four RM_DPT_GetMinRecLsn(Four, DirtyPageTable_T*, Lsn_T*)
 *
 * Notes:
 *  This restart dirty page table is used only on restarting; therefor,
 *  it needs no concurrency control.
 */

#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "dirtyPageTable.h"
#include "RM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


#define DPT_HASH(pid,h_1) ((pid)->volNo & (h_1))


/*
 * Function: Four RM_DPT_InitTable(Four, DirtyPageTable_T*, Four, Four)
 *
 * Description:
 *  Initialize the dirty page table.
 *
 * Returns:
 *  error code
 *    eNOERROR
 *    eMALLOCERR_RM
 *    errors caused by other function calls
 */
Four RM_DPT_InitTable(
    Four 		handle,
    DirtyPageTable_T 	*dpt,			/* INOUT dirty page table to be initialized */
    Four 		hashTableSize,		/* IN # of slots in the hash table */
    Four 		initFreeEntries)	/* IN # of initial free entries in the pool */
{
    Four 		e;			/* error returned  */
    Four 		i;			/* index variable */
    Four 		type;                  	/* buffer type */


    TR_PRINT(handle, TR_RM, TR1,
	     ("RM_DPT_InitTable(handle, dpt=%P, hashTablSize=%ld, initFreeEntries=%ld)",
	      dpt, hashTableSize, initFreeEntries));


#ifdef DEBUG
    /* check if the hashTableSize is power of 2. */

#endif /* DEBUG */


    /*
     * Initialize the hash table.
     */
    dpt->hashTableSize_1 = hashTableSize - 1;

    for (type = 0; type < NUM_BUF_TYPES; type++) {
        dpt->hashTable[type] = (DirtyPageTableEntry_T**)malloc(sizeof(DirtyPageTableEntry_T*)*hashTableSize);
        if (dpt->hashTable[type] == NULL)	{
            for (type--; type >= 0; type--)
                free(dpt->hashTable[type]);
            ERR(handle, eMEMORYALLOCERR);
        }
    }


    /*
     * Initialzie the pool used for the entry allocation.
     */
    e = Util_initLocalPool(handle, &(dpt->freeEntryPool), sizeof(DirtyPageTableEntry_T), initFreeEntries);
    if (e < eNOERROR) {
        for (type = 0; type < NUM_BUF_TYPES; type++)
            free(dpt->hashTable[type]);
	ERR(handle, e);
    }


    /*
     * initialize the hash table entries.
     */
    for (type = 0; type < NUM_BUF_TYPES; type++) {
        for (i = 0; i < hashTableSize; i++) {
            dpt->hashTable[type][i] = NULL;
        }
    }


    return(eNOERROR);

} /* RM_DPT_InitTable() */



/*
 * Function: Four RM_DPT_FinalTable(Four, DirtyPageTable_T*)
 *
 * Description:
 *  Finalize the dirty page table.
 *
 * Returns:
 *  error code
 *    eNOERROR
 *    some errors caused by other function calls
 */
Four RM_DPT_FinalTable(
    Four 		handle,
    DirtyPageTable_T 	*dpt)			/* INOUT dirty page table to finalize */
{
    Four 		e;			/* error code */
    Four 		type;                  	/* buffer type */


    TR_PRINT(handle, TR_RM, TR1, ("RM_DPT_FinalTable(dpt=%P)", dpt));


    /*
     * Finalize the pool used for the entry allocation.
     */
    e = Util_finalLocalPool(handle, &(dpt->freeEntryPool));
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Finalize the hash table.
     */
    for (type = 0; type < NUM_BUF_TYPES; type++)
        free(dpt->hashTable[type]);

    return(eNOERROR);

} /* RM_DPT_FinalTable() */



/*
 * Function: Four RM_DPT_InsertEntry(Four, DirtyPageTable_T*, PageID*, Lsn_T*)
 *
 * Description:
 *   Insert an entry into the dirty page table. If the given entry already
 *   exist in the table, then set the RecLSN to new RecLSN.
 *
 * Returns:
 *   error code
 *     eNOERROR
 */
Four RM_DPT_InsertEntry(
    Four 			handle,
    DirtyPageTable_T 		*dpt,			/* INOUT dirty page table */
    PageID 			*pid,			/* IN page id of the inserted entry */
    Four 			bufferType,           	/* IN buffer type */
    Lsn_T 			*recLsn)		/* IN recovery lsn of the inserted entry */
{
    Four 			e;			/* error code */
    Four 			hashValue;		/* hash value */
    DirtyPageTableEntry_T 	*entryPtr; 		/* points to an entry in dirty page table */
    DirtyPageTableEntry_T 	*newEntry; 		/* new entry to insert */


    TR_PRINT(handle, TR_RM, TR1,
	     ("RM_DPT_InsertEntry(handle, dpt=%P, pid=%P, recLsn=%P)", dpt, pid, recLsn));


    /*
     * apply hash function and find the corresponding entry
     */
    hashValue = DPT_HASH(pid, dpt->hashTableSize_1);


    /*
     * search corresponding entry in hash chain
     */
    for (entryPtr = dpt->hashTable[bufferType][hashValue];
	 entryPtr != NULL && !EQUAL_PAGEID(entryPtr->pid, *pid);
	 entryPtr = entryPtr->nextEntry);


    if (entryPtr == NULL) { /* not found!!! */
	/* insert the new entry into the front of the hash chain */

	/* allocate a new entry */
	e = Util_getElementFromLocalPool(handle, &(dpt->freeEntryPool), &newEntry);
	if (e < eNOERROR) ERR(handle, e);

	newEntry->pid = *pid;
	newEntry->recLsn = *recLsn;
	newEntry->nextEntry = dpt->hashTable[bufferType][hashValue];

	dpt->hashTable[bufferType][hashValue] = newEntry;

    } else { /* found!!! */
	/* update the recovery lsn to the new value */
	entryPtr->recLsn = *recLsn;
    }

    return(eNOERROR);

} /* RM_DPT_InsertEntry() */



/*
 * Function: Four RM_DPT_DeleteEntry(DirtyPageTable_T*, PageID*)
 *
 * Description:
 *  Delete the given entry from the dirty page table.
 *
 * Returns:
 *  eNOERROR
 *  error code returned by other function calls
 */
Four RM_DPT_DeleteEntry(
    Four			handle,
    DirtyPageTable_T 		*dpt,			/* INOUT dirty page table */
    PageID 			*pid,			/* IN page id of the deleted entry */
    Four 			bufferType)            	/* IN buffer type */
{
    Four 			e;			/* error code */
    Four 			hashValue;		/* hash value */
    DirtyPageTableEntry_T 	*entryPtr; 		/* points to an entry in a hash chain */
    DirtyPageTableEntry_T 	*prevEntryPtr; 		/* points to a previous entry of the entry pointed by entryPtr */


    TR_PRINT(handle, TR_RM, TR1, ("RM_DPT_DeleteEntry(dpt=%P, pid=%P)", dpt, pid));


    /*
     * apply hash function and find the corresponding entry
     */
    hashValue = DPT_HASH(pid, dpt->hashTableSize_1);


    /*
     * search corresponding entry in hash chain
     */
    for (prevEntryPtr = NULL, entryPtr = dpt->hashTable[bufferType][hashValue];
	 entryPtr != NULL && !EQUAL_PAGEID(entryPtr->pid, *pid);
	 prevEntryPtr = entryPtr, entryPtr = prevEntryPtr->nextEntry) ;

    /* return successfully code if the given entry is not found */
    if (entryPtr == NULL) return(eNOERROR);


    /*
     * delete the entry
     */
    if (prevEntryPtr == NULL)
	dpt->hashTable[bufferType][hashValue] = entryPtr->nextEntry;
    else
	prevEntryPtr->nextEntry = entryPtr->nextEntry;

    e = Util_freeElementToLocalPool(handle, &(dpt->freeEntryPool), entryPtr);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* RM_DPT_DeleteEntry() */



/*
 * Function: Four RM_DPT_DeleteEntries(Four, DirtyPageTable_T*, Four)
 *
 * Description:
 *  Delete all the entries from the dirty page table which have the given volNo.
 *
 */
Four RM_DPT_DeleteEntries(
    Four 			handle,
    DirtyPageTable_T 		*dpt,			/* INOUT dirty page table */
    Four 			volNo)			/* IN volume no to be deleted in the table */
{
    Four 			e;			/* error code */
    Four 			i;			/* index variable */
    Four 			type;                  	/* buffer type */
    DirtyPageTableEntry_T 	*entryPtr; 		/* points to an entry in a hash chain */
    DirtyPageTableEntry_T 	*prevEntryPtr; 		/* points to a previous entry of the entry pointed by entryPtr */


    TR_PRINT(handle, TR_RM, TR1, ("RM_DPT_DeleteEntries(dpt=%P, volNo=%P)", dpt, volNo));


    /* search Dirty Page Table and delete the entries that have given volNo */
    for (type = 0; type < NUM_BUF_TYPES; type++) {
        for (i = 0; i <= dpt->hashTableSize_1; i++) {

            /* search corresponding entry in hash chain */
            prevEntryPtr = NULL;
            entryPtr = dpt->hashTable[type][i];

            while (entryPtr != NULL) {

                if (entryPtr->pid.volNo == volNo) { /* found !!! */
                    /* delete the current entry */

                    if (prevEntryPtr == NULL)
                        dpt->hashTable[type][i] = entryPtr->nextEntry;
                    else
                        prevEntryPtr->nextEntry = entryPtr->nextEntry;

                    e = Util_freeElementToLocalPool(handle, &(dpt->freeEntryPool), entryPtr);
                    if (e < eNOERROR) ERR(handle, e);

                    entryPtr = (prevEntryPtr == NULL) ? dpt->hashTable[type][i] : prevEntryPtr->nextEntry;

                } else {
                    /* points to the next entry */
                    prevEntryPtr = entryPtr;
                    entryPtr = prevEntryPtr->nextEntry;
                }
            } /* end of while */
        } /* end of for */
    }

    return(eNOERROR);

} /* RM_DPT_DeleteEntries() */



/*
 * Function: Boolean RM_DPT_GetEntry(Four, DirtyPageTable_T*, PageID*, Lsn_T*)
 *
 * Description:
 *   Find an entry with the given pageID.
 *
 * Returns:
 *  flag indicating whether the search-for-entry exists
 *    TRUE if the entry exist
 *    FALSE otherwise
 */
Boolean RM_DPT_GetEntry(
    Four 			handle,
    DirtyPageTable_T 		*dpt,			/* IN dirty page table */
    PageID 			*pid,			/* IN page id of entry to look up */
    Four 			bufferType,            	/* IN buffer type */
    Lsn_T 			*recLsn)		/* OUT recovery lsn of the found entry */
{
    Four 			e;			/* error code */
    Four 			hashValue;		/* hash value */
    DirtyPageTableEntry_T 	*entryPtr;		/* points to an entry in dirty page table */


    TR_PRINT(handle, TR_RM, TR1, ("RM_DPT_GetEntry(dpt=%P, pid=%P, recLsn=%P)", dpt, pid, recLsn));


    /*
     * apply hash function and find the corresponding entry
     */
    hashValue = DPT_HASH(pid, dpt->hashTableSize_1);


    /*
     * search corresponding entry in hash chain
     */
    for (entryPtr = dpt->hashTable[bufferType][hashValue];
	 entryPtr != NULL && !EQUAL_PAGEID(entryPtr->pid, *pid);
	 entryPtr = entryPtr->nextEntry);

    /* if the entry is not found, return FALSE. */
    if (entryPtr == NULL) return(FALSE);

    /* return the recovery lsn */
    *recLsn = entryPtr->recLsn;

    return(TRUE);

} /* RM_DPT_GetEtnry() */



/*
 * Function: Four RM_DPT_GetMinRecLsn(Four, RM_DPT_DirtyPageTable_T*, Lsn_T*)
 *
 * Description:
 *  Get the Minimum Recovery LSN among all the entries in the dirty page table.
 *
 * Returns:
 *  error code
 *    eNOERROR
 */
Four RM_DPT_GetMinRecLsn(
    Four 			handle,
    DirtyPageTable_T 		*dpt,			/* IN dirty page table */
    Lsn_T 			*minRecLsn)		/* OUT Minimum Recovery LSN */
{
    Four 			e;			/* return status */
    Four 			i;			/* index variable */
    Four 			type;                  	/* buffer type */
    DirtyPageTableEntry_T 	*entryPtr; 		/* points to an entry in a hash chain */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RM, TR1, ("RM_DPT_GetMinRecLsn(dpt=%P, minRecLsn=%P)", dpt, minRecLsn));


    /* initialize minRecLSN as MAXIMUM */
    *minRecLsn = common_perThreadDSptr->maxLsn;

    /* look at the all the entries in dirty page table */
    for (type = 0; type < NUM_BUF_TYPES; type++) {
        for (i = 0; i <= dpt->hashTableSize_1; i++) {

            /* look at all the entries in a hash chain */
            for (entryPtr = dpt->hashTable[type][i]; entryPtr != NULL; entryPtr = entryPtr->nextEntry)
                if (LSN_CMP_LT(entryPtr->recLsn, *minRecLsn))
                    *minRecLsn = entryPtr->recLsn;
        }
    }

    return(eNOERROR);

} /* RM_DPT_GetMinRecLsn() */
