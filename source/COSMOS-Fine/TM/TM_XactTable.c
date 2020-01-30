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
 * Module: TM_XactTable.c
 *
 * Description:
 *  Manages the transaction table.
 *
 * Exports:
 *  Four TM_XT_InitTable(Four)
 *  Boolean TM_XT_GetEntryPtr(Four, XactID*, TM_XactTableEntry_T**)
 *  Four TM_XT_InsertEntry(Four, TM_ActiveXactRec_T*, TM_XactTableEntry_T**)
 *  void TM_XT_DeleteEntry(Four, XactID*)
 *  void TM_XT_GetMinDeallocLsn(Four, LOG_Lsn_T*)
 *  void TM_XT_DeleteEndedXactEntries(Four)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "xactTable.h"
#include "TM.h"
#include "LOG.h"
#include "SHM.h"
#include "RDsM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * macro definitions
 */

#define NUM_OF_ENTRIES_IN_XACTTBL     	(TOTALTHREADS)
#define TM_XACTTBL_HASH(xactId) 	((xactId).low & (TM_XACTTBL.hashTableSize_1))



/*
 * Function: Four TM_XT_InitTable(Four)
 *
 * Description:
 *  Initialize the transaction table. No concurrency control because this
 *  function runs only at the system bootup.
 *
 * Returns:
 *  error code
 *    eNOERROR
 */
Four TM_XT_InitTable(
    Four    		handle)
{
    Four 		e;			/* error code */
    Four 		i;			/* loop index */
    UFour 		hashTableSize;		/* hash table size */
    XactTableEntry_T 	*anEntry; 		/* a transaction control block */
    void 		*physical_ptr; 


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_InitTable()"));


    SHM_initLatch(handle, &TM_XACTTBL.latch);


    /*
     * Initialize the hash table.
     */
    /* hash table size is the power of 2 and the greatest but no greater than */
    /* NUM_OF_ENTRIES_IN_XACTTBL. */
    hashTableSize = 1;
    for (i = NUM_OF_ENTRIES_IN_XACTTBL; i > 1; i >>= 1)
	hashTableSize <<= 1;

    /* save the 'hash table size - 1' for fast hashing function */
    TM_XACTTBL.hashTableSize_1 = hashTableSize - 1;

    e = SHM_alloc(handle, sizeof(XactTableEntry_T*)*hashTableSize,
		  procIndex, (char**)&physical_ptr);
    TM_XACTTBL.hashTable = LOGICAL_PTR(physical_ptr);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Initialzie the pool used for the entry allocation.
     */
    e = SHM_alloc(handle, sizeof(XactTableEntry_T)*NUM_OF_ENTRIES_IN_XACTTBL,
		  procIndex, (char**)&physical_ptr);
    TM_XACTTBL.freeEntryListHdr = LOGICAL_PTR(physical_ptr);
    if (e < eNOERROR) ERR(handle, e);

    /* make the free list by linking the entries */
    anEntry = PHYSICAL_PTR(TM_XACTTBL.freeEntryListHdr);
    for (i = 0; i < NUM_OF_ENTRIES_IN_XACTTBL; i++) {
        SHM_initLatch(handle, &anEntry->latch);
	anEntry->nextEntry = LOGICAL_PTR(anEntry + 1);
	anEntry = PHYSICAL_PTR(anEntry->nextEntry);
    }
    anEntry->nextEntry = LOGICAL_PTR(NULL);


    /*
     * initialize the hash table entries.
     */
    for (i = 0; i < hashTableSize; i++)
	TM_XACTTBL_HASHTBLENTRY(i) = LOGICAL_PTR(NULL);

    return(eNOERROR);

} /* TM_XT_InitTable() */


/*
 * Function: void TM_XT_InitXactTableEntry(Four, XactTableEntry_T*)
 *
 * Description:
 *  initialize a transaction table entry
 *
 * Returns:
 *  error code
 *
 * Assumption:
 *  The caller should acquire the transaction table entry latch.
 */
void TM_XT_InitXactTableEntry(
    Four 	     	handle,
    XactTableEntry_T 	*entryPtr) /* INOUT pointer to transaction entry */
{

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_InitXactTableEntry(entryPtr=%P)", entryPtr));

    /* initialize the transaction entry */
    entryPtr->xactId = common_perThreadDSptr->nilXactId;
    entryPtr->globalXactId = NULL;
    entryPtr->status = X_NORMAL;
    entryPtr->firstLsn = common_perThreadDSptr->nilLsn;
    entryPtr->lastLsn = common_perThreadDSptr->nilLsn;
    entryPtr->undoNextLsn = common_perThreadDSptr->nilLsn;
    entryPtr->deallocLsn = common_perThreadDSptr->nilLsn;

    entryPtr->dlHead.next = NULL;

    entryPtr->nestedTopActionStackIdx = 0;
    entryPtr->nestedTopActionNoCounter = 0;
    entryPtr->nestedTopActions[0].nestedTopActionNo = -1; /* determined at end point */
    entryPtr->nestedTopActions[0].undoNextLsn = common_perThreadDSptr->nilLsn;
    entryPtr->nestedTopActions[0].deallocLsn = common_perThreadDSptr->nilLsn;
    entryPtr->nestedTopActions[0].idxOnDeallocPageArray = -1;
    entryPtr->nestedTopActions[0].idxOnDeallocTrainArray = -1;

    entryPtr->deallocPageArray = LOGICAL_PTR(NULL);
    entryPtr->sizeOfDeallocPageArray = 0;
    entryPtr->deallocTrainArray = LOGICAL_PTR(NULL);
    entryPtr->sizeOfDeallocTrainArray = 0;

    /* entryPtr->stackIdxForNestedTopLsns = -1; */

    entryPtr->idxOnDeallocPageSegmentArray = -1;
    entryPtr->idxOnDeallocTrainSegmentArray = -1;
    entryPtr->deallocPageSegmentArray = LOGICAL_PTR(NULL);
    entryPtr->sizeOfDeallocPageSegmentArray = 0;
    entryPtr->deallocTrainSegmentArray = LOGICAL_PTR(NULL);
    entryPtr->sizeOfDeallocTrainSegmentArray = 0;

    /*
     * We should be careful for initializing the 'nextEntry' field.
     * Because it is used both in the free entry list and in the hash chain.
     */
    /* entryPtr->nextEntry = NULL; */


} /* TM_XT_InitXactTableEntry() */



/*
 * Function: TM_XT_FinalXactTableEntry(handle, XactTableEntry_T*)
 *
 * Description:
 *  finalize a transaction table entry.
 *
 * Returns:
 *  error code
 */
Four TM_XT_FinalXactTableEntry(
    Four 		handle,
    XactTableEntry_T 	*entryPtr) 	/* INOUT transaction for which transaction table entry is freed */
{
    Four 		e;              /* error code */
    DeallocListElem 	*dlElem;    	/* pointer to an element of dealloc list */

    /* pointer for TM Data Structure of perThreadTable */
    TM_PerThreadDS_T *tm_perThreadDSptr = TM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_FinalXactTableEntry(entryPtr=%P)", entryPtr));


    /*
     * clean up the entry
     */
    if (PHYSICAL_PTR(entryPtr->deallocPageArray) != NULL) {
        e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocPageArray), procIndex);
        if (e < eNOERROR) ERRL1(handle, e, &(TM_XACTTBL.latch));
    }

    if (PHYSICAL_PTR(entryPtr->deallocTrainArray) != NULL) {
        e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocTrainArray), procIndex);
        if (e < eNOERROR) ERRL1(handle, e, &(TM_XACTTBL.latch));
    }

    if (PHYSICAL_PTR(entryPtr->deallocPageSegmentArray) != NULL) {
        e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocPageSegmentArray), procIndex);
        if (e < eNOERROR) ERRL1(handle, e, &(TM_XACTTBL.latch));
    }

    if (PHYSICAL_PTR(entryPtr->deallocTrainSegmentArray) != NULL) {
        e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocTrainSegmentArray), procIndex);
        if (e < eNOERROR) ERRL1(handle, e, &(TM_XACTTBL.latch));
    }

    dlElem = entryPtr->dlHead.next;
    while (dlElem) {
        DeallocListElem *dlTemp;

        dlTemp = dlElem->next;

        e = Util_freeElementToLocalPool(handle, &(tm_perThreadDSptr->tm_dlPool), dlElem);
        if (e < eNOERROR) ERRL1(handle, e, &(TM_XACTTBL.latch));

        dlElem = dlTemp;
    }

    return(eNOERROR);

} /* TM_XT_FinalXactTableEntry( ) */



/*
 * Function: Four TM_XT_AllocAndInitXactTableEntry(Four, XactID*)
 *
 * Description:
 *  allocate a transaction table entry and initialize it.
 *
 * Returns:
 *  error code
 *    eNOERROR
 *    eNOFREEXACTTBLENTRY_TM - no free transaction table entry
 */
Four TM_XT_AllocAndInitXactTableEntry(
    Four 		handle,
    XactID 		*xactId,		/* IN new transaction id */
    XactTableEntry_T 	**returnEntryPtr) 	/* OUT pointer to transaction entry */
{
    Four 		e;                     	/* error code */
    Four 		hashValue;		/* hash value */
    XactTableEntry_T 	*entryPtr; 		/* points to an transaction table entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_AllocAndInitXactTableEntry(xactId=%P, returnEntryPtr=%P)", xactId, returnEntryPtr));


    /*
     *	check input parameters
     */
    if (xactId == NULL) ERR(handle, eBADPARAMETER);

    /* find the hash chain including the transaction entry. */
    hashValue = TM_XACTTBL_HASH(*xactId);

    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * allocate an entry
     */
    entryPtr = PHYSICAL_PTR(TM_XACTTBL.freeEntryListHdr);
    if (entryPtr == NULL) {
	(Four)SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
	ERR(handle, eNOFREEXACTTBLENTRY_TM);
    }
    TM_XACTTBL.freeEntryListHdr = entryPtr->nextEntry;


    /*
     * initialize the transaction entry
     */
    TM_XT_InitXactTableEntry(handle, entryPtr);

    /*
     * Set transaction id.
     */
    entryPtr->xactId = *xactId;

    /*
     * insert the entry into the hash table.
     */
    entryPtr->nextEntry = TM_XACTTBL_HASHTBLENTRY(hashValue);
    TM_XACTTBL_HASHTBLENTRY(hashValue) = LOGICAL_PTR(entryPtr);


    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /* return the transaction table entry pointer. */
    *returnEntryPtr = entryPtr;

    return(eNOERROR);

} /* TM_XT_AllocAndInitXactTableEntry() */



/*
 * Function: TM_XT_FreeXactTableEntry(handle, XactID*)
 *
 * Description:
 *  free a transaction table entry.
 *
 * Returns:
 *  error code
 *    eNOERROR
 */
Four TM_XT_FreeXactTableEntry(
    Four 		handle,
    XactID 		*xactId)             	/* IN transaction for which transaction table entry is freed */
{
    Four 		e;                     	/* error code */
    Four 		hashValue;		/* hash value */
    XactTableEntry_T 	*entryPtr; 		/* points to a transaction table entry */
    XactTableEntry_T 	*prevEntryPtr; 		/* points to a previous transaction table entry */
    DeallocListElem 	*dlElem;    		/* pointer to an element of dealloc list */
    Lsn_T 		lsn;                  	/* lsn of the newly written log record */
    Four 		logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 		/* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_FreeXactTableEntry(xactId=%P)", xactId));


    /* finding a hash chain including the transaction entry. */
    hashValue = TM_XACTTBL_HASH(*xactId);

    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* delete the transaction table entry from the hash chain */
    /* We are sure that my transaction table entry is in the hash chain; */
    /* therefore, no error checking is undid. */
    prevEntryPtr = NULL;
    entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(hashValue));
    while (!XACTID_CMP_EQ(entryPtr->xactId, *xactId)) {
        prevEntryPtr = entryPtr;
        entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
    }

    /*
     * Write the end log reocrd.
     *
     * CAUTION: This log record should be synchronized with writing
     *          the checkpoint log record.
     */
    if (common_shmPtr->recoveryFlag) {

        LOG_FILL_LOGRECINFO_0(logRecInfo, entryPtr->xactId, LOG_TYPE_TRANSACTION,
                              LOG_ACTION_XACT_END_TRANSACTION, LOG_NO_REDO_UNDO,
                              common_perThreadDSptr->nilPid, entryPtr->lastLsn, common_perThreadDSptr->nilLsn);

        e = LOG_WriteLogRecord(handle, entryPtr, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRL1(handle, e, &TM_XACTTBL.latch);
    }

    if (prevEntryPtr == NULL)
        TM_XACTTBL_HASHTBLENTRY(hashValue) = entryPtr->nextEntry;
    else
        prevEntryPtr->nextEntry = entryPtr->nextEntry;

    /* add the the deleted entry into the free entry list. */
    entryPtr->nextEntry = TM_XACTTBL.freeEntryListHdr;
    TM_XACTTBL.freeEntryListHdr = LOGICAL_PTR(entryPtr);

    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* TM_XT_FreeXactTableEntry( ) */


/*
 * Function: Four TM_XT_BeginNestedTopAction(Four, XactTableEntry_T*)
 *
 * Description:
 *  Begin a new nested top action.
 *
 * Returns:
 *  error code
 */
Four TM_XT_BeginNestedTopAction(
    Four 		handle,
    XactTableEntry_T 	*entryPtr, 	/* IN transaction where a nested top action begins */
    Lsn_T 		*undoNextLsn)   /* IN undo next lsn : log record to undo next time */
{
    Four 		idx;            /* stack pointer to the nested top action stack */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_BeginNestedTopAction()"));


    if (entryPtr->nestedTopActionStackIdx+1 == MAX_DEPTH_OF_NESTED_TOP_ACTIONS) 
        ERR(handle, eTOODEEPNESTEDTOPACTION_TM);

    idx = ++entryPtr->nestedTopActionStackIdx;

    entryPtr->nestedTopActions[idx].nestedTopActionNo = ++entryPtr->nestedTopActionNoCounter;
    entryPtr->nestedTopActions[idx].undoNextLsn = *undoNextLsn;
    entryPtr->nestedTopActions[idx].deallocLsn = common_perThreadDSptr->nilLsn;
    entryPtr->nestedTopActions[idx].idxOnDeallocPageArray =  entryPtr->nestedTopActions[idx-1].idxOnDeallocPageArray;
    entryPtr->nestedTopActions[idx].idxOnDeallocTrainArray =  entryPtr->nestedTopActions[idx-1].idxOnDeallocTrainArray;


    return(eNOERROR);

} /* TM_XT_BeginNestedTopAction() */


/*
 * Function: Four TM_XT_EndNestedTopAction(Four, XactTableEntry_T*)
 *
 * Description:
 *  End a new nested top action.
 *
 * Returns:
 *  error code
 */
Four TM_XT_EndNestedTopAction(
    Four 		handle,
    XactTableEntry_T 	*entryPtr, 	/* IN transaction where a nested top action ends */
    LogParameter_T 	*logParam)   	/* IN log parameter */
{
    Four 		e;              /* error code */
    Four 		idx;            /* stack pointer to the nested top action stack */
    Lsn_T 		lsn;            /* lsn of the newly written log record */
    Four 		logRecLen;      /* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 	/* log record information */
    Four 		i;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_EndNestedTopAction()"));


    if (entryPtr->nestedTopActionStackIdx == 0) ERR(handle, eNONESTEDTOPACTION_TM);

    idx = entryPtr->nestedTopActionStackIdx;

    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING || 
	(logParam->logFlag & LOG_FLAG_VOLUME_SPACE_LOGGING && logParam->logFlag & LOG_FLAG_EXTENT_MAP_LOGGING)) { 

        /*
         * Write the compensation log reocrd.
         */
        LOG_FILL_LOGRECINFO_0(logRecInfo, entryPtr->xactId, LOG_TYPE_COMPENSATION,
                              LOG_ACTION_DUMMY_CLR, LOG_NO_REDO_UNDO,
                              common_perThreadDSptr->nilPid, entryPtr->lastLsn,
			      entryPtr->nestedTopActions[idx].undoNextLsn);

        e = LOG_WriteLogRecord(handle, entryPtr, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
     * Deallocate the pages/trains.
     */
    for (i = entryPtr->nestedTopActions[idx].idxOnDeallocPageArray;
         i > entryPtr->nestedTopActions[idx-1].idxOnDeallocPageArray; i--) {
        e = RDsM_FreeTrain(handle, entryPtr, &(((PageID*)PHYSICAL_PTR(entryPtr->deallocPageArray))[i]), PAGESIZE2, TRUE, logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    for (i = entryPtr->nestedTopActions[idx].idxOnDeallocTrainArray;
         i > entryPtr->nestedTopActions[idx-1].idxOnDeallocTrainArray; i--) {
        e = RDsM_FreeTrain(handle, entryPtr, &(((PageID*)PHYSICAL_PTR(entryPtr->deallocTrainArray))[i]), TRAINSIZE2, TRUE, logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    entryPtr->nestedTopActionStackIdx --;


    return(eNOERROR);

} /* TM_XT_EndNestedTopAction() */


/*
 * Function: Four TM_XT_AddToDeallocList(Four, XactTableEntry_T*, PageID*, DLType)
 *
 * Description:
 *  Add a file to the deallocated list.
 *
 * Returns:
 *  error code
 */
Four TM_XT_AddToDeallocList(
    Four                        handle,                 /* IN handle */
    XactTableEntry_T 		*entryPtr, 		/* IN transaction where a nested top action begins */
    PageID 			*pid,                	/* IN file id or index id */
    SegmentID_T			*pageSegmentID, 	/* IN page segment id concerned with 'pid' */
    SegmentID_T			*trainSegmentID, 	/* IN train segment id concerned with 'pid' */
    DLType 			type                  	/* IN file/index */
)
{
    Four 			e;                     	/* error code */
    DeallocListElem 		*dlElem;    		/* dealloc list element */

    /* pointer for TM Data Structure of perThreadTable */
    TM_PerThreadDS_T *tm_perThreadDSptr = TM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_AddToDeallocList()"));


    /*
    ** Insert a new node for the dropped file.
    */
    e = Util_getElementFromLocalPool(handle, &(tm_perThreadDSptr->tm_dlPool), &dlElem);
    if (e < eNOERROR) ERR(handle, e);

    dlElem->type = type;	/* set the type of DeallocList */
    switch (type) {
      case DL_LRGOBJ: dlElem->elem.pid = *pid; break;
      case DL_FILE: dlElem->elem.pFid = *(PhysicalFileID*)pid; break;
      case DL_BTREEINDEX: dlElem->elem.pIid = *(PhysicalIndexID*)pid; break;
      case DL_MLGFINDEX: dlElem->elem.pIid = *(PhysicalIndexID*)pid; break;
      default:
        ERR(handle, eBADDEALLOCLISTTYPE_TM);
    }

    if (pageSegmentID != NULL) dlElem->pageSegmentID = *pageSegmentID;
    else INIT_SEGMENT_ID(&(dlElem->pageSegmentID));

    if (trainSegmentID != NULL) dlElem->trainSegmentID = *trainSegmentID;
    else INIT_SEGMENT_ID(&(dlElem->trainSegmentID));

    dlElem->next = entryPtr->dlHead.next; /* insert to the list */
    entryPtr->dlHead.next = dlElem;       /* new first element of the list */

    return(eNOERROR);

} /* TM_XT_AddToDeallocList() */


/*
 * Function: Four TM_XT_AddToDeallocPageList(Four, XactTableEntry_T*, PageID*)
 *
 * Description:
 *  Add a page to the deallocated page list.
 *
 * Returns:
 *  error code
 */
Four TM_XT_AddToDeallocPageList(
    Four 		handle,
    XactTableEntry_T 	*entryPtr, 	/* IN transaction where a nested top action begins */
    PageID 		*pid)           /* IN page to be inseted */
{
    Four 		e;              /* error code */
    Four 		idx;            /* stack pointer to the nested top action stack */
    PageID 		*pageArray;     /* newly allocated page array */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_AddToDeallocPageList()"));


    idx = entryPtr->nestedTopActionStackIdx;

    if (entryPtr->nestedTopActions[idx].idxOnDeallocPageArray+1 == entryPtr->sizeOfDeallocPageArray) {
        e = SHM_alloc(handle, sizeof(PageID)*(entryPtr->sizeOfDeallocPageArray + DEALLOC_PAGE_ARRAY_INCREASE_SIZE), procIndex, (char**)&pageArray);
        if (e < eNOERROR) ERR(handle, e);

        memcpy(pageArray, PHYSICAL_PTR(entryPtr->deallocPageArray), sizeof(PageID)*entryPtr->sizeOfDeallocPageArray);

        if (PHYSICAL_PTR(entryPtr->deallocPageArray) != NULL) {
            e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocPageArray), procIndex);
            if (e < eNOERROR) ERR(handle, e);
        }

        entryPtr->deallocPageArray = LOGICAL_PTR(pageArray);
        entryPtr->sizeOfDeallocPageArray += DEALLOC_PAGE_ARRAY_INCREASE_SIZE;
    }

    ((PageID*)PHYSICAL_PTR(entryPtr->deallocPageArray))[++entryPtr->nestedTopActions[idx].idxOnDeallocPageArray] = *pid;

    return(eNOERROR);

} /* TM_XT_AddToDeallocPageList() */



/*
 * Function: Four TM_XT_AddToDeallocTrainList(Four, XactTableEntry_T*, PageID*)
 *
 * Description:
 *  Add a train to the deallocated train list.
 *
 * Returns:
 *  error code
 */
Four TM_XT_AddToDeallocTrainList(
    Four 		handle,
    XactTableEntry_T 	*entryPtr, 	/* IN transaction where a nested top action begins */
    PageID 		*pid)           /* IN page to be inseted */
{
    Four 		e;              /* error code */
    Four 		idx;            /* stack pointer to the nested top action stack */
    PageID 		*trainArray;    /* newly allocated train array */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_AddToDeallocTrainList()"));


    idx = entryPtr->nestedTopActionStackIdx;

    if (entryPtr->nestedTopActions[idx].idxOnDeallocTrainArray+1 == entryPtr->sizeOfDeallocTrainArray) {
        e = SHM_alloc(handle, sizeof(TrainID)*(entryPtr->sizeOfDeallocTrainArray + DEALLOC_TRAIN_ARRAY_INCREASE_SIZE), procIndex, (char**)&trainArray);
        if (e < eNOERROR) ERR(handle, e);

        memcpy(trainArray, PHYSICAL_PTR(entryPtr->deallocTrainArray), sizeof(TrainID)*entryPtr->sizeOfDeallocTrainArray);

        if (PHYSICAL_PTR(entryPtr->deallocTrainArray) != NULL) {
            e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocTrainArray), procIndex);
            if (e < eNOERROR) ERR(handle, e);
        }

        entryPtr->deallocTrainArray = LOGICAL_PTR(trainArray);
        entryPtr->sizeOfDeallocTrainArray += DEALLOC_TRAIN_ARRAY_INCREASE_SIZE;
    }

    ((PageID*)PHYSICAL_PTR(entryPtr->deallocTrainArray))[++entryPtr->nestedTopActions[idx].idxOnDeallocTrainArray] = *pid;

    return(eNOERROR);

} /* TM_XT_AddToDeallocTrainList() */



/*
 * Function: Four TM_XT_AddToDeallocPageSegmentList(Four, XactTableEntry_T*, Segment_ID_T*)
 *
 * Description:
 *  Add a page segment to the deallocated page segment list.
 *
 * Returns:
 *  error code
 */
Four TM_XT_AddToDeallocPageSegmentList(
    Four                handle,                 /* IN handle */
    XactTableEntry_T 	*entryPtr, 		/* IN transaction where a nested top action begins */
    SegmentID_T 	*segmentID)     	/* IN segment to be inseted */
{
    Four 		e;                     	/* error code */
    SegmentID_T		*pageSegmentArray;   	/* newly allocated page segment array */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_AddToDeallocPageSegmentList()"));


    if (entryPtr->idxOnDeallocPageSegmentArray+1 == entryPtr->sizeOfDeallocPageSegmentArray) {
        e = SHM_alloc(handle, sizeof(SegmentID_T)*(entryPtr->sizeOfDeallocPageSegmentArray + DEALLOC_PAGE_ARRAY_INCREASE_SIZE),
		      procIndex, (char**)&pageSegmentArray);
        if (e < eNOERROR) ERR(handle, e);

        memcpy(pageSegmentArray, PHYSICAL_PTR(entryPtr->deallocPageSegmentArray),
               sizeof(SegmentID_T)*entryPtr->sizeOfDeallocPageSegmentArray);

        if (PHYSICAL_PTR(entryPtr->deallocPageSegmentArray) != NULL) {
            e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocPageSegmentArray), procIndex);
            if (e < eNOERROR) ERR(handle, e);
        }

        entryPtr->deallocPageSegmentArray = LOGICAL_PTR(pageSegmentArray);
        entryPtr->sizeOfDeallocPageSegmentArray += DEALLOC_PAGE_ARRAY_INCREASE_SIZE;
    }

    ((SegmentID_T*)PHYSICAL_PTR(entryPtr->deallocPageSegmentArray))[++entryPtr->idxOnDeallocPageSegmentArray] = *segmentID;


    return(eNOERROR);

} /* TM_XT_AddToDeallocPageSegmentList() */



/*
 * Function: Four TM_XT_AddToDeallocTrainSegmentList(Four, XactTableEntry_T*, Segment_ID_T*)
 *
 * Description:
 *  Add a train segment to the deallocated train segment list.
 *
 * Returns:
 *  error code
 */
Four TM_XT_AddToDeallocTrainSegmentList(
    Four                handle,                 /* IN handle */
    XactTableEntry_T 	*entryPtr, 		/* IN transaction where a nested top action begins */
    SegmentID_T 	*segmentID)     	/* IN segment to be inseted */
{
    Four 		e;                     	/* error code */
    SegmentID_T		*trainSegmentArray;   	/* newly allocated train segment array */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_AddToDeallocTrainSegmentList()"));


    if (entryPtr->idxOnDeallocTrainSegmentArray+1 == entryPtr->sizeOfDeallocTrainSegmentArray) {
        e = SHM_alloc(handle, sizeof(SegmentID_T)*(entryPtr->sizeOfDeallocTrainSegmentArray + DEALLOC_TRAIN_ARRAY_INCREASE_SIZE),
		      procIndex, (char**)&trainSegmentArray);
        if (e < eNOERROR) ERR(handle, e);

        memcpy(trainSegmentArray, PHYSICAL_PTR(entryPtr->deallocTrainSegmentArray),
               sizeof(SegmentID_T)*entryPtr->sizeOfDeallocTrainSegmentArray);

        if (PHYSICAL_PTR(entryPtr->deallocTrainSegmentArray) != NULL) {
            e = SHM_free(handle, (char*)PHYSICAL_PTR(entryPtr->deallocTrainSegmentArray), procIndex);
            if (e < eNOERROR) ERR(handle, e);
        }

        entryPtr->deallocTrainSegmentArray = LOGICAL_PTR(trainSegmentArray);
        entryPtr->sizeOfDeallocTrainSegmentArray += DEALLOC_TRAIN_ARRAY_INCREASE_SIZE;
    }

    ((SegmentID_T*)PHYSICAL_PTR(entryPtr->deallocTrainSegmentArray))[++entryPtr->idxOnDeallocTrainSegmentArray] = *segmentID;


    return(eNOERROR);

} /* TM_XT_AddToDeallocTrainSegmentList() */



/*
 * Function: Four TM_XT_DeleteLastElemFromDeallocPageSegmentList(Four, XactTableEntry_T*, Segment_ID_T*)
 *
 * Description:
 *  Delete a last element from the deallocated page segment list.
 *
 * Returns:
 *  error code
 */
Four TM_XT_DeleteLastElemFromDeallocPageSegmentList(
    Four                handle,                 /* IN handle */
    XactTableEntry_T 	*entryPtr, 		/* IN transaction where a nested top action begins */
    SegmentID_T 	*segmentID)     	/* IN segment to be inseted */
{
    Four 		e;                     	/* error code */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_DeleteLastElemFromDeallocPageSegmentList()"));


    /* check last element of dealloc page segment list */
    if (!IS_SAME_SEGMENT_ID(segmentID, &((SegmentID_T*)PHYSICAL_PTR(entryPtr->deallocPageSegmentArray))[entryPtr->idxOnDeallocPageSegmentArray]))
	ERR(handle, eBADPARAMETER);


    /* delete last element from dealloc page segment list */
    entryPtr->idxOnDeallocPageSegmentArray--;


    return(eNOERROR);

} /* TM_XT_DeleteLastElemFromDeallocPageSegmentList() */



/*
 * Function: Four TM_XT_DeleteLastElemFromDeallocTrainSegmentList(Four, XactTableEntry_T*, Segment_ID_T*)
 *
 * Description:
 *  Delete a last element from the deallocated train segment list.
 *
 * Returns:
 *  error code
 */
Four TM_XT_DeleteLastElemFromDeallocTrainSegmentList(
    Four                handle,                 /* IN handle */
    XactTableEntry_T 	*entryPtr, 		/* IN transaction where a nested top action begins */
    SegmentID_T 	*segmentID)     	/* IN segment to be inseted */
{
    Four 		e;                     	/* error code */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_DeleteLastElemFromTrainSegmentList()"));


    /* check last element of dealloc train segment list */
    if (!IS_SAME_SEGMENT_ID(segmentID, &((SegmentID_T*)PHYSICAL_PTR(entryPtr->deallocTrainSegmentArray))[entryPtr->idxOnDeallocTrainSegmentArray]))
	ERR(handle, eBADPARAMETER);


    /* delete last element from dealloc train segment list */
    entryPtr->idxOnDeallocTrainSegmentArray--;


    return(eNOERROR);

} /* TM_XT_DeleteLastElemFromTrainSegmentList() */



/*
 * Function: Four TM_XT_LogActiveXacts(Four)
 *
 * Description:
 *  Log the active transactions. It is a part of checkpoint log record.
 *
 * Returns:
 *  error code
 */
Four TM_XT_LogActiveXacts(
    Four    		handle)
{
    Four 		e;
    Four 		i;              /* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */
    ActiveXactRec_T 	activeXacts[LOG_MAX_IMAGE_SIZE/sizeof(ActiveXactRec_T)];
    Four 		count;
    Lsn_T 		lsn;            /* LSN of the newly written log record */
    Four 		logRecLen;      /* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 	/* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    count = 0;
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

            if (count == (sizeof(activeXacts)/sizeof(ActiveXactRec_T))) {
                LOG_FILL_LOGRECINFO_1(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_CHECKPOINT,
                                      LOG_ACTION_CHKPT_ACTIVE_XACTS, LOG_REDO_ONLY,
                                      common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(ActiveXactRec_T)*count, activeXacts);

                e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRL1(handle, e, &TM_XACTTBL.latch);

                count = 0;
            }

            e = SHM_getLatch(handle, &entryPtr->latch, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL1(handle, e, &TM_XACTTBL.latch);

            activeXacts[count].status = entryPtr->status;
            activeXacts[count].xactId = entryPtr->xactId;
            activeXacts[count].firstLsn = entryPtr->firstLsn; 
            activeXacts[count].lastLsn = entryPtr->lastLsn;
            activeXacts[count].undoNextLsn = entryPtr->undoNextLsn;
            activeXacts[count].deallocLsn = entryPtr->deallocLsn;
            count++;

            e = SHM_releaseLatch(handle, &entryPtr->latch, procIndex);
            if (e < eNOERROR) ERRL1(handle, e, &TM_XACTTBL.latch);

    	    /* points to the next entry */
	    entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
	}
    }

    if (count != 0) {
        LOG_FILL_LOGRECINFO_1(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_CHECKPOINT,
			      LOG_ACTION_CHKPT_ACTIVE_XACTS, LOG_REDO_ONLY,
			      common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn,
			      sizeof(ActiveXactRec_T)*count, activeXacts);

        e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRL1(handle, e, &TM_XACTTBL.latch);
    }

    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* TM_XT_LogActiveXacts() */


/*
 * Function: void TM_XT_GetMinXactId(handle, XactID*)
 *
 * Description:
 *  get the minimum transaction id
 *
 * Returns:
 *  error code
 */
Four TM_XT_GetMinXactId(
    Four 		handle,
    XactID 		*xactId)        /* OUT minimum transactin id */
{
    Four        	e;
    Four		i;		/* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_GetMinXactId(xactId=%P)", xactId));


    /*
     *	Set the output parameter to the maximum transaction id.
     */
    SET_MAX_XACTID(*xactId);


    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	find the respective entry and get the field value
     */
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

	    if (XACTID_CMP_LT(entryPtr->xactId, *xactId))
		*xactId = entryPtr->xactId;

	    /* points to the next entry */
	    entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
	}
    }

    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* TM_XT_GetMinXactId( ) */



/*
 * Function: void TM_XT_GetMinFirstLsn(handle, XactID*)
 *
 * Description:
 *  get the minimum first LSN among all active transactions
 *
 * Returns:
 *  error code
 */
Four TM_XT_GetMinFirstLsn(
    Four 		handle,
    Lsn_T 		*minFirstLsn)   /* OUT minimum first LSN */
{
    Four        	e;
    Four		i;		/* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_GetMinFirstLsn(firstLsn=%P)", minFirstLsn));


    /*
     *	Set the output parameter to the maximum LSN.
     */
    SET_MAX_LSN(*minFirstLsn);


    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	find the respective entry and get the field value
     */
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

            e = SHM_getLatch(handle, &entryPtr->latch, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL1(handle, e, &TM_XACTTBL.latch);

	    if (!IS_NIL_LSN(entryPtr->firstLsn) && LSN_CMP_LT(entryPtr->firstLsn, *minFirstLsn))
		*minFirstLsn = entryPtr->firstLsn;

            e = SHM_releaseLatch(handle, &entryPtr->latch, procIndex);
            if (e < eNOERROR) ERRL1(handle, e, &TM_XACTTBL.latch);

	    /* points to the next entry */
	    entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
	}
    }

    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* TM_XT_GetMinFirstLsn() */



/*
 * Function: void TM_XT_GetXactIdFromGlobalXactId(GlobalXactID*)
 *
 * Description:
 *  get the transaction id corresponding to the given global transaction id
 *
 * Returns:
 *  error code
 */
Four TM_XT_GetXactIdFromGlobalXactId(
    Four         	handle,
    GlobalXactID 	*globalXactId, 	/* IN global transaction id*/
    XactID 		*xactId)       	/* OUT local transactin id */
{
    Four        	e;
    Four		i;		/* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_GetXactIdFromGlobalXactId(globalXactId=%P, xactId=%P)", globalXactId, xactId));


    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	find the respective entry and get the field value
     */
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

	    if (entryPtr->globalXactId != NULL && GLOBALXACTID_CMP_EQ(entryPtr->globalXactId, globalXactId)) {
		*xactId = entryPtr->xactId;

                /* release latch */
                e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
                if (e < eNOERROR) ERR(handle, e);

                return(eNOERROR);
            }

	    /* points to the next entry */
	    entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
	}
    }

    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOTFOUNDGLOBALXACTID_TM);

} /* TM_XT_GetXactIdFromGlobalXactId() */



/*
 * Function: void TM_XT_GetPreparedTransactions(Four, GlobalXactID[], Four*)
 *
 * Description:
 *  get the prepared transactions
 *
 * Returns:
 *  error code
 */
Four TM_XT_GetPreparedTransactions(
    Four 		handle,
    Four 		resultBufSize,         	/* IN size of the result array */
    GlobalXactID 	*resultBuf,    		/* OUT prepared transactions */
    Four 		*num)                  	/* OUT number of prepared transactions */
{
    Four        	e;
    Four		i;			/* loop index */
    XactTableEntry_T 	*entryPtr; 		/* points to an transaction table entry */
    Four        	count;


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_GetPreparedTransactions()"));


    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	find the respective entry
     */
    count = 0;
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

	    if (entryPtr->globalXactId != NULL && entryPtr->status == X_PREPARE) {
		if (count < resultBufSize)
                    memcpy(&resultBuf[count], entryPtr->globalXactId, sizeof(GlobalXactID));
                count ++;
            }

	    /* points to the next entry */
	    entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
	}
    }

    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);

    if (num != NULL) *num = count;

    return(eNOERROR);

} /* TM_XT_GetPreparedTransactions() */



/******** The following functions are called only at the restart time. *******/



/*
 * Function: Boolean TM_XT_GetEntryPtr(Four, XactID*, TM_XactTableEntry_T**)
 *
 * Description:
 *  check whether an entry of the specified transaction exists in the transaction table
 *  There is no need for the concurrency control because the restart
 *  process runs alone.
 *
 * Returns:
 *  existance flag
 *    TRUE if exist
 *    FALSE otherwise
 */
Boolean TM_XT_GetEntryPtr(
    Four 		handle,
    XactID		*xactId,		/* IN transaction identifier */
    XactTableEntry_T 	**returnEntryPtr) 	/* OUT pointer to transaction entry */
{
    Four 		hashValue;		/* hash value */
    XactTableEntry_T 	*entryPtr; 		/* points to an transaction table entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_IsInXactTable(xactId=%P, returnEntryPtr=%P)", xactId, returnEntryPtr));


    /* find the deleted transaction entry. */
    hashValue = TM_XACTTBL_HASH(*xactId);

    /* search corresponding entry in hash chain */
    entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(hashValue));

    while (entryPtr != NULL) {

	if (XACTID_CMP_EQ(entryPtr->xactId, *xactId)) {
	    *returnEntryPtr = entryPtr;
	    return (TRUE);
	}

	/* points to the next entry */
	entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
    }

    return(FALSE);

} /* TM_XT_GetEntryPtr() */


/*
 * Function: Four TM_XT_InsertEntry(Four, TM_ActiveXactRec_T*, TM_XactTableEntry_T**)
 *
 * Description:
 *  insert an entry of the specified transaction into the transaction table
 *  There is no need for the concurrency control because the restart
 *  process runs alone.
 *
 * Returns:
 *  error code
 *
 * Assumption:
 *  The transaction entry does not exist in the table; so there is no check.
 */
Four TM_XT_InsertEntry(
    Four 		handle,
    ActiveXactRec_T 	*activeXact, 		/* IN information for active xact */
    XactTableEntry_T 	**returnEntryPtr) 	/* OUT pointer to the inserted transaction entry */
{
    Four 		hashValue;		/* hash value */
    XactTableEntry_T 	*entryPtr; 		/* points to an transaction table entry */
    XactTableEntry_T 	*prevEntryPtr; 		/* points to previous entry of the current entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_InsertEntry(activeXact=%P, returnEntryPtr=%P)", activeXact, returnEntryPtr));


    /*
     *	allocate a free entry
     */
    if (PHYSICAL_PTR(TM_XACTTBL.freeEntryListHdr) == NULL) ERR(handle, eNOFREEXACTTBLENTRY_TM);

    entryPtr = PHYSICAL_PTR(TM_XACTTBL.freeEntryListHdr);
    TM_XACTTBL.freeEntryListHdr = entryPtr->nextEntry;

    TM_XT_InitXactTableEntry(handle, entryPtr);

    /* write the active transaction information. */
    entryPtr->xactId = activeXact->xactId;
    entryPtr->status = activeXact->status;
    entryPtr->firstLsn = activeXact->firstLsn;
    entryPtr->lastLsn = activeXact->lastLsn;
    entryPtr->undoNextLsn = activeXact->undoNextLsn;
    entryPtr->deallocLsn = activeXact->deallocLsn;


    /* find the insertion position. */
    hashValue = TM_XACTTBL_HASH(activeXact->xactId);

    /* insert the new entry */
    entryPtr->nextEntry = TM_XACTTBL_HASHTBLENTRY(hashValue);
    TM_XACTTBL_HASHTBLENTRY(hashValue) = LOGICAL_PTR(entryPtr);


    /* return the new entry pointer */
    *returnEntryPtr = entryPtr;

    return(eNOERROR);

} /* TM_XT_InsertEntry() */



/*
 * Function: void TM_XT_DeleteEntry(Four, XactID*)
 *
 * Description:
 *  delete an entry of the specified transaction from the transaction table.
 *  There is no need for the concurrency control because the restart
 *  process runs alone.
 *
 * Returns:
 *  error code
 *    eNOERROR
 *    eBADPARAM_TM - bad parameter
 *
 * Assumption:
 *  The transaction table has the given entry; so there is no check.
 */
void TM_XT_DeleteEntry(
    Four 		handle,
    XactID 		*xactId)	/* IN xact to delete */
{
    Four 		hashValue;	/* hash value */
    XactTableEntry_T 	*entryPtr;	/* points to an transaction table entry */
    XactTableEntry_T 	*prevEntryPtr; 	/* points to previous entry of the current entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_DeleteEntry(xactId=%P)", xactId));


    /* find the deleted transaction entry. */
    hashValue = TM_XACTTBL_HASH(*xactId);

    /* search corresponding entry in hash chain */
    prevEntryPtr = NULL;
    entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(hashValue));

    while (entryPtr != NULL) {

	if (XACTID_CMP_EQ(entryPtr->xactId, *xactId)) {

	    /* delete this transaction */
            if (prevEntryPtr == NULL)
                TM_XACTTBL_HASHTBLENTRY(hashValue) = entryPtr->nextEntry;
            else
                prevEntryPtr->nextEntry = entryPtr->nextEntry;

	    /* return this entry to the free pool. */
	    entryPtr->nextEntry = TM_XACTTBL.freeEntryListHdr;
	    TM_XACTTBL.freeEntryListHdr = LOGICAL_PTR(entryPtr);

	    return;

	} else {

	    /* points to the next entry */
	    prevEntryPtr = entryPtr;
	    entryPtr = PHYSICAL_PTR(prevEntryPtr->nextEntry);
	}
    }

#ifndef NDEBUG
    printf("TM_XT_DeleteEntry(): no such entry\n");
#endif /* NDEBUG */

} /* TM_XT_DeleteEntry()*/



/*
 * Function: void TM_XT_DeleteRollbackedXactEntries(Four)
 *
 * Description:
 *  delete each entry whose status is ROLLBACK from the transaction
 *  table. There is no need for the concurrency control because the restart
 *  process runs alone.
 *
 * Returns:
 *  None
 */
void TM_XT_DeleteRollbackedXactEntries(
    Four    		handle)
{
    Four		i;		/* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */
    XactTableEntry_T 	*prevEntryPtr; 	/* points to previous entry of the current entry */


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_DeleteRollbackedXactEntries()"));


    /*
     *	find the respective entry and delete the entry
     */
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	prevEntryPtr = NULL;
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

	    if (entryPtr->status == X_NORMAL && IS_NIL_LSN(entryPtr->undoNextLsn)) {

		/* delete this transaction */
                if (prevEntryPtr == NULL)
                    TM_XACTTBL_HASHTBLENTRY(i) = entryPtr->nextEntry;
                else
                    prevEntryPtr->nextEntry = entryPtr->nextEntry;

		/* return this entry to the free pool. */
		entryPtr->nextEntry = TM_XACTTBL.freeEntryListHdr;
		TM_XACTTBL.freeEntryListHdr = LOGICAL_PTR(entryPtr);

		/* points to the next entry */
                entryPtr = (prevEntryPtr == NULL) ? PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i)) : PHYSICAL_PTR(prevEntryPtr->nextEntry);

	    } else {

		/* points to the next entry */
		prevEntryPtr = entryPtr;
		entryPtr = PHYSICAL_PTR(prevEntryPtr->nextEntry);
	    }
	}
    }

} /* TM_XT_DeleteRollbackedXactEntries() */



/*
 * Function: void TM_XT_GetMinDeallocLsn(Four, LOG_Lsn_T*)
 *
 * Description:
 *  get the minimum deallocLsn from the transaction table. Called from the
 *  restart processing component. There is no need
 *  for the concurrency control because the restart process runs alone.
 *
 * Returns:
 *  None
 */
void TM_XT_GetMinDeallocLsn(
    Four 		handle,
    Lsn_T 		*lsn)           /* OUT minimum dealloc Lsn */
{
    Four		i;		/* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_GetMinDeallocLsn(lsn=%P)", lsn));


    /*
     *	Set the output parameter to the maximum lsn.
     */
    *lsn = common_perThreadDSptr->maxLsn;


    /*
     *	find the respective entry and get the field value
     */
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

	    if (!IS_NIL_LSN(entryPtr->deallocLsn) &&
		LSN_CMP_LT(entryPtr->deallocLsn, *lsn))
		*lsn = entryPtr->deallocLsn;

	    /* points to the next entry */
	    entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
	}
    }


} /* TM_XT_GetMinDeallocLsn( ) */


/*
 * Function: void TM_XT_GetMaxUndoNextLsn(Four, LOG_Lsn_T*)
 *
 * Description:
 *  get the maximum undoNextLsn from the transaction table for the undo pass
 *  of the restart processing. There is no need for the concurrency control
 *  because the restart process runs alone.
 *
 * Returns:
 *  error code
 *    eNOERROR
 *    eBADPARAM_TM - bad parameter
 */
void TM_XT_GetMaxUndoNextLsn(
    Four 		handle,
    Lsn_T 		*lsn)           /* OUT maximum undoNextLsn */
{
    Four		i;		/* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_GetMaxUndoNextLsn(lsn=%P)", lsn));


    /*
     *	Set the output parameter to the minimum lsn.
     */
    *lsn = common_perThreadDSptr->minLsn;


    /*
     *	find the respective entry and get the field value
     */
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

	    if (!IS_NIL_LSN(entryPtr->undoNextLsn) &&
		LSN_CMP_GT(entryPtr->undoNextLsn, *lsn))
		*lsn = entryPtr->undoNextLsn;

	    /* points to the next entry */
	    entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
	}
    }

    if (LSN_CMP_EQ(*lsn, common_perThreadDSptr->minLsn)) *lsn = common_perThreadDSptr->nilLsn;

} /* TM_XT_GetMaxUndoNextLsn() */


/*
 * Function: Four TM_XT_DoPendingActionsOfCommittedTransaction(void)
 *
 * Description:
 *  Do pending actions for the committed transactions.
 *
 * Returns:
 *  error code
 */
Four TM_XT_DoPendingActionsOfCommittedTransactions(
    Four    		handle)
{
    Four 		e;              /* error code */
    Four		i;		/* loop index */
    XactTableEntry_T 	*entryPtr; 	/* points to an transaction table entry */
    XactTableEntry_T 	*prevEntryPtr; 	/* points to an transaction table entry */
    Lsn_T 		lsn;            /* lsn of the newly written log record */
    Four 		logRecLen;      /* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 	/* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_TM, TR1, ("TM_XT_DoPendingActionsOfCommittedTransactions()"));


    /*
     * do pending actions for the committed transactions
     */
    for (i = 0; i <= TM_XACTTBL.hashTableSize_1; i++) {

	/* search corresponding entry in hash chain */
        prevEntryPtr = NULL;
	entryPtr = PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i));

	while (entryPtr != NULL) {

	    if (entryPtr->status == X_COMMIT) {

                e = TM_DoPendingActionsOfCommittedTransaction(handle, entryPtr);
                if (e < eNOERROR) ERR(handle, e);

                /*
                 * Write the end log reocrd.
                 */
                LOG_FILL_LOGRECINFO_0(logRecInfo, entryPtr->xactId, LOG_TYPE_TRANSACTION,
                                      LOG_ACTION_XACT_END_TRANSACTION, LOG_NO_REDO_UNDO,
                                      common_perThreadDSptr->nilPid, entryPtr->lastLsn, common_perThreadDSptr->nilLsn);

                e = LOG_WriteLogRecord(handle, entryPtr, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERR(handle, e);

                /*
                 * delete the corresponding entry from the transaction table
                 */
                e = TM_XT_FinalXactTableEntry(handle, entryPtr);
                if (e < eNOERROR) ERR(handle, e);

                if (prevEntryPtr == NULL)
                    TM_XACTTBL_HASHTBLENTRY(i) = entryPtr->nextEntry;
                else
                    prevEntryPtr->nextEntry = entryPtr->nextEntry;

                /* add the the deleted entry into the free entry list. */
                entryPtr->nextEntry = TM_XACTTBL.freeEntryListHdr;
                TM_XACTTBL.freeEntryListHdr = LOGICAL_PTR(entryPtr);

                /* points to the next entry */
                entryPtr = (prevEntryPtr == NULL) ? PHYSICAL_PTR(TM_XACTTBL_HASHTBLENTRY(i)) : PHYSICAL_PTR(prevEntryPtr->nextEntry);

            } else {
                /* points to the next entry */
                prevEntryPtr = entryPtr;
                entryPtr = PHYSICAL_PTR(entryPtr->nextEntry);
            }
	}
    }

    return(eNOERROR);

} /* TM_XT_DoPendingActionsOfCommittedTransactions() */

