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
#ifndef __TM_H__
#define __TM_H__

#include "Util.h"
#include "xactTable.h"


/* Initial Pool Size */
#define INITDPLPOOL   100


/*
 * Type Definition
 */
typedef struct {
    Boolean 		logFlag;		/* reserved: do logging if TRUE */
    XactTableEntry_T 	*myXactTableEntryPtr; 	/* pointer to my transaction table entry */
} TM_XactDesc_T;



/*
 * Macro Definitions
 */

#define MY_XACT_LOG_FLAG(_handle)       (perThreadTable[_handle].tmDS.TM_myXactDesc.logFlag)
#define MY_XACT_TABLE_ENTRY(_handle)    (perThreadTable[_handle].tmDS.TM_myXactDesc.myXactTableEntryPtr)

#define MY_XACT_STATUS(_handle)   	(perThreadTable[_handle].tmDS.TM_myXactDesc.myXactTableEntryPtr->status)
#define MY_XACTID(_handle)        	(perThreadTable[_handle].tmDS.TM_myXactDesc.myXactTableEntryPtr->xactId)
#define MY_LAST_LSN(_handle)      	(perThreadTable[_handle].tmDS.TM_myXactDesc.myXactTableEntryPtr->lastLsn)
#define MY_UNDO_NEXT_LSN(_handle) 	(perThreadTable[_handle].tmDS.TM_myXactDesc.myXactTableEntryPtr->undoNextLsn)
#define MY_DEALLOC_LSN(_handle)   	(perThreadTable[_handle].tmDS.TM_myXactDesc.myXactTableEntryPtr->deallocLsn)
#define MY_XACT_LATCH(_handle)    	(perThreadTable[_handle].tmDS.TM_myXactDesc.myXactTableEntryPtr->latch)


/*** BEGIN_OF_SHM_RELATED_AREA ***/

/*
 * Constant Definitions
 */
#define MAXNESTEDTOPLSNS 10


/*
 * Type Definitions
 */

/* Shared Memory in TM */
typedef struct {

    Four	status;
    Four	procCounter;	/* number of active proccess */
    PIB 	processTable[MAXPROCS];
    TCB         TCB_Pool[TOTALTHREADS];
    XactTable_T xactTable;

    XactID	xactIdCounter;  /* for allocation of transaction id */
    LATCH_TYPE	latch_xactIdCounter; /* mutex for update xactIdCounter */
  
    Pool	globalXactIdPool; /* global transaction id pool */
    
} TM_SHM;

extern TM_SHM *tm_shmPtr;
extern Four procIndex;

#define TM_PROCESSTABLE(_handle)	perThreadTable[_handle].TCBptr
#define MY_PROCENTRY(_handle) 		perThreadTable[_handle].TCBptr
#define TM_XACTTBL			tm_shmPtr->xactTable
#define TM_GLOBALXACTIDPOOL		tm_shmPtr->globalXactIdPool
#ifdef USE_LOGICAL_PTR
#define TM_XACTTBL_HASHTBLENTRY(_i)	(((LogicalPtr_T*)PHYSICAL_PTR(tm_shmPtr->xactTable.hashTable))[_i])
#else
#define TM_XACTTBL_HASHTBLENTRY(_i)	(tm_shmPtr->xactTable.hashTable[_i])
#endif
#define MY_NUMGRANTED(_handle) (perThreadTable[_handle].TCBptr)->nGranted
#define MY_GRANTEDLATCHSTRUCT(_handle) (perThreadTable[_handle].TCBptr->grantedLatchStruct)
#define MY_GRANTEDLATCHLIST(_handle) ((LatchEntry *)(perThreadTable[_handle].TCBptr->grantedLatchStruct)->ptr)
#define MY_GRANTEDLATCHENTRY(_handle, num) ((LatchEntry *)perThreadTable[_handle].TCBptr->grantedLatchStruct->ptr)[num]

/*** END_OF_SHM_RELATED_AREA ***/


/*
 * Function Prototypes
 */
Four TM_InitSharedDS(Four);
Four TM_InitLocalDS(Four);
Four TM_FinalSharedDS(Four);
Four TM_FinalLocalDS(Four);
Four TM_XT_AllocAndInitXactTableEntry(Four, XactID*, XactTableEntry_T**);
Four TM_XT_FreeXactTableEntry(Four, XactID*);
void TM_XT_DeleteEndedXactEntries(Four);
void TM_XT_DeleteEntry(Four, XactID*);
Boolean TM_XT_GetEntryPtr(Four, XactID*, XactTableEntry_T**);
void TM_XT_GetMaxUndoNextLsn(Four, Lsn_T*);
void TM_XT_GetMinDeallocLsn(Four, Lsn_T*);
Four TM_XT_GetMinFirstLsn(Four, Lsn_T*);
Four TM_XT_GetPreparedTransactions(Four, Four, GlobalXactID*, Four*);
Four TM_XT_GetXactIdFromGlobalXactId(Four, GlobalXactID*, XactID*);
Four TM_XT_InitTable(Four);
Four TM_XT_InsertEntry(Four, ActiveXactRec_T*, XactTableEntry_T**);
void TM_SetXactIdCounter(Four, XactID*);
Four TM_XT_GetMinXactId(Four, XactID*);
Four TM_AbortTransaction(Four, XactID*);
Four TM_BeginTransaction(Four, XactID*, ConcurrencyLevel); 
Four TM_CommitTransaction(Four, XactID*);
Four TM_DoPendingActionsOfCommittedTransaction(Four, XactTableEntry_T*);
Four tm_AllocXactId(Four, XactID*);
Four TM_XT_LogActiveXacts(Four);
void TM_XT_DeleteRollbackedXactEntries(Four);
Four TM_XT_DoPendingActionsOfCommittedTransactions(Four);
Four TM_XT_AddToDeallocPageList(Four, XactTableEntry_T*, PageID*);
Four TM_XT_AddToDeallocTrainList(Four, XactTableEntry_T*, TrainID*);
Four TM_XT_AddToDeallocList(Four, XactTableEntry_T*, PageID*, SegmentID_T*, SegmentID_T*, DLType); 
Four TM_XT_AddToDeallocPageSegmentList(Four, XactTableEntry_T*, SegmentID_T*);
Four TM_XT_AddToDeallocTrainSegmentList(Four, XactTableEntry_T*, SegmentID_T*);
Four TM_XT_DeleteLastElemFromDeallocPageSegmentList(Four, XactTableEntry_T*, SegmentID_T*);
Four TM_XT_DeleteLastElemFromDeallocTrainSegmentList(Four, XactTableEntry_T*, SegmentID_T*);
Four TM_XT_BeginNestedTopAction(Four, XactTableEntry_T*, Lsn_T*);
Four TM_XT_EndNestedTopAction(Four, XactTableEntry_T*, LogParameter_T*);
void TM_XT_InitXactTableEntry(Four, XactTableEntry_T*);
Four TM_XT_FinalXactTableEntry(Four, XactTableEntry_T*);
Four TM_EnterTwoPhaseCommit(Four, XactID*, GlobalXactID*);
Four TM_IsReadOnlyTransaction(Four, XactID*, Boolean*);
Four TM_IsDuplicatedGXID(Four, GlobalXactID*, Boolean*);
Four TM_RecoverTwoPhaseCommit(Four, GlobalXactID*, XactID*);
Four TM_PrepareTransaction(Four, XactID*);
Four tm_DL_ConvertIntoSmallUnits(Four, XactTableEntry_T*);
Four tm_DL_Log(Four, XactTableEntry_T*);
Four tm_DL_FreeReally(Four, XactTableEntry_T*);
#endif /* __TM_H__ */
