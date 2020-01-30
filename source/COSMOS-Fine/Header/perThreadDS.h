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
#ifndef __PERTHREADDS_H__
#define __PERTHREADDS_H__


#include "common.h"
#include "param.h"
#include "error.h"
#include "RDsM.h"
#include "OM.h"
#include "BL_OM.h"
#include "LOT.h"
#include "MLGF.h"
#include "LRDS.h"
#include "BL_LRDS.h"
#include "LOG.h"
#include "BfM.h"
#include "BtM.h"
#include "BL_BtM.h"
#include "Util.h"
#include "TM.h"
#include "RM.h"
#include "SM.h"
#include "BL_SM.h"
#include "LM.h"
#include "SHM.h"
#include "LRDS_XA.h"
#include "COSMOS_XA_ThinLayer.h"
#include "THM_cosmosThread.h"


/* COMMON per Thread Data Structures */
typedef struct COMMON_PerThreadDS_T_tag {

    IndexID	nilIid;
    PageID 	nilPid;
    XactID 	nilXactId;
    Lsn_T 	nilLsn;
    Lsn_T 	minLsn;
    Lsn_T 	maxLsn;

} COMMON_PerThreadDS_T;

/* XA per Thread Data Structures */
typedef struct XA_PerThreadDS_T_tag {

       COSMOSXAopenStatus         cosmos_xa_rmOpenStatus;
       COSMOSXAassociationStatus  cosmos_xa_rmAssociationStatus;
       COSMOSXAtranStatus         cosmos_xa_rmTranStatus;

       COSMOSXAscanStatus         cosmos_xa_scanStatus;
       Four                       cosmos_xa_preparedNum;
       Four                       cosmos_xa_currentPos;
       COSMOS_XA_XID*             cosmos_xa_preparedList;

       Four                       cosmos_xa_volId; 

} XA_PerThreadDS_T;

/* SHM per Thread Data Structures */
typedef struct SHM_PerThreadDS_T_tag {

    Four 	i;  /* this is gabage */

} SHM_PerThreadDS_T;

/* LM per Thread Data Structures */
typedef struct LM_PerThreadDS_T_tag {

    Boolean LM_actionFlag; 
    Boolean LM_autoActionFlag; 


} LM_PerThreadDS_T;

/* SM per Thread Data Structures */
#define MAX_SM_IDXBLKLD_TABLE_SIZE    10
#define SM_IDXBLKLD_TABLE_SIZE        MAX_SM_IDXBLKLD_TABLE_SIZE

typedef struct SM_PerThreadDS_T_tag {

        VarArray 		smScanTable;   	/* scan table of the scan manager */
        LocalPool 		smDLPool;     	/* scan table of the scan manager */

        /* Temporary File Management */
        VarArray 		sm_sysTablesForTmpFiles;  /* SM_SYSTABLES for temporary files */
        VarArray 		sm_sysIndexesForTmpFiles; /* SM_SYSINDEXES for temporary files */
        SM_IdxBlkLdTableEntry   smIdxBlkLdTable[MAX_SM_IDXBLKLD_TABLE_SIZE];

} SM_PerThreadDS_T;

/* RM per Thread Data Structures */
typedef struct RM_PerThreadDS_T_tag {

        PageID 		 	RM_pid4RecoveryLock;
        DirtyPageTable_T 	rm_dirtyPageTable;

} RM_PerThreadDS_T;

/* TM per Thread Data Structures */
typedef struct TM_PerThreadDS_T_tag {

        ConcurrencyLevel 	myCCLevel;
        TM_XactDesc_T 		TM_myXactDesc;
        LocalPool 		tm_dlPool;

} TM_PerThreadDS_T;

/* Util per Thread Data Structures */
#define MAX_SORT_STREAM_TABLE_SIZE   20
#define SORT_STREAM_TABLE_SIZE       MAX_SORT_STREAM_TABLE_SIZE

typedef struct Util_PerThreadDS_T_tag {

        SortStreamTableEntry 	sortStreamTable[MAX_SORT_STREAM_TABLE_SIZE];
        Four 			traceFlag;
        Four 			traceLevel;

} Util_PerThreadDS_T;

/* BtM per Thread Data Structures */
#define MAX_BTM_BLKLD_TABLE_SIZE   10
#define BTM_BLKLD_TABLE_SIZE       MAX_BTM_BLKLD_TABLE_SIZE

typedef struct BtM_PerThreadDS_T_tag {

    VarArray 			btmLockStack;
    VarArray 			btmCache4TreeLatch;        /* cache of tree latch pointers */
    BtM_BlkLdTableEntry  	btmBlkLdTable[MAX_BTM_BLKLD_TABLE_SIZE];
    btm_IdMappingTableEntry_T 	*btm_HashTableForIdMapping[BTM_SIZE_OF_HASH_TABLE_FOR_ID_MAPPING]; 
    LocalPool 			btm_IdMappingTableEntryPool; 

} BtM_PerThreadDS_T;

/* BfM per Thread Data Structures */
typedef struct BfM_PerThreadDS_T_tag {

    LocalPool      		BACB_pool;    /* pool of Buffer_ACC_CB block */
    Buffer_ACC_CB  		MyFixed_BACB; /* pointer to the doubly linked list of BCBs which are fixed by this process  */
    Lock_ctrlBlock 		*myLCB;

} BfM_PerThreadDS_T;

/* LOGper Thread Data Structures */
#define LOGRECTABLESIZE 102 

typedef struct LOG_PerThreadDS_T_tag {

        LOG_LogRecTableEntry_T 	LOG_logRecTbl[LOGRECTABLESIZE];
        Lsn_T 			log_CurrentLsn;
        Buffer_ACC_CB 		*logPage_BCBP;

} LOG_PerThreadDS_T;

/* RDsM per Thread Data Structures */
typedef struct RDsM_PerThreadDS_T_tag {

        RDsM_UserDS_T           rdsm_userDS;
	Four    		io_head_fd;
	Four    		io_head_offset;
	Four    		io_num_of_reads;
	Four    		io_num_of_sequential_reads;
	Four    		io_num_of_writes;
	Four    		io_num_of_sequential_writes;
	RDsM_ReadWriteBuffer_T  rdsm_ReadWriteBuffer;

} RDsM_PerThreadDS_T;


/* OM per Thread Data Structures */
#define MAX_OM_BLKLD_TABLE_SIZE   10
#define OM_BLKLD_TABLE_SIZE       MAX_OM_BLKLD_TABLE_SIZE

typedef struct OM_PerThreadDS_T_tag {

        Four            	curVolNo;
        XactID          	oldMin;
        OM_BlkLdTableEntry   	omBlkLdTable[MAX_OM_BLKLD_TABLE_SIZE];

} OM_PerThreadDS_T;

/* LOT per Thread Data Structures */
typedef struct LOT_PerThreadDS_T_tag {

        VarArray        lot_pageidArray;
        VarArray        lot_entryArray[2];
        Four            lot_entryArraySwitch;

} LOT_PerThreadDS_T;

/* MLGF per Thread Data Structures */
typedef struct MLGF_PerThread_T_tag {

        MLGF_LockStack 			mlgfLockStack;
	mlgf_IdMappingTableEntry_T 	*mlgf_HashTableForIdMapping[MLGF_SIZE_OF_HASH_TABLE_FOR_ID_MAPPING];
	LocalPool 			mlgf_IdMappingTableEntryPool; 

} MLGF_PerThreadDS_T;

typedef struct SignalHandler_T_tag {

    int 		sigNo;
    void 		(*handler)(int);

} SignalHandler_T;

/* LRDS per Thread Data Structures */
#define CATALOGTABLESIZE                3
#define CATALOGTABLEENTRYSIZE           256
#define MAX_LRDS_BLKLD_TABLE_SIZE       10
#define LRDS_BLKLD_TABLE_SIZE           MAX_LRDS_BLKLD_TABLE_SIZE

typedef struct LRDS_PerThreadDS_T_tag {

        /* Catalog Table Names */
        char                        catalogTable[CATALOGTABLESIZE][CATALOGTABLEENTRYSIZE];

        KeyDesc                     lrds_SysCountersCounterNameIndexKdesc;

        VarArray                    lrdsScanTable;     /* Scan Table of LRDS */
        VarArray                    lrdsSetScanTable;  /* Set Scan Table of LRDS */
        VarArray                    lrdsOrderedSetScanTable; /* Ordered Set Scan Table of LRDS */
        VarArray                    lrdsCollectionScanTable; /* Collection Scan Table of LRDS */
        LocalHeap                   lrdsBoolTableHeap;    /* heap for Boolean Table */
        LocalPool                   lrdsOrderedSetAuxColInfoLocalPool; /* AuxColInfo Pool for Ordered Set */
        LocalPool                   lrdsOrderedSetElementLengthLocalPool; 
        
        /* relation table for temporary relations */
        lrds_RelTableEntry          lrdsRelTableForTmpRelations[LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS];

        /* User Open Relation Table of LRDS */
        lrds_UserOpenRelTableEntry  lrdsUserOpenRelTable[LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE];

        lrds_MountTableEntry        lrds_userMountTable[LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE];

        Two_Invariable              lrdsformat_tmpTwo;
        Four_Invariable             lrdsformat_tmpFour;
        Eight_Invariable            lrdsformat_tmpEight;

        LRDS_BlkLdTableEntry        lrdsBlkLdTable[MAX_LRDS_BLKLD_TABLE_SIZE];

	/* XA Interface */
        LRDSXAscanStatus            lrds_xa_scanStatus;
        Four                        lrds_xa_preparedNum;
        Four                        lrds_xa_currentPos;
        LRDS_XA_XID*                lrds_xa_preparedList;
        Four                        lrds_xa_volId;

} LRDS_PerThreadDS_T;


/*
   Per-Thread Data Structure
*/
typedef struct PerThreadTableEntry_T_tag {

	LRDS_PerThreadDS_T		lrdsDS;
	RDsM_PerThreadDS_T		rdsmDS;
	OM_PerThreadDS_T		omDS;
	LOT_PerThreadDS_T		lotDS;
	MLGF_PerThreadDS_T		mlgfDS;
	LOG_PerThreadDS_T		logDS;
	BfM_PerThreadDS_T		bfmDS;
	BtM_PerThreadDS_T		btmDS;
	Util_PerThreadDS_T		utilDS;
	TM_PerThreadDS_T		tmDS;
	RM_PerThreadDS_T		rmDS;
	SM_PerThreadDS_T		smDS;
	LM_PerThreadDS_T		lmDS;
	SHM_PerThreadDS_T		shmDS;
	XA_PerThreadDS_T                xaDS;
	COMMON_PerThreadDS_T	        commonDS;

	TCB				*TCBptr;

	Boolean				used;

} PerThreadTableEntry_T;

/*
 *  MACRO Definition
 */
#define RDsM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].rdsmDS)
#define OM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].omDS)
#define LOT_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].lotDS)
#define MLGF_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].mlgfDS)
#define LRDS_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].lrdsDS)
#define LOG_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].logDS)
#define BfM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].bfmDS)
#define BtM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].btmDS)
#define Util_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].utilDS)
#define TM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].tmDS)
#define RM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].rmDS)
#define SM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].smDS)
#define LM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].lmDS)
#define SHM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].shmDS)
#define XA_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].xaDS)
#define COMMON_PER_THREAD_DS_PTR(_handle)	(&perThreadTable[_handle].commonDS)

/*
 *  Global Variables
 */
extern PerThreadTableEntry_T 	perThreadTable[MAXTHREADS];
extern Four		        previousHandle;

extern cosmos_thread_mutex_t	mutex_perThreadTable; 		/* mutex for per-thread table */
extern cosmos_thread_sem_t      *semaphore_shredMemory;

#define MUTEX_PERTHREADTABLE   (&mutex_perThreadTable)
#define SEMAPHORE_SHAREDMEMORY (semaphore_shredMemory)



/*
 * Function Definition
 */
Four THM_AllocHandle(Four*);
Four THM_FreeHandle(Four);

Four thm_InitPerThreadDS(Four);
Four thm_FinalPerThreadDS(Four);
#endif  /* __PERTHREADDS_H__ */
