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
/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
#define	__PERTHREADDS_H__

/*@
 * Inclusion of Header Files
 */

#include "common.h"
#include "param.h"
#include "error.h"
#include "RDsM_Internal.h"
#include "OM_Internal.h"
#include "LOT_Internal.h"
#include "MLGF_Internal.h"
#include "BfM_Internal.h"
#include "BtM_Internal.h"
#include "Util_Internal.h"
#include "RM_Internal.h"
#include "SM_Internal.h"
#include "LRDS.h"
#include "BL_OM_Internal.h"
#include "BL_BtM_Internal.h"
#include "BL_SM_Internal.h"
#include "BL_LRDS.h"
#include "LRDS_XA.h"
#include "COSMOS_XA_ThinLayer.h"


/*@
 * Type Definitions for PerThread Data Structures
 */

/* 
 * COMMON Per-Thread Data Structures 
 */
typedef struct COMMON_PerThreadDS_T_tag {
    Two_Invariable 	lrdsformat_tmpTwo;
    Four_Invariable	lrdsformat_tmpFour;
    Eight_Invariable 	lrdsformat_tmpEight;
} COMMON_PerThreadDS_T;


/* 
 * BfM Per-Thread Data Structures 
 */
typedef struct {
    PageID	pageId;
    Two		index;
} bfm_PageIdSortElement;

typedef struct BfM_PerThreadDS_T_tag {
#ifdef USE_SHARED_MEMORY_BUFFER
    Four		    nMountedVols;
    Four		    mountedVolNos[MAXNUMOFVOLS];
    FixedBufferInfo	    fixedBufInfo;
    Lock_ctrlBlock*	    myLCB;
    Four		    signalBlockCounter;
#ifndef USE_MUTEX
    VarArray		    grantedLatchStruct; 
    TCB*		    threadCtrlBlock;
#endif
#endif
    char*                   bfm_flushBufferMemory;
    TrainID*                bfm_trainIdsMemory;
    bfm_PageIdSortElement*  bfm_pageIdSortBufferMemory;
    Two*                    bfm_logBulkFlushesMemory;
    Two*                    bfm_rawBulkFlushesMemory;
} BfM_PerThreadDS_T;

#define MY_TCB(_handle)				(BFM_PER_THREAD_DS(_handle).threadCtrlBlock)
#define MY_NUMGRANTED(_handle)			(BFM_PER_THREAD_DS(_handle).threadCtrlBlock->nGranted)
#define MY_GRANTEDLATCHSTRUCT(_handle)		(BFM_PER_THREAD_DS(_handle).threadCtrlBlock->grantedLatchStruct)
#define MY_GRANTEDLATCHLIST(_handle)		((LatchEntry* )(MY_GRANTEDLATCHSTRUCT(_handle)->ptr))
#define MY_GRANTEDLATCHENTRY(_handle, idx)    	((LatchEntry* )(MY_GRANTEDLATCHSTRUCT(_handle)->ptr))[idx]	

#define FBI_NFIXEDBUFS(_handle)			(BFM_PER_THREAD_DS(_handle).fixedBufInfo.nFixedBufs)
#define FBI_NUNFIXEDBUFS(_handle)       	(BFM_PER_THREAD_DS(_handle).fixedBufInfo.nUnfixedBufs)
#define FBI_BUFTABLESIZE(_handle)		(BFM_PER_THREAD_DS(_handle).fixedBufInfo.fixedBufTableSize)
#define FBI_FIXED_BUFTABLE(_handle)		(BFM_PER_THREAD_DS(_handle).fixedBufInfo.fixedBufTable)
#define FBI_UNFIXED_BUFTABLE(_handle)		(BFM_PER_THREAD_DS(_handle).fixedBufInfo.unfixedBufTable)

#define SHARED_MEMORY_BUFFER_CHECK_FIXED_BUFFERS(_handle) \
BEGIN_MACRO \
    if (BFM_PER_THREAD_DS(_handle).signalBlockCounter == 0) { \
        FBI_NFIXEDBUFS(_handle) = FBI_NUNFIXEDBUFS(_handle) = 0; \
    } \
END_MACRO

#define SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(_handle)     (BFM_PER_THREAD_DS(_handle).signalBlockCounter++)

#define SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(_handle)   (BFM_PER_THREAD_DS(_handle).signalBlockCounter--)

#define SHARED_MEMORY_BUFFER_CHECK_SIGNAL(_handle) \
BEGIN_MACRO \
    if (BFM_PER_THREAD_DS(_handle).signalBlockCounter == 0 && BFM_PER_PROCESS_DS.receivedSignalNo != -1) { \
        bfm_SignalHandler(BFM_PER_PROCESS_DS.receivedSignalNo); \
    } \
END_MACRO


/* 
 * BtM Per-Thread Data Strucutres 
 */
#define MAX_BTM_BLKLD_TABLE_SIZE 10

typedef struct BtM_PerThreadDS_T_tag {
    BtM_BlkLdTableEntry	btmBlkLdTable[MAX_BTM_BLKLD_TABLE_SIZE];
} BtM_PerThreadDS_T;

#define BTM_BLKLD_TABLE(_handle)	BTM_PER_THREAD_DS(_handle).btmBlkLdTable
#define BTM_BLKLD_TABLE_SIZE 		MAX_BTM_BLKLD_TABLE_SIZE

/* 
 * LOT Per-Thread Data Structures 
 */
typedef struct LOT_PerThreadDS_T_tag {
    VarArray	lot_pageidArray;
    VarArray	lot_entryArray[2];
    Four	lot_entryArraySwitch;
} LOT_PerThreadDS_T;

#define LOT_GET_ENTRY_ARRAY(_handle) 	(LOT_PER_THREAD_DS(_handle).lot_entryArraySwitch ^= CONSTANT_ONE, \
					&LOT_PER_THREAD_DS(_handle).lot_entryArray[LOT_PER_THREAD_DS(_handle).lot_entryArraySwitch])


/* 
 * LRDS Per-Thread Data Structures 
 */
#define MAX_LRDS_BLKLD_TABLE_SIZE 10

typedef struct LRDS_PerThreadDS_T_tag {
    LRDS_SHM			lrds_table_in_memory;
    VarArray 			lrdsScanTable; 
    VarArray 			lrdsSetScanTable;
    VarArray 			lrdsOrderedSetScanTable;
    VarArray 			lrdsCollectionScanTable;
    LocalHeap 			lrdsBoolTableHeap;
    LocalPool 			lrdsOrderedSetAuxColInfoLocalPool;
    lrds_RelTableEntry 		lrdsRelTableForTmpRelations[LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS];
    lrds_UserOpenRelTableEntry 	lrdsUserOpenRelTable[LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE];
    lrds_MountTableEntry 	lrds_userMountTable[LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE];
    LocalPool 			lrdsOrderedSetElementLengthLocalPool;
    LRDS_BlkLdTableEntry	lrdsBlkLdTable[MAX_LRDS_BLKLD_TABLE_SIZE];
    LRDSXAscanStatus        	lrds_xa_scanStatus;
    Four                    	lrds_xa_preparedNum;
    Four                    	lrds_xa_currentPos;
    LRDS_XA_XID*            	lrds_xa_preparedList;
    Four                    	lrds_xa_volId;
} LRDS_PerThreadDS_T;

#define LRDS_BLKLD_TABLE(_handle)      		(LRDS_PER_THREAD_DS(_handle).lrdsBlkLdTable)
#define LRDS_BLKLD_TABLE_SIZE 			MAX_LRDS_BLKLD_TABLE_SIZE

#define LRDS_XA_SCANSTATUS(_handle)     	(LRDS_PER_THREAD_DS(_handle).lrds_xa_scanStatus)
#define LRDS_XA_PREPAREDNUM(_handle)           	(LRDS_PER_THREAD_DS(_handle).lrds_xa_preparedNum)
#define LRDS_XA_CURRENTPOS(_handle)            	(LRDS_PER_THREAD_DS(_handle).lrds_xa_currentPos)
#define LRDS_XA_PREPAREDLIST(_handle)          	(LRDS_PER_THREAD_DS(_handle).lrds_xa_preparedList)
#define LRDS_XA_VOLID(_handle)                 	(LRDS_PER_THREAD_DS(_handle).lrds_xa_volId)


/* 
 * OM Per-Thread Data Structures
 */
#define MAX_OM_BLKLD_TABLE_SIZE 10

typedef struct OM_PerThreadDS_T_tag {
    OM_BlkLdTableEntry 	omBlkLdTable[MAX_OM_BLKLD_TABLE_SIZE];
    VolNo		curVolNo;
} OM_PerThreadDS_T;

#define OM_BLKLD_TABLE(_handle)      	(OM_PER_THREAD_DS(_handle).omBlkLdTable)
#define OM_BLKLD_TABLE_SIZE 		MAX_OM_BLKLD_TABLE_SIZE

/* 
 * RDsM Per-Thread Data Strucutures 
 */
typedef struct RDsM_PerThreadDS_T_tag {
    VolumeTable 		volTable[MAXNUMOFVOLS];
#ifdef READ_WRITE_BUFFER_ALIGN_FOR_LINUX
    RDsM_ReadWriteBuffer_T	rdsm_ReadWriteBuffer;
#endif
    Four            		io_num_of_writes;
    Four            		io_num_of_sequential_writes;
    Four            		io_num_of_reads;
    Four            		io_num_of_sequential_reads;
    Four            		io_head_offset;
    FileDesc        		io_head_fd;
} RDsM_PerThreadDS_T;

/* 
 * RM Per-Thread Data Structures 
 */

typedef struct logTableEntry_t_tag {
    PageID dataPid;
    PageNo logPageNo;
    struct logTableEntry_t_tag *nextEntry;
} logTableEntry_t;

typedef struct logTable_t_tag {
    logTableEntry_t **hashTable;
    Four hashTableSize_1;       /* log table size - 1 */
    Four nInsertedEntries;      /* number of inserted entries */
    Pool poolForEntries;        /* pool for allocation of entries */
} logTable_t;

typedef struct logTableScanCursor_t_tag {
    Four                        idx;    	/* index on the hash table */
    logTableEntry_t             *entryPtr;    	/* pointer to the current entry */
} logTableScanCursor_t;

typedef struct RM_PerThreadDS_T_tag {
    logTable_t 			logTable;
    logTableScanCursor_t 	cursor;
    GlobalXactID    		rm_globalXactID;
    Boolean         		rm_onPrepareFlag;
    rm_LogVolumeInfo_t  	rm_LogVolumeInfo;
    Boolean           		RM_RollbackRequiredFlag;
} RM_PerThreadDS_T;

#define RM_IS_ROLLBACK_REQUIRED(_handle) (RM_PER_THREAD_DS(_handle).RM_RollbackRequiredFlag)

/* 
 * SM Per-Thread Data Structures 
 */
#define MAX_SM_IDXBLKLD_TABLE_SIZE 10

typedef struct SM_PerThreadDS_T_tag {
    sm_MountTableEntry 	    smMountTable[MAXNUMOFVOLS];
    VarArray 		    smScanTable; 
    VarArray 		    smTmpFileIdTable;
    Pool     		    dlPool; 
    KeyDesc 		    smSysTablesDataFileIdIndexKdesc;
    KeyDesc 		    smSysTablesBtreeFileIdIndexKdesc;
    KeyDesc 		    smSysIndexesIndexFileIdIndexKdesc;
    KeyDesc 		    smSysIndexesIndexIdIndexKdesc;
    KeyDesc 		    smSysCountersCounterNameIndexKdesc;
    SM_IdxBlkLdTableEntry   smIdxBlkLdTable[MAX_SM_IDXBLKLD_TABLE_SIZE];
    XactID		    dummy;
    Boolean 		    xactRunningFlag;
    Boolean         	    globalXactRunningFlag;
} SM_PerThreadDS_T;

#define SM_SCANTABLE(_handle)  		((sm_ScanTableEntry *)SM_PER_THREAD_DS(_handle).smScanTable.ptr)
#define SM_TMPFILEIDTABLE(_handle) 	((FileID *)SM_PER_THREAD_DS(_handle).smTmpFileIdTable.ptr)
#define VALID_SCANID(_handle, x) 	( ((x) >= 0) && ((x) < SM_PER_THREAD_DS(_handle).smScanTable.nEntries) && \
					SM_SCANTABLE(_handle)[(x)].scanType != NIL )

#define SM_IDXBLKLD_TABLE(_handle)  	SM_PER_THREAD_DS(_handle).smIdxBlkLdTable
#define SM_IDXBLKLD_TABLE_SIZE 		MAX_SM_IDXBLKLD_TABLE_SIZE

#define MY_XACTID(_handle) 		SM_PER_THREAD_DS(_handle).dummy

#define SM_OpenSortStream(handle, volId, sortTupleDesc) Util_OpenSortStream(handle, volId, sortTupleDesc)
#define SM_CloseSortStream(handle, streamId)            Util_CloseSortStream(handle, streamId)
#define SM_SortingSortStream(handle, streamId)          Util_SortingSortStream(handle, streamId)
#define SM_PutTuplesIntoSortStream(handle, streamId, numTuples, tuples) \
        Util_PutTuplesIntoSortStream(handle, streamId, numTuples, tuples)
#define SM_GetTuplesFromSortStream(handle, streamId, numTuples, tuples, eof) \
        Util_GetTuplesFromSortStream(handle, streamId, numTuples, tuples, eof)
#define SM_GetNumTuplesInSortStream(handle, streamId)   Util_GetNumTuplesInSortStream(handle, streamId)
#define SM_GetSizeOfSortStream(handle, streamId)        Util_GetSizeOfSortStream(handle, streamId)

#define SM_OpenStream(handle, volId)                    Util_OpenStream(handle, volId)
#define SM_CloseStream(handle, streamId)                Util_CloseStream(handle, streamId)
#define SM_ChangePhaseStream(handle, streamId)          Util_ChangePhaseStream(handle, streamId)
#define SM_PutTuplesIntoStream(handle, streamId, numTuples, tuples) \
        Util_PutTuplesIntoStream(handle, streamId, numTuples, tuples)
#define SM_GetTuplesFromStream(handle, streamId, numTuples, tuples, eof) \
        Util_GetTuplesFromStream(handle, streamId, numTuples, tuples, eof)
#define SM_GetNumTuplesInStream(handle, streamId)       Util_GetNumTuplesInStream(handle, streamId)
#define SM_GetSizeOfStream(handle, streamId)            Util_GetSizeOfStream(handle, streamId)

#define SM_initVarArray(varArray, size, number) Util_initVarArray(varArray, size, number)
#define SM_doublesizeVarArray(varArray, size)   Util_doublesizeVarArray(varArrary, size)
#define SM_finalVarArray(varArray)              Util_finalVarArray(varArray)


/* 
 * Util Per-Thread Data Strucutures 
 */
#define MAX_SORT_STREAM_TABLE_SIZE 20

typedef struct Util_PerThreadDS_T_tag {
    SortStreamTableEntry sortStreamTable[MAX_SORT_STREAM_TABLE_SIZE];
    Four		 traceFlag;
    Four		 traceLevel;
} Util_PerThreadDS_T;

#define SORT_STREAM_TABLE(_handle)	UTIL_PER_THREAD_DS(_handle).sortStreamTable
#define SORT_STREAM_TABLE_SIZE 		MAX_SORT_STREAM_TABLE_SIZE


/* 
 * XA Per-Thread Data Structures 
 */
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

#define COSMOS_XA_RMOPENSTATUS(_handle)          (XA_PER_THREAD_DS(_handle).cosmos_xa_rmOpenStatus)
#define COSMOS_XA_RMASSOCIATIONSTATUS(_handle)	 (XA_PER_THREAD_DS(_handle).cosmos_xa_rmAssociationStatus)
#define COSMOS_XA_RMTRANSTATUS(_handle)          (XA_PER_THREAD_DS(_handle).cosmos_xa_rmTranStatus)
#define COSMOS_XA_SCANSTATUS(_handle)            (XA_PER_THREAD_DS(_handle).cosmos_xa_scanStatus)
#define COSMOS_XA_PREPAREDNUM(_handle)           (XA_PER_THREAD_DS(_handle).cosmos_xa_preparedNum)
#define COSMOS_XA_CURRENTPOS(_handle)            (XA_PER_THREAD_DS(_handle).cosmos_xa_currentPos)
#define COSMOS_XA_PREPAREDLIST(_handle)          (XA_PER_THREAD_DS(_handle).cosmos_xa_preparedList)
#define COSMOS_XA_VOLID(_handle)                 (XA_PER_THREAD_DS(_handle).cosmos_xa_volId)


/* 
 * Per-Thread Data Structures 
 */
typedef struct PerThreadTableEntry_T_tag {
    LRDS_PerThreadDS_T		lrdsDS;
    BfM_PerThreadDS_T		bfmDS;
    BtM_PerThreadDS_T		btmDS;
    LOT_PerThreadDS_T		lotDS;
    OM_PerThreadDS_T		omDS;
    RDsM_PerThreadDS_T		rdsmDS;
    RM_PerThreadDS_T		rmDS;
    SM_PerThreadDS_T	 	smDS;
    Util_PerThreadDS_T		utilDS;
    XA_PerThreadDS_T		xaDS;	
    COMMON_PerThreadDS_T	commonDS;

    Boolean			used;
} PerThreadTableEntry_T;


/*
 * Macro Definitions
 */
#define COMMON_PER_THREAD_DS(_handle)		(perThreadTable[_handle].commonDS)
#define BFM_PER_THREAD_DS(_handle)		(perThreadTable[_handle].bfmDS)
#define BTM_PER_THREAD_DS(_handle)		(perThreadTable[_handle].btmDS)
#define LOT_PER_THREAD_DS(_handle)		(perThreadTable[_handle].lotDS)
#define LRDS_PER_THREAD_DS(_handle)		(perThreadTable[_handle].lrdsDS)
#define OM_PER_THREAD_DS(_handle)		(perThreadTable[_handle].omDS)
#define RDSM_PER_THREAD_DS(_handle)		(perThreadTable[_handle].rdsmDS)
#define RM_PER_THREAD_DS(_handle)		(perThreadTable[_handle].rmDS)
#define SM_PER_THREAD_DS(_handle)		(perThreadTable[_handle].smDS)
#define UTIL_PER_THREAD_DS(_handle)		(perThreadTable[_handle].utilDS)
#define XA_PER_THREAD_DS(_handle)		(perThreadTable[_handle].xaDS)

#define COMMON_PER_THREAD_DS_PTR(_handle)	(&perThreadTable[_handle].commonDS)
#define BFM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].bfmDS)
#define BTM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].btmDS)
#define LOT_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].lotDS)
#define LRDS_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].lrdsDS)
#define OM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].omDS)
#define RDSM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].rdsmDS)
#define RM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].rmDS)
#define SM_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].smDS)
#define UTIL_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].utilDS)
#define XA_PER_THREAD_DS_PTR(_handle)		(&perThreadTable[_handle].xaDS)


/*
 * Global Variables
 */

extern PerThreadTableEntry_T	perThreadTable[MAXTHREADS];

#endif /* __PERTHREADDS_H__ */
