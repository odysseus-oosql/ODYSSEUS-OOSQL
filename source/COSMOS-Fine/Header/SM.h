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
#ifndef _SM_H_
#define _SM_H_


#include "Util_pool.h"
#include "Util_varArray.h"
#include "Util.h"


/* Initial Table Size */
#define INITMOUNT      		5

/* Scan Type */
#define	SEQUENTIAL		0
#define	BTREEINDEX		1
#define MLGFINDEX		2 

/* Scan Direction of the Sequential Scan */
#define FORWARD         	0
#define BACKWARD        	1
#define BACKWARD_NOORDERING	2
#define BACKWARD_ORDERING 	3


/*
** BoundCond Type: used in a range scan to give bound condition
*/
typedef struct {
    KeyValue key;		/* Key Value */
    CompOp   op;		/* The key value is included? */
} BoundCond;

typedef struct {
    Two nparts;
    struct {
        Four type;
        Four offset;
        Four length;
    } parts[MAXNUMKEYPARTS];
} SortKeyAttrInfo;

typedef Four (*GetKeyAttrsFuncPtr_T) (Four, Object*, void*, SortKeyDesc*, SortKeyAttrInfo*);

/*
** Macros for SYSTABLES caltalog table
*/
#define SM_SYSTABLES_BTREE_START sizeof(sm_CatOverlayForData)


/*
** Macro for SYSINDEXES caltalog table
*/

/*
** Volume Mount Table used in scan manager
*/
typedef struct {

    Two      volId;		/* the mounted volume ID  */

    DataFileInfo sysTablesInfo;	/* file information for SM_SYSTABLES */
    DataFileInfo sysIndexesInfo; /* file information for SM_SYSINDEXES */

    BtreeIndexInfo sysTablesDataFileIdIndexInfo;    /* B+ tree on data FileID of SM_SYSTABLES */
    /* BtreeIndexInfo sysTablesBtreeFileIdIndexInfo; */ /* B+ tree on B+tree FileID of SM_SYSTABLES */

    BtreeIndexInfo sysIndexesIndexIdIndexInfo;      /* B+ tree on IndexID of SM_SYSINDEXES */
    BtreeIndexInfo sysIndexesDataFileIdIndexInfo;   /* B+ tree on Data FileID of SM_SYSINDEXES */

    DataFileInfo    sysCountersInfo;             /* entry for SM_SYSINDEXES in SM_SYSTABLES */
    BtreeIndexInfo sysCountersCounterNameIndexInfo; /* B+ tree on IndexID of SM_SYSINDEXES */

    Four 	nMount;		/* number of Mount */
    LATCH_TYPE	latch;		/* mutex for this update/read Mount Table Entry */

} sm_MountTableEntry;


/*
** Scan Table used in scan manager ( Not Located in Shred Memory )
*/
typedef struct {
    One direction;		/* scan direction: FORWARD or BACKWARD */
} sm_ScanInfoForSeqScan;

typedef struct {
    BtreeIndexInfo iinfo;	/* the used Btree index information */
    KeyDesc   kdesc;		/* key descriptor of the index */
    BoundCond startCond;	/* start condition of the Btree range scan */
    BoundCond stopCond;		/* stop condition of the Btree range scan */
} sm_ScanInfoForBtreeScan;

typedef struct {
    MLGFIndexInfo iinfo;	/* the used MLGF index information */ 
    MLGF_KeyDesc kdesc;		/* key description of the index */
    MLGF_HashValue lowerBound[MLGF_MAXNUM_KEYS]; /* lower bounds */
    MLGF_HashValue upperBound[MLGF_MAXNUM_KEYS]; /* upper bounds */
} sm_ScanInfoForMLGF_Scan;

typedef struct {
    DataFileInfo finfo;		/* data file information */
    /* ObjectID  catalogEntry;*/	/* points an object in a catalog SM_SYSTABLES */
				/* The object is an entry for the scaned file. */
    One       scanType;		/* NIL / SEQUENTIAL / BTREEINDEX / MLGFINDEX */
    union {
	sm_ScanInfoForSeqScan seq;
	sm_ScanInfoForBtreeScan btree;
	sm_ScanInfoForMLGF_Scan mlgf;
    } scanInfo;
    Cursor    cursor;		/* Cursor of this scan */
    LockMode  acquiredFileLock; /* acquired lock for file */
} sm_ScanTableEntry;


typedef struct ComponentInfo_T_tag {
    Four (*initLocalDS)(Four);
    Four (*initSharedDS)(Four);
    Four (*finalComponent)(Four);
    Four (*finalLocalDS)(Four);
    Four (*finalSharedDS)(Four);
} ComponentInfo_T;

/*
** Macro Definitions
*/
/* acces sm_sysTablesForTmpFiles */
#define SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(_handle) (perThreadTable[_handle].smDS.sm_sysTablesForTmpFiles.nEntries) 
#define SM_ST_FOR_TMP_FILES(_handle)  ((sm_CatOverlayForSysTables*)(perThreadTable[_handle].smDS.sm_sysTablesForTmpFiles.ptr))
#define SM_SET_TO_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(entry) (entry).data.fid.volNo = NIL
#define SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(entry) ((entry).data.fid.volNo == NIL)

/* acces sm_sysIndexesForTmpFiles */
#define SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(_handle) (perThreadTable[_handle].smDS.sm_sysIndexesForTmpFiles.nEntries) 
#define SM_SI_FOR_TMP_FILES(_handle)  ((sm_CatOverlayForSysIndexes*)(perThreadTable[_handle].smDS.sm_sysIndexesForTmpFiles.ptr))
#define SM_SET_TO_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(entry) (entry).iid.volNo = NIL
#define SM_IS_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(entry) ((entry).iid.volNo == NIL)

/* access smScanTable entry */
#define SM_SCANTABLE(_handle)     ((sm_ScanTableEntry *)perThreadTable[_handle].smDS.smScanTable.ptr)

/* Is 'x' the valid scan identifier? */
#define VALID_SCANID(_handle, x) ( ((x) >= 0) && ((x) < (perThreadTable[_handle].smDS.smScanTable.nEntries)) && \
			  SM_SCANTABLE(_handle)[(x)].scanType != NIL )

/* action control */
#define SM_NEED_AUTO_ACTION(_handle) (!USER_ACTION_ON(_handle) && MY_CC_LEVEL(_handle) != X_RR_RR && MY_XACT_TABLE_ENTRY(_handle) != NULL) 

#define SM_DLPOOL(_handle)                  (perThreadTable[_handle].smDS.smDLPool)


/*-------------------- BEGIN OF Shared Memory Section -----------------------*/
/*
 * Shared Memory Data Structures
 */
typedef struct {

    LATCH_TYPE	latch_smMountTable; /* latch for allocate/deallocate smMountTable entry */
    sm_MountTableEntry smMountTable[MAXNUMOFVOLS]; /* Mount Table of Scan Manager */


    KeyDesc smSysTablesDataFileIdIndexKdesc;
    KeyDesc smSysIndexesDataFileIdIndexKdesc;
    KeyDesc smSysIndexesIndexIdIndexKdesc;
    KeyDesc smSysCountersCounterNameIndexKdesc; 

} SM_SHM;

extern SM_SHM *sm_shmPtr;
extern Four procIndex;

/* macro definition */

#define SM_MOUNTTABLE                  		sm_shmPtr->smMountTable
#define SM_LATCH_MOUNTTABLE        	   	sm_shmPtr->latch_smMountTable
#define SM_SYSTBL_DFILEIDIDX_KEYDESC   		sm_shmPtr->smSysTablesDataFileIdIndexKdesc
#define SM_SYSIDX_INDEXID_KEYDESC      		sm_shmPtr->smSysIndexesIndexIdIndexKdesc
#define SM_SYSIDX_DATAFILEID_KEYDESC  		sm_shmPtr->smSysIndexesDataFileIdIndexKdesc
#define SM_SYSCNTR_CNTRNAME_KEYDESC  		sm_shmPtr->smSysCountersCounterNameIndexKdesc 


/*-------------------- END OF Shared Memory Section -------------------------*/

/*-------------------- BEGIN OF Per Process Data Structure ------------------*/

/* Per Process Data Structure */

#ifdef LRDS_INCLUDED
#define NUMBER_OF_COMPONENTINFOS 15
#else
#define NUMBER_OF_COMPONENTINFOS 14
#endif

typedef struct SM_PerProcessDs_T_tag {

	/* key descriptors of the indexes on the catalog tables */
	KeyDesc sm_sysTablesDataFileIdIndexKdesc;
	KeyDesc sm_sysIndexesDataFileIdIndexKdesc;
	KeyDesc sm_sysIndexesIndexIdIndexKdesc;

} SM_PerProcessDS_T;

extern	ComponentInfo_T 	componentInfos[NUMBER_OF_COMPONENTINFOS];
extern	KeyDesc 		sm_sysTablesDataFileIdIndexKdesc;
extern	KeyDesc 		sm_sysIndexesDataFileIdIndexKdesc;
extern	KeyDesc 		sm_sysIndexesIndexIdIndexKdesc;
/*-------------------- END OF Per Process Data Structure --------------------*/



/*
** Scan Manager Internal Functions Prototypes
*/
Four sm_GetCatalogEntryFromDataFileId(Four, Four, FileID*, ObjectID*);
Four sm_GetCatalogEntryFromIndexId(Four, Four, IndexID*, ObjectID*, PhysicalIndexID*); 
Four sm_GetIndexInfoFromIndexId(Four, Four, IndexID*, BtreeIndexInfo*, FileID*); 
Four sm_GetNewFileId(Four, Four, FileID*);
Four sm_GetNewIndexId(Four, Four, IndexID*);
Four sm_mlgf_GetIndexInfoFromIndexId(Four, Four, IndexID*, MLGFIndexInfo*, FileID*); 

/*
** Scan Manager Interface Functions Prototypes
*/
Four SM_AddIndex(Four, FileID*, IndexID*, LockParameter*);
Four SM_CloseScan(Four, Four);
Four SM_CloseAllScan(Four);
Four SM_CreateFile(Four, Four, FileID*, Boolean, LockParameter *);
Four SM_CreateObject(Four, Four, ObjectID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*);
Four SM_CreateObjectWithoutScan(Four, FileID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*, LockParameter*);
Four SM_DeleteIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*); 
Four SM_DeleteMetaDictEntry(Four, Four, char*);
Four SM_DestroyFile(Four, FileID*, LockParameter*);
Four SM_DestroyObject(Four, Four, ObjectID*, LockParameter*);
Four SM_DestroyObjectWithoutScan(Four, FileID*, ObjectID*, LockParameter*, LockParameter*);
Four SM_Dismount(Four, Four);
Four SM_DropIndex(Four, IndexID*, LockParameter*);
char *SM_Err(Four, Four);
Four SM_Final(void);
Four SM_FinalLocalDS(Four);
Four SM_FinalSharedDS(Four);
Four SM_FetchObject(Four, Four, ObjectID*, Four, Four, char*, LockParameter*);
Four SM_FetchObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, char*, LockParameter*, LockParameter*);
Four SM_GetMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_GetObjectHdr(Four, Four, ObjectID*, ObjectHdr*, LockParameter*);
Four SM_Init(void);
Four SM_InitializeGlobalData(Four);
Four SM_InitAllLocalDS(Four);
Four SM_InitAllSharedDS(Four);
Four SM_InitLocalDS(Four);
Four SM_InitSharedDS(Four);
Four SM_InsertIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*);
Four SM_InsertMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_Mount(Four, Four, char**, Four*);
Four SM_NextObject(Four, Four, ObjectID*, ObjectHdr*, char*, Cursor**, LockParameter*);
Four SM_OpenIndexScan(Four, FileID*, IndexID*, KeyDesc*, BoundCond*, BoundCond*, LockParameter*);
Four SM_OpenSeqScan(Four, FileID*, Four, LockParameter*);
Four SM_SetMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_SetObjectHdr(Four, Four, ObjectID*, ObjectHdr*, LockParameter*);
Four SM_UpdateObject(Four, Four, ObjectID*, Four, Four, void*, Four, LockParameter*);
Four SM_UpdateObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, void*, Four, LockParameter*, LockParameter*);
Four SM_WriteObjectRedoOnlyWithoutScan(Four, FileID*, ObjectID*, Four, Four, char*, LockParameter*, LockParameter*);
Four SM_MLGF_AddIndex(Four, FileID*, IndexID*, MLGF_KeyDesc*, LockParameter*);
Four SM_MLGF_DeleteIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, LockParameter*);
Four SM_MLGF_DropIndex(Four, IndexID*, LockParameter*);
Four SM_MLGF_InsertIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, void*, LockParameter*);
Four SM_MLGF_OpenIndexScan(Four, FileID*, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], MLGF_HashValue[], LockParameter*);
Four SM_MLGF_SearchNearObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, LockParameter*);
Four SM_FormatDataVolume(Four, Four, char**, char*, Four, Four, Four*, Four);
Four SM_FormatTempDataVolume(Four, Four, char**, char*, Four, Four, Four*, Four);
Four SM_FormatLogVolume(Four, Four, char**, char*, Four, Four, Four*);
Four SM_SetCfgParam(Four, char*, char*);
char *SM_GetCfgParam(Four, char*);
Four SM_SortFile(Four, VolID, FileID*, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, Boolean, Boolean, FileID*, LockParameter*); 
Four SM_BeginTransaction(Four, XactID*, ConcurrencyLevel);
Four SM_CommitTransaction(Four, XactID*);
Four SM_AbortTransaction(Four, XactID*);
Four SM_BeginAction(Four); 
Four SM_EndAction(Four); 
Four SM_GetStatistics_numDataFileAndIndex(Four, Four, Four*, sm_NumIndexes*, LockParameter*);
Four SM_GetStatistics_DataFilePageInfo(Four, FileID*, PageID*, Four*, sm_PageInfo*, LockParameter*);
Four SM_GetStatistics_BtreePageInfo(Four, IndexID*, Four, Four*, sm_PageInfo*, LockParameter*);
Four SM_GetStatistics_numExtents(Four, Four, Four*, Four*, Four*, LockParameter*);
Four SM_GetStatistics_numPages(Four, Four, sm_NumPages*, Boolean, LockParameter*);
Four _SM_GetStatistics_numExtents(Four, Four, Four*, Four*, Four*);
Four _SM_GetStatistics_numPages(Four, Four, sm_NumPages*, Boolean, Boolean);
Four SM_ExpandDataVolume(Four, Four, Four, char **, Four*); 
Four SM_IsReadOnlyTransaction(Four, XactID*, Boolean*);
Four SM_SetSavepoint(Four, SavepointID *);
Four SM_RollbackSavepoint(Four, SavepointID);
Four SM_EnterTwoPhaseCommit(Four, XactID*, GlobalXactID*);
Four SM_GetNumberOfPreparedTransactions(Four, Four*);
Four SM_GetPreparedTransactions(Four, Four, GlobalXactID[]);
Four SM_PrepareTransaction(Four, XactID*);
Four SM_RecoverTwoPhaseCommit(Four, GlobalXactID*, XactID*);
Four SM_CreateCounter(Four, Four, char*, Four, CounterID*);
Four SM_DestroyCounter(Four, Four, char*);
Four SM_GetCounterId(Four, Four, char*, CounterID*);
Four SM_SetCounter(Four, Four, CounterID*, Four);
Four SM_ReadCounter(Four, Four, CounterID*, Four*);
Four SM_GetCounterValues(Four, Four, CounterID*, Four, Four*);

#include "Util_Sort.h"
Four SM_OpenSortStream(Four, Four, SortTupleDesc*);
Four SM_CloseSortStream(Four, Four);
Four SM_SortingSortStream(Four, Four);
Four SM_PutTuplesIntoSortStream(Four, Four, Four, SortStreamTuple*);
Four SM_GetTuplesFromSortStream(Four, Four, Four*, SortStreamTuple*, Boolean*);
Four SM_GetNumTuplesInSortStream(Four, Four);
Four SM_GetSizeOfSortStream(Four, Four);

Four SM_OpenStream(Four, Four);
Four SM_CloseStream(Four, Four);
Four SM_PutTuplesIntoStream(Four, Four, Four, SortStreamTuple*);
Four SM_ChangePhaseStream(Four, Four);
Four SM_GetTuplesFromStream(Four, Four, Four*, SortStreamTuple*, Boolean*);
Four SM_GetNumTuplesInStream(Four, Four);
Four SM_GetSizeOfStream(Four, Four);

Four SM_InitIndexBulkLoad(Four, VolID, KeyDesc*);
Four SM_NextIndexBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four SM_FinalIndexBulkLoad(Four, Four, IndexID*, Two, Two, LockParameter*); 

Four SM_InitSortedIndexBulkLoad(Four, IndexID*, KeyDesc*, Two, Two, LockParameter*); 
Four SM_NextSortedIndexBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four SM_FinalSortedIndexBulkLoad(Four, Four);

Four SM_InitIndexBulkInsert (Four, VolID, KeyDesc*);
Four SM_NextIndexBulkInsert (Four, Four, KeyValue*, ObjectID*);
Four SM_FinalIndexBulkInsert (Four, Four, IndexID*, KeyDesc*, LockParameter*);

Four SM_InitDataFileBulkLoad(Four, VolID, FileID*, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, Boolean, Two, Two, PageID*, LockParameter*);
Four SM_NextDataFileBulkLoad(Four, Four, char*, Four, Boolean, ObjectID*);
Four SM_NextDataFileBulkLoadWriteLOT(Four, Four, Four, Four, char*, Boolean, ObjectID*);
Four SM_FinalDataFileBulkLoad(Four, Four blkLdId);


/*
 * Thread API
 */
Four SM_AllocHandle(Four*);
Four SM_FreeHandle(Four);

#endif /* _SM_H_ */
