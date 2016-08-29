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
#ifndef _SM_H_
#define _SM_H_



/*@
 * Constant Definitions
 */
/* Scan Direction of the Sequential Scan */
#define FORWARD         0
#define BACKWARD        1
#define BACKWARD_NOORDERING 2
#define BACKWARD_ORDERING 3


/*@
 * Type Definitions
 */
/*
** BoundCond Type: used in a range scan to give bound condition
*/
typedef struct {
    KeyValue key;		/* Key Value */
    CompOp   op;		/* The key value is included? */
} BoundCond;


/* for porting */

typedef struct {
    Two nparts;
    struct {
        Two           	type;
        Four 		offset;
        Two      	length;
    } parts[MAXNUMKEYPARTS];
} SortKeyAttrInfo;

typedef Four (*GetKeyAttrsFuncPtr_T) (Object*, void*, SortKeyDesc*, SortKeyAttrInfo*);


/*
** Cursor definition
*/
/* AnyCursor:
 *  All cursors should have the following members at the front of them
 *  in the same order.
 */
typedef struct {
    One flag;                   /* state of the cursor */
                                /* CURSOR_INVALID, CURSOR_BOS, CURSOR_ON, CURSOR_EOS */
    ObjectID oid;               /* object pointed by the cursor */
} SM_AnyCursor;

/* DataCursor:
 *  sequential scan using the data file
 */
typedef struct {
    One      flag;                      /* state of the cursor */
    ObjectID oid;               /* object pointed by the cursor */
} SM_DataCursor;

/* BtreeCursor:
 *  scan using a B+ tree
 */
typedef struct {
    One                         flag;              /* state of the cursor */
    ObjectID                    oid;               /* object pointed by the cursor */
    KeyValue                    key;               /* what key value? */
    PageID                      leaf;              /* which leaf page? */
    PageID                      overflow;          /* which overflow page? */
    Two                         slotNo;            /* which slot? */
    Two                         oidArrayElemNo;    /* which element of the object array? */
} SM_BtreeCursor;

typedef struct {
    One                         flag;              /* state of the cursor */
    ObjectID                    oid;               /* object pointed by the cursor */
    MLGF_HashValue              keys[MLGF_MAXNUM_KEYS]; /* what key values? */
    PageID                      leaf;              /* which leaf page? */
    PageID                      overflow;          /* which overflow page? */
    Two                         entryNo;           /* which entry? */
    Two                         oidArrayElemNo;    /* which element of the object array? */
    Two                         pathTop;           /* top of path stack */
    VarArray                    path;              /* traverse path from root to leaf */
} SM_MLGF_Cursor;

/* Universal Cursor */
typedef union {
    SM_AnyCursor any;           /* for access of 'flag' and 'oid' */
    SM_DataCursor seq;          /* sequential scan */
    SM_BtreeCursor btree;               /* scan using a B+ tree */
    SM_MLGF_Cursor mlgf;                /* scan using MLGF index */
} SM_Cursor;

/* for porting */


/*@
 * Function Prototypes
 */
/*
** Scan Manager Interface Functions Prototypes
*/
Four _SM_AddIndex(Four, FileID*, IndexID*);
Four _SM_CloseScan(Four, Four);
Four _SM_CreateFile(Four, Four, Two, Two, FileID*, Boolean); 
Four _SM_CreateObject(Four, Four, ObjectID*, ObjectHdr*, Four, char*, ObjectID*);
Four _SM_CreateObjectWithoutScan(Four, FileID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*);
Four _SM_DeleteIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*);
Four _SM_DestroyFile(Four, FileID*);
Four _SM_DestroyObject(Four, Four, ObjectID*);
Four _SM_DestroyObjectWithoutScan(Four, FileID*, ObjectID*);
Four _SM_Dismount(Four, Four);
Four _SM_DropIndex(Four, IndexID*);
Four _SM_FetchObject(Four, Four, ObjectID*, Four, Four, char*);
Four _SM_FetchObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, char*);
Four _SM_Final();
Four _SM_GetMetaDictEntry(Four, Four, char*, void*, Four);
Four _SM_GetObjectHdr(Four, Four, ObjectID*, ObjectHdr*);
Four _SM_Init(Four handle);            
Four _SM_InsertIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*);
Four _SM_InsertMetaDictEntry(Four, Four, char*, void*, Four);
Four _SM_Mount(Four, Four, char**, Four*);
Four _SM_NextObject(Four, Four, ObjectID*, ObjectHdr*, char*, SM_Cursor**); 
Four _SM_OpenIndexScan(Four, FileID*, IndexID*, KeyDesc*, BoundCond*, BoundCond*);
Four _SM_OpenSeqScan(Four, FileID*, Four);
Four _SM_SetMetaDictEntry(Four, Four, char*, void*, Four);
Four _SM_SetObjectHdr(Four, Four, ObjectID*, ObjectHdr*);
Four _SM_UpdateObject(Four, Four, ObjectID*, Four, Four, void*, Four);
Four _SM_UpdateObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, void*, Four);
Four _SM_WriteObjectRedoOnlyWithoutScan(Four, FileID*, ObjectID*, Four, Four, char*);
Four _SM_SetCfgParam(Four, char*, char*);
char* _SM_GetCfgParam(Four, char*);
Four _SM_SortFile(Four, VolID, FileID*, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, Boolean, Boolean, FileID*); 
Four _SM_GetStatistics_DataFilePageInfo(Four, FileID*, PageID*, Four*, sm_PageInfo*);
Four _SM_GetStatistics_BtreePageInfo(Four, IndexID*, Four, Four*, sm_PageInfo*);
Four _SM_GetStatistics_numExtents(Four, Four, Two*, Four*, Four*);
Four _SM_GetStatistics_numPages(Four, Four, sm_NumPages*, Boolean, Boolean); 
Four _SM_BeginTransaction(Four, Boolean); 
Four _SM_AbortTransaction(Four);
Four _SM_CommitTransaction(Four);
Four _SM_FormatDataVolume(Four, Four, char**, char*, Four, Two, Four*);
Four _SM_FormatLogVolume(Four, Four, char**, char*, Four, Two, Four*);
Four _SM_ExpandDataVolume(Four, Four, Four, char**, Four*); 
#ifdef USE_COHERENCY_VOLUME
Four _SM_FormatCoherencyVolume(Four, char*, char*, Four);
#endif

Four _SM_InitIndexBulkLoad(Four, VolID, KeyDesc*);
Four _SM_NextIndexBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four _SM_FinalIndexBulkLoad(Four, Four, IndexID*, Two, Two);

Four _SM_InitSortedIndexBulkLoad(Four, IndexID*, KeyDesc*, Two, Two);
Four _SM_NextSortedIndexBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four _SM_FinalSortedIndexBulkLoad(Four, Four);

Four _SM_InitDataFileBulkLoad(Four, VolID, FileID*, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, Boolean, Two, Two, PageID*);
Four _SM_NextDataFileBulkLoad(Four, Four, char*, Four, Boolean, ObjectID*);
Four _SM_NextDataFileBulkLoadWriteLOT(Four, Four, Four, Four, char*, Boolean, ObjectID*);
Four _SM_FinalDataFileBulkLoad(Four, Four);

Four _SM_GetKeyOIDSortStream(Four, VolID, FileID*, Two, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, PageID*, Four*); 

Four _SM_InitIndexBulkInsert(Four, VolID, KeyDesc *);
Four _SM_NextIndexBulkInsert(Four, Four, KeyValue*, ObjectID*);
Four _SM_FinalIndexBulkInsert(Four, Four, IndexID*, KeyDesc*);

Four _SM_CreateCounter(Four, Four, char*, Four, CounterID*);
Four _SM_DestroyCounter(Four, Four, char*);
Four _SM_GetCounterId(Four, Four, char*, CounterID*);
Four _SM_SetCounter(Four, Four, CounterID*, Four);
Four _SM_ReadCounter(Four, Four, CounterID*, Four*);
Four _SM_GetCounterValues(Four, Four, CounterID*, Four, Four*);


/* Interfaces for multi users */
Four SM_AddIndex(Four, FileID*, IndexID*, LockParameter*);
Four SM_CloseScan(Four, Four);
Four SM_CloseAllScan(Four);
Four SM_CreateFile(Four, Four, FileID*, Boolean, LockParameter*);
Four SM_CreateObject(Four, Four, ObjectID*, ObjectHdr*, Four, void*, ObjectID*, LockParameter*);
Four SM_dump(Four);
Four SM_DeleteIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*);
Four SM_DestroyFile(Four, FileID*, LockParameter*);
Four SM_DestroyObject(Four, Four, ObjectID*, LockParameter*);
Four SM_Dismount(Four, Four);
Four SM_DropIndex(Four, IndexID*, LockParameter*);
char *SM_Err(Four, Four);
Four SM_FetchObject(Four, Four, ObjectID*, Four, Four, char*, LockParameter*);
Four SM_Final();
Four SM_FreeHandle(Four handle);
Four SM_FinalLocalDS(Four);
Four SM_FinalSharedDS(Four);
Four SM_GetMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_GetObjectHdr(Four, Four, ObjectID*, ObjectHdr*, LockParameter*);
Four SM_Init();
Four SM_AllocHandle(Four* handle);	
Four SM_InitLocalDS(Four);
Four SM_InitSharedDS(Four);
Four SM_InsertIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*);
Four SM_Mount(Four, Four, char**, Four*);
Four SM_NextObject(Four, Four, ObjectID*, ObjectHdr*, char*, Cursor**, LockParameter*); 
Four SM_OpenIndexScan(Four, FileID*, IndexID*, KeyDesc*, BoundCond*, BoundCond*, LockParameter*);
Four SM_OpenSeqScan(Four, FileID*, Four, LockParameter*);
Four SM_SetMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_SetObjectHdr(Four, Four, ObjectID*, ObjectHdr*, LockParameter*);
Four SM_UpdateObject(Four, Four, ObjectID*, Four, Four, void*, Four, LockParameter*);

Four SM_MLGF_AddIndex(Four, FileID*, IndexID*, MLGF_KeyDesc*, LockParameter*);
Four SM_MLGF_DeleteIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*, ObjectID*, LockParameter*);
Four SM_MLGF_DropIndex(Four, IndexID*, LockParameter*);
Four SM_MLGF_InsertIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*, ObjectID*, void*, LockParameter*);
Four SM_MLGF_OpenIndexScan(Four, FileID*, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*, MLGF_HashValue*, LockParameter*);
Four SM_MLGF_SearchNearObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*, ObjectID*, LockParameter*);
Four SM_SortFile(Four, VolID, FileID*, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, Boolean, Boolean, FileID*, LockParameter*); 
Four SM_FormatDataVolume(Four, Four, char**, char*, Four, Two, Four*, Four);
Four SM_FormatTempDataVolume(Four, Four, char**, char*, Four, Two, Four*, Four); 
Four SM_FormatLogVolume(Four, Four, char**, char*, Four, Two, Four*);
Four SM_ExpandDataVolume(Four, Four, Four, char**, Four*); 
#ifdef USE_COHERENCY_VOLUME
Four SM_FormatCoherencyVolume(Four, char*, char*, Four);
#endif

Four SM_BeginTransaction(Four, XactID *, ConcurrencyLevel);  
Four SM_AbortTransaction(Four, XactID *);
Four SM_CommitTransaction(Four, XactID *);

Four SM_CreateObjectWithoutScan(Four, FileID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*, LockParameter*);
Four SM_DestroyObjectWithoutScan(Four, FileID*, ObjectID*, LockParameter*, LockParameter*);
Four SM_FetchObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, char*, LockParameter*, LockParameter*);
Four SM_UpdateObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, void*, Four, LockParameter*, LockParameter*);
Four SM_WriteObjectRedoOnlyWithoutScan(Four, FileID*, ObjectID*, Four, Four, char*, LockParameter*, LockParameter*);
Four SM_InsertMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_SetCfgParam(Four, char*, char*);
char* SM_GetCfgParam(Four, char*);

Four SM_InitIndexBulkLoad(Four, VolID, KeyDesc*);
Four SM_NextIndexBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four SM_FinalIndexBulkLoad(Four, Four, IndexID*, Two, Two, LockParameter*);

Four SM_InitSortedIndexBulkLoad(Four, IndexID*, KeyDesc*, Two, Two, LockParameter*);
Four SM_NextSortedIndexBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four SM_FinalSortedIndexBulkLoad(Four, Four);

Four SM_InitDataFileBulkLoad(Four, VolID, FileID*, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, Boolean, Two, Two, PageID*, LockParameter*); 
Four SM_NextDataFileBulkLoad(Four, Four, char*, Four, Boolean, ObjectID*);
Four SM_NextDataFileBulkLoadWriteLOT(Four, Four, Four, Four, char*, Boolean, ObjectID*);
Four SM_FinalDataFileBulkLoad(Four, Four);

Four SM_GetKeyOIDSortStream(VolID, FileID*, Two, SortKeyDesc*, GetKeyAttrsFuncPtr_T, void*, PageID*, Four*); 

Four SM_InitIndexBulkInsert(Four, VolID, KeyDesc *);
Four SM_NextIndexBulkInsert(Four, Four, KeyValue*, ObjectID*);
Four SM_FinalIndexBulkInsert(Four, Four, IndexID*, KeyDesc*, LockParameter *);

Four SM_CreateCounter(Four, Four, char*, Four, CounterID*);
Four SM_DestroyCounter(Four, Four, char*);
Four SM_GetCounterId(Four, Four, char*, CounterID*);
Four SM_SetCounter(Four, Four, CounterID*, Four);
Four SM_ReadCounter(Four, Four, CounterID*, Four*);
Four SM_GetCounterValues(Four, Four, CounterID*, Four, Four*);

#include "Util_Sort.h"

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

#include "Util_varArray.h"

/* function prototype */
Four Util_initVarArray(Four, VarArray*, Four, Four);
Four Util_doublesizeVarArray(Four, VarArray*, Four);
Four Util_finalVarArray(Four, VarArray*);

/* APIs */
#define SM_initVarArray(varArray, size, number) Util_initVarArray(varArray, size, number)
#define SM_doublesizeVarArray(varArray, size)   Util_doublesizeVarArray(varArrary, size)
#define SM_finalVarArray(varArray)              Util_finalVarArray(varArray)

#ifdef DBLOCK
Four SM_GetVolumeLock(Four, Four volNo, LockMode lockMode);
Four SM_ReleaseVolumeLock(Four, Four volNo);
Four SM_ReleaseAllVolumeLock(Four);
Four SM_SetUseShareLockFlag(Four, Boolean useSharedLockFlag);
Four SM_CheckVolumeLock(Four, Four volNo, LockMode lockMode);
Four SM_GetLogVolumeLock(Four);
Four SM_ReleaseLogVolumeLock(Four);
#endif

Four SM_EnterTwoPhaseCommit(Four, XactID*, GlobalXactID*);
Four SM_GetNumberOfPreparedTransactions(Four, Four*);
Four SM_GetPreparedTransactions(Four, Four, GlobalXactID[]);
Four SM_PrepareTransaction(Four, XactID*);
Four SM_RecoverTwoPhaseCommit(Four, GlobalXactID*, XactID*);
Four SM_IsReadOnlyTransaction(Four, XactID*, Boolean*);

#endif /* _SM_H_ */
