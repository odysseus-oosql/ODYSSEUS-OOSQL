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
#ifndef	__LOG_H__
#define	__LOG_H__

#include <assert.h>
#include "xactTable.h"
#include "dirtyPageTable.h"
#include "common.h"
#include "LM.h"

/*
 * Constants
 */
#define LOG_MASTER_PAGE_NO 					0
#define LOG_MAX_IMAGE_SIZE 					TRAINSIZE
#define LOG_MAX_DEALLOC_ELEMENTS_PER_LOG_RECORD 		(LOG_MAX_IMAGE_SIZE/sizeof(PageID))
#define LOG_MAX_DEALLOC_SEGMENT_ID_ELEMENTS_PER_LOG_RECORD 	(LOG_MAX_IMAGE_SIZE/sizeof(SegmentID_T))
#define LOG_MAX_DIRTY_PAGES_PER_LOG_RECORD 			(LOG_MAX_IMAGE_SIZE/sizeof(DirtyPage_T))
#define LOG_MAX_LOCKS_OF_PREPARED_XACT_PER_LOG_RECORD 		(LOG_MAX_IMAGE_SIZE/sizeof(LOG_Image_LM_LocksOfPreparedXact_T)) 

/*
 * Types of log records
 */
typedef enum LOG_Type_T_tag {
    LOG_TYPE_UPDATE=0x1,
    LOG_TYPE_COMPENSATION,
    LOG_TYPE_TRANSACTION,
    LOG_TYPE_CHECKPOINT,
    LOG_TYPE_VOLUME
} LOG_Type_T;


/*
 * Redo/Undo Types of log records
 */
typedef enum LOG_RedoUndo_T_tag {
    LOG_NO_REDO_UNDO=0x0,
    LOG_REDO_ONLY=0x1,
    LOG_UNDO_ONLY=0x2,
    LOG_REDO_UNDO=0x3
} LOG_RedoUndo_T;


/*
 * Type Definition for log data of log records
 */
/* LOG_Data_T is defined in common.h */



/*
 * LOG_LogRecInfo_T
 */
/* LOG_LogRecInfo_T is defined in common.h */


typedef struct LOG_Image_LOT_ReplaceInternalEntries_T_tag {
    Four start;                 /* start position */
    Four nNewEntries;           /* # of new entries */
    Four nOldEntries;           /* # of old entries */
} LOG_Image_LOT_ReplaceInternalEntries_T;

typedef struct LOG_Image_LOT_DeleteInternalEntries_T_tag {
    Four start;                 /* start position */
    Four nEntries;              /* # of deleted entries */
    Four nDeletedBytesBefore;   /* # of deleted bytes before deleted entris */
    Four nDeletedBytesAfter;    /* # of deleted bytes after deleted entries */
} LOG_Image_LOT_DeleteInternalEntries_T;

typedef struct LOG_Image_LOT_ModifyLeafData_T_tag {
    Four start;                 /* start position */
    Four length;                /* updated bytes */
    Four oldTotalLength;        /* length before update */
} LOG_Image_LOT_ModifyLeafData_T;

typedef struct LOG_Image_LOT_UpdateCountFields_T_tag {
    Four start;                 /* start position */
    Four delta;                 /* amounts changed (negative/positive) */
} LOG_Image_LOT_UpdateCountFields_T;


/*
 * LM
 */
typedef struct LOG_Image_LM_LocksOfPreparedXact_T_tag {

    TargetID		target;		/* lock object identifier */
    FileID		fileID;		/* file ID to which the target belongs */
    LockLevel		level;		/* L_OBJECT, L_PAGE, L_KEYVALUE, L_FLAT_OBJECT, L_FLAT_PAGE, L_FILE */
    LockMode		mode;		/* requested lock mode */
    LockDuration	duration;	/* requested lock duration */

} LOG_Image_LM_LocksOfPreparedXact_T;


/*
 * OM
 */
typedef union LOG_Image_OM_ChangeObject_T_tag {
    /* CHANGE_OBJECT_TYPE_NONE */
    struct {
        One type;                /* object header update type */
        Two slotNo;              /* slot no of the object */
        Two deltaOfDataAreaSize; /* delta between old size and new size of data area */
    } any;

    /* CHANGE_OBJECT_TYPE_PROPERTIES */
    struct {
        One type;                /* object header update type */
        Two slotNo;              /* slot no of the object */
        Two deltaOfDataAreaSize; /* delta between old size and new size of data area */
        Two propertiesXor;       /* bitwise xor value of old and new properties */
    } p;

    /* CHANGE_OBJECT_TYPE_PROPERTIES_AND_LENGTH */
    struct {
        One type;                /* object header update type */
        Two slotNo;              /* slot no of the object */
        Two deltaOfDataAreaSize; /* delta between old size and new size of data area */
        Two propertiesXor;       /* bitwise xor value of old and new properties */
        Four deltaInLengthField; /* difference between old length and new length */
    } p_l;
} LOG_Image_OM_ChangeObject_T;

/* object header updated type */
#define CHANGE_OBJECT_TYPE_NONE                  0
#define CHANGE_OBJECT_TYPE_PROPERTIES            1
#define CHANGE_OBJECT_TYPE_PROPERTIES_AND_LENGTH 2


typedef struct LOG_Image_OM_GetUniques_T_tag {
    Unique unique;              /* start value of a sequece of allocated values */
    Unique uniqueLimit;         /* last value of a sequence of allocated values */
} LOG_Image_OM_GetUniques_T;

typedef struct LOG_Image_OM_CreateSmallObject_T_tag {
    Two    slotNo;                 /* slot no of the object id */
    One    addSlotFlag;            /* TRUE if a new slot is created */
    Unique unique;                 /* unique number allocated to the new object */
} LOG_Image_OM_CreateSmallObject_T;

typedef struct LOG_Image_OM_ObjectInPage_T_tag {
    Two    slotNo;              /* slot no of the object id */
    Unique unique;              /* unique value of the object */
} LOG_Image_OM_ObjectInPage_T;


typedef struct LOG_Image_OM_ObjDataInPage_T_tag {
    Two slotNo;                 /* slot no of the object id */
    Two start;                  /* starting offset of the data */
    Two length;                 /* # of bytes */
} LOG_Image_OM_ObjDataInPage_T;

typedef struct LOG_Image_OM_ModifyObjHdrLen_T_tag {
    Two slotNo;                 /* slot no of the object id */
    Two length;                 /* incremental or decremental of the length */
} LOG_Image_OM_ModifyObjHdrLen_T;


/*
 * RDsM
 */
typedef struct LOG_Image_RDsM_GetUniqueInfo_T_tag {
    Four   partitionNo;         /* partition number */
    Unique uniqueNum;           /* unique number */
} LOG_Image_RDsM_GetUniqueInfo_T;

typedef struct LOG_Image_RDsM_MountedVol_T_tag {
    Two volNo;
    Two nDevices;
} LOG_Image_RDsM_MountedVol_T;

typedef struct LOG_Image_RDsM_UpdateBitmap_T_tag {
    UFour start; 
    UFour nBits;
} LOG_Image_RDsM_UpdateBitmap_T;

typedef struct LOG_Image_RDsM_Modify_ExtentLink_T_tag {
    Four prevExt;
    Four nextExt;
} LOG_Image_RDsM_Modify_ExtentLink_T;

typedef struct LOG_Image_RDsM_Modify_FreeExtentListHeader_T_tag {
    Four freeExtentNo;
    Four differenceOfNumOfFreeExtent;
} LOG_Image_RDsM_Modify_FreeExtentListHeader_T;


/*
 * BtM
 */
typedef struct LOG_Image_BtM_ChangeLeafEntry_T_tag {
    Two slotNo;
    Two deltaOfObjectArrayAreaSize;
    Two deltaInNumOfObjects;
} LOG_Image_BtM_ChangeLeafEntry_T;

typedef struct LOG_Image_BtM_InitInternalPage_T_tag {
    IndexID     iid;
    Boolean     rootFlag;
    ShortPageID p0;
    One         height;
} LOG_Image_BtM_InitInternalPage_T;

typedef struct LOG_Image_BtM_InitLeafPage_T_tag {
    IndexID     iid;
    Boolean     rootFlag;
    ShortPageID prevPage;
    ShortPageID nextPage;
} LOG_Image_BtM_InitLeafPage_T;

#define BTREE_INIT_LEAF_UPDATE_TYPE_ROOT_DELETE 0

typedef One LOG_Image_BtM_InitLeafPage_UpdateType_T;

typedef struct LOG_Image_BtM_InitOverflowPage_T_tag {
    IndexID     iid;
    ShortPageID prevPage;
    ShortPageID nextPage;
} LOG_Image_BtM_InitOverflowPage_T;

typedef struct LOG_Image_BtM_SpecifyEntries_T_tag {
    Two startSlotNo;            /* starting point */
    Two nEntries;               /* # of entries */
} LOG_Image_BtM_SpecifyEntries_T;

typedef struct LOG_Image_BtM_OidInLeafEntry_T_tag {
    Two      slotNo;
    Two      oidArrayElemNo;
    ObjectID oid;
} LOG_Image_BtM_OidInLeafEntry_T;

typedef struct LOG_Image_BtM_ModifyChainLink_T_tag {
    PageNo oldLink;
    PageNo newLink;
} LOG_Image_BtM_ModifyChainLink_T;

typedef struct LOG_Image_BtM_OidsInOverflow_T_tag {
    Two startOidArrayElemNo;
    Two nObjects;
} LOG_Image_BtM_OidsInOverflow_T;

typedef struct LOG_Image_BtM_OidInOverflow_T_tag {
    Two      oidArrayElemNo;
    ObjectID oid;
} LOG_Image_BtM_OidInOverflow_T;

typedef struct LOG_Image_BtM_ConvertToOverflow_T_tag {
    Two    slotNo;
    PageNo ovPageNo;
} LOG_Image_BtM_ConvertToOverflow_T;

typedef struct LOG_Image_BtM_IndexInfo_T_tag {
    IndexID  iid;
    ObjectID catEntry;
} LOG_Image_BtM_IndexInfo_T;


/*
 * MLGF
 */
typedef struct LOG_Image_MLGF_ChangeLeafEntry_T_tag {
    Two entryNo;
    Two deltaOfObjectArrayAreaSize;
    Two deltaInNumOfObjects;
} LOG_Image_MLGF_ChangeLeafEntry_T;

typedef struct LOG_Image_MLGF_InitDirectoryPage_T_tag {
    IndexID iid;                    /* index id */
    One     height;                 /* height of this page */
    One     rootFlag;               /* this page is the root page? */
    One     nKeys;                  /* # of keys of MLGF index */
} LOG_Image_MLGF_InitDirectoryPage_T;

typedef struct LOG_Image_MLGF_InitLeafPage_T_tag {
    IndexID iid;                    /* index id */
    One     nKeys;                  /* # of keys of MLGF index */
    Two     extraDataLen;           /* extra data length */
} LOG_Image_MLGF_InitLeafPage_T;

typedef struct LOG_Image_MLGF_InitOverflowPage_T_tag {
    IndexID     iid;                    /* index id */
    ShortPageID prevPage;
    ShortPageID nextPage;
    Two         extraDataLen;           /* extra data length */
} LOG_Image_MLGF_InitOverflowPage_T;

typedef struct LOG_Image_MLGF_LogicalUndoIno_T_tag {
    IndexID      iid;		/* index id */
    ObjectID     catEntry;	
    MLGF_KeyDesc kdesc;         /* key descriptor */
} LOG_Image_MLGF_LogicalUndoInfo_T;

typedef struct LOG_Image_MLGF_ObjectInLeafEntry_T_tag {
    Two entryNo;                /* entry no */
    Two objArrayElemNo;         /* element no in object array */
} LOG_Image_MLGF_ObjectInLeafEntry_T;

typedef struct LOG_Image_MLGF_ObjecsInOverflow_T_tag {
    Two startObjArrayElemNo;
    Two nObjects;
} LOG_Image_MLGF_ObjectsInOverflow_T;

typedef struct LOG_Image_MLGF_SpecifyEntries_T_tag {
    Two startEntryNo;
    Two nEntries;
} LOG_Image_MLGF_SpecifyEntries_T;


/*
 * Log Master
 */
typedef struct LOG_LogMaster_T_tag {

    Four volNo;                 /* volume number */
    Four logMasterPageNo;       /* number of the page in the log volume where logMaster is stored */
    Four firstPageNoOfFirstLogFile; /* the first page number of the first log file */
    Four headWrapCount;         /* wrapCount of the most recently used page */
    Four headPageNo;            /* page number of the most recently used page */
    Four tailWrapCount;         /* wrapCount of the leastly used page not archived into a tape */
    Four tailPageNo;            /* page number of the leastly used page not archived into a tape */
    Four nLogFiles;             /* number of log files */
    Four numPages;              /* number of pages in the log file */
    Four numBytes;              /* number of bytes in the log file */
    Four numBytesRemained;      /* number of bytes remained */
    Four logRecordCount;        /* # of log records written after the latest checkpoint log record */
    Lsn_T nextLsn;              /* LSN of the log record to be written next to the last valid log record */
    Lsn_T checkpointLsn;        /* LSN of the recent checkpoint log record */

} LOG_LogMaster_T;


/*
 * Log Master Page
 */
typedef struct log_LogMasterPageHdr_T_tag {
    PageID pid;                 /* page id of this page */
    Four   flags;
    Four   reserved;
    Lsn_T  lsn;                 /* page lsn */
} log_LogMasterPageHdr_T;

typedef struct log_LogMasterPage_T_tag {
    log_LogMasterPageHdr_T hdr;
    LOG_LogMaster_T        master;     /* log master */
    char                   dummy[PAGESIZE-sizeof(log_LogMasterPageHdr_T)-sizeof(LOG_LogMaster_T)];
} log_LogMasterPage_T;


/*
 * Log Page
 */
typedef struct log_LogPageHdr_T_tag {
    PageID pid;                 /* page id of this page */
    Four   flags;
    Four   reserved;
    Lsn_T  lsn;                 /* page lsn */
} log_LogPageHdr_T;

typedef struct log_LogPage_T_tag {
    log_LogPageHdr_T hdr;
    char             data[PAGESIZE-sizeof(log_LogPageHdr_T)];
} log_LogPage_T;

#define LOG_GET_SPACE_SIZE_FROM_OFFSET_IN_PAGE(offset) \
(PAGESIZE-sizeof(log_LogPageHdr_T)-offset)

/* get the page no containing the byte corresponding to the given lsn */
#define LOG_GET_PAGE_NO_FROM_LSN_OFFSET(offset) \
(offset / (PAGESIZE-sizeof(log_LogPageHdr_T)))

/* get the offset within a page corresponding to the given lsn offset */
#define LOG_GET_PAGE_OFFSET_IN_PAGE_FROM_LSN_OFFSET(offset) \
(offset % (PAGESIZE-sizeof(log_LogPageHdr_T)))

/* get the lsn offset corresponding to first byte of the given page */
#define LOG_GET_LSN_OFFSET_FROM_PAGE_NO(pageNo) \
((pageNo) * (PAGESIZE-sizeof(log_LogPageHdr_T)))

/*
 * Macros
 */
#define LOG_IS_LOGGING_TURNED_ON(_handle)  (perThreadTable[_handle].tmDS.TM_myXactDesc.logFlag)
#define LOG_FILL_LOGRECINFO_0(logRecInfo, _xactId, _type, _action, _redoUndo, _pid, _lastLsn, _undoNextLsn) \
BEGIN_MACRO \
(logRecInfo).xactId = (_xactId); \
(logRecInfo).type = (_type); \
(logRecInfo).action = (_action); \
(logRecInfo).redoUndo = (_redoUndo); \
(logRecInfo).pid = (_pid); \
(logRecInfo).prevLsn = (_lastLsn); \
(logRecInfo).undoNextLsn = (_undoNextLsn); \
(logRecInfo).nImages = 0; \
END_MACRO

#define LOG_FILL_LOGRECINFO_1(logRecInfo, _xactId, _type, _action, _redoUndo, _pid, _lastLsn, _undoNextLsn, _imageSize1, _imageData1) \
BEGIN_MACRO \
(logRecInfo).xactId = (_xactId); \
(logRecInfo).type = (_type); \
(logRecInfo).action = (_action); \
(logRecInfo).redoUndo = (_redoUndo); \
(logRecInfo).pid = (_pid); \
(logRecInfo).prevLsn = (_lastLsn); \
(logRecInfo).undoNextLsn = (_undoNextLsn); \
(logRecInfo).nImages = 1; \
(logRecInfo).imageSize[0] = (_imageSize1); \
(logRecInfo).imageData[0] = (_imageData1); \
END_MACRO

#define LOG_FILL_LOGRECINFO_2(logRecInfo, _xactId, _type, _action, _redoUndo, _pid, _lastLsn, _undoNextLsn, _imageSize1, _imageData1, _imageSize2, _imageData2) \
BEGIN_MACRO \
(logRecInfo).xactId = (_xactId); \
(logRecInfo).type = (_type); \
(logRecInfo).action = (_action); \
(logRecInfo).redoUndo = (_redoUndo); \
(logRecInfo).pid = (_pid); \
(logRecInfo).prevLsn = (_lastLsn); \
(logRecInfo).undoNextLsn = (_undoNextLsn); \
(logRecInfo).nImages = 2; \
(logRecInfo).imageSize[0] = (_imageSize1); \
(logRecInfo).imageData[0] = (_imageData1); \
(logRecInfo).imageSize[1] = (_imageSize2); \
(logRecInfo).imageData[1] = (_imageData2); \
END_MACRO

#define LOG_FILL_LOGRECINFO_3(logRecInfo, _xactId, _type, _action, _redoUndo, _pid, _lastLsn, _undoNextLsn, _imageSize1, _imageData1, _imageSize2, _imageData2, _imageSize3, _imageData3) \
BEGIN_MACRO \
(logRecInfo).xactId = (_xactId); \
(logRecInfo).type = (_type); \
(logRecInfo).action = (_action); \
(logRecInfo).redoUndo = (_redoUndo); \
(logRecInfo).pid = (_pid); \
(logRecInfo).prevLsn = (_lastLsn); \
(logRecInfo).undoNextLsn = (_undoNextLsn); \
(logRecInfo).nImages = 3; \
(logRecInfo).imageSize[0] = (_imageSize1); \
(logRecInfo).imageData[0] = (_imageData1); \
(logRecInfo).imageSize[1] = (_imageSize2); \
(logRecInfo).imageData[1] = (_imageData2); \
(logRecInfo).imageSize[2] = (_imageSize3); \
(logRecInfo).imageData[2] = (_imageData3); \
END_MACRO

#define LOG_FILL_LOGRECINFO_4(logRecInfo, _xactId, _type, _action, _redoUndo, _pid, _lastLsn, _undoNextLsn, _imageSize1, _imageData1, _imageSize2, _imageData2, _imageSize3, _imageData3, _imageSize4, _imageData4) \
BEGIN_MACRO \
(logRecInfo).xactId = (_xactId); \
(logRecInfo).type = (_type); \
(logRecInfo).action = (_action); \
(logRecInfo).redoUndo = (_redoUndo); \
(logRecInfo).pid = (_pid); \
(logRecInfo).prevLsn = (_lastLsn); \
(logRecInfo).undoNextLsn = (_undoNextLsn); \
(logRecInfo).nImages = 4; \
(logRecInfo).imageSize[0] = (_imageSize1); \
(logRecInfo).imageData[0] = (_imageData1); \
(logRecInfo).imageSize[1] = (_imageSize2); \
(logRecInfo).imageData[1] = (_imageData2); \
(logRecInfo).imageSize[2] = (_imageSize3); \
(logRecInfo).imageData[2] = (_imageData3); \
(logRecInfo).imageSize[3] = (_imageSize4); \
(logRecInfo).imageData[3] = (_imageData4); \
END_MACRO

#define LOG_INIT_LOG_MASTER_PAGE(_apage,_pid) \
BEGIN_MACRO \
SET_PAGE_TYPE(_apage, LOG_MASTER_PAGE_TYPE); \
(_apage)->hdr.pid = _pid; \
END_MACRO

#define LOG_INIT_LOG_PAGE(_apage,_pid,_offset,_wrapCount) \
BEGIN_MACRO \
SET_PAGE_TYPE(_apage, LOG_PAGE_TYPE); \
(_apage)->hdr.pid = _pid; \
(_apage)->hdr.lsn.offset = _offset; \
(_apage)->hdr.lsn.wrapCount = _wrapCount; \
END_MACRO

#define LOG_INCREASE_LSN(_lsn, _n) \
BEGIN_MACRO \
assert((_lsn).offset + (_n) < LOG_LOGMASTER.numBytes); \
(_lsn).offset += (_n); \
END_MACRO


#define LOG_GET_DISTANCE_BTW_PAGES(w1,p1,w2,p2) \
 (((w1) - (w2))*(LOG_LOGMASTER.numPages) + (p1) - (p2))

#define LOG_GET_PHYSICAL_PAGENO(_logMaster, _wrapCount, _pageNo) \
 ( (_pageNo) + (_logMaster).firstPageNoOfFirstLogFile + (((_wrapCount) % (_logMaster).nLogFiles) * (_logMaster).numPages) )

/*
 * Log Record Table
 */
#include "BfM.h"                /* for buffer type definitions, which used in logRecTbl.h */
#include "logRecTbl.h"          /* need the type definition LOG_LogRecInfo_T */





/*-------------------- BEGIN OF Shared Memory Section -----------------------*/
/*
 * Shared Memory Data Structures
 */
typedef struct LOG_LogBufferTableEntry_T_tag {
    Four pageNo;                /* page number of the page whose contents are in the buffer page */
    Four wrapCount;             /* wrapcount of the page whose contents are in the buffer page */
} LOG_LogBufferTableEntry_T;

typedef struct LOG_LogBufferInfo_T_tag {
    Four tail;                  /* points to the first log buffer page not being reflected in disk */
    Four head;                  /* points to the currently used log buffer page */
} LOG_LogBufferInfo_T;


typedef struct {

    LATCH_TYPE 			latch_logHead;
    LATCH_TYPE 			latch_logTail;
    LATCH_TYPE 			latch_logFileSwitch;

    log_LogPage_T 		logBufferPage[NUM_WRITE_LOG_BUFS];
    LOG_LogBufferTableEntry_T 	logBufferTable[NUM_WRITE_LOG_BUFS];
    LOG_LogBufferInfo_T 	logBufferInfo;
    LOG_LogMaster_T 		logMaster;

} LOG_SHM;

extern LOG_SHM *log_shmPtr;
extern Four procIndex;

/*
 *	useful macros for handling LogBufferTableEntry and LogBufferInfo
 */

#define LOG_LATCH4HEAD       		(log_shmPtr->latch_logHead)
#define LOG_LATCH4TAIL       		(log_shmPtr->latch_logTail)
#define LOG_LATCH4LOGFILESWITCH    	(log_shmPtr->latch_logFileSwitch)

#define	LOG_LBI_TAIL			(log_shmPtr->logBufferInfo.tail)
#define	LOG_LBI_HEAD			(log_shmPtr->logBufferInfo.head)

#define	LOG_LBT_PAGENO(_index)		(log_shmPtr->logBufferTable[_index].pageNo)
#define	LOG_LBT_WRAPCOUNT(_index)	(log_shmPtr->logBufferTable[_index].wrapCount)

#define LOG_LOGMASTER	        	(log_shmPtr->logMaster)
#define LOG_LOGBUFFERPAGE		(log_shmPtr->logBufferPage)

/*-------------------- END OF Shared Memory Section -------------------------*/



extern LOG_LogRecTableEntry_T LOG_logRecTbl[];



/*
 * Internal Function Prototypes
 */
Four log_AllocLogBuffer(Four, Four, Four);
Four log_AllocPage(Four, Four*, Four*);
Four log_BufFinal(Four);
Four log_BufInit(Four);
Four log_FlushLogBuffers(Four, Four, Boolean);
Four log_GetAndFixBuffer(Four, Four, Four, log_LogPage_T**);
Four log_GetLogRecordLength(Four, LOG_LogRecInfo_T*);
Four log_ReadLogRecord(Four, Lsn_T*, LOG_LogRecInfo_T*);
Four log_UnfixBuffer(Four);
Four log_WriteLogRecord(Four, LOG_LogRecInfo_T*);


/*
 * External Function Prototypes
 */
Four LOG_CloseScan(Four);
Four LOG_GetCheckpointLsn(Four, Lsn_T*);
Four LOG_GetNextLogRecordLsn(Four, Lsn_T*);
Four LOG_FlushLogRecords(Four, Lsn_T*, Four);
Four LOG_InitLocalDS(Four);
Four LOG_InitSharedDS(Four);
Four LOG_FinalLocalDS(Four);
Four LOG_FinalSharedDS(Four);
Four LOG_InitLogVolume(Four, Four);
Four LOG_NextRecord(Four, Lsn_T*, LOG_LogRecInfo_T*, Four*);
Four LOG_OpenScan(Four, Lsn_T*);
Four LOG_OpenVolume(Four, Four);
Four LOG_ReadLogRecord(Four, Lsn_T*, LOG_LogRecInfo_T*, Four*);
Four LOG_SetCheckpointLsn(Four, Lsn_T*);
Four LOG_SetNextLogRecordLsn(Four, Lsn_T*);
Four LOG_SwitchLogFile(Four, Four); 
Four LOG_WriteLogRecord(Four, XactTableEntry_T*, LOG_LogRecInfo_T*, Lsn_T*, Four*);
Four LOG_PrintLogRecord(Four, Lsn_T*, LOG_LogRecInfo_T*);

#endif /* __LOG_H__ */
