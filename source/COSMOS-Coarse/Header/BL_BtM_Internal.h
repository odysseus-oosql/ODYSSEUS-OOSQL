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
#ifndef _BL_BTM_INTERNAL_H_
#define _BL_BTM_INTERNAL_H_


#include "Util_Sort.h"


/*@
 * Constant Definitions
 */
#define INITNUMOFLEVEL      5                               /* initial number of B+ tree depth */ 
#define NUMOFWINDOWPAGE     2                               /* number of window page in buffer */
#define NUMOFWRITEBUFFER    20                              /* number of write buffer */

#define MINPFF              (100*1/2)                       /* minimum page fill factor of B+ tree */
#define MAXPFF              100                             /* maximum page fill factor of B+ tree */



/*@
 * Type Definitions
 */

/*
** BlkLdBtreeInternal :
**  B+ tree internal window buffer for bulkload
*/ 
typedef struct {
    KeyValue                    iKey[NUMOFWINDOWPAGE];      /* array of first pointer's key value */ 
    BtreeInternal               iPage[NUMOFWINDOWPAGE];     /* array of internal page buffer */ 
    Four                        iCount;                     /* internal page counter in any level of B+ tree */
} BlkLdBtreeInternal;


/*
** BlkLdBtreeInternals :
**  B+ tree internal buffer for bulkload which is array of internal window buffer. 
**  One internal window buffer for each B+ tree level.
*/ 
typedef struct {
    VarArray                    iArray;                     /* variable array of internal window buffer */ 
    Four                        nLevel;                     /* maximum level of B+ tree index */
} BlkLdBtreeInternals;


/*
** BlkLdBtreeLeaf :
**  B+ tree leaf window buffer for bulkload
*/ 
typedef struct {
    BtreeLeaf                   lPage[NUMOFWINDOWPAGE];     /* array of leaf page buffer */
    Four                        lCount;                     /* leaf page counter in B+ tree */
} BlkLdBtreeLeaf;


/*
** BlkLdBtreeOverflow :
**  B+ tree overflow window buffer for bulkload.
*/ 
typedef struct {
    BtreeOverflow               oPage[NUMOFWINDOWPAGE];     /* array of overflow page buffer */
    Four                        oCount;                     /* overflow page counter in any overflow chain of B+ tree */ 
} BlkLdBtreeOverflow;


/*
** BlkLdBtreeWriteBuffer :
**  B+ tree write window buffer for bulkload.
*/ 
typedef struct {
    Four                        bufSize;                                /* maximum buffer size by page */
    Four                        startPageIdx;                           /* start page index in first extent */

    Four                        allocedBufCount;                        /* buffer counter for allocating */
    Four                        flushedBufCount;                        /* buffer counter for flushing */

    Four                        allocCount;                             /* alloc page counter */
    PageID                      *allocPageIdArray;                      /* array of allocated page id */

    Four                        flushCount[NUMOFWRITEBUFFER];           /* # of page not to be flushed in write buffer */
    PageID                      *flushPageIdArray[NUMOFWRITEBUFFER];    /* array of flushed page id */ 

    BtreePage                   *bufArray[NUMOFWRITEBUFFER];            /* array of write buffer page */
    char                        **bufPtrArray[NUMOFWRITEBUFFER];        /* array of write buffer page's pointer */
} BlkLdBtreeWriteBuffer;


/*
** BlkLdBtreeInfo :
**  B+ tree index information for bulkload.
*/ 
typedef struct {
    PhysicalFileID              pFid;                       /* physical file id of file in which B+ tree index is */ 
    PageID                      root;                       /* root page id of B+ tree index */
    Two                         sizeOfExt;                  /* # of pages per extent */
    Two                         eff;                        /* extent fill factor of B+ tree index */
    Two                         pff;                        /* page fill factor of B+ tree index */
    Boolean                     isTmp;                      /* flag which indicates B+ tree is temporary or not */
    Boolean                     isAppend;                   /* flag which indicates appending bulkload */
    KeyDesc                     kdesc;                      /* key descriptor of B+ tree index */
} BlkLdBtreeInfo;


/*
** BlkLdScanInfo :
**  B+ tree index scan information for append bulkload.
*/ 
typedef struct {
    KeyValue                    startKval;                  /* key value of start condition */
    Four                        startCompOp;                /* comparison operator of start condition */
    KeyValue                    stopKval;                   /* key value of stop condition */
    Four                        stopCompOp;                 /* comparison operator of stop condition */
    BtreeCursor                 currCursor;                 /* Current Btree Cursor */
    BtreeCursor                 nextCursor;                 /* Next Btree Cursor */
} BlkLdScanInfo;


typedef struct {
    Boolean                 isUsed;                 /* flag which indicates that this entry is used */
    BlkLdBtreeInfo          btmBlkLdblkldInfo;      /* Btree information used in Btree bulkloading */
    BlkLdBtreeInternals     btmBlkLdiBuffers;       /* bulkload internal window buffer */
    BlkLdBtreeLeaf          btmBlkLdlBuffer;        /* bulkload leaf window buffer */
    BlkLdBtreeOverflow      btmBlkLdoBuffer;        /* bulkload overflow window buffer */
    BtreeLeaf               btmBlkLdoidBuffer;      /* bulkload oid buffer */
    BlkLdBtreeWriteBuffer   btmBlkLdwBuffer;        /* bulkload write buffer */
    BlkLdScanInfo           btmBlkLdscanInfo;       /* Scan information used in B+ tree index append bulkloading */
    Boolean                 btmBlkLdoverflow;       /* flag which indicates bulkloading occur in overflow page */
    Boolean                 btmBlkLdisAppend;       /* flag which indicates append bulkloading */
} BtM_BlkLdTableEntry;

/*@
 * Global Variables 
 */


/*@
 * Macro Definitions
 */
#define BI_LIMIT(pff)   ((CONSTANT_CASTING_TYPE)(((PAGESIZE-BI_FIXED) * pff) / 100))
#define BI_USED(p)      ((p)->hdr.free + ((p)->hdr.nSlots-1)*((CONSTANT_CASTING_TYPE)sizeof(Two))) 

#define BL_LIMIT(pff)   ((CONSTANT_CASTING_TYPE)(((PAGESIZE-BL_FIXED) * pff) / 100))
#define BL_USED(p)      ((p)->hdr.free + ((p)->hdr.nSlots-1)*((CONSTANT_CASTING_TYPE)sizeof(Two))) 

#define BO_LIMIT(pff)   ((CONSTANT_CASTING_TYPE)(((PAGESIZE-BO_FIXED-BO_DUMMY) * pff) / 100))
#define BO_USED(p)      (((p)->hdr.nObjects) * OBJECTID_SIZE)


#define IARRAY(iBuffers)  ((BlkLdBtreeInternal *)((iBuffers).iArray.ptr))



/*@
 * Function Prototypes
 */
/*
** B+tree Manager Internal function prototypes
*/
Four btm_BlkLdGetInfo(Four, Four, ObjectID*, PageID*, KeyDesc*, Two, Two);
Four btm_BlkLdAllocPage(Four, Four, PageID*);
Four btm_BlkLdAllocPages(Four, Four);
Four btm_BlkLdFlushPage(Four, Four, BtreePage*);
Four btm_BlkLdFlushPages(Four, Four, Four);

Four btm_BlkLdInitInternal(Four, Four, BtreeInternal*, Boolean);
Four btm_BlkLdInitLeaf(Four, Four, BtreeLeaf*, Boolean);
Four btm_BlkLdInitOverflow(Four, Four, BtreeOverflow*, Boolean);

Four btm_BlkLdInitInternalBuffer(Four, Four);
Four btm_BlkLdInitLeafBuffer(Four, Four);
Four btm_BlkLdInitOverflowBuffer(Four, Four);
Four btm_BlkLdInitWriteBuffer(Four, Four);

Four btm_BlkLdDoubleSizeInternal(Four, Four);

Four btm_BlkLdFinalInternalBuffer(Four, Four);
Four btm_BlkLdFinalWriteBuffer(Four, Four);


Four btm_BlkLdInsertLeaf(Four, Four, KeyValue*, ObjectID*);
Four btm_BlkLdInsertInternal(Four, Four, InternalItem*, Four);
Four btm_BlkLdInsertOverflow(Four, Four, ObjectID*);
Four btm_BlkLdCreateOverflow(Four, Four);

Four btm_BlkLdSplitInternal(Four, Four, Four);
Four btm_BlkLdSplitLeaf(Four, Four);
Four btm_BlkLdSplitOverflow(Four, Four);

Four btm_BlkLdEndInternal(Four, Four);
Four btm_BlkLdEndLeaf(Four, Four);
Four btm_BlkLdEndOverflow(Four, Four);
Four btm_BlkLdEndOidBuffer(Four, Four);
Four btm_BlkLdEndWriteBuffer(Four, Four);
Four btm_BlkLdFreeIndex(Four, Four, Pool*, DeallocListElem*);


/*
** B+tree Manager Interface function prototypes
*/
Four BtM_IsAppendBulkload (Four, PageID*, Boolean*);

Four BtM_BulkLoad(Four, ObjectID*, PageID *, Four, Two, Two);
Four BtM_InitSortedBulkLoad(Four, ObjectID*, PageID*, KeyDesc*, Two, Two);
Four BtM_NextSortedBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four BtM_FinalSortedBulkLoad(Four, Four);

Four BtM_AppendBulkLoad(Four, ObjectID*, PageID *, Four, Two, Two, Pool*, DeallocListElem*);
Four BtM_InitSortedAppendBulkLoad(Four, ObjectID*, PageID*, KeyDesc*, Two, Two);
Four BtM_NextSortedAppendBulkLoad(Four, Four, KeyValue*, ObjectID*);
Four BtM_FinalSortedAppendBulkLoad(Four, Four, Pool*, DeallocListElem*);



#endif /* _BL_BTM_INTERNAL_H_ */
