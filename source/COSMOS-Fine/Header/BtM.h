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
#ifndef _BTM_H_
#define _BTM_H_


#include "Util_pool.h"
#include "xactTable.h"
#include "BfM.h"


#define BTM_MAXDEPTH    30

/* special height values */
#define BTM_OVERFLOW_HEIGHT -1
#define BTM_FREEPAGE_HEIGHT -2


#define OBJECTID_SIZE   sizeof(ObjectID)


/*
 * Comparison result
 */
#define EQUAL 0
#define GREAT 1
#define LESS  2

/*
 * Btree Operations
 */
#define BTM_FETCH 0
#define BTM_INSERT 1
#define BTM_DELETE 2

/*
 * Return Values
 */
#define BTM_RETRAVERSE		1
#define BTM_NOSPACE		2
#define BTM_EMPTYPAGE  		3
#define BTM_EMPTYROOTPAGE 	4
#define BTM_DUPLICATEDKEY 	10
#define BTM_NOTFOUND   		11
#define BTM_FOUND      		12
#define BTM_RELATCHED  		13
#define BTM_DEADLOCK            14 

/*
 * Boundary Slot No
 */
#define EOI_SLOTNO	-1      /* end of index slot no */
#define BOI_SLOTNO	-2      /* begin of index slot no */

/*************************************************************
 * The structure of Btree Pages - Internal / Leaf / Overflow *
 *************************************************************/

/* For bit masking of status bits */
#define BTM_SM_BIT     0x1
#define BTM_DELETE_BIT 0x2

/*
** BtreeAny Page:
**  All pages in B+ tree files have some common fields in the same place.
**  Any Page describe the common fields.
*/
typedef struct BtreeAnyPageHdr_T_tag {
    PageID 	pid;                 	/* page id of this page */
    Four 	pageFlags;             	/* flags for page characteristics */
    Four 	pageReserved;          	/* reserved space for page characteristics */
    Lsn_T 	lsn;                  	/* page lsn */
    Four 	logRecLen;             	/* log record length */
    IndexID 	iid;                	/* Index Identifier */
    One		type;                   /* Internal, Leaf, or Overflow */
    One 	height;                 /* at which the node is located */
    One 	statusBits;             /* SM_BIT, DELETE_BIT */
} BtreeAnyPageHdr_T;

#define BANY_FIXED (sizeof(BtreeAnyPageHdr_T))

typedef struct { /* Any Page */
    BtreeAnyPageHdr_T 	hdr;      			/* page header */
    char		data[PAGESIZE-BANY_FIXED]; 	/* data area */
} BtreeAny;


/*
** BtreeInternal Page:
**  Page for the internal node of B+ tree.
*/
typedef struct BtreeInternalPageHdr_T_tag {
    PageID 		pid;                 	/* page id of this page */
    Four 		pageFlags;             	/* flags for page characteristics */
    Four 		pageReserved;         	/* reserved space for page characteristics */
    Lsn_T 		lsn;                  	/* page lsn */
    Four 		logRecLen;             	/* log record length */
    IndexID 		iid;                	/* Index Identifier */
    One			type;                   /* Internal, Leaf, or Overflow */
    One 		height;                 /* at which the node is located */
    One 		statusBits;             /* smBit */
    Two			nSlots;                 /* # of entries in this page */
    Two			free;                   /* starting point of the free space */
    Two 		unused;                 /* number of unused bytes which are not */
						/* part of the contiguous freespace */
    ShortPageID 	p0;			/* the first pointer */
} BtreeInternalPageHdr_T;

#define BI_FIXED (sizeof(BtreeInternalPageHdr_T) + sizeof(Two))

typedef struct {   /* Internal (Nonleaf) Page */
    BtreeInternalPageHdr_T 	hdr;     			/* internal page header */
    char			data[PAGESIZE-BI_FIXED]; 	/* data area */
    Two				slot[1];                	/* the first slot */
} BtreeInternal;



/* free space of BI; 'f' is a pointer to a buffer of an internal page. */
#define BI_FREE(p)    ((p)->hdr.unused + BI_CFREE(p))
#define BI_CFREE(p) \
(PAGESIZE - BI_FIXED - (p)->hdr.free - ((p)->hdr.nSlots-1)*((Four)sizeof(Two))) 
#define BI_FULL         ((Four)(PAGESIZE-BI_FIXED))
#define BI_HALF         ((Four)(BI_FULL/2))


/*
** BtreeLeaf:
**  Page for the leaf node of a B+ tree
*/
typedef struct BtreeLeafPageHdr_T_tag {
    PageID 		pid;                 	/* page id of this page */
    Four 		pageFlags;             	/* flags for page characteristics */
    Four 		pageReserved;         	/* reserved space for page characteristics */
    Lsn_T 		lsn;                  	/* page lsn */
    Four 		logRecLen;             	/* log record length */
    IndexID 		iid;                	/* Index Identifier */
    One			type;                   /* Internal, Leaf, or Overflow */
    One			height;                 /* 0 if leaf page */
    One			statusBits;             /* smBit, deleteBit */
    Two			nSlots;                 /* # of entries in this page */
    Two			free;                   /* starting point of the free space */
    Two			unused;                 /* number of unused bytes which are
                                   		   not part of the contiguous freespace */
    ShortPageID 	prevPage;       	/* Previous page */
    ShortPageID 	nextPage;       	/* Next page */
} BtreeLeafPageHdr_T;

#define BL_FIXED  (sizeof(BtreeLeafPageHdr_T) + sizeof(Two))

typedef struct {   /* Leaf Page */
    BtreeLeafPageHdr_T 		hdr;     			/* btree leaf page header */
    char			data[PAGESIZE-BL_FIXED]; 	/* data area */
    Two				slot[1];                	/* the first slot */
} BtreeLeaf;

/* free space of BL; 'p' is a pointer to a buffer of a leaf page. */
#define BL_FREE(p)    ((p)->hdr.unused + BL_CFREE(p))
#define BL_CFREE(p) \
(PAGESIZE - BL_FIXED - (p)->hdr.free - ((p)->hdr.nSlots-1)*((Four)sizeof(Two)))
#define BL_HALF         ((Four)((PAGESIZE-BL_FIXED)/2))
#define OVERFLOW_SPLIT  ((Four)(PAGESIZE-BL_FIXED)/3)
#define OVERFLOW_MERGE  ((Four)(PAGESIZE-BL_FIXED)/4)


/*
** BteeOverflow:
**  overflow page of a B+ tree
*/
typedef struct BtreeOverflowPageHdr_T_tag {
    PageID 		pid;                 	/* page id of this page */
    Four 		pageFlags;             	/* flags for page characteristics */
    Four 		pageReserved;          	/* reserved space for page characteristics */
    Lsn_T 		lsn;                  	/* page lsn */
    Four 		logRecLen;             	/* log record length */
    IndexID 		iid;                	/* Index Identifier */
    One			type;                   /* Internal, Leaf, or Overflow */
    One			height;                 /* -1 if Overflow */
    Two			nObjects;               /* # of ObjectIDs in this page */
    ShortPageID 	nextPage;       	/* Next Page */
    ShortPageID 	prevPage;       	/* Previous Page */
} BtreeOverflowPageHdr_T;

#define BO_FIXED  (sizeof(BtreeOverflowPageHdr_T))

#define BO_MAXOBJECTIDS ((PAGESIZE-BO_FIXED)/sizeof(ObjectID))
#define BO_DUMMY (PAGESIZE-BO_FIXED-BO_MAXOBJECTIDS*sizeof(ObjectID))

typedef struct {   /* Overflow page */
    BtreeOverflowPageHdr_T 	hdr; 			/* btree overflow page */
    ObjectID 			oid[BO_MAXOBJECTIDS]; 	/* ObjectID area */
} BtreeOverflow;

#define NO_OF_OBJECTS		BO_MAXOBJECTIDS
#define HALF_OF_OBJECTS		((Four)(NO_OF_OBJECTS/2))
#define A_THIRD_OF_OBJECTS	((Four)(NO_OF_OBJECTS/3))
#define A_FOURTH_OF_OBJECTS	((Four)(NO_OF_OBJECTS/4))



/*
** BtreePage:
**  Page type contains all page types
*/
typedef union {
    /*
     * Object Manager & B+ Tree Manager Page Types
     */
    BtreeAny      	any;		/* btree any page */
    BtreeInternal 	bi;		/* btree internal page */
    BtreeLeaf     	bl;		/* btree leaf page */
    BtreeOverflow 	bo;		/* btree overflow page */
} BtreePage;

/* Btree Page Type */
#define ROOT        0x01
#define INTERNAL    0x02
#define LEAF        0x04
#define OVERFLOW    0x08


/****************************************************************
 * Entry Types of a B+ tree
 ****************************************************************/

/*
 * The data types 'btm_InternalEntry' and 'btm_LeafEntry' are used to access
 * the entries directly in the buffer of the page.
 */

/* Data type of Internal Entry */
typedef struct {
    ShortPageID 	spid;			/* pointer to the child page */
						/* The child has key values greater than or */
						/* equal to 'kval' of this entry. */
    /* 'klen' and 'kval' should be attached in this order */
    /* to cast this variables the type KeyVlaue. */
    Two  		klen;			/* key length */
    char 		kval[MAXKEYLEN];       	/* key value */
} btm_InternalEntry;

/* Data type of Leaf Entry */
#define BTM_LEAFENTRY_FIXED (sizeof(Two)+sizeof(Two))

typedef struct {
    Two nObjects;		/* # of ObjectIDs */
    /* 'klen' and 'kval' should be attached in this order */
    /* to cast this variables the type KeyVlaue. */
    Two klen;			/* key length */
				/* From this point, alignment is counted. */
    char kval[MAXKEYLEN];       /* key value and (ObjectID array or overflow PageID) */
} btm_LeafEntry;

/*
** Type Definition for btm_TraversePath
*/
typedef struct {
    struct {
	Buffer_ACC_CB *bcbPtr;
	Lsn_T lsn;
    } elem[BTM_MAXDEPTH];	/* Remember the nodes' information */

    Two top;			/* points to the top element */
    LATCH_TYPE *treeLatchPtr;	/* Save Tree Latch Pointer */
} btm_TraversePath;


/*
 *  TreeLatchCell structure support tree latch in ARIES/IM
 */


#define BTM_TREELATCH_HASHSIZE 		99
#define BTM_TREELATCH_INITPOOLSIZE 	100
#define BTM_TREELATCH_INITCACHESIZE  	20

typedef struct treelatchcell btmTreeLatchCell;

struct treelatchcell {
    IndexID      				iid;		/* index id */
    LATCH_TYPE  				latch;		/* latch itself */
    Four					counter;	/* number of opened scan */
    LOGICAL_PTR_TYPE(struct treelatchcell *) 	next; 		/* pointer to hash chain */ 

};

typedef struct cache4treelatchcell btmCache4TreeLatchCell;

struct cache4treelatchcell {
    IndexID		iid;    	/* index id */
    btmTreeLatchCell 	*tLatchCellPtr; /* Save Tree Latch Pointer */
};

/*
** Macros
*/

#define BTM_INTERNAL_ENTRY_LENGTH(_klen) \
(sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two)+(_klen)))

#define BTM_LEAF_ENTRY_LENGTH(_klen,_nObjects) \
(ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED + _klen) + (((_nObjects) <= 0) ? sizeof(ShortPageID):((_nObjects)*OBJECTID_SIZE)))

#define BTM_INSERT_SLOTS_IN_BTREE_PAGE(_aPage, _slotNo, _n) \
memmove(&(_aPage)->slot[-((_aPage)->hdr.nSlots-1+(_n))], &(_aPage)->slot[-((_aPage)->hdr.nSlots-1)], sizeof(Two)*((_aPage)->hdr.nSlots-(_slotNo)))

#define BTM_DELETE_SLOTS_IN_BTREE_PAGE(_aPage, _slotNo, _n) \
memmove(&(_aPage)->slot[-((_aPage)->hdr.nSlots-1-(_n))], &(_aPage)->slot[-((_aPage)->hdr.nSlots-1)], sizeof(Two)*((_aPage)->hdr.nSlots-(_n)-(_slotNo)))

#define BTM_WRITE_SLOTS_IN_BTREE_PAGE(_aPage, _slotNo, _n, _slots) \
memcpy(&(_aPage)->slot[-((_slotNo)+(_n)-1)], (_slots), sizeof(Two)*(_n))

#define BTM_DELETE_OIDS_SPACE_FROM_OID_ARRAY(_array, _total, _idx, _n) \
memmove((_array) + (_idx), (_array) + (_idx) + (_n), ((_total)-(_idx)-(_n)) * OBJECTID_SIZE)

#define BTM_INSERT_OIDS_SPACE_INTO_OID_ARRAY(_array, _total, _idx, _n) \
memmove((_array) + (_idx) + (_n), (_array) + (_idx), ((_total)-(_idx)) * OBJECTID_SIZE)

#define BTM_WRITE_OIDS_IN_OID_ARRAY(_array, _idx, _n, _oids) \
memcpy((_array)+(_idx), _oids, OBJECTID_SIZE*(_n))

/* Access of the B+ tree part in a catalog table SM_SYSINDEXES */
#define GET_PTR_TO_CATENTRY_FOR_BTREE(slotNo, catPage, catEntry) \
BEGIN_MACRO \
    Object *obj = (Object*)&(catPage->data[catPage->slot[-slotNo].offset]); \
    catEntry = ((sm_CatOverlayForSysIndexes*)obj->data);\
END_MACRO

#define BTM_INIT_INTERNAL_PAGE(_apage, _iid, _pid, _p0, _height, _root) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_apage, BTREE_PAGE_TYPE); \
    (_apage)->hdr.statusBits = 0; \
    (_apage)->hdr.height = (_height); \
    (_apage)->hdr.type = (_root) ? (INTERNAL | ROOT) : INTERNAL; \
    (_apage)->hdr.iid = (_iid); \
    (_apage)->hdr.pid = (_pid); \
    (_apage)->hdr.p0 = (_p0); \
    (_apage)->hdr.nSlots = 0; \
    (_apage)->hdr.free = 0; \
    (_apage)->hdr.unused = 0; \
END_MACRO

#define BTM_INIT_LEAF_PAGE(_apage, _iid, _pid, _prevPage, _nextPage, _root) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_apage, BTREE_PAGE_TYPE); \
    (_apage)->hdr.statusBits = 0; \
    (_apage)->hdr.type = (_root) ? (LEAF | ROOT) : (LEAF); \
    (_apage)->hdr.height = 0; \
    (_apage)->hdr.iid = (_iid);  \
    (_apage)->hdr.pid = (_pid); \
    (_apage)->hdr.prevPage = (_prevPage); \
    (_apage)->hdr.nextPage = (_nextPage); \
    (_apage)->hdr.nSlots = 0; \
    (_apage)->hdr.free = 0; \
    (_apage)->hdr.unused = 0; \
END_MACRO

#define BTM_INIT_OVERFLOW_PAGE(_apage, _iid, _pid, _prevPage, _nextPage) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_apage, BTREE_PAGE_TYPE); \
    (_apage)->hdr.type = OVERFLOW; \
    (_apage)->hdr.height = BTM_OVERFLOW_HEIGHT; \
    (_apage)->hdr.iid = (_iid); \
    (_apage)->hdr.pid = (_pid); \
    (_apage)->hdr.nObjects = 0; \
    (_apage)->hdr.prevPage = (_prevPage); \
    (_apage)->hdr.nextPage = (_nextPage); \
END_MACRO

#define BTM_CACHE4TREELATCH(_handle) ((btmCache4TreeLatchCell *)(perThreadTable[_handle].btmDS.btmCache4TreeLatch.ptr))

/*
 * Logical-ID Mapping Table 
 */
typedef struct btm_IdMappingTableEntry_T_tag {
    IndexID 					logicalId;          	/* logical index id */
    PhysicalIndexID 				physicalId; 		/* physical index id */
    struct btm_IdMappingTableEntry_T_tag 	*nextEntry;
} btm_IdMappingTableEntry_T;

#define BTM_MAPPINGTBL_HASH(logicalId) (((logicalId).volNo + (logicalId).serial) & (BTM_SIZE_OF_HASH_TABLE_FOR_ID_MAPPING - 1))

/*-------------------- BEGIN OF Shared Memory Section -----------------------*/
/*
 * Shared Memory Data Structures
 */
typedef struct {

    LATCH_TYPE        				latch4TreeLatchTable;
    LOGICAL_PTR_TYPE(btmTreeLatchCell *) 	tLatchPtrTable[BTM_TREELATCH_HASHSIZE]; 

    /* Each cell is used for tree latch of btree (in ARIES/IM) */
    Pool        				tLatchCellPool; 			/* pool of element of TreeLatchCell */

} BtM_SHM;

extern BtM_SHM *btm_shmPtr;
extern Four procIndex;

#define BTM_LATCH4TREELATCHTABLE       btm_shmPtr->latch4TreeLatchTable
#define BTM_TREELATCHPTRTABLE          btm_shmPtr->tLatchPtrTable
#define BTM_TREELATCHPOOL              btm_shmPtr->tLatchCellPool

/*-------------------- END OF Shared Memory Section -------------------------*/



/*
** B+tree Manager Internal function prototypes
*/
Four btm_AllocPage(Four, XactTableEntry_T*, ObjectID*, PageID*, PageID*, LogParameter_T*);
Boolean btm_BinarySearchInternal(Four, BtreeInternal*, KeyDesc*, KeyValue*, Four*);
Boolean btm_BinarySearchLeaf(Four, BtreeLeaf*, KeyDesc*, KeyValue*, Four*);
Boolean btm_BinarySearchOidArray(Four, ObjectID[], ObjectID*, Four, Four*);
void btm_CompactInternalPage(Four, BtreeInternal*, Four);
void btm_CompactLeafPage(Four, BtreeLeaf*, Four);
Four btm_KeyCompare(Four, KeyDesc*, KeyValue*, KeyValue*);
Four btm_ObjectIdComp(Four, ObjectID*, ObjectID*);
Four btm_DeleteLeaf(Four, XactTableEntry_T*, BtreeIndexInfo*, FileID*, btm_TraversePath*, PageID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*, LogParameter_T*);
Four btm_DeleteLeafPage(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, Buffer_ACC_CB*, Buffer_ACC_CB*, LogParameter_T*);
Four btm_DeletePage(Four, XactTableEntry_T*, BtreeIndexInfo*, btm_TraversePath*, PageID*, KeyDesc*, 
                    KeyValue*, LogParameter_T*);
Four btm_InsertLeaf(Four, XactTableEntry_T*, BtreeIndexInfo*, FileID*, btm_TraversePath*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*, LogParameter_T*); 
Four btm_DumpBtreePage(Four, PageID*);
Four btm_FirstObject(Four, XactTableEntry_T*, IndexID*, FileID*, PageID*, btm_TraversePath*, BtreeCursor*, LockParameter*); 
Four btm_LastObject(Four, XactTableEntry_T*, IndexID*, FileID*, PageID*, btm_TraversePath*, BtreeCursor*, LockParameter*); 
Four btm_CreateOverflow(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, Four, LogParameter_T*); 
Four btm_InsertOverflow(Four, XactTableEntry_T*, BtreeIndexInfo*, PageID*, KeyDesc*, KeyValue*, ObjectID*, LogParameter_T*); 
Four btm_DeleteOverflow(Four, XactTableEntry_T*, BtreeIndexInfo*, PageID*, KeyDesc*, KeyValue*, ObjectID*, Four*, LogParameter_T*); 
Four btm_Split(Four, XactTableEntry_T*, btm_TraversePath*, BtreeIndexInfo*, PageID*, KeyDesc*, KeyValue*, LogParameter_T*);
Four btm_get_objectid_from_leaf(Four, XactTableEntry_T*, BtreeCursor*);
Four btm_get_objectid_from_overflow(Four, XactTableEntry_T*, BtreeCursor*);
void btm_ChangeInternalEntrySize(Four, BtreeInternal*, Four, Four);
void btm_ChangeLeafEntrySize(Four, BtreeLeaf*, Four, Four);
void btm_InitPath(Four, btm_TraversePath*, LATCH_TYPE*);
Four btm_FinalPath(Four, btm_TraversePath*);
Boolean btm_IsEmptyPath(Four, btm_TraversePath*);
Four btm_PushElemIntoPath(Four, btm_TraversePath*, Buffer_ACC_CB*, Lsn_T*);
Four btm_PopElemFromPath(Four, btm_TraversePath*, Buffer_ACC_CB**, Lsn_T*);
Four btm_ReadTopElemFromPath(Four, btm_TraversePath*, Buffer_ACC_CB**, Lsn_T*);
Four btm_GetTreeLatchInPath(Four, btm_TraversePath*, Four, LatchConditional);
Four btm_ReleaseTreeLatchInPath(Four, btm_TraversePath*);
Four btm_GetCursorForObjectInSlot(Four, BtreeLeaf*, Four, Boolean, BtreeCursor*);
Four btm_ResetStatusBitsLeaf(Four, btm_TraversePath*, KeyDesc*, KeyValue*, Buffer_ACC_CB*, LATCH_TYPE*);
Boolean btm_IsCorrectInternal(Four, BtreeInternal*, KeyDesc*, KeyValue*, IndexID*, Lsn_T*);
Boolean btm_IsCorrectLeaf(Four, BtreeLeaf*, KeyDesc*, KeyValue*, IndexID*, Lsn_T*);
Four btm_GetObjectId(Four, BtreeLeaf*, Four, ObjectID*, PageID*);
Four btm_GetNextObjectLock(Four, XactTableEntry_T*, FileID*, btm_TraversePath*, Buffer_ACC_CB*, KeyDesc*, KeyValue*, ObjectID*, Four, PageID*); 
Four btm_FetchForward(Four, XactTableEntry_T*, FileID*, btm_TraversePath*, KeyDesc*, KeyValue*, Four, BtreeCursor*, LockParameter*); 
Four btm_FetchNextFwd(Four, XactTableEntry_T*, IndexID*, FileID*, PageID*, LATCH_TYPE*, KeyDesc*, BtreeCursor*, BtreeCursor*, LockParameter*); 
Four btm_FetchBackward(Four, XactTableEntry_T*, FileID*, btm_TraversePath*, KeyDesc*, KeyValue*, Four, BtreeCursor*, LockParameter*);
Four btm_FetchNextBwd(Four, XactTableEntry_T*, IndexID*, FileID*, PageID*, LATCH_TYPE*, KeyDesc*, BtreeCursor*, BtreeCursor*, LockParameter*); 
Four btm_FirstObjectIdOfOverflow(Four, PageID*, ObjectID*);
Four btm_LastObjectIdOfOverflow(Four, PageID*, ObjectID*, Two*);
Four btm_SearchFirstOrLastPage(Four, IndexID*, PageID*, Boolean, btm_TraversePath*); 
Four btm_SearchLeafHavingCursor(Four, BtreeCursor*, LATCH_TYPE*, IndexID*, PageID*, KeyDesc*, Four*, Buffer_ACC_CB**, Buffer_ACC_CB**); 
Four btm_Search(Four, IndexID*, PageID*, KeyDesc*, KeyValue*, Four, Four, btm_TraversePath*); 
Four btm_CheckDuplicatedKey(Four, XactTableEntry_T*, FileID*, Buffer_ACC_CB*, KeyDesc*, KeyValue*, LockParameter*);
Four btm_SearchLeafHavingCursor(Four, BtreeCursor *,LATCH_TYPE *, IndexID*, PageID *, KeyDesc *, Four *, Buffer_ACC_CB **, Buffer_ACC_CB **); 
Four btm_SearchOverflowHavingCursor(Four, IndexID*, PageID*, BtreeCursor *, BtreeCursor*, Boolean, PageID*); 
Four btm_OpenPreviousLeaf(Four, PageID *, PageID *, Buffer_ACC_CB *, Buffer_ACC_CB **, LATCH_TYPE *, LockParameter *);	
Four btm_CheckStatusOfLeaf(Four, Buffer_ACC_CB *, LATCH_TYPE *);
Four btm_SearchNextLeaf(Four, PageID*,KeyDesc*,KeyValue*,Four, Boolean, Buffer_ACC_CB**,Boolean*,Four*);
Four btm_RequestUnconditionalLock(Four, XactTableEntry_T*, ObjectID*, FileID*, KeyDesc *, KeyValue *, Boolean, Four, Buffer_ACC_CB *);
Four btm_GetRootPid(Four, XactTableEntry_T*, BtreeIndexInfo*, PageID*, LockParameter*); 



/*
** B+tree Manager Interface function prototypes
*/
Four BtM_CreateIndex(Four, XactTableEntry_T*, Four, PageID*, SegmentID_T*, LogParameter_T*); 
Four BtM_DeleteObject(Four, XactTableEntry_T*, BtreeIndexInfo*, FileID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*, LogParameter_T*);
Four BtM_DropIndex(Four, XactTableEntry_T*, IndexID*, PhysicalIndexID*, SegmentID_T*, Boolean, LogParameter_T*); 
Four BtM_Fetch(Four, XactTableEntry_T*, BtreeIndexInfo*, FileID*, KeyDesc*, KeyValue*, Four, KeyValue*, Four, BtreeCursor*, PageID*, LockParameter *); 
Four BtM_FetchNext(Four, XactTableEntry_T*, BtreeIndexInfo*, FileID*, KeyDesc*, KeyValue*, Four, BtreeCursor*, BtreeCursor*, LockParameter*); 
Four BtM_InsertObject(Four, XactTableEntry_T*, BtreeIndexInfo*, FileID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*, LogParameter_T*);
Four BtM_InitSharedDS(Four);
Four BtM_InitLocalDS(Four);
Four BtM_Final(Four);
Four BtM_GetStatistics(Four, PageID*, Four*);
Four BtM_GetStatistics_BtreePageInfo(Four, PageID*, Four, Four*, sm_PageInfo*, LockParameter*); 
Four BtM_GetTreeLatchPtrFromIndexId(Four, IndexID*, LATCH_TYPE**);
Four BtM_ReleaseTreeLatchPtr(Four, IndexID*);
Four BtM_ReleaseAllTreeLatchPtr(Four);

Four btm_GetSegmentIDFromIndexInfo(Four, XactTableEntry_T*, BtreeIndexInfo*, SegmentID_T*, Four);

Four btm_IdMapping_InitTable(Four);
Four btm_IdMapping_FinalTable(Four);
Four btm_IdMapping_GetPhysicalId(Four, XactTableEntry_T*, BtreeIndexInfo*, PhysicalIndexID*, LockParameter*);
Four btm_IdMapping_InsertEntry(Four, IndexID*, PhysicalIndexID*);
Four btm_IdMapping_DeleteEntry(Four, IndexID*);

#endif /* _BTM_H_ */

