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
#ifndef _BTM_INTERNAL_H_
#define _BTM_INTERNAL_H_


#include "param.h"
#include "Util_pool.h"


/*@
 * Constant Definitions
 */
#ifndef MAXKEYLEN
#define MAXKEYLEN	256
#endif


#define OBJECTID_SIZE   sizeof(ObjectID)

/*
 * Comparison result
 */
#define EQUAL 0
#define GREAT 1
#define LESS  2


/*@
 * Type Definitions
 */
/*************************************************************
 * The structure of Btree Pages - Internal / Leaf / Overflow *
 *************************************************************/

/*
** BtreeAny Page:
**  All pages in B+ tree files have some common fields in the same place.
**  Any Page describe the common fields.
*/
typedef struct {    
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
    One    type;		/* Internal, Leaf, or Overflow */
} BtreeAnyHdr;

#define BANY_FIXED  (sizeof(BtreeAnyHdr))

typedef struct { /* Any Page */
    BtreeAnyHdr hdr;            /* header of pages */
    char	data[PAGESIZE-BANY_FIXED]; /* data area */
} BtreeAny;


/*
** BtreeInternal Page:
**  Page for the internal node of B+ tree.
*/
typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
    One 	type;		/* Internal, Leaf, or Overflow */
    ShortPageID p0;		/* the first pointer */
    Two  	nSlots;		/* # of entries in this page */
    Two  	free;		/* starting point of the free space */
    Two         unused;		/* number of unused bytes which are not */
				/* part of the contiguous freespace */
} BtreeInternalHdr;

#define BI_FIXED  (sizeof(BtreeInternalHdr) + sizeof(Two))

typedef struct {   /* Internal (Nonleaf) Page */
    BtreeInternalHdr    hdr;       /* header of the slotted page */
    char	        data[PAGESIZE-BI_FIXED];  /* data area */
    Two  	        slot[1];	/* the first slot */
} BtreeInternal;

/* free space of BI; 'f' is a pointer to a buffer of an internal page. */
#define BI_FREE(p)    ((p)->hdr.unused + BI_CFREE(p))
#define BI_CFREE(p)   (PAGESIZE - BI_FIXED - (p)->hdr.free - ((p)->hdr.nSlots-1)*((CONSTANT_CASTING_TYPE)sizeof(Two))) 
#define BI_HALF       ((CONSTANT_CASTING_TYPE)((PAGESIZE-BI_FIXED)/2))


/*
** BtreeLeaf:
**  Page for the leaf node of a B+ tree
*/
typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
    One		type;			 /* Internal, Leaf, or Overflow */
    Two  	nSlots;			 /* # of entries in this page */
    Two  	free;			 /* starting point of the free space */
    ShortPageID prevPage;		 /* Previous page */
    ShortPageID nextPage;		 /* Next page */
    Two		unused;			 /* number of unused bytes which are not part of the contiguous freespace */
} BtreeLeafHdr;


#define BL_FIXED  (sizeof(BtreeLeafHdr) + sizeof(Two))

typedef struct {   /* Leaf Page */
    BtreeLeafHdr hdr;           /* header of btree leaf page */
    char  	 data[PAGESIZE-BL_FIXED]; /* data area */
    Two  	 slot[1];		 /* the first slot */
} BtreeLeaf;

/* free space of BL; 'p' is a pointer to a buffer of a leaf page. */
#define BL_FREE(p)    ((p)->hdr.unused + BL_CFREE(p))
#define BL_CFREE(p)    (PAGESIZE - BL_FIXED - (p)->hdr.free - ((p)->hdr.nSlots-1)*((CONSTANT_CASTING_TYPE)sizeof(Two))) 
#define BL_HALF        ((CONSTANT_CASTING_TYPE)((PAGESIZE-BL_FIXED)/2))
#define OVERFLOW_SPLIT ((CONSTANT_CASTING_TYPE)(PAGESIZE-BL_FIXED)/3)


/*
** BteeOverflow:
**  overflow page of a B+ tree
*/
typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
    One		type;		      /* Internal, Leaf, or Overflow */
    ShortPageID nextPage;	      /* Next Page */
    ShortPageID prevPage;	      /* Previous Page */
    Two		nObjects;	      /* # of ObjectIDs in this page */
} BtreeOverflowHdr;


#define BO_FIXED  sizeof(BtreeOverflowHdr)
#define BO_MAXOBJECTIDS ((PAGESIZE-BO_FIXED)/sizeof(ObjectID))
#define BO_DUMMY (PAGESIZE-BO_FIXED-BO_MAXOBJECTIDS*sizeof(ObjectID))

typedef struct {   /* Overflow page */
    BtreeOverflowHdr    hdr;       /* header of the btree overflow page */
    ObjectID	        oid[BO_MAXOBJECTIDS]; /* ObjectID area */
#if !defined(SUPPORT_LARGE_DATABASE2)    
    char	        dummy_fill[BO_DUMMY]; /* dummy fill area */
#endif
} BtreeOverflow;

#define NO_OF_OBJECTS	BO_MAXOBJECTIDS
#define HALF_OF_OBJECTS         ((CONSTANT_CASTING_TYPE)(NO_OF_OBJECTS/2))
#define A_THIRD_OF_OBJECTS      ((CONSTANT_CASTING_TYPE)(NO_OF_OBJECTS/3))
#define A_FOURTH_OF_OBJECTS     ((CONSTANT_CASTING_TYPE)(NO_OF_OBJECTS/4))



/*
** BtreePage:
**  Page type contains all page types
*/
typedef union {
    /*
     * Object Manager & B+ Tree Manager Page Types
     */
    BtreeAny      any;		/* btree any page */
    BtreeInternal bi;		/* btree internal page */
    BtreeLeaf     bl;		/* btree leaf page */
    BtreeOverflow bo;		/* btree overflow page */
} BtreePage;

/* Btree Page Type */
#define ROOT        0x01
#define INTERNAL    0x02
#define LEAF        0x04
#define OVERFLOW    0x08
#define FREEPAGE    0x10


/****************************************************************
 * Entry Types of a B+ tree 
 ****************************************************************/

/*
 * The data types 'btm_InternalEntry' and 'btm_LeafEntry' are used to access
 * the entries directly in the buffer of the page. Whereas the data types
 * 'InternalItem' and 'LeafItem' are used to pass the entry value between
 * two functions.
 */

/* Data type of Internal Entry */
typedef struct {
    ShortPageID spid;		/* pointer to the child page */
				/* The child has key values greater than or */
				/* equal to 'kval' of this entry. */
    /* 'klen' and 'kval' should be attached in this order */
    /* to cast this variables the type KeyVlaue. */
    Two  klen;			/* key length */
    char kval[1];		/* key value */
} btm_InternalEntry;

/* Data type of Leaf Entry */
#define BTM_LEAFENTRY_FIXED OFFSET_OF(btm_LeafEntry, kval[0]) 

typedef struct {
    Two nObjects;		/* # of ObjectIDs */
    /* 'klen' and 'kval' should be attached in this order */
    /* to cast this variables the type KeyVlaue. */
    Two klen;			/* key length */
				/* From this point, alignment is counted. */
    char kval[1];		/* key value and (ObjectID array or overflow PageID) */
} btm_LeafEntry;


/* Data type for representing an internal item */
typedef struct {
    ShortPageID spid;		/* points to the child page */
    Two         klen;		/* key length */
    char        kval[MAXKEYLEN]; /* key value */
} InternalItem;

/* Data type for representing a leaf item */
typedef struct {
    ObjectID oid;		/* an ObjectID */
    Two nObjects;		/* # of ObjectIDs */
    Two  klen;			/* key length */
    char kval[MAXKEYLEN];	/* key value */
} LeafItem;



/*@
** Macro Definitions
*/
/* Access of the B+ tree part in a catalog table SM_SYSTABLES */
#define GET_PTR_TO_CATENTRY_FOR_BTREE(catObjForFile, catPage, catEntry) \
BEGIN_MACRO \
    sm_CatOverlayForSysTables *smSysTables; \
    Object *obj = (Object*)&(catPage->data[catPage->slot[-catObjForFile->slotNo].offset]); \
    catEntry = &(((sm_CatOverlayForSysTables*)&(obj->data))->btree);\
END_MACRO
    


/*@
 * Function Prototypes
 */
/*
** B+tree Manager Internal function prototypes
*/
Four btm_AllocPage(Four, ObjectID*, PageID*, PageID*);
Boolean btm_BinarySearchInternal(Four, BtreeInternal*, KeyDesc*, KeyValue*, Two*);
Boolean btm_BinarySearchLeaf(Four, BtreeLeaf*, KeyDesc*, KeyValue*, Two*);
Boolean btm_BinarySearchOidArray(Four, ObjectID[], ObjectID*, Two, Two*);
void btm_CompactInternalPage(Four, BtreeInternal*, Two);
void btm_CompactLeafPage(Four, BtreeLeaf*, Two);
Four btm_KeyCompare(Four, KeyDesc*, KeyValue*, KeyValue*);
Four btm_ObjectIdComp(Four, ObjectID*, ObjectID*);
Four btm_Delete(Four, ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*, Boolean*, Boolean*, InternalItem*, Pool*, DeallocListElem*);
Four btm_Insert(Four, ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*, Boolean*, Boolean*, InternalItem*, Pool*, DeallocListElem*);
Four btm_InsertLeaf(Four, ObjectID*, PageID*, BtreeLeaf*, KeyDesc*, KeyValue*, ObjectID*, Boolean*, Boolean*, InternalItem*);
Four btm_InsertInternal(Four, ObjectID*, BtreeInternal*, InternalItem*, Two, Boolean*, InternalItem*);
Four btm_DumpBtreePage(Four, PageID*);
Four btm_FirstObject(Four, PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*);
Four btm_FreePages(Four, PhysicalFileID*, PageID*, Pool*, DeallocListElem*); 
Four btm_InitInternal(Four, PageID*, Boolean, Boolean); 
Four btm_InitLeaf(Four, PageID*, Boolean, Boolean); 
Four btm_InitOverflow(Four, PageID*, Boolean);      
Four btm_LastObject(Four, PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*);
Four btm_CreateOverflow(Four, ObjectID*, BtreeLeaf*, Two, ObjectID*);
Four btm_InsertOverflow(Four, ObjectID*, PageID*, ObjectID*);
Four btm_DeleteOverflow(Four, PhysicalFileID*, PageID*, ObjectID*, Two*, Pool*, DeallocListElem*); 
Four btm_SplitInternal(Four, ObjectID*, BtreeInternal*, Two, InternalItem*, InternalItem*);
Four btm_SplitOverflow(Four, ObjectID*, PageID*, BtreeOverflow*);
Four btm_SplitLeaf(Four, ObjectID*, PageID*, BtreeLeaf*, Two, LeafItem*, InternalItem*);
Four btm_SplitOverflow(Four, ObjectID*, PageID*, BtreeOverflow*);
Four btm_Underflow(Four, PhysicalFileID*, BtreePage*, PageID*, Two, Boolean*, 
		   Boolean*, InternalItem*, Pool*, DeallocListElem*);
Four btm_get_objectid_from_leaf(Four, BtreeCursor*);
Four btm_get_objectid_from_overflow(Four, BtreeCursor*);
Four btm_root_delete(Four, PhysicalFileID*, PageID*, Pool*, DeallocListElem*); 
Four btm_root_insert(Four, ObjectID*, PageID*, InternalItem*);
Four btm_IsTemporary(Four, ObjectID*, Boolean*); 

/*
** B+tree Manager Interface function prototypes
*/
Four BtM_CreateFile(Four, FileID*, Two, sm_CatOverlayForBtree*);            
Four BtM_CreateIndex(Four, ObjectID*, PageID*);
Four BtM_DeleteObject(Four, ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*, Pool*, DeallocListElem*);
Four BtM_DropFile(Four, PhysicalFileID*, Pool*, DeallocListElem*);           
Four BtM_DropIndex(Four, PhysicalFileID*, PageID*, Pool*, DeallocListElem*); 
char *BtM_Err(Four);
Four BtM_Fetch(Four, PageID*, KeyDesc*, KeyValue*, Four, KeyValue*, Four, BtreeCursor*);
Four BtM_FetchNext(Four, PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*, BtreeCursor*);
Four BtM_InsertObject(Four, ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*, Pool*, DeallocListElem*);
Four BtM_GetStatistics_BtreePageInfo(Four, PageID*, Four, Four*, sm_PageInfo*); 

#endif /* _BTM_INTERNAL_H_ */
