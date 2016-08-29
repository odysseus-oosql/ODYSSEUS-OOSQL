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
#ifndef _MLGF_INTERNAL_H_
#define _MLGF_INTERNAL_H_

#include <limits.h>		/* CHAR_BIT */
#include "Util.h"
#include "BfM.h"

/* maximum length of extra data */
#define MLGF_MAXLEN_EXTRADATA 100

/* type definition of MortonValue */
typedef MLGF_HashValue MortonValue;

typedef struct {
    UFour       nBits;
    MortonValue val[MLGF_MAXNUM_KEYS];
} mlgf_MortonValue;

/* get absolute value */
#define	ABS(x)	(((x) <0) ? (-(x)) : (x))

/* Constant definitions */
#define MLGF_MAXNUM_VALIDBITS           (CHAR_BIT*sizeof(MLGF_HashValue))
#define MLGF_HASHVALUE_MSB_SET          ((MLGF_HashValue)(((MLGF_HashValue)CONSTANT_ONE)<<(sizeof(MLGF_HashValue)*CHAR_BIT-1)))
#define MLGF_HASHVALUE_ALL_BITS_SET     ((MLGF_HashValue)(CONSTANT_ALL_BITS_SET(MLGF_HashValue)))
#define MLGF_HASHVALUE_MAXNUM_BITS      (CHAR_BIT*sizeof(MLGF_HashValue))
#define MLGF_MORTONVALUE_MAXNUM_BITS    (CHAR_BIT*sizeof(MortonValue))
#define MLGF_MORTONVALUE_MSB_SET        ((MortonValue)(((MortonValue)CONSTANT_ONE)<<(sizeof(MortonValue)*CHAR_BIT-1)))
#define MLGF_MORTONVALUE_ALL_BITS_SET   ((MortonValue)(CONSTANT_ALL_BITS_SET(MortonValue)))

/* page status */
#define MLGF_NORMAL    0
#define MLGF_EMPTY     1
#define MLGF_UNDERFLOW 2
#define MLGF_OVERFLOW  3

/* page type */
#define MLGF_DIRECTORYPAGE 1
#define MLGF_LEAFPAGE      2
#define MLGF_OVERFLOWPAGE  4
#define MLGF_ROOTPAGE      8
#define MLGF_FREEPAGE      0x10

/* Program Status */
#define MLGF_STATUS_INSERT      1
#define MLGF_STATUS_OVERFLOW    2
#define MLGF_STATUS_FOUND       3
#define MLGF_STATUS_NOTFOUND    4
#define MLGF_STATUS_DELETE      5
#define MLGF_STATUS_UNDERFLOW   6
#define MLGF_STATUS_EMPTYPAGE   7
#define MLGF_STATUS_INVALIDPAGE 8
#define MLGF_STATUS_MBRCHANGE   9

/* Comparison Related Constants */
#define EQUAL 0
#define GREAT 1
#define LESS  2

/* Init entry no */
#define MLGF_NOENTRY  -1

/*
 * Type definition for program status variable
 */
typedef union mlgf_InsertStatus_T_tag {
    struct {
        unsigned objectInserted      : 1;
        unsigned mbrUpdated          : 1;
        unsigned thetaUpdated        : 1;
        unsigned overflow            : 1;
        unsigned objectInEmptyRegion : 1;
    } flags;
    UFour_Invariable allFlags;
} mlgf_InsertStatus_T;

typedef union mlgf_DeleteStatus_T_tag {
    struct {
        unsigned notFound            : 1;
        unsigned mbrUpdated          : 1;
        unsigned thetaUpdated        : 1;
        unsigned underflow           : 1;
        unsigned emptyPage           : 1;
    } flags;
    UFour_Invariable allFlags;
} mlgf_DeleteStatus_T;


/*
 * Type definition for mlgf_AnyPage
 */
typedef struct mlgf_AnyPageHdr_T_tag {
    PageID pid;                 /* page id of this page */
    Four flags;             /* flags for page characteristics */
    Four reserved;          /* reserved space for page characteristics */
    One	type;                   /* Internal, Leaf, or Overflow */
} mlgf_AnyPageHdr_T;

#define MLGF_AP_FIXED (sizeof(mlgf_AnyPageHdr_T))

typedef struct {
    mlgf_AnyPageHdr_T hdr;      /* page header */
    char data[PAGESIZE-MLGF_AP_FIXED]; /* data area */
} mlgf_AnyPage;


/*
 * Type definition for the directory page
 */
typedef struct mlgf_DirectoryPageHdr_T_tag {
    PageID pid;                 /* page id of this page */
    Four flags;             /* flags for page characteristics */
    Four reserved;          /* reserved space for page characteristics */
    One	type;                   /* Internal, Leaf, or Overflow */
    One nKeys;                  /* # of keys for this index */
    One height;			/* height of this page */
    Two nEntries;		/* number of the stored entries */
} mlgf_DirectoryPageHdr_T;

#define MLGF_DP_FIXED (sizeof(mlgf_DirectoryPageHdr_T))

typedef struct {
    mlgf_DirectoryPageHdr_T hdr; /* page header */
    char data[PAGESIZE-MLGF_DP_FIXED]; /* data area */
} mlgf_DirectoryPage;

#define MLGF_DP_THETA(dirPage, entryLen) ((dirPage)->hdr.nEntries*(entryLen))

/*
 * Type definition for directory entry
 */
typedef struct {
    ShortPageID spid;		/* PageID of the child page */
    Two theta;			/* number of used bytes in child page */
    One nValidBits[MLGF_MAXNUM_KEYS]; /* number of valid bits of hash values */
				      /* implicitly followed by hash values */
    /* The following field is defined only for storage reservation. */
    MLGF_HashValue hashValue[MLGF_MAXNUM_KEYS]; /* hash values */
} mlgf_DirectoryEntry;

#define MLGF_DIRENTRY_FIXED OFFSET_OF(mlgf_DirectoryEntry, nValidBits[0]) 

#define MLGF_DIRENTRY_LENGTH(nKeys) (ALIGNED_LENGTH(MLGF_DIRENTRY_FIXED+sizeof(One)*nKeys) + \
                                     nKeys*sizeof(MLGF_HashValue))

#define MLGF_DIRENTRY_HASHVALUEPTR(entry, nKeys) \
((MLGF_HashValue*) ((char*)entry + ALIGNED_LENGTH(MLGF_DIRENTRY_FIXED + sizeof(One)*nKeys)))

#define MLGF_MAX_DIRENTRIES(nKeys) ((PAGESIZE-MLGF_DP_FIXED)/MLGF_DIRENTRY_LENGTH(nKeys))

#define MLGF_ITH_DIRENTRY(dirPage, entryNo, entryLen) \
((mlgf_DirectoryEntry*)&(dirPage)->data[(entryLen)*(entryNo)])

#define MLGF_NEXT_DIRENTRY(dirEntry, entryLen) \
((mlgf_DirectoryEntry*)((char*)(dirEntry) + entryLen))

/*
 * Type definition for the leaf page
 */
typedef struct mlgf_LeafPageHdr_T_tag {
    PageID pid;                 /* page id of this page */
    Four flags;             /* flags for page characteristics */
    Four reserved;          /* reserved space for page characteristics */
    One	type;                   /* Internal, Leaf, or Overflow */
    One nKeys;                  /* # of keys for this index */
    Two extraDataLen;           /* length of the extra data for an object */
    Two nEntries;		/* number of the stored entries */
    Two free;                   /* offset of contiguous free area on page */
    Two unused;                 /* number of unused bytes which are not */
                                /* part of the contiguous free space */
} mlgf_LeafPageHdr_T;

#define MLGF_LP_FIXED (sizeof(mlgf_LeafPageHdr_T) + sizeof(Two))

typedef struct {
    mlgf_LeafPageHdr_T hdr;
    char data[PAGESIZE-MLGF_LP_FIXED]; /* data area */
    Two slot[1];		       /* slot array, indexes backward */
} mlgf_LeafPage;

#define MLGF_LP_FREE(apage) (MLGF_LP_CFREE(apage) + (apage)->hdr.unused)
#define MLGF_LP_CFREE(apage) \
(PAGESIZE - MLGF_LP_FIXED - (apage)->hdr.free - ((apage)->hdr.nEntries-1)*sizeof(Two))
#define MLGF_LP_THETA(apage) \
((apage)->hdr.free - (apage)->hdr.unused + sizeof(Two)*((apage)->hdr.nEntries)) 


/*
 * Type definition for leaf entry
 */
typedef struct {
    Four nObjects;		/* number of objects which have same keys */
    MLGF_HashValue keys[MLGF_MAXNUM_KEYS];	/* key values */
    char dummy[sizeof(ObjectID)+MLGF_MAXLEN_EXTRADATA]; /* space reservation */
    
    /* After the real number of keys there comes the list of objects. */
    /* The list is an array of pairs of ObjectID */
    /* and its associated data. */
    /* If the list is too long, then overflow */
    /* page no of the first overflow page is placed. */
} mlgf_LeafEntry;

#define MLGF_LEAFENTRY_LENGTH(_nKeys, _extraDataLen, _nObjects) \
( sizeof(Four) + sizeof(MLGF_HashValue)*(_nKeys) + \
  (((_nObjects) < 0) ? \
   sizeof(ShortPageID):MLGF_LEAFENTRY_OBJECTITEM_LEN(_extraDataLen)*(_nObjects)) )
  
#define MLGF_NEW_LEAFENTRY_LENGTH(_nKeys, _extraDataLen) \
(sizeof(Four) + sizeof(MLGF_HashValue)*(_nKeys) + MLGF_LEAFENTRY_OBJECTITEM_LEN(_extraDataLen))
 
#define MLGF_LEAFENTRY_FIRST_OVERFLOW(_nKeys, entry) \
(*((ShortPageID*)((entry)->keys + (_nKeys))))

#define MLGF_LEAFENTRY_FIRST_OBJECT(_nKeys, entry) \
((char*)(entry->keys + (_nKeys)))

#define MLGF_LEAFENTRY_ITH_OBJECTITEM(_nKeys, _extraDataLen, entry, i) \
((char*)((entry)->keys + (_nKeys)) + MLGF_LEAFENTRY_OBJECTITEM_LEN(_extraDataLen)*(i))

#define MLGF_LEAFENTRY_OBJECTITEM_LEN(_extraDataLen) \
(ALIGNED_LENGTH(sizeof(ObjectID) + (_extraDataLen)))

#define MLGF_ITH_LEAFENTRY(leafPage, entryNo) \
((mlgf_LeafEntry*)&(leafPage)->data[(leafPage)->slot[-(entryNo)]])


/*
 * Type definition for overflow page
 */
typedef struct mlgf_OverflowPageHdr_T_tag {
    PageID pid;                 /* page id of this page */
    Four flags;             /* flags for page characteristics */
    Four reserved;          /* reserved space for page characteristics */
    One	type;                   /* Internal, Leaf, or Overflow */
    Two extraDataLen;           /* length of the extra data for an object */
    Two nObjects;               /* number of the stored objects */
    ShortPageID prevPage;       /* previous overflow page */
    ShortPageID nextPage;       /* next overflow page */
} mlgf_OverflowPageHdr_T;

#define MLGF_OP_FIXED (sizeof(mlgf_OverflowPageHdr_T))

typedef struct {
    mlgf_OverflowPageHdr_T hdr;
    char data[PAGESIZE-MLGF_OP_FIXED]; /* data area */
} mlgf_OverflowPage;

#define MLGF_OVERFLOW_ITH_OBJECTITEM(elemLen, opage, i) \
((char*)(&(opage)->data[(elemLen)*(i)]))
#define MLGF_OVERFLOW_MAXNUM_OBJECTS(elemLen) ((PAGESIZE-MLGF_OP_FIXED)/(elemLen))
#define MLGF_OVERFLOW_HALF_OF_OBJECTS(elemLen) \
((MLGF_OVERFLOW_MAXNUM_OBJECTS(elemLen)+1)/2)
#define MLGF_OVERFLOW_A_FOURTH_OF_OBJECTS(elemLen) \
(MLGF_OVERFLOW_MAXNUM_OBJECTS(elemLen)/4)

/*
 * Type definiton for mlgf_Page
 */
typedef union {
    mlgf_AnyPage       any;	  /* page which consists of common fileds */
    mlgf_DirectoryPage directory; /* directory page */
    mlgf_LeafPage      leaf;	  /* leaf page */
    mlgf_OverflowPage  overflow;  /* overflow page */
} mlgf_Page;


/* threshold */
#define MLGF_DP_THRESHOLD ((PAGESIZE-MLGF_DP_FIXED)/2)
#define MLGF_LP_THRESHOLD ((PAGESIZE-MLGF_LP_FIXED)/2)
/* Threshold for converting from normal entry to overflow page */
#define MLGF_OP_IN_THRESHOLD ((PAGESIZE-MLGF_OP_FIXED)/3)
/* Threshold for converting from overflow page to normal entry */
#define MLGF_OP_OUT_THRESHOLD ((PAGESIZE-MLGF_OP_FIXED)/4)



/*
 * Type Definition for mlgf_PathElem
 */
typedef struct {
    PageID pid;
} mlgf_PathElem;

#define MLGF_CURSOR_PATH_INIT(cursor) ((cursor)->pathTop = -1)
#define MLGF_CURSOR_PATH_PID(path, top) (((mlgf_PathElem*)((path)->ptr))[top].pid)

#define MLGF_CURSOR_PATH_IS_EMPTY(cursor) ((cursor)->pathTop == -1)

#define MLGF_CURSOR_PATH_PUSH(handle, cursor, pid) \
BEGIN_MACRO \
Four e; \
if (++((cursor)->pathTop) >= (cursor)->path.nEntries) { \
    e = Util_doublesizeVarArray((handle), &((cursor)->path), sizeof(mlgf_PathElem)); \
    if (e < 0) ERR(handle, e); \
} \
MLGF_CURSOR_PATH_PID(&((cursor)->path), cursor->pathTop) = pid; \
END_MACRO

#define MLGF_CURSOR_PATH_POP(cursor) (cursor)->pathTop --

#define MLGF_CURSOR_PATH_READ_TOP(cursor, pid) \
pid = MLGF_CURSOR_PATH_PID(&(cursor)->path, (cursor)->pathTop)
						    
/* MSB's bit no is 1. */
#define MLGF_HASHVALUE_ITH_BIT_SET(i) ((MLGF_HashValue)0x1 << (MLGF_HashValue)(MLGF_HASHVALUE_MAXNUM_BITS - (i)))
				       
#define MLGF_HASHVALUE_UPPER_N_BITS_SET(n) \
((n) ? (MLGF_HASHVALUE_ALL_BITS_SET << (MLGF_HashValue)(MLGF_HASHVALUE_MAXNUM_BITS - n)):(MLGF_HashValue)0)

#define MLGF_HASHVALUE_MASK_UPPER_N_BITS(val, n) \
( (MLGF_HashValue)val & MLGF_HASHVALUE_UPPER_N_BITS_SET(n) )

#define MLGF_HASHVALUE_SET_EXCEPT_UPPER_N_BITS(n) \
(((n) == MLGF_HASHVALUE_MAXNUM_BITS) ? 0:(MLGF_HASHVALUE_ALL_BITS_SET >> (n)))

#define MLGF_HASHVALUE_LOWER_N_BITS_SET(n) \
((n) ? (MLGF_HASHVALUE_ALL_BITS_SET >> (MLGF_HashValue)(MLGF_HASHVALUE_MAXNUM_BITS - n)):(MLGF_HashValue)0)

#define MLGF_HASHVALUE_MASK_LOWER_N_BITS(val, n) \
( (MLGF_HashValue)val & MLGF_HASHVALUE_LOWER_N_BITS_SET(n) )

#define MLGF_MORTONVALUE_UPPER_N_BITS_SET(n) \
((n) ? (MLGF_MORTONVALUE_ALL_BITS_SET << (MortonValue)(MLGF_MORTONVALUE_MAXNUM_BITS - n)):(MortonValue)0)

#define MLGF_MORTONVALUE_MASK_UPPER_N_BITS(val, n) \
( (MortonValue)val & MLGF_MORTONVALUE_UPPER_N_BITS_SET(n) )

#define MLGF_MORTONVALUE_LOWER_N_BITS_SET(n) \
((n) ? (MLGF_MORTONVALUE_ALL_BITS_SET >> (MortonValue)(MLGF_MORTONVALUE_MAXNUM_BITS - n)):(MortonValue)0)

#define MLGF_MORTONVALUE_MASK_LOWER_N_BITS(val, n) \
( (MortonValue)val & MLGF_MORTONVALUE_LOWER_N_BITS_SET(n) )


#define MLGF_COPY_DIRECTORY_PAGE(dstPage, dstPid, srcPage, isRoot) \
BEGIN_MACRO \
    *(dstPage) = *(srcPage); \
    (dstPage)->hdr.type = MLGF_DIRECTORYPAGE; \
    if (isRoot) (dstPage)->hdr.type |= MLGF_ROOTPAGE; \
    (dstPage)->hdr.pid = dstPid; \
END_MACRO

#define MLGF_INIT_DIRECTORY_PAGE(_apage, _isTmp, _pid, _height, _isRoot, _nKeys) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_apage, MLGF_PAGE_TYPE); \
    if (_isTmp) SET_TEMP_PAGE_FLAG(_apage); \
    else        RESET_TEMP_PAGE_FLAG(_apage); \
    (_apage)->hdr.pid = _pid; \
    (_apage)->hdr.type = (_isRoot == TRUE) ? (MLGF_DIRECTORYPAGE | MLGF_ROOTPAGE) : MLGF_DIRECTORYPAGE; \
    (_apage)->hdr.height = _height; \
    (_apage)->hdr.nEntries = 0; \
    (_apage)->hdr.nKeys = _nKeys; \
END_MACRO

#define MLGF_INIT_LEAF_PAGE(_apage, _isTmp, _pid, _nKeys, _extraDataLen) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_apage, MLGF_PAGE_TYPE); \
    if (_isTmp) SET_TEMP_PAGE_FLAG(_apage); \
    else        RESET_TEMP_PAGE_FLAG(_apage); \
    (_apage)->hdr.pid = _pid; \
    (_apage)->hdr.type = MLGF_LEAFPAGE; \
    (_apage)->hdr.nEntries = 0; \
    (_apage)->hdr.free = 0; \
    (_apage)->hdr.unused = 0; \
    (_apage)->hdr.nKeys = _nKeys; \
    (_apage)->hdr.extraDataLen = _extraDataLen; \
END_MACRO

#define MLGF_INIT_OVERFLOW_PAGE(_apage, _isTmp, _pid, _prevPage, _nextPage, _extraDataLen) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_apage, MLGF_PAGE_TYPE); \
    if (_isTmp) SET_TEMP_PAGE_FLAG(_apage); \
    else        RESET_TEMP_PAGE_FLAG(_apage); \
    (_apage)->hdr.pid = _pid; \
    (_apage)->hdr.type = MLGF_OVERFLOWPAGE; \
    (_apage)->hdr.nObjects = 0; \
    (_apage)->hdr.prevPage = _prevPage; \
    (_apage)->hdr.nextPage = _nextPage; \
    (_apage)->hdr.extraDataLen = _extraDataLen; \
END_MACRO

#define MLGF_INSERT_DIRECTORY_ENTRIES(_apage, _entryNo, _n, _entries, _entryLen) \
BEGIN_MACRO \
    memmove(MLGF_ITH_DIRENTRY(_apage, (_entryNo)+(_n), _entryLen), \
            MLGF_ITH_DIRENTRY(_apage, _entryNo, _entryLen), \
            (_entryLen)*((_apage)->hdr.nEntries - (_entryNo))); \
    memcpy(MLGF_ITH_DIRENTRY(_apage, _entryNo, _entryLen), \
           _entries, (_entryLen)*(_n)); \
END_MACRO
                
#define MLGF_DELETE_DIRECTORY_ENTRIES(_apage, _entryNo, _n, _entryLen) \
    memmove(MLGF_ITH_DIRENTRY(_apage, _entryNo, _entryLen), \
            MLGF_ITH_DIRENTRY(_apage, (_entryNo)+(_n), _entryLen), \
            (_entryLen)*((_apage)->hdr.nEntries - (_n) - (_entryNo)))
    
#define MLGF_INSERT_SLOTS_IN_MLGF_PAGE(_apage, _idx, _n) \
    memmove(&((_apage)->slot[-((_apage)->hdr.nEntries - 1 + (_n))]), \
            &((_apage)->slot[-((_apage)->hdr.nEntries - 1)]), \
            sizeof(Two)*((_apage)->hdr.nEntries - (_idx)))

#define MLGF_DELETE_SLOTS_IN_MLGF_PAGE(_apage, _idx, _n) \
    memmove(&((_apage)->slot[-((_apage)->hdr.nEntries - 1 - (_n))]), \
            &((_apage)->slot[-((_apage)->hdr.nEntries - 1)]), \
            sizeof(Two)*((_apage)->hdr.nEntries - (_n) - (_idx)))

#define MLGF_DELETE_OBJECTS_SPACE_FROM_OBJECT_ARRAY(_array, _total, _idx, _n, _objectSize) \
    memmove((char*)(_array) + (_idx)*(_objectSize), (char*)(_array) + ((_idx) + (_n))*(_objectSize), ((_total) - (_idx) - (_n))*(_objectSize))

#define MLGF_INSERT_OBJECTS_SPACE_INTO_OBJECT_ARRAY(_array, _total, _idx, _n, _objectSize) \
    memmove((char*)(_array) + ((_idx) + (_n))*(_objectSize), (char*)(_array) + (_idx)*(_objectSize), ((_total) - (_idx))*(_objectSize)) 

#define MLGF_WRITE_OBJECTS_IN_OBJECT_ARRAY(_array, _idx, _n, _objects, _objectSize) \
    memcpy((char*)(_array) + (_idx)*(_objectSize), (_objects), (_n)*(_objectSize))

/* Access of the index part in a catalog table SM_SYSTABLES */
#define GET_PTR_TO_CATENTRY_FOR_INDEX(catObjForFile, catPage, catEntry) \
BEGIN_MACRO \
    sm_CatOverlayForSysTables *smSysTables; \
    Object *obj = (Object*)&(catPage->data[catPage->slot[-catObjForFile->slotNo].offset]); \
    catEntry = &(((sm_CatOverlayForSysTables*)&(obj->data))->btree);\
END_MACRO


/* MLGF Internal Function Prototypes */
Four mlgf_AllocPage(Four, ObjectID*, PageID*, PageID*);
Boolean mlgf_BinarySearchObjectArray(Four, char*, ObjectID*, Four, Two, Two*);
Boolean mlgf_BuddyTest(Four, One, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*, One*);
Four mlgf_ChangeLeafEntrySize(Four, mlgf_LeafPage*, One, Two, Two, Two, Two);
Boolean mlgf_CommonRegionTest(Four, One, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*);
void mlgf_CompactLeafPage(Four, mlgf_LeafPage*,One, Two, Two);
Four mlgf_CreateOverflow(Four, ObjectID*, PageID*, mlgf_LeafPage*, MLGF_KeyDesc*, Two); 
Four mlgf_DeleteFromDirectory(Four, mlgf_DirectoryPage*, MLGF_KeyDesc*, Two);
Four mlgf_DeleteObjectFromLeaf(Four, PageID*, mlgf_LeafPage*, MLGF_KeyDesc*, MLGF_HashValue[], 
			       ObjectID*, char*, mlgf_DirectoryEntry*, Pool*, DeallocListElem*, mlgf_DeleteStatus_T*);
Four mlgf_DeleteOverflow(Four, PageID*, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], 
                         ObjectID*, char*, Four*, Pool*, DeallocListElem*);
Four mlgf_DumpDirectoryPage(Four, PageID*);
Four mlgf_DumpLeafPage(Four, PageID*, MLGF_KeyDesc*);
Four mlgf_DumpOverflowPage(Four, PageID*, MLGF_KeyDesc*);
Boolean mlgf_EqualKeys(Four, MLGF_KeyDesc*, MLGF_HashValue[], MLGF_HashValue[]);
Boolean mlgf_FindEntry(Four, mlgf_DirectoryPage*, One, MLGF_HashValue[], Two*);
Boolean mlgf_FindBuddyEntry(Four, mlgf_DirectoryPage*, One, mlgf_DirectoryEntry*, Two*, One*);
void mlgf_GetMaxRegion(Four, MLGF_KeyDesc*, mlgf_DirectoryPage*, mlgf_DirectoryEntry*, MLGF_HashValue[], mlgf_DirectoryEntry*);
Four mlgf_GetMortonValue(Four, MLGF_HashValue[], One[], mlgf_MortonValue*, One);
Four mlgf_CompareMortonValue(Four, mlgf_MortonValue*, mlgf_MortonValue*, Boolean); 
Four mlgf_GetObjectFromOverflow(Four, PageID*, MLGF_KeyDesc*, ObjectID*, char*);
Four mlgf_MaximizeRegions(Four, mlgf_DirectoryPage*, MLGF_KeyDesc*, mlgf_DirectoryEntry*);

Four mlgf_InsertIntoDirectory(Four, mlgf_DirectoryPage*, MLGF_KeyDesc*, mlgf_DirectoryEntry*);
Four mlgf_InsertIntoLeaf(Four, ObjectID*, PageID*, mlgf_LeafPage*, MLGF_KeyDesc*, MLGF_HashValue*, 
                         ObjectID*, char*, mlgf_DirectoryEntry*, mlgf_InsertStatus_T*);
Four mlgf_InsertOverflow(Four, ObjectID*, PageID*, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, char*); 
Boolean mlgf_IsContained(Four, MLGF_KeyDesc*, MLGF_HashValue*, MLGF_HashValue*, mlgf_DirectoryEntry*);
Four mlgf_IsTemporary(Four, ObjectID*, Boolean*); 
Four mlgf_MergeDirectoryPage(Four, mlgf_DirectoryPage*, MLGF_KeyDesc*, Two, Pool*, DeallocListElem*);
Four mlgf_MergeLeafPage(Four, mlgf_DirectoryPage*, MLGF_KeyDesc*, Two, Pool*, DeallocListElem*);
Four mlgf_ObjectIdComp(Four, ObjectID*, ObjectID*);
Boolean mlgf_SearchDirPageInMortonOrder(Four, mlgf_DirectoryPage*, MLGF_KeyDesc*, mlgf_MortonValue*, Boolean, Two*); 
Boolean mlgf_SearchLeafPageInMortonOrder(Four, mlgf_LeafPage*, MLGF_KeyDesc*, mlgf_MortonValue*, Two*);
Four mlgf_SplitDirectoryPage(Four, ObjectID*, PageID*, MLGF_KeyDesc*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*);
void mlgf_SplitDirectoryRegion(Four, mlgf_DirectoryPage*, One, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*, One*);
Four mlgf_SplitLeafPage(Four, ObjectID*, PageID*, MLGF_KeyDesc*, MLGF_HashValue*, ObjectID*, char*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*);
void mlgf_SplitLeafRegion(Four, mlgf_LeafPage*, MLGF_KeyDesc*, MLGF_HashValue[], mlgf_DirectoryEntry*, mlgf_DirectoryEntry*, One*);

/* MLGF Interface Function Prototypes */
Four MLGF_CreateIndex(Four, ObjectID*, MLGF_KeyDesc*, PageID*);
Four MLGF_DeleteObject(Four, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, Pool*, DeallocListElem*);
Four MLGF_DropIndex(Four, PhysicalFileID*, PageID*, Pool*, DeallocListElem*);
char *MLGF_Err(Four);
Four MLGF_Fetch(Four, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], MLGF_HashValue[], MLGF_Cursor*, char*);
Four MLGF_FetchNext(Four, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], MLGF_HashValue[], MLGF_Cursor*, char*);
Four MLGF_InsertObject(Four, ObjectID*, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, char*);
Four MLGF_SearchNearObject(Four, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*);

#endif /* _MLGF_INTERNAL_H_ */
