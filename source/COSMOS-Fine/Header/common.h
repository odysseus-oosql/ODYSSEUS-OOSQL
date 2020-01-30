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
#ifndef _COMMON_H_
#define _COMMON_H_

#include <stdio.h>
#include "param.h"				
#include "basictypes.h"			
#include "primitivetypes.h"                   
#include "Util_varArray.h"

#ifdef FALSE
#undef FALSE
#endif

#ifdef TRUE
#undef TRUE
#endif

#ifndef WIN32
typedef int FileDesc;
#else
typedef void* FileDesc;
#endif /* WIN32 */

/* Boolean Type */
typedef enum { FALSE, TRUE } Boolean;

/* Comparison Operator */
/* WARNING: DO NOT change the number. The numbers have some meanings; bit properties. */
typedef enum {SM_EQ=0x1, SM_LT=0x2, SM_LE=0x3, SM_GT=0x4, SM_GE=0x5, SM_NE=0x6, SM_EOF=0x10, SM_BOF=0x20} CompOp;

#define BEGIN_MACRO do {
#define END_MACRO } while(0)

/* number of bits per byte */
#define BITSPERBYTE     8

#define OFFSET_OF(type, mem) \
((size_t)((char *)&((type *) 0)->mem - (char *)((type *) 0)))

#define CEIL_AFTER_DIVIDE(x,y) (((x) + (y) - CONSTANT_ONE)/(y))

#undef NIL
#define NIL -1			/* special value meaning "end of list", */
				/* "currently not used", ... */

/* catalog tables for the temporary files */
#define INIT_SM_ST_FOR_TMP_FILES 3
#define INIT_SM_SI_FOR_TMP_FILES 3
#define INIT_LRDS_ST_FOR_TMP_FILES INIT_SM_ST_FOR_TMP_FILES
#define INIT_LRDS_SI_FOR_TMP_FILES INIT_SM_SI_FOR_TMP_FILES



/* Return value */
#define EOS    1		/* end of the scan */


/* special parameter values */
#define REMAINDER -1
#define END       -2

/* bit mask for temporary volume id */
#define TEMP_VOLID_MASK             (CONSTANT_ONE<<(sizeof(Two)*BITSPERBYTE-2))
#define IS_TEMP_VOLUME(volNo)       (((volNo) & TEMP_VOLID_MASK)?1:0)

/*
 * Data Type Supported by the B+ tree
 */
#define SM_DESC         0x80    /* special flag: indicates descending order */
                                /* used by ORing with other data type */
                                /* if not specified, ascending order is default
*/
#define SM_TYPE_MASK    0x7F    /* mask for extracting the data type value */

#define SM_SHORT        	0
#define SM_INT          	1
#define SM_LONG         	2
#define SM_FLOAT        	3
#define SM_DOUBLE       	4
#define SM_STRING       	5	/* fixed-length string */
#define SM_VARSTRING    	6	/* variable-length string */
#define SM_PAGEID       	7	/* PageID type */
#define SM_FILEID       	8       /* FileID type */
#define SM_INDEXID      	9       /* IndexID type */
#define SM_OID			10	/* OID(volume no, page no, slot no, unique no, class id) type */
					/* NOTICE: OID is different with ObjectID */
#define SM_TEXT			11	/* Text Type */
#define SM_MBR			12	/* MBR Type */
#define SM_OBJECT_ID    	13  /* ObjectID Type */
#define SM_LONG_LONG    	14 
#define SM_SHORT_SIZE   	sizeof(Two_Invariable)
#define SM_INT_SIZE     	sizeof(Four_Invariable)
#define SM_LONG_SIZE    	sizeof(Four_Invariable)
#define SM_FLOAT_SIZE   	sizeof(float)
#define SM_DOUBLE_SIZE  	sizeof(double)
#define SM_PAGEID_SIZE  	sizeof(PageID)
#define SM_INDEXID_SIZE 	sizeof(IndexID)
#define SM_FILEID_SIZE  	sizeof(FileID)
#define SM_OID_SIZE		sizeof(OID) 
#define SM_MBR_SIZE		(MBR_NUM_PARTS*sizeof(MLGF_HashValue)) 
#define SM_OBJECT_ID_SIZE 	sizeof(ObjectID) 
#define SM_LONG_LONG_SIZE    	sizeof(Eight_Invariable)

/*
 * Shared memory access policy : logical/physical pointer
 */
#ifdef USE_LOGICAL_PTR

#define SHM_BASE_ADDRESS (shmPtr) 
extern struct SemStruct_tag *shmPtr;

#ifdef CHECK_LOGICAL_PTR

/* This section is for compilation check. Do not use as a binary. */

typedef struct {MEMORY_ALIGN_TYPE i;} LogicalPtr_T;

extern LogicalPtr_T dummyNullLogicalPtr;

#define LOGICAL_PTR_TYPE(_type) LogicalPtr_T
#define LOGICAL_PTR(_p) (dummyNullLogicalPtr)
#define PHYSICAL_PTR(_p) ((void*)((_p.i) + (MEMORY_ALIGN_TYPE)SHM_BASE_ADDRESS))
#define NULL_LOGICAL_PTR (dummyNullLogicalPtr)

#else /* CHECK_LOGICAL_PTR */

typedef MEMORY_ALIGN_TYPE LogicalPtr_T;

#define LOGICAL_PTR_TYPE(_type) LogicalPtr_T
#define LOGICAL_PTR(_p) ((_p == NULL) ? NULL_LOGICAL_PTR : ((LogicalPtr_T)(_p) - (LogicalPtr_T)SHM_BASE_ADDRESS))
#define PHYSICAL_PTR(_p) ((_p == NULL_LOGICAL_PTR) ? (void*)NULL : ((void*)((LogicalPtr_T)(_p) + (LogicalPtr_T)SHM_BASE_ADDRESS)))
#define NULL_LOGICAL_PTR ((LogicalPtr_T)0x0)

#endif /* CHECK_LOGICAL_PTR */

#else /* USE_LOGICAL_PTR */

/* Use physical address. */

#define LOGICAL_PTR_TYPE(_type) _type
#define LOGICAL_PTR(_p) (_p)
#define PHYSICAL_PTR(_p) (_p)
#define NULL_LOGICAL_PTR NULL

#endif /* USE_LOGICAL_PTR */

/*
 * Configuration Parameters
 */
typedef struct CfgParams_T_tag {
    char logVolumeDeviceList[MAX_DEVICE_NAME_SIZE*MAX_DEVICES_IN_LOG_VOLUME]; /* log device name */
} CfgParams_T;


/*
** Type Definition of System(OS) type
*/
typedef unsigned int Seed;


/*
** Type Definition of PageID
*/
typedef Four PageNo;
typedef Two VolNo;
typedef VolNo VolID;

typedef struct {
    PageNo pageNo;		/* a PageNo */
    VolNo volNo;		/* a VolNo */
} PageID;

/* construct page ID from the given volNo & pageNo */
#define SET_NILPAGEID(x)   ((x).pageNo = NIL)
#define EQUAL_PAGEID(x, y)					\
	(((x).volNo == (y).volNo && (x).pageNo == (y).pageNo) ? TRUE:FALSE)
#define IS_NILPAGEID(x)    (((x).pageNo == NIL) ? TRUE:FALSE)
#define MAKE_PAGEID(pid, volume, page)	\
    (pid).volNo = (volume),			\
    (pid).pageNo = (page)

/* Print Page ID : x is the variable name, y is the PageID */
#define PRINT_PAGEID(x, y)	\
    (y == NULL) ? printf("%s = NULL", (x)) :	\
    printf("%s = {%-2ld, %4ld}", (x), (y)->volNo, (y)->pageNo )


/*
** ShortPageID : referenc a page within a given volume
*/
typedef PageNo ShortPageID;


/*
** Type Definition for Train
*/
typedef PageID TrainID;		/* use its first page's PageID as the TrainID */

#define PRINT_TRAINID(x,y) PRINT_PAGEID(x,y)
#define MAKEI_TRAINID(fid,v,p) MAKE_PAGEID(fid,v,p)

#define TRAINID_SIZE 	PAGEID_SIZE
#define EQUAL_TRAINID 	EQUAL_PAGEID
#define CLEAR_TRAINID	CLEAR_PAGEID


/*
** Typedefs for ObjectID
*/
typedef UFour Unique;
typedef Two SlotNo;

typedef struct {		/* ShortObjectID is used within a volume */
    PageNo	page;
    SlotNo	slot;
    Unique	unique;
} ShortObjectID;

typedef struct {		/* ObjectID is used accross the volumes */
    PageNo pageNo;		/* specify the page holding the object */
    VolID  volNo;		/* specify the volume in which object is in */
    SlotNo slotNo;		/* specify the slot within the page */
    Unique unique;		/* Unique No for checking dangling object */
} ObjectID;

#define MAKE_OBJECTID(oid, v, p, s, u) \
(oid).volNo = (v), (oid).pageNo = (p), (oid).slotNo = (s), (oid).unique = (u)

#define EQUAL_OBJECTID(x, y) \
((x).volNo == (y).volNo && (x).pageNo == (y).pageNo && (x).slotNo == (y).slotNo)

#define SET_NILOBJECTID(x) ((x).pageNo = NIL)

#define IS_NILOBJECTID(x) ((x).pageNo == NIL)

#define PRINT_OBJECTID(x, oid) \
    (oid == NULL) ? printf("%s = NULL", (x)) :    \
    printf("%s = {%-2ld,%4ld,%4ld,%4ld}", (x), (oid)->volNo, (oid)->pageNo, (oid)->slotNo, (oid)->unique )



/*
 *  Definition for Logical ID
 */

typedef Four Serial;
typedef struct {
    Serial serial;      /* a logical serial number */
    VolNo volNo;        /* a VolNo */
} LogicalID;

#define SET_NILLOGICALID(x)   ((x).serial = NIL)
#define EQUAL_LOGICALID(x, y)                  \
	(((x).volNo == (y).volNo && (x).serial == (y).serial) ? TRUE:FALSE)
#define IS_NILLOGICALID(x)    (((x).serial == NIL) ? TRUE:FALSE)
#define MAKE_LOGICALID(fid, v, s)  \
	(fid).volNo = (v), (fid).serial = (s)
#define PRINT_LOGICALID(x, y)  \
	(y == NULL) ? printf("%s = NULL", (x)) :    \
	printf("%s = {%-2ld, %4ld}", (x), (y)->volNo, (y)->serial )
#define LOGICALID_SIZE  sizeof(LogicalID)


/*
 *  Definition for Physical File ID
 */
typedef PageID  PhysicalFileID;     /* use the first Page's PageID as the physical file ID */

#define MAKE_PHYSICALFILEID(fid,v,p) MAKE_PAGEID(fid,v,p)


/*
 *  Definition for File ID
 */

typedef LogicalID FileID;

#define SET_NILFILEID(x)     SET_NILLOGICALID(x)
#define EQUAL_FILEID(x,y)    EQUAL_LOGICALID(x, y)
#define IS_NILFILEID(x)      IS_NILLOGICALID(x)
#define MAKE_FILEID(fid,v,p) MAKE_LOGICALID(fid,v,p)
#define PRINT_FILEID(x,y)    PRINT_LOGICALID(x,y)
#define FILEID_SIZE          LOGICALID_SIZE


/*
 *  Definition for Physical Index ID
 */
typedef PageID  PhysicalIndexID;  /* use the root Page's PageID as the physical index ID */

#define MAKE_PHYSICALINDEXID(iid,v,p) MAKE_PAGEID(iid,v,p)
#define IS_NILPHYSICALINDEXID(x)      IS_NILPAGEID(x)
#define SET_NILPHYSICALINDEXID(x)     SET_NILPAGEID(x)
#define PRINT_PHYSICALINDEXID(x,y)    PRINT_PAGEID(x,y)
#define EQUAL_PHYSICALINDEXID(x,y)    EQUAL_PAGEID(x, y)


/*
** Type definition for IndexID
*/

typedef LogicalID IndexID;

#define SET_NILINDEXID(x)     SET_NILLOGICALID(x)
#define EQUAL_INDEXID(x,y)    EQUAL_LOGICALID(x, y)
#define IS_NILINDEXID(x)      IS_NILLOGICALID(x)
#define MAKE_INDEXID(fid,v,p) MAKE_LOGICALID(fid,v,p)
#define PRINT_INDEXID(x,y)    PRINT_LOGICALID(x,y)
#define INDEXID_SIZE          LOGICALID_SIZE


/*
** Type definition for OID
*/
typedef Four ClassID;

typedef struct {		/* OID is used accross the volumes */
    PageNo pageNo;		/* specify the page holding the object */
    VolID  volNo;		/* specify the volume in which object is in */
    SlotNo slotNo;		/* specify the slot within the page */
    Unique unique;		/* Unique No for checking dangling object */
    ClassID classID;		/* specify the class including the object */
} OID;

#define MAKE_NULL_OID(oid) ((oid).classID = -1)
#define IS_NULL_OID(oid)   ((oid).classID == -1)


/*
** log sequence number
*/
typedef struct Lsn_T_tag {
    UFour offset;               /* byte position in a log volume */
    UFour wrapCount;            /* # of wrapping around a log volume */
} Lsn_T;

/*
 * wrapCount 0 is reserved for the special meaning LSNs.
 */
#define LSN_STARTING_WRAP_COUNT  	CONSTANT_ONE
#define LSN_MAX_OFFSET       		(CONSTANT_ALL_BITS_SET(UFour))
#define LSN_MAX_WRAP_COUNT   		(CONSTANT_ALL_BITS_SET(UFour))

#define SET_NIL_LSN(lsn) ((lsn).wrapCount = (lsn).offset = 0)
#define IS_NIL_LSN(lsn)  ((lsn).wrapCount == 0 && (lsn).offset == 0)

#define SET_MIN_LSN(lsn) ((lsn).wrapCount = 0, (lsn).offset = 1)
#define SET_MAX_LSN(lsn) ((lsn).wrapCount = LSN_MAX_WRAP_COUNT, (lsn).offset = LSN_MAX_OFFSET)
#define SET_STARTING_LSN(lsn) ((lsn).wrapCount = LSN_STARTING_WRAP_COUNT, (lsn).offset = 0)

#define LSN_CMP_EQ(x,y) \
((x).wrapCount == (y).wrapCount && (x).offset == (y).offset)
#define LSN_CMP_LT(x,y) \
((x).wrapCount < (y).wrapCount || ((x).wrapCount == (y).wrapCount && (x).offset < (y).offset))
#define LSN_CMP_GT(x,y) \
((x).wrapCount > (y).wrapCount || ((x).wrapCount == (y).wrapCount && (x).offset > (y).offset))
#define LSN_CMP_LE(x,y) \
((x).wrapCount < (y).wrapCount || ((x).wrapCount == (y).wrapCount && (x).offset <= (y).offset))
#define LSN_CMP_GE(x,y) \
((x).wrapCount > (y).wrapCount || ((x).wrapCount == (y).wrapCount && (x).offset >= (y).offset))

#define INCREASE_LSN_BY_ONE(x)	if (++((x).offset) == 0) ((x).wrapCount)++ 

/*
** Type Definition for Savepoint ID
*/
typedef Lsn_T SavepointID;


/* Page Type */
/* typedef struct { */
/*    char data[PAGESIZE]; */
/* } Page; */

/*
       < Usage of 'flags' in 'PageHdr' >

       --------------------------------
 flags |          unused        |A| B |
       --------------------------------
       A (1bit): 'temp vector' which indicates page is temporary or not
       B (4bit): 'type vector' which indicates page type
       unused (27bit)
*/

/*
** Common Page
*/
/*
 * Page
 */
typedef struct PageHdr_T_tag {
    PageID pid;                 /* page id of this page */
    Four flags;
    Four reserved;
    Lsn_T lsn;                  /* page lsn */
    Four logRecLen;             /* log record length */
} PageHdr_T;

typedef struct Page_T_tag {
    PageHdr_T header;
    char data[PAGESIZE-sizeof(PageHdr_T)];
} Page_T;


/*
** Type Vector indicates page's type
*/

#define PAGE_TYPE_VECTOR_MASK       	0xf
#define PAGE_TYPE_VECTOR_RESET_MASK 	(CONSTANT_ALL_BITS_SET(Four)-PAGE_TYPE_VECTOR_MASK)

#define UNKNOWN_PAGE_TYPE     0x0
#define FREE_PAGE_TYPE        0x1
#define SLOTTED_PAGE_TYPE     0x2
#define LOT_I_NODE_TYPE       0x3
#define LOT_L_NODE_TYPE       0x4
#define BTREE_PAGE_TYPE       0x5
#define MLGF_PAGE_TYPE        0x6
#define EXT_ENTRY_PAGE_TYPE   0x7
#define BITMAP_TRAIN_TYPE     0x8
#define MASTER_PAGE_TYPE      0x9
#define VOL_INFO_PAGE_TYPE    0xa
#define META_DIC_PAGE_TYPE    0xb
#define UNIQUE_NUM_PAGE_TYPE  0xc
#define LOG_MASTER_PAGE_TYPE  0xd
#define LOG_PAGE_TYPE         0xe

#define RESET_PAGE_TYPE(page)       	(((Page_T *)(page))->header.flags &= PAGE_TYPE_VECTOR_RESET_MASK)
#define SET_PAGE_TYPE(page, type)   	(RESET_PAGE_TYPE(page), ((Page_T *)(page))->header.flags |= type)
#define GET_PAGE_TYPE(page)   		(((Page_T *)(page))->header.flags & PAGE_TYPE_VECTOR_MASK)

/*
** Temp Vector indicates that page is included in temporary file or not
*/

#define PAGE_TEMP_VECTOR_MASK       	0x10
#define PAGE_TEMP_VECTOR_RESET_MASK 	(CONSTANT_ALL_BITS_SET(Four)-PAGE_TEMP_VECTOR_MASK)

#define IS_TEMP_PAGE(page)    		(((Page_T *)(page))->header.flags & PAGE_TEMP_VECTOR_MASK)
#define SET_TEMP_PAGE_FLAG(page)   	(((Page_T *)(page))->header.flags |= PAGE_TEMP_VECTOR_MASK)
#define RESET_TEMP_PAGE_FLAG(page) 	(((Page_T *)(page))->header.flags &= PAGE_TEMP_VECTOR_RESET_MASK)



/*
 * Typedef for generic object header
 *
 * Be CAREFUL: The fields must match the initial fields of
 *			   SMALLOBJHDR and LARGEOBJHDR.
 */
typedef struct {
    Two	 properties;		/* the properties bit vector */
    Two	 tag;			/* the object's tag */
    Four length;		/* the object's data size */
} ObjectHdr;

/*
 * 'properties' bits; only a few now
 *
 * The 'properties' field is used to record the object's properties.
 */
#define P_CLEAR              0x0 /* clear all bits to 0 */
#define P_LRGOBJ	     0x1 /* whether this is a large object */
#define P_LRGOBJ_ROOTWITHHDR 0x2 /* large object header is on the page */
#define P_MOVED		     0x4 /* object has been moved to a new page */
#define P_FORWARDED	     0x8 /* this is the forwarded record */

/*
 *-------------- Type definitions for Storage Objects --------------
 */

/* object length's minimum limit */
/* When only the Small object */
#define MIN_OBJECT_DATA_SIZE sizeof(ObjectID)
/* When use the large object tree */
/* #define MIN_OBJECT_DATA_SIZE sizeof(ShortPageID) */

/*
 * Typedef for Small object
 *
 * Small object is one which can be stored within one page
 */
typedef struct {
    ObjectHdr   header;         /* the object's header */
    char        data[MIN_OBJECT_DATA_SIZE]; /* data area */
    /*
     * Making the data area is required to enforce
     * the requirement that objects at least be large
     * enough to hold a large object header (LARGEOBJHDR).
     */
} SmallObject;

/*
 * Typedef for generic object.
 * This is used to determine what type of object we are dealing with.
 * Once the object type is determined, then either the SMALLOBJ or
 * LARGEOBJ typedef is used, depending, of course, on whether we are
 * dealing with a large or small object. For now the basic structure
 * of an object is that of a small object. In case of a large object
 * the data area is used to hold control information.
 */
typedef SmallObject             Object;

/*
** Btree Related Types
*/
/* a Btree key value */
typedef struct {
    Two len;
    char val[MAXKEYLEN];
} KeyValue;

/* key part */
typedef struct {
    Two   type;			/* VARIABLE or FIXED */
    Two   offset;		/* where ? */
    Two   length;		/* how ?   */
} KeyPart;

/* key descriptor */
typedef struct {
    Two   flag;			/* flag for some more informations */
    Two   nparts;		/* the number of key parts */
    KeyPart kpart[MAXNUMKEYPARTS]; /* eight key parts */
} KeyDesc;

#define KEYDESC_USED_SIZE(_kdesc) (OFFSET_OF(KeyDesc, kpart[(_kdesc)->nparts]))
#define KEYFLAG_UNIQUE 0x1
#define KEYFLAG_CLUSTERING 0x2

/*
** Type Definition for SortKeyDesc
*/
#define SORTKEYDESC_ATTR_ORDER   0x3 /* attribute ORDER mask */
#define SORTKEYDESC_ATTR_ASC     0x2 /* ascending order */
#define SORTKEYDESC_ATTR_DESC    0x1 /* descending order */
typedef struct {
    Two flag;                   /* UNIQUE, ... */
    Two nparts;                 /* # of key parts */
    struct {
        Four attrNo;
        Four flag;              /* ascending/descendig */
    } parts[MAXNUMKEYPARTS];
} SortKeyDesc;

/*
 * MLGF Index Related Types
 */
typedef UFour_Invariable        MLGF_HashValue;
#define MLGF_MIN_HASHVALUE 	0
#define MLGF_MAX_HASHVALUE 	((MLGF_HashValue)(CONSTANT_ALL_BITS_SET(MLGF_HashValue)))
#define MLGF_RADIX 10

typedef struct {
    One 		flag;			/* flag */
    One 		nKeys;			/* number of keys */
    Two 		extraDataLen;		/* length of the extra data for an object */
    MLGF_HashValue 	minMaxTypeVector;	/* bit vector of flags indicating MIN/MAX of MBR for each attribute */
} MLGF_KeyDesc;

#define MLGF_KEYDESC_CLEAR_MINMAXTYPEVECTOR(kdesc) \
((kdesc).minMaxTypeVector = 0)

#define MLGF_KEYDESC_SET_MINTYPE(kdesc, i) \
((kdesc).minMaxTypeVector |= (((MLGF_HashValue)CONSTANT_ONE)<<(sizeof(MLGF_HashValue)*CHAR_BIT-1)) >> (i))

#define MLGF_KEYDESC_SET_MAXTYPE(kdesc, i) \
((kdesc).minMaxTypeVector &= ~((((MLGF_HashValue)CONSTANT_ONE)<<(sizeof(MLGF_HashValue)*CHAR_BIT-1)) >> (i)))

#define MLGF_KEYDESC_IS_MINTYPE(kdesc, i) \
((kdesc).minMaxTypeVector & ((((MLGF_HashValue)CONSTANT_ONE)<<(sizeof(MLGF_HashValue)*CHAR_BIT-1)) >> (i)))

#define MLGF_KEYDESC_IS_MAXTYPE(kdesc, i) \
(!((kdesc).minMaxTypeVector & ((((MLGF_HashValue)CONSTANT_ONE)<<(sizeof(MLGF_HashValue)*CHAR_BIT-1)) >> (i))))



/*
** Cursor definition
*/
/* AnyCursor:
 *  All cursors should have the following members at the front of them
 *  in the same order.
 */
typedef struct {
    One flag;			/* state of the cursor */
				/* CURSOR_INVALID, CURSOR_BOS, CURSOR_ON, CURSOR_EOS */
    ObjectID oid;		/* object pointed by the cursor */
} AnyCursor;

/* DataCursor:
 *  sequential scan using the data file
 */
typedef struct {
    One      flag;			/* state of the cursor */
    ObjectID oid;		/* object pointed by the cursor */
} DataCursor;

/* BtreeCursor:
 *  scan using a B+ tree
 */
typedef struct {
    One      flag;		/* state of the cursor */
    ObjectID oid;		/* object pointed by the cursor */
    KeyValue key;		/* what key value? */
    PageID   leaf;		/* which leaf page? */
    PageID   overflow;		/* which overflow page? */
    Two      slotNo;		/* which slot? */
    Two      oidArrayElemNo;	/* which element of the object array? */
#ifdef CCRL
    Lsn_T leafLsn;              /* LSN of the leaf page */
    Lsn_T overflowLsn;          /* LSN of the overflow page */
#endif /* CCRL */
} BtreeCursor;

typedef struct {
    One      flag;		/* state of the cursor */
    ObjectID oid;		/* object pointed by the cursor */
    MLGF_HashValue keys[MLGF_MAXNUM_KEYS]; /* what key values? */
    PageID   leaf;		/* which leaf page? */
    PageID   overflow;		/* which overflow page? */
    Two      entryNo;		/* which entry? */
    Two      oidArrayElemNo;	/* which element of the object array? */
    Two      pathTop;			   /* top of path stack */
    VarArray path;			   /* traverse path from root to leaf */
} MLGF_Cursor;

/* Universal Cursor */
typedef union {
    AnyCursor any;		/* for access of 'flag' and 'oid' */
    DataCursor seq;		/* sequential scan */
    BtreeCursor btree;		/* scan using a B+ tree */
    MLGF_Cursor mlgf;		/* scan using MLGF index */
} Cursor;

/* values of 'flag' field; cursor status */
#define CURSOR_INVALID 0	/* invalid cursor */
#define CURSOR_BOS     1	/* begion of scan */
#define CURSOR_ON      2	/* cursor points an object. */
#define CURSOR_EOS     3	/* end of scan */

/*
 * structure of a segment identifier
 */
typedef struct SegmentID_T_tag {
    VolNo       volNo;          /* volume identification */
    Four        firstExtent;    /* 1st extent No. of extent list */
    Four        sizeOfTrain;    /* size of train in extent */
} SegmentID_T;

/*
** Main Memory Data Structure of Scan Manager Catalog Table SM_SYSTABLES
*/
typedef struct {
    FileID      	fid;			/* data file's file identifier */
    ShortPageID 	firstPage;  		/* data file's first page No */ 
    ShortPageID 	lastPage;		/* data file's last page No */
    SegmentID_T 	pageSegmentID;		/* data file's page segment ID */
    SegmentID_T 	trainSegmentID;		/* data file's train segment ID */
} sm_CatOverlayForData;

typedef struct {
    sm_CatOverlayForData  data;
} sm_CatOverlayForSysTables;

/*
** Main Memory Data Structure of Scan Manager Catalog Table SM_SYSINDEXES
*/
#define SM_INDEXTYPE_BTREE 1
#define SM_INDEXTYPE_MLGF  2
typedef struct {
    FileID 		dataFid;		/* FileID of the data file related to B+ tree */
    IndexID 		iid;			/* IndexID of a B+ tree or MLGF*/
    ShortPageID 	rootPage;		/* index root's page No */ 
    SegmentID_T 	pageSegmentID;		/* page segment ID of B+ tree or MLGF */ 
    /* key descriptor is added for MLGF index */
    /* SM_DestroyFile() needs key descriptor for MLGF index */
    /* when it calls MLGF_DropIndex(). */
    union {
	KeyDesc 	btree;			/* RESERVED: Key Description for Btree Index */
	MLGF_KeyDesc 	mlgf;			/* Key Description for MLGF Index */
    } kdesc;					/* key descriptor */
    One 		indexType;		/* type of index */
} sm_CatOverlayForSysIndexes;


#define SM_COUNTER_NAME_MAX_LEN     MAXRELNAME

typedef ObjectID CounterID;

typedef struct sm_SysCountersOverlay_T_tag {
    char counterName[SM_COUNTER_NAME_MAX_LEN];
    Four counterValue;
} sm_SysCountersOverlay_T;


/* Type Definition for Transaction Identifier */
typedef struct {		/* 8 byte unsigned integer */
    UFour high;
    UFour low;
} XactID;

#define XACTID_CMP_EQ(x,y) \
((x).high == (y).high && (x).low == (y).low)
#define XACTID_CMP_LT(x,y) \
((x).high < (y).high || ((x).high == (y).high && (x).low < (y).low))
#define XACTID_CMP_GT(x,y) \
((x).high > (y).high || ((x).high == (y).high && (x).low > (y).low))
#define XACTID_CMP_LE(x,y) \
((x).high < (y).high || ((x).high == (y).high && (x).low <= (y).low))
#define XACTID_CMP_GE(x,y) \
((x).high > (y).high || ((x).high == (y).high && (x).low >= (y).low))

#define SET_NIL_XACTID(x)	((x).high = 0, (x).low = 0)
#define SET_MIN_XACTID(x)	((x).high = 0, (x).low = 1)
#define SET_STARTING_XACTID(x)	((x).high = 0, (x).low = 2)
#define SET_MAX_XACTID(x)	((x).high = (CONSTANT_ALL_BITS_SET(UFour)), (x).low = (CONSTANT_ALL_BITS_SET(UFour)))

#define IS_NIL_XACTID(x)	((x).high == 0 && (x).low == 0)
#define IS_MIN_XACTID(x)	((x).high == 0 && (x).low == 1)
#define IS_MAX_XACTID(x)	((x).high == (CONSTANT_ALL_BITS_SET(UFour)) && (x).low == (CONSTANT_ALL_BITS_SET(UFour)))
#define ASSIGN_XACTID(x1, x2)	((x1).high = (x2).high, (x1).low = (x2).low)
#define INCREASE_XACTID(x)	if (++((x).low) == 0) ((x).high)++
#define PRINT_XACTID(x)		printf("%ld,%ld ", (x).high, (x).low)
#define EQUAL_XACTID(x1, x2)    XACTID_CMP_EQ(x1, x2)


/*
 * Global transaction id.
 */
typedef char GlobalXactID[MAX_GLOBAL_XACTID_LEN];

#define GLOBALXACTID_CMP_EQ(_t1, _t2) (memcmp(_t1, _t2, sizeof(GlobalXactID)) == 0)

/*
 * Lock Mode
 */

/* type definition for access lock */

typedef enum { L_OBJECT, L_PAGE, L_KEYVALUE, L_FLAT_OBJECT, L_FLAT_PAGE, L_FILE} LockLevel; /* lock hierarchy */ 


typedef enum { L_NL, L_IS, L_IX, L_S, L_SIX, L_X } LockMode; /* lock mode */

typedef enum { L_INSTANT, L_MANUAL, L_COMMIT } LockDuration; /* lock duration */

typedef enum { LR_NL, LR_IS, LR_IX, LR_S, LR_SIX, LR_X, LR_NOTOK, LR_DEADLOCK } LockReply; /* return value of lock request */

typedef enum { L_GRANTED, L_WAITING, L_CONVERTING, L_DENIED } LockStatus; /* lock acquire status */

typedef enum { L_CONDITIONAL, L_UNCONDITIONAL} LockConditional; /* conditional/unconditional lock */

typedef enum { X_NORMAL, X_WAITING, X_CONVERTING, X_DEADLOCK, X_PREPARE, X_COMMIT, X_ABORT } XactStatus; /* transaction status */

/* Original ConcurrencyLevel type leaves out one level, X_BROWSE_BROWSE.
   So, add X_BROWSE_BROWSE level. */
typedef enum { X_BROWSE_BROWSE, X_CS_BROWSE, X_CS_CS, X_RR_BROWSE, X_RR_CS, X_RR_RR } ConcurrencyLevel; /* isolation degree */

typedef struct {
    LockMode mode;
    LockDuration duration;
} LockParameter;

#define SET_ISOLATION_DEGREE(_lu, _idLevel) ((_lu)->iDegree = (_idLevel)) 

#define ACTION_ON(_handle)      (perThreadTable[_handle].lmDS.LM_autoActionFlag) 
#define USER_ACTION_ON(_handle) (perThreadTable[_handle].lmDS.LM_actionFlag) 
#define USER_ACTION  1 
#define AUTO_ACTION  2 

#define MY_CC_LEVEL(_handle) (perThreadTable[_handle].tmDS.myCCLevel) 


/*
 * LOG
 */ 

#define LOG_MAX_NUM_IMAGES 4

/*
 * Type Definition for log data of log records
 */
typedef struct LOG_Data_T {
    One nImages;                /* # of log images */
    Two imageSize[LOG_MAX_NUM_IMAGES]; /* log image size */
    void *imageData[LOG_MAX_NUM_IMAGES]; /* log image data */
} LOG_Data_T;


/*
 * LOG_LogRecInfo_T
 */
typedef struct LOG_LogRecInfo_T_tag {
    One type;                   /* log record type */
    One action;                 /* log record action */
    One redoUndo;               /* redo, redo/undo, undo */
    One nImages;                /* # of images */
    XactID xactId;              /* transaction doing the update */
    PageID pid;                 /* updated page */
    Lsn_T prevLsn;              /* previous log record related to the transaction */
    Lsn_T undoNextLsn;          /* next undoable log record related to the transaction */
    Two  imageSize[LOG_MAX_NUM_IMAGES];  /* log image size */
    void *imageData[LOG_MAX_NUM_IMAGES]; /* log images */
} LOG_LogRecInfo_T;


/*
 * Logging Control Variable
 */
typedef enum LogFlag_T_tag {
    LOG_FLAG_ALL_CLEAR=0x0,     /* all clear */
    LOG_FLAG_VOLUME_SPACE_LOGGING=0x1,
    LOG_FLAG_DATA_LOGGING=0x2,
    LOG_FLAG_EXTENT_MAP_LOGGING=0x4, 
    LOG_FLAG_UNDO=0x8000
} LogFlag_T;

typedef struct LogParameter_T_tag {
    LogFlag_T logFlag;
    struct {
        Lsn_T undoLsn;          /* log record to undo */
        Lsn_T undoNextLsn;      /* log record to undo next time */
    } undo;
} LogParameter_T;

#define SET_LOG_PARAMETER(_logParam, _recoveryFlag, _tmpFileFlag) \
BEGIN_MACRO \
if (_recoveryFlag) (_logParam).logFlag = (_tmpFileFlag) ? LOG_FLAG_VOLUME_SPACE_LOGGING:(LOG_FLAG_VOLUME_SPACE_LOGGING | LOG_FLAG_DATA_LOGGING); \
else (_logParam).logFlag = LOG_FLAG_ALL_CLEAR; \
END_MACRO

/*
** Type Definition: DataFileInfo
**   This structure is used in OM(Object Manager).
**   Scan Manager fill and pass this structure into the OM.
*/
typedef struct {
    FileID fid;			/* Data File ID */
    Boolean tmpFileFlag;	/* TRUE if this file is the temporary file */
    union {
	ObjectID oid;		/* catalog entry for the file */
	sm_CatOverlayForSysTables *entry; /* catalog entry of SM_SYSTABLES */
    } catalog;
} DataFileInfo;

/*
** Type Definition: BtreeIndexInfo
**   This structure is used in BtM(Btree Manager).
**   Scan Manager fill and pass this structure into the BtM.
*/
typedef struct {
    IndexID iid;                /* Btree Index ID */
    Boolean tmpIndexFlag;	/* TRUE if this index is a temporary index */
    union {
	ObjectID oid;		/* catalog entry for this index */
	sm_CatOverlayForSysIndexes *entry; /* catalog entry of SM_SYSINDEXES */
    } catalog;
} BtreeIndexInfo;

/*
** Type Definition: MLGFIndexInfo
**   This structure is used in MLGF(MLGF Manager).
**   Scan Manager fill and pass this structure into the MLGF.
*/
typedef struct {    
    IndexID iid;                /* MLGF Index ID */
    Boolean tmpIndexFlag;	/* TRUE if this index is a temporary index */
    union {
	ObjectID oid;		/* catalog entry for this index */
	sm_CatOverlayForSysIndexes *entry; /* catalog entry of SM_SYSINDEXES */
    } catalog;
} MLGFIndexInfo;



/*
** Deallocated Page List
*/
typedef enum { DL_PAGE, DL_TRAIN, DL_DATAPAGE, DL_BTREEPAGE, DL_MLGFPAGE, DL_BTREEINDEX, DL_BTREE, DL_FILE, DL_MLGFINDEX, DL_LRGOBJ, DL_PAGE_SEGMENT, DL_TRAIN_SEGMENT} DLType;

struct _DeallocListElem {

	DLType type;
	union{
	    PageID 		pid;			/* page to be deallocated */
 	    PhysicalFileID 	pFid;			/* file which had the page before deallocation */
  	    PhysicalIndexID 	pIid;
	} elem;
	SegmentID_T		pageSegmentID;		/* page segment id to be dropped */
	SegmentID_T		trainSegmentID;		/* train segment id to be dropped */

    struct _DeallocListElem *next; /* pointer to next element */
};

typedef struct _DeallocListElem DeallocListElem;


/* Type for Statistics */
typedef struct {
    Four numBtrees;
    Four numMLGFs;
} sm_NumIndexes;

typedef struct {
    Four numTotalPages;
    Four numSlottedPage;
    Four numLOT_I_Node;
    Four numLOT_L_Node;
    Four numBtree_I_Node;
    Four numBtree_L_Node;
    Four numBtree_O_Node;
    Four numMLGF_I_Node;
    Four numMLGF_L_Node;
    Four numMLGF_O_Node;
    Four numExtEntryPage;
    Four numBitMapPage;
    Four numMasterPage;
    Four numVolInfoPage;
    Four numMetaDicPage;
    Four numUniqueNumPage;
} sm_NumPages;

typedef enum {
    SlottedPageType,
    LOT_I_NodeType,
    LOT_L_NodeType,
    Btree_I_NodeType,
    Btree_L_NodeType,
    Btree_O_NodeType,
    MLGF_I_NodeType,
    MLGF_L_NodeType,
    MLGF_O_NodeType,
    ExtEntryPageType,
    BitMapPageType,
    masterPageType,
    VolInfoPageType,
    MetaDicPageType,
    UniqueNumPageType
} sm_PageTypes;

typedef struct {
    sm_PageTypes type;                /* type of this page */
    Two          nSlots;              /* slots in use on the page */
    Two          free;                /* offset of contiguous free area on page */
    Two          unused;              /* number of unused bytes which are not part of the contiguous free area */
} sm_PageInfo;


/*
 * Error Handling
 */
/* Error Number Indicating NO ERROR */
#define eNOERROR 0

/*
** Macro Definitions
*/
#undef MIN
#define MIN(a,b) (((a) < (b)) ? (a):(b))

#undef MAX
#define MAX(a,b) (((a) >= (b)) ? (a):(b))

/*
** Calculate the alignment boundary
*/
#define ALIGN sizeof(ALIGN_TYPE)
#define ALIGNED_LENGTH(l) \
(((l)%ALIGN) ? ((l) - ((l)%ALIGN) + (((l) < 0) ? -ALIGN:ALIGN)) : (l))


/*** BEGIN_OF_SHM_RELATED_AREA ***/
/*
 * Shared Memory Structure
 */
typedef struct {
    CfgParams_T cfgParams;
    Boolean recoveryFlag;       /* Recovery Facility Turn On/Off */
} COMMON_SHM;

extern COMMON_SHM *common_shmPtr;
extern Four procIndex;


#define CFG_LOGVOLUMEDEVICELIST (common_shmPtr->cfgParams.logVolumeDeviceList)

/*** END_OF_SHM_RELATED_AREA ***/


#endif /* _COMMON_H_ */
