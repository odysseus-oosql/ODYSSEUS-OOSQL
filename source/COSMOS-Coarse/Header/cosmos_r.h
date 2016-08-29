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
#ifndef __COSMOS_R_H__
#define __COSMOS_R_H__

#include <stdio.h>
#include <semaphore.h>	
#include <pthread.h>
#include "param.h"

#ifdef  __cplusplus
extern "C" {
#endif

/*
 * Constants
 */
#ifdef PAGESIZE
#undef PAGESIZE
#endif
#define PAGESIZE 4096

/* maximum number of mounted volumes */
#define MAXNUMOFVOLS      20

/* maximum number of open relations */
#define MAXNUMOFOPENRELS    400			

/* maximum relation name */
#define MAXRELNAME 250

/* maximum number of columns */
#define MAXNUMOFCOLS 256

/* maximum number of indexes */
#define MAXNUMOFINDEXES 128

/* maximum number of boolean expressions */
#define MAXNUMOFBOOLS    20	

/* maximum key value length for Btree */
#define MAXKEYLEN 256

/* number of attributes consisting MBR */
#define MBR_NUM_PARTS 4

/* maximum number of key parts in Btree */
#define MAXNUMKEYPARTS 8

/* maximum number of key parts in MLGF */
#define MLGF_MAXNUM_KEYS 10

/* NIL value */
#define NIL -1

/* scan direction */
#define FORWARD  0
#define BACKWARD 1
#define BACKWARD_NOORDERING 2
#define BACKWARD_ORDERING 3


/* end of scan */
#define EOS 1

#define MAXKEYWORDLEN (MAXKEYLEN-sizeof(Two))
#define INITSCAN         20
#define LRDS_SYSTABLES_RELNAME     "lrdsSysTables"
#define LRDS_SYSCOLUMNS_RELNAME     "lrdsSysColumns"
#define LRDS_SYSINDEXES_RELNAME     "lrdsSysIndexes"

/*
 * Type definitions for the basic types
 *
 * Note: it is used not to COSMOS but to ODYSSEUS.
 */

#ifndef __DBLABLIB_H__

#if defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2) 

/* one byte data type (in fact, it is a two byte data type) */
typedef short                   One;
typedef unsigned short          UOne;

/* two bytes data type (in fact, it is a four byte data type) */
typedef int                     Two;
typedef unsigned int            UTwo;

/* four bytes data type (in fact, it is a eight byte data type) */
typedef long                    Four;
typedef unsigned long           UFour;

/* eight bytes data type */
typedef long                    Eight;
typedef unsigned long           UEight;

/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef One                     Two_Invariable;
typedef UOne                    UTwo_Invariable;
typedef Two                     Four_Invariable;
typedef UTwo                    UFour_Invariable;
typedef Four                    Eight_Invariable;
typedef UFour                   UEight_Invariable;

/* data & memory align type */
typedef Eight_Invariable        ALIGN_TYPE;
typedef Eight_Invariable	MEMORY_ALIGN_TYPE;

#elif defined(_LP64) && !defined(SUPPORT_LARGE_DATABASE2)

/* one byte data type */
typedef char                    One;
typedef unsigned char           UOne;

/* two bytes data type */
typedef short                   Two;
typedef unsigned short          UTwo;

/* four bytes data type */
typedef int                     Four;
typedef unsigned int            UFour;

/* eight bytes data type */
typedef long                    Eight;
typedef unsigned long           UEight;

/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef Two                     Two_Invariable;
typedef UTwo                    UTwo_Invariable;
typedef Four                    Four_Invariable;
typedef UFour                   UFour_Invariable;
typedef Eight                   Eight_Invariable;
typedef UEight                  UEight_Invariable;

/* data & memory align type */
typedef Four_Invariable         ALIGN_TYPE;
typedef Eight_Invariable	MEMORY_ALIGN_TYPE;

#elif !defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2)

/* one byte data type (in fact, it is a two byte data type) */
typedef short                   One;
typedef unsigned short          UOne;

/* two bytes data type (in fact, it is a four byte data type) */
typedef long                    Two;
typedef unsigned long           UTwo;

/* four bytes data type (in fact, it is a eight byte data type) */
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)
typedef long long               Four;
typedef unsigned long long      UFour;
#elif defined(WIN64) || defined(WIN32)
typedef __int64                 Four;
typedef unsigned __int64        UFour;
#else
#define EIGHT_NOT_DEFINED
#endif /* defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) */

/* eight bytes data type */
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)
typedef long long               Eight;
typedef unsigned long long      UEight;
#elif defined(WIN64) || defined(WIN32)
typedef __int64                 Eight;
typedef unsigned __int64        UEight;
#else
#define EIGHT_NOT_DEFINED
#endif /* defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) */
 
/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef One                     Two_Invariable;
typedef UOne                    UTwo_Invariable;
typedef Two                     Four_Invariable;
typedef UTwo                    UFour_Invariable;
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) || defined(WIN64) || defined(WIN32)
typedef Four                    Eight_Invariable;
typedef UFour                   UEight_Invariable;
#endif

/* data & memory align type */
typedef Eight_Invariable        ALIGN_TYPE;
typedef Four_Invariable		MEMORY_ALIGN_TYPE;

#elif !defined(_LP64) && !defined(SUPPORT_LARGE_DATABASE2) 

/* one byte data type */
typedef char                    One;
typedef unsigned char           UOne;

/* two bytes data type */
typedef short                   Two;
typedef unsigned short          UTwo;

/* four bytes data type */
typedef long                    Four;
typedef unsigned long           UFour;
 
/* eight bytes data type */
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)
typedef long long               Eight;
typedef unsigned long long      UEight;
#elif defined(WIN64) || defined(WIN32)
typedef __int64                 Eight;
typedef unsigned __int64        UEight;
#else
#define EIGHT_NOT_DEFINED
#endif /* defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) */
 
/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef Two                     Two_Invariable;
typedef UTwo                    UTwo_Invariable;
typedef Four                    Four_Invariable;
typedef UFour                   UFour_Invariable;
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) || defined(WIN64) || defined(WIN32)
typedef Eight                   Eight_Invariable;
typedef UEight                  UEight_Invariable;
#endif

/* data & memory align type */
typedef Four_Invariable		ALIGN_TYPE;
typedef Four_Invariable		MEMORY_ALIGN_TYPE;

#endif 

#endif /* __DBLABLIB_H__ */

/* Boolean Type */
typedef enum { SM_FALSE, SM_TRUE } Boolean;

/* Comparison Operator */
typedef enum {SM_EQ=0x1, SM_LT=0x2, SM_LE=0x3, SM_GT=0x4, SM_GE=0x5, SM_NE=0x6, SM_EOF=0x10, SM_BOF=0x20} CompOp;

/* TraceFlag & TraceLevel & NumOfArgs */
typedef Four    TraceFlag;
typedef Four    TraceLevel;
typedef Four    NumOfArgs;

/* hash value */
typedef UFour_Invariable MLGF_HashValue;

/* MBR type */
typedef struct {
    MLGF_HashValue values[MBR_NUM_PARTS];
} LRDS_MBR;

/* Btree Key Value */
typedef struct {
    Two len;
    char val[MAXKEYLEN];
} KeyValue;


/*
 * BoundCond Type: used in a range scan of Btree to give bound condition
 */
typedef struct {
    KeyValue key;		/* Key Value */
    CompOp   op;		/* The key value is included? */
} BoundCond;


/*
 * Type Definition for Transaction Identifier
 */
typedef struct {		/* 8 byte unsigned integer */
    UFour high;
    UFour low;
} XactID;


/*
 * Lock Parameter
 */
typedef enum { L_NL, L_IS, L_IX, L_S, L_SIX, L_X } LockMode; /* lock mode */
typedef enum { L_INSTANT, L_MANUAL, L_COMMIT } LockDuration; /* lock duration */

typedef struct {
    LockMode mode;
    LockDuration duration;
} LockParameter;

typedef enum { X_BROWSE_BROWSE, X_CS_BROWSE, X_CS_CS, X_RR_BROWSE, X_RR_CS, X_RR_RR } ConcurrencyLevel; /* isolation degree */ 

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


/*
 *  Definition for Logical ID
 */
typedef Four Serial;
typedef struct {
    Serial serial;      /* a logical serial number */
    VolNo volNo;        /* a VolNo */
} LogicalID;

/*
 * Type Definition for FileID and IndexID
 */
typedef LogicalID FileID;
typedef LogicalID IndexID;

/*
 * Type Definition for ObjectID and TupleID
 */
typedef UFour Unique;
typedef Two SlotNo;

typedef struct {
    PageNo pageNo;		/* specify the page holding the object */
    VolID  volNo;		/* specify the volume in which object is in */
    SlotNo slotNo;		/* specify the slot within the page */
    Unique unique;		/* Unique No for checking dangling object */
} ObjectID;

typedef ObjectID TupleID;

#define SET_NILTUPLEID(tid) (tid).pageNo = NIL
#define IS_NILTUPLEID(tid) (((tid).pageNo == NIL) ? SM_TRUE:SM_FALSE)


/*
 * Type Definition for OID
 */
typedef Four ClassID;

typedef struct {
    PageNo pageNo;		/* specify the page holding the object */
    VolID  volNo;		/* specify the volume in which object is in */
    SlotNo slotNo;		/* specify the slot within the page */
    Unique unique;		/* Unique No for checking dangling object */
    ClassID classID;            /* specify the class including the object */
} OID;

/*
 * The definition of MAKE_NULL_OID and IS_NULL_OID in common.h are different from those in cosmos_r.h
 * The definitions in common.h are
 * 	MAKE_NULL_OID(oid) ((oid).classID = -1),
 * 	IS_NULL_OID(oid)   ((oid).classID == -1).
 * But, we use following macros for a long time and have no problems.
 * So, we had better remain the macros to prevent the future bugs caused by correction.
 */
#define MAKE_NULL_OID(oid)	SET_NILTUPLEID(oid)
#define IS_NULL_OID(oid)	IS_NILTUPLEID(oid)


/*
 * Counter ID
 */
typedef ObjectID CounterID;   


/*
** Type Definition for ColInfo
*/
/* key part */
typedef struct {
    Two   type;			/* types supported by COSMOS */
    Two   offset;		/* where ? */
    Two   length;		/* how ?   */
} KeyPart;

/* key descriptor */
typedef struct {
    Two   flag;			/* flag for some more informations */
    Two   nparts;		/* the number of key parts */
    KeyPart kpart[MAXNUMKEYPARTS]; /* key parts */
} KeyDesc;

typedef struct OrderedSetAuxColInfo_T_tag {
    KeyDesc kdesc;
    Boolean nestedIndexFlag;
} OrderedSetAuxColInfo_T;

typedef struct {
    Two  complexType;		/* data type of column */
    Two  type;			/* data type of column */
    Four length;		/* length(maximum in case SM_STRING) of column */
    union {        
        OrderedSetAuxColInfo_T *orderedSet;
    } auxInfo;
} ColInfo;

typedef union AuxColInfo_T_tag {
    OrderedSetAuxColInfo_T orderedSet;
} AuxColInfo_T;

typedef Four OrderedSet_ElementLength;


/*
 * Index Description Definiton
 */
#define KEYFLAG_CLEAR      0x0
#define KEYFLAG_UNIQUE     0x1
#define KEYFLAG_CLUSTERING 0x2

#define KEYINFO_COL_ORDER  0x3 /* ORDER mask */
#define KEYINFO_COL_ASC    0x2 /* ascending order */
#define KEYINFO_COL_DESC   0x1 /* descending order */

typedef struct {
    Two flag;                   /* KEYFLAG_CLEAR, KEYFLAG_UNIQUE, KEYFLAG_CLUSTERING */
    Two nColumns;               /* # of columns on which the index is defined */
    struct {
        Four colNo;
        Four flag;              /* ascending/descendig */
    } columns[MAXNUMKEYPARTS];
} KeyInfo;

typedef struct {
    Two flag;			/* KEYFLAG_CLEAR, KEYFLAG_CLUSTERING */
    Two nColumns;		/* # of columns on which the index is defined */
    Two colNo[MLGF_MAXNUM_KEYS]; /* column numbers */
    Two extraDataLen;		/* length of the extra data for an object */
} MLGF_KeyInfo;

#define SM_INDEXTYPE_BTREE 1
#define SM_INDEXTYPE_MLGF  2

typedef struct {
    One indexType;
    union {
	KeyInfo btree;		/* Key Information for Btree */
	MLGF_KeyInfo mlgf;	/* Key Information for MLGF */
    } kinfo;    
} LRDS_IndexDesc;


/*
** Type Definition for Boolean Expression
*/
typedef struct {
    Two  	op;		/* SM_EQ, SM_LT, SM_LE, SM_GT, SM_GE, SM_NE */
    Two  	colNo;		/* which column ? */
    Two  	length;		/* length of the value: used for SM_VARSTRING */
    union {
	Two_Invariable    	s;		/* SM_SHORT */
	Four_Invariable    	i;		/* SM_INT */
	Four_Invariable   	l;		/* SM_LONG */
	Eight_Invariable   	ll;		/* SM_LONG_LONG */
	float  			f;		/* SM_FLOAT */
	double 			d;		/* SM_DOUBLE */
	PageID 		pid;			/* SM_PAGEID */
	FileID 		fid;			/* SM_FILEID */
	IndexID 		iid;		/* SM_INDEXID */
	OID    			oid;		/* SM_OID */ 
	LRDS_MBR 		mbr;		/* SM_MBR */ 
	char   			str[MAXKEYLEN];	/* SM_STRING or SM_VARSTRING */
    } data;			/* the value to be compared */
} BoolExp;

/*
** Cursor definition
*/
/* AnyCursor:
 *  All cursors should have the following members at the front of them
 *  in the same order.
 */
typedef struct {
    One opaque;                 /* opaque member */
    TupleID tid;		/* object pointed by the cursor */
} LRDS_AnyCursor;

/* DataCursor:
 *  sequential scan using the data file
 */
typedef struct {
    One opaque;                 /* opaque member */
    TupleID tid;		/* object pointed by the cursor */
} LRDS_DataCursor;

/* BtreeCursor:
 *  scan using a B+ tree
 */
typedef struct {
    One opaque;                 /* opaque member */
    TupleID tid;		/* object pointed by the cursor */    
    KeyValue key;		/* what key value? */
} LRDS_BtreeCursor;

typedef struct {
    One opaque;                 /* opaque member */
    TupleID tid;		/* object pointed by the cursor */
    MLGF_HashValue keys[MLGF_MAXNUM_KEYS]; /* what key values? */
} LRDS_MLGF_Cursor;

/* Universal Cursor */
typedef union {
    LRDS_AnyCursor any;		/* for access of 'flag' and 'oid' */
    LRDS_DataCursor seq;        /* sequential scan */
    LRDS_BtreeCursor btree;     /* scan using a B+ tree */
    LRDS_MLGF_Cursor mlgf;      /* scan using MLGF index */
} LRDS_Cursor;


/*
** Type Definition for ColListStruct
*/
#define ALL_VALUE   -1		/* special value of 'start' */
#define TO_END      -1		/* special value of 'length' */

typedef struct {
    Two 	colNo;			/* IN column number */
    Boolean 	nullFlag;           	/* TRUE if it has null value */
    Four 	start;			/* IN starting offset within a column */
					/*   ALL_VALUE: read all data of this column */
    Four 	length;			/* IN amount of old data within a column */
                                	/*   REMAINDER: to the end of the column */
    Four 	dataLength;		/* IN amount of new data */
    union {
	Two_Invariable    	s;		/* SM_SHORT */
	Four_Invariable    	i;		/* SM_INT */
	Four_Invariable   	l;		/* SM_LONG */
	Eight_Invariable   	ll;		/* SM_LONG_LONG */
	float  			f;		/* SM_FLOAT */
	double 			d;		/* SM_DOUBLE */
	PageID 			pid;		/* SM_PAGEID */
	FileID 			fid;		/* SM_FILEID */
	IndexID 		iid;		/* SM_INDEXID */
	OID    			oid;		/* SM_OID */
	LRDS_MBR 		mbr;		/* SM_MBR */
	void   			*ptr;		/* pointer to data: SM_STRING, SM_VARSTRING */
    } data;
    Four retLength;		/* OUT return length of Read/Write */
} ColListStruct;

/*
 * to read the length of columns
 */
typedef struct {
    Two colNo;
    Four length;
}ColLengthInfoListStruct;


/*
 * Data Types Supported by COSMOS
 */    
#define SM_COMPLEXTYPE_BASIC 		0
#define SM_COMPLEXTYPE_SET   		1
#define SM_COMPLEXTYPE_ORDEREDSET 	2
#define SM_COMPLEXTYPE_COLLECTIONSET    3
#define SM_COMPLEXTYPE_COLLECTIONBAG    4
#define SM_COMPLEXTYPE_COLLECTIONLIST   5

#define SM_SHORT        0
#define SM_INT          1
#define SM_LONG         2
#define SM_FLOAT        3
#define SM_DOUBLE       4
#define SM_STRING       5	/* fixed-length string */
#define SM_VARSTRING    6	/* variable-length string */
#define SM_PAGEID       7	/* PageID type */
#define SM_FILEID       8       /* FileID type */
#define SM_INDEXID      9       /* IndexID type */
#define SM_OID		10	/* OID(volume no, page no, slot no, unique no, class id) type */
				/* NOTICE: OID is different with ObjectID */
#define SM_TEXT		11	/* Text Type */
#define SM_MBR		12	/* MBR Type */
#define SM_LONG_LONG	14	

#define SM_SHORT_SIZE   	sizeof(Two_Invariable)
#define SM_INT_SIZE     	sizeof(Four_Invariable)
#define SM_LONG_SIZE    	sizeof(Four_Invariable)
#define SM_FLOAT_SIZE   sizeof(float)
#define SM_DOUBLE_SIZE  sizeof(double)
#define SM_PAGEID_SIZE  sizeof(PageID)
#define SM_INDEXID_SIZE sizeof(IndexID)
#define SM_FILEID_SIZE  sizeof(FileID)
#define SM_OID_SIZE	sizeof(OID)
#define SM_MBR_SIZE		(MBR_NUM_PARTS*sizeof(MLGF_HashValue))
#define SM_OBJECT_ID_SIZE       sizeof(ObjectID)        
#define SM_LONG_LONG_SIZE    	sizeof(Eight_Invariable)


/*
** Interface Function Prototypes of LRDS
*/
Four LRDS_AddIndex(Four, Four, char*, LRDS_IndexDesc*, IndexID*); 
Four LRDS_DropIndex(Four, Four, char*, IndexID*);
Four LRDS_AddColumn(Four, Four, char*, ColInfo*); 
Four LRDS_CreateRelation(Four, Four, char*, LRDS_IndexDesc*, Two, ColInfo*, Boolean); 
Four LRDS_DestroyRelation(Four, Four, char*);
Four LRDS_CreateTuple(Four, Four, Boolean, Two, ColListStruct*, TupleID*);
Four LRDS_DestroyTuple(Four, Four, Boolean, TupleID*);
Four LRDS_FetchTuple(Four, Four, Boolean, TupleID*, Two, ColListStruct*);
Four LRDS_FetchColLength(Four, Four, Boolean, TupleID*, Two, ColLengthInfoListStruct*);
Four LRDS_UpdateTuple(Four, Four, Boolean, TupleID*, Two, ColListStruct*);
Four LRDS_Mount(Four, Four, char**, Four*);
Four LRDS_Dismount(Four, Four);
Four LRDS_OpenRelation(Four, Four, char*);
Four LRDS_CloseRelation(Four, Four);
Four LRDS_CloseAllRelations(Four); 
Four LRDS_OpenIndexScan(Four, Four, IndexID*, BoundCond*, BoundCond*, Four, BoolExp*, LockParameter*);
Four LRDS_OpenSeqScan(Four, Four, Four, Four, BoolExp*, LockParameter*);
Four LRDS_CloseScan(Four, Four);
Four LRDS_CloseAllScans(Four); 
Four LRDS_NextTuple(Four, Four, TupleID*, LRDS_Cursor**); 
Four LRDS_Text_AddKeywords(Four, Four, Boolean, TupleID*, Two, Four, char*);
Four LRDS_Text_DeleteKeywords(Four, Four, Boolean, TupleID*, Two, Four, char*);
Four LRDS_Text_GetIndexID(Four, Four, Two, IndexID*);
Four LRDS_MLGF_OpenIndexScan(Four, Four, IndexID*, MLGF_HashValue[], MLGF_HashValue[], Four, BoolExp[], LockParameter*);
Four LRDS_MLGF_SearchNearTuple(Four, Four, IndexID*, MLGF_HashValue[], TupleID*, LockParameter*);
Four LRDS_Init();
Four LRDS_Final();
Four LRDS_InitLocalDS(Four);
Four LRDS_InitSharedDS(Four);
Four LRDS_FinalLocalDS(Four);
Four LRDS_FinalSharedDS(Four);
Four LRDS_FormatDataVolume(Four, Four, char**, char*, Four, Two, Four*, Four);
Four LRDS_FormatTempDataVolume(Four, Four, char**, char*, Four, Two, Four*, Four); 
Four LRDS_FormatLogVolume(Four, Four, char**, char*, Four, Two, Four*);
Four LRDS_ExpandDataVolume(Four, Four, Four, char**, Four*); 
Four LRDS_FormatCoherencyVolume(Four, char*, char*, Four);
Four LRDS_GetFileIdOfRelation(Four, Four, char*, FileID*); 
Four LRDS_SortRelation(Four, Four, Four, char*, KeyInfo*, Boolean, char*, Boolean, LockParameter*); 
Four LRDS_BeginTransaction(Four, XactID*, ConcurrencyLevel);
Four LRDS_AbortTransaction(Four, XactID*);
Four LRDS_CommitTransaction(Four, XactID*);
Four LRDS_CreateCounter(Four, Four, char*, Four, CounterID*);
Four LRDS_DestroyCounter(Four, Four, char*);
Four LRDS_GetCounterId(Four, Four, char*, CounterID*);
Four LRDS_SetCounter(Four, Four, CounterID*, Four);
Four LRDS_ReadCounter(Four, Four, CounterID*, Four*);
Four LRDS_GetCounterValues(Four, Four, CounterID*, Four, Four*);
Four LRDS_SetCfgParam(Four handle, char*, char*);
char* LRDS_GetCfgParam(Four handle, char*);
char *LRDS_Err(Four, Four);

Four LRDS_Set_Create(Four, Four, Boolean, TupleID*, Two);
Four LRDS_Set_Destroy(Four, Four, Boolean, TupleID*, Two);
Four LRDS_Set_InsertElements(Four, Four, Boolean, TupleID*, Two, Four, void*);
Four LRDS_Set_DeleteElements(Four, Four, Boolean, TupleID*, Two, Four, void*);
Four LRDS_Set_IsMember(Four, Four, Boolean, TupleID*, Two, void*);
Four LRDS_Set_Scan_Open(Four, Four, Boolean, TupleID*, Two);
Four LRDS_Set_Scan_Close(Four, Four);
Four LRDS_Set_Scan_NextElements(Four, Four, Four, void*);
Four LRDS_Set_Scan_InsertElements(Four, Four, Four, void*);
Four LRDS_Set_Scan_DeleteElements(Four, Four);
Four LRDS_Set_IsNull(Four, Four, Boolean, TupleID*, Two); 

Four LRDS_CollectionSet_Create(Four, Four, Boolean, TupleID*, Two, Four);
Four LRDS_CollectionSet_Destroy(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_GetN_Elements(Four, Four, Boolean, TupleID*, Two, Four*);
Four LRDS_CollectionSet_Assign(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_AssignElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionSet_InsertElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionSet_DeleteElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionSet_DeleteAll(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_IsMember(Four, Four, Boolean, TupleID*, Two, Four, void*);
Four LRDS_CollectionSet_IsEqual(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_IsSubset(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_RetrieveElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*, Four, void*);
Four LRDS_CollectionSet_GetSizeOfElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*);
Four LRDS_CollectionSet_Union(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_Intersect(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_Difference(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_UnionWith(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_IntersectWith(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_DifferenceWith(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_Scan_Open(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionSet_Scan_Close(Four, Four);
Four LRDS_CollectionSet_Scan_NextElements(Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionSet_Scan_GetSizeOfNextElements(Four, Four, Four, Four*);
Four LRDS_CollectionSet_Scan_InsertElements(Four, Four, Four, Four*, void*);
Four LRDS_CollectionSet_Scan_DeleteElements(Four, Four);
Four LRDS_CollectionSet_IsNull(Four, Four, Boolean, TupleID*, Two); 

Four LRDS_CollectionBag_Create(Four, Four, Boolean, TupleID*, Two, Four);
Four LRDS_CollectionBag_Destroy(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_GetN_Elements(Four, Four, Boolean, TupleID*, Two, Four*);
Four LRDS_CollectionBag_Assign(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_AssignElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionBag_InsertElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionBag_DeleteElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionBag_DeleteAll(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_IsMember(Four, Four, Boolean, TupleID*, Two, Four, void*);
Four LRDS_CollectionBag_IsEqual(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_IsSubset(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_RetrieveElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*, Four, void*);
Four LRDS_CollectionBag_GetSizeOfElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*);
Four LRDS_CollectionBag_Union(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_Intersect(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_Difference(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_UnionWith(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_IntersectWith(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_DifferenceWith(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_Scan_Open(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionBag_Scan_Close(Four, Four);
Four LRDS_CollectionBag_Scan_NextElements(Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionBag_Scan_GetSizeOfNextElements(Four, Four, Four, Four*);
Four LRDS_CollectionBag_Scan_InsertElements(Four, Four, Four, Four*, void*);
Four LRDS_CollectionBag_Scan_DeleteElements(Four, Four);
Four LRDS_CollectionBag_IsNull(Four, Four, Boolean, TupleID*, Two); 

Four LRDS_CollectionList_Create(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionList_Destroy(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionList_GetN_Elements(Four, Four, Boolean, TupleID*, Two, Four*);
Four LRDS_CollectionList_Assign(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionList_AssignElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionList_InsertElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*, void*);
Four LRDS_CollectionList_DeleteElements(Four, Four, Boolean, TupleID*, Two, Four, Four);
Four LRDS_CollectionList_DeleteAll(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionList_IsMember(Four, Four, Boolean, TupleID*, Two, Four, void*, Four*);
Four LRDS_CollectionList_IsEqual(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionList_AppendElements(Four, Four, Boolean, TupleID*, Two, Four, Four*, void*);
Four LRDS_CollectionList_GetSizeOfElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*);
Four LRDS_CollectionList_RetrieveElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*, Four, void*);
Four LRDS_CollectionList_UpdateElements(Four, Four, Boolean, TupleID*, Two, Four, Four, Four*, void*);
Four LRDS_CollectionList_Concatenate(Four, Four, Boolean, TupleID*, Two, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionList_Resize(Four, Four, Boolean, TupleID*, Two, Four);
Four LRDS_CollectionList_Scan_Open(Four, Four, Boolean, TupleID*, Two);
Four LRDS_CollectionList_Scan_Close(Four, Four);
Four LRDS_CollectionList_Scan_NextElements(Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionList_Scan_GetSizeOfNextElements(Four, Four, Four, Four*);
Four LRDS_CollectionList_Scan_InsertElements(Four, Four, Four, Four*, void*);
Four LRDS_CollectionList_Scan_DeleteElements(Four, Four);
Four LRDS_CollectionList_IsNull(Four, Four, Boolean, TupleID*, Two); 

Four LRDS_OrderedSet_SpecifyKeyOfElement(Four, Four, char*, Two, KeyDesc*);
Four LRDS_OrderedSet_Create(Four, Four, Boolean, TupleID*, Two, LockParameter*);
Four LRDS_OrderedSet_Destroy(Four, Four, Boolean, TupleID*, Two, LockParameter*);
Four LRDS_OrderedSet_GetTotalLengthOfElements(Four, Four, Boolean, TupleID*, Two, Four*, LockParameter*);
Four LRDS_OrderedSet_GetN_Elements(Four, Four, Boolean, TupleID*, Two, Four*, LockParameter*);
Four LRDS_OrderedSet_HasNestedIndex(Four, Four, Boolean, TupleID*, Two, LockParameter*);
Four LRDS_OrderedSet_IsMember(Four, Four, Boolean, TupleID*, Two, KeyValue*, Four, char*, LockParameter*);
Four LRDS_OrderedSet_InsertElement(Four, Four, Boolean, TupleID*, Two, char*, LockParameter*);
Four LRDS_OrderedSet_AppendSortedElements(Four, Four, Boolean, TupleID*, Two, Four, Four, char*, LockParameter*);
Four LRDS_OrderedSet_DeleteElement(Four, Four, Boolean, TupleID*, Two, KeyValue*, LockParameter*);
Four LRDS_OrderedSet_UpdateElement(Four, Four, Boolean, TupleID*, Two, KeyValue*, Four, Four, Four, void*, LockParameter*);
Four LRDS_OrderedSet_DeleteElements(Four, Four, Boolean, TupleID*, Two, Four, KeyValue*, LockParameter*);
#ifndef ORDEREDSET_BACKWARD_SCAN
Four LRDS_OrderedSet_Scan_Open(Four, Four, Boolean, TupleID*, Two, LockParameter*);
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*);
#else
Four LRDS_OrderedSet_Scan_Open(Four, Four, Boolean, TupleID*, Two, Four, LockParameter*); 
#ifndef COMPRESSION 
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*, Four, char*); 
#else
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*, Four, char*, Four*); 
#endif
#endif
Four LRDS_OrderedSet_Scan_SkipElementsUntilGivenKeyValue(Four, Four, Two, char*);
Four LRDS_OrderedSet_Scan_Close(Four, Four);
Four LRDS_OrderedSet_CreateNestedIndex(Four, Four, Two);
Four LRDS_OrderedSet_DestroyNestedIndex(Four, Four, Two);
Four LRDS_OrderedSet_IsNull(Four, Four, Boolean, TupleID*, Two); 
#ifdef COMPRESSION 
Four LRDS_OrderedSet_SpecifyVolNo(Four, Four, Boolean, TupleID*, Two, Four, LockParameter*);
Four LRDS_OrderedSet_GetVolNo(Four, Four, Boolean, TupleID*, Two, VolNo*, LockParameter*);
#endif

Four LRDS_AllocHandle(Four*);
Four LRDS_FreeHandle(Four);


/*
 *  Bulkload API
 */
Four LRDS_InitRelationBulkLoad(Four, Four, Four, char*, Boolean, Boolean, Two, Two, LockParameter*); 
Four LRDS_NextRelationBulkLoad(Four, Four, Two, ColListStruct*, Boolean, TupleID*);
Four LRDS_FinalRelationBulkLoad(Four, Four);

#ifndef COMPRESSION 
Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(Four, Four, Four, Two, Four, Four, char*, Boolean, TupleID*); 
#else
Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(Four, Four, Four, Two, Four, Four, char*, Boolean, TupleID*, char*, Four, Four); 
#endif
Four LRDS_NextRelationBulkLoad_Collection(Four, Four, Four, Four, Boolean, Boolean, TupleID*, Two, Four); 
Four LRDS_OrderedSetAppendBulkLoad(Four, Four, Four, Boolean, TupleID*, Two, Four, Four, char*, LockParameter*); 

Four LRDS_InitTextBulkLoad(Four, Four, Four, Boolean, Boolean, Two, LockParameter*); 
Four LRDS_NextTextBulkLoad(Four, Four, TupleID*, Four, char*);
Four LRDS_FinalTextBulkLoad(Four, Four);


/*
 *  Stream utility
 */

#define SORTKEYDESC_ATTR_ORDER   0x3 /* attribute ORDER mask */
#define SORTKEYDESC_ATTR_ASC     0x2 /* ascending order */
#define SORTKEYDESC_ATTR_DESC    0x1 /* descending order */

typedef struct {
    Two nparts;                 /* # of key parts */
    Two hdrSize;                /* size of header in front of sorted tuple */
    struct {
        Four type;              /* part's type */
        Four length;            /* part's length */
        Four flag;              /* ascending/descendig = SORTKEYDESC_ATTR_ASC/SORTKEYDESC_ATTR_DESC */
    } parts[MAXNUMKEYPARTS];
} SortTupleDesc;

typedef struct {
    Two         len;
    char*       data;
} SortStreamTuple;

Four Util_OpenSortStream(Four, VolID, SortTupleDesc*);
Four Util_CloseSortStream(Four, Four);
Four Util_SortingSortStream(Four, Four);
Four Util_PutTuplesIntoSortStream(Four, Four, Four, SortStreamTuple*);
Four Util_GetTuplesFromSortStream(Four, Four, Four*, SortStreamTuple*, Boolean*);

Four Util_OpenStream(Four, VolID);
Four Util_CloseStream(Four, Four);
Four Util_PutTuplesIntoStream(Four, Four, Four, SortStreamTuple*);
Four Util_ChangePhaseStream(Four, Four);
Four Util_GetTuplesFromStream(Four, Four, Four*, SortStreamTuple*, Boolean*);
Four Util_Sleep(double secs);

#define LRDS_OpenSortStream(_handle, volId, sortTupleDesc) Util_OpenSortStream(_handle, volId, sortTupleDesc)
#define LRDS_CloseSortStream(_handle, streamId)            Util_CloseSortStream(_handle, streamId)
#define LRDS_SortingSortStream(_handle, streamId)          Util_SortingSortStream(_handle, streamId)
#define LRDS_PutTuplesIntoSortStream(_handle, streamId, numTuples, tuples) \
	Util_PutTuplesIntoSortStream(_handle, streamId, numTuples, tuples)
#define LRDS_GetTuplesFromSortStream(_handle, streamId, numTuples, tuples, eof) \
	Util_GetTuplesFromSortStream(_handle, streamId, numTuples, tuples, eof)
#define LRDS_GetNumTuplesInSortStream(_handle, streamId)   Util_GetNumTuplesInSortStream(_handle, streamId)
#define LRDS_GetSizeOfSortStream(_handle, streamId)        Util_GetSizeOfSortStream(_handle, streamId)

#define LRDS_OpenStream(_handle, volId)                    Util_OpenStream(_handle, volId)
#define LRDS_CloseStream(_handle, streamId)                Util_CloseStream(_handle, streamId)
#define LRDS_ChangePhaseStream(_handle, streamId)          Util_ChangePhaseStream(_handle, streamId)
#define LRDS_PutTuplesIntoStream(_handle, streamId, numTuples, tuples) \
	Util_PutTuplesIntoStream(_handle, streamId, numTuples, tuples)
#define LRDS_GetTuplesFromStream(_handle, streamId, numTuples, tuples, eof) \
	Util_GetTuplesFromStream(_handle, streamId, numTuples, tuples, eof)
#define LRDS_GetNumTuplesInStream(_handle, streamId)       Util_GetNumTuplesInStream(_handle, streamId)
#define LRDS_GetSizeOfStream(_handle, streamId)            Util_GetSizeOfStream(_handle, streamId)


/*
 *  APIs for variable array
 */

/* Type definition for the variable size array */
typedef struct {
    Four nEntries;      /* # of entries in this array */
    void *ptr;          /* pointer to the chunk of memory */
} VarArray;

/* function prototype */
Four Util_initVarArray(Four, VarArray*, Four, Four);
Four Util_doublesizeVarArray(Four, VarArray*, Four);
Four Util_finalVarArray(Four, VarArray*);

/* APIs */
#define LRDS_initVarArray(_handle, varArray, size, number) Util_initVarArray(_handle, varArray, size, number)
#define LRDS_doublesizeVarArray(_handle, varArray, size)   Util_doublesizeVarArray(_handle, varArray, size)
#define LRDS_finalVarArray(_handle, varArray)              Util_finalVarArray(_handle, varArray)


/* 
 * APIs for large files
 */
#ifdef USE_LARGE_FILE
    /* file I/O interfaces */
    #ifdef EIGHT_NOT_DEFINED
    typedef Four_Invariable filepos_t;
    #else
    typedef Eight_Invariable filepos_t;
    #endif
#else
    typedef Four_Invariable filepos_t;
#endif

FILE*	    Util_fopen(const char *, const char *);
Four	    Util_fclose(FILE*);
Four	    Util_fseek(FILE *, filepos_t, Four);
filepos_t   Util_ftell(FILE *);
#define	    Util_fprintf				fprintf
#define	    Util_fscanf					fscanf
#define	    Util_fgets(string, n, stream)		fgets(string, n, stream)
#define	    Util_fputs(string, stream)			fputs(string, stream)
#define	    Util_fread(buffer, size, count, stream)	fread(buffer, size, count, stream)
#define	    Util_fwrite(buffer, size, count, stream)    fwrite(buffer, size, count, stream)
#define	    Util_fflush(stream)				fflush(stream)


/*
 * XA Interface 
 */

/*
 * Type Definition
 */
typedef enum { LRDS_XA_SCANSTARTED, LRDS_XA_SCANENDED } LRDSXAscanStatus;

/*
 * Transaction branch identification: XID and NULLXID:
 */
#ifndef LRDS_XA_XIDDATASIZE                   /* guard against redefinition in tx.h */

#define LRDS_XA_XIDDATASIZE   128         /* size in bytes */
#define LRDS_XA_MAXGTRIDSIZE  64              /* maximum size in bytes of gtrid */
#define LRDS_XA_MAXBQUALSIZE  64              /* maximum size in bytes of bqual */
typedef struct {
    long formatID;                      /* format identifier */
    long gtrid_length;                  /* value from 1 through 64 */
    long bqual_length;                  /* value from 1 through 64 */
    char data[LRDS_XA_XIDDATASIZE];
} LRDS_XA_XID;

#endif /* LRDS_XA_XIDDATASIZE */

/*
 * Constant Variable Definition
 */
/* Flag definitions for the RM switch */
#define LRDS_XA_TMASYNC      0x80000000L        /* perform routine asynchronously */
#define LRDS_XA_TMONEPHASE   0x40000000L        /* caller is using on-phase commit optimisation */
#define LRDS_XA_TMFAIL       0x20000000L        /* dissociates caller and marks transaction branch rollback-only */
#define LRDS_XA_TMNOWAIT     0x10000000L        /* return if blocking condition exists */
#define LRDS_XA_TMRESUME     0x08000000L        /* caller is resuming association with suspended transaction branch */
#define LRDS_XA_TMSUCCESS    0x04000000L        /* dissociate caller from transaction branch*/
#define LRDS_XA_TMSUSPEND    0x02000000L        /* caller is suspending, not ending, association */
#define LRDS_XA_TMSTARTRSCAN 0x01000000L        /* start a recovery scan */
#define LRDS_XA_TMENDRSCAN   0x00800000L        /* end a recovery scan */
#define LRDS_XA_TMMULTIPLE   0x00400000L        /* wait for any asynchronous operation */
#define LRDS_XA_TMJOIN       0x00200000L        /* caller is joining existing transaction branch */
#define LRDS_XA_TMMIGRATE    0x00100000L        /* caller intends to perfrom migration */

/*
 *Flag definitions for the RM switch
 */
#define LRDS_XA_TMNOFLAGS    0x00000000L        /* no resource manager features selected */
#define LRDS_XA_TMREGISTER   0x00000001L        /* resource manager dynamically registers */
#define LRDS_XA_TMNOMIGRATE  0x00000002L        /* resource manager does not support association migration */
#define LRDS_XA_TMUSEASYNC   0x00000004L        /* resource manager supports asynchronous operations */

/*
 * Macro Definitions
 */
#define LRDS_XA_OPENSTRINGHEADER      "COSMOS_XA"
#define LRDS_XA_MAXOPENSTRINGLEN      1024

/* change the way of accessing lrds_xa_... fields */
/* access the fields through the perThreadTable structure */
#define LRDS_XA_SCANSTATUS            (perThreadTable[_handle].lrdsDS.lrds_xa_scanStatus)
#define LRDS_XA_PREPAREDNUM           (perThreadTable[_handle].lrdsDS.lrds_xa_preparedNum)
#define LRDS_XA_CURRENTPOS            (perThreadTable[_handle].lrdsDS.lrds_xa_currentPos)
#define LRDS_XA_PREPAREDLIST          (perThreadTable[_handle].lrdsDS.lrds_xa_preparedList)

#define LRDS_XA_VOLID                 (perThreadTable[_handle].lrdsDS.lrds_xa_volId)

/*
** Interface Function Prototypes of LRDS
*/
Four LRDS_XA_Commit(Four, LRDS_XA_XID *, int, long);
Four LRDS_XA_Forget(Four, LRDS_XA_XID *, int, long);
Four LRDS_XA_Prepare(Four, LRDS_XA_XID *, int, long);
Four LRDS_XA_Start(Four, LRDS_XA_XID *, int, long);
Four LRDS_XA_Complete(Four, int *, int *, int, long);
Four LRDS_XA_Open(Four, Four, char**, Four*, int, long);
Four LRDS_XA_Recover(Four, LRDS_XA_XID *, long, int, long);
Four LRDS_XA_Close(Four, char *, int, long);
Four LRDS_XA_End(Four, LRDS_XA_XID *, int, long);
Four LRDS_XA_Rollback(Four, LRDS_XA_XID *, int, long);
Four LRDS_XA_AxReg(Four, int, LRDS_XA_XID * , long);
Four LRDS_XA_AxUnreg(Four, int, long);


/* 
 *  Function prototype for error log function
 */

void Util_ErrorLog_Printf(char* msg, ...);


/*
 * error codes
 */
#define eNOERROR				0
#define eINVALIDLICENSE                          -65536
#define eINTERNAL                                -65537
#define eBADPARAMETER                            -65538
#define eBADVOLUMEID                             -65539
#define eBADFILEID                               -65540
#define eBADINDEXID                              -65541
#define eBADPAGEID                               -65542
#define eBADCATOBJ                               -65543
#define eDEADLOCK                                -65544
#define eBADCURSOR                               -65545
#define eNOTFOUND                                -65546
#define eNULLPTR                                 -65547
#define eMEMORYALLOCERR                          -65548
#define eTOOMANYVOLUMES                          -65549
#define eLOCKREQUESTFAIL                         -65550
#define eSCANOPENATSAVEPOINT                     -65551
#define eBADBUFSIZE                              -65552
#define eBLKLDTABLEFULL                          -65553
#define eVOLUMELOCKTIMEOUT                       -65554
#define eVOLUMELOCKBLOCK                         -65555
#define eVOLUMELOCKRELEASE                       -65556
#define eBADFREEDELEMENT_UTIL                    -131072
#define eBADFREEDARRAY_UTIL                      -131073
#define eSORTSTREAMTABLEFULL_UTIL                -131074
#define eNOTENOUGHSORTTUPLEBUF_UTIL              -131075
#define eOVERFLOWQUICKSORTSTACK_UTIL             -131076
#define eVOLNOTMOUNTED_RDSM                      -196608
#define eUSEDDEVICE_RDSM                         -196609
#define eTOOMANYVOLUMES_RDSM                     -196610
#define eDEVICEOPENFAIL_RDSM                     -196611
#define eDEVICECLOSEFAIL_RDSM                    -196612
#define eREADFAIL_RDSM                           -196613
#define eWRITEFAIL_RDSM                          -196614
#define eLSEEKFAIL_RDSM                          -196615
#define eINVALIDTRAINSIZE_RDSM                   -196616
#define eINVALIDFIRSTEXT_RDSM                    -196617
#define eINVALIDPID_RDSM                         -196618
#define eINVALIDMETAENTRY_RDSM                   -196619
#define eINVALIDEFF_RDSM                         -196620
#define eNODISKSPACE_RDSM                        -196621
#define eNOEMPTYMETAENTRY_RDSM                   -196622
#define eDUPMETADICTENTRY_RDSM                   -196623
#define eVOLALREADYMOUNTED_RDSM                  -196624
#define eALREADYSETBIT_RDSM                      -196625
#define eINVALIDPAGETYPE_RDSM                    -196626
#define eMETADICTENTRYNOTFOUND_RDSM              -196627
#define eBADBUFFERTYPE_BFM                       -262144
#define eBADLATCHMODE_BFM                        -262145
#define eBADBUFFER_BFM                           -262146
#define eBADHASHKEY_BFM                          -262147
#define eBADBUFTBLENTRY_BFM                      -262148
#define eFLUSHFIXEDBUF_BFM                       -262149
#define eNOTFOUND_BFM                            -262150
#define eNOUNFIXEDBUF_BFM                        -262151
#define eBADBUFINDEX_BFM                         -262152
#define eNULLBUFACCESSCB_BFM                     -262153
#define eNOSUCHLOCKEXIST_BFM                     -262154
#define eALREADYMOUNTEDCOHERENCYVOLUME_BFM       -262155
#define eNOTMOUNTEDCOHERENCYVOLUME_BFM           -262156
#define eSHMGETFAILED_BFM                        -262157
#define eSHMCTLFAILED_BFM                        -262158
#define eSHMATFAILED_BFM                         -262159
#define eSHMDTFAILED_BFM                         -262160
#define eBADLATCHCONVERSION_BFM                  -262161
#define eSIGHANDLERINSTALLFAILED_BFM             -262162
#define eFULLPROCTABLE_BFM                       -262163
#define eNOMORELOCKCONTROLBLOCKS_BFM             -262164
#define eBADOFFSET_LOT                           -327680
#define eTOOLARGELENGTH_LOT                      -327681
#define eMEMALLOCERR_LOT                         -327682
#define eBADPARAMETER_LOT                        -327683
#define eBADLENGTH_LOT                           -327684
#define eBADCATALOGOBJECT_LOT                    -327685
#define eEXCEEDMAXDEPTH_LOT                      -327686
#define eEMPTYPATH_LOT                           -327687
#define eBADPAGEID_LOT                           -327688
#define eBADSLOTNO_LOT                           -327689
#define eBADOBJECTID_LOT                         -327690
#define eBADPARAMETER_OM                         -393216
#define eBADOBJECTID_OM                          -393217
#define eBADCATALOGOBJECT_OM                     -393218
#define eBADLENGTH_OM                            -393219
#define eBADSTART_OM                             -393220
#define eBADFILEID_OM                            -393221
#define eBADUSERBUF_OM                           -393222
#define eBADPAGEID_OM                            -393223
#define eTOOLARGESORTKEY_OM                      -393224
#define eCANTALLOCEXTENT_BL_OM                   -393225
#define eBADPARAMETER_BTM                        -458752
#define eBADBTREEPAGE_BTM                        -458753
#define eBADPAGE_BTM                             -458754
#define eNOTFOUND_BTM                            -458755
#define eDUPLICATEDOBJECTID_BTM                  -458756
#define eBADCOMPOP_BTM                           -458757
#define eDUPLICATEDKEY_BTM                       -458758
#define eBADPAGETYPE_BTM                         -458759
#define eEXCEEDMAXDEPTHOFBTREE_BTM               -458760
#define eTRAVERSEPATH_BTM                        -458761
#define eNOSUCHTREELATCH_BTM                     -458762
#define eDELETEOBJECTFAILED_BTM                  -458763
#define eBADCACHETREELATCHCELLPTR_BTM            -458764
#define eBADPARAMETER_SM                         -524288
#define eNOTMOUNTEDVOLUME_SM                     -524289
#define eEXCEEDMAXKEYLENGTH_SM                   -524290
#define eBADINDEXID_SM                           -524291
#define eINDEXIDFULL_SM                          -524292
#define eBADFILEID_SM                            -524293
#define eFILEIDFULL_SM                           -524294
#define eNOTFOUNDCATALOGENTRY_SM                 -524295
#define eOPENINDEX_SM                            -524296
#define eOPENFILE_SM                             -524297
#define eTOOMANYVOLUMES_SM                       -524298
#define eEXCLUSIVELOCKREQUIRED_SM                -524299
#define eINVALIDHIERARCHICALLOCK_SM              -524300
#define eBADLOCKMODE_SM                          -524301
#define eCOMMITDURATIONLOCKREQUIRED_SM           -524302
#define eINVALIDMOUNTCOUNTER_SM                  -524303
#define eINTERNAL_SM                             -524304
#define eEXISTACTIVETRANSACTION_SM               -524305
#define eNOACTIVETRANSACTION_SM                  -524306
#define eINVALIDCFGPARAM_SM                      -524307
#define eXACTNOTSTARTED_SM                       -524308
#define eTMPFILEEXISTATSAVEPOINT_SM              -524309
#define eVOLUMEDISMOUNTDISALLOWED_SM             -524310
#define eNOACTIVEPREPAREDTRANSACTION_SM          -524311
#define eACTIVEGLOBALTRANSACTION_SM              -524312
#define eNOACTIVEGLOBALTRANSACTION_SM            -524313
#define eDUPLICATEDGTID_SM                       -524314
#define eLOGVOLOPENFAIL_RM                       -589824
#define eLSEEKFAIL_RM                            -589825
#define eLOGVOLUMEFULL_RM                        -589826
#define eREADFAIL_RM                             -589827
#define eWRITEFAIL_RM                            -589828
#define eNOLOGGEDTRANSACTION_RM                  -589829
#define eLOGVOLCLOSEFAIL_RM                      -589830
#define eNOLOGVOLUME_RM                          -589831
#define eINVALIDTRAINTYPE_RM                     -589832
#define eINVALIDOBJECTID_RM                      -589833
#define eACTIVEPREPAREDTRANSACTION_RM            -589834
#define eNOACTIVEPREPAREDTRANSACTION_RM          -589835
#define eRELATIONNOTFOUND_LRDS                   -655360
#define eINDEXNOTFOUND_LRDS                      -655361
#define eCOUNTERNOTFOUND_LRDS                    -655362
#define eTOOMANYOPENRELATIONS_LRDS               -655363
#define eRELATIONEXIST_LRDS                      -655364
#define eINDEXEXIST_LRDS                         -655365
#define eOPENRELATION_LRDS                       -655366
#define eTOOMANYTMPRELATIONS_LRDS                -655367
#define eSET_EXIST_LRDS                          -655368
#define eSET_NOTEXIST_LRDS                       -655369
#define eSET_OPENSCAN_LRDS                       -655370
#define eORDEREDSET_EXIST_LRDS                   -655371
#define eORDEREDSET_NOTEXIST_LRDS                -655372
#define eORDEREDSET_OPENSCAN_LRDS                -655373
#define eORDEREDSET_ELEMENTNOTFOUND_LRDS         -655374
#define eCOLLECTION_EXIST_LRDS                   -655375
#define eCOLLECTION_NOTEXIST_LRDS                -655376
#define eCOLLECTION_OPENSCAN_LRDS                -655377
#define eCOLLECTIONSET_ELEMENTEXIST_LRDS         -655378
#define eCOLLECTIONSET_ELEMENTNOTFOUND_LRDS      -655379
#define eCOLLECTIONSET_NOTCOMPATIBLE_LRDS        -655380
#define eCOLLECTIONBAG_ELEMENTNOTFOUND_LRDS      -655381
#define eCOLLECTIONBAG_NOTCOMPATIBLE_LRDS        -655382
#define eCOLUMNVALUEEXPECTED_LRDS                -655383
#define eWRONGCOLUMNVALUE_LRDS                   -655384
#define eINVALIDCURRENTTUPLE_LRDS                -655385
#define eTOOLARGELENGTH_LRDS                     -655386
#define eCATALOGNOTFOUND_LRDS                    -655387
#define eRELATIONOPENATSAVEPOINT_LRDS            -655388
#define eTMPRELEXISTATSAVEPOINT_LRDS             -655389
#define eXA_RBROLLBACK_LRDS_XA                   -655390
#define eXA_RBCOMMFAIL_LRDS_XA                   -655391
#define eXA_RBDEADLOCK_LRDS_XA                   -655392
#define eXA_RBINTEGRITY_LRDS_XA                  -655393
#define eXA_RBOTHER_LRDS_XA                      -655394
#define eXA_RBPROTO_LRDS_XA                      -655395
#define eXA_RBTIMEOUT_LRDS_XA                    -655396
#define eXA_RBTRANSIENT_LRDS_XA                  -655397
#define eXA_RBEND_LRDS_XA                        -655398
#define eXA_NOMIGRATE_LRDS_XA                    -655399
#define eXA_HEURHAZ_LRDS_XA                      -655400
#define eXA_HEURCOM_LRDS_XA                      -655401
#define eXA_HEURRB_LRDS_XA                       -655402
#define eXA_HEURMIX_LRDS_XA                      -655403
#define eXA_RETRY_LRDS_XA                        -655404
#define eXA_RDONLY_LRDS_XA                       -655405
#define eXAER_ASYNC_LRDS_XA                      -655406
#define eXAER_RMERR_LRDS_XA                      -655407
#define eXAER_NOTA_LRDS_XA                       -655408
#define eXAER_INVAL_LRDS_XA                      -655409
#define eXAER_PROTO_LRDS_XA                      -655410
#define eXAER_RMFAIL_LRDS_XA                     -655411
#define eXAER_DUPID_LRDS_XA                      -655412
#define eXAER_OUTSIDE_LRDS_XA                    -655413
#define eINVALIDOPENSTRING_LRDS_XA               -655414
#define eDUPLICATEDGTID_LRDS_XA                  -655415
#define eCREATEFILEFAILED_THM                    -720896
#define eFILELOCKAGAIN_THM                       -720897
#define eFILELOCKUNKNOWN_THM                     -720898
#define eMUTEXCREATEBUSY_THM                     -720899
#define eMUTEXCREATEINVAL_THM                    -720900
#define eMUTEXCREATEFAULT_THM                    -720901
#define eMUTEXCREATEUNKNOWN_THM                  -720902
#define eMUTEXDESTROYINVAL_THM                   -720903
#define eMUTEXDESTROYUNKNOWN_THM                 -720904
#define eMUTEXLOCKAGAIN_THM                      -720905
#define eMUTEXLOCKDEADLK_THM                     -720906
#define eMUTEXLOCKUNKNOWN_THM                    -720907
#define eMUTEXUNLOCKPERM_THM                     -720908
#define eMUTEXUNLOCKUNKNOWN_THM                  -720909
#define eMUTEXINITFAILED_THM                     -720910
#define eRWLOCKCREATEBUSY_THM                    -720911
#define eRWLOCKCREATEINVAL_THM                   -720912
#define eRWLOCKCREATEFAULT_THM                   -720913
#define eRWLOCKCREATEUNKNOWN_THM                 -720914
#define eRWLOCKDESTROYINVAL_THM                  -720915
#define eRWLOCKDESTROYUNKNOWN_THM                -720916
#define eRWLOCKLOCKAGAIN_THM                     -720917
#define eRWLOCKLOCKDEADLK_THM                    -720918
#define eRWLOCKLOCKUNKNOWN_THM                   -720919
#define eRWLOCKUNLOCKPERM_THM                    -720920
#define eRWLOCKUNLOCKUNKNOWN_THM                 -720921
#define eRWLOCKINITFAILED_THM                    -720922
#define eSEMCREATEACCES_THM                      -720923
#define eSEMCREATEEXIST_THM                      -720924
#define eSEMCREATEINTR_THM                       -720925
#define eSEMCREATEINVAL_THM                      -720926
#define eSEMCREATEMFILE_THM                      -720927
#define eSEMCREATENAMETOOLONG_THM                -720928
#define eSEMCREATENFILE_THM                      -720929
#define eSEMCREATENOENT_THM                      -720930
#define eSEMCREATENOSPC_THM                      -720931
#define eSEMCREATENOSYS_THM                      -720932
#define eSEMCREATEUNKNOWN_THM                    -720933
#define eSEMCLOSEINVAL_THM                       -720934
#define eSEMCLOSENOSYS_THM                       -720935
#define eSEMCLOSEUNKNOWN_THM                     -720936
#define eSEMDESTROYACCES_THM                     -720937
#define eSEMDESTROYNAMETOOLONG_THM               -720938
#define eSEMDESTROYENOENT_THM                    -720939
#define eSEMDESTROYNOSYS_THM                     -720940
#define eSEMDESTROYUNKNOWN_THM                   -720941
#define eSEMPOSTINVAL_THM                        -720942
#define eSEMPOSTUNKNOWN_THM                      -720943
#define eSEMWAITINVAL_THM                        -720944
#define eSEMWAITINTR_THM                         -720945
#define eSEMWAITUNKNOWN_THM                      -720946
#define eFULLTHREADTABLE_THM                     -720947
#define eINVALIDOPENSTRING_COSMOS_XA             -786432
#define eINVALIDERRORCODE_COSMOS_XA              -786433
/*###########################################################################*/
/*
 * The followings are for ODYSSEUS.
 * These should be deleted when COSMOS is released outside.
 */

/*
** Type Definition for LRDS Mount Table Entry.
*/
#define LRDS_NUMOFCATALOGTABLES 3

typedef struct {
    Two  volId;			/* volume identifier */
    /* open relation numbers of catalog tables */
    Four catalogTableOrn[LRDS_NUMOFCATALOGTABLES];
    
    Four nMount;		/* number of Mount */
} lrds_MountTableEntry;

#define LRDS_MOUNTTABLE(_handle)	perThreadTable[_handle].lrdsDS.lrds_table_in_memory.lrdsMountTable


/*
** Type Definition for RelationInfo.
*/
typedef struct {
    ObjectID catalogEntry;	/* ObjectID of an entry in LRDS_SYSTABLES */
    FileID fid;			/* identifier of data file mapped to this relation */
    Four nTuples;		/* number of tuples */
    Four maxTupleLen;		/* maximum length of a tuple */
    Two  nIndexes;		/* number of indexes */
    Two  nColumns;		/* number of columns */
    Two  nVarColumns;		/* number of variable-length columns */
    char relName[MAXRELNAME];	/* relation name */

} RelationInfo;


/*
** Type Definition for IndexInfo.
*/
typedef struct {
    One flag;			/* flag */
    One nKeys;			/* number of keys */
    Two extraDataLen;		/* length of the extra data for an object */
    MLGF_HashValue 	minMaxTypeVector;	/* bit vector of flags indicating MIN/MAX of MBR for each attribute */
} MLGF_KeyDesc;

typedef struct {
    IndexID iid;			/* index identifier */
    union {	
	KeyDesc btree;			/* a key descriptor for btree */
	MLGF_KeyDesc mlgf;		/* a key descriptro for mlgf */
    } kdesc;
    Two     colNo[MAXNUMKEYPARTS];	/* column numbers */
    One     indexType;			/* index type */
} IndexInfo;


/*
** Type Definition for ColDesc
**   - in-core memory data structure
**   - ColInfo is used after converted to ColDesc
*/
typedef struct {
    Two  complexType;		/* SM_COMPLEX_BASIC, SM_COMPLEX_SET */
    Two  type;			/* SM_SHORT, SM_LONG, SM_FLOAT, ... */
    Two  varColNo;		/* column number of variable-length columns */
    Two  fixedColNo;		/* column number of fixed-length columns */
    Four offset;		/* offset of the field */
    Four length;		/* length of the field */
    void *auxInfo;              /* auxiliary column information */
} ColDesc;

/*
 * Type Definition for COSMOS thread
 */
#ifdef WIN32

#else
typedef sem_t                           cosmos_thread_sem_t;
typedef char                            cosmos_thread_semName_t;
typedef pthread_mutex_t                 cosmos_thread_mutex_t;
typedef pthread_mutexattr_t             cosmos_thread_mutex_attr_t;
#endif

#define MAXSEMAPHORENAME        256

#define COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS   PTHREAD_MUTEX_INITIALIZER

/*
 * APIs for Critical Section 
 */
#ifndef WIN32
typedef int FileDesc;
#else
typedef void* FileDesc;
#endif
Four THM_CriticalSection(Four, Four, Four, FileDesc*); 
Four THM_ControlFileLock(int , int , FileDesc*); 
Four THM_CreateUnnamedSemaphore(Four, cosmos_thread_sem_t*, unsigned int);
Four THM_DestroyUnnamedSemaphore(Four, cosmos_thread_sem_t*);
Four THM_SemWait(Four, cosmos_thread_sem_t*);
Four THM_SemPost(Four, cosmos_thread_sem_t*);
Four THM_CreateMutex(cosmos_thread_mutex_t*, cosmos_thread_mutex_attr_t*);
Four THM_DestroyMutex(cosmos_thread_mutex_t*);
Four THM_AcquireMutex(cosmos_thread_mutex_t*);
Four THM_ReleaseMutex(cosmos_thread_mutex_t*);

#define cosmos_thread_mutex_create(_mp, _flag)	THM_CreateMutex(_mp, _flag)
#define cosmos_thread_mutex_destroy(_mp)	THM_DestroyMutex(_mp)
#define cosmos_thread_mutex_lock(_mp)		THM_AcquireMutex(_mp)
#define cosmos_thread_mutex_unlock(_mp)		THM_ReleaseMutex(_mp)


/*
 * Type Definition for Latch Conditions
 */
typedef enum LatchMode_tag {
    M_FREE=0x0,
    M_SHARED=0x1,
    M_EXCLUSIVE=0x2
} LatchMode;

typedef enum LatchConditional_tag {
    M_UNCONDITIONAL=0x1,
    M_CONDITIONAL=0x2,
    M_INSTANT=0x4
} LatchConditional;

typedef	struct pcell PIB;	/* PCB - process control block structure */

struct pcell {
    Four procIndex;
};

typedef struct tcell TCB;

struct tcell{
    cosmos_thread_sem_t	    semID;			/* semaphore ID for this process  old: Four semNo */
    cosmos_thread_semName_t semName[MAXSEMAPHORENAME];	/* semaphore name for this process */	
    
    /* Data structure for latch algorithm */
    LatchMode		mode;				/* latch request mode */
    LatchConditional    condition;			/* check whether M_INSTANT or not */
    TCB 		*next; 				/* physical pointer used for latch->queue structure */
    Four 		nGranted;			/* number of granted latch */
    VarArray    	*grantedLatchStruct; 		/* keep information for granted latch */
};

typedef struct GlobalHandle_T {
    Four	procIndex;
    Four 	threadIndex;
} GlobalHandle;

typedef struct {
    One_Invariable   		sync;			/* testandset target */
    One    			dummy;			/* alignment */
    LatchMode    		mode;			/* M_FREE, M_SHARD or M_EXCLUSIVE */
    Two    			latchCounter;		/* number of granted */
    GlobalHandle		grantedGlobalHandle;	/* index of process which owns this latch */
    TCB*			queue; 			/* physical pointer of waiting queue */ 
    cosmos_thread_mutex_t	mutex;			/* mutex for accessing this latch concurrently */ 
} LATCH_TYPE;

/*
** Type Definition for LRDS Open Relation Entry.
*/
typedef struct {
    Four  		count;			/* # of opens */
    Four  		clusteringIndex;	/* array index of clustering index on IndexInfo */
    RelationInfo 	ri;			/* information for relation */    
    IndexInfo    	*ii;			/* information for indexes */
    ColDesc      	*cdesc;			/* array of column descriptors */
    
    Boolean   		isCatalog;		/* is this a catalog relation? (flag) */
    LATCH_TYPE 		latch;			/* support mutex for an entry of RelTable */
    
} lrds_RelTableEntry;


/*
** Type Definition for lrds_UserOpenRelTableEntry
**   - open relation per process
**   - 'sysOrn' is an index of an entry in LRDS_RELTABLE_FOR_TMP_RELS
**     if it is a temporary relation. 'sysOrn' is an index of an entry
**     in LRDS_RELTABLE if it is a ordinary relation.
** 
*/
typedef struct {
    Four  sysOrn;		/* system open relation number */
    Four  count;			/* # of opens */
    Boolean tmpRelationFlag;	/* Is this a temporary relation? */    
} lrds_UserOpenRelTableEntry;


/* change the way of accessing tables */
/* access the fields through the perThreadTable structure */
#define LRDS_RELTABLE(_handle)			(perThreadTable[_handle].lrdsDS.lrds_table_in_memory.lrdsRelTable)
#define LRDS_RELTABLE_FOR_TMP_RELS(_handle) 	(perThreadTable[_handle].lrdsDS.lrdsRelTableForTmpRelations)
#define LRDS_USEROPENRELTABLE(_handle) 		(perThreadTable[_handle].lrdsDS.lrdsUserOpenRelTable)
#define LRDS_GET_RELTABLE_ENTRY(_handle, orn) \
((LRDS_USEROPENRELTABLE(_handle)[orn].tmpRelationFlag) ? &LRDS_RELTABLE_FOR_TMP_RELS(_handle)[LRDS_USEROPENRELTABLE(_handle)[orn].sysOrn]: &LRDS_RELTABLE(_handle)[LRDS_USEROPENRELTABLE(_handle)[orn].sysOrn])
#define LRDS_IS_TEMPORARY_RELATION(_handle, orn) (LRDS_USEROPENRELTABLE(_handle)[orn].tmpRelationFlag)

#define LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(_re)   ((IndexInfo*)_re->ii)
#define LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(_re)   ((ColDesc*)_re->cdesc)
Four SM_MLGF_InsertIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*, ObjectID*, void*, LockParameter*);
Four SM_MLGF_DeleteIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*, ObjectID*, void*, LockParameter*);

/*
** Type Definition for LRDS Scan Table Entry.
*/
typedef struct {
    Four  orn;			/* open relation number */
    Four  smScanId;		/* Scan Manager Level Scan Identifier */
    Four  nBools;		/* # of boolean expressions */
#ifdef __cplusplus
    BoolExp *boolexp;   /* array of boolean expressions */
#else
    BoolExp *bool;		/* array of boolean expressions */
#endif
    TupleID tid;		/* a current tuple id */
} lrds_ScanTableEntry;

/* access lrdsScanTable entry */
#define LRDS_SCANTABLE(_handle)        ((lrds_ScanTableEntry*)perThreadTable[_handle].lrdsDS.lrdsScanTable.ptr)

/*
** Type Definition for LRDS Collection Scan Table Etnry.
*/
typedef struct {
    Four      ornOrScanId;            /* open relation no or relation scan id for the relation containing the set */
    Boolean   useScanFlag;            /* use relation scan if TRUE */
    Two       colNo;                  /* column on which the set is defined */
    TupleID   tid;                    /* tuple containing the set */
    Four      ithElementToRead;       /* points to an element to read */
    Four      ithElementPrevRead;     /* points to the first element of previous read */
} lrds_CollectionScanTableEntry;

/* access lrdsCollectionScanTable entry */
#define LRDS_COLLECTIONSCANTABLE(_handle)     ((lrds_CollectionScanTableEntry*)perThreadTable[_handle].lrdsDS.lrdsCollectionScanTable.ptr)


/*
 * Type Definition for HeapWord
 * HeapWord is a basic unit of a heap.
 */
union _HeapWord {
    union _HeapWord 	*ptr;		/* pointer to the next control HeapWord. */
    MEMORY_ALIGN_TYPE  	dummay;		/* alignment unit */
};

typedef union _HeapWord HeapWord;
 
/*
 * Type Definition for subheap header
 * For each subheap, a subheap header is defined.
 * The subheap header is dynamically allocated at once with the subheap.
 */
struct _SubheapHdr {
    Four  		count;		/* # of heap words in the heap */
    HeapWord 		*searchPtr;	/* start position of search */
    struct _SubheapHdr 	*nextSubheap;   /* pointer to next subheap */
};

typedef struct _SubheapHdr SubheapHdr;


/*
 * Type definition for Heap.
 * Heap consists of the subheaps of same size.
 */
struct _Heap {
    Four 	elemSize;		/* element size */
    Four 	maxWordsInSubheap;	/* maximum heap words in a subheap */
    SubheapHdr  *subheapPtr;		/* pointer to the first subheap */
};

typedef struct _Heap Heap;


/*
 * Type Definition for subpool header
 * For each subpool, a subpool header is defined.
 * The subpool header is dynamically allocated at once with the subpool.
 */
struct _SubpoolHdr {
    Four        	count;		/* # of elements in the freed list */
    char  		*firstElem;	/* pointer to the first freed element */
					/* freed elements make a linked list. */
    struct _SubpoolHdr 	*nextSubpool; 	/* pointer to next subpool */
};

typedef struct _SubpoolHdr SubpoolHdr;


/*
 * Type definition for Pool.
 * Pool consists of the subpools of same size.
 */
struct _Pool {
    Four        elemSize;		/* element size */
    Four        maxElemInSubpool; 	/* maximum elements in subpool */
    SubpoolHdr 	*subpoolPtr;	 	/* pointer to the first subpool */
};

typedef struct _Pool Pool;

/*
 * Shared Memory Data Structures
 */

typedef struct {
    
    LATCH_TYPE latch_openRelation; 		       /* latch for mount table and relation table */
    lrds_MountTableEntry lrdsMountTable[MAXNUMOFVOLS]; /* Mount Table of LRDS */
    lrds_RelTableEntry lrdsRelTable[MAXNUMOFOPENRELS]; /* Open Relation Table of LRDS */
    
    Heap lrdsColumnTableHeap;			       /* heap for Column Table */
    Heap lrdsIndexTableHeap;			       /* heap for Index Table */

    Pool lrdsOrderedSetAuxColInfoPool; /* pool for Auxiliary Column Information of Ordered Set */
    
} LRDS_SHM;


/*
 * Per-Thread Data Structures
 */

#define LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS	MAX_NUM_OF_TMP_RELS
#define LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE		MAX_NUM_OF_USEROPENRELS		
#define LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE		MAXNUMOFVOLS

typedef Heap LocalHeap;
typedef Pool LocalPool;

typedef struct LRDS_PerThreadDS_T_tag {
    LRDS_SHM			lrds_table_in_memory;
    VarArray                    lrdsScanTable;
    VarArray                    lrdsSetScanTable;
    VarArray                    lrdsOrderedSetScanTable;
    VarArray                    lrdsCollectionScanTable;
    LocalHeap                   lrdsBoolTableHeap;
    LocalPool                   lrdsOrderedSetAuxColInfoLocalPool;
    lrds_RelTableEntry          lrdsRelTableForTmpRelations[LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS];
    lrds_UserOpenRelTableEntry  lrdsUserOpenRelTable[LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE];
    lrds_MountTableEntry        lrds_userMountTable[LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE];
    LocalPool                   lrdsOrderedSetElementLengthLocalPool;
    char			dummy_Bulkload[2240];
    LRDSXAscanStatus            lrds_xa_scanStatus;
    Four                        lrds_xa_preparedNum;
    Four                        lrds_xa_currentPos;
    LRDS_XA_XID*                lrds_xa_preparedList;
    Four                        lrds_xa_volId;
} LRDS_PerThreadDS_T;

typedef struct PerThreadTableEntry_T_tag {
    LRDS_PerThreadDS_T		lrdsDS;
    char			dummy_Thread[241112]; /* Be careful the size of dummy */
} PerThreadTableEntry_T;


/*
 * LRDS Reset/Get number of Disk IO
 */
Four LRDS_ResetNumberOfDiskIO(Four);
Four LRDS_GetNumberOfDiskIO(Four, Four*, Four*);

/*
 * Global Variables
 */
extern PerThreadTableEntry_T perThreadTable[MAXTHREADS];

/* Is 'x' the valid scan identifier? */
#define LRDS_VALID_ORN(_handle, x) ( ((x) >= 0) && ((x) < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE) && \
			  !LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(_handle, x) )
#define LRDS_VALID_SCANID(_handle, x) ( ((x) >= 0) && ((x) < perThreadTable[_handle].lrdsDS.lrdsScanTable.nEntries) && \
			  LRDS_SCANTABLE(_handle)[(x)].orn != NIL )

#ifdef  __cplusplus
}
#endif

#endif /* __COSMOS_R_H__ */
