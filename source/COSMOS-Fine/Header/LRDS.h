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
#ifndef _LRDS_H_
#define _LRDS_H_

#include <limits.h>		/* for CHAR_BIT */

#include "Util.h"		/* for VarArray */
#include "SM.h"                 /* for BoundCond */

#ifdef COSMOS_S
/* for porting */
#include "LRDS_dummy_for_porting.h"
#endif /* COSMOS_S */

/*
** Define Constants
*/

/* Complex Type Definitions */
#define SM_COMPLEXTYPE_BASIC 0
#define SM_COMPLEXTYPE_SET   1
#define SM_COMPLEXTYPE_ORDEREDSET 2 
#define SM_COMPLEXTYPE_COLLECTIONSET    3
#define SM_COMPLEXTYPE_COLLECTIONBAG    4
#define SM_COMPLEXTYPE_COLLECTIONLIST   5
/* maximum keyword length for SM_TEXT */
#define MAXKEYWORDLEN (MAXKEYLEN-sizeof(Two))

/* Return Values */
#define NULL_LENGTH -1


/*
** For catalog table access
*/
#define LRDS_NUMOFCATALOGTABLES 3
#define LRDS_SYSTABLES   0
#define LRDS_SYSCOLUMNS  1
#define LRDS_SYSINDEXES  2


/*
** For access of columns of catalog tables
*/
#define LRDS_SYSTABLES_NUM_COLS          6
#define LRDS_SYSTABLES_NUM_FIXEDCOLS     5
#define LRDS_SYSTABLES_NUM_VARCOLS       1
#define LRDS_SYSTABLES_DATAFILEID_COLNO  0
#define LRDS_SYSTABLES_NTUPLES_COLNO     1
#define LRDS_SYSTABLES_MAXTUPLELEN_COLNO 2
#define LRDS_SYSTABLES_NCOLUMNS_COLNO    3
#define LRDS_SYSTABLES_NINDEXES_COLNO    4
#define LRDS_SYSTABLES_RELNAME_COLNO     5

#define LRDS_SYSCOLUMNS_NUM_COLS         4 
#define LRDS_SYSCOLUMNS_NUM_FIXEDCOLS    3
#define LRDS_SYSCOLUMNS_NUM_VARCOLS      1 
#define LRDS_SYSCOLUMNS_DATAFILEID_COLNO 0
#define LRDS_SYSCOLUMNS_COLUMNNO_COLNO   1
#define LRDS_SYSCOLUMNS_COLINFO_COLNO    2
#define LRDS_SYSCOLUMNS_AUXCOLINFO_COLNO 3

#define LRDS_SYSINDEXES_NUM_COLS         3
#define LRDS_SYSINDEXES_NUM_FIXEDCOLS    3
#define LRDS_SYSINDEXES_NUM_VARCOLS      0
#define LRDS_SYSINDEXES_DATAFILEID_COLNO 0
#define LRDS_SYSINDEXES_INDEXID_COLNO    1
#define LRDS_SYSINDEXES_INDEXDESC_COLNO  2

/*
** Tuple Values of the Catalog Tables for the Tuples Stored in SM_SYSTABLES
*/
#define LRDS_SYSTABLES_MAXTUPLELEN       \
(sizeof(FileID)+sizeof(Four)+sizeof(Four)+sizeof(Two)+sizeof(Two)+MAXRELNAME)
#define LRDS_SYSTABLES_NUM_INDEXES 1
#define LRDS_SYSTABLES_RELNAME     "lrdsSysTables"

#define LRDS_SYSCOLUMNS_MAXTUPLELEN      \
(sizeof(FileID)+sizeof(Two)+sizeof(ColInfo))+sizeof(AuxColInfo_T)
#define LRDS_SYSCOLUMNS_NUM_INDEXES 1
#define LRDS_SYSCOLUMNS_RELNAME     "lrdsSysColumns"

#define LRDS_SYSINDEXES_MAXTUPLELEN      \
(sizeof(FileID)+sizeof(IndexID)+sizeof(LRDS_IndexDesc))
#define LRDS_SYSINDEXES_NUM_INDEXES 1
#define LRDS_SYSINDEXES_RELNAME     "lrdsSysIndexes"

/*
** Typedefs for TupleID
*/
typedef ObjectID TupleID;
#define MAKE_TUPLEID MAKE_OBJECTID
#define EQUAL_TUPLEID EQUAL_OBJECTID
#define SET_NILTUPLEID(tid) (tid).pageNo = NIL
#define IS_NILTUPLEID(tid) (((tid).pageNo == NIL) ? TRUE:FALSE)


/*
 * Comparison result
 */
#define EQUAL 0
#define GREAT 1
#define LESS  2


/*
** SM call parameter constant
*/
#define LRDS_DEFF  100
#define LRDS_BEFF  100


/* type definition for the Set_ElementLength : length of an element */
typedef Two Set_ElementLength;


/*
** Typedefs for TupleHdr
*/
#define TUPLE_HEADER_FIXED (sizeof(Two)*2 + sizeof(Four))

typedef struct {
    Two nFixedCols;		/* # of fixed-length columns stored */
    Two nVarCols;		/* # of variable-length columns stored */
    Four firstVarColOffset;	/* starting offset of the first variable-length column */

    Four varColOffset[MAXNUMOFCOLS]; /* starting offset of next variable column */
    /* The nullVector may be placed in the 'varColOffset' array. */
    /* So the 'nullVector' field doesn't used explicitly. */
    /* Rather than it reserves some space. */
    unsigned char nullVector[(MAXNUMOFCOLS+(CHAR_BIT-1))/CHAR_BIT]; /* bit array of null flags */

} TupleHdr;

#define TUPLE_NULLVECTOR_SIZE(_nCols) ( ((_nCols)+CHAR_BIT-1)/CHAR_BIT )
#define TUPLE_HEADER_SIZE(_nCols, _nVarCols) \
(TUPLE_HEADER_FIXED + sizeof(Four)*(_nVarCols) + TUPLE_NULLVECTOR_SIZE(_nCols))


/*
** Type Definition for LRDS_MBR
*/
typedef struct {
    MLGF_HashValue values[MBR_NUM_PARTS];
} LRDS_MBR;


/*
** Type Definition for ColListStruct
*/
/* 
   Added the following macro in LOM layer, we have to redefine ColListStruct for new types.
   That's why I put the following macro.
 */
#ifndef LOM_DEFINED
typedef struct {
    Two 	colNo;			/* IN column number */
    Boolean 	nullFlag;           	/* TRUE if it has null value */
    Four 	start;			/* IN starting offset within a column */
					/*   ALL_VALUE: read all data of this column */
    Four 	length;			/* IN amount of old data within a column */
    Four 	dataLength;		/* IN amount of new data */
    union {
	Two_Invariable		s;		/* SM_SHORT */
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
    Four 	retLength;		/* OUT return length of Read/Write */
} ColListStruct;
#endif

typedef struct {
    Two  colNo;
    Four length;
}ColLengthInfoListStruct;

#define ALL_VALUE   -1		/* special value of 'start' */
#define TO_END      -1		/* special value of 'length' */


/*
** Type Definition for ColInfo
*/
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
** Type Definition for KeyInfo
** It is same as SortKeyDesc except the naming.
*/
#define KEYINFO_COL_ORDER   0x3 /* ORDER mask */
#define KEYINFO_COL_ASC     0x2 /* ascending order */
#define KEYINFO_COL_DESC    0x1 /* descending order */
typedef struct {
    Two flag;                   /* KEYFLAG_UNIQUE, KEYFLAG_CLUSTERING, ... */
    Two nColumns;                 /* # of key parts */
    struct {
        Four colNo;
        Four flag;              /* ascending/descendig */
    } columns[MAXNUMKEYPARTS];
} KeyInfo;

/*
** Type Definition for MLGF_KeyInfo
*/
typedef struct {
    Two flag;			/* CLUSTERING, ... */
    Two nColumns;		/* # of columns on which the index is defined */
    Two colNo[MLGF_MAXNUM_KEYS]; /* column numbers */
    Two extraDataLen;		/* length of the extra data for an object */
} MLGF_KeyInfo;

/*
** Type Definition for ColDesc
**   - in-core memory data structure
**   - ColInfo is used after converted to ColDesc
*/

typedef struct {
    Two  			complexType;	/* SM_COMPLEX_BASIC, SM_COMPLEX_SET */
    Two  			type;		/* SM_SHORT, SM_LONG, SM_FLOAT, ... */
    Two  			varColNo;	/* column number of variable-length columns */
    Two  			fixedColNo;	/* column number of fixed-length columns */
    Four 			offset;		/* offset of the field */
    Four 			length;		/* length of the field */
    LOGICAL_PTR_TYPE(void *)	auxInfo; 	/* auxiliary column information */
} ColDesc;

/*
** Type Definition for Boolean Expression
*/
typedef struct {
    Two  	op;		/* operator : EQUAL, LESS, etc */
    Two  	colNo;		/* which column ? */
    Two  	length;		/* length of the value: used for SM_VARSTRING */
    union {
	Two_Invariable    	s;		/* SM_SHORT */
	Four_Invariable    	i;		/* SM_INT */
	Four_Invariable		l;		/* SM_LONG */
	Eight_Invariable   	ll;		/* SM_LONG_LONG */
	float  			f;		/* SM_FLOAT */
	double 			d;		/* SM_DOUBLE */
	PageID 			pid;		/* SM_PAGEID, SM_FILEID, SM_INDEXID */
        FileID                  fid;            /* SM_FILEID */
        IndexID                 iid;            /* SM_INDEXID */
	OID    			oid;		/* SM_OID */
	LRDS_MBR 		mbr;		/* SM_MBR */
	char   			str[MAXKEYLEN];	/* SM_STRING or SM_VARSTRING */
    } data;					/* the value to be compared */
} BoolExp;

/*
** Type Definition for LRDS Mount Table Entry.
*/
typedef struct {
    Two  volId;			/* volume identifier */
    /* open relation numbers of catalog tables */
    Four catalogTableOrn[LRDS_NUMOFCATALOGTABLES];

    Four nMount;		/* number of Mount */
} lrds_MountTableEntry;


/*
** Type Definition for LRDS Scan Table Entry.
*/
typedef struct {
    Four  orn;			/* open relation number */
    Four  smScanId;		/* Scan Manager Level Scan Identifier */
    Four  nBools;		/* # of boolean expressions */
    BoolExp *bool;		/* array of boolean expressions */
    TupleID tid;		/* a current tuple id */
} lrds_ScanTableEntry;

/*
** Type Definition for LRDS Collection Scan Table Etnry.
*/
typedef struct {
    Four		ornOrScanId;			/* open relation no or relation scan id for the relation containing the set */
    Boolean		useScanFlag;        		/* use relation scan if TRUE */
    Two			colNo;				/* column on which the set is defined */
    TupleID		tid;				/* tuple containing the set */
    Four		ithElementToRead;		/* points to an element to read */
    Four		ithElementPrevRead;		/* points to the first element of previous read */
} lrds_CollectionScanTableEntry;

/* access lrdsCollectionScanTable entry */
#define LRDS_COLLECTIONSCANTABLE(_handle) ((lrds_CollectionScanTableEntry*)(perThreadTable[_handle].lrdsDS.lrdsCollectionScanTable.ptr))
/* Is 'x' the valid scan identifier? */
#define LRDS_VALID_COLLECTIONSCANID(_handle, x) \
    ( ((x) >= 0) && ((x) < perThreadTable[_handle].lrdsDS.lrdsCollectionScanTable.nEntries) && \
    LRDS_COLLECTIONSCANTABLE(_handle)[(x)].ornOrScanId != NIL )


/*
** Type Definition for LRDS Set Scan Table Etnry.
*/
typedef struct {
    Four ornOrRelScanId;	/* open relation no or relation scan id for the relation containing the set */
    Two colNo;			/* column on which the set is defined */
    Boolean useRelScanFlag;     /* use relation scan if TRUE */
    TupleID tid;		/* tuple containing the set */
    Four byteOffsetToRead;	/* points to an element to read */
    Four byteOffsetPrevRead;	/* points to the first element of previous read */
} lrds_SetScanTableEntry;


typedef struct OrderedSetColHdr_T_tag {
    Four nElements;
#ifdef COMPRESSION 
    VolNo volNo;
    Four  lastDocId;
#endif
    IndexID nestedIndexId;
    Four nSlots;
    Four freeSlotListHdr;
} OrderedSetColHdr_T;

/*
** Type Definition for LRDS Ordered Set Scan Table Etnry.
*/
#ifndef ORDEREDSET_BACKWARD_SCAN
typedef struct {
    Four ornOrRelScanId;	/* open relation no or relation scan id for the relation containing the ordered set */
    Two colNo;			/* column on which the ordered set is defined */
    Boolean useRelScanFlag;     /* use relation scan if TRUE */
    TupleID tid;		/* tuple containing the ordered set */
    Four byteOffsetToRead;	/* points to an element to read */
    Four byteOffsetPrevRead;	/* points to the first element of previous read */
    OrderedSetColHdr_T orderedSetColHdr; /* column header */
} lrds_OrderedSetScanTableEntry;
#else
typedef struct {
    Four 			ornOrRelScanId;        			/* open relation no or relation scan id for the relation containing the ordered set */
    Two 			colNo;                  		/* column on which the ordered set is defined */
    Boolean 			useRelScanFlag;     			/* use relation scan if TRUE */
    TupleID 			tid;                			/* tuple containing the ordered set */
    Four 			byteOffsetOfElementOrderedOffset;  	/* points to an element ordered offset to read */
    Four 			byteOffsetOfElement;      	     	/* points to an element to read */
    Four 			scanDirection;                 		/* direction of scanning */
    OrderedSet_ElementLength	*ptrOfElementLengthArray;		/* points to an element length array */
    Four                        firstElementIndexOfElementLengthArray;  /*first element index of element length array */
    Four                        nElementOfElementLengthArray;           /* num of elemnts of element length array */
#ifdef COMPRESSION 
    Four			lastDocId;
#endif
    OrderedSetColHdr_T 		orderedSetColHdr; /* column header */
} lrds_OrderedSetScanTableEntry;
#endif

/* access lrdsOrderedSetScanTable entry */
#define LRDS_ORDEREDSETSCANTABLE(_handle) ((lrds_OrderedSetScanTableEntry*)(perThreadTable[_handle].lrdsDS.lrdsOrderedSetScanTable.ptr))

/* access lrdsOrderedSetElementLengthLocalPool */
#define LRDS_ORDEREDSET_ELEMENT_LENGTH_POOL(_handle) (perThreadTable[_handle].lrdsDS.lrdsOrderedSetElementLengthLocalPool)

/* Is 'x' the valid scan identifier? */
#define LRDS_VALID_ORDEREDSETSCANID(_handle, x) \
	( ((x) >= 0) && ((x) < perThreadTable[_handle].lrdsDS.lrdsOrderedSetScanTable.nEntries) && \
	  LRDS_ORDEREDSETSCANTABLE(_handle)[(x)].ornOrRelScanId != NIL )


/*
** Type Definition for RelationInfo.
*/
typedef struct {
    ObjectID 		catalogEntry;		/* ObjectID of an entry in LRDS_SYSTABLES */
    FileID 		fid;			/* identifier of data file mapped to this relation */
    Four 		nTuples;		/* number of tuples */
    Four 		maxTupleLen;		/* maximum length of a tuple */
    Two  		nIndexes;		/* number of indexes */
    Two  		nColumns;		/* number of columns */
    Two  		nVarColumns;		/* number of variable-length columns */
    char 		relName[MAXRELNAME];	/* relation name */

} RelationInfo;


/*
** Type Definition for IndexInfo.
*/
typedef struct {
    IndexID iid;		/* index identifier */
    union {
	KeyDesc btree;		/* a key descriptor for btree */
	MLGF_KeyDesc mlgf;	/* a key descriptro for mlgf */
    } kdesc;
    Two     colNo[MAXNUMKEYPARTS];	/* column numbers */
    One     indexType;		/* index type */
} IndexInfo;


/*
** Type Definition for LRDS Open Relation Entry.
*/
typedef struct {
    Four 				count;			/* # of opens */
    Four 				clusteringIndex;	/* array index of clustering index on IndexInfo */
    RelationInfo 			ri;			/* information for relation */
    LOGICAL_PTR_TYPE(IndexInfo *) 	ii; 			/* information for indexes */ 
    LOGICAL_PTR_TYPE(ColDesc *) 	cdesc; 			/* array of column descriptors */ 

    Boolean   				isCatalog;		/* is this a catalog relation? (flag) */
    LATCH_TYPE 				latch;			/* support mutex for an entry of RelTable */
	
} lrds_RelTableEntry;


/*
** Macros for access to System Relation Table
*/
#define LRDS_GET_COLINFO_FROM_COLDESC(cinfo, cdesc) \
BEGIN_MACRO \
(cinfo).complexType = (cdesc).complexType; \
(cinfo).type = (cdesc).type; \
(cinfo).length = (cdesc).length; \
END_MACRO

#define LRDS_VARCOL_START_OFFSET(_varColNo, _tupHdr) \
    ((_varColNo == 0) ? (_tupHdr).firstVarColOffset : (_tupHdr).varColOffset[_varColNo - 1])

#define LRDS_VARCOL_END_OFFSET_PLUS_1(_varColNo, _tupHdr) \
    ((_tupHdr).varColOffset[_varColNo])

#define LRDS_VARCOL_REAL_SIZE(_varColNo, _start, _tupHdr) \
    ((_tupHdr).varColOffset[_varColNo] - _start)

#define LRDS_IS_VAR_LENGTH_COLUMN(complexType, type) \
    (complexType != SM_COMPLEXTYPE_BASIC || (type == SM_VARSTRING || type == SM_TEXT))

#define LRDS_IS_ACCESSED_BY_PTR(complexType, type) \
    (complexType == SM_COMPLEXTYPE_BASIC && (type == SM_VARSTRING || type == SM_STRING || type == SM_TEXT))

#define LRDS_CAN_BE_ACCESSED_PARTIALLY(complexType, type) \
    (complexType == SM_COMPLEXTYPE_BASIC && (type == SM_VARSTRING || type == SM_STRING || type == SM_TEXT))


/* For LRDS_RELTABLE definition, refer to LRDS_SHM.h */
/* #define LRDS_RELTABLE lrds_shmPtr->lrdsRelTable */
#define LRDS_NUM_OF_ENTRIES_OF_RELTABLE MAXNUMOFOPENRELS
#define LRDS_SET_TO_UNUSED_ENTRY_OF_RELTABLE(entryNo) LRDS_RELTABLE[entryNo].count = NIL
#define LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(entryNo) (LRDS_RELTABLE[entryNo].count == NIL)


/*
** Macros for access to temporary relations
*/
#define LRDS_RELTABLE_FOR_TMP_RELS(_handle) (perThreadTable[_handle].lrdsDS.lrdsRelTableForTmpRelations)
#define LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS MAX_NUM_OF_TMP_RELS
#define LRDS_SET_TO_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(_handle, entryNo) LRDS_RELTABLE_FOR_TMP_RELS(_handle)[entryNo].count = NIL
#define LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(_handle, entryNo) (LRDS_RELTABLE_FOR_TMP_RELS(_handle)[entryNo].count == NIL)

#define LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(_handle) (perThreadTable[_handle].lrdsDS.lrdsOrderedSetAuxColInfoLocalPool)

/*
** Type Definition for lrds_SysTablesOverlay
**   - access the catalog table entry of LRDS_SYSTABLES
*/
typedef struct {
    FileID fid;			/* data file corresponding to the relation */
    Four   nTuples;		/* number of tuples inserted */
    Four   maxTupleLen;		/* maximum tuple length */
    Two    nIndexes;		/* number of indexes defined */
    Two    nColumns;		/* number of columns of the relation */
    char   name[MAXRELNAME];	/* relation name */
} lrds_SysTablesOverlay;


/*
** Type Definition for lrds_SysIndexesOverlay
**   - access the catalog table entry of LRDS_SYSINDEXES
*/
typedef struct {
    FileID  fid;		/* data file corresponding to the relation */
    KeyInfo kinfo;		/* key descriptor */
    IndexID iid;		/* index identifier */
} lrds_SysIndexesOverlay;


/*
** Type Definition for lrds_SysColumnsOverlay
**   - access the catalog table entry of LRDS_SYSCOLUMNS
*/
typedef struct {
    FileID  fid;		/* data file corresponding to the relation */
    Two     colNo;		/* column number */
    ColInfo cinfo;		/* column information */
} lrds_SysColumnsOverlay;


/*
** Type Definition for LRDS_IndexDesc
**   - access INDEXDESC column of the ctalog table LRDS_SYSINDEXES
*/
typedef struct {
    One indexType;              /* SM_INDEXTYPE_BTREE, SM_INDEXTYPE_MLGF */
    union {
	KeyInfo btree;		/* Key Information for Btree */
	MLGF_KeyInfo mlgf;	/* Key Information for MLGF */
    } kinfo;
} LRDS_IndexDesc;

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
    Four  count;		/* # of opens */
    Boolean tmpRelationFlag;	/* Is this a temporary relation? */
} lrds_UserOpenRelTableEntry;

#define LRDS_USEROPENRELTABLE(_handle) (perThreadTable[_handle].lrdsDS.lrdsUserOpenRelTable)
#define LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE MAX_NUM_OF_USEROPENRELS
#define LRDS_SET_TO_UNUSED_ENTRY_OF_USEROPENRELTABLE(_handle, entryNo) \
(LRDS_USEROPENRELTABLE(_handle)[entryNo].count = NIL)
#define LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(_handle, entryNo) \
(LRDS_USEROPENRELTABLE(_handle)[entryNo].count == NIL)
#define LRDS_GET_RELTABLE_ENTRY(_handle, orn) \
((LRDS_USEROPENRELTABLE(_handle)[orn].tmpRelationFlag) ? &LRDS_RELTABLE_FOR_TMP_RELS(_handle)[LRDS_USEROPENRELTABLE(_handle)[orn].sysOrn]: &LRDS_RELTABLE[LRDS_USEROPENRELTABLE(_handle)[orn].sysOrn])


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
    TupleID tid;		/* object pointed by the cursor */
} LRDS_AnyCursor;

/* DataCursor:
 *  sequential scan using the data file
 */
typedef struct {
    One      flag;		/* state of the cursor */
    TupleID  tid;		/* object pointed by the cursor */
} LRDS_DataCursor;

/* BtreeCursor:
 *  scan using a B+ tree
 */
typedef struct {
    One      flag;		/* state of the cursor */
    TupleID tid;		/* object pointed by the cursor */
    KeyValue key;		/* what key value? */
    char opaque[sizeof(Two)+sizeof(PageID)*2+sizeof(Two)*2]; /* sizeof(Two) is for the alignment */
} LRDS_BtreeCursor;

typedef struct {
    One      flag;		/* state of the cursor */
    TupleID tid;		/* object pointed by the cursor */
    MLGF_HashValue keys[MLGF_MAXNUM_KEYS]; /* what key values? */
    char opaque[sizeof(PageID)*2 + sizeof(Two)*3 + sizeof(VarArray)];
} LRDS_MLGF_Cursor;

/* Universal Cursor */
typedef union {
    LRDS_AnyCursor any;		/* for access of 'flag' and 'oid' */
    LRDS_DataCursor seq;	/* sequential scan */
    LRDS_BtreeCursor btree;	/* scan using a B+ tree */
    LRDS_MLGF_Cursor mlgf;	/* scan using MLGF index */
} LRDS_Cursor;

/*
** Macros
*/

/* return starting point of the null vector in tuple header */
#define NULLVECTOR_PTR(tupHdr, nVarCols) ((unsigned char*)&((tupHdr).varColOffset[nVarCols]))

/* access lrdsScanTable entry */
#define LRDS_SCANTABLE(_handle)        ((lrds_ScanTableEntry*)(perThreadTable[_handle].lrdsDS.lrdsScanTable.ptr))

/* access lrdsSetScanTable entry */
#define LRDS_SETSCANTABLE(_handle)     ((lrds_SetScanTableEntry*)(perThreadTable[_handle].lrdsDS.lrdsSetScanTable.ptr))

#define LRDS_BOOLTABLEHEAP(_handle)    (perThreadTable[_handle].lrdsDS.lrdsBoolTableHeap)


/* access lrds_userMountTable entry */
#define LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE  MAXNUMOFVOLS
#define LRDS_USERMOUNTTABLE(_handle)   (perThreadTable[_handle].lrdsDS.lrds_userMountTable)
#define LRDS_SET_TO_UNUSED_ENTRY_OF_USERMOUNTTABLE(_handle, entryNo) \
(LRDS_USERMOUNTTABLE(_handle)[entryNo].volId = NIL)
#define LRDS_IS_UNUSED_ENTRY_OF_USERMOUNTTABLE(_handle, entryNo) \
(LRDS_USERMOUNTTABLE(_handle)[entryNo].volId == NIL)

/* mount table in shared memory */
/* For LRDS_MOUNTTABLE definition, refer to LRDS_SHM.h */
/* #define LRDS_USERMOUNTTABLE   lrds_userMountTable */
#define LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE  MAXNUMOFVOLS
#define LRDS_SET_TO_UNUSED_ENTRY_OF_MOUNTTABLE(entryNo) \
(LRDS_MOUNTTABLE[entryNo].volId = NIL)
#define LRDS_IS_UNUSED_ENTRY_OF_MOUNTTABLE(entryNo) \
(LRDS_MOUNTTABLE[entryNo].volId == NIL)

/* Is 'x' the valid scan identifier? */
#define LRDS_VALID_ORN(_handle, x) ( ((x) >= 0) && ((x) < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE) && \
			  !LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(_handle, x) )

#define LRDS_VALID_SCANID(_handle, x) ( ((x) >= 0) && ((x) < perThreadTable[_handle].lrdsDS.lrdsScanTable.nEntries) && \
			               LRDS_SCANTABLE(_handle)[(x)].orn != NIL )

/* Is 'x' the valid scan identifier? */
#define LRDS_VALID_SETSCANID(_handle, x) ( ((x) >= 0) && ((x) < perThreadTable[_handle].lrdsDS.lrdsSetScanTable.nEntries) && \
			                  LRDS_SETSCANTABLE(_handle)[(x)].ornOrRelScanId != NIL )

#define LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(_handle, v,i) \
(&LRDS_RELTABLE[ LRDS_USEROPENRELTABLE(_handle)[ LRDS_USERMOUNTTABLE(_handle)[v].catalogTableOrn[i] ].sysOrn ])

#define LRDS_IS_TEMPORARY_RELATION(_handle, orn) (LRDS_USEROPENRELTABLE(_handle)[orn].tmpRelationFlag)

#define LRDS_IS_VAR_LENGTH_TYPE(complexType, type) \
    (complexType == SM_COMPLEXTYPE_SET || \
     complexType == SM_COMPLEXTYPE_ORDEREDSET || \
     complexType == SM_COMPLEXTYPE_COLLECTIONSET || \
     complexType == SM_COMPLEXTYPE_COLLECTIONBAG || \
     complexType == SM_COMPLEXTYPE_COLLECTIONLIST || \
     type == SM_VARSTRING || type == SM_TEXT)

#define LRDS_HAS_ITS_OWN_API(complexType, type) \
    (complexType == SM_COMPLEXTYPE_SET || \
     complexType == SM_COMPLEXTYPE_ORDEREDSET || \
     complexType == SM_COMPLEXTYPE_COLLECTIONSET || \
     complexType == SM_COMPLEXTYPE_COLLECTIONBAG || \
     complexType == SM_COMPLEXTYPE_COLLECTIONLIST)

#define LRDS_HAS_AUXCOLINFO(_complexType, _type) \
    ((_complexType) == SM_COMPLEXTYPE_ORDEREDSET)

#define LRDS_CREATEOBJECT(_handle, _fid, _smScanId, _useScanFlag, _nearOid, _objHdr, _length, _data, _oid, _fileLockup, _objLockup) \
( (_useScanFlag) ? \
  SM_CreateObject(_handle, _smScanId, _nearOid, _objHdr, _length, _data, _oid, _objLockup) : \
  SM_CreateObjectWithoutScan(_handle, _fid, _nearOid, _objHdr, _length, _data, _oid, _fileLockup, _objLockup))

#define LRDS_FETCHOBJECT(_handle, _fid, _smScanId, _useScanFlag, _oid, _start, _length, _buf, _fileLockup, _objLockup) \
( (_useScanFlag) ? \
  SM_FetchObject(_handle, _smScanId, _oid, _start, _length, _buf, _objLockup) : \
  SM_FetchObjectWithoutScan(_handle, _fid, _oid, _start, _length, _buf, _fileLockup, _objLockup))

#define LRDS_UPDATEOBJECT(_handle, _fid, _smScanId, _useScanFlag, _oid, _start, _length, _data, _dataLength, _fileLockup, _objLockup) \
( (_useScanFlag) ? \
  SM_UpdateObject(_handle, _smScanId, _oid, _start, _length, _data, _dataLength, _objLockup) : \
  SM_UpdateObjectWithoutScan(_handle, _fid, _oid, _start, _length, _data, _dataLength, _fileLockup, _objLockup))

#define LRDS_DESTROYOBJECT(_handle, _fid, _smScanId, _useScanFlag, _oid, _fileLockup, _objLockup) \
( (_useScanFlag) ? \
  SM_DestroyObject(_handle, _smScanId, _oid, _objLockup) : \
  SM_DestroyObjectWithoutScan(_handle, _fid, _oid, _fileLockup, _objLockup))

#define LRDS_HAS_NULL_VALUE(_colNo, _cdesc, _tupHdr, _nullVector) \
( ( (_colNo) >= ((_tupHdr).nFixedCols + (_tupHdr).nVarCols) ) || \
  BITTEST(_nullVector, _colNo) )

#ifdef LOM_DEFINED 
#include "LOM_Common.h"
#endif



/*-------------------- BEGIN OF Shared Memory Section -----------------------*/
/*
 * Shared Memory Data Structures
 */
typedef struct {

    LATCH_TYPE latch_openRelation; /* latch for mount table and relation table */
    lrds_MountTableEntry lrdsMountTable[MAXNUMOFVOLS]; /* Mount Table of LRDS */
    lrds_RelTableEntry lrdsRelTable[MAXNUMOFOPENRELS]; /* Open Relation Table of LRDS */

    Heap lrdsColumnTableHeap;			       /* heap for Column Table */
    Heap lrdsIndexTableHeap;			       /* heap for Index Table */

    Pool lrdsOrderedSetAuxColInfoPool; /* pool for Auxiliary Column Information of Ordered Set */ 

} LRDS_SHM;

extern LRDS_SHM *lrds_shmPtr;
extern Four procIndex;

/* macro definition */

#define LRDS_MOUNTTABLE			lrds_shmPtr->lrdsMountTable
#define LRDS_RELTABLE			lrds_shmPtr->lrdsRelTable
#define LRDS_LATCH_OPENRELATION   	lrds_shmPtr->latch_openRelation
#define LRDS_INDEXTABLEHEAP		lrds_shmPtr->lrdsIndexTableHeap
#define LRDS_COLUMNTABLEHEAP		lrds_shmPtr->lrdsColumnTableHeap
#define LRDS_ORDEREDSET_AUXCOLINFO_POOL lrds_shmPtr->lrdsOrderedSetAuxColInfoPool 

/*-------------------- END OF Shared Memory Section -------------------------*/

#define LRDS_COUNTER_NAME_MAX_LEN       50


/* typedef TupleID CounterID; */ 

typedef struct lrds_SysCountersOverlay_T_tag {
    char counterName[LRDS_COUNTER_NAME_MAX_LEN+1];
    Four counterValue;
} lrds_SysCountersOverlay_T;

/*
** Noninterface Function Prototypes of LRDS
*/
Four lrds_FormatDataVolume(Four, Four, char**, char*, Four, Four, Four*, Four);
Four lrds_AddColumns(Four, Four, Boolean, TupleID*, TupleHdr*);
Four lrds_Set_RemoveFromIndex(Four, Four, Boolean, Four, TupleID*, Four, Four, Four, LockParameter*, LockParameter*);
Four lrds_Set_InsertIntoIndex(Four, Four, Boolean, IndexInfo*, TupleID*, LockParameter*, LockParameter*);
void lrds_KeyInfoToKeyDesc(Four, ColInfo*, ColDesc*, KeyInfo*, KeyDesc*);
void lrds_MLGF_KeyInfoToMLGF_KeyDesc(Four, ColInfo*, ColDesc*, MLGF_KeyInfo*, MLGF_KeyDesc*);
Boolean lrds_CheckColInfo(Four, Four, ColInfo*);
Boolean lrds_CheckKeyInfo(Four, Four, ColInfo*, ColDesc*, KeyInfo*);
Boolean lrds_CheckMLGF_KeyInfo(Four, Four, ColInfo*, ColDesc*, MLGF_KeyInfo*); 
Boolean lrds_CheckIndexDesc(Four, Four, ColInfo*, ColDesc*, LRDS_IndexDesc*); 
Four lrds_CloseSharedRelation(Four, Four); 
Four lrds_Dismount(Four, Four);	    

Four lrds_Collection_Create(Four, Four, Boolean, TupleID*, Four, Four);
Four lrds_Collection_Destroy(Four, Four, Boolean, TupleID*, Four);

Four lrds_Collection_GetN_Elements(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four*);
Four lrds_Collection_SetN_Elements(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four);

Four lrds_Collection_Assign(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);

Four lrds_Collection_FixedElementInsert(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four, void*);
Four lrds_Collection_FixedElementDelete(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four);
Four lrds_Collection_FixedElementFetch(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four, void*);
Four lrds_Collection_FixedElementUpdate(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four, void*);

Four lrds_Collection_VarElementInsert(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four, Four*, void*);
Four lrds_Collection_VarElementDelete(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four);
Four lrds_Collection_VarElementFetchSizes(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four, Four*);
Four lrds_Collection_VarElementFetch(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four, void*);
Four lrds_Collection_VarElementUpdate(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four, Four, Four*, void*);

Four lrds_Collection_FixedElementRemoveFromIndex(Four, Four, Boolean, IndexInfo*, TupleID*, Four, Four, void*, LockParameter*);
Four lrds_Collection_FixedElementInsertIntoIndex(Four, Four, Boolean, IndexInfo*, TupleID*, Four, Four, void*, LockParameter*);

Four lrds_Collection_VarElementRemoveFromIndex(Four, Four, Boolean, IndexInfo*, TupleID*, Four, Four, Four*, void*, LockParameter*);
Four lrds_Collection_VarElementInsertIntoIndex(Four, Four, Boolean, IndexInfo*, TupleID*, Four, Four, Four*, void*, LockParameter*);

Four lrds_Collection_Scan_Open(Four, Four, Boolean, TupleID*, Four);
Four lrds_Collection_Scan_Close(Four, Four);
Four lrds_Collection_Scan_NextElements(Four, Four, Four, Four*, Four, void*); 
Four lrds_Collection_Scan_GetSizeOfNextElements(Four, Four, Four, Four*);
Four lrds_Collection_Scan_InsertElements(Four, Four, Four, Four*, void*);
Four lrds_Collection_Scan_DeleteElements(Four, Four);

Four lrds_Collection_InsertIntoIndex(Four, Four, Boolean, IndexInfo*, TupleID*, Four, LockParameter*, LockParameter*);
Four lrds_Collection_RemoveFromIndex(Four, Four, Boolean, IndexInfo*, TupleID*, Four, LockParameter*, LockParameter*);

Four lrds_Collection_CompareData(Four, Four, Four, void*, Four, void*, Four);
Four lrds_Collection_GetKeySize(Four, Four, Boolean, TupleID*, TupleHdr*, Four, Four*);
Four lrds_Collection_IsNull(Four, Four, Boolean, TupleID*, Four); 

/*
** Interface Function Prototypes of LRDS
*/
Four LRDS_AddIndex(Four, Four, char*, LRDS_IndexDesc*, IndexID*); 
Four LRDS_AddColumn(Four, Four, char*, ColInfo*); 
Four LRDS_CloseAllRelations(Four); 
Four LRDS_CloseRelation(Four, Four);
Four LRDS_CloseAllScans(Four); 
Four LRDS_CloseScan(Four, Four);
Four LRDS_CreateRelation(Four, Four, char*, LRDS_IndexDesc*, Four, ColInfo*, Boolean); 
Four LRDS_CreateTuple(Four, Four, Boolean, Four, ColListStruct*, TupleID*);
Four LRDS_DestroyRelation(Four, Four, char*);
Four LRDS_DestroyTuple(Four, Four, Boolean, TupleID*);
Four LRDS_Dismount(Four, Four);
Four LRDS_DropIndex(Four, Four, char*, IndexID*);
char *LRDS_Err(Four, Four);
Four LRDS_FetchTuple(Four, Four, Boolean, TupleID*, Four, ColListStruct*);
Four LRDS_FetchColLength(Four, Four, Boolean, TupleID*, Four, ColLengthInfoListStruct*);
Four LRDS_Final();
Four LRDS_Init();
Four LRDS_Mount(Four, Four, char**, Four*);
Four LRDS_NextTuple(Four, Four, TupleID*, LRDS_Cursor**); 
Four LRDS_OpenRelation(Four, Four, char*);
Four LRDS_OpenIndexScan(Four, Four, IndexID*, BoundCond*, BoundCond*, Four, BoolExp*, LockParameter*);
Four LRDS_OpenSeqScan(Four, Four, Four, Four, BoolExp*, LockParameter*);
Four LRDS_UpdateTuple(Four, Four, Boolean, TupleID*, Four, ColListStruct*);
Four LRDS_Set_Create(Four, Four, Boolean, TupleID*, Four);
Four LRDS_Set_Destroy(Four, Four, Boolean, TupleID*, Four);
Four LRDS_Set_InsertElements(Four, Four, Boolean, TupleID*, Four, Four, void*);
Four LRDS_Set_DeleteElements(Four, Four, Boolean, TupleID*, Four, Four, void*);
Four LRDS_Set_IsMember(Four, Four, Boolean, TupleID*, Four, void*);
Four LRDS_Set_Scan_Open(Four, Four, Boolean, TupleID*, Four);
Four LRDS_Set_Scan_Close(Four, Four);
Four LRDS_Set_Scan_NextElements(Four, Four, Four, void*);
Four LRDS_Set_Scan_InsertElements(Four, Four, Four, void*);
Four LRDS_Set_Scan_DeleteElements(Four, Four);
Four LRDS_Set_IsNull(Four, Four, Boolean, TupleID*, Four); 
Four LRDS_Text_AddKeywords(Four, Four, Boolean, TupleID*, Four, Four, char*);
Four LRDS_Text_DeleteKeywords(Four, Four, Boolean, TupleID*, Four, Four, char*);
Four LRDS_Text_GetIndexID(Four, Four, Four, IndexID*);
Four LRDS_MLGF_OpenIndexScan(Four, Four, IndexID*, MLGF_HashValue[], MLGF_HashValue[], Four, BoolExp[], LockParameter*);
Four LRDS_MLGF_SearchNearTuple(Four, Four, IndexID*, MLGF_HashValue[], TupleID*, LockParameter*);
Four LRDS_InitLocalDS(Four);
Four LRDS_InitSharedDS(Four);
Four LRDS_FinalLocalDS(Four);
Four LRDS_FinalSharedDS(Four);
Four LRDS_FormatDataVolume(Four, Four, char**, char*, Four, Four, Four*, Four);
Four LRDS_FormatTempDataVolume(Four, Four, char**, char*, Four, Four, Four*, Four);
Four LRDS_FormatLogVolume(Four, Four, char**, char*, Four, Four, Four*);
Four LRDS_ExpandDataVolume(Four, Four, Four, char**, Four*); 
Four LRDS_FormatCoherencyVolume(Four, char*, char*, Four);
Four LRDS_GetFileIdOfRelation(Four, Four, char*, FileID*);
Four LRDS_SortRelation(Four, Four, Four, char*, KeyInfo*, Boolean, char*, Boolean, LockParameter*); 
Four LRDS_BeginTransaction(Four, XactID*, ConcurrencyLevel); 
Four LRDS_AbortTransaction(Four, XactID*);
Four LRDS_CommitTransaction(Four, XactID*);
Four LRDS_CollectionSet_Create(Four, Four, Boolean, TupleID*, Four, Four);
Four LRDS_CollectionSet_Destroy(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_GetN_Elements(Four, Four, Boolean, TupleID*, Four, Four*);
Four LRDS_CollectionSet_Assign(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_AssignElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionSet_InsertElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionSet_DeleteElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionSet_DeleteAll(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_IsMember(Four, Four, Boolean, TupleID*, Four, Four, void*);
Four LRDS_CollectionSet_IsEqual(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_IsSubset(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_RetrieveElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionSet_GetSizeOfElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*);
Four LRDS_CollectionSet_Union(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_Intersect(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_Difference(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_UnionWith(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_IntersectWith(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_DifferenceWith(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_Scan_Open(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionSet_Scan_Close(Four, Four);
Four LRDS_CollectionSet_Scan_NextElements(Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionSet_Scan_GetSizeOfNextElements(Four, Four, Four, Four*);
Four LRDS_CollectionSet_Scan_InsertElements(Four, Four, Four, Four*, void*);
Four LRDS_CollectionSet_Scan_DeleteElements(Four, Four);
Four LRDS_CollectionSet_IsNull(Four, Four, Boolean, TupleID*, Four); 

Four LRDS_CollectionBag_Create(Four, Four, Boolean, TupleID*, Four, Four);
Four LRDS_CollectionBag_Destroy(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_GetN_Elements(Four, Four, Boolean, TupleID*, Four, Four*);
Four LRDS_CollectionBag_Assign(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_AssignElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionBag_InsertElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionBag_DeleteElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionBag_DeleteAll(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_IsMember(Four, Four, Boolean, TupleID*, Four, Four, void*);
Four LRDS_CollectionBag_IsEqual(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_IsSubset(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_RetrieveElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionBag_GetSizeOfElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*);
Four LRDS_CollectionBag_Union(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_Intersect(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_Difference(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_UnionWith(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_IntersectWith(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_DifferenceWith(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_Scan_Open(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionBag_Scan_Close(Four, Four);
Four LRDS_CollectionBag_Scan_NextElements(Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionBag_Scan_GetSizeOfNextElements(Four, Four, Four, Four*);
Four LRDS_CollectionBag_Scan_InsertElements(Four, Four, Four, Four*, void*);
Four LRDS_CollectionBag_Scan_DeleteElements(Four, Four);
Four LRDS_CollectionBag_IsNull(Four, Four, Boolean, TupleID*, Four); 

Four LRDS_CollectionList_Create(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionList_Destroy(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionList_GetN_Elements(Four, Four, Boolean, TupleID*, Four, Four*);
Four LRDS_CollectionList_Assign(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionList_AssignElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionList_InsertElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*, void*);
Four LRDS_CollectionList_DeleteElements(Four, Four, Boolean, TupleID*, Four, Four, Four);
Four LRDS_CollectionList_DeleteAll(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionList_AppendElements(Four, Four, Boolean, TupleID*, Four, Four, Four*, void*);
Four LRDS_CollectionList_GetSizeOfElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*);
Four LRDS_CollectionList_RetrieveElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionList_UpdateElements(Four, Four, Boolean, TupleID*, Four, Four, Four, Four*, void*);
Four LRDS_CollectionList_Concatenate(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionList_Resize(Four, Four, Boolean, TupleID*, Four, Four);
Four LRDS_CollectionList_IsMember(Four, Four, Boolean, TupleID*, Four, Four, void*, Four*);
Four LRDS_CollectionList_IsEqual(Four, Four, Boolean, TupleID*, Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionList_Scan_Open(Four, Four, Boolean, TupleID*, Four);
Four LRDS_CollectionList_Scan_Close(Four, Four);
Four LRDS_CollectionList_Scan_NextElements(Four, Four, Four, Four*, Four, void*);
Four LRDS_CollectionList_Scan_GetSizeOfNextElements(Four, Four, Four, Four*);
Four LRDS_CollectionList_Scan_InsertElements(Four, Four, Four, Four*, void*);
Four LRDS_CollectionList_Scan_DeleteElements(Four, Four);
Four LRDS_CollectionList_IsNull(Four, Four, Boolean, TupleID*, Four); 

Four LRDS_OrderedSet_SpecifyKeyOfElement(Four, Four, char*, Four, KeyDesc*);
Four LRDS_OrderedSet_Create(Four, Four, Boolean, TupleID*, Four, LockParameter*);
Four LRDS_OrderedSet_Destroy(Four, Four, Boolean, TupleID*, Four, LockParameter*);
Four LRDS_OrderedSet_GetTotalLengthOfElements(Four, Four, Boolean, TupleID*, Four, Four*, LockParameter*);
Four LRDS_OrderedSet_GetN_Elements(Four, Four, Boolean, TupleID*, Four, Four*, LockParameter*);
Four LRDS_OrderedSet_HasNestedIndex(Four, Four, Boolean, TupleID*, Four, LockParameter*);
Four LRDS_OrderedSet_IsMember(Four, Four, Boolean, TupleID*, Four, KeyValue*, Four, char*, LockParameter*);
Four LRDS_OrderedSet_InsertElement(Four, Four, Boolean, TupleID*, Four, char*, LockParameter*);
Four LRDS_OrderedSet_AppendSortedElements(Four, Four, Boolean, TupleID*, Four, Four, Four, char*, LockParameter*);
Four LRDS_OrderedSet_DeleteElement(Four, Four, Boolean, TupleID*, Four, KeyValue*, LockParameter*);
Four LRDS_OrderedSet_UpdateElement(Four, Four, Boolean, TupleID*, Four colNo, KeyValue*, Four, Four, Four, void*, LockParameter*);
Four LRDS_OrderedSet_DeleteElements(Four, Four, Boolean, TupleID*, Four, Four, KeyValue*, LockParameter*);
#ifndef ORDEREDSET_BACKWARD_SCAN
Four LRDS_OrderedSet_Scan_Open(Four, Four, Boolean, TupleID*, Four, LockParameter*);
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*);
#else
Four LRDS_OrderedSet_Scan_Open(Four, Four, Boolean, TupleID*, Four, Four, LockParameter*); 
#ifndef COMPRESSION 
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*, Four, char*); 
#else
Four LRDS_OrderedSet_Scan_NextElements(Four, Four, Four, char*, Four, char*, Four *); 
#endif
#endif
Four LRDS_OrderedSet_Scan_Close(Four, Four);
Four LRDS_OrderedSet_CreateNestedIndex(Four, Four, Four);
Four LRDS_OrderedSet_DestroyNestedIndex(Four, Four, Four);
Four LRDS_OrderedSet_IsNull(Four, Four, Boolean, TupleID*, Four);
#ifdef COMPRESSION 
Four LRDS_OrderedSet_SpecifyVolNo(Four, Four, Boolean, TupleID*, Four, VolNo, LockParameter*);
Four LRDS_OrderedSet_GetVolNo(Four, Four, Boolean, TupleID*, Four, VolNo*, LockParameter*);
#endif
Four LRDS_BeginAction(Four); 
Four LRDS_EndAction(Four);

Four LRDS_SetSavepoint(Four, SavepointID *);
Four LRDS_RollbackSavepoint(Four, SavepointID);

Four LRDS_SetCfgParam(Four, char*, char*);
char* LRDS_GetCfgParam(Four, char*);


/*
 * LRDS Reset/Get number of Disk IO 
 */

Four LRDS_ResetNumberOfDiskIO(Four);
Four LRDS_GetNumberOfDiskIO(Four, Four* , Four* );

/*
 * Thread API
 */
Four LRDS_AllocHandle(Four*);
Four LRDS_FreeHandle(Four);
Boolean LRDS_IsFirstThread(void);
Boolean LRDS_IsFinalThread(void);

/*
 * For Compression 
 */
#define LRDS_VARIABLE_BYTE_DECODING(encodedData, i, decodedData)          \
{                                                                         \
    char tmp = 0x1;                                                       \
    decodedData = 0;                                                      \
                                                                          \
    while((tmp & 0x1) == 0x1)                                             \
    {                                                                     \
        tmp = (encodedData)[i++];                                         \
        decodedData = (decodedData << 7) + ((tmp >> 1) & 0x7f);           \
    }                                                                     \
}

/*
    - When Four bytes integer, the number of bytes used 

    1 ~ 127:                1
    128 ~ 16383:            2
    16384 ~ 2097151:        3
    2097152 ~ 268435455:    4
    268435456 ~ 2147483647: 5

    - Whe Two bytes integer, the number of bytes used

    1 ~ 127:                1
    128 ~ 16383:            2
    16384 ~ 32767:          3
*/

/*  
    Note: after termination of the macro, the values of data are to be zero.
*/
#define LRDS_VARIABLE_BYTE_ENCODING(data, tempData, encodedData, encodedDataLength) \
{                                                                                   \
    Two i, j = 0;                                                                   \
                                                                                    \
    if(data > 0)                                                                    \
    {                                                                               \
        encodedDataLength = 0;                                                      \
        for(i = 0; data != 0; i++)                                                  \
        {                                                                           \
            tempData[i] = (data % 128) << 1;                                        \
            data /= 128;                                                            \
            encodedDataLength++;                                                    \
        }                                                                           \
                                                                                    \
        for(i = encodedDataLength -1; i > 0; i--, j++)                              \
        {                                                                           \
            tempData[i] |= 0x1;                                                     \
            encodedData[j] = tempData[i];                                           \
        }                                                                           \
                                                                                    \
        tempData[0] |= 0x0;                                                         \
        encodedData[j++] = tempData[0];                                             \
    }                                                                               \
    else if(data == 0)                                                              \
    {                                                                               \
        encodedDataLength = 1;                                                      \
        encodedData[j++] = 0;                                                       \
    }                                                                               \
}


#endif /* _LRDS_H_ */
