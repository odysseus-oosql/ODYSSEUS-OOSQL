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
#ifndef _KAOSS_LRDS_H_
#define _KAOSS_LRDS_H_

#include "basictypes.h"
#include "primitivetypes.h"                     
#include "LRDS_Err.h"

/*
** Define Constants
*/

/* Return Values */
#define NULL_LENGTH -1

/* Maximum number of Entries in Open Relation Table of LRDS */
/* Note: It is desirable to move this definition into the common.h. */
/*       This is here because the LRDS will not be deliverd to KT. */
#define MAXNUMOFOPENRELS 100

#define MAXRELNAME       250	/* maximum length of relation name */
#define MAXNUMOFCOLS     256	/* maximum number of columns */
#define MAXNUMOFINDEXES  128	/* maximum number of indexes */
#define MAXNUMOFBOOLS    20	/* maximum number of boolean expressions */


/* Return value */
#define EOS    1		/* end of the scan */


/* Scan Direction of the Sequential Scan */
#define FORWARD         0
#define BACKWARD        1
#define BACKWARD_NOORDERING 2
#define BACKWARD_ORDERING 3


/* special parameter values */
#define REMAINDER -1		/* special value of 'length' of ColListStruct */
#define END       -2		/* special value of 'start' of ColListStruct */
#define ALL_VALUE -3		/* special value of 'start' of ColListStruct */


/* Btree Maximum Key Length */
#define MAXKEYLEN  256

/* Data Types Supported */
#define SM_SHORT        0
#define SM_INT          1
#define SM_LONG         2
#define SM_FLOAT        3
#define SM_DOUBLE       4
#define SM_STRING       5	/* fixed-length string */
#define SM_VARSTRING    6	/* variable-length string */
#define SM_LONG_LONG    14
#define SM_SHORT_SIZE   sizeof(Two_Invariable)
#define SM_INT_SIZE     sizeof(Four_Invariable)
#define SM_LONG_SIZE    sizeof(Four_Invariable)
#define SM_FLOAT_SIZE   sizeof(float)
#define SM_DOUBLE_SIZE  sizeof(double)
#define SM_LONG_LONG_SIZE sizeof(Eight_Invariable)

/* Meta Dictionary Entry */
#define METADICTENTRYNAME_MAX 64
#define METADICTENTRYDATA_MAX 16


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

#define LRDS_SYSCOLUMNS_NUM_COLS         3
#define LRDS_SYSCOLUMNS_NUM_FIXEDCOLS    1
#define LRDS_SYSCOLUMNS_NUM_VARCOLS      0
#define LRDS_SYSCOLUMNS_DATAFILEID_COLNO 0
#define LRDS_SYSCOLUMNS_COLUMNNO_COLNO   1
#define LRDS_SYSCOLUMNS_COLINFO_COLNO    2

#define LRDS_SYSINDEXES_NUM_COLS         3
#define LRDS_SYSINDEXES_NUM_FIXEDCOLS    3
#define LRDS_SYSINDEXES_NUM_VARCOLS      0
#define LRDS_SYSINDEXES_DATAFILEID_COLNO 0
#define LRDS_SYSINDEXES_INDEXID_COLNO    1
#define LRDS_SYSINDEXES_KEYINFO_COLNO    2


/*
** Tuple Values of the Catalog Tables for the Tuples Stored in SM_SYSTABLES
*/
#define LRDS_SYSTABLES_MAXTUPLELEN       \
(sizeof(FileID)+sizeof(Four)+sizeof(Four)+sizeof(Two)+sizeof(Two)+MAXRELNAME)
#define LRDS_SYSTABLES_NUM_INDEXES 1
#define LRDS_SYSTABLES_RELNAME     "lrdsSysTables"

#define LRDS_SYSCOLUMNS_MAXTUPLELEN      \
(sizeof(FileID)+sizeof(Two)+sizeof(ColInfo))
#define LRDS_SYSCOLUMNS_NUM_INDEXES 1
#define LRDS_SYSCOLUMNS_RELNAME     "lrdsSysColumns"

#define LRDS_SYSINDEXES_MAXTUPLELEN      \
(sizeof(FileID)+sizeof(IndexID)+sizeof(KeyInfo))
#define LRDS_SYSINDEXES_NUM_INDEXES 1
#define LRDS_SYSINDEXES_RELNAME     "lrdsSysIndexes"


/* Comparison Operator */
typedef enum {SM_EQ=0x1, SM_GT=0x2, SM_GE=0x3, SM_LT=0x4, SM_LE=0x5, SM_EOF=0x10, SM_BOF=0x20} CompOp;

/*
** Typedefs for FileID
*/
typedef struct {
    PageNo pageNo;
    VolNo  volNo;
} FileID;


/*
** Typedefs for IndexID
*/
typedef struct {
    PageNo pageNo;
    VolNo  volNo;
} IndexID;


/*
** Typedefs for TupleID
*/
typedef struct {
    PageNo pageNo;		/* specify the page holding the object */
    VolNo  volNo;		/* specify the volume in which object is in */
    SlotNo slotNo;		/* specify the slot within the page */
    Unique unique;		/* Unique No for checking dangling object */
} TupleID;


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
    Two         type;           /* VARIABLE or FIXED */
    Four        offset;         /* where ? */
    Two         length;         /* how ?   */
} KeyPart;

/*
** BoundCond Type: used in a range scan to give bound condition
*/
typedef struct {
    KeyValue key;		/* Key Value */
    CompOp   op;		/* The key value is included? */
} BoundCond;


/*
** Type Definition for ColListStruct
*/
typedef struct {
    Two         		colNo;		/* IN column number */
    Boolean             	nullFlag;       /* TRUE if it has null value */
    Four        		start;		/* IN starting offset within a column */
				        	/*   ALL_VALUE: read all data of this column */
    Four        		length;		/* IN amount of old data within a column */
    Four        		dataLength;	/* IN amount of new data */
    union {
	Two_Invariable          s;		/* SM_SHORT */
	Four_Invariable         i;		/* SM_INT */
	Four_Invariable         l;		/* SM_LONG */
	Eight_Invariable        ll;     	/* SM_LONG_LONG */
	float                   f;		/* SM_FLOAT */
	double                  d;		/* SM_DOUBLE */
        PageID          	pid;            /* SM_PAGEID */
        FileID          	fid;            /* SM_FILEID */
        IndexID         	iid;            /* SM_INDEXID */
        OID             	oid;            /* SM_OID */
        LRDS_MBR        	mbr;            /* SM_MBR */
	void                    *ptr;		/* pointer to data */
    } data;
    Four retLength;		        	/* OUT return length of Read/Write */
} ColListStruct;


/*
** Type Definition for ColInfo
*/
typedef struct {
    Two  type;			/* data type of column */
    Four length;		/* length(maximum in case SM_STRING) of column */
} ColInfo;


/*
** Type Definition for KeyInfo
*/
typedef struct {
    Two flag;			/* UNIQUE, ... */
    Two nColumns;		/* # of columns on which the index is defined */
    Two colNo[MAXNUMKEYPARTS];	/* column numbers */
} KeyInfo;

#define KEYFLAG_CLUSTERING 0x1


/*
** Type Definition for Boolean Expression
*/
typedef struct {
    Two  op;			                /* operator : EQUAL, LESS, etc */
    Two  colNo;			                /* which column ? */
    Two  length;		                /* length of the value: used for SM_VARSTRING */
    union {
	Two_Invariable          s;	        /* SM_SHORT */
	Four_Invariable         i;	        /* SM_INT */
	Four_Invariable         l;	        /* SM_LONG */
	Eight_Invariable        ll;             /* SM_LONG_LONG */
	float                   f;		/* SM_FLOAT */
	double                  d;		/* SM_DOUBLE */
	char                    str[MAXKEYLEN];	/* SM_STRING or SM_VARSTRING */
    } data;			                /* the value to be compared */
} BoolExp;


/*
** Interface Function Prototypes of LRDS
*/
Four LRDS_AddIndex(Four, Four, char*, LRDS_IndexDesc*, IndexID*);
Four LRDS_CloseScan(Four, Four);
Four LRDS_CreateRelation(Four, Four, char*, LRDS_IndexDesc*, Two, ColInfo*, Boolean);
Four LRDS_CreateTuple(Four, Four, Boolean, Two, ColListStruct*, TupleID*);
Four LRDS_DestroyRelation(Four, Four, char*);
Four LRDS_DestroyTuple(Four, Four, Boolean, TupleID*);
Four LRDS_Dismount(Four, Four);

Four LRDS_DropIndex(Four, Four, char*, IndexID*);
char *LRDS_Err(Four);
Four LRDS_FetchTuple(Four, Four, Boolean, TupleID*, Two, ColListStruct*);
Four LRDS_Final(Four);
Four LRDS_Init(Four);
Four LRDS_Mount(Four, Four, char**, Four*);
Four LRDS_NextTuple(Four, Four, TupleID*, LRDS_Cursor**);
Four LRDS_OpenIndexScan(Four, Four, IndexID*, BoundCond*, BoundCond*, Four, BoolExp*, LockParameter*);
Four LRDS_OpenSeqScan(Four, Four, char*, Four, BoolExp*, LockParameter*);
Four LRDS_UpdateTuple(Four, Four, Boolean, TupleID*, Two, ColListStruct*);


#endif /* _KAOSS_LRDS_H_ */
