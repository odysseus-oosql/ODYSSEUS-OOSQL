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
#ifndef __COSMOS_H__
#define __COSMOS_H__

#ifdef WIN32

#else
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#endif  /* WIN32 */

#include <semaphore.h>
#include "param.h"

#ifdef  __cplusplus
extern "C" {
#endif

/*
 * Constants
 */

/* maximum number of mounted volumes */
#define MAXNUMOFVOLS      20

/* maximum key value length for Btree */
#define MAXKEYLEN 256

/* maximum number of key parts in Btree */
#define MAXNUMKEYPARTS 8

/* maximum number of key parts in MLGF */
#define MLGF_MAXNUM_KEYS 10		

/* NIL value */
#define NIL -1

/* scan direction */
#define FORWARD  0
#define BACKWARD 1

/* end of scan */
#define EOS 1

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

#endif

#endif /* __DBLABLIB_H__ */

/* Boolean Type */
typedef enum { SM_FALSE, SM_TRUE } Boolean;

/* Comparison Operator */
typedef enum {SM_EQ=0x1, SM_LT=0x2, SM_LE=0x3, SM_GT=0x4, SM_GE=0x5, SM_NE=0x6, SM_EOF=0x10, SM_BOF=0x20} CompOp;

/* hash value */
typedef UFour_Invariable MLGF_HashValue;

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
 * Type Definition for FileID and IndexID
 */
typedef PageID FileID;
typedef PageID IndexID;


/*
 * Type Definition for ObjectID
 */
typedef UFour Unique;
typedef Two SlotNo;

typedef struct {
    PageNo pageNo;		/* specify the page holding the object */
    VolID  volNo;		/* specify the volume in which object is in */
    SlotNo slotNo;		/* specify the slot within the page */
    Unique unique;		/* Unique No for checking dangling object */
} ObjectID;

#define SET_NILOBJECTID(oid) (oid).pageNo = NIL
#define IS_NILOBJECTID(oid) (((oid).pageNo == NIL) ? SM_TRUE:SM_FALSE)

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

#define MAKE_NULL_OID(oid)	SET_NILOBJECTID(oid)
#define IS_NULL_OID(oid)	IS_NILOBJECTID(oid)

/*
** Type Definition for KeyDesc
*/
#define KEYFLAG_CLEAR      0x0
#define KEYFLAG_UNIQUE     0x1
#define KEYFLAG_CLUSTERING 0x2

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

/*
 * Type Definition for MLGF_KeyDesc
 */
typedef struct {
    One 		flag;			/* flag */
    One 		nKeys;			/* number of keys */
    Two 		extraDataLen;		/* length of the extra data for an object */
    MLGF_HashValue 	minMaxTypeVector;	/* bit vector of flags indicating MIN/MAX of MBR for each attribute */
} MLGF_KeyDesc;


#define SM_INDEXTYPE_BTREE 1
#define SM_INDEXTYPE_MLGF  2


/*
** Cursor definition
*/
/* AnyCursor:
 *  All cursors should have the following members at the front of them
 *  in the same order.
 */
typedef struct {
    One opaque;                 /* opaque member */
    ObjectID oid;		/* object pointed by the cursor */
} SM_AnyCursor;

/* DataCursor:
 *  sequential scan using the data file
 */
typedef struct {
    One opaque;                 /* opaque member */
    ObjectID oid;		/* object pointed by the cursor */
} SM_DataCursor;

/* BtreeCursor:
 *  scan using a B+ tree
 */
typedef struct {
    One opaque;                 /* opaque member */
    ObjectID oid;		/* object pointed by the cursor */
    KeyValue key;		/* what key value? */
} SM_BtreeCursor;

typedef struct {
    One opaque;                 /* opaque member */
    ObjectID oid;		/* object pointed by the cursor */
    MLGF_HashValue keys[MLGF_MAXNUM_KEYS]; /* what key values? */
} SM_MLGF_Cursor;

/* Universal Cursor */
typedef union {
    SM_AnyCursor any;		/* for access of 'flag' and 'oid' */
    SM_DataCursor seq;        /* sequential scan */
    SM_BtreeCursor btree;     /* scan using a B+ tree */
    SM_MLGF_Cursor mlgf;      /* scan using MLGF index */
} SM_Cursor;


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

typedef struct {
    Two nparts;
    struct {
        Four type;
        Four offset; /* offset from the start of tuple */
        Four length;
    } parts[MAXNUMKEYPARTS];
} AttrInfo;


/*
 * Type definition for object
 */
/*
 * Typedef for generic object header
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

#define MIN_OBJECT_DATA_SIZE sizeof(ObjectID)

typedef struct {
    ObjectHdr   header;         /* the object's header */
    char        data[MIN_OBJECT_DATA_SIZE]; /* data area */
    /*
     * Making the data area is required to enforce
     * the requirement that objects at least be large
     * enough to hold a large object header (LARGEOBJHDR).
     */
} Object;

typedef Four (*GetKeyAttrsFuncPtrForSort_T) (Object*, void*, SortKeyDesc*, AttrInfo*);


#define REMAINDER -1            /* special length: to end of object */
#define END       -2            /* special starting offset: from the end */

/*
 * Data Types Supported by COSMOS
 */
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
#define SM_LONG_LONG	14	

#define SM_SHORT_SIZE   sizeof(Two_Invariable)
#define SM_INT_SIZE     sizeof(Four_Invariable)
#define SM_LONG_SIZE    sizeof(Four_Invariable)
#define SM_FLOAT_SIZE   sizeof(float)
#define SM_DOUBLE_SIZE  sizeof(double)
#define SM_PAGEID_SIZE  sizeof(PageID)
#define SM_INDEXID_SIZE sizeof(IndexID)
#define SM_FILEID_SIZE  sizeof(FileID)
#define SM_OID_SIZE	sizeof(OID)
#define SM_LONG_LONG_SIZE    	sizeof(Eight_Invariable)


/*
** COSMOS External Functions
*/
Four SM_AbortTransaction(Four, XactID*);
Four SM_AddIndex(Four, FileID*, IndexID*, LockParameter*);
Four SM_BeginTransaction(Four, ConcurrencyLevel, XactID*); 
Four SM_BeginAction(Four);
Four SM_CloseScan(Four, Four);
Four SM_CommitTransaction(Four, XactID*);
Four SM_CreateFile(Four, Four, FileID*, Boolean, LockParameter *);
Four SM_CreateObject(Four, Four, ObjectID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*);
Four SM_CreateObjectWithoutScan(Four, FileID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*, LockParameter*);
Four SM_DeleteIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*);
Four SM_DeleteMetaDictEntry(Four, Four, char*);
Four SM_DestroyFile(Four, FileID*, LockParameter*);
Four SM_DestroyObject(Four, Four, ObjectID*, LockParameter*);
Four SM_DestroyObjectWithoutScan(Four, FileID*, ObjectID*, LockParameter*, LockParameter*);
Four SM_Dismount(Four, Four);
Four SM_DropIndex(Four, IndexID*, LockParameter*);
Four SM_EndAction(Four);
char *SM_Err(Four);
Four SM_Final(Four);
Four SM_FetchObject(Four, Four, ObjectID*, Four, Four, char*, LockParameter*);
Four SM_FetchObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, char*, LockParameter*, LockParameter*);
Four SM_FormatDataVolume(Four, Four, One**, One*, Four, Four, Four*, Four);
Four SM_FormatLogVolume(Four, Four, One**, One*, Four, Four, Four*);
Four SM_ExpandDataVolume(Four, Four, Four, One **, Four*);
Four SM_GetMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_GetObjectHdr(Four, Four, ObjectID*, ObjectHdr*, LockParameter*);
Four SM_Init(Four);
Four SM_InsertIndexEntry(Four, IndexID*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*);
Four SM_InsertMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_MLGF_AddIndex(Four, FileID*, IndexID*, MLGF_KeyDesc*, LockParameter*);
Four SM_MLGF_DeleteIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, LockParameter*);
Four SM_MLGF_DropIndex(Four, IndexID*, LockParameter*);
Four SM_MLGF_InsertIndexEntry(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, void*, LockParameter*);
Four SM_MLGF_OpenIndexScan(Four, FileID*, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], MLGF_HashValue[], LockParameter*);
Four SM_MLGF_SearchNearObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, LockParameter*);
Four SM_Mount(Four, Four, char**, Four*);
Four SM_NextObject(Four, Four, ObjectID*, ObjectHdr*, char*, SM_Cursor**, LockParameter*);
Four SM_OpenIndexScan(Four, FileID*, IndexID*, KeyDesc*, BoundCond*, BoundCond*, LockParameter*);
Four SM_OpenSeqScan(Four, FileID*, Four, LockParameter*);
Four SM_SetCfgParam(char*, char*);
Four SM_SetMetaDictEntry(Four, Four, char*, void*, Four);
Four SM_SetObjectHdr(Four, Four, ObjectID*, ObjectHdr*, LockParameter*);
Four SM_SortFile(Four, Four, FileID*, SortKeyDesc*, GetKeyAttrsFuncPtrForSort_T, void*, Boolean, Boolean, FileID*, LockParameter*);
Four SM_UpdateObject(Four, Four, ObjectID*, Four, Four, void*, Four, LockParameter*);
Four SM_UpdateObjectWithoutScan(Four, FileID*, ObjectID*, Four, Four, void*, Four, LockParameter*, LockParameter*);
Four SM_ReleaseAllSharedLocks(Four);


/*
 * error codes
 */
#define eNOERROR			0
#define eBADPARAMETER                  -65536
#define eINTERNAL                      -65537
#define eBADFILEID                     -65538
#define eBADINDEXID                    -65539
#define eBADPAGEID                     -65540
#define eBADOBJECTID                   -65541
#define eBADCATOBJ                     -65542
#define eDEADLOCK                      -65543
#define eINVALIDCURSOR                 -65544
#define eNOTFOUND                      -65545
#define eNONEXTOBJECT                  -65546
#define eNOPREVOBJECT                  -65547
#define eNULLPTR                       -65548
#define eMEMORYALLOCERR                -65549
#define eMEMORYFREEERR                 -65550
#define eTOOMANYVOLS                   -65551
#define eLOCKREQUESTFAIL               -65552
#define eBADFREEDELEMENT               -131072
#define eBADFREEDARRAY                 -131073
#define eVOLNOTMOUNTED                 -196608
#define eVOLIDMISMATCH                 -196609
#define eUSEDDEVICE                    -196610
#define eDEVICEOPENFAIL                -196611
#define eDEVICECLOSEFAIL               -196612
#define eREADFAIL                      -196613
#define eWRITEFAIL                     -196614
#define eLSEEKFAIL                     -196615
#define eINVALIDTRAINSIZE              -196616
#define eINVALIDFIRSTEXT               -196617
#define eINVALIDEFF                    -196618
#define eNOMOREMEMORY                  -196619
#define eNODISKSPACE                   -196620
#define eNOEMPTYMETADICTENTRY          -196621
#define eDUPMETADICTENTRY              -196622
#define eVOLALREADYMOUNTED             -196623
#define eUNFORMATTEDDEVICE             -196624
#define eUNKNOWNVOLUMETYPE             -196625
#define eMETADICTENTRYNOTFOUND         -196626
#define eBADBUFFERTYPE                 -262144
#define eBADLATCHMODE                  -262145
#define eBADBUFFER                     -262146
#define eBADHASHKEYFORBUFFERHASHTABLE  -262147
#define eBADBUFTBLENTRY                -262148
#define eFLUSHFIXEDBUF                 -262149
#define eNOTFOUNDINBUFFERHASHTABLE     -262150
#define eNOUNFIXEDBUF                  -262151
#define eNOTEMPTYPAGE                  -262152
#define eNULLBUFACCESSCB               -262153
#define eLOCKNOTFOUNDINBUFFERLOCKTABLE -262154
#define eBADROOTPID                    -327680
#define eBADOFFSET                     -327681
#define eTOOLARGELENGTH                -327682
#define eEXCEEDMAXDEPTH                -327683
#define eEMPTYPATH                     -327684
#define eBADSLOTNO                     -327685
#define eBADLENGTH                     -393216
#define eBADSTART                      -393217
#define eBADNEAROBJECTID               -393218
#define eBADUSERBUF                    -393219
#define eTOOLARGESORTKEY               -393220
#define eBADBTREEPAGE                  -458752
#define eOUTOFPAGE                     -458753
#define eBADOBJECTBUF                  -458754
#define eBADKEYDESC                    -458755
#define eBADPAGE                       -458756
#define eDUPLICATEDOBJECTID            -458757
#define eBADCOMPOP                     -458758
#define eBADPAGETYPE                   -458759
#define eRANGEERROR                    -458760
#define eDUPLICATEDKEY                 -458761
#define eMODIFIEDONSEARCH              -524288
#define eNOTMOUNTEDVOLUME              -589824
#define eMETADICTENTRYNAME             -589825
#define eMETADICTENTRYDATA             -589826
#define eMETADICTENTRYDATALENGTH       -589827
#define eCATALOGENTRYNOTFOUND          -589828
#define eOPENEDINDEX                   -589829
#define eOPENEDFILE                    -589830
#define eBADDISMOUNT                   -589831
#define eEXCLUSIVELOCKREQUIRED         -589832
#define eINVALIDHIERARCHICALLOCK       -589833
#define eBADLOCKMODE                   -589834
#define eBADDEALLOCTYPE                -589835
#define eCOMMITDURATIONLOCKREQUIRED    -589836
#define eINVALIDMOUNTCOUNTER           -589837
#define eINVALIDCFGPARAM               -589838
#define eNEWXACTNOTALLOWED             -655360
#define eWRONGXACTID                   -655361
#define eBADDEALLOCLISTTYPE            -655362
#define eWRONGTMSTART                  -655363
#define eNOFREEXACTTBLENTRY            -655364
#define eNONESTEDTOPACTION             -655365
#define eTOODEEPNESTEDTOPACTION        -655366
#define eNOELEMINQUEUE                 -720896
#define eNOFREESPACEINGHEAP            -720897
#define eNOSUCHLATCH                   -720898
#define eSHMGETFAILED                  -720899
#define eSHMATFAILED                   -720900
#define eUNIXOPENFAILED                -720901
#define eUNIXFILELOCKFAILED            -720902
#define eSEMGETFAILED                  -720903
#define eSHMCTLFAILED                  -720904
#define eSHMDTFAILED                   -720905
#define eFULLPROCTABLE                 -720906
#define eSEMCTLFAILED                  -720907
#define eSEMOPFAILED                   -720908
#define eSEMCTLSETVALFAILED            -720909
#define eSEMCTLGETVALFAILED            -720910
#define eSEMOPSENDERROR                -720911
#define eSEMOPWAITERROR                -720912
#define eBADLATCHCONVERSION            -720913
#define eBADLATCHCOUNTER               -720914
#define eDEMONFORKFAILED               -720915
#define eBADXACTID                     -786432
#define eBADOLDLOCKREQUEST             -786433
#define eBADOBJECTLOCK                 -786434
#define eBADPAGELOCK                   -786435
#define eBADFILELOCK                   -786436
#define eBADKEYVALUELOCK               -786437
#define eLOCKHIERARCHYVIOLATE          -786438
#define eLOCKREQUESTFAILED             -786439
#define eWRONGLOCKSTATUS               -786440
#define eWRONGLOCKMODE                 -786441
#define eWRONGDURATION                 -786442
#define eDUPXACTID                     -786443
#define eFATALERROR                    -786444
#define eINVALIDLSN                    -851968
#define eINVALIDPAGENO                 -851969
#define eNOEMPTYPAGE                   -851970
#define eENDOFLOG                      -851971
#define eNOOPENEDLOGVOLUME             -851972
#define eLOGVOLUMEALREADYOPENED        -851973
#define eINVALIDTRAINTYPE              -917504
#define eINVALIDOBJECTID               -917505
#define eTOOMANYOPENRELS               -983040
#define eVOLUMENOTMOUNTED              -983041
#define eCATALOGNOTFOUND               -983042
#define eRELATIONNOTFOUND              -983043
#define eINDEXNOTFOUND                 -983044
#define eNUMOFCOLUMNSMISMATCH          -983045
#define eNUMOFINDEXESMISMATCH          -983046
#define eFETCHERROR                    -983047
#define eCOLUMNVALUEEXPECTED           -983048
#define eWRONGCOLUMNVALUE              -983049
#define eTOOLARGECOLUMNLENGTH          -983050
#define eRELATIONDUPLICATED            -983051
#define eINDEXDUPLICATED               -983052
#define eINVALIDCURRENTTUPLE           -983053
#define eOPENEDRELATION                -983054
#define eTOOLONGKEY                    -983055
#define eSET_ALREADYEXIST              -983056
#define eSET_NOTEXIST                  -983057
#define eSET_SCANOPENED                -983058
#define eBADORDEREDSETSCANID           -983059
#define eORDEREDSETSCANOPENED          -983060
#define eORDEREDSET_ALREADYEXIST       -983061
#define eORDEREDSET_NOTEXIST           -983062
#define eORDEREDSET_SCANOPENED         -983063
#define eORDEREDSET_ELEMENTNOTFOUND    -983064
#define eTOOMANYTMPRELS                -983065
#define eCOLLECTIONSET_ELEMENTEXISTS   -983066
#define eCOLLECTIONSET_ELEMENTDOESNOTEXIST -983067
#define eCOLLECTIONBAG_ELEMENTDOESNOTEXIST -983068
#define eCOLLECTIONSET_NOTCOMPATIBLE   -983069
#define eCOLLECTIONBAG_NOTCOMPATIBLE   -983070
#define eCOLLECTION_ALREADYEXIST       -983071
#define eCOLLECTION_NOTEXIST           -983072
#define eCOLLECTION_SCANOPENED         -983073
#define eCOUNTERNOTFOUND               -983074

#ifdef  __cplusplus
}
#endif

#endif /* __COSMOS_H__ */
