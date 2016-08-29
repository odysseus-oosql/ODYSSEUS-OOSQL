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
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
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

#ifndef _OOSQL_STORAGEMANAGER_H_
#define _OOSQL_STORAGEMANAGER_H_

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"

// constant definition
#ifndef CMP_EQ
#define CMP_EQ				0       // equal 
#endif
#ifndef CMP_GT
#define CMP_GT				1       // greater than 
#endif
#ifndef CMP_LT
#define CMP_LT				2       // less than 
#endif
#ifndef CMP_NE
#define CMP_NE				3       // not equal 
#endif
#ifndef CMP_UN
#define CMP_UN              4       // unknown 
#endif
#ifndef KEYINFO_COL_ORDER
#define KEYINFO_COL_ORDER	3		
#endif
#ifndef KEYINFO_COL_ASC
#define KEYINFO_COL_ASC		2		
#endif
#ifndef KEYINFO_COL_DESC
#define KEYINFO_COL_DESC	1		
#endif
#ifndef MBR_NUM_PARTS
#define MBR_NUM_PARTS		4
#endif
#ifndef MAXNUMKEYPARTS
#define MAXNUMKEYPARTS		8	
#endif
#ifndef MAXKEYLEN
#define MAXKEYLEN			256
#endif
#ifndef MLGF_MAXNUM_KEYS
#define MLGF_MAXNUM_KEYS	32
#endif
#ifndef MAXATTRNAME
#define MAXATTRNAME			250
#endif
#ifndef MAXCLASSNAME
#define MAXCLASSNAME		250
#endif
#ifndef MAXDIRPATH
#define MAXDIRPATH			256
#endif
#ifndef MAXMETHODNAME
#define MAXMETHODNAME		246
#endif
#ifndef MAXFUNCTIONNAME
#define MAXFUNCTIONNAME		246
#endif
#ifndef MAXPROCEDURENAME
#define MAXPROCEDURENAME    246
#endif
#ifndef MAXNARGUMENT
#define MAXNARGUMENT		60
#endif
#ifndef MAXINDEXNAME
#define MAXINDEXNAME		250
#endif
#ifndef MAX_NUM_KEYS
#define MAX_NUM_KEYS		32
#endif
#ifndef ALL_VALUE
#define ALL_VALUE			-1
#endif
#ifndef NIL
#define NIL					-1
#endif
#ifndef MAXKEYWORDSIZE
#define MAXKEYWORDSIZE		128
#endif
#ifndef FORWARD
#define FORWARD				0
#endif
#ifndef BACKWARD
#define BACKWARD			1
#endif
#ifndef BACKWARD_NOORDERING
#define BACKWARD_NOORDERING	2
#endif
#ifndef BACKWARD_ORDERING
#define BACKWARD_ORDERING	3
#endif
#ifndef EOS
#define EOS					1
#endif
#ifndef SM_COMPLEXTYPE_BASIC 
#define SM_COMPLEXTYPE_BASIC			0
#endif
#ifndef SM_COMPLEXTYPE_SET 
#define SM_COMPLEXTYPE_SET				1
#endif
#ifndef SM_COMPLEXTYPE_COLLECTIONSET 
#define SM_COMPLEXTYPE_COLLECTIONSET	3
#endif
#ifndef SM_COMPLEXTYPE_COLLECTIONBAG 
#define SM_COMPLEXTYPE_COLLECTIONBAG	4
#endif
#ifndef SM_COMPLEXTYPE_COLLECTIONLIST
#define SM_COMPLEXTYPE_COLLECTIONLIST	5
#endif
#ifndef LOM_COMPLEXTYPE_BASIC 
#define LOM_COMPLEXTYPE_BASIC			0
#endif
#ifndef LOM_COMPLEXTYPE_SET 
#define LOM_COMPLEXTYPE_SET				1
#endif
#ifndef LOM_COMPLEXTYPE_COLLECTIONSET 
#define LOM_COMPLEXTYPE_COLLECTIONSET	3
#endif
#ifndef LOM_COMPLEXTYPE_COLLECTIONBAG 
#define LOM_COMPLEXTYPE_COLLECTIONBAG	4
#endif
#ifndef LOM_COMPLEXTYPE_COLLECTIONLIST
#define LOM_COMPLEXTYPE_COLLECTIONLIST	5
#endif
#ifndef LOM_COMPLEXTYPE_ODMG_COLLECTIONSET
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONSET		6
#endif
#ifndef LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG		7
#endif
#ifndef LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST		8
#endif
#ifndef LOM_COMPLEXTYPE_ODMG_COLLECTIONARRARY
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONARRAY	9
#endif

#ifndef OOSQL_COMPLEXTYPE_SET
#define OOSQL_COMPLEXTYPE_SET			LOM_COMPLEXTYPE_COLLECTIONSET
#endif
#ifndef OOSQL_COMPLEXTYPE_BAG
#define OOSQL_COMPLEXTYPE_BAG			LOM_COMPLEXTYPE_COLLECTIONBAG
#endif
#ifndef OOSQL_COMPLEXTYPE_LIST
#define OOSQL_COMPLEXTYPE_LIST			LOM_COMPLEXTYPE_COLLECTIONLIST
#endif
#ifndef OOSQL_COMPLEXTYPE_ODMG_SET
#define OOSQL_COMPLEXTYPE_ODMG_SET		LOM_COMPLEXTYPE_ODMG_COLLECTIONSET
#endif
#ifndef OOSQL_COMPLEXTYPE_ODMG_BAG
#define OOSQL_COMPLEXTYPE_ODMG_BAG		LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG
#endif
#ifndef OOSQL_COMPLEXTYPE_ODMG_LIST
#define OOSQL_COMPLEXTYPE_ODMG_LIST		LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST
#endif
#ifndef OOSQL_COMPLEXTYPE_ODMG_ARRAY
#define OOSQL_COMPLEXTYPE_ODMG_ARRAY	LOM_COMPLEXTYPE_ODMG_COLLECTIONARRAY
#endif

#ifndef MAXSUPERCLASSID
#define MAXSUPERCLASSID					60
#endif
#ifndef MAXNUMOFATTRIBUTE
#define MAXNUMOFATTRIBUTE				LOM_MAXNUMOFATTRIBUTE
#endif
#ifndef SM_INDEXTYPE_BTREE
#define SM_INDEXTYPE_BTREE				1
#endif
#ifndef SM_INDEXTYPE_MLGF
#define	SM_INDEXTYPE_MLGF				2
#endif
#ifndef KEYFLAG_UNIQUE
#define KEYFLAG_UNIQUE					1
#endif
#ifndef KEYFLAG_CLUSTERING
#define KEYFLAG_CLUSTERING				2
#endif
#ifndef SORTKEYDESC_ATTR_ORDER
#define SORTKEYDESC_ATTR_ORDER			3		// attribute ORDER mask */
#endif
#ifndef SORTKEYDESC_ATTR_ASC
#define SORTKEYDESC_ATTR_ASC			2		// ascending order */
#endif
#ifndef SORTKEYDESC_ATTR_DESC
#define SORTKEYDESC_ATTR_DESC			1		// descending order *
#endif
#ifndef KEYWORD
#define KEYWORD							0
#endif
#ifndef REVERSEKEYWORD
#define REVERSEKEYWORD					1
#endif
/* GEOM region query spatial operators */
#ifndef GEO_SPATIAL_INTERSECT
#define	GEO_SPATIAL_INTERSECT 	1
#endif
#ifndef GEO_SPATIAL_CONTAIN
#define	GEO_SPATIAL_CONTAIN		2
#endif
#ifndef GEO_SPATIAL_CONTAINED
#define	GEO_SPATIAL_CONTAINED	4
#endif
#ifndef GEO_SPATIAL_EQUAL
#define GEO_SPATIAL_EQUAL		8
#endif
#ifndef GEO_SPATIAL_DISJOINT
#define GEO_SPATIAL_DISJOINT	16
#endif
#ifndef GEO_SPATIAL_NORTH
#define GEO_SPATIAL_NORTH		32
#endif
#ifndef GEO_SPATIAL_SOUTH
#define GEO_SPATIAL_SOUTH		64
#endif
#ifndef GEO_SPATIAL_EAST
#define GEO_SPATIAL_EAST		128
#endif
#ifndef GEO_SPATIAL_WEST
#define GEO_SPATIAL_WEST		256
#endif
#ifndef GEO_SPATIAL_KNN
#define GEO_SPATIAL_KNN			512
#endif
#ifndef MAX_NUM_EMBEDDEDATTRIBUTES
#define MAX_NUM_EMBEDDEDATTRIBUTES		24
#endif

#if !defined(SLIMDOWN_OPENGIS) && !defined(_GEO_INTERNAL_H_)
typedef LOM_Handle GEO_Handle;
#endif

class OOSQL_StorageManager : public OOSQL_MemoryManagedObject {
public:
	// Interface Type Definition
	typedef Four		PageNo;
	typedef Two			VolNo;
	typedef VolNo		VolID;
	typedef UFour		Unique;
	typedef Two			SlotNo;
	typedef UFour_Invariable	MLGF_HashValue;
	typedef One			Octet;
	typedef UFour_Invariable	Date;
	typedef double		Interval;
	typedef Four		ClassID;
	typedef enum {SM_EQ=0x1, SM_LT=0x2, SM_LE=0x3, SM_GT=0x4, SM_GE=0x5, SM_NE=0x6, SM_EOF=0x10, SM_BOF=0x20} CompOp;
	
	struct MBR {
		Four_Invariable values[MBR_NUM_PARTS];
	};
	struct Time {
		short _tzHour;
		short _tzMinute;
		short _Hour;
		short _Minute;
		short _Second;
		short _100thSec;
	};
	struct Timestamp {
		Date d;
		Time t;
	};
	enum TimeZone {
		GMT   = 0,  GMT12 = 12,     GMT_12 = -12,
		GMT1  = 1,  GMT_1 = -1,     GMT2   =  2,    GMT_2 = -2,
		GMT3  = 3,  GMT_3 = -3,     GMT4   =  4,    GMT_4 = -4,
		GMT5  = 5,  GMT_5 = -5,     GMT6   =  6,    GMT_6 = -6,
		GMT7  = 7,  GMT_7 = -7,     GMT8   =  8,    GMT_8 = -8,
		GMT9  = 9,  GMT_9 = -9,     GMT10  =  10,   GMT_10= -10,
		GMT11 = 11, GMT_11= -11,    USeastern = -5, UScentral = -6,
		USmoutain = -7, USpacific = -8 
	};

	// ID definition
	struct PageID {
		PageNo			pageNo;					// a PageNo 
		VolNo			volNo;					// a VolNo 
	};
	struct ObjectID {							// ObjectID is used accross the volumes
		PageNo			pageNo;					// specify the page holding the object
		VolID			volNo;					// specify the volume in which object is in
		SlotNo			slotNo;					// specify the slot within the page
		Unique			unique;					// Unique No for checking dangling object
	};
	typedef ObjectID	TupleID;
	struct OID {								// OID is used accross the volumes 
		PageNo			pageNo;					// specify the page holding the object 
		VolID			volNo;					// specify the volume in which object is in 
		SlotNo			slotNo;					// specify the slot within the page 
		Unique			unique;					// Unique No for checking dangling object 
		ClassID			classID;				// specify the class including the object 
	};
	
	typedef Four Serial;
	typedef struct {
		Serial serial;      /* a logical serial number */
		VolNo volNo;        /* a VolNo */
	} LogicalID;

	typedef LogicalID	FileID;
	typedef LogicalID	PsyicalIndexID;
	typedef TupleID		LogicalIndexID;
	struct IndexID {
		Boolean isLogical;
		union {
			PsyicalIndexID	physical_iid;
			LogicalIndexID	logical_iid;
		}index;
	};
	struct XactID {    
		UFour high;
		UFour low;
	};

	// Key
	struct BTreeKeyInfo {
		Two				flag;                   // UNIQUE, ... 
		Two				nColumns;               // # of key parts 
		struct {
			Four		colNo;
			Four		flag;					// ascending/descendig 
		} columns[MAXNUMKEYPARTS];
	};
	struct MLGF_KeyInfo {
		Two				flag;					// CLUSTERING, ... */
		Two				nColumns;				// # of columns on which the index is defined */
		Two				colNo[MLGF_MAXNUM_KEYS];// column numbers */    
		Two				extraDataLen;			// length of the extra data for an object */
	};
	struct KeyValue {
		Two		len;
		char	val[MAXKEYLEN];
	};

	// Cursor Type Defineition
	struct AnyCursor {
	    One				opaque;                 // opaque member 
	    TupleID			tid;					// object pointed by the cursor 
	};
	struct DataCursor {
	    One				opaque;                 // opaque member 
	    TupleID			tid;					// object pointed by the cursor 
	};
	struct BtreeCursor {
	    One				opaque;                 // opaque member 
	    TupleID			tid;					// object pointed by the cursor     
	    KeyValue		key;					// what key value? 
	};
	struct MLGF_Cursor {
	    One				opaque;                 // opaque member 
	    TupleID			tid;					// object pointed by the cursor 
	    MLGF_HashValue	keys[MLGF_MAXNUM_KEYS]; // what key values? 
	};
	union Cursor{
		AnyCursor		any;					// for access of 'flag' and 'oid' 
		DataCursor		seq;					// sequential scan 
		BtreeCursor		btree;					// scan using a B+ tree 
		MLGF_Cursor		mlgf;					// scan using MLGF index 
	};

	// Index Desc	
	struct IndexDesc {
		One indexType;
		union {
			BTreeKeyInfo	btree;				// Key Information for Btree
			MLGF_KeyInfo	mlgf;				// Key Information for MLGF
		} kinfo;    
	};
	
	typedef struct {
		Two type;			/* types supported by COSMOS */
		Two offset;		/* where ? */
		Two length;		/* how ?   */
	} KeyPart;

	typedef struct {
		Two flag;						/* flag for some more informations */
		Two nparts;					/* the number of key parts */
		KeyPart kpart[MAXNUMKEYPARTS];	/* key parts */
	} KeyDesc;

	typedef struct {
		One		flag;				/* flag */
		One		nKeys;				/* number of keys */
		Two		extraDataLen;		/* length of the extra data for an object */
		UFour	minMaxTypeVector;	/* bit vector of flags indicating MIN/MAX of MBR for each attribute */
	} MLGF_KeyDesc;

	typedef struct {
		Boolean isContainingTupleID;
		Boolean isContainingSentenceAndWordNum;
		Boolean isContainingByteOffset;
		Two		nEmbeddedAttributes;
		Two		embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
		Two		embeddedAttrOffset[MAX_NUM_EMBEDDEDATTRIBUTES];
		Two		embeddedAttrVarColNo[MAX_NUM_EMBEDDEDATTRIBUTES];
	} InMemory_PostingStructureInfo;

	typedef struct {
		PsyicalIndexID	keywordIndex;			/* btree index on keyword attribute of inverted index table */
		PsyicalIndexID	reverseKeywordIndex;	/* SM_TEXT index on reverse-keyword attribtue of inverted index table */
		PsyicalIndexID	docIdIndex;				/* SM_TEXT index on document id of document-id index table */
		char			invertedIndexName[MAXINDEXNAME];
		char			docIdIndexTableName[MAXINDEXNAME];
		InMemory_PostingStructureInfo postingInfo;
	} InvertedIndexDesc;
	
	// AttrInfo
	struct AttrInfo {
		Two  	complexType;					// data type of column 
		Two		type;							// Attribute type 
		Four	length;							// Attribute length ,for maximum length of STRING 
		char	name[MAXATTRNAME];				// Attribute name 
		Four	inheritedFrom;					// super class ID 
		Four	domain;							// domain of this attribute 
		Four	offset;							// offset of attribute 
	};

	// MethodInfo
	struct MethodInfo {
		char	dirPath[MAXDIRPATH];			// path where this method is implemented 
		char	name[MAXMETHODNAME];			// Method name 
		char	functionName[MAXFUNCTIONNAME];	// c function name 
		Two	nArguments;						// # of arguments 	
		Four 	ArgumentsList[MAXNARGUMENT];	// list of arguments
		Four	inheritedFrom;					
		Four	returnType;
	};
	
	// ColListStruct
	struct ColListStruct {
		Two			colNo;                  // IN column number 
		Boolean		nullFlag;				// INOUT is it NULL ? 
		Four		start;					// IN starting offset within a column, ALL_VALUE: read all data of this column 
		Four		length;					// IN amount of old data within a column 
		Four		dataLength;				// IN amount of new data 
		union {								
			Boolean		b;	
			Octet		o;	

			Two_Invariable		s;  
			UTwo_Invariable 	us;		
			Four_Invariable		i;      
			Four_Invariable		l;      
			UFour_Invariable	ul;		
			Eight_Invariable	ll;      

			float		f;      
			double		d;      
			PageID		pid;
			FileID		fid;
			PsyicalIndexID iid;
			OID			oid;  
			MBR			mbr;  
			Date		date;	
			Time		time;		
			Timestamp	timestamp;	
			Interval	interval;	
			void*		ptr;        
		} data;
		Four		retLength;				// OUT return length of Read/Write */
	};

	struct ColLengthInfoListStruct {
		Two				colNo;
		Four			length;
	};

	struct BoundCond {
		KeyValue		key;				// Key Value 
		CompOp			op;					// The key value is included? 
	};

	struct BoolExp {
		Two				op;					// operator : EQUAL, LESS, etc 
		Two				colNo;				// which column ? 
		Two				length;				// length of the value: used for SM_VARSTRING 
		union {
			Two			s;					
			int			i;					
			Four		l;					
			float		f;					
			double		d;					
			PageID		pid;
			FileID		fid;
			PsyicalIndexID iid;
			OID			oid;				
			MBR			mbr;				
			/* add date, time, timestamp */
			Date		date;
			Time		time;
			Timestamp	timestamp;
			char		str[MAXKEYLEN];		
		} data;								// the value to be compared
	};

	struct TextDesc {
		One			isIndexed;
		One			hasBeenIndexed;			
		TupleID		contentTid;
		Four		size;
	};
	struct TextColStruct {
		Four		start;					// IN starting offset within a column, ALL_VALUE: read all data of this column 
		Four		length;					// IN amount of old data within a column 
		Four		dataLength;				// IN amount of new data 
		Four		indexMode;
		void*		data;					// pointer to data: SM_STRING, SM_VARSTRING 
		Four		retLength;				// OUT return length of Read/Write 
	};
	
	struct EmbeddedAttrInfo {
		Four type;
		Four start;
		Four length;
	};

	struct EmbeddedAttrTranslationInfo {
		Four							nEmbeddedAttributes;
		EmbeddedAttrInfo				embeddedAttrInfo[MAX_NUM_EMBEDDEDATTRIBUTES];
		Two								realColNoToEmbeddedColNo[LOM_MAXNUMOFATTRIBUTE];
		Two								embeddedAttrVarColNo[MAX_NUM_EMBEDDEDATTRIBUTES];
		Four							embeddedAttributeOffset;
		Four							nVarCols;
	};

	typedef enum { L_NL, L_IS, L_IX, L_S, L_SIX, L_X } LockMode; // lock mode 
	typedef enum { L_INSTANT, L_MANUAL, L_COMMIT } LockDuration; // lock duration 
	struct LockParameter {
		LockMode mode;
		LockDuration duration;
	};

	typedef double PostingWeight;

	struct Point{
		float		x, y;
	};

	struct Region{
		float		x1, y1, x2, y2;
	};
	enum {NONSPATIAL_CLASSTYPE, POINT_CLASSTYPE, LINESEG_CLASSTYPE, POLYGON_CLASSTYPE, POLYLINE_CLASSTYPE};	
	
	enum {DEFERRED_MODE, IMMEDIATE_MODE};

	typedef struct {
		Boolean isContainingTupleID;
		Boolean isContainingSentenceAndWordNum;
		Boolean isContainingByteOffset;
		Two nEmbeddedAttributes;
		Two embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
	} PostingStructureInfo;

	typedef TupleID CounterID;

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

public:
	// Interface
	OOSQL_StorageManager(OOSQL_SystemHandle* oosqlSystemHandle) { m_oosqlSystemHandle = oosqlSystemHandle; }
	virtual ~OOSQL_StorageManager() {}

	// Class Interface
	virtual Four AlterClass(Four volId, char* className, Four nAddCol, AttrInfo* addColInfo, Four nDropCol, AttrInfo* dropColInfo) { return 0; }
	virtual Four CreateSequence(Four volId, char* seqName, Four startWith) { return 0; }
	virtual Four CheckSequence(Four volId, char* seqName) { return 0; }
	virtual Four DropSequence(Four volId, char* seqName) { return 0; }
	virtual Four SetSeqVal(Four volId, char* seqName, Four value) { return 0; }
	virtual Four GetSeqCurrVal(Four volId, char* seqName, Four *currValue) { return 0; }
	virtual Four GetSeqNextVal(Four volId, char* seqName, Four *nextValue) { return 0; }
	virtual Four CreateClass(Four volId, char* className, char *indexName, IndexDesc* indexDesc, Four nAttrs, AttrInfo* attrInfo, Four nSuperClasses, char (*superClasses)[MAXCLASSNAME], Four nMethods, MethodInfo* methodInfo, Boolean isTempClass, Four * classId) { return 0; }
	virtual Four DestroyClass(Four volId,char* className) { return 0; }
	virtual Four OpenClass(Four volId, char* className) { return 0; }
	virtual Four OpenClass(Four volId, Four classId) { return 0; }
	virtual Four CloseClass(Four ocn) { return 0; }
	virtual Four GetOpenClassNum(Four volId, char* className) { return 0; }
	virtual Four GetOpenClassNum(Four volId, Four classId) { return 0; }
	virtual Four GetClassID(Four volId, char* className, Four* classId) { return 0; }

	// Transaction
	virtual Four TransBegin(XactID* xctId, ConcurrencyLevel ccLevel) { return 0; }
	virtual Four TransCommit(XactID* xctId) { return 0; }
	virtual Four TransAbort(XactID* xctId) { return 0; }

	// Mount & Dismount
	virtual Four Mount(Four numDevices, char** deviceNames, Four* volumeId) { return 0; }
	virtual Four Dismount(Four volumeId) { return 0; }

	// Object Manipulation
	virtual Four NextObject(Four scanId, OID* oid, Cursor **cursor) { return 0; }
	virtual Four DestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid) { return 0; }
	virtual Four DeferredDestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid) { return 0; }
	virtual Four CreateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, Four nCols, ColListStruct* clist, OID* oid) { return 0; }
	virtual Four FetchObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist) { return 0; }
	virtual Four UpdateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist) { return 0; }
	virtual Four FetchColLength(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColLengthInfoListStruct* clengthlist) { return 0; }
	
	// Relationship
	virtual Four Relationship_Create(Four volId, char *relationshipName, Four fromClassId, Two fromAttrNum, Four toClassId, Two toAttrNum, One cardinality, One direction, Four* relationshipId) { return 0; }
	virtual Four Relationship_Destroy(Four volId, char *relationshipName) { return 0; }
	virtual Four Relationship_CreateInstance(Four fromOcnOrScanId, Boolean fromUseScanFlag, Four toOcnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOid, OID* toOid) { return 0; }
	virtual Four Relationship_DestroyInstance(Four fromOcnOrScanId, Boolean fromUseScanFlag, Four toOcnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOid, OID* toOid) { return 0; }
	virtual Four Relationship_OpenScan(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four relationshipId) { return 0; }
	virtual Four Relationship_CloseScan(Four relationshipScanId) { return 0; }
	virtual Four Relationship_NextInstance(Four relationshipScanId, Four nOIDs, OID* oids) { return 0; }
	virtual Four Relationship_GetId(Four volId, char* relationshipName, Four* relationshipId) { return 0; }

	// Error Message
	virtual char *Err(Four errorCode) { return 0; }

	// Scan
	virtual Four CloseScan(Four scanId) { return 0; }
	virtual Four OpenIndexScan(Four ocn, IndexID* iid, BoundCond* startBound, BoundCond* stopBound, Four nBools, BoolExp* bools, LockParameter* lockup) { return 0; }
	virtual Four OpenSeqScan(Four ocn, Four scanDirection, Four nBools, BoolExp* bools, LockParameter* lockup) { return 0; }
	virtual Four OpenNearestObjQueryScan(Four ocn, IndexID *iid, Point queryPoint, Four nBools, BoolExp* bools, LockParameter *lockup) { return 0; }
	virtual Four OpenPointQueryScan(Four ocn, IndexID *iid, Point queryPoint, Four nBools, BoolExp* bools, LockParameter *lockup) { return 0; }
	virtual Four OpenRegionQueryScan(Four ocn, IndexID *iid, Region queryRegion, Four spatialOp, Four nBools, BoolExp* bools, LockParameter *lockup) { return 0; }
	virtual Four OpenMBRqueryScan(Four ocn, IndexID *iid, Region queryRegion, Four spatialOp, Four nBools, BoolExp* bools, LockParameter *lockup,
	IndexID* tidJoinIndexID = 0, BoundCond* tidJoinIndexStartBound = 0, BoundCond* tidJoinIndexStopBound = 0) { return 0; }
	virtual Four OpenMlgfIndexScan(Four ocn, IndexID* iid, MLGF_HashValue lowerBounds[], MLGF_HashValue upperBounds[], Four nBools, BoolExp* bools, LockParameter* lockup) { return 0; }

	// Index
	virtual Four AddIndex(Four volId, char *className, char *indexName, IndexDesc *indexDesc, IndexID *indexID) { return 0; }
	virtual Four DropIndex(Four volId, char *indexName) { return 0; }

	// Text Interface
	virtual Four Text_CreateContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNum, TextColStruct *text,	TextDesc *textDesc) { return 0; }
	virtual Four Text_GetDescriptor(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextDesc *textDesc) { return 0; }
	virtual Four Text_DestroyContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextDesc *textDesc) { return 0; }	
	virtual Four Text_FetchContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, TextDesc *textDesc) { return 0; }	
	virtual Four Text_UpdateContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, TextDesc *textDesc) { return 0; }	
	virtual Four Text_GetNPostingsOfCurrentKeyword(Four textScan, Four *nPostings) { return 0; }
	virtual Four Text_OpenIndexScan(Four ocn, IndexID *indexId,	Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup) { return 0; }
	virtual Four Text_OpenIndexScan_GivenInvertedEntryTupleID(Four ocn, Two colNo, TupleID* invertedTableEntryTupleID, LockParameter* lockup) { return 0; }
	virtual Four Text_Scan_Open(Four ocn, OID *oid, Two colNo, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound,	LockParameter *lockup) { return 0; }
	virtual Four Text_Scan_Close(Four osn) { return 0; }
	virtual Four Text_GetNPostings(Four ocn, IndexID *indexId, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup, Four *nPostings) { return 0; }
	virtual Four Text_Scan_NextPosting(Four textScan, Four bufferLength, char *postingBuffer, Four *requiredSize, PostingWeight *weight) { return 0; }
#ifndef COMPRESSION
	virtual Four Text_NextPostings(Four textScan, Four postingLengthBufferSize, char *postingLengthBuffer, Four postingBufferSize, char *postingBuffer, Four scanDirection, Four logicalIdHints, Four *nReturnedPosting, Four *requiredSize) { return 0; }
#else
	virtual Four Text_NextPostings(Four textScan, Four postingLengthBufferSize, char *postingLengthBuffer, Four postingBufferSize, char *postingBuffer, Four scanDirection, Four logicalIdHints, Four *nReturnedPosting, Four *requiredSize, VolNo *volNoOfPostingTupleID, Four *lastDocId) { return 0; }
#endif
	virtual Four Text_GetCursorKeyword(Four textScan, char *keyword) { return 0; }
	virtual Four Text_MakeIndex(Four volId, Four temporaryVolId, char *className) { return 0; }
	virtual Four Text_BatchInvertedIndexBuild(Four volId, Four temporaryVolId, char *className) { return 0; }
	virtual Four Text_DefinePostingStructure(Four volId, char *className, char *attrName, PostingStructureInfo *postingInfo) { return 0; }
	virtual Four Text_GetLogicalId(Four ocnOrScanId, Boolean useScanFlag, OID* oid) { return 0; }
	virtual Four Text_GetStemizerFPtr(Four ocn, Two colNo, void** stemizerFPtr) { return 0; }	
	virtual Four Text_GetOIDFromLogicalDocId(Four ocn, Four logicalId, OID* oid) { return 0; }
	virtual Four Text_GetNumOfTextObjectsInClass(Four ocn, Four* nObjects) { return 0; }

	// Misc Text Interface
	virtual Four GetEmbeddedAttrTranslationInfo(Four textScanId, EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo) { return 0; }
	virtual Four GetEmbeddedAttrsVal(Four textScanId, char *ptrToEmbeddedAttrsBuf, Four embeddedAttrSize, Four nCols, ColListStruct *clist) { return 0; }

	// Time Related
	virtual TimeZone GetLocalTimeZone()	{ return GMT; }
	virtual void SetCurTime(Time *_time, TimeZone tz) {}
	virtual unsigned short GetHour(Time *time) { return 0; }
	virtual unsigned short GetMinute(Time *time) { return 0; }
	virtual unsigned short GetSecond(Time *time) { return 0; }
	virtual long GetJulDay(unsigned short m, unsigned short d,  unsigned short y) { return 0; }
	virtual void GetGregorianDate(Date *date, unsigned short *mm, unsigned short *dd, unsigned short *yy) {}
	virtual void SetCurDate(Date *date) {}
	virtual void SetDate(unsigned short year, unsigned short month, unsigned short day, Date *date) {}
	virtual int CompareDate(Date *date1, Date *date2) { return 0; }
	virtual int CompareTime(Time *time1, Time *time2) { return 0; }
	virtual int CompareTimestamp(Timestamp* timestamp1, Timestamp* timestamp2) { return 0; } 
	virtual unsigned short GetYear(Date *date) { return 0; }
	virtual unsigned short GetMonth(Date *date) { return 0; }
	virtual unsigned short GetDay(Date *date) { return 0; }

	// Sort Interface
	virtual Four SortRelation(Four volId, Four temporaryVolId, char *inRelName, BTreeKeyInfo *kinfo, Boolean newRelFlag, char *outRelName, Boolean tmpRelFlag, LockParameter *lockup) { return 0; }

	// Collection Interface
	virtual Four CollectionSet_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four keySize) { return 0; }
	virtual Four CollectionSet_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionSet_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionSet_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo) { return 0; }
	virtual Four CollectionSet_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements) { return 0; }
	virtual Four CollectionSet_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionSet_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionSet_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionSet_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element) { return 0; }
	virtual Four CollectionSet_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionSet_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo) { return 0; }
	virtual Four CollectionSet_IsSubset(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo) { return 0; }
	virtual Four CollectionSet_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize) { return 0; }
	virtual Four CollectionSet_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements) { return 0; }
	virtual Four CollectionSet_Union(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionSet_Intersect(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionSet_Difference(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionSet_UnionWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB) { return 0; }
	virtual Four CollectionSet_IntersectWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB) { return 0; }
	virtual Four CollectionSet_DifferenceWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB) { return 0; }
	virtual Four CollectionSet_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionSet_Scan_Close(Four CollectionScanId) { return 0; }
	virtual Four CollectionSet_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements) { return 0; }
	virtual Four CollectionSet_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize) { return 0; }
	virtual Four CollectionSet_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionSet_Scan_DeleteElements(Four CollectionScanId) { return 0; }

	virtual Four CollectionBag_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four keySize) { return 0; }
	virtual Four CollectionBag_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionBag_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionBag_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo) { return 0; }
	virtual Four CollectionBag_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements) { return 0; }
	virtual Four CollectionBag_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionBag_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionBag_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionBag_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element) { return 0; }
	virtual Four CollectionBag_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionBag_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo) { return 0; }
	virtual Four CollectionBag_IsSubset(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo) { return 0; }
	virtual Four CollectionBag_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize) { return 0; }
	virtual Four CollectionBag_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements) { return 0; }
	virtual Four CollectionBag_Union(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionBag_Intersect(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionBag_Difference(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionBag_UnionWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB) { return 0; }
	virtual Four CollectionBag_IntersectWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB) { return 0; }
	virtual Four CollectionBag_DifferenceWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB) { return 0; }
	virtual Four CollectionBag_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionBag_Scan_Close(Four CollectionScanId) { return 0; }
	virtual Four CollectionBag_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements) { return 0; }
	virtual Four CollectionBag_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize) { return 0; }
	virtual Four CollectionBag_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionBag_Scan_DeleteElements(Four CollectionScanId) { return 0; }

	virtual Four CollectionList_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionList_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionList_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements) { return 0; }
	virtual Four CollectionList_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionList_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo) { return 0; }
	virtual Four CollectionList_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionList_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements) { return 0; }
	virtual Four CollectionList_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionList_AppendElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionList_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize) { return 0; }
	virtual Four CollectionList_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements) { return 0; }
	virtual Four CollectionList_UpdateElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionList_Concatenate(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four concatnatedOrnOrScanId, Boolean concatnatedUseScanFlag, OID* concatnatedOid, Four concatnatedColNo) { return 0; }
	virtual Four CollectionList_Resize(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four size) { return 0; }
	virtual Four CollectionList_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element, Four* pos) { return 0; }
	virtual Four CollectionList_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionList_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo) { return 0; }
	virtual Four CollectionList_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo) { return 0; }
	virtual Four CollectionList_Scan_Close(Four CollectionScanId) { return 0; }
	virtual Four CollectionList_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements) { return 0; }
	virtual Four CollectionList_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize) { return 0; }
	virtual Four CollectionList_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements) { return 0; }
	virtual Four CollectionList_Scan_DeleteElements(Four CollectionScanId) { return 0; }

    virtual Four Geometry_GetMBR(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four colNo, float* xmin, float* ymin, float* xmax, float* ymax) { return 0; }
    virtual Four Geometry_GetMBR(char* data, Four length, float* xmin, float* ymin, float* xmax, float* ymax) { return 0; }



	// Member Functions of Predefined Class
	virtual Four PDC_Contain(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_Contained(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_Cover(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_Covered(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_Intersect(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_Meet(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_Equal(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_Disjoint(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_East(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_West(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_North(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }
	virtual Four PDC_South(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB) { return 0; }

	virtual Four PDC_Contain(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_Contained(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_Cover(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_Covered(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_Intersect(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_Meet(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_Equal(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_Disjoint(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_East(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_West(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_North(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }
	virtual Four PDC_South(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB) { return 0; }

	virtual Four PDC_ContainRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_ContainedInRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_CoverRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_CoveredInRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_IntersectWithRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_EqualWithRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_MeetWithRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_DisjointWithRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_EastOfRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_WestOfRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_NorthOfRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }
	virtual Four PDC_SouthOfRegion(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Region queryRegion) { return 0; }

	virtual Four PDC_ContainPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_CoverPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_IntersectWithPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_MeetWithPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_DisjointWithPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_EastOfPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_WestOfPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_NorthOfPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }
	virtual Four PDC_SouthOfPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint) { return 0; }

	virtual Four PDC_GetPoints(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four startPoint, Four nPoints, Point* points) { return 0; }
	virtual Four PDC_GetMBR(Four ocnOrScanId, Boolean useScanFlag, OID* oid, MBR* mbr) { return 0; }

	virtual Four PDC_Point_SetPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four x, Four y) { return 0; }
	virtual Four PDC_LineSeg_SetPoints(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four x1, Four y1, Four x2, Four y2) { return 0; }
	virtual Four PDC_LineSeg_AddToPoly(Four lineOcnOrScanId, Boolean lineUseScanFlag, OID *line, Four polyOcnOrScanId, Boolean polyUseScanFlag, OID *poly) { return 0; }
	virtual Four PDC_LineSeg_DeleteFromPoly(Four lineOcnOrScanId, Boolean lineUseScanFlag, OID *line, Four polyOcnOrScanId, Boolean polyUseScanFlag, OID *poly) { return 0; }
	virtual Four PDC_Poly_AddLineSegs(Four polyOcnOrScanId, Boolean polyUseScanFlag, OID *poly, Four lineOcnOrScanId, Boolean lineUseScanFlag, Four nOIDs, OID *lineSeg) { return 0; }
	virtual Four PDC_Poly_DeleteLineSegs(Four polyOcnOrScanId, Boolean polyUseScanFlag, OID *poly, Four lineOcnOrScanId, Boolean lineUseScanFlag, Four nOIDs, OID *lineSeg) { return 0; }

	virtual Four PDC_GetDistance(Four classTypeA, Four nPointsA, Point* pointsA, Four classTypeB, Four nPointsB, Point* pointsB, double* distance) { return 0; }
	virtual Four PDC_GetDistance(Four ocnOrScanIdA, Boolean useScanFlagA, OID* oidA, Four ocnOrScanIdB, Boolean useScanFlagB, OID* oidB, double* distance) { return 0; }
	virtual Four PDC_GetDistanceWithPoint(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Point queryPoint, double* distance) { return 0; }
	virtual Four PDC_GetArea(Four classType, Four nPoints, Point* points, double* area) { return 0; }
	virtual Four PDC_GetArea(Four ocnOrScanId, Boolean useScanFlag, OID* oid, double* area) { return 0; }
	virtual Four PDC_GetLength(Four classType, Four nPoints, Point* points, double* area) { return 0; }
	virtual Four PDC_GetLength(Four ocnOrScanId, Boolean useScanFlag, OID* oid, double* area) { return 0; }

	// Utility Function
	static char* TYPE_TO_DOMAIN_NAME(Four typeId);
	static void  OIDCLEAR(OID& a)        { a.pageNo = 0, a.volNo = 0, a.slotNo = 0, a.classID = 0; }
	static int   TESTOIDCLEARPTR(OID& a) { return a.pageNo == 0 && a.volNo == 0 && a.slotNo == 0 && a.classID == 0; }
	static void  SetNilTupleID(TupleID& tid) { tid.pageNo = NIL; }
	static int   DoesNoContentExistTextDesc(TextDesc &textDesc) { return textDesc.size == NIL; }
	static void  MakeNullTextDesc(TextDesc& textDesc) { SetNilTupleID(textDesc.contentTid), 
					                                    textDesc.size = NIL, 
					                                    textDesc.isIndexed = SM_FALSE,	
														textDesc.hasBeenIndexed = SM_FALSE; }
	static int   OIDGT(OID& a, OID& b)	{ return (a.volNo > b.volNo || (a.volNo == b.volNo && a.pageNo > b.pageNo) || 
		                                         (a.volNo == b.volNo && a.pageNo == b.pageNo && a.slotNo > b.slotNo)); }
	static int   OIDLT(OID& a, OID& b)	{ return (a.volNo < b.volNo || (a.volNo == b.volNo && a.pageNo < b.pageNo) || 
		                                         (a.volNo == b.volNo && a.pageNo == b.pageNo && a.slotNo < b.slotNo)); }
	static int   OIDEQ(OID& a, OID& b)	{ return (a.unique == b.unique && a.classID == b.classID && a.pageNo  == b.pageNo && 
		                                          a.volNo == b.volNo && a.slotNo == b.slotNo); }
#if defined (EXTENDED_BOOLEAN_MODEL)
	static double CalculateWeight(double nPositions, double nPostings) { return nPositions / nPostings; }
#elif defined (HEURISTIC_MODEL)
	static double CalculateWeight(double nPositions, double nPostingsPerObject) { return nPositions / nPostingsPerObject; }
#endif

	OOSQL_SystemHandle* GetOOSQL_SystemHandle() { return m_oosqlSystemHandle; }

	virtual Four CreateCounter(Four volId, char *cntrName, Four initialValue, CounterID *cntrId) { return 0; }
	virtual Four DestroyCounter(Four volId, char *cntrName) { return 0; }
	virtual Four GetCounterId(Four volId, char *cntrName, CounterID *cntrId) { return 0; }
	virtual Four SetCounter(Four volId, CounterID *cntrId, Four value) { return 0; }
	virtual Four ReadCounter(Four volId, CounterID *cntrId, Four *value) { return 0; }
	virtual Four GetCounterValues(Four volId, CounterID *cntrId, Four nValues, Four *startValue) { return 0; }


	/* Sort stream functions */
	virtual Four OpenSortStream(VolID, SortTupleDesc*) { return 0; }
	virtual Four CloseSortStream(Four) { return 0; }
	virtual Four SortingSortStream(Four) { return 0; }
	virtual Four PutTuplesIntoSortStream(Four, Four, SortStreamTuple*) { return 0; }
	virtual Four GetTuplesFromSortStream(Four, Four*, SortStreamTuple*, Boolean*) { return 0; }
	virtual Four OpenStream(VolID) { return 0; }
	virtual Four CloseStream(Four) { return 0; }
	virtual Four ChangePhaseStream(Four) { return 0; }
	virtual Four PutTuplesIntoStream(Four, Four, SortStreamTuple*) { return 0; }
	virtual Four GetTuplesFromStream(Four, Four*, SortStreamTuple*, Boolean*) { return 0; }

protected:
	OOSQL_SystemHandle* m_oosqlSystemHandle;
};

#undef OOSQL_TYPE_SHORT
#undef OOSQL_TYPE_INT
#undef OOSQL_TYPE_LONG
#undef OOSQL_TYPE_LONG_LONG
#undef OOSQL_TYPE_LONG_VAR
#undef OOSQL_TYPE_FLOAT
#undef OOSQL_TYPE_DOUBLE
#undef OOSQL_TYPE_STRING
#undef OOSQL_TYPE_VARSTRING
#undef OOSQL_TYPE_PAGEID
#undef OOSQL_TYPE_FILEID
#undef OOSQL_TYPE_INDEXID
#undef OOSQL_TYPE_OID
#undef OOSQL_TYPE_MBR
#undef OOSQL_TYPE_TEXT
#undef OOSQL_TYPE_DATE
#undef OOSQL_TYPE_TIME
#undef OOSQL_TYPE_TIMESTAMP
#undef OOSQL_TYPE_INTERVAL

#define OOSQL_TYPE_SHORT				0
#define OOSQL_TYPE_INT					1
#define OOSQL_TYPE_LONG					2
#define OOSQL_TYPE_LONG_LONG    14
#define OOSQL_TYPE_FLOAT				3
#define OOSQL_TYPE_DOUBLE				4
#define OOSQL_TYPE_STRING				5    
#define OOSQL_TYPE_VARSTRING			6     
#define OOSQL_TYPE_PAGEID				7     
#define OOSQL_TYPE_FILEID				8     
#define OOSQL_TYPE_INDEXID				9     
#define OOSQL_TYPE_OID					10   
#define OOSQL_TYPE_MBR					12
#define OOSQL_TYPE_TEXT					39   
#define OOSQL_TYPE_DATE					50 
#define OOSQL_TYPE_TIME					51 
#define OOSQL_TYPE_TIMESTAMP			52 
#define OOSQL_TYPE_INTERVAL				53
#define OOSQL_TYPE_BOOL					100

#undef OOSQL_TYPE_SHORT_SIZE
#undef OOSQL_TYPE_INT_SIZE
#undef OOSQL_TYPE_LONG_SIZE
#undef OOSQL_TYPE_LONG_SIZE_VAR
#undef OOSQL_TYPE_LONG_LONG_SIZE
#undef OOSQL_TYPE_FLOAT_SIZE
#undef OOSQL_TYPE_DOUBLE_SIZE
#undef OOSQL_TYPE_STRING_SIZE
#undef OOSQL_TYPE_VARSTRING_SIZE
#undef OOSQL_TYPE_PAGEID_SIZE
#undef OOSQL_TYPE_FILEID_SIZE
#undef OOSQL_TYPE_INDEXID_SIZE
#undef OOSQL_TYPE_OID_SIZE
#undef OOSQL_TYPE_MBR_SIZE
#undef OOSQL_TYPE_TEXT_SIZE
#undef OOSQL_TYPE_DATE_SIZE
#undef OOSQL_TYPE_TIME_SIZE
#undef OOSQL_TYPE_TIMESTAMP_SIZE
#undef OOSQL_TYPE_INTERVAL_SIZE

#define OOSQL_TYPE_SHORT_SIZE			sizeof(Two_Invariable)
#define OOSQL_TYPE_INT_SIZE				sizeof(Four_Invariable)
#define OOSQL_TYPE_LONG_SIZE			sizeof(Four_Invariable)
#define OOSQL_TYPE_LONG_LONG_SIZE		sizeof(Eight_Invariable)

#ifndef SUPPORT_LARGE_DATABASE2
#define OOSQL_TYPE_LONG_VAR         OOSQL_TYPE_LONG
#define OOSQL_TYPE_LONG_SIZE_VAR    OOSQL_TYPE_LONG_SIZE
#else
#define OOSQL_TYPE_LONG_VAR         OOSQL_TYPE_LONG_LONG
#define OOSQL_TYPE_LONG_SIZE_VAR    OOSQL_TYPE_LONG_LONG_SIZE
#endif
#define OOSQL_TYPE_FLOAT_SIZE			sizeof(float)
#define OOSQL_TYPE_DOUBLE_SIZE			sizeof(double)
#define OOSQL_TYPE_STRING_SIZE			0
#define OOSQL_TYPE_VARSTRING_SIZE		0
#define OOSQL_TYPE_PAGEID_SIZE			sizeof(OOSQL_StorageManager::PageID)     
#define OOSQL_TYPE_FILEID_SIZE			sizeof(OOSQL_StorageManager::FileID)
#define OOSQL_TYPE_INDEXID_SIZE			sizeof(OOSQL_StorageManager::FileID)
#define OOSQL_TYPE_OID_SIZE				sizeof(OOSQL_StorageManager::OID)
#define OOSQL_TYPE_MBR_SIZE				sizeof(OOSQL_StorageManager::MBR)
#define OOSQL_TYPE_TEXT_SIZE			0
#define OOSQL_TYPE_DATE_SIZE			sizeof(OOSQL_StorageManager::Date)
#define OOSQL_TYPE_TIME_SIZE			sizeof(OOSQL_StorageManager::Time)
#define OOSQL_TYPE_TIMESTAMP_SIZE		sizeof(OOSQL_StorageManager::Timestamp)
#define OOSQL_TYPE_INTERVAL_SIZE		sizeof(OOSQL_StorageManager::Interval)
#define OOSQL_TYPE_BOOL_SIZE			sizeof(Boolean)

enum ComplexTypeID {
	COMPLEXTYPEID_BASIC      = OOSQL_COMPLEXTYPE_BASIC,
	COMPLEXTYPEID_SET        = OOSQL_COMPLEXTYPE_SET,
	COMPLEXTYPEID_BAG        = OOSQL_COMPLEXTYPE_BAG,
	COMPLEXTYPEID_LIST       = OOSQL_COMPLEXTYPE_LIST,
	COMPLEXTYPEID_ODMG_SET   = OOSQL_COMPLEXTYPE_ODMG_SET,
	COMPLEXTYPEID_ODMG_BAG   = OOSQL_COMPLEXTYPE_ODMG_BAG,
	COMPLEXTYPEID_ODMG_LIST  = OOSQL_COMPLEXTYPE_ODMG_LIST,
	COMPLEXTYPEID_ODMG_ARRAY = OOSQL_COMPLEXTYPE_ODMG_ARRAY
};

enum TypeID {
    TYPEID_SHORT      =  0,
    TYPEID_INT        =  1,
    TYPEID_LONG       =  2,
    TYPEID_LONG_LONG  =  14,
    TYPEID_FLOAT      =  3,
    TYPEID_DOUBLE     =  4,
    TYPEID_STRING     =  5,    
    TYPEID_VARSTRING  =  6,     
    TYPEID_PAGEID     =  7,     
    TYPEID_FILEID     =  8,     
    TYPEID_INDEXID    =  9,     
    TYPEID_OID        =  10,   
    TYPEID_MBR        =  12,
    TYPEID_STRUCTURE  =  15,
    TYPEID_NIL, 
    TYPEID_BOOL, 
    TYPEID_ID, 
    TYPEID_DOMAIN,
    TYPEID_NONE, 
    TYPEID_NULL, 
	TYPEID_COMPLEX,
    TYPEID_SET, 
    TYPEID_BAG, 
    TYPEID_LIST, 
    TYPEID_ARRAY,
	TYPEID_TEXT      =  39,   
    TYPEID_DATE      =  50, 
    TYPEID_TIME		 =  51, 
    TYPEID_TIMESTAMP =  52, 
    TYPEID_INTERVAL  =  53,

    TYPEID_OGIS_GEOMETRY			= 128,
    TYPEID_OGIS_POINT				= 129,
    TYPEID_OGIS_LINESTRING			= 130,
    TYPEID_OGIS_POLYGON				= 131,
    TYPEID_OGIS_GEOMETRYCOLLECTION	= 132,
    TYPEID_OGIS_MULTIPOINT			= 133,
    TYPEID_OGIS_MULTILINESTRING		= 134,
    TYPEID_OGIS_MULTIPOLYGON		= 135,

    TYPEID_SET_SHORT      =  (COMPLEXTYPEID_SET << 16) | TYPEID_SHORT,
    TYPEID_SET_INT        =  (COMPLEXTYPEID_SET << 16) | TYPEID_INT,
    TYPEID_SET_LONG       =  (COMPLEXTYPEID_SET << 16) | TYPEID_LONG,
    TYPEID_SET_LONG_LONG  =  (COMPLEXTYPEID_SET << 16) | TYPEID_LONG_LONG,
    TYPEID_SET_FLOAT      =  (COMPLEXTYPEID_SET << 16) | TYPEID_FLOAT,
    TYPEID_SET_DOUBLE     =  (COMPLEXTYPEID_SET << 16) | TYPEID_DOUBLE,
    TYPEID_SET_STRING     =  (COMPLEXTYPEID_SET << 16) | TYPEID_STRING,    
    TYPEID_SET_VARSTRING  =  (COMPLEXTYPEID_SET << 16) | TYPEID_VARSTRING,     
    TYPEID_SET_PAGEID     =  (COMPLEXTYPEID_SET << 16) | TYPEID_PAGEID,     
    TYPEID_SET_FILEID     =  (COMPLEXTYPEID_SET << 16) | TYPEID_FILEID,     
    TYPEID_SET_INDEXID    =  (COMPLEXTYPEID_SET << 16) | TYPEID_INDEXID,     
    TYPEID_SET_OID        =  (COMPLEXTYPEID_SET << 16) | TYPEID_OID,   
    TYPEID_SET_MBR        =  (COMPLEXTYPEID_SET << 16) | TYPEID_MBR,

    TYPEID_BAG_SHORT      =  (COMPLEXTYPEID_BAG << 16) | TYPEID_SHORT,
    TYPEID_BAG_INT        =  (COMPLEXTYPEID_BAG << 16) | TYPEID_INT,
    TYPEID_BAG_LONG       =  (COMPLEXTYPEID_BAG << 16) | TYPEID_LONG,
    TYPEID_BAG_LONG_LONG  =  (COMPLEXTYPEID_BAG << 16) | TYPEID_LONG_LONG,
    TYPEID_BAG_FLOAT      =  (COMPLEXTYPEID_BAG << 16) | TYPEID_FLOAT,
    TYPEID_BAG_DOUBLE     =  (COMPLEXTYPEID_BAG << 16) | TYPEID_DOUBLE,
    TYPEID_BAG_STRING     =  (COMPLEXTYPEID_BAG << 16) | TYPEID_STRING,    
    TYPEID_BAG_VARSTRING  =  (COMPLEXTYPEID_BAG << 16) | TYPEID_VARSTRING,     
    TYPEID_BAG_PAGEID     =  (COMPLEXTYPEID_BAG << 16) | TYPEID_PAGEID,     
    TYPEID_BAG_FILEID     =  (COMPLEXTYPEID_BAG << 16) | TYPEID_FILEID,     
    TYPEID_BAG_INDEXID    =  (COMPLEXTYPEID_BAG << 16) | TYPEID_INDEXID,     
    TYPEID_BAG_OID        =  (COMPLEXTYPEID_BAG << 16) | TYPEID_OID,   
    TYPEID_BAG_MBR        =  (COMPLEXTYPEID_BAG << 16) | TYPEID_MBR,

    TYPEID_LIST_SHORT      =  (COMPLEXTYPEID_LIST << 16) | TYPEID_SHORT,
    TYPEID_LIST_INT        =  (COMPLEXTYPEID_LIST << 16) | TYPEID_INT,
    TYPEID_LIST_LONG       =  (COMPLEXTYPEID_LIST << 16) | TYPEID_LONG,
    TYPEID_LIST_LONG_LONG  =  (COMPLEXTYPEID_LIST << 16) | TYPEID_LONG_LONG,
    TYPEID_LIST_FLOAT      =  (COMPLEXTYPEID_LIST << 16) | TYPEID_FLOAT,
    TYPEID_LIST_DOUBLE     =  (COMPLEXTYPEID_LIST << 16) | TYPEID_DOUBLE,
    TYPEID_LIST_STRING     =  (COMPLEXTYPEID_LIST << 16) | TYPEID_STRING,    
    TYPEID_LIST_VARSTRING  =  (COMPLEXTYPEID_LIST << 16) | TYPEID_VARSTRING,     
    TYPEID_LIST_PAGEID     =  (COMPLEXTYPEID_LIST << 16) | TYPEID_PAGEID,     
    TYPEID_LIST_FILEID     =  (COMPLEXTYPEID_LIST << 16) | TYPEID_FILEID,     
    TYPEID_LIST_INDEXID    =  (COMPLEXTYPEID_LIST << 16) | TYPEID_INDEXID,     
    TYPEID_LIST_OID        =  (COMPLEXTYPEID_LIST << 16) | TYPEID_OID,   
    TYPEID_LIST_MBR        =  (COMPLEXTYPEID_LIST << 16) | TYPEID_MBR,

    TYPEID_ODMG_SET_SHORT      =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_SHORT,
    TYPEID_ODMG_SET_INT        =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_INT,
    TYPEID_ODMG_SET_LONG       =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_LONG,
    TYPEID_ODMG_SET_LONG_LONG  =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_LONG_LONG,
    TYPEID_ODMG_SET_FLOAT      =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_FLOAT,
    TYPEID_ODMG_SET_DOUBLE     =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_DOUBLE,
    TYPEID_ODMG_SET_STRING     =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_STRING,    
    TYPEID_ODMG_SET_VARSTRING  =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_VARSTRING,     
    TYPEID_ODMG_SET_PAGEID     =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_PAGEID,     
    TYPEID_ODMG_SET_FILEID     =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_FILEID,     
    TYPEID_ODMG_SET_INDEXID    =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_INDEXID,     
    TYPEID_ODMG_SET_OID        =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_OID,   
    TYPEID_ODMG_SET_MBR        =  (COMPLEXTYPEID_ODMG_SET << 16) | TYPEID_MBR,

    TYPEID_ODMG_BAG_SHORT      =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_SHORT,
    TYPEID_ODMG_BAG_INT        =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_INT,
    TYPEID_ODMG_BAG_LONG       =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_LONG,
    TYPEID_ODMG_BAG_LONG_LONG  =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_LONG_LONG,
    TYPEID_ODMG_BAG_FLOAT      =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_FLOAT,
    TYPEID_ODMG_BAG_DOUBLE     =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_DOUBLE,
    TYPEID_ODMG_BAG_STRING     =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_STRING,    
    TYPEID_ODMG_BAG_VARSTRING  =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_VARSTRING,     
    TYPEID_ODMG_BAG_PAGEID     =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_PAGEID,     
    TYPEID_ODMG_BAG_FILEID     =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_FILEID,     
    TYPEID_ODMG_BAG_INDEXID    =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_INDEXID,     
    TYPEID_ODMG_BAG_OID        =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_OID,   
    TYPEID_ODMG_BAG_MBR        =  (COMPLEXTYPEID_ODMG_BAG << 16) | TYPEID_MBR,

    TYPEID_ODMG_LIST_SHORT      =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_SHORT,
    TYPEID_ODMG_LIST_INT        =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_INT,
    TYPEID_ODMG_LIST_LONG       =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_LONG,
    TYPEID_ODMG_LIST_LONG_LONG  =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_LONG_LONG,
    TYPEID_ODMG_LIST_FLOAT      =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_FLOAT,
    TYPEID_ODMG_LIST_DOUBLE     =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_DOUBLE,
    TYPEID_ODMG_LIST_STRING     =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_STRING,    
    TYPEID_ODMG_LIST_VARSTRING  =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_VARSTRING,     
    TYPEID_ODMG_LIST_PAGEID     =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_PAGEID,     
    TYPEID_ODMG_LIST_FILEID     =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_FILEID,     
    TYPEID_ODMG_LIST_INDEXID    =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_INDEXID,     
    TYPEID_ODMG_LIST_OID        =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_OID,   
    TYPEID_ODMG_LIST_MBR        =  (COMPLEXTYPEID_ODMG_LIST << 16) | TYPEID_MBR,

    TYPEID_ODMG_ARRAY_SHORT      =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_SHORT,
    TYPEID_ODMG_ARRAY_INT        =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_INT,
    TYPEID_ODMG_ARRAY_LONG       =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_LONG,
    TYPEID_ODMG_ARRAY_LONG_LONG  =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_LONG_LONG,
    TYPEID_ODMG_ARRAY_FLOAT      =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_FLOAT,
    TYPEID_ODMG_ARRAY_DOUBLE     =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_DOUBLE,
    TYPEID_ODMG_ARRAY_STRING     =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_STRING,    
    TYPEID_ODMG_ARRAY_VARSTRING  =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_VARSTRING,     
    TYPEID_ODMG_ARRAY_PAGEID     =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_PAGEID,     
    TYPEID_ODMG_ARRAY_FILEID     =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_FILEID,     
    TYPEID_ODMG_ARRAY_INDEXID    =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_INDEXID,     
    TYPEID_ODMG_ARRAY_OID        =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_OID,   
    TYPEID_ODMG_ARRAY_MBR        =  (COMPLEXTYPEID_ODMG_ARRAY << 16) | TYPEID_MBR
};

#define TYPEID_NONE_SIZE        0
#define TYPEID_NULL_SIZE        0
#define TYPEID_SHORT_SIZE       sizeof(Two_Invariable)
#define TYPEID_INT_SIZE         sizeof(Four_Invariable)
#define TYPEID_LONG_SIZE        sizeof(Four_Invariable)
#define TYPEID_LONG_LONG_SIZE   sizeof(Eight_Invariable)
#define TYPEID_FLOAT_SIZE       sizeof(float)
#define TYPEID_DOUBLE_SIZE      sizeof(double)
#define TYPEID_STRING_SIZE      0               // variable
#define TYPEID_VARSTRING_SIZE   0               // variable
#define TYPEID_SET_SIZE         0				// variable
#define TYPEID_BAG_SIZE         0				// variable
#define TYPEID_LIST_SIZE        0				// variable
#define TYPEID_ARRAY_SIZE       0				// variable
#define TYPEID_DATE_SIZE        sizeof(OOSQL_StorageManager::Date)
#define TYPEID_TIME_SIZE        sizeof(OOSQL_StorageManager::Time)
#define TYPEID_TIMESTAMP_SIZE   sizeof(OOSQL_StorageManager::Timestamp)
#define TYPEID_INTERVAL_SIZE    sizeof(OOSQL_StorageManager::Interval)
#define TYPEID_STRUCTURE_SIZE   0               // variable
#define TYPEID_OID_SIZE         sizeof(OOSQL_StorageManager::OID)
#define TYPEID_NIL_SIZE         0
#define TYPEID_BOOL_SIZE        4
#define TYPEID_MBR_SIZE         sizeof(OOSQL_StorageManager::MBR);
#define TYPEID_ID_SIZE          0
#define TYPEID_DOMAIN_SIZE      0
#define TYPEID_TEXT_SIZE        0				// variable
#define TYPEID_PAGEID_SIZE		sizeof(OOSQL_StorageManager::PageID)
#define TYPEID_FILEID_SIZE		sizeof(OOSQL_StorageManager::FileID)
#define TYPEID_INDEXID_SIZE		sizeof(OOSQL_StorageManager::FileID)

#define TYPEID_OGIS_GEOMETRY_SIZE			0
#define TYPEID_OGIS_POINT_SIZE				0
#define TYPEID_OGIS_LINESTRING_SIZE			0
#define TYPEID_OGIS_POLYGON_SIZE			0
#define TYPEID_OGIS_GEOMETRYCOLLECTION_SIZE	0
#define TYPEID_OGIS_MULTIPOINT_SIZE			0
#define TYPEID_OGIS_MULTILINESTRING_SIZE	0
#define TYPEID_OGIS_MULTIPOLYGON_SIZE		0

#define OOSQL_COMPOSE_TYPE(x, y)	(TypeID)(((UFour_Invariable)(x) << 16) | (y))
#define OOSQL_MASK_COMPLEXTYPE(x)	(ComplexTypeID)(0xffff & ((UFour_Invariable)(x) >> 16))
#define OOSQL_MASK_TYPE(x)			(TypeID)(0xffff & (UFour_Invariable)(x))

#ifndef SUPPORT_LARGE_DATABASE2
#define TYPEID_SHORT_VAR		TYPEID_SHORT
#define TYPEID_SHORT_SIZE_VAR	TYPEID_SHORT_SIZE
#define TYPEID_LONG_VAR			TYPEID_LONG
#define TYPEID_LONG_SIZE_VAR	TYPEID_LONG_SIZE
#else
#define TYPEID_SHORT_VAR		TYPEID_LONG
#define TYPEID_SHORT_SIZE_VAR	TYPEID_LONG_SIZE
#define TYPEID_LONG_VAR			TYPEID_LONG_LONG
#define TYPEID_LONG_SIZE_VAR	TYPEID_LONG_LONG_SIZE
#endif

class OOSQL_StorageManagerLOM : public OOSQL_StorageManager {
public:
	OOSQL_StorageManagerLOM(OOSQL_SystemHandle* oosqlSystemHandle, LOM_Handle* systemHandle);
	~OOSQL_StorageManagerLOM();

	// Class Interface
	Four AlterClass(Four volId, char* className, Four nAddCol, AttrInfo* addColInfo, Four nDropCol, AttrInfo* dropColInfo);
	Four CreateSequence(Four volId, char* seqName, Four startWith);
	Four CheckSequence(Four volId, char* seqName);
	Four DropSequence(Four volId, char* seqName);
	Four SetSeqVal(Four volId, char* seqName, Four value);
	Four GetSeqCurrVal(Four volId, char* seqName, Four *currValue);
	Four GetSeqNextVal(Four volId, char* seqName, Four *nextValue);
	Four CreateClass(Four volId, char* className, char *indexName, IndexDesc* indexDesc, Four nAttrs, AttrInfo* attrInfo, Four nSuperClasses, char (*superClasses)[MAXCLASSNAME], Four nMethods, MethodInfo* methodInfo, Boolean isTempClass, Four * classId);
	Four DestroyClass(Four volId,char* className);
	Four OpenClass(Four volId, char* className);
	Four OpenClass(Four volId, Four classId);
	Four GetOpenClassNum(Four volId, char* className);
	Four GetOpenClassNum(Four volId, Four classId);
	Four CloseClass(Four ocn);
	Four GetClassID(Four volId, char* className, Four* classId);

	// Transaction
	Four TransBegin(XactID* xctId, ConcurrencyLevel ccLevel);
	Four TransCommit(XactID* xctId);
	Four TransAbort(XactID* xctId);

	// Mount & Dismount
	Four Mount(Four numDevices, char** deviceNames, Four* volumeId);
	Four Dismount(Four volumeId);

	// Object Manipulation
	Four NextObject(Four scanId, OID* oid, Cursor **cursor);
	Four DestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid);
	Four DeferredDestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid);
	Four CreateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, Four nCols, ColListStruct* clist, OID* oid);
	Four FetchObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist);
	Four UpdateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist);
	Four FetchColLength(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColLengthInfoListStruct* clengthlist);

	// Relationship
	Four Relationship_Create(Four volId, char *relationshipName, Four fromClassId, Two fromAttrNum, Four toClassId, Two toAttrNum, One cardinality, One direction, Four* relationshipId);
	Four Relationship_Destroy(Four volId, char *relationshipName);
	Four Relationship_CreateInstance(Four fromOcnOrScanId, Boolean fromUseScanFlag, Four toOcnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOid, OID* toOid);
	Four Relationship_DestroyInstance(Four fromOcnOrScanId, Boolean fromUseScanFlag, Four toOcnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOid, OID* toOid);
	Four Relationship_OpenScan(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four relationshipId);
	Four Relationship_CloseScan(Four relationshipScanId);
	Four Relationship_NextInstance(Four relationshipScanId, Four nOIDs, OID* oids);
	Four Relationship_GetId(Four volId, char* relationshipName, Four* relationshipId);

	// Error Message
	char *Err(Four errorCode);

	// Scan
	Four CloseScan(Four scanId);
	Four OpenIndexScan(Four ocn, IndexID* iid, BoundCond* startBound, BoundCond* stopBound, Four nBools, BoolExp* bools, LockParameter* lockup);
	Four OpenSeqScan(Four ocn, Four scanDirection, Four nBools, BoolExp* bools, LockParameter* lockup);
	Four OpenMlgfIndexScan(Four ocn, IndexID* iid, MLGF_HashValue lowerBounds[], MLGF_HashValue upperBounds[], Four nBools, BoolExp* bools, LockParameter* lockup);

	// Index
	Four AddIndex(Four volId, char *className, char *indexName, IndexDesc *indexDesc, IndexID *indexID);
	Four DropIndex(Four volId, char *indexName);

	// Text Interface
	Four Text_CreateContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNum, TextColStruct *text, TextDesc *textDesc);
	Four Text_GetDescriptor(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextDesc *textDesc);
	Four Text_DestroyContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextDesc *textDesc);
	Four Text_FetchContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, TextDesc *textDesc);
	Four Text_UpdateContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, TextDesc *textDesc);
	Four Text_GetNPostingsOfCurrentKeyword(Four textScan, Four *nPostings);
	Four Text_OpenIndexScan(Four ocn, IndexID *indexId,	Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup);
	Four Text_OpenIndexScan_GivenInvertedEntryTupleID(Four ocn, Two colNo, TupleID* invertedTableEntryTupleID, LockParameter* lockup);
	Four Text_Scan_Open(Four ocn, OID *oid, Two colNo, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound,	LockParameter *lockup);
	Four Text_Scan_Close(Four osn);
	Four Text_GetNPostings(Four ocn, IndexID *indexId, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup, Four *nPostings);
	Four Text_Scan_NextPosting(Four textScan, Four bufferLength, char *postingBuffer, Four *requiredSize, PostingWeight *weight);
#ifndef COMPRESSION
	Four Text_NextPostings(Four textScan, Four postingLengthBufferSize, char *postingLengthBuffer, Four postingBufferSize, char *postingBuffer, Four scanDirection, Four logicalIdHints, Four *nReturnedPosting, Four *requiredSize);
#else
	Four Text_NextPostings(Four textScan, Four postingLengthBufferSize, char *postingLengthBuffer, Four postingBufferSize, char *postingBuffer, Four scanDirection, Four logicalIdHints, Four *nReturnedPosting, Four *requiredSize, VolNo *volNoOfPostingTupleID, Four* lastDocId);
#endif
	Four Text_GetCursorKeyword(Four textScan, char *keyword);
	Four Text_MakeIndex(Four volId, Four temporaryVolId, char *className);
	Four Text_BatchInvertedIndexBuild(Four volId, Four temporaryVolId, char *className);
	Four Text_DefinePostingStructure(Four volId, char *className, char *attrName, PostingStructureInfo *postingInfo);
	Four Text_GetLogicalId(Four ocnOrScanId, Boolean useScanFlag, OID* oid);
	Four Text_GetStemizerFPtr(Four ocn, Two colNo, void** stemizerFPtr);			
	Four Text_GetOIDFromLogicalDocId(Four ocn, Four logicalId, OID* oid);
	Four Text_GetNumOfTextObjectsInClass(Four ocn, Four* nObjects);

	// Misc Text Interface
	Four GetEmbeddedAttrTranslationInfo(Four textScanId, EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo);
	Four GetEmbeddedAttrsVal(Four textScanId, char *ptrToEmbeddedAttrsBuf, Four embeddedAttrSize, Four nCols, ColListStruct *clist);

	// Sort Interface
	Four SortRelation(Four volId, Four temporaryVolId, char *inRelName, BTreeKeyInfo *kinfo, Boolean newRelFlag, char *outRelName, Boolean tmpRelFlag, LockParameter *lockup);

	// Collection Interface
	Four CollectionSet_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four keySize);
	Four CollectionSet_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionSet_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionSet_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo);
	Four CollectionSet_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements);
	Four CollectionSet_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionSet_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionSet_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionSet_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element);
	Four CollectionSet_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionSet_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo);
	Four CollectionSet_IsSubset(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo);
	Four CollectionSet_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize);
	Four CollectionSet_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
	Four CollectionSet_Union(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionSet_Intersect(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionSet_Difference(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionSet_UnionWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB);
	Four CollectionSet_IntersectWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB);
	Four CollectionSet_DifferenceWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB);
	Four CollectionSet_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionSet_Scan_Close(Four CollectionScanId);
	Four CollectionSet_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
	Four CollectionSet_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize);
	Four CollectionSet_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements);
	Four CollectionSet_Scan_DeleteElements(Four CollectionScanId);

	Four CollectionBag_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four keySize);
	Four CollectionBag_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionBag_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionBag_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo);
	Four CollectionBag_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements);
	Four CollectionBag_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionBag_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionBag_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionBag_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element);
	Four CollectionBag_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionBag_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo);
	Four CollectionBag_IsSubset(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo);
	Four CollectionBag_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize);
	Four CollectionBag_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
	Four CollectionBag_Union(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionBag_Intersect(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionBag_Difference(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionBag_UnionWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB);
	Four CollectionBag_IntersectWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB);
	Four CollectionBag_DifferenceWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB);
	Four CollectionBag_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionBag_Scan_Close(Four CollectionScanId);
	Four CollectionBag_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
	Four CollectionBag_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize);
	Four CollectionBag_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements);
	Four CollectionBag_Scan_DeleteElements(Four CollectionScanId);

	Four CollectionList_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionList_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionList_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements);
	Four CollectionList_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionList_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo);
	Four CollectionList_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, void* elements);
	Four CollectionList_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements);
	Four CollectionList_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionList_AppendElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements);
	Four CollectionList_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize);
	Four CollectionList_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
	Four CollectionList_UpdateElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, void* elements);
	Four CollectionList_Concatenate(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four concatnatedOrnOrScanId, Boolean concatnatedUseScanFlag, OID* concatnatedOid, Four concatnatedColNo);
	Four CollectionList_Resize(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four size);
	Four CollectionList_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element, Four* pos);
	Four CollectionList_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionList_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo);
	Four CollectionList_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo);
	Four CollectionList_Scan_Close(Four CollectionScanId);
	Four CollectionList_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
	Four CollectionList_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize);
	Four CollectionList_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements);
	Four CollectionList_Scan_DeleteElements(Four CollectionScanId);

	// Time Related
	TimeZone GetLocalTimeZone();
	void SetCurTime(Time *_time, TimeZone tz);
	unsigned short GetHour(Time *time);
	unsigned short GetMinute(Time *time);
	unsigned short GetSecond(Time *time);
	long GetJulDay(unsigned short m, unsigned short d,  unsigned short y);
	void GetGregorianDate(Date *date, unsigned short *mm, unsigned short *dd, unsigned short *yy);
	void SetCurDate(Date *date);
	void SetDate(unsigned short year, unsigned short month, unsigned short day, Date *date);
	int CompareDate(Date *date1, Date *date2);
	int CompareTime(Time *time1, Time *time2);
	int CompareTimestamp(Timestamp* timestamp1, Timestamp* timestamp2);
	unsigned short GetYear(Date *date);
	unsigned short GetMonth(Date *date);
	unsigned short GetDay(Date *date);

	Four CreateCounter(Four volId, char *cntrName, Four initialValue, CounterID *cntrId);
	Four DestroyCounter(Four volId, char *cntrName);
	Four GetCounterId(Four volId, char *cntrName, CounterID *cntrId);
	Four SetCounter(Four volId, CounterID *cntrId, Four value);
	Four ReadCounter(Four volId, CounterID *cntrId, Four *value);
	Four GetCounterValues(Four volId, CounterID *cntrId, Four nValues, Four *startValue);

	/* Sort stream functions */
	Four OpenSortStream(VolID, SortTupleDesc*);
	Four CloseSortStream(Four);
	Four SortingSortStream(Four);
	Four PutTuplesIntoSortStream(Four, Four, SortStreamTuple*);
	Four GetTuplesFromSortStream(Four, Four*, SortStreamTuple*, Boolean*);
	Four OpenStream(VolID);
	Four CloseStream(Four);
	Four ChangePhaseStream(Four);
	Four PutTuplesIntoStream(Four, Four, SortStreamTuple*);
	Four GetTuplesFromStream(Four, Four*, SortStreamTuple*, Boolean*);

private:
	LOM_Handle*	m_systemHandle;
};

inline void ConvertToUserLevelColNoInColListStruct(Four nCols, OOSQL_StorageManager::ColListStruct* clist)
{
    int i;

    for(i = 0; i < nCols; i++)
        clist[i].colNo --;
}

inline void ConvertToSystemLevelColNoInColListStruct(Four nCols, OOSQL_StorageManager::ColListStruct* clist)
{
    int i;

    for(i = 0; i < nCols; i++)
        clist[i].colNo ++;
}

inline void ConvertToUserLevelColNoInColLengthInfoListStruct(Four nCols, OOSQL_StorageManager::ColLengthInfoListStruct* clengthlist)
{
    int i;

    for(i = 0; i < nCols; i++)
        clengthlist[i].colNo --;
}

inline void ConvertToSystemLevelColNoInColLengthInfoListStruct(Four nCols, OOSQL_StorageManager::ColLengthInfoListStruct* clengthlist)
{
    int i;

    for(i = 0; i < nCols; i++)
        clengthlist[i].colNo ++;
}

inline void ConvertToUserLevelColNoInBoolExp(Four nBools, OOSQL_StorageManager::BoolExp* bools)
{
    for(int i = 0; i < nBools; i++)
        bools[i].colNo --;
}

inline void ConvertToSystemLevelColNoInBoolExp(Four nBools, OOSQL_StorageManager::BoolExp* bools)
{
    for(int i = 0; i < nBools; i++)
        bools[i].colNo ++;
}

inline void ConvertToUserLevelColNoInIndexDesc(OOSQL_StorageManager::IndexDesc* indexDesc)
{
    int i;

    if(indexDesc)
    {
        if(indexDesc->indexType == SM_INDEXTYPE_BTREE)
            for(i = 0; i < indexDesc->kinfo.btree.nColumns; i++)
                indexDesc->kinfo.btree.columns[i].colNo --;
        else 
            // SM_INDEXTYPE_MLGF
            for(i = 0; i < indexDesc->kinfo.mlgf.nColumns; i++)
                indexDesc->kinfo.mlgf.colNo[i] --;
    }
}

inline void ConvertToSystemLevelColNoInIndexDesc(OOSQL_StorageManager::IndexDesc* indexDesc)
{
    int i;

    if(indexDesc)
    {
        if(indexDesc->indexType == SM_INDEXTYPE_BTREE)
            for(i = 0; i < indexDesc->kinfo.btree.nColumns; i++)
                indexDesc->kinfo.btree.columns[i].colNo ++;
        else 
            // SM_INDEXTYPE_MLGF
            for(i = 0; i < indexDesc->kinfo.mlgf.nColumns; i++)
                indexDesc->kinfo.mlgf.colNo[i] ++;
    }
}

inline void ConvertToUserLevelColNoInEmbeddedAttrTranslationInfo(OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
    Two newRealColNoToEmbeddedColNo[LOM_MAXNUMOFATTRIBUTE];
    
    memcpy(&newRealColNoToEmbeddedColNo[0], &embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[1], (LOM_MAXNUMOFATTRIBUTE - 1) * sizeof(Two));
    memcpy(&newRealColNoToEmbeddedColNo[LOM_MAXNUMOFATTRIBUTE - 1], &embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[0], sizeof(Two));

    memcpy(embeddedAttrTranslationInfo->realColNoToEmbeddedColNo, newRealColNoToEmbeddedColNo, sizeof(newRealColNoToEmbeddedColNo));
}

inline void ConvertToSystemLevelColNoInEmbeddedAttrTranslationInfo(OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
    Two newRealColNoToEmbeddedColNo[LOM_MAXNUMOFATTRIBUTE];
    
    memcpy(&newRealColNoToEmbeddedColNo[1], &embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[0], (LOM_MAXNUMOFATTRIBUTE - 1) * sizeof(Two));
    memcpy(&newRealColNoToEmbeddedColNo[0], &embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[LOM_MAXNUMOFATTRIBUTE - 1], sizeof(Two));

    memcpy(embeddedAttrTranslationInfo->realColNoToEmbeddedColNo, newRealColNoToEmbeddedColNo, sizeof(newRealColNoToEmbeddedColNo));
}


inline Four OOSQL_StorageManagerLOM::FetchObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist)
{
	Four			i, j;
	Four			e;
	ColListStruct	clistToUse[MAXNUMOFATTRIBUTE];
    Four            nColsToUse;

	for(i = 0, j = 0; i < nCols; i++)
	{
		if(clist[i].dataLength > 0)
		{
			clistToUse[j] = clist[i];
			j++;
		}
	}
    nColsToUse = j;

	if(nColsToUse > 0)
	{
        ConvertToUserLevelColNoInColListStruct(nColsToUse, clistToUse);
		e = LOM_FetchObjectByColList(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, nColsToUse, (::LOM_ColListStruct*)clistToUse);
        ConvertToSystemLevelColNoInColListStruct(nColsToUse, clistToUse);
		OOSQL_CHECK_ERR(e);
	}

	for(i = 0, j = 0; i < nCols; i++)
	{
		if(clist[i].dataLength > 0)
		{
			clist[i] = clistToUse[j];
			j++;
		}
		else
			clist[i].retLength = 0;
	}

	return eNOERROR;
}

inline Four OOSQL_StorageManagerLOM::GetEmbeddedAttrTranslationInfo(Four textScanId, EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
	Four e = LOM_GetEmbeddedAttrTranslationInfo(m_systemHandle, textScanId, (::LOM_EmbeddedAttrTranslationInfo*)embeddedAttrTranslationInfo);
    ConvertToSystemLevelColNoInEmbeddedAttrTranslationInfo(embeddedAttrTranslationInfo);
    return e;
}

inline Four OOSQL_StorageManagerLOM::GetEmbeddedAttrsVal(Four textScanId, char *ptrToEmbeddedAttrsBuf, Four embeddedAttrSize, Four nCols, ColListStruct *clist)
{
    ConvertToUserLevelColNoInColListStruct(nCols, clist);
	Four e = LOM_GetEmbeddedAttrsVal(m_systemHandle, textScanId, ptrToEmbeddedAttrsBuf, embeddedAttrSize, nCols, (::LOM_ColListStruct*)clist);
    ConvertToSystemLevelColNoInColListStruct(nCols, clist);
    return e;
}

inline Four OOSQL_StorageManagerLOM::Text_GetNPostings(Four ocn, IndexID *indexId, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup, Four *nPostings)
{
	return LOM_Text_GetNPostings(m_systemHandle, ocn, (::LOM_IndexID*)indexId, keywordKind, (::BoundCond*)keywordStartBound, (::BoundCond*)keywordStopBound, (::LockParameter*)lockup, nPostings);
}

inline Four OOSQL_StorageManagerLOM::Text_Scan_NextPosting(Four textScan, Four bufferLength, char *postingBuffer, Four *requiredSize, PostingWeight *weight)
{
	Four e;

	e = LOM_Text_Scan_NextPosting(m_systemHandle, textScan, bufferLength, postingBuffer, requiredSize, (::PostingWeight*)weight);
	if(e == eBIGGERPOSTINGBUFFERNEEDED_LOM)
		return eBIGGERPOSTINGBUFFERNEEDED_OOSQL;
	else
		OOSQL_CHECK_ERR(e);

	return e;
}

// Argument postingLengthBufferSize, postingLengthBuffer, scanDirection are added.

inline Four OOSQL_StorageManagerLOM::Text_NextPostings(
	Four	textScan,					// IN: text scan Id
	Four	postingLengthBufferSize,	// IN: size of buffer to hold postings' length	
	char*	postingLengthBuffer,		// IN: buffer to hold postings' length
	Four	postingBufferSize,			// IN: size of buffer to hold postings
	char*	postingBuffer,				// IN: buffer to hold postings
	Four	scanDirection,				// IN: scan direction to read postings (FORWARD | BACKWARD)
	Four	logicalIdHints,				// IN: logical Id hints to skip postings
	Four*	nReturnedPosting,			// OUT: number of read postings
#ifndef COMPRESSION
	Four*	requiredSize				// OUT: sufficient size of buffer to hold postings
#else
	Four*	requiredSize,				// OUT: sufficient size of buffer to hold postings
	VolNo*	volNoOfPostingTupleID,
	Four*	lastDocId
#endif
)
{
	Four e;

#if defined (ORDEREDSET_BACKWARD_SCAN)

#ifndef COMPRESSION
	e = LOM_Text_NextPostings(m_systemHandle, textScan, postingLengthBufferSize, postingLengthBuffer, postingBufferSize, postingBuffer, scanDirection, logicalIdHints, nReturnedPosting, requiredSize);
#else
	e = LOM_Text_NextPostings(m_systemHandle, textScan, postingLengthBufferSize, postingLengthBuffer, postingBufferSize, postingBuffer, scanDirection, logicalIdHints, nReturnedPosting, requiredSize, volNoOfPostingTupleID, lastDocId);
#endif

#else
	e = LOM_Text_NextPostings(m_systemHandle, textScan, 0, NULL, postingBufferSize, postingBuffer, FORWARD, logicalIdHints, nReturnedPosting, requiredSize);
#endif	/* #if defined (ORDEREDSET_BACKWARD_SCAN) */
	if(e == eBIGGERPOSTINGBUFFERNEEDED_LOM)
		return eBIGGERPOSTINGBUFFERNEEDED_OOSQL;
	else
		OOSQL_CHECK_ERR(e);

	return e;
}

inline Four OOSQL_StorageManagerLOM::Text_GetCursorKeyword(Four textScan, char *keyword)
{
	return LOM_Text_GetCursorKeyword(m_systemHandle, textScan, keyword);
}

inline Four OOSQL_StorageManagerLOM::Text_GetLogicalId(Four ocnOrScanId, Boolean useScanFlag, OID* oid)
{
	return lom_Text_GetLogicalId(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid);
}

inline Four OOSQL_StorageManagerLOM::Text_GetNumOfTextObjectsInClass(Four ocn, Four* nObjects)
{
	return LOM_Text_GetNumOfTextObjectsInClass(m_systemHandle, ocn, nObjects);
}

#ifndef SLIMDOWN_OPENGIS

class OOSQL_StorageManagerGEO : public OOSQL_StorageManagerLOM {
public:
    OOSQL_StorageManagerGEO(OOSQL_SystemHandle* oosqlSystemHandle, GEO_Handle* systemHandle);
    ~OOSQL_StorageManagerGEO();

    Four AddIndex(Four volId, char *className, char *indexName, IndexDesc *indexDesc, IndexID *indexID);
    Four AddIndex(Four volId, Four schemaId, char *tableName, char *indexName, IndexDesc *indexDesc, IndexID *indexID);
    Four DropIndex(Four volId, char *indexName);

    Four OpenClass(Four volId, char* className);
    Four OpenClass(Four volId, Four classId);
    Four GetOpenClassNum(Four volId, Four classId);
    Four CloseClass(Four ocn);

    // Object Manipulation
    Four NextObject(Four scanId, OID* oid, Cursor **cursor);
    Four DestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid);
    Four CreateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, Four nCols, ColListStruct* clist, OID* oid);
    Four UpdateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist);

    Four Geometry_GetMBR(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four colNo, float* xmin, float* ymin, float* xmax, float* ymax);
    Four Geometry_GetMBR(char* data, Four length, float* xmin, float* ymin, float* xmax, float* ymax);

    Four OpenMBRqueryScan(Four ocn, IndexID *iid, Region queryRegion, Four spatialOp, Four nBools, BoolExp* bools, LockParameter *lockup,
	IndexID* tidJoinIndexID = 0, BoundCond* tidJoinIndexStartBound = 0, BoundCond* tidJoinIndexStopBound = 0 );
    Four CloseScan(Four scanId);

private:
    GEO_Handle* m_systemHandle;
};

#endif // SLIMDOWN_OPENGIS

#endif //_OOSQL_STORAGEMANAGER_H_

