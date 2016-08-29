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

#ifndef _LOM_INTERNAL_H
#define _LOM_INTERNAL_H

#include <limits.h>
#ifndef CHAR_BIT
#define CHAR_BIT        8               /* max # of bits in a "char" */
#endif

#define BITMASK(bit) ((unsigned)0x80 >> ((bit) % CHAR_BIT))
#define BITSLOT(bit) ((bit) / CHAR_BIT)
#define BITSET(ary, bit) ((ary)[BITSLOT(bit)] |= BITMASK(bit))
#define BITRESET(ary, bit) ((ary)[BITSLOT(bit)] &= ~BITMASK(bit))
#define BITTEST(ary, bit) ((ary)[BITSLOT(bit)] & BITMASK(bit))

#ifdef  __cplusplus
extern "C" {
#endif

#include "LOM_Param.h"
#include "cosmos_r.h"
#ifdef USE_RPC
#include <rpc/rpc.h> 
#endif /* USE_RPC */
#include "LOM_macro.h"
#include "LOM_Err.h"
#include "LOM_PrimitiveTypes.h"

#ifdef LOM_CLIENT
#include "odys_cs.h"
#include "rename_rpc_proto.h"
#include "odys_conversion.h"
#endif

#ifdef USE_RPC
#undef bcopy
#define bcopy(a,b,c) memcpy(b,a,c)
#undef bcmp
#define bcmp(a,b,c) memcmp(b,a,c)
#undef bzero
#define bzero(a, size) memset(a, 0, size)
#endif

typedef enum {
	LOM_GMT   = 0,  LOM_GMT12 = 12,     LOM_GMT_12 = -12,
	LOM_GMT1  = 1,  LOM_GMT_1 = -1,     LOM_GMT2   =  2,    LOM_GMT_2 = -2,
	LOM_GMT3  = 3,  LOM_GMT_3 = -3,     LOM_GMT4   =  4,    LOM_GMT_4 = -4,
	LOM_GMT5  = 5,  LOM_GMT_5 = -5,     LOM_GMT6   =  6,    LOM_GMT_6 = -6,
	LOM_GMT7  = 7,  LOM_GMT_7 = -7,     LOM_GMT8   =  8,    LOM_GMT_8 = -8,
	LOM_GMT9  = 9,  LOM_GMT_9 = -9,     LOM_GMT10  =  10,   LOM_GMT_10= -10,
	LOM_GMT11 = 11, LOM_GMT_11= -11,    LOM_USeastern = -5, LOM_UScentral = -6,
	LOM_USmoutain = -7, LOM_USpacific = -8 } LOM_TimeZone;

#define MIN(a,b) (((a) < (b)) ? (a):(b))

#define eNOERROR	0
#define BEGIN_MACRO	do {
#define END_MACRO	} while(0)
#define NULL_LENGTH -1
#define LOM_REMAINDER   -1

#define LOM_FALSE	0
#define LOM_TRUE	1

#ifndef USE_RPC
#define FALSE	0
#define TRUE	1
#endif

#define LOM_INITBULKLOAD	10		/* number of initial bulk loading table entry */
#define LOM_INITDNFCN		10		/* number of initial dynamic linking table entry */

typedef One	LOM_Octet;
typedef Four 	LOM_Boolean;
/* date */
typedef UFour_Invariable LOM_Date;
/* time */
typedef struct {
	short _tzHour;
	short _tzMinute;
	short _Hour;
	short _Minute;
	short _Second;
	short _100thSec;
}LOM_Time;
/* timestamp */
typedef struct {
	LOM_Date d;
	LOM_Time t;
}LOM_Timestamp;
/* interval */
typedef double LOM_Interval;

typedef struct {
	Two colNo;                  /* IN column number */
	Boolean nullFlag;           /* TRUE if it has null value */
	Four start;                 /* IN starting offset within a column */
								/*   ALL_VALUE: read all data of this column */
	Four length;                /* IN amount of old data within a column */
	Four dataLength;            /* IN amount of new data */
	union {
		LOM_Boolean b;			/* LOM_BOOLEAN */
		LOM_Octet o;			/* LOM_OCTET */
		Two_Invariable    s;               /* LOM_SHORT */
		UTwo_Invariable   us;				/* LOM_USHORT */
		Four_Invariable    i;               /* LOM_INT */
		Four_Invariable   l;               /* LOM_LONG */
		UFour_Invariable    ul;			/* LOM_ULONG */
		Eight_Invariable	ll;
		float  f;               /* LOM_FLOAT */
		double d;               /* LOM_DOUBLE */
		PageID pid;				/* SM_PAGEID */
		FileID fid;				/* SM_FILEID */
		IndexID iid;			/* SM_INDEXID */
		OID    oid;             /* LOM_OID */ 
		LRDS_MBR mbr;           /* LOM_MBR */ 
		LOM_Date date;			/* LOM_DATE */
		LOM_Time time;			/* LOM_TIME */ 
		LOM_Timestamp timestamp;/* LOM_TIMESTAMP */
		LOM_Interval interval;	/* LOM_INTERVAL */ 
		void   *ptr;            /* pointer to data: SM_STRING, SM_VARSTRING */
	} data;
	Four retLength;             /* OUT return length of Read/Write */
} LOM_ColListStruct;

/* For ODMG Collection */
#define LOM_COMPLEXTYPE_BASIC					SM_COMPLEXTYPE_BASIC
#define LOM_COMPLEXTYPE_SET						SM_COMPLEXTYPE_SET
#define LOM_COMPLEXTYPE_COLLECTIONSET			SM_COMPLEXTYPE_COLLECTIONSET
#define LOM_COMPLEXTYPE_COLLECTIONBAG			SM_COMPLEXTYPE_COLLECTIONBAG
#define LOM_COMPLEXTYPE_COLLECTIONLIST			SM_COMPLEXTYPE_COLLECTIONLIST
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONSET		SM_COMPLEXTYPE_COLLECTIONLIST + 1
#define	LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG		SM_COMPLEXTYPE_COLLECTIONLIST + 2
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST		SM_COMPLEXTYPE_COLLECTIONLIST + 3
#define LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY	SM_COMPLEXTYPE_COLLECTIONLIST + 4

#define	LOM_SHORT	SM_SHORT
#define LOM_INT		SM_INT
#define LOM_LONG	SM_LONG
#define LOM_LONG_LONG	SM_LONG_LONG
#define	LOM_FLOAT	SM_FLOAT
#define	LOM_DOUBLE	SM_DOUBLE
#define LOM_STRING	SM_STRING
#define LOM_VARSTRING	SM_VARSTRING
#define LOM_PAGEID	SM_PAGEID
#define LOM_FILEID	SM_FILEID
#define LOM_INDEXID	SM_INDEXID
#define LOM_OID		SM_OID
#define	LOM_MBR		SM_MBR
#define LOM_REF		30
#define LOM_LINK	31
#define	LOM_TEXT	39
#define LOM_DATE	50
#define LOM_TIME	51
#define LOM_TIMESTAMP	52
#define LOM_INTERVAL	53

/* The following types are relevant to ODMG type system */
#define LOM_USHORT	54
#define LOM_ULONG	55
#define LOM_BOOLEAN	56
#define LOM_OCTET	57

#define LOM_OGIS_GEOMETRY		128
#define LOM_OGIS_POINT			129
#define LOM_OGIS_LINESTRING		130
#define LOM_OGIS_POLYGON		131
#define LOM_OGIS_GEOMETRYCOLLECTION	132
#define LOM_OGIS_MULTIPOINT		133
#define LOM_OGIS_MULTILINESTRING	134
#define LOM_OGIS_MULTIPOLYGON		135

#define	LOM_SHORT_SIZE		SM_SHORT_SIZE
#define LOM_INT_SIZE		SM_INT_SIZE
#define LOM_LONG_SIZE		SM_LONG_SIZE
#define LOM_LONG_LONG_SIZE	SM_LONG_LONG_SIZE
#define	LOM_FLOAT_SIZE		SM_FLOAT_SIZE
#define	LOM_DOUBLE_SIZE		SM_DOUBLE_SIZE
#define LOM_PAGEID_SIZE		SM_PAGEID_SIZE
#define LOM_FILEID_SIZE		SM_FILEID_SIZE
#define LOM_INDEXID_SIZE	SM_INDEXID_SIZE
#define LOM_OID_SIZE		SM_OID_SIZE
#define	LOM_MBR_SIZE		SM_MBR_SIZE
#define LOM_REF_SIZE		SM_OID_SIZE
#define LOM_LINK_SIZE		LOM_REF_SIZE


#define LOM_DATE_SIZE		sizeof(UFour_Invariable)
#define LOM_TIME_SIZE		sizeof(short)*6
#define LOM_TIMESTAMP_SIZE	(sizeof(UFour_Invariable) + sizeof(Two_Invariable)*6)
#define LOM_INTERVAL_SIZE	sizeof(double)
#define LOM_USHORT_SIZE		LOM_SHORT_SIZE
#define LOM_ULONG_SIZE		LOM_LONG_SIZE
#define LOM_BOOLEAN_SIZE	sizeof(LOM_Boolean)
#define LOM_OCTET_SIZE		sizeof(LOM_Octet)

#ifdef USE_RPC
#define LOM_MAXHOSTADDRESS      256
#define LOM_MAXPROTOCOLSTRING   60
#define LOM_TO_RPC 1
#define RPC_TO_LOM 2

typedef struct {
	CLIENT *serverHandle;
	CLIENT *brokerHandle;
	Four serverNo;
	char hostAddress[LOM_MAXHOSTADDRESS];
	char protocolString[LOM_MAXPROTOCOLSTRING];
}LOM_ConnectionInfo;

#define LOM_ENCODEDCOLDESC_SIZE sizeof(Two) + sizeof(One) + sizeof(Four)*3
#define LOM_UPDATE_COLDESC_SIZE sizeof(Two) + sizeof(One) + sizeof(Four)*3
#define LOM_FETCH_COLDESC_SIZE sizeof(Two) + sizeof(Four)*3

#define RPC_CALL(f)     f
#endif /* USE_RPC*/

/* misc macro */
#define LOM_MAXVARSTRINGSIZE 1024
#define LOM_MAXLARGEOBJECTSIZE	LONG_MAX
#define LOM_LOGICALID_COLNO	0
#define LOM_LOGICALID_COLNAME	"_logicalId"
#define KEYWORD		0
#define REVERSEKEYWORD	1
#define LOM_RPCKE_BULKBUFSIZE 16384

#define LOM_DEFAULT_KEYWORDEXTRACTOR_NO	0
#define LOM_DEFAULT_FILTER_NO			0
#define LOM_DEFAULT_STEMIZER_NO			0

/* For ODMG Collection */
#define LOM_MAXNUMOFODMGCOLLCOLUMN 10

/* 
** LOM Catalog tables 
*/

#define LOM_NUMOFCATALOGTABLE 8
#define LOM_SYSCLASSES 0
#define LOM_SYSINHERITANCE 1
#define LOM_SYSCOLUMNS 2
#define LOM_SYSMETHODS 3
#define LOM_CLASSID	4
#define LOM_RELATIONSHIP 5
#define LOM_RELATIONSHIPID 6
#define LOM_SYSINDEXES	7

/* 
** For access of columns of LOM Catalog tables 
*/ 

/* LOM_SYSCLASSES */
#define LOM_SYSCLASSES_NUM_COLS					3
#define LOM_SYSCLASSES_NUM_FIXEDCOLS			2
#define LOM_SYSCLASSES_NUM_VARCOLS       		1
#define LOM_SYSCLASSES_CLASSNAME_COLNO 			0
#define LOM_SYSCLASSES_CLASSID_COLNO 	  		1
#define LOM_SYSCLASSES_DATAFILEID_COLNO 		2
#define LOM_SYSCLASSES_CLASSNAME_COLNAME 		"className"
#define LOM_SYSCLASSES_CLASSID_COLNAME 	  		"classId"
#define LOM_SYSCLASSES_DATAFILEID_COLNAME 		"dataFileId"

/* LOM_INHERITANCE */
#define LOM_INHERITANCE_NUM_COLS			3
#define LOM_INHERITANCE_NUM_FIXEDCOLS			3
#define LOM_INHERITANCE_NUM_VARCOLS       		0
#define LOM_INHERITANCE_CLASSID_COLNO			0
#define LOM_INHERITANCE_SUPERCLASSID_COLNO 		1
#define LOM_INHERITANCE_ORDER_COLNO 			2
#define LOM_INHERITANCE_CLASSID_COLNAME			"classId"
#define LOM_INHERITANCE_SUPERCLASSID_COLNAME 	"superClassId"
#define LOM_INHERITANCE_ORDER_COLNAME 			"order"

/* LOM_SYSCLOLUMNS */
/*#define LOM_SYSCOLUMNS_NUM_COLS					8 */
#define LOM_SYSCOLUMNS_NUM_COLS					9
#define LOM_SYSCOLUMNS_NUM_FIXEDCOLS			6
#define LOM_SYSCOLUMNS_NUM_VARCOLS       		2
#define LOM_SYSCOLUMNS_CLASSID_COLNO 	   		0
#define LOM_SYSCOLUMNS_COLUMNNO_COLNO 			1
#define LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO		2 /* For ODMG Collection */
#define LOM_SYSCOLUMNS_COLTYPE_COLNO			3 
#define LOM_SYSCOLUMNS_COLNAME_COLNO			4
#define LOM_SYSCOLUMNS_COLOFFSET_COLNO 			5
#define LOM_SYSCOLUMNS_INHERITEDFROM_COLNO		6
#define LOM_SYSCOLUMNS_DOMAINID_COLNO			7
#define LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO		8
#define LOM_SYSCOLUMNS_CLASSID_COLNAME 	   		"classId"
#define LOM_SYSCOLUMNS_COLUMNNO_COLNAME 		"columnNo"
#define LOM_SYSCOLUMNS_COMPLEXTYPE_COLNAME		"complexType"
#define LOM_SYSCOLUMNS_COLTYPE_COLNAME			"colType"
#define LOM_SYSCOLUMNS_COLNAME_COLNAME			"colName"
#define LOM_SYSCOLUMNS_COLOFFSET_COLNAME 		"colOffset"
#define LOM_SYSCOLUMNS_INHERITEDFROM_COLNAME	"inheritedFrom"
#define LOM_SYSCOLUMNS_DOMAINID_COLNAME			"domainId"
#define LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNAME	"activeColumnNo"

/* LOM_SYSMETHODS */
#define LOM_SYSMETHODS_NUM_COLS				8
#define LOM_SYSMETHODS_NUM_FIXEDCOLS			4
#define LOM_SYSMETHODS_NUM_VARCOLS       		4
#define LOM_SYSMETHODS_CLASSID_COLNO	 	    	0
#define LOM_SYSMETHODS_DIRPATH_COLNO 			1
#define LOM_SYSMETHODS_METHODNAME_COLNO 		2
#define	LOM_SYSMETHODS_FUNCTIONNAME_COLNO		3
#define LOM_SYSMETHODS_NARGUMENTS_COLNO 	  	4
#define LOM_SYSMETHODS_ARGUMENTLIST_COLNO 		5
#define LOM_SYSMETHODS_RETURNTYPE_COLNO 		6
#define LOM_SYSMETHODS_INHERITEDFROM_COLNO		7
#define LOM_SYSMETHODS_CLASSID_COLNAME	 	    "classId"
#define LOM_SYSMETHODS_DIRPATH_COLNAME 			"dirPath"
#define LOM_SYSMETHODS_METHODNAME_COLNAME 		"methodName"
#define	LOM_SYSMETHODS_FUNCTIONNAME_COLNAME		"functionName"
#define LOM_SYSMETHODS_NARGUMENTS_COLNAME 	  	"nArguments"
#define LOM_SYSMETHODS_ARGUMENTLIST_COLNAME 	"argumentList"
#define LOM_SYSMETHODS_RETURNTYPE_COLNAME 		"returnType"
#define LOM_SYSMETHODS_INHERITEDFROM_COLNAME	"inheritedFrom"

/* LOM_SYSRELATIONSHIP */
/*  
	two attributes are appended : relationshipId, direction
*/
#define LOM_SYSRELATIONSHIP_NUM_COLS			8
#define LOM_SYSRELATIONSHIP_NUM_FIXEDCOLS		7
#define LOM_SYSRELATIONSHIP_NUM_VARCOLS    		1
#define LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO		0
#define LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO		1
#define LOM_SYSRELATIONSHIP_TOCLASSID_COLNO 		2
#define LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO		3
#define LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO	4
#define LOM_SYSRELATIONSHIP_DIRECTION_COLNO		5
#define LOM_SYSRELATIONSHIP_CARDINALITY_COLNO		6
#define LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO	7
#define LOM_SYSRELATIONSHIP_FROMCLASSID_COLNAME		"fromClassId"
#define LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNAME		"fromAttrNum"
#define LOM_SYSRELATIONSHIP_TOCLASSID_COLNAME 		"toClassId"
#define LOM_SYSRELATIONSHIP_TOATTRNUM_COLNAME		"toAttrNum"
#define LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNAME	"relationshipId"
#define LOM_SYSRELATIONSHIP_DIRECTION_COLNAME		"direction"
#define LOM_SYSRELATIONSHIP_CARDINALITY_COLNAME		"cardinality"
#define LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNAME	"relationshipName"

/* LOM_TEXT_FILTERID */
#define LOM_TEXT_FILTERID_NUM_COLS			1
#define LOM_TEXT_FILTERID_NUM_FIXEDCOLS			1
#define LOM_TEXT_FILTERID_NUM_VARCOLS       		0
#define LOM_TEXT_FILTERID_FILTERID_COLNO	 	0
#define LOM_TEXT_FILTERID_FILTERID_COLNAME	 	"filterId"

#define INITIAL_FILTERID				0

/* LOM_TEXT_KEYWORDEXTRACTORID */
#define LOM_TEXT_KEYWORDEXTRACTORID_NUM_COLS		1
#define LOM_TEXT_KEYWORDEXTRACTORID_NUM_FIXEDCOLS	1
#define LOM_TEXT_KEYWORDEXTRACTORID_NUM_VARCOLS       	0
#define LOM_TEXT_KEYWORDEXTRACTORID_COLNO    		0
#define LOM_TEXT_KEYWORDEXTRACTORID_COLNAME    		"keywordExtractorId"

#define INITIAL_KEYWORDEXTRACTORID			0

/* LOM_TEXT_STEMIZERID */
#define LOM_TEXT_STEMIZERID_NUM_COLS		1
#define LOM_TEXT_STEMIZERID_NUM_FIXEDCOLS	1
#define LOM_TEXT_STEMIZERID_NUM_VARCOLS       	0
#define LOM_TEXT_STEMIZERID_COLNO    		0
#define LOM_TEXT_STEMIZERID_COLNAME    		"stemizerId"

#define INITIAL_STEMIZERID			0

/* LOM_CLASSID */
/* LOM_CLASSID */
#define LOM_CLASSID_NUM_COLS					1
#define LOM_CLASSID_NUM_FIXEDCOLS				1
#define LOM_CLASSID_NUM_VARCOLS       			0
#define LOM_CLASSID_CLASSID_COLNO	 	    	0
#define LOM_CLASSID_CLASSID_COLNAME	 	    	"classId"
#define INITIAL_CLASSID							1000

/* LOM_RELATIONSHPIID */
#define LOM_RELATIONSHIPID_NUM_COLS					1
#define LOM_RELATIONSHIPID_NUM_FIXEDCOLS			1
#define LOM_RELATIONSHIPID_NUM_VARCOLS       		0
#define LOM_RELATIONSHIPID_RELATIONSHIPID_COLNO	   	0
#define LOM_RELATIONSHIPID_RELATIONSHIPID_COLNAME	"relationshipId"
#define INITIAL_RELATIONSHIPID						100

/* LOM_DBARRAY */
#define LOM_DBARRAY_NUM_COLS				5
#define LOM_DBARRAY_NUM_FIXEDCOLS			4
#define LOM_DBARRAY_NUM_VARCOLS       		1
#define LOM_DBARRAY_HASPARENT_COLNO    		0
#define LOM_DBARRAY_SIZE_COLUMN	    		1
#define LOM_DBARRAY_NUMELEM_COLNO	   		2
#define LOM_DBARRAY_ELEMCLASSID_COLNO		3
#define LOM_DBARRAY_DATA_COLNO	   			4
#define LOM_DBARRAY_HASPARENT_COLNAME    	"hasParent"
#define LOM_DBARRAY_SIZE_COLNAME    		"size"
#define LOM_DBARRAY_NUMELEM_COLNAME	   		"numElement"
#define LOM_DBARRAY_ELEMCLASSID_COLNAME		"elementClassId"
#define LOM_DBARRAY_DATA_COLNAME	   		"data"

/* LOM_SYSINDEXES */
#define LOM_SYSINDEXES_NUM_COLS					3
#define LOM_SYSINDEXES_NUM_FIXEDCOLS			1
#define LOM_SYSINDEXES_NUM_VARCOLS       		2
#define LOM_SYSINDEXES_INDEXNAME_COLNO 			0
#define LOM_SYSINDEXES_INDEXID_COLNO 			1
#define LOM_SYSINDEXES_CLASSNAME_COLNO			2
#define LOM_SYSINDEXES_INDEXNAME_COLNAME 		"indexName"
#define LOM_SYSINDEXES_INDEXID_COLNAME 			"indexId"
#define LOM_SYSINDEXES_CLASSNAME_COLNAME		"className"

/* LOM_SYSTEXTINDEXES */
#define LOM_SYSTEXTINDEXES_NUM_COLS						7
#define LOM_SYSTEXTINDEXES_NUM_FIXEDCOLS				5
#define LOM_SYSTEXTINDEXES_NUM_VARCOLS					2
#define LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNO		0
#define LOM_SYSTEXTINDEXES_KEYWORDINDEXID_COLNO			1
#define LOM_SYSTEXTINDEXES_REVKEYWORDINDEXID_COLNO		2
#define LOM_SYSTEXTINDEXES_DOCIDINDEXTABLENAME_COLNO	3
#define LOM_SYSTEXTINDEXES_DOCIDINDEXID_COLNO			4
#define LOM_SYSTEXTINDEXES_COLUMNNO_COLNO				5
#define LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNO	6
#define LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNAME	"invertedIndexName"
#define LOM_SYSTEXTINDEXES_KEYWORDINDEXID_COLNAME		"keywordIndexId"
#define LOM_SYSTEXTINDEXES_REVKEYWORDINDEXID_COLNAME	"revKeywordIndexId"
#define LOM_SYSTEXTINDEXES_DOCIDINDEXTABLENAME_COLNAME	"docIdIndexTableName"
#define LOM_SYSTEXTINDEXES_DOCIDINDEXID_COLNAME			"docIdIndexId"
#define LOM_SYSTEXTINDEXES_COLUMNNO_COLNAME				"columnNo"
#define LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNAME	"postingStructureInfo"

/* for filter information and keyword extractor information */
/* LOM_SYSTEXTFILTERINFO */
#define LOM_SYSTEXTFILTERINFO_NUM_COLS						5
#define LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO				0
#define LOM_SYSTEXTFILTERINFO_VERSION_COLNO					1
#define LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO			2
#define LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO		3
#define LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO				4
#define LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNAME			"filterName"
#define LOM_SYSTEXTFILTERINFO_VERSION_COLNAME				"version"
#define LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNAME		"filterFilePath"
#define LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNAME	"filterFunctionName"
#define LOM_SYSTEXTFILTERINFO_FILTERNO_COLNAME				"filterNo"

/* LOM_SYSTEXTKEYWORDEXTRACTORINFO */
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS									7
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO					0
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO								1
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO				2
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO			3
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO		4
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO	5
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO					6
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNAME				"keywordExtractorName"
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNAME								"version"
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNAME			"keywordExtractorFilePath"
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNAME		"keywordExtractorFunctionName"
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNAME		"getNextPostingInfoFunctionName"
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNAME "finalizeKeywordExtractorFunctionName"
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNAME					"keywordExtractorNo"

/* LOM_SYSTEXTSTEMIZERINFO */
#define LOM_SYSTEXTSTEMIZERINFO_NUM_COLS					5
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO			0
#define LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO				1
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO		2
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO 	3
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO			4
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNAME			"stemizerName"
#define LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNAME					"version"
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNAME		"stemizerFilePath"
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNAME 	"stemizerFunctionName"
#define LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNAME				"stemizerNo"

/* LOM_SYSTEXTPREFERENCES */
#define LOM_SYSTEXTPREFERENCES_NUM_COLS					5
#define LOM_SYSTEXTPREFERENCES_CLASSID_COLNO			0
#define LOM_SYSTEXTPREFERENCES_COLNO_COLNO				1
#define LOM_SYSTEXTPREFERENCES_FILTERNO_COLNO			2
#define LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNO	3
#define LOM_SYSTEXTPREFERENCES_STEMIZERNO_COLNO			4
#define LOM_SYSTEXTPREFERENCES_CLASSID_COLNAME			"classId"
#define LOM_SYSTEXTPREFERENCES_COLNO_COLNAME			"colNo"
#define LOM_SYSTEXTPREFERENCES_FILTERNO_COLNAME			"filterNo"
#define LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNAME	"keywordExtractorNo"
#define LOM_SYSTEXTPREFERENCES_STEMIZERNO_COLNAME		"stemizerNo"

/* LOM_LARGEOBJECT */
#define	LOM_LARGEOBJECT_NUM_COLS			4
#define LOM_LARGEOBJECT_NUM_FIXEDCOLS			4
#define LOM_LARGEOBJECT_NUM_VARCOLS       		0
#define LOM_LARGEOBJECT_HASPARENT_COLNO    		0
#define LOM_LARGEOBJECT_SIZE_COLNO 		   		1
#define LOM_LARGEOBJECT_OID_COLUMN	    		2
#define LOM_LARGEOBJECT_OPENCLASSNUM_COLNO		3
#define LOM_LARGEOBJECT_HASPARENT_COLNAME    	"hasParent"
#define LOM_LARGEOBJECT_SIZE_COLNAME 		   	"size"
#define LOM_LARGEOBJECT_OID_COLNAME	    		"oid"
#define LOM_LARGEOBJECT_OPENCLASSNUM_COLNAME	"openClassNum"

#ifndef LRDS_COUNTER

/* catalog information */
#define	LOM_SYSCOUNTER_NUM_COLS				3
#define LOM_SYSCOUNTER_NAME_COLNO			0
#define LOM_SYSCOUNTER_ID_COLNO				1
#define LOM_SYSCOUNTER_VALUE_COLNO			2
#define LOM_SYSCOUNTER_NAME_COLNAME			"name"
#define LOM_SYSCOUNTER_ID_COLNAME			"id"
#define LOM_SYSCOUNTER_VALUE_COLNAME		"value"

#define LOM_COUNTER_CLASSNAME		"lomSysCounter"
#endif

 


/*
** For Tuple Values of the LOM Catalog Tables 
*/

#define LOM_SYSCLASSES_CLASSNAME					"lomSysClasses"
#define LOM_INHERITANCE_CLASSNAME					"lomInheritance"	
#define LOM_SYSCOLUMNS_CLASSNAME					"lomSysColumns"		
#define LOM_SYSMETHODS_CLASSNAME					"lomSysMethods"
#define LOM_CLASSID_CLASSNAME						"lomClassId"
#define LOM_SYSRELATIONSHIP_CLASSNAME				"lomSysRelationship"
#define LOM_RELATIONSHIPID_CLASSNAME				"lomRelationshipId"
#define LOM_SYSINDEXES_CLASSNAME					"lomSysIndexes"
#define LOM_SYSTEXTINDEXES_CLASSNAME				"lomSysTextIndexes"
#define LOM_SYSNAMEDOBJECT_CLASSNAME				"lomSysNamedObject"
#define LOM_SYSTEXTFILTERINFO_CLASSNAME				"lomSysTextFilterInfo"
#define LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME	"lomSysTextKeywordExtractorInfo"
#define LOM_SYSTEXTPREFERENCES_CLASSNAME			"lomSysTextPreferences"
#define LOM_TEXT_FILTERID_CLASSNAME					"lomTextFilterId"
#define LOM_TEXT_KEYWORDEXTRACTORID_CLASSNAME		"lomTextKeywordExtractorId"
#define LOM_SYSTEXTSTEMIZERINFO_CLASSNAME			"lomSysTextStemizerInfo"
#define LOM_TEXT_STEMIZERID_CLASSNAME				"lomTextStemizerId"

/*
 ** TEXT-related system tables
 */
#define LOM_CONTENT_NUM_COLS					1
#define LOM_CONTENT_CONTENT_COLNO				0

#define LOM_DELETIONLIST_NUM_COLS					1
#define LOM_DELETIONLIST_DELETIONLIST_COLNO			0
#define LOM_DELETIONLIST_DELETIONLIST_ELEMENTSIZE	(sizeof(Four) + sizeof(OID))

#define LOM_INVERTEDINDEX_NUM_COLS				5
#define LOM_INVERTEDINDEX_KEYWORD_COLNO			0
#define LOM_INVERTEDINDEX_REVKEYWORD_COLNO		1
#define LOM_INVERTEDINDEX_NPOSTINGS_COLNO		2
#define LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO	3
#define LOM_INVERTEDINDEX_POSTINGLIST_COLNO		4
#define LOM_INVERTEDINDEX_KEYWORD_COLNAME		"keyword"
#define LOM_INVERTEDINDEX_REVKEYWORD_COLNAME	"revKeyword"
#define LOM_INVERTEDINDEX_NPOSTINGS_COLNAME		"nPostings"
#define LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNAME	"sizeOfPosting"
#define LOM_INVERTEDINDEX_POSTINGLIST_COLNAME	"postingList"


#define LOM_DOCIDTABLE_NUM_COLS					2
#define LOM_DOCIDTABLE_DOCID_COLNO				0
#define LOM_DOCIDTABLE_POINTERLIST_COLNO		1
#define LOM_DOCIDTABLE_DOCID_COLNAME			"docId"
#define LOM_DOCIDTABLE_POINTERLIST_COLNAME		"pointerList"

/* Named Object Table */
#define LOM_NAMEDOBJECTTABLE_NUM_COLS			2
#define LOM_NAMEDOBJECTTABLE_OBJNAME_COLNO		0
#define LOM_NAMEDOBJECTTABLE_OBJECTID_COLNO		1
#define LOM_NAMEDOBJECTTABLE_OBJNAME_COLNAME		"objName"
#define LOM_NAMEDOBJECTTABLE_OBJECTID_COLNAME		"objectId"

/*
** For catalog table access
*/
#define LOM_NUMOFCATALOGTABLES 4
#define LOM_SYSCLASSES	0
#define LOM_INHERITANCE	1
#define LOM_COLUMNS	2
#define LOM_METHODS	3

/*
 ** object descriptor
 */

#define LOM_OBJDESC_SIZE sizeof(objectDescriptor)
#define LOM_OBJREF_SIZE sizeof(objectRef)
#define LENGTH_NULLVECTOR(nCols) ((nCols+7)/8)
#define LENGTH_UPDATEVECTOR(nCols) ((nCols+7)/8)

/* Type definition for LOM_IndexDesc */

typedef LRDS_IndexDesc	LOM_IndexDesc;

/*
** Type definition for Attribute Information used as a parameter of 
** LOM_CreateClass Interface.
*/


typedef struct {
	Two  	complexType;       /* data type of column */
	Two		type;			/* Attribute type */
	Four	length;			/* Attribute length ,for maximum length of SM_STRING */ 
	char	name[LOM_MAXATTRNAME];			/* Attribute name */
	Four	inheritedFrom;		/*super class ID */
#ifndef	GEOM	/* needless in GEOM */
	Four	domain;			/* domain id */
	Four	offset;						/* offset of attribute */
#endif	/* GEOM */
}AttrInfo;

/*
** Type definition for Method Information used as a parameter of 
** LOM_CreateClass Interface.
*/

/* 
** to use MethodInfo, you should allocate for the field ,i.e.,
** dirPath, name, ArgumentsList, inheritedFrom.
*/

typedef struct {
	char	dirPath[LOM_MAXDIRPATH];		/* path where this method is implemented */
	char	name[LOM_MAXMETHODNAME];			/* Method name */
	char	functionName[LOM_MAXFUNCTIONNAME];	/* c function name */
	short		nArguments;		/* # of arguments */	
	Four 	ArgumentsList[LOM_MAXNARGUMENT];	/* list of arguments */
	Four	inheritedFrom;					
	Four	returnType;
}MethodInfo;


typedef struct {
	char	dirPath[LOM_MAXDIRPATH];		/* path where this method is implemented */
	char	name[LOM_MAXMETHODNAME];			/* Method name */
	char	functionName[LOM_MAXFUNCTIONNAME];	/* c function name */
	Two		nArguments;						/* # of arguments */	
	Four 	ArgumentsList[LOM_MAXNARGUMENT];	/* list of arguments */
	Four	inheritedFrom;					/* indicated where this method is inherited from */
	Four 	returnType;						/* return type */ 
} lom_SysMethodsOverlay;

/* Error Handling */
#define LOM_ERROR(handle, e) \
BEGIN_MACRO \
Util_ErrorLog_Printf("Error : %ld(%s) in %s:%ld\n", (e), "LOM", __FILE__, __LINE__); \
if (1) return(e);  \
END_MACRO

#define LOM_PTRERROR(handle, e) \
BEGIN_MACRO \
Util_ErrorLog_Printf("Error : %ld(%s) in %s:%ld\n", (e), "LOM", __FILE__, __LINE__); \
END_MACRO

#ifdef NEW_COSMOS 
#ifndef LRDS_ERROR
#define LRDS_ERROR ERR
#endif
#endif

typedef struct {
	Four	baseScanId;
	Four	complexType;		/* Complextype of the column to scan */
	Four	nextFreeEntryIndex;
} lom_RelationshipScanTableEntry;

typedef struct {
    Two			lrdsCollectionScanId;		/* relation scan id for the relation containing the collection */
	Four		complexType;				/* complextype of the column */
	Four		type;						/* type of the column */
	Four		length;						/* length of the column */
	Four		nextFreeEntryIndex;
} lom_ODMG_CollectionScanTableEntry;

typedef struct {
/* the following "serverInstanceId" variable is only useful if LOM_CLIENT is defined */
	Four serverInstanceId;
	Four instanceId;	
	Four lrdsHandle;
}LOM_Handle;

#define LOM_GET_LRDS_HANDLE(lomHandle)  (lomHandle)->lrdsHandle

typedef Four (*lom_FilterFunc)(Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, char *outFileName);
typedef Four (*lom_KeywordExtractorFunc)(Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, Four *resultHandle);
typedef Four (*lom_GetNextPostingInfoFunc)(Four resultHandle, char *keyword, Four *nPositions, char *positionList);
typedef Four (*lom_FinalizeKeywordExtractionFunc)(Four resultHandle);
typedef Four (*lom_StemizerFunc)(char *keyword, char *stemizedKeyword);

typedef void* lom_FptrToFilter;
typedef void* lom_FptrToKeywordExtractor;
typedef void* lom_FptrToGetNextPostingInfo;
typedef void* lom_FptrToFinalizeKeywordExtraction;
typedef void* lom_FptrToStemizer;

/*
** Type Definition for lom_UserOpenClassTableEntry
**   - open class per process
**   - index for LRDSOpenRelationTable
*/
typedef struct {
	Two			count;
	Two			numOfTextAttrs;	/* number of text-attribute */

	/* START: CLIENT ONLY */
	Two			volId;
	/* END: CLIENT ONLY */

	Boolean		openedByLOM_GetOpenClassNum;
	Four		classID;

	/* START: SERVER ONLY */
	CounterID	counterId;	/* counter for logical id */
	Four		ornForContentTable;
	Four		ornForDeletionListTable;
	TupleID		tidForDeletionList;
	Two			textColNo[LOM_MAXNUMOFTEXTCOLUMN];
	Four		ornForInvertedIndexTable[LOM_MAXNUMOFTEXTCOLUMN];
	Four		ornForDocIdIndexTable[LOM_MAXNUMOFTEXTCOLUMN];

	Four		lrdsScanIdForTextScan;			
	Four		contentTableScanIdForTextScan;	

	/* DL stands for "Dynamic Linking" */
	void		*handleForDLOfKeywordExtractor[LOM_MAXNUMOFTEXTCOLUMN];
	lom_FptrToKeywordExtractor fptrToKeywordExtractor[LOM_MAXNUMOFTEXTCOLUMN];

	lom_FptrToGetNextPostingInfo fptrToGettingNextPostingInfo[LOM_MAXNUMOFTEXTCOLUMN];
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction[LOM_MAXNUMOFTEXTCOLUMN];

	void		*handleForDLOfFilter[LOM_MAXNUMOFTEXTCOLUMN];
	lom_FptrToFilter fptrToFilter[LOM_MAXNUMOFTEXTCOLUMN];

	void		*handleForDLOfStemizer[LOM_MAXNUMOFTEXTCOLUMN];
	lom_FptrToStemizer fptrToStemizer[LOM_MAXNUMOFTEXTCOLUMN];

	/* For ODMG Collection */
	Four		numOfodmgCollAttrs;
	Four		odmgCollColNo[LOM_MAXNUMOFODMGCOLLCOLUMN];
	Four		odmgCollDataOrn[LOM_MAXNUMOFODMGCOLLCOLUMN];
	/* END: SERVER ONLY */
} lom_UserOpenClassTableEntry;


#define LOM_NUM_OF_ENTRIES_OF_USEROPENCLASSTABLE MAXNUMOFOPENRELS
#define LOM_IS_UNUSED_ENTRY_OF_USEROPENCLASSTABLE(entryNo) \
(LOM_USEROPENCLASSTABLE[entryNo].count == NIL)

/* scan type supported by lom-level */
#define LOM_GENERAL_SCAN	0
#define LOM_INVERTEDFILE_KEYWORDBASED_SCAN	1
#define LOM_INVERTEDFILE_OIDBASED_SCAN	2

/* 
 * posting buffer 
 */
typedef struct {
	VarArray buffer;
	Four freeOffset;
}lom_PostingBuffer;

typedef struct {
	Boolean isContainingTupleID;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingByteOffset;
	Two nEmbeddedAttributes;
	Two embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
}PostingStructureInfo;

typedef struct {
	Four type;
	Four start;
	Four length;
} LOM_EmbeddedAttrInfo;

typedef struct {
	Four							nEmbeddedAttributes;
	LOM_EmbeddedAttrInfo			embeddedAttrInfo[MAX_NUM_EMBEDDEDATTRIBUTES];
	Two								realColNoToEmbeddedColNo[LOM_MAXNUMOFATTRIBUTE];
	Two								embeddedAttrVarColNo[MAX_NUM_EMBEDDEDATTRIBUTES];
	Four							embeddedAttributeOffset;
	Four							nVarCols;
} LOM_EmbeddedAttrTranslationInfo;
/*
** Type Definition for LOM Scan Table Entry.
*/
typedef struct {
	Four lrdsScanId;					/* lrds scan id */
	Four contentTableScanId;			/* content table scan Id */
	Four scanType;						/* scan type */

	Four ocn;
	Four osnForServer;

	LockParameter lockup;
	Four odmgCollDataScanId[LOM_MAXNUMOFODMGCOLLCOLUMN];
	union {
		/* inverted table information */
		struct {
			Two			colNo;
			Boolean		onlyFetchMode;
			Four		invertedOrn;
			Four		invertedScanId;
			TupleID		invertedTableEntryTid;
			Four        invertedTableEntryTidCount;
			LRDS_Cursor *cursor;
			Four		nPostings;				
			Four		nRemainedPostings;		/* the number of remained postings */
			Four		currentPostingOffset;	/* current offset of posting list */
#ifdef SUBINDEX
			Four		orderedSetScanId;
#endif
		} keywordBasedScan;

		/* posting table information */
		struct {
			Two			colNo;
			Boolean		onlyFetchMode;
			Four		invertedOrn;
			Four		invertedScanId;
			Boolean		isContainingTupleID;
			LRDS_Cursor *cursor;
			Four		logicalDocId;
		} oidBasedScan;

		
	} textScanInfo;
	LOM_EmbeddedAttrTranslationInfo embeddedAttrTranslationInfo;
	Four							nextFreeEntryIndex;
} lom_ScanTableEntry;

typedef struct {
	Four					lrdsBulkLoadId;
	Four					lrdsTextBulkLoadId;
	Boolean					textColumnExist;
	Four					classId;
	Four					ocn;
	lrds_RelTableEntry*		relTableEntry; 
	Four                    logicalId;
	Four					userGivenLogicalId;
	Boolean					newlyCreatedObjectFlag;
} lom_BulkLoadTableEntry;

typedef struct {
	void*   dllHandle;
#ifdef USE_RPC
	CLIENT* dllClient;
#endif /* USE_RPC */
} lom_dllHandle;

typedef struct {
	void*	dllFunc;
#ifdef USE_RPC
	CLIENT*	dllClient;
#endif /* USE_RPC */
	char	funcName[LOM_MAXPATHLENGTH];
} lom_dllFunc;

typedef struct {
	char					moduleName[LOM_MAXPATHLENGTH];
	lom_dllHandle			handle;
	lom_dllFunc				func[LOM_MAXDLLFUNCPTRS];
} lom_DllfcnTableEntry;

typedef struct {
	Four sysclasses;
}CatalogORN;

/*
** Type Definition for Catalog Mount Table Entry.
*/
typedef struct {
	Two  volId;         /* volume identifier */
	Two  count;		/* number of mouting volumn */
	ClassID temporaryClassIdToBeAssigned; /* temporary class id to be assigned */
	VarArray classInfoTbl;	/* pointer to class information pool */
	VarArray attrInfoTbl;		/* pointer to attribute info. pool */
	VarArray methodInfoTbl;	/* pointer to method info. pool */
	VarArray indexInfoTbl;		/* pointer to index info. pool */
	VarArray superClassInfoTbl;	/* pointer to superclass info. pool */
	VarArray subClassInfoTbl;	/* pointer to subclass info. pool */
	VarArray relationshipInfoTbl; /* For Relationship, pointer to relationship info. pool */
	Two freeClassInfoEntryIndex;
	Two freeAttrInfoEntryIndex;
	Two freeMethodInfoEntryIndex;
	Two freeIndexInfoEntryIndex;
	Two freeSuperClassInfoEntryIndex;
	Two freeSubClassInfoEntryIndex;
	Two freeRelationshipInfoEntryIndex; /* For Relationship */
	CatalogORN catalogORN;
} catalog_MountTableEntry;

typedef struct {
#ifdef USE_RPC
	/* CLIENT ONLY */
	LOM_ConnectionInfo connectionInfo;
	/* CLIENT ONLY */
#endif /* USE_RPC */

	/* open class table */
	lom_UserOpenClassTableEntry lomUserOpenClassTable[LOM_NUM_OF_ENTRIES_OF_USEROPENCLASSTABLE];

	VarArray lomScanTable;					/* scan table of LOM */
	VarArray lomRelationshipScanTable;		/* relationship scan table of LOM */
	VarArray lomODMGcollectionScanTable;	/* odmg collection scan table of LOM */
	VarArray lomBulkLoadTable;				/* bulk loading information table */
	VarArray lomDlfcnTable;					/* dynamic linking library information */

	catalog_MountTableEntry catalogMountTable[CATALOG_MAXNUMOFVOLS];
	char inUse;

	Four	 firstFreeScanTableEntryIndex;
	Four	 firstFreeRelationshipScanTableEntryIndex;
	Four     firstFreeODMGcollectionScanTableEntryIndex;

	Four	 useRpcForDllFunctionsFlag;
}lom_GDSInstance;

extern VarArray lomGDSInstanceTable;

#define LOM_GDSTABLE			((lom_GDSInstance*)(lomGDSInstanceTable.ptr))
#define LOM_SCANTABLE_PTR(handle)	&(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomScanTable)
#define LOM_SCANTABLE(handle)	((lom_ScanTableEntry*)((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomScanTable.ptr)
#define LOM_USEROPENCLASSTABLE(handle) ((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomUserOpenClassTable
#define LOM_SCANTABLE_ENTRIES(handle)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomScanTable.nEntries
#define LOM_RELATIONSHIPSCANTABLE(handle)     ((lom_RelationshipScanTableEntry*)((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomRelationshipScanTable.ptr)
#define LOM_RELATIONSHIPSCANTABLE_PTR(handle)     &(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomRelationshipScanTable)
#define LOM_RELATIONSHIPSCANTABLE_ENTRIES(handle)     (((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomRelationshipScanTable.nEntries)
#define LOM_ODMG_COLLECTIONSCANTABLE(handle)     ((lom_ODMG_CollectionScanTableEntry*)((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomODMGcollectionScanTable.ptr)
#define LOM_ODMG_COLLECTIONSCANTABLE_PTR(handle)     &(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomODMGcollectionScanTable)
#define LOM_ODMG_COLLECTIONSCANTABLE_ENTRIES(handle)     (((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomODMGcollectionScanTable.nEntries)
#define LOM_FIRSTFREESCANTABLEENTRYINDEX(handle)				(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].firstFreeScanTableEntryIndex)
#define LOM_FIRSTFREERELATIONSHIPSCANTABLEENTRYINDEX(handle)	(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].firstFreeRelationshipScanTableEntryIndex)
#define LOM_FIRSTFREEODMGCOLLECTIONSCANTABLEENTRYINDEX(handle)	(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].firstFreeODMGcollectionScanTableEntryIndex)

#define LOM_BULKLOADTABLE_PTR(handle)		&(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomBulkLoadTable)
#define LOM_BULKLOADTABLE(handle)			((lom_BulkLoadTableEntry*)((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomBulkLoadTable.ptr)
#define LOM_BULKLOADTABLE_ENTRIES(handle)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomBulkLoadTable.nEntries

#define LOM_DLFCNTABLE_PTR(handle)		&(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomDlfcnTable)
#define LOM_DLFCNTABLE(handle)			((lom_DllfcnTableEntry*)((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomDlfcnTable.ptr)
#define LOM_DLFCNTABLE_ENTRIES(handle)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].lomDlfcnTable.nEntries

#define LOM_USERPCFORDLLFUNCTIONSFLAG(handle) ((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].useRpcForDllFunctionsFlag

#define lom_GetVolIdFromOcn(handle, ocn)        LOM_USEROPENCLASSTABLE(handle)[ocn].volId
#define lom_GetClassIdFromOcn(handle, ocn)      LOM_USEROPENCLASSTABLE(handle)[ocn].classID
#define LOM_CLIENTINFO(handle)  LOM_GDSTABLE[(handle)->instanceId].connectionInfo


#define LOM_VALID_RELATIONSHIPSCANID(handle, x) \
    ( ((x) >= 0) && ((x) < LOM_RELATIONSHIPSCANTABLE_ENTRIES(handle)) && \
    LOM_RELATIONSHIPSCANTABLE(handle)[x].baseScanId != NIL )

#define LOM_ODMG_VALID_COLLECTIONSCANID(handle, x) \
    ( ((x) >= 0) && ((x) < LOM_ODMG_COLLECTIONSCANTABLE_ENTRIES(handle)) && \
    LOM_ODMG_COLLECTIONSCANTABLE(handle)[x].lrdsCollectionScanId != NIL )

#define LOM_GETINDEXID(orn,indexNo) (LRDS_RELTABLE[(orn)].ii[(indexNo)].iid)


#define  KAOSS_NewProcess 	new_process
#define  KAOSS_EndProcess 	end_process
#define	 LOM_GetClassName 	lom_GetClassName


/* index type */
#define LOM_INDEXTYPE_BTREE	SM_INDEXTYPE_BTREE
#define LOM_INDEXTYPE_MLGF	SM_INDEXTYPE_MLGF
#define LOM_INDEXTYPE_TEXT	5

typedef TupleID	LOM_LogicalIndexID;
typedef double	PostingWeight;

/*
 * LOM_IndexID
 *
 */
typedef struct {
	Boolean isLogical;
	union {
		IndexID physical_iid;
		LOM_LogicalIndexID logical_iid;
	}index;
}LOM_IndexID;

typedef struct {
	Boolean isContainingTupleID;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingByteOffset;
	Two		nEmbeddedAttributes;
	Two		embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
	Four    maxEmbeddedAttrLength;
	Four    attrInfoIndex;
	Four    volumeIndex;
}lom_Text_PostingInfoForReading;

typedef struct {
	Boolean isUsingBulkLoading;						
	Boolean isUsingKeywordIndexBulkLoading;			
	Boolean isUsingReverseKeywordIndexBulkLoading;
	Boolean isBuildingExternalReverseKeywordFile;	
	Boolean isBuildingDocIdIndex;				
	Boolean isSortingPostingFile;					
	Boolean isUsingStoredPosting;					
} lom_Text_ConfigForInvertedIndexBuild;

typedef struct {
	Four osn;
	TupleID tid;
}lomTextObjectHandleForKeywordExtractor;

Four LOM_AllocPostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer, Four length);
Four LOM_ReallocPostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer, Four length);
Four LOM_FreePostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer);

#define LOM_PTR_POSTINGBUFFER(postingBuffer) ((char *)((postingBuffer).buffer.ptr))

#define LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) (postingBuffer).freeOffset

#define LOM_ALLOCATEDSIZE_POSTINGBUFFER(postingBuffer) (postingBuffer).freeOffset

#define LOM_FREEBYTES_POSTINGBUFFER(postingBuffer) (postingBuffer).buffer.nEntries - (postingBuffer).freeOffset

#define LOM_SIZE_POSTINGBUFFER(postingBuffer) (postingBuffer).buffer.nEntries

#define LOM_POSTING_SIZE(nPositions, sizeOfPosition)	\
	(sizeof(Four) +	/* logical id */	\
	sizeof(TupleID) +	/* document oid */ \
	sizeof(Four) + /* the number of positions */ \
	(sizeOfPosition) * (nPositions))

/*
 * LOM_TextDesc type
 *
 */
typedef struct {
	One isIndexed;
	One hasBeenIndexed;	/* INTERNAL */
	TupleID contentTid;
	Four size;
}LOM_TextDesc;


#define	MAKE_NULLTEXTDESC(textDesc) SET_NILTUPLEID((textDesc).contentTid), \
					(textDesc).size = NIL, \
					(textDesc).isIndexed = SM_FALSE,	\
					(textDesc).hasBeenIndexed = SM_FALSE;

#define RESET_CONTENTTID_TEXTDESC(textDesc)	SET_NILTUPLEID((textDesc).contentTid);

#define SET_CONTENTSIZE_TEXTDESC(textDesc, size)	((textDesc).size = (size))
#define RESET_CONTENTSIZE_TEXTDESC(textDesc)	((textDesc).size = NIL)
/* for accessing the size of text content data */
#define GET_CONTENTSIZE_TEXTDESC(textDesc)	(textDesc).size

#define SET_HASBEENINDEXED_TEXTDESC(textDesc)	((textDesc).hasBeenIndexed = SM_TRUE)
#define RESET_HASBEENINDEXED_TEXTDESC(textDesc)	((textDesc).hasBeenIndexed = SM_FALSE)

#define SET_ISINDEXED_TEXTDESC(textDesc)	((textDesc).isIndexed = SM_TRUE)
#define RESET_ISINDEXED_TEXTDESC(textDesc)	((textDesc).isIndexed = SM_FALSE)

#define DOES_NOCONTENT_EXIST_TEXTDESC(textDesc) ((textDesc).size == NIL)
#define ISINDEXED_TEXTDESC(textDesc) ((textDesc).isIndexed == SM_TRUE)
#define HASBEENINDEXED_TEXTDESC(textDesc) ((textDesc).hasBeenIndexed == SM_TRUE)

#define LOM_DEFERRED_MODE		0
#define LOM_IMMEDIATE_MODE		1

typedef struct {
	Four start;                 /* IN starting offset within a column */
/*   ALL_VALUE: read all data of this column */
	Four length;                /* IN amount of old data within a column */
	Four dataLength;            /* IN amount of new data */
	Four indexMode;
	void   *data;            /* pointer to data: SM_STRING, SM_VARSTRING */
	Four retLength;             /* OUT return length of Read/Write */
} TextColStruct;

#define	GET_SYSTEMLEVEL_COLNO(userLevelColNo)	(userLevelColNo + 1)
#define	GET_USERLEVEL_COLNO(systemLevelColNo)	(systemLevelColNo - 1)

#define CALCULATE_WEIGHT(nPositions, nPostings)	(float)((float)nPositions)/((float)nPostings)

#define GETCLASSID(oid)	(oid).classID
#define OIDPRINT(oid) \
	printf("pageNo = %ld, volNo = %ld, slotNo = %ld, unique = %ld, classID = %ld",(oid).pageNo, (oid).volNo, (oid).slotNo, (oid).unique, (oid).classID)

/* For ODMG Collection */
#define IS_NULL_COLLECTION(collectionDesc) IS_NILTUPLEID((collectionDesc).dataTid)

typedef struct {
    Four    nElements;
    TupleID dataTid;
} ODMG_CollectionDesc;

typedef struct {
	Four    ith;
    Four    nElements;
    Four*   elementSizes;
    Four    sizeOfElements;
    void*   elements;
    Four    retN_Elements;
} ODMG_CollectionColStruct;

#define LOM_ODMG_COLLECTION_SET_PREFIX			"lomSysSet_"
#define LOM_ODMG_COLLECTION_BAG_PREFIX			"lomSysBag_"
#define LOM_ODMG_COLLECTION_LIST_PREFIX			"lomSysList_"
#define LOM_ODMG_COLLECTION_VARARRAY_PREFIX		"lomSysVararray_"
#define LOM_ODMG_COLLECTION_SHORT_POSTFIX		"shortData"
#define LOM_ODMG_COLLECTION_INT_POSTFIX			"intData"
#define LOM_ODMG_COLLECTION_LONG_POSTFIX		"longData"
#define LOM_ODMG_COLLECTION_LONG_LONG_POSTFIX	"longlongData"
#define LOM_ODMG_COLLECTION_FLOAT_POSTFIX		"floatData"
#define LOM_ODMG_COLLECTION_DOUBLE_POSTFIX		"doubleData"
#define LOM_ODMG_COLLECTION_STRING_POSTFIX		"stringData"
#define LOM_ODMG_COLLECTION_VARSTRING_POSTFIX	"varstringData"
#define LOM_ODMG_COLLECTION_PAGEID_POSTFIX		"pageidData"
#define LOM_ODMG_COLLECTION_FILEID_POSTFIX		"fileidData"
#define LOM_ODMG_COLLECTION_INDEXID_POSTFIX		"indexidData"
#define LOM_ODMG_COLLECTION_OID_POSTFIX			"oidData"
#define LOM_ODMG_COLLECTION_MBR_POSTFIX			"mbrData"
#define LOM_ODMG_COLLECTION_REF_POSTFIX			"refData"
#define LOM_ODMG_COLLECTION_LINK_POSTFIX		"linkData"
#define LOM_ODMG_COLLECTION_TEXT_POSTFIX		"textData"
#define LOM_ODMG_COLLECTION_DATE_POSTFIX		"dateData"
#define LOM_ODMG_COLLECTION_TIME_POSTFIX		"timeData"
#define LOM_ODMG_COLLECTION_TIMESTAMP_POSTFIX	"timestampData"
#define LOM_ODMG_COLLECTION_INTERVAL_POSTFIX	"intervalData"

#ifdef  __cplusplus
}
#endif

#endif /* LOM_INTERNAL_H */
