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

#ifndef _ODYS_CS_H
#define _ODYS_CS_H

#include "rpc/rpc.h"

typedef int CS_FOUR;
bool_t xdr_CS_FOUR();

typedef long CS_Eight;

typedef u_long CS_UFOUR;
bool_t xdr_CS_UFOUR();


typedef short CS_TWO;
bool_t xdr_CS_TWO();


typedef char CS_ONE;
bool_t xdr_CS_ONE();

#define CS_MBR_NUM_PARTS 4
#define CS_MAXDATABASENAME 256
#define CS_MAXVOLUMENAME 1024
#define CS_MAXDEVICESINVOLUME 10
#define CS_MAXDEVICENAMELIST 10240
#define CS_RELATIONSHIPNAME LOM_MAXCLASSNAME 
#define CS_OBJECTNAME LOM_MAXCLASSNAME
#define CS_MAXQUERYSTRING 4096
#define CS_MAXRELATIONSHIPNAME LOM_MAXCLASSNAME
#define CS_MAXNUMOFATTR 128
#define CS_MAXNUMOFMETHOD 128
#define CS_MAXATTRNAME LOM_MAXCLASSNAME
#define CS_MAXDIRPATH 256
#define CS_MAXFUNCTIONNAME 80
#define CS_MAXNARGUMNET 30
#define CS_MAXMETHODNAME 80
#define CS_MAXKEYLEN 120
#define CS_MAXFIELD 240
#define CS_MAXNUMOFATTRIBUTE 120
#define CS_MAXCLASSNAME LOM_MAXCLASSNAME
#define CS_MAXINDEXNAME LOM_MAXCLASSNAME
#define CS_MAXNUMSUPERCLASS LOM_MAXCLASSNAME
#define CS_MAXNUMKEYPARTS 32
#define CS_UNIQUE 1
#define CS_CLUSTERING 2
#define CS_FORWARD 0
#define CS_BACKWARD 1
#define CS_EOF 1
#define CS_MAXBOOLEXP CS_MAXNUMOFATTR
#define CS_BIGENDIAN 0
#define CS_LITTLEENDIAN 1
#define CS_MAXNARGUMENT 36
#define CS_MAXKEYWORDSIZE 128
#define CS_MAXOBJECTNAME 128
#ifndef CS_COLINFO
#define CS_COLINFO
#define CS_FETCHCOLINFO_ENCODING_LENGTH()  (sizeof(CS_TWO) + sizeof(CS_FOUR)*3)
#define CS_COLINFO_ENCODING_LENGTH(dataLength) (sizeof(CS_TWO) + sizeof(CS_ONE) + sizeof(CS_FOUR)*3 + dataLength)
#endif
#define CS_MLGF_MAXNUM_KEYS 32
#define CS_MAXFILTERNAME 256
#define CS_MAXFILTERFILEPATH 512
#define CS_MAXFILTERFUNCTIONNAME 256
#define CS_MAXKEYWORDEXTRACTORNAME 256
#define CS_MAXKEYWORDEXTRACTORFILEPATH 512
#define CS_MAXKEYWORDEXTRACTORFUNCTIONNAME 256
#define CS_MAXNUMOFVOLS 10
#define CS_LOM_INDEXTYPE_BTREE 1
#define CS_LOM_INDEXTYPE_MLGF 2
#define CS_LOM_INDEXTYPE_TEXT 5

typedef u_char odys_data_t;
bool_t xdr_odys_data_t();


typedef char *className_t;
bool_t xdr_className_t();


typedef char *attrName_t;
bool_t xdr_attrName_t();


typedef char *indexName_t;
bool_t xdr_indexName_t();


typedef char *databaseName_t;
bool_t xdr_databaseName_t();


typedef char *volumeName_t;
bool_t xdr_volumeName_t();


typedef struct {
	u_int devNameList_t_len;
	odys_data_t *devNameList_t_val;
} devNameList_t;
bool_t xdr_devNameList_t();


typedef char *queryString_t;
bool_t xdr_queryString_t();


typedef char *relationshipName_t;
bool_t xdr_relationshipName_t();


typedef char *objectName_t;
bool_t xdr_objectName_t();


typedef char *filterName_t;
bool_t xdr_filterName_t();


typedef char *filterFilePath_t;
bool_t xdr_filterFilePath_t();


typedef char *filterFunctionName_t;
bool_t xdr_filterFunctionName_t();


typedef char *keywordExtractorName_t;
bool_t xdr_keywordExtractorName_t();


typedef char *keywordExtractorFilePath_t;
bool_t xdr_keywordExtractorFilePath_t();


typedef char *keywordExtractorFunctionName_t;
bool_t xdr_keywordExtractorFunctionName_t();


enum Server_LockMode {
	CS_ODYS_L_NL = 0,
	CS_ODYS_L_IS = 1,
	CS_ODYS_L_IX = 2,
	CS_ODYS_L_S = 3,
	CS_ODYS_L_SIX = 4,
	CS_ODYS_L_X = 5,
};
typedef enum Server_LockMode Server_LockMode;
bool_t xdr_Server_LockMode();


enum Server_LockDuration {
	CS_ODYS_L_INSTANT = 0,
	CS_ODYS_L_MANUAL = 1,
	CS_ODYS_L_COMMIT = 2,
};
typedef enum Server_LockDuration Server_LockDuration;
bool_t xdr_Server_LockDuration();


struct Server_LockParameter {
	Server_LockMode mode;
	Server_LockDuration duration;
};
typedef struct Server_LockParameter Server_LockParameter;
bool_t xdr_Server_LockParameter();


struct Server_KeyValue {
	CS_TWO len;
	char val[CS_MAXKEYLEN];
};
typedef struct Server_KeyValue Server_KeyValue;
bool_t xdr_Server_KeyValue();


enum Server_CompOp {
	CS_ODYS_EQ = 0,
	CS_ODYS_GT = 1,
	CS_ODYS_GE = 2,
	CS_ODYS_LT = 3,
	CS_ODYS_LE = 4,
	CS_ODYS_EOF = 5,
	CS_ODYS_BOF = 6,
};
typedef enum Server_CompOp Server_CompOp;
bool_t xdr_Server_CompOp();


struct Server_BoundCond {
	Server_KeyValue key;
	Server_CompOp op;
};
typedef struct Server_BoundCond Server_BoundCond;
bool_t xdr_Server_BoundCond();


struct Server_KeyColInfo {
	CS_FOUR colNo;
	CS_FOUR flag;
};
typedef struct Server_KeyColInfo Server_KeyColInfo;
bool_t xdr_Server_KeyColInfo();


struct Server_KeyInfo {
	CS_TWO flag;
	CS_TWO nColumns;
	Server_KeyColInfo columns[CS_MAXNUMKEYPARTS];
};
typedef struct Server_KeyInfo Server_KeyInfo;
bool_t xdr_Server_KeyInfo();


struct Server_MLGF_KeyInfo {
	CS_TWO flag;
	CS_TWO nColumns;
	CS_TWO colNo[CS_MLGF_MAXNUM_KEYS];
	CS_TWO extraDataLen;
};
typedef struct Server_MLGF_KeyInfo Server_MLGF_KeyInfo;
bool_t xdr_Server_MLGF_KeyInfo();


enum INDEXTYPE {
	CS_BTREE = 0,
	CS_MLGF = 1,
	CS_TEXT = 2,
};
typedef enum INDEXTYPE INDEXTYPE;
bool_t xdr_INDEXTYPE();


struct Server_IndexDesc {
	INDEXTYPE indexType;
	union {
		Server_KeyInfo btree;
		Server_MLGF_KeyInfo mlgf;
	} Server_IndexDesc_u;
};
typedef struct Server_IndexDesc Server_IndexDesc;
bool_t xdr_Server_IndexDesc();


struct SERVER_MBR {
	CS_FOUR values[4];
};
typedef struct SERVER_MBR SERVER_MBR;
bool_t xdr_SERVER_MBR();


struct CS_OID {
	CS_FOUR pageNo;
	CS_TWO volNo;
	CS_TWO slotNo;
	CS_UFOUR unique;
	CS_FOUR classID;
};
typedef struct CS_OID CS_OID;
bool_t xdr_CS_OID();


struct CS_TupleID {
	CS_FOUR pageNo;
	CS_TWO volNo;
	CS_TWO slotNo;
	CS_UFOUR unique;
};
typedef struct CS_TupleID CS_TupleID;
bool_t xdr_CS_TupleID();


struct CS_PageID {
	CS_FOUR pageNo;
	CS_TWO volNo;
};
typedef struct CS_PageID CS_PageID;
bool_t xdr_CS_PageID();


typedef CS_PageID CS_FileID;
bool_t xdr_CS_FileID();


typedef CS_PageID CS_IndexID;
bool_t xdr_CS_IndexID();


struct Server_IndexID {
	CS_FOUR isLogical;
	CS_TupleID iid;
};
typedef struct Server_IndexID Server_IndexID;
bool_t xdr_Server_IndexID();


struct Server_TextDesc {
	char isIndexed;
	char hasBeenIndexed;
	CS_TupleID contentTid;
	CS_FOUR size;
};
typedef struct Server_TextDesc Server_TextDesc;
bool_t xdr_Server_TextDesc();


struct Server_TextColStruct {
	CS_FOUR start;
	CS_FOUR length;
	CS_FOUR dataLength;
	CS_FOUR indexMode;
};
typedef struct Server_TextColStruct Server_TextColStruct;
bool_t xdr_Server_TextColStruct();


struct CS_MBR {
	CS_FOUR values[CS_MBR_NUM_PARTS];
};
typedef struct CS_MBR CS_MBR;
bool_t xdr_CS_MBR();


enum COSMOS_TYPE {
	CS_SHORT_TYPE = 0,
	CS_USHORT_TYPE = 0,
	CS_INT_TYPE = 1,
	CS_UINT_TYPE = 1,
	CS_LONG_TYPE = 2,
	CS_LONG_LONG_TYPE = 14, 
	CS_ULONG_TYPE = 2,
	CS_FLOAT_TYPE = 3,
	CS_DOUBLE_TYPE = 4,
	CS_STRING_TYPE = 5,
	CS_VARSTRING_TYPE = 6,
	CS_PAGEID_TYPE = 7,
	CS_FILEID_TYPE = 8,
	CS_INDEXID_TYPE = 9,
	CS_OID_TYPE = 10,
	CS_LRDSTEXT_TYPE = 11,
	CS_MBR_TYPE = 12,
	CS_REF_TYPE = 30,
	CS_LINK_TYPE = 31,
	CS_TEXT_TYPE = 39,
	CS_CHARSTRING_TYPE = 41,
};
typedef enum COSMOS_TYPE COSMOS_TYPE;
bool_t xdr_COSMOS_TYPE();


struct boolData_t {
	COSMOS_TYPE type;
	union {
		CS_TWO s;
		int i;
		CS_FOUR l;
		CS_Eight ll;
		float f;
		double d;
		CS_PageID pid;
		CS_OID oid;
		CS_MBR mbr;
		char str[CS_MAXFIELD];
	} boolData_t_u;
};
typedef struct boolData_t boolData_t;
bool_t xdr_boolData_t();


struct Server_BoolExp {
	CS_TWO op;
	CS_TWO colNo;
	CS_TWO length;
	boolData_t data;
};
typedef struct Server_BoolExp Server_BoolExp;
bool_t xdr_Server_BoolExp();


struct Server_AttrInfo {
	CS_TWO complexType;
	CS_TWO type;
	CS_FOUR length;
	char name[CS_MAXATTRNAME];
	CS_FOUR inheritedFrom;
	char domain[CS_MAXCLASSNAME];
	CS_FOUR offset;
};
typedef struct Server_AttrInfo Server_AttrInfo;
bool_t xdr_Server_AttrInfo();


struct Server_MethodInfo {
	char dirPath[CS_MAXDIRPATH];
	char functionName[CS_MAXFUNCTIONNAME];
	CS_TWO nArguments;
	CS_FOUR argumentsList[CS_MAXNARGUMNET];
	CS_FOUR inheritedFrom;
	CS_FOUR returnType;
};
typedef struct Server_MethodInfo Server_MethodInfo;
bool_t xdr_Server_MethodInfo();


struct Server_TransID {
	u_long high;
	u_long low;
};
typedef struct Server_TransID Server_TransID;
bool_t xdr_Server_TransID();


struct Catalog_SuperClassInfo {
	CS_FOUR superClassId;
};
typedef struct Catalog_SuperClassInfo Catalog_SuperClassInfo;
bool_t xdr_Catalog_SuperClassInfo();


struct Catalog_SubClassInfo {
	CS_FOUR subClassId;
};
typedef struct Catalog_SubClassInfo Catalog_SubClassInfo;
bool_t xdr_Catalog_SubClassInfo();


struct Server_InvertedIndexDesc {
	CS_IndexID keywordIndex;
	CS_IndexID reverseKeywordIndex;
	CS_IndexID docIdIndex;
	CS_IndexID postingIndex;
	char invertedIndexName[CS_MAXINDEXNAME];
	char postingTableName[CS_MAXINDEXNAME];
};
typedef struct Server_InvertedIndexDesc Server_InvertedIndexDesc;
bool_t xdr_Server_InvertedIndexDesc();


struct Server_KeyPart {
	short type;
	short offset;
	short length;
};
typedef struct Server_KeyPart Server_KeyPart;
bool_t xdr_Server_KeyPart();


struct Server_KeyDesc {
	short flag;
	short nparts;
	Server_KeyPart kpart[CS_MAXNUMKEYPARTS];
};
typedef struct Server_KeyDesc Server_KeyDesc;
bool_t xdr_Server_KeyDesc();


struct Server_MLGF_KeyDesc {
	char flag;
	char nKeys;
	short extraDataLen;
	u_long minMaxTypeVector;
};
typedef struct Server_MLGF_KeyDesc Server_MLGF_KeyDesc;
bool_t xdr_Server_MLGF_KeyDesc();


struct Catalog_IndexInfo {
	Server_IndexID iid;
	Server_KeyDesc btree;
	Server_MLGF_KeyDesc mlgf;
	Server_InvertedIndexDesc invertedIndex;
	short colNo[CS_MAXNUMKEYPARTS];
	char indexType;
	char name[CS_MAXINDEXNAME];
};
typedef struct Catalog_IndexInfo Catalog_IndexInfo;
bool_t xdr_Catalog_IndexInfo();


struct Catalog_AttributeInfo {
	short colNo;
	short complexType;
	short type;
	short varColNo;
	long offset;
	long length;
	long inheritedFrom;
	long domain;
	char name[CS_MAXATTRNAME];
};
typedef struct Catalog_AttributeInfo Catalog_AttributeInfo;
bool_t xdr_Catalog_AttributeInfo();


struct Catalog_MethodInfo {
	char dirPath[CS_MAXDIRPATH];
	char name[CS_MAXMETHODNAME];
	char functionName[CS_MAXFUNCTIONNAME];
	short nArguments;
	long argumentsList[CS_MAXNARGUMENT];
	long inheritedFrom;
	long returnType;
};
typedef struct Catalog_MethodInfo Catalog_MethodInfo;
bool_t xdr_Catalog_MethodInfo();


struct Catalog_RelationshipInfo {
	long fromClassId;
	long toClassId;
	long relationshipID;
	short fromAttrNum;
	short toAttrNum;
	char direction;
	char cardinality;
	char relationshipName[CS_MAXRELATIONSHIPNAME];
};
typedef struct Catalog_RelationshipInfo Catalog_RelationshipInfo;
bool_t xdr_Catalog_RelationshipInfo();


struct catalog_getclassinfo_reply {
	short nIndexes;
	short nCols;
	short nVarCols;
	short nSuperclasses;
	short nSubclasses;
	short nMethods;
	short nRelationships;
	long objectSize;
	long classId;
	long ocn;
	long osn;
	long numOfOpen;
	char name[CS_MAXCLASSNAME];
	struct {
		u_int superClassInfo_len;
		Catalog_SuperClassInfo *superClassInfo_val;
	} superClassInfo;
	struct {
		u_int subClassInfo_len;
		Catalog_SubClassInfo *subClassInfo_val;
	} subClassInfo;
	struct {
		u_int attrInfo_len;
		Catalog_AttributeInfo *attrInfo_val;
	} attrInfo;
	struct {
		u_int methodInfo_len;
		Catalog_MethodInfo *methodInfo_val;
	} methodInfo;
	struct {
		u_int indexInfo_len;
		Catalog_IndexInfo *indexInfo_val;
	} indexInfo;
	struct {
		u_int relationshipInfo_len;
		Catalog_RelationshipInfo *relationshipInfo_val;
	} relationshipInfo;
	CS_FOUR errCode;
};
typedef struct catalog_getclassinfo_reply catalog_getclassinfo_reply;
bool_t xdr_catalog_getclassinfo_reply();


struct catalog_getclassinfo_arg {
	long threadId;
	CS_FOUR volId;
	CS_FOUR classId;
};
typedef struct catalog_getclassinfo_arg catalog_getclassinfo_arg;
bool_t xdr_catalog_getclassinfo_arg();


typedef long oosql_handle_t;
bool_t xdr_oosql_handle_t();


typedef char *oosql_queryString_t;
bool_t xdr_oosql_queryString_t();


enum oosql_sortBufferMode_t {
	CS_OOSQL_SB_USE_DISK = 0,
	CS_OOSQL_SB_USE_MEMORY = 1,
	CS_OOSQL_SB_USE_MEMORY_WITH_DISK = 2,
};
typedef enum oosql_sortBufferMode_t oosql_sortBufferMode_t;
bool_t xdr_oosql_sortBufferMode_t();


struct oosql_diskSortBufferInfo_t {
	long sortVolID;
};
typedef struct oosql_diskSortBufferInfo_t oosql_diskSortBufferInfo_t;
bool_t xdr_oosql_diskSortBufferInfo_t();


struct oosql_MemorySortBuffer_t {
	struct {
		u_int sortBufferPtr_len;
		char *sortBufferPtr_val;
	} sortBufferPtr;
	long sortBufferLength;
	long sortBufferUsedLength;
};
typedef struct oosql_MemorySortBuffer_t oosql_MemorySortBuffer_t;
bool_t xdr_oosql_MemorySortBuffer_t();


struct oosql_sortBufferInfo_t {
	oosql_sortBufferMode_t mode;
	oosql_diskSortBufferInfo_t diskInfo;
	oosql_MemorySortBuffer_t memoryInfo;
};
typedef struct oosql_sortBufferInfo_t oosql_sortBufferInfo_t;
bool_t xdr_oosql_sortBufferInfo_t();


struct lom_createthread_reply {
	long threadId;
	CS_FOUR procId;
	CS_FOUR errCode;
};
typedef struct lom_createthread_reply lom_createthread_reply;
bool_t xdr_lom_createthread_reply();


struct lom_createthread_arg {
	CS_FOUR padding;
};
typedef struct lom_createthread_arg lom_createthread_arg;
bool_t xdr_lom_createthread_arg();


struct lom_destroythread_reply {
	CS_FOUR errCode;
};
typedef struct lom_destroythread_reply lom_destroythread_reply;
bool_t xdr_lom_destroythread_reply();


struct lom_destroythread_arg {
	long threadId;
	CS_FOUR procId;
};
typedef struct lom_destroythread_arg lom_destroythread_arg;
bool_t xdr_lom_destroythread_arg();


struct lom_mount_reply {
	CS_FOUR volId;
	CS_FOUR errCode;
	CS_FOUR databaseId;
};
typedef struct lom_mount_reply lom_mount_reply;
bool_t xdr_lom_mount_reply();


struct lom_mount_arg {
	long threadId;
	volumeName_t volName;
	CS_FOUR numOfDevices;
	devNameList_t devNameList;
};
typedef struct lom_mount_arg lom_mount_arg;
bool_t xdr_lom_mount_arg();


struct lom_dismount_reply {
	CS_FOUR errCode;
};
typedef struct lom_dismount_reply lom_dismount_reply;
bool_t xdr_lom_dismount_reply();


struct lom_dismount_arg {
	long threadId;
	CS_FOUR databaseId;
};
typedef struct lom_dismount_arg lom_dismount_arg;
bool_t xdr_lom_dismount_arg();


struct lom_begintrans_reply {
	CS_FOUR errCode;
	Server_TransID transId;
};
typedef struct lom_begintrans_reply lom_begintrans_reply;
bool_t xdr_lom_begintrans_reply();


struct lom_begintrans_arg {
	long threadId;
	CS_FOUR rollbackFlag;
};
typedef struct lom_begintrans_arg lom_begintrans_arg;
bool_t xdr_lom_begintrans_arg();


struct lom_committrans_reply {
	CS_FOUR errCode;
};
typedef struct lom_committrans_reply lom_committrans_reply;
bool_t xdr_lom_committrans_reply();


struct lom_committrans_arg {
	long threadId;
	Server_TransID transId;
};
typedef struct lom_committrans_arg lom_committrans_arg;
bool_t xdr_lom_committrans_arg();


struct lom_aborttrans_reply {
	CS_FOUR errCode;
};
typedef struct lom_aborttrans_reply lom_aborttrans_reply;
bool_t xdr_lom_aborttrans_reply();


struct lom_aborttrans_arg {
	long threadId;
	Server_TransID transId;
};
typedef struct lom_aborttrans_arg lom_aborttrans_arg;
bool_t xdr_lom_aborttrans_arg();


struct lom_addindex_reply {
	Server_IndexID indexId;
	CS_FOUR errCode;
};
typedef struct lom_addindex_reply lom_addindex_reply;
bool_t xdr_lom_addindex_reply();


struct lom_addindex_arg {
	long threadId;
	CS_FOUR databaseId;
	className_t className;
	indexName_t indexName;
	Server_IndexDesc indexDesc;
};
typedef struct lom_addindex_arg lom_addindex_arg;
bool_t xdr_lom_addindex_arg();


struct lom_dropindex_reply {
	CS_FOUR errCode;
};
typedef struct lom_dropindex_reply lom_dropindex_reply;
bool_t xdr_lom_dropindex_reply();


struct lom_dropindex_arg {
	long threadId;
	CS_FOUR databaseId;
	indexName_t indexName;
};
typedef struct lom_dropindex_arg lom_dropindex_arg;
bool_t xdr_lom_dropindex_arg();


struct lom_openclass_reply {
	CS_FOUR errCode;
	CS_FOUR classID;
};
typedef struct lom_openclass_reply lom_openclass_reply;
bool_t xdr_lom_openclass_reply();


struct lom_openclass_arg {
	long threadId;
	CS_FOUR databaseId;
	className_t className;
};
typedef struct lom_openclass_arg lom_openclass_arg;
bool_t xdr_lom_openclass_arg();


struct lom_closeclass_reply {
	CS_FOUR errCode;
};
typedef struct lom_closeclass_reply lom_closeclass_reply;
bool_t xdr_lom_closeclass_reply();


struct lom_closeclass_arg {
	long threadId;
	CS_FOUR ocn;
};
typedef struct lom_closeclass_arg lom_closeclass_arg;
bool_t xdr_lom_closeclass_arg();


struct lom_openseqscan_reply {
	CS_FOUR errCode;
};
typedef struct lom_openseqscan_reply lom_openseqscan_reply;
bool_t xdr_lom_openseqscan_reply();


struct lom_openseqscan_arg {
	long threadId;
	CS_FOUR ocn;
	CS_FOUR scanDirection;
	Server_LockParameter *lockup;
	struct {
		u_int boolexp_len;
		Server_BoolExp *boolexp_val;
	} boolexp;
};
typedef struct lom_openseqscan_arg lom_openseqscan_arg;
bool_t xdr_lom_openseqscan_arg();


struct lom_closescan_reply {
	CS_FOUR errCode;
};
typedef struct lom_closescan_reply lom_closescan_reply;
bool_t xdr_lom_closescan_reply();


struct lom_closescan_arg {
	long threadId;
	CS_FOUR scanId;
};
typedef struct lom_closescan_arg lom_closescan_arg;
bool_t xdr_lom_closescan_arg();


struct lom_openindexscan_reply {
	CS_FOUR errCode;
};
typedef struct lom_openindexscan_reply lom_openindexscan_reply;
bool_t xdr_lom_openindexscan_reply();


struct lom_openindexscan_arg {
	long threadId;
	CS_FOUR ocn;
	Server_IndexID iid;
	Server_BoundCond *startBound;
	Server_BoundCond *stopBound;
	struct {
		u_int boolexp_len;
		Server_BoolExp *boolexp_val;
	} boolexp;
	Server_LockParameter *lockup;
};
typedef struct lom_openindexscan_arg lom_openindexscan_arg;
bool_t xdr_lom_openindexscan_arg();


struct lom_createclass_reply {
	CS_FOUR errCode;
};
typedef struct lom_createclass_reply lom_createclass_reply;
bool_t xdr_lom_createclass_reply();


struct lom_createclass_arg {
	long threadId;
	CS_FOUR databaseId;
	className_t className;
	indexName_t indexName;
	Server_IndexDesc *idesc;
	struct {
		u_int ainfo_len;
		Server_AttrInfo *ainfo_val;
	} ainfo;
	struct {
		u_int superClassList_len;
		char *superClassList_val;
	} superClassList;
	struct {
		u_int minfo_len;
		Server_MethodInfo *minfo_val;
	} minfo;
	CS_FOUR tmpClassFlag;
	CS_FOUR classId;
};
typedef struct lom_createclass_arg lom_createclass_arg;
bool_t xdr_lom_createclass_arg();


struct lom_destroyclass_reply {
	CS_FOUR errCode;
};
typedef struct lom_destroyclass_reply lom_destroyclass_reply;
bool_t xdr_lom_destroyclass_reply();


struct lom_destroyclass_arg {
	long threadId;
	CS_FOUR databaseId;
	className_t className;
};
typedef struct lom_destroyclass_arg lom_destroyclass_arg;
bool_t xdr_lom_destroyclass_arg();


struct lom_getclassid_reply {
	CS_FOUR classId;
	CS_FOUR errCode;
};
typedef struct lom_getclassid_reply lom_getclassid_reply;
bool_t xdr_lom_getclassid_reply();


struct lom_getclassid_arg {
	long threadId;
	CS_FOUR databaseId;
	className_t className;
};
typedef struct lom_getclassid_arg lom_getclassid_arg;
bool_t xdr_lom_getclassid_arg();


struct lom_getnewclassid_reply {
	CS_FOUR classId;
	CS_FOUR errCode;
};
typedef struct lom_getnewclassid_reply lom_getnewclassid_reply;
bool_t xdr_lom_getnewclassid_reply();


struct lom_getnewclassid_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR tmpClassFlag;
};
typedef struct lom_getnewclassid_arg lom_getnewclassid_arg;
bool_t xdr_lom_getnewclassid_arg();


struct lom_getclassname_reply {
	char className[CS_MAXCLASSNAME];
	CS_FOUR errCode;
};
typedef struct lom_getclassname_reply lom_getclassname_reply;
bool_t xdr_lom_getclassname_reply();


struct lom_getclassname_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
};
typedef struct lom_getclassname_arg lom_getclassname_arg;
bool_t xdr_lom_getclassname_arg();


struct lom_getsubclasses_reply {
	struct {
		u_int subClasses_len;
		long *subClasses_val;
	} subClasses;
	CS_FOUR errCode;
};
typedef struct lom_getsubclasses_reply lom_getsubclasses_reply;
bool_t xdr_lom_getsubclasses_reply();


struct lom_getsubclasses_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
	CS_FOUR fromNthSubClass;
	CS_FOUR sizeOfSubClasses;
};
typedef struct lom_getsubclasses_arg lom_getsubclasses_arg;
bool_t xdr_lom_getsubclasses_arg();


struct lom_createobject_reply {
	CS_OID oid;
	CS_FOUR errCode;
};
typedef struct lom_createobject_reply lom_createobject_reply;
bool_t xdr_lom_createobject_reply();


struct lom_createobject_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_FOUR nCols;
	struct {
		u_int encodedObject_len;
		char *encodedObject_val;
	} encodedObject;
};
typedef struct lom_createobject_arg lom_createobject_arg;
bool_t xdr_lom_createobject_arg();


struct lom_createlargeobject_reply {
	CS_OID oid;
	CS_FOUR errCode;
};
typedef struct lom_createlargeobject_reply lom_createlargeobject_reply;
bool_t xdr_lom_createlargeobject_reply();


struct lom_createlargeobject_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	struct {
		u_int encodedObject_len;
		char *encodedObject_val;
	} encodedObject;
};
typedef struct lom_createlargeobject_arg lom_createlargeobject_arg;
bool_t xdr_lom_createlargeobject_arg();


struct lom_destroyobject_reply {
	CS_FOUR errCode;
};
typedef struct lom_destroyobject_reply lom_destroyobject_reply;
bool_t xdr_lom_destroyobject_reply();


struct lom_destroyobject_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
};
typedef struct lom_destroyobject_arg lom_destroyobject_arg;
bool_t xdr_lom_destroyobject_arg();


struct lom_updateobject_reply {
	CS_FOUR errCode;
};
typedef struct lom_updateobject_reply lom_updateobject_reply;
bool_t xdr_lom_updateobject_reply();


struct lom_updateobject_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_FOUR nCols;
	struct {
		u_int encodedObject_len;
		char *encodedObject_val;
	} encodedObject;
};
typedef struct lom_updateobject_arg lom_updateobject_arg;
bool_t xdr_lom_updateobject_arg();


struct lom_updatelargeobject_reply {
	CS_FOUR errCode;
};
typedef struct lom_updatelargeobject_reply lom_updatelargeobject_reply;
bool_t xdr_lom_updatelargeobject_reply();


struct lom_updatelargeobject_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	struct {
		u_int encodedObject_len;
		char *encodedObject_val;
	} encodedObject;
};
typedef struct lom_updatelargeobject_arg lom_updatelargeobject_arg;
bool_t xdr_lom_updatelargeobject_arg();


struct lom_fetchobject_reply {
	struct {
		u_int encodedObject_len;
		char *encodedObject_val;
	} encodedObject;
	CS_FOUR errCode;
};
typedef struct lom_fetchobject_reply lom_fetchobject_reply;
bool_t xdr_lom_fetchobject_reply();


struct lom_fetchobject_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_FOUR nCols;
	struct {
		u_int fetchColInfo_len;
		char *fetchColInfo_val;
	} fetchColInfo;
};
typedef struct lom_fetchobject_arg lom_fetchobject_arg;
bool_t xdr_lom_fetchobject_arg();


struct lom_fetchobject2_reply {
	struct {
		u_int encodedObject_len;
		char *encodedObject_val;
	} encodedObject;
	CS_FOUR errCode;
};
typedef struct lom_fetchobject2_reply lom_fetchobject2_reply;
bool_t xdr_lom_fetchobject2_reply();


struct lom_fetchobject2_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_FOUR nCols;
	struct {
		u_int fetchColInfo_len;
		char *fetchColInfo_val;
	} fetchColInfo;
};
typedef struct lom_fetchobject2_arg lom_fetchobject2_arg;
bool_t xdr_lom_fetchobject2_arg();


struct lom_fetchlargeobject_reply {
	struct {
		u_int encodedObject_len;
		char *encodedObject_val;
	} encodedObject;
	CS_FOUR retLength;
	CS_FOUR errCode;
};
typedef struct lom_fetchlargeobject_reply lom_fetchlargeobject_reply;
bool_t xdr_lom_fetchlargeobject_reply();


struct lom_fetchlargeobject_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	struct {
		u_int fetchColInfo_len;
		char *fetchColInfo_val;
	} fetchColInfo;
};
typedef struct lom_fetchlargeobject_arg lom_fetchlargeobject_arg;
bool_t xdr_lom_fetchlargeobject_arg();


struct lom_nextobject_reply {
	CS_OID oid;
	CS_FOUR errCode;
};
typedef struct lom_nextobject_reply lom_nextobject_reply;
bool_t xdr_lom_nextobject_reply();


struct lom_nextobject_arg {
	long threadId;
	CS_FOUR scanId;
};
typedef struct lom_nextobject_arg lom_nextobject_arg;
bool_t xdr_lom_nextobject_arg();


struct oosql_createsystemhandle_reply {
	CS_FOUR errCode;
	CS_FOUR procId;
	long threadId;
};
typedef struct oosql_createsystemhandle_reply oosql_createsystemhandle_reply;
bool_t xdr_oosql_createsystemhandle_reply();


struct oosql_createsystemhandle_arg {
	CS_FOUR padding;
};
typedef struct oosql_createsystemhandle_arg oosql_createsystemhandle_arg;
bool_t xdr_oosql_createsystemhandle_arg();


struct oosql_destroysystemhandle_reply {
	CS_FOUR errCode;
};
typedef struct oosql_destroysystemhandle_reply oosql_destroysystemhandle_reply;
bool_t xdr_oosql_destroysystemhandle_reply();


struct oosql_destroysystemhandle_arg {
	long threadId;
	CS_FOUR procId;
};
typedef struct oosql_destroysystemhandle_arg oosql_destroysystemhandle_arg;
bool_t xdr_oosql_destroysystemhandle_arg();


struct oosql_allochandle_reply {
	CS_FOUR errCode;
	oosql_handle_t handle;
};
typedef struct oosql_allochandle_reply oosql_allochandle_reply;
bool_t xdr_oosql_allochandle_reply();


struct oosql_allochandle_arg {
	long threadId;
	CS_FOUR volumeId;
};
typedef struct oosql_allochandle_arg oosql_allochandle_arg;
bool_t xdr_oosql_allochandle_arg();


struct oosql_freehandle_reply {
	CS_FOUR errCode;
};
typedef struct oosql_freehandle_reply oosql_freehandle_reply;
bool_t xdr_oosql_freehandle_reply();


struct oosql_freehandle_arg {
	long threadId;
	oosql_handle_t handle;
};
typedef struct oosql_freehandle_arg oosql_freehandle_arg;
bool_t xdr_oosql_freehandle_arg();


struct oosql_mount_reply {
	CS_FOUR errCode;
	CS_FOUR volumeId;
};
typedef struct oosql_mount_reply oosql_mount_reply;
bool_t xdr_oosql_mount_reply();


struct oosql_mount_arg {
	long threadId;
	volumeName_t volumeName;
};
typedef struct oosql_mount_arg oosql_mount_arg;
bool_t xdr_oosql_mount_arg();


struct oosql_dismount_reply {
	CS_FOUR errCode;
};
typedef struct oosql_dismount_reply oosql_dismount_reply;
bool_t xdr_oosql_dismount_reply();


struct oosql_dismount_arg {
	long threadId;
	CS_FOUR volumeId;
};
typedef struct oosql_dismount_arg oosql_dismount_arg;
bool_t xdr_oosql_dismount_arg();


struct oosql_setuserdefaultvolumeid_reply {
	CS_FOUR errCode;
};
typedef struct oosql_setuserdefaultvolumeid_reply oosql_setuserdefaultvolumeid_reply;
bool_t xdr_oosql_setuserdefaultvolumeid_reply();


struct oosql_setuserdefaultvolumeid_arg {
	CS_FOUR threadId;
	CS_FOUR databaseId;
	CS_FOUR volumeId;
};
typedef struct oosql_setuserdefaultvolumeid_arg oosql_setuserdefaultvolumeid_arg;
bool_t xdr_oosql_setuserdefaultvolumeid_arg();


struct oosql_getuserdefaultvolumeid_reply {
	CS_FOUR errCode;
	CS_FOUR volId;
};
typedef struct oosql_getuserdefaultvolumeid_reply oosql_getuserdefaultvolumeid_reply;
bool_t xdr_oosql_getuserdefaultvolumeid_reply();


struct oosql_getuserdefaultvolumeid_arg {
	CS_FOUR threadId;
	CS_FOUR databaseId;
};
typedef struct oosql_getuserdefaultvolumeid_arg oosql_getuserdefaultvolumeid_arg;
bool_t xdr_oosql_getuserdefaultvolumeid_arg();


struct oosql_getvolumeid_reply {
	CS_FOUR errCode;
	CS_FOUR volId;
};
typedef struct oosql_getvolumeid_reply oosql_getvolumeid_reply;
bool_t xdr_oosql_getvolumeid_reply();


struct oosql_getvolumeid_arg {
	CS_FOUR threadId;
	CS_FOUR databaseId;
	volumeName_t volumeName;
};
typedef struct oosql_getvolumeid_arg oosql_getvolumeid_arg;
bool_t xdr_oosql_getvolumeid_arg();


struct oosql_mountdb_reply {
	CS_FOUR errCode;
	CS_FOUR databaseId;
	CS_FOUR volumeIdList[CS_MAXNUMOFVOLS];
};
typedef struct oosql_mountdb_reply oosql_mountdb_reply;
bool_t xdr_oosql_mountdb_reply();


struct oosql_mountdb_arg {
	long threadId;
	databaseName_t databaseName;
};
typedef struct oosql_mountdb_arg oosql_mountdb_arg;
bool_t xdr_oosql_mountdb_arg();


struct oosql_dismountdb_reply {
	CS_FOUR errCode;
};
typedef struct oosql_dismountdb_reply oosql_dismountdb_reply;
bool_t xdr_oosql_dismountdb_reply();


struct oosql_dismountdb_arg {
	long threadId;
	CS_FOUR databaseId;
};
typedef struct oosql_dismountdb_arg oosql_dismountdb_arg;
bool_t xdr_oosql_dismountdb_arg();


struct oosql_transbegin_reply {
	CS_FOUR errCode;
	Server_TransID transId;
};
typedef struct oosql_transbegin_reply oosql_transbegin_reply;
bool_t xdr_oosql_transbegin_reply();


struct oosql_transbegin_arg {
	long threadId;
	CS_FOUR rollbackFlag;
};
typedef struct oosql_transbegin_arg oosql_transbegin_arg;
bool_t xdr_oosql_transbegin_arg();


struct oosql_transcommit_reply {
	CS_FOUR errCode;
};
typedef struct oosql_transcommit_reply oosql_transcommit_reply;
bool_t xdr_oosql_transcommit_reply();


struct oosql_transcommit_arg {
	long threadId;
	Server_TransID transId;
};
typedef struct oosql_transcommit_arg oosql_transcommit_arg;
bool_t xdr_oosql_transcommit_arg();


struct oosql_transabort_reply {
	CS_FOUR errCode;
};
typedef struct oosql_transabort_reply oosql_transabort_reply;
bool_t xdr_oosql_transabort_reply();


struct oosql_transabort_arg {
	long threadId;
	Server_TransID transId;
};
typedef struct oosql_transabort_arg oosql_transabort_arg;
bool_t xdr_oosql_transabort_arg();


struct oosql_prepare_reply {
	CS_FOUR errCode;
};
typedef struct oosql_prepare_reply oosql_prepare_reply;
bool_t xdr_oosql_prepare_reply();


struct oosql_prepare_arg {
	long threadId;
	oosql_handle_t handle;
	oosql_queryString_t stmtText;
	oosql_sortBufferInfo_t sortBufferInfo;
};
typedef struct oosql_prepare_arg oosql_prepare_arg;
bool_t xdr_oosql_prepare_arg();


struct oosql_execute_reply {
	CS_FOUR errCode;
};
typedef struct oosql_execute_reply oosql_execute_reply;
bool_t xdr_oosql_execute_reply();


struct oosql_execute_arg {
	long threadId;
	oosql_handle_t handle;
};
typedef struct oosql_execute_arg oosql_execute_arg;
bool_t xdr_oosql_execute_arg();


struct oosql_execdirect_reply {
	CS_FOUR errCode;
};
typedef struct oosql_execdirect_reply oosql_execdirect_reply;
bool_t xdr_oosql_execdirect_reply();


struct oosql_execdirect_arg {
	long threadId;
	oosql_handle_t handle;
	oosql_queryString_t stmtText;
	oosql_sortBufferInfo_t sortBufferInfo;
};
typedef struct oosql_execdirect_arg oosql_execdirect_arg;
bool_t xdr_oosql_execdirect_arg();


struct oosql_next_reply {
	CS_FOUR errCode;
};
typedef struct oosql_next_reply oosql_next_reply;
bool_t xdr_oosql_next_reply();


struct oosql_next_arg {
	long threadId;
	oosql_handle_t handle;
};
typedef struct oosql_next_arg oosql_next_arg;
bool_t xdr_oosql_next_arg();


struct oosql_getdata_reply {
	CS_FOUR errCode;
	struct {
		u_int columnValuePtr_len;
		char *columnValuePtr_val;
	} columnValuePtr;
	CS_FOUR dataLength;
};
typedef struct oosql_getdata_reply oosql_getdata_reply;
bool_t xdr_oosql_getdata_reply();


struct oosql_getdata_arg {
	long threadId;
	oosql_handle_t handle;
	CS_TWO columnNumber;
	CS_FOUR startPos;
	CS_FOUR bufferLength;
};
typedef struct oosql_getdata_arg oosql_getdata_arg;
bool_t xdr_oosql_getdata_arg();


struct oosql_putdata_reply {
	CS_FOUR errCode;
};
typedef struct oosql_putdata_reply oosql_putdata_reply;
bool_t xdr_oosql_putdata_reply();


struct oosql_putdata_arg {
	long threadId;
	oosql_handle_t handle;
	CS_TWO columnNumber;
	CS_FOUR startPos;
	struct {
		u_int columnValuePtr_len;
		char *columnValuePtr_val;
	} columnValuePtr;
	CS_FOUR bufferLength;
};
typedef struct oosql_putdata_arg oosql_putdata_arg;
bool_t xdr_oosql_putdata_arg();


struct oosql_getoid_reply {
	CS_FOUR errCode;
	CS_OID oid;
};
typedef struct oosql_getoid_reply oosql_getoid_reply;
bool_t xdr_oosql_getoid_reply();


struct oosql_getoid_arg {
	long threadId;
	oosql_handle_t handle;
	CS_TWO targetNumber;
};
typedef struct oosql_getoid_arg oosql_getoid_arg;
bool_t xdr_oosql_getoid_arg();


struct oosql_getnumresultcols_reply {
	CS_FOUR errCode;
	CS_TWO nCols;
};
typedef struct oosql_getnumresultcols_reply oosql_getnumresultcols_reply;
bool_t xdr_oosql_getnumresultcols_reply();


struct oosql_getnumresultcols_arg {
	long threadId;
	oosql_handle_t handle;
};
typedef struct oosql_getnumresultcols_arg oosql_getnumresultcols_arg;
bool_t xdr_oosql_getnumresultcols_arg();


struct oosql_getresultcolname_reply {
	CS_FOUR errCode;
	struct {
		u_int columnNameBuffer_len;
		char *columnNameBuffer_val;
	} columnNameBuffer;
};
typedef struct oosql_getresultcolname_reply oosql_getresultcolname_reply;
bool_t xdr_oosql_getresultcolname_reply();


struct oosql_getresultcolname_arg {
	long threadId;
	oosql_handle_t handle;
	CS_TWO columnNumber;
	CS_FOUR bufferLength;
};
typedef struct oosql_getresultcolname_arg oosql_getresultcolname_arg;
bool_t xdr_oosql_getresultcolname_arg();


struct oosql_getresultcoltype_reply {
	CS_FOUR errCode;
	CS_FOUR columnType;
};
typedef struct oosql_getresultcoltype_reply oosql_getresultcoltype_reply;
bool_t xdr_oosql_getresultcoltype_reply();


struct oosql_getresultcoltype_arg {
	long threadId;
	oosql_handle_t handle;
	CS_TWO columnNumber;
};
typedef struct oosql_getresultcoltype_arg oosql_getresultcoltype_arg;
bool_t xdr_oosql_getresultcoltype_arg();


struct oosql_getresultcollength_reply {
	CS_FOUR errCode;
	CS_FOUR resultColLength;
};
typedef struct oosql_getresultcollength_reply oosql_getresultcollength_reply;
bool_t xdr_oosql_getresultcollength_reply();


struct oosql_getresultcollength_arg {
	long threadId;
	oosql_handle_t handle;
	CS_TWO columnNumber;
};
typedef struct oosql_getresultcollength_arg oosql_getresultcollength_arg;
bool_t xdr_oosql_getresultcollength_arg();


struct oosql_geterrormessage_reply {
	CS_FOUR errCode;
	struct {
		u_int messageBuffer_len;
		char *messageBuffer_val;
	} messageBuffer;
};
typedef struct oosql_geterrormessage_reply oosql_geterrormessage_reply;
bool_t xdr_oosql_geterrormessage_reply();


struct oosql_geterrormessage_arg {
	long threadId;
	CS_FOUR errorCode;
	CS_FOUR bufferLength;
};
typedef struct oosql_geterrormessage_arg oosql_geterrormessage_arg;
bool_t xdr_oosql_geterrormessage_arg();


struct oosql_oidtooidstring_reply {
	CS_FOUR errCode;
	struct {
		u_int oidString_len;
		char *oidString_val;
	} oidString;
};
typedef struct oosql_oidtooidstring_reply oosql_oidtooidstring_reply;
bool_t xdr_oosql_oidtooidstring_reply();


struct oosql_oidtooidstring_arg {
	long threadId;
	CS_OID *oid;
};
typedef struct oosql_oidtooidstring_arg oosql_oidtooidstring_arg;
bool_t xdr_oosql_oidtooidstring_arg();


struct oosql_text_addkeywordextractor_reply {
	CS_FOUR errCode;
	CS_FOUR keywordExtractorNo;
};
typedef struct oosql_text_addkeywordextractor_reply oosql_text_addkeywordextractor_reply;
bool_t xdr_oosql_text_addkeywordextractor_reply();


struct oosql_text_addkeywordextractor_arg {
	long threadId;
	CS_FOUR volumeId;
	keywordExtractorName_t keywordExtractorName;
	CS_FOUR version1;
	keywordExtractorFilePath_t keywordExtractorFilePath;
	keywordExtractorFunctionName_t keywordExtractorFunctionName;
};
typedef struct oosql_text_addkeywordextractor_arg oosql_text_addkeywordextractor_arg;
bool_t xdr_oosql_text_addkeywordextractor_arg();


struct oosql_text_adddefaultkeywordextractor_reply {
	CS_FOUR errCode;
	CS_FOUR keywordExtractorNo;
};
typedef struct oosql_text_adddefaultkeywordextractor_reply oosql_text_adddefaultkeywordextractor_reply;
bool_t xdr_oosql_text_adddefaultkeywordextractor_reply();


struct oosql_text_adddefaultkeywordextractor_arg {
	long threadId;
	CS_FOUR volumeId;
	keywordExtractorName_t keywordExtractorName;
	CS_FOUR version1;
	keywordExtractorFilePath_t keywordExtractorFilePath;
	keywordExtractorFunctionName_t keywordExtractorFunctionName;
};
typedef struct oosql_text_adddefaultkeywordextractor_arg oosql_text_adddefaultkeywordextractor_arg;
bool_t xdr_oosql_text_adddefaultkeywordextractor_arg();


struct oosql_text_dropkeywordextractor_reply {
	CS_FOUR errCode;
};
typedef struct oosql_text_dropkeywordextractor_reply oosql_text_dropkeywordextractor_reply;
bool_t xdr_oosql_text_dropkeywordextractor_reply();


struct oosql_text_dropkeywordextractor_arg {
	long threadId;
	CS_FOUR volumeId;
	keywordExtractorName_t keywordExtractorName;
	CS_FOUR version1;
	keywordExtractorFilePath_t keywordExtractorFilePath;
	keywordExtractorFunctionName_t keywordExtractorFunctionName;
};
typedef struct oosql_text_dropkeywordextractor_arg oosql_text_dropkeywordextractor_arg;
bool_t xdr_oosql_text_dropkeywordextractor_arg();


struct oosql_text_setkeywordextractor_reply {
	CS_FOUR errCode;
};
typedef struct oosql_text_setkeywordextractor_reply oosql_text_setkeywordextractor_reply;
bool_t xdr_oosql_text_setkeywordextractor_reply();


struct oosql_text_setkeywordextractor_arg {
	long threadId;
	CS_FOUR volumeId;
	className_t className;
	attrName_t colName;
	CS_FOUR keywordExtractorNo;
};
typedef struct oosql_text_setkeywordextractor_arg oosql_text_setkeywordextractor_arg;
bool_t xdr_oosql_text_setkeywordextractor_arg();


struct oosql_text_addfilter_reply {
	CS_FOUR errCode;
	CS_FOUR filterNo;
};
typedef struct oosql_text_addfilter_reply oosql_text_addfilter_reply;
bool_t xdr_oosql_text_addfilter_reply();


struct oosql_text_addfilter_arg {
	long threadId;
	CS_FOUR volumeId;
	filterName_t filterName;
	CS_FOUR version1;
	filterFilePath_t filterFilePath;
	filterFunctionName_t filterFunctionName;
};
typedef struct oosql_text_addfilter_arg oosql_text_addfilter_arg;
bool_t xdr_oosql_text_addfilter_arg();


struct oosql_text_dropfilter_reply {
	CS_FOUR errCode;
};
typedef struct oosql_text_dropfilter_reply oosql_text_dropfilter_reply;
bool_t xdr_oosql_text_dropfilter_reply();


struct oosql_text_dropfilter_arg {
	long threadId;
	CS_FOUR volumeId;
	filterName_t filterName;
	CS_FOUR version1;
};
typedef struct oosql_text_dropfilter_arg oosql_text_dropfilter_arg;
bool_t xdr_oosql_text_dropfilter_arg();


struct oosql_text_setfilter_reply {
	CS_FOUR errCode;
};
typedef struct oosql_text_setfilter_reply oosql_text_setfilter_reply;
bool_t xdr_oosql_text_setfilter_reply();


struct oosql_text_setfilter_arg {
	long threadId;
	CS_FOUR volumeId;
	className_t className;
	attrName_t colName;
	CS_FOUR filterNo;
};
typedef struct oosql_text_setfilter_arg oosql_text_setfilter_arg;
bool_t xdr_oosql_text_setfilter_arg();


struct oosql_text_makeindex_reply {
	CS_FOUR errCode;
};
typedef struct oosql_text_makeindex_reply oosql_text_makeindex_reply;
bool_t xdr_oosql_text_makeindex_reply();


struct oosql_text_makeindex_arg {
	long threadId;
	CS_FOUR volumeId;
	className_t className;
};
typedef struct oosql_text_makeindex_arg oosql_text_makeindex_arg;
bool_t xdr_oosql_text_makeindex_arg();


struct lom_set_create_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_create_reply lom_set_create_reply;
bool_t xdr_lom_set_create_reply();


struct lom_set_create_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
};
typedef struct lom_set_create_arg lom_set_create_arg;
bool_t xdr_lom_set_create_arg();


struct lom_set_destroy_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_destroy_reply lom_set_destroy_reply;
bool_t xdr_lom_set_destroy_reply();


struct lom_set_destroy_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
};
typedef struct lom_set_destroy_arg lom_set_destroy_arg;
bool_t xdr_lom_set_destroy_arg();


struct lom_set_insertelements_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_insertelements_reply lom_set_insertelements_reply;
bool_t xdr_lom_set_insertelements_reply();


struct lom_set_insertelements_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_FOUR colNo;
	CS_FOUR nElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
};
typedef struct lom_set_insertelements_arg lom_set_insertelements_arg;
bool_t xdr_lom_set_insertelements_arg();


struct lom_set_deleteelements_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_deleteelements_reply lom_set_deleteelements_reply;
bool_t xdr_lom_set_deleteelements_reply();


struct lom_set_deleteelements_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_FOUR colNo;
	CS_FOUR nElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
};
typedef struct lom_set_deleteelements_arg lom_set_deleteelements_arg;
bool_t xdr_lom_set_deleteelements_arg();


struct lom_set_ismember_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_ismember_reply lom_set_ismember_reply;
bool_t xdr_lom_set_ismember_reply();


struct lom_set_ismember_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	struct {
		u_int element_len;
		char *element_val;
	} element;
};
typedef struct lom_set_ismember_arg lom_set_ismember_arg;
bool_t xdr_lom_set_ismember_arg();


struct lom_set_scan_open_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_scan_open_reply lom_set_scan_open_reply;
bool_t xdr_lom_set_scan_open_reply();


struct lom_set_scan_open_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
};
typedef struct lom_set_scan_open_arg lom_set_scan_open_arg;
bool_t xdr_lom_set_scan_open_arg();


struct lom_set_scan_close_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_scan_close_reply lom_set_scan_close_reply;
bool_t xdr_lom_set_scan_close_reply();


struct lom_set_scan_close_arg {
	long threadId;
	CS_FOUR setScanId;
};
typedef struct lom_set_scan_close_arg lom_set_scan_close_arg;
bool_t xdr_lom_set_scan_close_arg();


struct lom_set_scan_next_reply {
	CS_FOUR errCode;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
};
typedef struct lom_set_scan_next_reply lom_set_scan_next_reply;
bool_t xdr_lom_set_scan_next_reply();


struct lom_set_scan_next_arg {
	long threadId;
	CS_FOUR setScanId;
	CS_FOUR nElements;
};
typedef struct lom_set_scan_next_arg lom_set_scan_next_arg;
bool_t xdr_lom_set_scan_next_arg();


struct lom_set_scan_insert_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_scan_insert_reply lom_set_scan_insert_reply;
bool_t xdr_lom_set_scan_insert_reply();


struct lom_set_scan_insert_arg {
	long threadId;
	CS_FOUR setScanId;
	CS_FOUR nElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
};
typedef struct lom_set_scan_insert_arg lom_set_scan_insert_arg;
bool_t xdr_lom_set_scan_insert_arg();


struct lom_set_scan_delete_reply {
	CS_FOUR errCode;
};
typedef struct lom_set_scan_delete_reply lom_set_scan_delete_reply;
bool_t xdr_lom_set_scan_delete_reply();


struct lom_set_scan_delete_arg {
	long threadId;
	CS_FOUR setScanId;
};
typedef struct lom_set_scan_delete_arg lom_set_scan_delete_arg;
bool_t xdr_lom_set_scan_delete_arg();


struct lom_rs_create_reply {
	CS_FOUR errCode;
	CS_FOUR relationshipId;
};
typedef struct lom_rs_create_reply lom_rs_create_reply;
bool_t xdr_lom_rs_create_reply();


struct lom_rs_create_arg {
	long threadId;
	CS_FOUR databaseId;
	relationshipName_t relationshipName;
	CS_FOUR fromClassId;
	CS_TWO fromAttrNum;
	CS_FOUR toClassId;
	CS_TWO toAttrNum;
	CS_ONE cardinality;
	CS_ONE directionality;
};
typedef struct lom_rs_create_arg lom_rs_create_arg;
bool_t xdr_lom_rs_create_arg();


struct lom_rs_destroy_reply {
	CS_FOUR errCode;
};
typedef struct lom_rs_destroy_reply lom_rs_destroy_reply;
bool_t xdr_lom_rs_destroy_reply();


struct lom_rs_destroy_arg {
	long threadId;
	CS_FOUR databaseId;
	relationshipName_t relationshipName;
};
typedef struct lom_rs_destroy_arg lom_rs_destroy_arg;
bool_t xdr_lom_rs_destroy_arg();


struct lom_rs_createinstance_reply {
	CS_FOUR errCode;
};
typedef struct lom_rs_createinstance_reply lom_rs_createinstance_reply;
bool_t xdr_lom_rs_createinstance_reply();


struct lom_rs_createinstance_arg {
	long threadId;
	CS_FOUR fromOcnOrScanId;
	bool_t fromUseScanFlag;
	CS_FOUR toOcnOrScanId;
	bool_t toUseScanFlag;
	CS_FOUR relationshipId;
	CS_OID *fromOID;
	CS_OID *toOID;
};
typedef struct lom_rs_createinstance_arg lom_rs_createinstance_arg;
bool_t xdr_lom_rs_createinstance_arg();


struct lom_rs_destroyinstance_reply {
	CS_FOUR errCode;
};
typedef struct lom_rs_destroyinstance_reply lom_rs_destroyinstance_reply;
bool_t xdr_lom_rs_destroyinstance_reply();


struct lom_rs_destroyinstance_arg {
	long threadId;
	CS_FOUR fromOcnOrScanId;
	bool_t fromUseScanFlag;
	CS_FOUR toOcnOrScanId;
	bool_t toUseScanFlag;
	CS_FOUR relationshipId;
	CS_OID *fromOID;
	CS_OID *toOID;
};
typedef struct lom_rs_destroyinstance_arg lom_rs_destroyinstance_arg;
bool_t xdr_lom_rs_destroyinstance_arg();


struct lom_rs_getid_reply {
	CS_FOUR relationshipId;
	CS_FOUR errCode;
};
typedef struct lom_rs_getid_reply lom_rs_getid_reply;
bool_t xdr_lom_rs_getid_reply();


struct lom_rs_getid_arg {
	long threadId;
	CS_FOUR databaseId;
	relationshipName_t relationshipName;
};
typedef struct lom_rs_getid_arg lom_rs_getid_arg;
bool_t xdr_lom_rs_getid_arg();


struct lom_rs_openscan_reply {
	CS_FOUR errCode;
};
typedef struct lom_rs_openscan_reply lom_rs_openscan_reply;
bool_t xdr_lom_rs_openscan_reply();


struct lom_rs_openscan_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *toOID;
	CS_FOUR relationshipId;
};
typedef struct lom_rs_openscan_arg lom_rs_openscan_arg;
bool_t xdr_lom_rs_openscan_arg();


struct lom_rs_closescan_reply {
	CS_FOUR errCode;
};
typedef struct lom_rs_closescan_reply lom_rs_closescan_reply;
bool_t xdr_lom_rs_closescan_reply();


struct lom_rs_closescan_arg {
	long threadId;
	CS_FOUR relationshipScanId;
};
typedef struct lom_rs_closescan_arg lom_rs_closescan_arg;
bool_t xdr_lom_rs_closescan_arg();


struct lom_rs_nextinstances_reply {
	CS_FOUR errCode;
	struct {
		u_int oids_len;
		CS_OID *oids_val;
	} oids;
};
typedef struct lom_rs_nextinstances_reply lom_rs_nextinstances_reply;
bool_t xdr_lom_rs_nextinstances_reply();


struct lom_rs_nextinstances_arg {
	long threadId;
	CS_FOUR relationshipScanId;
	CS_FOUR nOIDs;
};
typedef struct lom_rs_nextinstances_arg lom_rs_nextinstances_arg;
bool_t xdr_lom_rs_nextinstances_arg();


struct lom_text_createcontent_reply {
	CS_FOUR errCode;
	Server_TextDesc textDesc;
};
typedef struct lom_text_createcontent_reply lom_text_createcontent_reply;
bool_t xdr_lom_text_createcontent_reply();


struct lom_text_createcontent_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_TextDesc *textDesc;
	Server_TextColStruct *text;
	struct {
		u_int content_len;
		char *content_val;
	} content;
};
typedef struct lom_text_createcontent_arg lom_text_createcontent_arg;
bool_t xdr_lom_text_createcontent_arg();


struct lom_text_fetchcontent_reply {
	CS_FOUR errCode;
	struct {
		u_int content_len;
		char *content_val;
	} content;
	Server_TextDesc textDesc;
};
typedef struct lom_text_fetchcontent_reply lom_text_fetchcontent_reply;
bool_t xdr_lom_text_fetchcontent_reply();


struct lom_text_fetchcontent_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_TextDesc *textDesc;
	Server_TextColStruct *text;
};
typedef struct lom_text_fetchcontent_arg lom_text_fetchcontent_arg;
bool_t xdr_lom_text_fetchcontent_arg();


struct lom_text_updatecontent_reply {
	CS_FOUR errCode;
	Server_TextDesc textDesc;
};
typedef struct lom_text_updatecontent_reply lom_text_updatecontent_reply;
bool_t xdr_lom_text_updatecontent_reply();


struct lom_text_updatecontent_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_TextDesc *textDesc;
	Server_TextColStruct *text;
	struct {
		u_int content_len;
		char *content_val;
	} content;
};
typedef struct lom_text_updatecontent_arg lom_text_updatecontent_arg;
bool_t xdr_lom_text_updatecontent_arg();


struct lom_text_destroycontent_reply {
	CS_FOUR errCode;
	Server_TextDesc textDesc;
};
typedef struct lom_text_destroycontent_reply lom_text_destroycontent_reply;
bool_t xdr_lom_text_destroycontent_reply();


struct lom_text_destroycontent_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_TextDesc *textDesc;
};
typedef struct lom_text_destroycontent_arg lom_text_destroycontent_arg;
bool_t xdr_lom_text_destroycontent_arg();


struct lom_text_makeindex_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_makeindex_reply lom_text_makeindex_reply;
bool_t xdr_lom_text_makeindex_reply();


struct lom_text_makeindex_arg {
	long threadId;
	CS_FOUR databaseId;
	className_t className;
};
typedef struct lom_text_makeindex_arg lom_text_makeindex_arg;
bool_t xdr_lom_text_makeindex_arg();


struct lom_text_getindexid_reply {
	Server_IndexID iid;
	CS_FOUR errCode;
};
typedef struct lom_text_getindexid_reply lom_text_getindexid_reply;
bool_t xdr_lom_text_getindexid_reply();


struct lom_text_getindexid_arg {
	long threadId;
	CS_FOUR ocn;
	CS_FOUR colNo;
};
typedef struct lom_text_getindexid_arg lom_text_getindexid_arg;
bool_t xdr_lom_text_getindexid_arg();


struct lom_text_getdescriptor_reply {
	CS_FOUR errCode;
	Server_TextDesc textDesc;
};
typedef struct lom_text_getdescriptor_reply lom_text_getdescriptor_reply;
bool_t xdr_lom_text_getdescriptor_reply();


struct lom_text_getdescriptor_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
};
typedef struct lom_text_getdescriptor_arg lom_text_getdescriptor_arg;
bool_t xdr_lom_text_getdescriptor_arg();


struct lom_text_openindexscan_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_openindexscan_reply lom_text_openindexscan_reply;
bool_t xdr_lom_text_openindexscan_reply();


struct lom_text_openindexscan_arg {
	long threadId;
	CS_FOUR ocn;
	Server_IndexID *indexId;
	CS_FOUR keywordKind;
	Server_BoundCond *keywordStartBound;
	Server_BoundCond *keywordStopBound;
	Server_LockParameter *lockup;
};
typedef struct lom_text_openindexscan_arg lom_text_openindexscan_arg;
bool_t xdr_lom_text_openindexscan_arg();


struct lom_text_scan_open_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_scan_open_reply lom_text_scan_open_reply;
bool_t xdr_lom_text_scan_open_reply();


struct lom_text_scan_open_arg {
	long threadId;
	CS_FOUR ocn;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR keywordKind;
	Server_BoundCond *keywordStartBound;
	Server_BoundCond *keywordStopBound;
	Server_LockParameter *lockup;
};
typedef struct lom_text_scan_open_arg lom_text_scan_open_arg;
bool_t xdr_lom_text_scan_open_arg();


struct lom_text_scan_close_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_scan_close_reply lom_text_scan_close_reply;
bool_t xdr_lom_text_scan_close_reply();


struct lom_text_scan_close_arg {
	long threadId;
	CS_FOUR osn;
};
typedef struct lom_text_scan_close_arg lom_text_scan_close_arg;
bool_t xdr_lom_text_scan_close_arg();


struct lom_text_scan_nextposting_reply {
	CS_FOUR errCode;
	float weight;
	CS_FOUR requiredSize;
	struct {
		u_int buffer_len;
		char *buffer_val;
	} buffer;
};
typedef struct lom_text_scan_nextposting_reply lom_text_scan_nextposting_reply;
bool_t xdr_lom_text_scan_nextposting_reply();


struct lom_text_scan_nextposting_arg {
	long threadId;
	CS_FOUR textScan;
	CS_FOUR bufferLength;
};
typedef struct lom_text_scan_nextposting_arg lom_text_scan_nextposting_arg;
bool_t xdr_lom_text_scan_nextposting_arg();


struct lom_text_nextpostings_reply {
	CS_FOUR errCode;
	CS_FOUR nReturnedPostings;
	CS_FOUR requiredSize;
	struct {
		u_int buffer_len;
		char *buffer_val;
	} buffer;
};
typedef struct lom_text_nextpostings_reply lom_text_nextpostings_reply;
bool_t xdr_lom_text_nextpostings_reply();


struct lom_text_nextpostings_arg {
	long threadId;
	CS_FOUR textScan;
	CS_FOUR bufferLength;
};
typedef struct lom_text_nextpostings_arg lom_text_nextpostings_arg;
bool_t xdr_lom_text_nextpostings_arg();


struct lom_text_getcursorkeyword_reply {
	CS_FOUR errCode;
	char keyword[CS_MAXKEYWORDSIZE];
};
typedef struct lom_text_getcursorkeyword_reply lom_text_getcursorkeyword_reply;
bool_t xdr_lom_text_getcursorkeyword_reply();


struct lom_text_getcursorkeyword_arg {
	long threadId;
	CS_FOUR textScan;
};
typedef struct lom_text_getcursorkeyword_arg lom_text_getcursorkeyword_arg;
bool_t xdr_lom_text_getcursorkeyword_arg();


struct lom_text_addfilter_arg {
	long threadId;
	CS_FOUR databaseId;
	filterName_t filterName;
	CS_FOUR version1;
	filterFilePath_t filterFilePath;
	filterFunctionName_t filterFunctionName;
};
typedef struct lom_text_addfilter_arg lom_text_addfilter_arg;
bool_t xdr_lom_text_addfilter_arg();


struct lom_text_addfilter_reply {
	CS_FOUR errCode;
	CS_FOUR filterNo;
};
typedef struct lom_text_addfilter_reply lom_text_addfilter_reply;
bool_t xdr_lom_text_addfilter_reply();


struct lom_text_dropfilter_arg {
	long threadId;
	CS_FOUR databaseId;
	filterName_t filterName;
	CS_FOUR version1;
};
typedef struct lom_text_dropfilter_arg lom_text_dropfilter_arg;
bool_t xdr_lom_text_dropfilter_arg();


struct lom_text_dropfilter_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_dropfilter_reply lom_text_dropfilter_reply;
bool_t xdr_lom_text_dropfilter_reply();


struct lom_text_getfilterno_arg {
	long threadId;
	CS_FOUR databaseId;
	filterName_t filterName;
	CS_FOUR version1;
};
typedef struct lom_text_getfilterno_arg lom_text_getfilterno_arg;
bool_t xdr_lom_text_getfilterno_arg();


struct lom_text_getfilterno_reply {
	CS_FOUR errCode;
	CS_FOUR filterNo;
};
typedef struct lom_text_getfilterno_reply lom_text_getfilterno_reply;
bool_t xdr_lom_text_getfilterno_reply();


struct lom_text_setfilter_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
	CS_FOUR colNo;
	CS_FOUR filterNo;
};
typedef struct lom_text_setfilter_arg lom_text_setfilter_arg;
bool_t xdr_lom_text_setfilter_arg();


struct lom_text_setfilter_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_setfilter_reply lom_text_setfilter_reply;
bool_t xdr_lom_text_setfilter_reply();


struct lom_text_getfilterinfo_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR filterNo;
};
typedef struct lom_text_getfilterinfo_arg lom_text_getfilterinfo_arg;
bool_t xdr_lom_text_getfilterinfo_arg();


struct lom_text_getfilterinfo_reply {
	CS_FOUR errCode;
	filterFilePath_t filterFilePath;
	filterFunctionName_t filterFunctionName;
};
typedef struct lom_text_getfilterinfo_reply lom_text_getfilterinfo_reply;
bool_t xdr_lom_text_getfilterinfo_reply();


struct lom_text_getcurrentfilterno_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
	CS_FOUR colNo;
};
typedef struct lom_text_getcurrentfilterno_arg lom_text_getcurrentfilterno_arg;
bool_t xdr_lom_text_getcurrentfilterno_arg();


struct lom_text_getcurrentfilterno_reply {
	CS_FOUR errCode;
	CS_FOUR filterNo;
};
typedef struct lom_text_getcurrentfilterno_reply lom_text_getcurrentfilterno_reply;
bool_t xdr_lom_text_getcurrentfilterno_reply();


struct lom_text_resetfilter_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
	CS_FOUR colNo;
};
typedef struct lom_text_resetfilter_arg lom_text_resetfilter_arg;
bool_t xdr_lom_text_resetfilter_arg();


struct lom_text_resetfilter_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_resetfilter_reply lom_text_resetfilter_reply;
bool_t xdr_lom_text_resetfilter_reply();


struct lom_text_addkeywordextractor_arg {
	long threadId;
	CS_FOUR databaseId;
	keywordExtractorName_t keywordExtractorName;
	CS_FOUR version1;
	keywordExtractorFilePath_t keywordExtractorFilePath;
	keywordExtractorFunctionName_t keywordExtractorFunctionName;
};
typedef struct lom_text_addkeywordextractor_arg lom_text_addkeywordextractor_arg;
bool_t xdr_lom_text_addkeywordextractor_arg();


struct lom_text_addkeywordextractor_reply {
	CS_FOUR errCode;
	CS_FOUR keywordExtractorNo;
};
typedef struct lom_text_addkeywordextractor_reply lom_text_addkeywordextractor_reply;
bool_t xdr_lom_text_addkeywordextractor_reply();


struct lom_text_adddefaultkeywordextractor_arg {
	long threadId;
	CS_FOUR databaseId;
	keywordExtractorName_t keywordExtractorName;
	CS_FOUR version1;
	keywordExtractorFilePath_t keywordExtractorFilePath;
	keywordExtractorFunctionName_t keywordExtractorFunctionName;
};
typedef struct lom_text_adddefaultkeywordextractor_arg lom_text_adddefaultkeywordextractor_arg;
bool_t xdr_lom_text_adddefaultkeywordextractor_arg();


struct lom_text_adddefaultkeywordextractor_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_adddefaultkeywordextractor_reply lom_text_adddefaultkeywordextractor_reply;
bool_t xdr_lom_text_adddefaultkeywordextractor_reply();


struct lom_text_dropkeywordextractor_arg {
	long threadId;
	CS_FOUR databaseId;
	keywordExtractorName_t keywordExtractorName;
	CS_FOUR version1;
};
typedef struct lom_text_dropkeywordextractor_arg lom_text_dropkeywordextractor_arg;
bool_t xdr_lom_text_dropkeywordextractor_arg();


struct lom_text_dropkeywordextractor_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_dropkeywordextractor_reply lom_text_dropkeywordextractor_reply;
bool_t xdr_lom_text_dropkeywordextractor_reply();


struct lom_text_getkeywordextractorno_arg {
	long threadId;
	CS_FOUR databaseId;
	keywordExtractorName_t keywordExtractorName;
	CS_FOUR version1;
};
typedef struct lom_text_getkeywordextractorno_arg lom_text_getkeywordextractorno_arg;
bool_t xdr_lom_text_getkeywordextractorno_arg();


struct lom_text_getkeywordextractorno_reply {
	CS_FOUR errCode;
	CS_FOUR keywordExtractorNo;
};
typedef struct lom_text_getkeywordextractorno_reply lom_text_getkeywordextractorno_reply;
bool_t xdr_lom_text_getkeywordextractorno_reply();


struct lom_text_setkeywordextractor_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
	CS_FOUR colNo;
	CS_FOUR keywordExtractorNo;
};
typedef struct lom_text_setkeywordextractor_arg lom_text_setkeywordextractor_arg;
bool_t xdr_lom_text_setkeywordextractor_arg();


struct lom_text_setkeywordextractor_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_setkeywordextractor_reply lom_text_setkeywordextractor_reply;
bool_t xdr_lom_text_setkeywordextractor_reply();


struct lom_text_getkeywordextractorinfo_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR keywordExtractorNo;
};
typedef struct lom_text_getkeywordextractorinfo_arg lom_text_getkeywordextractorinfo_arg;
bool_t xdr_lom_text_getkeywordextractorinfo_arg();


struct lom_text_getkeywordextractorinfo_reply {
	CS_FOUR errCode;
	keywordExtractorFilePath_t keywordExtractorFilePath;
	keywordExtractorFunctionName_t keywordExtractorFunctionName;
};
typedef struct lom_text_getkeywordextractorinfo_reply lom_text_getkeywordextractorinfo_reply;
bool_t xdr_lom_text_getkeywordextractorinfo_reply();


struct lom_text_getcurrentkeywordextractorno_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
	CS_FOUR colNo;
};
typedef struct lom_text_getcurrentkeywordextractorno_arg lom_text_getcurrentkeywordextractorno_arg;
bool_t xdr_lom_text_getcurrentkeywordextractorno_arg();


struct lom_text_getcurrentkeywordextractorno_reply {
	CS_FOUR errCode;
	CS_FOUR keywordExtractorNo;
};
typedef struct lom_text_getcurrentkeywordextractorno_reply lom_text_getcurrentkeywordextractorno_reply;
bool_t xdr_lom_text_getcurrentkeywordextractorno_reply();


struct lom_text_resetkeywordextractor_arg {
	long threadId;
	CS_FOUR databaseId;
	CS_FOUR classId;
	CS_FOUR colNo;
};
typedef struct lom_text_resetkeywordextractor_arg lom_text_resetkeywordextractor_arg;
bool_t xdr_lom_text_resetkeywordextractor_arg();


struct lom_text_resetkeywordextractor_reply {
	CS_FOUR errCode;
};
typedef struct lom_text_resetkeywordextractor_reply lom_text_resetkeywordextractor_reply;
bool_t xdr_lom_text_resetkeywordextractor_reply();


struct server_getpid_arg {
	long threadId;
	CS_FOUR padding;
};
typedef struct server_getpid_arg server_getpid_arg;
bool_t xdr_server_getpid_arg();


struct lom_opennamedobjecttable_reply {
	CS_FOUR errCode;
};
typedef struct lom_opennamedobjecttable_reply lom_opennamedobjecttable_reply;
bool_t xdr_lom_opennamedobjecttable_reply();


struct lom_opennamedobjecttable_arg {
	long threadId;
	CS_FOUR databaseId;
};
typedef struct lom_opennamedobjecttable_arg lom_opennamedobjecttable_arg;
bool_t xdr_lom_opennamedobjecttable_arg();


struct lom_closenamedobjecttable_reply {
	CS_FOUR errCode;
};
typedef struct lom_closenamedobjecttable_reply lom_closenamedobjecttable_reply;
bool_t xdr_lom_closenamedobjecttable_reply();


struct lom_closenamedobjecttable_arg {
	long threadId;
	CS_FOUR ocn;
};
typedef struct lom_closenamedobjecttable_arg lom_closenamedobjecttable_arg;
bool_t xdr_lom_closenamedobjecttable_arg();


struct lom_setobjectname_reply {
	CS_FOUR errCode;
};
typedef struct lom_setobjectname_reply lom_setobjectname_reply;
bool_t xdr_lom_setobjectname_reply();


struct lom_setobjectname_arg {
	long threadId;
	CS_FOUR ocn;
	objectName_t objectName;
	CS_OID *oid;
};
typedef struct lom_setobjectname_arg lom_setobjectname_arg;
bool_t xdr_lom_setobjectname_arg();


struct lom_lookupnamedobject_reply {
	CS_OID oid;
	CS_FOUR errCode;
};
typedef struct lom_lookupnamedobject_reply lom_lookupnamedobject_reply;
bool_t xdr_lom_lookupnamedobject_reply();


struct lom_lookupnamedobject_arg {
	long threadId;
	CS_FOUR ocn;
	objectName_t objectName;
};
typedef struct lom_lookupnamedobject_arg lom_lookupnamedobject_arg;
bool_t xdr_lom_lookupnamedobject_arg();


struct lom_resetobjectname_reply {
	CS_FOUR errCode;
};
typedef struct lom_resetobjectname_reply lom_resetobjectname_reply;
bool_t xdr_lom_resetobjectname_reply();


struct lom_resetobjectname_arg {
	long threadId;
	CS_FOUR ocn;
	objectName_t objectName;
};
typedef struct lom_resetobjectname_arg lom_resetobjectname_arg;
bool_t xdr_lom_resetobjectname_arg();


struct lom_renamenamedobject_reply {
	CS_FOUR errCode;
};
typedef struct lom_renamenamedobject_reply lom_renamenamedobject_reply;
bool_t xdr_lom_renamenamedobject_reply();


struct lom_renamenamedobject_arg {
	long threadId;
	CS_FOUR ocn;
	objectName_t oldName;
	objectName_t newName;
};
typedef struct lom_renamenamedobject_arg lom_renamenamedobject_arg;
bool_t xdr_lom_renamenamedobject_arg();


struct lom_getobjectname_reply {
	CS_FOUR errCode;
	objectName_t objectName;
};
typedef struct lom_getobjectname_reply lom_getobjectname_reply;
bool_t xdr_lom_getobjectname_reply();


struct lom_getobjectname_arg {
	long threadId;
	CS_FOUR ocn;
	CS_OID *oid;
};
typedef struct lom_getobjectname_arg lom_getobjectname_arg;
bool_t xdr_lom_getobjectname_arg();


struct RPC_PoolIndex {
	int startIndex;
	int size;
};
typedef struct RPC_PoolIndex RPC_PoolIndex;
bool_t xdr_RPC_PoolIndex();


struct RPC_SDP_PlanElement {
	int command;
	int kind;
	RPC_PoolIndex schemaName;
	RPC_PoolIndex className;
	int classId;
	RPC_PoolIndex typeName;
	int typeId;
	RPC_PoolIndex superclassInfoList;
	int classKind;
	int onCommitAction;
	RPC_PoolIndex attributeList;
	RPC_PoolIndex methodList;
	RPC_PoolIndex referenceList;
	RPC_PoolIndex keyList;
	char order;
	RPC_PoolIndex orderingFunction;
	RPC_PoolIndex constructor;
	int isAbstractType;
	RPC_PoolIndex relationshipName;
	RPC_PoolIndex fromClassName;
	RPC_PoolIndex fromAttrName;
	RPC_PoolIndex toClassName;
	RPC_PoolIndex toAttrName;
	int cardinality;
	RPC_PoolIndex indexName;
	int indexType;
	int indexFlag;
	RPC_PoolIndex indexColumnNameList;
};
typedef struct RPC_SDP_PlanElement RPC_SDP_PlanElement;
bool_t xdr_RPC_SDP_PlanElement();


struct RPC_SDP_AttributeElement {
	RPC_PoolIndex name;
	RPC_PoolIndex type;
	int encapsulationLevel;
	int columnKind;
	RPC_PoolIndex defaultValue;
	int isNullable;
	int inheritedFrom;
	RPC_PoolIndex inheritedFromClassName;
	RPC_PoolIndex inheritedFromSchemaName;
};
typedef struct RPC_SDP_AttributeElement RPC_SDP_AttributeElement;
bool_t xdr_RPC_SDP_AttributeElement();


struct RPC_SDP_MethodElement {
	RPC_PoolIndex name;
	RPC_PoolIndex dirPath;
	int encapsulationLevel;
	RPC_PoolIndex funcName;
	RPC_PoolIndex argTypeList;
	RPC_PoolIndex returnType;
	int methodType;
	int language;
	int inheritedFrom;
	RPC_PoolIndex inheritedFromClassName;
	RPC_PoolIndex inheritedFromSchemaName;
};
typedef struct RPC_SDP_MethodElement RPC_SDP_MethodElement;
bool_t xdr_RPC_SDP_MethodElement();


struct RPC_SDP_ReferenceElement {
	RPC_PoolIndex referencedClassName;
	RPC_PoolIndex referencedColumnNameList;
	int matchType;
	int action;
	RPC_PoolIndex columnNames;
};
typedef struct RPC_SDP_ReferenceElement RPC_SDP_ReferenceElement;
bool_t xdr_RPC_SDP_ReferenceElement();


struct RPC_SDP_KeyElement {
	int kind;
	RPC_PoolIndex columnNames;
};
typedef struct RPC_SDP_KeyElement RPC_SDP_KeyElement;
bool_t xdr_RPC_SDP_KeyElement();


struct RPC_SDP_SuperclassElement {
	RPC_PoolIndex superclassName;
	RPC_PoolIndex superclassSchemaName;
};
typedef struct RPC_SDP_SuperclassElement RPC_SDP_SuperclassElement;
bool_t xdr_RPC_SDP_SuperclassElement();


struct RPC_SDP_TypeElement {
	int type;
	int complexType;
	int length;
	RPC_PoolIndex domain;
};
typedef struct RPC_SDP_TypeElement RPC_SDP_TypeElement;
bool_t xdr_RPC_SDP_TypeElement();


struct sdp_execute_arg {
	long threadId;
	int volId;
	int sessionId;
	struct {
		u_int plans_len;
		RPC_SDP_PlanElement *plans_val;
	} plans;
	struct {
		u_int attrs_len;
		RPC_SDP_AttributeElement *attrs_val;
	} attrs;
	struct {
		u_int methods_len;
		RPC_SDP_MethodElement *methods_val;
	} methods;
	struct {
		u_int refs_len;
		RPC_SDP_ReferenceElement *refs_val;
	} refs;
	struct {
		u_int keys_len;
		RPC_SDP_KeyElement *keys_val;
	} keys;
	struct {
		u_int superclasses_len;
		RPC_SDP_SuperclassElement *superclasses_val;
	} superclasses;
	struct {
		u_int types_len;
		RPC_SDP_TypeElement *types_val;
	} types;
	struct {
		u_int chars_len;
		char *chars_val;
	} chars;
	struct {
		u_int stringListIndex_len;
		RPC_PoolIndex *stringListIndex_val;
	} stringListIndex;
};
typedef struct sdp_execute_arg sdp_execute_arg;
bool_t xdr_sdp_execute_arg();


struct sdp_execute_reply {
	CS_FOUR errCode;
};
typedef struct sdp_execute_reply sdp_execute_reply;
bool_t xdr_sdp_execute_reply();


struct Server_CollectionDesc {
	CS_FOUR nElements;
	CS_TupleID dataTid;
};
typedef struct Server_CollectionDesc Server_CollectionDesc;
bool_t xdr_Server_CollectionDesc();


struct lom_odmg_collection_createdata_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_createdata_reply lom_odmg_collection_createdata_reply;
bool_t xdr_lom_odmg_collection_createdata_reply();


struct lom_odmg_collection_createdata_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_createdata_arg lom_odmg_collection_createdata_arg;
bool_t xdr_lom_odmg_collection_createdata_arg();


struct lom_odmg_collection_destroydata_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_destroydata_reply lom_odmg_collection_destroydata_reply;
bool_t xdr_lom_odmg_collection_destroydata_reply();


struct lom_odmg_collection_destroydata_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_destroydata_arg lom_odmg_collection_destroydata_arg;
bool_t xdr_lom_odmg_collection_destroydata_arg();


struct lom_odmg_collection_getdescriptor_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_getdescriptor_reply lom_odmg_collection_getdescriptor_reply;
bool_t xdr_lom_odmg_collection_getdescriptor_reply();


struct lom_odmg_collection_getdescriptor_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_getdescriptor_arg lom_odmg_collection_getdescriptor_arg;
bool_t xdr_lom_odmg_collection_getdescriptor_arg();


struct lom_odmg_collection_setdescriptor_reply {
	CS_FOUR errCode;
};
typedef struct lom_odmg_collection_setdescriptor_reply lom_odmg_collection_setdescriptor_reply;
bool_t xdr_lom_odmg_collection_setdescriptor_reply();


struct lom_odmg_collection_setdescriptor_arg {
	long threadId;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_setdescriptor_arg lom_odmg_collection_setdescriptor_arg;
bool_t xdr_lom_odmg_collection_setdescriptor_arg();


struct lom_odmg_collection_assign_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_assign_reply lom_odmg_collection_assign_reply;
bool_t xdr_lom_odmg_collection_assign_reply();


struct lom_odmg_collection_assign_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
	CS_FOUR assignedOcnOrScanId;
	bool_t assignedUseScanFlag;
	CS_OID *assignedOid;
	CS_TWO assignedColNo;
	Server_CollectionDesc *assignedCollDesc;
};
typedef struct lom_odmg_collection_assign_arg lom_odmg_collection_assign_arg;
bool_t xdr_lom_odmg_collection_assign_arg();


struct lom_odmg_collection_assignelements_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_assignelements_reply lom_odmg_collection_assignelements_reply;
bool_t xdr_lom_odmg_collection_assignelements_reply();


struct lom_odmg_collection_assignelements_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_assignelements_arg lom_odmg_collection_assignelements_arg;
bool_t xdr_lom_odmg_collection_assignelements_arg();


struct lom_odmg_collection_insertelements_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_insertelements_reply lom_odmg_collection_insertelements_reply;
bool_t xdr_lom_odmg_collection_insertelements_reply();


struct lom_odmg_collection_insertelements_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR ith;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_insertelements_arg lom_odmg_collection_insertelements_arg;
bool_t xdr_lom_odmg_collection_insertelements_arg();


struct lom_odmg_collection_deleteelements_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_deleteelements_reply lom_odmg_collection_deleteelements_reply;
bool_t xdr_lom_odmg_collection_deleteelements_reply();


struct lom_odmg_collection_deleteelements_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR ith;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_deleteelements_arg lom_odmg_collection_deleteelements_arg;
bool_t xdr_lom_odmg_collection_deleteelements_arg();


struct lom_odmg_collection_deleteall_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_deleteall_reply lom_odmg_collection_deleteall_reply;
bool_t xdr_lom_odmg_collection_deleteall_reply();


struct lom_odmg_collection_deleteall_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_deleteall_arg lom_odmg_collection_deleteall_arg;
bool_t xdr_lom_odmg_collection_deleteall_arg();


struct lom_odmg_collection_ismember_reply {
	CS_FOUR ith;
	CS_FOUR errCode;
};
typedef struct lom_odmg_collection_ismember_reply lom_odmg_collection_ismember_reply;
bool_t xdr_lom_odmg_collection_ismember_reply();


struct lom_odmg_collection_ismember_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_ismember_arg lom_odmg_collection_ismember_arg;
bool_t xdr_lom_odmg_collection_ismember_arg();


struct lom_odmg_collection_isequal_reply {
	CS_FOUR errCode;
};
typedef struct lom_odmg_collection_isequal_reply lom_odmg_collection_isequal_reply;
bool_t xdr_lom_odmg_collection_isequal_reply();


struct lom_odmg_collection_isequal_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
	CS_FOUR comparedOcnOrScanId;
	bool_t comparedUseScanFlag;
	CS_OID *comparedOid;
	CS_TWO comparedColNo;
	Server_CollectionDesc *comparedCollDesc;
};
typedef struct lom_odmg_collection_isequal_arg lom_odmg_collection_isequal_arg;
bool_t xdr_lom_odmg_collection_isequal_arg();


struct lom_odmg_collection_issubset_reply {
	CS_FOUR errCode;
};
typedef struct lom_odmg_collection_issubset_reply lom_odmg_collection_issubset_reply;
bool_t xdr_lom_odmg_collection_issubset_reply();


struct lom_odmg_collection_issubset_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
	CS_FOUR comparedOcnOrScanId;
	bool_t comparedUseScanFlag;
	CS_OID *comparedOid;
	CS_TWO comparedColNo;
	Server_CollectionDesc *comparedCollDesc;
};
typedef struct lom_odmg_collection_issubset_arg lom_odmg_collection_issubset_arg;
bool_t xdr_lom_odmg_collection_issubset_arg();


struct lom_odmg_collection_union_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_union_reply lom_odmg_collection_union_reply;
bool_t xdr_lom_odmg_collection_union_reply();


struct lom_odmg_collection_union_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanIdA;
	bool_t useScanFlagA;
	CS_OID *oidA;
	CS_TWO colNoA;
	Server_CollectionDesc *colDescA;
	CS_FOUR ocnOrScanIdB;
	bool_t useScanFlagB;
	CS_OID *oidB;
	CS_TWO colNoB;
	Server_CollectionDesc *colDescB;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_union_arg lom_odmg_collection_union_arg;
bool_t xdr_lom_odmg_collection_union_arg();


struct lom_odmg_collection_intersect_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_intersect_reply lom_odmg_collection_intersect_reply;
bool_t xdr_lom_odmg_collection_intersect_reply();


struct lom_odmg_collection_intersect_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanIdA;
	bool_t useScanFlagA;
	CS_OID *oidA;
	CS_TWO colNoA;
	Server_CollectionDesc *colDescA;
	CS_FOUR ocnOrScanIdB;
	bool_t useScanFlagB;
	CS_OID *oidB;
	CS_TWO colNoB;
	Server_CollectionDesc *colDescB;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_intersect_arg lom_odmg_collection_intersect_arg;
bool_t xdr_lom_odmg_collection_intersect_arg();


struct lom_odmg_collection_difference_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_difference_reply lom_odmg_collection_difference_reply;
bool_t xdr_lom_odmg_collection_difference_reply();


struct lom_odmg_collection_difference_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanIdA;
	bool_t useScanFlagA;
	CS_OID *oidA;
	CS_TWO colNoA;
	Server_CollectionDesc *colDescA;
	CS_FOUR ocnOrScanIdB;
	bool_t useScanFlagB;
	CS_OID *oidB;
	CS_TWO colNoB;
	Server_CollectionDesc *colDescB;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_difference_arg lom_odmg_collection_difference_arg;
bool_t xdr_lom_odmg_collection_difference_arg();


struct lom_odmg_collection_unionwith_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_unionwith_reply lom_odmg_collection_unionwith_reply;
bool_t xdr_lom_odmg_collection_unionwith_reply();


struct lom_odmg_collection_unionwith_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanIdA;
	bool_t useScanFlagA;
	CS_OID *oidA;
	CS_TWO colNoA;
	Server_CollectionDesc *colDescA;
	CS_FOUR ocnOrScanIdB;
	bool_t useScanFlagB;
	CS_OID *oidB;
	CS_TWO colNoB;
	Server_CollectionDesc *colDescB;
};
typedef struct lom_odmg_collection_unionwith_arg lom_odmg_collection_unionwith_arg;
bool_t xdr_lom_odmg_collection_unionwith_arg();


struct lom_odmg_collection_intersectwith_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_intersectwith_reply lom_odmg_collection_intersectwith_reply;
bool_t xdr_lom_odmg_collection_intersectwith_reply();


struct lom_odmg_collection_intersectwith_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanIdA;
	bool_t useScanFlagA;
	CS_OID *oidA;
	CS_TWO colNoA;
	Server_CollectionDesc *colDescA;
	CS_FOUR ocnOrScanIdB;
	bool_t useScanFlagB;
	CS_OID *oidB;
	CS_TWO colNoB;
	Server_CollectionDesc *colDescB;
};
typedef struct lom_odmg_collection_intersectwith_arg lom_odmg_collection_intersectwith_arg;
bool_t xdr_lom_odmg_collection_intersectwith_arg();


struct lom_odmg_collection_differencewith_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_differencewith_reply lom_odmg_collection_differencewith_reply;
bool_t xdr_lom_odmg_collection_differencewith_reply();


struct lom_odmg_collection_differencewith_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanIdA;
	bool_t useScanFlagA;
	CS_OID *oidA;
	CS_TWO colNoA;
	Server_CollectionDesc *colDescA;
	CS_FOUR ocnOrScanIdB;
	bool_t useScanFlagB;
	CS_OID *oidB;
	CS_TWO colNoB;
	Server_CollectionDesc *colDescB;
};
typedef struct lom_odmg_collection_differencewith_arg lom_odmg_collection_differencewith_arg;
bool_t xdr_lom_odmg_collection_differencewith_arg();


struct lom_odmg_collection_appendelements_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_appendelements_reply lom_odmg_collection_appendelements_reply;
bool_t xdr_lom_odmg_collection_appendelements_reply();


struct lom_odmg_collection_appendelements_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR ith;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_appendelements_arg lom_odmg_collection_appendelements_arg;
bool_t xdr_lom_odmg_collection_appendelements_arg();


struct lom_odmg_collection_retrieveelements_reply {
	CS_FOUR errCode;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
};
typedef struct lom_odmg_collection_retrieveelements_reply lom_odmg_collection_retrieveelements_reply;
bool_t xdr_lom_odmg_collection_retrieveelements_reply();


struct lom_odmg_collection_retrieveelements_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR ith;
	Server_CollectionDesc *colDesc;
	CS_FOUR nElements;
	CS_FOUR sizeOfElements;
};
typedef struct lom_odmg_collection_retrieveelements_arg lom_odmg_collection_retrieveelements_arg;
bool_t xdr_lom_odmg_collection_retrieveelements_arg();


struct lom_odmg_collection_updateelements_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_updateelements_reply lom_odmg_collection_updateelements_reply;
bool_t xdr_lom_odmg_collection_updateelements_reply();


struct lom_odmg_collection_updateelements_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR ith;
	CS_FOUR nElements;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_updateelements_arg lom_odmg_collection_updateelements_arg;
bool_t xdr_lom_odmg_collection_updateelements_arg();


struct lom_odmg_collection_concatenate_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_concatenate_reply lom_odmg_collection_concatenate_reply;
bool_t xdr_lom_odmg_collection_concatenate_reply();


struct lom_odmg_collection_concatenate_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanIdA;
	bool_t useScanFlagA;
	CS_OID *oidA;
	CS_TWO colNoA;
	Server_CollectionDesc *colDescA;
	CS_FOUR ocnOrScanIdB;
	bool_t useScanFlagB;
	CS_OID *oidB;
	CS_TWO colNoB;
	Server_CollectionDesc *colDescB;
};
typedef struct lom_odmg_collection_concatenate_arg lom_odmg_collection_concatenate_arg;
bool_t xdr_lom_odmg_collection_concatenate_arg();


struct lom_odmg_collection_resize_reply {
	CS_FOUR errCode;
	Server_CollectionDesc colDesc;
};
typedef struct lom_odmg_collection_resize_reply lom_odmg_collection_resize_reply;
bool_t xdr_lom_odmg_collection_resize_reply();


struct lom_odmg_collection_resize_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	CS_FOUR size;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_resize_arg lom_odmg_collection_resize_arg;
bool_t xdr_lom_odmg_collection_resize_arg();


struct lom_odmg_collection_scan_open_reply {
	CS_FOUR errCode;
};
typedef struct lom_odmg_collection_scan_open_reply lom_odmg_collection_scan_open_reply;
bool_t xdr_lom_odmg_collection_scan_open_reply();


struct lom_odmg_collection_scan_open_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR ocnOrScanId;
	bool_t useScanFlag;
	CS_OID *oid;
	CS_TWO colNo;
	Server_CollectionDesc *colDesc;
};
typedef struct lom_odmg_collection_scan_open_arg lom_odmg_collection_scan_open_arg;
bool_t xdr_lom_odmg_collection_scan_open_arg();


struct lom_odmg_collection_scan_close_reply {
	CS_FOUR errCode;
};
typedef struct lom_odmg_collection_scan_close_reply lom_odmg_collection_scan_close_reply;
bool_t xdr_lom_odmg_collection_scan_close_reply();


struct lom_odmg_collection_scan_close_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR collectionScanId;
};
typedef struct lom_odmg_collection_scan_close_arg lom_odmg_collection_scan_close_arg;
bool_t xdr_lom_odmg_collection_scan_close_arg();


struct lom_odmg_collection_scan_nextelements_reply {
	CS_FOUR errCode;
	struct {
		u_int elementSizes_len;
		long *elementSizes_val;
	} elementSizes;
	CS_FOUR sizeofElements;
	struct {
		u_int elements_len;
		char *elements_val;
	} elements;
};
typedef struct lom_odmg_collection_scan_nextelements_reply lom_odmg_collection_scan_nextelements_reply;
bool_t xdr_lom_odmg_collection_scan_nextelements_reply();


struct lom_odmg_collection_scan_nextelements_arg {
	long threadId;
	CS_FOUR collectionKind;
	CS_FOUR collectionScanId;
	CS_FOUR nElements;
};
typedef struct lom_odmg_collection_scan_nextelements_arg lom_odmg_collection_scan_nextelements_arg;
bool_t xdr_lom_odmg_collection_scan_nextelements_arg();


#define ODYSSEUS_RPC ((u_long)0x20000000)
#define ODYSSEUS_RPC_VERSION ((u_long)1)
#define SERVER_CATALOG_GETCLASSINFO ((u_long)3538)
#if defined(ODYS_SERVER)
extern catalog_getclassinfo_reply *server_catalog_getclassinfo_1(catalog_getclassinfo_arg *);
#elif defined(ODYS_CLIENT)
extern catalog_getclassinfo_reply *server_catalog_getclassinfo_1(catalog_getclassinfo_arg *, CLIENT *);
#else 
extern catalog_getclassinfo_reply *server_catalog_getclassinfo_1();
#endif
#define SERVER_LOM_CREATETHREAD ((u_long)3541)
#if defined(ODYS_SERVER)
extern lom_createthread_reply *server_lom_createthread_1(lom_createthread_arg *);
#elif defined(ODYS_CLIENT)
extern lom_createthread_reply *server_lom_createthread_1(lom_createthread_arg *, CLIENT *);
#else 
extern lom_createthread_reply *server_lom_createthread_1();
#endif
#define SERVER_LOM_DESTROYTHREAD ((u_long)3542)
#if defined(ODYS_SERVER)
extern lom_destroythread_reply *server_lom_destroythread_1(lom_destroythread_arg *);
#elif defined(ODYS_CLIENT)
extern lom_destroythread_reply *server_lom_destroythread_1(lom_destroythread_arg *, CLIENT *);
#else 
extern lom_destroythread_reply *server_lom_destroythread_1();
#endif
#define SERVER_LOM_MOUNT ((u_long)3545)
#if defined(ODYS_SERVER)
extern lom_mount_reply *server_lom_mount_1(lom_mount_arg *);
#elif defined(ODYS_CLIENT)
extern lom_mount_reply *server_lom_mount_1(lom_mount_arg *, CLIENT *);
#else 
extern lom_mount_reply *server_lom_mount_1();
#endif
#define SERVER_LOM_DISMOUNT ((u_long)3546)
#if defined(ODYS_SERVER)
extern lom_dismount_reply *server_lom_dismount_1(lom_dismount_arg *);
#elif defined(ODYS_CLIENT)
extern lom_dismount_reply *server_lom_dismount_1(lom_dismount_arg *, CLIENT *);
#else 
extern lom_dismount_reply *server_lom_dismount_1();
#endif
#define SERVER_LOM_BEGINTRANS ((u_long)3549)
#if defined(ODYS_SERVER)
extern lom_begintrans_reply *server_lom_begintrans_1(lom_begintrans_arg *);
#elif defined(ODYS_CLIENT)
extern lom_begintrans_reply *server_lom_begintrans_1(lom_begintrans_arg *, CLIENT *);
#else 
extern lom_begintrans_reply *server_lom_begintrans_1();
#endif
#define SERVER_LOM_COMMITTRANS ((u_long)3550)
#if defined(ODYS_SERVER)
extern lom_committrans_reply *server_lom_committrans_1(lom_committrans_arg *);
#elif defined(ODYS_CLIENT)
extern lom_committrans_reply *server_lom_committrans_1(lom_committrans_arg *, CLIENT *);
#else 
extern lom_committrans_reply *server_lom_committrans_1();
#endif
#define SERVER_LOM_ABORTTRANS ((u_long)3551)
#if defined(ODYS_SERVER)
extern lom_aborttrans_reply *server_lom_aborttrans_1(lom_aborttrans_arg *);
#elif defined(ODYS_CLIENT)
extern lom_aborttrans_reply *server_lom_aborttrans_1(lom_aborttrans_arg *, CLIENT *);
#else 
extern lom_aborttrans_reply *server_lom_aborttrans_1();
#endif
#define SERVER_LOM_ADDINDEX ((u_long)3554)
#if defined(ODYS_SERVER)
extern lom_addindex_reply *server_lom_addindex_1(lom_addindex_arg *);
#elif defined(ODYS_CLIENT)
extern lom_addindex_reply *server_lom_addindex_1(lom_addindex_arg *, CLIENT *);
#else 
extern lom_addindex_reply *server_lom_addindex_1();
#endif
#define SERVER_LOM_DROPINDEX ((u_long)3555)
#if defined(ODYS_SERVER)
extern lom_dropindex_reply *server_lom_dropindex_1(lom_dropindex_arg *);
#elif defined(ODYS_CLIENT)
extern lom_dropindex_reply *server_lom_dropindex_1(lom_dropindex_arg *, CLIENT *);
#else 
extern lom_dropindex_reply *server_lom_dropindex_1();
#endif
#define SERVER_LOM_OPENCLASS ((u_long)3558)
#if defined(ODYS_SERVER)
extern lom_openclass_reply *server_lom_openclass_1(lom_openclass_arg *);
#elif defined(ODYS_CLIENT)
extern lom_openclass_reply *server_lom_openclass_1(lom_openclass_arg *, CLIENT *);
#else 
extern lom_openclass_reply *server_lom_openclass_1();
#endif
#define SERVER_LOM_CLOSECLASS ((u_long)3559)
#if defined(ODYS_SERVER)
extern lom_closeclass_reply *server_lom_closeclass_1(lom_closeclass_arg *);
#elif defined(ODYS_CLIENT)
extern lom_closeclass_reply *server_lom_closeclass_1(lom_closeclass_arg *, CLIENT *);
#else 
extern lom_closeclass_reply *server_lom_closeclass_1();
#endif
#define SERVER_LOM_CLOSESCAN ((u_long)3562)
#if defined(ODYS_SERVER)
extern lom_closescan_reply *server_lom_closescan_1(lom_closescan_arg *);
#elif defined(ODYS_CLIENT)
extern lom_closescan_reply *server_lom_closescan_1(lom_closescan_arg *, CLIENT *);
#else 
extern lom_closescan_reply *server_lom_closescan_1();
#endif
#define SERVER_LOM_OPENINDEXSCAN ((u_long)3563)
#if defined(ODYS_SERVER)
extern lom_openindexscan_reply *server_lom_openindexscan_1(lom_openindexscan_arg *);
#elif defined(ODYS_CLIENT)
extern lom_openindexscan_reply *server_lom_openindexscan_1(lom_openindexscan_arg *, CLIENT *);
#else 
extern lom_openindexscan_reply *server_lom_openindexscan_1();
#endif
#define SERVER_LOM_OPENSEQSCAN ((u_long)3564)
#if defined(ODYS_SERVER)
extern lom_openseqscan_reply *server_lom_openseqscan_1(lom_openseqscan_arg *);
#elif defined(ODYS_CLIENT)
extern lom_openseqscan_reply *server_lom_openseqscan_1(lom_openseqscan_arg *, CLIENT *);
#else 
extern lom_openseqscan_reply *server_lom_openseqscan_1();
#endif
#define SERVER_LOM_NEXTOBJECT ((u_long)3565)
#if defined(ODYS_SERVER)
extern lom_nextobject_reply *server_lom_nextobject_1(lom_nextobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_nextobject_reply *server_lom_nextobject_1(lom_nextobject_arg *, CLIENT *);
#else 
extern lom_nextobject_reply *server_lom_nextobject_1();
#endif
#define SERVER_LOM_CREATECLASS ((u_long)3568)
#if defined(ODYS_SERVER)
extern lom_createclass_reply *server_lom_createclass_1(lom_createclass_arg *);
#elif defined(ODYS_CLIENT)
extern lom_createclass_reply *server_lom_createclass_1(lom_createclass_arg *, CLIENT *);
#else 
extern lom_createclass_reply *server_lom_createclass_1();
#endif
#define SERVER_LOM_DESTROYCLASS ((u_long)3569)
#if defined(ODYS_SERVER)
extern lom_destroyclass_reply *server_lom_destroyclass_1(lom_destroyclass_arg *);
#elif defined(ODYS_CLIENT)
extern lom_destroyclass_reply *server_lom_destroyclass_1(lom_destroyclass_arg *, CLIENT *);
#else 
extern lom_destroyclass_reply *server_lom_destroyclass_1();
#endif
#define SERVER_LOM_GETCLASSID ((u_long)3570)
#if defined(ODYS_SERVER)
extern lom_getclassid_reply *server_lom_getclassid_1(lom_getclassid_arg *);
#elif defined(ODYS_CLIENT)
extern lom_getclassid_reply *server_lom_getclassid_1(lom_getclassid_arg *, CLIENT *);
#else 
extern lom_getclassid_reply *server_lom_getclassid_1();
#endif
#define SERVER_LOM_GETNEWCLASSID ((u_long)3571)
#if defined(ODYS_SERVER)
extern lom_getnewclassid_reply *server_lom_getnewclassid_1(lom_getnewclassid_arg *);
#elif defined(ODYS_CLIENT)
extern lom_getnewclassid_reply *server_lom_getnewclassid_1(lom_getnewclassid_arg *, CLIENT *);
#else 
extern lom_getnewclassid_reply *server_lom_getnewclassid_1();
#endif
#define SERVER_LOM_GETCLASSNAME ((u_long)3572)
#if defined(ODYS_SERVER)
extern lom_getclassname_reply *server_lom_getclassname_1(lom_getclassname_arg *);
#elif defined(ODYS_CLIENT)
extern lom_getclassname_reply *server_lom_getclassname_1(lom_getclassname_arg *, CLIENT *);
#else 
extern lom_getclassname_reply *server_lom_getclassname_1();
#endif
#define SERVER_LOM_GETSUBCLASSES ((u_long)3573)
#if defined(ODYS_SERVER)
extern lom_getsubclasses_reply *server_lom_getsubclasses_1(lom_getsubclasses_arg *);
#elif defined(ODYS_CLIENT)
extern lom_getsubclasses_reply *server_lom_getsubclasses_1(lom_getsubclasses_arg *, CLIENT *);
#else 
extern lom_getsubclasses_reply *server_lom_getsubclasses_1();
#endif
#define SERVER_LOM_CREATEOBJECT ((u_long)3576)
#if defined(ODYS_SERVER)
extern lom_createobject_reply *server_lom_createobject_1(lom_createobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_createobject_reply *server_lom_createobject_1(lom_createobject_arg *, CLIENT *);
#else 
extern lom_createobject_reply *server_lom_createobject_1();
#endif
#define SERVER_LOM_UPDATEOBJECT ((u_long)3577)
#if defined(ODYS_SERVER)
extern lom_updateobject_reply *server_lom_updateobject_1(lom_updateobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_updateobject_reply *server_lom_updateobject_1(lom_updateobject_arg *, CLIENT *);
#else 
extern lom_updateobject_reply *server_lom_updateobject_1();
#endif
#define SERVER_LOM_FETCHOBJECT ((u_long)3578)
#if defined(ODYS_SERVER)
extern lom_fetchobject_reply *server_lom_fetchobject_1(lom_fetchobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_fetchobject_reply *server_lom_fetchobject_1(lom_fetchobject_arg *, CLIENT *);
#else 
extern lom_fetchobject_reply *server_lom_fetchobject_1();
#endif
#define SERVER_LOM_FETCHOBJECT2 ((u_long)3579)
#if defined(ODYS_SERVER)
extern lom_fetchobject2_reply *server_lom_fetchobject2_1(lom_fetchobject2_arg *);
#elif defined(ODYS_CLIENT)
extern lom_fetchobject2_reply *server_lom_fetchobject2_1(lom_fetchobject2_arg *, CLIENT *);
#else 
extern lom_fetchobject2_reply *server_lom_fetchobject2_1();
#endif
#define SERVER_LOM_DESTROYOBJECT ((u_long)3580)
#if defined(ODYS_SERVER)
extern lom_destroyobject_reply *server_lom_destroyobject_1(lom_destroyobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_destroyobject_reply *server_lom_destroyobject_1(lom_destroyobject_arg *, CLIENT *);
#else 
extern lom_destroyobject_reply *server_lom_destroyobject_1();
#endif
#define SERVER_LOM_CREATELARGEOBJECT ((u_long)3583)
#if defined(ODYS_SERVER)
extern lom_createlargeobject_reply *server_lom_createlargeobject_1(lom_createlargeobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_createlargeobject_reply *server_lom_createlargeobject_1(lom_createlargeobject_arg *, CLIENT *);
#else 
extern lom_createlargeobject_reply *server_lom_createlargeobject_1();
#endif
#define SERVER_LOM_UPDATELARGEOBJECT ((u_long)3584)
#if defined(ODYS_SERVER)
extern lom_updatelargeobject_reply *server_lom_updatelargeobject_1(lom_updatelargeobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_updatelargeobject_reply *server_lom_updatelargeobject_1(lom_updatelargeobject_arg *, CLIENT *);
#else 
extern lom_updatelargeobject_reply *server_lom_updatelargeobject_1();
#endif
#define SERVER_LOM_FETCHLARGEOBJECT ((u_long)3585)
#if defined(ODYS_SERVER)
extern lom_fetchlargeobject_reply *server_lom_fetchlargeobject_1(lom_fetchlargeobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_fetchlargeobject_reply *server_lom_fetchlargeobject_1(lom_fetchlargeobject_arg *, CLIENT *);
#else 
extern lom_fetchlargeobject_reply *server_lom_fetchlargeobject_1();
#endif
#define SERVER_LOM_SET_CREATE ((u_long)3588)
#if defined(ODYS_SERVER)
extern lom_set_create_reply *server_lom_set_create_1(lom_set_create_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_create_reply *server_lom_set_create_1(lom_set_create_arg *, CLIENT *);
#else 
extern lom_set_create_reply *server_lom_set_create_1();
#endif
#define SERVER_LOM_SET_DESTROY ((u_long)3589)
#if defined(ODYS_SERVER)
extern lom_set_destroy_reply *server_lom_set_destroy_1(lom_set_destroy_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_destroy_reply *server_lom_set_destroy_1(lom_set_destroy_arg *, CLIENT *);
#else 
extern lom_set_destroy_reply *server_lom_set_destroy_1();
#endif
#define SERVER_LOM_SET_INSERTELEMENTS ((u_long)3590)
#if defined(ODYS_SERVER)
extern lom_set_insertelements_reply *server_lom_set_insertelements_1(lom_set_insertelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_insertelements_reply *server_lom_set_insertelements_1(lom_set_insertelements_arg *, CLIENT *);
#else 
extern lom_set_insertelements_reply *server_lom_set_insertelements_1();
#endif
#define SERVER_LOM_SET_DELETEELEMENTS ((u_long)3591)
#if defined(ODYS_SERVER)
extern lom_set_deleteelements_reply *server_lom_set_deleteelements_1(lom_set_deleteelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_deleteelements_reply *server_lom_set_deleteelements_1(lom_set_deleteelements_arg *, CLIENT *);
#else 
extern lom_set_deleteelements_reply *server_lom_set_deleteelements_1();
#endif
#define SERVER_LOM_SET_ISMEMBER ((u_long)3592)
#if defined(ODYS_SERVER)
extern lom_set_ismember_reply *server_lom_set_ismember_1(lom_set_ismember_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_ismember_reply *server_lom_set_ismember_1(lom_set_ismember_arg *, CLIENT *);
#else 
extern lom_set_ismember_reply *server_lom_set_ismember_1();
#endif
#define SERVER_LOM_SET_SCAN_OPEN ((u_long)3593)
#if defined(ODYS_SERVER)
extern lom_set_scan_open_reply *server_lom_set_scan_open_1(lom_set_scan_open_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_scan_open_reply *server_lom_set_scan_open_1(lom_set_scan_open_arg *, CLIENT *);
#else 
extern lom_set_scan_open_reply *server_lom_set_scan_open_1();
#endif
#define SERVER_LOM_SET_SCAN_CLOSE ((u_long)3594)
#if defined(ODYS_SERVER)
extern lom_set_scan_close_reply *server_lom_set_scan_close_1(lom_set_scan_close_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_scan_close_reply *server_lom_set_scan_close_1(lom_set_scan_close_arg *, CLIENT *);
#else 
extern lom_set_scan_close_reply *server_lom_set_scan_close_1();
#endif
#define SERVER_LOM_SET_SCAN_NEXT ((u_long)3595)
#if defined(ODYS_SERVER)
extern lom_set_scan_next_reply *server_lom_set_scan_next_1(lom_set_scan_next_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_scan_next_reply *server_lom_set_scan_next_1(lom_set_scan_next_arg *, CLIENT *);
#else 
extern lom_set_scan_next_reply *server_lom_set_scan_next_1();
#endif
#define SERVER_LOM_SET_SCAN_INSERT ((u_long)3596)
#if defined(ODYS_SERVER)
extern lom_set_scan_insert_reply *server_lom_set_scan_insert_1(lom_set_scan_insert_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_scan_insert_reply *server_lom_set_scan_insert_1(lom_set_scan_insert_arg *, CLIENT *);
#else 
extern lom_set_scan_insert_reply *server_lom_set_scan_insert_1();
#endif
#define SERVER_LOM_SET_SCAN_DELETE ((u_long)3597)
#if defined(ODYS_SERVER)
extern lom_set_scan_delete_reply *server_lom_set_scan_delete_1(lom_set_scan_delete_arg *);
#elif defined(ODYS_CLIENT)
extern lom_set_scan_delete_reply *server_lom_set_scan_delete_1(lom_set_scan_delete_arg *, CLIENT *);
#else 
extern lom_set_scan_delete_reply *server_lom_set_scan_delete_1();
#endif
#define SERVER_LOM_RS_CREATE ((u_long)3600)
#if defined(ODYS_SERVER)
extern lom_rs_create_reply *server_lom_rs_create_1(lom_rs_create_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_create_reply *server_lom_rs_create_1(lom_rs_create_arg *, CLIENT *);
#else 
extern lom_rs_create_reply *server_lom_rs_create_1();
#endif
#define SERVER_LOM_RS_DESTROY ((u_long)3601)
#if defined(ODYS_SERVER)
extern lom_rs_destroy_reply *server_lom_rs_destroy_1(lom_rs_destroy_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_destroy_reply *server_lom_rs_destroy_1(lom_rs_destroy_arg *, CLIENT *);
#else 
extern lom_rs_destroy_reply *server_lom_rs_destroy_1();
#endif
#define SERVER_LOM_RS_CREATEINSTANCE ((u_long)3602)
#if defined(ODYS_SERVER)
extern lom_rs_createinstance_reply *server_lom_rs_createinstance_1(lom_rs_createinstance_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_createinstance_reply *server_lom_rs_createinstance_1(lom_rs_createinstance_arg *, CLIENT *);
#else 
extern lom_rs_createinstance_reply *server_lom_rs_createinstance_1();
#endif
#define SERVER_LOM_RS_DESTROYINSTANCE ((u_long)3603)
#if defined(ODYS_SERVER)
extern lom_rs_destroyinstance_reply *server_lom_rs_destroyinstance_1(lom_rs_destroyinstance_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_destroyinstance_reply *server_lom_rs_destroyinstance_1(lom_rs_destroyinstance_arg *, CLIENT *);
#else 
extern lom_rs_destroyinstance_reply *server_lom_rs_destroyinstance_1();
#endif
#define SERVER_LOM_RS_GETID ((u_long)3604)
#if defined(ODYS_SERVER)
extern lom_rs_getid_reply *server_lom_rs_getid_1(lom_rs_getid_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_getid_reply *server_lom_rs_getid_1(lom_rs_getid_arg *, CLIENT *);
#else 
extern lom_rs_getid_reply *server_lom_rs_getid_1();
#endif
#define SERVER_LOM_RS_OPENSCAN ((u_long)3605)
#if defined(ODYS_SERVER)
extern lom_rs_openscan_reply *server_lom_rs_openscan_1(lom_rs_openscan_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_openscan_reply *server_lom_rs_openscan_1(lom_rs_openscan_arg *, CLIENT *);
#else 
extern lom_rs_openscan_reply *server_lom_rs_openscan_1();
#endif
#define SERVER_LOM_RS_CLOSESCAN ((u_long)3606)
#if defined(ODYS_SERVER)
extern lom_rs_closescan_reply *server_lom_rs_closescan_1(lom_rs_closescan_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_closescan_reply *server_lom_rs_closescan_1(lom_rs_closescan_arg *, CLIENT *);
#else 
extern lom_rs_closescan_reply *server_lom_rs_closescan_1();
#endif
#define SERVER_LOM_RS_NEXTINSTANCES ((u_long)3607)
#if defined(ODYS_SERVER)
extern lom_rs_nextinstances_reply *server_lom_rs_nextinstances_1(lom_rs_nextinstances_arg *);
#elif defined(ODYS_CLIENT)
extern lom_rs_nextinstances_reply *server_lom_rs_nextinstances_1(lom_rs_nextinstances_arg *, CLIENT *);
#else 
extern lom_rs_nextinstances_reply *server_lom_rs_nextinstances_1();
#endif
#define SERVER_LOM_TEXT_CREATECONTENT ((u_long)3610)
#if defined(ODYS_SERVER)
extern lom_text_createcontent_reply *server_lom_text_createcontent_1(lom_text_createcontent_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_createcontent_reply *server_lom_text_createcontent_1(lom_text_createcontent_arg *, CLIENT *);
#else 
extern lom_text_createcontent_reply *server_lom_text_createcontent_1();
#endif
#define SERVER_LOM_TEXT_DESTROYCONTENT ((u_long)3611)
#if defined(ODYS_SERVER)
extern lom_text_destroycontent_reply *server_lom_text_destroycontent_1(lom_text_destroycontent_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_destroycontent_reply *server_lom_text_destroycontent_1(lom_text_destroycontent_arg *, CLIENT *);
#else 
extern lom_text_destroycontent_reply *server_lom_text_destroycontent_1();
#endif
#define SERVER_LOM_TEXT_FETCHCONTENT ((u_long)3612)
#if defined(ODYS_SERVER)
extern lom_text_fetchcontent_reply *server_lom_text_fetchcontent_1(lom_text_fetchcontent_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_fetchcontent_reply *server_lom_text_fetchcontent_1(lom_text_fetchcontent_arg *, CLIENT *);
#else 
extern lom_text_fetchcontent_reply *server_lom_text_fetchcontent_1();
#endif
#define SERVER_LOM_TEXT_UPDATECONTENT ((u_long)3613)
#if defined(ODYS_SERVER)
extern lom_text_updatecontent_reply *server_lom_text_updatecontent_1(lom_text_updatecontent_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_updatecontent_reply *server_lom_text_updatecontent_1(lom_text_updatecontent_arg *, CLIENT *);
#else 
extern lom_text_updatecontent_reply *server_lom_text_updatecontent_1();
#endif
#define SERVER_LOM_TEXT_MAKEINDEX ((u_long)3614)
#if defined(ODYS_SERVER)
extern lom_text_makeindex_reply *server_lom_text_makeindex_1(lom_text_makeindex_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_makeindex_reply *server_lom_text_makeindex_1(lom_text_makeindex_arg *, CLIENT *);
#else 
extern lom_text_makeindex_reply *server_lom_text_makeindex_1();
#endif
#define SERVER_LOM_TEXT_GETDESCRIPTOR ((u_long)3616)
#if defined(ODYS_SERVER)
extern lom_text_getdescriptor_reply *server_lom_text_getdescriptor_1(lom_text_getdescriptor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getdescriptor_reply *server_lom_text_getdescriptor_1(lom_text_getdescriptor_arg *, CLIENT *);
#else 
extern lom_text_getdescriptor_reply *server_lom_text_getdescriptor_1();
#endif
#define SERVER_LOM_TEXT_OPENINDEXSCAN ((u_long)3617)
#if defined(ODYS_SERVER)
extern lom_text_openindexscan_reply *server_lom_text_openindexscan_1(lom_text_openindexscan_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_openindexscan_reply *server_lom_text_openindexscan_1(lom_text_openindexscan_arg *, CLIENT *);
#else 
extern lom_text_openindexscan_reply *server_lom_text_openindexscan_1();
#endif
#define SERVER_LOM_TEXT_SCAN_OPEN ((u_long)3618)
#if defined(ODYS_SERVER)
extern lom_text_scan_open_reply *server_lom_text_scan_open_1(lom_text_scan_open_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_scan_open_reply *server_lom_text_scan_open_1(lom_text_scan_open_arg *, CLIENT *);
#else 
extern lom_text_scan_open_reply *server_lom_text_scan_open_1();
#endif
#define SERVER_LOM_TEXT_SCAN_CLOSE ((u_long)3619)
#if defined(ODYS_SERVER)
extern lom_text_scan_close_reply *server_lom_text_scan_close_1(lom_text_scan_close_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_scan_close_reply *server_lom_text_scan_close_1(lom_text_scan_close_arg *, CLIENT *);
#else 
extern lom_text_scan_close_reply *server_lom_text_scan_close_1();
#endif
#define SERVER_LOM_TEXT_SCAN_NEXTPOSTING ((u_long)3620)
#if defined(ODYS_SERVER)
extern lom_text_scan_nextposting_reply *server_lom_text_scan_nextposting_1(lom_text_scan_nextposting_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_scan_nextposting_reply *server_lom_text_scan_nextposting_1(lom_text_scan_nextposting_arg *, CLIENT *);
#else 
extern lom_text_scan_nextposting_reply *server_lom_text_scan_nextposting_1();
#endif
#define SERVER_LOM_TEXT_NEXTPOSTINGS ((u_long)3621)
#if defined(ODYS_SERVER)
extern lom_text_nextpostings_reply *server_lom_text_nextpostings_1(lom_text_nextpostings_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_nextpostings_reply *server_lom_text_nextpostings_1(lom_text_nextpostings_arg *, CLIENT *);
#else 
extern lom_text_nextpostings_reply *server_lom_text_nextpostings_1();
#endif
#define SERVER_LOM_TEXT_GETCURSORKEYWORD ((u_long)3622)
#if defined(ODYS_SERVER)
extern lom_text_getcursorkeyword_reply *server_lom_text_getcursorkeyword_1(lom_text_getcursorkeyword_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getcursorkeyword_reply *server_lom_text_getcursorkeyword_1(lom_text_getcursorkeyword_arg *, CLIENT *);
#else 
extern lom_text_getcursorkeyword_reply *server_lom_text_getcursorkeyword_1();
#endif
#define SERVER_LOM_TEXT_GETINDEXID ((u_long)3623)
#if defined(ODYS_SERVER)
extern lom_text_getindexid_reply *server_lom_text_getindexid_1(lom_text_getindexid_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getindexid_reply *server_lom_text_getindexid_1(lom_text_getindexid_arg *, CLIENT *);
#else 
extern lom_text_getindexid_reply *server_lom_text_getindexid_1();
#endif
#define SERVER_LOM_TEXT_ADDFILTER ((u_long)3625)
#if defined(ODYS_SERVER)
extern lom_text_addfilter_reply *server_lom_text_addfilter_1(lom_text_addfilter_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_addfilter_reply *server_lom_text_addfilter_1(lom_text_addfilter_arg *, CLIENT *);
#else 
extern lom_text_addfilter_reply *server_lom_text_addfilter_1();
#endif
#define SERVER_LOM_TEXT_DROPFILTER ((u_long)3626)
#if defined(ODYS_SERVER)
extern lom_text_dropfilter_reply *server_lom_text_dropfilter_1(lom_text_dropfilter_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_dropfilter_reply *server_lom_text_dropfilter_1(lom_text_dropfilter_arg *, CLIENT *);
#else 
extern lom_text_dropfilter_reply *server_lom_text_dropfilter_1();
#endif
#define SERVER_LOM_TEXT_GETFILTERNO ((u_long)3627)
#if defined(ODYS_SERVER)
extern lom_text_getfilterno_reply *server_lom_text_getfilterno_1(lom_text_getfilterno_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getfilterno_reply *server_lom_text_getfilterno_1(lom_text_getfilterno_arg *, CLIENT *);
#else 
extern lom_text_getfilterno_reply *server_lom_text_getfilterno_1();
#endif
#define SERVER_LOM_TEXT_SETFILTER ((u_long)3628)
#if defined(ODYS_SERVER)
extern lom_text_setfilter_reply *server_lom_text_setfilter_1(lom_text_setfilter_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_setfilter_reply *server_lom_text_setfilter_1(lom_text_setfilter_arg *, CLIENT *);
#else 
extern lom_text_setfilter_reply *server_lom_text_setfilter_1();
#endif
#define SERVER_LOM_TEXT_GETFILTERINFO ((u_long)3629)
#if defined(ODYS_SERVER)
extern lom_text_getfilterinfo_reply *server_lom_text_getfilterinfo_1(lom_text_getfilterinfo_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getfilterinfo_reply *server_lom_text_getfilterinfo_1(lom_text_getfilterinfo_arg *, CLIENT *);
#else 
extern lom_text_getfilterinfo_reply *server_lom_text_getfilterinfo_1();
#endif
#define SERVER_LOM_TEXT_GETCURRENTFILTERNO ((u_long)3630)
#if defined(ODYS_SERVER)
extern lom_text_getcurrentfilterno_reply *server_lom_text_getcurrentfilterno_1(lom_text_getcurrentfilterno_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getcurrentfilterno_reply *server_lom_text_getcurrentfilterno_1(lom_text_getcurrentfilterno_arg *, CLIENT *);
#else 
extern lom_text_getcurrentfilterno_reply *server_lom_text_getcurrentfilterno_1();
#endif
#define SERVER_LOM_TEXT_RESETFILTER ((u_long)3631)
#if defined(ODYS_SERVER)
extern lom_text_resetfilter_reply *server_lom_text_resetfilter_1(lom_text_resetfilter_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_resetfilter_reply *server_lom_text_resetfilter_1(lom_text_resetfilter_arg *, CLIENT *);
#else 
extern lom_text_resetfilter_reply *server_lom_text_resetfilter_1();
#endif
#define SERVER_LOM_TEXT_ADDKEYWORDEXTRACTOR ((u_long)3632)
#if defined(ODYS_SERVER)
extern lom_text_addkeywordextractor_reply *server_lom_text_addkeywordextractor_1(lom_text_addkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_addkeywordextractor_reply *server_lom_text_addkeywordextractor_1(lom_text_addkeywordextractor_arg *, CLIENT *);
#else 
extern lom_text_addkeywordextractor_reply *server_lom_text_addkeywordextractor_1();
#endif
#define SERVER_LOM_TEXT_ADDDEFAULTKEYWORDEXTRACTOR ((u_long)3633)
#if defined(ODYS_SERVER)
extern lom_text_adddefaultkeywordextractor_reply *server_lom_text_adddefaultkeywordextractor_1(lom_text_adddefaultkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_adddefaultkeywordextractor_reply *server_lom_text_adddefaultkeywordextractor_1(lom_text_adddefaultkeywordextractor_arg *, CLIENT *);
#else 
extern lom_text_adddefaultkeywordextractor_reply *server_lom_text_adddefaultkeywordextractor_1();
#endif
#define SERVER_LOM_TEXT_DROPKEYWORDEXTRACTOR ((u_long)3634)
#if defined(ODYS_SERVER)
extern lom_text_dropkeywordextractor_reply *server_lom_text_dropkeywordextractor_1(lom_text_dropkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_dropkeywordextractor_reply *server_lom_text_dropkeywordextractor_1(lom_text_dropkeywordextractor_arg *, CLIENT *);
#else 
extern lom_text_dropkeywordextractor_reply *server_lom_text_dropkeywordextractor_1();
#endif
#define SERVER_LOM_TEXT_GETKEYWORDEXTRACTORNO ((u_long)3635)
#if defined(ODYS_SERVER)
extern lom_text_getkeywordextractorno_reply *server_lom_text_getkeywordextractorno_1(lom_text_getkeywordextractorno_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getkeywordextractorno_reply *server_lom_text_getkeywordextractorno_1(lom_text_getkeywordextractorno_arg *, CLIENT *);
#else 
extern lom_text_getkeywordextractorno_reply *server_lom_text_getkeywordextractorno_1();
#endif
#define SERVER_LOM_TEXT_SETKEYWORDEXTRACTOR ((u_long)3636)
#if defined(ODYS_SERVER)
extern lom_text_setkeywordextractor_reply *server_lom_text_setkeywordextractor_1(lom_text_setkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_setkeywordextractor_reply *server_lom_text_setkeywordextractor_1(lom_text_setkeywordextractor_arg *, CLIENT *);
#else 
extern lom_text_setkeywordextractor_reply *server_lom_text_setkeywordextractor_1();
#endif
#define SERVER_LOM_TEXT_GETKEYWORDEXTRACTORINFO ((u_long)3637)
#if defined(ODYS_SERVER)
extern lom_text_getkeywordextractorinfo_reply *server_lom_text_getkeywordextractorinfo_1(lom_text_getkeywordextractorinfo_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getkeywordextractorinfo_reply *server_lom_text_getkeywordextractorinfo_1(lom_text_getkeywordextractorinfo_arg *, CLIENT *);
#else 
extern lom_text_getkeywordextractorinfo_reply *server_lom_text_getkeywordextractorinfo_1();
#endif
#define SERVER_LOM_TEXT_GETCURRENTKEYWORDEXTRACTORNO ((u_long)3638)
#if defined(ODYS_SERVER)
extern lom_text_getcurrentkeywordextractorno_reply *server_lom_text_getcurrentkeywordextractorno_1(lom_text_getcurrentkeywordextractorno_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_getcurrentkeywordextractorno_reply *server_lom_text_getcurrentkeywordextractorno_1(lom_text_getcurrentkeywordextractorno_arg *, CLIENT *);
#else 
extern lom_text_getcurrentkeywordextractorno_reply *server_lom_text_getcurrentkeywordextractorno_1();
#endif
#define SERVER_LOM_TEXT_RESETKEYWORDEXTRACTOR ((u_long)3639)
#if defined(ODYS_SERVER)
extern lom_text_resetkeywordextractor_reply *server_lom_text_resetkeywordextractor_1(lom_text_resetkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_text_resetkeywordextractor_reply *server_lom_text_resetkeywordextractor_1(lom_text_resetkeywordextractor_arg *, CLIENT *);
#else 
extern lom_text_resetkeywordextractor_reply *server_lom_text_resetkeywordextractor_1();
#endif
#define SERVER_OOSQL_CREATESYSTEMHANDLE ((u_long)3643)
#if defined(ODYS_SERVER)
extern oosql_createsystemhandle_reply *server_oosql_createsystemhandle_1(oosql_createsystemhandle_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_createsystemhandle_reply *server_oosql_createsystemhandle_1(oosql_createsystemhandle_arg *, CLIENT *);
#else 
extern oosql_createsystemhandle_reply *server_oosql_createsystemhandle_1();
#endif
#define SERVER_OOSQL_DESTROYSYSTEMHANDLE ((u_long)3644)
#if defined(ODYS_SERVER)
extern oosql_destroysystemhandle_reply *server_oosql_destroysystemhandle_1(oosql_destroysystemhandle_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_destroysystemhandle_reply *server_oosql_destroysystemhandle_1(oosql_destroysystemhandle_arg *, CLIENT *);
#else 
extern oosql_destroysystemhandle_reply *server_oosql_destroysystemhandle_1();
#endif
#define SERVER_OOSQL_ALLOCHANDLE ((u_long)3645)
#if defined(ODYS_SERVER)
extern oosql_allochandle_reply *server_oosql_allochandle_1(oosql_allochandle_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_allochandle_reply *server_oosql_allochandle_1(oosql_allochandle_arg *, CLIENT *);
#else 
extern oosql_allochandle_reply *server_oosql_allochandle_1();
#endif
#define SERVER_OOSQL_FREEHANDLE ((u_long)3646)
#if defined(ODYS_SERVER)
extern oosql_freehandle_reply *server_oosql_freehandle_1(oosql_freehandle_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_freehandle_reply *server_oosql_freehandle_1(oosql_freehandle_arg *, CLIENT *);
#else 
extern oosql_freehandle_reply *server_oosql_freehandle_1();
#endif
#define SERVER_OOSQL_MOUNT ((u_long)3647)
#if defined(ODYS_SERVER)
extern oosql_mount_reply *server_oosql_mount_1(oosql_mount_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_mount_reply *server_oosql_mount_1(oosql_mount_arg *, CLIENT *);
#else 
extern oosql_mount_reply *server_oosql_mount_1();
#endif
#define SERVER_OOSQL_DISMOUNT ((u_long)3648)
#if defined(ODYS_SERVER)
extern oosql_dismount_reply *server_oosql_dismount_1(oosql_dismount_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_dismount_reply *server_oosql_dismount_1(oosql_dismount_arg *, CLIENT *);
#else 
extern oosql_dismount_reply *server_oosql_dismount_1();
#endif
#define SERVER_OOSQL_SETUSERDEFAULTVOLUMEID ((u_long)3649)
#if defined(ODYS_SERVER)
extern oosql_setuserdefaultvolumeid_reply *server_oosql_setuserdefaultvolumeid_1(oosql_setuserdefaultvolumeid_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_setuserdefaultvolumeid_reply *server_oosql_setuserdefaultvolumeid_1(oosql_setuserdefaultvolumeid_arg *, CLIENT *);
#else 
extern oosql_setuserdefaultvolumeid_reply *server_oosql_setuserdefaultvolumeid_1();
#endif
#define SERVER_OOSQL_GETUSERDEFAULTVOLUMEID ((u_long)3650)
#if defined(ODYS_SERVER)
extern oosql_getuserdefaultvolumeid_reply *server_oosql_getuserdefaultvolumeid_1(oosql_getuserdefaultvolumeid_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getuserdefaultvolumeid_reply *server_oosql_getuserdefaultvolumeid_1(oosql_getuserdefaultvolumeid_arg *, CLIENT *);
#else 
extern oosql_getuserdefaultvolumeid_reply *server_oosql_getuserdefaultvolumeid_1();
#endif
#define SERVER_OOSQL_GETVOLUMEID ((u_long)3651)
#if defined(ODYS_SERVER)
extern oosql_getvolumeid_reply *server_oosql_getvolumeid_1(oosql_getvolumeid_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getvolumeid_reply *server_oosql_getvolumeid_1(oosql_getvolumeid_arg *, CLIENT *);
#else 
extern oosql_getvolumeid_reply *server_oosql_getvolumeid_1();
#endif
#define SERVER_OOSQL_MOUNTDB ((u_long)3652)
#if defined(ODYS_SERVER)
extern oosql_mountdb_reply *server_oosql_mountdb_1(oosql_mountdb_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_mountdb_reply *server_oosql_mountdb_1(oosql_mountdb_arg *, CLIENT *);
#else 
extern oosql_mountdb_reply *server_oosql_mountdb_1();
#endif
#define SERVER_OOSQL_DISMOUNTDB ((u_long)3653)
#if defined(ODYS_SERVER)
extern oosql_dismountdb_reply *server_oosql_dismountdb_1(oosql_dismountdb_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_dismountdb_reply *server_oosql_dismountdb_1(oosql_dismountdb_arg *, CLIENT *);
#else 
extern oosql_dismountdb_reply *server_oosql_dismountdb_1();
#endif
#define SERVER_OOSQL_TRANSBEGIN ((u_long)3654)
#if defined(ODYS_SERVER)
extern oosql_transbegin_reply *server_oosql_transbegin_1(oosql_transbegin_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_transbegin_reply *server_oosql_transbegin_1(oosql_transbegin_arg *, CLIENT *);
#else 
extern oosql_transbegin_reply *server_oosql_transbegin_1();
#endif
#define SERVER_OOSQL_TRANSCOMMIT ((u_long)3655)
#if defined(ODYS_SERVER)
extern oosql_transcommit_reply *server_oosql_transcommit_1(oosql_transcommit_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_transcommit_reply *server_oosql_transcommit_1(oosql_transcommit_arg *, CLIENT *);
#else 
extern oosql_transcommit_reply *server_oosql_transcommit_1();
#endif
#define SERVER_OOSQL_TRANSABORT ((u_long)3656)
#if defined(ODYS_SERVER)
extern oosql_transabort_reply *server_oosql_transabort_1(oosql_transabort_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_transabort_reply *server_oosql_transabort_1(oosql_transabort_arg *, CLIENT *);
#else 
extern oosql_transabort_reply *server_oosql_transabort_1();
#endif
#define SERVER_OOSQL_PREPARE ((u_long)3657)
#if defined(ODYS_SERVER)
extern oosql_prepare_reply *server_oosql_prepare_1(oosql_prepare_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_prepare_reply *server_oosql_prepare_1(oosql_prepare_arg *, CLIENT *);
#else 
extern oosql_prepare_reply *server_oosql_prepare_1();
#endif
#define SERVER_OOSQL_EXECUTE ((u_long)3658)
#if defined(ODYS_SERVER)
extern oosql_execute_reply *server_oosql_execute_1(oosql_execute_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_execute_reply *server_oosql_execute_1(oosql_execute_arg *, CLIENT *);
#else 
extern oosql_execute_reply *server_oosql_execute_1();
#endif
#define SERVER_OOSQL_EXECDIRECT ((u_long)3659)
#if defined(ODYS_SERVER)
extern oosql_execdirect_reply *server_oosql_execdirect_1(oosql_execdirect_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_execdirect_reply *server_oosql_execdirect_1(oosql_execdirect_arg *, CLIENT *);
#else 
extern oosql_execdirect_reply *server_oosql_execdirect_1();
#endif
#define SERVER_OOSQL_NEXT ((u_long)3660)
#if defined(ODYS_SERVER)
extern oosql_next_reply *server_oosql_next_1(oosql_next_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_next_reply *server_oosql_next_1(oosql_next_arg *, CLIENT *);
#else 
extern oosql_next_reply *server_oosql_next_1();
#endif
#define SERVER_OOSQL_GETDATA ((u_long)3661)
#if defined(ODYS_SERVER)
extern oosql_getdata_reply *server_oosql_getdata_1(oosql_getdata_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getdata_reply *server_oosql_getdata_1(oosql_getdata_arg *, CLIENT *);
#else 
extern oosql_getdata_reply *server_oosql_getdata_1();
#endif
#define SERVER_OOSQL_PUTDATA ((u_long)3662)
#if defined(ODYS_SERVER)
extern oosql_putdata_reply *server_oosql_putdata_1(oosql_putdata_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_putdata_reply *server_oosql_putdata_1(oosql_putdata_arg *, CLIENT *);
#else 
extern oosql_putdata_reply *server_oosql_putdata_1();
#endif
#define SERVER_OOSQL_GETOID ((u_long)3663)
#if defined(ODYS_SERVER)
extern oosql_getoid_reply *server_oosql_getoid_1(oosql_getoid_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getoid_reply *server_oosql_getoid_1(oosql_getoid_arg *, CLIENT *);
#else 
extern oosql_getoid_reply *server_oosql_getoid_1();
#endif
#define SERVER_OOSQL_GETNUMRESULTCOLS ((u_long)3664)
#if defined(ODYS_SERVER)
extern oosql_getnumresultcols_reply *server_oosql_getnumresultcols_1(oosql_getnumresultcols_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getnumresultcols_reply *server_oosql_getnumresultcols_1(oosql_getnumresultcols_arg *, CLIENT *);
#else 
extern oosql_getnumresultcols_reply *server_oosql_getnumresultcols_1();
#endif
#define SERVER_OOSQL_GETRESULTCOLNAME ((u_long)3665)
#if defined(ODYS_SERVER)
extern oosql_getresultcolname_reply *server_oosql_getresultcolname_1(oosql_getresultcolname_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getresultcolname_reply *server_oosql_getresultcolname_1(oosql_getresultcolname_arg *, CLIENT *);
#else 
extern oosql_getresultcolname_reply *server_oosql_getresultcolname_1();
#endif
#define SERVER_OOSQL_GETRESULTCOLTYPE ((u_long)3666)
#if defined(ODYS_SERVER)
extern oosql_getresultcoltype_reply *server_oosql_getresultcoltype_1(oosql_getresultcoltype_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getresultcoltype_reply *server_oosql_getresultcoltype_1(oosql_getresultcoltype_arg *, CLIENT *);
#else 
extern oosql_getresultcoltype_reply *server_oosql_getresultcoltype_1();
#endif
#define SERVER_OOSQL_GETRESULTCOLLENGTH ((u_long)3667)
#if defined(ODYS_SERVER)
extern oosql_getresultcollength_reply *server_oosql_getresultcollength_1(oosql_getresultcollength_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_getresultcollength_reply *server_oosql_getresultcollength_1(oosql_getresultcollength_arg *, CLIENT *);
#else 
extern oosql_getresultcollength_reply *server_oosql_getresultcollength_1();
#endif
#define SERVER_OOSQL_GETERRORMESSAGE ((u_long)3668)
#if defined(ODYS_SERVER)
extern oosql_geterrormessage_reply *server_oosql_geterrormessage_1(oosql_geterrormessage_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_geterrormessage_reply *server_oosql_geterrormessage_1(oosql_geterrormessage_arg *, CLIENT *);
#else 
extern oosql_geterrormessage_reply *server_oosql_geterrormessage_1();
#endif
#define SERVER_OOSQL_OIDTOOIDSTRING ((u_long)3669)
#if defined(ODYS_SERVER)
extern oosql_oidtooidstring_reply *server_oosql_oidtooidstring_1(oosql_oidtooidstring_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_oidtooidstring_reply *server_oosql_oidtooidstring_1(oosql_oidtooidstring_arg *, CLIENT *);
#else 
extern oosql_oidtooidstring_reply *server_oosql_oidtooidstring_1();
#endif
#define SERVER_OOSQL_TEXT_ADDKEYWORDEXTRACTOR ((u_long)3670)
#if defined(ODYS_SERVER)
extern oosql_text_addkeywordextractor_reply *server_oosql_text_addkeywordextractor_1(oosql_text_addkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_addkeywordextractor_reply *server_oosql_text_addkeywordextractor_1(oosql_text_addkeywordextractor_arg *, CLIENT *);
#else 
extern oosql_text_addkeywordextractor_reply *server_oosql_text_addkeywordextractor_1();
#endif
#define SERVER_OOSQL_TEXT_ADDDEFAULTKEYWORDEXTRACTOR ((u_long)3671)
#if defined(ODYS_SERVER)
extern oosql_text_adddefaultkeywordextractor_reply *server_oosql_text_adddefaultkeywordextractor_1(oosql_text_adddefaultkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_adddefaultkeywordextractor_reply *server_oosql_text_adddefaultkeywordextractor_1(oosql_text_adddefaultkeywordextractor_arg *, CLIENT *);
#else 
extern oosql_text_adddefaultkeywordextractor_reply *server_oosql_text_adddefaultkeywordextractor_1();
#endif
#define SERVER_OOSQL_TEXT_DROPKEYWORDEXTRACTOR ((u_long)3672)
#if defined(ODYS_SERVER)
extern oosql_text_dropkeywordextractor_reply *server_oosql_text_dropkeywordextractor_1(oosql_text_dropkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_dropkeywordextractor_reply *server_oosql_text_dropkeywordextractor_1(oosql_text_dropkeywordextractor_arg *, CLIENT *);
#else 
extern oosql_text_dropkeywordextractor_reply *server_oosql_text_dropkeywordextractor_1();
#endif
#define SERVER_OOSQL_TEXT_SETKEYWORDEXTRACTOR ((u_long)3673)
#if defined(ODYS_SERVER)
extern oosql_text_setkeywordextractor_reply *server_oosql_text_setkeywordextractor_1(oosql_text_setkeywordextractor_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_setkeywordextractor_reply *server_oosql_text_setkeywordextractor_1(oosql_text_setkeywordextractor_arg *, CLIENT *);
#else 
extern oosql_text_setkeywordextractor_reply *server_oosql_text_setkeywordextractor_1();
#endif
#define SERVER_OOSQL_TEXT_ADDFILTER ((u_long)3674)
#if defined(ODYS_SERVER)
extern oosql_text_addfilter_reply *server_oosql_text_addfilter_1(oosql_text_addfilter_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_addfilter_reply *server_oosql_text_addfilter_1(oosql_text_addfilter_arg *, CLIENT *);
#else 
extern oosql_text_addfilter_reply *server_oosql_text_addfilter_1();
#endif
#define SERVER_OOSQL_TEXT_DROPFILTER ((u_long)3675)
#if defined(ODYS_SERVER)
extern oosql_text_dropfilter_reply *server_oosql_text_dropfilter_1(oosql_text_dropfilter_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_dropfilter_reply *server_oosql_text_dropfilter_1(oosql_text_dropfilter_arg *, CLIENT *);
#else 
extern oosql_text_dropfilter_reply *server_oosql_text_dropfilter_1();
#endif
#define SERVER_OOSQL_TEXT_SETFILTER ((u_long)3676)
#if defined(ODYS_SERVER)
extern oosql_text_setfilter_reply *server_oosql_text_setfilter_1(oosql_text_setfilter_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_setfilter_reply *server_oosql_text_setfilter_1(oosql_text_setfilter_arg *, CLIENT *);
#else 
extern oosql_text_setfilter_reply *server_oosql_text_setfilter_1();
#endif
#define SERVER_OOSQL_TEXT_MAKEINDEX ((u_long)3677)
#if defined(ODYS_SERVER)
extern oosql_text_makeindex_reply *server_oosql_text_makeindex_1(oosql_text_makeindex_arg *);
#elif defined(ODYS_CLIENT)
extern oosql_text_makeindex_reply *server_oosql_text_makeindex_1(oosql_text_makeindex_arg *, CLIENT *);
#else 
extern oosql_text_makeindex_reply *server_oosql_text_makeindex_1();
#endif
#define SERVER_RELEASECONNECTIONANDQUIT ((u_long)3679)
#if defined(ODYS_SERVER)
extern void *server_releaseconnectionandquit_1(server_getpid_arg *);
#elif defined(ODYS_CLIENT)
extern void *server_releaseconnectionandquit_1(server_getpid_arg *, CLIENT *);
#else 
extern void *server_releaseconnectionandquit_1();
#endif
#define SERVER_LOM_OPENNAMEDOBJECTTABLE ((u_long)3686)
#if defined(ODYS_SERVER)
extern lom_opennamedobjecttable_reply *server_lom_opennamedobjecttable_1(lom_opennamedobjecttable_arg *);
#elif defined(ODYS_CLIENT)
extern lom_opennamedobjecttable_reply *server_lom_opennamedobjecttable_1(lom_opennamedobjecttable_arg *, CLIENT *);
#else 
extern lom_opennamedobjecttable_reply *server_lom_opennamedobjecttable_1();
#endif
#define SERVER_LOM_CLOSENAMEDOBJECTTABLE ((u_long)3687)
#if defined(ODYS_SERVER)
extern lom_closenamedobjecttable_reply *server_lom_closenamedobjecttable_1(lom_closenamedobjecttable_arg *);
#elif defined(ODYS_CLIENT)
extern lom_closenamedobjecttable_reply *server_lom_closenamedobjecttable_1(lom_closenamedobjecttable_arg *, CLIENT *);
#else 
extern lom_closenamedobjecttable_reply *server_lom_closenamedobjecttable_1();
#endif
#define SERVER_LOM_SETOBJECTNAME ((u_long)3688)
#if defined(ODYS_SERVER)
extern lom_setobjectname_reply *server_lom_setobjectname_1(lom_setobjectname_arg *);
#elif defined(ODYS_CLIENT)
extern lom_setobjectname_reply *server_lom_setobjectname_1(lom_setobjectname_arg *, CLIENT *);
#else 
extern lom_setobjectname_reply *server_lom_setobjectname_1();
#endif
#define SERVER_LOM_LOOKUPNAMEDOBJECT ((u_long)3689)
#if defined(ODYS_SERVER)
extern lom_lookupnamedobject_reply *server_lom_lookupnamedobject_1(lom_lookupnamedobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_lookupnamedobject_reply *server_lom_lookupnamedobject_1(lom_lookupnamedobject_arg *, CLIENT *);
#else 
extern lom_lookupnamedobject_reply *server_lom_lookupnamedobject_1();
#endif
#define SERVER_LOM_RESETOBJECTNAME ((u_long)3690)
#if defined(ODYS_SERVER)
extern lom_resetobjectname_reply *server_lom_resetobjectname_1(lom_resetobjectname_arg *);
#elif defined(ODYS_CLIENT)
extern lom_resetobjectname_reply *server_lom_resetobjectname_1(lom_resetobjectname_arg *, CLIENT *);
#else 
extern lom_resetobjectname_reply *server_lom_resetobjectname_1();
#endif
#define SERVER_LOM_RENAMENAMEDOBJECT ((u_long)3691)
#if defined(ODYS_SERVER)
extern lom_renamenamedobject_reply *server_lom_renamenamedobject_1(lom_renamenamedobject_arg *);
#elif defined(ODYS_CLIENT)
extern lom_renamenamedobject_reply *server_lom_renamenamedobject_1(lom_renamenamedobject_arg *, CLIENT *);
#else 
extern lom_renamenamedobject_reply *server_lom_renamenamedobject_1();
#endif
#define SERVER_LOM_GETOBJECTNAME ((u_long)3692)
#if defined(ODYS_SERVER)
extern lom_getobjectname_reply *server_lom_getobjectname_1(lom_getobjectname_arg *);
#elif defined(ODYS_CLIENT)
extern lom_getobjectname_reply *server_lom_getobjectname_1(lom_getobjectname_arg *, CLIENT *);
#else 
extern lom_getobjectname_reply *server_lom_getobjectname_1();
#endif
#define SERVER_SDP_EXECUTE ((u_long)3693)
#if defined(ODYS_SERVER)
extern sdp_execute_reply *server_sdp_execute_1(sdp_execute_arg *);
#elif defined(ODYS_CLIENT)
extern sdp_execute_reply *server_sdp_execute_1(sdp_execute_arg *, CLIENT *);
#else 
extern sdp_execute_reply *server_sdp_execute_1();
#endif
#define SERVER_LOM_ODMG_COL_CREATEDATA ((u_long)3772)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_createdata_reply *server_lom_odmg_col_createdata_1(lom_odmg_collection_createdata_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_createdata_reply *server_lom_odmg_col_createdata_1(lom_odmg_collection_createdata_arg *, CLIENT *);
#else 
extern lom_odmg_collection_createdata_reply *server_lom_odmg_col_createdata_1();
#endif
#define SERVER_LOM_ODMG_COL_DESTROYDATA ((u_long)3773)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_destroydata_reply *server_lom_odmg_col_destroydata_1(lom_odmg_collection_destroydata_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_destroydata_reply *server_lom_odmg_col_destroydata_1(lom_odmg_collection_destroydata_arg *, CLIENT *);
#else 
extern lom_odmg_collection_destroydata_reply *server_lom_odmg_col_destroydata_1();
#endif
#define SERVER_LOM_ODMG_COL_GETDESCRIPTOR ((u_long)3774)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_getdescriptor_reply *server_lom_odmg_col_getdescriptor_1(lom_odmg_collection_getdescriptor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_getdescriptor_reply *server_lom_odmg_col_getdescriptor_1(lom_odmg_collection_getdescriptor_arg *, CLIENT *);
#else 
extern lom_odmg_collection_getdescriptor_reply *server_lom_odmg_col_getdescriptor_1();
#endif
#define SERVER_LOM_ODMG_COL_SETDESCRIPTOR ((u_long)3775)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_setdescriptor_reply *server_lom_odmg_col_setdescriptor_1(lom_odmg_collection_setdescriptor_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_setdescriptor_reply *server_lom_odmg_col_setdescriptor_1(lom_odmg_collection_setdescriptor_arg *, CLIENT *);
#else 
extern lom_odmg_collection_setdescriptor_reply *server_lom_odmg_col_setdescriptor_1();
#endif
#define SERVER_LOM_ODMG_COL_ASSIGN ((u_long)3776)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_assign_reply *server_lom_odmg_col_assign_1(lom_odmg_collection_assign_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_assign_reply *server_lom_odmg_col_assign_1(lom_odmg_collection_assign_arg *, CLIENT *);
#else 
extern lom_odmg_collection_assign_reply *server_lom_odmg_col_assign_1();
#endif
#define SERVER_LOM_ODMG_COL_ASSIGNELEMENTS ((u_long)3777)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_assignelements_reply *server_lom_odmg_col_assignelements_1(lom_odmg_collection_assignelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_assignelements_reply *server_lom_odmg_col_assignelements_1(lom_odmg_collection_assignelements_arg *, CLIENT *);
#else 
extern lom_odmg_collection_assignelements_reply *server_lom_odmg_col_assignelements_1();
#endif
#define SERVER_LOM_ODMG_COL_INSERTELEMENTS ((u_long)3778)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_insertelements_reply *server_lom_odmg_col_insertelements_1(lom_odmg_collection_insertelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_insertelements_reply *server_lom_odmg_col_insertelements_1(lom_odmg_collection_insertelements_arg *, CLIENT *);
#else 
extern lom_odmg_collection_insertelements_reply *server_lom_odmg_col_insertelements_1();
#endif
#define SERVER_LOM_ODMG_COL_DELETEELEMENTS ((u_long)3779)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_deleteelements_reply *server_lom_odmg_col_deleteelements_1(lom_odmg_collection_deleteelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_deleteelements_reply *server_lom_odmg_col_deleteelements_1(lom_odmg_collection_deleteelements_arg *, CLIENT *);
#else 
extern lom_odmg_collection_deleteelements_reply *server_lom_odmg_col_deleteelements_1();
#endif
#define SERVER_LOM_ODMG_COL_DELETEALL ((u_long)3780)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_deleteall_reply *server_lom_odmg_col_deleteall_1(lom_odmg_collection_deleteall_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_deleteall_reply *server_lom_odmg_col_deleteall_1(lom_odmg_collection_deleteall_arg *, CLIENT *);
#else 
extern lom_odmg_collection_deleteall_reply *server_lom_odmg_col_deleteall_1();
#endif
#define SERVER_LOM_ODMG_COL_ISMEMBER ((u_long)3781)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_ismember_reply *server_lom_odmg_col_ismember_1(lom_odmg_collection_ismember_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_ismember_reply *server_lom_odmg_col_ismember_1(lom_odmg_collection_ismember_arg *, CLIENT *);
#else 
extern lom_odmg_collection_ismember_reply *server_lom_odmg_col_ismember_1();
#endif
#define SERVER_LOM_ODMG_COL_ISEQUAL ((u_long)3782)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_isequal_reply *server_lom_odmg_col_isequal_1(lom_odmg_collection_isequal_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_isequal_reply *server_lom_odmg_col_isequal_1(lom_odmg_collection_isequal_arg *, CLIENT *);
#else 
extern lom_odmg_collection_isequal_reply *server_lom_odmg_col_isequal_1();
#endif
#define SERVER_LOM_ODMG_COL_ISSUBSET ((u_long)3783)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_issubset_reply *server_lom_odmg_col_issubset_1(lom_odmg_collection_issubset_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_issubset_reply *server_lom_odmg_col_issubset_1(lom_odmg_collection_issubset_arg *, CLIENT *);
#else 
extern lom_odmg_collection_issubset_reply *server_lom_odmg_col_issubset_1();
#endif
#define SERVER_LOM_ODMG_COL_UNION ((u_long)3784)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_union_reply *server_lom_odmg_col_union_1(lom_odmg_collection_union_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_union_reply *server_lom_odmg_col_union_1(lom_odmg_collection_union_arg *, CLIENT *);
#else 
extern lom_odmg_collection_union_reply *server_lom_odmg_col_union_1();
#endif
#define SERVER_LOM_ODMG_COL_INTERSECT ((u_long)3785)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_intersect_reply *server_lom_odmg_col_intersect_1(lom_odmg_collection_intersect_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_intersect_reply *server_lom_odmg_col_intersect_1(lom_odmg_collection_intersect_arg *, CLIENT *);
#else 
extern lom_odmg_collection_intersect_reply *server_lom_odmg_col_intersect_1();
#endif
#define SERVER_LOM_ODMG_COL_DIFFERENCE ((u_long)3786)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_difference_reply *server_lom_odmg_col_difference_1(lom_odmg_collection_difference_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_difference_reply *server_lom_odmg_col_difference_1(lom_odmg_collection_difference_arg *, CLIENT *);
#else 
extern lom_odmg_collection_difference_reply *server_lom_odmg_col_difference_1();
#endif
#define SERVER_LOM_ODMG_COL_UNIONWITH ((u_long)3787)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_unionwith_reply *server_lom_odmg_col_unionwith_1(lom_odmg_collection_unionwith_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_unionwith_reply *server_lom_odmg_col_unionwith_1(lom_odmg_collection_unionwith_arg *, CLIENT *);
#else 
extern lom_odmg_collection_unionwith_reply *server_lom_odmg_col_unionwith_1();
#endif
#define SERVER_LOM_ODMG_COL_INTERSECTWITH ((u_long)3788)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_intersectwith_reply *server_lom_odmg_col_intersectwith_1(lom_odmg_collection_intersectwith_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_intersectwith_reply *server_lom_odmg_col_intersectwith_1(lom_odmg_collection_intersectwith_arg *, CLIENT *);
#else 
extern lom_odmg_collection_intersectwith_reply *server_lom_odmg_col_intersectwith_1();
#endif
#define SERVER_LOM_ODMG_COL_DIFFERENCEWITH ((u_long)3789)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_differencewith_reply *server_lom_odmg_col_differencewith_1(lom_odmg_collection_differencewith_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_differencewith_reply *server_lom_odmg_col_differencewith_1(lom_odmg_collection_differencewith_arg *, CLIENT *);
#else 
extern lom_odmg_collection_differencewith_reply *server_lom_odmg_col_differencewith_1();
#endif
#define SERVER_LOM_ODMG_COL_APPENDELEMENTS ((u_long)3790)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_appendelements_reply *server_lom_odmg_col_appendelements_1(lom_odmg_collection_appendelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_appendelements_reply *server_lom_odmg_col_appendelements_1(lom_odmg_collection_appendelements_arg *, CLIENT *);
#else 
extern lom_odmg_collection_appendelements_reply *server_lom_odmg_col_appendelements_1();
#endif
#define SERVER_LOM_ODMG_COL_RETRIEVEELEMENTS ((u_long)3791)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_retrieveelements_reply *server_lom_odmg_col_retrieveelements_1(lom_odmg_collection_retrieveelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_retrieveelements_reply *server_lom_odmg_col_retrieveelements_1(lom_odmg_collection_retrieveelements_arg *, CLIENT *);
#else 
extern lom_odmg_collection_retrieveelements_reply *server_lom_odmg_col_retrieveelements_1();
#endif
#define SERVER_LOM_ODMG_COL_UPDATEELEMENTS ((u_long)3792)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_updateelements_reply *server_lom_odmg_col_updateelements_1(lom_odmg_collection_updateelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_updateelements_reply *server_lom_odmg_col_updateelements_1(lom_odmg_collection_updateelements_arg *, CLIENT *);
#else 
extern lom_odmg_collection_updateelements_reply *server_lom_odmg_col_updateelements_1();
#endif
#define SERVER_LOM_ODMG_COL_CONCATENATE ((u_long)3793)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_concatenate_reply *server_lom_odmg_col_concatenate_1(lom_odmg_collection_concatenate_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_concatenate_reply *server_lom_odmg_col_concatenate_1(lom_odmg_collection_concatenate_arg *, CLIENT *);
#else 
extern lom_odmg_collection_concatenate_reply *server_lom_odmg_col_concatenate_1();
#endif
#define SERVER_LOM_ODMG_COL_RESIZE ((u_long)3794)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_resize_reply *server_lom_odmg_col_resize_1(lom_odmg_collection_resize_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_resize_reply *server_lom_odmg_col_resize_1(lom_odmg_collection_resize_arg *, CLIENT *);
#else 
extern lom_odmg_collection_resize_reply *server_lom_odmg_col_resize_1();
#endif
#define SERVER_LOM_ODMG_COL_SCAN_OPEN ((u_long)3795)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_scan_open_reply *server_lom_odmg_col_scan_open_1(lom_odmg_collection_scan_open_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_scan_open_reply *server_lom_odmg_col_scan_open_1(lom_odmg_collection_scan_open_arg *, CLIENT *);
#else 
extern lom_odmg_collection_scan_open_reply *server_lom_odmg_col_scan_open_1();
#endif
#define SERVER_LOM_ODMG_COL_SCAN_CLOSE ((u_long)3796)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_scan_close_reply *server_lom_odmg_col_scan_close_1(lom_odmg_collection_scan_close_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_scan_close_reply *server_lom_odmg_col_scan_close_1(lom_odmg_collection_scan_close_arg *, CLIENT *);
#else 
extern lom_odmg_collection_scan_close_reply *server_lom_odmg_col_scan_close_1();
#endif
#define SERVER_LOM_ODMG_COL_SCAN_NEXTELEMENTS ((u_long)3797)
#if defined(ODYS_SERVER)
extern lom_odmg_collection_scan_nextelements_reply *server_lom_odmg_col_scan_nextelements_1(lom_odmg_collection_scan_nextelements_arg *);
#elif defined(ODYS_CLIENT)
extern lom_odmg_collection_scan_nextelements_reply *server_lom_odmg_col_scan_nextelements_1(lom_odmg_collection_scan_nextelements_arg *, CLIENT *);
#else 
extern lom_odmg_collection_scan_nextelements_reply *server_lom_odmg_col_scan_nextelements_1();
#endif


#endif /* _ODYS_CS_H */
