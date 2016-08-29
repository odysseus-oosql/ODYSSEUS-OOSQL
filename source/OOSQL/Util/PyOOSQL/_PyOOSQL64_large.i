%module _PyOOSQL

#ifdef EMBED_INTERPRETER
%include embed.i
#endif

%{
#include "OOSQL_APIs_Internal.hxx"
#include "QuickFitMM.hxx"
#include "OOSQL_errorcodes.h"
#include <signal.h>
#ifndef WIN32
#include <unistd.h>
#endif

#undef OOSQL_GET_LOM_SYSTEMHANDLE
#undef OOSQL_GET_LOM_HANDLE
#undef OOSQL_MASK_TYPE
#undef OOSQL_MASK_COMPLEXTYPE

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	Boolean isUsingBulkLoading;                     
	Boolean isUsingKeywordIndexBulkLoading;         
	Boolean isUsingReverseKeywordIndexBulkLoading;  
	Boolean isBuildingExternalReverseKeywordFile;   
	Boolean isBuildingDocIdIndex;                   
	Boolean isSortingPostingFile;                   
	Boolean isUsingStoredPosting;                   
} lom_Text_ConfigForInvertedIndexBuild;

Four oosql_Tool_BuildTextIndex(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* attrName, lom_Text_ConfigForInvertedIndexBuild* config);
Four oosql_Tool_DeleteTextIndex(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName);
Four oosql_Tool_GetDatabaseStatistics(OOSQL_SystemHandle* systemHandle, char* databaseName, Four databaseId);
Four oosql_Tool_ExtractKeyword(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName, char* dataFileName, char* outputFileName, Four startObjectNo, Four endObjectNo, Four alwaysUsePreviousPostingFile);
Four oosql_Tool_MapPosting(OOSQL_SystemHandle *systemHandle, Four volId, char* className, char* attrName, Four nPostingFiles, char** postingFileNames, char* newPostingFileName, char* oidFileName, Four sortMergeMode, char* pageRankFile, Four pageRankMode);
Four oosql_Tool_MergePosting(OOSQL_SystemHandle* systemHandle, Four nPostingFiles, char** postingFileNames, char* newPostingFileName);
Four oosql_Tool_SortPosting(char* postingFileName, char* sortedPostingFileName);
Four oosql_Tool_SortStoredPosting(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* attrName);
Four oosql_Tool_StorePosting(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName, Boolean clearFlag);
Four oosql_Tool_UpdateTextDescriptor(OOSQL_SystemHandle* systemHandle, Four volId, char* className);
Four oosql_Tool_LoadDB(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, Boolean isDeferredTextIndexMode, Boolean smallUpdateFlag, Boolean useBulkloading, Boolean useDescriptorUpdating, char* datafileName, char* pageRankFileName, Four pageRankMode);
Four oosql_Tool_BatchDeleteFromFile(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* oidFileName);
Four oosql_Tool_BatchDeleteByDeferredDeletionList(OOSQL_SystemHandle* systemHandle,	Four volId, Four temporaryVolId, char* className);
Four oosql_Tool_ShowBatchDeleteStatus(OOSQL_SystemHandle* systemHandle,	Four volId, char* className);

#ifdef __cplusplus
}
#endif

%}

%include typemaps.i
%include pointer.i
%include memory.i
%include typemaps_ex.i

#define OOSQL_COMPLEXTYPE_BASIC 0
#define OOSQL_COMPLEXTYPE_SET   3
#define OOSQL_COMPLEXTYPE_BAG   4
#define OOSQL_COMPLEXTYPE_LIST  5

#define OOSQL_TYPE_SMALLINT		0
#define OOSQL_TYPE_SHORT		0

#define OOSQL_TYPE_INTEGER		1
#define OOSQL_TYPE_INT			1

#define OOSQL_TYPE_LONG			2

#define OOSQL_TYPE_REAL			3
#define OOSQL_TYPE_FLOAT		3

#define OOSQL_TYPE_DOUBLE		4

#define OOSQL_TYPE_CHAR			5	
#define OOSQL_TYPE_STRING		5

#define OOSQL_TYPE_VARCHAR		6
#define OOSQL_TYPE_VARSTRING	6

#define OOSQL_TYPE_OID			10

#define OOSQL_TYPE_TEXT			39

#define OOSQL_TYPE_DATE			50

#define OOSQL_TYPE_TIME			51

#define OOSQL_TYPE_TIMESTAMP	52

#define OOSQL_TYPE_LONG_LONG        14

#define OOSQL_TYPE_SMALLINT_SIZE    sizeof(Two_Invariable)
#define OOSQL_TYPE_SHORT_SIZE       sizeof(Two_Invariable)

#define OOSQL_TYPE_INTEGER_SIZE     sizeof(Four_Invariable)
#define OOSQL_TYPE_INT_SIZE         sizeof(Four_Invariable)

#define OOSQL_TYPE_LONG_SIZE        sizeof(Four_Invariable)
#define OOSQL_TYPE_LONG_LONG_SIZE   sizeof(Eight_Invariable)

#define OOSQL_TYPE_REAL_SIZE		sizeof(float)
#define OOSQL_TYPE_FLOAT_SIZE		sizeof(float)

#define OOSQL_TYPE_DOUBLE_SIZE		sizeof(double)

#define OOSQL_TYPE_OID_SIZE			sizeof(OID)

#define OOSQL_TYPE_DATE_SIZE		sizeof(long)

#define OOSQL_TYPE_TIME_SIZE		sizeof(long)

#define OOSQL_TYPE_TIMESTAMP_SIZE	sizeof(long)

#define OOSQL_TYPE_SET_SMALLINT		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_SMALLINT)
#define OOSQL_TYPE_SET_SHORT		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_SHORT)
#define OOSQL_TYPE_SET_INTEGER		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_INTEGER)
#define OOSQL_TYPE_SET_INT			((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_INT)
#define OOSQL_TYPE_SET_LONG			((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_LONG)
#define OOSQL_TYPE_SET_REAL			((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_REAL)
#define OOSQL_TYPE_SET_FLOAT		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_FLOAT)
#define OOSQL_TYPE_SET_DOUBLE		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_DOUBLE)
#define OOSQL_TYPE_SET_CHAR			((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_CHAR)
#define OOSQL_TYPE_SET_STRING		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_STRING)
#define OOSQL_TYPE_SET_VARCHAR		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_VARCHAR)
#define OOSQL_TYPE_SET_VARSTRING	((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_VARSTRING)
#define OOSQL_TYPE_SET_OID       	((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_OID)

#define OOSQL_TYPE_BAG_SMALLINT		((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_SMALLINT)
#define OOSQL_TYPE_BAG_SHORT		((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_SHORT)
#define OOSQL_TYPE_BAG_INTEGER		((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_INTEGER)
#define OOSQL_TYPE_BAG_INT			((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_INT)
#define OOSQL_TYPE_BAG_LONG			((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_LONG)
#define OOSQL_TYPE_BAG_REAL			((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_REAL)
#define OOSQL_TYPE_BAG_FLOAT		((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_FLOAT)
#define OOSQL_TYPE_BAG_DOUBLE		((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_DOUBLE)
#define OOSQL_TYPE_BAG_CHAR			((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_CHAR)
#define OOSQL_TYPE_BAG_STRING		((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_STRING)
#define OOSQL_TYPE_BAG_VARCHAR		((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_VARCHAR)
#define OOSQL_TYPE_BAG_VARSTRING	((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_VARSTRING)
#define OOSQL_TYPE_BAG_OID       	((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_OID)

#define OOSQL_TYPE_LIST_SMALLINT	((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_SMALLINT)
#define OOSQL_TYPE_LIST_SHORT		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_SHORT)
#define OOSQL_TYPE_LIST_INTEGER		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_INTEGER)
#define OOSQL_TYPE_LIST_INT			((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_INT)
#define OOSQL_TYPE_LIST_LONG		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_LONG)
#define OOSQL_TYPE_LIST_REAL		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_REAL)
#define OOSQL_TYPE_LIST_FLOAT		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_FLOAT)
#define OOSQL_TYPE_LIST_DOUBLE		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_DOUBLE)
#define OOSQL_TYPE_LIST_CHAR		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_CHAR)
#define OOSQL_TYPE_LIST_STRING		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_STRING)
#define OOSQL_TYPE_LIST_VARCHAR		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_VARCHAR)
#define OOSQL_TYPE_LIST_VARSTRING	((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_VARSTRING)
#define OOSQL_TYPE_LIST_OID       	((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_OID)

#define OOSQL_TEXT_IN_DB							0
#define OOSQL_TEXT_IN_FILE							1
#define OOSQL_TEXT_IN_MEMORY						2
#define OOSQL_TEXT_DONE								1

/* invariable size data type */
typedef short                   Two_Invariable;
typedef unsigned short          UTwo_Invariable;
typedef int                     Four_Invariable;
typedef unsigned int            UFour_Invariable;

/* one byte data type (in fact, it is a two byte data type) */
typedef short                   One;
typedef unsigned short          UOne;

/* two bytes data type (in fact, it is a four byte data type) */
typedef int                     Two;
typedef unsigned int            UTwo;

/* four bytes data type (in fact, it is a eight byte data type) */
typedef long                    Four;
typedef unsigned long           UFour;

/* Boolean Type */
/* typedef enum { FALSE, TRUE }    Boolean; */
typedef int Boolean;
#define TRUE  1
#define FALSE 0

%inline %{
#undef OOSQL_MASK_COMPLEXTYPE
unsigned int OOSQL_MASK_COMPLEXTYPE(unsigned int x)
{
    return (0xffff & ((UFour_Invariable)(x) >> 16));
}
#undef OOSQL_MASK_TYPE
unsigned int OOSQL_MASK_TYPE(unsigned int x)
{
    return (0xffff & (UFour_Invariable)(x));
}
%}

%inline %{
#undef OOSQL_MULTIPLERESULT_HEADER_SIZE
Four OOSQL_MULTIPLERESULT_HEADER_SIZE(Four nColumns)
{
    return (sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + 
            sizeof(Four) * (nColumns) + sizeof(OID) * (nColumns) + sizeof(Four) * (nColumns));
}

#undef OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET
Four OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(void* headerBuffer, Four nColumns, Four i)
{
	return (*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i)));
}
	
#undef OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE
Four OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(void* headerBuffer, Four nColumns, Four i)
{
	return (*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four)));
}

#undef OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_ISNULL
Four OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_ISNULL(void* headerBuffer, Four nColumns, Four i, Four j)
{
	return (((char*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four)))[((j) / 8)] & ((unsigned)0x80 >>((j) % 8)));
}
	
#undef OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_SIZE
Four OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_SIZE(void* headerBuffer, Four nColumns, Four i, Four j)
{
	return (*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (j)));
}

#undef OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_REALSIZE
int OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_REALSIZE(void* headerBuffer, Four nColumns, Four i, Four j)
{
	return (*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + sizeof(Four) * (j)));
}

#undef OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_OID
OID* OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_OID(void* headerBuffer, Four nColumns, Four i, Four j)
{
	return ((OID*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + sizeof(Four) * (nColumns) + sizeof(OID) * (j)));
}

#undef OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_COLNO
Four OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_COLNO(void* headerBuffer, Four nColumns, Four i, Four j)
{
	return (*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + sizeof(Four) * (nColumns) + sizeof(OID) * (nColumns) + sizeof(Four) * (j)));
}
%}

/* Type Definition for Transaction Identifier */
struct XactID {
	XactID();
	
	Four high;
	Four low;
};

typedef struct {                /* OID is used accross the volumes */
	OID();
	
	Four			pageNo;					/* specify the page holding the object */
	Two				volNo;					/* specify the volume in which object is in */
	Two				slotNo;					/* specify the slot within the page */
	Four			unique;					/* Unique No for checking dangling object */
	Four			classID;				/* specify the class including the object  */
} OID;

typedef enum { X_BROWSE_BROWSE, X_CS_BROWSE, X_CS_CS, X_RR_BROWSE, X_RR_CS, X_RR_RR } ConcurrencyLevel; /* isolation degree */

#define NIL -1

#define  ENDOFEVAL							1
#define  eNOERROR							0

typedef enum {OOSQL_SB_USE_DISK, OOSQL_SB_USE_MEMORY, OOSQL_SB_USE_MEMORY_WITH_DISK} OOSQL_SortBufferMode;
struct OOSQL_DiskSortBufferInfo {
	OOSQL_DiskSortBufferInfo();
	~OOSQL_DiskSortBufferInfo();

	Four						sortVolID;
};
struct OOSQL_MemorySortBufferInfo {
	OOSQL_MemorySortBufferInfo();
	~OOSQL_MemorySortBufferInfo();

	void*						sortBufferPtr;
	Four						sortBufferLength;
	Four						sortBufferUsedLength;
};
struct OOSQL_SortBufferInfo {
	OOSQL_SortBufferInfo(); 
	~OOSQL_SortBufferInfo(); 

	OOSQL_SortBufferMode		mode;
	OOSQL_DiskSortBufferInfo	diskInfo;
	OOSQL_MemorySortBufferInfo	memoryInfo;
};
struct OOSQL_GetDataStruct {
	OOSQL_GetDataStruct();
	~OOSQL_GetDataStruct();
	
	Two							columnNumber;
	Four						startPos;
	void*						bufferPtr;
	Four						bufferLength;
	Four						returnLength;
};

struct LOM_Handle {
	LOM_Handle();
	~LOM_Handle();
	
	Four serverInstanceId;
	Four instanceId;	
};

#define DBM_MAXVOLUMENAME			60 
#define DBM_MAXDATABASENAME			60 
#define MAXNUMOFVOLS				10 /* MAXNUMOFVOLS is defined in param.h */
#define MAXNUMOFDEVICES				100
#define MAX_NUM_EMBEDDEDATTRIBUTES	24

struct OOSQL_SystemHandle {
	OOSQL_SystemHandle();
	~OOSQL_SystemHandle();
	
	LOM_Handle					lomSystemHandle;
	Four						instanceId;
}; 

struct QuickFitMM_Handle {
    QuickFitMM_Handle();
    ~QuickFitMM_Handle();

    Four dummy[4];
};

typedef enum {
    OOSQL_GMT   = 0,  OOSQL_GMT12 = 12,     OOSQL_GMT_12 = -12,
    OOSQL_GMT1  = 1,  OOSQL_GMT_1 = -1,     OOSQL_GMT2   =  2,    OOSQL_GMT_2 = -2,
    OOSQL_GMT3  = 3,  OOSQL_GMT_3 = -3,     OOSQL_GMT4   =  4,    OOSQL_GMT_4 = -4,
    OOSQL_GMT5  = 5,  OOSQL_GMT_5 = -5,     OOSQL_GMT6   =  6,    OOSQL_GMT_6 = -6,
    OOSQL_GMT7  = 7,  OOSQL_GMT_7 = -7,     OOSQL_GMT8   =  8,    OOSQL_GMT_8 = -8,
    OOSQL_GMT9  = 9,  OOSQL_GMT_9 = -9,     OOSQL_GMT10  =  10,   OOSQL_GMT_10= -10,
    OOSQL_GMT11 = 11, OOSQL_GMT_11= -11,    OOSQL_USeastern = -5, OOSQL_UScentral = -6,
    OOSQL_USmoutain = -7, OOSQL_USpacific = -8
} OOSQL_TimeZone;
struct OOSQL_Time {
	OOSQL_Time();
	~OOSQL_Time();
	
    short _tzHour;
    short _tzMinute;
    short _Hour;
    short _Minute;
    short _Second;
    short _100thSec;
};
struct OOSQL_Timestamp {
	OOSQL_Timestamp();
	~OOSQL_Timestamp();
	
    UFour_Invariable d; 
    OOSQL_Time t;
};
typedef double OOSQL_Interval;

struct OOSQL_PostingStructureInfo {
	OOSQL_PostingStructureInfo();
	~OOSQL_PostingStructureInfo();

	Boolean isContainingTupleID;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingByteOffset;
	Two		nEmbeddedAttributes;
	Two		embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
};

struct OOSQL_ComplexTypeInfo {
	OOSQL_ComplexTypeInfo();
	~OOSQL_ComplexTypeInfo();
	
	OOSQL_SystemHandle* systemHandle;
	OID					oid;
	Four				colNo;
	Four				orn;
	Four				complexType;
	Four				elementType;
};

%inline %{
#undef OOSQL_GET_LOM_SYSTEMHANDLE
LOM_Handle* OOSQL_GET_LOM_SYSTEMHANDLE(OOSQL_SystemHandle* oosqlHandle)
{
	return &(oosqlHandle)->lomSystemHandle;
}
#undef OOSQL_GET_LOM_HANDLE
LOM_Handle* OOSQL_GET_LOM_HANDLE(OOSQL_SystemHandle* oosqlHandle)
{
	return &(oosqlHandle)->lomSystemHandle;
}
%}

typedef Four OOSQL_Handle;

%apply long*  OUTPUT { Four* OUTPUT }
%apply int*   OUTPUT { Two* OUTPUT }
%apply long*  OUTPUT { OOSQL_Handle* OUTPUT }

Four 	OOSQL_CreateSystemHandle(OOSQL_SystemHandle* systemHandle, Four *OUTPUT /* procIndex */);
Four 	OOSQL_DestroySystemHandle(OOSQL_SystemHandle* systemHandle, Four procIndex);

Four    OOSQL_Connect(char* hostAddress, char* protocolString, OOSQL_SystemHandle* systemHandle);
Four    OOSQL_Connect2(char* hostAddress, char* protocolString, char* serverPath, OOSQL_SystemHandle* systemHandle);
Four    OOSQL_Disconnect(OOSQL_SystemHandle* systemHandle);

Four 	OOSQL_SetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four volumeID);
Four 	OOSQL_GetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four* OUTPUT /* volumeID */);
Four 	OOSQL_GetVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, char* volumeName, Four* OUTPUT /* volumeID */);

Four 	OOSQL_MountDB(OOSQL_SystemHandle* systemHandle, char* databaseName, Four* OUTPUT /* databaseID */);
Four 	OOSQL_DismountDB(OOSQL_SystemHandle* systemHandle, Four databaseID);
Four	OOSQL_MountVolumeByVolumeName(OOSQL_SystemHandle* systemHandle, char* databaseName,	char* volumeName, Four* OUTPUT /* volID */);

Four	OOSQL_AllocHandle(OOSQL_SystemHandle* systemHandle, Four volID, OOSQL_Handle* OUTPUT /* handle */);
Four	OOSQL_FreeHandle(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);

Four	OOSQL_Mount(OOSQL_SystemHandle* systemHandle, Four numDevices, char** devNames, Four* OUTPUT /* volID */);
Four	OOSQL_Dismount(OOSQL_SystemHandle* systemHandle, Four volID);

Four	OOSQL_TransBegin(OOSQL_SystemHandle* systemHandle, XactID *xactId, ConcurrencyLevel ccLevel);
Four	OOSQL_TransCommit(OOSQL_SystemHandle* systemHandle, XactID *xactId);
Four	OOSQL_TransAbort(OOSQL_SystemHandle* systemHandle, XactID *xactId);

Four	OOSQL_Prepare(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four	OOSQL_Execute(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
Four	OOSQL_ExecDirect(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four	OOSQL_Next(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);

Four	OOSQL_GetData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* bufferPtr, Four bufferLength, Four* OUTPUT /* returnLength */);
Four	OOSQL_GetMultiColumnData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nColumns, OOSQL_GetDataStruct* getDataStruct);
Four    OOSQL_GetMultipleResults(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* OUTPUT /* nResultsRead */);
Four	OOSQL_GetComplexTypeInfo(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, OOSQL_ComplexTypeInfo* complexTypeInfo);
Four	OOSQL_PutData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength);
Four	OOSQL_GetOID(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two targetNumber, OID* oid);

Four	OOSQL_GetNumResultCols(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two* OUTPUT /* nCols */);
Four	OOSQL_GetResultColName(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, char* columnNameBuffer, Four bufferLength);
Four	OOSQL_GetResultColType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* OUTPUT /* columnType */);
Four	OOSQL_GetResultColLength(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* OUTPUT /* resultColLength */);
Four	OOSQL_GetPutDataParamType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two paramNumber, Four* OUTPUT /* paramType */);

Four	OOSQL_GetErrorMessage(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
Four	OOSQL_GetErrorName(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
Four	OOSQL_GetQueryErrorMessage(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* messageBuffer, Four bufferLength);
Four	OOSQL_OIDToOIDString(OOSQL_SystemHandle* systemHandle, OID* oid, char* oidString);

Four	OOSQL_Text_AddKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName, Four *keywordExtractorNo);
Four	OOSQL_Text_AddDefaultKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName);
Four	OOSQL_Text_DropKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractorName, Four version);
Four	OOSQL_Text_GetKeywordExtractorNo(OOSQL_SystemHandle* systemHandle, Four volID, char* keywordExtractorName, Four version, Four* OUTPUT /* keywordExtractorNo */);
Four	OOSQL_Text_SetKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columeName, Four keywordExtractorNo);
Four	OOSQL_Text_AddFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version, char *filterFilePath, char *filterFunctionName, Four *filterNo);
Four	OOSQL_Text_DropFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version);
Four	OOSQL_Text_GetFilterNo(OOSQL_SystemHandle* systemHandle, Four volID, char* filterName, Four version, Four* OUTPUT /* filterNo */);
Four	OOSQL_Text_SetFilter(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, Four filterNo);
Four	OOSQL_Text_MakeIndex(OOSQL_SystemHandle* systemHandle, Four volID, Four temporaryVolId, char* className);

Four 	OOSQL_Text_KeywordInfoScan_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, char* keyword);
Four 	OOSQL_Text_KeywordInfoScan_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
Four 	OOSQL_Text_KeywordInfoScan_Next(OOSQL_SystemHandle* systemHandle, Four scanId, char* keyword, Four* OUTPUT /* nDocuments */, Four* OUTPUT /* nPositions */);

Four 	OOSQL_Text_KeywordInfoScanForDocument_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, OID* oid, char* columnName, char* keyword);
Four 	OOSQL_Text_KeywordInfoScanForDocument_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
Four 	OOSQL_Text_KeywordInfoScanForDocument_Next(OOSQL_SystemHandle* systemHandle, Four  scanId, char* keyword, Four* OUTPUT /* nDocuments */, Four* OUTPUT /* nPositions */);
Four	OOSQL_Text_FetchContent(OOSQL_SystemHandle* systemHandle, Four volId, char* className, Four colNo, OID* oid, Four bufferLength, void* buffer, Four* OUTPUT /* returnLength */);
Four	OOSQL_Text_DefinePostingStructure(OOSQL_SystemHandle *systemHandle, Four volID, char *className, char *attrName, OOSQL_PostingStructureInfo *postingInfo);
Four	OOSQL_Text_GetNumKeywordsInDocument(OOSQL_SystemHandle* systemHandle, Four volId, OID* oid, char* columnName, Four* OUTPUT /* numKeywords */);
Four	OOSQL_Text_GetIthKeywordInDocument(OOSQL_SystemHandle* systemHandle, Four volId, OID* oid, char* columnName, Four ith, char* keyword);

Four	OOSQL_ComplexType_GetElementType(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* OUTPUT /* elementType */);
Four	OOSQL_ComplexType_GetComplexType(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* OUTPUT /* complexType */);
Four	OOSQL_ComplexType_GetNumElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* OUTPUT /* numElements */);
Four	OOSQL_ComplexType_GetElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
Four	OOSQL_ComplexType_GetElementsString(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, char* string, Four stringLength);
Four    OOSQL_ComplexType_IsNULL(OOSQL_ComplexTypeInfo* complexTypeInfo);

%typemap (python, in) Four* elementSizesArrayToString 
{
    if(PyString_Check($source))
    {
        $target = ($type)PyString_AsString($source);
    }
    else
    {
        PyErr_SetString(PyExc_TypeError, "expected a string generated from array.tostring");
        return NULL;
    }
}
%apply Four* elementSizesArrayToString { void* elementsArrayToString };

Four    OOSQL_ComplexType_InsertElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, 
                                         Four* elementSizesArrayToString, 
                                         void* elementsArrayToString);    

OOSQL_TimeZone OOSQL_GetLocalTimeZone(OOSQL_SystemHandle* systemHandle);
void OOSQL_SetCurTime(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time, OOSQL_TimeZone tz);
unsigned short OOSQL_GetHour(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short OOSQL_GetMinute(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short OOSQL_GetSecond(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short OOSQL_GetYear(OOSQL_SystemHandle* systemHandle, UFour_Invariable* date);
unsigned short OOSQL_GetMonth(OOSQL_SystemHandle* systemHandle, UFour_Invariable* date);
unsigned short OOSQL_GetDay(OOSQL_SystemHandle* systemHandle, UFour_Invariable* date);

char* OOSQL_GetVersionString(void);
char* OOSQL_GetCompilationParamString(void);
Four OOSQL_GetTimeElapsed(OOSQL_SystemHandle* systemHandle, Four* OUTPUT /* timeInMilliSeconds */);
Four OOSQL_ResetTimeElapsed(OOSQL_SystemHandle* systemHandle);

Four OOSQL_EstimateNumResults(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four* OUTPUT /* nResults */);

struct lom_Text_ConfigForInvertedIndexBuild {
	lom_Text_ConfigForInvertedIndexBuild();
	~lom_Text_ConfigForInvertedIndexBuild();
	
	Boolean isUsingBulkLoading;				
	Boolean isUsingKeywordIndexBulkLoading;
	Boolean isUsingReverseKeywordIndexBulkLoading;	
	Boolean isBuildingExternalReverseKeywordFile;
	Boolean isBuildingDocIdIndex;		
	Boolean isSortingPostingFile;	
	Boolean isUsingStoredPosting;		
};

Four oosql_Tool_BuildTextIndex(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* attrName, lom_Text_ConfigForInvertedIndexBuild* config);
Four oosql_Tool_DeleteTextIndex(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName);
Four oosql_Tool_GetDatabaseStatistics(OOSQL_SystemHandle* systemHandle, char* databaseName, Four databaseId);
Four oosql_Tool_ExtractKeyword(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName, char* dataFileName, char* outputFileName, Four startObjectNo, Four endObjectNo, Four alwaysUsePreviousPostingFile);
Four oosql_Tool_MapPosting(OOSQL_SystemHandle *systemHandle, Four volId, char* className, char* attrName, Four nPostingFiles, char** STRING_LIST /* postingFileNames */, char* newPostingFileName, char* oidFileName, Four sortMergeMode, char* pageRankFile, Four pageRankMode);
Four oosql_Tool_MergePosting(OOSQL_SystemHandle* systemHandle, Four nPostingFiles, char** STRING_LIST /* postingFileNames */, char* newPostingFileName);
Four oosql_Tool_SortPosting(char* postingFileName, char* sortedPostingFileName);
Four oosql_Tool_SortStoredPosting(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* attrName);
Four oosql_Tool_StorePosting(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName, Boolean clearFlag);
Four oosql_Tool_UpdateTextDescriptor(OOSQL_SystemHandle* systemHandle, Four volId, char* className);
Four oosql_Tool_LoadDB(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, Boolean isDeferredTextIndexMode, Boolean smallUpdateFlag, Boolean useBulkloading, Boolean useDescriptorUpdating, char* datafileName, char* pageRankFileName, Four pageRankMode);
Four oosql_Tool_BatchDeleteFromFile(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* oidFileName);
Four oosql_Tool_BatchDeleteByDeferredDeletionList(OOSQL_SystemHandle* systemHandle,	Four volId, Four temporaryVolId, char* className);
Four oosql_Tool_ShowBatchDeleteStatus(OOSQL_SystemHandle* systemHandle,	Four volId, char* className);

Four OOSQL_GetNumTextObjectsInVolume(OOSQL_SystemHandle* systemHandle, Four volId, Four* OUTPUT /* numObjects */);
Four OOSQL_GetNumObjectsInVolume(OOSQL_SystemHandle* systemHandle, Four volId, Four* OUTPUT/* numObjects */);
Four OOSQL_GetNumObjectsInClass(OOSQL_SystemHandle* systemHandle, Four volId, char* className, Four* OUTPUT /* numObjects */);

Four  OOSQL_SetCfgParam(OOSQL_SystemHandle* systemHandle, char *name, char *value);
char* OOSQL_GetCfgParam(OOSQL_SystemHandle* systemHandle, char *name);
Four  OOSQL_DumpPlan(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, void* outBuffer, int outBufferSize);

Four QuickFitMM_Init(QuickFitMM_Handle *mm_handle, Four size);
Four QuickFitMM_Final(QuickFitMM_Handle *mm_handle);
void* QuickFitMM_Alloc(QuickFitMM_Handle *mm_handle, Four size);
Four QuickFitMM_Free(QuickFitMM_Handle *mm_handle, char *p);
Four QuickFitMM_Free_Void_Pointer(QuickFitMM_Handle *mm_handle, void *p);

%inline %{
/* type conversion utility functions */
void* util_malloc(int size)
{
	return malloc(size);
}
void util_free(void* buffer)
{
	free(buffer);
}

char* util_convert_to_char_pointer(void* buffer)
{
    return (char*)buffer;
}

void util_print_address(void* buffer)
{
    fprintf(stderr, "Address = %p\n", buffer);
}

unsigned long* util_get_unsigned_long_ptr(void* buffer, unsigned long value)
{
	memcpy(buffer, &value, sizeof(unsigned long));
	return (unsigned long*)buffer;
}

short util_convert_to_short(void* data)
{
	short value;
	memcpy(&value, data, sizeof(short));
	return value;
}
int util_convert_to_int(void* data)
{
	int value;
	memcpy(&value, data, sizeof(int));
	return value;
}
float util_convert_to_float(void* data)
{
	float value;
	memcpy(&value, data, sizeof(float));
	return value;
}
double util_convert_to_double(void* data)
{
	double value;
	memcpy(&value, data, sizeof(double));
	return value;
}
OID util_convert_to_oid(void* data)
{
	OID value;
	memcpy(&value, data, sizeof(OID));
	return value;
}
unsigned long util_convert_to_date(void* data)
{
	unsigned long value;
	memcpy(&value, data, sizeof(unsigned long));
	return value;
}
OOSQL_Time util_convert_to_time(void* data)
{
	OOSQL_Time value;
	memcpy(&value, data, sizeof(OOSQL_Time));
	return value;
}
OOSQL_Timestamp util_convert_to_timestamp(void* data)
{
	OOSQL_Timestamp value;
	memcpy(&value, data, sizeof(OOSQL_Timestamp));
	return value;
}
PyObject* util_convert_to_string_with_size(void* data, int size)
{
	return PyString_FromStringAndSize((char*)data, size);
}
PyObject* util_convert_to_string(void* data)
{
	return PyString_FromString((char*)data);
}

void* util_convert_from_string_to_void(char* data, int size)
{
    void * result = malloc(size);
    memcpy(result, data, size);
    return result;
}

int util_convert_from_short(short value, char* data)
{
	int length = sizeof(short);
	memcpy(data, &value, length);
	return length;
}
int util_convert_from_int(int value, char* data)
{
	int length = sizeof(int);
	memcpy(data, &value, length);
	return length;
}
int util_convert_from_float(float value, char* data)
{
	int length = sizeof(float);
	memcpy(data, &value, length);
	return length;
}
int util_convert_from_double(double value, char* data)
{
	int length = sizeof(double);
	memcpy(data, &value, length);
	return length;
}
int util_convert_from_oid(OID value, char* data)
{
	int length = sizeof(OID);
	memcpy(data, &value, length);
	return length;
}
int util_convert_from_date(unsigned long value, char* data)
{
	int length = sizeof(unsigned long);
	memcpy(data, &value, length);
	return length;
}
int util_convert_from_time(OOSQL_Time value, char* data)
{
	int length = sizeof(OOSQL_Time);
	memcpy(data, &value, length);
	return length;
}
int util_convert_from_timestamp(OOSQL_Timestamp value, char* data)
{
	int length = sizeof(OOSQL_Timestamp);
	memcpy(data, &value, length);
	return length;
}
void* util_convert_from_string(PyObject* string)
{
	return (void*)PyString_AsString(string);
}

static char restart_command[4096];
void util_segfault_handler_for_restart(int a)
{
	printf("segment fault is occured. start command '%s' for restarting\n", restart_command);
	strcat(restart_command, "&");
	system(restart_command);
	exit(1);
}
void util_set_restart_command_after_segfault(char* command)
{
	strcpy(restart_command, command);
	signal(SIGSEGV, util_segfault_handler_for_restart);
}
void util_make_segfault(void)
{
	*(int*)0x0 = 0;
}


int getMachineEndian()
{
    int         i;
    int         lastByte;
    int         firstByte;
    char        buffer[8];

    i = 1;
    memcpy(buffer, &i, sizeof(int));

    firstByte = (int)buffer[0];
    lastByte = (int)buffer[sizeof(int)-1];
    if(firstByte == 1)
    {
        return 0; /* OOSQL_LITTLE_ENDIAN */
    }
    else if(lastByte == 1)
    {
        return 1; /* OOSQL_BIG_ENDIAN */
    }
    else
    {
        fprintf(stderr, "This machine doesn't use BIG_ENDIAN nor LITTLE_ENDIAN\n");
        exit(0);
    }
}
#define ERRORLOG_FILENAME "odysseus_error.log"
void util_errorlog_printf(char* msg)
{
    char    timeString[128];
    int     timeStringLength;
    time_t  currentTime;
    FILE*   fp;

    /* get current time */
    time(&currentTime);

    /* get time string from current time */
    strcpy(timeString, ctime(&currentTime));
    timeStringLength = strlen(timeString);

    /* remove cariage return from time string */
    if(timeString[timeStringLength - 1] == '\n')
        timeString[timeStringLength - 1] = '\0';


    /* open error log file */
    fp = fopen(ERRORLOG_FILENAME, "a+");

    /* file open fail */
    if (fp == NULL) {
        /* print out error message */
        fprintf(stderr, "Can't open error log file '%s'\n", ERRORLOG_FILENAME);
        fprintf(stderr, "[PID=%u][%s] ", getpid(), timeString);
        fprintf(stderr, "%s\n", msg);
    }
    else {
        /* write process ID and time string into error log file */
        fprintf(fp, "[PID=%u][%s] ", getpid(), timeString);
        fprintf(fp, "%s\n", msg);
    }

    /* close error log file */
    if (fp) fclose(fp);
}


unsigned long util_count_lines_in_loaddb_file(char* filename)
{
	char  			line[4096];
	unsigned long	numLinesInDataFile;
	int   			length;
	FILE* 			fp;
	
	fp = Util_fopen(filename, "r");
	if(fp == NULL)
		return 0;
	
	numLinesInDataFile = 0L;
	while(1)
	{
		if(Util_fgets(line, sizeof(line), fp))
		{
			length = strlen(line);
			if(length <= 1)				
				continue;
			else if(line[length - 1] != '\n') 
				continue;		
			else if(line[length - 3] == '\\' && line[length - 2] == 'n')
				continue;				
			else if(line[length - 2] == '\\')
				continue;		
			else
				numLinesInDataFile ++;
		}
		else
			break;
	}
	Util_fclose(fp);
	return numLinesInDataFile - 1;
}
int util_merge_loaddb_files(int file_num, char **STRING_LIST, char *mergedfilename)
{
	char	line[4096];
	FILE* 	fp1;
	FILE*	fp2;
	int		i;

	fp1 = Util_fopen(mergedfilename, "w");
	if(fp1 == NULL) return -1;

	fp2 = Util_fopen(STRING_LIST[0], "r");
	if(fp2 == NULL) return -1;

	while(Util_fgets(line, sizeof(line), fp2) != NULL)
		fputs(line, fp1);

	Util_fclose(fp2);

	for(i = 1; i < file_num; i++)
	{
		fp2 = Util_fopen(STRING_LIST[i], "r");
		if(fp2 == NULL) return -1;

		Util_fgets(line, sizeof(line), fp2);

		while(Util_fgets(line, sizeof(line), fp2) != NULL)
			Util_fputs(line, fp1);

		Util_fclose(fp2);
	}

	Util_fclose(fp1);

	return file_num;
}
%}
