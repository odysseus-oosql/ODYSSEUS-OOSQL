%module OOSQL

%{
#include "OOSQL_APIs.h"
#include "OOSQL_errorcodes.h"
#include "OOSQL_Util_APIs.h"

typedef struct OOSQL_JavaTime {
    short _tzHour;
    short _tzMinute;
    short _Hour;
    short _Minute;
    short _Second;
    short _100thSec;
} OOSQL_JavaTime;

typedef struct OOSQL_JavaDate {
    unsigned long   date;
} OOSQL_JavaDate;

typedef struct OOSQL_JavaTimestamp {
    OOSQL_JavaDate  d;
    OOSQL_JavaTime  t;
} OOSQL_JavaTimestamp;

%}

%include typemaps.i

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

#define OOSQL_TYPE_SMALLINT_SIZE	sizeof(short)
#define OOSQL_TYPE_SHORT_SIZE		sizeof(short)

#define OOSQL_TYPE_INTEGER_SIZE		sizeof(int)
#define OOSQL_TYPE_INT_SIZE			sizeof(int)

#define OOSQL_TYPE_LONG_SIZE		sizeof(long)

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
#define OOSQL_TEXT_MAXPOSITIONLISTLENGTH			61440   /* (4096 * 12 * 10 / 8) */

typedef char                    One;
typedef unsigned char           UOne;

/* two bytes data type */
typedef short                   Two;
typedef unsigned short          UTwo;

/* four bytes data type */
typedef int                     Four;
typedef unsigned int            UFour;

/* Boolean Type */
/* typedef enum { FALSE, TRUE }    Boolean; */
typedef int Boolean;
#define TRUE  1
#define FALSE 0

%inline %{
#undef OOSQL_MASK_COMPLEXTYPE
unsigned int OOSQL_MASK_COMPLEXTYPE(unsigned int x)
{
	return (0xffff & ((UFour)(x) >> 16));
}
#undef OOSQL_MASK_TYPE
unsigned int OOSQL_MASK_TYPE(unsigned int x)
{
	return (0xffff & (UFour)(x));
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
	~XactID();
	
    char	dummy[8];	
};

struct  OID {                /* OID is used accross the volumes */
	OID();
	~OID();
	
	char	dummy[16];
};
%addmethods OID {
    void setDummy(char* BYTE) {
        memcpy(self->dummy, BYTE, 16);
    }
};            

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
%addmethods OOSQL_MemorySortBufferInfo {
    void setBufferSize(int size) {
        self->sortBufferPtr = (char *)malloc(sizeof(char) * size);
    }
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

%addmethods OOSQL_GetDataStruct {
    void setBufferPtr(char* BYTE) {
        self->bufferPtr = (char *)BYTE;
    }
};            


struct LOM_Handle {
	LOM_Handle();
	~LOM_Handle();
	
	Four serverInstanceId;
	Four lrdsHandle;
	Four instanceId;	
};

#define DBM_MAXVOLUMENAME			60 
#define DBM_MAXDATABASENAME			60 
#define MAXNUMOFVOLS				10 /* MAXNUMOFVOLS is defined in param.h */
#define MAXNUMOFDEVICES				100

struct OOSQL_SystemHandle {
	OOSQL_SystemHandle();
	~OOSQL_SystemHandle();
	
	LOM_Handle					lomSystemHandle;
	Four						instanceId;
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
struct OOSQL_JavaTime {
	OOSQL_JavaTime();
	~OOSQL_JavaTime();
	
    short _tzHour;
    short _tzMinute;
    short _Hour;
    short _Minute;
    short _Second;
    short _100thSec;
};

struct OOSQL_JavaDate {
    OOSQL_JavaDate();
    ~OOSQL_JavaDate();
    
    unsigned long   date;
};
struct OOSQL_JavaTimestamp {
	OOSQL_JavaTimestamp();
	~OOSQL_JavaTimestamp();
	
    OOSQL_JavaDate d; 
    OOSQL_JavaTime t;
};

typedef double OOSQL_Interval;

#define MAX_NUM_EMBEDDEDATTRIBUTES			24
typedef enum {OOSQL_SERVER_MACHINE_WIN32, OOSQL_SERVER_MACHINE_UNIX} OOSQL_ServerMachineType;

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





typedef Four OOSQL_UtilHandle;

struct OOSQL_Util_SortKeyInfo {
    OOSQL_Util_SortKeyInfo();
    ~OOSQL_Util_SortKeyInfo();
    
	Two                     columnNo;
	Two                     sortOrder;
};

struct OOSQL_Util_SortOption {
    OOSQL_Util_SortOption();
    ~OOSQL_Util_SortOption();

	Two						nSortKeys;
	OOSQL_Util_SortKeyInfo	sortKeyInfo[128];
	Four					nResultsToSort;	
};
%addmethods OOSQL_Util_SortOption {
    void setSortKeyInfoColumnNo(int index, short columnNo) {
        self->sortKeyInfo[index].columnNo = columnNo;
    }
    void setSortKeyInfoSortOrder(int index, short sortOrder) {
        self->sortKeyInfo[index].sortOrder = sortOrder;
    }
};            
#define UTIL_CREATE     0
#define UTIL_APPEND     1
#define UTIL_ASCE_ORDER 0
#define UTIL_DESC_ORDER 1



        
%apply int*   INT_OUT { Four* OUTPUT }     
%apply int*   INT_OUT { Two* OUTPUT }
%apply int*   INT_OUT { OOSQL_Handle* OUTPUT }
/*
%apply int*   INT_OUT { OOSQL_Date* OUTPUT }
*/
%apply int*   INT_OUT { OOSQL_UtilHandle* OUTPUT }


Four 	OOSQL_CreateSystemHandle(OOSQL_SystemHandle* systemHandle, Four* OUTPUT /* procIndex */);
Four 	OOSQL_DestroySystemHandle(OOSQL_SystemHandle* systemHandle, Four procIndex);

/* remove RPC API
Four    OOSQL_Connect(char* hostAddress, char* protocolString, OOSQL_SystemHandle* systemHandle);
Four    OOSQL_Disconnect(OOSQL_SystemHandle* systemHandle);
*/
Four	OOSQL_GetServerMachineType(OOSQL_SystemHandle* systemHandle, Four* OUTPUT /* serverMachineType */);

Four 	OOSQL_SetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four volumeID);
Four 	OOSQL_GetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four* OUTPUT /* volumeID */);
Four 	OOSQL_GetVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, char* volumeName, Four* OUTPUT /* volumeID */);
Four	OOSQL_GetVolumeIDByNumber(OOSQL_SystemHandle* systemHandle,	Four databaseID, Four number, Four* OUTPUT /* volumeID */);

Four 	OOSQL_MountDB(OOSQL_SystemHandle* systemHandle, char* databaseName, Four* OUTPUT /* databaseID */);
Four 	OOSQL_DismountDB(OOSQL_SystemHandle* systemHandle, Four databaseID);
Four	OOSQL_MountVolumeByVolumeName(OOSQL_SystemHandle* systemHandle, char* databaseName,	char* volumeName, Four* OUTPUT /* volID */);

Four	OOSQL_AllocHandle(OOSQL_SystemHandle* systemHandle, Four volID, OOSQL_Handle* OUTPUT /* handle */);
Four	OOSQL_FreeHandle(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);

Four	OOSQL_Mount(OOSQL_SystemHandle* systemHandle, Four numDevices, char **STRING_IN, Four* OUTPUT /* volID */);
Four	OOSQL_Dismount(OOSQL_SystemHandle* systemHandle, Four volID);

Four	OOSQL_TransBegin(OOSQL_SystemHandle* systemHandle, XactID *xactId, ConcurrencyLevel ccLevel);
Four	OOSQL_TransCommit(OOSQL_SystemHandle* systemHandle, XactID *xactId);
Four	OOSQL_TransAbort(OOSQL_SystemHandle* systemHandle, XactID *xactId);

Four	OOSQL_Prepare(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four	OOSQL_Execute(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
Four	OOSQL_ExecDirect(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four	OOSQL_Next(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);

Four	OOSQL_GetData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, char* BYTE, Four bufferLength, Four* OUTPUT /* returnLength */);
Four	OOSQL_GetMultiColumnData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nColumns, OOSQL_GetDataStruct* getDataStruct);
Four    OOSQL_GetMultipleResults(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nResultsToRead, char* BYTE, Four headerBufferSize, char* BYTE, Four dataBufferSize, Four* OUTPUT /* nResultsRead */);
Four	OOSQL_GetComplexTypeInfo(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, OOSQL_ComplexTypeInfo* complexTypeInfo);
Four	OOSQL_PutData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, char* BYTE, Four bufferLength);
Four	OOSQL_GetOID(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two targetNumber, OID* oid);

Four	OOSQL_GetNumResultCols(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two* OUTPUT /* nCols */);
Four	OOSQL_GetResultColName(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, char* BYTE, Four bufferLength);
Four	OOSQL_GetResultColType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* OUTPUT /* columnType */);
Four	OOSQL_GetResultColLength(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* OUTPUT /* resultColLength */);
Four	OOSQL_GetPutDataParamType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two paramNumber, Four* OUTPUT /* paramType */);

Four	OOSQL_GetErrorMessage(OOSQL_SystemHandle* systemHandle, Four errorCode, char* BYTE, Four bufferLength);
Four	OOSQL_GetErrorName(OOSQL_SystemHandle* systemHandle, Four errorCode, char* BYTE, Four bufferLength);
Four	OOSQL_GetQueryErrorMessage(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* BYTE, Four bufferLength);
Four	OOSQL_OIDToOIDString(OOSQL_SystemHandle* systemHandle, OID* oid, char* BYTE);

Four	OOSQL_Text_AddKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName, Four *OUTPUT);
Four	OOSQL_Text_AddDefaultKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName);
Four	OOSQL_Text_DropKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractorName, Four version);
Four	OOSQL_Text_GetKeywordExtractorNo(OOSQL_SystemHandle* systemHandle, Four volID, char* keywordExtractorName, Four version, Four* OUTPUT /* keywordExtractorNo */);
Four	OOSQL_Text_SetKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columeName, Four keywordExtractorNo);
Four	OOSQL_Text_AddFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version, char *filterFilePath, char *filterFunctionName, Four *OUTPUT);
Four	OOSQL_Text_DropFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version);
Four	OOSQL_Text_GetFilterNo(OOSQL_SystemHandle* systemHandle, Four volID, char* filterName, Four version, Four* OUTPUT /* filterNo */);
Four	OOSQL_Text_SetFilter(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, Four filterNo);
Four	OOSQL_Text_MakeIndex(OOSQL_SystemHandle* systemHandle, Four volID, Four temporaryVolId, char* className);

Four 	OOSQL_Text_KeywordInfoScan_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, char* keyword);
Four 	OOSQL_Text_KeywordInfoScan_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
Four 	OOSQL_Text_KeywordInfoScan_Next(OOSQL_SystemHandle* systemHandle, Four scanId, char* BYTE, Four* OUTPUT /* nDocuments */, Four* OUTPUT /* nPositions */);

Four 	OOSQL_Text_KeywordInfoScanForDocument_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, OID* oid, char* columnName, char* keyword);
Four 	OOSQL_Text_KeywordInfoScanForDocument_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
Four 	OOSQL_Text_KeywordInfoScanForDocument_Next(OOSQL_SystemHandle* systemHandle, Four  scanId, char* BYTE, Four* OUTPUT /* nDocuments */, Four* OUTPUT /* nPositions */);

Four	OOSQL_Text_FetchContent(OOSQL_SystemHandle* systemHandle, Four volId, char* className, Four colNo, OID* oid, Four bufferLength, char* BYTE, Four* OUTPUT /* returnLength */);
Four	OOSQL_Text_DefinePostingStructure(OOSQL_SystemHandle *systemHandle, Four volID, char *className, char *attrName, OOSQL_PostingStructureInfo *postingInfo);
Four	OOSQL_Text_ConvertStatementToQueryString(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* columnName, char* operatorString, char* statement, Four queryBufferLength, char* BYTE);
Four	OOSQL_Text_GetNumKeywordsInDocument(OOSQL_SystemHandle* systemHandle, Four volId, OID* oid, char* columnName, Four* OUTPUT /* numKeywords */);
Four	OOSQL_Text_GetIthKeywordInDocument(OOSQL_SystemHandle* systemHandle, Four volId, OID* oid, char* columnName, Four ith, char* BYTE);

Four	OOSQL_ComplexType_GetElementType(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* OUTPUT /* elementType */);
Four	OOSQL_ComplexType_GetComplexType(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* OUTPUT /* complexType */);
Four	OOSQL_ComplexType_GetNumElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* OUTPUT /* numElements */);
Four	OOSQL_ComplexType_GetElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, Four* OUTPUT, Four sizeOfElements, char* BYTE);
Four	OOSQL_ComplexType_GetElementsString(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, char* BYTE, Four stringLength);
Four    OOSQL_ComplexType_InsertElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, Four* OUTPUT /* elementSizes */, char* BYTE);
Four    OOSQL_ComplexType_IsNULL(OOSQL_ComplexTypeInfo*  complexTypeInfo);

OOSQL_TimeZone  OOSQL_GetLocalTimeZone(OOSQL_SystemHandle* systemHandle);
void            OOSQL_SetCurTime(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time, OOSQL_TimeZone tz);
unsigned short  OOSQL_GetHour(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short  OOSQL_GetMinute(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short  OOSQL_GetSecond(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short  OOSQL_GetYear(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
unsigned short  OOSQL_GetMonth(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
unsigned short  OOSQL_GetDay(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);

char*   OOSQL_GetVersionString(void);
char*   OOSQL_GetCompilationParamString(void);
Four    OOSQL_GetTimeElapsed(OOSQL_SystemHandle* systemHandle, Four* OUTPUT /* timeInMilliSeconds */);
Four    OOSQL_ResetTimeElapsed(OOSQL_SystemHandle* systemHandle);
Four OOSQL_ReportTimeAndPageAccess(OOSQL_SystemHandle* systemHandle);
Four OOSQL_ResetPageAccessed(OOSQL_SystemHandle* systemHandle);

Four OOSQL_FormatDataVolume(OOSQL_SystemHandle* systemHandle, Four numOfDevices, char **devNameList, char *volName, Four volId, Four extentSize, Four *numPagesInDevice, Four segmentSize);
Four OOSQL_FormatTempDataVolume(OOSQL_SystemHandle* systemHandle, Four numOfDevices, char **devNameList, char *volName, Four volId, Four extentSize, Four *numPagesInDevice, Four segmentSize);

Four    OOSQL_GetNumTextObjectsInVolume(OOSQL_SystemHandle* systemHandle, Four volId, Four* OUTPUT /* numObjects */);
Four    OOSQL_GetNumObjectsInVolume(OOSQL_SystemHandle* systemHandle, Four volId, Four* OUTPUT/* numObjects */);
Four    OOSQL_GetNumObjectsInClass(OOSQL_SystemHandle* systemHandle, Four volId, char* className, Four* OUTPUT /* numObjects */);

Four OOSQL_EstimateNumResults(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four* OUTPUT /* nResults */);

Four    OOSQL_SetCfgParam(OOSQL_SystemHandle* systemHandle, char *name, char *value);
char*   OOSQL_GetCfgParam(OOSQL_SystemHandle* systemHandle, char *name);

Four  OOSQL_Sleep(double sec);

/* OOSQL Util API */
/*
Four    OOSQL_Util_ExecuteAndWriteToFile(OOSQL_SystemHandle* systemHandle, Four volumeId, Four temporaryVolId, 
        char* queryString, char* fileName, Four maxRows, OOSQL_Util_SortOption* sortOption, One fileMode, Four* OUTPUT);
*/
Four OOSQL_Util_ExecuteAndWriteToFile(OOSQL_SystemHandle* systemHandle, Four volumeId, Four temporaryVolId,
		char* queryString, char* fileName, Four maxRows, OOSQL_Util_SortOption* sortOption, One fileMode,
		Four* elapsedTime, Four* nEstimatedResults, Four* nResults);
Four    OOSQL_Util_GetDataFromFile(OOSQL_SystemHandle* systemHandle, OOSQL_UtilHandle utilHandle, Four rowNo, 
        Four nColumns, OOSQL_GetDataStruct* getDataStruct);
Four    OOSQL_Util_AllocHandle(char* fileName, OOSQL_UtilHandle* OUTPUT);
Four    OOSQL_Util_FinalHandle(OOSQL_UtilHandle handle);


/* Stem() */
int     Stem(char* word);
