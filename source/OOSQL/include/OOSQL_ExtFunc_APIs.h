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

#ifndef _OOSQL_APIs_h_
#define _OOSQL_APIs_h_

#include "OOSQL_errorcodes.h"

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

/* one byte data type */
#if !defined(_BASICTYPES_H_) && !defined(__COSMOS_R_H__) && !defined(__DBLABLIB_)
typedef char                    One;
typedef unsigned char           UOne;

/* two bytes data type */
typedef short                   Two;
typedef unsigned short          UTwo;

/* four bytes data type */
typedef long                    Four;
typedef unsigned long           UFour;

/* Boolean Type */
typedef enum { FALSE, TRUE }    Boolean;

/* Type Definition for Transaction Identifier */
typedef struct {
	char	dummy[8];
} XactID;

typedef struct {                /* OID is used accross the volumes */
	char	dummy[16];
} OID;
#endif

#define  ENDOFEVAL							1
#define  eNOERROR							0

typedef enum {OOSQL_SB_USE_DISK, OOSQL_SB_USE_MEMORY, OOSQL_SB_USE_MEMORY_WITH_DISK} OOSQL_SortBufferMode;
typedef struct {
	Four						sortVolID;
} OOSQL_DiskSortBufferInfo;
typedef struct {
	void*						sortBufferPtr;
	Four						sortBufferLength;
	Four						sortBufferUsedLength;
} OOSQL_MemorySortBufferInfo;
typedef struct {
	OOSQL_SortBufferMode		mode;
	OOSQL_DiskSortBufferInfo	diskInfo;
	OOSQL_MemorySortBufferInfo	memoryInfo;
} OOSQL_SortBufferInfo;
typedef struct {
	Two							columnNumber;
	Four						startPos;
	void*						columnValuePtr;
	Four						bufferLength;
	Four*						dataLength;
} OOSQL_GetDataStruct;
#ifndef _LOM_INTERNAL_H

typedef struct {
	Four serverInstanceId;
	Four lrdsHandle;
	Four instanceId;	
} LOM_Handle;

#endif

#ifndef DBM_MAXVOLUMENAME 
#define DBM_MAXVOLUMENAME     60 
#endif

#ifndef DBM_MAXDATABASENAME
#define DBM_MAXDATABASENAME   60 
#endif

#ifndef MAXNUMOFVOLS             
#define MAXNUMOFVOLS          20 /* MAXNUMOFVOLS is defined in param.h */
#endif

#ifndef MAXNUMOFDEVICES
#define MAXNUMOFDEVICES	      500
#endif

#ifndef MAX_NUM_EMBEDDEDATTRIBUTES
#define MAX_NUM_EMBEDDEDATTRIBUTES	24
#endif

typedef struct {
	Four						volID;
	char						volumeName[DBM_MAXVOLUMENAME];	
	Four						nMounts;
} oosql_UserMountVolumeTable;

typedef struct {
	LOM_Handle					lomSystemHandle;
	Four						instanceId;
	oosql_UserMountVolumeTable	userMountVolumeTable[MAXNUMOFVOLS];	
	Four						userDefaultVolumeID;
	char						databaseName[DBM_MAXDATABASENAME];
	void*						gdsInstance;
	void*						externalFunctionManager;
	void*						externalFunctionDispatcher;
	void*						memoryManager;
} OOSQL_SystemHandle; 

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
typedef UFour_Invariable OOSQL_Date;
typedef struct {
    short _tzHour;
    short _tzMinute;
    short _Hour;
    short _Minute;
    short _Second;
    short _100thSec;
} OOSQL_Time;
typedef struct {
    OOSQL_Date d;
    OOSQL_Time t;
} OOSQL_Timestamp;
typedef double OOSQL_Interval;

typedef struct {
	Boolean isContainingTupleID;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingByteOffset;
	Two		nEmbeddedAttributes;
	Two		embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
} OOSQL_PostingStructureInfo;

#define OOSQL_GET_LOM_SYSTEMHANDLE(oosqlHandle)	(oosqlHandle)->lomSystemHandle
#define OOSQL_GET_LOM_HANDLE(oosqlHandle)	(oosqlHandle)->lomSystemHandle
#define OOSQL_CLIENTINFO(oosqlHandle)  LOM_GDSTABLE[(&OOSQL_GET_LOM_SYSTEMHANDLE(oosqlHandle))->instanceId].connectionInfo

#ifndef WIN32
#define OOSQL_API_FN 
#else
#define OOSQL_API_FN __declspec(dllexport)
#endif

typedef struct {
	void* data;
	Four  size;
} OOSQL_ExtFunc_ScratchPad;

#ifdef __cplusplus
extern "C" {
#endif

typedef Four OOSQL_Handle;

Four (*OOSQL_CreateSystemHandle)(OOSQL_SystemHandle* systemHandle, Four *procIndex);
Four (*OOSQL_DestroySystemHandle)(OOSQL_SystemHandle* systemHandle, Four procIndex);
Four (*OOSQL_Connect)(char* hostAddress, char* protocolString, char* serverPath, OOSQL_SystemHandle* systemHandle);
Four (*OOSQL_Disconnect)(OOSQL_SystemHandle* systemHandle);
Four (*OOSQL_SetUserDefaultVolumeID)(OOSQL_SystemHandle* systemHandle, Four databaseID, Four volumeID);
Four (*OOSQL_GetUserDefaultVolumeID)(OOSQL_SystemHandle* systemHandle, Four databaseID, Four* volumeID);
Four (*OOSQL_GetVolumeID)(OOSQL_SystemHandle* systemHandle, Four databaseID, char* volumeName, Four* volumeID);
Four (*OOSQL_MountDB)(OOSQL_SystemHandle* systemHandle, char* databaseName, Four* databaseID);
Four (*OOSQL_DismountDB)(OOSQL_SystemHandle* systemHandle, Four databaseID);
Four (*OOSQL_AllocHandle)(OOSQL_SystemHandle* systemHandle, Four volID, OOSQL_Handle* handle);
Four (*OOSQL_FreeHandle)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
Four (*OOSQL_Mount)(OOSQL_SystemHandle* systemHandle, Four numDevices, char** devNames, Four* volID);
Four (*OOSQL_Dismount)(OOSQL_SystemHandle* systemHandle, Four volID);
Four (*OOSQL_TransBegin)(OOSQL_SystemHandle* systemHandle, XactID *xactId);
Four (*OOSQL_TransCommit)(OOSQL_SystemHandle* systemHandle, XactID *xactId);
Four (*OOSQL_TransAbort)(OOSQL_SystemHandle* systemHandle, XactID *xactId);
Four (*OOSQL_Prepare)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four (*OOSQL_Execute)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
Four (*OOSQL_ExecDirect)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four (*OOSQL_Next)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
Four (*OOSQL_GetData)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength, Four* dataLength);
Four (*OOSQL_GetMultiColumnData)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nColumns, OOSQL_GetDataStruct* getDataStruct);
Four (*OOSQL_PutData)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength);
Four (*OOSQL_GetOID)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two targetNumber, OID* oid);
Four (*OOSQL_GetNumResultCols)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two* nCols);
Four (*OOSQL_GetResultColName)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, char* columnNameBuffer, Four bufferLength);
Four (*OOSQL_GetResultColType)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* columnType);
Four (*OOSQL_GetResultColLength)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* resultColLength);
Four (*OOSQL_GetErrorMessage)(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
Four (*OOSQL_GetErrorName)(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
Four (*OOSQL_GetQueryErrorMessage)(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* messageBuffer, Four bufferLength);
Four (*OOSQL_OIDToOIDString)(OOSQL_SystemHandle* systemHandle, OID* oid, char* oidString);
Four (*OOSQL_Text_AddKeywordExtractor)(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName, Four *keywordExtractorNo);
Four (*OOSQL_Text_AddDefaultKeywordExtractor)(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName);
Four (*OOSQL_Text_DropKeywordExtractor)(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractorName, Four version);
Four (*OOSQL_Text_SetKeywordExtractor)(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columeName, Four keywordExtractorNo);
Four (*OOSQL_Text_AddFilter)(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version, char *filterFilePath, char *filterFunctionName, Four *filterNo);
Four (*OOSQL_Text_DropFilter)(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version);
Four (*OOSQL_Text_SetFilter)(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, Four filterNo);
Four (*OOSQL_Text_MakeIndex)(OOSQL_SystemHandle* systemHandle, Four volID, char* className);
Four (*OOSQL_Text_KeywordInfoScan_Open)(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, char* keyword);
Four (*OOSQL_Text_KeywordInfoScan_Close)(OOSQL_SystemHandle* systemHandle, Four scanId);
Four (*OOSQL_Text_KeywordInfoScan_Next)(OOSQL_SystemHandle* systemHandle, Four scanId, char* keyword, Four* nDocuments, Four* nPositions);
Four (*OOSQL_Text_KeywordInfoScanForDocument_Open)(OOSQL_SystemHandle* systemHandle, Four volID, char* className, OID* oid, char* columnName, char* keyword);
Four (*OOSQL_Text_KeywordInfoScanForDocument_Close)(OOSQL_SystemHandle* systemHandle, Four scanId);
Four (*OOSQL_Text_KeywordInfoScanForDocument_Next)(OOSQL_SystemHandle* systemHandle, Four  scanId, char* keyword, Four* nDocuments, Four* nPositions);
Four (*OOSQL_Text_DefinePostingStructure)(OOSQL_SystemHandle *systemHandle, Four volID, char *className, char *attrName, OOSQL_PostingStructureInfo *postingInfo);
OOSQL_TimeZone (*OOSQL_GetLocalTimeZone)(OOSQL_SystemHandle* systemHandle);
void (*OOSQL_SetCurTime)(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time, OOSQL_TimeZone tz);
unsigned short (*OOSQL_GetHour)(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short (*OOSQL_GetMinute)(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short (*OOSQL_GetSecond)(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short (*OOSQL_GetYear)(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
unsigned short (*OOSQL_GetMonth)(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
unsigned short (*OOSQL_GetDay)(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
char* (*OOSQL_GetVersionString)(void);
Four (*OOSQL_GetTimeElapsed)(OOSQL_SystemHandle* systemHandle, Four* timeInMilliSeconds);
Four (*OOSQL_ResetTimeElapsed)(OOSQL_SystemHandle* systemHandle);

#ifdef __cplusplus
}
#endif

#endif
