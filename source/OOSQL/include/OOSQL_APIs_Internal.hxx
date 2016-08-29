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

#ifndef _OOSQL_APIS_INTERNAL_H_
#define _OOSQL_APIS_INTERNAL_H_

#include "OOSQL_Common.h"

#if !defined(__OOSQL_STORAGESYSTEMHEADERS_H__) && !defined(__COSMOS_R_H__)
#define OOSQL_TEXT_IN_DB							0
#define OOSQL_TEXT_IN_FILE							1
#define OOSQL_TEXT_IN_MEMORY						2
#define OOSQL_TEXT_DONE								1

typedef Four		    PageNo;
typedef Two			    VolNo;
typedef VolNo		    VolID;
typedef Two			    SlotNo;
typedef UFour		    Unique;
typedef Four		    ClassID;

#undef TRUE
#undef FALSE
typedef enum { FALSE, TRUE }    Boolean;

typedef struct XactID {    
	UFour high;
	UFour low;
} XactID;
typedef struct OID {						// OID is used accross the volumes 
	PageNo			pageNo;					// specify the page holding the object 
	VolID			volNo;					// specify the volume in which object is in 
	SlotNo			slotNo;					// specify the slot within the page 
	Unique			unique;					// Unique No for checking dangling object 
	ClassID			classID;				// specify the class including the object 
} OID;
typedef enum { X_BROWSE_BROWSE, X_CS_BROWSE, X_CS_CS, X_RR_BROWSE, X_RR_CS, X_RR_RR } ConcurrencyLevel; /* isolation degree */
#endif

#ifndef _LOM_INTERNAL_H
#define LOM_GET_LRDS_HANDLE(lomHandle)  (lomHandle)->lrdsHandle
typedef struct LOM_Handle {
	Four serverInstanceId;
    Four lrdsHandle;
	Four instanceId;	
} LOM_Handle;
#endif

#define ENDOFEVAL				1

#define OOSQL_COMPLEXTYPE_BASIC 0
#define OOSQL_COMPLEXTYPE_SET   3
#define OOSQL_COMPLEXTYPE_BAG   4
#define OOSQL_COMPLEXTYPE_LIST  5

#define OOSQL_TYPE_SMALLINT		0
#define OOSQL_TYPE_SHORT		0

#define OOSQL_TYPE_INTEGER		1
#define OOSQL_TYPE_INT			1

#define OOSQL_TYPE_LONG			2
#define OOSQL_TYPE_LONG_LONG	14	

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

#define OOSQL_TYPE_GEOMETRY		128
#define OOSQL_TYPE_POINT		129
#define OOSQL_TYPE_LINESTRING		130
#define OOSQL_TYPE_POLYGON		131
#define OOSQL_TYPE_GEOMETRYCOLLECTION	132
#define OOSQL_TYPE_MULTIPOINT		133
#define OOSQL_TYPE_MULTILINESTRING	134
#define OOSQL_TYPE_MULTIPOLYGON		135

#define OOSQL_TYPE_SET_SMALLINT		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_SMALLINT)
#define OOSQL_TYPE_SET_SHORT		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_SHORT)
#define OOSQL_TYPE_SET_INTEGER		((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_INTEGER)
#define OOSQL_TYPE_SET_INT			((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_INT)
#define OOSQL_TYPE_SET_LONG			((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_LONG)
#define OOSQL_TYPE_SET_LONG_LONG	((OOSQL_COMPLEXTYPE_SET << 16) | OOSQL_TYPE_LONG_LONG)
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
#define OOSQL_TYPE_BAG_LONG_LONG	((OOSQL_COMPLEXTYPE_BAG << 16) | OOSQL_TYPE_LONG_LONG)
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
#define OOSQL_TYPE_LIST_LONG_LONG	((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_LONG_LONG)
#define OOSQL_TYPE_LIST_REAL		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_REAL)
#define OOSQL_TYPE_LIST_FLOAT		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_FLOAT)
#define OOSQL_TYPE_LIST_DOUBLE		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_DOUBLE)
#define OOSQL_TYPE_LIST_CHAR		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_CHAR)
#define OOSQL_TYPE_LIST_STRING		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_STRING)
#define OOSQL_TYPE_LIST_VARCHAR		((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_VARCHAR)
#define OOSQL_TYPE_LIST_VARSTRING	((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_VARSTRING)
#define OOSQL_TYPE_LIST_OID       	((OOSQL_COMPLEXTYPE_LIST << 16) | OOSQL_TYPE_OID)

#undef  OOSQL_TYPE_SMALLINT_SIZE
#define OOSQL_TYPE_SMALLINT_SIZE	sizeof(Two_Invariable)

#undef  OOSQL_TYPE_SHORT_SIZE
#define OOSQL_TYPE_SHORT_SIZE		sizeof(Two_Invariable)

#undef  OOSQL_TYPE_INTEGER_SIZE
#define OOSQL_TYPE_INTEGER_SIZE		sizeof(Four_Invariable)

#undef  OOSQL_TYPE_INT_SIZE
#define OOSQL_TYPE_INT_SIZE			sizeof(Four_Invariable)

#undef  OOSQL_TYPE_LONG_SIZE
#define OOSQL_TYPE_LONG_SIZE		sizeof(Four_Invariable)

#undef  OOSQL_TYPE_LONG_LONG_SIZE
#define OOSQL_TYPE_LONG_LONG_SIZE	sizeof(Eight_Invariable)

#undef	OOSQL_TYPE_LONG_VAR
#undef	OOSQL_TYPE_LONG_SIZE_VAR
#ifndef SUPPORT_LARGE_DATABASE2
#define OOSQL_TYPE_LONG_VAR			OOSQL_TYPE_LONG
#define OOSQL_TYPE_LONG_SIZE_VAR	OOSQL_TYPE_LONG_SIZE
#else
#define OOSQL_TYPE_LONG_VAR			OOSQL_TYPE_LONG_LONG
#define OOSQL_TYPE_LONG_SIZE_VAR	OOSQL_TYPE_LONG_LONG_SIZE
#endif

#undef  OOSQL_TYPE_REAL_SIZE
#define OOSQL_TYPE_REAL_SIZE		sizeof(float)
#undef  OOSQL_TYPE_FLOAT_SIZE
#define OOSQL_TYPE_FLOAT_SIZE		sizeof(float)

#undef  OOSQL_TYPE_DOUBLE_SIZE
#define OOSQL_TYPE_DOUBLE_SIZE		sizeof(double)

#undef  OOSQL_TYPE_OID_SIZE
#define OOSQL_TYPE_OID_SIZE			sizeof(OID)

#undef  OOSQL_TYPE_DATE_SIZE
#define OOSQL_TYPE_DATE_SIZE		sizeof(Four_Invariable) 
#undef  OOSQL_TYPE_TIME_SIZE
#define OOSQL_TYPE_TIME_SIZE		sizeof(Four_Invariable)

#undef  OOSQL_TYPE_TIMESTAMP_SIZE
#define OOSQL_TYPE_TIMESTAMP_SIZE	sizeof(Four_Invariable)

#define	OOSQL_NONSPATIAL_CLASSTYPE		0
#define	OOSQL_POINT_CLASSTYPE			1
#define	OOSQL_LINESEG_CLASSTYPE			2
#define	OOSQL_POLYGON_CLASSTYPE			3
#define	OOSQL_POLYLINE_CLASSTYPE		4

#define INITQUERYINSTANCETABLE		20

#define OOSQL_MAXHOSTADDRESS      256
#define OOSQL_MAXPROTOCOLSTRING   60

#ifndef DBM_MAXVOLUMENAME
#define DBM_MAXVOLUMENAME 			60
#endif

#ifndef DBM_MAXDATABASENAME
#define DBM_MAXDATABASENAME 		60
#endif

#ifndef MAXNUMOFVOLS             
#define MAXNUMOFVOLS				20 /* MAXNUMOFVOLS is defined in param.h */
#endif

#ifndef MAXNUMOFDEVICES
#define MAXNUMOFDEVICES				500
#endif

#ifndef MAX_NUM_EMBEDDEDATTRIBUTES
#define MAX_NUM_EMBEDDEDATTRIBUTES	24
#endif

#define OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) \
	(sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + \
     sizeof(Four) * (nColumns) + sizeof(OID) * (nColumns) + sizeof(Four) * (nColumns))

#define OOSQL_MULTIPLERESULT_NTH_OBJECT_OFFSET(headerBuffer, nColumns, i) \
	(*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i)))
	
#define OOSQL_MULTIPLERESULT_NTH_OBJECT_SIZE(headerBuffer, nColumns, i) \
	(*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four)))

#define OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_ISNULL(headerBuffer, nColumns, i, j) \
	(((char*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four)))[((j) / 8)] & ((unsigned int)0x80 >>((j) % 8)))
	
#define OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_SIZE(headerBuffer, nColumns, i, j) \
	(*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (j)))

#define OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_REALSIZE(headerBuffer, nColumns, i, j) \
	(*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + sizeof(Four) * (j)))

#define OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_OID(headerBuffer, nColumns, i, j) \
	(*(OID*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + sizeof(Four) * (nColumns) + sizeof(OID) * (j)))

#define OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_COLNO(headerBuffer, nColumns, i, j) \
	(*(Four*)(((char*)headerBuffer) + OOSQL_MULTIPLERESULT_HEADER_SIZE(nColumns) * (i) + sizeof(Four) + sizeof(Four) + ((((nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) + sizeof(Four) * (nColumns) + sizeof(Four) * (nColumns) + sizeof(OID) * (nColumns) + sizeof(Four) * (j)))

typedef struct oosql_UserMountVolumeTable {
	Four						volID;
	char						volumeName[DBM_MAXVOLUMENAME];	
	Four						nMounts;
	Four						nObjects;
} oosql_UserMountVolumeTable;

#ifdef __cplusplus
class OOSQL_ExternalFunctionManager;
class OOSQL_ExternalFunctionDispatcher;
class OOSQL_MemoryManager;
class OOSQL_ServerQuery;
#else
typedef int OOSQL_ExternalFunctionManager;
typedef int OOSQL_ExternalFunctionDispatcher;
typedef int OOSQL_MemoryManager;
typedef int OOSQL_ServerQuery;
#endif

typedef struct OOSQL_ScratchPad {
	void* data;
	Four  size;
} OOSQL_ScratchPad;

typedef struct OOSQL_DbInfo {
	char dbname;
} OOSQL_DbInfo;

typedef struct OOSQL_SystemHandle {
	LOM_Handle							lomSystemHandle;
	Four								instanceId;
} OOSQL_SystemHandle; 

typedef struct OOSQL_QueryInstance {
	OOSQL_ServerQuery*	query;
	Boolean				inUse;
} OOSQL_QueryInstance;

#if !defined(__RPC_HEADER__) && !defined(_RPC_RPC_H)
typedef int CLIENT;
#endif

typedef enum {OOSQL_SERVER_MACHINE_WIN32, OOSQL_SERVER_MACHINE_UNIX} OOSQL_ServerMachineType;
typedef struct OOSQL_ConnectionInfo {
	CLIENT*		serverHandle;
	CLIENT*		brokerHandle;
	Four		serverNo;
	pid_t 		serverPid;		
	char		hostAddress[OOSQL_MAXHOSTADDRESS];
	char		protocolString[OOSQL_MAXPROTOCOLSTRING];
	OOSQL_ServerMachineType serverMachineType;
} OOSQL_ConnectionInfo;

typedef struct OOSQL_GDSInstance {
	VarArray							queryInstanceTable;
	Boolean								inUse;
	oosql_UserMountVolumeTable			userMountVolumeTable[MAXNUMOFVOLS];	
	Four								userDefaultVolumeID;
	char								databaseName[DBM_MAXDATABASENAME];
	OOSQL_ExternalFunctionManager*		externalFunctionManager;
	OOSQL_ExternalFunctionDispatcher*	externalFunctionDispatcher;
	OOSQL_MemoryManager*				memoryManager;
	OOSQL_DbInfo						dbInfo;
	OOSQL_ConnectionInfo				connectionInfo;
} OOSQL_GDSInstance;

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

typedef struct OOSQL_Time {
    short _tzHour;
    short _tzMinute;
    short _Hour;
    short _Minute;
    short _Second;
    short _100thSec;
} OOSQL_Time;
typedef struct OOSQL_Timestamp {
    OOSQL_Date d;
    OOSQL_Time t;
} OOSQL_Timestamp;
typedef double OOSQL_Interval;

typedef struct OOSQL_PostingStructureInfo {
	Boolean isContainingTupleID;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingByteOffset;
	Two		nEmbeddedAttributes;
	Two		embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
} OOSQL_PostingStructureInfo;

typedef struct OOSQL_ComplexTypeInfo {
	OOSQL_SystemHandle* systemHandle;
	OID					oid;
	Four				colNo;
	Four				orn;
	Four				complexType;
	Four				elementType;
} OOSQL_ComplexTypeInfo;

typedef enum {OOSQL_SB_USE_DISK, OOSQL_SB_USE_MEMORY, OOSQL_SB_USE_MEMORY_WITH_DISK} OOSQL_SortBufferMode;
typedef struct OOSQL_DiskSortBufferInfo {
	Four						sortVolID;
} OOSQL_DiskSortBufferInfo;
typedef struct OOSQL_MemorySortBufferInfo {
	void*						sortBufferPtr;
	Four						sortBufferLength;
	Four						sortBufferUsedLength;
} OOSQL_MemorySortBufferInfo;
typedef struct OOSQL_SortBufferInfo {
	OOSQL_SortBufferMode		mode;
	OOSQL_DiskSortBufferInfo	diskInfo;
	OOSQL_MemorySortBufferInfo	memoryInfo;
} OOSQL_SortBufferInfo;
typedef struct OOSQL_GetDataStruct {
	Two							columnNumber;
	Four						startPos;
	void*						bufferPtr;
	Four						bufferLength;
	Four						returnLength;
} OOSQL_GetDataStruct;

typedef struct OOSQL_MBR {
    Four_Invariable values[4];
} OOSQL_MBR;

typedef struct OOSQL_Point {
	Four_Invariable	x, y;
} OOSQL_Point;

extern VarArray oosqlGDSInstanceTable;

#define OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle)	(systemHandle->lomSystemHandle)
#define OOSQL_GDSINSTTABLE							((OOSQL_GDSInstance*)oosqlGDSInstanceTable.ptr)
#define OOSQL_GDSINSTTABLE_ENTRIES					(oosqlGDSInstanceTable.nEntries)
#define OOSQL_QUERYINSTTABLE(systemHandle)			((OOSQL_QueryInstance*)(OOSQL_GDSINSTTABLE[(systemHandle)->instanceId].queryInstanceTable.ptr))
#define OOSQL_QUERYINSTTABLE_ENTRIES(systemHandle)	(OOSQL_GDSINSTTABLE[(systemHandle)->instanceId].queryInstanceTable.nEntries)
#define OOSQL_CHECKGDSINSTTABLE(systemhandle)		((systemHandle)->instanceId >=0 && OOSQL_GDSINSTTABLE[(systemHandle)->instanceId].inUse)
#define OOSQL_CLIENTINFO(systemHandle)				(OOSQL_GDSINSTTABLE[(systemHandle)->instanceId].connectionInfo)



typedef struct OOSQL_UDTObject {
    char*   encodedUDTObjectData;
    Four    encodedUDTObjectSize;
} OOSQL_UDTObject;


#define UDTOBJECT_DEFAULT_SIZE                  1024 * 8
#define UDTOBJECT_MAX_SIZE                      1024 * 1024


#define UDTOBJECT_POINTER_ELEMENT_SIZE          4
#define UDTOBJECT_TYPE_ELEMENT_SIZE             4

#define UDTOBJECT_LENGTH_OFFSET                 0
#define UDTOBJECT_TYPEID_OFFSET                 4
#define UDTOBJECT_NATTRS_OFFSET                 8
#define UDTOBJECT_POINTER_ARRAY_OFFSET          12
#define UDTOBJECT_TYPE_ARRAY_OFFSET(nAttrs)     (UDTOBJECT_POINTER_ARRAY_OFFSET + nAttrs*UDTOBJECT_POINTER_ELEMENT_SIZE)

#define UDTOBJECT_FIXED_HEADER_SIZE             UDTOBJECT_POINTER_ARRAY_OFFSET
#define UDTOBJECT_VARIABLE_HEADER_SIZE(nAttrs)  (nAttrs*UDTOBJECT_POINTER_ELEMENT_SIZE + nAttrs*UDTOBJECT_TYPE_ELEMENT_SIZE)
#define UDTOBJECT_HEADER_SIZE(nAttrs)           (UDTOBJECT_FIXED_HEADER_SIZE + UDTOBJECT_VARIABLE_HEADER_SIZE(nAttrs))


#ifdef __cplusplus
extern "C" {
#endif

typedef Four OOSQL_Handle;

Four    OOSQL_CreateSystemHandle(OOSQL_SystemHandle* systemHandle, Four *procIndex);
Four    OOSQL_DestroySystemHandle(OOSQL_SystemHandle* systemHandle, Four procIndex);

Four    OOSQL_Connect(char* hostAddress, char* protocolString, OOSQL_SystemHandle* systemHandle);
Four    OOSQL_Connect2(char* hostAddress, char* protocolString, char* serverPath, OOSQL_SystemHandle* systemHandle);
Four    OOSQL_Disconnect(OOSQL_SystemHandle* systemHandle);
Four	OOSQL_GetServerMachineType(OOSQL_SystemHandle* systemHandle, OOSQL_ServerMachineType* serverMachineType);

Four    OOSQL_SetUserDefaultVolume(OOSQL_SystemHandle* systemHandle, char* volumeName);
Four    OOSQL_SetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four volumeID);
Four    OOSQL_GetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four* volumeID);
Four 	OOSQL_GetVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, char* volumeName, Four* volumeID);
Four	OOSQL_GetVolumeIDByNumber(OOSQL_SystemHandle* systemHandle,	Four databaseID, Four number, Four* volumeID);

Four    OOSQL_MountDB(OOSQL_SystemHandle* systemHandle, char* databaseName, Four* databaseId);
Four    OOSQL_DismountDB(OOSQL_SystemHandle* systemHandle, Four databaseID);
Four	OOSQL_MountVolumeByVolumeName(OOSQL_SystemHandle* systemHandle, char* databaseName,	char* volumeName, Four* volID);

Four	OOSQL_AllocHandle(OOSQL_SystemHandle* systemHandle, Four volID, OOSQL_Handle* handle);
Four	OOSQL_FreeHandle(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);

Four	OOSQL_Mount(OOSQL_SystemHandle* systemHandle, Four numDevices, char** devNames, Four* volID);
Four	OOSQL_Dismount(OOSQL_SystemHandle* systemHandle, Four volID);

Four	OOSQL_TransBegin(OOSQL_SystemHandle* systemHandle, XactID *xactId, ConcurrencyLevel ccLevel);
Four	OOSQL_TransCommit(OOSQL_SystemHandle* systemHandle, XactID *xactId);
Four	OOSQL_TransAbort(OOSQL_SystemHandle* systemHandle, XactID *xactId);

Four	OOSQL_Prepare(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four	OOSQL_Execute(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
Four	OOSQL_ExecDirect(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
Four	OOSQL_Next(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);

Four	OOSQL_GetData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength, Four* dataLength);
Four	OOSQL_GetMultiColumnData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nColumns, OOSQL_GetDataStruct* getDataStruct);
Four	OOSQL_GetMultipleResults(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nResultsRead);
Four	OOSQL_GetComplexTypeInfo(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, OOSQL_ComplexTypeInfo* complexTypeInfo);
Four	OOSQL_PutData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength);
Four	OOSQL_GetOID(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two targetNumber, OID* oid);
Four	OOSQL_GetClassName(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two targetNumner, char* classNameBuffer, Four bufferLength);

Four	OOSQL_GetNumResultCols(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two* nCols);
Four	OOSQL_GetResultColName(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, char* columnNameBuffer, Four bufferLength);
Four	OOSQL_GetResultColType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* columnType);
Four	OOSQL_GetResultColLength(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* resultColLength);
Four	OOSQL_GetResutlSpatialClassType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* columnType);
Four	OOSQL_GetPutDataParamType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two paramNumber, Four* paramType);

Four	OOSQL_GetErrorMessage(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
Four	OOSQL_GetErrorName(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
Four	OOSQL_GetQueryErrorMessage(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* messageBuffer, Four bufferLength);
Four	OOSQL_OIDToOIDString(OOSQL_SystemHandle* systemHandle, OID* oid, char* oidString);

Four	OOSQL_Text_AddKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName, Four *keywordExtractorNo);
Four	OOSQL_Text_AddDefaultKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName);
Four	OOSQL_Text_DropKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractorName, Four version);
Four	OOSQL_Text_GetKeywordExtractorNo(OOSQL_SystemHandle* systemHandle, Four volID, char* keywordExtractorName, Four version, Four* keywordExtractorNo);
Four	OOSQL_Text_SetKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columeName, Four keywordExtractorNo);
Four	OOSQL_Text_AddFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version, char *filterFilePath, char *filterFunctionName, Four *filterNo);
Four	OOSQL_Text_DropFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version);
Four	OOSQL_Text_GetFilterNo(OOSQL_SystemHandle* systemHandle, Four volID, char* filterName, Four version, Four* filterNo);
Four	OOSQL_Text_SetFilter(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, Four filterNo);
Four	OOSQL_Text_MakeIndex(OOSQL_SystemHandle* systemHandle, Four volID, Four temporaryVolId, char* className);

Four    OOSQL_Text_KeywordInfoScan_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, char* keyword);
Four    OOSQL_Text_KeywordInfoScan_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
Four    OOSQL_Text_KeywordInfoScan_Next(OOSQL_SystemHandle* systemHandle, Four scanId, char* keyword, Four* nDocuments, Four* nPositions);

Four    OOSQL_Text_KeywordInfoScanForDocument_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, OID* oid, char* columnName, char* keyword);
Four    OOSQL_Text_KeywordInfoScanForDocument_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
Four    OOSQL_Text_KeywordInfoScanForDocument_Next(OOSQL_SystemHandle* systemHandle, Four  scanId, char* keyword, Four* nDocuments, Four* nPositions);

Four	OOSQL_Text_FetchContent(OOSQL_SystemHandle* systemHandle, Four volId, char* className, Four colNo, OID* oid, Four bufferLength, void* buffer, Four* returnLength);
Four	OOSQL_Text_DefinePostingStructure(OOSQL_SystemHandle *systemHandle, Four volID, char *className, char *attrName, OOSQL_PostingStructureInfo *postingInfo);
Four	OOSQL_Text_ConvertStatementToQueryString(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* columnName, char* operatorString, char* statement, Four queryBufferLength, char* queryBuffer);
Four	OOSQL_Text_GetNumKeywordsInDocument(OOSQL_SystemHandle* systemHandle, Four volId, OID* oid, char* columnName, Four* numKeywords);
Four	OOSQL_Text_GetIthKeywordInDocument(OOSQL_SystemHandle* systemHandle, Four volId, OID* oid, char* columnName, Four ith, char* keyword);

Four	OOSQL_ComplexType_GetElementType(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* elementType);
Four	OOSQL_ComplexType_GetComplexType(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* complexType);
Four	OOSQL_ComplexType_GetNumElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four* numElements);
Four	OOSQL_ComplexType_GetElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements);
Four	OOSQL_ComplexType_GetElementsString(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, char* string, Four stringLength);
Four    OOSQL_ComplexType_InsertElements(OOSQL_ComplexTypeInfo* complexTypeInfo, Four start, Four nElements, Four* elementSizes, void* elements);
Four    OOSQL_ComplexType_IsNULL(OOSQL_ComplexTypeInfo*  complexTypeInfo);

Four	OOSQL_Spatial_IsSpatialObject(OOSQL_SystemHandle* systemHandle, Four volID, OID* oid);
Four	OOSQL_Spatial_GetPoints(OOSQL_SystemHandle* systemHandle, Four volID, OID* oid, Four startPoint, Four nPoints, OOSQL_Point* points);
Four	OOSQL_Spatial_GetMBR(OOSQL_SystemHandle* systemHandle, Four volID, OID* oid, OOSQL_MBR* mbr);
Four	OOSQL_Spatial_GetSpatialClassType(OOSQL_SystemHandle* systemHandle, Four volID, char* className, Four* classType);

OOSQL_TimeZone OOSQL_GetLocalTimeZone(OOSQL_SystemHandle* systemHandle);
void OOSQL_SetCurTime(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time, OOSQL_TimeZone tz);
unsigned short OOSQL_GetHour(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short OOSQL_GetMinute(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short OOSQL_GetSecond(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
unsigned short OOSQL_GetYear(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
unsigned short OOSQL_GetMonth(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
unsigned short OOSQL_GetDay(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);

Four OOSQL_DumpPlan(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, void* outBuffer, int outBufferSize);

char* OOSQL_GetVersionString(void);
char* OOSQL_GetCompilationParamString(void);
Four OOSQL_GetTimeElapsed(OOSQL_SystemHandle* systemHandle, Four* timeInMilliSeconds);
Four OOSQL_ResetTimeElapsed(OOSQL_SystemHandle* systemHandle);
Four OOSQL_ReportTimeAndPageAccess(OOSQL_SystemHandle* systemHandle);
Four OOSQL_ResetPageAccessed(OOSQL_SystemHandle* systemHandle);

Four OOSQL_FormatDataVolume(OOSQL_SystemHandle* systemHandle, Four numOfDevices, char **devNameList, char *volName, Four volId, Four extentSize, Four *numPagesInDevice, Four segmentSize);
Four OOSQL_FormatTempDataVolume(OOSQL_SystemHandle* systemHandle, Four numOfDevices, char **devNameList, char *volName, Four volId, Four extentSize, Four *numPagesInDevice, Four segmentSize);
Four OOSQL_GetNumObjectsInVolume(OOSQL_SystemHandle* systemHandle, Four volId, Four* numObjects);
Four OOSQL_GetNumTextObjectsInVolume(OOSQL_SystemHandle* systemHandle, Four volId, Four* numObjects);
Four OOSQL_GetNumObjectsInClass(OOSQL_SystemHandle* systemHandle, Four volId, char* className, Four* numObjects);
Four OOSQL_CheckFeature(OOSQL_SystemHandle* systemHandle, char* feature, Boolean* result);

Four OOSQL_EstimateNumResults(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four* nResults);

Four  OOSQL_SetCfgParam(OOSQL_SystemHandle* systemHandle, char *name, char *value);
char* OOSQL_GetCfgParam(OOSQL_SystemHandle* systemHandle, char *name);

Four  OOSQL_Sleep(double sec);




Four OOSQL_UDTObject_AllocBuffer(
    OOSQL_UDTObject*    UDTObject,
    Four                size);

Four OOSQL_UDTObject_FreeBuffer(
    OOSQL_UDTObject*    UDTObject);

char* OOSQL_UDTObject_GetData(
    OOSQL_UDTObject*    UDTObject);

void OOSQL_UDTObject_SetData(
    OOSQL_UDTObject*    UDTObject,
    char*               data);

Four OOSQL_UDTObject_GetSize(
    OOSQL_UDTObject*    UDTObject);

void OOSQL_UDTObject_SetSize(
    OOSQL_UDTObject*    UDTObject,
    Four                size);

Four OOSQL_UDTObject_GetLength(
    OOSQL_UDTObject*    UDTObject);

void OOSQL_UDTObject_SetLength(
    OOSQL_UDTObject*    UDTObject,
    Four                length);

Four OOSQL_UDTObject_GetTypeID(
    OOSQL_UDTObject*    UDTObject);

void OOSQL_UDTObject_SetTypeID(
    OOSQL_UDTObject*    UDTObject,
    Four                UDTId);

Four OOSQL_UDTObject_GetNAttrs(
    OOSQL_UDTObject*    UDTObject);

void OOSQL_UDTObject_SetNAttrs(
    OOSQL_UDTObject*    UDTObject,
    Four nAttrs);

Four OOSQL_UDTObject_GetNTHAttrOffset(
    OOSQL_UDTObject*    UDTObject,
    Four                nth);

void OOSQL_UDTObject_SetNTHAttrOffset(
    OOSQL_UDTObject*    UDTObject,
    Four                nth,
    Four                offset);

Four OOSQL_UDTObject_GetNTHAttrType(
    OOSQL_UDTObject*    UDTObject,
    Four                nth);

void OOSQL_UDTObject_SetNTHAttrType(
    OOSQL_UDTObject*    UDTObject,
    Four                nth,
    Four                attrType);

Four OOSQL_UDTObject_GetNTHAttrLength(
    OOSQL_UDTObject*    UDTObject,
    Four                nth);

void OOSQL_UDTObject_SetNTHAttrLength(
    OOSQL_UDTObject*    UDTObject,
    Four                nth,
    Four                length);

char* OOSQL_UDTObject_GetNTHAttrData(
    OOSQL_UDTObject*    UDTObject,
    Four                nth);

Four OOSQL_UDTObject_SetNTHAttrData(
    OOSQL_UDTObject*    UDTObject,
    Four                nth,
    Four                length,
    void*               data);

#ifdef __cplusplus
}
#endif

#endif /* _OOSQL_APIS_INTERNAL_H_ */
