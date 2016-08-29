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

#ifndef DBM_H
#define DBM_H

#include <stdio.h>
#ifndef WIN32	
#include <unistd.h> 
#include <sys/param.h>
#endif
#include <stdlib.h>
#include "OOSQL_Error.h"

#define DBM_OODB_DIR           "ODYS_OODB"
#define DBM_FILENAME           "OOSQL_SysDirFile"
#define DBM_BACKUPFILENAME     "OOSQL_SysDirFile.BAK"

#define ODYS_BINDIR            "ODYSSEUS"
#define ODYS_SCHEMA_DIR        "ODYS_SCHEMA_DIR"

#define DBM_MAXDATABASENAME 60
#define DBM_MAXVOLUMENAME   60
#define DBM_MAXDEVICENAME   1024
#define DBM_MAXATTRNAME     256
#define DBM_MAXDEVICESINVOLS 500

#define DBM_DEFAULT_PAGE_NUMBER_OF_DEVICE 16000

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dbm_DevInfo
{
    char devName[DBM_MAXDEVICENAME];
    Four numOfPages;
} dbm_DevInfo;

typedef struct dbm_VolInfo
{
    char volName[DBM_MAXVOLUMENAME];
    Four volId;
    Four extSize;
    Four segmentSize;
    Four eff;
    Four numOfDevices;
    dbm_DevInfo* devices;
} dbm_VolInfo;

#ifndef MAXPATHLEN
#define MAXPATHLEN 255
#endif

typedef struct dbm_DBInfo
{
    char dbName[DBM_MAXDATABASENAME];
    char dbdir[MAXPATHLEN];
    Four dbId;
    Four numOfVolumes;
    dbm_VolInfo* volumes;
} dbm_DBInfo;

#ifndef WIN32
typedef void* HANDLE;
#endif

Four dbm_GetDBList(Four* numOfDatabases, dbm_DBInfo** dbmDBList);
Four dbm_FreeDBList(Four numOfDatabases, dbm_DBInfo* dbmDBList);
Four dbm_CheckDBList(Four numOfDatabases, dbm_DBInfo* dbmDBList);
Four dbm_ReadDBMFile(int fd, Four* numOfDatabases, dbm_DBInfo** dbmDBList, Four* nextDBId, Four* nextVolId);
Four dbm_WriteDBMFile(int fd, Four numOfDatabases, dbm_DBInfo* dbmDBList, Four nextDBId, Four nextVolId);
Four dbm_BackupDBList(int* fd1, HANDLE* handle1, int* fd2, HANDLE* handle2);
Four dbm_RestoreDBList(int fd1, HANDLE handle1, int fd2, HANDLE handle2);
Four dbm_RemoveBackup(int fd1, HANDLE handle1, int fd2, HANDLE handle2);
Four dbm_LockFile(char* dbmFileName, int* fd, HANDLE* handle);
Four dbm_UnlockFile(int fd, HANDLE handle);
Four dbm_GetAndIncreaseVolId(Four* nextVolId);
Four dbm_GetAndIncreaseDBId(Four* nextDBId);
Four dbm_InitDBMFile(int* fd, HANDLE* handle);
Four dbm_CreateDBMFile();
Four dbm_AddDatabase
(
    char*  dbName,
    Four   dbId,
    char*  dbdir,
    char*  volName,
    Four   volId,
    Four   numOfDevices,
    char   **devNameList,
    Four*  numPagesInDevice,
    Four   extSize,
    Four   segmentSize,
    Four   eff
);

Four dbm_AddVolume
(
    char* dbName,
    char* volName,
    Four  volId,
    Four  numOfDevices,
    char  **devNameList,
    Four* numPagesInDevice,
    Four  extSize,
    Four  segmentSize,
    Four  eff
);

Four dbm_AddDevice
(
    char* dbName,
    char* volName,
    Four  numOfDevices,
    char  **devNameList,
    Four* numPagesInDevice
);

Four DBM_GetVolumeList
(
    char* dbName,
    Four* numOfVolumes,
    char  **volNameList,
    Four* volIdList
);

Four DBM_GetDeviceList
(
    char* dbName,
    char* volName,
    Four* numOfDevices,
    char  **devNameList
);

Four DBM_AddDatabase
(
    char*  dbName,
    char*  dbdir,
    char*  volName,
    Four   numOfDevices,
    char   **devNameList,
    Four*  numPagesInDevice,
    Four   extSize,
    Four   segmentSize,
    Four   eff
);

Four DBM_AddVolume
(
    char* dbName,
    char* volName,
    Four  numOfDevices,
    char  **devNameList,
    Four* numPagesInDevice,
    Four  extSize,
    Four  segmentSize,
    Four  eff
);

Four DBM_AddDevice
(
    char* dbName,
    char* volName,
    Four  numOfDevices,
    char  **devNameList,
    Four* numPagesInDevice
);

Four DBM_CheckExistence(char* dbName, Boolean* b);
Four DBM_GetVolumeID(char* dbName, char* volName, Four* volId);
Four DBM_GetDefaultVolumeID(char* dbName, Four* volId);
Four DBM_GetDatabaseID(char* dbName, Four* dbId);
Four DBM_RemoveDatabase(char* dbName);
Four DBM_RemoveVolume(char* dbName, char* volName);
Four DBM_FormatDB(char* dbName, Boolean isTemporaryVolume);
Four DBM_FormatVolume(char* dbName, char* volName, Boolean isTemporaryVolume);
Four DBM_InitSchemaFile(char* dbName, Four volId);
Four DBM_DropVolumeFromSchemaFile(char* dbName, Four volId);
Four DBM_GetDBDirectory(char* dbName, char *dbdir);

#ifdef __cplusplus
extern "C" {
#endif
Four run(char* command);
#ifdef __cplusplus
}
#endif

#ifndef BEGIN_MACRO
#define BEGIN_MACRO do {
#endif

#ifndef END_MACRO
#define END_MACRO } while(0)
#endif

#define DBM_ERROR(e) \
OOSQL_ERR(e);

#define DBM_DO(s) \
BEGIN_MACRO \
Four e; \
e = (s); \
if (e < 0) \
{ \
	OOSQL_ERR(e); \
} \
END_MACRO

#define DBM_DO2(s,l, h) \
BEGIN_MACRO \
Four e; \
e = (s); \
if (e < 0) \
{ \
    dbm_UnlockFile(l, h); \
	OOSQL_ERR(e); \
} \
END_MACRO

#define DBM_DO3(s,l1,h1, l2, h2) \
BEGIN_MACRO \
Four e; \
e = (s); \
if (e < 0) \
{ \
    dbm_UnlockFile(l1, h1); \
    dbm_UnlockFile(l2, h2); \
	OOSQL_ERR(e); \
} \
END_MACRO

char* DBM_Err(Four errCode);
extern Four procIndex;

#ifdef __cplusplus
}
#endif

#endif /* DBM_H */
