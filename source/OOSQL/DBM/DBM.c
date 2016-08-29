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

#include "OOSQL_StorageSystemHeaders.h"
#include "DBM.h"
#include "OOSQL_APIs.h"
#include <string.h>

#ifdef WIN32
#include <io.h>
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#define F_OK 00
#else
#define DIRECTORY_SEPARATOR "/"
#endif

Four DBM_GetDBDirectory
(
    char* dbName,
    char* dbdir
)
{
    Four numOfDatabases;
    dbm_DBInfo* dbList;
    Four i, j;

    DBM_DO(dbm_GetDBList(&numOfDatabases, &dbList));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);

    strcpy(dbdir, dbList[i].dbdir);

    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

    return eNOERROR;
}

Four DBM_GetDatabaseID
(
    char* dbName,
    Four* dbId
)
{
    Four numOfDatabases;
    dbm_DBInfo* dbList;
    Four i, j;

    DBM_DO(dbm_GetDBList(&numOfDatabases, &dbList));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);

    *dbId = dbList[i].dbId;

    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

    return eNOERROR;
}

Four DBM_GetVolumeID
(
    char* dbName,
    char* volName,
    Four* volId
)
{
    Four numOfDatabases;
    dbm_DBInfo* dbList;
    Four i, j;

    DBM_DO(dbm_GetDBList(&numOfDatabases, &dbList));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);

    for (j=0; j<dbList[i].numOfVolumes; j++)
        if (strcmp(volName, dbList[i].volumes[j].volName) == 0)
            break;

    if (j == dbList[i].numOfVolumes) DBM_ERROR(eNOSUCHVOLUME_DBM);

    *volId = dbList[i].volumes[j].volId;

    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

    return eNOERROR;
}

Four DBM_GetDefaultVolumeID
(
    char* dbName,
    Four* volId
)
{
    Four numOfDatabases;
    dbm_DBInfo* dbList;
    Four i, j;

    DBM_DO(dbm_GetDBList(&numOfDatabases, &dbList));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);

    *volId = dbList[i].volumes[0].volId;

    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

    return eNOERROR;
}

Four DBM_GetVolumeList
(
    char* dbName,       /* IN  database name */
    Four* numOfVolumes, /* OUT number of volumes in databases */
    char  **volNameList,  /* OUT volume name list */
    Four* volIdList     /* OUT volume id list */
)
{
    Four numOfDatabases;
    dbm_DBInfo* dbList;
    Four i, j;

    DBM_DO(dbm_GetDBList(&numOfDatabases, &dbList));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);


    if (*numOfVolumes < dbList[i].numOfVolumes)
    {
        *numOfVolumes = dbList[i].numOfVolumes;
        DBM_ERROR(eNEEDMOREBUFFER_DBM);
    }


    *numOfVolumes = dbList[i].numOfVolumes;

    for (j=0; j<dbList[i].numOfVolumes; j++)
    {
        if (strlen(dbList[i].volumes[j].volName)+1 > DBM_MAXVOLUMENAME)
            DBM_ERROR(eSMALLBUFFER_DBM);

        strcpy(volNameList[j], dbList[i].volumes[j].volName);
        volIdList[j] = dbList[i].volumes[j].volId;
    }

    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

    return eNOERROR;
}

Four DBM_GetDeviceList
(
    char*  dbName,      /* IN  databases name */
    char*  volName,     /* IN  volume name */
    Four*  numOfDevices,/* OUT number of devices of volume */
    char   **devNameList/* OUT device name list */
)
{
    Four numOfDatabases;
    dbm_DBInfo* dbList;
    Four i, j, k;

    DBM_DO(dbm_GetDBList(&numOfDatabases, &dbList));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);

    for (j=0; j<dbList[i].numOfVolumes; j++)
        if (strcmp(volName, dbList[i].volumes[j].volName) == 0)
            break;

    if (j == dbList[i].numOfVolumes) DBM_ERROR(eNOSUCHVOLUME_DBM);

    if (*numOfDevices < dbList[i].volumes[j].numOfDevices)
    {
        *numOfDevices = dbList[i].volumes[j].numOfDevices;
        DBM_ERROR(eNEEDMOREBUFFER_DBM);
    }

    *numOfDevices = dbList[i].volumes[j].numOfDevices;

    for (k=0; k<dbList[i].volumes[j].numOfDevices; k++)
    {
        if (strlen(dbList[i].volumes[j].devices[k].devName)+1 > DBM_MAXDEVICENAME)
            DBM_ERROR(eSMALLBUFFER_DBM);

        strcpy(devNameList[k], dbList[i].volumes[j].devices[k].devName);
    }

    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

    return eNOERROR;
}

Four DBM_AddDatabase
(
    char* dbName,           /* IN  database name */
    char* dbdir,            /* IN  database directory */
    char* volName,          /* IN  new volume name */
    Four  numOfDevices,     /* IN  number of devices of the new volume */
    char  **devNameList,    /* IN  device list */
    Four* numPagesInDevice, /* IN  number of pages of each device */
    Four  extSize,          /* IN  extent size of volume in page number */
    Four  segmentSize,      /* IN  segment size of volume in page number */
    Four  eff               /* IN  extent fill factor in percent */
)
{
    Four         volId;
    Four         DBId;

    if (dbName == NULL)   DBM_ERROR(eBADPARAMETER_DBM);
    if (volName == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (numOfDevices < 1) DBM_ERROR(eBADPARAMETER_DBM);
    if (devNameList == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (numPagesInDevice == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (extSize < 1)      DBM_ERROR(eBADPARAMETER_DBM);
    if (segmentSize < 1)  DBM_ERROR(eBADPARAMETER_DBM);
    if (eff < 1)          DBM_ERROR(eBADPARAMETER_DBM);

    DBM_DO(dbm_GetAndIncreaseDBId(&DBId));
    DBM_DO(dbm_GetAndIncreaseVolId(&volId));
    DBM_DO(dbm_AddDatabase(dbName, DBId, dbdir, volName, volId, numOfDevices, devNameList, numPagesInDevice, extSize, segmentSize, eff));

    return eNOERROR;
}

Four DBM_AddVolume
(
    char* dbName,           /* IN  database name */
    char* volName,          /* IN  new volume name */
    Four  numOfDevices,     /* IN  number of devices of the new volume */
    char  **devNameList,    /* IN  device list */
    Four* numPagesInDevice, /* IN  number of pages of each device */
    Four  extSize,          /* IN  extent size of volume in page number */
    Four  segmentSize,      /* IN  segment size of volume in page number */
    Four  eff               /* IN  extent fill factor in percent */
)
{
    Four         volId;

    if (dbName == NULL)   DBM_ERROR(eBADPARAMETER_DBM);
    if (volName == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (numOfDevices < 1) DBM_ERROR(eBADPARAMETER_DBM);
    if (devNameList == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (numPagesInDevice == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (extSize < 1)      DBM_ERROR(eBADPARAMETER_DBM);
    if (segmentSize < 1)  DBM_ERROR(eBADPARAMETER_DBM);
    if (eff < 1)          DBM_ERROR(eBADPARAMETER_DBM);

    DBM_DO(dbm_GetAndIncreaseVolId(&volId));
    DBM_DO(dbm_AddVolume(dbName, volName, volId, numOfDevices, devNameList, numPagesInDevice, extSize, segmentSize, eff));

    return eNOERROR;
}

Four DBM_AddDevice
(
    char* dbName,       /* IN  database name */
    char* volName,      /* IN  new volume name */
    Four  numOfDevices, /* IN  number of devices to add */
    char  **devNameList,/* IN  device name */
    Four* numPagesInDevice
)
{
    LOM_Handle handle;
    char oldDevNames[DBM_MAXDEVICESINVOLS][DBM_MAXDEVICENAME];
    char *oldDevNameList[DBM_MAXDEVICESINVOLS];
    Four oldNumOfDevices = DBM_MAXDEVICESINVOLS;
    Four volId;
    Four i;
    Four e;

    if (dbName == NULL)   DBM_ERROR(eBADPARAMETER_DBM);
    if (volName == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (devNameList == NULL)  DBM_ERROR(eBADPARAMETER_DBM);
    if (numPagesInDevice == NULL)  DBM_ERROR(eBADPARAMETER_DBM);

    for (i=0; i<DBM_MAXDEVICESINVOLS; i++)
        oldDevNameList[i] = oldDevNames[i];

    DBM_DO(DBM_GetDeviceList(dbName, volName, &oldNumOfDevices, oldDevNameList));

    DBM_DO(LRDS_Init());

    e = LRDS_AllocHandle(&(LOM_GET_LRDS_HANDLE(&handle)));
    if (e < 0)
    {
        LRDS_Final();
        DBM_ERROR(e);
    }

    e = LRDS_Mount(LOM_GET_LRDS_HANDLE(&handle), oldNumOfDevices, oldDevNameList, &volId);
    if (e < 0)
    {
        LRDS_Final();
        DBM_ERROR(e);
    }

    e = LRDS_ExpandDataVolume(LOM_GET_LRDS_HANDLE(&handle), volId, numOfDevices, devNameList, numPagesInDevice);
    if (e < 0)
    {
        LRDS_Final();
        DBM_ERROR(e);
    }

    e = LRDS_Dismount(LOM_GET_LRDS_HANDLE(&handle), volId);
    if (e < 0)
    {
        LRDS_Final();
        DBM_ERROR(e);
    }

    e = LRDS_FreeHandle(LOM_GET_LRDS_HANDLE(&handle));
    if (e < 0)
    {
        LRDS_Final();
        DBM_ERROR(e);
    }

    DBM_DO(LRDS_Final());

    DBM_DO(dbm_AddDevice(dbName, volName, numOfDevices, devNameList, numPagesInDevice));

    return eNOERROR;
}

Four DBM_RemoveDatabase
(
    char*        dbName       /* IN  database name */
)
{
    Four numOfDatabases;
    dbm_DBInfo*  dbList;
    Four nextDBId;
    Four nextVolId;
    Four i, j, k;
    Four e;
    int  fd1, fd2;
	HANDLE handle1;
	HANDLE handle2;

    DBM_DO(dbm_BackupDBList(&fd1, &handle1, &fd2, &handle2));
    e = dbm_ReadDBMFile(fd1, &numOfDatabases, &dbList, &nextDBId, &nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbList[i].dbName, dbName) == 0)
            break;

    if (i == numOfDatabases)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eNOSUCHDB_DBM);
    }

	printf("Database '%s' is removed.\n", dbName);
    for (j=0; j<dbList[i].numOfVolumes; j++)
	{
		for(k = 0; k < dbList[i].volumes[j].numOfDevices; k++)
			printf("Device '%s' must be removed by manually\n", dbList[i].volumes[j].devices[k].devName);
        free(dbList[i].volumes[j].devices);
	}

    for (; i<numOfDatabases-1; i++)
    {
        strcpy(dbList[i].dbName, dbList[i+1].dbName);
        dbList[i].numOfVolumes = dbList[i+1].numOfVolumes;
        dbList[i].volumes = dbList[i+1].volumes;
    }

    numOfDatabases--;

    /* Initialize DBM file */
    e = dbm_InitDBMFile(&fd1, &handle1);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    /* write DB list to file */
    e = dbm_WriteDBMFile(fd1, numOfDatabases, dbList, nextDBId, nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    /* free DB list */
    e = dbm_FreeDBList(numOfDatabases, dbList);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    /* remove backup file */
    DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));

    return eNOERROR;
}

Four DBM_RemoveVolume
(
    char*        dbName,       /* IN  database name */
    char*        volName       /* IN  volume name */
)
{
    Four numOfDatabases;
    dbm_DBInfo*  dbList;
    Four nextVolId;
    Four nextDBId;
    Four i, j, k;
    Four e;
    int  fd1, fd2;
	HANDLE handle1;
	HANDLE handle2;

    DBM_DO(dbm_BackupDBList(&fd1, &handle1, &fd2, &handle2));
    e = dbm_ReadDBMFile(fd1, &numOfDatabases, &dbList, &nextDBId, &nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eNOSUCHDB_DBM);
    }

    for (j=0; j<dbList[i].numOfVolumes; j++)
        if (strcmp(volName, dbList[i].volumes[j].volName) == 0)
            break;

    if (j == dbList[i].numOfVolumes)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eNOSUCHVOLUME_DBM);
    }

	printf("Volume '%s' is removed.\n", volName);
	for(k = 0; k < dbList[i].volumes[j].numOfDevices; k++)
		printf("Device '%s' must be removed by manually\n", dbList[i].volumes[j].devices[k].devName);
    free(dbList[i].volumes[j].devices);

    for (; j<dbList[i].numOfVolumes-1; j++)
    {
        strcpy(dbList[i].volumes[j].volName, dbList[i].volumes[j+1].volName);
        dbList[i].volumes[j].volId = dbList[i].volumes[j+1].volId;
        dbList[i].volumes[j].numOfDevices = dbList[i].volumes[j+1].numOfDevices;
        dbList[i].volumes[j].devices = dbList[i].volumes[j+1].devices;
    }

    dbList[i].numOfVolumes--;

    /* Initialize DBM file */
    e = dbm_InitDBMFile(&fd1, &handle1);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    /* write DB list to file */
    e = dbm_WriteDBMFile(fd1, numOfDatabases, dbList, nextDBId, nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RestoreDBList(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    /* free DB list */
    e = dbm_FreeDBList(numOfDatabases, dbList);
    if (e < 0)
    {
        DBM_DO(dbm_RestoreDBList(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    /* remove backup file */
    DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));

    return eNOERROR;
}

Four DBM_CheckExistence
(
    char*    dbName,   /* IN  database name */
    Boolean* b         /* OUT existence flag */
)
{
    Four numOfDatabases;
    dbm_DBInfo*  dbList;
    Four nextVolId;
    Four nextDBId;
    Four i, j, k;
    Four e;
    int  fd;
	HANDLE handle;
    char dbmFileName[MAXPATHLEN];
    char *dbDir;

    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
    sprintf(dbmFileName, "%s%s%s", dbDir, DIRECTORY_SEPARATOR, DBM_FILENAME);
    DBM_DO(dbm_LockFile(dbmFileName, &fd, &handle));

    DBM_DO2(dbm_ReadDBMFile(fd, &numOfDatabases, &dbList, &nextDBId, &nextVolId), fd, handle);

    *b = SM_FALSE;
    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbName, dbList[i].dbName) == 0)
        {
            *b = SM_TRUE;
            break;
        }

    /* free DB list */
    DBM_DO2(dbm_FreeDBList(numOfDatabases, dbList), fd, handle);

    DBM_DO(dbm_UnlockFile(fd, handle));

    return eNOERROR;
}

Four DBM_FormatVolume
(
    char*	dbName,
    char*	volName,
	Boolean isTemporaryVolume
)
{
    Four e;
    char command[1024];
    dbm_DBInfo* dbList;
    Four numOfDatabases;
    Four nextDBId, nextVolId;
    Four i, j, k;
    Four numOfDevices;
    char **devNameList;
    Four *numPagesInDevice;
    Four volId;
    Four extentSize;
    Four segmentSize;
    Four eff;
    char dbmFileName[MAXPATHLEN];
    int  fd;
	HANDLE handle;
    Four   numOfExtent;
    char *dbDir;
    OOSQL_SystemHandle oosqlHandle;

    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
    sprintf(dbmFileName, "%s%s%s", dbDir, DIRECTORY_SEPARATOR, DBM_FILENAME);
    DBM_DO(dbm_LockFile(dbmFileName, &fd, &handle));
    DBM_DO2(dbm_ReadDBMFile(fd, &numOfDatabases, &dbList, &nextDBId, &nextVolId), fd, handle);
    DBM_DO(dbm_UnlockFile(fd, handle));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbList[i].dbName, dbName) == 0)
            break;
    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);

    for (j=0; j<dbList[i].numOfVolumes; j++)
        if (strcmp(dbList[i].volumes[j].volName, volName) == 0)
            break;
    if (j == dbList[i].numOfVolumes) DBM_ERROR(eNOSUCHVOLUME_DBM);

    numOfDevices = dbList[i].volumes[j].numOfDevices;

    devNameList = (char**)malloc(numOfDevices*sizeof(char*));
    if (devNameList == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

    for (k=0; k<numOfDevices; k++)
    {
        devNameList[k] = (char*)malloc(DBM_MAXDEVICENAME);
        if (devNameList[k] == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);
    }

    numPagesInDevice = (Four*)malloc(sizeof(Four)*numOfDevices);
    if (numPagesInDevice == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

    for (k=0; k<numOfDevices; k++)
    {
        strcpy(devNameList[k], dbList[i].volumes[j].devices[k].devName);
        numPagesInDevice[k] = dbList[i].volumes[j].devices[k].numOfPages;
    }

    volId = dbList[i].volumes[j].volId;
    extentSize = dbList[i].volumes[j].extSize;
    segmentSize = dbList[i].volumes[j].segmentSize;
    eff = dbList[i].volumes[j].eff;

    /* OOSQL Format */
    e = OOSQL_CreateSystemHandle(&oosqlHandle, &procIndex);
    if (e < 0) DBM_ERROR(e);

	if(isTemporaryVolume)
		e = OOSQL_FormatTempDataVolume(&oosqlHandle, numOfDevices, devNameList, volName, volId, extentSize, numPagesInDevice, segmentSize);
	else
		e = OOSQL_FormatDataVolume(&oosqlHandle, numOfDevices, devNameList, volName, volId, extentSize, numPagesInDevice, segmentSize);
    if (e < 0)
    {
        OOSQL_DestroySystemHandle(&oosqlHandle, procIndex);
        DBM_ERROR(e);
    }

    e = OOSQL_DestroySystemHandle(&oosqlHandle, procIndex);
    if (e < 0) DBM_ERROR(e);

    /* free DB list */
    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

    for (k=0; k<numOfDevices; k++)
        free(devNameList[k]);
    free(devNameList);

    return eNOERROR;
}

Four DBM_FormatDB
(
    char*	dbName,
	Boolean isTemporaryVolume
)
{
    Four e;
    char command[1024];
    dbm_DBInfo* dbList;
    Four numOfDatabases;
    Four nextDBId, nextVolId;
    Four i, j;
    int  fd;
	HANDLE handle;
    char dbmFileName[MAXPATHLEN];
    char *dbDir;

    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
    sprintf(dbmFileName, "%s%s%s", dbDir, DIRECTORY_SEPARATOR, DBM_FILENAME);
    DBM_DO(dbm_LockFile(dbmFileName, &fd, &handle));
    DBM_DO2(dbm_ReadDBMFile(fd, &numOfDatabases, &dbList, &nextDBId, &nextVolId), fd, handle);
    DBM_DO(dbm_UnlockFile(fd, handle));

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbList[i].dbName, dbName) == 0)
            break;
    if (i == numOfDatabases) DBM_ERROR(eNOSUCHDB_DBM);

    for (j=0; j<dbList[i].numOfVolumes; j++)
        DBM_DO2(DBM_FormatVolume(dbName, dbList[i].volumes[j].volName, isTemporaryVolume), fd, handle);

    /* free DB list */
    DBM_DO(dbm_FreeDBList(numOfDatabases, dbList));

	return eNOERROR;
}

Four DBM_InitSchemaFile
(
    char *dbName,
    Four defaultVolId
)
{
    char defaultSchemaFile[1024];
    char newSchemaFile[1024];
    char line[1024], *p;
    FILE *fp1, *fp2;
    char *binDir;
    char *schemaDir;
    Four numClasses;
    char str[256], className[256];
    Four oldVolId, classId;

    binDir = getenv(ODYS_BINDIR);
    if (binDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
    schemaDir = getenv(ODYS_SCHEMA_DIR);
    if (binDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
    sprintf(defaultSchemaFile, "%s%sbin%sODYS_Primitive.sch", DIRECTORY_SEPARATOR, DIRECTORY_SEPARATOR, binDir);
    sprintf(newSchemaFile, "%s%s%s.sch", schemaDir, DIRECTORY_SEPARATOR, dbName);

    fp1 = fopen(defaultSchemaFile, "r");
    if (fp1 == NULL) DBM_ERROR(eCANNOTOPENFILE_DBM);

    fp2 = fopen(newSchemaFile, "w");
    if (fp2 == NULL) DBM_ERROR(eCANNOTOPENFILE_DBM);

    /* read numClasses */
    fgets(line, 1024, fp1);
    fputs(line, fp2);

    p = fgets(line, 1024, fp1);
    while (!feof(fp1) && p != NULL)
    {
        if (strncmp("class:", line, 6) == 0)
        {
            sscanf(line, "%s %s %ld %ld", str, className, &oldVolId, &classId);
            fprintf(fp2, "class: %s %ld %ld\n", className, defaultVolId, classId);
        }
        else
            fprintf(fp2, line);

        p = fgets(line, 1024, fp1);
    }

    return eNOERROR;
}

Four DBM_DropVolumeFromSchemaFile
(
    char *dbName,
    Four volId
)
{
    char schemaFile[1024];
    char newSchemaFile[1024];
    char line[1024], *p;
    FILE *fp1, *fp2;
    char *schemaDir;
    Four numClasses;
    char str[256], className[256];
    Four oldVolId, classId;
    Boolean print = SM_TRUE;

    schemaDir = getenv(ODYS_SCHEMA_DIR);
    if (schemaDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
    sprintf(schemaFile, "%s%s%s.sch", schemaDir, DIRECTORY_SEPARATOR, dbName);
    sprintf(newSchemaFile, "%s%s%s.sch.new", schemaDir, DIRECTORY_SEPARATOR, dbName);

    fp1 = fopen(schemaFile, "r");
    if (fp1 == NULL) DBM_ERROR(eCANNOTOPENFILE_DBM);

    fp2 = fopen(newSchemaFile, "w");
    if (fp2 == NULL) DBM_ERROR(eCANNOTOPENFILE_DBM);

    /* skip numClasses */
    fgets(line, 1024, fp1);

    numClasses = 0;
    p = fgets(line, 1024, fp1);
    while (!feof(fp1) && p != NULL)
    {
        if (strncmp("class:", line, 6) == 0)
        {
            sscanf(line, "%s %s %ld %ld", str, className, &oldVolId, &classId);
            if (oldVolId != volId)
               numClasses++;
        }

        p = fgets(line, 1024, fp1);
    }

    rewind(fp1);

    /* skip numClasses */
    fgets(line, 1024, fp1);
    fprintf(fp2, "NumClasses %ld\n", numClasses);

    p = fgets(line, 1024, fp1);
    while (!feof(fp1) && p != NULL)
    {
        if (strncmp("class:", line, 6) == 0)
        {
            sscanf(line, "%s %s %ld %ld", str, className, &oldVolId, &classId);
            if (oldVolId == volId) print = SM_FALSE;
            else                   print = SM_TRUE;
        }

        if (print)
            fprintf(fp2, line);

        p = fgets(line, 1024, fp1);
    }

    fclose(fp1);
    fclose(fp2);

    if (unlink(schemaFile) < 0)
        DBM_ERROR(eCANNOTREMOVEFILE_DBM);

    if (rename(newSchemaFile, schemaFile) < 0)
        DBM_ERROR(eCANNOTRENAMEFILE_DBM);

    return eNOERROR;
}
