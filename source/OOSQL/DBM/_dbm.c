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

#include "OOSQL_Param.h"
#include "cosmos_r.h"
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "DBM.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>	
#include <fcntl.h>
#include <stdlib.h>

#ifndef WIN32			
#include <unistd.h> 
#else
#include <io.h>
#define F_OK   00
#define W_OK   02
#define R_OK   04
#define RW_OK  06
#endif

#include <string.h>

Four dbm_FreeDBList
(
    Four        numOfDatabases,  /* IN  number of databases  */
    dbm_DBInfo* dbmDBList        /* OUT database information */
)
{
    Four i, j, k;

    /* check parameter */
    if (numOfDatabases < 0) DBM_ERROR(eBADPARAMETER_DBM);
    if (dbmDBList == NULL)  DBM_ERROR(eBADPARAMETER_DBM);

    for (i=0; i<numOfDatabases; i++)
    {
        for (j=0; j<dbmDBList[i].numOfVolumes; j++)
            free(dbmDBList[i].volumes[j].devices);
        free(dbmDBList[i].volumes);
    }
    free(dbmDBList);

    return eNOERROR;
}

#define LOCK_TIMEOUT_RETRY_WAIT		0.01	/* 10 ms waiting */
#define LOCK_TIMEOUT_RETRY_NUMBER	60000	/* 60000 retry -> timeout in sec is 600 sec */

Four dbm_LockFile
(
    char*   fileName,
    int*    fd,
	HANDLE* handle
)
{
	Four			i;
	char			lockFileName[MAXPATHLEN + 10];
#ifndef WIN32			
    struct flock	lock;
#endif

    /* check parameter */
    if (fileName == NULL) DBM_ERROR(eBADPARAMETER_DBM);
    if (fd == NULL)       DBM_ERROR(eBADPARAMETER_DBM);

    *fd = open(fileName, O_RDWR);
    if (*fd < 0) DBM_ERROR(eCANNOTOPENFILE_DBM);

#ifndef WIN32			
    lock.l_type   = F_WRLCK;
    lock.l_start  = 0;
    lock.l_whence = SEEK_SET;
    lock.l_len    = 0;

	for(i = 0; i < LOCK_TIMEOUT_RETRY_NUMBER; i++)
	{
		if (fcntl(*fd, F_SETLK, &lock) < 0) 
			Util_Sleep(LOCK_TIMEOUT_RETRY_WAIT);
		else
			break;
	}	
	if(i == LOCK_TIMEOUT_RETRY_NUMBER)
		DBM_ERROR(eCANNOTLOCKFILE_DBM);
#else
	sprintf(lockFileName, "%s.lock", fileName);
	*handle = CreateFile(lockFileName, GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
                         OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
	if(*handle == INVALID_HANDLE_VALUE) 
		DBM_ERROR(eCANNOTOPENFILE_DBM);

	for(i = 0; i < LOCK_TIMEOUT_RETRY_NUMBER; i++)
	{
		if(LockFile(*handle, 0, 0, 1, 0) == 0)
			Util_Sleep(LOCK_TIMEOUT_RETRY_WAIT);
		else
			break;
	}	
	if(i == LOCK_TIMEOUT_RETRY_NUMBER)
		DBM_ERROR(eCANNOTLOCKFILE_DBM);
#endif

    return eNOERROR;
}

Four dbm_UnlockFile
(
    int		fd,
	HANDLE	handle
)
{
#ifndef WIN32			
    struct flock lock;
#endif

    /* check parameter */
    if (fd < 0) DBM_ERROR(eBADPARAMETER_DBM);

#ifndef WIN32			
    lock.l_type   = F_UNLCK;
    lock.l_start  = 0;
    lock.l_whence = SEEK_SET;
    lock.l_len    = 0;

    if (fcntl(fd, F_SETLK, &lock) < 0) 
		DBM_ERROR(eCANNOTUNLOCKFILE_DBM);
#else
	if (UnlockFile(handle, 0, 0, 1, 0) == 0) 
		DBM_ERROR(eCANNOTUNLOCKFILE_DBM);

	CloseHandle(handle);
#endif

    close(fd);

    return eNOERROR;
}

#define GETSTR(buf, size, c) \
do { \
    int     n, p; \
    char    tmp; \
    for(p = 0; p < size; p++) \
    { \
        n = read(fd, &tmp, 1); \
        if (n == 1 && tmp != c) \
            buf[p] = tmp; \
        else \
            break; \
    } \
    buf[p] = 0; \
} while(0);


Four dbm_ReadDBMFile
(
    int          fd,              /* IN  file descriptor of a file containing database directory information */
    Four*        numOfDatabases,  /* OUT number of databases  */
    dbm_DBInfo** dbmDBList,       /* OUT database information */
    Four*        nextDBId,        /* OUT next DB id */
    Four*        nextVolId        /* OUT next volume id */
)
{
    Four e;
    FILE *fp;
    Four i, j, k;
    char buf[1024];

    /* check parameters */
    if (fd < 0)                 DBM_ERROR(eBADPARAMETER_DBM);
    if (numOfDatabases == NULL) DBM_ERROR(eBADPARAMETER_DBM);
    if (dbmDBList == NULL)      DBM_ERROR(eBADPARAMETER_DBM);

	fd = dup(fd);				

    lseek(fd, 0, SEEK_SET);

    GETSTR(buf, 1024, '\n');
    e = sscanf(buf, "%ld", nextDBId);
    if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

    GETSTR(buf, 1024, '\n');
    e = sscanf(buf, "%ld", nextVolId);
    if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

    GETSTR(buf, 1024, '\n');
    e = sscanf(buf, "%ld", numOfDatabases);
    if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

    *dbmDBList = (dbm_DBInfo*) malloc(sizeof(dbm_DBInfo)*(*numOfDatabases) + 1);
    if (*dbmDBList == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

    for (i=0; i<*numOfDatabases; i++)
    {
        GETSTR(buf, 1024, '\t');
        e = sscanf(buf, "%s", (*dbmDBList)[i].dbName);
        if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

        GETSTR(buf, 1024, '\n');
        e = sscanf(buf, "%ld", &(*dbmDBList)[i].dbId);
        if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

        GETSTR(buf, 1024, '\t');
        GETSTR(buf, 1024, '\n');
        e = sscanf(buf, "%s", (*dbmDBList)[i].dbdir);
        if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

        GETSTR(buf, 1024, '\t');
        GETSTR(buf, 1024, '\n');
        e = sscanf(buf, "%ld", &(*dbmDBList)[i].numOfVolumes);
        if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

        (*dbmDBList)[i].volumes = (dbm_VolInfo*) malloc(sizeof(dbm_VolInfo)*((*dbmDBList)[i].numOfVolumes + 1));
        if ((*dbmDBList)[i].volumes == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

        for (j=0; j<(*dbmDBList)[i].numOfVolumes; j++)
        {
            GETSTR(buf, 1024, '\t');
            GETSTR(buf, 1024, '\n');
            e = sscanf(buf, "%s", (*dbmDBList)[i].volumes[j].volName);
            if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

            GETSTR(buf, 1024, '\t');
            GETSTR(buf, 1024, '\t');
            e = sscanf(buf, "%ld", &(*dbmDBList)[i].volumes[j].volId);
            if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

            GETSTR(buf, 1024, '\t');
            e = sscanf(buf, "%ld", &(*dbmDBList)[i].volumes[j].extSize);
            if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

            GETSTR(buf, 1024, '\t');
            e = sscanf(buf, "%ld", &(*dbmDBList)[i].volumes[j].segmentSize);
            if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

            GETSTR(buf, 1024, '\t');
            e = sscanf(buf, "%ld", &(*dbmDBList)[i].volumes[j].eff);
            if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

            GETSTR(buf, 1024, '\n');
            e = sscanf(buf, "%ld", &(*dbmDBList)[i].volumes[j].numOfDevices);
            if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);


            (*dbmDBList)[i].volumes[j].devices = (dbm_DevInfo*)
                    malloc(sizeof(dbm_DevInfo)*((*dbmDBList)[i].volumes[j].numOfDevices + 1));
            if ((*dbmDBList)[i].volumes[j].devices == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

            for (k=0; k<(*dbmDBList)[i].volumes[j].numOfDevices; k++)
            {
                GETSTR(buf, 1024, '\t');
                GETSTR(buf, 1024, '\t');
                GETSTR(buf, 1024, '\t');
                e = sscanf(buf, "%s", (*dbmDBList)[i].volumes[j].devices[k].devName);
                if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);

                GETSTR(buf, 1024, '\n');
                e = sscanf(buf, "%ld", &(*dbmDBList)[i].volumes[j].devices[k].numOfPages);
                if (e != 1) DBM_ERROR(eBADFILEFORMAT_DBM);
            }
        }
    }

	close(fd);		

    return eNOERROR;
}

Four dbm_ReadDBMFile_old
(
    int          fd,              /* IN  file descriptor of a file containing database directory information */
    Four*        numOfDatabases,  /* OUT number of databases  */
    dbm_DBInfo** dbmDBList,       /* OUT database information */
    Four*        nextDBId,        /* OUT next DB id */
    Four*        nextVolId        /* OUT next volume id */
)
{
    Four e;
    FILE *fp;
    Four i, j, k;

    /* check parameters */
    if (fd < 0)                 DBM_ERROR(eBADPARAMETER_DBM);
    if (numOfDatabases == NULL) DBM_ERROR(eBADPARAMETER_DBM);
    if (dbmDBList == NULL)      DBM_ERROR(eBADPARAMETER_DBM);

	fd = dup(fd);				
    fp = fdopen(fd, "r+");
    if (fp == NULL) DBM_ERROR(eCANNOTOPENFILE_DBM);

    fseek(fp, SEEK_SET, 0);

    e = fscanf(fp, "%ld %ld %ld", nextDBId, nextVolId, numOfDatabases);
    if (e != 3) DBM_ERROR(eBADFILEFORMAT_DBM);

    *dbmDBList = (dbm_DBInfo*) malloc(sizeof(dbm_DBInfo)*(*numOfDatabases) + 1);
    if (*dbmDBList == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

    for (i=0; i<*numOfDatabases; i++)
    {
        e = fscanf(fp, "%s %ld %s %ld", (*dbmDBList)[i].dbName, &(*dbmDBList)[i].dbId, (*dbmDBList)[i].dbdir, &(*dbmDBList)[i].numOfVolumes);
        if (e != 4) DBM_ERROR(eBADFILEFORMAT_DBM);

        (*dbmDBList)[i].volumes = (dbm_VolInfo*) malloc(sizeof(dbm_VolInfo)*((*dbmDBList)[i].numOfVolumes + 1));
        if ((*dbmDBList)[i].volumes == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

        for (j=0; j<(*dbmDBList)[i].numOfVolumes; j++)
        {
            e = fscanf(fp, "%s %ld %ld %ld %ld %ld", (*dbmDBList)[i].volumes[j].volName,
                                       &(*dbmDBList)[i].volumes[j].volId,
                                       &(*dbmDBList)[i].volumes[j].extSize,
                                       &(*dbmDBList)[i].volumes[j].segmentSize,
                                       &(*dbmDBList)[i].volumes[j].eff,
                                       &(*dbmDBList)[i].volumes[j].numOfDevices);
            if (e != 6) DBM_ERROR(eBADFILEFORMAT_DBM);

            (*dbmDBList)[i].volumes[j].devices = (dbm_DevInfo*)
                    malloc(sizeof(dbm_DevInfo)*((*dbmDBList)[i].volumes[j].numOfDevices + 1));
            if ((*dbmDBList)[i].volumes[j].devices == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

            for (k=0; k<(*dbmDBList)[i].volumes[j].numOfDevices; k++)
            {
                e = fscanf(fp, "%s %ld", (*dbmDBList)[i].volumes[j].devices[k].devName,
                                       &(*dbmDBList)[i].volumes[j].devices[k].numOfPages);
                if (e != 2) DBM_ERROR(eBADFILEFORMAT_DBM);
            }
        }
    }

	fclose(fp);		

    return eNOERROR;
}

Four dbm_WriteDBMFile
(
    int         fd,              /* IN file descriptor of a file containing database directory information */
    Four        numOfDatabases,  /* IN number of databases  */
    dbm_DBInfo* dbmDBList,       /* IN database information */
    Four        nextDBId,        /* IN next DB id */
    Four        nextVolId        /* IN next volume id */
)
{
    Four e;
    FILE *fp;
    Four i, j, k;

    /* check parameters */
    if (fd < 0)             DBM_ERROR(eBADPARAMETER_DBM);
    if (numOfDatabases < 0) DBM_ERROR(eBADPARAMETER_DBM);
    if (dbmDBList == NULL && numOfDatabases != 0)  DBM_ERROR(eBADPARAMETER_DBM);
    if (nextDBId < 0)		DBM_ERROR(eBADPARAMETER_DBM);
    if (nextVolId < 0)      DBM_ERROR(eBADPARAMETER_DBM);

	fd = dup(fd);			
    fp = fdopen(fd, "w");
    if (fp == NULL) DBM_ERROR(eCANNOTOPENFILE_DBM);

    fseek(fp, SEEK_SET, 0);

    e = fprintf(fp, "%ld\n%ld\n%ld\n", nextDBId, nextVolId, numOfDatabases);
    if (e < 0) DBM_ERROR(eCANNOTWRITEFILE_DBM);

    for (i=0; i<numOfDatabases; i++)
    {
        e = fprintf(fp, "%s\t%ld\n\t%s\n\t%ld\n", dbmDBList[i].dbName, dbmDBList[i].dbId, dbmDBList[i].dbdir, dbmDBList[i].numOfVolumes);
        if (e < 0) DBM_ERROR(eCANNOTWRITEFILE_DBM);

        for (j=0; j<dbmDBList[i].numOfVolumes; j++)
        {
            e = fprintf(fp, "\t%s\n\t%ld\t%ld\t%ld\t%ld\t%ld\n", dbmDBList[i].volumes[j].volName,
                                                dbmDBList[i].volumes[j].volId,
                                                dbmDBList[i].volumes[j].extSize,
                                                dbmDBList[i].volumes[j].segmentSize,
                                                dbmDBList[i].volumes[j].eff,
                                                dbmDBList[i].volumes[j].numOfDevices);
            if (e < 0) DBM_ERROR(eCANNOTWRITEFILE_DBM);

            for (k=0; k<dbmDBList[i].volumes[j].numOfDevices; k++)
            {
                e = fprintf(fp, "\t\t%s\t%ld\n", dbmDBList[i].volumes[j].devices[k].devName,
                                                dbmDBList[i].volumes[j].devices[k].numOfPages);
                if (e < 0) DBM_ERROR(eCANNOTWRITEFILE_DBM);
            }
        }
    }

    fflush(fp);
	
	fclose(fp);		

    return eNOERROR;
}

Four dbm_GetDBList
(
    Four*        numOfDatabases,  /* OUT number of databases  */
    dbm_DBInfo** dbmDBList        /* OUT database information */
)
{
    char dbmFileName[MAXPATHLEN];
    char dbmBackupFileName[MAXPATHLEN];
    int  fd1, fd2;
	HANDLE handle1;
	HANDLE handle2;
    Four nextDBId;
    Four nextVolId;
    char *dbDir;

    /* check parameter */
    if (numOfDatabases == NULL) DBM_ERROR(eBADPARAMETER_DBM);
    if (dbmDBList == NULL)      DBM_ERROR(eBADPARAMETER_DBM);

    /* make dbm file name from environment variable and default values */
    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
#ifndef WIN32			
    sprintf(dbmFileName, "%s/%s", dbDir, DBM_FILENAME);
	sprintf(dbmBackupFileName, "%s/%s", dbDir, DBM_BACKUPFILENAME);
#else
	sprintf(dbmFileName, "%s\\%s", dbDir, DBM_FILENAME);
	sprintf(dbmBackupFileName, "%s\\%s", dbDir, DBM_BACKUPFILENAME);
#endif
    

    if (access(dbmFileName, F_OK) != 0)
        DBM_DO(dbm_CreateDBMFile());
    DBM_DO(dbm_LockFile(dbmFileName, &fd1, &handle1));
#ifdef TEST
    getchar();
#endif

    /* if a backup file exists */
    if (access(dbmBackupFileName, R_OK|W_OK) == 0)
    {
        DBM_DO(dbm_LockFile(dbmBackupFileName, &fd2, &handle2));
        DBM_DO3(dbm_ReadDBMFile(fd2, numOfDatabases, dbmDBList, &nextDBId, &nextVolId), fd1, handle1, fd2, handle2);
        DBM_DO3(dbm_CheckDBList(*numOfDatabases, *dbmDBList), fd1, handle1, fd2, handle2);
        DBM_DO3(dbm_WriteDBMFile(fd1, *numOfDatabases, *dbmDBList, nextDBId, nextVolId), fd1, handle1, fd2, handle2);
        DBM_DO2(dbm_UnlockFile(fd2, handle2), fd1, handle1);

        if (unlink(dbmBackupFileName) < 0)
        {
            DBM_DO(dbm_UnlockFile(fd1, handle1));
            DBM_ERROR(eCANNOTREMOVEFILE_DBM);
        }

        DBM_DO(dbm_UnlockFile(fd1, handle1));

        return eNOERROR;
    }
    else
    {
        DBM_DO2(dbm_ReadDBMFile(fd1, numOfDatabases, dbmDBList, &nextDBId, &nextVolId), fd1, handle1);
        DBM_DO(dbm_UnlockFile(fd1, handle1));
        return eNOERROR;
    }
}

Four dbm_CheckDBList
(
    Four        numOfDatabases,  /* IN number of databases  */
    dbm_DBInfo* dbmDBList        /* IN database information */
)
{
    Four i, j, k, l;

    /* check parameter */
    if (numOfDatabases < 0) DBM_ERROR(eBADPARAMETER_DBM);
    if (dbmDBList == NULL)  DBM_ERROR(eBADPARAMETER_DBM);

    for (i=0; i<numOfDatabases; i++)
    {
        if (dbmDBList[i].numOfVolumes < 1)
            DBM_ERROR(eNOVOLUMEEXISTINDB_DBM);

        for (l=i+1; l<numOfDatabases; l++)
        {
            if (strcmp(dbmDBList[i].dbName, dbmDBList[l].dbName) == 0)
                DBM_ERROR(eDBNAMEDUPLICATED_DBM);
        }

        for (j=0; j<dbmDBList[i].numOfVolumes; j++)
        {
            if (dbmDBList[i].volumes[j].volId < 0)
                DBM_ERROR(eBADVOLUMEID_DBM);

            for (l=j+1; l<dbmDBList[i].numOfVolumes; l++)
            {
                if (strcmp(dbmDBList[i].volumes[j].volName,
                           dbmDBList[i].volumes[l].volName) == 0)
                    DBM_ERROR(eVOLUMENAMEDUPLICATED_DBM);
                if (dbmDBList[i].volumes[j].volId == dbmDBList[i].volumes[l].volId)
                    DBM_ERROR(eVOLUMEIDDUPLICATED_DBM);
            }

            for (k=0; k<dbmDBList[i].volumes[j].numOfDevices; k++)
            {
                for (l=k+1; l<dbmDBList[i].volumes[j].numOfDevices; l++)
                {
                    if (strcmp(dbmDBList[i].volumes[j].devices[k].devName,
                               dbmDBList[i].volumes[j].devices[l].devName) == 0)
                        DBM_ERROR(eDEVICENAMEDUPLICATED_DBM);
                }
            }
        }
    }

    return eNOERROR;
}

Four dbm_BackupDBList
(
    int* fd1,
	HANDLE* handle1,
    int* fd2,
	HANDLE* handle2
)
{
    char dbmFileName[MAXPATHLEN];
    char dbmBackupFileName[MAXPATHLEN];
    Four nextDBId;
    Four nextVolId;
    Four numOfDatabases;
    dbm_DBInfo* dbmDBList;
    char *dbDir;
	Four fd;

    /* check parameter */
    if (fd1 == NULL) DBM_ERROR(eBADPARAMETER_DBM);
    if (fd2 == NULL) DBM_ERROR(eBADPARAMETER_DBM);

    /* make dbm file name from environment variable and default values */
    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
#ifndef WIN32		
    sprintf(dbmFileName, "%s/%s", dbDir, DBM_FILENAME);
    sprintf(dbmBackupFileName, "%s/%s", dbDir, DBM_BACKUPFILENAME);
#else
	sprintf(dbmFileName, "%s\\%s", dbDir, DBM_FILENAME);
    sprintf(dbmBackupFileName, "%s\\%s", dbDir, DBM_BACKUPFILENAME);
#endif

    if (access(dbmFileName, F_OK) != 0)
        DBM_DO(dbm_CreateDBMFile());
    DBM_DO(dbm_LockFile(dbmFileName, fd1, handle1));

    /* if a backup file exists */
    if (access(dbmBackupFileName, F_OK) < 0)
    {
        if ((fd = creat(dbmBackupFileName, S_IREAD | S_IWRITE )) < 0)	
        {
            DBM_DO(dbm_UnlockFile(*fd1, *handle1));
            DBM_ERROR(eCANNOTCREATEFILE_DBM);
        }
		else
			close(fd);												

        DBM_DO2(dbm_LockFile(dbmBackupFileName, fd2, handle2), *fd1, *handle1);
        DBM_DO3(dbm_ReadDBMFile(*fd1, &numOfDatabases, &dbmDBList, &nextDBId, &nextVolId), *fd1, *handle1, *fd2, *handle2);
        DBM_DO3(dbm_CheckDBList(numOfDatabases, dbmDBList), *fd1, *handle1, *fd2, *handle2);
        DBM_DO3(dbm_WriteDBMFile(*fd2, numOfDatabases, dbmDBList, nextDBId, nextVolId), *fd1, *handle1, *fd2, *handle2);

        return eNOERROR;
    }
    else
    {
        DBM_DO(dbm_UnlockFile(*fd1, *handle1));
        DBM_ERROR(eBACKUPFILEEXIST_DBM);
    }
}

Four dbm_RestoreDBList
(
    int fd1,
	HANDLE handle1,
    int fd2,
	HANDLE handle2
)
{
    Four nextDBId;
    Four nextVolId;
    Four numOfDatabases;
    dbm_DBInfo* dbmDBList;

    /* check parameter */
    if (fd1 < 0) DBM_ERROR(eBADPARAMETER_DBM);
    if (fd2 < 0) DBM_ERROR(eBADPARAMETER_DBM);

    /* restore backup file */
    DBM_DO3(dbm_ReadDBMFile(fd2, &numOfDatabases, &dbmDBList, &nextDBId, &nextVolId), fd1, handle1, fd2, handle2);
    DBM_DO3(dbm_WriteDBMFile(fd1, numOfDatabases, dbmDBList, nextDBId, nextVolId), fd1, handle1, fd2, handle2);

    DBM_DO2(dbm_UnlockFile(fd2, handle2), fd1, handle1);
    DBM_DO(dbm_UnlockFile(fd1, handle1));

    return eNOERROR;
}

Four dbm_RemoveBackup
(
    int fd1,
	HANDLE handle1,
    int fd2,
	HANDLE handle2
)
{
    char dbmBackupFileName[MAXPATHLEN];
    char *dbDir;

    /* check parameter */
    if (fd1 < 0) DBM_ERROR(eBADPARAMETER_DBM);
    if (fd2 < 0) DBM_ERROR(eBADPARAMETER_DBM);

    /* make dbm file name from environment variable and default values */
    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
#ifndef WIN32			
    sprintf(dbmBackupFileName, "%s/%s", dbDir, DBM_BACKUPFILENAME);
#else
	sprintf(dbmBackupFileName, "%s\\%s", dbDir, DBM_BACKUPFILENAME);
#endif
    DBM_DO2(dbm_UnlockFile(fd2, handle2), fd1, handle1);

    if (unlink(dbmBackupFileName) < 0)
    {
        DBM_DO(dbm_UnlockFile(fd1, handle1));
        DBM_ERROR(eCANNOTREMOVEFILE_DBM);
    }

    DBM_DO(dbm_UnlockFile(fd1, handle1));

    return eNOERROR;
}

Four dbm_CreateDBMFile()
{
    char dbmFileName[MAXPATHLEN];
    int  fd;
	HANDLE handle;
    char *dbDir;

    /* make dbm file name from environment variable and default values */
    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
#ifndef WIN32			
    sprintf(dbmFileName, "%s/%s", dbDir, DBM_FILENAME);
#else
	sprintf(dbmFileName, "%s\\%s", dbDir, DBM_FILENAME);
#endif

    if (creat(dbmFileName, S_IREAD | S_IWRITE ) < 0)	
        DBM_ERROR(eCANNOTCREATEFILE_DBM);

    DBM_DO(dbm_LockFile(dbmFileName, &fd, &handle));
    DBM_DO(dbm_WriteDBMFile(fd, 0, NULL, 1000, 1000));
    DBM_DO(dbm_UnlockFile(fd, handle));

    return eNOERROR;
}

Four dbm_InitDBMFile
(
    int* fd,
	HANDLE* handle
)
{
    char dbmFileName[MAXPATHLEN];
    char *dbDir;

    /* check parameter */
    if (fd == NULL) DBM_ERROR(eBADPARAMETER_DBM);

    /* make dbm file name from environment variable and default values */
    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
#ifndef WIN32		
    sprintf(dbmFileName, "%s/%s", dbDir, DBM_FILENAME);
#else
	sprintf(dbmFileName, "%s\\%s", dbDir, DBM_FILENAME);
#endif
    DBM_DO(dbm_UnlockFile(*fd, *handle));

    if (unlink(dbmFileName) < 0)
        DBM_ERROR(eCANNOTREMOVEFILE_DBM);

    DBM_DO(dbm_CreateDBMFile());
    DBM_DO(dbm_LockFile(dbmFileName, fd, handle));

    return eNOERROR;
}

Four dbm_AddDatabase
(
    char* dbName,           /* IN  database name */
    Four  dbId,             /* IN  database id */
    char* dbdir,            /* IN  database directory */
    char* volName,          /* IN  new volume name */
    Four  volId,            /* IN  new volume id */
    Four  numOfDevices,     /* IN  number of devices of the new volume */
    char  **devNameList,    /* IN  device name list */
    Four* numPagesInDevice, /* IN  number of pages of the device */
    Four  extSize,          /* IN  extent size */
    Four  segmentSize,      /* IN  segment size */
    Four  eff               /* IN  extent fill factor */
)
{
    Four numOfDatabases;
    dbm_DBInfo*  dbList;
    dbm_VolInfo* volList;
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

    dbList = (dbm_DBInfo*) realloc(dbList, sizeof(dbm_DBInfo)*(numOfDatabases+1));
    if (dbList == NULL)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eMEMORYALLOCERROR_DBM);
    }

    numOfDatabases++;
    strcpy(dbList[numOfDatabases-1].dbName, dbName);
    dbList[numOfDatabases-1].dbId = dbId;
    strcpy(dbList[numOfDatabases-1].dbdir, dbdir);

    volList = (dbm_VolInfo*) malloc(sizeof(dbm_VolInfo));
    if (volList == NULL)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eMEMORYALLOCERROR_DBM);
    }

    /* copy new volume to new volume list */
    strcpy(volList[0].volName, volName);
    volList[0].volId = volId;
    volList[0].extSize = extSize;
    volList[0].segmentSize = segmentSize;
    volList[0].eff = eff;

    volList[0].numOfDevices = numOfDevices;
    volList[0].devices = (dbm_DevInfo*) malloc(sizeof(dbm_DevInfo)*numOfDevices + 1);
    if (volList[0].devices == NULL)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eMEMORYALLOCERROR_DBM);
    }

    for (j=0; j<numOfDevices; j++)
    {
        strcpy(volList[0].devices[j].devName, devNameList[j]);
        volList[0].devices[j].numOfPages = numPagesInDevice[j];
    }

    dbList[numOfDatabases-1].volumes = volList;
    dbList[numOfDatabases-1].numOfVolumes = 1;

    e = dbm_CheckDBList(numOfDatabases, dbList);
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

Four dbm_AddVolume
(
    char* dbName,           /* IN  database name */
    char* volName,          /* IN  new volume name */
    Four  volId,            /* IN  new volume id */
    Four  numOfDevices,     /* IN  number of devices of the new volume */
    char  **devNameList,    /* IN  device name list */
    Four* numPagesInDevice, /* IN  number of pages of the device */
    Four  extSize,          /* IN  extent size */
    Four  segmentSize,      /* IN  segment size */
    Four  eff               /* IN  extent fill factor */
)
{
    Four numOfDatabases;
    dbm_DBInfo*  dbList;
    dbm_VolInfo* volList;
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
        if (strcmp(dbName, dbList[i].dbName) == 0)
            break;

    if (i == numOfDatabases)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eNOSUCHDB_DBM);
    }

    volList = (dbm_VolInfo*) realloc(dbList[i].volumes, sizeof(dbm_VolInfo)*(dbList[i].numOfVolumes+1));
    if (volList == NULL)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eMEMORYALLOCERROR_DBM);
    }

    /* copy new volume to new volume list */
    strcpy(volList[dbList[i].numOfVolumes].volName, volName);
    volList[dbList[i].numOfVolumes].volId = volId;
    volList[dbList[i].numOfVolumes].extSize = extSize;
    volList[dbList[i].numOfVolumes].segmentSize = segmentSize;
    volList[dbList[i].numOfVolumes].eff = eff;

    volList[dbList[i].numOfVolumes].numOfDevices = numOfDevices;
    volList[dbList[i].numOfVolumes].devices = (dbm_DevInfo*) malloc(sizeof(dbm_DevInfo)*numOfDevices + 1);
    if (volList[dbList[i].numOfVolumes].devices == NULL)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eMEMORYALLOCERROR_DBM);
    }

    for (j=0; j<numOfDevices; j++)
    {
        strcpy(volList[dbList[i].numOfVolumes].devices[j].devName, devNameList[j]);
        volList[dbList[i].numOfVolumes].devices[j].numOfPages = numPagesInDevice[j];
    }

    dbList[i].volumes = volList;
    dbList[i].numOfVolumes++;

    e = dbm_CheckDBList(numOfDatabases, dbList);
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

Four dbm_AddDevice
(
    char* dbName,       /* IN  database name */
    char* volName,      /* IN  new volume name */
    Four  numOfDevices, /* IN  number of devices to add */
    char  **devNameList,/* IN  device name */
    Four* numPagesInDevice
)
{
    Four numOfDatabases;
    dbm_DBInfo*  dbList;
    dbm_DevInfo* devList;
    Four nextDBId;
    Four nextVolId;
    Four i, j, k, l;
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

    /* search DB and Volume from DB list */
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

    /* Mount volume */

    /* Format volume with new device */

    devList = (dbm_DevInfo*) realloc(dbList[i].volumes[j].devices,
                                 sizeof(dbm_DevInfo)*(dbList[i].volumes[j].numOfDevices+numOfDevices));
    if (devList == NULL)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(eMEMORYALLOCERROR_DBM);
    }

    /* copy new device to new device list */
    for (k = dbList[i].volumes[j].numOfDevices, l=0; l<numOfDevices; k++, l++)
    {
        strcpy(devList[k].devName, devNameList[l]);
        devList[k].numOfPages = numPagesInDevice[l];
    }

    dbList[i].volumes[j].devices = devList;
    dbList[i].volumes[j].numOfDevices += numOfDevices;

    e = dbm_CheckDBList(numOfDatabases, dbList);
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

Four dbm_GetAndIncreaseVolId
(
    Four* volId   /* OUT volume id */
)
{
    Four numOfDatabases;
    Four nextDBId;
    Four nextVolId;
    Four e;
    int  fd1, fd2;
    dbm_DBInfo* dbmDBList;
	HANDLE handle1;
	HANDLE handle2;

    DBM_DO(dbm_BackupDBList(&fd1, &handle1, &fd2, &handle2));

    e = dbm_ReadDBMFile(fd1, &numOfDatabases, &dbmDBList, &nextDBId, &nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    *volId = nextVolId++;

    e = dbm_WriteDBMFile(fd1, numOfDatabases, dbmDBList, nextDBId, nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RestoreDBList(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));

    return eNOERROR;
}

Four dbm_GetAndIncreaseDBId
(
    Four* dbId   /* OUT volume id */
)
{
    Four numOfDatabases;
    Four nextVolId, nextDBId;
    Four e;
    int  fd1, fd2;
    dbm_DBInfo* dbmDBList;
	HANDLE handle1;
	HANDLE handle2;

    DBM_DO(dbm_BackupDBList(&fd1, &handle1, &fd2, &handle2));

    e = dbm_ReadDBMFile(fd1, &numOfDatabases, &dbmDBList, &nextDBId, &nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    *dbId = nextDBId++;

    e = dbm_WriteDBMFile(fd1, numOfDatabases, dbmDBList, nextDBId, nextVolId);
    if (e < 0)
    {
        DBM_DO(dbm_RestoreDBList(fd1, handle1, fd2, handle2));
        DBM_ERROR(e);
    }

    DBM_DO(dbm_RemoveBackup(fd1, handle1, fd2, handle2));

    return eNOERROR;
}
