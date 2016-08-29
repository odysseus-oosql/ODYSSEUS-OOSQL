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
#include <iostream>
extern "C" {
#include <string.h>
#include <stdlib.h>
#include "cosmos_r.h"
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
}
#include "DBM.h"
#include "Error.hxx"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"

void usage(char* command)
{
    printf("%s [-temporary] <database name> <volume name> [-extentSize <extent size>] [-extentFillFactor <extent fill factor>] [-segmentSize <segment size>] [-device <device path> <number of page> [-device <device path> <number of page>]...]\n", command);
}

Four extentSize;
Four extentFillFactor;
Four segmentSize;
Four numberOfDevices = 0;
char *devNameList[DBM_MAXDEVICESINVOLS];
Four *numPagesInDevice = NULL;
char *databaseName;
char *volName;
Boolean isTemporaryVolume;

Four setOption
(
    int    argc,
    char** argv
)
{
    int		i;
	int		count = 1;

	if(!strcmp(argv[count], "-temporary"))
	{
		isTemporaryVolume = SM_TRUE; 
		count ++;
	}
	else
		isTemporaryVolume = SM_FALSE;

	databaseName = argv[count];
	count ++;

	volName = argv[count];
	count ++;

    for (i=count; i<argc; i++)
    {
        if (strcmp(argv[i], "-extentSize") == 0)
        {
            i++;
            if (i < argc)
                extentSize = atol(argv[i]);
            else
                ERROR_CHECK(eINVALIDOPTION_DBM);
        }
        else if (strcmp(argv[i], "-extentFillFactor") == 0)
        {
            i++;
            if (i < argc)
                extentFillFactor = atol(argv[i]);
            else
                ERROR_CHECK(eINVALIDOPTION_DBM);
        }
        else if (strcmp(argv[i], "-segmentSize") == 0)
        {
            i++;
            if (i < argc)
                segmentSize = atol(argv[i]);
            else
                ERROR_CHECK(eINVALIDOPTION_DBM);
        }
        else if (strcmp(argv[i], "-device") == 0)
        {
            numberOfDevices++;

            numPagesInDevice = (Four*)realloc(numPagesInDevice, sizeof(Four)*numberOfDevices);
            if (numPagesInDevice == NULL) ERROR_CHECK(eMEMORYALLOCERROR_DBM);

            i++;
            if (i < argc)
                devNameList[numberOfDevices-1] = argv[i];
            else
                ERROR_CHECK(eINVALIDOPTION_DBM);

            i++;
            if (i < argc)
                numPagesInDevice[numberOfDevices-1] = atol(argv[i]);
            else
                ERROR_CHECK(eINVALIDOPTION_DBM);
        }
    }

    return eNOERROR;
}

int main
(
    int    argc,
    char** argv
)
{
    Four        nextDBId, nextVolId, numOfDatabases;
    dbm_DBInfo* dbList;
    char        *dbDir;
    int         fd;
	HANDLE		handle;
    char        dbmFileName[MAXPATHLEN];
    Four        i, j, k;
    Four        e;
    Four        volId;

    if (argc < 3)
    {
        usage(argv[0]);
        exit(1);
    }

    e = setOption(argc, argv);
    if (e == eINVALIDOPTION_DBM)
    {
        usage(argv[0]);
        exit(1);
    }
    else if (e < 0)
        exit(1);

    dbDir = getenv(DBM_OODB_DIR);
    if (dbDir == NULL)
        ERROR_CHECK(eENVNOTDEFINED_DBM);
#ifndef WIN32	
    sprintf(dbmFileName, "%s/%s", dbDir, DBM_FILENAME);
#else
	sprintf(dbmFileName, "%s\\%s", dbDir, DBM_FILENAME);
#endif
    MAIN_DO(dbm_LockFile(dbmFileName, &fd, &handle));
    MAIN_DO2(dbm_ReadDBMFile(fd, &numOfDatabases, &dbList, &nextDBId, &nextVolId), fd, handle);

    for (i=0; i<numOfDatabases; i++)
        if (strcmp(dbList[i].dbName, databaseName) == 0)
            break;
    if (i == numOfDatabases) ERROR_CHECK(eNOSUCHDB_DBM);

    for (j=0; j<dbList[i].numOfVolumes; j++)
        if (strcmp(dbList[i].volumes[j].volName, volName) == 0)
            break;
    if (j == dbList[i].numOfVolumes) ERROR_CHECK(eNOSUCHVOLUME_DBM);

    volId = dbList[i].volumes[j].volId;

    extentSize = dbList[i].volumes[j].extSize;
    extentFillFactor = dbList[i].volumes[j].eff;
    segmentSize = dbList[i].volumes[j].segmentSize;

    if (numberOfDevices != 0)
    {
        free(dbList[i].volumes[j].devices);
        dbList[i].volumes[j].devices = (dbm_DevInfo*) malloc(sizeof(dbm_DevInfo)*numberOfDevices);
        if (dbList[i].volumes[j].devices == NULL) ERROR_CHECK(eMEMORYALLOCERROR_DBM);
        for (k=0; k<numberOfDevices; k++)
        {
            strcpy(dbList[i].volumes[j].devices[k].devName, devNameList[k]);
            dbList[i].volumes[j].devices[k].numOfPages = numPagesInDevice[k];
        }
        dbList[i].volumes[j].numOfDevices = numberOfDevices;
    }

    /* Initialize DBM file */
    e = dbm_InitDBMFile(&fd, &handle);
    if (e < 0) ERROR_CHECK(e);

    MAIN_DO2(dbm_WriteDBMFile(fd, numOfDatabases, dbList, nextDBId, nextVolId), fd, handle);
    MAIN_DO(dbm_UnlockFile(fd, handle));

    MAIN_DO(DBM_FormatVolume(databaseName, volName, isTemporaryVolume));

    printf("%s terminated successfuly\n", argv[0]);
    return 0;
}
