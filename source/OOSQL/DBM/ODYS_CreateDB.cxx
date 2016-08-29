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

#include <iostream.h>
extern "C" {
#include <string.h>
#include <stdlib.h>
#include "dblablib.h"
#include "cosmos_r.h"
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
}
#include "DBM.h"

void usage(char* command)
{
    printf("%s <database name> [<volume name>] [-dbdir <database directory>] [-extentSize <extent size>] [-extentFillFactor <extent fill factor>] [-segmentSize <segment size>] [-device <device path> <number of page> [-device <device path> <number of page>]...]\n", command);
}

#ifdef WIN32
#include <io.h>
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#define F_OK 00
#else
#define DIRECTORY_SEPARATOR "/"
#endif

Four extentSize = 16;
Four extentFillFactor = 100;
Four segmentSize = 64;
Four numberOfDevices = 0;
char *devNameList[DBM_MAXDEVICESINVOLS];
Four *numPagesInDevice = NULL;
char dbDir[MAXPATHLEN];
char *volName = NULL;

Four setOption
(
    int    argc,
    char** argv
)
{
    int   i;
    char* p;
    Boolean volNameFlag = (Boolean)FALSE;

    p = getenv(DBM_OODB_DIR);
    if (p == NULL)
        DBM_ERROR(eENVNOTDEFINED_DBM);
    sprintf(dbDir, "%s/%s", p, argv[1]);

    for (i=2; i<argc; i++)
    {
        if (strcmp(argv[i], "-extentSize") == 0)
        {
            i++;
            if (i < argc)
                extentSize = atol(argv[i]);
            else
                return eINVALIDOPTION_DBM;
        }
        else if (strcmp(argv[i], "-extentFillFactor") == 0)
        {
            i++;
            if (i < argc)
                extentFillFactor = atol(argv[i]);
            else
                return eINVALIDOPTION_DBM;
        }
        else if (strcmp(argv[i], "-segmentSize") == 0)
        {
            i++;
            if (i < argc)
                segmentSize = atol(argv[i]);
            else
                return eINVALIDOPTION_DBM;
        }
        else if (strcmp(argv[i], "-dbdir") == 0)
        {
            i++;
            if (i < argc)
                strcpy(dbDir, argv[i]);
            else
                return eINVALIDOPTION_DBM;
        }
        else if (strcmp(argv[i], "-device") == 0)
        {
            numberOfDevices++;

            numPagesInDevice = (Four*)realloc(numPagesInDevice, sizeof(Four)*numberOfDevices);
            if (numPagesInDevice == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

            i++;
            if (i < argc)
                devNameList[numberOfDevices-1] = argv[i];
            else
                return eINVALIDOPTION_DBM;

            i++;
            if (i < argc)
                numPagesInDevice[numberOfDevices-1] = atol(argv[i]);
            else
                return eINVALIDOPTION_DBM;
        }
        else
        {
            if (volNameFlag == FALSE)
            {
                volName = argv[i];
                volNameFlag = (Boolean)TRUE;
            }
            else
                return eINVALIDOPTION_DBM;
        }
    }

    if (volName == NULL)
        volName = argv[1];

    if (numberOfDevices == 0)
    {
        numberOfDevices++;

        devNameList[numberOfDevices-1] = (char*)malloc(DBM_MAXDEVICENAME);
        if (devNameList == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

        numPagesInDevice = (Four*)malloc(sizeof(Four));
        if (numPagesInDevice == NULL) DBM_ERROR(eMEMORYALLOCERROR_DBM);

        sprintf(devNameList[numberOfDevices-1], "%s/%s", dbDir, volName);
        numPagesInDevice[numberOfDevices-1] = DBM_DEFAULT_PAGE_NUMBER_OF_DEVICE;
    }

    return eNOERROR;
}

int main
(
    int    argc,
    char** argv
)
{
    Four e;
    Four volId;
    char command[1024];
    char *binDir, *schemaDir;

    if (argc < 2)
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

    DBM_DO(DBM_AddDatabase(argv[1], dbDir, volName, numberOfDevices, devNameList, numPagesInDevice, extentSize, segmentSize, extentFillFactor));

	if(access(dbDir, F_OK) == 0)
	{
		printf("The directory '%s' is already existed.\nProceeds to next operation.\n", dbDir);
	}
	else
	{
		sprintf(command, "mkdir %s", dbDir);
		e = run(command);
		if (e < 0)
		{
			DBM_DO(DBM_RemoveDatabase(argv[1]));
			exit(1);
		}
	}

    e = DBM_FormatDB(argv[1]);
    if (e < 0)
    {
        DBM_DO(DBM_RemoveDatabase(argv[1]));
        exit(1);
    }

    e = DBM_GetDefaultVolumeID(argv[1], &volId);
    if (e < 0)
    {
        DBM_DO(DBM_RemoveDatabase(argv[1]));
        exit(1);
    }

    printf("%s terminated successfuly\n", argv[0]);
    return 0;
}
