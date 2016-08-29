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
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_ServerQuery.hxx"
#include "DBM.h"
#include <string.h>
#include <stdlib.h>

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_SetUserDefaultVolumeID(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				databaseID,		// IN  
	Four				volumeID		// IN  
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID == NIL)
		OOSQL_ERR(eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL);

	OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = volumeID;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetUserDefaultVolumeID(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				databaseID,		// IN  
	Four*				volumeID		// OUT 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID == NIL)
		OOSQL_ERR(eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL);

	*volumeID = OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetVolumeIDByNumber(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				databaseID,		// IN  
	Four				number,			// IN  
	Four*				volumeID		// OUT 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

    if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID == NIL)
        OOSQL_ERR(eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL);

	*volumeID = OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[number].volID;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL 
eNO_SUCH_VOLUME_MOUNTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetVolumeID(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				databaseID,		// IN  
	char*				volumeName,		// IN  
	Four*				volumeID		// OUT 
)
{
   	Four i;
    Four volID;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

    if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID == NIL)
        OOSQL_ERR(eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL);

    volID = NIL;
    for(i = 0; i < MAXNUMOFVOLS; i++)
    {
        if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID != NIL &&
           !strcmp(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volumeName, volumeName))
        {
            volID = OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID;
            break;
        }
    }

    if(volID == NIL)
        OOSQL_ERR(eNO_SUCH_VOLUME_MOUNTED_OOSQL);

	*volumeID = volID;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Mount(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				numDevices,		// IN  
	char**				devNames,		// IN  
	Four*				volID           // OUT 
)
{
	Four e;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	e = LOM_Mount(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), numDevices, devNames, volID);
	if(e == eDEVICEOPENFAIL_RDSM)
	{
		OOSQL_CHECK_ERR(eDEVICEOPENFAIL_OOSQL);
	}
	else
	{
		OOSQL_CHECK_ERR(e);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_MountVolumeByVolumeName(
	OOSQL_SystemHandle* systemHandle,	// IN 
	char*				databaseName,	// IN  
	char*				volumeName,		// IN  
	Four*				volID           // OUT
)
{
	Four numOfVolumes = MAXNUMOFVOLS;
	Four* volIdList;
	char* volNameList;
	char* devNameList;
	char* devNames[MAXNUMOFDEVICES];
	char* volNames[MAXNUMOFVOLS];
	Four numOfDevices = MAXNUMOFDEVICES;
	Four e;
	Four i, j;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	/* allocate memory */
	volIdList   = (Four*)malloc(sizeof(Four) * MAXNUMOFVOLS);
	volNameList = (char*)malloc(sizeof(char) * MAXNUMOFVOLS * DBM_MAXVOLUMENAME);
	devNameList = (char*)malloc(sizeof(char) * MAXNUMOFDEVICES * DBM_MAXDEVICENAME);

	memset(volNameList, 0, sizeof(char) * MAXNUMOFVOLS * DBM_MAXVOLUMENAME);
	for(i = 0; i < MAXNUMOFVOLS; i++)
		volNames[i] = &volNameList[i * DBM_MAXVOLUMENAME];

	memset(devNameList, 0, sizeof(char) * MAXNUMOFDEVICES * DBM_MAXDEVICENAME);
	for(i = 0; i < MAXNUMOFDEVICES; i++)
		devNames[i] = &devNameList[i * DBM_MAXDEVICENAME];

	e = DBM_GetVolumeList(databaseName, &numOfVolumes, volNames, volIdList);
	OOSQL_CHECK_ERR(e);

	for(i = 0; i < numOfVolumes; i++)
	{
		if(!strcmp(volNames[i], volumeName))
		{
			numOfDevices = MAXNUMOFDEVICES;
			e = DBM_GetDeviceList(databaseName, volNames[i], &numOfDevices, devNames);
			OOSQL_CHECK_ERR(e);

			e = OOSQL_Mount(systemHandle, numOfDevices, devNames, volID);
			OOSQL_CHECK_ERR(e);

			break;
		}
	}

	/* free memory */
	free(volIdList);
	free(volNameList);
	free(devNameList);

	if(i == numOfVolumes)
		OOSQL_ERR(eNO_SUCH_VOLUME_MOUNTED_OOSQL);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Dismount(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volID			// OUT 
)
{
	Four e;
	
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	e = LOM_Dismount(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
eDATABASE_ALREADY_MOUNTED_OOSQL 
eDATABASENAME_TOO_LONG_OOSQL 
eVOLUME_TITLE_TOO_LONG_OOSQL 
eTOOMANYVOLS_OOSQL  
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_MountDB(
	OOSQL_SystemHandle* systemHandle,	// IN  
	char*				databaseName,	// IN  
	Four*				databaseID		// OUT 
)
{
	Four numOfVolumes = MAXNUMOFVOLS;
	Four* volIdList;
	char* volNameList;
	char* devNameList;
	char* devNames[MAXNUMOFDEVICES];
	char* volNames[MAXNUMOFVOLS];
	Four numOfDevices = MAXNUMOFDEVICES;
	Four e;
	Four volID;
	Four i, j;
	XactID xactId;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID != NIL)
		OOSQL_ERR(eDATABASE_ALREADY_MOUNTED_OOSQL);

	/* allocate memory */
	volIdList   = (Four*)malloc(sizeof(Four) * MAXNUMOFVOLS);
	volNameList = (char*)malloc(sizeof(char) * MAXNUMOFVOLS * DBM_MAXVOLUMENAME);
	devNameList = (char*)malloc(sizeof(char) * MAXNUMOFDEVICES * DBM_MAXDEVICENAME);

	memset(volNameList, 0, sizeof(char) * MAXNUMOFVOLS * DBM_MAXVOLUMENAME);
	for(i = 0; i < MAXNUMOFVOLS; i++)
		volNames[i] = &volNameList[i * DBM_MAXVOLUMENAME];

	memset(devNameList, 0, sizeof(char) * MAXNUMOFDEVICES * DBM_MAXDEVICENAME);
	for(i = 0; i < MAXNUMOFDEVICES; i++)
		devNames[i] = &devNameList[i * DBM_MAXDEVICENAME];

	if(strlen(databaseName) >= sizeof(OOSQL_GDSINSTTABLE[systemHandle->instanceId].databaseName))
		OOSQL_ERR(eDATABASENAME_TOO_LONG_OOSQL);
	strcpy(OOSQL_GDSINSTTABLE[systemHandle->instanceId].databaseName, databaseName);

	e = DBM_GetVolumeList(databaseName, &numOfVolumes, volNames, volIdList);
	OOSQL_CHECK_ERR(e);

	e = DBM_GetDatabaseID(databaseName, databaseID);
	OOSQL_CHECK_ERR(e);

	for(i = 0; i < numOfVolumes; i++)
	{
		/* find the volume in mount table */
		for(j = 0; j < MAXNUMOFVOLS; j++)
		{
			if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volID != NIL &&
			   !strcmp(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volumeName, volNames[i]))
			{
				OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].nMounts ++;
				break;
			}
		}
	
		/* get free entries */
		if(j == MAXNUMOFVOLS)
		{
			for(j = 0; j < MAXNUMOFVOLS; j++)
				if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volID == NIL)
					break;

			if(j == MAXNUMOFVOLS)
				OOSQL_ERR(eTOOMANYVOLS_OOSQL);

			OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].nMounts = 1;

			if(strlen(volNames[i]) >= sizeof(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volumeName))
				OOSQL_ERR(eVOLUME_TITLE_TOO_LONG_OOSQL);

			strcpy(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volumeName, volNames[i]);
		}

		numOfDevices = MAXNUMOFDEVICES;
		e = DBM_GetDeviceList(databaseName, volNames[i], &numOfDevices, devNames);
		OOSQL_CHECK_ERR(e);

		e = OOSQL_Mount(systemHandle, numOfDevices, devNames, &volID);
		OOSQL_CHECK_ERR(e);

		OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volID = volID;

		if(i == 0)
		{
			OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = volID;
		}
	}

	/* free memory */
	free(volIdList);
	free(volNameList);
	free(devNameList);

	e = OOSQL_TransBegin(systemHandle, &xactId, X_BROWSE_BROWSE);
    if(e < eNOERROR)
    {
        OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = NIL;
        return e;
    }

    if(e < eNOERROR)
    {
        OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = NIL;
        return e;
    }

#if defined (HEURISTIC_MODEL)
	Four numObjects;
	for (i = 0; i < MAXNUMOFVOLS; i++)
	{
		if (OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID != NIL)
		{
			volID = OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID;

			e = OOSQL_GetNumTextObjectsInVolume(systemHandle, volID, &numObjects);
            if(e < eNOERROR)
            {
                OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = NIL;
                OOSQL_CHECK_ERR(e);
            }

			OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].nObjects = numObjects;
		}
	}
#endif

	e = OOSQL_TransCommit(systemHandle, &xactId);
    if(e < eNOERROR)
    {
        OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = NIL;
        return e;
    }

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_DismountDB(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				databaseID      // IN 
)
{
	Four numOfVolumes = MAXNUMOFVOLS;
	Four* volIdList;
	char* volNameList;
	char* volNames[MAXNUMOFVOLS];
	Four e;
	Four volID;
	Four i, j;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID == NIL)
		OOSQL_ERR(eDATABASE_HAS_NOT_BEEN_MOUNTED_OOSQL);

	/* allocate memory */
	volIdList   = (Four*)malloc(sizeof(Four) * MAXNUMOFVOLS);
	volNameList = (char*)malloc(sizeof(char) * MAXNUMOFVOLS * DBM_MAXVOLUMENAME);

	memset(volNameList, 0, sizeof(char) * MAXNUMOFVOLS * DBM_MAXVOLUMENAME);
	for(i = 0; i < MAXNUMOFVOLS; i++)
		volNames[i] = &volNameList[i * DBM_MAXVOLUMENAME];

	e = DBM_GetVolumeList(OOSQL_GDSINSTTABLE[systemHandle->instanceId].databaseName, &numOfVolumes, volNames, volIdList);
	OOSQL_CHECK_ERR(e);

	for(i = 0; i < numOfVolumes; i++)
	{
		/* find the volume in mount table */
		for(j = 0; j < MAXNUMOFVOLS; j++)
		{
			if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volID == volIdList[i])
			{
				OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].nMounts --;

				if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].nMounts <= 0)
				{
					/* free this entry */
					volID = OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volID;
					OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[j].volID = NIL;

				}
				break;
			}
		}

		if(j != MAXNUMOFVOLS && volID != NIL)
		{
			e = OOSQL_Dismount(systemHandle, volID);
			OOSQL_CHECK_ERR(e);
		}
	}
	/* free memory */
	free(volIdList);
	free(volNameList);

	OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = NIL;

	return eNOERROR;
}
