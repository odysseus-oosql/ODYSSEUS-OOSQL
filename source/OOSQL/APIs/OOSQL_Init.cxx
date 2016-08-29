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
#include "OOSQL_ExternalFunctionManager.hxx"
#include "OOSQL_ExternalFunctionDispatcher.hxx"

VarArray oosqlGDSInstanceTable;

#ifdef COSMOS_MULTITHREAD
cosmos_thread_mutex_t mutexVar = COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS;
Four isParamInitialized = 0;
#endif


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_CreateSystemHandle(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four*				procIndex		// OUT 
)
{
	Four	e;
	Four	i, j;
	char*   s;
	char*	logDevName;
	char*	coherencyVolumeDevName;

	/* Make parameter setting to be a critical section */
#ifdef COSMOS_MULTITHREAD
	e = cosmos_thread_mutex_lock(&mutexVar);
	if (e < eNOERROR) OOSQL_ERR(e);
#endif

#ifdef COSMOS_MULTITHREAD
	if (!isParamInitialized) {	/* only one thread can initialize parameters */
		isParamInitialized = 1;
#endif
		// Move paramter setting of SM layer to this position

		// set log volume device parameter
		logDevName = getenv("COSMOS_LOG_VOLUME");
		if(logDevName != NULL)
		{
			e = LOM_SetCfgParam(NULL, "LOG_VOLUME_DEVICE_LIST", logDevName);
			if (e < eNOERROR) OOSQL_ERR(e);
		}

		// set coherency volume device parameter
    	coherencyVolumeDevName = getenv("COSMOS_COHERENCY_VOLUME");
    	if(coherencyVolumeDevName != NULL)
    	{
			e = LOM_SetCfgParam(NULL, "COHERENCY_VOLUME_DEVICE", coherencyVolumeDevName);
			if (e < eNOERROR) OOSQL_ERR(e);
    	}

		// set deadlock avoidance parameter
		// set USE_DEADLOCK_AVOIDANCE to FALSE only if NO_USE_DEADLOCK_AVOIDANCE
		if (getenv("NO_USE_DEADLOCK_AVOIDANCE") != NULL)
		{
			e = LOM_SetCfgParam(NULL, "USE_DEADLOCK_AVOIDANCE", "FALSE");
			if (e < eNOERROR) OOSQL_ERR(e);
		}
		else
		{
			e = LOM_SetCfgParam(NULL, "USE_DEADLOCK_AVOIDANCE", "TRUE");
			if (e < eNOERROR) OOSQL_ERR(e);
		}


#ifdef COSMOS_MULTITHREAD
	}
#endif

#ifdef SLIMDOWN_OPENGIS
	e = LOM_CreateHandle(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), procIndex);
#else /* SLIMDOWN_OPENGIS */
	e = GEO_CreateHandle(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), procIndex);
#endif /* SLIMDOWN_OPENGIS */

	if (e < 0) OOSQL_ERR(e);

	// initialize global data structure table
	if(OOSQL_GDSINSTTABLE_ENTRIES == 0) 
	{
        e = LOM_initVarArray(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), &oosqlGDSInstanceTable, sizeof(OOSQL_GDSInstance), LOM_INIT_NUM_OF_THREADS);
        if (e < eNOERROR) OOSQL_ERR(e);
        for (i = 0; i < OOSQL_GDSINSTTABLE_ENTRIES; i++) 
            OOSQL_GDSINSTTABLE[i].inUse = SM_FALSE;
    }
	
	/* find the available entry of global data structure table */
    for (i = 0; i < OOSQL_GDSINSTTABLE_ENTRIES; i++) 
        if(OOSQL_GDSINSTTABLE[i].inUse == SM_FALSE) break;

    /* if we donot find the available entry, do doubling */
    if(i == OOSQL_GDSINSTTABLE_ENTRIES) 
	{
        e = LOM_doublesizeVarArray(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), &oosqlGDSInstanceTable, sizeof(OOSQL_GDSInstance));
        if (e < eNOERROR) OOSQL_ERR(e);

        /* Initialize the newly allocated entries */
        for (j = i; j < OOSQL_GDSINSTTABLE_ENTRIES; j++) 
            OOSQL_GDSINSTTABLE[j].inUse = SM_FALSE;
    }

	systemHandle->instanceId = i;
	OOSQL_GDSINSTTABLE[i].inUse = SM_TRUE;
	
#ifdef COSMOS_MULTITHREAD
    e = cosmos_thread_mutex_unlock(&mutexVar);
    if (e < eNOERROR) OOSQL_ERR(e);
#endif

	// init memory manager
#ifndef _LP64
	OOSQL_GDSINSTTABLE[systemHandle->instanceId].memoryManager = new OOSQL_QuickFitMemoryManager;
#else
	OOSQL_GDSINSTTABLE[systemHandle->instanceId].memoryManager = new OOSQL_MemoryManager;
#endif
	
	// init external function manager and dispatcher
	OOSQL_NEW(OOSQL_GDSINSTTABLE[systemHandle->instanceId].externalFunctionManager, OOSQL_GDSINSTTABLE[systemHandle->instanceId].memoryManager, OOSQL_ExternalFunctionManager(systemHandle));
	OOSQL_NEW(OOSQL_GDSINSTTABLE[systemHandle->instanceId].externalFunctionDispatcher, OOSQL_GDSINSTTABLE[systemHandle->instanceId].memoryManager, OOSQL_ExternalFunctionDispatcher(systemHandle, OOSQL_GDSINSTTABLE[systemHandle->instanceId].dbInfo));
	
	// init OOSQL query instance table
	e = LOM_initVarArray(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), &OOSQL_GDSINSTTABLE[systemHandle->instanceId].queryInstanceTable, sizeof(OOSQL_QueryInstance), INITQUERYINSTANCETABLE);
	if (e < 0) OOSQL_ERR(e);

	for(i = 0; i < OOSQL_QUERYINSTTABLE_ENTRIES(systemHandle); i++)
	{
		OOSQL_QUERYINSTTABLE(systemHandle)[i].query = new OOSQL_ServerQuery(OOSQL_GDSINSTTABLE[systemHandle->instanceId].externalFunctionManager, 
			                                                                OOSQL_GDSINSTTABLE[systemHandle->instanceId].externalFunctionDispatcher);
		OOSQL_QUERYINSTTABLE(systemHandle)[i].inUse = SM_FALSE;

		if(OOSQL_QUERYINSTTABLE(systemHandle)[i].query == NULL)
			return eMEMORYALLOCERR_OOSQL;
	}

	// init volume table
	for(i = 0; i < MAXNUMOFVOLS; i++)
		OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID = -1;
	OOSQL_GDSINSTTABLE[systemHandle->instanceId].userDefaultVolumeID = -1;

	return eNOERROR;
}
