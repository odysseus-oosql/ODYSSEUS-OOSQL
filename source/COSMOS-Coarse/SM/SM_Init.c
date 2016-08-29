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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Coarse-Granule Locking (Volume Lock) Version                            */
/*    Version 3.0                                                             */
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
/*
 * Module: SM_Init.c
 *
 * Description:
 *  Initialize the storage system. 
 *
 * Exports:
 *  Four SM_Init(Boolean, char*)
 */


#include <string.h>
#include <stdlib.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "Util_Sort.h" 		
#include "RDsM_Internal.h"
#include "BfM.h"
#include "RM.h"
#include "SM_Internal.h"
#include "OM_Internal.h"
#include "BtM_Internal.h"
#include "BL_OM_Internal.h"
#include "BL_BtM_Internal.h"
#include "BL_SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"
#include "THM_Internal.h"


/*@ global variables */
CfgParams_T sm_cfgParams = {
    NULL,			    /* logVolumeDeviceList */
#ifdef USE_COHERENCY_VOLUME
    NULL,			    /* coherencyVolumeDevice */
#endif
    TRUE,			    /* useDeadlockAvoidance */
    FALSE			    /* useBulkFlush */
};

char* cosmosReleaseString = COSMOS_RELEASE_STRING;
char* __xyz__ 	= ENCODED_EMBEDDED_SYSTEM_VERSION_SERIAL_STRING;

Boolean  isPerProcessDSInitialized = FALSE;

/*@================================
 * SM_Init()
 *================================*/
/*
 * Function: Four SM_Init(Boolean, char*)
 *
 * Description:
 *  Initialize the storage system. This routine calls RDsM_Initialize() and
 *  BfM_Init() to initialize the Raw Disk Manager and the Buffer Manager,
 *  respectively. And then, initialize the main memory data structure used in
 *  the Scan Manager; smMountTable, SM_ScanTable, and the deletedPageList.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four _SM_Init()
{
    Four e;
    Four handle;
    FileDesc	fd1;	/* lock file descriptor for starting critical section */ 
    FileDesc	fd2;	/* lock file descriptor for ending critical section */ 

    handle = -1;

    TR_PRINT(TR_SM, TR1, ("_SM_Init()"));

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* start of critical section for shared memory */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd1); 
#endif

    if (IS_PER_PROCESS_DS_INITIALIZED == FALSE) {
	IS_PER_PROCESS_DS_INITIALIZED = TRUE;

    	e = THM_InitProcess();
    	if (e < 0) ERR(handle, e);
    }

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* End of critical section for shared memory */
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd2); 

    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd2);
#endif

    return(eNOERROR);
} /* SM_Init() */


Four _SM_AllocHandle(Four* handle)
{
    Four e;
    Four dummyHandle = -1;
    FileDesc	fd1;	/* lock file descriptor for starting critical section */ 
    FileDesc	fd2;	/* lock file descriptor for ending critical section */ 


    TR_PRINT(TR_SM, TR1, ("_SM_AllocHandle(handle)"));

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* start of critical section for shared memory */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(dummyHandle, &fd1); 
#endif

    e = THM_AllocHandle(handle);
    if (e < eNOERROR) ERR(*handle, e); 

    e = sm_InitializeGlobalData(*handle);
    if (e < eNOERROR) ERR(*handle, e);

#ifdef LRDS_INCLUDED
    e = LRDS_InitSharedDS(*handle);
    if (e < eNOERROR) ERR(handle, e);

    e = LRDS_InitLocalDS(*handle);
    if (e < eNOERROR) ERR(*handle, e);
#endif
    
#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)
    /* End of critical section for shared memory */
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(*handle, &fd2); 

    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd2);
#endif

    return(eNOERROR);
} /* SM_AllocHandle(Four* ) */


Four sm_InitializeGlobalData(Four handle)
{
    Four 		e;			/* error number */
    char		*logDevName;
    Four                indexBulkLoadID;
    Four                dataBulkLoadID;
    Four                mountTableIndex;
    Four                scanTableIndex;
    Four                tempFileIdTableIndex;
#ifdef USE_COHERENCY_VOLUME
    char		*coherencyVolumeDevName;
#endif

    TR_PRINT(TR_SM, TR1, ("_sm_InitializeGlobalData(handle)"));

    /* Initialize global variables here for multi threading */
    SM_PER_THREAD_DS(handle).xactRunningFlag = FALSE;
    SM_PER_THREAD_DS(handle).globalXactRunningFlag = FALSE;

    /* Initialize RDsM level. */
    e = RDsM_Initialize(handle);
    if (e < 0) ERR(handle, e);

#ifdef DO_NOT_USE_OLD_SCHEME_YKL05MAY98
    /* get log volume name from environment list */
    logDevName = getenv("COSMOS_LOG_VOLUME");

    /* set logUseFlag */
    if(logDevName != NULL) {        
        Four len;               /* length of string */
        char *ptr;

        len = strlen(logDevName) + 1;

        if ((ptr = realloc(sm_cfgParams.logVolumeDeviceList, len)) == NULL)
            ERR(handle, eMEMORYALLOCERR);
         
        strcpy(ptr, logDevName);
        
        sm_cfgParams.logVolumeDeviceList = ptr;
    }

#ifdef USE_COHERENCY_VOLUME
    coherencyVolumeDevName = getenv("COSMOS_COHERENCY_VOLUME");

    if(coherencyVolumeDevName != NULL)
    {
        Four len;
        char* ptr;

        len = strlen(coherencyVolumeDevName) + 1;

        if ((ptr = realloc(sm_cfgParams.coherencyVolumeDevice, len)) == NULL)
            ERR(handle, eMEMORYALLOCERR);
         
        strcpy(ptr, coherencyVolumeDevName);
        
        sm_cfgParams.coherencyVolumeDevice = ptr;
    }
#endif

    if(getenv("COSMOS_DEADLOCK_AVOIDANCE"))
    {
	sm_cfgParams.useDeadlockAvoidance = TRUE;
    }
    else
    {
	sm_cfgParams.useDeadlockAvoidance = FALSE;
    }
#endif /* DO_NOT_USE_OLD_SCHEME_YKL05MAY98 */

    /* Initialize BfM level. */
    e = BfM_InitPerThreadDS(handle);
    if (e < 0) ERR(handle, e);

#ifdef USE_COHERENCY_VOLUME
    e = BfM_MountCoherencyVolume(handle, sm_cfgParams.coherencyVolumeDevice);
    if (e < 0) ERR(handle, e);
#endif

    /*
     * Initialize RM_PER_THREAD data structures
     */
    RM_PER_THREAD_DS(handle).rm_onPrepareFlag = FALSE;
    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo = NO_LOG_VOLUME;
    
    e = RM_Restart(handle, sm_cfgParams.logVolumeDeviceList);
    if (e < 0) ERR(handle, e);
    
    e = LOT_Init(handle);
    if (e < 0) ERR(handle, e);
    
    e = Util_Sort_Init(handle);
    if (e < 0) ERR(handle, e);

    /*
     * Initialize bulkload table
     */

    /* BtM bulkload table */
    for (indexBulkLoadID = 0; indexBulkLoadID < BTM_BLKLD_TABLE_SIZE; indexBulkLoadID++ ) {
        BTM_BLKLD_TABLE(handle)[indexBulkLoadID].isUsed = FALSE; /* insert a handle into BTM_BLKLD_TABLE */
    }

    /* OM bulkload table */
    for (dataBulkLoadID = 0; dataBulkLoadID < OM_BLKLD_TABLE_SIZE; dataBulkLoadID++ ) {
        OM_BLKLD_TABLE(handle)[dataBulkLoadID].isUsed = FALSE; /* insert a handle into OM_BLKLD_TABLE */
    }

    /* SM bulkload table */
    for (indexBulkLoadID = 0; indexBulkLoadID < SM_IDXBLKLD_TABLE_SIZE; indexBulkLoadID++ ) {
        SM_IDXBLKLD_TABLE(handle)[indexBulkLoadID].isUsed = FALSE;
    }

    /*
    ** Initailze the main memory data structure used in Scan Manager.
    */
    /* Initialize the entries in the Mount Table. */
    for (mountTableIndex = 0; mountTableIndex < MAXNUMOFVOLS; mountTableIndex++)
        SM_PER_THREAD_DS(handle).smMountTable[mountTableIndex].volId = NIL;

    /* Allocate some entries in the Scan Table */
    e = Util_initVarArray(handle, &SM_PER_THREAD_DS(handle).smScanTable, sizeof(sm_ScanTableEntry), INITSCAN);
    if (e < 0) ERR(handle, e);

    /*@ for each entry */
    /* Initialize the allocated entries in the Scan Table */
    for (scanTableIndex = 0; scanTableIndex < SM_PER_THREAD_DS(handle).smScanTable.nEntries; scanTableIndex++)
        SM_SCANTABLE(handle)[scanTableIndex].scanType = NIL;

    /* Allocate some entries in the temporary file ID table */
    e = Util_initVarArray(handle, &SM_PER_THREAD_DS(handle).smTmpFileIdTable, sizeof(FileID), INITTMPFILE);
    if (e < 0) ERR(handle, e);

    /* Initialize the allocated entries in the temporary file ID table */
    for (tempFileIdTableIndex = 0; tempFileIdTableIndex < SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries; tempFileIdTableIndex++)
        SET_NILFILEID(SM_TMPFILEIDTABLE(handle)[tempFileIdTableIndex]);

    /* Initialize the dealloctedPageList Pool. */
    e = Util_initPool(handle, &SM_PER_THREAD_DS(handle).dlPool, sizeof(DeallocListElem), INITDLPOOL);
    if (e < 0) ERR(handle, e);


    /*
    ** Construct key descriptors of the indexes on the catalog tables.
    */
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.flag = 0;
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.nparts = 2;
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.kpart[0].type = SM_VOLNO;
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.kpart[0].offset = 0;
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.kpart[0].length = SM_VOLNO_SIZE;
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.kpart[1].type = SM_SERIAL;
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.kpart[1].offset = SM_VOLNO_SIZE;
    SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc.kpart[1].length = SM_SERIAL_SIZE;
    
    SM_PER_THREAD_DS(handle).smSysTablesBtreeFileIdIndexKdesc = SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc;
    SM_PER_THREAD_DS(handle).smSysIndexesIndexFileIdIndexKdesc = SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc; 
    SM_PER_THREAD_DS(handle).smSysIndexesIndexIdIndexKdesc = SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc;

    SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc.flag = 0;
    SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc.nparts = 1;
    SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc.kpart[0].type = SM_VARSTRING;
    SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc.kpart[0].offset = 0;
    SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc.kpart[0].length = SM_COUNTER_NAME_MAX_LEN+1;
	
    return(eNOERROR);
} /* sm_InitializeGlobalData */
