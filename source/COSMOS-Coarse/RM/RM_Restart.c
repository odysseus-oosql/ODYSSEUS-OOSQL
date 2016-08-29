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
#include <assert.h>
#include <stdlib.h> /* for malloc & free */
#include <string.h>

#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "RM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*
 * Declaration of global variables
 */

/* Internal Function Prototype */
Four rm_MountVolume(Four, char *, Four*);
Four rm_GetVolNo(Four, char *, Four*);


/*@================================
 * RM_Restart()
 *================================*/
/*
 * Function: Four RM_Restart(char *)
 *
 * Description:
 *  Restart the storage system.
 *
 * Returns:
 *  error code
 */
Four RM_Restart(
    Four handle,
    char *logDevList)           		/* IN device name for log volume */
{
    Four e;                     		/* error code */
    Four 		dataVolNos[MAXNUMOFVOLS];
    LockMode 		prevLock[MAXNUMOFVOLS];
    Boolean 		prevMounted[MAXNUMOFVOLS];
    Two 		i, j;
    Four 		compare;
    Four 		volNo;                 /* volume no */
    Two  		nMountedDataVolumes;
    char 		*mountedDataVolumeList;
    char 		*ptr, *ptr_end;
    Four 		nDevicesInDataVolume;
    rm_LogVolumeInfo_t 	logVolumeInfo;

    TR_PRINT(TR_RM, TR1, ("RM_Restart(handle, logDevList)", logDevList));

    if (logDevList == NULL || !strcmp(logDevList, "")) return(eNOERROR);
   
    if(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo == NO_LOG_VOLUME) 
    {
	e = rm_MountVolume(handle, logDevList, &volNo);
	if (e < eNOERROR) ERR(handle, e);

	e = rm_ReadLogVolumeInfo(handle, volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 	
	if (e < eNOERROR) ERR(handle, e);

	if (RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onPrepareFlag == FALSE)			
	    RM_INIT_ALLOC_POSITION_OF_LOG_VOLUME(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 

	RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo = volNo; 

	/*
	 * Initialize the recovery manager.
	 */
	e = rm_InitLogTable(handle);
	if (e < eNOERROR) ERR(handle, e);

#ifdef USE_LOG_COHERENCY_VOLUME		
	/* Initialize coherency information for log volume */
	e = BfM_InitCoherencyInfo(handle, volNo);
	if (e < eNOERROR) ERR(handle, e);
#endif
    }
    else
	volNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo; 
        
#ifdef DBLOCK
    /* 
     *  Acquire volume lock for log volume
     *  Note!! restart operation must be performed by only one process, so we use exclusive lock
     */
    e = RDsM_GetVolumeLock(handle, volNo, L_X, FALSE);
    //e = RDsM_GetVolumeLock(handle, volNo, L_X, TRUE);
    if (e == eVOLUMELOCKBLOCK)
    {
	/* 
	   log volume에 lock이 걸려 있다는 것은 다른 process가 log를 사용중이므로 log의 내용을
	   읽어 restart를 할 수 없다. 또한 다른 process가 log를 사용중이라면, log에는 restart할 내용이 
	   없을 것이다. 그러므로 일단 restart를 포기하고 정상적으로 수행을 마친다.
	   정상적으로 수행을 마치는 이유는, 일단 read transaction은 수행할 수 있게 하기 위함이다. 
	*/
	/*
	   1) the log volume is locked(used) by other process means that other process use the log.
              So, we can not read the contents of the log and can not restart.
	   2) the log volume is locked(used) by other process means that other process has restarted using the log, 
              so there is nothing to be restart.
	   Because of above two, give up restart and terminate normally to let read transaction be executed for a time. 
	*/
		return eNOERROR; 
    }
    else if (e < eNOERROR) ERR(handle, e);
#endif

    /* read the log volume information : rm_LogVolumeInfo의 내용을 보존하기 위해 임시로 logVolumeInfo에 데이타를 읽는다. 
       만약 rm_LogVolumeInfo에 내용을 읽는다면, onCommitFlag가 FALSE인 경우, 이전에 사용한 내용이 rm_LogVolumeInfo에 남아 
       있어, transaction commit시, 오동작을 하게 될 가능성이 생기게 된다. */
    /* read the log volume information : Read the logVolumeInfo to temporary variable to preserve the contents of rm_LogVolumeInfo. 
       When onCommitFlag is FALSE, the contents of rm_LogVolumeInfo are used for previous transaction. 
       So, if use rm_LogVolumeInfo, an error will be occured on transaction commit.
    */   
    e = rm_ReadLogVolumeInfo(handle, volNo, &logVolumeInfo);
    if (e < eNOERROR) ERR(handle, e);

    if (logVolumeInfo.onCommitFlag) 
    {
#ifdef VERBOSE
        printf("Start Restart Operation\n", volNo);
        printf("Mount log volume %ld\n", volNo);
#endif

	e = RDsM_ReleaseVolumeLock(handle, volNo);  /* release log volume lock, to avoid deadlock sequence */
	if (e < eNOERROR) ERR(handle, e);

        if ((mountedDataVolumeList = malloc(logVolumeInfo.mountedDataVolumeListSize)) == NULL)
            ERR(handle, eMEMORYALLOCERR);

	RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.firstPageNoForMountedDataVolumeList = logVolumeInfo.firstPageNoForMountedDataVolumeList;
	RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.mountedDataVolumeListSize           = logVolumeInfo.mountedDataVolumeListSize;
        
	e = rm_LoadDataVolumeList(handle, mountedDataVolumeList);
        if (e < eNOERROR) {
            free(mountedDataVolumeList);
            ERR(handle, e);
        }
        
        /* Mount the data volumes which were being mounted. */
        ptr = mountedDataVolumeList;
        nMountedDataVolumes = 0;
        while (*ptr != '\0') {
            ptr_end = strchr(ptr, '\n');
            assert(ptr_end != NULL);
            *ptr_end = '\0';    /* convert newline character into null character */
            
	    e = rm_GetVolNo(handle, ptr, &dataVolNos[nMountedDataVolumes]);
	    if(e == eVOLNOTMOUNTED_RDSM)
	    {
#ifdef VERBOSE
		printf("Mount data volumes : %s\n", ptr);
#endif
		prevMounted[nMountedDataVolumes] = FALSE;
		e = rm_MountVolume(handle, ptr, &dataVolNos[nMountedDataVolumes]);
		if (e < eNOERROR) 
		{
		    free(mountedDataVolumeList);
		    ERR(handle, e);
		}
	    }
	    else if(e < eNOERROR)
	    {
		free(mountedDataVolumeList);
		ERR(handle, e);
	    }
	    else
	    {
#ifdef VERBOSE
		printf("Using mounted data volumes : %s\n", ptr);
#endif
		prevMounted[nMountedDataVolumes] = TRUE;
	    }

            /* Note!! we must skip log volume and coherency volume */
#ifdef USE_COHERENCY_VOLUME
	    if (volNo != dataVolNos[nMountedDataVolumes] && !BfM_IsCoherencyVolume(handle, dataVolNos[nMountedDataVolumes])) 
#else
	    if (volNo != dataVolNos[nMountedDataVolumes])	
#endif
	    {
		nMountedDataVolumes ++;
	    }

	    ptr = ptr_end + 1;  /* skip the new line, which indicates end of device list for a volume */
        }    
	free(mountedDataVolumeList);

#ifdef DBLOCK
	/* sort dataVolNos using simple bubble sort */
	/* volume lock으로 인한 dead lock을 피하기 위해, 데이타 볼륨에 순서를 두어 그 순서 대로 lock을 잡는다.
	   temporary volume, data volume, log volume순이며, 같은 종류에서는 volNo순이다 */
        /* To avoid dead lock owing to volume lock, lock volumes in order of a certain order. 
           The order is (1) temporary volume, (2) data volume, (3) log volume. 
           In addition, lock volumes in order of the volNo in same kind of volume. */
	for(i = 0; i < nMountedDataVolumes - 1; i++)
	{
	    for(j = i + 1; j < nMountedDataVolumes; j++)
	    {
		if(IS_TEMP_VOLUME(dataVolNos[i]) && !IS_TEMP_VOLUME(dataVolNos[j]))
		    compare = -1;
		else if(!IS_TEMP_VOLUME(dataVolNos[i]) && IS_TEMP_VOLUME(dataVolNos[j]))
		    compare = 1;
                else if (RM_IsLogVolume(handle, dataVolNos[i]) && !RM_IsLogVolume(handle, dataVolNos[j]))
                    compare = 1;
                else if (!RM_IsLogVolume(handle, dataVolNos[i]) && RM_IsLogVolume(handle, dataVolNos[j]))
                    compare = -1;
		else
		    compare = dataVolNos[i] - dataVolNos[j];
		if(compare > 0) /* means dataVolNos[i] > dataVolNos[j] */
		{
		    Four    tmp1;
		    Boolean tmp2;

		    tmp1 = dataVolNos[i];
		    dataVolNos[i] = dataVolNos[j];
		    dataVolNos[j] = tmp1;

		    tmp2 = prevMounted[i];
		    prevMounted[i] = prevMounted[j];
		    prevMounted[j] = tmp2;
		}
	    }
	}

	for(i = 0; i < nMountedDataVolumes; i++)
	{
	    prevLock[i] = RDsM_GetVolumeLockMode(handle, dataVolNos[i]);

            /* acquire lock for data volume */
	    e = RDsM_GetVolumeLock(handle, dataVolNos[i], L_X, FALSE);
	    if (e < eNOERROR) ERR(handle, e);
	}

	/* lastly, lock log volume */
        e = RDsM_GetVolumeLock(handle, volNo, L_X, FALSE);
        if (e < eNOERROR) ERR(handle, e);
#endif
        
	/* read the log volume information */
	/* 잠시 log volume lock을 놓은 사이에, 다른 process가 restart할 가능성이 있다. 이러한 경우를 처리함 */
        /* After the log volume lock is released, other process can be restarted. It handles this situation. */
	e = rm_ReadLogVolumeInfo(handle, volNo, &logVolumeInfo);
	if (e < eNOERROR) ERR(handle, e);

	if (logVolumeInfo.onCommitFlag) 
	{
#ifdef VERBOSE
	    printf("Start Redo operation\n");
#endif
    	    e = rm_ReadLogVolumeInfo(handle, volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 
    	    if (e < eNOERROR) ERR(handle, e);

            /* Redo the transaction operations. */
	    e = rm_Redo(handle);
	    if (e < eNOERROR) ERR(handle, e);
#ifdef VERBOSE
    	    printf("Done Redo operation\n");
#endif
	    /*
	     * Clear the flag so that there is no committed transaction
	     * which aren't saved.
	     */
	    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onCommitFlag = FALSE;
	    RM_INIT_ALLOC_POSITION_OF_LOG_VOLUME(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 
	    e = rm_WriteLogVolumeInfo(handle, volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo);
	    if (e < eNOERROR) ERR(handle, e);
	}
	else
	{
#ifdef VERBOSE
    	    printf("Skip Redo operation\n");
#endif
	}
        
        /* Dismount the data volumes which were being mounted. */
        for (i = 0; i < nMountedDataVolumes; i++) 
	{
#ifdef DBLOCK
	    if(prevLock[i] == L_NL) {	/* 이전에 볼륨 lock이 안걸린것만 푼다. 이전에 이미 걸린 lock들은 transaction commit 시점에 푼다. */
	    				/* 이전에 이미 lock이 걸렸다는것은, BfM에서 데이타를 수정하였다는 의미로, BfM이 flush되기
					   전에 lock을 풀어버리면 안되기 때문이다 */
                                        /* Release the volume locks that are not acquired before. 
					   Release the volume locks that are already acquired before when the transaction is committed. */ 
                                        /* That the lock is acquired before means that buffers are updated in BfM. 
                                           So, do not release the lock until the buffers are flushed. */
		/* release lock for data volume */
		e = RDsM_ReleaseVolumeLock(handle, dataVolNos[i]);
		if (e < eNOERROR) ERR(handle, e);
	    }
#endif
	    if(!prevMounted[i])
	    {
#ifdef VERBOSE
		printf("Dismount data volumes : %ld\n", dataVolNos[i]);
#endif
		e = RDsM_Dismount(handle, dataVolNos[i]);
		if (e < eNOERROR) ERR(handle, e);
	    }
        }
    }

#ifdef DBLOCK
    /* 
     * Release volume lock for log volume
     */
    e = RDsM_ReleaseVolumeLock(handle, volNo);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(eNOERROR);
    
} /* RM_Restart() */

#ifdef USE_LOG_COHERENCY_VOLUME
/*@================================
 * RM_RestartAndSyncBuffer()
 *================================*/
/*
 * Function: Four RM_RestartAndSyncBuffer(char *, Four)
 *
 * Description:
 *  Restart the storage system and sync buffers.
 *
 * Returns:
 *  error code
 */
Four RM_RestartAndSyncBuffer(
    Four handle,
    char 			*logDevList,     	/* In device name for log volume */		
    Four 			volNoToSync)     	/* data volume number to sync buffer */ 
{
    Four 			e;                     	/* error code */
    Four 			dataVolNos[MAXNUMOFVOLS];
    LockMode 			prevLock[MAXNUMOFVOLS];
    Boolean 			prevMounted[MAXNUMOFVOLS];
    Four 			i, j;
    Four 			compare;
    Four 			volNo;                 	/* volume no */
    Two  			nMountedDataVolumes;
    char 			*mountedDataVolumeList;
    char 			*ptr, *ptr_end;
    Four 			nDevicesInDataVolume;
    rm_LogVolumeInfo_t 		logVolumeInfo;
    rm_CoherencyHeaderPage 	coherencyHeaderPage;
    rm_CoherencyPageInfosPage 	coherencyPageInfosPage;

    TR_PRINT(TR_RM, TR1, ("RM_Restart(handle, logDevList)", logDevList));

    if (logDevList == NULL || !strcmp(logDevList, "")) return(eNOERROR);
   
    if(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo == NO_LOG_VOLUME) 
    {
	e = rm_MountVolume(handle, logDevList, &volNo);
	if (e < eNOERROR) ERR(handle, e);

	e = rm_ReadLogVolumeInfo(handle, volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo);
	if (e < eNOERROR) ERR(handle, e);
	
	if (RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onPrepareFlag == FALSE) RM_INIT_ALLOC_POSITION_OF_LOG_VOLUME(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 

	RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo = volNo;
 

	/*
	 * Initialize the recovery manager.
	 */
	e = rm_InitLogTable(handle);
	if (e < eNOERROR) ERR(handle, e);

	/* Initialize coherency informaiont for log volume */
	e = BfM_InitCoherencyInfo(handle, volNo);
	if (e < eNOERROR) ERR(handle, e);
    }
    else
	volNo = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo;
        
#ifdef DBLOCK
    /* 
     *  Acquire volume lock for log volume
     *  Note!! restart operation must be performed by only one process, so we use exclusive lock
     */
    e = RDsM_GetVolumeLock(handle, volNo, L_X, FALSE);
    //e = RDsM_GetVolumeLock(handle, volNo, L_X, TRUE);
    if (e == eVOLUMELOCKBLOCK)
    {
		return eNOERROR;
    }
    else if (e < eNOERROR) ERR(handle, e);
#endif

    /* read the log volume information : rm_LogVolumeInfo의 내용을 보존하기 위해 임시로 logVolumeInfo에 데이타를 읽는다. 
       만약 rm_LogVolumeInfo에 내용을 읽는다면, onCommitFlag가 FALSE인 경우, 이전에 사용한 내용이 rm_LogVolumeInfo에 남아 
       있어, transaction commit시, 오동작을 하게 될 가능성이 생기게 된다. */

    /* Read log volume information with coherency information. */
    e = rm_ReadLogVolumeInfoWithCoherencyPage(handle, volNo, &logVolumeInfo, &coherencyHeaderPage, &coherencyPageInfosPage);
    if (e < eNOERROR) ERR(handle, e);

    if (logVolumeInfo.onCommitFlag) 
    {
#ifdef VERBOSE
        printf("Start Restart Operation\n");
	printf("Mount log volume %ld\n", volNo);
#endif

	e = RDsM_ReleaseVolumeLock(handle, volNo);  /* release log volume lock, to avoid deadlock sequence */
	if (e < eNOERROR) ERR(handle, e);

        if ((mountedDataVolumeList = malloc(logVolumeInfo.mountedDataVolumeListSize)) == NULL)
            ERR(handle, eMEMORYALLOCERR);
	
	RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.firstPageNoForMountedDataVolumeList = logVolumeInfo.firstPageNoForMountedDataVolumeList;
	RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.mountedDataVolumeListSize           = logVolumeInfo.mountedDataVolumeListSize;
        
	e = rm_LoadDataVolumeList(handle, mountedDataVolumeList);
        if (e < eNOERROR) {
            free(mountedDataVolumeList);
            ERR(handle, e);
        }
        
        /* Mount the data volumes which were being mounted. */
        ptr = mountedDataVolumeList;
        nMountedDataVolumes = 0;
        while (*ptr != '\0') {
            ptr_end = strchr(ptr, '\n');
            assert(ptr_end != NULL);
            *ptr_end = '\0';    /* convert newline character into null character */
            
	    e = rm_GetVolNo(handle, ptr, &dataVolNos[nMountedDataVolumes]);
	    if(e == eVOLNOTMOUNTED_RDSM)
	    {
#ifdef VERBOSE
		printf("Mount data volumes : %s\n", ptr);
#endif
		prevMounted[nMountedDataVolumes] = FALSE;
		e = rm_MountVolume(handle, ptr, &dataVolNos[nMountedDataVolumes]);
		if (e < eNOERROR) 
		{
		    free(mountedDataVolumeList);
		    ERR(handle, e);
		}
	    }
	    else if(e < eNOERROR)
	    {
		free(mountedDataVolumeList);
		ERR(handle, e);
	    }
	    else
	    {
#ifdef VERBOSE
		printf("Using mounted data volumes : %s\n", ptr);
#endif
		prevMounted[nMountedDataVolumes] = TRUE;
	    }

            /* Note!! we must skip log volume and coherency volume */
#ifdef USE_COHERENCY_VOLUME
	    if (volNo != dataVolNos[nMountedDataVolumes] && !BfM_IsCoherencyVolume(handle, dataVolNos[nMountedDataVolumes])) 
#else
	    if (volNo != dataVolNos[nMountedDataVolumes])	
#endif
	    {
		nMountedDataVolumes ++;
	    }

	    ptr = ptr_end + 1;  /* skip the new line, which indicates end of device list for a volume */
        }    
	free(mountedDataVolumeList);

#ifdef DBLOCK
	/* sort dataVolNos using simple bubble sort */
	/* volume lock으로 인한 dead lock을 피하기 위해, 데이타 볼륨에 순서를 두어 그 순서 대로 lock을 잡는다.
	   temporary volume, data volume, log volume순이며, 같은 종류에서는 volNo순이다 */
	for(i = 0; i < nMountedDataVolumes - 1; i++)
	{
	    for(j = i + 1; j < nMountedDataVolumes; j++)
	    {
		if(IS_TEMP_VOLUME(dataVolNos[i]) && !IS_TEMP_VOLUME(dataVolNos[j]))
		    compare = -1;
		else if(!IS_TEMP_VOLUME(dataVolNos[i]) && IS_TEMP_VOLUME(dataVolNos[j]))
		    compare = 1;
                else if (RM_IsLogVolume(handle, dataVolNos[i]) && !RM_IsLogVolume(dataVolNos[j]))
                    compare = 1;
                else if (!RM_IsLogVolume(handle, dataVolNos[i]) && RM_IsLogVolume(dataVolNos[j]))
                    compare = -1;
		else
		    compare = dataVolNos[i] - dataVolNos[j];
		if(compare > 0) /* means dataVolNos[i] > dataVolNos[j] */
		{
		    Four    tmp1;
		    Boolean tmp2;

		    tmp1 = dataVolNos[i];
		    dataVolNos[i] = dataVolNos[j];
		    dataVolNos[j] = tmp1;

		    tmp2 = prevMounted[i];
		    prevMounted[i] = prevMounted[j];
		    prevMounted[j] = tmp2;
		}
	    }
	}

	for(i = 0; i < nMountedDataVolumes; i++)
	{
	    prevLock[i] = RDsM_GetVolumeLockMode(handle, dataVolNos[i]);

            /* acquire lock for data volume */
	    e = RDsM_GetVolumeLock(handle, dataVolNos[i], L_X, FALSE);
	    if (e < eNOERROR) ERR(handle, e);
	}

	/* lastly, lock log volume */
        e = RDsM_GetVolumeLock(handle, volNo, L_X, FALSE);
        if (e < eNOERROR) ERR(handle, e);
#endif
        
	/* read the log volume information with coherency information */
    e = rm_ReadLogVolumeInfoWithCoherencyPage(handle, volNo, &logVolumeInfo, &coherencyHeaderPage, &coherencyPageInfosPage);
	if (e < eNOERROR) ERR(handle, e);

	if (logVolumeInfo.onCommitFlag) 
	{
#ifdef VERBOSE
	    printf("Start Redo operation\n");
#endif
    	    e = rm_ReadLogVolumeInfo(handle, volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 
    	    if (e < eNOERROR) ERR(handle, e);

            /* Redo the transaction operations. */
	    e = rm_Redo(handle);
	    if (e < eNOERROR) ERR(handle, e);
#ifdef VERBOSE
    	    printf("Done Redo operation\n");
#endif
	    /*
	     * Clear the flag so that there is no committed transaction
	     * which aren't saved.
	     */
	    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onCommitFlag = FALSE;
	    RM_INIT_ALLOC_POSITION_OF_LOG_VOLUME(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 
	    e = rm_WriteLogVolumeInfo(handle, volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo);
	    if (e < eNOERROR) ERR(handle, e);
	}
	else
	{
#ifdef VERBOSE
    	    printf("Skip Redo operation\n");
#endif
	}
        
        /* Dismount the data volumes which were being mounted. */
        for (i = 0; i < nMountedDataVolumes; i++) 
	{
#ifdef DBLOCK
	    if(prevLock[i] == L_NL)	
	    {				
		/* release lock for data volume */
		e = RDsM_ReleaseVolumeLock(handle, dataVolNos[i]);
		if (e < eNOERROR) ERR(handle, e);
	    }
#endif
	    if(!prevMounted[i])
	    {
#ifdef VERBOSE
		printf("Dismount data volumes : %ld\n", dataVolNos[i]);
#endif
		e = RDsM_Dismount(handle, dataVolNos[i]);
		if (e < eNOERROR) ERR(handle, e);
	    }
        }
    }

    /* 
     * Sync buffer using log volume. 
     */
    e = BfM_SyncBufferUsingLogVolume(handle, volNoToSync, (char* )&coherencyHeaderPage, (char* )&coherencyPageInfosPage);
    if (e < eNOERROR) ERR(handle, e);

#ifdef DBLOCK
    /* 
     * Release volume lock for log volume
     */
    e = RDsM_ReleaseVolumeLock(handle, volNo);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(eNOERROR);
    
} /* RM_Restart() */
#endif

Four rm_MountVolume(
    Four handle,
    char                *devList,                                       /* IN device list for volume */
    Four                *volId)                                         /* OUT volume id */
{
    Four                e;                                              
    Four                nDevices;                                       
    Four                len;                                            
    char                *ptr, *ptr_tmp, *ptr_tmp1, *ptr_tmp2, *p;
    char                **arrayOfPtrToDevice;
    Four                i;                                              

    len = strlen(devList) + 1;
    
    if ((ptr = malloc(len)) == NULL) ERR(handle, eMEMORYALLOCERR);
    p = ptr;
    strcpy(ptr, devList);

    /* delete the trailing colons */
    for (i = len-2; i >= 0; i--)
#ifndef WIN32
        if (ptr[i] != ':' && ptr[i] !=';') break;
#else
        if (ptr[i] != ';') break;
#endif
    ptr[i+1] = '\0';

    if (*ptr == '\0') ERR(handle, eBADPARAMETER);
    
    /* get the # of devices */
    for (nDevices = 1, i = 0; ptr[i]; i++)
#ifndef WIN32
        if (ptr[i] == ':' || ptr[i] == ';') nDevices++;
#else
        if (ptr[i] == ';') nDevices++;
#endif

    if ((ptr_tmp = realloc(ptr, len + nDevices*sizeof(char*))) == NULL) {
        free(p);
        ERR(handle, eMEMORYALLOCERR);
    }
    else
	p = ptr_tmp;

    arrayOfPtrToDevice = (char**)ptr_tmp;
    ptr = ptr_tmp + nDevices*sizeof(char*);
    memmove(ptr, ptr_tmp, len);
    
    /* set the device list */
    for (i = 0; i < nDevices; i++) {
        arrayOfPtrToDevice[i] = ptr;
#ifndef WIN32
        ptr_tmp1 = strchr(ptr,':');
        ptr_tmp2 = strchr(ptr,';');

        if (ptr_tmp1 == NULL && ptr_tmp2 == NULL)
            ptr_tmp = NULL;
        else if (ptr_tmp1 == NULL && ptr_tmp2 != NULL)
            ptr_tmp = ptr_tmp2;
        else if (ptr_tmp1 != NULL && ptr_tmp2 == NULL)
            ptr_tmp = ptr_tmp1;
        else
            ptr_tmp = (ptr_tmp1 < ptr_tmp2) ? (ptr_tmp1) : (ptr_tmp2);
#else
        ptr_tmp = strchr(ptr, ';');
#endif
        if (ptr_tmp != NULL) {
            *ptr_tmp = '\0';
            ptr = ptr_tmp + 1;
        }
    }

    e = RDsM_Mount(handle, nDevices, arrayOfPtrToDevice, volId);
    if (e < eNOERROR) ERR(handle, e);

    free(p);

    return(eNOERROR);
    
} /* rm_MountVolume() */

Four rm_GetVolNo(
    Four handle,
    char                *devList,                                       /* IN device list for volume */
    Four                *volId)                                         /* OUT volume id */
{
    Four                e;                                              
    Four                nDevices;                                       
    Four                len;                                            
    char                *ptr, *ptr_tmp, *ptr_tmp1, *ptr_tmp2, *p;
    char                **arrayOfPtrToDevice;
    Four                i;                                              

    len = strlen(devList) + 1;
    
    if ((ptr = malloc(len)) == NULL) ERR(handle, eMEMORYALLOCERR);
    p = ptr;
    strcpy(ptr, devList);

    /* delete the trailing colons */
    for (i = len-2; i >= 0; i--)
#ifndef WIN32
        if (ptr[i] != ':' && ptr[i] !=';') break;
#else
        if (ptr[i] != ';') break;
#endif
    ptr[i+1] = '\0';

    if (*ptr == '\0') ERR(handle, eBADPARAMETER);
    
    /* get the # of devices */
    for (nDevices = 1, i = 0; ptr[i]; i++)
#ifndef WIN32
        if (ptr[i] == ':' || ptr[i] == ';') nDevices++;
#else
        if (ptr[i] == ';') nDevices++;
#endif

    if ((ptr_tmp = realloc(ptr, len + nDevices*sizeof(char*))) == NULL) {
        free(p);
        ERR(handle, eMEMORYALLOCERR);
    }
    else
	p = ptr_tmp;

    arrayOfPtrToDevice = (char**)ptr_tmp;
    ptr = ptr_tmp + nDevices*sizeof(char*);
    memmove(ptr, ptr_tmp, len);
    
    /* set the device list */
    for (i = 0; i < nDevices; i++) {
        arrayOfPtrToDevice[i] = ptr;
#ifndef WIN32
        ptr_tmp1 = strchr(ptr,':');
        ptr_tmp2 = strchr(ptr,';');

        if (ptr_tmp1 == NULL && ptr_tmp2 == NULL)
            ptr_tmp = NULL;
        else if (ptr_tmp1 == NULL && ptr_tmp2 != NULL)
            ptr_tmp = ptr_tmp2;
        else if (ptr_tmp1 != NULL && ptr_tmp2 == NULL)
            ptr_tmp = ptr_tmp1;
        else
            ptr_tmp = (ptr_tmp1 < ptr_tmp2) ? (ptr_tmp1) : (ptr_tmp2);
#else
        ptr_tmp = strchr(ptr, ';');
#endif
        if (ptr_tmp != NULL) {
            *ptr_tmp = '\0';
            ptr = ptr_tmp + 1;
        }
    }

    e = RDsM_GetVolNo(handle, arrayOfPtrToDevice[0], volId);
    if (e < eNOERROR) ERR(handle, e);

    free(p);

    return(eNOERROR);
}
