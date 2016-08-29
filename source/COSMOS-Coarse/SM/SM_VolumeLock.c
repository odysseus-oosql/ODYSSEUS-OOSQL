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
#ifdef DBLOCK
/*
 * Module: SM_VolumeLock.c
 *
 * Description:
 *  SM level volume lock interface
 *
 * Exports:
 *  SM_GetVolumeLock
 *  SM_ReleaseVolumeLock
 *  SM_ReleaseAllVolumeLock
 *  SM_SetUseShareLockFlag
 *  SM_CheckVolumeLock
 */

#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "RM.h"
#include "BfM.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

extern CfgParams_T sm_cfgParams;


Four SM_GetVolumeLock(
    Four handle,
    Four           volNo,               /* IN  ID of volume which will be locked */
    LockMode       lockMode)            /* IN  locking mode */
{
    Four	    e;
    LockMode	    prevLockMode;
    Two 	    nMountedVols;
    Four	    mountedVolNos[MAXNUMOFVOLS];
    Four	    i;

    TR_PRINT(TR_SM, TR1,("SM_GetVolumeLock(handle)"));
    
    prevLockMode = RDsM_GetVolumeLockMode(handle, volNo);
    if(prevLockMode == lockMode)		/* 이전에 걸린 lock이 다시 걸리는 경우에는, 아무것도 하지 않는다. */
                                                /* If the lock is already acruired, do nothing. */
	return eNOERROR;
    else if(prevLockMode == L_X && (lockMode == L_S || lockMode == L_IS || lockMode == L_X || lockMode == L_IX  || lockMode == L_SIX))
	return eNOERROR;			/* compatible lock이 이미 걸려 있단면, 아무것도 하지 않는다. */
                                                /* If the lock mode is compatible with previous one, do nothing. */
    else if(prevLockMode == L_S && (lockMode == L_S || lockMode == L_IS))
	return eNOERROR;			/* compatible lock이 이미 걸려 있단면, 아무것도 하지 않는다. */
                                                /* If the lock mode is compatible with previous one, do nothing. */
    else if(!RDsM_GetUseShareLockFlag(handle) && (lockMode == L_S || lockMode == L_IS))
	return eNOERROR;			/* shared lock을 걸지 않는 모드에서, shared lock을 걸고자 하면, 아무것도 하지 않는다. */
                                                /* If the lock mode is a shared lock in no-shared-lock-mode, do nothing. */

    /* 주어진 volume에 lock을 건다. */
    /* lock을 걸때는 순서에 주의 한다. 모든 lock은 순서대로 걸리며, 만약 순서를 무시한 lock이 걸릴 경우에는,
       이전 순서에 강제적으로 lock을 부여한다. 이는 volume lock들에 의한 deadlock이 발생하지 않게 하기 위함이다. */
    /* Acquire the lock on the given volume. */
    /* When acquire locks, pay attention to order of locks. All locks have to be acquired  in a certain order. It is to avoid deadlock. 
       If try acquiring a lock ignoring this order we acquire all locks that have a high priority in the order focibly. */ 
    e = RDsM_GetAllMountedVolNos(handle, mountedVolNos);
    if(e < eNOERROR) ERR(handle, e);
    nMountedVols = e;

    if (prevLockMode == L_S && (lockMode == L_X || lockMode == L_IX || lockMode == L_SIX))
    {
        for (i = nMountedVols-1; i >= 0; i--)
        {
            if (RDsM_GetVolumeLockMode(handle, mountedVolNos[i]) == L_S) 
	    {
                e = RDsM_ReleaseVolumeLock(handle, mountedVolNos[i]);
                if(e < eNOERROR) ERR(handle, e);
            }
        }
    }

    if((lockMode == L_X || lockMode == L_IX || lockMode == L_SIX ) && !IS_TEMP_VOLUME(volNo))
    {
	if(sm_cfgParams.useDeadlockAvoidance)
	{
	    /* log volume과 모든 데이타 볼륨에 대하여, L_X lock을 부여한다. */
            /* Acquire the lock with L_X mode on the log volume and all data volumes. */
	    for(i = 0; i < nMountedVols; i++)
	    {
#ifdef USE_COHERENCY_VOLUME
		if(!RM_IsLogVolume(handle, mountedVolNos[i]) && !BfM_IsCoherencyVolume(handle, mountedVolNos[i]))
#else
		if(!RM_IsLogVolume(handle, mountedVolNos[i]))
#endif
		{
		    prevLockMode = RDsM_GetVolumeLockMode(handle, mountedVolNos[i]);

		    e = RDsM_GetVolumeLock(handle, mountedVolNos[i], L_X, FALSE);
		    if(e < eNOERROR) ERR(handle, e);

		    /* 만약 주어진 볼륨에 처음 lock이 걸리거나, lock이 upgrade 되는 것이라면, Coherency volume을 사용한 
		     * buffer sync를 시도한다
		     * temporary volume에 대해서는 buffer sync를 하지 않는다. 
		     * 왜냐하면, temporary volume의 내용은 각 transaction간에 공유되지 않기 때문이다. 
		     */
                    /* If the lock on the given volume is acquired for the first time or the lock is upgraded, 
		     * try to sync buffer using coherency volume. 
		     * Don't try to sync buffer on the temporary volume, since pages of the temporary volume are not shared 
		     *  among transactions. 
		     */
		    if((prevLockMode == L_NL || prevLockMode == L_S) && !IS_TEMP_VOLUME(mountedVolNos[i]))  
		    {
			/*
			 * Check and do restart if restart information exists.
			 * single process 환경에서는 이때 restart를 할 필요가 없으나, multi process에서는 다른 process가 만들어 놓은
			 * restart information을 즉시 처리할 필요성이 있다.
			 * buffer를 sync하기 전에는 반드시, disk에 이전 process의 내용이 반영되어 있음을 보장해야 한다.
			 * 만약 commit시 fail로 disk에 내용이 반영되어 있지 않다면, restart를 통해, 이를 보장한다.
			 */
                        /*
			 * Check and do restart if restart information exists.
			 * In single process environment, we need not to restart in this case. But, 
			 * in multi process environment, we have to handle the restart information that is created by other process.
			 * Before buffer-synch, the contents of the previous processes must be flushed to the disk.
			 * If the contents are not flushed completely owing to system failure during commit processing, 
			 * we guarante that throught the restart. 
			 */
			/* RM_Restart는 이미 획득한 lock에 대해서는 L_X lock으로 upgrade하여 보존하므로, lock이 없어지는
			   것은 신경 안써도 된다 */
			/* RM_Restart upgrades it's mode to L_X about the already acquired lock and then, preserves it.
                           So never mind that the lock is released. */
#ifdef USE_LOG_COHERENCY_VOLUME
			e = RM_RestartAndSyncBuffer(handle, sm_cfgParams.logVolumeDeviceList, mountedVolNos[i]);
#else
			e = RM_Restart(handle, sm_cfgParams.logVolumeDeviceList);
#endif
			if (e < 0) ERR(handle, e);
#ifdef USE_COHERENCY_VOLUME
			e = BfM_SyncBufferUsingCoherencyVolume(handle, mountedVolNos[i]);
			if(e < eNOERROR) ERR(handle, e);
#endif
		    }
		}
	    }
	}
	else
	{
	    /* 주어진 볼륨에 대해서만 lock을 잡는다. */
            /* Acquire the lock on only the given volume */
#ifdef USE_COHERENCY_VOLUME
	    if(!RM_IsLogVolume(handle, volNo) && !BfM_IsCoherencyVolume(handle, volNo))
#else
	    if(!RM_IsLogVolume(handle, volNo))
#endif
	    {
		prevLockMode = RDsM_GetVolumeLockMode(handle, volNo);

		e = RDsM_GetVolumeLock(handle, volNo, L_X, FALSE);
		if(e < eNOERROR) ERR(handle, e);

		if((prevLockMode == L_NL || prevLockMode == L_S) && !IS_TEMP_VOLUME(volNo))  
		{
#ifdef USE_LOG_COHERENCY_VOLUME
		    e = RM_RestartAndSyncBuffer(handle, sm_cfgParams.logVolumeDeviceList, volNo);
#else
		    e = RM_Restart(handle, sm_cfgParams.logVolumeDeviceList);
#endif
		    if (e < 0) ERR(handle, e);
#ifdef USE_COHERENCY_VOLUME
		    e = BfM_SyncBufferUsingCoherencyVolume(handle, volNo);
		    if(e < eNOERROR) ERR(handle, e);
#endif
		}
	    }
	}
        e = SM_GetLogVolumeLock(handle);
        if (e < eNOERROR) ERR(handle, e);
    }
    else
    {
	if(sm_cfgParams.useDeadlockAvoidance)
	{
	    /* 주어진 볼륨과 주어진 볼륨 보다 이전 순위를 가지는 볼륨들에 대하여, 주어진 모드의 lock을 부여한다. */
            /* Acquire the lock with the given lock mode on the given volume and volumes that have higher priority. */ 
	    for(i = 0; i < nMountedVols; i++)
	    {
#ifdef USE_COHERENCY_VOLUME
		if(!RM_IsLogVolume(handle, mountedVolNos[i]) && !BfM_IsCoherencyVolume(handle, mountedVolNos[i]))
#else
		if(!RM_IsLogVolume(handle, mountedVolNos[i]))
#endif
		{
		    prevLockMode = RDsM_GetVolumeLockMode(handle, mountedVolNos[i]);

		    e = RDsM_GetVolumeLock(handle, mountedVolNos[i], lockMode, FALSE);
		    if(e < eNOERROR) ERR(handle, e);

		    /* 만약 주어진 볼륨에 처음 lock이 걸리거나, lock이 upgrade 되는 것이라면, 
		     * Coherency volume을 사용한 buffer sync를 시도한다 
		     * temporary volume에 대해서는 buffer sync를 하지 않는다. 왜냐하면, temporary volume의 내용은 
		     * 각 transaction간에 공유되지 않기 때문이다. 
		     */
                    /* If the lock on the given volume is acquired for the first time or the lock is upgraded, 
		     * try to sync buffer using coherency volume. 
		     * Don't try to sync buffer on the temporary volume, since pages of the temporary volume are not shared 
		     * among transactions. 
		     */
		    if((prevLockMode == L_NL || (prevLockMode == L_S && (lockMode == L_X || lockMode == L_IX || lockMode == L_SIX))) && 
			!IS_TEMP_VOLUME(mountedVolNos[i]))  
		    {
			/*
			 * Check and do restart if restart information exists.
			 * single process 환경에서는 이때 restart를 할 필요가 없으나, multi process에서는 다른 process가 만들어 놓은
			 * restart information을 즉시 처리할 필요성이 있다.
			 * buffer를 sync하기 전에는 반드시, disk에 이전 process의 내용이 반영되어 있음을 보장해야 한다.
			 * 만약 commit시 fail로 disk에 내용이 반영되어 있지 않다면, restart를 통해, 이를 보장한다.
			 */
                        /*
			 * Check and do restart if restart information exists.
			 * In single process environment, we need not to restart in this case. But, 
			 * in multi process environment, we have to handle the restart information that is created by other process.
			 * Before buffer-synch, the contents of the previous processes must be flushed to the disk.
			 * If the contents are not flushed completely owing to system failure during commit processing, 
			 * we guarante that throught the restart. 
			 */
			/* RM_Restart는 이미 획득한 lock에 대해서는 L_X lock으로 upgrade하여 보존하므로, lock이 없어지는
			   것은 신경 안써도 된다 */
			/* RM_Restart upgrades it's mode to L_X about the already acquired lock and then, preserves it.
                           So never mind that the lock is released. */
#ifdef USE_LOG_COHERENCY_VOLUME		
			e = RM_RestartAndSyncBuffer(handle, sm_cfgParams.logVolumeDeviceList, mountedVolNos[i]);
#else
			e = RM_Restart(handle, sm_cfgParams.logVolumeDeviceList);
#endif
			if (e < 0) ERR(handle, e);
#ifdef USE_COHERENCY_VOLUME
			e = BfM_SyncBufferUsingCoherencyVolume(handle, mountedVolNos[i]);
			if(e < eNOERROR) ERR(handle, e);
#endif
		    }
		}
		if (mountedVolNos[i] == volNo) break; 
	    }
	}
	else
	{
	    /* 주어진 볼륨에 대해서만 lock을 잡는다. */
            /* Acquire only the lock on given volume. */
#ifdef USE_COHERENCY_VOLUME
	    if(!RM_IsLogVolume(handle, volNo) && !BfM_IsCoherencyVolume(handle, volNo))
#else
	    if(!RM_IsLogVolume(handle, volNo))
#endif
	    {
		prevLockMode = RDsM_GetVolumeLockMode(handle, volNo);

		e = RDsM_GetVolumeLock(handle, volNo, lockMode, FALSE);
		if(e < eNOERROR) ERR(handle, e);

		if((prevLockMode == L_NL || prevLockMode == L_S) && !IS_TEMP_VOLUME(volNo))  
		{
#ifdef USE_LOG_COHERENCY_VOLUME
		    e = RM_RestartAndSyncBuffer(handle, sm_cfgParams.logVolumeDeviceList, volNo);
#else
		    e = RM_Restart(handle, sm_cfgParams.logVolumeDeviceList);
		    if (e < 0) ERR(handle, e);
#endif
#ifdef USE_COHERENCY_VOLUME
		    e = BfM_SyncBufferUsingCoherencyVolume(handle, volNo);
		    if(e < eNOERROR) ERR(handle, e);
#endif
		}
	    }
	}
    }

    return eNOERROR;
}

Four SM_ReleaseVolumeLock(
    Four handle,
    Four           volNo)               /* IN  ID of volume which will be locked */
{
    Four e;

    TR_PRINT(TR_SM, TR1,("SM_ReleaseVolumeLock(handle)"));

    e = RDsM_ReleaseVolumeLock(handle, volNo);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four SM_ReleaseAllVolumeLock(Four handle)
{
    Four e;

    TR_PRINT(TR_SM, TR1,("SM_ReleaseAllVolumeLock(handle)"));

    e = RDsM_ReleaseAllVolumeLock(handle);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four SM_SetUseShareLockFlag(
    Four handle,
    Boolean         useSharedLockFlag)  /* IN  flag which indicates shared lock will be used or not */
{
    Four e;

    TR_PRINT(TR_SM, TR1,("SM_SetUseShareLockFlag(handle)"));

    e = RDsM_SetUseShareLockFlag(handle, useSharedLockFlag);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four SM_CheckVolumeLock(
    Four handle,
    Four            volNo,              /* IN  ID of volume which will be locked */
    LockMode        lockMode)           /* IN  locking mode */
{
    Four e;

    TR_PRINT(TR_SM, TR1,("SM_CheckVolumeLock(handle)"));

    e = RDsM_CheckVolumeLock(handle, volNo, lockMode);
    if(e < eNOERROR) ERR(handle, e);

    return e;
}

Four SM_GetLogVolumeLock(Four handle)
{
    Four e;

    TR_PRINT(TR_SM, TR1,("SM_GetLogVolumeLock(handle)"));

    e = RM_GetLogVolumeLock(handle);
    if(e < eNOERROR) ERR(handle, e);

    return e;
}

Four SM_ReleaseLogVolumeLock(Four handle)
{
    Four e;

    TR_PRINT(TR_SM, TR1,("SM_ReleaseLogVolumeLock(handle)"));

    e = RM_ReleaseLogVolumeLock(handle);
    if(e < eNOERROR) ERR(handle, e);

    return e;
}
#endif

