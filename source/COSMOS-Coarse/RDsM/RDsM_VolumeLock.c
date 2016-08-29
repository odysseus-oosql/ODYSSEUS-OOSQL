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
 * Module: RDsM_VolumeLock.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_GetVolumeLock(Four, LockMode)
 *  Four RDsM_ReleaseVolumeLock(Four)
 *  Four RDsM_ReleaseAllVolumeLock(void)
 *  Four RDsM_SetUseShareLockFlag(Boolean)
 */

#ifndef WIN32
#include <sys/types.h>
#include <fcntl.h>
#else
#include "windows.h"
#endif

#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "BfM.h"             
#include "Util.h"
#include "RM_Internal.h"     
#include "perThreadDS.h"
#include "perProcessDS.h"
#include <stdlib.h>		


/*
 * Internal Function Definition
 */
Four rdsm_GetVolumeLock(Four, Four, LockMode, Boolean);

/*@================================
 * RDsM_GetVolumeLock()
 *================================*/
/*
 * Function: Four RDsM_GetVolumeLock(Four, LockMode, Boolean)
 *
 * Description:
 *  Acquire lock for given volume
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  none
 */
Four RDsM_GetVolumeLock(
    Four 	   			handle,
    Four           		volNo,        /* IN  ID of volume which will be locked */
    LockMode       		lockMode,     /* IN  locking mode */
    Boolean        		failImmediate /* IN  return immediately if fails in acquiring lock */
)
{
    Four 	   			e;
    rm_LogVolumeInfo_t	logVolumeInfo;

   
    while (RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo != NO_LOG_VOLUME && SM_PER_THREAD_DS(handle).xactRunningFlag != FALSE) { 

        e = rm_ReadLogVolumeInfo(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &logVolumeInfo); 
        if (e < eNOERROR) ERR(handle, e);

        if (logVolumeInfo.onPrepareFlag != TRUE || GLOBALXACTID_CMP_EQ(RM_PER_THREAD_DS(handle).rm_globalXactID, 
	   &logVolumeInfo.globalXactID) == TRUE) {
	    break;
	}

        Util_Sleep(PREPARE_TIMEOUT_RETRY_WAIT);
    }

    e = rdsm_GetVolumeLock(handle, volNo, lockMode, failImmediate);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

Four rdsm_GetVolumeLock(
    Four 				handle,	
    Four                volNo,          /* IN  ID of volume which will be locked */
    LockMode            lockMode,       /* IN  locking mode */
    Boolean             failImmediate   /* IN  return immediately if fails in acquiring lock */
)
{
    Four                e;              /* error code */
    Two                 idx;            /* index variable */
    VolumeTable*        v;              /* pointer for an volume table entry */
    LockMode            volumeLockMode; /* lock mode for volume */
#ifndef WIN32
    struct flock        flock;          /* flock structure */
#else
    Four                fileSize;
    Four_Invariable     lockType;
    OVERLAPPED          overlapped;
#endif
    Four                i;
#ifdef COSMOS_MULTITHREAD
    VolumeLockTable*	vl;
#endif
	float				wait_time;


    /*
     *  O. Set volume lock mode
     */

    /* if given lock mode is none, just return */
    if (lockMode == L_NL) return(eNOERROR);

    /* set volumeLockMode */
    volumeLockMode = (lockMode == L_X || lockMode == L_IX || lockMode == L_SIX) ? L_X : L_S;

    /*
     *  I. Get volume table entry pointer
     */

    /* get the corresponding volume table entry via searching the volTable */
    for (idx = 0; idx < MAXNUMOFVOLS; idx++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[idx].volNo == volNo) break; 
    }
    if (idx >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);

    /*@ set v to point to the corresponding entry */
    v  = &(RDSM_PER_THREAD_DS(handle).volTable[idx]); 	
#ifdef COSMOS_MULTITHREAD
    vl = &(volLockTable[idx]);	
#endif

    /* check shared lock must be acquired */
    if (volumeLockMode == L_S && RDSM_PER_PROCESS_DS.rdsmUseSharedLock == FALSE) return(eNOERROR); 


    /* check there is already volume lock */
    if (v->lockMode == L_X || v->lockMode == volumeLockMode) return(eNOERROR);


    /* 만약 write lock이 걸린다면, volume information중에서, space allocation관련 값을 다시 읽어 들일 수 있도록 invalidation을 수행한다. */
    /* 이 값은 lock이 풀려 있는 동안, 얼마든지 바뀔수 있는 값이며, write lock을 건 이후, 반드시 사용하는 값이기 때문에, */
    /* 이 값을 새로 update해야 한다. */
    /* When write lock is acquired, invalidate volume informations about space allocation to read them again. */
    /* The volume informations about space allocation can be updated during the write lock is released and must be used after acquiring write lock,
       so it has to be updated. */
    if (volumeLockMode == L_X)
    {
	v->numOfFreeExts = NOT_ASSIGNED;
	v->firstFreeExt  = NOT_ASSIGNED;
    }

    /*
     *  II. Acquire volume lock
     */

#ifndef WIN32

#ifndef COSMOS_MULTITHREAD
    /* set flock */
    flock.l_type = (volumeLockMode == L_X) ? F_WRLCK : F_RDLCK;
    flock.l_start = 0;         /* from start of file */
    flock.l_whence = SEEK_SET; /* absolute offset */
    flock.l_len = 0;           /* extends to end of file */

    /* acquire lock for first device */
    for(i = 0; i < VOLUMELOCK_TIMEOUT_RETRY_NUMBER; i++)
    {
	if (fcntl(DEVINFO_ARRAY(v->devInfo)[0].devAddr, F_SETLK, &flock) == -1)
	{
	    if(failImmediate)
		ERR(handle, eVOLUMELOCKBLOCK);
	    Util_Sleep(VOLUMELOCK_TIMEOUT_RETRY_WAIT);
	}
	else 
	    break;
    }
    if(i == VOLUMELOCK_TIMEOUT_RETRY_NUMBER)
	ERR(handle, eVOLUMELOCKTIMEOUT);
#else

	for (i = 0; i < VOLUMELOCK_TIMEOUT_RETRY_NUMBER; i++)
    {
	// acquire read/write lock on the volume table for multi-thread concurrency
	switch (volumeLockMode) 
	{
	    case L_S:
	    	e = RWLOCK_TRYRDLOCK(&(vl->rwlock));
	     	break;
	    case L_X:
   			e = RWLOCK_TRYWRLOCK(&(vl->rwlock));
	   	break; 
	    default:
	    	break;
	}

    if (e == EBUSY) 
	{
	    if (failImmediate) {
			ERR(handle, eVOLUMELOCKBLOCK);
		}
	    else {
			srand( (unsigned int)time(NULL) * handle );
			wait_time = (rand() % 1000000) / 1000000.0;
			
			Util_Sleep(wait_time);
		}
    }
	else{ 
	    break;
	}
    }
    if(i == VOLUMELOCK_TIMEOUT_RETRY_NUMBER)
	ERR(handle, eVOLUMELOCKTIMEOUT);
#endif


#else
    /* set overlapped */
    GetFileSize(DEVINFO_ARRAY(v->devInfo)[0].devAddr, &fileSize);
    overlapped.hEvent = DEVINFO_ARRAY(v->devInfo)[0].devAddr;
    overlapped.Offset = fileSize - 1;
    overlapped.OffsetHigh = 0;

    if(volumeLockMode == L_X)
    {
	if(v->lockMode == L_S) /* if previous shared lock is already exist, then remove it */
	    UnlockFileEx(DEVINFO_ARRAY(v->devInfo)[0].devAddr, 0, 1, 0, &overlapped);
	lockType = LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK;
    }
    else
	lockType = LOCKFILE_FAIL_IMMEDIATELY;

    /* acquire lock for first device */
    for(i = 0; i < VOLUMELOCK_TIMEOUT_RETRY_NUMBER; i++)
    {
        if(LockFileEx(DEVINFO_ARRAY(v->devInfo)[0].devAddr, lockType, 0, 1, 0, &overlapped) == 0)
        {
            /* LockFileEx is not supported in Windows 95, 98, so we use another method.
               In Windows 95, 98, we cannot support shared lock. Therefore we ignore it,
               that is we only support L_X mode locking. */

            if(GetLastError() == ERROR_CALL_NOT_IMPLEMENTED)
            {
#ifndef NDEBUG
                printf("LockFileEx fails, use LockFile instead. In Windows 95, 98, this situation is normal.\n");
                if(volumeLockMode != L_X)
                    printf("Shared mode lock is ignored because LockFile doesn't support shared mode lock.\n");
#endif
                if(volumeLockMode == L_X && LockFile(DEVINFO_ARRAY(v->devInfo)[0].devAddr, fileSize - 1, 0, 1, 0) == 0)
                    Util_Sleep(VOLUMELOCK_TIMEOUT_RETRY_WAIT);
                else
                    break;
            }
            else
            {
		if(failImmediate)
		{
		    if(volumeLockMode == L_X && v->lockMode == L_S)
		    {
			/* 앞에서 unlock한 shared lock을 다시 건다 */
                        /* Re-acquire the shared lock which is released before */
			lockType = LOCKFILE_FAIL_IMMEDIATELY;
			LockFileEx(DEVINFO_ARRAY(v->devInfo)[0].devAddr, lockType, 0, 1, 0, &overlapped);
		    }
		    ERR(handle, eVOLUMELOCKBLOCK);
		}
                Util_Sleep(VOLUMELOCK_TIMEOUT_RETRY_WAIT);
            }
        }
        else
            break;
    }
    if(i == VOLUMELOCK_TIMEOUT_RETRY_NUMBER)
    {
        if(volumeLockMode == L_X && v->lockMode == L_S)
        {
            /* 앞에서 unlock한 shared lock을 다시 건다 */
            /* Re-acquire the shared lock which is released before */
            lockType = LOCKFILE_FAIL_IMMEDIATELY;
            LockFileEx(DEVINFO_ARRAY(v->devInfo)[0].devAddr, lockType, 0, 1, 0, &overlapped);
        }
        ERR(handle, eVOLUMELOCKTIMEOUT);
    }
#endif

#ifndef NDEBUG
    printf("Volume Lock for %ld (Mode = %ld) is acquired!!\n", volNo, lockMode);
#endif


    /*
     *  III. Set isLocked flag
     */

    v->lockMode = volumeLockMode;

    return(eNOERROR);

} /* RDsM_GetVolumeLock() */


/*@================================
 * RDsM_ReleaseVolumeLock()
 *================================*/
/*
 * Function: Four ReleaseVolumeLock(Four)
 *
 * Description:
 *  Release lock for given volume
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  none
 */
Four RDsM_ReleaseVolumeLock(
    Four handle,
    Four                volNo)        /* IN  ID of volume which will be locked */
{
    Four                e;            /* error code */
    Two                 i;            /* index variable */
    VolumeTable*        v;            /* pointer for an volume table entry */
#ifndef WIN32
    struct flock        flock;        /* flock structure */
#else
    Four                fileSize;
    OVERLAPPED          overlapped;
#endif
#ifdef COSMOS_MULTITHREAD
    VolumeLockTable*	vl;
#endif


    /*
     *  I. Get volume table entry pointer
     */

    /* get the corresponding volume table entry via searching the volTable */
    for (i = 0; i < MAXNUMOFVOLS; i++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[i].volNo == volNo) break; 
    }
    if (i >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);


    /*@ set v to point to the corresponding entry */
    v  = &(RDSM_PER_THREAD_DS(handle).volTable[i]); 
#ifdef COSMOS_MULTITHREAD
    vl = &(volLockTable[i]);	
#endif

    /* check there is volume lock */
    if (v->lockMode == L_NL) return(eNOERROR);


    /*
     *  II. Release volume lock
     */

#ifndef WIN32
#ifndef COSMOS_MULTITHREAD
    /* set operation */
    flock.l_type = F_UNLCK;
    flock.l_start = 0;         /* from start of file */
    flock.l_whence = SEEK_SET; /* absolute offset */
    flock.l_len = 0;           /* extends to end of file */

    /* release lock for first device */
    if (fcntl(DEVINFO_ARRAY(v->devInfo)[0].devAddr, F_SETLK, &flock) == -1)
        ERR(handle, eVOLUMELOCKRELEASE);
#else
    e = RWLOCK_UNLOCK(&(vl->rwlock));
    if (e != eNOERROR) ERR(handle, eVOLUMELOCKRELEASE);
#endif 

#else
    /* set overlapped */
    GetFileSize(DEVINFO_ARRAY(v->devInfo)[0].devAddr, &fileSize);
    overlapped.hEvent = DEVINFO_ARRAY(v->devInfo)[0].devAddr;
    overlapped.Offset = fileSize - 1;
    overlapped.OffsetHigh = 0;

    /* release lock for first device */
    if (UnlockFileEx(DEVINFO_ARRAY(v->devInfo)[0].devAddr, 0, 1, 0, &overlapped) == 0) {

        /* LockFileEx is not supported in Windows 95, 98, so we use another method.
           In Windows 95, 98, we cannot support shared lock. Therefore we ignore it,
           that is we only support L_X mode locking. */

        if(GetLastError() == ERROR_CALL_NOT_IMPLEMENTED) {
#ifndef NDEBUG
            printf("LockFileEx fails, use LockFile instead. In Windows 95, 98, this situation is normal.\n");
            if(v->lockMode != L_X)
                printf("Shared mode lock is ignored because LockFile doesn't support shared mode lock.\n");
#endif
            if(v->lockMode == L_X && UnlockFile(DEVINFO_ARRAY(v->devInfo)[0].devAddr, fileSize - 1, 0, 1, 0) == 0)
                ERR(handle, eVOLUMELOCKRELEASE);
        }
        else {
            ERR(handle, eVOLUMELOCKRELEASE);
        }
    }
#endif

#ifndef NDEBUG
    printf("Volume Lock for %ld (Mode = %ld) is released!!\n", volNo, v->lockMode);
#endif


    /*
     *  III. Reset isLocked flag
     */

    v->lockMode = L_NL;

    return(eNOERROR);

} /* RDsM_ReleaseVolumeLock() */


/*@================================
 * RDsM_ReleaseAllVolumeLock()
 *================================*/
/*
 * Function: Four RDsM_ReleaseAllVolumeLock(void)
 *
 * Description:
 *  Release all lock of mounted volumes
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  none
 */
Four RDsM_ReleaseAllVolumeLock(Four handle)
{
    Four         e;            /* error code */
    Two          i;            /* index variable */

    /* for each mounted volume, release volume lock */
    for (i = 0; i < MAXNUMOFVOLS; i++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[i].volNo != NOVOL) { 
            e = RDsM_ReleaseVolumeLock(handle, RDSM_PER_THREAD_DS(handle).volTable[i].volNo);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    return(eNOERROR);

} /* RDsM_ReleaseAllVolumeLock() */



/*@================================
 * RDsM_SetUseShareLockFlag()
 *================================*/
/*
 * Function: Four RDsM_SetUseShareLockFlag(Boolean)
 *
 * Description:
 *  Set useSharedLockFlag as given value
 *
 * Returns:
 *  error code
 *
 * Side Effects:
 *  none
 */
Four RDsM_SetUseShareLockFlag(
    Four 		handle,
    Boolean     useSharedLockFlag)  /* IN  flag which indicates shared lock will be used or not */
{
    /* Set useSharedLockFlag */
    RDSM_PER_PROCESS_DS.rdsmUseSharedLock = useSharedLockFlag; 

    return(eNOERROR);

} /* RDsM_SetUseShareLockFlag() */

Boolean RDsM_GetUseShareLockFlag(Four handle)
{
    /* Get useSharedLockFlag */
    return RDSM_PER_PROCESS_DS.rdsmUseSharedLock; 

} /* RDsM_GetUseShareLockFlag() */

Boolean RDsM_CheckVolumeLock(
    Four 	  		handle,
    Four          	volNo,          /* IN  ID of volume which will be locked */
    LockMode      	lockMode)       /* IN  locking mode */
{
    Two           	i;              /* index variable */
    VolumeTable*  	v;              /* pointer for an volume table entry */
    Boolean       	result = FALSE; /* result of check */

    /* get the corresponding volume table entry via searching the volTable */
    for (i = 0; RDSM_PER_THREAD_DS(handle).volTable[i].volNo != volNo; i++) 
        if (i >= MAXNUMOFVOLS) return FALSE;

    /*@ set v to point to the corresponding entry */
    v = &(RDSM_PER_THREAD_DS(handle).volTable[i]); 

    switch(lockMode) {

      case L_NL : result = TRUE;
                  break;

      case L_S  : if (v->lockMode != L_NL) result = TRUE;
                  break;

      case L_X  : if (v->lockMode == L_X) result = TRUE;
                  break;
    }

    return result;
}

LockMode RDsM_GetVolumeLockMode(
    Four	  handle, 
    Four          volNo           /* IN  ID of volume which will be locked */
)
{
    Two           i;              /* index variable */
    VolumeTable*  v;              /* pointer for an volume table entry */

    /* get the corresponding volume table entry via searching the volTable */
    for (i = 0; RDSM_PER_THREAD_DS(handle).volTable[i].volNo != volNo; i++) 
        if (i >= MAXNUMOFVOLS) return L_NL;

    /*@ set v to point to the corresponding entry */
    v = &(RDSM_PER_THREAD_DS(handle).volTable[i]); 

    return v->lockMode;
}

#endif /* DBLOCK */
