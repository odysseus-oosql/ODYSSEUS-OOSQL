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
 * Module: THM_ControlLock.c 
 * 
 * Description: 
 *
 * Exports: 
 *  Four THM_ControlFileLock(int, int)
 *  Four THM_CreateUnnamedSemaphore(cosmos_thread_sem_t* semID, unsigned int value)
 *  Four THM_DestroyUnnamedSemaphore(cosmos_thread_sem_t* semID)
 *  Four THM_SemWait(cosmos_thread_sem_t* semID)
 *  Four THM_SemPost(cosmos_thread_sem_t* semID)
 */

#if defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER)

#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include "common.h"
#include "THM_lock.h"

extern errno;

/* for mutex */
cosmos_thread_mutex_attr_t cosmos_thread_mutex_attr;
cosmos_thread_mutex_t 	   mutexForSharedMemoryAccess 	= {COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS};
cosmos_thread_mutex_t      mutexForPerProcessDSAccess 	= {COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS};
cosmos_thread_mutex_t      mutexForErrorLogFileAccess	= {COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS};

/* for rwlock */
cosmos_thread_rwlock_attr_t cosmos_thread_rwlock_attr;

/*@
 * Functions for file lock 
 */

/*
 * Function: Four THM_ControlFileLock(int, int)
 *
 * Description:
 *  Control file lock.
 *
 * Returns:
 */
/* Four THM_CriticalSection(Four handle, Four type, Four action) */
Four THM_CriticalSection(Four handle, Four type, Four action, FileDesc* fd) 
{
    Four e;
    pid_t my_pid;

    my_pid = getpid();

    switch(type) {

    case SHARED_MEMORY_ACCESS:
    	if (action == START_CRITICAL_SECTION) {
            e = THM_AcquireMutex(&mutexForSharedMemoryAccess);
            if (e < eNOERROR) ERR(handle, e);

            e = THM_ControlFileLock(F_SETLKW, F_WRLCK, fd); 
	    if (e < eNOERROR) ERR(handle, e);
        }
        else if (action == END_CRITICAL_SECTION) {
            e = THM_ReleaseMutex(&mutexForSharedMemoryAccess);
            if (e < eNOERROR) ERR(handle, e);
		
	    e = THM_ControlFileLock(F_SETLK, F_UNLCK, fd); 
	    if (e < eNOERROR) ERR(handle, e);
        }
	else {
	    ERR(handle, eBADPARAMETER);
	}

        break;
    case PER_PROCESS_DS_ACCESS:
        if (action == START_CRITICAL_SECTION) {
            e = THM_AcquireMutex(&mutexForPerProcessDSAccess);
            if (e < eNOERROR) ERR(handle, e);
        }
        else if (action == END_CRITICAL_SECTION) {
            e = THM_ReleaseMutex(&mutexForPerProcessDSAccess);
            if (e < eNOERROR) ERR(handle, e);
        }
        else {
	    ERR(handle, eBADPARAMETER);
	}

        break;
    case ERROR_LOG_FILE_ACCESS: 
        if (action == START_CRITICAL_SECTION) {
            e = THM_AcquireMutex(&mutexForErrorLogFileAccess);
            if (e < eNOERROR) ERR(handle, e);
        }
        else if (action == END_CRITICAL_SECTION) {
            e = THM_ReleaseMutex(&mutexForErrorLogFileAccess);
            if (e < eNOERROR) ERR(handle, e);
        }
        else {
	    ERR(handle, eBADPARAMETER);
	}

        break;
    default:
	ERR(handle, eBADPARAMETER);
    }

    return (eNOERROR);
}

/*
 * Function: Four THM_ControlFileLock(int, int)
 *
 * Description:
 *  Control file lock.
 *
 * Returns:
 */
Four THM_ControlFileLock(int cmd, int type, FileDesc* fd) 
{
    struct flock        lock;
    char		fileLockName[256];
    char*               shmkeyPtr;
    Four                shmkey;

    /* Create temporary lock file */
    if ((shmkeyPtr = (char* )getenv("COSMOS_SHARED_MEMORY_BUFFER_KEY")) == NULL) 
        shmkey = COSMOS_SHARED_MEMORY_BUFFER_KEY;
    else 
        shmkey = atol(shmkeyPtr);

    sprintf(fileLockName, "%s.%ld", COSMOS_BUFFER_FILE_LOCK_NAME, shmkey);
    if ((*fd = open(fileLockName, O_RDWR | O_CREAT, PERMS)) < 0) 
	return (eCREATEFILEFAILED_THM); 

    /* Initialize flock attribute */
    lock.l_type     = type;
    lock.l_start    = 0;            /* file lock */
    lock.l_whence   = SEEK_SET; 
    lock.l_len      = 0;

    /* Control the file lock */
    if (fcntl(*fd, cmd, &lock) < 0) { 
	switch (errno) {
	    case EAGAIN:	/* the file lock could not be acquired */
		return (eFILELOCKAGAIN_THM);
	    default:
		return (eFILELOCKUNKNOWN_THM);
	}
    }
    return (eNOERROR);
} /* THM_FileLock() */


/*@
 * Functions for semaphore
 */  

/*
 * Function: Four THM_CreateUnnamedSemaphore(cosmos_thread_sem_t*, unsigned int) 
 *
 * Description:
 *  Create new unnamed semaphore.
 *
 * Returns:
 *  error code
 */ 
Four THM_CreateUnnamedSemaphore(Four handle, cosmos_thread_sem_t* semID, unsigned int value)
{
    Four e;	/* error code */

    e = sem_init(semID, 1, value);

    /* error check */
    if (e < -1 ) {
        switch (errno) {
            case EACCES:
            /*  The named semaphore exists and the O_RDWR  permissions  are denied, or the named semaphore does not
                exist and permission to create the named semaphore is denied */
                return (eSEMCREATEACCES_THM);
            case EEXIST:
            /*  O_CREAT and  O_EXCL are set and  the  named  semaphore already exists */
                return (eSEMCREATEEXIST_THM);
            case EINTR:
            /*  The sem_open() function was interrupted by a  signal */
                return (eSEMCREATEINTR_THM);
            case EINVAL:
            /*  The sem_open() operation is not supported for the given name, or O_CREAT was set in oflag and value
                is greater than SEM_VALUE_MAX */
                return (eSEMCREATEINVAL_THM);
            case EMFILE:
            /*  The number of open semaphore descriptors  in  this process  exceeds   SEM_NSEMS_MAX, or the number of
                open file  descriptors  in  this  process  exceeds OPEN_MAX */
                return (eSEMCREATEMFILE_THM);
            case ENAMETOOLONG:
            /*  The length of name string exceeds PATH_MAX, or a pathname  component  is longer than NAME_MAX while
                _POSIX_NO_TRUNC is in effect */
                return (eSEMCREATENAMETOOLONG_THM);
            case ENFILE:
            /*  Too many semaphores are currently open in the system */
                return (eSEMCREATENFILE_THM);
            case ENOENT:
            /*  O_CREAT is not set and the named semaphore does not exist */
                return (eSEMCREATENOENT_THM);
            case ENOSPC:
            /*  There is insufficient space for  the  creation  of the new named semaphore */
                return (eSEMCREATENOSPC_THM);
            case ENOSYS:
            /*  The sem_open() function is not  supported  by the system */
                return (eSEMCREATENOSYS_THM);
            default:        /* unknown error */
                return (eSEMCREATEUNKNOWN_THM);
        }
    }
    else {
        return (eNOERROR);
    }
}

/*
 * Function: Four THM_DestroyUnnamedSemaphore(cosmos_thread_sem_t* ) 
 *
 * Description:
 *  Destroy unnamed semaphore.
 *
 * Returns:
 *  error code
 */ 
Four THM_DestroyUnnamedSemaphore(Four handle, cosmos_thread_sem_t* semID)
{
    Four e;	/* error code */

    e = sem_destroy(semID);

    /* error check */
    if (e != 0) {
        switch (errno) {
            case EACCES:    /* Permission is denied to  unlink  the  named  semaphore */
                return (eSEMDESTROYACCES_THM);
            case ENAMETOOLONG: /* The length of name string exceeds PATH_MAX, or a pathname component is longer than NAME_MAX */
                return (eSEMDESTROYNAMETOOLONG_THM);    /* while _POSIX_NO_TRUNC is in effect */
            case ENOENT:    /* The named semaphore does not exist */
                return (eSEMDESTROYENOENT_THM);
            case ENOSYS:    /* The sem_unlink() function is not supported by the system */
                return (eSEMDESTROYNOSYS_THM);
            default:        /* unknown error */
                return (eSEMDESTROYUNKNOWN_THM);
        }
    }
    else {
        return (eNOERROR);
    }
}

/*
 * Function: Four THM_SemWait(cosmos_thread_sem_t* ) 
 *
 * Description:
 *  Decrement the semaphore. (semaphore-V)
 *
 * Returns:
 *  error code
 */ 
Four THM_SemWait(Four handle, cosmos_thread_sem_t* semID)
{
    Four e;	/* error code */

    e = sem_wait(semID);

    /* error check */
    if (e != 0) {
        switch (errno) {
            case EINVAL:    /* sem points to an illegal address */
                return (eSEMWAITINVAL_THM);
            case EINTR:     /* A signal interrupted this function */
                return (eSEMWAITINTR_THM);
            default:        /* unknown error */
                return (eSEMWAITUNKNOWN_THM);
        }
    }
    else {
        return (eNOERROR);
    }
}

/*
 * Function: Four THM_SemPost(cosmos_thread_sem_t* ) 
 *
 * Description:
 *  Increment the semaphore. (semaphore-P)
 *
 * Returns:
 *  error code
 */ 
Four THM_SemPost(Four handle, cosmos_thread_sem_t* semID)
{
    Four e;	/* error code */

    e = sem_post(semID);

    /* error check */
    if (e != 0) {
        switch (errno) {
            case EINVAL:    /* sem points to an illegal address */
                return (eSEMPOSTINVAL_THM);
            default:        /* unknown error */
                return (eSEMPOSTUNKNOWN_THM);
        }
    }
    else {
        return (eNOERROR);
    }
}

/*@
 * Functions for mutex
 */

/*
 * Function: Four THM_CreateMutex(cosmos_thread_mutex*, Four option) 
 *
 * Description:
 *  create a mutex 
 *
 * Returns:
 *  error code
 */ 
Four THM_CreateMutex(cosmos_thread_mutex_t* mp, Four option)
{
#ifdef WIN32

#else
    cosmos_thread_mutex_attr_t 	ma;
    Four 			e;

	
    e = pthread_mutexattr_init(&ma);
    if (e != 0) return (eMUTEXCREATEUNKNOWN_THM);

    e = pthread_mutexattr_setpshared(&ma, option);
    if (e != 0) return (eMUTEXCREATEUNKNOWN_THM);
    
    e = pthread_mutex_init(mp, &ma);
    if (e != 0)  {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EBUSY:     /* the mutex cannot be reinitialized or modified because it still exists */
                return (eMUTEXCREATEBUSY_THM);
            case EINVAL:    /* the attribute value is invalid. the mutex has not been modified */
                return (eMUTEXCREATEINVAL_THM);
            case EFAULT:    /* the address for the mutex pointed at by mp is invalid */
                return (eMUTEXCREATEFAULT_THM);
            default:        /* unknown error */
                return (eMUTEXCREATEUNKNOWN_THM);
        }
    }

    e = pthread_mutexattr_destroy(&ma);
    if (e != 0) return (eMUTEXCREATEUNKNOWN_THM);
#endif

    return e;
}

/*
 * Function: Four THM_DestroyMutex(cosmos_thread_mutex* ) 
 *
 * Description:
 *  destroy a mutex 
 *
 * Returns:
 *  error code
 */ 
Four THM_DestroyMutex(cosmos_thread_mutex_t* mp)
{
#ifdef WIN32

#else
    Four e;

    e = pthread_mutex_destroy(mp);

    if (e != 0) {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EINVAL:    /* the value specified by mp doesn't refer to an initialized mutex object */
                return (eMUTEXDESTROYINVAL_THM);
            default:        /* unknown error */
                return (eMUTEXDESTROYUNKNOWN_THM);
        }
    }
#endif

    return e;
}

/*
 * Function: Four THM_AcquireMutex(cosmos_thread_mutex* ) 
 *
 * Description:
 *  acquire a mutex 
 *
 * Returns:
 *  error code
 */ 
Four THM_AcquireMutex(cosmos_thread_mutex_t *mp)
{
#ifdef WIN32

#else
    Four e;

    e = pthread_mutex_lock(mp);

    if (e != 0) {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EAGAIN:    /* the mutex could not be acquired because the maximum number of recursive locks for mutex has been exceeded */
                return (eMUTEXLOCKAGAIN_THM);
            case EDEADLK:   /* the current thread already owns the mutex */
                return (eMUTEXLOCKDEADLK_THM);
            default:        /* unknown error */
                return (eMUTEXLOCKUNKNOWN_THM);
        }
    }
#endif

    return e;
}

/*
 * Function: Four THM_ReleaseMutex(cosmos_thread_mutex* ) 
 *
 * Description:
 *  acquire a mutex 
 *
 * Returns:
 *  error code
 */ 
Four THM_ReleaseMutex(cosmos_thread_mutex_t *mp)
{
#ifdef WIN32

#else
    Four e;

    e = pthread_mutex_unlock(mp);

    if (e != 0) {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EPERM:     /* the current thread doesn't own the mutex */
                return (eMUTEXUNLOCKPERM_THM);
            default:        /* unknown error */
                return (eMUTEXUNLOCKUNKNOWN_THM);
        }
    }
#endif

    return e;
}

/*@
 * Functions for Read/Write Lock 
 */

/*
 * Function: Four THM_CreateRWLock(cosmos_thread_rwlock*, Four option) 
 *
 * Description:
 *  create a read/write lock 
 *
 * Returns:
 *  error code
 */ 
Four THM_CreateRWLock(cosmos_thread_rwlock_t* lp, Four option)
{
#ifdef WIN32

#else
    cosmos_thread_rwlock_attr_t la;
    Four 			e;

	
    e = pthread_rwlockattr_init(&la);
    if (e != 0) return (eMUTEXCREATEUNKNOWN_THM);

    e = pthread_rwlockattr_setpshared(&la, option);
    if (e != 0) return (eMUTEXCREATEUNKNOWN_THM);
    
    e = pthread_rwlock_init(lp, &la);
    if (e != 0)  {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EBUSY:     /* the mutex cannot be reinitialized or modified because it still exists */
                return (eRWLOCKCREATEBUSY_THM);
            case EINVAL:    /* the attribute value is invalid. the mutex has not been modified */
                return (eRWLOCKCREATEINVAL_THM);
            case EFAULT:    /* the address for the mutex pointed at by mp is invalid */
                return (eRWLOCKCREATEFAULT_THM);
            default:        /* unknown error */
                return (eRWLOCKCREATEUNKNOWN_THM);
        }
    }

    e = pthread_rwlockattr_destroy(&la);
    if (e != 0) return (eMUTEXCREATEUNKNOWN_THM);
#endif

    return e;
}

/*
 * Function: Four THM_DestroyRWLock(cosmos_thread_rwlock* ) 
 *
 * Description:
 *  destroy a read/write lock 
 *
 * Returns:
 *  error code
 */ 
Four THM_DestroyRWLock(cosmos_thread_rwlock_t* lp)
{
#ifdef WIN32

#else
    Four e;

    e = pthread_rwlock_destroy(lp);

    if (e != 0) {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EINVAL:    /* the value specified by mp doesn't refer to an initialized mutex object */
                return (eRWLOCKDESTROYINVAL_THM);
            default:        /* unknown error */
                return (eRWLOCKDESTROYUNKNOWN_THM);
        }
    }
#endif

    return e;
}

/*
 * Function: Four THM_AcquireReadLock(cosmos_thread_rwlock* ) 
 *
 * Description:
 *  acquire a read/write lock 
 *
 * Returns:
 *  error code
 */ 
Four THM_AcquireReadLock(cosmos_thread_rwlock_t *lp)
{
#ifdef WIN32

#else
    Four e;

    e = pthread_rwlock_rdlock(lp);

    if (e != 0) {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EAGAIN:    /* the mutex could not be acquired because the maximum number of recursive locks for mutex has been exceeded */
                return (eRWLOCKLOCKAGAIN_THM);
            case EDEADLK:   /* the current thread already owns the mutex */
                return (eRWLOCKLOCKDEADLK_THM);
            default:        /* unknown error */
                return (eRWLOCKLOCKUNKNOWN_THM);
        }
    }
#endif

    return e;
}

/*
 * Function: Four THM_AcquireWriteLock(cosmos_thread_rwlock* ) 
 *
 * Description:
 *  acquire a read/write lock 
 *
 * Returns:
 *  error code
 */ 
Four THM_AcquireWriteLock(cosmos_thread_rwlock_t *lp)
{
#ifdef WIN32

#else
    Four e;

    e = pthread_rwlock_wrlock(lp);

    if (e != 0) {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EAGAIN:    /* the mutex could not be acquired because the maximum number of recursive locks for mutex has been exceeded */
                return (eRWLOCKLOCKAGAIN_THM);
            case EDEADLK:   /* the current thread already owns the mutex */
                return (eRWLOCKLOCKDEADLK_THM);
            default:        /* unknown error */
                return (eRWLOCKLOCKUNKNOWN_THM);
        }
    }
#endif

    return e;
}


/*
 * Function: Four THM_ReleaseMutex(cosmos_thread_rwlock* ) 
 *
 * Description:
 *  acquire a mutex 
 *
 * Returns:
 *  error code
 */ 
Four THM_ReleaseRWLock(cosmos_thread_rwlock_t *lp)
{
#ifdef WIN32

#else
    Four e;

    e = pthread_rwlock_unlock(lp);

    if (e != 0) {
        /* error check */
        switch (errno) {
            case 0:         /* success */
                return (eNOERROR);
            case EPERM:     /* the current thread doesn't own the mutex */
                return (eMUTEXUNLOCKPERM_THM);
            default:        /* unknown error */
                return (eMUTEXUNLOCKUNKNOWN_THM);
        }
    }
#endif

    return e;
}

#endif /* defined(COSMOS_MULTITHREAD) || defined(USE_SHARED_MEMORY_BUFFER) */

