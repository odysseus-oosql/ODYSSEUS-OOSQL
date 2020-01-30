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
/*    Fine-Granule Locking Version                                            */
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
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef WIN32

#else
#include "common.h"
#include "error.h"
#include "trace.h"
#include "THM_cosmosThread.h"

#define COSMOS_FILE_LOCK_RETRY_TIMEOUT	0.01

extern errno;

cosmos_thread_mutex_t mutexForSharedMemoryAccess   = {COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS};
cosmos_thread_mutex_t mutexForPerThreadTableAccess = {COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS};
cosmos_thread_mutex_t mutexForErrorLogFileAccess   = {COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS};

Four cosmos_file_lock(Four, FileDesc);  /* old: cosmos_file_lock(FileDesc) */
Four cosmos_file_unlock(FileDesc);

#endif


/* *******************************************************
 * API for Critical Section                              *
 * *******************************************************/ 
Four THM_CriticalSectionForSharedMemory(
    Four        	handle,
    Four        	action,
    FileDesc*           lockFileDesc 
)
{

    Four        	e;
    FileDesc            fd;
    char        	fileLockName[1024];
    char                *shmkeyPtr;
    Four        	shmkey;


    /* get the shared memory key */
    if ((shmkeyPtr = getenv("COSMOS_SHARED_MEMORY_KEY")) == NULL)
        shmkey = COSMOS_SHARED_MEMORY_KEY;
    else
        shmkey = atoi(shmkeyPtr);

#if !defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2)
    sprintf(fileLockName, "%s.%ld.%lld", COSMOS_FILE_LOCK_NAME, getuid(), shmkey);
#else
    sprintf(fileLockName, "%s.%ld.%ld", COSMOS_FILE_LOCK_NAME, getuid(), shmkey);
#endif


#ifndef WIN32
    fd = open(fileLockName, O_RDWR | O_SYNC | O_CREAT, CREATED_VOLUME_PERM);
    if (fd == -1) {
	return (eINTERNAL);
    }
#else
    fd = CreateFile(fileLockName, GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
		    OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (fd == INVALID_HANDLE_VALUE) {
	return (eINTERNAL);
    }
#endif /* WIN32 */

    if (action == START_CRITICAL_SECTION) {

        e = cosmos_thread_mutex_lock(&mutexForSharedMemoryAccess);
       	if (e < eNOERROR) ERR(handle, e);

	e = cosmos_file_lock(handle, fd); /* old: cosmos_file_lock(fd) */
       	if (e < eNOERROR) ERR(handle, e);
    }
    else if (action == END_CRITICAL_SECTION) {

        e = cosmos_file_unlock(fd);
       	if (e < eNOERROR) ERR(handle, e);

        e = cosmos_thread_mutex_unlock(&mutexForSharedMemoryAccess);
        if (e < eNOERROR) ERR(handle, e);
    }
    else if (action == INIT_CRITICAL_SECTION) {

        e = pthread_mutex_init(&mutexForSharedMemoryAccess, NULL);
        if (e < eNOERROR) ERR(handle, e);

    } else ERR(handle, eBADPARAMETER);

    *lockFileDesc = fd; 

    return (eNOERROR);
}

Four THM_CriticalSectionForPerThreadTable(
    Four        	handle,
    Four        	action
)
{

    Four        	e;


    if (action == START_CRITICAL_SECTION) {

        e = cosmos_thread_mutex_lock(&mutexForPerThreadTableAccess);
        if (e < eNOERROR) ERR(handle, e);
    }
    else if (action == END_CRITICAL_SECTION) {

        e = cosmos_thread_mutex_unlock(&mutexForPerThreadTableAccess);
        if (e < eNOERROR) ERR(handle, e);
    }
    else ERR(handle, eBADPARAMETER);


    return (eNOERROR);
}

Four THM_CriticalSectionForErrorLogFile(
    Four        	handle,
    Four        	action,
    FileDesc    	fd
)
{

    Four        	e;


    if (action == START_CRITICAL_SECTION) {
            
        e = cosmos_thread_mutex_lock(&mutexForErrorLogFileAccess);            
        if (e < eNOERROR) ERR_PRINT_LOG(handle, e);

	e = cosmos_file_lock(handle, fd);           /* old: cosmod_file_lock(fd) */
        if (e < eNOERROR) ERR_PRINT_LOG(handle, e);
    }
    else if (action == END_CRITICAL_SECTION) {

        e = cosmos_file_unlock(fd);
        if (e < eNOERROR) ERR_PRINT_LOG(handle, e);

        e = cosmos_thread_mutex_unlock(&mutexForErrorLogFileAccess);          
        if (e < eNOERROR) ERR_PRINT_LOG(handle, e);
    }
    else ERR(handle, eBADPARAMETER);
            

    return (eNOERROR);
}

Four cosmos_file_lock(
    Four                handle, 
    FileDesc		fd
)
{
    Four        	e;
    struct flock        lock;

    /* initialize flock attribute */
    lock.l_type     = F_WRLCK;
    lock.l_start    = 0;            /* file lock */
    lock.l_whence   = SEEK_SET;
    lock.l_len      = 0;

    while (1) {
    	e = fcntl(fd, F_SETLK, &lock);
	if (e != -1) break;

	Util_Sleep(handle, COSMOS_FILE_LOCK_RETRY_TIMEOUT); 
    }


    return (eNOERROR);
}

Four cosmos_file_unlock(
    FileDesc		fd
)
{
    Four        	e;
    struct flock        lock;

    /* initialize flock attribute */
    lock.l_type     = F_UNLCK;
    lock.l_start    = 0;            /* file lock */
    lock.l_whence   = SEEK_SET;
    lock.l_len      = 0;

    e = fcntl(fd, F_SETLK, &lock);
    if (e == -1) {
	fprintf(stderr, "ERROR: %s:%d\n", __FILE__, __LINE__);
	return(eINTERNAL);
    }


    return (eNOERROR);
}


/* ***********************************************
 * API for thread                                *
 * *********************************************** */ 
Four cosmos_thread_create(cosmos_thread_t *tid, void*(*start_routine)(void*), void *arg, Four flag)
/*
    description:
        create new thread
    parameter:
        tid: new thread identification
        start_routine: new thread's start routine
        arg: start routine's argument
        flag:
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = pthread_create(tid, &cosmos_thread_attr_default, start_routine, arg);

    /* error check */
    if (e != 0) {
        switch (errno) {
            case EAGAIN:
            /* a system limit is exceeded, such as when too many LWPs have been created */
                return (eTHREADCREATEAGAIN_THM);
            case EINVAL:    /* the value of tattr is invalid */
                return (eTHREADCREATEINVAL_THM);
            default:        /* unknown error */
                return (eTHREADCREATEUNKNOWN_THM);
        }
    }
    else {
        return (eNOERROR);
    }

#endif
}

Four cosmos_thread_wait(cosmos_thread_t tid)
/*
    description:
        wait for thread
    parameter:
        tid: thread identification
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = pthread_join(tid, NULL);

    /* error check */
    switch (errno) {
        case 0:         /* success */
            return (eNOERROR);
        case ESRCH:     /* tid is not a valid, undetached thread in the current process */
            return (eTHREADWAITSRCH_THM);
        case EDEADLK:   /* tid specifies the calling thread */
            return (eTHREADWAITDEADLK_THM);
        case EINVAL:    /* the value of tid is invalid */
            return (eTHREADWAITINVAL_THM);
        default:        /* unknown error */
            return (eTHREADWAITUNKNOWN_THM);
    }
#endif
}

/* ***********************************************
 * API for mutex                                 *
 * *********************************************** */ 
Four cosmos_thread_mutex_create(cosmos_thread_mutex_t *mp, Four flag)
/*
    description:
        create new mutex
    parameter:
        mp:     new mutex variable
        flag:
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = pthread_mutexattr_init(&cosmos_thread_mutex_attr_default);
    e = pthread_mutexattr_setpshared(&cosmos_thread_mutex_attr_default, PTHREAD_PROCESS_SHARED);

    e = pthread_mutex_init(mp, &cosmos_thread_mutex_attr_default);

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
#endif

    return e;
}

Four cosmos_thread_mutex_destroy(cosmos_thread_mutex_t *mp)
/*
    description:
        destroy the mutex
    parameter:
        mp:     mutex variable
    return:
*/
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

Four cosmos_thread_mutex_lock(cosmos_thread_mutex_t *mp)
/*
    description:
        lock the mutex
    parameter:
        mp: mutex variable
    return:
*/
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
Four cosmos_thread_mutex_unlock(cosmos_thread_mutex_t *mp)
/*
    description:
        unlock the mutex
    parameter:
        mp:
    return:
*/
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

/* ***********************************************
 * API for condition variable                    *
 * *********************************************** */ 
Four cosmos_thread_cond_create(cosmos_thread_cond_t *cv, Four flag)
/*
    description:
        create new condition variable
    parameter:
        cv: condition variable
        flag:
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = pthread_cond_init(cv, &cosmos_thread_cond_attr_default);

    /* error check */
    switch (errno) {
        case 0:         /* success */
            return (eNOERROR);
        case EINVAL:    /* the value specified by cattr is invalid */
            return (eCONDCREATEINVAL_THM);
        case EBUSY:     /* the condition variable is being used */
            return (eCONDCREATEBUSY_THM);
        case EAGAIN:    /* the necessary resources are not available */
            return (eCONDCREATEAGAIN_THM);
        case ENOMEM:    /* there is not enough memory to initialize the condition variable */
            return (eCONDCREATENOMEM_THM);
        default:        /* unknown error */
            return (eCONDCREATEUNKNOWN_THM);
    }
#endif
}

Four cosmos_thread_cond_destroy(cosmos_thread_cond_t *cv)
/*
    description:
        destroy the condition variable
    parameter:
        cv: condition variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = pthread_cond_destroy(cv);

    /* error check */
    switch (errno) {
        case 0:         /* success */
            return (eNOERROR);
        case EINVAL:    /* the value specified by cv is invalid */
            return (eCONDDESTROYINVAL_THM);
        default:        /* unknown error */
            return (eCONDDESTROYUNKNOWN_THM);
    }
#endif
}

Four cosmos_thread_cond_wait(cosmos_thread_cond_t *cv, cosmos_thread_mutex_t *mp)
/*
    description:
        block on a condition variable
    parameter:
        cv:     condition variable
        mp:     mutex variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = pthread_cond_wait(cv, mp);

    /* error check */
    switch (errno) {
        case 0:         /* success */
            return (eNOERROR);
        case EINVAL:    /* the value specified by cv or mp is invalid */
            return (eCONDWAITINVAL_THM);
        default:        /* unknown error */
            return (eCONDWAITUNKNOWN_THM);
    }
#endif
}

Four cosmos_thread_cond_signal(cosmos_thread_cond_t *cv)
/*
    description:
        unblock a specific thread
    parameter:
        cv:     condition variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = pthread_cond_signal(cv);

    /* error check */
    switch (errno) {
        case 0:         /* success */
            return (eNOERROR);
        case EINVAL:    /* cv points to an illegal address */
            return (eCONDSIGNALINVAL_THM);
        default:        /* unknown error */
            return (eCONDSIGNALUNKNOWN_THM);
    }
#endif
}

/* ***********************************************
 * API for semaphore                             *
 * *********************************************** */ 
Four cosmos_thread_unnamed_sem_create(cosmos_thread_sem_t* returnSem, unsigned int value, Four flag)
/*
    description:
        create new semaphore
    parameter:
        sem:    new semaphore variable
        value:  init value (semaphore value)
        flag:
    return:
*/
{
#ifdef WIN32

#else
    Four                    e;


    e = sem_init(returnSem, 1, value);

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
        return ((Four)eNOERROR);
    }

#endif
}

Four cosmos_thread_named_sem_create(cosmos_thread_sem_t** returnSem, cosmos_thread_semName_t *semName, unsigned int value, Four flag)
/*
    description:
        create new semaphore
    parameter:
        sem:    new semaphore variable
        value:  init value (semaphore value)
        flag:
    return:
*/
{
#ifdef WIN32

#else
    Four                    e;
    Four                    flags, modes;

    flags = O_RDWR | O_CREAT;
    modes  = S_IWUSR | S_IRUSR;

    *returnSem = sem_open(semName, flags, modes, value);

    /* error check */
    /* 나중에 고칠 것 -1 대신 SEM_FAILED*/
    /* Replace -1 with SEM_FAILED later */
    if ((int)*returnSem == -1) { 

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

#endif
}

Four cosmos_thread_named_sem_close(cosmos_thread_sem_t *sem)
/*
    description:
        close the semaphore
    parameter:
        sem:    semaphore's variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = sem_close(sem);

    /* error check */
    if (e != 0) {
        switch (errno) {
            case EINVAL:    /* The sem argument is not a valid semaphore descriptor */
                return (eSEMCLOSEINVAL_THM);
            case ENOSYS:    /* The sem_close() function is not supported  by  the system */
                return (eSEMCLOSENOSYS_THM);
            default:        /* unknown error */
                return (eSEMCLOSEUNKNOWN_THM);
        }
    }
    else {
        return (eNOERROR);
    }
#endif
}

Four cosmos_thread_unnamed_sem_destroy(cosmos_thread_sem_t *sem_ID)
/*
    description:
        destroy the semaphore
    parameter:
        sem:    semaphore's variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = sem_destroy(sem_ID);

    /* error check */
    if (e != 0) {
        switch (errno) {
            case EACCES:    /* Permission is denied to  unlink  the  named  semaphore */
                return (eSEMDESTROYACCES_THM);
            case ENAMETOOLONG:      /* The length of name string exceeds PATH_MAX, or a pathname component is longer than NAME_MAX */
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
#endif
}

Four cosmos_thread_named_sem_destroy(cosmos_thread_semName_t *sem_name)
/*
    description:
        destroy the semaphore
    parameter:
        sem:    semaphore's variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = sem_unlink(sem_name);

    /* error check */
    if (e != 0) {
        switch (errno) {
            case EACCES:    /* Permission is denied to  unlink  the  named  semaphore */
                return (eSEMDESTROYACCES_THM);
            case ENAMETOOLONG:      /* The length of name string exceeds PATH_MAX, or a pathname component is longer than NAME_MAX */
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
#endif
}

Four cosmos_thread_sem_post(cosmos_thread_sem_t *sem)
/*
    description:
        increment the semaphore (semaphore-P)
    parameter:
        sem:    semaphore variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = sem_post(sem);

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
#endif
}

Four cosmos_thread_sem_wait(cosmos_thread_sem_t *sem)
/*
    description:
        decrement the semaphore (semaphore-V)
    parameter:
        sem:    semaphore variable
    return:
*/
{
#ifdef WIN32

#else
    Four e;

    e = sem_wait(sem);

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

#endif
}


