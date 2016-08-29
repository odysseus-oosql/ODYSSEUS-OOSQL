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
#ifndef _THM_LOCK_H_
#define _THM_LOCK_H_

#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>

#ifndef WIN32
typedef int FileDesc;
#else
typedef void* FileDesc;
#endif


/*@
 * Constant Definitions
 */ 

/* Permmision of shared memory buffer */
#define PERMS 			0666

/* File mode to create lock file */
#define FILE_MODE 		S_IWUSR 

/* Maximum size of semaphore name */
#define MAXSEMAPHORENAME        256

/* Default mutex initializer */
#define COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS       PTHREAD_MUTEX_INITIALIZER

/* Default rwlock initializer */
#define COSMOS_THREAD_RWLOCK_INIT_FOR_INTRAPROCESS	PTHREAD_RWLOCK_INITIALIZER


/*@
 * Type Definitions
 */

/* Types for Semaphore */ 
typedef sem_t                   cosmos_thread_sem_t;
typedef char                    cosmos_thread_semName_t;

/* Types for Mutex */
typedef pthread_mutex_t         cosmos_thread_mutex_t;
typedef pthread_mutexattr_t     cosmos_thread_mutex_attr_t;

/* Types for Read/Write Lock */
typedef pthread_rwlock_t	cosmos_thread_rwlock_t;
typedef pthread_rwlockattr_t	cosmos_thread_rwlock_attr_t;


/*@
 * Macro Definitions
 */

/*
 * Macros for critical section
 */
/* Critical Section Access Flag */
#define SHARED_MEMORY_ACCESS	0
#define PER_PROCESS_DS_ACCESS	1
#define ERROR_LOG_FILE_ACCESS	2

#define START_CRITICAL_SECTION	0
#define END_CRITICAL_SECTION	1

/* Critical section for shared memory */
#define START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(_handle, _fdPointer) \
    	THM_CriticalSection(_handle, SHARED_MEMORY_ACCESS, START_CRITICAL_SECTION, _fdPointer)

#define END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(_handle, _fdPointer) \
	THM_CriticalSection(_handle, SHARED_MEMORY_ACCESS, END_CRITICAL_SECTION, _fdPointer)
#define CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(_fd) \
	close(_fd)

/* Critical section for perProcess data */
#define START_CRITICAL_SECTION_FOR_PER_PROCESS_DS_ACCESS(_handle) \
    	THM_CriticalSection(_handle, PER_PROCESS_DS_ACCESS, START_CRITICAL_SECTION, NULL)

#define END_CRITICAL_SECTION_FOR_PER_PROCESS_DS_ACCESS(_handle) \
	THM_CriticalSection(_handle, PER_PROCESS_DS_ACCESS, END_CRITICAL_SECTION, NULL)

/* Critical section for log */
#define START_CRITICAL_SECTION_FOR_ERROR_LOG_FILE_ACCESS(_handle) \
    	THM_CriticalSection(_handle, ERROR_LOG_FILE_ACCESS, START_CRITICAL_SECTION, NULL)

#define END_CRITICAL_SECTION_FOR_ERROR_LOG_FILE_ACCESS(_handle) \
    	THM_CriticalSection(_handle, ERROR_LOG_FILE_ACCESS, END_CRITICAL_SECTION, NULL)


/*
 * Macros for Mutex 
 */
/* Initialize Mutex */
#define MUTEX_INIT(__mutexPtr, __mutexAttrPtr, __option) \
BEGIN_MACRO \
    if (pthread_mutexattr_init(__mutexAttrPtr) != 0) \
        return (eMUTEXCREATEUNKNOWN_THM); \
    if (pthread_mutexattr_setpshared(__mutexAttrPtr, __option) != 0) \
        return (eMUTEXCREATEUNKNOWN_THM); \
    if (pthread_mutex_init(__mutexPtr, __mutexAttrPtr) != 0) \
        return (eMUTEXCREATEUNKNOWN_THM); \
    if (pthread_mutexattr_destroy(__mutexAttrPtr) != 0) \
        return (eMUTEXCREATEUNKNOWN_THM); \
END_MACRO

/* Finalize Mutex */
#define MUTEX_FINAL(__mutexPtr) \
BEGIN_MACRO \
    if (pthread_mutex_destroy(__mutexPtr) != 0) \
        return (eMUTEXDESTROYUNKNOWN_THM); \
END_MACRO

/* Acquire/Release Mutex */
#define MUTEX_LOCK(__mutexPtr)          pthread_mutex_lock(__mutexPtr)
#define MUTEX_TRYLOCK(__mutexPtr)       pthread_mutex_trylock(__mutexPtr)
#define MUTEX_UNLOCK(__mutexPtr)        pthread_mutex_unlock(__mutexPtr)


/*
 * Macros for RW Lock 
 */
/* Initialize RW Lock */
#define RWLOCK_INIT(__rwlockPtr, __rwlockAttrPtr, __option) \
BEGIN_MACRO \
    if (pthread_rwlockattr_init(__rwlockAttrPtr) != 0) \
        return (eRWLOCKCREATEUNKNOWN_THM); \
    if (pthread_rwlockattr_setpshared(__rwlockAttrPtr, __option) != 0) \
        return (eRWLOCKCREATEUNKNOWN_THM); \
    if (pthread_rwlock_init(__rwlockPtr, __rwlockAttrPtr) != 0) \
        return (eRWLOCKCREATEUNKNOWN_THM); \
    if (pthread_rwlockattr_destroy(__rwlockAttrPtr) != 0) \
        return (eRWLOCKCREATEUNKNOWN_THM); \
END_MACRO

/* Finalize RW Lock */
#define RWLOCK_FINAL(__rwlockPtr) \
BEGIN_MACRO \
    if (pthread_rwlock_destroy(__rwlockPtr) != 0) \
        return (eRWLOCKDESTROYUNKNOWN_THM); \
END_MACRO

/* Acquire/Release R/W Lock */
#define RWLOCK_RDLOCK(__rwlockPtr)	pthread_rwlock_rdlock(__rwlockPtr)
#define RWLOCK_TRYRDLOCK(__rwlockPtr)	pthread_rwlock_tryrdlock(__rwlockPtr)
#define RWLOCK_WRLOCK(__rwlockPtr)	pthread_rwlock_wrlock(__rwlockPtr)
#define RWLOCK_TRYWRLOCK(__rwlockPtr)	pthread_rwlock_trywrlock(__rwlockPtr)
#define RWLOCK_UNLOCK(__rwlockPtr)	pthread_rwlock_unlock(__rwlockPtr)

/*@
 * Global Variables
 */
/* for mutex */
extern cosmos_thread_mutex_attr_t	cosmos_thread_mutex_attr;
extern cosmos_thread_mutex_t		mutexForSharedMemoryAccess;
extern cosmos_thread_mutex_t		mutexForPerProcessDSAccess;
extern cosmos_thread_mutex_t		mutexForErrorLogFileAccess;

/* for rwlock */
extern cosmos_thread_rwlock_attr_t	cosmos_thread_rwlock_attr;

/*@
 * Function Definitions
 */
/* Critical Section Control */
Four THM_CriticalSection(Four, Four, Four, FileDesc*); 

/* Acquire/release file lock */
Four THM_ControlFileLock(int cmd, int type, FileDesc*); 

/* 
 * Functions for Semaphore
 */
Four THM_CreateUnnamedSemaphore(Four, cosmos_thread_sem_t*, unsigned int);
Four THM_DestroyUnnamedSemaphore(Four, cosmos_thread_sem_t*);
Four THM_SemWait(Four, cosmos_thread_sem_t*);
Four THM_SemPost(Four, cosmos_thread_sem_t*);

/*
 * Functions for Mutex
 */
Four THM_CreateMutex(cosmos_thread_mutex_t*, Four);
Four THM_DestroyMutex(cosmos_thread_mutex_t*);
Four THM_AcquireMutex(cosmos_thread_mutex_t*);
Four THM_ReleaseMutex(cosmos_thread_mutex_t*);

/*
 * Functions for Read/Write Lock
 */
Four THM_CreateRWLock(cosmos_thread_rwlock_t*, Four);
Four THM_DestroyRWLock(cosmos_thread_rwlock_t*); 
Four THM_AcquireReadLock(cosmos_thread_rwlock_t*);
Four THM_AcquireWriteLock(cosmos_thread_rwlock_t*);
Four THM_ReleaseRWLock(cosmos_thread_rwlock_t*);

#endif 	/* _THM_LOCK_H_ */

