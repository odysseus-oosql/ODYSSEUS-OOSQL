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
#ifndef __THM_COSMOSTHREAD_H__
#define __THM_COSMOSTHREAD_H__

#ifdef  __cplusplus
extern "C" {
#endif

#ifdef WIN32

#else
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#endif  /* WIN32 */

#include "common.h"

#ifdef WIN32


#else
typedef 	sem_t					cosmos_thread_sem_t;
typedef 	char					cosmos_thread_semName_t;
typedef		pthread_mutex_t				cosmos_thread_mutex_t;
typedef     	pthread_mutexattr_t			cosmos_thread_mutex_attr_t;
typedef     	pthread_cond_t				cosmos_thread_cond_t;
typedef		pthread_condattr_t			cosmos_thread_cond_attr_t;
typedef		pthread_t				cosmos_thread_t;
typedef		pthread_attr_t				cosmos_thread_attr_t;

extern 		cosmos_thread_mutex_attr_t		cosmos_thread_mutex_attr_default;
extern 		cosmos_thread_cond_attr_t		cosmos_thread_cond_attr_default;
extern		cosmos_thread_attr_t			cosmos_thread_attr_default;
#endif

#define 	MAXSEMAPHORENAME			256

#define 	FILE_MODE                   		S_IWUSR
#define 	COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS	PTHREAD_MUTEX_INITIALIZER

#define 	SHARED_MEMORY_ACCESS			0
#define		PER_THREAD_TABLE_ACCESS			1
#define		ERROR_LOG_FILE_ACCESS			2
#define 	INIT_CRITICAL_SECTION			-1
#define 	START_CRITICAL_SECTION			0
#define		END_CRITICAL_SECTION			1


#define INIT_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(_handle, _ptr_lock_fd) \
        THM_CriticalSectionForSharedMemory(_handle, INIT_CRITICAL_SECTION, _ptr_lock_fd)
#define START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(_handle, _ptr_lock_fd) \
	THM_CriticalSectionForSharedMemory(_handle, START_CRITICAL_SECTION, _ptr_lock_fd)
#define END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(_handle, _ptr_lock_fd) \
	THM_CriticalSectionForSharedMemory(_handle, END_CRITICAL_SECTION, _ptr_lock_fd)
#define CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(lockfd) \
	close(lockfd)

#define START_CRITICAL_SECTION_FOR_PER_THREAD_TABLE_ACCESS(_handle) \
	THM_CriticalSectionForPerThreadTable(_handle, START_CRITICAL_SECTION)
#define END_CRITICAL_SECTION_FOR_PER_THREAD_TABLE_ACCESS(_handle) \
	THM_CriticalSectionForPerThreadTable(_handle, END_CRITICAL_SECTION)

#define START_CRITICAL_SECTION_FOR_ERROR_LOG_FILE_ACCESS(_handle, _fd) \
	THM_CriticalSectionForErrorLogFile(_handle, START_CRITICAL_SECTION, _fd)
#define END_CRITICAL_SECTION_FOR_ERROR_LOG_FILE_ACCESS(_handle, _fd) \
	THM_CriticalSectionForErrorLogFile(_handle, END_CRITICAL_SECTION, _fd)

/* ***********************************************
 * API for critical section			 *
 * *********************************************** */
Four THM_CriticalSectionForSharedMemory(Four, Four, FileDesc*);
Four THM_CriticalSectionForPerThreadTable(Four, Four);
Four THM_CriticalSectionForErrorLogFile(Four, Four, FileDesc);

/* ***********************************************
 * API for filelock                              *
 * *********************************************** */
Four file_lock_control(FileDesc, int, int);

/* ***********************************************
 * thread ฐüทร Init Final                        *
 * *********************************************** */
Four THM_threadOperator_Init(void);
Four THM_threadOperator_Final(void);


/* ***********************************************
 * API for thread                                *
 * *********************************************** */
Four cosmos_thread_create(cosmos_thread_t *tid, void*(*start_routine)(void*), void *arg, Four flag);
Four cosmos_thread_wait(cosmos_thread_t tid);

/* ***********************************************
 * API for mutex                                 *
 * *********************************************** */
Four cosmos_thread_mutex_create(cosmos_thread_mutex_t *mp, Four flag);
Four cosmos_thread_mutex_destroy(cosmos_thread_mutex_t *mp);
Four cosmos_thread_mutex_lock(cosmos_thread_mutex_t *mp);
Four cosmos_thread_mutex_unlock(cosmos_thread_mutex_t *mp);

/* ***********************************************
 * API for condition variable                    *
 * *********************************************** */
Four cosmos_thread_cond_create(cosmos_thread_cond_t *cv, Four flag);
Four cosmos_thread_cond_destroy(cosmos_thread_cond_t *cv);
Four cosmos_thread_cond_wait(cosmos_thread_cond_t *cv, cosmos_thread_mutex_t *mp);
Four cosmos_thread_cond_signal(cosmos_thread_cond_t *cv);

/* ***********************************************
 * API for semaphore                             *
 * *********************************************** */
Four cosmos_thread_unnamed_sem_create(cosmos_thread_sem_t* returnSem, unsigned int  value, Four flag);
Four cosmos_thread_named_sem_create(cosmos_thread_sem_t** returnSem, cosmos_thread_semName_t *semName, unsigned int value, Four flag);
Four cosmos_thread_named_sem_close(cosmos_thread_sem_t *sem);
Four cosmos_thread_unnamed_sem_destroy(cosmos_thread_sem_t *sem_ID);
Four cosmos_thread_named_sem_destroy(cosmos_thread_semName_t *sem_name);
Four cosmos_thread_sem_post(cosmos_thread_sem_t *sem);
Four cosmos_thread_sem_wait(cosmos_thread_sem_t *sem);

#ifdef  __cplusplus
}
#endif


#endif /*  __THM_COSMOSTHREAD_H__ */



