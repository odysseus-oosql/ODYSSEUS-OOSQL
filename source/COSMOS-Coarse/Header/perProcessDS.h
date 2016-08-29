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
#ifndef __PERPROCESSDS_H__
#define __PERPROCESSDS_H__

#include "common.h"
#include "param.h"
#include "error.h"
#include "RDsM_Internal.h"
#include "LRDS.h"
#include "BfM_Internal.h"
#include "RM_Internal.h"
#include "SM_Internal.h"
#include "xa.h"

/*
 * Type Definitions for PerProcess Data Structures
 */

/* BfM Per-Thread Data Structures */
typedef struct BfM_PerProcessDS_T_tag {
#ifdef USE_SHARED_MEMORY_BUFFER
    Four 			shmBufferId;
    Four 			receivedSignalNo;
#endif
#if defined(USE_COHERENCY_VOLUME) || defined(USE_LOG_COHERENCY_VOLUME)
    bfm_CoherencyVolumeInfo_t	bfm_CoherencyVolumeInfo;
#endif
} BfM_PerProcessDS_T;

/* LRDS Per-Thread Data Structures */
typedef struct LRDS_PerProcessDS_T_tag {
    char 	*catalogTable[LRDS_NUMOFCATALOGTABLES];
} LRDS_PerProcessDS_T;

/* RDsM Per-Thread Data Strucutures */
typedef struct RDsM_PerProcessDS_T_tag {
    Boolean 			rdsmUseSharedLock;
} RDsM_PerProcessDS_T;

/* XA Per-Thread Data Structures */
typedef struct XA_PerProcessDS_T_tag {
    struct xa_switch_t	cosmosxa;
} XA_PerProcessDS_T;

/* Per-Thread Data Structures */
typedef struct PerProcessDS_T_tag {
    BfM_PerProcessDS_T	bfmDS;
    LRDS_PerProcessDS_T	lrdsDS;
    RDsM_PerProcessDS_T	rdsmDS;
    XA_PerProcessDS_T	xaDS;

    Four		nThread;
} PerProcessDS_T;

/*
 * Macro Definitions
 */

#define BFM_PER_PROCESS_DS		(perProcessDS.bfmDS)
#define LRDS_PER_PROCESS_DS		(perProcessDS.lrdsDS)
#define RDSM_PER_PROCESS_DS		(perProcessDS.rdsmDS)
#define RM_PER_PROCESS_DS		(perProcessDS.rmDS)
#define XA_PER_PROCESS_DS		(perProcessDS.xaDS)

#define BFM_PER_PROCESS_DS_PTR		(&perProcessDS.bfmDS)
#define LRDS_PER_PROCESS_DS_PTR		(&perProcessDS.lrdsDS)
#define RDSM_PER_PROCESS_DS_PTR		(&perProcessDS.rdsmDS)
#define RM_PER_PROCESS_DS_PTR		(&perProcessDS.rmDS)
#define XA_PER_PROCESS_DS_PTR		(&perProcessDS.xaDS)

#define IS_PER_PROCESS_DS_INITIALIZED	(isPerProcessDSInitialized)
#define NUM_OF_THREADS_IN_PROCESS	(perProcessDS.nThread)


/*
 * Global Variables
 */
#ifdef COSMOS_S
extern Four		procIndex;
#endif
extern PerProcessDS_T	perProcessDS;
extern Boolean	  	isPerProcessDSInitialized;	
extern CfgParams_T	sm_cfgParams;
extern char*		cosmosReleaseString;
extern char*		__xyz__;


/* 
 * Temporary Data Structure for Volume lock in multi-thread env.
 * We have to move the following structures to SHM layer.
 */
#ifdef COSMOS_MULTITHREAD
typedef struct VolumeLockTable_T {
    cosmos_thread_rwlock_t	rwlock;
} VolumeLockTable;

extern VolumeLockTable*	volLockTable;
#endif

#endif /* __PERPROCESSDS_H__ */
