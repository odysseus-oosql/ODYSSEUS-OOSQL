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
/*
 * Module: SM_InitFinal.c
 *
 * Description:
 *  Initialize/Finalize the storage system.
 *
 * Exports:
 *  Four SM_Init(Four)
 *  Four SM_Final(Four)
 */


#include <string.h>
#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "Util_Sort.h" 
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "RM.h"
#include "RDsM.h"
#include "BfM.h"
#include "OM.h"
#include "LOT.h"
#include "BtM.h"
#include "MLGF.h"
#include "SM.h"
#include "SHM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


ComponentInfo_T componentInfos[] = {
    {Util_Sort_Init, NULL, NULL, NULL, NULL},
    {XA_InitLocalDS, NULL, NULL, NULL, NULL}, 
    {SHM_initLocalDS, SHM_initSharedDS, NULL, SHM_finalLocalDS, SHM_finalSharedDS}, 
    {TM_InitLocalDS, TM_InitSharedDS, NULL, TM_FinalLocalDS, TM_FinalSharedDS},
    {RM_InitLocalDS, RM_InitSharedDS, NULL, RM_FinalLocalDS, RM_FinalSharedDS},
    {LM_initLocalDS, LM_initSharedDS, NULL, LM_finalLocalDS, LM_finalSharedDS},
    {RDsM_InitLocalDS, RDsM_InitSharedDS, NULL, RDsM_finalLocalDS, RDsM_finalSharedDS},
    {BfM_initLocalDS, BfM_initSharedDS, NULL, BfM_finalLocalDS, BfM_finalSharedDS},
    {LOG_InitLocalDS, LOG_InitSharedDS, NULL, LOG_FinalLocalDS, LOG_FinalSharedDS},
    {LOT_InitLocalDS, LOT_InitSharedDS, NULL, LOT_FinalLocalDS, LOT_FinalSharedDS}, 
    {OM_InitLocalDS, OM_InitSharedDS, NULL, NULL, NULL},
    {BtM_InitLocalDS, BtM_InitSharedDS, NULL, NULL, NULL},
    {MLGF_InitLocalDS, MLGF_InitSharedDS, NULL, MLGF_FinalLocalDS, MLGF_FinalSharedDS},
    {SM_InitLocalDS, SM_InitSharedDS, NULL, SM_FinalLocalDS, SM_FinalSharedDS},
#ifdef LRDS_INCLUDED
    {LRDS_InitLocalDS, LRDS_InitSharedDS, NULL, LRDS_FinalLocalDS, LRDS_FinalSharedDS},
#endif /* LRDS_INCLUDED */
}; 

extern CfgParams_T sm_cfgParams;

Four isPerProcessDSInitialized = 0;

 /*@================================
 * SM_Init( )
 *================================*/
/*
 * Function: Four SM_Init()
 *
 * Description:
 *  Initialize the storage system. This routine calls RDsM_Initialize() and
 *  BfM_init() to initialize the Raw Disk Manager and the Buffer Manager,
 *  respectively. And then, initialize the main memory data structure used in
 *  the Scan Manager; smMountTable, SM_ScanTable, and the deletedPageList.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four SM_Init( )
{
    Four 	e;                  /* error code */
    Boolean 	firstProcFlag;      /* TRUE if this is the first process */
    Four	handle = -1; 
    FileDesc    fd_1, fd_2; 


    TR_PRINT(handle, TR_SM, TR1, ("SM_Init()"));


    /*
     * CRITICAL SECTION BEGIN
     */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_1);

    if (!IS_PER_PROCESS_DS_INITIALIZED) {

	IS_PER_PROCESS_DS_INITIALIZED = 1;

#ifdef SIGNAL_HANDLER 
        /* Install Signal Handler */
        e = SHM_InstallSignalHandler();
        if (e < eNOERROR) ERR(handle, e);

        /* Install Exit Handler */
	e = SHM_InstallExitHandler();
        if (e < eNOERROR) ERR(handle, e);
#endif 
        
    	e = THM_InitPerProcess();
    	if (e < eNOERROR) ERR(handle, e);

    	e = SHM_beginProcess(handle, &sm_cfgParams, componentInfos, sizeof(componentInfos)/sizeof(componentInfos[0]));
    	if (e < eNOERROR) ERR(handle, e);

    }

    /*
     * CRITICAL SECTION END
     */
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_2);

    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_2);

    return(eNOERROR);

} /* SM_Init( ) */


/*@================================
 * SM_Final( )
 *================================*/
/*
 * Function: Four SM_Final( )
 *
 * Description:
 *  Finalize the storage system. At first, dismount the mounted volume. Then
 *  free all dynamically allocated memory. At last, finalize the sub layers;
 *  Buffer Manager and Raw Disk Manager.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four SM_Final( )
{
    Four e;                     /* error code */
    Four handle = -1; 
    FileDesc    fd_1, fd_2; 


    TR_PRINT(handle, TR_SM, TR1, ("SM_Final()"));


    /*
     * CRITICAL SECTION BEGIN
     */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_1);

    if (IS_PER_PROCESS_DS_INITIALIZED && NUM_OF_THREADS_IN_PROCESS == 0) {

	e = SHM_endProcess(handle, &sm_cfgParams, componentInfos, sizeof(componentInfos)/sizeof(componentInfos[0]));
    	if (e < eNOERROR) ERR(handle, e);

    	e = THM_FinalPerProcess();
    	if (e < eNOERROR) ERR(handle, e);

#ifdef SIGNAL_HANDLER 
        /* Install Signal Handler */
        e = SHM_UninstallSignalHandler();
        if (e < eNOERROR) ERR(handle, e);
#endif 

	IS_PER_PROCESS_DS_INITIALIZED = 0;
    }

    /*
     * CRITICAL SECTION END
     */
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_2);

    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_2);

    return(eNOERROR);

} /* SM_Final() */

Four SM_AllocHandle(Four *handle)
{

    Four e;
    FileDesc    fd_1, fd_2; 


    /*
     * CRITICAL SECTION BEGIN
     */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(*handle, &fd_1);

    e = THM_AllocHandle(handle);
    if (e < eNOERROR) ERR(*handle, e);

    /*
     * CRITICAL SECTION END
     */
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(*handle, &fd_2);

    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_2);

    return(eNOERROR);
}

Four SM_FreeHandle(Four handle)
{

    Four e;
    FileDesc    fd_1, fd_2; 


    /*
     * CRITICAL SECTION BEGIN
     */
    START_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_1);

    e = THM_FreeHandle(handle);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * CRITICAL SECTION END
     */
    END_CRITICAL_SECTION_FOR_SHARED_MEMORY_ACCESS(handle, &fd_2);
    
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_1);
    CLOSE_LOCK_FILE_DESC_FOR_SHARED_MEMORY_ACCESS(fd_2);

    return(eNOERROR);
}

