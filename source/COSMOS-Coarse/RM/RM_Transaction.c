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
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "RM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@================================
 * RM_BeginTransaction()
 *================================*/
/*
 * Function: Four RM_BeginTransaction(Boolean)
 *
 * Description:
 *  Prepare for beginning a new transaction
 *
 * Returns:
 *  error code
 */
Four RM_BeginTransaction(
    Four handle,
    Boolean rollbackRequiredFlag) /* IN TRUE if rollback facility is required */
{
    Four e;                     /* error code */

    
    TR_PRINT(TR_RM, TR1, ("RM_BeginTransaction(handle, rollbackRequiredFlag=%ld)", rollbackRequiredFlag));


    /* Remember whether the transaction requires the rollback facility. */
    RM_PER_THREAD_DS(handle).RM_RollbackRequiredFlag = rollbackRequiredFlag; 

    /* Return if rollback is not required. */
    if (!RM_PER_THREAD_DS(handle).RM_RollbackRequiredFlag) return(eNOERROR); 
    
    /* Check whether a log volume is mounted. */    
    if (RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo == NO_LOG_VOLUME) ERR(handle, eNOLOGVOLUME_RM); 
    
    /*
     * initialize the log volume for reuse.
     */
    e = rm_ReadLogVolumeInfo(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo);
    if (e < eNOERROR) ERR(handle, e);
    RM_INIT_ALLOC_POSITION_OF_LOG_VOLUME(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo);
    
    return(eNOERROR);
    
} /* RM_BeginTransaction() */


/*@================================
 * RM_CommitTransaction()
 *================================*/
/*
 * Function: Four RM_CommitTransaction(void)
 *
 * Description:
 *  Commit the currently running transaction.
 *
 * Returns:
 *  error code
 */
Four RM_CommitTransaction(Four handle)
{
    Four 			e;                     		/* error code */
    Four 			mountedDataVolumeListSize;
    char 			*mountedDataVolumeList;
#ifdef USE_LOG_COHERENCY_VOLUME	
    rm_CoherencyHeaderPage 	headerPage;
    rm_CoherencyPageInfosPage 	pageInfosPage;
#endif

    
    TR_PRINT(TR_RM, TR1, ("RM_CommitTransaction(handle)"));

    /* modify the way of accessing rm_onPrepareFlag */
    if (!RM_PER_THREAD_DS(handle).RM_RollbackRequiredFlag) { 

        if (RM_PER_THREAD_DS(handle).rm_onPrepareFlag == TRUE) 
	    RM_PER_THREAD_DS(handle).rm_onPrepareFlag = FALSE;

	return(eNOERROR);
    }


    /*
     * When there is no logging we can commit without any added operation.
     */
    if (RM_NOT_EXIST_ANY_LOGGED_PAGE_OR_TRAIN(RM_PER_THREAD_DS(handle).rm_LogVolumeInfo) && RM_PER_THREAD_DS(handle).rm_onPrepareFlag == FALSE) { 
	
#ifdef USE_LOG_COHERENCY_VOLUME	
	/* 
	 * Lock log volume lock to update coherency page information of log volume.
	 */
	e = RDsM_GetVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, L_X, FALSE); 
	if (e < eNOERROR) ERR(handle, e);

	/*
	 * Read coherency page information of log volume.
	 */
	e = rm_ReadCoherencyPage(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &headerPage, &pageInfosPage); 
	if (e < eNOERROR) ERR(handle, e);

	/*
	 * Update Coherency page of log volume.
	 */
	e = BfM_UpdateCoherencyPageOfLogVolume(handle, (char* )&headerPage, (char* )&pageInfosPage);
	if (e < eNOERROR) ERR(handle, e);

	/*
	 * Write coherency page information of log volume.
	 */
	e = rm_WriteCoherencyPage(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &headerPage, &pageInfosPage); 
	if (e < eNOERROR) ERR(handle, e);

	/*
	 * Release log volume lock.
	 */
	e = RDsM_ReleaseVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo); 
	if (e < eNOERROR) ERR(handle, e);
#endif	/* USE_LOG_COHERENCY_VOLUME */

	return(eNOERROR);
    }
#if defined(DBLOCK) && defined(NDEBUG)
    /* check lock for log volume is acquired */
    if ( !RDsM_CheckVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, L_X) ) ERR(handle, eINTERNAL);	
#endif


    /*
     * Collect the device names of the mounted data volumes.
     */
    mountedDataVolumeListSize = PAGESIZE;
    if ((mountedDataVolumeList = malloc(mountedDataVolumeListSize)) == NULL)
        ERR(handle, eMEMORYALLOCERR);

    while ((e = RDsM_GetMountedVolumes(handle, mountedDataVolumeListSize, mountedDataVolumeList)) == eBADBUFSIZE) {
        char *ptr_backup = mountedDataVolumeList;
        
        mountedDataVolumeListSize *= 2;        
        if ((mountedDataVolumeList = realloc(mountedDataVolumeList, mountedDataVolumeListSize)) == NULL) {
            free(ptr_backup);
            ERR(handle, eMEMORYALLOCERR);
        }        
    }

    e = rm_SaveDataVolumeList(handle, mountedDataVolumeList);
    free(mountedDataVolumeList);
    if (e < eNOERROR) ERR(handle, e);

    
    /*
     * write a flag into the log volume so that we are saving the committed
     * transaction's pages.
     */

    if (RM_PER_THREAD_DS(handle).rm_onPrepareFlag == TRUE && SM_PER_THREAD_DS(handle).globalXactRunningFlag == TRUE) {
        RM_PER_THREAD_DS(handle).rm_onPrepareFlag = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onPrepareFlag = FALSE;
        memset(&RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.globalXactID, NULL, sizeof(GlobalXactID)); 
    }
   
#ifdef USE_LOG_COHERENCY_VOLUME
    /*
      * Read coherency page information of log volume.
     */
    e = rm_ReadCoherencyPage(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &headerPage, &pageInfosPage);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Update Coherency page of log volume.
     */
    e = BfM_UpdateCoherencyPageOfLogVolume(handle, (char* )&headerPage, (char* )&pageInfosPage);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Write log volume info and coherency page information at the same time.
     */ 
    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onCommitFlag = TRUE;
    e = rm_WriteLogVolumeInfoWithCoherencyPage(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo, &headerPage, &pageInfosPage);
#else /* USE_LOG_COHERENCY_VOLUME */

    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onCommitFlag = TRUE;
    e = rm_WriteLogVolumeInfo(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 

#endif
    if (e < eNOERROR) {        
        RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onCommitFlag = FALSE; /* reset the flag */ 
        ERR(handle, e);
    }

    /*
     * Since we committed the transaction just above,
     * there should be no failure.
     */
    /* Redo the transaction operations. */
    e = rm_Redo(handle);
    if (e < eNOERROR) ERR(handle, e); 

    /* write the log volume information */
    RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onCommitFlag = FALSE;
    e = rm_WriteLogVolumeInfo(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo); 
    if (e < eNOERROR) ERR(handle, e); 

    /*
     * Discard all entries in the log table.
     */
    e = rm_DeleteAllFromLogTable(handle);
    if (e < eNOERROR) ERR(handle, e); 


    /*
     * Reset rollback required flag for next transaction
     */
    RM_PER_THREAD_DS(handle).RM_RollbackRequiredFlag = FALSE; 

    return(eNOERROR);
    
} /* RM_CommitTransaction() */



/*@================================
 * RM_AbortTransaction()
 *================================*/
/*
 * Function: Four RM_AbortTransaction(void)
 *
 * Description:
 *  Abort the currently running transaction.
 *
 * Returns:
 *  error code
 */
Four RM_AbortTransaction(Four handle)
{
    Four 			e;                     /* error code */
#ifdef USE_LOG_COHERENCY_VOLUME
    Two	 			lockCheck;
    rm_CoherencyHeaderPage 	headerPage;
    rm_CoherencyPageInfosPage 	pageInfosPage;
#endif

    
    TR_PRINT(TR_RM, TR1, ("RM_AbortTransaction(handle)"));

    if (!RM_PER_THREAD_DS(handle).RM_RollbackRequiredFlag) {		
	if (RM_PER_THREAD_DS(handle).rm_onPrepareFlag == TRUE) RM_PER_THREAD_DS(handle).rm_onPrepareFlag = FALSE; 

	ERR(handle, eNOLOGGEDTRANSACTION_RM);
    } 

#ifdef USE_LOG_COHERENCY_VOLUME
    lockCheck = RDsM_CheckVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, L_X); 
    
    /* 
     * Lock log volume lock to update coherency page information of log volume.
     */
    if (!lockCheck) {
        e = RDsM_GetVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, L_X, FALSE);
        if (e < eNOERROR) ERR(handle, e);
    }

    /*
     * Read coherency page information of log volume.
     */
    e = rm_ReadCoherencyPage(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &headerPage, &pageInfosPage); 
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Update Coherency page of log volume.
     */
    e = BfM_UpdateCoherencyPageOfLogVolume(handle, (char* )&headerPage, (char* )&pageInfosPage);
    if (e < eNOERROR) ERR(handle, e);
#endif /* USE_LOG_COHERENCY_VOLUME */

    if (RM_PER_THREAD_DS(handle).rm_onPrepareFlag == TRUE && SM_PER_THREAD_DS(handle).globalXactRunningFlag == TRUE) {

	RM_PER_THREAD_DS(handle).rm_onPrepareFlag = RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.onPrepareFlag = FALSE;
        memset( &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.globalXactID, NULL, sizeof(GlobalXactID)); 

#ifdef USE_LOG_COHERENCY_VOLUME	
	/*
	 * Write coherency page information of log volume.
	 */
	e = rm_WriteLogVolumeInfoWithCoherencyPage(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo, 
						  &headerPage, &pageInfosPage); 
	if (e < eNOERROR) ERR(handle, e);

#else /* USE_LOG_COHERENCY_VOLUME */
        e = rm_WriteLogVolumeInfo(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &RM_PER_THREAD_DS(handle).rm_LogVolumeInfo);
        if (e < eNOERROR) ERR(handle, e);
#endif 	/* USE_LOG_COHERENCY_VOLUME */

    }  	
#ifdef USE_LOG_COHERENCY_VOLUME	
    else {
	/* Write coherency page information of log volume. */
	e = rm_WriteCoherencyPage(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo, &headerPage, &pageInfosPage);
	if (e < eNOERROR) ERR(handle, e);
    }

    /*
     * Release log volume lock.
     */
    if (!lockCheck) {
        e = RDsM_ReleaseVolumeLock(handle, RM_PER_THREAD_DS(handle).rm_LogVolumeInfo.volNo); 
        if (e < eNOERROR) ERR(handle, e);
    }
#endif

    /*
     * Discard all entries in the log table.
     */
    e = rm_DeleteAllFromLogTable(handle);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AbortTransaction(handle);
    if (e < eNOERROR) ERR(handle, e);
    
    /*
     * Reset rollback required flag for next transaction
     */
    RM_PER_THREAD_DS(handle).RM_RollbackRequiredFlag = FALSE; 

    return(eNOERROR);
    
} /* RM_AbortTransaction() */
