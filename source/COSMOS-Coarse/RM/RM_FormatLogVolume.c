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
 * Module: RM_FormatLogVolume.c
 *
 * Description:
 *  Format the log volume.
 *
 * Exports:
 *  Four RM_FormatLogVolume(Four)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "RM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


#define PERM 0640                /* permission */

Four RM_FormatLogVolume(
    Four handle,
    Four			volId)			/* IN volume ID */
{
    Four 			e;                     	/* error code */
    Four 			i;                     	
    rm_LogVolumeInfo_t 		logVolInfo;
#ifdef USE_LOG_COHERENCY_VOLUME	
    rm_CoherencyHeaderPage 		headerPage;	
    rm_CoherencyPageInfosPage 	pageInfosPage;
#endif
    RDsM_VolInfo 		volInfo;       
   
    
    TR_PRINT(TR_RM, TR1, ("RM_FormatLogVolume(handle)"));


    e = RDsM_GetVolumeInfo(handle, volId, &volInfo);
    if (e < eNOERROR) ERR(handle, e);
    
    /* Write Log Master Page */
    logVolInfo.volNo = volId;
    logVolInfo.sizeOfExt = volInfo.sizeOfExt;
    logVolInfo.numOfExts = volInfo.numOfExts - NUM_EXTS_FOR_MASTER_PAGES*volInfo.numDevices;
    logVolInfo.onCommitFlag = FALSE;

    logVolInfo.onPrepareFlag = FALSE;
    logVolInfo.isReadOnlyFlag = FALSE;
    logVolInfo.RollbackRequiredFlag = FALSE;
    memcpy(&logVolInfo.globalXactID, "",sizeof(GlobalXactID));
    RM_INIT_ALLOC_POSITION_OF_LOG_VOLUME(logVolInfo); 
       
#ifdef USE_LOG_COHERENCY_VOLUME	
    /*
     * Initialize header page of coherency information in log volume
     */
    headerPage.header.circularListHead = 0;
    headerPage.header.circularListSize = RM_N_COHERENCY_PAGEINFOS;
    headerPage.header.circularListTail = 0;
    headerPage.header.nPageInfos       = 0;
    headerPage.header.timestamp        = 0;

    /*
     * Initialize pageInfos page of coherency information in log volume
     */
    for(i = 0; i < headerPage.header.circularListSize; i++) {
        /* shared memory buffer를 사용시 buffer sync를 위한 hostid 및 shared memory buffer Id */
        pageInfosPage.pageInfos[0].hostId        = NIL;
        pageInfosPage.pageInfos[0].shmBufferId   = NIL;
        pageInfosPage.pageInfos[0].timestamp     = NIL;
        pageInfosPage.pageInfos[0].pageId.pageNo = NIL;
        pageInfosPage.pageInfos[0].pageId.volNo  = NIL;
    }
#endif

#ifdef USE_LOG_COHERENCY_VOLUME		
    /* Write log volume information with coherency information */
    e = rm_WriteLogVolumeInfoWithCoherencyPage(handle, volId, &logVolInfo, &headerPage, &pageInfosPage);
    if (e < eNOERROR) ERR(handle, e);
#else
    e = rm_WriteLogVolumeInfo(handle, volId, &logVolInfo);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(eNOERROR);
    
} /* RM_FormatLogVolume() */
