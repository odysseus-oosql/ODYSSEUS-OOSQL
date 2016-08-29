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
#include <string.h>  /* for memcpy() */
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "RM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*@================================
 * rm_ReadLogVolumeInfo()
 *================================*/
/*
 * Function: Four rm_ReadLogVolumeInfo(rm_LogVolumeInfo_t*)
 *
 * Description:
 *  Read the log volume information from the log volume.
 *
 * Returns:
 *  error code
 */
Four rm_ReadLogVolumeInfo(
    Four handle,
    Four 			volId,                  /* IN volume id for log volume */ 
    rm_LogVolumeInfo_t 		*logVolInfo) 		/* OUT log volume information */
{
    Four 			e;
    rm_LogVolumeInfoPage_t 	logVolumeInfoPage; 	
    PageID 			pid;                 	
    
    TR_PRINT(TR_RM, TR1, ("rm_ReadLogVolumeInfo(handle, logVolInfo=%P)", logVolInfo));


#ifdef DBLOCK
#ifndef NDEBUG
    /* check lock for log volume is acquired */
    if ( !RDsM_CheckVolumeLock(handle, volId, L_S) ) ERR(handle, eINTERNAL);
#endif
#endif


    pid.volNo = volId;
    pid.pageNo = RM_LOG_VOLUME_INFO_PAGE_NO;

    /* read logVolumeInfoPage from disk */
    e = RDsM_ReadTrainForLogVolume(handle, &pid, (char*)&logVolumeInfoPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    /* get logVolInfo from logVolumeInfoPage */
    memcpy(logVolInfo, &logVolumeInfoPage.logVolumeInfo, sizeof(rm_LogVolumeInfo_t));
    
    return(eNOERROR);
    
} /* rm_ReadLogVolumeInfo() */



/*@================================
 * rm_WriteLogVolumeInfo()
 *================================*/
/*
 * Function: Four rm_WriteLogVolumeInfo(rm_LogVolumeInfo_t*)
 *
 * Description:
 *  Write the log volume information into the log volume.
 *
 * Returns:
 *  error code
 */
Four rm_WriteLogVolumeInfo(
    Four handle,
    Four 			volId,                  /* IN volume id for log volume */ 
    rm_LogVolumeInfo_t 		*logVolInfo) 		/* IN log volume information */
{
    Four 			e;
    rm_LogVolumeInfoPage_t 	logVolumeInfoPage; 	
    PageID 			pid;                 	
    
    TR_PRINT(TR_RM, TR1, ("rm_WriteLogVolumeInfo(handle, logVolInfo=%P)", logVolInfo));


#ifdef DBLOCK
#ifndef NDEBUG
    /* check lock for log volume is acquired */
    if ( !RDsM_CheckVolumeLock(handle, volId, L_X) ) ERR(handle, eINTERNAL);
#endif
#endif


    pid.volNo = volId;
    pid.pageNo = RM_LOG_VOLUME_INFO_PAGE_NO;

    /* write logVolInfo into logVolumeInfoPage */
    memcpy(&logVolumeInfoPage.logVolumeInfo, logVolInfo, sizeof(rm_LogVolumeInfo_t));

    /* write logVulumeInfoPage into disk */
    e = RDsM_WriteTrainForLogVolume(handle, (char*)&logVolumeInfoPage, &pid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
    
} /* rm_WriteLogVolumeInfo() */

#ifdef USE_LOG_COHERENCY_VOLUME

/*@========================================
 * rm_ReadLogVolumeInfoWithCoherencyPage()
 *=======================================*/
/*
 * Function: Four rm_ReadLogVolumeInfoWithCoherencyPage(rm_LogVolumeInfo_t*)
 *
 * Description:
 *  Read the log volume information with coherency information 
 *  from the log volume.
 *
 * Returns:
 *  error code
 */
Four rm_ReadLogVolumeInfoWithCoherencyPage(
    Four handle,
    Four  			volId,          	/* IN volume id for log volume */ 
    rm_LogVolumeInfo_t* 	logVolInfo, 		/* OUT log volume information */
    rm_CoherencyHeaderPage*	coherencyHeaderPage,	/* OUT header page for buffer coherency */
    rm_CoherencyPageInfosPage*	coherencyPageInfosPage)	/* OUT page informations page have to be synchronized */
{
    Four 			e;
    rm_LogVolumeInfoPage_t 	logVolumeInfoPage; 	
    PageID 			pid;                 	
    
    TR_PRINT(TR_RM, TR1, ("rm_ReadLogVolumeInfoWithCoherencyPage(handle, logVolInfo=%P)", logVolInfo));


#ifdef DBLOCK
#ifndef NDEBUG
    /* check lock for log volume is acquired */
    if ( !RDsM_CheckVolumeLock(handle, volId, L_S) ) ERR(handle, eINTERNAL);
#endif
#endif

    /* 
     * This part of codes must be optimized.
     * Read three pages from disk bypassing RDSM layer.
     */
    /* read logVolumeInfoPage from disk */
    pid.volNo = volId;
    pid.pageNo = RM_LOG_VOLUME_INFO_PAGE_NO;

    e = RDsM_ReadTrainForLogVolume(handle, &pid, (char*)&logVolumeInfoPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    memcpy(logVolInfo, &logVolumeInfoPage.logVolumeInfo, sizeof(rm_LogVolumeInfo_t));

    /* read coherency header page from disk */
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_HEADER_PAGE_NO;
    e = RDsM_ReadTrainForLogVolume(handle, &pid, (char*)coherencyHeaderPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);
    
    /* read coherency page infos page from disk */
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_PAGEINFOS_PAGE_NO;
    e = RDsM_ReadTrainForLogVolume(handle, &pid, (char*)coherencyPageInfosPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);
    
    return(eNOERROR);
    
} /* rm_ReadLogVolumeInfo() */

/*@=========================================
 * rm_WriteLogVolumeInfoWithCoherencyPage()
 *========================================*/
/*
 * Function: Four rm_WriteLogVolumeInfoWithCoherencyPage(rm_LogVolumeInfo_t*)
 *
 * Description:
 *  Write the log volume information with coherency information 
 *  into the log volume.
 *
 * Returns:
 *  error code
 */
Four rm_WriteLogVolumeInfoWithCoherencyPage(
    Four handle,
    Four 			volId,              	/* IN volume id for log volume */ 
    rm_LogVolumeInfo_t* 	logVolInfo, 		/* IN log volume information */
    rm_CoherencyHeaderPage*	coherencyHeaderPage,	/* IN header page for buffer coherency */
    rm_CoherencyPageInfosPage*	coherencyPageInfosPage)	/* IN page informations have to be synchronized */
{
    Four 			e;
    rm_LogVolumeInfoPage_t 	logVolumeInfoPage; 	
    PageID 			pid;                 	
    
    TR_PRINT(TR_RM, TR1, ("rm_WriteLogVolumeInfo(handle, logVolInfo=%P)", logVolInfo));


#ifdef DBLOCK
#ifndef NDEBUG
    /* check lock for log volume is acquired */
    if ( !RDsM_CheckVolumeLock(handle, volId, L_X) ) ERR(handle, eINTERNAL);
#endif
#endif

    /*
     * This part of codes must be optimized.
     * Write three pages into disk bypassing RDSM layer.
     */
    memcpy(&logVolumeInfoPage.logVolumeInfo, logVolInfo, sizeof(rm_LogVolumeInfo_t));

    /* write logVulumeInfoPage into disk */
    pid.volNo = volId;
    pid.pageNo = RM_LOG_VOLUME_INFO_PAGE_NO;

    e = RDsM_WriteTrainForLogVolume(handle, (char*)&logVolumeInfoPage, &pid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    /* write coherency header page into disk */
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_HEADER_PAGE_NO;
    
    e = RDsM_WriteTrainForLogVolume(handle, (char*)coherencyHeaderPage, &pid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    /* write coherency infos page into disk */
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_PAGEINFOS_PAGE_NO;
    
    e = RDsM_WriteTrainForLogVolume(handle, (char*)coherencyPageInfosPage, &pid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
    
} /* rm_WriteLogVolumeInfo() */

/*@=========================================
 * rm_ReadCoherencyPage()
 *========================================*/
/*
 * Function: Four rm_ReadCoherencyPage(Four, rm_CoherencyHeaderPage*, rm_CoherencyPageInfosPage* )
 *
 * Description:
 *  Read coherency information from log volume.
 *
 * Returns:
 *  error code
 */
Four rm_ReadCoherencyPage(
    Four handle,
    Four 			volId,          	/* IN volume id for log volume */ 
    rm_CoherencyHeaderPage*	coherencyHeaderPage,	/* OUT header page for buffer coherency */
    rm_CoherencyPageInfosPage*	coherencyPageInfosPage)	/* OUT page informations page have to be synchronized */
{
    Four 			e;
    rm_LogVolumeInfoPage_t 	logVolumeInfoPage; 
    PageID 			pid;                 	
    
    TR_PRINT(TR_RM, TR1, ("rm_ReadLogVolumeInfoWithCoherencyPage(handle, logVolInfo=%P)", logVolInfo));

#ifdef DBLOCK
#ifndef NDEBUG
    /* check lock for log volume is acquired */
    if ( !RDsM_CheckVolumeLock(handle, volId, L_S) ) ERR(handle, eINTERNAL);
#endif
#endif

    /* 
     * This part of codes must be optimized.
     * Read three pages from disk bypassing RDSM layer.
     */
    /* read coherency header page from disk */
    pid.volNo = volId;
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_HEADER_PAGE_NO;
    e = RDsM_ReadTrainForLogVolume(handle, &pid, (char*)coherencyHeaderPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);
    
    /* read coherency page infos page from disk */
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_PAGEINFOS_PAGE_NO;
    e = RDsM_ReadTrainForLogVolume(handle, &pid, (char*)coherencyPageInfosPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);
    
    return(eNOERROR);
    
} /* rm_ReadLogVolumeInfo() */

/*@=========================================
 * rm_WriteCoherencyPage()
 *========================================*/
/*
 * Function: Four rm_WriteCoherencyPage(Four, rm_CoherencyHeaderPage*, rm_CoherencyPageInfosPage* )
 *
 * Description:
 *  Write coherency information into log volume.
 *
 * Returns:
 *  error code
 */
Four rm_WriteCoherencyPage(
    Four handle,
    Four 			volId,              	/* IN volume id for log volume */ 
    rm_CoherencyHeaderPage*	coherencyHeaderPage,	/* IN header page for buffer coherency */
    rm_CoherencyPageInfosPage*	coherencyPageInfosPage)	/* IN page informations have to be synchronized */
{
    Four 			e;
    rm_LogVolumeInfoPage_t 	logVolumeInfoPage; 	
    PageID 			pid;                 	
    
    TR_PRINT(TR_RM, TR1, ("rm_WriteLogVolumeInfo(handle, logVolInfo=%P)", logVolInfo));

#ifdef DBLOCK
#ifndef NDEBUG
    /* check lock for log volume is acquired */
    if ( !RDsM_CheckVolumeLock(handle, volId, L_X) ) ERR(handle, eINTERNAL);
#endif
#endif

    /*
     * This part of codes must be optimized.
     * Write three pages into disk bypassing RDSM layer.
     */
    /* write coherency header page into disk */
    pid.volNo = volId;
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_HEADER_PAGE_NO;
    
    e = RDsM_WriteTrainForLogVolume(handle, (char*)coherencyHeaderPage, &pid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    /* write coherency infos page into disk */
    pid.pageNo = RM_LOG_VOLUME_COHERENCY_PAGEINFOS_PAGE_NO;
    
    e = RDsM_WriteTrainForLogVolume(handle, (char*)coherencyPageInfosPage, &pid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
    
} /* rm_WriteLogVolumeInfo() */

#endif


