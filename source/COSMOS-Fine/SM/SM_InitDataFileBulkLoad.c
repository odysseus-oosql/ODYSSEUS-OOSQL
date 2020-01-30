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
 * Module: SM_InitDataFileBulkLoad.c
 *
 * Description:
 *  Initialize data file bulk load.
 *
 * Exports:
 *  Four SM_InitDataFileBulkLoad(FileID*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*, Boolean, Two, Two)
 */

#include <string.h>
#include "common.h"
#include "error.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "OM.h"
#include "BL_OM.h"
#include "BtM.h"
#include "SM.h"
#include "BL_SM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@========================================
 *  SM_InitDataFileBulkLoad()
 * =======================================*/

/*
 * Function : Four SM_InitDataFileBulkLoad()
 *
 * Description :
 *  Initialize data file bulk loading.
 *
 * Return Values :
 *  bulkload ID
 *  error code.
 *
 * Side Effects :
 *    0)
 */
Four SM_InitDataFileBulkLoad(
    Four		    handle,
    VolID                   tmpVolId,    /* IN  temporary volume in which sort stream is created */ 
    FileID                  *fid,        /* IN  file that data bulk load is to be processed */
    SortKeyDesc             *kdesc,      /* IN  sort key description */
    GetKeyAttrsFuncPtr_T    getKeyAttrs, /* IN  object analysis function */
    void                    *schema,     /* IN  schema for analysis function */
    Boolean                 isNeedSort,  /* IN  flag indicating input data must be sorted by clustering index key */
    Two                     pff,         /* IN  Page fill factor */
    Two                     eff,         /* IN  Extent fill factor */
    PageID                  *firstPageID,/* OUT first page ID of this bulkload */
    LockParameter           *lockup)     /* IN lockup parameter */

{
    Four                    e;           /* error number */
    Four                    v;           /* array index on scan manager mount table */
    Four                    blkLdId;     /* bulkload ID */ 
    DataFileInfo            finfo;       /* data file info */
    ObjectID                catObjForDataFile;  /* catalog object id for data file bulk load */

    LockReply               lockReply;
    LockMode                oldMode;
    LogParameter_T          logParam;


    /* Check parameters */
    if (fid == NULL) ERR(handle, eBADPARAMETER);
    if (isNeedSort == TRUE && (kdesc == NULL || getKeyAttrs == NULL)) ERR(handle, eBADPARAMETER);


    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == fid->volNo) break;  /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* lock on the data file */
    if (lockup) {

        e = LM_getFileLock(handle, &MY_XACTID(handle), fid, L_X, L_COMMIT, L_UNCONDITIONAL, &lockReply, &oldMode);
        if ( e < eNOERROR ) ERR(handle, e);
        if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);

	}

    /* set datafile information */
    finfo.fid = *fid;
    finfo.tmpFileFlag = FALSE;
    e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &(finfo.catalog.oid));
    if (e < eNOERROR) ERR(handle, e);

    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);

    /* Initialize data file bulkload in OM level */
    blkLdId = OM_InitBulkLoad(handle, MY_XACT_TABLE_ENTRY(handle), tmpVolId, &finfo, kdesc, (omGetKeyAttrsFuncPtr_T) getKeyAttrs, schema,
			      isNeedSort, pff, eff, firstPageID, &logParam);
    if(blkLdId < 0) ERR(handle, blkLdId);


    return(blkLdId);

}
