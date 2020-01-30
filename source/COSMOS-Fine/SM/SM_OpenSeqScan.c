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
 * Module: SM_OpenSeqScan.c
 *
 * Description:
 *  Open a sequential scan.
 *
 * Exports:
 *  Four SM_OpenSeqScan(Four, FileID*, Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_OpenSeqScan( )
 *================================*/
/*
 * Function: Four SM_OpenSeqScan(Four, FileID*, Four)
 *
 * Description:
 *  Open a sequential scan. The scan is used to fetch all objects in the
 *  given data file. The scan order is specified by 'direction'; FORWARD
 *  means the physical order and BACKWARD the reverse physical order.
 *
 * Returns:
 *  1) Scan Identifier - the return value is 0 or positive number
 *  2) Error code - negative number
 *       eBADPARAMETER
 *       eNOTMOUNTEDVOLUME_SM
 *       some errors caused by function calls
 */
Four SM_OpenSeqScan(
    Four handle,
    FileID *fid,		/* IN file to open */
    Four   direction,		/* IN direction of the scan */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e;			/* error number */
    Four v;			/* index for the used volume on the mount table */
    Four i;			/* temporary variable */
    Four scanId;		/* scan identifier of the new scan */
    LockMode lockMode;
    LockReply lockReply;
    LockMode oldMode;

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1,
	     ("SM_OpenSeqScan(handle, fid=%P, direction=%ld, lockup=%P)", fid, direction, lockup));


    /*@
    ** Check parameters
    */
    if (fid == NULL) ERR(handle, eBADPARAMETER);

    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == fid->volNo) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);


    /*@ for each entry */
    /* find the empty scan table entry */
    for (scanId = 0; scanId < sm_perThreadDSptr->smScanTable.nEntries; scanId++)
	if (SM_SCANTABLE(handle)[scanId].scanType == NIL) break;

    if (scanId == sm_perThreadDSptr->smScanTable.nEntries) {
	/* There is no empty entry. */

	e = Util_doublesizeVarArray(handle, &(sm_perThreadDSptr->smScanTable), sizeof(sm_ScanTableEntry));
	if (e < eNOERROR) ERR(handle, e);

	/* Initialize the newly allocated entries. */
	for (i = scanId; i < sm_perThreadDSptr->smScanTable.nEntries; i++)
            SM_SCANTABLE(handle)[i].scanType = NIL; 
    }

    SM_SCANTABLE(handle)[scanId].acquiredFileLock = L_NL; 

    /* Check if the file is a temporary file. */
    SM_SCANTABLE(handle)[scanId].finfo.tmpFileFlag = FALSE; /* initialize */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) 
	if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i]) &&
	    EQUAL_FILEID(*fid, SM_ST_FOR_TMP_FILES(handle)[i].data.fid)) {

	    SM_SCANTABLE(handle)[scanId].finfo.tmpFileFlag = TRUE;
	    SM_SCANTABLE(handle)[scanId].finfo.catalog.entry = &(SM_ST_FOR_TMP_FILES(handle)[i]);
	    break;
	}

    if (!SM_SCANTABLE(handle)[scanId].finfo.tmpFileFlag) {
	if (lockup) {
	    /* check the lockup mode */
	    switch(lockup->mode){
	      case L_SIX:
	      case L_S:
	      case L_X: lockMode = lockup->mode; break;
	      case L_IS: lockMode = L_S; break;
	      case L_IX: lockMode = L_SIX; break;
	      default : ERR(handle, eBADLOCKMODE_SM);
	    }
	    if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

	    /* lock on the data file */
	    e = LM_getFileLock(handle,  &MY_XACTID(handle), fid, lockMode, lockup->duration,
				L_UNCONDITIONAL, &lockReply, &oldMode);
	    if ( e < eNOERROR ) ERR(handle, e);

	    if ( lockReply == LR_DEADLOCK)
		ERR(handle, eDEADLOCK);

	    /* acquiredFileLock is the lock mode to be acquired */
	    /* old code -> SM_SCANTABLE(handle)[scanId].acquiredFileLock = lockup->mode; */
	    SM_SCANTABLE(handle)[scanId].acquiredFileLock = (LockMode)lockReply;

	}

	/* Get catalog entry of */
	e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &(SM_SCANTABLE(handle)[scanId].finfo.catalog.oid));
	if (e < eNOERROR) ERR(handle, e);
    }

    SM_SCANTABLE(handle)[scanId].finfo.fid = *fid;
    SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_BOS;
    SM_SCANTABLE(handle)[scanId].scanInfo.seq.direction = direction;
    SM_SCANTABLE(handle)[scanId].scanType = SEQUENTIAL;

    if(ACTION_ON(handle)){  
	e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
        if(e < eNOERROR) ERR(handle, e);
    }

    return(scanId);

} /* SM_OpenSeqScan() */
