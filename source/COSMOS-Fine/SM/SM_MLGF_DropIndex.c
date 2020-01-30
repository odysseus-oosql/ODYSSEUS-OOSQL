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
 * Module: SM_MLGF_DropIndex.c
 *
 * Description:
 *  Drop the given MLGF index.
 *
 * Exports:
 *  Four SM_MLGF_DropIndex(Four, handle, IndexID*, LockParameter*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"	
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "MLGF.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_MLGF_DropIndex
 *================================*/
/*
 * Function: Four SM_MLGF_DropIndex(Four, handle, IndexID*, LockParameter*)
 *
 * Description:
 *  Drop the given MLGF index.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eOPENINDEX_SM
 *    some errors caused by function calls
 */
Four SM_MLGF_DropIndex(
    Four handle,
    IndexID *index,		/* IN index to drop */
    LockParameter *lockup)       /* IN request lock or not */
{
    Four e;			/* error number */
    Four v;			/* index for the used volume on the mount table */
    Four i;			/* temporary variable */
    KeyValue kval;		/* key value of a B+ tree */
    BtreeCursor schBid;		/* a B+ tree cursor */
    sm_CatOverlayForSysIndexes sysIndexesOverlay; /* entry of SM_SYSINDEXES */
    LockReply lockReply;
    LockMode oldMode;
    LockParameter indexLockup, *indexLockupPtr; 
    Boolean tmpFileFlag;       /* TRUE if the index is defined on a temporary file */
    Four entryNo;	       /* entry no of an entry of sm_sysIndexesForTmpFiles */
    LogParameter_T logParam;
    PhysicalIndexID pIid;	/* physical index ID */ 

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1, ("SM_MLGF_DropIndex(index=%P, lockup=%P)", index, lockup));


    /*@ check parameters */
    if (index == NULL) ERR(handle, eBADPARAMETER);

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == index->volNo) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    /*@ for each entry */
    /*
    ** Check if a scan is opened on the dropped index.
    */
    for (i = 0; i < sm_perThreadDSptr->smScanTable.nEntries; i++)
	if (SM_SCANTABLE(handle)[i].scanType == MLGFINDEX &&
	    EQUAL_INDEXID(SM_SCANTABLE(handle)[i].scanInfo.mlgf.iinfo.iid, *index))
	    ERR(handle, eOPENINDEX_SM);


    /*
    ** Check if the index is defined on a temporary file.
    */
    tmpFileFlag = FALSE; /* initialize */
    for (entryNo = 0; entryNo < SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(handle); entryNo++) 
	if (!SM_IS_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[entryNo]) &&
	    EQUAL_INDEXID(*index, SM_SI_FOR_TMP_FILES(handle)[entryNo].iid)) {

	    tmpFileFlag = TRUE;
	    break;
	}

    if (tmpFileFlag) {
	/* Drop the index. */
	/* We don't deffer the dropping of the temporary files/indexes. */
	MAKE_PHYSICALINDEXID(pIid, index->volNo, SM_SI_FOR_TMP_FILES(handle)[entryNo].rootPage);

        SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, TRUE);

	e = MLGF_DropIndex(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SI_FOR_TMP_FILES(handle)[entryNo].iid), &pIid,
			   &(SM_SI_FOR_TMP_FILES(handle)[entryNo].pageSegmentID), TRUE, &logParam); 
	if (e < eNOERROR) ERR(handle, e);

	/* Remove the corresponding entry from the sm_sysIndexesForTmpFiles. */
	SM_SET_TO_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[entryNo]);

    } else {

        SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);

	/*
	** Get the ObjectID of the catalog entry in SM_SYSINDEXES.
	*/
	if(lockup){
		indexLockup.mode = L_S;
		indexLockup.duration = L_COMMIT;
		indexLockupPtr = &indexLockup;
	} else indexLockupPtr = NULL;

	/* Construct a key for the B+ tree index on IndexID field of SM_SYSINDEXES. */
	kval.len = sizeof(Two) + sizeof(Four);
	memcpy(&(kval.val[0]), &(index->volNo), sizeof(Two)); /* volNo is Two. */
	memcpy(&(kval.val[sizeof(Two)]), &(index->serial), sizeof(Four)); 

	e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo), 
		      &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
                      &SM_SYSIDX_INDEXID_KEYDESC, &kval, SM_EQ, &kval, SM_EQ, &schBid, NULL, indexLockupPtr); 
	if (e < eNOERROR) ERR(handle, e);

	/* The IndexID 'index' is invalid. */
	if (schBid.flag != CURSOR_ON) ERR(handle, eBADINDEXID);

	/* Read the catalog object to get the FileID of the Data File. */
	e = OM_ReadObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesInfo.fid),
			  &schBid.oid, 0, sizeof(sm_CatOverlayForSysIndexes),
			  (char *)&sysIndexesOverlay, indexLockupPtr); 
	if (e < eNOERROR) ERR(handle, e);

	if(lockup){
	    /* request commit duration exclusive lock on the file */
	    /*check lockup parameter */
	    if(lockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
	    if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

	    /* get lock on the data file */
	    e = LM_getFileLock(handle,  &MY_XACTID(handle), &(sysIndexesOverlay.dataFid), lockup->mode,
				lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if ( e < eNOERROR ) ERR(handle, e);

	    if ( lockReply == LR_DEADLOCK)
		ERR(handle, eDEADLOCK);
	}

	/* Delete the catalog entry from the B+ tree on IndexID field of SM_SYSINDEXES . */
	e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
			     &SM_SYSIDX_INDEXID_KEYDESC,
			     &kval, &schBid.oid, lockup, &logParam); 
	if (e < eNOERROR) ERR(handle, e);

	/* Delete the catalog entry from the B+ tree on Data FileID field of SM_SYSINDEXES . */
	/* Construct a key for the B+ tree index on DFid field of SM_SYSINDEXES. */
	kval.len = sizeof(Two) + sizeof(Four);
	memcpy(&(kval.val[0]), &((sysIndexesOverlay.dataFid).volNo), sizeof(Two)); /* volNo is Two. */
	memcpy(&(kval.val[sizeof(Two)]), &((sysIndexesOverlay.dataFid).serial), sizeof(Four)); 
	e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
			     &SM_SYSIDX_DATAFILEID_KEYDESC,
			     &kval, &schBid.oid, lockup, &logParam); 
	if (e < eNOERROR) ERR(handle, e);

	/* Delete the catalog entry from the data file SM_SYSINDEXES. */
	e = OM_DestroyObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesInfo),
			     &schBid.oid, lockup, &logParam); 
	if (e < eNOERROR) ERR(handle, e);

	/* get physiscal index ID */
	MAKE_PHYSICALINDEXID(pIid, index->volNo, sysIndexesOverlay.rootPage);

	/* Destroy Index
	 * for individual transaction rollback,
	 * Insert a deallocated node for the dropped index.
	 */

	e = MLGF_DropIndex(handle, MY_XACT_TABLE_ENTRY(handle), &(sysIndexesOverlay.iid), &pIid, &(sysIndexesOverlay.pageSegmentID), FALSE, &logParam); 
	if (e < eNOERROR) ERR(handle, e);
    }

    if(ACTION_ON(handle)){  
	e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
        if(e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* SM_MLGF_DropIndex() */


