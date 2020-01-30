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
 * Module: SM_CreateFile.c
 *
 * Description:
 *  Create a file in the specified volume, 'volId'. SM_CreateFile( ) returns
 *  FileID 'fid' with which the user can identify the file from the others.
 *
 * Exports:
 *  Four SM_CreateFile(Four, Four, FileID*, Boolean, LockParameter*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LOG.h"
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_CreateFile( )
 *================================*/
/*
 * Function: Four SM_CreateFile(Four, Four, FileID*, Boolean, LockParameter*)
 *
 * Description:
 *  Create a file in the specified volume, 'volId'. SM_CreateFile( ) returns
 *  FileID 'fid' with which the user can identify the file from the others.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eNOTMOUNTEDVOLUME_SM
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter fid
 *     'fid' is filled with the newly created file's identifier.
 */
Four SM_CreateFile(
    Four handle,
    Four volId,			/* IN on which volume to put the file */
    FileID *fid,		/* OUT newly created file's FileID */
    Boolean tmpFileFlag,	/* IN TRUE if the file is a temporary file */
    LockParameter *lockup)	/* IN request lock or not */
{
    Four   e;			/* error code */
    Four   v;			/* array index on scan manager mount table */
    Four   freeEntryNo;		/* unused entry in sm_sysTablesForTmpFiles */
    KeyValue kval;		/* a key value */
    ObjectID oid;		/* an ObjectID */
    sm_CatOverlayForSysTables sysTablesOverlay; /* entry of SM_SYSTABLES */
    LockReply lockReply;
    LockMode oldMode;
    Four i;			/* temporary variable */
    LogParameter_T logParam;    /* log parameter */

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1,
	     ("SM_CreateFile(handle, volId=%ld, fid=%P, tmpFileFlag=%ld, lockup=%P)",
	      volId, fid, tmpFileFlag, lockup));


    /*@
    ** check parameters
    */
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == volId) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    if (fid == NULL) ERR(handle, eBADPARAMETER);

    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, tmpFileFlag);

    /* allocate data file ID */
    e = sm_GetNewFileId(handle, v, fid);
    if (e < eNOERROR) ERR(handle, e);

    /*@ create the file */
    /* Create a data file in the given volume.*/
    e = OM_CreateFile(handle, MY_XACT_TABLE_ENTRY(handle), fid, &(sysTablesOverlay.data), &logParam);
    if (e < eNOERROR) ERR(handle, e);

    if (tmpFileFlag) {

	/* find the empty temporary file table entry */
	for (freeEntryNo = 0; freeEntryNo < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); freeEntryNo++) 
	    if (SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[freeEntryNo])) break;

	if (freeEntryNo == SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle)) { 
	    /* There is no empty entry. */

	    e = Util_doublesizeVarArray(handle, &(sm_perThreadDSptr->sm_sysTablesForTmpFiles), sizeof(sm_CatOverlayForSysTables));
	    if (e < eNOERROR) ERR(handle, e);

	    /* Initialize the newly allocated entries. */
	    for (i = freeEntryNo; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) 
		SM_SET_TO_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i]);
	}

	/* Register the file in the tempory file table. */
	SM_ST_FOR_TMP_FILES(handle)[freeEntryNo] = sysTablesOverlay;

    } else {
	if(lockup){
	    /* request exclusive commit duration file lock */
	    if(lockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
	    if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

	    e = LM_getFileLock(handle, &MY_XACTID(handle), &(sysTablesOverlay.data.fid),
			       lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if(e < eNOERROR) ERR(handle, e);

	    if ( lockReply == LR_DEADLOCK)
		ERR(handle, eDEADLOCK);
	}

	/* Create a B+-tree file in the given volume. ==> REMOVED */


	/*
	** Register the files in the catalog table SM_SYSTABLES.
	*/
	e = OM_CreateObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysTablesInfo),
			    NULL, NULL, sizeof(sm_CatOverlayForSysTables),
			    (char *)&sysTablesOverlay, &oid, NULL, &logParam);
	if (e < eNOERROR) ERR(handle, e);

	/*@ construct kval */
	/* Insert the new ObjectID into B+ tree on data FileID of SM_SYSTABLES. */
	kval.len = sizeof(Two) + sizeof(Four);
	memcpy(&(kval.val[0]), &(sysTablesOverlay.data.fid.volNo), sizeof(Two));
	memcpy(&(kval.val[sizeof(Two)]), &(sysTablesOverlay.data.fid.serial), sizeof(Four)); 

#ifdef CCPL
	e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysTablesInfo.fid), 
			     &SM_SYSTBL_DFILEIDIDX_KEYDESC,
			     &kval, &oid, lockup, &logParam);
#endif /* CCPL */

#ifdef CCRL
	e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysTablesInfo.fid), 
			     &SM_SYSTBL_DFILEIDIDX_KEYDESC,
			     &kval, &oid, NULL, &logParam);
#endif /* CCRL */
	if (e < eNOERROR) ERR(handle, e);

	/* Insert the new ObjectID into B+ tree on Btree FileID of SM_SYSTABLES. ==> REMOVED */
    }

    /* return the FileId */
    *fid = sysTablesOverlay.data.fid;

    if(ACTION_ON(handle)){  
	e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
        if(e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* SM_CreateFile( ) */




