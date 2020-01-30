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
 * Module: SM_AddIndex.c
 *
 * Description:
 *  Create an index on the given data file. It returns the IndexId which
 *  will be used to identify the index to use.
 *
 * Exports:
 *  Four SM_AddIndex(Four, FileID*, IndexID*, LockParameter*)
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
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_AddIndex( )
 *================================*/
/*
 * Function: Four SM_AddIndex(Four, FileID*, IndexID*, LockParameter*)
 *
 * Description:
 *  Create an index on the given data file. It returns the IndexId which
 *  will be used to identify the index to use.
 *
 * Returns:
 *  Error code
 *   eBADPARAMETER
 *   some errors caused by function calls
 */
Four SM_AddIndex(
    Four handle,
    FileID *fid,		/* IN data file on which an index is created */
    IndexID *iid,		/* OUT index idetifier of the new index */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e;			/* error number */
    Four v;			/* array index on the mount table */
    Four freeEntryNo;		/* unused entry in sm_sysIndexesForTmpFiles */
    KeyValue kval;		/* a key value */
    ObjectID catalogEntry;	/* ObjectID of the catalog entry in SM_SYSTABLES */
    ObjectID oid;		/* new object created in SM_SYSINDEXES */
    sm_CatOverlayForSysIndexes sysIndexesOverlay; /* contents of catalog entry in SM_SYSINDEXES */
    LockReply lockReply;
    LockMode oldMode;
    Boolean tmpFileFlag;	/* TRUE if the new index is defined on a temporary file */
    Four i;			/* temporary variable */
    LogParameter_T logParam;
    PhysicalIndexID pIid;	/* physical index ID, i.e. page ID of index's root */ 
    SegmentID_T pageSegmentID;  /* page segment ID */

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1, ("SM_AddIndex(fid=%P, iid=%P, lockup=%P)", fid, iid, lockup));


    /*@ check parameters */
    if (fid == NULL) ERR(handle, eBADPARAMETER);

    if (iid == NULL) ERR(handle, eBADPARAMETER);

    if(SM_NEED_AUTO_ACTION(handle)) {
	e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
	if(e < eNOERROR) ERR(handle, e);
    }


    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == fid->volNo) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* Check if the file is a temporary file. */
    tmpFileFlag = FALSE; /* initialize */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) 
	if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i]) &&
	    EQUAL_FILEID(*fid, SM_ST_FOR_TMP_FILES(handle)[i].data.fid)) {

	    tmpFileFlag = TRUE;
	    break;
	}

    if (!tmpFileFlag) {
	if(lockup){
	    /* request exclusive commit duration lock on the file */
	    /*check lockup parameter */
	    if(lockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
	    if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

	    e = LM_getFileLock(handle, &MY_XACTID(handle), fid, lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK)
		ERR(handle, eDEADLOCK);
	}

	/* Get the ObjectID of the catalog entry in SM_SYSTABLES. */
	e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &catalogEntry);
	if (e < eNOERROR) ERR(handle, e);

	/* Read the catalog entry. ==> REMOVED */
    }

    /* allocate new logical ID */
    e = sm_GetNewIndexId(handle, v, iid);
    if (e < eNOERROR) ERR(handle, e);

    /*@ Create an index. */
    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, tmpFileFlag);
    e = BtM_CreateIndex(handle, MY_XACT_TABLE_ENTRY(handle), fid->volNo, &pIid, &pageSegmentID, &logParam); 
    if (e < eNOERROR) ERR(handle, e);

    /* Construct an entry of SM_SYSINDEXES. */
    sysIndexesOverlay.dataFid = *fid; /* data file id */
    sysIndexesOverlay.iid = *iid;
    sysIndexesOverlay.rootPage = pIid.pageNo; 
    sysIndexesOverlay.indexType = SM_INDEXTYPE_BTREE; 
    sysIndexesOverlay.pageSegmentID = pageSegmentID; 

    if (tmpFileFlag) {
	/* find the empty temporary index table entry */
	for (freeEntryNo = 0; freeEntryNo < SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(handle); freeEntryNo++) 
	    if (SM_IS_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[freeEntryNo])) break;

	if (freeEntryNo == SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(handle)) { 
	    /* There is no empty entry. */

	    e = Util_doublesizeVarArray(handle, &(sm_perThreadDSptr->sm_sysIndexesForTmpFiles), sizeof(sm_CatOverlayForSysIndexes));
	    if (e < eNOERROR) ERR(handle, e);

	    /* Initialize the newly allocated entries. */
	    for (i = freeEntryNo; i < SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(handle); i++) 
		SM_SET_TO_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[i]);
	}

	/* Register the file in the tempory file table. */
	SM_SI_FOR_TMP_FILES(handle)[freeEntryNo] = sysIndexesOverlay;

    } else {
        SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);

	/* Insert the information on the new index in SM_SYSINDEXES. */
	e = OM_CreateObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesInfo),
			    NULL, NULL,sizeof(sm_CatOverlayForSysIndexes),
			    (char *)&sysIndexesOverlay, &oid, lockup, &logParam); 
	if (e < eNOERROR) ERR(handle, e);


	/*@ construct kval */
	/*
	** Insert the object into the B+ tree on IndexId field of SM_SYSINDEXES.
	*/
	kval.len = sizeof(Two) + sizeof(Four);
	memcpy(&(kval.val[0]), &(iid->volNo), sizeof(Two)); /* volNo is Two. */
	memcpy(&(kval.val[sizeof(Two)]), &(iid->serial), sizeof(Four)); 

#ifdef CCPL
	e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
			     &SM_SYSIDX_INDEXID_KEYDESC, &kval, &oid,
			     lockup, &logParam);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
			     &SM_SYSIDX_INDEXID_KEYDESC, &kval, &oid,
			     lockup, &logParam); 
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */


	/*@ construct kval */
	/*
	** Insert the object into the B+ tree on Data FileID of SM_SYSINDEXES.
	*/
	kval.len = sizeof(Two) + sizeof(Four);
	memcpy(&(kval.val[0]), &(fid->volNo), sizeof(Two)); 
	memcpy(&(kval.val[sizeof(Two)]), &(fid->serial), sizeof(Four)); 

	/* sysIndexesBtreeFileIdIndex => sysIndexesDataFileIdIndex */
	/* SM_SYSIDX_BTREEFILEID_KEYDESC => SM_SYSIDX_DATAFILEID_KEYDESC */
#ifdef CCPL
	e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
			     &SM_SYSIDX_DATAFILEID_KEYDESC, &kval, &oid,
			     lockup, &logParam);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
			     &SM_SYSIDX_DATAFILEID_KEYDESC, &kval, &oid,
			     lockup, &logParam); 
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */
    }

    if(ACTION_ON(handle)){ 
	e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
	if(e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* SM_AddIndex( ) */

