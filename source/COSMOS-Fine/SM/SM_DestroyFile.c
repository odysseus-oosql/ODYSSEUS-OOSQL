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
 * Module: SM_DestroyFile.c
 *
 * Description:
 *  SM_DestroyFile( ) destroys the specified file from the volume.
 *  It also destroys the corresponding B+-tree file from the volume.
 *
 * Exports:
 *  Four SM_DestroyFile(Four, FileID*, LockParameter*)
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
 * SM_DestroyFile( )
 *================================*/
/*
 * Function: Four SM_DestroyFile(Four, FileID*, LockParameter*)
 *
 * Description:
 *  SM_DestroyFile( ) destroys the specified file from the volume.
 *  It also destroys the corresponding B+-tree file from the volume.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eOPENFILE_SM
 *    some errors caused by function calls
 */
Four SM_DestroyFile(
    Four handle,
    FileID *fid,		/* IN file to destroy */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e;			/* error code */
    Four v;			/* array index on scan manager mount table */
    Four i;			/* temporary variable */
    KeyValue kval;		/* a key value */
    KeyValue kval2;		/* another key value */
    BtreeCursor curBid;		/* a B+ tree cursor */
    BtreeCursor nextBid;	/* a B+ tree cursor */
    ObjectID catalogEntry;	/* ObjectID of the catalog entry in SM_SYSTABLES */
    sm_CatOverlayForSysIndexes sysIndexesOverlay; /* entry of SM_SYSINDEXES */
    LockReply lockReply;
    LockMode oldMode;
    LockParameter indexLockup;  /* lockup parameter for the index of catalog */ 
    Boolean tmpFileFlag;	/* TRUE if the new index is defined on a temporary file */
    Four entryNo;	       /* entry no of an entry of sm_sysIndexesForTmpFiles */
    LogParameter_T logParam;
    PhysicalFileID pFid;	/* physical file ID */
    PhysicalIndexID pIid;	/* physical index ID */
    sm_CatOverlayForSysTables sysTablesOverlay; /* entry of SM_SYSTABLES */


    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1, ("SM_DestroyFile(fid=%P, lockup=%P)", fid, lockup));


    /*@ check parameter */
    if (fid == NULL) ERR(handle, eBADPARAMETER);

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == fid->volNo) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);


    /*@ for each entry */
    /*
    ** Check if a scan is opened on the destroyed file.
    */
    for (i = 0; i < sm_perThreadDSptr->smScanTable.nEntries; i++)
	if (SM_SCANTABLE(handle)[i].scanType != NIL &&
	    EQUAL_FILEID(SM_SCANTABLE(handle)[i].finfo.fid, *fid))
	    ERR(handle, eOPENFILE_SM);

    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    /*
    ** Check if the index is defined on a temporary file.
    */
    tmpFileFlag = FALSE; /* initialize */
    for (entryNo = 0; entryNo < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); entryNo++) 
	if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[entryNo]) &&
	    EQUAL_FILEID(*fid, SM_ST_FOR_TMP_FILES(handle)[entryNo].data.fid)) {

	    tmpFileFlag = TRUE;
	    break;
	}

    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, tmpFileFlag);

    if (tmpFileFlag) {

	/* Drop all indexes defined on the given data file. */
	for (i = 0; i < SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(handle); i++) { 
	    if (!(SM_IS_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[i])) &&
		EQUAL_FILEID(*fid, SM_SI_FOR_TMP_FILES(handle)[i].dataFid)) {

		/* get physical index ID */
		MAKE_PHYSICALFILEID(pIid, fid->volNo, SM_SI_FOR_TMP_FILES(handle)[i].rootPage);

		/* Drop the index. */
		/* We don't deffer the dropping of the temporary files/indexes. */
		switch (SM_SI_FOR_TMP_FILES(handle)[i].indexType) {
		  case SM_INDEXTYPE_BTREE:
		    e = BtM_DropIndex(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SI_FOR_TMP_FILES(handle)[i].iid), &pIid,
				      &(SM_SI_FOR_TMP_FILES(handle)[i].pageSegmentID), TRUE, &logParam); 
		    if (e < eNOERROR) ERR(handle, e);
		    break;

		  case SM_INDEXTYPE_MLGF:
		    e = MLGF_DropIndex(handle, MY_XACT_TABLE_ENTRY(handle),  &(SM_SI_FOR_TMP_FILES(handle)[i].iid), &pIid,
				       &(SM_SI_FOR_TMP_FILES(handle)[i].pageSegmentID), TRUE, &logParam); 
		    if (e < eNOERROR) ERR(handle, e);
		    break;

		  default:
		    ERR(handle, eINTERNAL);
		}

		/* Clear this entry. */
		SM_SET_TO_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[i]);
	    }
	}

	/*@ Destroy the data file. */
	/* We don't deffer the dropping of the temporary files/indexes. */
	MAKE_PHYSICALFILEID(pFid, fid->volNo, SM_ST_FOR_TMP_FILES(handle)[entryNo].data.firstPage);
	e = OM_DropFile(handle, MY_XACT_TABLE_ENTRY(handle), &pFid,
			&(SM_ST_FOR_TMP_FILES(handle)[entryNo].data.pageSegmentID), 
			&(SM_ST_FOR_TMP_FILES(handle)[entryNo].data.trainSegmentID), 
			TRUE, &logParam);
	if (e < eNOERROR) ERR(handle, e);

	/* Clear this entry. */
	SM_SET_TO_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[entryNo]);

    } else {
	if (lockup) {

	    /*check lockup parameter */
	    if(lockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
	    if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

	    if ( (e = LM_getFileLock(handle,  &MY_XACTID(handle), fid, lockup->mode,
				      lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode)) < eNOERROR )
		ERR(handle, e);

	    if ( lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);


#ifdef CCPL 
	    /* set the index lockup */
	    indexLockup.mode = L_S;
	    indexLockup.duration = L_COMMIT;
#endif 

	}

	/* Get the ObjectID of the catalog entry in SM_SYSTABLES. */
	e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &catalogEntry);
	if (e < eNOERROR) ERR(handle, e);

        /* Read the catalog entry. */
	e = OM_ReadObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysTablesInfo.fid),
                          &catalogEntry, 0, REMAINDER, (char *)&sysTablesOverlay, NULL); 
        if (e < eNOERROR) ERR(handle, e);

	/*
	** Destroy the data file.
	**
	** This operation must be defered to handle the individual rollback.
	** Instead of that, insert this file into the deallocated list.
	*/
	/*
	** e = OM_DropFile(handle, fid, &SM_DLPOOL(handle), &MY_DLHEAD(handle));
	** if (e < eNOERROR) ERR(handle, e);
	*/

	MAKE_PHYSICALFILEID(pFid, fid->volNo, sysTablesOverlay.data.firstPage);
	e = OM_DropFile(handle, MY_XACT_TABLE_ENTRY(handle), &pFid,
			&sysTablesOverlay.data.pageSegmentID, &sysTablesOverlay.data.trainSegmentID, FALSE, &logParam);
	if (e < eNOERROR) ERR(handle, e);
	


	/* Destroy a segment corresponding the B+-tree file. ==> REMOVED */

	/*@ construct kval. */
	/*
	** Destroy all the corresponding entries from the catalog, SM_SYSINDEXES.
	*/
	kval.len = sizeof(Two) + sizeof(Four);
	memcpy(&(kval.val[0]), &(fid->volNo), sizeof(Two));
	memcpy(&(kval.val[sizeof(Two)]), &(fid->serial), sizeof(Four)); 

#ifdef CCPL
	if(lockup)
	    e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
			  &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
                          &SM_SYSIDX_DATAFILEID_KEYDESC,
			  &kval, SM_EQ, &kval, SM_EQ, &curBid, NULL, &indexLockup);
	else
	    e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
			  &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
                          &SM_SYSIDX_DATAFILEID_KEYDESC,
			  &kval, SM_EQ, &kval, SM_EQ, &curBid, NULL, NULL);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
        e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
                      &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
                      &SM_SYSIDX_DATAFILEID_KEYDESC,
                      &kval, SM_EQ, &kval, SM_EQ, &curBid, NULL, NULL); /* no lock request */
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	while (curBid.flag != CURSOR_EOS) { /* drop all indexes */

	    /*@ Read the object. */
	    e = OM_ReadObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesInfo.fid),
                              &(curBid.oid), 0, REMAINDER, (char *)&sysIndexesOverlay, NULL); 
	    if (e < eNOERROR) ERR(handle, e);

	    /* get physical index ID */
	    MAKE_PHYSICALFILEID(pIid, fid->volNo, sysIndexesOverlay.rootPage);

	    /*
	    ** Destroy Index.
	    ** This operation must be defered to handle the individual rollback.
	    ** Instead of that, insert this index into the deallocated list.
	    */
	    switch (sysIndexesOverlay.indexType) {
	      case SM_INDEXTYPE_BTREE:
                e = BtM_DropIndex(handle, MY_XACT_TABLE_ENTRY(handle), &(sysIndexesOverlay.iid), &pIid, &(sysIndexesOverlay.pageSegmentID), FALSE, &logParam); 
                if (e < eNOERROR) ERR(handle, e);
                break;

	      case SM_INDEXTYPE_MLGF:
                e = MLGF_DropIndex(handle, MY_XACT_TABLE_ENTRY(handle), &(sysIndexesOverlay.iid), &pIid, &(sysIndexesOverlay.pageSegmentID), FALSE, &logParam); 
                if (e < eNOERROR) ERR(handle, e);
                break;

	      default:
                ERR(handle, eINTERNAL);
	    }

	    /* Destroy the object from the SM_SYSINDEXES. */
	    e = OM_DestroyObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesInfo),
				 &curBid.oid, NULL, &logParam); 
	    if (e < eNOERROR) ERR(handle, e);


	    /* index on BtreeFileID ==> index on DataFileID */
	    /* Delete the ObjectID from B+ tree on Data FileID of SM_SYSINDEXES. */
#ifdef CCPL
	    e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
				 &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
				 &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
				 &SM_SYSIDX_DATAFILEID_KEYDESC,
				 &kval, &curBid.oid, lockup, &logParam); 
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	    e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
				 &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
				 &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
				 &SM_SYSIDX_DATAFILEID_KEYDESC,
				 &kval, &curBid.oid, NULL, &logParam);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */
	    /*@ construct kval2 */
	    /* Delete the ObjectID from B+ tree on IndexID of SM_SYSINDEXES. */
	    kval2.len = sizeof(Two) + sizeof(Four);
	    memcpy(&(kval2.val[0]), &(sysIndexesOverlay.iid.volNo), sizeof(Two));
	    memcpy(&(kval2.val[sizeof(Two)]), &(sysIndexesOverlay.iid.serial), sizeof(Four)); 

#ifdef CCPL
	    e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
				 &(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo), 
				 &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
				 &SM_SYSIDX_INDEXID_KEYDESC,
				 &kval2, &curBid.oid, lockup, &logParam);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	    e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
				 &(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo), 
				 &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
				 &SM_SYSIDX_INDEXID_KEYDESC,
				 &kval2, &curBid.oid, NULL, &logParam);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */


	    /* Fetch the ObjectID of the next object. */
	    /* sysIndexesBtreeFileIdIndex => sysIndexesDataFileIdIndex */
	    /* SM_SYSIDX_BTREEFILEID_KEYDESC => SM_SYSIDX_DATAFILEID_KEYDESC */
#ifdef CCPL
	    if(lockup)
		e = BtM_FetchNext(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
				  &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
                                  &SM_SYSIDX_DATAFILEID_KEYDESC,
				  &kval, SM_EQ, &curBid, &nextBid, &indexLockup);
	    else
		e = BtM_FetchNext(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
				  &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
                                  &SM_SYSIDX_DATAFILEID_KEYDESC,
				  &kval, SM_EQ, &curBid, &nextBid, NULL);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
            e = BtM_FetchNext(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo), 
                              &(SM_MOUNTTABLE[v].sysIndexesInfo.fid), 
                              &SM_SYSIDX_DATAFILEID_KEYDESC,
                              &kval, SM_EQ, &curBid, &nextBid, NULL); /* no lock request */
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	    curBid = nextBid;
	}


	/*
	** Destroy the corresponding entry from the catalog, SM_SYSTABLES.
	*/
	e = OM_DestroyObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysTablesInfo),
			     &catalogEntry, NULL, &logParam);
	if (e < eNOERROR) ERR(handle, e);


	/*@ construct kval */
	/*
	** Delete the ObjectID from the B+ tree on data FileID of SM_SYSTABLES.
	*/
	kval.len = sizeof(Two) + sizeof(Four);
	memcpy(&(kval.val[0]), &(fid->volNo), sizeof(Two));
	memcpy(&(kval.val[sizeof(Two)]), &(fid->serial), sizeof(Four)); 

#ifdef CCPL
	e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysTablesInfo.fid), 
			     &SM_SYSTBL_DFILEIDIDX_KEYDESC,
			     &kval, &catalogEntry, lockup, &logParam); 
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
			     &(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysTablesInfo.fid), 
			     &SM_SYSTBL_DFILEIDIDX_KEYDESC,
			     &kval, &catalogEntry, NULL, &logParam);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */


	/* Delete the ObjectID from the B+ tree on Btree FileID of SM_SYSTABLES. ==> REMOVED */
    }

    if(ACTION_ON(handle)){ 
	e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
        if(e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* SM_DestroyFile() */
