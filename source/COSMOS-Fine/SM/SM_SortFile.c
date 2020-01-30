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
 * Module: SM_SortFile.c
 *
 * Description:
 *  Sort the given data file.
 *
 * Exports:
 *  Four SM_SortFile( )
 */


#include <string.h> /* for memcpy */
#include "perProcessDS.h"
#include "perThreadDS.h"

#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "OM.h"
#include "LM.h"
#include "BtM.h"
#include "SM.h"
#include "RDsM.h"

#include "TM.h"

Four sm_CreateCatalogEntriesForFile(Four, Four, sm_CatOverlayForSysTables*, Boolean, LockParameter*, LogParameter_T*);
Four sm_UpdateCatalogEntriesForFile(Four, Four, DataFileInfo*, sm_CatOverlayForData*, LockParameter*, LogParameter_T*);


/*
 * Function: Four SM_SortFile(Four, Fi)
 *
 * Description:
 *  Sort the given data file.
 *
 * Returns:
 *  error code
 */
Four SM_SortFile(
    Four handle,
    VolID  tmpVolId,            /* IN temporary volume in which sort stream is created */
    FileID *inFid,              /* IN file to sort */
    SortKeyDesc *kdesc,          /* IN sort key description */
    GetKeyAttrsFuncPtr_T getKeyAttrsFn, /* IN tuple analysis function */
    void *schema,               /* IN schema for analysis function */
    Boolean newFileFlag,        /* IN whether we make new file for sort result */
    Boolean tmpFileFlag,        /* IN new file is a temporary file? */
    FileID *outFid,             /* OUT new file storing sort result */
    LockParameter *lockup)      /* IN lockup parameter */
{
    Four e, v;
    DataFileInfo inFileInfo;
    sm_CatOverlayForSysTables catOverlayForOutFile;
    LockMode lockMode;
    LockReply lockReply;
    LockMode oldMode;
    Four i;
    LogParameter_T logParam;
    FileID localOutFid; 


    /*
     * Check parameters
     */

    if (inFid == NULL || kdesc == NULL || getKeyAttrsFn == NULL) ERR(handle, eBADPARAMETER);
    if (newFileFlag && outFid == NULL) ERR(handle, eBADPARAMETER);


    /*
     * find the given volume in the scan manager mount table
     */

    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == inFid->volNo) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);


    /*
     * Get data file information of given input file
     */

    inFileInfo.fid = *inFid;

    /* Check if the file is a temporary file. */
    inFileInfo.tmpFileFlag = FALSE; /* initialize */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) 
	if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i]) &&
	    EQUAL_FILEID(*inFid, SM_ST_FOR_TMP_FILES(handle)[i].data.fid)) {

	    inFileInfo.tmpFileFlag = TRUE;
            inFileInfo.catalog.entry = &(SM_ST_FOR_TMP_FILES(handle)[i]);
	    break;
	}

    if (!inFileInfo.tmpFileFlag) {
	if (lockup) {
	    /* check the lockup mode */
	    switch(lockup->mode){		
	      case L_SIX:
	      case L_S:
	      case L_X:
	      case L_IS:
	      case L_IX: lockMode = lockup->mode; break;

	      default : ERR(handle, eBADLOCKMODE_SM);
	    }
	    if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

	    /* lock on the data file */
	    e = LM_getFileLock(handle,  &MY_XACTID(handle), inFid, lockMode, lockup->duration,
				L_UNCONDITIONAL, &lockReply, &oldMode);
	    if ( e < eNOERROR ) ERR(handle, e);

	    if ( lockReply == LR_DEADLOCK)
		ERR(handle, eLOCKREQUESTFAIL);

	    /* acquiredFileLock is the lock mode to be acquired */
	    /* old code -> SM_SCANTABLE(handle)[scanId].acquiredFileLock = lockup->mode; */
	}

	/* Get catalog entry of */
	e = sm_GetCatalogEntryFromDataFileId(handle, v, inFid, &inFileInfo.catalog.oid);
	if (e < eNOERROR) ERR(handle, e);
    }


    /*
     * Sort
     */

    /* get 'localOutFid' */
    if (newFileFlag) {

        /* allocate new data file ID */
        e = sm_GetNewFileId(handle, v, &localOutFid);
        if (e < eNOERROR) ERR(handle, e);
    }
    else {

        /* same as input data file ID */
        localOutFid = *inFid;
    }

    /* set log parameter */
    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, inFileInfo.tmpFileFlag);

    /* sort data file */
    /* Note!! if regular file is sorted, sort into another file - for recovery */
    e = OM_SortInto(handle, MY_XACT_TABLE_ENTRY(handle), tmpVolId,
                    &inFileInfo, &localOutFid, &catOverlayForOutFile.data,
                    kdesc, (omGetKeyAttrsFuncPtr_T)getKeyAttrsFn, schema,
                    newFileFlag, tmpFileFlag, &logParam);
    if(e < eNOERROR) ERR(handle, e);


    /*
     * Catalog manipulation
     */

    /* if new file is created, create catalog entry */
    if (newFileFlag) {
        e = sm_CreateCatalogEntriesForFile(handle, v, &catOverlayForOutFile, tmpFileFlag, lockup, &logParam);
        if (e < eNOERROR) ERR(handle, e);
    }
    /* if exist file is sorted, update catalog entry */
    else {
        e = sm_UpdateCatalogEntriesForFile(handle, v, &inFileInfo, &catOverlayForOutFile.data, lockup, &logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* set output parameter */
    if(outFid != NULL) *outFid = localOutFid;


    return(eNOERROR);

} /* SM_SortFile() */


Four sm_CreateCatalogEntriesForFile(
    Four    handle,
    Four   v,                   /* IN array index on scan manager mount table */
    sm_CatOverlayForSysTables *sysTablesOverlay, /* IN entry of SM_SYSTABLES */
    Boolean tmpFileFlag,        /* IN TRUE if the file is a temporary file */
    LockParameter *lockup,      /* IN request lock or not */
    LogParameter_T *logParam)
{
    Four   e;                   /* error code */
    Four   freeEntryNo;         /* unused entry in sm_sysTablesForTmpFiles */
    KeyValue kval;              /* a key value */
    ObjectID oid;               /* an ObjectID */
    LockReply lockReply;
    LockMode oldMode;
    Four i;                     /* temporary variable */

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

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
        SM_ST_FOR_TMP_FILES(handle)[freeEntryNo] = *sysTablesOverlay;

    } else {
        if(lockup){
            /* request exclusive commit duration file lock */
            if(lockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

            e = LM_getFileLock(handle, &MY_XACTID(handle), &(sysTablesOverlay->data.fid),
                               lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
            if(e < eNOERROR) ERR(handle, e);

            if ( lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
        }

        /* Create a B+-tree file in the given volume. ==> REMOVED */


        /*
        ** Register the files in the catalog table SM_SYSTABLES.
        */
        e = OM_CreateObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysTablesInfo),
                            NULL, NULL, sizeof(sm_CatOverlayForSysTables),
                            (char *)sysTablesOverlay, &oid, NULL, logParam);
        if (e < eNOERROR) ERR(handle, e);

        /*@ construct kval */
        /* Insert the new ObjectID into B+ tree on data FileID of SM_SYSTABLES. */
        kval.len = sizeof(Two) + sizeof(Four);
        memcpy(&(kval.val[0]), &(sysTablesOverlay->data.fid.volNo), sizeof(Two));
        memcpy(&(kval.val[sizeof(Two)]), &(sysTablesOverlay->data.fid.serial), sizeof(Four)); 

        e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
                             &(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo), 
			     &(SM_MOUNTTABLE[v].sysTablesInfo.fid), 
                             &SM_SYSTBL_DFILEIDIDX_KEYDESC,
                             &kval, &oid, lockup, logParam);
        if (e < eNOERROR) ERR(handle, e);

        /* Insert the new ObjectID into B+ tree on Btree FileID of SM_SYSTABLES. ==> REMOVED */
    }

    return(eNOERROR);

} /* sm_CreateCatalogEntriesForFile() */


Four sm_UpdateCatalogEntriesForFile(
    Four    handle,
    Four   v,                       /* IN array index on scan manager mount table */
    DataFileInfo *fileInfo,         /* INOUT information about updated file */
    sm_CatOverlayForData *newEntry, /* IN data catalog entry of SM_SYSTABLES */
    LockParameter *lockup,          /* IN request lock or not */
    LogParameter_T *logParam)
{
    Four   e;                   /* error code */
    LockReply lockReply;
    LockMode oldMode;


    if (fileInfo->tmpFileFlag) {

        /* update catalog about temporary file */
        fileInfo->catalog.entry->data = *newEntry;

    } else {
        if(lockup){
            /* request exclusive commit duration file lock */
            if(lockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

            e = LM_getFileLock(handle, &MY_XACTID(handle), &(newEntry->fid),
                               lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
            if(e < eNOERROR) ERR(handle, e);

            if ( lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
        }

        /*
        ** Update the data part of catalog entry in the catalog table SM_SYSTABLES.
        */
        e = OM_WriteObject(handle, MY_XACT_TABLE_ENTRY(handle),  &(SM_MOUNTTABLE[v].sysTablesInfo),
                           &fileInfo->catalog.oid, 0, sizeof(sm_CatOverlayForData), (char*)newEntry, NULL, logParam); 
        if (e < eNOERROR) ERR(handle, e);
    }


    return(eNOERROR);

} /* sm_UpdateCatalogEntriesForFile() */
