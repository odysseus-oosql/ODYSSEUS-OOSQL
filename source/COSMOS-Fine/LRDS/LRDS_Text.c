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
 * Module: LRDS_Text.c
 *
 * Description:
 *  Supports SM_TEXT data type.
 *
 * Exports:
 *  Four LRDS_Text_AddKeywords(Four, Four, TupleID*, Four, Four, char*)
 *  Four LRDS_Text_DeleteKeywords(Four, Four, TupleID*, Four, Four, char*)
 *  Four LRDS_Text_GetIndexID(Four, Four, Four, IndexID*)
 */


#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM.h"
#include "LRDS.h"
#include <assert.h>
#include "BL_LRDS.h"
#include "RDsM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four LRDS_Text_AddKeywords(Four, Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Add the keywords relatted to the text which is stored in the
 *  given column.
 *
 * Returns:
 *  error code
 */
Four LRDS_Text_AddKeywords(
    Four handle,
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the related text */
    Four colNo,			/* IN column which contains the related text */
    Four nKeywords,		/* IN number of keywords */
    char *keywords)		/* IN keywords to store */
{
    Two klen;			/* key length, should be Two */
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four indexIdx;		/* index on array of index informations */
    KeyValue kval;		/* key value */
    char *ptr;			/* pointer to the current keyword */
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    IndexInfo *relTableEntry_ii; 
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_Text_AddKeywords()"));


    /*
    ** check parameters
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    if (relTableEntry_cdesc[colNo].type != SM_TEXT) ERR(handle, eBADPARAMETER); 

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (nKeywords < 0 || nKeywords != 0 && keywords == NULL) return(eBADPARAMETER);


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /* Search the index defined on the given column. */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
	if (relTableEntry_ii[indexIdx].colNo[0] == colNo) break; 

    /* Insert keywords into the index. */
    for (ptr = keywords, i = 0; i < nKeywords; i++) {
	memcpy(&klen, ptr, sizeof(Two));

	kval.len = sizeof(Two) + klen;
	memcpy(&kval.val[0], ptr, kval.len);
	ptr += kval.len;

	/* Insert element into the index. */
	e = SM_InsertIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
				&(relTableEntry_ii[indexIdx].kdesc.btree),
				&kval, (ObjectID*)tid, objLockupPtr); 
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_Text_AddKeywords() */



/*
 * Function: Four LRDS_Text_DeleteKeywords(Four, Four, TupleID*, Four, Four, char*)
 *
 * Description:
 *  Delete keywords which is not related to the text on the given column any more.
 *
 * Returns:
 *  error code
 */
Four LRDS_Text_DeleteKeywords(
    Four handle,
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple containing the related text */
    Four colNo,			/* IN column which contains the related text */
    Four nKeywords,		/* IN number of keywords */
    char *keywords)		/* IN keywords to delete */
{
    Two klen;			/* key length, should be Two */
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four indexIdx;		/* index on array of index informations */
    KeyValue kval;		/* key value */
    char *ptr;			/* pointer to the current keyword */
    Four i;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    IndexInfo *relTableEntry_ii; 
    ColDesc *relTableEntry_cdesc; 
    LockReply     lockReply;    /* lock reply */
    LockMode      oldMode;      /* lock mode */


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_Text_DeleteKeywords()"));


    /*
    ** check parameters
    */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    if (relTableEntry_cdesc[colNo].type != SM_TEXT) ERR(handle, eBADPARAMETER); 

    /* Get the current tuple if the tid is NULL. */
    if (useScanFlag && tid == NULL) tid = &LRDS_SCANTABLE(handle)[ornOrScanId].tid;

    if (nKeywords < 0 || nKeywords != 0 && keywords == NULL) return(eBADPARAMETER);


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IX;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_X;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /* Search the index defined on the given column. */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
	if (relTableEntry_ii[indexIdx].colNo[0] == colNo) break; 

    /*
    ** Automatic Index Support
    **  - delete index entry if there is an index on the updated column.
    */
    /* lock on the data file that the automatic index is built on to support the automatic index */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag && !relTableEntry->isCatalog) {

        /* check lock parameter */
        if (fileLockup.duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        if (fileLockup.mode != L_X && fileLockup.mode != L_IX && fileLockup.mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);

        /* lock on the data file */
        e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid, fileLockup.mode, fileLockup.duration,
                           L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    /* Insert keywords into the index. */
    for (ptr = keywords, i = 0; i < nKeywords; i++) {
	memcpy(&klen, ptr, sizeof(Two));

	kval.len = sizeof(Two) + klen;
	memcpy(&kval.val[0], ptr, kval.len);
	ptr += kval.len;

	/* Insert element into the index. */
	e = SM_DeleteIndexEntry(handle, &(relTableEntry_ii[indexIdx].iid),
				&(relTableEntry_ii[indexIdx].kdesc.btree),
				&kval, (ObjectID*)tid, objLockupPtr); 
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_Text_DeleteKeywords() */



/*
 * Function: Four LRDS_Text_GetIndexID(Four, Four, Four, IndexID*)
 *
 * Description:
 *  Returns the ID of the index which is defined on the given column.
 *
 * Returns:
 *  error code
 */
Four LRDS_Text_GetIndexID(
    Four handle,
    Four orn,			/* IN open relation number of the relation */
    Four colNo,			/* IN column which has data type of SM_TEXT */
    IndexID *iid)		/* OUT id of index defined on the given column */
{
    Four indexIdx;		/* index on array of index informations */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii; 
    ColDesc *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_Text_GetIndexID()"));


    /*
    ** check parameters
    */
    if (orn < 0) ERR(handle, eBADPARAMETER);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii); 

    /* Is the column valid? */
    if (colNo >= relTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);

    if (relTableEntry_cdesc[colNo].type != SM_TEXT) ERR(handle, eBADPARAMETER); 

    if (iid == NULL) return(eBADPARAMETER);


    /* Search the index defined on the given column. */
    for (indexIdx = 0; indexIdx < relTableEntry->ri.nIndexes; indexIdx++)
	if (relTableEntry_ii[indexIdx].colNo[0] == colNo) break; 

    *iid = relTableEntry_ii[indexIdx].iid; 

    return(eNOERROR);

} /* LRDS_Text_GetIndexID() */



/*
 * Function: Four LRDS_InitTextBulkLoad(Four, Boolean, Four)
 *
 * Description:
 *  Initialize text type bulkload
 *
 * Returns:
 *  bulkload ID
 *  error code
 */
Four LRDS_InitTextBulkLoad(
    Four	     handle,
    Four             tmpVolId,            /* IN temporary volume in which sort stream is created */ 
    Four             ornOrScanId,         /* IN open relation no or scan id*/
    Boolean          useScanFlag,         /* IN TRUE if above parameter is scan id */
    Boolean          indexBlkLdFlag,      /* IN flag which indicates index is built by bulkload or bulk insert */
    Four             colNo,               /* IN column number of ordered set type */
    LockParameter    *lockup)             /* IN lock parameter for data volume */
{
    Four             e;	                  /* error code */
    Four             indexIdx;            /* index on array of index informations */
    Four             blkLdId;
    LRDS_BlkLdTableEntry* blkLdEntry;
    IndexInfo *blkLdRelTableEntry_ii;
    ColDesc *blkLdRelTableEntry_cdesc;


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_InitTextBulkLoad()"));


    /*
     *  Check parameters
     */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);
    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);


    /*
     *  Find empty entry from LRDS bulkload table
     */

    for (blkLdId = 0; blkLdId < LRDS_BLKLD_TABLE_SIZE; blkLdId++ ) {
	if (LRDS_BLKLD_TABLE(handle)[blkLdId].isUsed == FALSE) break;
    }
    if (blkLdId == LRDS_BLKLD_TABLE_SIZE) ERR(handle, eBLKLDTABLEFULL);

    /* set entry for fast access */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];

    /* set isUsed flag */
    blkLdEntry->isUsed = TRUE;


    /*
     *  set orn and scanId
     */
    if (useScanFlag) {
        blkLdEntry->lrdsBlkLdOrn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        blkLdEntry->lrdsBlkLdScanId = ornOrScanId;
    } else {
        blkLdEntry->lrdsBlkLdOrn = ornOrScanId;
        blkLdEntry->lrdsBlkLdScanId = NIL;
    }


    /*
     *  Get relTableEntry, cdesc, and kdesc
     */

    /* Get the relation table entry. */
    blkLdEntry->lrdsBlkLdRelTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, blkLdEntry->lrdsBlkLdOrn);
    blkLdRelTableEntry_cdesc = PHYSICAL_PTR(blkLdEntry->lrdsBlkLdRelTableEntry->cdesc);
    blkLdRelTableEntry_ii = PHYSICAL_PTR(blkLdEntry->lrdsBlkLdRelTableEntry->ii);

    /* Is the column valid? */
    if (colNo >= blkLdEntry->lrdsBlkLdRelTableEntry->ri.nColumns) ERR(handle, eBADPARAMETER);
    if (blkLdRelTableEntry_cdesc[colNo].type != SM_TEXT) ERR(handle, eBADPARAMETER);


    /*
     *  Search the index defined on the given column.
     */
    for (indexIdx = 0; indexIdx < blkLdEntry->lrdsBlkLdRelTableEntry->ri.nIndexes; indexIdx++)
        if (blkLdRelTableEntry_ii[indexIdx].colNo[0] == colNo) break;

    /* set index ID */
    blkLdEntry->lrdsBlkLdTxtIndexId = blkLdRelTableEntry_ii[indexIdx].iid;


    /*
     *  Initialize bulkload
     */

    /* set lockup parameter */
    blkLdEntry->lockup = *lockup;

    /* allocate memory */
    blkLdEntry->smIndexBlkLdIdArray = (Four *) malloc(sizeof(Four));
    if (blkLdEntry->smIndexBlkLdIdArray == NULL) ERR(handle, eMEMORYALLOCERR);

    /* set 'indexBlkLdFlag' */
    blkLdEntry->indexBlkLdFlag = indexBlkLdFlag;

    /* initialize bulkload or bulkinsert */
    if (blkLdEntry->indexBlkLdFlag == TRUE) {
        blkLdEntry->smIndexBlkLdIdArray[0] = SM_InitIndexBulkLoad(handle, tmpVolId,
                                                                  &(blkLdRelTableEntry_ii[indexIdx].kdesc.btree));
        if (blkLdEntry->smIndexBlkLdIdArray[0] < 0) ERR(handle, blkLdEntry->smIndexBlkLdIdArray[0]);
    }
    else {
        blkLdEntry->smIndexBlkLdIdArray[0] = SM_InitIndexBulkInsert(handle, tmpVolId,
                                                                    &(blkLdRelTableEntry_ii[indexIdx].kdesc.btree));
        if (blkLdEntry->smIndexBlkLdIdArray[0] < 0) ERR(handle, blkLdEntry->smIndexBlkLdIdArray[0]);
    }

    return blkLdId;

} /* LRDS_InitTextBulkLoad() */


/*
 * Function: Four LRDS_NextTextBulkLoad(Four, TupleID*, Four, char*)
 *
 * Description:
 *  Add the keywords relatted to the text which is stored in the given column.
 *
 * Returns:
 *  error code
 */
Four LRDS_NextTextBulkLoad(
    Four	     handle,
    Four             blkLdId,             /* IN bulkload ID */
    TupleID*         tid,                 /* IN tuple containing the related text */
    Four             nKeywords,           /* IN number of keywords */
    char*            keywords)            /* IN keywords to store */
{
    Four             i;                   /* index variable */
    Four             e;	                  /* error code */
    Two              klen;                /* key length, should be Two */
    KeyValue         kval;                /* key value */
    char*            ptr;
    LRDS_BlkLdTableEntry* blkLdEntry;


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_NextTextBulkLoad()"));


    /*
     *  Set entry for fast access
     */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];


    /*
     *  Get the current tuple if the tid is NULL.
     */
    if (tid == NULL) {
        if (blkLdEntry->lrdsBlkLdScanId != NIL) tid = &LRDS_SCANTABLE(handle)[blkLdEntry->lrdsBlkLdScanId].tid;
        else                                    ERR(handle, eBADPARAMETER);
    }


    /*
     *  Insert keywords into the index.
     */
    for (ptr = keywords, i = 0; i < nKeywords; i++) {

        memcpy(&klen, ptr, sizeof(Two));

        kval.len = sizeof(Two) + klen;
        memcpy(&kval.val[0], ptr, kval.len);
        ptr += kval.len;

        /* Insert element into the index by bulkload */
        if (blkLdEntry->indexBlkLdFlag == TRUE) {
            e = SM_NextIndexBulkLoad(handle, blkLdEntry->smIndexBlkLdIdArray[0], &kval, (ObjectID*)tid);
            if (e < eNOERROR) ERR(handle, e);
        }
        else {
            e = SM_NextIndexBulkInsert(handle, blkLdEntry->smIndexBlkLdIdArray[0], &kval, (ObjectID*)tid);
            if (e < eNOERROR) ERR(handle, e);
        }
    }


    return eNOERROR;

} /* LRDS_NextTextBulkLoad() */


/*
 * Function: Four LRDS_FinalTextBulkLoad(Four)
 *
 * Description:
 *  Finalize text type bulkload
 *
 * Returns:
 *  error code
 */
Four LRDS_FinalTextBulkLoad(
    Four	     handle,
    Four             blkLdId)             /* IN bulkload ID */
{
    Four             e;	                  /* error code */
    Four             indexIdx;            /* index on array of index informations */
    LRDS_BlkLdTableEntry* blkLdEntry;
    IndexInfo *blkLdRelTableEntry_ii;
    ColDesc *blkLdRelTableEntry_cdesc;


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_FinalTextBulkLoad()"));


    /*
     *  Set entry for fast access
     */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];

    blkLdRelTableEntry_cdesc = PHYSICAL_PTR(blkLdEntry->lrdsBlkLdRelTableEntry->cdesc);
    blkLdRelTableEntry_ii = PHYSICAL_PTR(blkLdEntry->lrdsBlkLdRelTableEntry->ii);

    /*
     *  Finalize bulkload
     */
    if (blkLdEntry->indexBlkLdFlag == TRUE) {
        e = SM_FinalIndexBulkLoad(handle, blkLdEntry->smIndexBlkLdIdArray[0], &blkLdEntry->lrdsBlkLdTxtIndexId, 100, 100, &blkLdEntry->lockup);
        if (e < eNOERROR) ERR(handle, e);
    }
    else {
        for (indexIdx = 0; indexIdx < blkLdEntry->lrdsBlkLdRelTableEntry->ri.nIndexes; indexIdx++)
            if (EQUAL_INDEXID(blkLdRelTableEntry_ii[indexIdx].iid, blkLdEntry->lrdsBlkLdTxtIndexId)) break;

        e = SM_FinalIndexBulkInsert(handle, blkLdEntry->smIndexBlkLdIdArray[0], &blkLdEntry->lrdsBlkLdTxtIndexId,
                                    &(blkLdRelTableEntry_ii[indexIdx].kdesc.btree), &blkLdEntry->lockup);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* free memory */
    free(blkLdEntry->smIndexBlkLdIdArray);


    /*
     *  Free allocated entry in bulkload table
     */
    blkLdEntry->isUsed = FALSE;


    return(eNOERROR);

} /* LRDS_FinalTextBulkLoad() */

