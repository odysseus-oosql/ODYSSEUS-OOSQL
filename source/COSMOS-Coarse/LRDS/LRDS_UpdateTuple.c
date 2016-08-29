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
 * Module: LRDS_UpdateTuple.c
 *
 * Description:
 *  Update the specified tuple. If the given tuple 'tid' is NULL, then update
 *  the tuple pointed by the current cursor. The update is performed in unit
 *  of column. The parameter 'nCols' give the number of columns to be updated
 *  and the parameter 'columns' defines which actions to be done on which
 *  column.
 *
 * Exports:
 *  Four LRDS_UpdateTuple(Four, TupleID*, Two, ColumnListStruct*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eTOOLARGELENGTH_LRDS
 *    some errors caused by function calls
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM_Internal.h" 	
#include "LRDS.h"
#include "LM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

Four LRDS_UpdateTuple(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple to update */
    Two                 nCols,                  /* IN # of columns to update */
    ColListStruct       *clist)                 /* IN update information on the updated column */
{
    Four                e;                      /* error number */
    Four                orn;
    Four                smScanId;
    Two                 i;
    Two                 i2;
    Two                 j, k;                   /* index variables */
    Four                oldTupHdrSize;          /* old size of a tuple header */
    Four                newTupHdrSize;          /* old size of a tuple header */
    Four                oldStart;               /* starting offset of a column */
    Four                oldLength;              /* old length of a column */
    Four                start;                  /* starting offset of update of a column */
    Four                length;                 /* amount of data to be updated of a column */
    Four                dataLength;             /* amount of new data */
    Two                 colNo;                  /* column number */
    Two                 s;                      /* variable used for converting to type Two       */
    void                *data;                  /* new data */
    TupleHdr            tupHdr;                 /* tuple header */
    ColDesc             *cdesc;                 /* pointer to cdesc of LRDS open relation table  */
    KeyValue            kval;                   /* a key value */
    unsigned char       *nullVector;            /* bit vector of null flags */
    ColListStruct       clistLocal[1];          /* a column list structure */
    char                ctag[MAXNUMOFCOLS];     /* denotes the updated column */
    char                itag[MAXNUMOFINDEXES];  /* denotes the index having updated column */
    char                buf[MAXKEYLEN];         /* to load a column value */
    MLGF_HashValue      mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       fileLockup;             /* lockup for SM_Fetch Tuple */
    LockParameter       objLockup;              /* lockup for SM_Fetch Tuple */
    LockParameter       *fileLockupPtr;         /* pointer to the lockup value */
    LockParameter       *objLockupPtr;          /* pointer to the lockup value */
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;  
    Boolean             isNullFlag;             /* flag which indicates index column is null or not */
    LockReply           lockReply;              /* lock reply */
    LockMode            oldMode;                /* lock mode */

    TR_PRINT(TR_LRDS, TR1, ("LRDS_UpdateTuple(handle)"));


    /* check parameters */
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

    if (nCols <= 0 || nCols > relTableEntry->ri.nColumns)
	ERR(handle, eBADPARAMETER);

    if (clist == NULL) ERR(handle, eBADPARAMETER);

    /* If the parameter tid is NULL, use the current TupleID. */
    if (useScanFlag && tid == NULL) {
	if (IS_NILTUPLEID(LRDS_SCANTABLE(handle)[ornOrScanId].tid))
	    ERR(handle, eINVALIDCURRENTTUPLE_LRDS);

	tid = &(LRDS_SCANTABLE(handle)[ornOrScanId].tid);
    }

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

    /* Fetch the tuple header. */
    oldTupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, oldTupHdrSize, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < 0) ERR(handle, e);

    /* Get the real tuple header size. */
    newTupHdrSize = oldTupHdrSize = TUPLE_HEADER_SIZE(tupHdr.nVarCols + tupHdr.nFixedCols, tupHdr.nVarCols);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* Construct 'ctag' whose entries are TRUE for the updated column, FALSE for others. */
    for (i = 0; i < relTableEntry->ri.nColumns; i++) ctag[i] = FALSE;
    for (i = 0; i < nCols; i++) {
	if (clist[i].colNo < 0 ||
	    clist[i].colNo > relTableEntry->ri.nColumns)
	    ERR(handle, eBADPARAMETER);

	/* Use LRDS_Set_xxx() interface for the set column. */
	if (LRDS_HAS_ITS_OWN_API(relTableEntry_cdesc[clist[i].colNo].complexType, relTableEntry_cdesc[clist[i].colNo].type)) 
	    ERR(handle, eBADPARAMETER);

	ctag[clist[i].colNo] = TRUE;
    }


    /* Construct 'itag' whose entries are TRUE for index having the updated column. */
    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {
	itag[i] = FALSE;

	/* consider the SM_INDEXTYPE_MLGF */
	switch (relTableEntry_ii[i].indexType) { 
	  case SM_INDEXTYPE_BTREE:
	    for (j = 0; j < relTableEntry_ii[i].kdesc.btree.nparts; j++) 
		if (ctag[relTableEntry_ii[i].colNo[j]]) {
		    itag[i] = TRUE;
		    break;
		}
	    break;

	  case SM_INDEXTYPE_MLGF:
	    for (j = 0; j < relTableEntry_ii[i].kdesc.mlgf.nKeys; j++) {
		if (ctag[relTableEntry_ii[i].colNo[j]]) { 
		    itag[i] = TRUE;
		    break;
		}
                if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[j]].type == SM_MBR) break; 
            }
	    break;

	  default:
	    ERR(handle, eINTERNAL);
	}
    }

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

    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {
	if (!itag[i]) continue;	/* unchanged index */

	if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT) continue; 

	switch (relTableEntry_ii[i].indexType) { 
	  case SM_INDEXTYPE_BTREE:
	    /* Construct 'kval'. */
	    kval.len = 0;
	    for (j = 0; j < relTableEntry_ii[i].kdesc.btree.nparts; j++) { 
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[i].colNo[j]; 

		/* 'cdesc' points to the corresponding column descriptor. */
		cdesc = &(relTableEntry_cdesc[colNo]); 

		/* Load the column value. */
		clistLocal[0].colNo = colNo;
		clistLocal[0].start = ALL_VALUE;
		clistLocal[0].dataLength = cdesc->length;
		if (cdesc->type == SM_STRING || cdesc->type == SM_VARSTRING)
		    clistLocal[0].data.ptr = &(buf[0]);

		e = LRDS_FetchTuple(handle, ornOrScanId, useScanFlag, tid, 1, &(clistLocal[0]));
		if (e < 0) ERR(handle, e);

                /* if index column has null, skip it!! */
                if (clistLocal[0].nullFlag == FALSE)
                    isNullFlag = FALSE;
                else {
                    isNullFlag = TRUE;
                    break;
                }

		switch (cdesc->type) {
		  case SM_VARSTRING:
		    /* copy the 'data length' to the temporary variable of type Two. */
		    s = clistLocal[0].retLength;
		    memcpy(&(kval.val[kval.len]), &s, sizeof(Two));
		    kval.len += sizeof(Two);
		    memcpy(&(kval.val[kval.len]), clistLocal[0].data.ptr, clistLocal[0].retLength);
		    kval.len += clistLocal[0].retLength;
		    break;

		  case SM_STRING:
		    memcpy(&(kval.val[kval.len]), clistLocal[0].data.ptr, cdesc->length);
		    kval.len += cdesc->length;
		    break;

		  default:
		    memcpy(&(kval.val[kval.len]), &(clistLocal[0].data), cdesc->length);
		    kval.len += cdesc->length;
		    break;
		}
	    }

	    if (isNullFlag == FALSE) {
		e = SM_DeleteIndexEntry(handle, &(relTableEntry_ii[i].iid),
					&(relTableEntry_ii[i].kdesc.btree),
					&kval, (ObjectID*)tid, objLockupPtr); 
		if (e < 0) ERR(handle, e);
	    }
	    break;

	  case SM_INDEXTYPE_MLGF:
	    /* supported types: SM_SHORT, SM_INT, SM_LONG, SM_MBR */

	    for (j = 0; j < relTableEntry_ii[i].kdesc.mlgf.nKeys; j++) { 
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[i].colNo[j]; 

		/* 'cdesc' points to the corresponding column descriptor. */
		cdesc = &(relTableEntry_cdesc[colNo]); 

		/* Load the column value. */
		clistLocal[0].colNo = colNo;
		clistLocal[0].start = ALL_VALUE;
		clistLocal[0].dataLength = cdesc->length;

		e = LRDS_FetchTuple(handle, ornOrScanId, useScanFlag, tid, 1, &(clistLocal[0]));
		if (e < 0) ERR(handle, e);

                /* if index column has null, skip it!! */
                if (clistLocal[0].nullFlag == FALSE)
                    isNullFlag = FALSE;
                else {
                    isNullFlag = TRUE;
                    break;
                }

		switch (cdesc->type) {
		  case SM_SHORT:
		    mlgfKval[j] = clistLocal[0].data.s;
		    break;

		  case SM_INT:
		    mlgfKval[j] = clistLocal[0].data.i;
		    break;

		  case SM_LONG:
		    mlgfKval[j] = clistLocal[0].data.l;
		    break;

		  case SM_MBR:
		    for (k = 0; k < MBR_NUM_PARTS; k++)
			mlgfKval[k] = clistLocal[0].data.mbr.values[k];
                    j = MBR_NUM_PARTS - 1; 
		    break;

          case SM_VARSTRING:
            isNullFlag = TRUE;
            j = MBR_NUM_PARTS - 1;
            break;

		  default:
		    ERR(handle, eINTERNAL);
		}
	    }

            if (isNullFlag == FALSE) {
		e = SM_MLGF_DeleteIndexEntry(handle, &(relTableEntry_ii[i].iid),
					     &(relTableEntry_ii[i].kdesc.mlgf),
					     mlgfKval, (ObjectID*)tid, objLockupPtr); 
		if (e < 0) ERR(handle, e);
	    }
	    break;

	  default:
	    ERR(handle, eINTERNAL);
	}
    }


    /*
     * Add columns if there is a column which was not stored.
     */
    for (i = 0; i < nCols; i++) {
        if (clist[i].colNo >= tupHdr.nFixedCols + tupHdr.nVarCols) {
            e = lrds_AddColumns(handle, ornOrScanId, useScanFlag, tid, &tupHdr);
            if (e < 0) ERR(handle, e);

            newTupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

            assert(relTableEntry->ri.nColumns == tupHdr.nFixedCols + tupHdr.nVarCols);
            assert(relTableEntry->ri.nVarColumns == tupHdr.nVarCols);

            nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

            oldTupHdrSize = newTupHdrSize;

            break;
        }
    }


    /*
    ** For each updated column, call the SM_UpdateObject().
    */
    for (i = 0; i < nCols; i++) {

	/* 'cdesc' points to corresponding 'cdesc' field of LRDS open relation table. */
	cdesc = &(relTableEntry_cdesc[clist[i].colNo]); 

	/* Get 'oldStart' and 'oldLength' of the current column. */
	if (cdesc->varColNo != NIL) { 
	    /* variable-length column */
            oldStart = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
	    oldLength = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, oldStart, tupHdr);
	} else {
	    /* fixed-length column */
	    oldStart = newTupHdrSize + cdesc->offset;
	    oldLength = cdesc->length;
	}

        /* Set NULL value */
        if (clist[i].nullFlag == TRUE && !BITTEST(nullVector, clist[i].colNo)) {
            BITSET(nullVector, clist[i].colNo);

            if (cdesc->varColNo != NIL) {
                /* Update the start offsets of the variable columns */
                for (i2 = cdesc->varColNo; i2 < tupHdr.nVarCols; i2++)
                    tupHdr.varColOffset[i2] -= oldLength;

                /* delete the data */
                e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                      (ObjectID*)tid, oldStart, oldLength, NULL, 0, NULL, NULL);
                if (e < 0) ERR(handle, e);
        }

	    continue;
	}
	if (clist[i].nullFlag == TRUE && BITTEST(nullVector, clist[i].colNo)) {

        continue; // do nothing
    }

	/* Copy the information of 'clist[i]' into the local variables. */
	if (cdesc->type == SM_VARSTRING || cdesc->type == SM_STRING || cdesc->type == SM_TEXT) { 
	    start = clist[i].start;
	    length = clist[i].length;

	    if (start == ALL_VALUE) { /* change default value to specific values */
		start = 0;
		length = REMAINDER;
	    } else if (start == END) { /* APPEND */
		start = oldLength;
		length = 0;
	    } else if (start < 0 || start > oldLength) {
		ERR(handle, eBADPARAMETER);
	    }

	    if (length == REMAINDER) {
		length = oldLength - start;
	    } else if (length < 0 || (start + length > oldLength)) {
		ERR(handle, eBADPARAMETER);
	    }

	    dataLength = (cdesc->type == SM_STRING) ? length:clist[i].dataLength;
	    data = clist[i].data.ptr;

	    if (dataLength < 0) ERR(handle, eBADPARAMETER);

	} else {
	    start = 0;
	    length = dataLength = cdesc->length;
	    data = &(clist[i].data);
	}

	start += oldStart;

	if (length == 0) {	/* INSERT or APPEND */
	    if (cdesc->varColNo == NIL) ERR(handle, eBADPARAMETER);


	    if (oldLength + dataLength > cdesc->length) ERR(handle, eTOOLARGELENGTH_LRDS);

	    /* Reset the corresponding bit of arry of null flags. */
	    BITRESET(nullVector, clist[i].colNo);

	    /* Update the start offsets of the variable columns */
            for (i2 = cdesc->varColNo; i2 < tupHdr.nVarCols; i2++)
                tupHdr.varColOffset[i2] += dataLength;

	} else if (dataLength == 0) { /* DELETE */
	    if (cdesc->varColNo == NIL) ERR(handle, eBADPARAMETER);

	    /* Update the start offsets of the variable columns */
            for (i2 = cdesc->varColNo; i2 < tupHdr.nVarCols; i2++) 
                tupHdr.varColOffset[i2] -= length;

	} else {		/* REPLACE */
	    /* Null vector must be set because in some case null vector is unset and user want to replace its contents. */

	    /* Reset the corresponding bit of arry of null flags. */
	    BITRESET(nullVector, clist[i].colNo);

	    if (cdesc->type == SM_VARSTRING || cdesc->type == SM_TEXT) { /* variable-length column */ 

		/* Update the start offsets of the variable columns */
                for (i2 = cdesc->varColNo; i2 < tupHdr.nVarCols; i2++)
                    tupHdr.varColOffset[i2] += dataLength - length;

	    } else { /* fixed-length column */

		if (cdesc->type != SM_STRING && length != cdesc->length)
		    ERR(handle, eBADPARAMETER);
	    }
	}

	/* Update the object corresponding to the tuple. */
	e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, start, length, data, dataLength, NULL, NULL);
	if (e < 0) ERR(handle, e);
    }

    /* Update the tuple header. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, oldTupHdrSize, (char*)&tupHdr, newTupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);

    /*
    ** Automatic Index Support
    **  : Insert index entry if there is a index on the updated column
    */
    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {
	if (!itag[i]) continue;	/* unchanged index */

	if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT) continue; 

	switch (relTableEntry_ii[i].indexType) { 
	  case SM_INDEXTYPE_BTREE:
	    /* Construct 'kval'. */
	    kval.len = 0;
	    for (j = 0; j < relTableEntry_ii[i].kdesc.btree.nparts; j++) { 
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[i].colNo[j]; 

		/* 'cdesc' points to the corresponding column descriptor. */
		cdesc = &(relTableEntry_cdesc[colNo]); 

		/* Load the column value. */
		clistLocal[0].colNo = colNo;
		clistLocal[0].start = ALL_VALUE;
		clistLocal[0].dataLength = cdesc->length;
		if (cdesc->type == SM_STRING || cdesc->type == SM_VARSTRING)
		    clistLocal[0].data.ptr = &(buf[0]);

		e = LRDS_FetchTuple(handle, ornOrScanId, useScanFlag, tid, 1, &(clistLocal[0]));
		if (e < 0) ERR(handle, e);

                /* if index column has null, skip it!! */
                if (clistLocal[0].nullFlag == FALSE)
                    isNullFlag = FALSE;
                else {
                    isNullFlag = TRUE;
                    break;
                }

		switch (cdesc->type) {
		  case SM_VARSTRING:
		    /* copy the 'data length' to the temporary variable of type Two. */
		    s = clistLocal[0].retLength;
		    memcpy(&(kval.val[kval.len]), &s, sizeof(Two));
		    kval.len += sizeof(Two);
		    memcpy(&(kval.val[kval.len]), clistLocal[0].data.ptr, clistLocal[0].retLength);
		    kval.len += clistLocal[0].retLength;
		    break;

		  case SM_STRING:
		    memcpy(&(kval.val[kval.len]), clistLocal[0].data.ptr, cdesc->length);
		    kval.len += cdesc->length;
		    break;

		  default:
		    memcpy(&(kval.val[kval.len]), &(clistLocal[0].data), cdesc->length);
		    kval.len += cdesc->length;
		    break;
		}
	    }

	    /*
	     * LRDS catalog file's index must be locked, too.
	     */
            if (isNullFlag == FALSE) {
		e = SM_InsertIndexEntry(handle, &(relTableEntry_ii[i].iid),
					&(relTableEntry_ii[i].kdesc.btree),
					&kval, (ObjectID*)tid, objLockupPtr); 
		if (e < 0) ERR(handle, e);
            }

	    break;

	  case SM_INDEXTYPE_MLGF:
	    /* supported types: SM_SHORT, SM_INT, SM_LONG, SM_MBR */

	    for (j = 0; j < relTableEntry_ii[i].kdesc.mlgf.nKeys; j++) { 
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[i].colNo[j]; 

		/* 'cdesc' points to the corresponding column descriptor. */
		cdesc = &(relTableEntry_cdesc[colNo]); 

		/* Load the column value. */
		clistLocal[0].colNo = colNo;
		clistLocal[0].start = ALL_VALUE;
		clistLocal[0].dataLength = cdesc->length;

		e = LRDS_FetchTuple(handle, ornOrScanId, useScanFlag, tid, 1, &(clistLocal[0]));
		if (e < 0) ERR(handle, e);

                /* if index column has null, skip it!! */
                if (clistLocal[0].nullFlag == FALSE)
                    isNullFlag = FALSE;
                else {
                    isNullFlag = TRUE;
                    break;
                }

		switch (cdesc->type) {
		  case SM_SHORT:
		    mlgfKval[j] = clistLocal[0].data.s;
		    break;

		  case SM_INT:
		    mlgfKval[j] = clistLocal[0].data.i;
		    break;

		  case SM_LONG:
		    mlgfKval[j] = clistLocal[0].data.l;
		    break;

		  case SM_MBR:
		    for (k = 0; k < MBR_NUM_PARTS; k++)
			mlgfKval[k] = clistLocal[0].data.mbr.values[k];
                    j = MBR_NUM_PARTS - 1; 
		    break;

          case SM_VARSTRING:
            isNullFlag = TRUE;
            j = MBR_NUM_PARTS - 1;
            break;

		  default:
		    ERR(handle, eINTERNAL);
		}
	    }

	    /*
	     * LRDS catalog file's index must be locked, too.
	     */
            if (isNullFlag == FALSE) {
		e = SM_MLGF_InsertIndexEntry(handle, &(relTableEntry_ii[i].iid),
					     &(relTableEntry_ii[i].kdesc.mlgf),
					     mlgfKval, (ObjectID*)tid, NULL, objLockupPtr);  
		if (e < 0) ERR(handle, e);
            }
	    break;

	  default:
	    ERR(handle, eINTERNAL);
	}
    }

    return(eNOERROR);

} /* LRDS_UpdateTuple() */



/*
 * Function: lrds_AddColumns(Four, Boolean, TupleID*, TupleHdr*)
 *
 * Description:
 *  Add columns so that the tuple header has all columns defined on that relation.
 */
Four lrds_AddColumns(
    Four handle,
    Four                ornOrScanId,            /* IN open relation no or scan id*/
    Boolean             useScanFlag,            /* IN TRUE if above parameter is scan id */
    TupleID             *tid,                   /* IN tuple to update */
    TupleHdr            *tupHdrPtr)             /* INOUT tuple header pointer */
{
    Four                e;                      /* error number */
    Four                orn;
    Four                smScanId;
    Two                 i;                      /* index variables */
    Four                oldTupHdrSize;          /* old size of a tuple header */
    Four                newTupHdrSize;          /* new size of a tuple header */
    Two                 colNo;                  /* column number */
    void                *data;                  /* new data */
    ColDesc             *cdesc;                 /* pointer to cdesc of LRDS open relation table  */
    unsigned char       *oldNullVector;         /* old bit vector of null flags */
    unsigned char       *newNullVector;         /* new bit vector of null flags */
    Two                 nOldCols;               /* # of old columns */
    Four                addedBytes;
    Four                length;
    lrds_RelTableEntry  *relTableEntry; /* pointer to an entry of relation table */
    ColDesc             *relTableEntry_cdesc; 


    TR_PRINT(TR_LRDS, TR1, ("LRDS_UpdateTuple(handle)"));


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

    nOldCols = tupHdrPtr->nFixedCols + tupHdrPtr->nVarCols;
    oldTupHdrSize = TUPLE_HEADER_SIZE(nOldCols, tupHdrPtr->nVarCols);
    newTupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    oldNullVector = NULLVECTOR_PTR(*tupHdrPtr, tupHdrPtr->nVarCols);
    newNullVector = NULLVECTOR_PTR(*tupHdrPtr, relTableEntry->ri.nVarColumns);

    /* reserve space for the variable-length column offsets */
    memmove(newNullVector, oldNullVector, TUPLE_NULLVECTOR_SIZE(nOldCols));

    addedBytes = 0;
    for (i = nOldCols; i < relTableEntry->ri.nColumns; i++) {
        /* For fast and simple access */
        cdesc = &(relTableEntry_cdesc[i]); 

        BITSET(newNullVector, i);

        if (cdesc->fixedColNo != NIL) addedBytes += cdesc->length;
    }

    /* addedBytes : # of bytes of newly stored fixed-length columns */
    length = addedBytes;
    while (length > 0) {
        /* Fill added fixed-length column area with the dummy data. */
        /* We break the large data into some pieces. */
        Two len;
        char dummy[PAGESIZE];

        len = MIN(PAGESIZE, length);
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              (ObjectID*)tid, tupHdrPtr->firstVarColOffset, 0,
                              &dummy[0], len, NULL, NULL); 
        if (e < 0) ERR(handle, e);

        length -= len;
    }

    /* addedBytes: add # of bytes of increased tuple header size */
    addedBytes += newTupHdrSize - oldTupHdrSize;

    tupHdrPtr->firstVarColOffset += addedBytes;

    for (i = 0; i < tupHdrPtr->nVarCols; i++)
        tupHdrPtr->varColOffset[i] += addedBytes;

    for ( ; i < relTableEntry->ri.nVarColumns; i++) {
        if (i == 0)
            tupHdrPtr->varColOffset[i] = tupHdrPtr->firstVarColOffset;
        else
            tupHdrPtr->varColOffset[i] = tupHdrPtr->varColOffset[i-1];
    }

    tupHdrPtr->nFixedCols = relTableEntry->ri.nColumns - relTableEntry->ri.nVarColumns;
    tupHdrPtr->nVarCols = relTableEntry->ri.nVarColumns;

    /* Update the tuple header. */
    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                          (ObjectID*)tid, 0, oldTupHdrSize, (char*)tupHdrPtr, newTupHdrSize, NULL, NULL);
    if (e < 0) ERR(handle, e);


    return(eNOERROR);

} /* lrds_AddColumns() */
