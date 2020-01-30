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
 * Module: LRDS_CreateTuple( )
 *
 * Description:
 *  Create a tuple in the given relation. The relation is specified by scanId.
 *
 * Exports:
 *  Four LRDS_CreateTuple( )
 *
 * Returns:
 *  Error code
 *     eBADPARAMETER
 *     eCOLUMNVALUEEXPECTED_LRDS
 *     eWRONGCOLUMNVALUE_LRDS
 *     some errors caused by function calls
 *
 * Assumption:
 *  1. The column values on which some index is definded should be given.
 *  2. In the ColListStruct only 'colNo' and 'dataLength' fields are used.
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#ifndef COSMOS_S
#include "LM.h"
#endif /* COSMOS_S */
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Internal Function Prototype
 */
Four lrds_EncodeTuple(Four, Four, Four, ColDesc[], Four, char*, Four, ColListStruct[], Four*);


Four LRDS_CreateTuple(
    Four handle,
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    Four nCols,			/* IN number of columns */
    ColListStruct *clist,	/* IN data of the created columns */
    TupleID *tid)		/* OUT TupleID of the newly created tuple */
{
    Four e, e2;			/* error number */
    Four orn;
    Four smScanId;
    Four i, j, k;		/* index variables */
    Four v;			/* index on LRDS mount table */
    Four length;		/* some lengths */
    Four tupHdrSize;            /* size of a tuple header */
    Four colNo;			/* variable for caching column number */
    Four nVarCols;		/* # of variable-length columns */
    Four smScanIdForNearObject; /* SM level scan for searching near object */
    Four catScanId;		/* scan on a catalog table */
    Four clusteringIndex;	/* array index of clustering index on an index table */
    Two  s;			/* temporary variable to conver to type Two */
    Two  tag[MAXNUMOFCOLS];	/* array used to sort the 'clist' by colNo */
    TupleHdr tupHdr;		/* a tuple header */
    KeyValue kval;		/* a key value */
    ObjectID oid;		/* ObjectID of the newly created object */
    ObjectID nearOid;		/* ObjectID of near object */
    ObjectID *nearOidPtr;	/* pointer to nearOid */
    unsigned char *nullVector;	/* bit array of null flags */
    BoundCond startBound;	/* start Bound condition of range scan */
    BoundCond stopBound;	/* stop Bound condition of range scan */
    ColListStruct clistLocal[1]; /* a column list structure */
    LockReply lockReply;		/* lock reply */
    LockMode oldMode;
    Boolean notSystable;	/* flag whether target is LRDS_SYSTABLE */
    PageID catalogPid;
    MLGF_HashValue mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    LockParameter nearLockup;    /* request lock for near object or not */
    LockParameter nearFileLockup;    /* request lock for file or not */
    LockParameter *nearLockupPtr;      /* pointer to the lockup value for near object */
    LockParameter *nearFileLockupPtr;  /* pointer to the lockup value for file */
    Four tupleLen;
    char tupleBuf[PAGESIZE/sizeof(ALIGN_TYPE)*sizeof(ALIGN_TYPE)];
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    Boolean        isNullFlag;   /* flag which indicates index column is null or not */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_CreateTuple()"));


    /* check parameter */
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

    if (nCols < 0) ERR(handle, eBADPARAMETER);

    if (nCols != 0 && clist == NULL) ERR(handle, eBADPARAMETER);

    /* set the nearLockup parameter for nearObj */
    /* no lock for the temporary relation */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	nearLockupPtr = NULL;
	nearFileLockupPtr = NULL;

    } else {
	nearLockupPtr = NULL;
	nearFileLockupPtr = NULL;
    }


    /* Find the corresponding mount table entry */
    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == relTableEntry->ri.fid.volNo) break;
    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    /* To avoid the name conflicts on LRDS_SYSTABLES */
    /* Follow the locking mechanism of btree and the hierarchical lock */
    notSystable = TRUE;
    if (strcmp(relTableEntry->ri.relName, lrds_perThreadDSptr->catalogTable[LRDS_SYSTABLES]) == 0) {
	notSystable = FALSE;
	e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid, L_IX, L_COMMIT, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    /* We can find the corresponding clist entry using 'tag' array. */
    /* tag[i] == NIL : no 'clist' entry */
    for (i = 0; i < relTableEntry->ri.nColumns; i++) tag[i] = NIL;

    for (i = 0; i < nCols; i++)	{
	tag[clist[i].colNo] = i;

	/* Use it's own API for the special types. */
	if (LRDS_HAS_ITS_OWN_API(relTableEntry_cdesc[clist[i].colNo].complexType, relTable_cdesc[clist[i].colNo].type)) 
	    ERR(handle, eBADPARAMETER);
    }


    /* Get the near tuple for clustering. */
    if (relTableEntry->clusteringIndex != NIL) {

	clusteringIndex = relTableEntry->clusteringIndex;

	switch (relTableEntry_ii[clusteringIndex].indexType) {
	  case SM_INDEXTYPE_BTREE:
	    /* Set the boundary condition. */
	    startBound.op = SM_GE;

	    /* Construct 'kval'. */
	    startBound.key.len = 0;
	    for (j = 0; j < relTableEntry_ii[clusteringIndex].kdesc.btree.nparts; j++) {
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[clusteringIndex].colNo[j];

		if (tag[colNo] == NIL || clist[tag[colNo]].nullFlag == TRUE) ERR(handle, eCOLUMNVALUEEXPECTED_LRDS);

		/* The column on which an index has been defined shoule have */
		/* a complete column value. */
		if (relTableEntry_cdesc[colNo].type == SM_STRING &&
		    clist[tag[colNo]].dataLength != relTableEntry_cdesc[colNo].length)
		    ERR(handle, eWRONGCOLUMNVALUE_LRDS);

		switch (relTableEntry_cdesc[colNo].type) {
		  case SM_VARSTRING:
		    /* copy the 'data length' to the temporary variable of type Two. */
		    s = clist[tag[colNo]].dataLength;
		    memcpy(&(startBound.key.val[startBound.key.len]), &s, sizeof(Two));
		    startBound.key.len += sizeof(Two);
		    memcpy(&(startBound.key.val[startBound.key.len]), clist[tag[colNo]].data.ptr,
			  clist[tag[colNo]].dataLength);
		    startBound.key.len += clist[tag[colNo]].dataLength;
		    break;

		  case SM_STRING:
		    memcpy(&(startBound.key.val[startBound.key.len]), clist[tag[colNo]].data.ptr,
			  relTableEntry_cdesc[colNo].length);
		    startBound.key.len += relTableEntry_cdesc[colNo].length;
		    break;

		  default:
		    memcpy(&(startBound.key.val[startBound.key.len]), &(clist[tag[colNo]].data),
			  relTableEntry_cdesc[colNo].length);
		    startBound.key.len += relTableEntry_cdesc[colNo].length;
		    break;
		}
	    }

	    stopBound.op = SM_EOF;

	    smScanIdForNearObject =
                SM_OpenIndexScan(handle, &(relTableEntry->ri.fid),
                                 &(relTableEntry_ii[relTableEntry->clusteringIndex].iid),
                                 &(relTableEntry_ii[relTableEntry->clusteringIndex].kdesc.btree),
                                 &startBound, &stopBound,
                                 (relTableEntry->isCatalog && notSystable) ? NULL:nearFileLockupPtr);
	    if (smScanIdForNearObject < 0) ERR(handle, smScanIdForNearObject);

	    e = SM_NextObject(handle, smScanIdForNearObject, &nearOid, (ObjectHdr*)NULL, NULL, NULL,
			      (relTableEntry->isCatalog && notSystable) ? NULL:nearLockupPtr);
	    if (e < eNOERROR) ERR(handle, e);

	    if (e == EOS) {
		e = SM_CloseScan(handle, smScanIdForNearObject);
		if (e < eNOERROR) ERR(handle, e);

		/* Find the object which have the key less than the given tuple. */
		startBound.op = SM_LT;
		stopBound.op = SM_BOF;
		smScanIdForNearObject = SM_OpenIndexScan(handle, &(relTableEntry->ri.fid),
					    &(relTableEntry_ii[relTableEntry->clusteringIndex].iid),
					    &(relTableEntry_ii[relTableEntry->clusteringIndex].kdesc.btree),
					    &startBound, &stopBound,
					    (relTableEntry->isCatalog && notSystable) ? NULL:nearFileLockupPtr);
		if (smScanIdForNearObject < 0) ERR(handle, smScanIdForNearObject);

		e = SM_NextObject(handle, smScanIdForNearObject, &nearOid, (ObjectHdr*)NULL, NULL, NULL,
				  (relTableEntry->isCatalog && notSystable) ? NULL:nearLockupPtr);
		if (e < eNOERROR) ERR(handle, e);

		if (e != EOS) nearOidPtr = &nearOid;
		else nearOidPtr = (ObjectID*)NULL; /* Set the nearOidPtr to NULL. */

		e = SM_CloseScan(handle, smScanIdForNearObject);
		if (e < eNOERROR) ERR(handle, e);

	    } else {
		nearOidPtr = &nearOid;

		e = SM_CloseScan(handle, smScanIdForNearObject);
		if (e < eNOERROR) ERR(handle, e);
	    }
	    break;

	  case SM_INDEXTYPE_MLGF:
	    for (j = 0; j < relTableEntry_ii[clusteringIndex].kdesc.mlgf.nKeys; j++) {
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[clusteringIndex].colNo[j];

		if (tag[colNo] == NIL || clist[tag[colNo]].nullFlag == TRUE) ERR(handle, eCOLUMNVALUEEXPECTED_LRDS);

		switch (relTableEntry_cdesc[colNo].type) {
		  case SM_SHORT:
		    mlgfKval[j] = clist[tag[colNo]].data.s;
		    break;

		  case SM_INT:
		    mlgfKval[j] = clist[tag[colNo]].data.i;
		    break;

		  case SM_LONG:
		    mlgfKval[j] = clist[tag[colNo]].data.l;
		    break;

		  case SM_MBR:
		    for (k = 0; k < MBR_NUM_PARTS; k++)
			mlgfKval[k] = clist[tag[colNo]].data.mbr.values[k];
                    j = MBR_NUM_PARTS - 1;
		    break;

		  default:
		    ERR(handle, eINTERNAL);
		}
	    }


	    e = SM_MLGF_SearchNearObject(handle, &(relTableEntry_ii[clusteringIndex].iid),
					 &(relTableEntry_ii[clusteringIndex].kdesc.mlgf),
					 mlgfKval, &nearOid,
					 (relTableEntry->isCatalog && notSystable) ? NULL:nearLockupPtr);
	    if (e < eNOERROR) ERR(handle, e);

	    nearOidPtr = &nearOid;
	    break;

	  default:
	    ERR(handle, eINTERNAL);
	}
    } else
	nearOidPtr = NULL;

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

    e = lrds_EncodeTuple(handle, relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns,
                         relTableEntry_cdesc, sizeof(tupleBuf), (char*)tupleBuf,
                         nCols, clist, &tupleLen);
    if (e < eNOERROR) ERR(handle, e);

    if (tupleLen != -1) {       /* tuple is normal size */
        /* Create an object. */
        e = LRDS_CREATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              nearOidPtr, (ObjectHdr*)NULL, tupleLen, (char*)tupleBuf, &oid,
                              (relTableEntry->isCatalog && notSystable) ? NULL:fileLockupPtr,
                              (relTableEntry->isCatalog && notSystable) ? NULL:objLockupPtr);
        if (e < 0) 	ERR(handle, e);

    } else {                    /* tuple is too large */
        /* Construct tuple header. */
        tupHdr.nFixedCols = relTableEntry->ri.nColumns - relTableEntry->ri.nVarColumns;
        tupHdr.nVarCols = relTableEntry->ri.nVarColumns;

        tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

        tupHdr.firstVarColOffset = tupHdrSize;
        for (i = 0; i < relTableEntry->ri.nColumns; i++)
            if (relTableEntry_cdesc[i].fixedColNo != NIL)
                tupHdr.firstVarColOffset += relTableEntry_cdesc[i].length;

        nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

        /* Create an object. */
        e = LRDS_CREATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              nearOidPtr, (ObjectHdr*)NULL, tupHdrSize, (char *)&tupHdr, &oid,
                              (relTableEntry->isCatalog && notSystable) ? NULL:fileLockupPtr,
                              (relTableEntry->isCatalog && notSystable) ? NULL:objLockupPtr);

        if (e < 0) 	ERR(handle, e);


        /*
        ** Store fixed-length columns.
        */
        for (i = 0; i < relTableEntry->ri.nColumns; i++) {
            /* For fast and simple access */
            cdesc = &(relTableEntry_cdesc[i]);

            /* Skip the variable-length column. */
            if (cdesc->fixedColNo == NIL) continue;

            /* length of data to store. */
            length = cdesc->length;

            if (tag[i] == NIL || clist[tag[i]].nullFlag == TRUE) {
                /* Set the corresponding bit of null vector. */
                BITSET(nullVector, i);
            } else {
                /* Reset the corresponding bit of null vector. */
                BITRESET(nullVector, i);

                /* Add the column. */
                if (cdesc->type == SM_STRING) {
                    if (clist[tag[i]].dataLength > length) ERR(handle, eBADPARAMETER);

                    length -= clist[tag[i]].dataLength;

                    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                          &oid, END, 0, clist[tag[i]].data.ptr,
                                          clist[tag[i]].dataLength, NULL, NULL);
                } else {
                    length -= cdesc->length;

                    e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                          &oid, END, 0, &(clist[tag[i]].data),
                                          cdesc->length, NULL, NULL);
                }
                if (e < eNOERROR) ERR(handle, e);
            }

            while (length > 0) {
                /* Fill remained area with the dummy data. */
                /* We break the large data into some pieces. */
                Two len;
                char dummy[PAGESIZE];

                len = MIN(PAGESIZE, length);
                e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                      &oid, END, 0, &dummy, len, NULL, NULL);
                if (e < eNOERROR) ERR(handle, e);

                length -= len;
            }
        }


        /*
        ** Store variable-length column.
        */
        for (i = 0; i < relTableEntry->ri.nColumns; i++) {
            /* For fast and simple access */
            cdesc = &(relTableEntry_cdesc[i]);

            /* Skip fixed-length columns. */
            if (cdesc->varColNo == NIL) continue;

            if (tag[i] == NIL || clist[tag[i]].nullFlag == TRUE) {
                /* Set the corresponding bit of null vector. */
                BITSET(nullVector, i);

                length = 0;
            } else {
                /* Reset the corresponding bit of null vector. */
                BITRESET(nullVector, i);

                /* Add the column. */
                e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                      &oid, END, 0, clist[tag[i]].data.ptr,
                                      clist[tag[i]].dataLength, NULL, NULL);
                if (e < eNOERROR) ERR(handle, e);

                length = clist[tag[i]].dataLength;
            }

            if (cdesc->varColNo == 0)
                tupHdr.varColOffset[cdesc->varColNo] = tupHdr.firstVarColOffset + length;
            else
                tupHdr.varColOffset[cdesc->varColNo] = tupHdr.varColOffset[cdesc->varColNo-1] + length;
        }

        /* Store 'varColOffset' and 'nullVector'. */
        e = LRDS_UPDATEOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                              &oid, TUPLE_HEADER_FIXED, tupHdrSize - TUPLE_HEADER_FIXED,
                              &(tupHdr.varColOffset[0]), tupHdrSize-TUPLE_HEADER_FIXED, NULL, NULL);
        if (e < eNOERROR) ERR(handle, e);
    }

    /*
    ** Automatic Index Supports
    */
    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

	if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT) continue;

	switch (relTableEntry_ii[i].indexType) {
	  case SM_INDEXTYPE_BTREE:
	    /* Construct 'kval'. */
	    kval.len = 0;
	    for (j = 0; j < relTableEntry_ii[i].kdesc.btree.nparts; j++) {
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[i].colNo[j];

		if (tag[colNo] != NIL && clist[tag[colNo]].nullFlag != TRUE)
                    isNullFlag = FALSE;
                else {
                    isNullFlag = TRUE;
                    break;
                }

		/* The column on which an index has been defined shoule have */
		/* a complete column value. */
		if (relTableEntry_cdesc[colNo].type == SM_STRING &&
		    clist[tag[colNo]].dataLength != relTableEntry_cdesc[colNo].length)
		    ERR(handle, eWRONGCOLUMNVALUE_LRDS);

		switch (relTableEntry_cdesc[colNo].type) {
		  case SM_VARSTRING:
		    /* copy the 'data length' to the temporary variable of type Two. */
		    s = clist[tag[colNo]].dataLength;
		    memcpy(&(kval.val[kval.len]), &s, sizeof(Two));
		    kval.len += sizeof(Two);
		    memcpy(&(kval.val[kval.len]), clist[tag[colNo]].data.ptr,
			  clist[tag[colNo]].dataLength);
		    kval.len += clist[tag[colNo]].dataLength;
		    break;

		  case SM_STRING:
		    memcpy(&(kval.val[kval.len]), clist[tag[colNo]].data.ptr,
			  relTableEntry_cdesc[colNo].length);
		    kval.len += relTableEntry_cdesc[colNo].length;
		    break;

		  default:
		    memcpy(&(kval.val[kval.len]), &(clist[tag[colNo]].data),
			  relTableEntry_cdesc[colNo].length);
		    kval.len += relTableEntry_cdesc[colNo].length;
		    break;
		}
	    }

	    /*
	     * LRDS catalog file's index must be locked, too
	     */
            if (isNullFlag == FALSE) {
		e = SM_InsertIndexEntry(handle, &(relTableEntry_ii[i].iid),
					&(relTableEntry_ii[i].kdesc.btree),
					&kval, &oid, objLockupPtr);
		if (e < eNOERROR) ERR(handle, e);
	    }
	    break;

	  case SM_INDEXTYPE_MLGF:
	    /* supported types: SM_SHORT, SM_INT, SM_LONG, SM_MBR */
	    for (j = 0; j < relTableEntry_ii[i].kdesc.mlgf.nKeys; j++) {
		/* save the colNo to local variable. */
		colNo = relTableEntry_ii[i].colNo[j];


		if (tag[colNo] != NIL && clist[tag[colNo]].nullFlag != TRUE)
                    isNullFlag = FALSE;
                else {
                    isNullFlag = TRUE;
                    break;
                }

		switch (relTableEntry_cdesc[colNo].type) {
		  case SM_SHORT:
		    mlgfKval[j] = clist[tag[colNo]].data.s;
		    break;

		  case SM_INT:
		    mlgfKval[j] = clist[tag[colNo]].data.i;
		    break;

		  case SM_LONG:
		    mlgfKval[j] = clist[tag[colNo]].data.l;
		    break;

		  case SM_MBR:
		    for (k = 0; k < MBR_NUM_PARTS; k++)
			mlgfKval[k] = clist[tag[colNo]].data.mbr.values[k];
                    j = MBR_NUM_PARTS - 1;
		    break;

          /* OpenGIS ... */
          /* GEOM handles automatic indexing for UDT of geometry type */
          case SM_VARSTRING:
            isNullFlag = TRUE;
            j = MBR_NUM_PARTS - 1;
            break;

		  default:
		    ERR(handle, eINTERNAL);
		}
	    }

	    /*
	     * LRDS catalog file's index must be locked, too
	     */
            if (isNullFlag == FALSE) {
		e = SM_MLGF_InsertIndexEntry(handle, &(relTableEntry_ii[i].iid),
					     &(relTableEntry_ii[i].kdesc.mlgf),
					     mlgfKval, &oid, NULL, objLockupPtr);
		if (e < eNOERROR) ERR(handle, e);
	    }
	    break;

	  default:
	    ERR(handle, eINTERNAL);
	}
    }


    /*
    ** Increment the number of tuples by 1.
    ** If this relation is an ordinary relation(= not temporary relation),
    ** update the value on the disk.
    */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/* Increment the number of tuples by 1. */
	relTableEntry->ri.nTuples++;

    } else {
	/* Request Manual Duration lock for update
	** Not follow Lock Hierarchy
	*/

#ifdef SYSTABLE_RECORD_LOCKING
	e = LM_getFlatObjectLock(handle, &MY_XACTID(handle),
			       &(relTableEntry->ri.catalogEntry),
			       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);
#else
	/* get the pid from the oid of the catalog entry */
	MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);

	e = LM_getFlatPageLock(handle, &MY_XACTID(handle),
			       &catalogPid,
			       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);
#endif

	if ( lockReply == LR_DEADLOCK )
	    ERR(handle, eDEADLOCK);

	/* Increment the number of tuples by 1. */
	relTableEntry->ri.nTuples++;

	/*
     * Update the catalog table LRDS_SYSTABLES.
	 */
	 
	/* Construct a column list. */
	clistLocal[0].colNo = LRDS_SYSTABLES_NTUPLES_COLNO;
	clistLocal[0].nullFlag = FALSE;
	clistLocal[0].start = ALL_VALUE;
	clistLocal[0].dataLength = sizeof(Four);
	ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clistLocal[0], relTableEntry->ri.nTuples, Four);

	e = LRDS_UpdateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES],
                             FALSE, (TupleID*)&(relTableEntry->ri.catalogEntry), 1, &(clistLocal[0]));
	if (e < 0) {

#ifdef SYSTABLE_RECORD_LOCKING
	    e2 = LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL);
	    if ( e2 < 0 ) ERR(handle, e2);
#else
	    e2 = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
	    if ( e2 < 0 ) ERR(handle, e2);
#endif

	    ERR(handle, e);
	}

#ifdef SYSTABLE_RECORD_LOCKING
	e = LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL);
	if ( e < eNOERROR ) ERR(handle, e);
#else
	e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
	if ( e < eNOERROR ) ERR(handle, e);
#endif
    }

    if (tid != NULL) *tid = *((TupleID*)&oid);

    return(eNOERROR);

} /* LRDS_CreateTuple() */



/*
 * Function: lrds_EncodeTuple(Four, Four, Four, ColDesc[], Four, char*, Four, ColListStruct[], Four*)
 *
 * Description:
 *  Encode a tuple.
 *
 * Returns:
 *  error code
 */
Four lrds_EncodeTuple(
    Four handle,
    Four nColumns,              /* IN # of columns in the relation */
    Four nVarColumns,           /* IN # of variable-length columns in the relation */
    ColDesc cdesc[],            /* IN column description for each column */
    Four tupleBufSize,          /* IN tuple buffer size */
    char *tupleBuf,             /* OUT encoded tuple buffer */
    Four nColumnsToStore,       /* IN # of columns to store */
    ColListStruct clist[],      /* IN column list structure */
    Four *tupleLen)             /* OUT tuple length after tuple encoding */
                                /*     it has -1 when buffer size is small than encoded size */
{
    Two  tag[MAXNUMOFCOLS];	/* array used to sort the 'clist' by colNo */
    TupleHdr *tupHdrPtr;        /* pointer to tuple header */
    Four tupHdrSize;            /* size of a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    Four length;		/* some lengths */
    Four tupleBufIdx;
    Four i, j;


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_EncodeTuple()"));


    /* We can find the corresponding clist entry using 'tag' array. */
    /* tag[i] == NIL : no 'clist' entry */
    for (i = 0; i < nColumns; i++) tag[i] = NIL;

    for (i = 0; i < nColumnsToStore; i++) tag[clist[i].colNo] = i;

    /* Construct tuple header. */
    tupHdrPtr = (TupleHdr*)tupleBuf;
    tupHdrPtr->nFixedCols = nColumns - nVarColumns;
    tupHdrPtr->nVarCols = nVarColumns;

    tupHdrSize = TUPLE_HEADER_SIZE(nColumns, nVarColumns);

    tupHdrPtr->firstVarColOffset = tupHdrSize;
    for (i = 0; i < nColumns; i++)
        if (cdesc[i].fixedColNo != NIL)
            tupHdrPtr->firstVarColOffset += cdesc[i].length;

    nullVector = NULLVECTOR_PTR(*tupHdrPtr, tupHdrPtr->nVarCols);


    /*
    ** Store fixed-length columns.
    */
    tupleBufIdx = tupHdrSize;

    for (i = 0; i < nColumns; i++) {
        /* Skip the variable-length column. */
        if (cdesc[i].fixedColNo == NIL) continue;

        /* length of data to store. */
        length = cdesc[i].length;

        if (tupleBufIdx + length > tupleBufSize) {
            *tupleLen = -1;
            return(eNOERROR);
        }

        if (tag[i] == NIL || clist[tag[i]].nullFlag == TRUE) {
            /* Set the corresponding bit of null vector. */
            BITSET(nullVector, i);
        } else {
            /* Reset the corresponding bit of null vector. */
            BITRESET(nullVector, i);

            /* Add the column. */
            if (cdesc[i].type == SM_STRING) {
                if (clist[tag[i]].dataLength > length) ERR(handle, eBADPARAMETER);

                memcpy(&tupleBuf[tupleBufIdx], clist[tag[i]].data.ptr, clist[tag[i]].dataLength);
                /* remained area of this column is filled with the dummy data. */

            } else {
                memcpy(&tupleBuf[tupleBufIdx], &(clist[tag[i]].data), length);
            }
        }

        tupleBufIdx += length;
    }



    /*
    ** Store variable-length column.
    */
    for (i = 0; i < nColumns; i++) {
        /* Skip fixed-length columns. */
        if (cdesc[i].varColNo == NIL) continue;

        if (tag[i] == NIL || clist[tag[i]].nullFlag == TRUE) {
            length = 0;

            /* Set the corresponding bit of null vector. */
            BITSET(nullVector, i);

        } else {
            length = clist[tag[i]].dataLength;


            if (tupleBufIdx + length > tupleBufSize) {
                *tupleLen = -1;
                return(eNOERROR);
            }

            /* Reset the corresponding bit of null vector. */
            BITRESET(nullVector, i);

            /* Add the column. */
            memcpy(&tupleBuf[tupleBufIdx], clist[tag[i]].data.ptr, length);
            tupleBufIdx += length;
        }


        if (cdesc[i].varColNo == 0)
            tupHdrPtr->varColOffset[cdesc[i].varColNo] = tupHdrPtr->firstVarColOffset + length;
        else
            tupHdrPtr->varColOffset[cdesc[i].varColNo] = tupHdrPtr->varColOffset[cdesc[i].varColNo-1] + length;
    }

    *tupleLen = tupleBufIdx;

    return(eNOERROR);

} /* lrds_EncodeTuple() */

