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
 * Module: LRDS_DestroyTuple.c
 *
 * Description:
 *  Destroy the given tuple. If the 'tid' is NULL then destroy the tuple
 *  pointed by the current cursor.
 *
 * Exports:
 *  Four LRDS_DestroyTuple(Four, Four, TupleID*)
 *
 * Returns:
 *  Error code
 *   eBADPARAMETER
 *   eINVALIDCURRENTTUPLE_LRDSID_LRDS
 *   some errors caused by function calls
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


Four LRDS_DestroyTuple(
    Four handle,
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid)		/* IN tuple to destroy */
{
    Four e;			/* error number */
    Four orn;
    Four smScanId;
    Four i, j, k;		/* index variable */
    Four v;			/* index on LRDS mount table */
    Four colNo;			/* column number */
    Four catScanId;		/* scan on a catalog table */
    Two  s;			/* a variable to conver into type Two */
    KeyValue kval;		/* a key value */
    ColDesc *cdesc;		/* pointer to a column descriptor */
    ColListStruct clist[1];	/* a column list structure */
    char buf[MAXKEYLEN];	/* buffer to load a column value */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;
    Boolean notSystable;	/* flag whether target is LRDS_SYSTABLE */
    PageID catalogPid;
    Four tupHdrSize;		/* size of tuple header */
    TupleHdr tupHdr;		/* a tuple header */
    LRDS_MBR mbr;		/* mbr */
    MLGF_HashValue mlgfKval[MLGF_MAXNUM_KEYS]; /* hash values */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;
    unsigned char* nullVector;  /* null vecor */
    Boolean        isNullFlag;   /* flag which indicates index column is null or not */

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_DestroyTuple()"));


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

    /* If 'tid' is NULL, make 'tid' points to the current tuple. */
    if (useScanFlag && tid == NULL) {
	if (IS_NILTUPLEID(LRDS_SCANTABLE(handle)[ornOrScanId].tid))
	    ERR(handle, eINVALIDCURRENTTUPLE_LRDS);

	tid = &(LRDS_SCANTABLE(handle)[ornOrScanId].tid);
    }

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == relTableEntry->ri.fid.volNo) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

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


    /* To avoid the name conflicts on LRDS_SYSTABLES */
    notSystable = TRUE;
    if (strcmp(relTableEntry->ri.relName, lrds_perThreadDSptr->catalogTable[LRDS_SYSTABLES]) == 0) {
	notSystable = FALSE;
	e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid,
			   L_IX, L_COMMIT, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    /*
    ** Automatic Index Supports
    */
    /* lock on the data file that the automatic index is built on to support the automatic index */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag && (!relTableEntry->isCatalog || !notSystable)) {

        /* check lock parameter */
        if (fileLockup.duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        if (fileLockup.mode != L_X && fileLockup.mode != L_IX && fileLockup.mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);

        /* lock on the data file */
        e = LM_getFileLock(handle, &MY_XACTID(handle), &relTableEntry->ri.fid, fileLockup.mode, fileLockup.duration,
                           L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
    }

    /* If tupHdr.nFixedCols is equal to -1, then we don't read the tuple header. */
    tupHdr.nFixedCols = -1;

    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

	if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT)
	continue;

	if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_SET) { 
	    /* set type column */
	    Four start;		/* starting offset of the column */

	    if (tupHdr.nFixedCols == -1) {
		/* Fetch the tuple header. */
		tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

		e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                     (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                                     (relTableEntry->isCatalog && notSystable) ? NULL : fileLockupPtr,
                                     (relTableEntry->isCatalog && notSystable) ? NULL : objLockupPtr);
		if (e < eNOERROR) ERR(handle, e);
	    }

	    /* 'cdesc' points to the corresponding column descriptor. */
	    cdesc = &(relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]]);


	    /*
	     *  Null check
	     */

            /* 'nullVector' points to bit array of null flags. */
            nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

            /* if index column has null, skip it!! */
            if (LRDS_HAS_NULL_VALUE(relTableEntry_ii[i].colNo[0], cdesc, tupHdr, nullVector)) continue;

	    /* Delete entries from the index. */
	    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
	    e = lrds_Set_RemoveFromIndex(handle, ornOrScanId, useScanFlag, i, (ObjectID*)tid,
					 relTableEntry_ii[i].colNo[0], start,
					 LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr),
                                         fileLockupPtr, objLockupPtr);
	    if (e < eNOERROR) ERR(handle, e);

	}
        else if(relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType ==
                SM_COMPLEXTYPE_COLLECTIONSET ||
                relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType ==
                SM_COMPLEXTYPE_COLLECTIONBAG ||
                relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType ==
                SM_COMPLEXTYPE_COLLECTIONLIST)
        {
            /* set type column */
	    Four start;		/* starting offset of the column */

	    if (tupHdr.nFixedCols == -1) {
		/* Fetch the tuple header. */
		tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns,
                                               relTableEntry->ri.nVarColumns);

		e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                                     (ObjectID*)tid, 0, tupHdrSize, (char*)&tupHdr,
                                     (relTableEntry->isCatalog && notSystable) ? NULL : fileLockupPtr,
                                     (relTableEntry->isCatalog && notSystable) ? NULL : objLockupPtr);
		if (e < eNOERROR) ERR(handle, e);
	    }

	    /* 'cdesc' points to the corresponding column descriptor. */
	    cdesc = &(relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]]);

            /*
             *  Null check
             */

            /* 'nullVector' points to bit array of null flags. */
            nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);

            /* if index column has null, skip it!! */
            if (LRDS_HAS_NULL_VALUE(relTableEntry_ii[i].colNo[0], cdesc, tupHdr, nullVector)) continue;

	    /* Delete entries from the index. */
	    start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
	    e = lrds_Collection_RemoveFromIndex(handle, ornOrScanId, useScanFlag,
                                                &relTableEntry_ii[i], tid,
                                                relTableEntry_ii[i].colNo[0],
                                                fileLockupPtr, objLockupPtr);
	    if (e < eNOERROR) ERR(handle, e);
        }
	else {
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
		    clist[0].colNo = colNo;
		    clist[0].start = ALL_VALUE;
		    clist[0].dataLength = cdesc->length;
		    if (cdesc->type == SM_STRING || cdesc->type == SM_VARSTRING)
			clist[0].data.ptr = &(buf[0]);

		    e = LRDS_FetchTuple(handle, ornOrScanId, useScanFlag, tid, 1, &(clist[0]));
		    if (e < eNOERROR) ERR(handle, e);

                    /* if index column has null, skip it!! */
                    if (clist[0].nullFlag == FALSE)
                        isNullFlag = FALSE;
                    else {
                        isNullFlag = TRUE;
                        break;
                    }

		    switch (cdesc->type) {
		      case SM_VARSTRING:
			/* copy the 'data length' to the temporary variable of type Two. */
			s = clist[0].retLength;
			memcpy(&(kval.val[kval.len]), &s, sizeof(Two));
			kval.len += sizeof(Two);
			memcpy(&(kval.val[kval.len]), clist[0].data.ptr, clist[0].retLength);
			kval.len += clist[0].retLength;
			break;

		      case SM_STRING:
			memcpy(&(kval.val[kval.len]), clist[0].data.ptr, cdesc->length);
			kval.len += cdesc->length;
			break;

		      default:
			memcpy(&(kval.val[kval.len]), &(clist[0].data), cdesc->length);
			kval.len += cdesc->length;
			break;
		    }
		}

		/*
		 * LRDS catalog file's index must be locked, too
		 */
                if (isNullFlag == FALSE) {
		    e = SM_DeleteIndexEntry(handle, &(relTableEntry_ii[i].iid),
					    &(relTableEntry_ii[i].kdesc.btree),
					    &kval, (ObjectID*)tid, objLockupPtr);
		    if (e < eNOERROR) ERR(handle, e);
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
		    clist[0].colNo = colNo;
		    clist[0].start = ALL_VALUE;
		    clist[0].dataLength = cdesc->length;

		    e = LRDS_FetchTuple(handle, ornOrScanId, useScanFlag, tid, 1, &(clist[0]));
		    if (e < eNOERROR) ERR(handle, e);

                    /* if index column has null, skip it!! */
                    if (clist[0].nullFlag == FALSE)
                        isNullFlag = FALSE;
                    else {
                        isNullFlag = TRUE;
                        break;
                    }

		    switch (cdesc->type) {
		      case SM_SHORT:
			mlgfKval[j] = clist[0].data.s;
			break;

		      case SM_INT:
			mlgfKval[j] = clist[0].data.i;
			break;

		      case SM_LONG:
			mlgfKval[j] = clist[0].data.l;
			break;

		      case SM_MBR:
			for (k = 0; k < MBR_NUM_PARTS; k++)
			    mlgfKval[k] = clist[0].data.mbr.values[k];
                        j = MBR_NUM_PARTS - 1;
			break;

              /* OpenGIS ... */
              /* Geometry Type의 UDT에 대한 automatic indexing은 GEOM에서 수행한다. */
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
		    e = SM_MLGF_DeleteIndexEntry(handle, &(relTableEntry_ii[i].iid),
					         &(relTableEntry_ii[i].kdesc.mlgf),
					         mlgfKval, (ObjectID*)tid, objLockupPtr);
		    if (e < eNOERROR) ERR(handle, e);
                }
		break;

	      default:
		ERR(handle, eINTERNAL);
	    } /* end of switch */
	} /* end of if */
    }


    /* Destroy the specified object. */
    e = LRDS_DESTROYOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag, tid,
                           (relTableEntry->isCatalog && notSystable) ? NULL : fileLockupPtr,
                           (relTableEntry->isCatalog && notSystable) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** Decrement the number of tuples by 1.
    ** If this relation is an ordinary relation(= not temporary relation),
    ** update the value on the disk.
    */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/* Decrement the number of tuples by 1. */
	relTableEntry->ri.nTuples--;

    } else {

#ifdef SYSTABLE_RECORD_LOCKING
	e = LM_getFlatObjectLock(handle, &MY_XACTID(handle),
			       &(relTableEntry->ri.catalogEntry),
			       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
	if ( e < eNOERROR ) ERR(handle, e);
#else
	/* get the pid from the oid of the catalog entry */
	MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);

	e = LM_getFlatPageLock(handle, &MY_XACTID(handle),
			       &catalogPid,
			       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
	if ( e < eNOERROR ) ERR(handle, e);
#endif

	if ( lockReply == LR_DEADLOCK )
	    ERR(handle, eDEADLOCK);

	/* Decrement the number of tuples by 1. */
	relTableEntry->ri.nTuples--;

	/*
         * Update the catalog table LRDS_SYSTABLES.
         */

	/* Construct a column list. */
	clist[0].colNo = LRDS_SYSTABLES_NTUPLES_COLNO;
	clist[0].nullFlag = FALSE;
	clist[0].start = ALL_VALUE;
	ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[0], relTableEntry->ri.nTuples, Four);

	e = LRDS_UpdateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES], FALSE, (TupleID*)&(relTableEntry->ri.catalogEntry), 1, &(clist[0]));
	if (e < 0) {

#ifdef SYSTABLE_RECORD_LOCKING
	    ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
#else
	    ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
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

    return(eNOERROR);

} /* LRDS_DestroyTuple() */
