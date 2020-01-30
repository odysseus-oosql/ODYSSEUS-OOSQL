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
 * Module: LRDS_OpenIndexScan.c
 *
 * Description:
 *  Open a LRDS level scan on the given relation. The scan will use a B+ tree
 *  to fectch all tuples in the given range and in the sorted order.
 *
 * Exports:
 *  Four LRDS_OpenIndexScan(Four, Four, IndexID*, BoundCond*, BoundCond*
 *                          Four, BoolExp[], LockParameter*)
 *
 * Returns:
 *  1) scan identifier if the return value is greater than or equal to 0.
 *  2) Error code if the return value is less than 0.
 *      eBADPARAMETER
 *      eNOTMOUNTEDVOLUME_LRDS
 *      eINDEXNOTFOUND_LRDS
 *      some errors caused by function calls
 */


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

#include <unistd.h>

Four LRDS_OpenIndexScan(
    Four handle,
    Four orn,			/* IN open relation number */
    IndexID *iid,		/* IN B+ tree to be used for this scan */
    BoundCond *startBound,	/* IN start boundary of the range */
    BoundCond *stopBound,	/* IN stop boundary of the range */
    Four nBools,		/* IN number of boolean expressions */
    BoolExp bool[],		/* IN array of boolean expressions */
    LockParameter *lockup)	/* IN lock mode & duration */
{
    Four e;			/* error code */
    Four i;			/* index variable */
    Four scanId;		/* scan id of new scan */
    Four smScanId;		/* SM level scan */
    Four indexInfoIndex;	/* corresponding entry of index info entry */
    LockReply lockReply;		/* lock reply */
    LockMode oldMode;
    PageID catalogPid;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;


    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1,
	     ("LRDS_OpenIndexScan(handle, orn=%ld, iid=%P, startBound=%P, stopBound=%P, nBools=%ld, bool=%P lockup=%P)",
	      orn, iid, startBound, stopBound, nBools, bool, lockup));


    /*
    ** check parameters.
    */
    if (!LRDS_VALID_ORN(handle, orn)) ERR(handle, eBADPARAMETER);

    if (nBools < 0 || nBools > MAXNUMOFBOOLS) ERR(handle, eBADPARAMETER);

    if (nBools != 0 && bool == NULL) ERR(handle, eBADPARAMETER);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);

    for (i = 0; i < nBools; i++) {
	if (relTableEntry_cdesc[bool[i].colNo].complexType != SM_COMPLEXTYPE_BASIC)
	    ERR(handle, eBADPARAMETER);
    }

    /*
    ** Request Manual Duration lock for read
    ** Not follow Lock Hierarchy
    */

#ifdef SYSTABLE_RECORD_LOCKING
    e = LM_getFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry),
			   L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
    if (e < eNOERROR) ERR(handle, e);
#else
    /* get the pid from the oid of the catalog entry */
    MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);

    e = LM_getFlatPageLock(handle, &MY_XACTID(handle), &catalogPid,
			   L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
    if (e < eNOERROR) ERR(handle, e);
#endif

    if ( lockReply == LR_DEADLOCK )
	 ERR(handle, eDEADLOCK);

    /* Find the empty scan table entry. */
    for (scanId = 0; scanId < lrds_perThreadDSptr->lrdsScanTable.nEntries; scanId++)
	if (LRDS_SCANTABLE(handle)[scanId].orn == NIL) break;

    if (scanId == lrds_perThreadDSptr->lrdsScanTable.nEntries) {
	/* There is no empty entry. */

	e = Util_doublesizeVarArray(handle, &(lrds_perThreadDSptr->lrdsScanTable), sizeof(lrds_ScanTableEntry));
	if (e < 0) { /* release the acquired lock */

#ifdef SYSTABLE_RECORD_LOCKING
	    ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	    ERR(handle, e);
#else
	    ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	    ERR(handle, e);
#endif

	}

	/* Initialize the newly allocated entries. */
	for (i = scanId; i < lrds_perThreadDSptr->lrdsScanTable.nEntries; i++)
	    LRDS_SCANTABLE(handle)[i].orn = NIL;
    }

    LRDS_SCANTABLE(handle)[scanId].orn = orn;

    /* Use the clustering index if the 'iid' is NULL. */
    if (iid == NULL) {
	if (relTableEntry->clusteringIndex == NIL) { /* release the acquired lock */

#ifdef SYSTABLE_RECORD_LOCKING
	    ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	    ERR(handle, eBADPARAMETER);
#else
	    ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	    ERR(handle, eBADPARAMETER);
#endif

	}

	iid = &relTableEntry_ii[relTableEntry->clusteringIndex].iid;
    }

    /* Check if the given index exist. */
    /* 'indexInfoIndex' points to the corresponding entry of index info array */
    for (indexInfoIndex = 0; indexInfoIndex < relTableEntry->ri.nIndexes; indexInfoIndex++)
	if (EQUAL_INDEXID(*iid, relTableEntry_ii[indexInfoIndex].iid)) break;

    if (indexInfoIndex == relTableEntry->ri.nIndexes) {

	/* BUG FIX */

#ifdef SYSTABLE_RECORD_LOCKING
	ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	ERR(handle, eINDEXNOTFOUND_LRDS);
#else
	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, eINDEXNOTFOUND_LRDS);
#endif
    }

    if (relTableEntry_ii[indexInfoIndex].indexType != SM_INDEXTYPE_BTREE) { 

#ifdef SYSTABLE_RECORD_LOCKING 
	ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	ERR(handle, eBADPARAMETER);
#else
	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, eBADPARAMETER);
#endif 

    }

    if ( nBools > 0 ) {
	/* Save the boolean expressions. */
	e = Util_getArrayFromLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle), nBools,
				  &(LRDS_SCANTABLE(handle)[scanId].bool));
	if ( e < eNOERROR ) {

#ifdef SYSTABLE_RECORD_LOCKING 
	    ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	    ERR(handle, e);
#else
	    ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	    ERR(handle, e);
#endif 

	}
	for (i = 0; i < nBools; i++)
	    LRDS_SCANTABLE(handle)[scanId].bool[i] = bool[i];
    } else
	LRDS_SCANTABLE(handle)[scanId].bool = NULL;

    LRDS_SCANTABLE(handle)[scanId].nBools = nBools;


    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* Open a SM level scan. */
    smScanId = SM_OpenIndexScan(handle, &(relTableEntry->ri.fid), iid,
				&(relTableEntry_ii[indexInfoIndex].kdesc.btree), 
				startBound, stopBound,
				(relTableEntry->isCatalog)?NULL:lockup);
    if (smScanId < 0) {

        /* if error occur, deallocate local memory and slot */

        /* Deallocates the memory used for boolean expressions table. */
        if (LRDS_SCANTABLE(handle)[scanId].nBools != 0) {
            e = Util_freeArrayToLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle), LRDS_SCANTABLE(handle)[scanId].bool);
            if (e < 0) { /* release the acquired lock */

#ifdef SYSTABLE_RECORD_LOCKING 
		ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
		ERR(handle, e);
#else
		ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
		ERR(handle, e);
#endif 

	    }
        }

        LRDS_SCANTABLE(handle)[scanId].orn = NIL;


#ifdef SYSTABLE_RECORD_LOCKING 
	ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	ERR(handle, smScanId);
#else
	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, smScanId);
#endif 
    }

    LRDS_SCANTABLE(handle)[scanId].smScanId = smScanId;

    /* Set the current tuple id to NIL tuple id. */
    SET_NILTUPLEID(LRDS_SCANTABLE(handle)[scanId].tid);

#ifdef SYSTABLE_RECORD_LOCKING 
    e = LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL);
    if ( e < eNOERROR ) ERR(handle, e);
#else
    e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
    if ( e < eNOERROR ) ERR(handle, e);
#endif 


    return(scanId);

} /* LRDS_OpenIndexScan( ) */
