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
 * Module: LRDS_MLGF_OpenIndexScan.c
 *
 * Description:
 *  Open a LRDS level scan on the given relation. The scan will use an MLGF
 *  index to fectch all tuples in the given range and in the sorted order.
 *
 * Exports:
 *  Four LRDS_MLGF_OpenIndexScan(Four, IndexID*, MLGF_HashValue[], MLGF_HashValue[], Four, BoolExp[], LockParameter*)
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
#include "SM_Internal.h" 
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four LRDS_MLGF_OpenIndexScan(
    Four handle,
    Four                orn,                    /* IN open relation number */
    IndexID             *iid,                   /* IN MLGF index to be used for this scan */
    MLGF_HashValue      lowerBounds[],          /* IN lower bounds of region */
    MLGF_HashValue      upperBounds[],          /* IN upper bounds of region */
    Four                nBools,                 /* IN number of boolean expressions */
    BoolExp             bool[],                 /* IN array of boolean expressions */
    LockParameter       *lockup)                /* IN lock mode & duration */
{
    Four                e;                      /* error code */
    Four                i;                      /* index variable */
    Four                scanId;                 /* scan id of new scan */
    Four                smScanId;               /* SM level scan */
    Four                indexInfoIndex;         /* corresponding entry of index info entry */
    LockReply           lockReply;              /* lock reply */
    LockMode            oldMode;
    PageID              catalogPid;
    lrds_RelTableEntry  *relTableEntry; /* pointer to an entry of relation table */
    IndexInfo           *relTableEntry_ii; 
    ColDesc             *relTableEntry_cdesc; 


    TR_PRINT(TR_LRDS, TR1,
             ("LRDS_MLGF_OpenIndexScan(handle, orn=%ld, iid=%P, lowerBounds=%P, upperBounds=%P, nBools=%ld, bool=%P lockup=%P)",
	      orn, iid, lowerBounds, upperBounds, nBools, bool, lockup));


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
    /* get the pid from the oid of the catalog entry */
    MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);
    e = LM_getFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
    if (e < eNOERROR) ERR(handle, e);
    if ( lockReply == LR_DEADLOCK )
	 ERR(handle, eDEADLOCK);

    /* Find the empty scan table entry. */
    for (scanId = 0; scanId < LRDS_PER_THREAD_DS(handle).lrdsScanTable.nEntries; scanId++)
	if (LRDS_SCANTABLE(handle)[scanId].orn == NIL) break;

    if (scanId == LRDS_PER_THREAD_DS(handle).lrdsScanTable.nEntries) {
	/* There is no empty entry. */

	e = Util_doublesizeVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable, sizeof(lrds_ScanTableEntry));
	if (e < 0) { /* release the acquired lock */
	    ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	    ERR(handle, e);
	}

	/* Initialize the newly allocated entries. */
	for (i = scanId; i < LRDS_PER_THREAD_DS(handle).lrdsScanTable.nEntries; i++)
	    LRDS_SCANTABLE(handle)[i].orn = NIL;
    }

    LRDS_SCANTABLE(handle)[scanId].orn = orn;

    /* Use the clustering index if the 'iid' is NULL. */
    if (iid == NULL) {
	if (relTableEntry->clusteringIndex == NIL) {  /* release the acquired lock */
	    ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	    ERR(handle, eBADPARAMETER);
	}

	iid = &relTableEntry_ii[relTableEntry->clusteringIndex].iid; 
    }

    /* Check if the given index exist. */
    /* 'indexInfoIndex' points to the corresponding entry of index info array */
    for (indexInfoIndex = 0; indexInfoIndex < relTableEntry->ri.nIndexes; indexInfoIndex++)
	if (EQUAL_INDEXID(*iid, relTableEntry_ii[indexInfoIndex].iid)) break; 

    if (indexInfoIndex == relTableEntry->ri.nIndexes) {

	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, eINDEXNOTFOUND_LRDS);
    }

    if (relTableEntry_ii[indexInfoIndex].indexType != SM_INDEXTYPE_MLGF) { 

	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, eBADPARAMETER);
    }

    if ( nBools > 0 ) {
	/* Save the boolean expressions. */
	e = Util_getArrayFromLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle), nBools, &(LRDS_SCANTABLE(handle)[scanId].bool));
	if ( e < eNOERROR ) {
	    ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	    ERR(handle, e);
	}
	for (i = 0; i < nBools; i++)
	    LRDS_SCANTABLE(handle)[scanId].bool[i] = bool[i];
    } else
	LRDS_SCANTABLE(handle)[scanId].bool = NULL;

    LRDS_SCANTABLE(handle)[scanId].nBools = nBools;


    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* Open a SM level scan. */
    smScanId = SM_MLGF_OpenIndexScan(handle, &(relTableEntry->ri.fid), iid,
				     &(relTableEntry_ii[indexInfoIndex].kdesc.mlgf),
				     lowerBounds, upperBounds,
				     (relTableEntry->isCatalog)?NULL:lockup); 
    if (smScanId < 0) {
	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, smScanId);
    }

    LRDS_SCANTABLE(handle)[scanId].smScanId = smScanId;

    /* Set the current tuple id to NIL tuple id. */
    SET_NILTUPLEID(LRDS_SCANTABLE(handle)[scanId].tid);


    e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
    if ( e < eNOERROR ) ERR(handle, e);

    return(scanId);

} /* LRDS_MLGF_OpenIndexScan() */
