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
 * Module: LRDS_DestroyRelation.c
 *
 * Description:
 *  Destroy a relation. We also remove the related indexes. And then
 *  update the catalog tables.
 *
 * Exports:
 *  Four LRDS_DestroyRelation(Four, char*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eBADVOLUMEID
 *    eOPENRELATION_LRDS
 *    eRELATIONNOTFOUND_LRDS
 *    some errors caused by function calls
 */


#include <assert.h>
#include <stdlib.h>
#include <string.h>
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


Four LRDS_DestroyRelation(
    Four handle,
    Four 		volId,			/* IN volume where the relation was placed */
    char                *relName)               /* IN relation to destroy */
{
    Four                e;                      /* error number */
    Two                 keyLen;                 /* should be Two to store key length */
    Four                v;                      /* index on LRDS mount table */
    Four                orn;                    /* open relation number; points to user open relation table entry */
    Four                sysOrn;                 /* open relation number; points to relation table entry */
    Four                catScanId;              /* scan id on a catalog table */
    Boolean             notFound;               /* TRUE if the given relation exist */
    TupleID             catalogEntry;           /* tuple id of catalog tuple in LRDS_SYSTABLES */
    BoundCond           bound;                  /* boundary condition of a range scan */
    ColListStruct       clist[1];               /* a column list structure */
    LockParameter       lockup;                 /* lockup for SM_Fetch Tuple */
    LockReply           lockReply;              /* lock reply */
    LockMode            oldMode;
    PageID              catalogPid;
    Four                i;                      /* temporary variable */
    Two                 colNo;
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    IndexInfo           *relTableEntry_ii;      
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1,
             ("LRDS_DestroyRelation(handle, volId=%ld, relName=%P)", volId, relName));


    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == volId) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    if (relName == NULL) ERR(handle, eBADPARAMETER);


    /*
    ** Check if the relation is opened. If opend relation, return an error.
    */
    for (orn = 0; orn < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; orn++) {
	/* skip if the entry is unused. */
	if (LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, orn)) continue;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);

	if (relTableEntry->ri.fid.volNo == volId && strcmp(relName, relTableEntry->ri.relName) == 0) 
	    ERR(handle, eOPENRELATION_LRDS);
    }


    /*
    ** If this relation is a temporary relation, then destroy it.
    */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; i++) {
	if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i) &&
	    LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.fid.volNo == volId &&
	    strcmp(relName, LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.relName) == 0) {

	    /* Destroy the SM file corresponding to this relation. */
	    e = SM_DestroyFile(handle, &(LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.fid), NULL); 
	    if (e < 0) ERR(handle, e);

            relTableEntry_cdesc = PHYSICAL_PTR(LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].cdesc); 
            relTableEntry_ii = PHYSICAL_PTR(LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ii); 

            /* ordered set */
            /* nColumns is always greater than 0 */
            for (colNo = 0; colNo < LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.nColumns; colNo++) {
                if (relTableEntry_cdesc[colNo].auxInfo != NULL) 
                {
                    assert(relTableEntry_cdesc[colNo].complexType == SM_COMPLEXTYPE_ORDEREDSET); 

                    e = Util_freeElementToLocalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(handle),
                                               relTableEntry_cdesc[colNo].auxInfo); 
                    if (e < 0) ERR(handle, e);
                }
            }

	    /* free the dynamically allocated memory */
	    free(relTableEntry_ii); 
	    free(relTableEntry_cdesc); 

	    /* Mark this entry to unused. */
	    LRDS_SET_TO_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i);

	    return(eNOERROR);
	}
    }


    /*
    ** Extract the corresponding catalog entry from LRDS_SYSTABLES.
    */
    /* start condition for the point scan. */
    /* The upper bound is same with the lower bound 'lb'. */
    bound.op = SM_EQ;
    keyLen = strlen(relName);
    bound.key.len = sizeof(Two) + keyLen;
    memcpy(&(bound.key.val[0]), &keyLen, sizeof(Two));
    memcpy(&(bound.key.val[sizeof(Two)]), relName, keyLen);
    catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES],
				 &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSTABLES)->ii))[0].iid), 
				 &bound, &bound, 0, (BoolExp*)NULL, NULL);
    if (catScanId < 0) ERR(handle, catScanId);

    /* Initialize a flag meaning the existence of data file search. */
    notFound = FALSE;

    e = LRDS_NextTuple(handle, catScanId, &catalogEntry, (LRDS_Cursor**)NULL);
    if (e < 0) ERR(handle, e);

    if (e == EOS) notFound = TRUE;
    else {
	/* Construct clist. */
	clist[0].colNo = LRDS_SYSTABLES_DATAFILEID_COLNO;
	clist[0].start = ALL_VALUE;

	e = LRDS_FetchTuple(handle, catScanId, TRUE, (TupleID*)NULL, 1, &(clist[0]));
	if (e < 0) ERR(handle, e);
    }

    e = LRDS_CloseScan(handle, catScanId);
    if (e < 0) ERR(handle, e);

    if (notFound) ERR(handle, eRELATIONNOTFOUND_LRDS);

    /* Mutex Begin :: */
    e = SHM_getLatch(handle, &LRDS_LATCH_OPENRELATION(handle), procIndex, M_SHARED, M_UNCONDITIONAL, NULL); 
    if (e < eNOERROR) ERR(handle, e);

    /* Check if the relation is already opened. */
    for (sysOrn = 0; sysOrn < LRDS_NUM_OF_ENTRIES_OF_RELTABLE; sysOrn++) {
	if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(handle, sysOrn) &&
	    EQUAL_FILEID(LRDS_RELTABLE(handle)[sysOrn].ri.fid, clist[0].data.fid)) { 
	    /* alreay opened */

	    ERRL1(handle, eOPENRELATION_LRDS, &LRDS_LATCH_OPENRELATION(handle));
	}
    }

    /* Mutex End :: */
    e = SHM_releaseLatch(handle, &LRDS_LATCH_OPENRELATION(handle), procIndex); 
    if (e < eNOERROR) ERR(handle, e);

    /* Prepare lock parameter */
    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */

    /* This file is not catalog relation */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    /* Destroy the SM file corresponding to this relation. */
    e = SM_DestroyFile(handle, &clist[0].data.fid, &lockup); 
    if (e < 0) ERR(handle, e);


    /*
    ** Updated the catalog tables.
    */
    /* Request Manual Duration lock for update
    ** Not follow Lock Hierarchy
    */
    /* get the pid from the oid of the catalog entry */
    MAKE_PAGEID(catalogPid, catalogEntry.volNo, catalogEntry.pageNo);
    e = LM_getFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
    if (e < eNOERROR) ERR(handle, e);

    if ( lockReply == LR_DEADLOCK )
	ERR(handle, eDEADLOCK);

    /*
    ** LRDS_SYSINDEXES
    */
    bound.op = SM_EQ;
    bound.key.len = sizeof(FileID);
    memcpy(&(bound.key.val[0]), &clist[0].data.pid, sizeof(FileID));
    catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSINDEXES],
				   &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSINDEXES)->ii))[0].iid), 
				   &bound, &bound, 0, (BoolExp*)NULL, NULL);
    if (catScanId < 0) ERR(handle, catScanId);

    e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
    if (e < 0) ERR(handle, e);

    while (e != EOS) {
	e = LRDS_DestroyTuple(handle, catScanId, TRUE, (TupleID*)NULL);
	if (e < 0) ERR(handle, e);

	e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
	if (e < 0) ERR(handle, e);
    }

    e = LRDS_CloseScan(handle, catScanId);
    if (e < 0) ERR(handle, e);


    /*
    ** LRDS_SYSCOLUMNS
    */
    /* 'bound' is same with the above. */
    catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS],
				&(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSCOLUMNS)->ii))[0].iid), 
				&bound, &bound, 0, (BoolExp*)NULL, NULL);
    if (catScanId < 0) ERR(handle, catScanId);

    e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
    if (e < 0) ERR(handle, e);

    while (e != EOS) {
	e = LRDS_DestroyTuple(handle, catScanId, TRUE, (TupleID*)NULL);
	if (e < 0) ERR(handle, e);

	e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
	if (e < 0) ERR(handle, e);
    }

    e = LRDS_CloseScan(handle, catScanId);
    if (e < 0) ERR(handle, e);


    /*
    ** LRDS_SYSTABLES
    */
    e = LRDS_DestroyTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES], FALSE, &catalogEntry);
    if (e < 0) ERR(handle, e);


    e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
    if ( e < eNOERROR ) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_DestroyRelation() */



