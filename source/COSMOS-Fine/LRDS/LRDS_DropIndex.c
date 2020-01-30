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
 * Module: LRDS_DropIndex.c
 *
 * Description:
 *  Drop the given index.
 *
 * Exports:
 *  Four LRDS_DropIndex(Four, Four, char*, IndexID*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eVOLUMNENOTMOUNTED_LRDS
 *    eINDEXNOTFOUND_LRDS
 *    some errors caused by function calls
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


Four LRDS_DropIndex(
    Four handle,
    Four volId,			/* IN volume where the index is placed */
    char *relName,		/* IN relation on which the index is definded */
    IndexID *iid)		/* IN index to drop */
{
    Four e;			/* error number */
    Four i;			/* index variable */
    Four v;			/* index on LRDS mount table */
    Four orn;			/* open relation number */
    Four catScanId;		/* a scan id for the catalog table */
    BoundCond lb;		/* a boundary condition of range scan */
    BoolExp   bool;		/* a boolean expression */
    ColListStruct clist[1];	/* a column list structure */
    LockParameter lockup;	/* lockup for SM_Fetch Tuple */
    LockReply lockReply;		/* lock reply */
    LockMode oldMode;
    PageID catalogPid;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii;


    TR_PRINT(handle, TR_LRDS, TR1,
	     ("LRDS_DropIndex(handle, volId=%ld, relName=%P, iid=%P)", volId, relName, iid));


    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == volId) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    if (relName == NULL) ERR(handle, eBADPARAMETER);

    if (iid == NULL) ERR(handle, eBADPARAMETER);


    /* Open the given relation. */
    orn = LRDS_OpenRelation(handle, volId, relName);
    if (orn < 0) ERR(handle, orn);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);


    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/*
	** Request Manual Duration lock for update
	** Not follow Lock Hierarchy
	*/

#ifdef SYSTABLE_RECORD_LOCKING
	e = LM_getFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry),
			       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);
#else
	/* get the pid from the oid of the catalog entry */
	MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);

	e = LM_getFlatPageLock(handle, &MY_XACTID(handle), &catalogPid,
			       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);
#endif

	if ( lockReply == LR_DEADLOCK )
	    ERR(handle, eDEADLOCK);
    }

    /* Check if the given index has been defined. */
    for (i = 0; i < relTableEntry->ri.nIndexes; i++)
	if (EQUAL_INDEXID(*iid, relTableEntry_ii[i].iid)) break;

    if (i == relTableEntry->ri.nIndexes) {
	e = LRDS_CloseRelation(handle, orn);
	if (e < eNOERROR) ERR(handle, e);

	if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {

#ifdef SYSTABLE_RECORD_LOCKING
	    e = LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL);
	    if ( e < eNOERROR ) ERR(handle, e);
#else
	    e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
	    if ( e < eNOERROR ) ERR(handle, e);
#endif
	}

	ERR(handle, eINDEXNOTFOUND_LRDS);
    }


    /* Prepare lock parameter */
    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* But, Dropping index for catalog is not allowed */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;


    /* Drop the given index. */
    /* If there is a scan using the given index, it is detected in SM level. */
    switch (relTableEntry_ii[i].indexType) {
      case SM_INDEXTYPE_BTREE:
	e = SM_DropIndex(handle, iid, (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) ? NULL:&lockup);
	break;
      case SM_INDEXTYPE_MLGF:
	e = SM_MLGF_DropIndex(handle, iid, (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) ? NULL:&lockup);
	break;
      default:
	e = eINTERNAL;
    }
    if (e < eNOERROR) ERR(handle, e);

    /*
    ** Update catalog tables.
    */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/* Construct Boundary Condition. */
	lb.op = SM_EQ;
	lb.key.len = sizeof(FileID);
	memcpy(&(lb.key.val[0]), (char*)&(relTableEntry->ri.fid), sizeof(FileID));

	/* Construct boolean expression. */
	bool.op = SM_EQ;
	bool.colNo = LRDS_SYSINDEXES_INDEXID_COLNO;
	memcpy((char*)&(bool.data.pid), (char*)iid, SM_INDEXID_SIZE);

	/* Delete an index information entry from LRDS_SYSINDEXES */
	catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSINDEXES],
				       &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSINDEXES)->ii))[0].iid),
				       &lb, &lb, 1, &bool, NULL);
	if (catScanId < 0) ERR(handle, catScanId);

	e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
	if (e < eNOERROR) ERR(handle, e);

	/* Internal error: some wrong was occured!! */
	if (e == EOS) ERR(handle, eINDEXNOTFOUND_LRDS);

	/* Destroy the current tuple. */
	e = LRDS_DestroyTuple(handle, catScanId, TRUE, (TupleID*)NULL);
	if (e < eNOERROR) ERR(handle, e);

	e = LRDS_CloseScan(handle, catScanId);
	if (e < eNOERROR) ERR(handle, e);
    }

    /* Delete the corresponding index entry from the index information table.*/
    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {
	if (EQUAL_INDEXID(relTableEntry_ii[i].iid, *iid)) {
	    memmove(&(relTableEntry_ii[i]), &(relTableEntry_ii[i+1]),
		    sizeof(IndexInfo)*(relTableEntry->ri.nIndexes-i-1));
	    break;
	}
    }

    /*
    ** nIndexes of LRDS_SYSTABLES.
    */
    relTableEntry->ri.nIndexes --;

    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	catScanId = LRDS_OpenSeqScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES], FORWARD, 0, (BoolExp*)NULL, NULL);
	if (catScanId < 0) ERR(handle, catScanId);

	/* Construct a column list. */
	clist[0].colNo = LRDS_SYSTABLES_NINDEXES_COLNO;
	clist[0].nullFlag = FALSE;
	clist[0].start = ALL_VALUE;	/* read all data */
	clist[0].dataLength = sizeof(Two);
	/* clist[0].data.s = relTableEntry->ri.nIndexes; */
	ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[0], relTableEntry->ri.nIndexes, Two);

	e = LRDS_UpdateTuple(handle, catScanId, TRUE, (ObjectID*)&(relTableEntry->ri.catalogEntry), 1, &(clist[0]));
	if (e < eNOERROR) ERR(handle, e);

	e = LRDS_CloseScan(handle, catScanId);
	if (e < eNOERROR) ERR(handle, e);

#ifdef SYSTABLE_RECORD_LOCKING
	e = LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL);
	if ( e < eNOERROR ) ERR(handle, e);
#else
	e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
	if ( e < eNOERROR ) ERR(handle, e);
#endif
    }

    /*
    ** Close the relation.
    */
    e = LRDS_CloseRelation(handle, orn);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_DropIndex( ) */
