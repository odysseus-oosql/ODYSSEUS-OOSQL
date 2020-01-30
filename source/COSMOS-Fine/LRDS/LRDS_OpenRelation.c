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
 * Module: LRDS_OpenRelation.c
 *
 * Description:
 *  Open a relation. The relation is mapped to one of the files of a SM level.
 *  So we look up the catalog entry of the corresponding SM level file and
 *  loads the information into the main memory data structure.
 *  The catalog tables are opend at the volume mount time.
 *
 * Exports:
 *  Four LRDS_OpenRelation(Four, Four, char*)
 *
 * Returns:
 *  1) Open Relation Number if greater than or equal to 0
 *  2) Error code
 *       eRELATIONNOTFOUND_LRDS
 *       eTOOMANYOPENRELATIONS_LRDS
 *       some errors caused by function calls
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



Four LRDS_OpenRelation(
    Four handle,
    Four volId,			/* IN volume ID */
    char *relName)		/* IN relation to open */
{
    Four e;			/* error number */
    Four v;			/* IN index on the LRDS Mount Table*/
    Four orn;			/* open relation number; points to user open relation table entry */
    Four sysOrn;		/* open relation number; points to relation table entry */
    Four catScanId;		/* a SM level scan */
    Four colNo;			/* column number */
    Four i, j;			/* index variables */
    Four sum;			/* to set the offset of the fixed-length column */
    Four varColNo;		/* count # of variable-length columns */
    Four fixedColNo;		/* count # of fixed-length columns */
    TupleID catalogEntry;	/* TupleID of the catalog entry of LRDS_SYSTABLES */
    KeyInfo kinfo;		/* a key information structure */
    ColInfo cinfo;		/* a column information structure */
    BoundCond lb;		/* bounary condition of range scan */
    Boolean notFound;		/* The given relation isn't found. */
    ColListStruct clist[6];	/* column list structure */
    Two keyLen;			/* should be 'Two' to store the key length */
    LRDS_IndexDesc idesc;	/* index description */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc; 
    AuxColInfo_T auxColInfo; /* auxiliary column infomation */

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_OpenRelation(v=%ld, relName=%P)", v, relName));

    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == volId) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    if (relName == NULL) ERR(handle, eBADPARAMETER);


    /*
    ** Check the per-process open table.
    */
    for (orn = 0; orn < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; orn++) {
	/* skip unused entry */
	if (LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, orn)) continue;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
	if ((volId == relTableEntry->ri.fid.volNo) && strcmp(relName, relTableEntry->ri.relName) == 0) { 
	    /* Increment the number of opens. */
	    LRDS_USEROPENRELTABLE(handle)[orn].count ++;
	    return(orn);
	}
    }


    /* find a free entry in LRDS_USEROPENTABLE */
    for (orn = 0; orn < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; orn++)
	if (LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, orn)) break;
    if (orn == LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE) ERR(handle, eTOOMANYOPENRELATIONS_LRDS);


    /*
    ** Check whether the given relation is a catalog table.
    */
    for (i = 0; i < LRDS_NUMOFCATALOGTABLES; i++)
	if (strcmp(relName, lrds_perThreadDSptr->catalogTable[i]) == 0) {
	    sysOrn = LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[i];

	    e = SHM_getLatch(handle, &LRDS_RELTABLE[sysOrn].latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	    if (e < eNOERROR) ERR(handle, e);

	    LRDS_RELTABLE[sysOrn].count++; /* global counter */

	    e = SHM_releaseLatch(handle, &LRDS_RELTABLE[sysOrn].latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    /* Set up the LRDS_USEROPENRELTABLE(handle) */
	    LRDS_USEROPENRELTABLE(handle)[orn].sysOrn = sysOrn;	/* points to relation table entry */
	    LRDS_USEROPENRELTABLE(handle)[orn].count = 1; /* local counter */
	    LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag = FALSE;

	    return(orn);
	}


    /*
    ** Check whether the given relation is a temporary relation.
    */
    for (sysOrn = 0; sysOrn < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; sysOrn++) {
	if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, sysOrn) &&
	    (volId == LRDS_RELTABLE_FOR_TMP_RELS(handle)[sysOrn].ri.fid.volNo) &&
	    strcmp(relName, LRDS_RELTABLE_FOR_TMP_RELS(handle)[sysOrn].ri.relName) == 0) {
	    /* the given relation is a temporary relation */

	    /* Set up the LRDS_USEROPENRELTABLE(handle) */
	    LRDS_USEROPENRELTABLE(handle)[orn].sysOrn = sysOrn; /* points to relation table entry */
	    LRDS_USEROPENRELTABLE(handle)[orn].count = 1; /* local counter */
	    LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag = TRUE;

	    return(orn);
	}
    }


    /* Mutex Begin ::
    **  - allocation/deallocation of entry in LRDS_RELTABLE
    */
    e = SHM_getLatch(handle, &LRDS_LATCH_OPENRELATION, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Table Scan would be improved using hash structure */
    /* Check if the relation is already opened. */
    for (sysOrn = 0; sysOrn < LRDS_NUM_OF_ENTRIES_OF_RELTABLE; sysOrn++) {
	if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(sysOrn) &&
	    LRDS_RELTABLE[sysOrn].ri.fid.volNo == volId &&
	    strcmp(relName, LRDS_RELTABLE[sysOrn].ri.relName) == 0) {

	    /* alreay opened */

	    e = SHM_getLatch(handle, &LRDS_RELTABLE[sysOrn].latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);

	    if (e < eNOERROR) ERR(handle, e);

	    if (LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(sysOrn)) {
		/* Already closed entry */
		e = SHM_releaseLatch(handle, &LRDS_RELTABLE[sysOrn].latch, procIndex);
		if (e < eNOERROR) ERR(handle, e);

		break;
	    }

	    e = SHM_releaseLatch(handle, &LRDS_LATCH_OPENRELATION, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    LRDS_RELTABLE[sysOrn].count++; /* increment the count by 1 */

	    e = SHM_releaseLatch(handle, &LRDS_RELTABLE[sysOrn].latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);


	    /* Set up the LRDS_USEROPENRELTABLE(handle) */
	    LRDS_USEROPENRELTABLE(handle)[orn].sysOrn = sysOrn; /* points to relation table entry */
	    LRDS_USEROPENRELTABLE(handle)[orn].count = 1; /* local counter */
	    LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag = FALSE;

	    return(orn);
	}
    }


    /*
    ** Extract the corresponding catalog entry from LRDS_SYSTABLES.
    */
    /* start condition for the point scan. */
    /* The upper bound is same with the lower bound 'lb'. */
    lb.op = SM_EQ;
    keyLen =   strlen(relName);
    lb.key.len = sizeof(Two) + keyLen;
    memcpy(&(lb.key.val[0]), (char*)&keyLen, sizeof(Two));
    memcpy(&(lb.key.val[sizeof(Two)]), relName, keyLen);
    catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES],
				   &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSTABLES)->ii))[0].iid), 
				   &lb, &lb, 0, (BoolExp*)NULL, NULL);
    if (catScanId < 0) ERRL1(handle, catScanId, &LRDS_LATCH_OPENRELATION);

    /* Initialize a flag meaning the existence of data file search. */
    notFound = FALSE;

    e = LRDS_NextTuple(handle, catScanId, &catalogEntry, (LRDS_Cursor**)NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    if (e == EOS) notFound = TRUE;
    else {
	/* Construct clist. */
	clist[0].colNo = LRDS_SYSTABLES_DATAFILEID_COLNO;
	clist[0].start = ALL_VALUE;

	clist[1].colNo = LRDS_SYSTABLES_NTUPLES_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].dataLength = sizeof(Four);

	clist[2].colNo = LRDS_SYSTABLES_MAXTUPLELEN_COLNO;
	clist[2].start = ALL_VALUE;
	clist[2].dataLength = sizeof(Four);

	clist[3].colNo = LRDS_SYSTABLES_NCOLUMNS_COLNO;
	clist[3].start = ALL_VALUE;
	clist[3].dataLength = sizeof(Two);

	clist[4].colNo = LRDS_SYSTABLES_NINDEXES_COLNO;
	clist[4].start = ALL_VALUE;
	clist[4].dataLength = sizeof(Two);

	e = LRDS_FetchTuple(handle, catScanId, TRUE, (TupleID*)NULL, 5, &(clist[0]));
	if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);
    }

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    if (notFound)
	ERRL1(handle, eRELATIONNOTFOUND_LRDS, &LRDS_LATCH_OPENRELATION);


    /* Find a free entry from the open relation table. */
    for (sysOrn = 0; sysOrn < LRDS_NUM_OF_ENTRIES_OF_RELTABLE; sysOrn++)
	if (LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(sysOrn)) break;

    if (sysOrn == LRDS_NUM_OF_ENTRIES_OF_RELTABLE) ERRL1(handle, eTOOMANYOPENRELATIONS_LRDS, &LRDS_LATCH_OPENRELATION);


    /* Initialize isCatalog Flag */
    LRDS_RELTABLE[sysOrn].isCatalog = FALSE;

    /* Initially the clustering index is set to NIL. */
    LRDS_RELTABLE[sysOrn].clusteringIndex = NIL;

    /* Save the TupleID of the catalog entry of the LRDS_SYSTABLES. */
    LRDS_RELTABLE[sysOrn].ri.catalogEntry = catalogEntry;

    /* Fill the relation information. */
    LRDS_RELTABLE[sysOrn].ri.fid = clist[0].data.fid;
    LRDS_RELTABLE[sysOrn].ri.nTuples = GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[1], Four);
    LRDS_RELTABLE[sysOrn].ri.maxTupleLen = GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[2], Four);
    LRDS_RELTABLE[sysOrn].ri.nColumns = GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[3], Two);
    LRDS_RELTABLE[sysOrn].ri.nIndexes =GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[4], Two);
    strcpy(LRDS_RELTABLE[sysOrn].ri.relName, relName);

    /* Mark the slot */
    LRDS_RELTABLE[sysOrn].count = 1;

    /* block the access for this entry */
    e = SHM_getLatch(handle, &LRDS_RELTABLE[sysOrn].latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    /* enable other open or mount operation */
    e = SHM_releaseLatch(handle, &LRDS_LATCH_OPENRELATION, procIndex);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);


    if (LRDS_RELTABLE[sysOrn].ri.nIndexes != 0) {
	e = Util_getArrayFromHeap(handle, &LRDS_INDEXTABLEHEAP,
				  LRDS_RELTABLE[sysOrn].ri.nIndexes,
				  &relTableEntry_ii);
	if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);
        LRDS_RELTABLE[sysOrn].ii = LOGICAL_PTR(relTableEntry_ii);
    } else
	LRDS_RELTABLE[sysOrn].ii = LOGICAL_PTR(NULL);


    /* nColumns is always greater than 0 */
    e = Util_getArrayFromHeap(handle, &LRDS_COLUMNTABLEHEAP,
			      LRDS_RELTABLE[sysOrn].ri.nColumns,
			      &relTableEntry_cdesc);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);
    LRDS_RELTABLE[sysOrn].cdesc = LOGICAL_PTR(relTableEntry_cdesc);


    /*
    ** Extract a catalog entry from the catalog table LRDS_SYSCOLUMNS.
    */
    lb.op = SM_EQ;
    lb.key.len = SM_FILEID_SIZE;
    memcpy(&(lb.key.val[0]), (char*)&clist[0].data.pid, SM_FILEID_SIZE);

    /* The upper bound is same with the lower bound 'lb'. */
    catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS],
				   &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSCOLUMNS)->ii))[0].iid),
				   &lb, &lb, 0, (BoolExp*)NULL, NULL);
    if (catScanId < 0) ERRL1(handle, catScanId, &LRDS_RELTABLE[sysOrn].latch);

    e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);

    /* Construct 'clist'. */
    clist[0].colNo = LRDS_SYSCOLUMNS_COLUMNNO_COLNO;
    clist[0].start = ALL_VALUE;

    clist[1].colNo = LRDS_SYSCOLUMNS_COLINFO_COLNO;
    clist[1].start = ALL_VALUE;
    clist[1].data.ptr = &cinfo;

    /* ordered set */
    clist[2].colNo = LRDS_SYSCOLUMNS_AUXCOLINFO_COLNO;
    clist[2].start = ALL_VALUE;
    clist[2].dataLength = sizeof(AuxColInfo_T);
    clist[2].data.ptr = &auxColInfo;

    /* initialize number of variable-length columns */
    LRDS_RELTABLE[sysOrn].ri.nVarColumns = 0;

    /* Copy the column descriptions to the in-core memory. */
    for (i = 0; i < LRDS_RELTABLE[sysOrn].ri.nColumns && e != EOS; i++) {

	e = LRDS_FetchTuple(handle, catScanId, TRUE, (TupleID*)NULL, 3, &(clist[0]));
	if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);

	/* Mismatch between nColumns of LRDS_SYSTABLES and the # of columns */
	/* in LRDS_SYSCOLUMNS.                                              */
	colNo = GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[0], Two);
	if (colNo >= LRDS_RELTABLE[sysOrn].ri.nColumns) break;

	relTableEntry_cdesc[colNo].complexType = cinfo.complexType;
	relTableEntry_cdesc[colNo].type = cinfo.type;
	/* 'offset's are set at the below. */
        relTableEntry_cdesc[colNo].length = cinfo.length;

        /* ordered set */
        if (LRDS_HAS_AUXCOLINFO(cinfo.complexType, cinfo.type)) {
            OrderedSetAuxColInfo_T *orderedSetAuxColInfo;

            assert(cinfo.complexType == SM_COMPLEXTYPE_ORDEREDSET);

            e = Util_getElementFromPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_POOL,
                                        &orderedSetAuxColInfo);
            if (e < eNOERROR) ERR(handle, e);

            *orderedSetAuxColInfo = auxColInfo.orderedSet;
            relTableEntry_cdesc[colNo].auxInfo = LOGICAL_PTR(orderedSetAuxColInfo);
        } else {
            relTableEntry_cdesc[colNo].auxInfo = LOGICAL_PTR(NULL);
        }

	if (LRDS_IS_VAR_LENGTH_TYPE(cinfo.complexType, cinfo.type))
	    LRDS_RELTABLE[sysOrn].ri.nVarColumns++;	/* # of variable-length columns */
	e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
	if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);
    }

    /* Internal Error: # of columns are mismatched. */
    assert(i == LRDS_RELTABLE[sysOrn].ri.nColumns && e == EOS);

    /* Set the offsets. */
    sum = 0;

    varColNo = fixedColNo = 0;
    for (i = 0; i < LRDS_RELTABLE[sysOrn].ri.nColumns; i++) {
	if (LRDS_IS_VAR_LENGTH_TYPE(relTableEntry_cdesc[i].complexType, relTableEntry_cdesc[i].type)) { 
	    /* variable-length column */
	    relTableEntry_cdesc[i].fixedColNo = NIL;
	    relTableEntry_cdesc[i].varColNo = varColNo++;
	} else {
	    /* fixed-length column */
	    relTableEntry_cdesc[i].fixedColNo = fixedColNo++;
	    relTableEntry_cdesc[i].varColNo = NIL; 
	    relTableEntry_cdesc[i].offset = sum;
	    sum += relTableEntry_cdesc[i].length;
	}
    }

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);


    /*
    ** Extract a catalog entry from the catalog table LRDS_SYSINDEXES.
    */
    /* 'lb' is equal to the aboves. */
    /* The upper bound is same with the lower bound 'lb'. */
    catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSINDEXES],
				   &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSINDEXES)->ii))[0].iid), 
				   &lb, &lb, 0, (BoolExp*)NULL, NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);

    e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);

    /* Construct 'clist'. */
    clist[0].colNo = LRDS_SYSINDEXES_INDEXDESC_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = sizeof(LRDS_IndexDesc);
    clist[0].data.ptr = &idesc;

    clist[1].colNo = LRDS_SYSINDEXES_INDEXID_COLNO;
    clist[1].start = ALL_VALUE;

    /* Copy the index information to the in-core memory. */
    for (i = 0; i < LRDS_RELTABLE[sysOrn].ri.nIndexes && e != EOS; i++) {

	e = LRDS_FetchTuple(handle, catScanId, TRUE, (TupleID*)NULL, 2, &(clist[0]));
	if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);

	relTableEntry_ii[i].iid = clist[1].data.iid;
	relTableEntry_ii[i].indexType = idesc.indexType;
	switch (idesc.indexType) {
	  case SM_INDEXTYPE_BTREE:
	    (void) lrds_KeyInfoToKeyDesc(handle, NULL, relTableEntry_cdesc,
					 &idesc.kinfo.btree,
					 &(relTableEntry_ii[i].kdesc.btree)); 

	    if (idesc.kinfo.btree.flag & KEYFLAG_CLUSTERING) 
		LRDS_RELTABLE[sysOrn].clusteringIndex = i;

	    for (j = 0; j < idesc.kinfo.btree.nColumns; j++)
		relTableEntry_ii[i].colNo[j] = idesc.kinfo.btree.columns[j].colNo;
	    break;

	  case SM_INDEXTYPE_MLGF:
	    (void) lrds_MLGF_KeyInfoToMLGF_KeyDesc(handle, NULL, relTableEntry_cdesc,
						   &idesc.kinfo.mlgf,
						   &(relTableEntry_ii[i].kdesc.mlgf)); 

	    if (idesc.kinfo.mlgf.flag & KEYFLAG_CLUSTERING)
		LRDS_RELTABLE[sysOrn].clusteringIndex = i;

	    for (j = 0; j < idesc.kinfo.mlgf.nColumns; j++)
		relTableEntry_ii[i].colNo[j] = idesc.kinfo.mlgf.colNo[j];
	    break;

	  default:
	    ERR(handle, eINTERNAL);
	}

	e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
	if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);
    }

    /* Internal Error: # of indexes are mismatched. */
    assert(i == LRDS_RELTABLE[sysOrn].ri.nIndexes && e == EOS);

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_RELTABLE[sysOrn].latch);


    /* Mutex End ::
    **  - allocate an entry in LRDS_RELTABLE
    */
    e = SHM_releaseLatch(handle, &LRDS_RELTABLE[sysOrn].latch, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /* Set up the LRDS_USEROPENRELTABLE(handle) */
    LRDS_USEROPENRELTABLE(handle)[orn].sysOrn = sysOrn; /* points to relation table entry */
    LRDS_USEROPENRELTABLE(handle)[orn].count = 1; /* local counter */
    LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag = FALSE;


    return(orn);

} /* LRDS_OpenRelation() */
