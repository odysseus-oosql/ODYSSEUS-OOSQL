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
 * Module: LRDS_CreateRelation.c
 *
 * Description:
 *  Create a relation.
 *
 * Exports:
 *  Four LRDS_CreateRelation(Four, Four, char*, LRDS_IndexDesc*, Four, ColInfo, Boolean)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eBADVOLUMEID
 *    eRELATIONEXIST_LRDS
 *    some errors caused by function calls
 */


#include <assert.h>
#include <stdlib.h> /* for malloc & free */
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


Four LRDS_CreateRelation(
    Four handle,
    Four    volId,		/* IN volume in which the relation will be placed */
    char    *relName,		/* IN relation name to create */
    LRDS_IndexDesc *idesc,	/* IN index descriptor for the culustering index */
    Four    nCols,		/* IN number of columns */
    ColInfo *cinfo,		/* IN column information */
    Boolean tmpRelationFlag)	/* IN TRUE if this relation is a temporary relation */
{
    Two  keyLen;		/* should be Two to store key length */
    Four e;			/* error number */
    Four i, j;			/* index variables */
    Four v;			/* index on LRDS mount table */
    Four sysOrn;		/* open relation number; points to relation table entry */
    Four sum;			/* to set the offset of the fixed-length column */
    Four fixedColNo;		/* count # of fixed-length columns */
    Four varColNo;		/* count # of variable-length columns */
    Four catScanId;		/* scan id for a catalog table access */
    Four maxTupleLen;		/* maximum tuple length */
    FileID fid;			/* SM level file identifier */
    IndexID iid;		/* a Btree index used for clustering */
    Boolean found;		/* TRUE if the relation is already defined */
    BoundCond bound;		/* boundary condition */
    ColListStruct clist[6];	/* column list */
    LockParameter lockup;
    LRDS_IndexDesc text_idesc;	/* key info for SM_TEXT column */
    IndexID text_iid;		/* index id for SM_TEXT column */
    MLGF_KeyDesc mlgfKdesc;	/* key descriptor for MLGF index */
    Four numOfTextIndexes;	/* the number of SM_TEXT type indexes */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_CreateRelation()"));


    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == volId) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    if (relName == NULL) ERR(handle, eBADPARAMETER);

    if (nCols <= 0) ERR(handle, eBADPARAMETER);

    if (cinfo == NULL || lrds_CheckColInfo(handle, nCols, cinfo) == FALSE) ERR(handle, eBADPARAMETER);

    if (idesc != NULL && lrds_CheckIndexDesc(handle, nCols, cinfo, NULL, idesc) == FALSE) ERR(handle, eBADPARAMETER);


    /*
    ** Check if this relation is already defined; Is it a temporary relation?
    */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; i++)
	if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i) &&
	    LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.fid.volNo == volId &&
	    strcmp(relName, LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.relName) == 0)
	    ERR(handle, eRELATIONEXIST_LRDS);


    /*
    ** Check if the relation is already defined; Is it an ordinary relation?
    */
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
    found = FALSE;

    e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
    if (e < eNOERROR) ERR(handle, e);

    if (e != EOS) found = TRUE;

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERR(handle, e);

    if (found) ERR(handle, eRELATIONEXIST_LRDS);


    /*
    ** Create a new relation.
    */
    /* If this relation is a temporary relaton, prepare a free entry of LRDS_RELTABLE_FOR_TMP_RELS(handle). */
    if (tmpRelationFlag) {
	for (sysOrn = 0; sysOrn < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; sysOrn++)
	    if (LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, sysOrn)) break;

	if (sysOrn == LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS) ERR(handle, eTOOMANYTMPRELATIONS_LRDS);
    }

    /* set the lockup parameter */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    /* Create a file corresponding to a new relation. */
    /* In SM_CreateFile, commit duration X lock is granted */
    e = SM_CreateFile(handle, volId, &fid, tmpRelationFlag, (tmpRelationFlag) ? NULL:&lockup);
    if (e < eNOERROR) ERR(handle, e);

    /*
    ** If the new relation is a temporary relation,
    ** then register it in LRDS_RELTABLE_FOR_TMP_RELS(handle).
    */
    if (tmpRelationFlag) {

        /* points the empty entry for fast access */
        relTableEntry = &LRDS_RELTABLE_FOR_TMP_RELS(handle)[sysOrn];

	/* Initialize isCatalog Flag */
	relTableEntry->isCatalog = FALSE;

	/* Initially the clustering index is set to NIL. */
	relTableEntry->clusteringIndex = NIL;


	/*
	** Relation Information
	*/
	/* Save the TupleID of the catalog entry of the LRDS_SYSTABLES. */
	/* We set to NIL ObjectID because this relation is a temporary relation. */
	SET_NILOBJECTID(relTableEntry->ri.catalogEntry);

	/* Fill the relation information. */
	relTableEntry->ri.fid = fid;
	relTableEntry->ri.nTuples = 0;
	relTableEntry->ri.maxTupleLen = maxTupleLen;
	relTableEntry->ri.nColumns = nCols;
	relTableEntry->ri.nIndexes = 0;
	strcpy(relTableEntry->ri.relName, relName);


	/*
	** Column Information
	*/
	/* nColumns is always greater than 0 */
	relTableEntry_cdesc =
	    (ColDesc*)malloc(sizeof(ColDesc)*relTableEntry->ri.nColumns);
	if (relTableEntry_cdesc == NULL) ERR(handle, eMEMORYALLOCERR);
        relTableEntry->cdesc = LOGICAL_PTR(relTableEntry_cdesc);

	/* initialize number of variable-length columns */
	relTableEntry->ri.nVarColumns = 0;

	/* Copy the column descriptions to the in-core memory. */
	for (i = 0; i < relTableEntry->ri.nColumns; i++) {
	    relTableEntry_cdesc[i].complexType = cinfo[i].complexType;
	    relTableEntry_cdesc[i].type = cinfo[i].type;
	    /* 'offset's are set at the below. */
	    relTableEntry_cdesc[i].length = cinfo[i].length;

            /* ordered set */
            if (LRDS_HAS_AUXCOLINFO(cinfo[i].complexType, cinfo[i].type)) {
                OrderedSetAuxColInfo_T *orderedSetAuxColInfo;

                assert(cinfo[i].complexType == SM_COMPLEXTYPE_ORDEREDSET);

                e = Util_getElementFromLocalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(handle),
                                                 &orderedSetAuxColInfo);
                if (e < eNOERROR) ERR(handle, e);

                *orderedSetAuxColInfo = *cinfo[i].auxInfo.orderedSet;
                relTableEntry_cdesc[i].auxInfo = LOGICAL_PTR(orderedSetAuxColInfo);
            } else
                relTableEntry_cdesc[i].auxInfo = LOGICAL_PTR(NULL);

	    if (LRDS_IS_VAR_LENGTH_TYPE(cinfo[i].complexType, cinfo[i].type))
                relTableEntry->ri.nVarColumns++;	/* # of variable-length columns */
	}

	/* Set the offsets. */
	sum = 0;

	varColNo = fixedColNo = 0;
	for (i = 0; i < nCols; i++) {            
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


	/*
	** Index Information
	*/
        relTableEntry->ii = LOGICAL_PTR(NULL);

	/* Mark the slot to used. */
	relTableEntry->count = 0;

    } else {

        /*
        ** Update the catalog tables.
        */
        /* LRDS_SYSCOLUMNS */
        catScanId = LRDS_OpenSeqScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS], FORWARD, 0, (BoolExp*)NULL, NULL);
        if (catScanId < 0) ERR(handle, catScanId);

        /* Construct 'clist'. */
        clist[0].colNo = LRDS_SYSCOLUMNS_DATAFILEID_COLNO;
	clist[0].nullFlag = FALSE;
        clist[0].start = ALL_VALUE;
        clist[0].data.fid = fid;

        clist[1].colNo = LRDS_SYSCOLUMNS_COLUMNNO_COLNO;
	clist[1].nullFlag = FALSE;
        clist[1].start = ALL_VALUE;
        clist[1].dataLength = sizeof(Two);

        clist[2].colNo = LRDS_SYSCOLUMNS_COLINFO_COLNO;
	clist[2].nullFlag = FALSE;
        clist[2].start = ALL_VALUE;
        clist[2].dataLength = sizeof(ColInfo);

        /* ordered set */
        clist[3].colNo = LRDS_SYSCOLUMNS_AUXCOLINFO_COLNO;
	clist[3].nullFlag = FALSE;
        clist[3].start = ALL_VALUE;

        for (maxTupleLen = 0, i = 0; i < nCols; i++) {

	    ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[1], i, Two);
            clist[2].data.ptr = &(cinfo[i]);

            /* ordered set */
            if (LRDS_HAS_AUXCOLINFO(cinfo[i].complexType, cinfo[i].type)) {
                assert(cinfo[i].complexType == SM_COMPLEXTYPE_ORDEREDSET);

                clist[3].dataLength = sizeof(OrderedSetAuxColInfo_T);
                clist[3].data.ptr = (void*)cinfo[i].auxInfo.orderedSet;

				e = LRDS_CreateTuple(handle, catScanId, TRUE, LRDS_SYSCOLUMNS_NUM_COLS, &(clist[0]), (TupleID*)NULL);
                if (e < eNOERROR) ERR(handle, e);

            } else {
                e = LRDS_CreateTuple(handle, catScanId, TRUE, LRDS_SYSCOLUMNS_NUM_COLS-1, &(clist[0]), (TupleID*)NULL);
                if (e < eNOERROR) ERR(handle, e);
            }

            maxTupleLen += cinfo[i].length;
        }

        e = LRDS_CloseScan(handle, catScanId);
        if (e < eNOERROR) ERR(handle, e);


        /* LRDS_SYSTABLES */
        /* Construct 'clist'. */
        clist[0].colNo = LRDS_SYSTABLES_DATAFILEID_COLNO;
	clist[0].nullFlag = FALSE;
        clist[0].start = ALL_VALUE;
        clist[0].data.fid = fid;

        clist[1].colNo = LRDS_SYSTABLES_NTUPLES_COLNO;
	clist[1].nullFlag = FALSE;
        clist[1].start = ALL_VALUE;
        clist[1].dataLength = sizeof(Four);
	ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[1], 0, Four);

        clist[2].colNo = LRDS_SYSTABLES_MAXTUPLELEN_COLNO;
	clist[2].nullFlag = FALSE;
        clist[2].start = ALL_VALUE;
        clist[2].dataLength = sizeof(Four);
	ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[2], maxTupleLen, Four);

        clist[3].colNo = LRDS_SYSTABLES_NINDEXES_COLNO;
	clist[3].nullFlag = FALSE;
        clist[3].start = ALL_VALUE;
        clist[3].dataLength = sizeof(Two);
	ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[3], 0, Two);

        clist[4].colNo = LRDS_SYSTABLES_NCOLUMNS_COLNO;
	clist[4].nullFlag = FALSE;
        clist[4].start = ALL_VALUE;
        clist[4].dataLength = sizeof(Two);
	ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[4], nCols, Two);

        clist[5].colNo = LRDS_SYSTABLES_RELNAME_COLNO;
	clist[5].nullFlag = FALSE;
        clist[5].start = ALL_VALUE;
        clist[5].dataLength = strlen(relName);
        clist[5].data.ptr = relName;

        e = LRDS_CreateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES], FALSE, LRDS_SYSTABLES_NUM_COLS, &(clist[0]), (TupleID*)NULL);
        if (e < eNOERROR) ERR(handle, e);
    }

    /*
     * Define the indexes on the newly created relation.
     */
    /*
     * Define the clustering index.
     */
    if (idesc != NULL) {
        e = LRDS_AddIndex(handle, volId, relName, idesc, &iid);
        if (e < eNOERROR) ERR(handle, e);
    }

    /*
     * Create an index for each column of SM_TEXT.
     */
    /* Set the index description */
    text_idesc.indexType = SM_INDEXTYPE_BTREE;
    text_idesc.kinfo.btree.flag = 0;
    text_idesc.kinfo.btree.nColumns = 1;
    text_idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

    for (i = 0; i < nCols; i++) {
        if (cinfo[i].type != SM_TEXT) continue;

        text_idesc.kinfo.btree.columns[0].colNo = i;

        /* Create an index. */
        e = LRDS_AddIndex(handle, volId, relName, &text_idesc, &iid);
        if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_CreateRelation() */
