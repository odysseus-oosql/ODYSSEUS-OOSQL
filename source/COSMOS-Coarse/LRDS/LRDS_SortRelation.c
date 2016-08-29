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
 * Module: LRDS_SortRelation.c
 *
 * Description:
 *  sort the given relation.
 *
 * Exports:
 *  Four LRDS_SortRelation(Four, Four, char*, KeyInfo*, Boolean, char*, Boolean, LockParameter*)
 */


#include <assert.h>
#include <string.h>
#include <stdlib.h> 	 /* for malloc & free */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM_Internal.h" 
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@
 * Internal Function Prototypes
 */
Four OM_ReadLargeObject(Four, Object*, Four, Four, char*);
Four lrds_GetKeyColumns(Four, Object*, void*, SortKeyDesc*, SortKeyAttrInfo*);


Four lrds_CreateCatalogEntriesForRelation(Four, Four, char*, FileID*, Two, ColInfo*, Boolean);
Four lrds_CreateCatalogEntriesForTmpRelation(Four, Four, char*, FileID*, Two, ColInfo*);
Four lrds_CreateCatalogEntriesForPlainRelation(Four, Four, char*, FileID*, Two, ColInfo*);


/*@================================
 * LRDS_SortRelation()
 *================================*/
/*
 * Function: Four LRDS_SortRelation(Four, Four, char*, KeyInfo*, Boolean, char*, Boolean, LockParameter*)
 *
 * Description:
 *  sort the given relation.
 *
 * Returns:
 *  error code
 */
Four LRDS_SortRelation(
    Four handle,
    Four 		volId,                  /* IN volume id */
    Four 		tmpVolId,               /* IN temporary volume id in which sort stream resides */ 
    char                *inRelName,             /* IN relation name to be sorted */
    KeyInfo             *kinfo,                 /* IN sort key description */
    Boolean             newRelFlag,             /* IN whether we make new relation for sort result */
    char                *outRelName,            /* IN relation name to store sort result */
    Boolean             tmpRelFlag,             /* IN new relation is temporary relation? */
    LockParameter       *lockup)                /* IN lock parameter */
{
    Four                e;                      /* error code */
    Two                 i;
    Two                 j;                      /* index variable */
    Four                user_v;                 /* index on user LRDS mount table */
    Four                orn;                    /* open relation number */
    Four                scanId;                 /* a scan identifier */
    Four                catScanId;              /* scan for catalog table */
    FileID              outFid;                 /* file id of the output file */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    SortKeyDesc         sortKeyDesc;
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1,
             ("LRDS_SortRelation(handle, volId=%lD, inRelName=%P, kinfo=%P, newRelFlag=%lD, outRelName=%P, tmpRelFlag=%lD, lockup=%P)",
              volId, inRelName, kinfo, newRelFlag, outRelName, tmpRelFlag, lockup));


    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);


    for (user_v = 0; user_v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; user_v++)
	if (LRDS_USERMOUNTTABLE(handle)[user_v].volId == volId) break;

    if (user_v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);


    /* Open the given relation */
    orn = LRDS_OpenRelation(handle, volId, inRelName);
    if (orn < 0) ERR(handle, orn); 

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    /*
     * Sort the relation.
     */
    sortKeyDesc.flag = kinfo->flag;
    sortKeyDesc.nparts = kinfo->nColumns;
    for (i = 0; i < sortKeyDesc.nparts; i++) {
        sortKeyDesc.parts[i].attrNo = kinfo->columns[i].colNo;
        sortKeyDesc.parts[i].flag = kinfo->columns[i].flag;
    }

    e = SM_SortFile(handle, tmpVolId, &relTableEntry->ri.fid, &sortKeyDesc, lrds_GetKeyColumns, 
                    relTableEntry_cdesc, newRelFlag, tmpRelFlag, &outFid, lockup); 
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Create a new relation if newRelFlag is TRUE.
     */
    if (newRelFlag) {
        ColInfo cinfo[MAXNUMOFCOLS];

        for (j = 0; j < relTableEntry->ri.nColumns; j++) {
            LRDS_GET_COLINFO_FROM_COLDESC(cinfo[j], relTableEntry_cdesc[j]);
        }

        e = lrds_CreateCatalogEntriesForRelation(handle, user_v, outRelName, &outFid,
                                                 relTableEntry->ri.nColumns,
                                                 cinfo, tmpRelFlag);
        if (e < eNOERROR) ERR(handle, e);
    }

    e = LRDS_CloseRelation(handle, orn);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_SortRelation() */



/*@================================
 * lrds_GetKeyColumns()
 *================================*/
Four lrds_GetKeyColumns(
    Four handle,
    Object              *obj,
    void                *schema,
    SortKeyDesc         *kdesc,
    SortKeyAttrInfo     *attrInfo)
{
    Four                e;                      /* error code */
    TupleHdr            tupHdr;                 /* tuple header */
    TupleHdr            *tupHdrPtr;             /* pointer to tuple header */
    Two                 i;
    ColDesc             *cur_cdesc;             /* pointer to column description for current column */
    ColDesc             *cdesc = schema;
    Four                tupHdrSize;


    attrInfo->nparts = kdesc->nparts;

    for (i = 0; i < kdesc->nparts; i++) {
        attrInfo->parts[i].type = cdesc[kdesc->parts[i].attrNo].type;
        attrInfo->parts[i].length = cdesc[kdesc->parts[i].attrNo].length;
    }

    /* When obj is NULL, type & length are returned. */
    if (obj == NULL) return(eNOERROR);

    /* Get the tuple header. */
    if (obj->header.properties & P_LRGOBJ) {/* large object */
        /* Fetch the tuple header. */
        /*
        size = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
        */
        e = OM_ReadLargeObject(handle, obj, 0, sizeof(tupHdr), (char*)&tupHdr);
        if (e < eNOERROR) ERR(handle, e);

        tupHdrPtr = &tupHdr;

    } else {                    /* small object */
        tupHdrPtr = (TupleHdr*)obj->data;
    }

    tupHdrSize = TUPLE_HEADER_SIZE(tupHdrPtr->nFixedCols + tupHdrPtr->nVarCols, tupHdrPtr->nVarCols);

    for (i = 0; i < kdesc->nparts; i++) {
        cur_cdesc = &cdesc[kdesc->parts[i].attrNo];

        if (cur_cdesc->varColNo == NIL) { /* fixed-length column */
            attrInfo->parts[i].offset = cdesc[kdesc->parts[i].attrNo].offset + tupHdrSize;
        } else {                /* variable-length column */
            attrInfo->parts[i].offset = LRDS_VARCOL_START_OFFSET(cur_cdesc->varColNo, *tupHdrPtr);
            attrInfo->parts[i].length = LRDS_VARCOL_REAL_SIZE(cur_cdesc->varColNo, attrInfo->parts[i].offset, *tupHdrPtr);
        }
    }

    return(eNOERROR);

} /* lrds_GetKeyColumns() */



/*@================================
 * lrds_CreateCatalogEntriesForRelation()
 *================================*/
/*
 * Function: Four lrds_CreateRelation(Four, char*, LRDS_IndexDesc*, Two, ColInfo, Boolean)
 *
 * Description:
 *  create a relation.
 *
 * Returns:
 *  Error code
 */
Four lrds_CreateCatalogEntriesForRelation(
    Four handle,
    Four                v,                      /* IN index on LRDS mount table */
    char                *relName,               /* IN relation name to create */
    FileID              *fid,                   /* IN file id crresponding to the relation */
    Two                 nCols,                  /* IN number of columns */
    ColInfo             *cinfo,                 /* IN column information */
    Boolean             tmpRelationFlag)        /* IN TRUE if this relation is a temporary relation */
{
    Two                 keyLen;                 /* should be Two       to store key length */
    Four                e;                      /* error number */
    Two                 i;
    Four                j;                      /* index variables */
    Four                catScanId;              /* scan id for a catalog table access */
    Four                maxTupleLen;            /* maximum tuple length */
    Boolean             found;                  /* TRUE if the relation is already defined */
    BoundCond           bound;                  /* boundary condition */
    IndexID             iid;                    /* index id */
    LRDS_IndexDesc      text_idesc;             /* key info for SM_TEXT column */


    TR_PRINT(TR_LRDS, TR1,
             ("lrds_CreateCatalogEntriesForRelation(handle, v=%ld, relName=%P, nCols=%ld, cinfo=%P, tmpRelationFlag=%lD)",
	      v, relName, nCols, cinfo, tmpRelationFlag));


    for (maxTupleLen = 0, i = 0; i < nCols; i++)
        maxTupleLen += cinfo[i].length;


    /*
    ** Check if this relation is already defined; Is it a temporary relation?
    */
    for (j = 0; j < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; j++)
        if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, j) &&
            LRDS_RELTABLE_FOR_TMP_RELS(handle)[j].ri.fid.volNo == LRDS_USERMOUNTTABLE(handle)[v].volId &&
            strcmp(relName, LRDS_RELTABLE_FOR_TMP_RELS(handle)[j].ri.relName) == 0)
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
				 &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle,v,LRDS_SYSTABLES)->ii))[0].iid), 
				 &bound, &bound, 0, (BoolExp*)NULL, NULL);
    if (catScanId < eNOERROR) ERR(handle, catScanId);

    /* Initialize a flag meaning the existence of data file search. */
    found = FALSE;

    e = LRDS_NextTuple(handle, catScanId, (TupleID*)NULL, (LRDS_Cursor**)NULL);
    if (e < eNOERROR) ERR(handle, e);

    if (e != EOS) found = TRUE;

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERR(handle, e);

    if (found) ERR(handle, eRELATIONEXIST_LRDS);


    /*
     * Create the relation.
     */
    if (tmpRelationFlag)
        e = lrds_CreateCatalogEntriesForTmpRelation(handle, v, relName, fid, nCols, cinfo);
    else
        e = lrds_CreateCatalogEntriesForPlainRelation(handle, v, relName, fid, nCols, cinfo);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_CreateCatalogEntriesForRelation() */



/*@================================
 * lrds_CreateCatalogEntriesForTmpRelation()
 *================================*/
/*
 * Function: Four lrds_CreateCatalogEntriesForTmpRelation()
 *
 * Description:
 *  Create a temporary relation.
 *
 * Returns:
 *  error code
 */
Four lrds_CreateCatalogEntriesForTmpRelation(
    Four handle,
    Four 		v,			/* IN index on LRDS mount table */
    char                *relName,               /* IN relation name to create */
    FileID              *fid,                   /* IN file id crresponding to the relation */
    Two                 nCols,                  /* IN number of columns */
    ColInfo             *cinfo)                 /* IN column information */
{
    Four                e;                      /* error code */
    Four                sysOrn;                 /* open relation number; points to relation table entry */
    Four                maxTupleLen;            /* maximum tuple length */
    Four                sum;                    /* to set the offset of the fixed-length column */
    Two                 fixedColNo;             /* count # of fixed-length columns */
    Two                 varColNo;               /* count # of variable-length columns */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    Two                 i;                      /* index variables */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("lrds_CreateCatalogEntriesForTmpRelation(handle, v=%ld, relName=%P, nCols=%ld, cinfo=%P)",
                            v, relName, nCols, cinfo));


    /*
     * Prepare a free entry of LRDS_RELTABLE_FOR_TMP_RELS.
     */
    for (sysOrn = 0; sysOrn < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; sysOrn++)
        if (LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, sysOrn)) break;

    if (sysOrn == LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS) ERR(handle, eTOOMANYTMPRELATIONS_LRDS);


    /*
     * Create a file corresponding to a new relation.
     */
    e = SM_CreateFile(handle, LRDS_USERMOUNTTABLE(handle)[v].volId, fid, TRUE, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
    ** Register the temporary relation in LRDS_RELTABLE_FOR_TMP_RELS.
    */
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
    relTableEntry->ri.fid = *fid;
    relTableEntry->ri.nTuples = 0;
    relTableEntry->ri.maxTupleLen = maxTupleLen;
    relTableEntry->ri.nColumns = nCols;
    relTableEntry->ri.nIndexes = 0;
    strcpy(relTableEntry->ri.relName, relName);


    /*
    ** Column Information
    */
    /* nColumns is always greater than 0 */
    relTableEntry_cdesc = (ColDesc*)malloc(sizeof(ColDesc)*relTableEntry->ri.nColumns);
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

        if (LRDS_IS_VAR_LENGTH_COLUMN(cinfo[i].complexType, cinfo[i].type))
            relTableEntry->ri.nVarColumns++;	/* # of variable-length columns */
    }

    /* Set the offsets. */
    sum = 0;

    varColNo = fixedColNo = 0;
    for (i = 0; i < nCols; i++) {
        if (LRDS_IS_VAR_LENGTH_COLUMN(cinfo[i].complexType, cinfo[i].type)) {
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

    return(eNOERROR);

} /* lrds_CreateCatalogEntriesForTmpRelation() */



/*@================================
 * lrds_CreateCatalogEntriesForPlainRelation()
 *================================*/
/*
 * Function: Four lrds_CreateCatalogEntriesForPlainRelation()
 *
 * Description:
 *  Create a plain relation.
 *
 * Returns:
 *  error code
 */
Four lrds_CreateCatalogEntriesForPlainRelation(
    Four handle,
    Four 		v,			/* IN index on LRDS mount table */
    char                *relName,               /* IN relation name to create */
    FileID              *fid,                   /* IN file id crresponding to the relation */
    Two                 nCols,                  /* IN number of columns */
    ColInfo             *cinfo)                 /* IN column information */
{
    Four                e;                      /* error code */
    Four                catScanId;              /* scan id for a catalog table scan */
    Four                sysOrn;                 /* open relation number; points to relation table entry */
    Four                maxTupleLen;            /* maximum tuple length */
    Four                sum;                    /* to set the offset of the fixed-length column */
    Two                 varColNo;               /* count # of variable-length columns */
    ColListStruct       clist[LRDS_SYSTABLES_NUM_COLS]; /* column list */
    LockParameter       lockup;
    Two                 i;                      /* index variables */


    TR_PRINT(TR_LRDS, TR1, ("lrds_CreateCatalogEntriesForPlainRelation(handle, v=%ld, relName=%P, nCols=%ld, cinfo=%P)",
                            v, relName, nCols, cinfo));



    /*
    ** Update the catalog tables.
    */
    /* LRDS_SYSCOLUMNS *//* THINK*/
    catScanId = LRDS_OpenSeqScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS], FORWARD, 0, (BoolExp*)NULL, NULL);
    if (catScanId < eNOERROR) ERR(handle, catScanId);

    /* Construct 'clist'. */
    assert(sizeof(clist)/sizeof(clist[0]) >= LRDS_SYSCOLUMNS_NUM_COLS);

    clist[0].colNo = LRDS_SYSCOLUMNS_DATAFILEID_COLNO;
    clist[0].nullFlag = FALSE;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = SM_FILEID_SIZE;
    clist[0].data.fid = *fid; 

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

    assert(LRDS_SYSCOLUMNS_NUM_COLS == 4);

    for (maxTupleLen = 0, i = 0; i < nCols; i++) {

        ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[1], i, Two); 
	clist[2].data.ptr = &(cinfo[i]);

        /* ordered set */
        if (LRDS_HAS_AUXCOLINFO(cinfo[i].complexType, cinfo[i].type)) {
            assert(cinfo[i].complexType == SM_COMPLEXTYPE_ORDEREDSET);

            clist[3].dataLength = sizeof(OrderedSetAuxColInfo_T);
            clist[3].data.ptr = (void*)cinfo[i].auxInfo.orderedSet;

            e = LRDS_CreateTuple(handle, catScanId, TRUE, LRDS_SYSCOLUMNS_NUM_COLS, &(clist[0]), (TupleID*)NULL);
            if (e < 0) ERR(handle, e);

        } else {

            e = LRDS_CreateTuple(handle, catScanId, TRUE, LRDS_SYSCOLUMNS_NUM_COLS-1, &(clist[0]), (TupleID*)NULL);
            if (e < 0) ERR(handle, e);
        }

	maxTupleLen += cinfo[i].length;
    }

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERR(handle, e);


    /* LRDS_SYSTABLES *//* THINK */
    catScanId = LRDS_OpenSeqScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES], FORWARD, 0, (BoolExp*)NULL, NULL);
    if (catScanId < eNOERROR) ERR(handle, catScanId);


    /* Construct 'clist'. */
    assert(sizeof(clist)/sizeof(clist[0]) >= LRDS_SYSTABLES_NUM_COLS);

    clist[0].colNo = LRDS_SYSTABLES_DATAFILEID_COLNO;
    clist[0].nullFlag = FALSE;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = SM_FILEID_SIZE;
    clist[0].data.fid = *fid;  

    clist[1].colNo = LRDS_SYSTABLES_NTUPLES_COLNO;
    clist[1].nullFlag = FALSE;
    clist[1].start = ALL_VALUE;
    clist[1].dataLength = sizeof(Four);
    ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[1], 0, Four); /* Be Carefull !! */

    clist[2].colNo = LRDS_SYSTABLES_MAXTUPLELEN_COLNO;
    clist[2].nullFlag = FALSE;
    clist[2].start = ALL_VALUE;
    clist[2].dataLength = sizeof(Four);
    ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[2], maxTupleLen, Four); /* Be Carefull !! */

    clist[3].colNo = LRDS_SYSTABLES_NINDEXES_COLNO;
    clist[3].nullFlag = FALSE;
    clist[3].start = ALL_VALUE;
    clist[3].dataLength = sizeof(Two);
    ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[3], 0, Two); /* Be Carefull !! */

    clist[4].colNo = LRDS_SYSTABLES_NCOLUMNS_COLNO;
    clist[4].nullFlag = FALSE;
    clist[4].start = ALL_VALUE;
    clist[4].dataLength = sizeof(Two);
    ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[4], nCols, Two); /* Be Carefull !! */

    clist[5].colNo = LRDS_SYSTABLES_RELNAME_COLNO;
    clist[5].nullFlag = FALSE;
    clist[5].start = ALL_VALUE;
    clist[5].dataLength = strlen(relName);
    clist[5].data.ptr = relName;

    assert(LRDS_SYSTABLES_NUM_COLS == 6);

    e = LRDS_CreateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES], FALSE, LRDS_SYSTABLES_NUM_COLS, &(clist[0]), (TupleID*)NULL);
    if (e < eNOERROR) ERR(handle, e);

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_CreateCatalogEntriesForPlainRelation() */
