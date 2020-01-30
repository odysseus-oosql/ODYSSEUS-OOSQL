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
 * Module: LRDS_AddColumn.c
 *
 * Description:
 *  Add a column to an exsiting relation.
 *
 * Exports:
 *  Four LRDS_AddColumn(Four, char*, ColInfo*)
 *
 * Returns:
 *  Error code
 */


#include <assert.h>
#include <stdlib.h> /* for malloc & free */
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
#include "perThreadDS.h"
#include "perProcessDS.h"


Four LRDS_AddColumn(
    Four    handle,
    Four    volId,      	/* IN volume in which the relation will be placed */
    char    *relName,       	/* IN relation name to create */
    ColInfo *cinfo)     	/* IN column information */
{
    Four e;         /* error number */
    Four i, j;          /* index variable */
    Four v;         /* index on LRDS mount table */
    Four orn;           /* open relation number */
    PageID catalogPid;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    ColDesc *relTableEntry_cdesc;
    LockParameter lockup;   /* relName will be locked commit duration */
    LockReply lockReply;    /* lock reply */
    LockMode oldMode;
    ColListStruct clist[8]; /* column list structure */
    Four nColumns;              /* number of already exsiting columns */
    ColDesc *columnTable;       /* new column table */

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_AddColumn()"));


    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
    if (LRDS_USERMOUNTTABLE(handle)[v].volId == volId) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    if (relName == NULL) ERR(handle, eBADPARAMETER);

    if (cinfo == NULL || lrds_CheckColInfo(handle, 1, cinfo) == FALSE) ERR(handle, eBADPARAMETER);

    /* Open the given relation */
    orn = LRDS_OpenRelation(handle, volId, relName);
    if (orn < 0) ERR(handle, orn);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);

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

    /* copy the number of columns to a local variable. */
    nColumns = relTableEntry->ri.nColumns;

    /* Add an column entry. */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
        if(relTableEntry_cdesc == NULL)
            columnTable = (ColDesc*)malloc(sizeof(ColDesc)*(nColumns+1));
        else
            columnTable = (ColDesc*)realloc(relTableEntry_cdesc, sizeof(ColDesc)*(nColumns+1));

        if (columnTable == NULL) ERR(handle, eMEMORYALLOCERR);

    } else {
        e = Util_getArrayFromHeap(handle, &LRDS_COLUMNTABLEHEAP, nColumns+1, &columnTable);
        if (e < eNOERROR) ERR(handle, e);

        memcpy((char*)&(columnTable[0]), (char*)&(relTableEntry_cdesc[0]), sizeof(ColDesc)*nColumns);

        if (nColumns > 0) {
            e = Util_freeArrayToHeap(handle, &LRDS_COLUMNTABLEHEAP, relTableEntry_cdesc);
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    relTableEntry->cdesc = LOGICAL_PTR(columnTable);


    /*
    ** Fill the last column description with the new column description.
    */
    columnTable[nColumns].complexType = cinfo->complexType;
    columnTable[nColumns].type = cinfo->type;
    columnTable[nColumns].length = cinfo->length;

    if (LRDS_IS_VAR_LENGTH_TYPE(cinfo->complexType, cinfo->type)) {
        /* variable-length column */
        columnTable[nColumns].varColNo = relTableEntry->ri.nVarColumns;
        columnTable[nColumns].fixedColNo = NIL;
    } else {
        /* fixed-length column */
        columnTable[nColumns].varColNo = NIL;
        columnTable[nColumns].fixedColNo = nColumns - relTableEntry->ri.nVarColumns;
        for (i = nColumns-1; i >= 0; i--) /* find the last fixed-length column */
            if (!LRDS_IS_VAR_LENGTH_TYPE(columnTable[i].complexType, columnTable[i].type)) break;
        columnTable[nColumns].offset = (i < 0) ? 0 : (columnTable[i].offset + columnTable[i].length);
    }

    if (LRDS_HAS_AUXCOLINFO(cinfo->complexType, cinfo->type)) {
        OrderedSetAuxColInfo_T *orderedSetAuxColInfo;

        assert(cinfo->complexType == SM_COMPLEXTYPE_ORDEREDSET);

        e = Util_getElementFromPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_POOL,
                                    &orderedSetAuxColInfo);
        if (e < eNOERROR) ERR(handle, e);

        *orderedSetAuxColInfo = *cinfo->auxInfo.orderedSet;
        columnTable[nColumns].auxInfo = LOGICAL_PTR(orderedSetAuxColInfo);
    } else {
        columnTable[nColumns].auxInfo = LOGICAL_PTR(NULL);
    }


    relTableEntry->ri.nColumns++;
    relTableEntry->ri.maxTupleLen += cinfo->length;
    if (LRDS_IS_VAR_LENGTH_TYPE(cinfo->complexType, cinfo->type))
        relTableEntry->ri.nVarColumns++;


    /*
    ** Update the catalog tables.
    */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {

        /*
        ** LRDS_SYSCOLUMNS
        */
        /* THINK*/
        /* Construct 'clist'. */
        clist[0].colNo = LRDS_SYSCOLUMNS_DATAFILEID_COLNO;
        clist[0].nullFlag = FALSE;
        clist[0].start = ALL_VALUE;
        clist[0].dataLength = SM_FILEID_SIZE;
        clist[0].data.fid = relTableEntry->ri.fid;

        clist[1].colNo = LRDS_SYSCOLUMNS_COLUMNNO_COLNO;
        clist[1].nullFlag = FALSE;
        clist[1].start = ALL_VALUE;
        clist[1].dataLength = sizeof(Two);
        ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[1], nColumns, Two);

        clist[2].colNo = LRDS_SYSCOLUMNS_COLINFO_COLNO;
        clist[2].nullFlag = FALSE;
        clist[2].start = ALL_VALUE;
        clist[2].dataLength = sizeof(ColInfo);
        clist[2].data.ptr = cinfo;

        clist[3].colNo = LRDS_SYSCOLUMNS_AUXCOLINFO_COLNO;
        clist[3].nullFlag = FALSE;
        clist[3].start = ALL_VALUE;

        if (LRDS_HAS_AUXCOLINFO(cinfo->complexType, cinfo->type)) {
            assert(cinfo->complexType == SM_COMPLEXTYPE_ORDEREDSET);

            clist[3].dataLength = sizeof(OrderedSetAuxColInfo_T);
            clist[3].data.ptr = (void*)cinfo->auxInfo.orderedSet;
            e = LRDS_CreateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS],
                                 FALSE, LRDS_SYSCOLUMNS_NUM_COLS, &(clist[0]), (TupleID*)NULL);
            if (e < eNOERROR) ERR(handle, e);

        } else {
            e = LRDS_CreateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS],
                                 FALSE, LRDS_SYSCOLUMNS_NUM_COLS-1, &(clist[0]), (TupleID*)NULL);
            if (e < eNOERROR) ERR(handle, e);
        }


        /*
        ** LRDS_SYSTABLES: NCOLUMNS and MAXTUPLELEN
        */
        /* Construct a column list. */
        clist[0].colNo = LRDS_SYSTABLES_NCOLUMNS_COLNO;
        clist[0].nullFlag = FALSE;
        clist[0].start = ALL_VALUE;
        clist[0].dataLength = sizeof(Two);
        ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[0], relTableEntry->ri.nColumns, Two);

        clist[1].colNo = LRDS_SYSTABLES_MAXTUPLELEN_COLNO;
        clist[1].nullFlag = FALSE;
        clist[1].start = ALL_VALUE;
        clist[1].dataLength = sizeof(Four);
        ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[1], relTableEntry->ri.maxTupleLen, Four);

        e = LRDS_UpdateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES],
                             FALSE, (TupleID*)&(relTableEntry->ri.catalogEntry), 2, &(clist[0]));
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
    ** Release the lock.
    */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {

#ifdef SYSTABLE_RECORD_LOCKING
        e = LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL);
        if ( e < eNOERROR ) ERR(handle, e);
#else
        e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
        if ( e < eNOERROR ) ERR(handle, e);
#endif
    }

    /* Close the given relation */
    e = LRDS_CloseRelation(handle, orn);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_AddColumn() */

