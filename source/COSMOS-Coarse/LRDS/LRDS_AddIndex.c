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
 * Module: LRDS_AddIndex.c
 *
 * Description:
 *  Add a new index on the given relation. We assume that the given index
 *  has no columns which can have NULL values.
 *
 * Exports:
 *  Four LRDS_AddIndex(Four, char*, LRDS_IndexDesc*, IndexID*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eBADVOLUMEID
 *    eINDEXEXIST_LRDS
 *    eMEMORYALLOCERR
 *    some errors caused by function calls
 */


#include <stdlib.h> 	 /* for malloc & free */
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



Four LRDS_AddIndex(
    Four handle,
    Four			 volId,			/* IN volume where the relation is placed */
    char                        *relName,               /* IN relation name */
    LRDS_IndexDesc              *idesc,                 /* IN index information */
    IndexID                     *iid)                   /* OUT IndexID of the newly added index */
{
    Four                        e;                      /* error number */
    Two                         i;
    Two                         j;                      /* index variable */
    Four                        k;
    Two                         l;
    Four                        v;                      /* index on LRDS mount table */
    Four                        scanId;                 /* a scan identifier */
    Four                        catScanId;              /* scan for catalog table */
    Two                         nIndexes;               /* # of indexes before adding a new index */
    Four                        orn;                    /* open relation number */
    TupleID                     tid;                    /* tuple of the given relation */
    KeyValue                    kval;                   /* a key value */
    IndexInfo                   *indexTable;            /* pointer to array of index info's */
    ColListStruct               clist[8];               /* column list structure */
    LockParameter               lockup;                 /* relName will be locked commit duration */
    LockReply                   lockReply;              /* lock reply */
    LockMode                    oldMode;
    Boolean                     found;                  /* TRUE if found something */
    MLGF_HashValue              mlgfKval[MLGF_MAXNUM_KEYS]; /* MLGF key values */
    Two                         idx_nCols;              /* number columns consisting in the index */

    Four                        count;
    PageID                      catalogPid;
    lrds_RelTableEntry          *relTableEntry;         /* pointer to an entry of relation table */
    char                        idxColsBuf[MAXKEYLEN];
    char                        *idxColsBufPtr;
    Boolean                     insertIntoIndexFlag;    /* insert a tuple into index if TRUE */
    IndexInfo                   *relTableEntry_ii;      
    ColDesc                     *relTableEntry_cdesc;   

    Four                        indexBlkLdId;           /* index bulkload ID */
    TupleHdr                    tupHdr;                 /* a tuple header */
    Four                        tupHdrSize;             /* size of tuple header */
    Four                        nElements;              /* # of elements */
    Four                        elementSize;            /* element size */
    Four                        elementSizes[100];      /* array of element size */
    char                        elements[PAGESIZE];     /* buffer for elements */
    char                        *elementPtr;            /* pointer of element */
    Two                         kvalLen;                /* length of key value */
    Four                        keySize;                /* size of key */
    Four                        collectionScanId;       /* collection scan ID */

    TR_PRINT(TR_LRDS, TR1, ("LRDS_AddIndex(handle)"));


    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == volId) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    if (relName == NULL) ERR(handle, eBADPARAMETER);

    if (idesc == NULL) ERR(handle, eBADPARAMETER);

    if (iid == NULL) ERR(handle, eBADPARAMETER);


    /* Open the given relation */
    orn = LRDS_OpenRelation(handle, volId, relName);
    if (orn < 0) ERR(handle, orn);

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 

    if (lrds_CheckIndexDesc(handle, relTableEntry->ri.nColumns, NULL, relTableEntry->cdesc, idesc) == FALSE) 
	ERR(handle, eBADPARAMETER);

    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/*
	** Request Manual Duration lock for update
	** Not follow Lock Hierarchy
	*/

	/* get the pid from the oid of the catalog entry */
	MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);

	e = LM_getFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if ( lockReply == LR_DEADLOCK )
	    ERR(handle, eDEADLOCK);
    }

    /* copy the number of indexes to a local variable. */
    nIndexes = relTableEntry->ri.nIndexes;

    /* Check if the same index is already defined. */
    /* From here 'idx_nCols' is set to # of columns consisting in the index. */
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    found = FALSE;
    switch (idesc->indexType) {
      case SM_INDEXTYPE_BTREE:
	idx_nCols = idesc->kinfo.btree.nColumns;
	for (i = 0; !found && i < nIndexes; i++) {
	    if (relTableEntry_ii[i].indexType != SM_INDEXTYPE_BTREE) continue;
	    if (relTableEntry_ii[i].kdesc.btree.nparts != idx_nCols) continue;

	    for (j = 0; j < idx_nCols; j++)
		if (relTableEntry_ii[i].colNo[j] != idesc->kinfo.btree.columns[j].colNo) break; 
	    if (j == idx_nCols) found = TRUE;
	}
	break;

      case SM_INDEXTYPE_MLGF:
	idx_nCols = idesc->kinfo.mlgf.nColumns;
	for (i = 0; !found && i < nIndexes; i++) {
	    if (relTableEntry_ii[i].kdesc.mlgf.nKeys != idx_nCols) continue;

	    for (j = 0; j < idx_nCols; j++)
		if (relTableEntry_ii[i].colNo[j] != idesc->kinfo.mlgf.colNo[j]) break;
	    if (j == idx_nCols) found = TRUE;;
	}
	break;

      default:
	ERR(handle, eINTERNAL);
    }

    if (found) {
	e = LRDS_CloseRelation(handle, orn);
	if (e < 0) ERR(handle, e);

	e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
	if ( e < eNOERROR ) ERR(handle, e);

	ERR(handle, eINDEXEXIST_LRDS);
    }

    /* Add an index entry. */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
    	/* check if a pointer to index information is null */
    	if(relTableEntry_ii == NULL)
	    indexTable = (IndexInfo*)malloc(sizeof(IndexInfo)*(nIndexes+1));
	else
	    indexTable = (IndexInfo*)realloc(relTableEntry_ii, sizeof(IndexInfo)*(nIndexes+1));

	if (indexTable == NULL) ERR(handle, eMEMORYALLOCERR);
        relTableEntry->ii = LOGICAL_PTR(indexTable);

    } else {
	e = Util_getArrayFromHeap(handle, &LRDS_INDEXTABLEHEAP(handle), nIndexes+1, &indexTable);
	if (e < 0) ERR(handle, e);

	memcpy((char*)&(indexTable[0]), (char*)&(relTableEntry_ii[0]), sizeof(IndexInfo)*nIndexes);
    }

    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    /* Fill the last index info with the new index information. */
    indexTable[nIndexes].indexType = idesc->indexType;

    switch (idesc->indexType) {
      case SM_INDEXTYPE_BTREE:
        (void) lrds_KeyInfoToKeyDesc(NULL, relTableEntry_cdesc,
				     &(idesc->kinfo.btree),
				     &indexTable[nIndexes].kdesc.btree);

        for (j = 0; j < idx_nCols; j++)
            indexTable[nIndexes].colNo[j] = idesc->kinfo.btree.columns[j].colNo;

	/* Creates a new index on the given relation. */
	e = SM_AddIndex(handle, &(relTableEntry->ri.fid), iid,
			(LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) ? NULL:&lockup);
	if (e < 0) ERR(handle, e);
	break;

      case SM_INDEXTYPE_MLGF:
        (void) lrds_MLGF_KeyInfoToMLGF_KeyDesc(NULL, relTableEntry_cdesc,
					       &(idesc->kinfo.mlgf),
					       &indexTable[nIndexes].kdesc.mlgf); 

        for (j = 0; j < idx_nCols; j++)
            indexTable[nIndexes].colNo[j] = idesc->kinfo.mlgf.colNo[j];

	/* Creates a new index on the given relation. */
	e = SM_MLGF_AddIndex(handle, &(relTableEntry->ri.fid), iid,
			     &indexTable[nIndexes].kdesc.mlgf,
			     (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) ? NULL:&lockup);
	if (e < 0) ERR(handle, e);
	break;

      default:
	ERR(handle, eINTERNAL);
    }

    indexTable[nIndexes].iid = *iid;

    if (relTableEntry_cdesc[indexTable[nIndexes].colNo[0]].complexType == SM_COMPLEXTYPE_SET) { 

	/* Open a sequential scan on the given relation. */
	/* lockup == NULL :: enough lock is already granted */
	scanId = LRDS_OpenSeqScan(handle, orn, FORWARD, 0, (BoolExp*)NULL,
				  (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) ? NULL:&lockup);
	if (scanId < 0) ERR(handle, scanId);

	/* Add entries into the newly created index. */
	e = LRDS_NextTuple(handle, scanId, &tid, (LRDS_Cursor**)NULL);
	if (e < 0) ERR(handle, e);

	while (e != EOS) {

	    e = lrds_Set_InsertIntoIndex(handle, scanId, TRUE, &indexTable[nIndexes], &tid, NULL, NULL);
	    if (e < 0) ERR(handle, e);

	    e = LRDS_NextTuple(handle, scanId, &tid, (LRDS_Cursor**)NULL);
	    if (e < 0) ERR(handle, e);
	}

	/* Close the scan for the given relation. */
	e = LRDS_CloseScan(handle, scanId);
	if (e < 0) ERR(handle, e);
    }
    else if(relTableEntry_cdesc[indexTable[nIndexes].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONSET ||
            relTableEntry_cdesc[indexTable[nIndexes].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONBAG ||
            relTableEntry_cdesc[indexTable[nIndexes].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONLIST) 
    {
        /* Open a sequential scan on the given relation. */
	/* lockup == NULL :: enough lock is already granted */
	scanId = LRDS_OpenSeqScan(handle, orn, FORWARD, 0, (BoolExp*)NULL,
				  (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) ? NULL:&lockup);
	if (scanId < 0) ERR(handle, scanId);

	/* Add entries into the newly created index. */
	e = LRDS_NextTuple(handle, scanId, &tid, (LRDS_Cursor**)NULL);
	if (e < 0) ERR(handle, e);

        /* initialize index bulkload */
        indexBlkLdId = SM_InitIndexBulkLoad(handle, volId, &(indexTable[nIndexes].kdesc.btree));
        if (indexBlkLdId < 0) ERR(handle, indexBlkLdId);

	while (e != EOS) {

            /* Get tupHdr */
            tupHdrSize = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);
            e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, LRDS_SCANTABLE(handle)[scanId].smScanId, TRUE,
                                (ObjectID*)&tid, 0, tupHdrSize, (char*)&tupHdr, NULL, NULL);
            if (e < 0) ERR(handle, e);

            /* get KeySize */
            e = lrds_Collection_GetKeySize(handle, scanId, TRUE, &tid, &tupHdr, indexTable[nIndexes].colNo[0], &keySize);
            if (e < 0) ERR(handle, e);

            /* insert retrieved kval into index */
            while(1) {

                /* get elements */
                nElements = lrds_Collection_Scan_NextElements(handle, collectionScanId, 100, elementSizes, PAGESIZE, elements);
                if(nElements < 0) ERR(handle, nElements);

                /* check end of stream */
                if (nElements == 0) break;

                for (k = 0, elementPtr = elements; k < nElements; k++) {

                    /* get elementSize */
                    elementSize = (relTableEntry_cdesc[indexTable[nIndexes].colNo[0]].type == SM_VARSTRING) ?
                                  elementSizes[k] : relTableEntry_cdesc[indexTable[nIndexes].colNo[0]].length;

                    /* calculate kvalLen */
                    kvalLen = (keySize == ALL_VALUE) ? elementSize : MIN(keySize, elementSize);

                    /* get kval */
                    if (relTableEntry_cdesc[indexTable[nIndexes].colNo[0]].type == SM_VARSTRING) {
                        kval.len = kvalLen + sizeof(Two);
                        memcpy(&kval.val[0], &kvalLen, sizeof(Two));
                        memcpy(&kval.val[sizeof(Two)], elementPtr, kvalLen);
                    }
                    else {
                        kval.len = kvalLen;
                        memcpy(&kval.val[0], elementPtr, kvalLen);
                    }

                    /* insert into index */
                    e = SM_NextIndexBulkLoad(handle, indexBlkLdId, &kval, &tid);
                    if (e < 0) ERR(handle, e);

                    /* update elementPtr */
                    elementPtr += elementSize;

                } /* for */

            } /* while */

	    e = LRDS_NextTuple(handle, scanId, &tid, (LRDS_Cursor**)NULL);
	    if (e < 0) ERR(handle, e);
	}

        /* finalize index bulkload */
        e = SM_FinalIndexBulkLoad(handle, indexBlkLdId, &(indexTable[nIndexes].iid), 100, 100, NULL);
        if(e < 0) ERR(handle, e);

	/* Close the scan for the given relation. */
	e = LRDS_CloseScan(handle, scanId);
	if (e < 0) ERR(handle, e);
    }
    else {			/* SM_COMPLEX_BASIC */

        if (idesc->indexType == SM_INDEXTYPE_BTREE &&
            relTableEntry_cdesc[idesc->kinfo.btree.columns[0].colNo].type == SM_TEXT) { 
            /* index on SM_TEXT: do nothing */
            goto LABEL_UpdateCatalogs;
        }

	/* Construct Column List Structure. */
        idxColsBufPtr = idxColsBuf;
        for (j = 0; j < idx_nCols; j++) {
            clist[j].colNo = indexTable[nIndexes].colNo[j];
            clist[j].start = ALL_VALUE; /* read all data */
            clist[j].dataLength = relTableEntry_cdesc[clist[j].colNo].length; 

            if (relTableEntry_cdesc[clist[j].colNo].type == SM_STRING ||
                relTableEntry_cdesc[clist[j].colNo].type == SM_VARSTRING) { 
                clist[j].data.ptr = idxColsBufPtr; 
                idxColsBufPtr += relTableEntry_cdesc[clist[j].colNo].length; 
	    }
	}


	/* Open a sequential scan on the given relation. */
	/* lockup == NULL :: enough lock is already granted */
	scanId = LRDS_OpenSeqScan(handle, orn, FORWARD, 0, (BoolExp*)NULL,
				  (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) ? NULL:&lockup);
	if (scanId < 0) ERR(handle, scanId);

	/* Add entries into the newly created index. */
	e = LRDS_NextTuple(handle, scanId, &tid, (LRDS_Cursor**)NULL);
	if (e < 0) ERR(handle, e);

	count = 0;

        if (indexTable[nIndexes].indexType == SM_INDEXTYPE_BTREE) {
            /* initialize index bulkload */
            indexBlkLdId = SM_InitIndexBulkLoad(handle, volId, &(indexTable[nIndexes].kdesc.btree));
            if (indexBlkLdId < 0) ERR(handle, indexBlkLdId);
        }


	while (e != EOS) {

#ifdef TRACE
	    if (count++ % 1 == 0)
                TR_PRINT(TR_LRDS, TR1, ("%ld inserted \n", count));
#endif
        if (indexTable[nIndexes].indexType == SM_INDEXTYPE_MLGF &&
                relTableEntry_cdesc[clist[0].colNo].type == SM_VARSTRING){
        /*
         * SM_VARSTRING은 MLGF가 색인을 하지 못한다. 하지만, SM_VARSTRING가 UDT를
         * 나타내는 경우에는 가능하다. 하지만, LRDS가 UDT를 모름으로 GEOM에서 처리하도록
         * 넘긴다.
         */
        /*
         * Generally, MLGF cannot index SM_VARSTRING type data.
         * But if data is UDT represented in SM_VARSTRING form, MLGF indexes SM_VARSTRING type data.
         * In this case, GEOM handles index processing because LRDS does not know how to decode the UDT
         */
        break;
        }

	    e = LRDS_FetchTuple(handle, scanId, TRUE, (TupleID*)NULL, idx_nCols, &(clist[0]));
	    if (e < 0) ERR(handle, e);

            insertIntoIndexFlag = TRUE; /* initialize the flag */

	    switch (indexTable[nIndexes].indexType) {
	      case SM_INDEXTYPE_BTREE:
		/* Construct key value. */
		/* We assume that the columns have no NULL values. */
		kval.len = 0;
                for (j = 0; j < idx_nCols; j++) {
                    if (clist[j].nullFlag) {
                        insertIntoIndexFlag = FALSE;
                        break;
                    }

                    if (relTableEntry_cdesc[clist[j].colNo].type == SM_VARSTRING) { 
                        Two       s = clist[j].retLength;

			memcpy((char*)&(kval.val[kval.len]), (char*)&s, sizeof(Two));
			kval.len += sizeof(Two);
		    }

                    if (relTableEntry_cdesc[clist[j].colNo].type == SM_STRING ||
                        relTableEntry_cdesc[clist[j].colNo].type == SM_VARSTRING) 
                        memcpy(&(kval.val[kval.len]), clist[j].data.ptr, clist[j].retLength);
		    else
                        memcpy((char*)&(kval.val[kval.len]), (char*)&(clist[j].data), clist[j].retLength);
                    kval.len += clist[j].retLength;
		}


                if (insertIntoIndexFlag) {
                    e = SM_NextIndexBulkLoad(handle, indexBlkLdId, &kval, (ObjectID*)&tid);
                    if (e < 0) ERR(handle, e);
                }

		break;

	      case SM_INDEXTYPE_MLGF:
		/* Construct key value. */
		/* We assume that the columns have no NULL values. */

		if (relTableEntry_cdesc[clist[0].colNo].type == SM_MBR) { 
                    if (clist[0].nullFlag) {
                        insertIntoIndexFlag = FALSE;
                    } else {
                        for (l = 0; l < MBR_NUM_PARTS; l++)
                            mlgfKval[l] = clist[0].data.mbr.values[l];
                    }
		} else {
                    for (j = 0; j < idx_nCols; j++) {
                        if (clist[j].nullFlag) {
                            insertIntoIndexFlag = FALSE;
                            break;
                        }

                        switch (relTableEntry_cdesc[clist[j].colNo].type) { 
			  case SM_SHORT:
                            mlgfKval[j] = clist[j].data.s;
			    break;
			  case SM_INT:
                            mlgfKval[j] = clist[j].data.i;
			    break;
			  case SM_LONG:
                            mlgfKval[j] = clist[j].data.l;
			    break;
			  default:
			    ERR(handle, eINTERNAL);
			}
		    }
		}

                if (insertIntoIndexFlag) {
                    e = SM_MLGF_InsertIndexEntry(handle, &(indexTable[nIndexes].iid),
                                                 &(indexTable[nIndexes].kdesc.mlgf),
                                                 mlgfKval, (ObjectID*)&tid, NULL, NULL);
                    if (e < 0) ERR(handle, e);
                }

		break;

	      default:
		ERR(handle, eINTERNAL);
	    }

	    e = LRDS_NextTuple(handle, scanId, &tid, (LRDS_Cursor**)NULL);
	    if (e < 0) ERR(handle, e);
	}

        if (indexTable[nIndexes].indexType == SM_INDEXTYPE_BTREE) {
            /* finalize index bulkload */
            e = SM_FinalIndexBulkLoad(handle, indexBlkLdId, &(indexTable[nIndexes].iid), 100, 100, NULL);
            if(e < 0) ERR(handle, e);
        }

	/* Close the scan for the given relation. */
	e = LRDS_CloseScan(handle, scanId);
	if (e < 0) ERR(handle, e);
    }


  LABEL_UpdateCatalogs:

    /*
    ** Update the catalog tables.
    */
    /* Index Information Table of LRDS_SYSINDEXES */
    /* Change the IndexInfo Table of open relation table. */
    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	if (nIndexes > 0) {
	    e = Util_freeArrayToHeap(handle, &LRDS_INDEXTABLEHEAP(handle), relTableEntry_ii); 
	    if (e < 0) ERR(handle, e);
	}

	relTableEntry->ii = LOGICAL_PTR(indexTable); 
    }

    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/* Construct 'clist' */
	clist[0].colNo = LRDS_SYSINDEXES_DATAFILEID_COLNO;
	clist[0].nullFlag = FALSE;
	clist[0].start = ALL_VALUE;	/* read all data */
	clist[0].data.fid = relTableEntry->ri.fid; 

	clist[1].colNo = LRDS_SYSINDEXES_INDEXDESC_COLNO;
	clist[1].nullFlag = FALSE;
	clist[1].start = ALL_VALUE;	/* read all data */
	clist[1].dataLength = sizeof(LRDS_IndexDesc);
	clist[1].data.ptr = idesc;

	clist[2].colNo = LRDS_SYSINDEXES_INDEXID_COLNO;
	clist[2].nullFlag = FALSE;
	clist[2].start = ALL_VALUE;	/* read all data */
	clist[2].data.iid = indexTable[nIndexes].iid; 

	e = LRDS_CreateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSINDEXES], FALSE, 3, clist, (TupleID*)NULL);
	if (e < 0) ERR(handle, e);
    }


    /*
    ** nIndexes of LRDS_SYSTABLES
    */
    relTableEntry->ri.nIndexes++;

    if (!LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/* Construct a column list. */
	clist[0].colNo = LRDS_SYSTABLES_NINDEXES_COLNO;
	clist[0].nullFlag = FALSE;
	clist[0].start = ALL_VALUE;	/* read all data */
	clist[0].dataLength = sizeof(Two);
        ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(handle, clist[0], relTableEntry->ri.nIndexes, Two); 

	e = LRDS_UpdateTuple(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES],
                             FALSE, (TupleID*)&(relTableEntry->ri.catalogEntry), 1, &(clist[0]));
	if (e < 0) ERR(handle, e);

	e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
	if ( e < eNOERROR ) ERR(handle, e);
    }

    /* Close the given relation */
    e = LRDS_CloseRelation(handle, orn);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_AddIndex() */
