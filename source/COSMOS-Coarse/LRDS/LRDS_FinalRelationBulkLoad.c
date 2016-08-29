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
 * Module: LRDS_FinalRelationBulkLoad.c
 *
 * Description:
 *  Finalize the data file bulk load.
 *
 * Exports:
 *  Four LRDS_FinalDatafileBulkLoad(void)
 */

#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#ifndef COSMOS_S	 
#include "LM.h"
#endif /* COSMOS_S */
#include "SM_Internal.h" 
#include "LRDS.h"
#include "BL_LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* internal function prototype */
Four lrds_InsertIndexEntryByBulkLoad(Four, LRDS_BlkLdTableEntry*, Four, IndexInfo*, Four, TupleID*);
Four lrds_InsertIndexEntriesForCollectionByBulkLoad(Four, LRDS_BlkLdTableEntry*, Four, Four, TupleID*, Two); 


/*@========================================
 *  LRDS_FinalRelationBulkLoad()
 * =======================================*/

/*
 * Function : Four LRDS_FinalRelationBulkLoad()
 *
 * Description :
 *  Finalize the data file bulk load.
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *  0)
 *
 */
Four LRDS_FinalRelationBulkLoad(
    Four handle,
    Four                        blkLdId)            /* IN  bulkload ID */ 
{
    Four                        e;                  /* error code */
    Two                         i;                  /* index variable */
    Four                        scanId;             /* a scan identifier */
    TupleID                     tid;                /* tuple of the given relation */
    Boolean                     isOn;               
    LRDS_BlkLdTableEntry        *blkLdEntry;        
    lrds_RelTableEntry          *relTableEntry;     /* pointer to an entry of relation table */
    PageID                      catalogPid;
    ColListStruct               clist[1];           /* a column list structure */
    Four                        v;                  /* index on LRDS mount table */
    LockReply                   lockReply;          /* lock reply */
    LockMode                    oldMode;
    IndexInfo                   *relTableEntry_ii;
    ColDesc                     *relTableEntry_cdesc;

    /*
     *  O. Set entry for fast access
     */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];
    relTableEntry = blkLdEntry->lrdsBlkLdRelTableEntry;
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);


    /*
     *  I. Finalize SM level data file bulkload
     */
    e = SM_FinalDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId);
    if(e < 0) ERR(handle, e);


    /*
     *  II. Index bulkload if needed
     */

    /* if there is no index, skip it */
    if (relTableEntry->ri.nIndexes == 0) goto skip;

    /* Note!! At this point, there always exists indexes */

    /* if sorting is needed, automatic index support must be performed here */
    if (blkLdEntry->clusteringFlag == TRUE) {

        /* initialize isOn */
        isOn = FALSE;               

        /* open sequential scan for bulkloaded relation */
        scanId = LRDS_OpenSeqScan(handle, blkLdEntry->lrdsBlkLdOrn, FORWARD, 0, (BoolExp*)NULL, NULL);

        /* for each tuples in the relation */
        while (e != EOS) {

            /* move to next tuple */
            e = LRDS_NextTuple(handle, scanId, &tid, (LRDS_Cursor**)NULL);
            if (e < 0) ERR(handle, e);

            /* if all tuples are scanned, break */
            if (e == EOS) break;

            /* check new tuple is extracted or not */
            if (!isOn) {
                /* if new tuple, turn on flag */
                if(tid.volNo == blkLdEntry->firstPageId.volNo && tid.pageNo == blkLdEntry->firstPageId.pageNo) {
                    isOn = TRUE;
                }
                /* if old tuple, skip it */
                else {
                    continue;
                }
            }

            /* for each index, insert index entry for current tuple */
            for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

                /* skip index for SM_TEXT type */
                if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT) continue; 

                /* automatic indexing for complex type */
                if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONSET ||
                    relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONBAG ||
                    relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONLIST) {

                    /* insert index entries */
                    e = lrds_InsertIndexEntriesForCollectionByBulkLoad(handle, blkLdEntry,
                                                                       blkLdEntry->smIndexBlkLdIdArray[i],
                                                                       scanId, &tid, relTableEntry_ii[i].colNo[0]);
                    if (e < 0) ERR(handle, e);

                    continue;

                }
                /* automatic indexing for basic type */
                else {
                    e = lrds_InsertIndexEntryByBulkLoad(handle, blkLdEntry,
                                                        blkLdEntry->smIndexBlkLdIdArray[i],
                                                        &relTableEntry_ii[i], scanId, &tid);
                    if (e < 0) ERR(handle, e);
                }

            } /* for 'i' */

        } /* while */

        /* close the sequential scan for bulkloaded relation */
        e = LRDS_CloseScan(handle, scanId);

    } /* if */

    /* finalize index bulkload for each index */
    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

        /* skip index for SM_TEXT type */
        if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT) continue; 

        /* Note!! only B+ tree index is supported by bulkload */
        switch (relTableEntry_ii[i].indexType) {

          case SM_INDEXTYPE_BTREE:
            if (blkLdEntry->indexBlkLdFlag == TRUE) {
                e = SM_FinalIndexBulkLoad(handle, blkLdEntry->smIndexBlkLdIdArray[i], &relTableEntry_ii[i].iid, 100, 100, &blkLdEntry->lockup);
                if(e < 0) ERR(handle, e);
            }
            else {
                e = SM_FinalIndexBulkInsert(handle, blkLdEntry->smIndexBlkLdIdArray[i],
                                            &relTableEntry_ii[i].iid, &relTableEntry_ii[i].kdesc.btree, &blkLdEntry->lockup);
                if(e < 0) ERR(handle, e);
            }

            break;

          case SM_INDEXTYPE_MLGF:
          default:
            ERR(handle, eINTERNAL);
        }

    } /* for 'i' */

    /* free allocated memory */
    free(blkLdEntry->smIndexBlkLdIdArray);

    /* free allocated memory */
    if (blkLdEntry->clusteringFlag != TRUE) {
        free(blkLdEntry->kvalColListArray);
        free(blkLdEntry->kvalBufArray);
        free(blkLdEntry->kvalStreamIdArray); 
    }

skip:

    /*
     *  III. Close the bulkloaded relation
     */
    e = LRDS_CloseRelation(handle, blkLdEntry->lrdsBlkLdOrn);
    if(e < 0) ERR(handle, e);


    /*
     *  IV. Free allocated memory
     */
    free(blkLdEntry->lrdsBlkLdTupBuf);
    free(blkLdEntry->lrdsBlkLdTupHdrBuf);
    free(blkLdEntry->lrdsBlkLdRemainFlagArray);

    /*
     *	update nTuples information
     */
    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == relTableEntry->ri.fid.volNo) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);

    /* get the pid from the oid of the catalog entry */
    MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);

    e = LM_getFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
    if ( e < eNOERROR ) ERR(handle, e);
    if ( lockReply == LR_DEADLOCK ) ERR(handle, eDEADLOCK);

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
	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, e);
    }

    e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
    if ( e < eNOERROR ) ERR(handle, e);

    /*
     *  V. Free allocated entry in bulkload table
     */
    blkLdEntry->isUsed = FALSE;


    return(eNOERROR);

} /* LRDS_FinalRelationBulkLoad() */



Four lrds_InsertIndexEntryByBulkLoad(
    Four handle,
    LRDS_BlkLdTableEntry *blkLdEntry,             /* IN  entry which includes information about bulkload */ 
    Four                 indexBlkLdId,            /* IN  ID of index bulkload */
    IndexInfo            *indexInfo,              /* IN  information about index */
    Four                 scanId,                  /* IN  scan ID for the bulkloaded relation */
    TupleID              *tid)                    /* IN  tuple ID */
{
    Four                 e;                       /* error code */
    Two                  i;                       /* index variable */
    Two                  idx_nCols;               /* # of columns in index key */
    KeyValue             kval;                    /* index key */
    Two                  len;                     /* length of variable length column */
    ColListStruct        clist[MAXNUMKEYPARTS];   /* column list structure */
    char                 idxColsBuf[MAXKEYLEN];   /* buffer which contain key value */
    char                 *idxColsBufPtr;          /* pointer which points 'idxColsBuf' */
    lrds_RelTableEntry   *relTableEntry;          /* pointer to an entry of relation table */
    IndexInfo            *relTableEntry_ii;
    ColDesc              *relTableEntry_cdesc;


    /*
     *  Set relTableEntry for fast access
     */
    relTableEntry = blkLdEntry->lrdsBlkLdRelTableEntry;
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);


    /*
     *  Set idx_nCols
     */
    switch (indexInfo->indexType) {

      case SM_INDEXTYPE_BTREE:
        idx_nCols = indexInfo->kdesc.btree.nparts; break;

      case SM_INDEXTYPE_MLGF:
        idx_nCols = indexInfo->kdesc.mlgf.nKeys; break;

      default:
        ERR(handle, eINTERNAL);
    }


    /*
     *  Construct Column List Structure.
     */

    /* initialize 'idxColsBuf' */
    idxColsBufPtr = idxColsBuf;

    /* for each columns in index key */
    for (i = 0; i < idx_nCols; i++) {

        clist[i].colNo = indexInfo->colNo[i];
        clist[i].start = ALL_VALUE; /* read all data */
        clist[i].dataLength = relTableEntry_cdesc[clist[i].colNo].length;

        if (relTableEntry_cdesc[clist[i].colNo].type == SM_STRING ||
            relTableEntry_cdesc[clist[i].colNo].type == SM_VARSTRING) {
            clist[i].data.ptr = idxColsBufPtr;
            idxColsBufPtr += relTableEntry_cdesc[clist[i].colNo].length;
        }
    }


    /*
     *  Fetch key value from tuple
     */
    e = LRDS_FetchTuple(handle, scanId, TRUE, tid, idx_nCols, &(clist[0]));
    if (e < 0) ERR(handle, e);


    /*
     *  Insert index entry into given index
     */
    switch (indexInfo->indexType) {

      case SM_INDEXTYPE_BTREE:

        /* initialize kval.len */
        kval.len = 0;

        /* for each column */
        for (i = 0; i < idx_nCols; i++) {

            /* if column of index key is missing, error */
            if (clist[i].nullFlag) ERR(handle, eINTERNAL);

            /* if column is variable-length, insert length field */
            if (relTableEntry_cdesc[clist[i].colNo].type == SM_VARSTRING) {

                /* get length of variable-length column */
                len = clist[i].retLength;

                /* insert length field */
                memcpy((char*)&(kval.val[kval.len]), (char*) &len, sizeof(Two));

                /* update 'kval,len' */
                kval.len += sizeof(Two);
            }

            /* insert column into key value */
            if (relTableEntry_cdesc[clist[i].colNo].type == SM_STRING ||
                relTableEntry_cdesc[clist[i].colNo].type == SM_VARSTRING)
                memcpy(&(kval.val[kval.len]), clist[i].data.ptr, clist[i].retLength);
            else
                memcpy(&(kval.val[kval.len]), (char*)&(clist[i].data), clist[i].retLength);

            /* update 'kval,len' */
            kval.len += clist[i].retLength;
        }

        /* insert index entry */
        if (blkLdEntry->indexBlkLdFlag == TRUE) {
            e = SM_NextIndexBulkLoad(handle, indexBlkLdId, &kval, (ObjectID *)tid);
            if (e < 0) ERR(handle, e);
        }
        else {
            e = SM_NextIndexBulkInsert(handle, indexBlkLdId, &kval, (ObjectID *)tid);
            if (e < 0) ERR(handle, e);
        }

        break;


      case SM_INDEXTYPE_MLGF:
      default:
        ERR(handle, eINTERNAL);
    }


    return(eNOERROR);

} /* lrds_InsertIndexEntryByBulkLoad() */
