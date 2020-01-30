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
 * Module: LRDS_NextRelationBulkLoad.c
 *
 * Description:
 *  Process the data file bulk load.
 *
 * Exports:
 *  Four LRDS_NextRelationBulkLoad(Four, ColListStruct*, Boolean)
 */

#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM.h"
#include "LRDS.h"
#include "BL_LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* internal function prototype */
Four lrds_NextRelationBulkLoad_FlushTuple(Four, LRDS_BlkLdTableEntry*, TupleID*);
Four lrds_InsertIndexEntriesForCollection(Four, Four, Four, Boolean, TupleID*);
Four lrds_NextRelationBulkLoad_GetKeyVal(Four, IndexInfo*, ColDesc*, ColListStruct*, KeyValue*);


/*@========================================
 *  LRDS_NextRelationBulkLoad()
 * =======================================*/

/*
 * Function : Four LRDS_NextRelationBulkLoad()
 *
 * Description :
 *  Process the data file bulk load.
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *    0)
 */
Four LRDS_NextRelationBulkLoad(
    Four	   handle,
    Four           blkLdId,           /* IN bulkload ID */
    Four           nCols,             /* IN number of columns */
    ColListStruct* clist,             /* IN data of the created columns */
    Boolean        endOfTuple,        /* IN flag which indicates given clist is end of object or not */
    TupleID*       tid)               /* OUT tuple ID */
{
    Four           e;                 /* error code */
    Four           i, j;              /* index variable */
    Four           length;            /* length of the column */
    ColDesc*       cdesc;             /* description of columns in the relation */
    Four           columnNo;          /* column number */
    Four           varColumnNo;       /* variable-length column number */
    Four           processedLen;      /* processed length of given column */
    Four           remainedLen;       /* remained length of given column */
    Four           nowInBufLen;       /* length of the column in tuple buffer */
    Four           tag[MAXNUMOFCOLS]; /* array used to sort the clist by columnNo */
    lrds_RelTableEntry* relTableEntry;/* pointer to an entry of relation table */
    LRDS_BlkLdTableEntry* blkLdEntry;
    ColListStruct* blkLdClist;
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;


    /*
     *  Set entry for fast access
     */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];
    relTableEntry = blkLdEntry->lrdsBlkLdRelTableEntry;
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);


    /*
     *  Set 'cdesc' for fast access
     */
    cdesc = PHYSICAL_PTR(relTableEntry->cdesc);


    /*
     *  Initialize  'tag'
     */
    for (i = 0; i < relTableEntry->ri.nColumns; i++) tag[i] = NIL;


    /*
     *  For each given columns, append it into tuple
     */
    for (i = 0; i < nCols; i++) {

        /* set columnNo of given clist's ith column for fast access */
        columnNo = clist[i].colNo;

        /* set corresponding tag */
        tag[columnNo] = i;

        /*
         *  Append dummy value for NULL fixed-length column
         */
        for (j = 0; j < columnNo; j++ ) {

            if (cdesc[j].fixedColNo != NIL && blkLdEntry->lrdsBlkLdRemainFlagArray[j] == TRUE) {

                /* set the corresponding bit of null vector */
                BITSET(blkLdEntry->lrdsBlkLdNullVector, j);

                /* set remain flag array as FALSE */
                blkLdEntry->lrdsBlkLdRemainFlagArray[j] = FALSE;

                /* if tuple buffer is overflowed, flush out tuple buffer */
                if (blkLdEntry->lrdsBlkLdTupBufOffset + cdesc[j].length > SIZE_OF_LRDS_TUPLE_BUFFER) {

                    /*** Flush ***/
                    e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                                blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
                    if (e < eNOERROR) ERR(handle, e);

                    /*** Reset ***/
                    blkLdEntry->lrdsBlkLdTupBufOffset = 0;
                    blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;

                }

                /* update 'lrdsBlkLdTupBufOffset' */
                blkLdEntry->lrdsBlkLdTupBufOffset += cdesc[j].length;

            } /* if */

        } /* for 'j' */

        /*
         *  I. in case of fixed-length column
         */
        if (cdesc[columnNo].fixedColNo != NIL) {

            /* error check */
            /* Note!! fixed-length column can be appended in only one function call */
            /* Note!! fixed-length column cannot be appended after variable-length column */
            if (blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols != -1 ||
                columnNo <= blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForFixedCols) ERR(handle, eINTERNAL);

            /* update 'lrdsBlkLdRemainFlagArrayIdxForFixedCols' */
            blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForFixedCols = columnNo;

            /* set length of clist's ith column for fast access */
            length = cdesc[columnNo].length;

            /* if tuple buffer is overflowed, flush out tuple buffer */
            if (blkLdEntry->lrdsBlkLdTupBufOffset + length > SIZE_OF_LRDS_TUPLE_BUFFER) {

                /*** Flush ***/
                e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                            blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
                if (e < eNOERROR) ERR(handle, e);

                /*** Reset ***/
                blkLdEntry->lrdsBlkLdTupBufOffset = 0;
                blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;

            }

            /* I-1. column's content isn't given, set as null column */
            if (clist[i].nullFlag == TRUE) {

                /* set the corresponding bit of null vector */
                BITSET(blkLdEntry->lrdsBlkLdNullVector, columnNo);
            }
            /* I-2. If column is given */
            else {

                /* reset the corresponding bit of null vector */
                BITRESET(blkLdEntry->lrdsBlkLdNullVector, columnNo);

                /* append the column. */
                if (cdesc[columnNo].type == SM_STRING) {

                    /* error check */
                    if (clist[i].dataLength > length) ERR(handle, eINTERNAL);

                    /* append column data */
                    /* Note!! remained area of this column is filled with the dummy data. */
                    memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], clist[i].data.ptr, clist[i].dataLength);

                } else {
                    /* append column data */
                    memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], &(clist[i].data), length);
                }

            } /* end of I-2 */

            /* update 'lrdsBlkLdTupBufOffset' */
            blkLdEntry->lrdsBlkLdTupBufOffset += length;

        } /* end of I */

        /*
         *  II. in case of variable-length column
         */
        else {

            /* assertion check */
            assert(cdesc[columnNo].varColNo != NIL);

            /* error check */
            /* Note!! variable-length column can be appended in more than one function call */
            if (columnNo < blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols) ERR(handle, eINTERNAL);

            /* update 'lrdsBlkLdRemainFlagArrayIdxForVarCols' */
            blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols = columnNo;

            /* II-1.  column isn't given, set as null column */
            if (clist[i].nullFlag == TRUE) {

                /* set length */
                length = 0;

                /* set the corresponding bit of null vector. */
                BITSET(blkLdEntry->lrdsBlkLdNullVector, columnNo);

            }
            /* II-2. column is given */
            else {

                /* set length */
                length = clist[i].dataLength;

                /* reset the corresponding bit of null vector. */
                BITRESET(blkLdEntry->lrdsBlkLdNullVector, columnNo);

                /* initialize 'remainedLen' & 'processedLen' */
                remainedLen  = length;
                processedLen = 0;

                /* if tuple buffer is overflowed, flush out tuple buffer */
                while (blkLdEntry->lrdsBlkLdTupBufOffset + remainedLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

                    /* calculate available space and insert part of variable-length column */
                    nowInBufLen = SIZE_OF_LRDS_TUPLE_BUFFER - blkLdEntry->lrdsBlkLdTupBufOffset;
                    memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], ((char*)clist[i].data.ptr) + processedLen, nowInBufLen);

                    /* update 'remainedLen' & 'processedLen' */
                    remainedLen -= nowInBufLen;
                    processedLen += nowInBufLen;

                    /* update 'lrdsBlkLdTupBufOffset' */
                    blkLdEntry->lrdsBlkLdTupBufOffset += nowInBufLen;

                    /*** Flush ***/
                    e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                                blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
                    if (e < eNOERROR) ERR(handle, e);

                    /*** Reset ***/
                    blkLdEntry->lrdsBlkLdTupBufOffset = 0;
                    blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
                }

                /* append the column. */
                memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], ((char*)clist[i].data.ptr) + processedLen, remainedLen);

                /* update 'lrdsBlkLdTupBufOffset' */
                blkLdEntry->lrdsBlkLdTupBufOffset += remainedLen;

            } /* end of II-2 */


            /*
             *  set varColOffset
             */

            /* set varColumnNo for fast access */
            varColumnNo = cdesc[columnNo].varColNo;

            /* if varColOffset isn't set yet, initialize it */
            if(blkLdEntry->lrdsBlkLdRemainFlagArray[columnNo] == TRUE) {
                blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo] = (varColumnNo == 0) ?
                                                        blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset + length :
                                                        blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1] + length;
            }
            /* if varColOffset is set before, update it */
            else {
                blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo] += length;
            }

        } /* end of II */

        /* set corresponding entry of 'lrdsBlkLdRemainFlagArray' as FALSE */
        blkLdEntry->lrdsBlkLdRemainFlagArray[columnNo] = FALSE;

    } /* for i */


    /*
     *  automatic indexing support
     */

    /* if sort for clustering is needed, skip automatic key support */
    if (blkLdEntry->clusteringFlag == TRUE) goto skip;

    for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

        /* skip index for SM_TEXT type */
        if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT) continue;

        /* skip index for collection set, bag, list type */
        /* Note!! automatic indexing for collection set, bag, list is managed differentlly */
        if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONSET ||
            relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONBAG ||
            relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONLIST) continue;

        /* error check */
        if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eINTERNAL);
        switch (relTableEntry_ii[i].indexType) {

          case SM_INDEXTYPE_BTREE :

            /* set blkLdClist for fast access */
            blkLdClist = &blkLdEntry->kvalColListArray[i][0];

            for (j = 0; j < relTableEntry_ii[i].kdesc.btree.nparts; j++) {

                /* get columnNo for fast access */
                columnNo = relTableEntry_ii[i].colNo[j];

                /* set jth column */
                if (tag[columnNo] != NIL) {

                    /* error check */
                    if (clist[tag[columnNo]].nullFlag == TRUE)  ERR(handle, eCOLUMNVALUEEXPECTED_LRDS);

                    /* set nullFlag */
                    blkLdClist[j].nullFlag = FALSE;

                    /* copy dataLength */
                    blkLdClist[j].dataLength = clist[tag[columnNo]].dataLength;

                    /* copy data */
                    if (relTableEntry_cdesc[columnNo].type == SM_VARSTRING) {
                        memcpy(blkLdClist[j].data.ptr, clist[tag[columnNo]].data.ptr, clist[tag[columnNo]].dataLength);
                    }
                    else if (relTableEntry_cdesc[columnNo].type == SM_STRING) {
                        memcpy(blkLdClist[j].data.ptr, clist[tag[columnNo]].data.ptr, relTableEntry_cdesc[columnNo].length);
                    }
                    else {
                        memcpy(&(blkLdClist[j].data), &(clist[tag[columnNo]].data), relTableEntry_cdesc[columnNo].length);
                    }
                }

            } /* for 'j' */

            break;

          case SM_INDEXTYPE_MLGF :
          default :
            ERR(handle, eINTERNAL);

        } /* switch */

    } /* for 'i' */


skip :
    /*
     *  if end of tuple, flush out tuple header
     */
    if (endOfTuple == TRUE) {
        e = lrds_NextRelationBulkLoad_FlushTuple(handle, blkLdEntry, tid);
        if (e < eNOERROR) ERR(handle, e);
    }


    return(eNOERROR);

} /* LRDS_NextRelationBulkLoad() */



Four lrds_NextRelationBulkLoad_FlushTuple(
    Four		  handle,
    LRDS_BlkLdTableEntry* blkLdEntry, /* IN  entry of bulkload table */
    TupleID*       tid)               /* OUT tuple ID */
{
    Four           e;                 /* error code */
    Four           i;                 /* index variable */
    Four           varColumnNo;       /* variable-length column number */
    TupleID        localTid;          /* tuple ID */
    lrds_RelTableEntry* relTableEntry;/* pointer to an entry of relation table */
    Four           j;                 /* index variable */
    Four           idx_nCols;         /* # of columns in index key */
    KeyValue       kval;              /* key value for index */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;

    /* set relTableEntry for fast access */
    relTableEntry = blkLdEntry->lrdsBlkLdRelTableEntry;
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);

    /* Before flush it, set null vector and varColOffset for each null column */
    for (i = 0; i < relTableEntry->ri.nColumns; i++) {

        /* if ith column is already set, skip it!! */
        if (blkLdEntry->lrdsBlkLdRemainFlagArray[i] == FALSE) continue;

        /* set the corresponding bit of null vector */
        BITSET(blkLdEntry->lrdsBlkLdNullVector, i);

        /* in case of variable-length, set varColOffset also */
        if (relTableEntry_cdesc[i].varColNo != NIL) {

            /* set varColumnNo for fast access */
            varColumnNo = relTableEntry_cdesc[i].varColNo;

            /* set varColOffset */
            blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo] = (varColumnNo == 0) ?
                                                                blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset :
                                                                blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1];
        }
    }


    /*** Flush ***/

    /* if tuple buffer contains first part of tuple, flush tuple content containing header */
    if(blkLdEntry->lrdsBlkLdIsFirstTupBuf == TRUE) {

        /* insert tuple header */
        memcpy(blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupHdrBuf, blkLdEntry->lrdsBlkLdTupHdrSize);

        /* flush tuples with header */
        e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                    blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, TRUE, (ObjectID*)&localTid);
        if (e < eNOERROR) ERR(handle, e);
    }
    /* if tuple buffer contains remains of tuple, flush it */
    else {

        /* flush remains of tuple */
        e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                    blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);

        /* update header */
        e = SM_NextDataFileBulkLoadWriteLOT(handle, blkLdEntry->smDataFileBlkLdId, 0, blkLdEntry->lrdsBlkLdTupHdrSize,
                                            (char*) blkLdEntry->lrdsBlkLdTupHdrPtr, TRUE, (ObjectID*)&localTid);
        if (e < eNOERROR) ERR(handle, e);
    }

    /*** Reset ***/

    /* reset 'varColOffset' in tuple header */
    for (i = 0; i < blkLdEntry->lrdsBlkLdTupHdrPtr->nVarCols; i++) {
        blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[i] = blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset;
    }

    /* reset 'lrdsBlkLdTupBufOffset' & 'lrdsBlkLdIsFirstTupBuf' */
    blkLdEntry->lrdsBlkLdTupBufOffset = blkLdEntry->lrdsBlkLdTupHdrSize;
    blkLdEntry->lrdsBlkLdIsFirstTupBuf = TRUE;

    /* reset 'lrdsBlkLdRemainFlagArrayIdx' in tuple header */
    blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols = -1;
    blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForFixedCols = -1;

    /* reset 'lrdsBlkLdRemainFlagArray' in tuple header */
    for (i = 0; i < relTableEntry->ri.nColumns; i++) blkLdEntry->lrdsBlkLdRemainFlagArray[i] = TRUE;


    /*
     *  insert index entry into each index
     */

    if (blkLdEntry->clusteringFlag == FALSE) {

        for (i = 0; i < relTableEntry->ri.nIndexes; i++) {

            /* skip index for SM_TEXT type */
            if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].type == SM_TEXT) continue;

            /* automatic indexing for complex type */
            if (relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONSET ||
                relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONBAG ||
                relTableEntry_cdesc[relTableEntry_ii[i].colNo[0]].complexType == SM_COMPLEXTYPE_COLLECTIONLIST) {

                /* insert index entries */
                e = lrds_InsertIndexEntriesForCollection(handle, blkLdEntry->kvalStreamIdArray[i],
                                                         blkLdEntry->smIndexBlkLdIdArray[i],
                                                         blkLdEntry->indexBlkLdFlag, &localTid);
                if (e < eNOERROR) ERR(handle, e);

                /* reset 'collectionKvalStreamId' */
                /* Note!! stream is closed in above function */
                blkLdEntry->kvalStreamIdArray[i] = NIL;
            }
            /* automatic indexing for basic type */
            else {
                /* get kval from stored clist */
                e = lrds_NextRelationBulkLoad_GetKeyVal(handle, &relTableEntry_ii[i], relTableEntry_cdesc,
                                                        blkLdEntry->kvalColListArray[i], &kval);
                if (e < eNOERROR) ERR(handle, e);

                /* insert into index */
                if (blkLdEntry->indexBlkLdFlag == TRUE) {
                    e = SM_NextIndexBulkLoad(handle, blkLdEntry->smIndexBlkLdIdArray[i], &kval, (ObjectID *)&localTid);
                    if (e < eNOERROR) ERR(handle, e);
                }
                else {
                    e = SM_NextIndexBulkInsert(handle, blkLdEntry->smIndexBlkLdIdArray[i], &kval, (ObjectID *)&localTid);
                    if (e < eNOERROR) ERR(handle, e);
                }

                /* get idx_nCols */
                switch (relTableEntry_ii[i].indexType) {

                  case SM_INDEXTYPE_BTREE :
                    idx_nCols = relTableEntry_ii[i].kdesc.btree.nparts; break;

                  case SM_INDEXTYPE_MLGF :
                    idx_nCols = relTableEntry_ii[i].kdesc.mlgf.nKeys; break;

                  default :
                    ERR(handle, eINTERNAL);
                }

                /* reset 'kvalColListArray' */
                for (j = 0; j < idx_nCols; j++) {
                    blkLdEntry->kvalColListArray[i][j].nullFlag = TRUE;
                }
            }
        } /* for 'i' */

    } /* if */


    /*
     *  Set output parameter
     */
    if (tid != NULL) {
        *tid = localTid;
    }

    /*
     * Increase nTuples
     */
    relTableEntry->ri.nTuples ++;

    return(eNOERROR);

} /* lrds_NextRelationBulkLoad_FlushTuple() */


Four lrds_NextRelationBulkLoad_GetKeyVal(
    Four	      handle,
    IndexInfo*        idxInfo,           /* IN  index information */
    ColDesc*          cdesc,             /* IN  column description */
    ColListStruct*    clist,             /* IN  colList which contains columns in index key */
    KeyValue*         kval)              /* OUT key value for index */
{
    Two               s;                 /* variable for length field */
    Four              i;                 /* index variable */
    Four              columnNo;          /* column number */


    switch (idxInfo->indexType) {

      case SM_INDEXTYPE_BTREE :

        /* initialize kval */
        kval->len = 0;

        /* for each column in index key */
        for (i = 0; i < idxInfo->kdesc.btree.nparts; i++) {

            /* get columnNo for fast access */
            columnNo = idxInfo->colNo[i];

            /* check all columns in key are exists */
            if (clist[i].nullFlag == TRUE)  ERR(handle, eCOLUMNVALUEEXPECTED_LRDS);

            /* The column on which an index has been defined should have a complete column value */
            if (cdesc[columnNo].type == SM_STRING && clist[i].dataLength != cdesc[columnNo].length)
                ERR(handle, eWRONGCOLUMNVALUE_LRDS);

            switch (cdesc[columnNo].type) {

              case SM_VARSTRING:

                /* copy the 'data length' to the temporary variable of type Two. */
                s = clist[i].dataLength;

                /* copy length field */
                memcpy(&kval->val[kval->len], &s, sizeof(Two));
                kval->len += sizeof(Two);

                /* copy data */
                memcpy(&kval->val[kval->len], clist[i].data.ptr, clist[i].dataLength);
                kval->len += clist[i].dataLength;

                break;

              case SM_STRING:

                /* copy data */
                memcpy(&kval->val[kval->len], clist[i].data.ptr, cdesc[columnNo].length);
                kval->len += cdesc[columnNo].length;

                break;

              default:

                /* copy data */
                memcpy(&kval->val[kval->len], &(clist[i].data), cdesc[columnNo].length);
                kval->len += cdesc[columnNo].length;

                break;

            } /* switch 'column type' */

        } /* for 'i' */

        break;

      case SM_INDEXTYPE_MLGF :
        default :
        ERR(handle, eINTERNAL);
    }


    return(eNOERROR);

} /* lrds_NextRelationBulkLoad_GetKeyVal() */
