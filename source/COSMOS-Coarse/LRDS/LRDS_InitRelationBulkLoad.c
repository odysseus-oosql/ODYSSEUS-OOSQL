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
 * Module: LRDS_InitRelationBulkLoad.c
 *
 * Description:
 *  Initialize data file bulk loading.
 *
 * Exports:
 *  Four LRDS_InitRelationBulkLoad(Four, Four, char*, Boolean, Two, Two, LockParameter*)
 */


#include <string.h>
#include <stdlib.h> 	   /* for malloc & free */

#include "common.h"
#include "error.h"
#include "param.h"
#include "trace.h"
#include "SM_Internal.h"   
#include "LRDS.h"
#include "BL_LRDS.h"
#include "RDsM_Internal.h" 
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@
 * Internal Function Prototypes
 */
Four lrds_GetKeyColumns(Four, Object*, void*, SortKeyDesc*, SortKeyAttrInfo*);


/*@
 * Global variables for data file bulkload
 */


/*@========================================
 *  LRDS_InitRelationBulkLoad()
 * =======================================*/

/*
 * Function : Four LRDS_InitRelationBulkLoad(Four, Four, char*, Boolean, Two, Two, LockParameter*)
 *
 * Description :
 *  Initialize data file bulk loading.
 *
 * Return Values :
 *  bulkload ID
 *  error code.
 *
 * Side Effects :
 *    0)
 */

Four LRDS_InitRelationBulkLoad(
    Four handle,
    Four           		volId,          /* IN volume id */
    Four           		tmpVolId,       /* IN temporary volume id in which sort stream is created */ 
    char                        *inRelName,     /* IN relation name that bulk load will be processed */
    Boolean                     isNeedSort,     /* IN flag which indicates input data must be sorted by clustering index key */
    Boolean                     indexBlkLdFlag, /* IN flag which indicates index is built by bulkload or insert */
    Two                         pff,            /* IN Page fill factor */
    Two                         eff,            /* IN Extent fill factor */
    LockParameter               *lockup)        /* IN lock parameter for data volume */
{

    Four                        e;              /* error code */
    Two                         i;              /* index variable */
    Four                        user_v;         /* index on user LRDS mount table */
    Two                         nColumns;       /* # of columns in given relation */
    Two                         nVarColumns;    /* # of variable-length columns in given relation */
    ColDesc                     *cdesc;         /* description of columns in given relation */
    KeyDesc                     *keyDesc;       /* key description of clustering index */
    Four                        clusteringIndex;/* array index of clustering index on an index table */
    lrds_RelTableEntry          *relTableEntry; /* pointer to an entry of relation table */
    Four                        blkLdId;
    LRDS_BlkLdTableEntry        *blkLdEntry;
    Two                         j;              /* index variable */
    Two                         colNo;          /* column number */
    Two                         idx_nCols;      /* # of columns in index key */
    char                        *idxColsBufPtr; /* pointer in kvalBufArray[i] */
    IndexInfo                   *relTableEntry_ii;
    ColDesc                     *relTableEntry_cdesc;
    Two                         k;


    /*
     *  O. check parameters
     */

    /* find the mount table entry */
    for (user_v = 0; user_v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; user_v++)
        if (LRDS_USERMOUNTTABLE(handle)[user_v].volId == volId) break;

    /* error check */
    if (user_v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE) ERR(handle, eBADVOLUMEID);


    /*
     *  O. Find empty entry from LRDS bulkload table
     */

    for (blkLdId = 0; blkLdId < LRDS_BLKLD_TABLE_SIZE; blkLdId++ ) {
        if (LRDS_BLKLD_TABLE(handle)[blkLdId].isUsed == FALSE) break;
    }
    if (blkLdId == LRDS_BLKLD_TABLE_SIZE) ERR(handle, eBLKLDTABLEFULL);

    /* set entry for fast access */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];

    /* set isUsed flag */
    blkLdEntry->isUsed = TRUE;


    /*
     *  II. Open the given relation and set general global variables
     */

    /* open relation */
    blkLdEntry->lrdsBlkLdOrn = LRDS_OpenRelation(handle, volId, inRelName);
    if (blkLdEntry->lrdsBlkLdOrn < 0) ERR(handle, blkLdEntry->lrdsBlkLdOrn);

    /* Get the relation table entry. */
    /* set relTableEntry for fast access */
    blkLdEntry->lrdsBlkLdRelTableEntry = relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, blkLdEntry->lrdsBlkLdOrn);
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);

    /* set 'nColumns', 'nVarColumns' & 'cdesc' for fast access */
    nColumns = relTableEntry->ri.nColumns;
    nVarColumns = relTableEntry->ri.nVarColumns;
    cdesc = PHYSICAL_PTR(relTableEntry->cdesc);


    /*
     *  III. Initialize LRDS bulk load data structure
     */

    /* III-1. initialize global variables for tuple header */

    /* calculate 'lrdsBlkLdTupHdrSize' */
    blkLdEntry->lrdsBlkLdTupHdrSize = TUPLE_HEADER_SIZE(nColumns, nVarColumns);

    /* allocate memory */
    blkLdEntry->lrdsBlkLdTupHdrBuf = (char *) malloc(blkLdEntry->lrdsBlkLdTupHdrSize);
    if (blkLdEntry->lrdsBlkLdTupHdrBuf == NULL) ERR(handle, eMEMORYALLOCERR);

    /* set 'lrdsBlkLdTupHdrPtr' */
    blkLdEntry->lrdsBlkLdTupHdrPtr = (TupleHdr *) blkLdEntry->lrdsBlkLdTupHdrBuf;

    /* set 'lrdsBlkLdNullVector' pointer */
    blkLdEntry->lrdsBlkLdNullVector = NULLVECTOR_PTR(*blkLdEntry->lrdsBlkLdTupHdrPtr, nVarColumns);

    /* LRDS Tuple header setting */
    blkLdEntry->lrdsBlkLdTupHdrPtr->nFixedCols = nColumns - nVarColumns;
    blkLdEntry->lrdsBlkLdTupHdrPtr->nVarCols = nVarColumns;

    /* calculate 'firstVarColOffset' of tuple header */
    /* Note!! varColOffset is set before flush out the tuple header */
    blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset = blkLdEntry->lrdsBlkLdTupHdrSize;
    for (i = 0; i < nColumns; i++) {
        if (cdesc[i].fixedColNo != NIL)
            blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset += cdesc[i].length;
    }

    /* initialize varColOffset array */
    for (i = 0; i < nVarColumns; i++) {
        blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[i] = blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset;
    }

    /* set lockup parameter */
    blkLdEntry->lockup = *lockup;


    /* III-2. initialize global variables for tuple buffer */

    /* allocate memory */
    blkLdEntry->lrdsBlkLdTupBuf = (char*) malloc(SIZE_OF_LRDS_TUPLE_BUFFER);
    if (blkLdEntry->lrdsBlkLdTupBuf == NULL) ERR(handle, eMEMORYALLOCERR);

    /* initialize 'lrdsBlkLdTupBufOffset' */
    blkLdEntry->lrdsBlkLdTupBufOffset  = blkLdEntry->lrdsBlkLdTupHdrSize;

    /* initialize flag */
    blkLdEntry->lrdsBlkLdIsFirstTupBuf = TRUE;


    /* III-3. initialize global variables for columns */

    /* allocate memory */
    blkLdEntry->lrdsBlkLdRemainFlagArray = (Boolean *) malloc(nColumns*sizeof(Boolean));
    if (blkLdEntry->lrdsBlkLdRemainFlagArray == NULL) ERR(handle, eMEMORYALLOCERR);

    /* initialize 'ColNoArrayIdx' & 'ColNoArray' */
    blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols = -1;
    blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForFixedCols = -1;
    for (i = 0; i < nColumns; i++ ) blkLdEntry->lrdsBlkLdRemainFlagArray[i] = TRUE;


    /*
     *  IV. Initialize Data File Bulk Load in SM level
     */
    if (isNeedSort == TRUE && relTableEntry->clusteringIndex != NIL) {

        /* set 'clusteringFlag' */
        blkLdEntry->clusteringFlag = TRUE;

        /* set 'clusteringIndex' & 'keyDesc' for fast access */
        clusteringIndex = relTableEntry->clusteringIndex;
        keyDesc = &(relTableEntry_ii[clusteringIndex].kdesc.btree);

        /* set the SortKeyDesc data structure to sort by clustering index key */
        blkLdEntry->lrdsBlkLdSortKeyDesc.flag = keyDesc->flag;
        blkLdEntry->lrdsBlkLdSortKeyDesc.nparts = keyDesc->nparts;
        for (k = 0; k < keyDesc->nparts; k++) {
            blkLdEntry->lrdsBlkLdSortKeyDesc.parts[k].attrNo = relTableEntry_ii[clusteringIndex].colNo[k];
            blkLdEntry->lrdsBlkLdSortKeyDesc.parts[k].flag = SORTKEYDESC_ATTR_ASC;
        }

        /* initialize SM level data file bulkload */
        blkLdEntry->smDataFileBlkLdId = SM_InitDataFileBulkLoad(handle, tmpVolId, &relTableEntry->ri.fid,
                                                        &blkLdEntry->lrdsBlkLdSortKeyDesc, lrds_GetKeyColumns,
                                                        relTableEntry_cdesc, TRUE, pff, eff,
                                                        &blkLdEntry->firstPageId, &blkLdEntry->lockup); 
        if (blkLdEntry->smDataFileBlkLdId < eNOERROR) ERR(handle, blkLdEntry->smDataFileBlkLdId);
    }
    else {

        /* set 'clusteringFlag' */
        blkLdEntry->clusteringFlag = FALSE;

        /* initialize SM level data file bulkload */
        blkLdEntry->smDataFileBlkLdId = SM_InitDataFileBulkLoad(handle, tmpVolId, &relTableEntry->ri.fid,
                                                        NULL, NULL, NULL, FALSE, pff, eff,
                                                        &blkLdEntry->firstPageId, &blkLdEntry->lockup); 
        if (blkLdEntry->smDataFileBlkLdId < eNOERROR) ERR(handle, blkLdEntry->smDataFileBlkLdId);
    }


    /*
     *  IV. Initialize Index Bulk Load in SM level
     */

    /* set 'indexBlkLdFlag' */
    blkLdEntry->indexBlkLdFlag = indexBlkLdFlag;

    /* if there is no index, skip it */
    if (relTableEntry->ri.nIndexes == 0) goto skip;

    /* allocate memory for 'smIndexBlkLdIdArray' */
    blkLdEntry->smIndexBlkLdIdArray = (Four *) malloc(sizeof(Four)*relTableEntry->ri.nIndexes);
    if (blkLdEntry->smIndexBlkLdIdArray == NULL) ERR(handle, eMEMORYALLOCERR);

    /* initialize index bulk load for each index */
    for (j = 0; j < relTableEntry->ri.nIndexes; j++) {

        /* skip index for SM_TEXT type */
        if (relTableEntry_cdesc[relTableEntry_ii[j].colNo[0]].type == SM_TEXT) continue;

        /* Note!! only B+ tree index is supported by bulkload */
        switch (relTableEntry_ii[j].indexType) {

          case SM_INDEXTYPE_BTREE:
            if (blkLdEntry->indexBlkLdFlag == TRUE) {
                blkLdEntry->smIndexBlkLdIdArray[j] = SM_InitIndexBulkLoad(handle, tmpVolId, &relTableEntry_ii[j].kdesc.btree);
                if (blkLdEntry->smIndexBlkLdIdArray[j] < 0) ERR(handle, blkLdEntry->smIndexBlkLdIdArray[j]);
            }
            else {
                blkLdEntry->smIndexBlkLdIdArray[j] = SM_InitIndexBulkInsert(handle, tmpVolId, &relTableEntry_ii[j].kdesc.btree);
                if (blkLdEntry->smIndexBlkLdIdArray[j] < 0) ERR(handle, blkLdEntry->smIndexBlkLdIdArray[j]);
            }

            break;

          case SM_INDEXTYPE_MLGF:
          default:
            ERR(handle, eINTERNAL);

        } /* switch */

    } /* for 'j' */

    /* if sorting isn't needed, prepare kvalArray & kvalStreamIdArray */
    if (blkLdEntry->clusteringFlag != TRUE) {

        /* allocate memory for 'kvalStreamIdArray' */
        blkLdEntry->kvalStreamIdArray = (Four *) malloc(sizeof(Four)*relTableEntry->ri.nIndexes);
        if (blkLdEntry->kvalStreamIdArray == NULL) ERR(handle, eMEMORYALLOCERR);

        /* allocate memory for 'kvalColListsArray' */
        blkLdEntry->kvalColListArray = (ColListForKval *) malloc(sizeof(ColListForKval)*relTableEntry->ri.nIndexes);
        if (blkLdEntry->kvalColListArray == NULL) ERR(handle, eMEMORYALLOCERR);

        /* allocate memory for 'kvalBufArray' */
        blkLdEntry->kvalBufArray = (KvalBuffer *) malloc(sizeof(KvalBuffer)*relTableEntry->ri.nIndexes);
        if (blkLdEntry->kvalBufArray == NULL) ERR(handle, eMEMORYALLOCERR);

        /* initialize kvalColListsArray & kvalStreamIdArray */
        for (j = 0; j < relTableEntry->ri.nIndexes; j++) {

            /* get idx_nCols */
            switch (relTableEntry_ii[j].indexType) {

              case SM_INDEXTYPE_BTREE :
                idx_nCols = relTableEntry_ii[j].kdesc.btree.nparts; break;

              case SM_INDEXTYPE_MLGF :
                idx_nCols = relTableEntry_ii[j].kdesc.mlgf.nKeys; break;

              default :
                ERR(handle, eINTERNAL);

            } /* switch */

            /* initialize 'idxColsBuf' */
            idxColsBufPtr = &blkLdEntry->kvalBufArray[j][0];

            /* initialize 'kvalColListArray' */
            for (i = 0; i < idx_nCols; i++) {

                /* set colNo for fast access */
                colNo = relTableEntry_ii[j].colNo[i];

                /* set 'nullFlag' */
                /* Note!! 'nullFlag' indicates that column value is set or not */
                blkLdEntry->kvalColListArray[j][i].nullFlag = TRUE;

                /* allocate space for string or variable string */
                if (relTableEntry_cdesc[colNo].type == SM_STRING ||
                    relTableEntry_cdesc[colNo].type == SM_VARSTRING) {
                    blkLdEntry->kvalColListArray[j][i].data.ptr = idxColsBufPtr;
                    idxColsBufPtr += relTableEntry_cdesc[colNo].length;
                }
            } /* for 'i' */

            /* initialize 'kvalStreamIdArray' */
            blkLdEntry->kvalStreamIdArray[j] = NIL;

        } /* for 'j' */

    } /* if */

skip:


    return(blkLdId);

} /* LRDS_InitRelationBulkLoad() */
