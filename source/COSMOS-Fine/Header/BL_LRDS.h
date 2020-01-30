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
#ifndef _BL_LRDS_H_
#define _BL_LRDS_H_


#include "LRDS.h"


/*@
 * Type Definitions
 */

typedef ColListStruct ColListForKval[MAXNUMKEYPARTS];
typedef char KvalBuffer[MAXKEYLEN]; 

typedef struct {
    Boolean             isUsed;

    Four                smDataFileBlkLdId;

    PageID              firstPageId;           

    SortKeyDesc         lrdsBlkLdSortKeyDesc;
    lrds_RelTableEntry* lrdsBlkLdRelTableEntry; /* pointer to an entry of relation table */
    Four                lrdsBlkLdOrn;
    Four                lrdsBlkLdScanId; 
    IndexID             lrdsBlkLdTxtIndexId;

    char*               lrdsBlkLdTupBuf;
    Four                lrdsBlkLdTupBufOffset;
    Boolean             lrdsBlkLdIsFirstTupBuf;

    char*               lrdsBlkLdTupHdrBuf;
    Four                lrdsBlkLdTupHdrSize;
    TupleHdr*           lrdsBlkLdTupHdrPtr;
    unsigned char*      lrdsBlkLdNullVector;

    Boolean*            lrdsBlkLdRemainFlagArray;
    Four                lrdsBlkLdRemainFlagArrayIdxForVarCols;
    Four                lrdsBlkLdRemainFlagArrayIdxForFixedCols;

    Boolean             indexBlkLdFlag;
    Boolean             clusteringFlag;
    Four*               smIndexBlkLdIdArray;
    ColListForKval*     kvalColListArray;
    KvalBuffer*         kvalBufArray;

    Four*               kvalStreamIdArray; 

    LockParameter       lockup;

} LRDS_BlkLdTableEntry;


/*
 *  Global variables for relation bulkload 
 */


#define LRDS_BLKLD_TABLE(_handle)      perThreadTable[_handle].lrdsDS.lrdsBlkLdTable



/*
 *  Function prototype for relation bulkload 
 */

Four LRDS_InitRelationBulkLoad(Four, Four, Four, char*, Boolean, Boolean, Two, Two, LockParameter*); 
Four LRDS_NextRelationBulkLoad(Four, Four, Four, ColListStruct*, Boolean, TupleID*);
Four LRDS_FinalRelationBulkLoad(Four, Four);

#ifndef COMPRESSION 
Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(Four, Four, Four, Four, Four, Four, char*, Boolean, TupleID*); 
#else
Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(Four, Four, Four, Four, Four, Four, char*, Boolean, TupleID*, char*, VolNo, Four); 
#endif
Four LRDS_NextRelationBulkLoad_Collection(Four, VolID, Four, Four, Boolean, Boolean, TupleID*, Four, Four); 
Four LRDS_OrderedSetAppendBulkLoad(Four, Four, Four, Boolean, TupleID*, Four, Four, Four, char*, LockParameter*);

Four LRDS_InitTextBulkLoad(Four, Four, Four, Boolean, Boolean, Four, LockParameter*); 
Four LRDS_NextTextBulkLoad(Four, Four, TupleID*, Four, char*);
Four LRDS_FinalTextBulkLoad(Four, Four);

#endif /* _BL_LRDS_H_ */
