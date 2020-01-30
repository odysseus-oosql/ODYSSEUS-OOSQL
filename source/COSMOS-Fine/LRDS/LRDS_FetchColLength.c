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
 * Module: LRDS_FetchColLength.c
 *
 * Description:
 *  Fetch some columns of the current tuple or the given tuple.
 *
 * Exports:
 *  Four LRDS_FetchColLength(Four, Four, TupleID*, Four, ColListStruct*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eFETCHERROR
 *    some errors caused by function calls
 *
 * Side effects:
 *  The fetched data are returned via ColListStruct 'lengthInfoList'.
 */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four LRDS_FetchColLength(
    Four handle,
    Four ornOrScanId,		/* IN open relation no or scan id*/
    Boolean useScanFlag,        /* IN TRUE if above parameter is scan id */
    TupleID *tid,		/* IN tuple to fetch */
    Four    nCols,		/* IN number of columns to fetch */
    ColLengthInfoListStruct lengthInfoList[])	/* INOUT columns to fetch */
{
    Four e;			/* error code */
    Four orn;
    Four smScanId;
    Four i;			/* index variable */
    Four size;			/* size of tuple header */
    Four varColNo;		/* column number of variable-length columns */
    Four start;			/* starting offset of fetch */
    Four length;		/* amount of data to fetch */
    ColDesc *cdesc;		/* pointer to the current column descriptor */
    TupleHdr tupHdr;		/* a tuple header */
    unsigned char *nullVector;	/* bit array of null flags */
    LockParameter lockup;	/* lockup for SM_Fetch Tuple */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    LockParameter fileLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter objLockup;	/* lockup for SM_Fetch Tuple */
    LockParameter *fileLockupPtr; /* pointer to the lockup value */
    LockParameter *objLockupPtr; /* pointer to the lockup value */
    ColDesc *relTableEntry_cdesc;


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_FetchColLength()"));


    /* check parameters */
    if (useScanFlag == TRUE && !LRDS_VALID_SCANID(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag == FALSE && !LRDS_VALID_ORN(handle, ornOrScanId)) ERR(handle, eBADPARAMETER);

    if (useScanFlag) {
        orn = LRDS_SCANTABLE(handle)[ornOrScanId].orn;
        smScanId = LRDS_SCANTABLE(handle)[ornOrScanId].smScanId;
    } else {
        orn = ornOrScanId;
        smScanId = NIL;
    }

    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc);

    if (nCols <= 0) ERR(handle, eBADPARAMETER);

    if (lengthInfoList == NULL) ERR(handle, eBADPARAMETER);


    /* Prepare lock parameter */

    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	fileLockupPtr = objLockupPtr = NULL;

    } else {
	fileLockup.mode = L_IS;
	fileLockup.duration = L_COMMIT;
	fileLockupPtr = &fileLockup;

	objLockup.mode = L_S;
	objLockup.duration = L_COMMIT;
	objLockupPtr = &objLockup;
    }

    /* Fetch the tuple header. */
    size = TUPLE_HEADER_SIZE(relTableEntry->ri.nColumns, relTableEntry->ri.nVarColumns);

    e = LRDS_FETCHOBJECT(handle, &relTableEntry->ri.fid, smScanId, useScanFlag,
                         (ObjectID*)tid, 0, size, (char*)&tupHdr,
                         (relTableEntry->isCatalog) ? NULL : fileLockupPtr,
                         (relTableEntry->isCatalog) ? NULL : objLockupPtr);
    if (e < eNOERROR) ERR(handle, e);


    /* 'nullVector' points to bit array of null flags. */
    nullVector = NULLVECTOR_PTR(tupHdr, tupHdr.nVarCols);


    /* For each entry in 'lengthInfoList', read the data from the tuple. */
    for (i = 0; i < nCols; i++) {

	if (lengthInfoList[i].colNo >= relTableEntry->ri.nColumns)
	    ERR(handle, eBADPARAMETER);

	/* For fast and simple access */
	cdesc = &(relTableEntry_cdesc[lengthInfoList[i].colNo]);

	/* check NULL value. */
	if (LRDS_HAS_NULL_VALUE(lengthInfoList[i].colNo, cdesc, tupHdr, nullVector)) {
	    lengthInfoList[i].length = NULL_LENGTH;
	    continue;
	}

	if (cdesc->varColNo != NIL) {
	    /* Variable Length Column */

            start = LRDS_VARCOL_START_OFFSET(cdesc->varColNo, tupHdr);
            length = LRDS_VARCOL_REAL_SIZE(cdesc->varColNo, start, tupHdr);

	    lengthInfoList[i].length = length;

	} else {
	    /* Fixed Length Column */

	    lengthInfoList[i].length = cdesc->length;
	}
    }

    return(eNOERROR);

} /* LRDS_FetchColLength() */

