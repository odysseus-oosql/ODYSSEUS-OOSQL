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
 * Module: LRDS_MLGF_SearchNearTuple.c
 *
 * Description:
 *  Search the near tuple of the given tuple when the tuples are ordered
 *  with the given MLGF index.
 *
 * Exports:
 *  Four LRDS_MLGF_SearchNearObject(handle, Four, IndexID*, MLGF_HashValue[], ObjectID*, LockParameter*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eDEADLOCK
 *    eINDEXNOTFOUND_LRDS
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#ifndef COSMOS_S
#include "LM.h"
#endif /* COSMOS_S */
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four LRDS_MLGF_SearchNearTuple(
    Four handle,
    Four orn,			/* IN open relation number */
    IndexID  *iid,		/* IN the used index */
    MLGF_HashValue kval[],	/* IN hash values of the given object */
    TupleID *tid,		/* OUT TupleID of the near tuple */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e;			/* error number */
    Four indexInfoIndex;	/* corresponding entry of index info entry */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    IndexInfo *relTableEntry_ii;
    PageID catalogPid;


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_MLGF_SearchNearTuple()"));


    /* check parameters */
    if (!LRDS_VALID_ORN(handle, orn)) ERR(handle, eBADPARAMETER);

    if (iid == NULL) ERR(handle, eBADPARAMETER);

    if (kval == NULL) ERR(handle, eBADPARAMETER);

    if (tid == NULL) ERR(handle, eBADPARAMETER);


    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
    relTableEntry_ii = PHYSICAL_PTR(relTableEntry->ii);


    /*
    ** Request Manual Duration lock for read
    ** Not follow Lock Hierarchy
    */

#ifdef SYSTABLE_RECORD_LOCKING
    e = LM_getFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry),
			   L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
    if (e < eNOERROR) ERR(handle, e);
#else
    /* get the pid from the tid of the catalog entry */
    MAKE_PAGEID(catalogPid, relTableEntry->ri.catalogEntry.volNo, relTableEntry->ri.catalogEntry.pageNo);

    e = LM_getFlatPageLock(handle, &MY_XACTID(handle), &catalogPid,
			   L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
    if (e < eNOERROR) ERR(handle, e);
#endif

    if ( lockReply == LR_DEADLOCK )
	 ERR(handle, eDEADLOCK);


    /* Search for the index info about the given index. */
    /* 'indexInfoIndex' points to the corresponding entry of index info array */
    for (indexInfoIndex = 0; indexInfoIndex < relTableEntry->ri.nIndexes; indexInfoIndex++)
	if (EQUAL_INDEXID(*iid, relTableEntry_ii[indexInfoIndex].iid)) break; 

    if (indexInfoIndex == relTableEntry->ri.nIndexes) {

#ifdef SYSTABLE_RECORD_LOCKING
	ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	ERR(handle, eINDEXNOTFOUND_LRDS);
#else
	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, eINDEXNOTFOUND_LRDS);
#endif

    }


    /* Search the near tuple. */
    e = SM_MLGF_SearchNearObject(handle, iid, &(relTableEntry_ii[indexInfoIndex].kdesc.mlgf),
				 kval, (ObjectID*)tid, lockup);
    if (e < 0) {

#ifdef SYSTABLE_RECORD_LOCKING
	ERROR_PASS(handle, LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL));
	ERR(handle, e);
#else
	ERROR_PASS(handle, LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL));
	ERR(handle, e);
#endif

    }

#ifdef SYSTABLE_RECORD_LOCKING
    e = LM_releaseFlatObjectLock(handle, &MY_XACTID(handle), &(relTableEntry->ri.catalogEntry), L_MANUAL);
    if ( e < eNOERROR ) ERR(handle, e);
#else
    e = LM_releaseFlatPageLock(handle, &MY_XACTID(handle), &catalogPid, L_MANUAL);
    if ( e < eNOERROR ) ERR(handle, e);
#endif


    return(eNOERROR);

} /* LRDS_MLGF_SearchNearTuple( ) */
