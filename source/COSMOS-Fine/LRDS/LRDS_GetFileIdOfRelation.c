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
 * Module: LRDS_GetFileIdOfRelation.c
 *
 * Description:
 *  Get file id of the given relation.
 *
 * Exports:
 *  Four LRDS_GetFileIdOfRelation(Four, Four, char*, FileID*)
 *
 * Returns:
 *  error code
 *    eNOERROR
 *    eBADPARAM_LRDS
 *    some errors caused by function calls
 */


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


Four LRDS_GetFileIdOfRelation(
    Four handle,
    Four volId,			/* IN volume ID */
    char *relName,		/* IN relation name */
    FileID *fid)		/* OUT file id of the given relation */
{
    Four e;			/* error number */
    Four v;			/* IN index on the LRDS Mount Table*/
    Four orn;			/* open relation number; points to user open relation table entry */
    Four sysOrn;		/* open relation number; points to relation table entry */
    Four catScanId;		/* a SM level scan */
    Four i, j;			/* index variables */
    TupleID catalogEntry;	/* TupleID of the catalog entry of LRDS_SYSTABLES */
    BoundCond lb;		/* bounary condition of range scan */
    Boolean notFound;		/* The given relation isn't found. */
    ColListStruct clist[1];	/* column list structure */
    Two keyLen;			/* should be 'Two' to store the key length */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_GetFileIdOfRelation(volId=%ld, relName=%P, fid=%P)", volId, relName, fid));


    /* check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; v++)
	if (LRDS_USERMOUNTTABLE(handle)[v].volId == volId) break;


    /*
    ** Check the per-process open table.
    */
    for (orn = 0; orn < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; orn++) {
	/* skip unused entry */
	if (LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, orn)) continue;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
	if (strcmp(relName, relTableEntry->ri.relName) == 0) {
	    *fid = relTableEntry->ri.fid;

	    return(eNOERROR);
	}
    }


    /*
    ** Check whether the given relation is a temporary relation.
    */
    for (sysOrn = 0; sysOrn < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; sysOrn++) {
	if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, sysOrn) &&
	    strcmp(relName, LRDS_RELTABLE_FOR_TMP_RELS(handle)[sysOrn].ri.relName) == 0) {
	    /* the given relation is a temporary relation */

	    *fid = LRDS_RELTABLE_FOR_TMP_RELS(handle)[sysOrn].ri.fid;

	    return(eNOERROR);
	}
    }


    /*
    ** Extract the corresponding catalog entry from LRDS_SYSTABLES.
    */
    /* start condition for the point scan. */
    /* The upper bound is same with the lower bound 'lb'. */
    lb.op = SM_EQ;
    keyLen =   strlen(relName);
    lb.key.len = sizeof(Two) + keyLen;
    memcpy(&(lb.key.val[0]), (char*)&keyLen, sizeof(Two));
    memcpy(&(lb.key.val[sizeof(Two)]), relName, keyLen);
    catScanId = LRDS_OpenIndexScan(handle, LRDS_USERMOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES],
				   &(((IndexInfo*)PHYSICAL_PTR(LRDS_GET_RELTABLE_ENTRY_FOR_CATALOG(handle, v,LRDS_SYSTABLES)->ii))[0].iid), 
				   &lb, &lb, 0, (BoolExp*)NULL, NULL);
    if (catScanId < 0) ERR(handle, catScanId);

    /* Initialize a flag meaning the existence of data file search. */
    notFound = FALSE;

    e = LRDS_NextTuple(handle, catScanId, &catalogEntry, (LRDS_Cursor**)NULL);
    if (e < eNOERROR) ERR(handle, e);

    if (e == EOS) notFound = TRUE;
    else {
	/* Construct clist. */
	clist[0].colNo = LRDS_SYSTABLES_DATAFILEID_COLNO;
	clist[0].start = ALL_VALUE;

	e = LRDS_FetchTuple(handle, catScanId, TRUE, (TupleID*)NULL, 1, &(clist[0]));
	if (e < eNOERROR) ERR(handle, e);

	/* Set fil id of the given relation. */
	*fid = clist[0].data.fid; 
    }

    e = LRDS_CloseScan(handle, catScanId);
    if (e < eNOERROR) ERR(handle, e);

    if (notFound) ERR(handle, eRELATIONNOTFOUND_LRDS);

    return(eNOERROR);

} /* LRDS_GetFileIdOfRelation() */

