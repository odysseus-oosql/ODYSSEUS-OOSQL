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
 * Module: LRDS_Dismount.c
 *
 * Description:
 *  Dismount the given volume.
 *
 * Exports:
 *  Four LRDS_Dismount(Four)
 *  Four lrds_Dismount(Four)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eBADVOLUMEID
 *    some errors caused by function calls
 */


#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "Util.h"
#include "SM_Internal.h" 	
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four LRDS_Dismount(
    Four handle,
    Four volId)			/* IN volume to dismount */
{
    Four e;			/* error number */
    Four v;			/* index on LRDS mount table */
    Four user_v;		/* index on LRDS user mount table */
    Four i;			/* index variable */
    Four orn;			/* open relation number */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_Dismount(handle, volId=%ld)", volId));


    /* check parameter */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    for (user_v = 0; user_v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; user_v++)
	if (LRDS_USERMOUNTTABLE(handle)[user_v].volId == volId) break;

    if (user_v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE)
	ERR(handle, eBADVOLUMEID);


    /*
    ** Cleanup Operation before dismount operation
    */
    /* Close its local scans  on the dismounted volume */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsScanTable.nEntries; i++) {
	if (LRDS_SCANTABLE(handle)[i].orn != NIL &&
	    LRDS_GET_RELTABLE_ENTRY(handle, LRDS_SCANTABLE(handle)[i].orn)->ri.fid.volNo == volId) {

	    /* Close this scan. */
	    e = LRDS_CloseScan(handle, i);
	    if (e < 0) ERR(handle, e);

	}
    }


    /* Close its  relation opened on the dismounted volume */
    for (i=0; i < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; i++) {
	/* skip the unused entry */
	if (LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, i)) continue;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, i);

	if (relTableEntry->ri.fid.volNo == volId) {

	    /* We reduce the number of opens to 1. Then we call LRdS_CloseRelation(). */
	    /* Otherwise we should call LRDS_CloseRelation as many as the number of opens. */
	    if(LRDS_USEROPENRELTABLE(handle)[i].count > 0)	/* Beware that you close a relation one more times. */ 
	    {
		LRDS_USEROPENRELTABLE(handle)[i].count = 1;

		e = LRDS_CloseRelation(handle, i);
		if (e < 0) ERR(handle, e);
	    }
	}
    }


    /*
    ** Destroy the temporary relation defined on this volume.
    */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; i++) {
	if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i) &&
	    LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.fid.volNo == volId) {
	    /* Assert that This relation is not opened because we have closed */
	    /* all relations opend by this process by above codes. */
	    /* Destroy this temporary file. */
	    e = LRDS_DestroyRelation(handle, volId, LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.relName);
	    if (e < 0) ERR(handle, e);
	}
    }


    /*
    ** Dismount the volume from the LRDS_USERMOUNTTABLE.
    */
    LRDS_USERMOUNTTABLE(handle)[user_v].nMount --;
    if (LRDS_USERMOUNTTABLE(handle)[user_v].nMount > 0) return(eNOERROR);

    /* set to unused the corresponding entry of the user mount table. */
    LRDS_SET_TO_UNUSED_ENTRY_OF_USERMOUNTTABLE(handle, user_v);

    /*
    ** Dismount action at the LRDS.
    */
    e = lrds_Dismount(handle, volId);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_Dismount() */



/*
 * Function: Four lrds_Dismount(Four)
 *
 * Description:
 *  Dismount the given volume from the mount table in the shared memory.
 *
 * Returns:
 *  Error Code
 */
Four lrds_Dismount(
    Four handle,
    Four volId)			/* IN volume to dismount */
{
    Four e;			/* error number */
    Four v;			/* index on LRDS mount table */
    Four i;			/* index variable */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(TR_LRDS, TR1, ("lrds_Dismount(handle, volId=%ld)", volId));


    /* check parameter */
    if (volId < 0) ERR(handle, eBADPARAMETER);



    /*
    ** Dismount action at the LRDS.
    */

    /* Mutex Begin ::
    ** serialize the allocation/deallocation of an antry in LRDS_MOUNTTABLE
    */
    ERROR_PASS(handle, SHM_getLatch(handle, &LRDS_LATCH_OPENRELATION(handle), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));

    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE; v++)
	if (LRDS_MOUNTTABLE(handle)[v].volId == volId) break;

    if (v == LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE)
	ERRL1(handle, eBADVOLUMEID, &LRDS_LATCH_OPENRELATION(handle));


    LRDS_MOUNTTABLE(handle)[v].nMount--;

    if ( LRDS_MOUNTTABLE(handle)[v].nMount == 0 ) {

	/*
	** Cleanup Operation before dismount operation
	*/
	/* Close its  relation opened on the dismounted volume */
	for (i=0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE; i++) {
	    /* skip the unused entry */
	    if (LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(handle, i)) continue;

	    if (LRDS_RELTABLE(handle)[i].ri.fid.volNo == volId) { 

		/* We reduce the number of opens to 1. Then we call LRdS_CloseRelation(). */
		/* Otherwise we should call LRDS_CloseRelation as many as the number of opens. */
		if(LRDS_RELTABLE(handle)[i].count > 0)	/* Beware that you close a relation one more times. */ 
		{
		    LRDS_RELTABLE(handle)[i].count = 1;

		    e = lrds_CloseSharedRelation(handle, i);
		    if (e < 0) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION(handle));
		}
	    }
	}


	/* Close the catalog tables. */
	if(!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(handle, LRDS_MOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES]))
	{   /* Beware that you close a relation one more times. */
	    e = lrds_CloseSharedRelation(handle, LRDS_MOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSTABLES]);
	    if (e < 0) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION(handle));
	}

	if(!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(handle, LRDS_MOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSINDEXES]))
	{   /* Beware that you close a relation one more times. */
	    e = lrds_CloseSharedRelation(handle, LRDS_MOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSINDEXES]);
	    if (e < 0) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION(handle));
	}

	if(!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(handle, LRDS_MOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS]))
	{   /* Beware that you close a relation one more times. */
	    e = lrds_CloseSharedRelation(handle, LRDS_MOUNTTABLE(handle)[v].catalogTableOrn[LRDS_SYSCOLUMNS]);
	    if (e < 0) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION(handle));
	}

	/* Set to unused the corresponding entry of the mount table. */
	LRDS_SET_TO_UNUSED_ENTRY_OF_MOUNTTABLE(handle, v);

    }

    /* Dismount action of the SM and its sublayers. */
    e = SM_Dismount(handle, volId);
    if (e < 0) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION(handle));


    /* Mutex End ::
    ** serialize the allocation/deallocation of an antry in LRDS_MOUNTTABLE
    */
    ERROR_PASS(handle, SHM_releaseLatch(handle, &LRDS_LATCH_OPENRELATION(handle), procIndex)); 

    return(eNOERROR);

} /* lrds_Dismount() */
