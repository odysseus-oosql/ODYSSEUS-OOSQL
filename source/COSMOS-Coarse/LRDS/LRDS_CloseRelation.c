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
 * Module: LRDS_CloseRelation.c
 *
 * Description:
 *  Relation close related functions.
 *
 * Exports:
 *  Four LRDS_CloseRelation(Four)
 *  Four LRDS_CloseAllRelations(void)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM_Internal.h"	
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*
 * Module: Four LRDS_CloseRelation(Four)
 *
 * Description:
 *  Close a relation. The parameter 'orn', which are used to specify
 *  the closed relation, is an index on the LRDS_USEROPENRELTABLE.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 */
Four LRDS_CloseRelation(
    Four handle,
    Four orn)			/* IN open relation number */
{
    Four e;			/* IN error number */
    Four sysOrn;		/* open relation number; points to relation table entry */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_CloseRelation(handle)"));


    /* check parameter */
    if (!LRDS_VALID_ORN(handle, orn)) {
	printf("handle: %d, orn: %d\n", handle, orn);
	fflush(stdout);
	ERR(handle, eBADPARAMETER);
    }


    /* Decrement the number of opens of per-process open table. */
    LRDS_USEROPENRELTABLE(handle)[orn].count--;	/* local counter */
    if (LRDS_USEROPENRELTABLE(handle)[orn].count != 0) return(eNOERROR);

    /* Get the 'sysOrn': orn of the system table. */
    sysOrn = LRDS_USEROPENRELTABLE(handle)[orn].sysOrn;

    /* Make this entry unused. */
    LRDS_SET_TO_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, orn);


    /*
    ** Decrement the # of opens of the temporary relation table if this is a
    ** temporary relation. Otherwise, close the temporary system relation table.
    */
    if (LRDS_USEROPENRELTABLE(handle)[orn].tmpRelationFlag) {
	/* this is a temporarty table */

	return(eNOERROR);
    }

    /* Close the relation in the open relation table in the shared memory. */
    e = lrds_CloseSharedRelation(handle, sysOrn);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_CloseRelation() */



/*
 * Function: Four LRDS_CloseAllRelations(void)
 *
 * Description:
 *  Close all opened relations.
 *
 * Returns:
 *  error code
 */
Four LRDS_CloseAllRelations(Four handle)
{
    Four e;			/* IN error number */
    Four orn;			/* open relation number */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_CloseAllRelations(handle)"));


    /*
    ** Look up the per-process open table.
    ** If there is an open relation, close it.
    */
    for (orn = 0; orn < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; orn++) {
	/* skip unused entry */
	if (LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, orn)) continue;

        relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, orn);
        if (relTableEntry->isCatalog) {
            /* Mount operation set the count to 1. */
            LRDS_USEROPENRELTABLE(handle)[orn].count = 1;

        } else {
            /* set the count to 1 to close the relation actually */
            LRDS_USEROPENRELTABLE(handle)[orn].count = 1;

	    e = LRDS_CloseRelation(handle, orn);
	    if (e < eNOERROR) ERR(handle, e);
	}
    }

    return(eNOERROR);

} /* LRDS_CloseAllRelations() */

