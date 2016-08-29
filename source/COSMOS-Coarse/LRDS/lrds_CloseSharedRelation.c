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
 * Module: lrds_CloseSharedRelation.c
 *
 * Description:
 *  Close the given relation. The relation information is in LRDS_RELTABLE
 *  in the shared memory. The parameter 'sysOrn', which are used to specify
 *  the closed relation, is an index on the LRDS_RELTABLE.
 *
 * Exports:
 *  Four lrds_CloseSharedRelation(Four)
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */


#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM_Internal.h"	
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four lrds_CloseSharedRelation(
    Four handle,
    Four sysOrn)		/* IN open relation number */
{
    Four e;			/* error number */
    Four i;                     
    ColDesc *relTableEntry_cdesc;


    TR_PRINT(TR_LRDS, TR1, ("lrds_CloseSharedRelation(handle, sysOrn=%ld)", sysOrn));


    /* Mutex Begin ::
    **  - allocation/deallocation of entry in LRDS_RELTABLE
    */
    e = SHM_getLatch(handle, &LRDS_RELTABLE(handle)[sysOrn].latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL); 
    if (e < eNOERROR) ERR(handle, e);


    /* Decrement the number of opens by 1. */
    LRDS_RELTABLE(handle)[sysOrn].count --;

    /* Free resources if the number of opens is 0. */
    if (LRDS_RELTABLE(handle)[sysOrn].count == 0) {

	if (LRDS_RELTABLE(handle)[sysOrn].ri.nIndexes != 0) {
	    e = Util_freeArrayToHeap(handle, &LRDS_INDEXTABLEHEAP(handle), PHYSICAL_PTR(LRDS_RELTABLE(handle)[sysOrn].ii)); 
	    if (e < 0) ERRL1(handle, e, &LRDS_RELTABLE(handle)[sysOrn].latch);
	}

        relTableEntry_cdesc = PHYSICAL_PTR(LRDS_RELTABLE(handle)[sysOrn].cdesc); 
	if (LRDS_RELTABLE(handle)[sysOrn].ri.nColumns != 0) {
            /* ordered set */
            for (i = 0; i < LRDS_RELTABLE(handle)[sysOrn].ri.nColumns; i++) {
                if (relTableEntry_cdesc[i].auxInfo != NULL) {
                    assert(relTableEntry_cdesc[i].complexType == SM_COMPLEXTYPE_ORDEREDSET);

                    e = Util_freeElementToPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_POOL(handle),
                                               relTableEntry_cdesc[i].auxInfo);
                    if (e < 0) ERRL1(handle, e, &LRDS_RELTABLE(handle)[sysOrn].latch);
                }
            }

	    e = Util_freeArrayToHeap(handle, &LRDS_COLUMNTABLEHEAP(handle), relTableEntry_cdesc);
	    if (e < 0) ERRL1(handle, e, &LRDS_RELTABLE(handle)[sysOrn].latch);
	}

	/* Unmark the entry; set it to unused. */
	LRDS_SET_TO_UNUSED_ENTRY_OF_RELTABLE(handle, sysOrn);
    }

    /* Mutex End ::
    **  - allocation/deallocation of entry in LRDS_RELTABLE
    */
    e = SHM_releaseLatch(handle, &LRDS_RELTABLE(handle)[sysOrn].latch, procIndex); 
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_CloseSharedRelation() */


