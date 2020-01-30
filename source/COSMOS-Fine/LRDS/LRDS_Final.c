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
 * Module: LRDS_Final.c
 *
 * Description:
 *  Finalize the LRDS and its sublayers. Sublayers are finalized via their
 *  interface function SM_Final( ). In the LRDS we free the allocated memory.
 *
 * Exports:
 *  Four LRDS_FinalSharedDS()
 *  Four LRDS_FinalLocalDS()
 *
 * Returns:
 *  Error code
 *   some erros caused by function calls
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


Four LRDS_FinalSharedDS(
    Four handle)
{
    Four e;			/* error number */
    Four v;			/* index on the LRDS mount table */


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_FinalSharedDS()"));


    /* Dismount the volumes that has been mounted but not dismounted. */
    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE; v++)
	if (!LRDS_IS_UNUSED_ENTRY_OF_MOUNTTABLE(v)) { /* entry for a mounted volume */

	    /* enforce the dismount operation */
	    LRDS_MOUNTTABLE[v].nMount = 1;
	    e = lrds_Dismount(handle, LRDS_MOUNTTABLE[v].volId);
	    if (e < eNOERROR) ERR(handle, e);
	}

    /*
    ** Finalize the shared memory data structure used in LRDS.
    */
    /* Finalize the column table heap. */
    e = Util_finalHeap(handle, &LRDS_COLUMNTABLEHEAP);
    if (e < eNOERROR) ERR(handle, e);

    /* Finalize the index table heap. */
    e = Util_finalHeap(handle, &LRDS_INDEXTABLEHEAP);
    if (e < eNOERROR) ERR(handle, e);

    /* Finalize the ordered set aux column info pool */
    e = Util_finalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_POOL);
    if (e < eNOERROR) ERR(handle, e);

#ifdef COSMOS_S
    e = SM_FinalSharedDS(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif /* COSMOS_S */

    return(eNOERROR);

} /* LRDS_FinalSharedDS() */


Four LRDS_FinalLocalDS(
    Four handle)
{
    Four e;			/* error number */
    Four i;			/* temporary variable */

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_FinalLocalDS()"));

    /* Dismount the mounted volumes. */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; i++)
	if (!LRDS_IS_UNUSED_ENTRY_OF_USERMOUNTTABLE(handle, i))
	    (Four)LRDS_Dismount(handle, LRDS_USERMOUNTTABLE(handle)[i].volId);

    /* Finalize the boolean table heap. */
    e = Util_finalLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle));
    if (e < eNOERROR) ERR(handle, e);

    /* Finalize the scan table.*/
    e = Util_finalVarArray(handle, &(lrds_perThreadDSptr->lrdsScanTable));
    if (e < eNOERROR) ERR(handle, e);

    /* Finalize the set scan table.*/
    e = Util_finalVarArray(handle, &(lrds_perThreadDSptr->lrdsSetScanTable));
    if (e < eNOERROR) ERR(handle, e);

    /* Finalize the ordered set scan table.*/
    e = Util_finalVarArray(handle, &(lrds_perThreadDSptr->lrdsOrderedSetScanTable));
    if (e < eNOERROR) ERR(handle, e);

    /* Finalize the ordered set aux column info pool */
    e = Util_finalLocalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(handle));
    if (e < eNOERROR) ERR(handle, e);

    /* Finalize the collection scan table.*/
    e = Util_finalVarArray(handle, &(lrds_perThreadDSptr->lrdsCollectionScanTable));
    if (e < eNOERROR) ERR(handle, e);

#ifdef COSMOS_S
    e = SM_FinalLocalDS(handle);
    if (e < 0)ERR(handle, e);
#endif /* COSMOS_S */

    /* Finalize the Ordered Set Element Length Pool */
    e = Util_finalLocalPool(handle, &LRDS_ORDEREDSET_ELEMENT_LENGTH_POOL(handle));
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LRDS_FinalLocalDS() */
