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
 * Module: LRDS_InitDS.c
 *
 * Description:
 *  Initialize the LRDS layer and its sublayers.
 *  In LRDS, we should initialize Mount Table,  Open Relation Table, Scan Table,
 *  Boolean Table Heap, Column Table Heap, and IndexTable Heap.
 *
 * Exports:
 *  Four LRDS_InitSharedDS()
 *  Four LRDS_InitGlobalDS()
 *  VarArray lrdsScanTable
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM_Internal.h"	
#include "LRDS.h"
#include "BL_LRDS.h"    
#include "perThreadDS.h"
#include "perProcessDS.h"

/*
** Global Variables(it was shared variables)
*/
Four procIndex;	

Four LRDS_InitSharedDS(Four handle)
{
    Four e;			/* error number */
    Four i;			/* index variable */


    TR_PRINT(TR_LRDS, TR1, ("LRDS_InitSharedDS(handle)"));

     /* Initialize the storage manager. */
#ifdef COSMOS_S		
    e = SM_InitSharedDS(handle);
    if (e < 0) ERR(handle, e);
#endif /* COSMOS_S */

    /*
    ** Initialize the main memory data structures used in LRDS.
    */
    /* Initialize the entries in the Mount Table. */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE; i++)
	LRDS_SET_TO_UNUSED_ENTRY_OF_MOUNTTABLE(handle, i);

    /* Initialize the entries in the Open Relation Table. */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE; i++) {
	LRDS_SET_TO_UNUSED_ENTRY_OF_RELTABLE(handle, i);
	e = SHM_initLatch(handle, &LRDS_RELTABLE(handle)[i].latch); 
        if (e < eNOERROR) ERR(handle, e);
    }

    /* Initialize the Column Table Heap. */
    e = Util_initHeap(handle, &LRDS_COLUMNTABLEHEAP(handle), sizeof(ColDesc), INITCOLUMN);
    if (e < 0) ERR(handle, e);

    /* Initialize the Index Table Heap. */
    e = Util_initHeap(handle, &LRDS_INDEXTABLEHEAP(handle), sizeof(IndexInfo), INITINDEX);
    if (e < 0) ERR(handle, e);

    /* Ordered set */
    /* Initialize the Ordered Set Auxiliary Column Info Pool. */
    e = Util_initPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_POOL(handle), sizeof(OrderedSetAuxColInfo_T), INIT_SIZE_LRDS_ORDEREDSET_AUXCOLINFO_POOL);
    if (e < 0) ERR(handle, e);

    /* Initialize the latch for mount table and relation table */
    e = SHM_initLatch(handle, &LRDS_LATCH_OPENRELATION(handle));
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_Init() */



Four LRDS_InitLocalDS(Four handle)
{
    Four e;			/* error number */
    Four i;			/* index variable */

    TR_PRINT(TR_LRDS, TR1, ("LRDS_InitLocalDS(handle)"));

    /*@ Initialize the storage manager. */
#ifdef COSMOS_S		
    e = SM_InitLocalDS(handle);
    if (e < 0) ERR(handle, e);
#endif /* COSMOS_S */

    /* Initialize the user mount table. */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; i++)
	LRDS_SET_TO_UNUSED_ENTRY_OF_USERMOUNTTABLE(handle, i);

    /*
    ** LRDS_RELTABLE_FOR_TMP_RELS
    */
    /* Initialize the entries. */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; i++)
	LRDS_SET_TO_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i);

    /* Allocate some entries to the Scan Table. */
    e = Util_initVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable, sizeof(lrds_ScanTableEntry), INITSCAN);
    if (e < 0) {
	(void) SM_FinalLocalDS(handle);
	ERR(handle, e);
    }

    /* Initialize the allocated entries in the Scan Table. */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsScanTable.nEntries; i++)
	LRDS_SCANTABLE(handle)[i].orn = NIL;

    /* Allocate some entries to the Set Scan Table. */
    e = Util_initVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsSetScanTable, sizeof(lrds_SetScanTableEntry), INITSETSCAN);
    if (e < 0) {
	(void) SM_FinalLocalDS(handle);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable);
	ERR(handle, e);
    }

    /* Initialize the allocated entries in the Set Scan Table. */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsSetScanTable.nEntries; i++)
	LRDS_SETSCANTABLE(handle)[i].ornOrRelScanId = NIL;

    /* ordered set */
    /* Allocate some entries to the Ordered Set Scan Table. */
    e = Util_initVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsOrderedSetScanTable, sizeof(lrds_OrderedSetScanTableEntry), INITORDEREDSETSCAN);
    if (e < 0) {
	(void) SM_FinalLocalDS(handle);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsSetScanTable);
	ERR(handle, e);
    }

    /* Initialize the allocated entries in the Ordered Set Scan Table. */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsOrderedSetScanTable.nEntries; i++)
	LRDS_ORDEREDSETSCANTABLE(handle)[i].ornOrRelScanId = NIL;

    /* Initialize the allocated entries in the User Open Relation Table. */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; i++)
	LRDS_SET_TO_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, i);

    /* Initialize the Boolean Table Heap.*/
    e = Util_initLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle), sizeof(BoolExp), INITBOOL);
    if (e < 0) {
	(void) SM_FinalLocalDS(handle);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsSetScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsOrderedSetScanTable);
	ERR(handle, e);
    }

    /* ordered set */
    /* Initialize the ordered set aux column info pool */
    e = Util_initLocalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(handle), sizeof(OrderedSetAuxColInfo_T), INIT_SIZE_LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL);
    if (e < 0) {
        (void) SM_FinalLocalDS(handle);
        (void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable);
        (void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsSetScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsOrderedSetScanTable); 
        (void) Util_finalLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle));
        ERR(handle, e);
    }

    /* Allocate some entries to the Collection Scan Table. */
    e = Util_initVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable, sizeof(lrds_CollectionScanTableEntry), INITCOLLECTIONSCAN);
    if (e < 0) {
	(void) SM_FinalLocalDS(handle);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsSetScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsOrderedSetScanTable); 
        (void) Util_finalLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle));
        (void) Util_finalLocalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(handle)); 
	ERR(handle, e);
    }

    /* Initialize the allocated entries in the Set Scan Table. */
    for (i = 0; i < LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable.nEntries; i++)
	LRDS_COLLECTIONSCANTABLE(handle)[i].ornOrScanId = NIL;

    /* Initialize bulkload table */
    for (i = 0; i < LRDS_BLKLD_TABLE_SIZE; i++ ) {
        LRDS_BLKLD_TABLE(handle)[i].isUsed = FALSE;
    }

    /* Initialize the Ordered Set Element Length Pool */
    e = Util_initLocalPool(handle, &LRDS_ORDEREDSET_ELEMENT_LENGTH_POOL(handle), 
			   LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE, 
			   LRDS_ORDEREDSET_ELEMENT_LENGTH_INIT_CACHE);
    if (e < 0) {
	(void) SM_FinalLocalDS(handle);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsSetScanTable);
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsOrderedSetScanTable); 
        (void) Util_finalLocalHeap(handle, &LRDS_BOOLTABLEHEAP(handle));
        (void) Util_finalLocalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(handle));
	(void) Util_finalVarArray(handle, &LRDS_PER_THREAD_DS(handle).lrdsCollectionScanTable);
	ERR(handle, e);
    }

    return(eNOERROR);

} /* LRDS_InitLocalDS() */

