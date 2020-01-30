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
 * Module: BtM_InitDS.c
 *
 * Description :
 *  Initialize data structures used in btree manager.
 *
 * Exports:
 *  Four BtM_InitSharedDS(Four)
 *  Four BtM_InitLocalDS(Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "Util.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"





/*@================================
 * Four BtM_InitSharedDS( )
 *================================*/
/*
 * Function: Four BtM_InitSharedDS(Four)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */
Four BtM_InitSharedDS(
    Four    	handle
)
{
    Four 	e;
    Four 	i;


    TR_PRINT(handle, TR_BTM, TR1, ("BtM_InitSharedDS()"));

    /*
    ** Initailze the main memory data structure used in Scan Manager.
    */
    /* Initialize the allocated entries in btm_treelatchPtrTable */
    for (i = 0; i < BTM_TREELATCH_HASHSIZE; i++)
	BTM_TREELATCHPTRTABLE[i] = LOGICAL_PTR(NULL); 

    e = SHM_initLatch(handle, &BTM_LATCH4TREELATCHTABLE);
    if (e < eNOERROR) ERR(handle, e);

    /* Initialize the treeLatchCell Pool. */
    e = Util_initPool(handle, &BTM_TREELATCHPOOL, sizeof(btmTreeLatchCell), BTM_TREELATCH_INITPOOLSIZE);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* BtM_InitSharedDS() */



/*@================================
 * Four BtM_InitLocalDS( )
 *================================*/
/*
 * Function: Four BtM_InitLocalDS(Four)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */
Four BtM_InitLocalDS(
    Four			handle
)
{
    Four 			e;                     /* error number */
    Four 			i;                     /* temporary index */

    /* pointer for BtM Data Structure of perThreadTable */
    BtM_PerThreadDS_T *btm_perThreadDSptr = BtM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1, ("BtM_InitLocalDS()"));

    /* Allocate some entries in the btmCache4TreeLatch */
    e = Util_initVarArray(handle, &(btm_perThreadDSptr->btmCache4TreeLatch), sizeof(btmCache4TreeLatchCell), BTM_TREELATCH_INITCACHESIZE);
    if (e < eNOERROR) ERR(handle, e);

    /* Initialize the allocated entries in the cache for tree latch */
    for (i = 0; i < btm_perThreadDSptr->btmCache4TreeLatch.nEntries; i++) {
        SET_NILINDEXID(BTM_CACHE4TREELATCH(handle)[i].iid);
        BTM_CACHE4TREELATCH(handle)[i].tLatchCellPtr = NULL;
    }

    /* Initialize the LogicalID Mapping Table for BtM */
    e = btm_IdMapping_InitTable(handle); 
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* BtM_InitLocalDS() */

