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
 * Module: sm_GetCatalogEntryFromIndexId.c
 *
 * Description:
 *  Get the object identifier of the catalog object for B+ tree file containing
 *  the given index. 
 *
 * Exports:
 *  Four sm_GetCatalogEntryFromIndexId(Four, IndexID *, ObjectID *)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "OM_Internal.h"	
#include "BtM.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * sm_GetCatalogEntryFromIndexId()
 *================================*/
/*
 * Function: Four sm_GetCatalogEntryFromIndexId(Four, IndexID *, ObjectID *)
 *
 * Description:
 *  Get the object identifier of the catalog object for B+ tree file containing
 *  the given index. We look up the catalog object from SM_SYSTABLES.
 *
 * Returns:
 *  Error code
 *    eBADINDEXED_SM
 *    eNOTFOUNDCATALOGENTRY_SM
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) catalogEntry - ObjectID of the catalog entry in SM_SYSTABLES
 */
Four sm_GetCatalogEntryFromIndexId(
    Four handle,
    Four v,			/* index for the used volume on the mount table */
    IndexID *index,		/* IN index identifier of the given index */
    ObjectID *catalogEntry,	/* OUT object identifier of the SYSTABLES catalog object */
    PhysicalIndexID *pIndex,	/* OUT physical index identifier */ 
    ObjectID *catalogEntryForSysIndexes) /* OUT object identifier of SYSINDEXES catalog object */ 
{
    Four e;			/* error number */
    BtreeCursor cursor;		/* a B+ tree cursor */
    KeyValue kval;		/* key value */
    sm_CatOverlayForSysIndexes sysIndexesOverlay; /* object in SM_SYSINDEXES */


    TR_PRINT(TR_SM, TR1,
             ("sm_GetCatalogEntryFromIndexId(handle, v=%ld, index=%P, catalogEntry=%P)",
	      v, index, catalogEntry));

    /*
    ** Get the B+ tree file's FileID.
    */
    /*@ Construct kval. */
    /* Construct a key for the B+ tree on IndexID field of SM_SYSINDEXES */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&kval.val[0], (char*)&(index->volNo), sizeof(VolNo)); /* volNo's type is Two */
    memcpy(&kval.val[sizeof(VolNo)], (char*)&(index->serial), sizeof(Serial)); 

    /* Get the ObjectID of the catalog object for 'index' in SM_SYSINDEXES */
    e = BtM_Fetch(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesIndexIdIndex),
		  &SM_PER_THREAD_DS(handle).smSysIndexesIndexIdIndexKdesc, &kval, SM_EQ, &kval, SM_EQ, &cursor);
    if (e < 0) ERR(handle, e);

    /* The IndexID 'index' is invalid. */
    if (cursor.flag != CURSOR_ON)	ERR(handle, eBADINDEXID_SM);

    /* Read the SM_SYSINDEXES's entry */
    e = OM_ReadObject(handle, &(cursor.oid), 0, REMAINDER, (char *)&sysIndexesOverlay);
    if (e < 0) ERR(handle, e);

    
    /*
    ** Get 'catalogEntryForSysIndexes'
    */
    if (catalogEntryForSysIndexes != NULL)
        *catalogEntryForSysIndexes = cursor.oid;

    /*
    ** Get physical index ID
    */
    MAKE_PHYSICALINDEXID(*pIndex, index->volNo, sysIndexesOverlay.rootPage);


    /*
    ** Get the ObjectID of the catalog object for the found B+ tree's FileID
    ** in SM_SYSTABLES.
    */
    /*@ Construct kval. */
    /* Construct the key for the B+ tree on BtreeFileId field of SM_SYSTABLES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(sysIndexesOverlay.indexFid.volNo), sizeof(VolNo)); 
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(sysIndexesOverlay.indexFid.serial), sizeof(Serial)); 

    /* Search the B+ tree file. */
    e = BtM_Fetch(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesBtreeFileIdIndex),
		  &SM_PER_THREAD_DS(handle).smSysTablesBtreeFileIdIndexKdesc, &kval, SM_EQ, &kval, SM_EQ, &cursor);
    if (e < 0) ERR(handle, e);

    /* This is a critical error. We cannot find the catalog object. */
    if (cursor.flag != CURSOR_ON) ERR(handle, eNOTFOUNDCATALOGENTRY_SM);
    
    *catalogEntry = cursor.oid;    
        
    return(eNOERROR);

} /* sm_GetCatalogEntryFromIndexId() */

