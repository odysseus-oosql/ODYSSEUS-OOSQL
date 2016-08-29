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
 * Module: SM_AddIndex.c
 *
 * Description:
 *  Create an index on the given data file.
 *
 * Exports:
 *  Four SM_AddIndex(FileID*, IndexID*)
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
 * SM_AddIndex()
 *================================*/
/*
 * Function: Four SM_AddIndex(FileID*, IndexID*)
 *
 * Description:
 *  Create an index on the given data file. It returns the IndexId which
 *  will be used to identify the index to use.
 *
 * Returns:
 *  error code
 *   eBADPARAMETER_SM
 *   some errors caused by function calls
 */
Four _SM_AddIndex(
    Four handle,
    FileID *fid,		/* IN data file on which an index is created */
    IndexID *iid)		/* OUT index idetifier of the new index */
{
    Four e;			/* error number */
    Four v;			/* array index on the mount table */
    KeyValue kval;		/* a key value */
    ObjectID catalogEntry;	/* ObjectID of the catalog entry in SM_SYSTABLES */
    ObjectID oid;		/* new object created in SM_SYSINDEXES */
    sm_CatOverlayForBtree catalogOverlay; /* Btree part of the catalog entry */
    sm_CatOverlayForSysIndexes sysIndexesOverlay; /* contents of catalog entry in SM_SYSINDEXES */
    PhysicalIndexID pIid;	
    
    
    TR_PRINT(TR_SM, TR1, ("SM_AddIndex(handle, fid=%P, iid=%P)", fid, iid));

    /*@ check parameters */
    if (fid == NULL) ERR(handle, eBADPARAMETER_SM);

    if (iid == NULL) ERR(handle, eBADPARAMETER_SM);

    /* Is there an active transactin? */
    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);
    
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == fid->volNo) break; /* found */ 
    
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    
    /* Get the ObjectID of the catalog entry in SM_SYSTABLES. */
    e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &catalogEntry);
    if (e < 0) ERR(handle, e);

    
    /*@ Create an index. */
    e = BtM_CreateIndex(handle, &catalogEntry, &pIid); 
    if (e < 0) ERR(handle, e);    


    /* Read the catalog entry. */
    e = OM_ReadObject(handle, &catalogEntry, SM_SYSTABLES_BTREE_START,
		      sizeof(sm_CatOverlayForBtree), &catalogOverlay);
    if (e < 0) ERR(handle, e);


    /* allocate new logical ID */
    e = sm_GetNewIndexId(handle, v, iid);
    if (e < 0) ERR(handle, e);
    
    /* Insert the information on the new index in SM_SYSINDEXES. */
    sysIndexesOverlay.indexFid = catalogOverlay.fid; 
    sysIndexesOverlay.iid = *iid;
    sysIndexesOverlay.rootPage = pIid.pageNo; 
    sysIndexesOverlay.indexType = SM_INDEXTYPE_BTREE;    
    e = OM_CreateObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry), 
			NULL, NULL,sizeof(sm_CatOverlayForSysIndexes),
			&sysIndexesOverlay, &oid);
    if (e < 0) ERR(handle, e);

    
    /*
    ** Insert the object into the B+ tree on IndexId field of SM_SYSINDEXES.
    */
    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(iid->volNo), sizeof(VolNo)); /* volNo's type is Two. */
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(iid->serial), sizeof(Serial)); 
	 
    e = BtM_InsertObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
			 &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesIndexIdIndex),
			 &SM_PER_THREAD_DS(handle).smSysIndexesIndexIdIndexKdesc, &kval, &oid,
			 &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);


    /*
    ** Insert the object into the B+ tree on index file FileID of SM_SYSINDEXES.
    */
    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(catalogOverlay.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(catalogOverlay.fid.serial), sizeof(Serial)); 

    e = BtM_InsertObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
			 &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesBtreeFileIdIndex),
			 &SM_PER_THREAD_DS(handle).smSysIndexesIndexFileIdIndexKdesc, &kval, &oid,
			 &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead)); 
    if (e < 0) ERR(handle, e);
	  
	  
    return(eNOERROR);

} /* SM_AddIndex() */
