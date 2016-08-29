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
 * Module: SM_DestroyFile.c
 *
 * Description:
 *  SM_DestroyFile() destroys the specified file from the volume.
 *
 * Exports:
 *  Four SM_DestroyObject(FileID*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "OM_Internal.h"
#include "BtM.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * SM_DestroyFile()
 *================================*/
/*
 * Function: Four SM_DestroyObject(FileID*)
 *
 * Description:
 *  SM_DestroyFile() destroys the specified file from the volume.
 *  It also destroys the corresponding B+-tree file from the volume.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    eOPENFILE_SM
 *    some errors caused by function calls
 */
Four _SM_DestroyFile(
    Four handle,
    FileID                      *fid)                   /* IN file to destroy */
{
    Four                        e;                      /* error code */
    Four                        v;                      /* array index on scan manager mount table */
    Four                        i;                      /* temporary variable */
    Four                        idx;
    KeyValue                    kval;                   /* a key value */
    KeyValue                    kval2;                  /* another key value */
    BtreeCursor                 curBid;                 /* a B+ tree cursor */
    BtreeCursor                 nextBid;                /* a B+ tree cursor */
    ObjectID                    catalogEntry;           /* ObjectID of the catalog entry in SM_SYSTABLES */
    PhysicalFileID              pFid;                   
    sm_CatOverlayForSysTables   sysTablesOverlay;       /* catalog entry */ 
    sm_CatOverlayForSysIndexes  sysIndexesOverlay;      /* entry of SM_SYSINDEXES */
    

    TR_PRINT(TR_SM, TR1, ("SM_DestroyFile(handle, fid=%P)", fid));

    
    /*@ check parameter */
    if (fid == NULL) ERR(handle, eBADPARAMETER_SM);
    
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == fid->volNo) break; /* found */ 
    
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);


    /* Is there an active transactin? */
    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);
	    
    /* Get the ObjectID of the catalog entry in SM_SYSTABLES. */
    e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &catalogEntry);
    if (e < 0) ERR(handle, e);


    /*@ for each entry */
    /*
    ** Check if a scan is opened on the destroyed file.
    */
    for (i = 0; i < SM_PER_THREAD_DS(handle).smScanTable.nEntries; i++)
	if (SM_SCANTABLE(handle)[i].scanType != NIL &&
	    EQUAL_OBJECTID(SM_SCANTABLE(handle)[i].catalogEntry, catalogEntry))
	    ERR(handle, eOPENFILE_SM);

    
    /* 
    ** Read the catalog entry. 
    */
    e = OM_ReadObject(handle, &catalogEntry, 0, sizeof(sm_CatOverlayForSysTables), &sysTablesOverlay);
    if (e < 0) ERR(handle, e);

    
    /*
    ** Destroy the data file.
    */
    MAKE_PHYSICALFILEID(pFid, fid->volNo, sysTablesOverlay.data.firstPage);
    e = OM_DropFile(handle, &pFid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead)); 
    if (e < 0) ERR(handle, e);
    
	    
    /*
    ** Destroy a segment corresponding the B+-tree file.
    */
    MAKE_PHYSICALFILEID(pFid, fid->volNo, sysTablesOverlay.btree.firstPage);
    e = BtM_DropFile(handle, &pFid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead)); 
    if (e < 0) ERR(handle, e);


    /*
    ** Destroy all the corresponding entries from the catalog, SM_SYSINDEXES.
    */
    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(sysTablesOverlay.btree.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(sysTablesOverlay.btree.fid.serial), sizeof(Serial)); 

    e = BtM_Fetch(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesBtreeFileIdIndex),    
		  &SM_PER_THREAD_DS(handle).smSysIndexesIndexFileIdIndexKdesc,
		  &kval, SM_EQ, &kval, SM_EQ, &curBid); 
    if (e < 0) ERR(handle, e);

    while (curBid.flag != CURSOR_EOS) {

	/*@ Read the object. */
	e = OM_ReadObject(handle, &(curBid.oid), 0, REMAINDER, &sysIndexesOverlay);
	if (e < 0) ERR(handle, e);

	
	/* Destroy the object from the SM_SYSINDEXES. */
	e = OM_DestroyObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
			     &curBid.oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
	if (e < 0) ERR(handle, e);
	

	/* Delete the ObjectID from B+ tree on Index File FileID of SM_SYSINDEXES. */
	e = BtM_DeleteObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
			     &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesBtreeFileIdIndex),
			     &SM_PER_THREAD_DS(handle).smSysIndexesIndexFileIdIndexKdesc,
			     &kval, &curBid.oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead)); 
	if (e < 0) ERR(handle, e);
	

	/*@ construct kval2 */
	/* Delete the ObjectID from B+ tree on IndexID of SM_SYSINDEXES. */
        kval2.len = sizeof(VolNo) + sizeof(Serial);
        memcpy(&(kval2.val[0]), (char*)&(sysIndexesOverlay.iid.volNo), sizeof(VolNo));
        memcpy(&(kval2.val[sizeof(VolNo)]), (char*)&(sysIndexesOverlay.iid.serial), sizeof(Serial)); 

	e = BtM_DeleteObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
			     &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesIndexIdIndex),
			     &SM_PER_THREAD_DS(handle).smSysIndexesIndexIdIndexKdesc,
			     &kval2, &curBid.oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
	if (e < 0) ERR(handle, e);
	

	/* Fetch the ObjectID of the next object. */
	e = BtM_FetchNext(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesBtreeFileIdIndex), 
			  &SM_PER_THREAD_DS(handle).smSysIndexesIndexFileIdIndexKdesc,
			  &kval, SM_EQ, &curBid, &nextBid); 
	if (e < 0) ERR(handle, e);

	curBid = nextBid;
    }

    
    /*
    ** Destroy the corresponding entry from the catalog, SM_SYSTABLES.
    */    
    e = OM_DestroyObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry), 
			 &catalogEntry, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);


    /*
    ** Delete the ObjectID from the B+ tree on data FileID of SM_SYSTABLES.
    */
    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(fid->volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(fid->serial), sizeof(Serial)); 

    e = BtM_DeleteObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
			 &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesDataFileIdIndex),
			 &SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc,
			 &kval, &catalogEntry, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);


    /*
    ** Delete the ObjectID from the B+ tree on Btree FileID of SM_SYSTABLES.
    */
    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(sysTablesOverlay.btree.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(sysTablesOverlay.btree.fid.serial), sizeof(Serial)); 

    e = BtM_DeleteObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
			 &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesBtreeFileIdIndex),
			 &SM_PER_THREAD_DS(handle).smSysTablesBtreeFileIdIndexKdesc,
			 &kval, &catalogEntry, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);
    
    /* Remove fild ID from temporary file ID table if needed */ 
    /*@ for each entry */
    for (idx = 0; idx < SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries; idx++)
        if (EQUAL_FILEID(*fid, SM_TMPFILEIDTABLE(handle)[idx])) {
            SET_NILFILEID(SM_TMPFILEIDTABLE(handle)[idx]);
            break;
        }
    
    return(eNOERROR);
    
} /* SM_DestroyFile() */
