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
 * Module: SM_OpenIndexScan.c
 *
 * Description:
 *  Open a scan using a B+ tree index; thus its scan type is BTREEINDEX.
 *
 * Exports:
 *  Four SM_OpenIndexScan(FileID*, IndexID*, KeyDesc*, BoundCond*, BoundCond*)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "BtM.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * SM_OpenSeqScan()
 *================================*/
/*
 * Function: Four SM_OpenIndexScan(FileID*, IndexID*, KeyDesc*, BoundCond*, BoundCond*)
 *
 * Description:
 *  Open a scan using a B+ tree index; thus its scan type is BTREEINDEX.
 *  User can define a region of objects so that one may fetch objects in
 *  the region. The 'startBound' is the start boundary condition of region and
 *  'stopBound' the stop boundary condition of the region. This is same with
 *  the sequential scan except it use an index.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    eNOTMOUNTEDVOLUME_SM
 *    some errors caused by function calls
 */
Four _SM_OpenIndexScan(
    Four handle,
    FileID *fid,		/* IN data file to open */
    IndexID *iid,		/* IN index to use */
    KeyDesc *kdesc,		/* IN key descriptor of the given index */
    BoundCond *startCond,	/* IN start boundary condition of a region */
    BoundCond *stopCond)	/* IN stop boundary condition of a region */
{
    Four e;			/* error code */
    Four v;			/* index for the used volume on the mount table */
    Four i;			/* temporay variable */
    Four scanId;		/* scan identifier of the new scan */
    PhysicalIndexID pIid;	


    TR_PRINT(TR_SM, TR1,
             ("SM_OpenIndexScan(handle, fid=%P, iid=%P, kdesc=%P, startCond=%P, stopCond=%P)",
	      fid, iid, kdesc, startCond, stopCond));


    /*@ check parameters */
    if (fid == NULL) ERR(handle, eBADPARAMETER_SM);
    
    if (iid == NULL) ERR(handle, eBADPARAMETER_SM);

    if (kdesc == NULL) ERR(handle, eBADPARAMETER_SM);
    
    if (startCond == NULL) ERR(handle, eBADPARAMETER_SM);

    if (stopCond == NULL) ERR(handle, eBADPARAMETER_SM);    

    
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == fid->volNo) break; /* found */
    
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /*@ for each entry */
    /* find the empty scan table entry */
    for (scanId = 0; scanId < SM_PER_THREAD_DS(handle).smScanTable.nEntries; scanId++)
	if (SM_SCANTABLE(handle)[scanId].scanType == NIL) break;

    if (scanId == SM_PER_THREAD_DS(handle).smScanTable.nEntries) {
	/* There is no empty entry. */

	e = Util_doublesizeVarArray(handle, &SM_PER_THREAD_DS(handle).smScanTable, sizeof(sm_ScanTableEntry));
	if (e < 0) ERR(handle, e);

	/* Initialize the newly allocated entries. */
	for (i = scanId; i < SM_PER_THREAD_DS(handle).smScanTable.nEntries; i++)
	    SM_SCANTABLE(handle)[i].scanType = NIL;
    }

    /* Get the ObjectID of the catlog entry in SM_SYSTABLES, and get pIid also. */
    e = sm_GetCatalogEntryFromIndexId(handle, v, iid, &(SM_SCANTABLE(handle)[scanId].catalogEntry), &pIid, NULL);
    if (e < 0) ERR(handle, e);

    /* Save the index information */
    SM_SCANTABLE(handle)[scanId].scanInfo.btree.iid = *iid;   
    SM_SCANTABLE(handle)[scanId].scanInfo.btree.pIid = pIid;   
    SM_SCANTABLE(handle)[scanId].scanInfo.btree.kdesc = *kdesc;

    /* Save the region conditions. */
    SM_SCANTABLE(handle)[scanId].scanInfo.btree.startCond = *startCond;
    SM_SCANTABLE(handle)[scanId].scanInfo.btree.stopCond = *stopCond;

    SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_BOS;
    SM_SCANTABLE(handle)[scanId].scanType = BTREEINDEX;
    
    return(scanId);

} /* SM_OpenIndexScan() */
