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
 * Module: SM_UpdateObjectWithoutScan.c
 *
 * Description:
 *  Update the given object's content.
 *
 * Exports:
 *  Four SM_UpdateObjectWithoutScan(FileID*, ObjectID*, Four, Four, void*, Four)
 */


#include "common.h"
#include "trace.h"
#include "OM_Internal.h"	
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * SM_UpdateObjectWithoutScan()
 *================================*/
/*
 * Function: Four  SM_UpdateObjectWithoutScan(FileID*, ObjectID*, Four, Four, void*, Four)
 *
 * Description:
 *  Update the given object's content.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    some errors caused by function calls
 */
Four _SM_UpdateObjectWithoutScan(
    Four handle,
    FileID    *fid,             /* IN file where the updated object exist */
    ObjectID *oid,		/* IN object to update */
    Four     start,		/* IN start position of update */
    Four     length,		/* IN amount of data to be updated */
    void     *data,		/* IN new data with which old data is overwritten */
    Four     dataLen)		/* IN amount of new data */
{
    Four      e;		/* error code */
    Four      v;		/* index for scan manager mount table */
    Four      min;		/* minimum of length & newLen */
    ObjectHdr objHdr;		/* object header */
    ObjectID catalogEntry;


    TR_PRINT(TR_SM, TR1,
             ("SM_UpdateObjectWithoutScan(handle, fid=%P, oid=%P, start=%ld, length=%ld, data=%P, dataLen=%ld)",
	      fid, oid, start, length, data, dataLen));

    /*@
     * Check parameters.
     */
    if (fid == NULL || oid == NULL) ERR(handle, eBADPARAMETER_SM);

    if (start != END && start < 0) ERR(handle, eBADPARAMETER_SM);

    if (length != REMAINDER && length < 0) ERR(handle, eBADPARAMETER_SM);

    if (dataLen != 0 && data == NULL) ERR(handle, eBADPARAMETER_SM);

    if (dataLen < 0) ERR(handle, eBADPARAMETER_SM);


    /* Is there an active transaction? */
    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);
        
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == fid->volNo) break; /* found */
    
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* Get catalog entry of */
    e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &catalogEntry);
    if (e < 0) ERR(handle, e);
    
    /*@ Update the given object. */
    if (dataLen == 0) {
	/* Case 1: delete operation */

	if (length == 0) return(eNOERROR);
	
	e = OM_DeleteFromObject(handle, &catalogEntry, oid, start, length, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
	if (e < 0) ERR(handle, e);
	
    } else if (length == 0) {
	/* Case 2: Add some data. */

	if (start == END) {
	    /* append */
	    
	    e = OM_AppendToObject(handle, &catalogEntry, oid,
				  dataLen, data, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
	    if (e < 0) ERR(handle, e);

	} else {
	    /* insertion */
	    
	    e = OM_InsertInObject(handle, &catalogEntry, oid,
				  start, dataLen, data, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
	    if (e < 0) ERR(handle, e);
	}

    } else {
	/* Case 3: replace old data with new data */

	/* If 'start' is END, then 'length' should be 0; it is case 2. */
	if (start == END) ERR(handle, eBADPARAMETER_SM);
	
	if (length == REMAINDER) {
	    e = OM_GetObjectHdr(handle, oid, &objHdr);
	    if (e < 0) ERR(handle, e);

	    length = objHdr.length - start;
	}

	min = MIN(length, dataLen);

	/* Write the new data on the intersection. */
	e = OM_WriteObject(handle, oid, start, min, data);
	if (e < 0) ERR(handle, e);
	
	if (length > dataLen) {
	    /* old data's length > new data's length */

	    e = OM_DeleteFromObject(handle, &catalogEntry, oid, start + min, length - min, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
	    if (e < 0) ERR(handle, e);
	    
	} else if (length < dataLen) {
	    /* The remainder of new data is added. */
	    
	    e = OM_InsertInObject(handle, &catalogEntry, oid, start + min, dataLen - min,
				  (char *)data + min, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
	    if (e < 0) ERR(handle, e);
	}
    }
    
    return(eNOERROR);

} /* SM_UpdateObjectWithoutScan() */
    
    
    
 
