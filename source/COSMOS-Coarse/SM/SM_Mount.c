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
 * Module: SM_Mount.c
 *
 * Description:
 *  Mount a given volume using a RDsM function call, RDsM_Mount().
 *
 * Exports:
 *  Four SM_Mount(char*, Four*)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"	
#include "OM_Internal.h"	
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * SM_Mount()
 *================================*/
/*
 * Function: Four SM_Mount(char*, Four*)
 *
 * Description:
 *  Mount a given volume using a RDsM function call, RDsM_Mount().
 *  If the mount is successful, returns its volume id.
 *  A volume id is given to the volume when the volume is formated.
 *
 * Returns:
 *  error code (when negative value is returned)
 *    eBADPARAMETER_SM
 *    eTOOMANYVOLUMES_SM
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter volId
 *     returns the mounted volume's volume id
 *     If mount is failed, the value is undefined.
 */
Four _SM_Mount(
    Four handle,
    Four numDevices,            /* IN  number of devices */
    char **devNames,            /* IN volume to be mounted */
    Four *volId)                /* OUT mounted volume's volume id */
{
    Four    e;			/* error number */
    Four    v;			/* array index on the mount table */


    TR_PRINT(TR_SM, TR1, ("SM_Mount(handle, numDevices=%lD, devNames=%P, volId=%P)", numDevices, devNames, volId));

    
    /*@ Check parameter */
    if (numDevices <= 0|| devNames == NULL) ERR(handle, eBADPARAMETER_SM);
    if (volId == NULL) ERR(handle, eBADPARAMETER_SM);

    
    if (SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eEXISTACTIVETRANSACTION_SM);
    
    
    /*@
    ** Mount the volume.
    */
    e = RDsM_Mount(handle, numDevices, devNames, volId);
    if (e < 0) ERR(handle, e);

#ifdef USE_SHARED_MEMORY_BUFFER
    /* Get numbers of mounted volumes. */
    e = BfM_Mount(handle);
    if (e < 0) ERR(handle, e);
#endif
    
 
    /*
    ** Check whether the volume is already mounted.
    */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == *volId)
	    return(eNOERROR);	/* already mounted */

    
    /*
    ** Fill the smMountTable entry
    */
    /*@ for each entry */
    /* Find an empty slot in smMountTable */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == NIL) break; /* empty slot */

    if (v == MAXNUMOFVOLS) ERR(handle, eTOOMANYVOLUMES_SM);


    /*@
     * save the volume information
     */
    /*
    ** Fixed Information for the catalog table SM_SYSTABLES.
    */
    e = RDsM_GetMetaEntry(handle, *volId, "smSysTablesSysTablesEntry",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
			  sizeof(ObjectID));
    if (e < 0) ERR(handle, e);
    
    e = RDsM_GetMetaEntry(handle, *volId, "smSysTablesDataFileIdIndex",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesDataFileIdIndex),
			  sizeof(PhysicalIndexID));   
    if (e < 0) ERR(handle, e);
    
    e = RDsM_GetMetaEntry(handle, *volId, "smSysTablesBtreeFileIdIndex",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesBtreeFileIdIndex),
			  sizeof(PhysicalIndexID));   
    if (e < 0) ERR(handle, e);

    
    /*
    **Fixed information for the catalog table SM_SYSINDEXES.
    */
    e = RDsM_GetMetaEntry(handle, *volId, "smSysTablesSysIndexesEntry",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
			  sizeof(ObjectID));
    if (e < 0) ERR(handle, e);

    e = RDsM_GetMetaEntry(handle, *volId, "smSysIndexesBtreeFileIdIndex",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesBtreeFileIdIndex),
			  sizeof(PhysicalIndexID));   
    if (e < 0) ERR(handle, e);
    
    e = RDsM_GetMetaEntry(handle, *volId, "smSysIndexesIndexIdIndex",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesIndexIdIndex),
			  sizeof(PhysicalIndexID));   
    if (e < 0) ERR(handle, e);
    
    /*
    **Fixed information for the catalog table SM_SYSCOUNTERS.
    */
    e = RDsM_GetMetaEntry(handle, *volId, "smSysTablesSysCountersEntry",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysCountersEntry),
			  sizeof(ObjectID));
    if (e < 0) ERR(handle, e);

    e = RDsM_GetMetaEntry(handle, *volId, "smSysCountersCounterNameIndex",
			  &(SM_PER_THREAD_DS(handle).smMountTable[v].sysCountersCounterNameIndex),
			  sizeof(PhysicalIndexID));
    if (e < 0) ERR(handle, e);

    /* Initialize the deallocatedPageList Header. */
    SM_PER_THREAD_DS(handle).smMountTable[v].dlHead.next = NULL;
    
    /* Set the volume identifier. */
    SM_PER_THREAD_DS(handle).smMountTable[v].volId = *volId;

    return(eNOERROR);
} /* SM_Mount() */
