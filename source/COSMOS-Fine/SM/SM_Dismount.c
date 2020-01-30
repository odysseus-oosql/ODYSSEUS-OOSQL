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
 * Module: SM_Dismount.c
 *
 * Description:
 *  Dismount the used volume.
 *
 * Exports:
 *  Four SM_Dismount(Four, Four)
 */


#include <unistd.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "RDsM.h"
#include "BfM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_Dismount( )
 *================================*/
/*
 * Function: Four SM_Dismount(Four, Four)
 *
 * Description:
 *  Dismount the used volume.
 *
 * Returns:
 *   Error code
 *     eBADPARAMETER
 *     eNOTMOUNTEDVOLUME_SM
 *     some errors caused by function calls
 */
Four SM_Dismount(
    Four handle,
    Four volId)			/* IN volume to dismount */
{
    Four e;			/* error number */
    Four v;			/* array index on the mount table */
    Four i;			/* temporary variable */

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1, ("SM_Dismount(volId=%P)", volId));

    /*@ check a parameter */
    if (volId < 0) ERR(handle, eBADPARAMETER);

    if (MY_XACT_TABLE_ENTRY(handle) != NULL) ERR(handle, eVOLUMEDISMOUNTDISALLOWED_SM); 


    /* get the latch on the mount table
       to support exclusive mount/dismount operation */
    ERROR_PASS(handle, SHM_getLatch(handle, &SM_LATCH_MOUNTTABLE, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == volId) break; /* found */

    if (v == MAXNUMOFVOLS)
        ERRL1(handle, eNOTMOUNTEDVOLUME_SM, &SM_LATCH_MOUNTTABLE);


    /*@ for each entry */
    /* Close the scans using this volume. */
    for (i = 0; i < sm_perThreadDSptr->smScanTable.nEntries; i++)
	if (SM_SCANTABLE(handle)[i].scanType != NIL &&
	    SM_SCANTABLE(handle)[i].finfo.fid.volNo == volId) {

	    /* Close this scan. */
	    SM_SCANTABLE(handle)[i].scanType = NIL;
	}

    SM_MOUNTTABLE[v].nMount--;  /* assume as Atomic Operation */

    if ( SM_MOUNTTABLE[v].nMount > 0) {
	/* another process still use this device */
	ERROR_PASS(handle, SHM_releaseLatch(handle, &SM_LATCH_MOUNTTABLE, procIndex));
	return(eNOERROR);
    }
    else if(SM_MOUNTTABLE[v].nMount < 0){
	/* this case can not happen */
	ERRL1(handle, eINVALIDMOUNTCOUNTER_SM, &SM_LATCH_MOUNTTABLE);
    }

    /* Delete the corresponding entry from the mount table. */
    SM_MOUNTTABLE[v].volId = NIL;

    /* Force out the dirty pages. */
    e = BfM_dismount(handle, volId);
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /*@ Dismount the volume. */
    e = RDsM_Dismount(handle, volId, common_shmPtr->recoveryFlag);
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    ERROR_PASS(handle, SHM_releaseLatch(handle, &SM_LATCH_MOUNTTABLE, procIndex));

    return(eNOERROR);

} /* SM_Dismount( ) */

