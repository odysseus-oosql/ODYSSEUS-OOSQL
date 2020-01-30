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
 * Module: SM_Savepoint.c
 *
 * Description:
 *  Close the given scan. The scan cannot be used any more.
 *
 * Exports:
 *  Four SM_SetSavepoint(Four, SavepointID *)
 *  Four SM_RollbackSavepoint(Four, SavepointID)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "RM.h"
#include "SM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_SetSavepoint( )
 *================================*/
/*
 * Function: Four SM_SetSavepoint(Four, SavepointID *)
 *
 * Description:
 *  Set Savepoint
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 */
Four SM_SetSavepoint(
    Four handle,
    SavepointID* spID)		/* OUT scan to close */
{
    Four         e;		/* error code */
    Four         i;		/* index variable */

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1, ("SM_SetSavepoint(SavepointID=%P)", spID));


    /* check any scan is opened */
    for (i = 0; i < sm_perThreadDSptr->smScanTable.nEntries; i++)
        if (SM_SCANTABLE(handle)[i].scanType != NIL) ERR(handle, eSCANOPENATSAVEPOINT);

    /* check any temporary file exists */
    /* Note!! temporary index cannot exist alone */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++ )
        if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i])) ERR(handle, eTMPFILEEXISTATSAVEPOINT_SM);

    /* get spID */
    e = RM_SetSavepoint(handle, MY_XACT_TABLE_ENTRY(handle), (Lsn_T *) spID);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SM_SetSavepoint() */



/*@================================
 * SM_RollbackSavepoint( )
 *================================*/
/*
 * Function: Four SM_RollbackSavepoint(Four, SavepointID)
 *
 * Description:
 *  Rollback to given savepoint
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 */
Four SM_RollbackSavepoint(
    Four handle,
    SavepointID  spID)		/* OUT scan to close */
{
    Four         e;		/* error code */
    Four         i;		/* index variable */


    TR_PRINT(handle, TR_SM, TR1, ("SM_RollbackSavepoint()"));


    /* close all scan */
    e = SM_CloseAllScan(handle);
    if (e < eNOERROR) ERR(handle, e);

    /* delete all entries from temporary file & index catalog */
    /* Note!! destroy of temporary file & index in disk is done during disk recovery */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++ ) {
        if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i])) {
            SM_SET_TO_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i]);
        }
    }
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(handle); i++) {
        if (!SM_IS_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[i])) {
            SM_SET_TO_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[i]);
        }
    }

    /* rollback disk state */
    e = RM_RollbackSavepoint(handle, MY_XACT_TABLE_ENTRY(handle), (Lsn_T *) &spID);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SM_RollbackSavepoint() */
