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
 * Module: LRDS_Savepoint.c
 *
 * Description:
 *  Close the given scan. The scan cannot be used any more.
 *
 * Exports:
 *  Four LRDS_SetSavepoint(SavepointID *)
 *  Four LRDS_RollbackSavepoint(SavepointID)
 */


#include <stdlib.h>
#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * LRDS_SetSavepoint()
 *================================*/
/*
 * Function: Four LRDS_SetSavepoint(SavepointID *)
 *
 * Description:
 *  Set Savepoint
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 */
Four LRDS_SetSavepoint(
    Four         handle,
    SavepointID* spID)		/* OUT ID of new savepoint */
{
    Four         e;		/* error code */
    Four         i;		/* index variable */

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_SM, TR1, ("LRDS_SetSavepoint(SavepointID=%P)", spID));


    /* check any scan is opened */
    for (i = 0; i < lrds_perThreadDSptr->lrdsScanTable.nEntries; i++)
        if (LRDS_SCANTABLE(handle)[i].orn != NIL) ERR(handle, eSCANOPENATSAVEPOINT);

    /* check any relation is opened */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE; i++)
        if (!LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, i)) ERR(handle, eRELATIONOPENATSAVEPOINT_LRDS);

    /* check any temporary relation exists */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; i++)
        if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i)) ERR(handle, eTMPRELEXISTATSAVEPOINT_LRDS);

    /* call SM function to set savepoint */
    e = SM_SetSavepoint(handle, spID);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_SetSavepoint() */



/*@================================
 * LRDS_RollbackSavepoint()
 *================================*/
/*
 * Function: Four LRDS_RollbackSavepoint(SavepointID)
 *
 * Description:
 *  Rollback to given savepoint
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 */
Four LRDS_RollbackSavepoint(
    Four         handle,
    SavepointID  spID)		       /* OUT ID of savepoint */
{
    Four         e;		       /* error code */
    Four         i;                    /* index variable */
    Four         colNo;                /* column number */
    ColDesc      *relTableEntry_cdesc; 


    TR_PRINT(handle, TR_SM, TR1, ("LRDS_RollbackSavepoint()"));


    /* close all scan */
    e = LRDS_CloseAllScans(handle);
    if (e < eNOERROR) ERR(handle, e);

    /* close all relations */
    e = LRDS_CloseAllRelations(handle);
    if (e < eNOERROR) ERR(handle, e);

    /* delete all entries from temporary file & index catalog */
    /* Note!! destroy of temporary file & index in disk is done during disk recovery */
    for (i = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE_FOR_TMP_RELS; i++) {
        if (!LRDS_IS_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i)) {

            relTableEntry_cdesc = PHYSICAL_PTR(LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].cdesc); 
            /* Note!! nColumns is always greater than 0 */
            for (colNo = 0; colNo < LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ri.nColumns; colNo++) {
                if (PHYSICAL_PTR(relTableEntry_cdesc[colNo].auxInfo) != NULL) { 

                    /* assertion check */
                    assert(relTableEntry_cdesc[colNo].complexType == SM_COMPLEXTYPE_ORDEREDSET);

                    /* free the dynamically allocated pool */
                    e = Util_freeElementToLocalPool(handle, &LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL(handle),
                                                    PHYSICAL_PTR(relTableEntry_cdesc[colNo].auxInfo)); 
                    if (e < eNOERROR) ERR(handle, e);
                }
            }

            /* free the dynamically allocated memory */
            free(PHYSICAL_PTR(LRDS_RELTABLE_FOR_TMP_RELS(handle)[i].ii));
            free(relTableEntry_cdesc);

            /* Mark this entry to unused. */
            LRDS_SET_TO_UNUSED_ENTRY_OF_RELTABLE_FOR_TMP_RELS(handle, i);
        }
    }

    /* rollback disk state */
    e = SM_RollbackSavepoint(handle, spID);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LRDS_RollbackSavepoint() */
