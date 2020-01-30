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
 * Module: TM_EnterTwoPhaseCommit.c
 *
 * Description:
 *  Indicate to the storage manager that this transaction is a member
 *  of a global transaction.
 *
 * Exports:
 *  Four TM_EnterTwoPhaseCommit(XactID*, GlobalXactID*)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "SHM.h"  
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four TM_EnterTwoPhaseCommit(XactID*, GlobalXactID*)
 *
 * Description:
 *  Indicate to the storage manager that this transaction is a member
 *  of a global transaction.
 *
 * Returns:
 *  error code
 *    eNOERROR
 */
Four TM_EnterTwoPhaseCommit(
    Four   		handle,
    XactID 		*xactId,             	/* IN transaction to enter 2pc */
    GlobalXactID 	*globalXactId) 		/* IN global transaction id */
{
    Four 		e;			/* error code */
    XactTableEntry_T 	*xactEntry;
    Boolean 		flag; 


    TR_PRINT(handle, TR_TM, TR1, ("TM_EnterTwoPhaseCommit(xactId=%P, globalXactId=%P)", xactId, globalXactId));


    /* check parameters */
    if (MY_XACT_TABLE_ENTRY(handle) == NULL || !EQUAL_XACTID(*xactId, MY_XACTID(handle))) ERR(handle, eWRONGXACTID_TM);

    xactEntry = MY_XACT_TABLE_ENTRY(handle);

    assert(xactEntry->status == X_NORMAL);


    /* acquire the latch for the transaction table */
    e = SHM_getLatch(handle, &(TM_XACTTBL.latch), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*@
     * Check if the transaction had entered 2pc.
     */
    if (xactEntry->globalXactId != NULL) ERR(handle, e2PCTRANSACTION_TM);

    /*
     * Check if the global transaction id is duplicated.
     */

    e = TM_IsDuplicatedGXID(handle, globalXactId, &flag);
    if (e < eNOERROR) ERR(handle, e);

    if (flag == TRUE) {

        /* release latch */
        e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
        if (e < eNOERROR) ERR(handle, e);

        return (eDUPLICATEDGTID_TM);
    }

    /*
     * Set the global transaction id.
     */
    e = Util_getElementFromPool(handle, &TM_GLOBALXACTIDPOOL, &xactEntry->globalXactId);
    if (e < eNOERROR) ERR(handle, e);

    memcpy(xactEntry->globalXactId, globalXactId, sizeof(GlobalXactID));

    /* release latch */
    e = SHM_releaseLatch(handle, &(TM_XACTTBL.latch), procIndex);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* TM_EnterTwoPhaseCommit() */
