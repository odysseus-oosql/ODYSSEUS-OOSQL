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
 * Module: SM_Transaction.c
 *
 * Description:
 *  Contains transaction related functions.
 *
 * Exports:
 *  Four SM_BeginTransaction( )
 *  Four SM_AbortTransaction( )
 *  Four SM_CommitTransaction( )
 */


#include "common.h"
#include "trace.h"
#include "error.h"
#include "TM.h"
#include "SM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: SM_BeginTransaction( )
 *
 * Description:
 *  Begin a transaction.
 *
 * Returns:
 *  error code
 */
Four SM_BeginTransaction(
    Four handle,
    XactID *xactId,		/* OUT transaction id of the newly started transaction */
    ConcurrencyLevel ccLevel)   /* IN concurrency level of this transaction */ 
{
    Four e;                     /* error code */

    TR_PRINT(handle, TR_SM, TR1, ("SM_BeginTransaction()"));

    e = TM_BeginTransaction(handle, xactId, ccLevel); 
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_BeginTransaction( ) */


/*
 * Function: SM_CommitTransaction( )
 *
 * Description:
 *  Commit a transaction.
 *
 * Returns:
 *  error code
 */
Four SM_CommitTransaction(
    Four handle,
    XactID *xactId)             /* IN transaction to commit */
{
    Four e;                     /* error code */
    Four i;

    TR_PRINT(handle, TR_SM, TR1, ("SM_CommitTransaction()"));

    /*
    ** Finalize the data structure for temporary files.
    */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) { 
	if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i])) {
	    /* undestroyed file: destroy it */
	    /* SM_DestroyFile( ) also drop all the indexes defined on the given file. */

	    e = SM_DestroyFile(handle, &(SM_ST_FOR_TMP_FILES(handle)[i].data.fid), NULL);
	    if (e < eNOERROR) ERR(handle, e);
	}
    }

    e = TM_CommitTransaction(handle, xactId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_CommitTransaction( ) */



/*
 * Function: SM_AbortTransaction( )
 *
 * Description:
 *  Abort a transaction.
 *
 * Returns:
 *  error code
 */
Four SM_AbortTransaction(
    Four handle,
    XactID *xactId)             /* IN transaction to abort */
{
    Four e;                     /* error code */
    Four i;

    TR_PRINT(handle, TR_SM, TR1, ("SM_AbortTransaction()"));

    /*
    ** Finalize the data structure for temporary files.
    */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) { 
	if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i])) {
	    /* undestroyed file: destroy it */
	    /* SM_DestroyFile( ) also drop all the indexes defined on the given file. */

	    e = SM_DestroyFile(handle, &(SM_ST_FOR_TMP_FILES(handle)[i].data.fid), NULL);
	    if (e < eNOERROR) ERR(handle, e);
	}
    }

    e = TM_AbortTransaction(handle, xactId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_AbortTransaction() */



/*
 * Function: SM_EnterTwoPhaseCommit()
 *
 * Description:
 *  Indicate to the storage manager that this transaction is a member
 *  of a global transaction.
 *
 * Returns:
 *  error code
 */
Four SM_EnterTwoPhaseCommit(
    Four   handle,
    XactID *xactId,             /* IN transaction to enter 2pc */
    GlobalXactID *globalXactId) /* IN global transaction id */
{
    Four e;                     /* error code */

    TR_PRINT(handle, TR_SM, TR1, ("SM_EnterTwoPhaseCommit()"));


    e = TM_EnterTwoPhaseCommit(handle, xactId, globalXactId);
    if (e < eNOERROR) ERR(handle, e);

    /* global transaction ID is duplicated */ 
    if ( e == eDUPLICATEDGTID_TM ) return (eDUPLICATEDGTID_SM);

    return(eNOERROR);

} /* SM_EnterTwoPhaseCommit() */



/*
 * Function: SM_PrepareTransaction()
 *
 * Description:
 * Prepare transaction.
 *
 * Returns:
 *  error code
 */
Four SM_PrepareTransaction(
    Four   handle,
    XactID *xactId)             /* IN transaction to enter 2pc */
{
    Four e;                     /* error code */

    TR_PRINT(handle, TR_SM, TR1, ("SM_PrepareTransaction()"));


    e = TM_PrepareTransaction(handle, xactId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_PrepareTransaction() */


/*
 * Function: SM_GetNumberOfPreparedTransactions()
 *
 * Description:
 *  Get the number of prepared transactions.
 *
 * Returns:
 *  error code
 */
Four SM_GetNumberOfPreparedTransactions(
    Four handle,
    Four *num)             /* OUT number of prepared transactions */
{
    Four e;                     /* error code */

    TR_PRINT(handle, TR_SM, TR1, ("SM_GetNumberOfPreparedTransactions()"));


    e = TM_XT_GetPreparedTransactions(handle, 0, NULL, num);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_GetNumberOfPreparedTransactions() */



/*
 * Function: SM_GetPreparedTransactions()
 *
 * Description:
 *  Get the list of prepared transactions.
 *
 * Returns:
 *  error code
 */
Four SM_GetPreparedTransactions(
    Four handle,
    Four num,                   /* IN size of reserved space */
    GlobalXactID preparedXacts[]) /* OUT list of prepared transactions */

{
    Four e;                     /* error code */

    TR_PRINT(handle, TR_SM, TR1, ("SM_GetPreparedTransactions()"));


    e = TM_XT_GetPreparedTransactions(handle, num, preparedXacts, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_GetPreparedTransactions() */



/*
 * Function: SM_RecoverTwoPhaseCommit()
 *
 * Description:
 *  Associate the given transaction.
 *
 * Returns:
 *  error code
 */
Four SM_RecoverTwoPhaseCommit(
    Four         handle,
    GlobalXactID *globalXactId, /* IN global transaction id */
    XactID *xactId)             /* OUT local transaction id */
{
    Four e;                     /* error code */

    TR_PRINT(handle, TR_SM, TR1, ("SM_RecoverTwoPhaseCommit()"));


    e = TM_RecoverTwoPhaseCommit(handle, globalXactId, xactId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_RecoverTwoPhaseCommit() */



/*
 * Function: SM_IsReadOnlyTransaction()
 *
 * Description:
 *  Inquiry whether the transaction is read-only.
 *
 * Returns:
 *  error code
 */
Four SM_IsReadOnlyTransaction(
    Four   handle,
    XactID *xactId,             /* IN local transaction id */
    Boolean *flag)              /* OUT TRUE if read-only; FALSE otherwise */
{
    Four e;                     /* error code */

    TR_PRINT(handle, TR_SM, TR1, ("SM_IsReadOnlyTransaction(xactId=%P, flag=%P)", xactId, flag));


    e = TM_IsReadOnlyTransaction(handle, xactId, flag);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_IsReadOnlyTransaction() */


