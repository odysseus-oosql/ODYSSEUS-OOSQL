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
#include <assert.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"
#include "BfM.h"
#include "RM.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*
 * Internal Functions 
 */
Four _sm_PreliminariesOfPrepareAndCoomit(Four);

/*
 * Function: Four SM_BeginTransaction(Boolean )
 *
 * Description:
 *  Begin a new transaction.
 *
 * Returns:
 *  error code
 */
Four _SM_BeginTransaction(
    Four handle,
    Boolean rollbackRequiredFlag) /* IN support rollback if it has TRUE */
{
    Four e;                     /* error code */

    
    TR_PRINT(TR_SM, TR1, ("SM_BeginTransaction(handle)"));

    
    /* Is there active transaction? */
    if (SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eEXISTACTIVETRANSACTION_SM);

    /*
     * Inform the recovery manager that a new transaction starts.
     */
    e = RM_BeginTransaction(handle, rollbackRequiredFlag);
    if (e < eNOERROR) ERR(handle, e);
    
    /* mark that a transaction is started */
    SM_PER_THREAD_DS(handle).xactRunningFlag = TRUE;
    
    return (eNOERROR);
    
} /* SM_BeginTransaction() */



/*
 * Function: Four SM_CommitTransaction(void)
 *
 * Description:
 *  Commit the currently running transaction.
 *
 * Returns:
 *  error code
 */
Four _SM_CommitTransaction(Four handle)
{
    Four e;                     /* error code */
    Four v;			/* array index on the mount table */
    Four firstExtNo;		/* first Extent No of a file */
    DeallocListElem *dlElem;	/* pointer to the dealloc list element */
    DeallocListElem *prevdlElem; /* pointer to the previous element */
    DeallocListElem *nextdlElem; /* pointer to the next element */
    Boolean flag;                /* flag which indicates that recover is done for two phase commit */

    
    TR_PRINT(TR_SM, TR1, ("SM_CommitTransaction(handle)"));


    if (SM_PER_THREAD_DS(handle).xactRunningFlag == FALSE) {
	e = RM_RestartTwoPhaseCommit(handle, RM_COMMIT, &flag);
	if (e < eNOERROR) ERR(handle, e);

	if (flag == TRUE) return (eNOERROR);
    }
    
    /* Is there an active transactin? */
    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);

    if (SM_PER_THREAD_DS(handle).globalXactRunningFlag == TRUE) {
	
	if (RM_PER_THREAD_DS(handle).rm_onPrepareFlag == FALSE) ERR(handle, eNOACTIVEPREPAREDTRANSACTION_SM);

    } else {
	    
        /*
         * To Next Operation
         *
         * 1. close all the scans
         * 2. destroy temporary files
         * 3. deallocate pages in the dealloc page list.
         * 4. flush the buffer
         * 5. do BfM_UpdateCoherencyVolume, if use Coherency volume
         * 6. log the list of mounted volumes
         */
        e = _sm_PreliminariesOfPrepareAndCoomit(handle); 
        if (e < eNOERROR) ERR(handle, e);
    }    


    /*
     * Commit the operations executed in the transaction.
     */
    e = RM_CommitTransaction(handle);
    if (e < eNOERROR) ERR(handle, e);
    
    /* There is no running transaction. */
    SM_PER_THREAD_DS(handle).xactRunningFlag = FALSE;

    if (SM_PER_THREAD_DS(handle).globalXactRunningFlag == TRUE) SM_PER_THREAD_DS(handle).globalXactRunningFlag = FALSE;

    return (eNOERROR);
    
} /* SM_CommitTransaction() */


/*
 * Function: Four SM_AbortTransaction(void)
 *
 * Description:
 *  Abort the currently running transaction.
 *
 * Returns:
 *  error code
 */
Four _SM_AbortTransaction(Four handle)
{
    Four            e;                  /* error code */
    Four            v;                  /* array index on the mount table */
    Four            firstExtNo;         /* first Extent No of a file */
    DeallocListElem *dlElem;            /* pointer to the dealloc list element */
    DeallocListElem *nextdlElem;        /* pointer to the next element */
    Four            i;
    Four            idx;
    Boolean         flag;               /* flag which indicates that recover is done for two phase commit */

    
    TR_PRINT(TR_SM, TR1, ("SM_AbortTransaction(handle)"));


    if (SM_PER_THREAD_DS(handle).xactRunningFlag == FALSE) {
	e = RM_RestartTwoPhaseCommit(handle, RM_ABORT, &flag);
	if (e < eNOERROR) ERR(handle, e);

	if (flag == TRUE) return (eNOERROR);
    }


    /* Is there an active transactin? */
    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);

    
    /* Close all the scans. */
    /*@ for each entry */
    for (i = 0; i < SM_PER_THREAD_DS(handle).smScanTable.nEntries; i++)
	if (SM_SCANTABLE(handle)[i].scanType != NIL) {

	    /* Close this scan. */
	    e = _SM_CloseScan(handle, i); 
	    if (e < eNOERROR) ERR(handle, e);
	}

    /* Destroy all temporary files */ 
    /*@ for each entry */
    for (idx = 0; idx < SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries; idx++)
        if (!IS_NILFILEID(SM_TMPFILEIDTABLE(handle)[idx])) {
	    /* Note!! By recovery mechanism, temporary file is automatically destroyed */
	    /* So, we only clean up temporary file table */
            SET_NILFILEID(SM_TMPFILEIDTABLE(handle)[idx]);
	}
    
    /* Ignore the deallocatedPageList and free the elements in the list. */
    for (v = 0; v < MAXNUMOFVOLS; v++) {
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == NIL) continue; /* not used */
        
        dlElem = SM_PER_THREAD_DS(handle).smMountTable[v].dlHead.next;
        
        /*@ loop if dlElem is not NULL */
        while (dlElem != NULL) {

            /* Point to the next element of the list. */
            nextdlElem = dlElem->next;
	
            /* Delete the current element from the list. */
            e = Util_freeElementToPool(handle, &SM_PER_THREAD_DS(handle).dlPool, dlElem);
            if (e < 0) ERR(handle, e);
            
            dlElem = nextdlElem;
        }

        SM_PER_THREAD_DS(handle).smMountTable[v].dlHead.next = NULL;
    }

    /* Discard all buffer contents. */
    e = BfM_DiscardAll(handle);
    if (e < eNOERROR) ERR(handle, e);

#ifdef USE_COHERENCY_VOLUME
    e = BfM_UpdateCoherencyVolume(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    SM_PER_THREAD_DS(handle).xactRunningFlag = FALSE;

    /* Abort the transaction. */
    e = RM_AbortTransaction(handle);
    if (e < eNOERROR) ERR(handle, e);
        
    if (SM_PER_THREAD_DS(handle).globalXactRunningFlag == TRUE) SM_PER_THREAD_DS(handle).globalXactRunningFlag = FALSE;


    /* bluechry test ... */
    printf("### [pID=%d, tID=%d] Transaction Abort.\n", procIndex, handle);
    fflush(stdout);	
    /* ... bluechry test */

    return(eNOERROR);
    
} /* SM_AbortTransaction() */

Four _SM_EnterTwoPhaseCommit(
    Four handle,
    XactID* 		xactId,
    GlobalXactID* 	globalXactId
)
{
    Four		e;

    /* check that there exists global transaction */
    if (SM_PER_THREAD_DS(handle).globalXactRunningFlag == TRUE) return (eACTIVEGLOBALTRANSACTION_SM);

    /* enter two phase commit */
    e = RM_EnterTwoPhaseCommit(handle, xactId, globalXactId);
    if (e < eNOERROR) ERR(handle, e);

    /* set 'globalXactRunningFlag' */
    SM_PER_THREAD_DS(handle).globalXactRunningFlag = TRUE;


    return (eNOERROR);
}

Four _SM_GetNumberOfPreparedTransactions(
    Four handle,
    Four* num)
{

    return (RM_GetNumberOfPreparedTransactions(handle, num));
}

Four _SM_GetPreparedTransactions(
    Four handle,
    Four num,
    GlobalXactID globalXactId[])
{

    return (RM_GetPreparedTransactions(handle, num, globalXactId));
}

Four _SM_PrepareTransaction(
    Four handle,
    XactID* xactId)
{

    Four e;

    /* Is there an active transactin? */
    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);

    /* check global transaction */
    if (SM_PER_THREAD_DS(handle).globalXactRunningFlag != TRUE) return (eNOACTIVEGLOBALTRANSACTION_SM);

    /*
     * To Next Operation
     *
     * 1. close all the scans
     * 2. destroy temporary files
     * 3. deallocate pages in the dealloc page list.
     * 4. flush the buffer
     * 5. do BfM_UpdateCoherencyVolume, if use Coherency volume
     * 6. log the list of mounted volumes
     */
    e = _sm_PreliminariesOfPrepareAndCoomit(handle);
    if (e < eNOERROR) ERR(handle, e);


    return (RM_PrepareTransaction(handle, xactId));

}

Four _SM_RecoverTwoPhaseCommit(
    Four handle,
    GlobalXactID* globalXactId,
    XactID* xactId)
{

    return (RM_RecoverTwoPhaseCommit(handle, globalXactId, xactId));

}


Four _SM_IsReadOnlyTransaction(
    Four handle,
    XactID* xactId,
    Boolean* flag)
{

    return (RM_IsReadOnlyTransaction(handle, xactId, flag));

}


/*
 * Function: Four _sm_PreliminariesOfPrepareAndCoomit(void)
 *
 * Description:
 *   To Next Operation
 * 	1. close all the scans
 * 	2. destroy temporary files
 * 	3. deallocate pages in the dealloc page list.
 * 	4. flush the buffer
 * 	5. do BfM_UpdateCoherencyVolume, if use Coherency volume
 * 	6. log the list of mounted volumes
 *
 * Returns:
 *  error code
 */
Four _sm_PreliminariesOfPrepareAndCoomit(Four handle)
{
    Four            e;                  /* error code */
    Four            v;                  /* array index on the mount table */
    Four            firstExtNo;         /* first Extent No of a file */
    DeallocListElem *dlElem;            /* pointer to the dealloc list element */
    DeallocListElem *prevdlElem;        /* pointer to the previous element */
    DeallocListElem *nextdlElem;        /* pointer to the next element */
    Four            i;
    Four            idx;


    /* Close all the scans. */
    /*@ for each entry */
    for (i = 0; i < SM_PER_THREAD_DS(handle).smScanTable.nEntries; i++)
        if (SM_SCANTABLE(handle)[i].scanType != NIL) {

            /* Close this scan. */
            e = _SM_CloseScan(handle, i); 
            if (e < eNOERROR) ERR(handle, e);
        }

    /* Destroy all temporary files */
    /*@ for each entry */
    for (idx = 0; idx < SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries; idx++)
        if (!IS_NILFILEID(SM_TMPFILEIDTABLE(handle)[idx])) {
            e = _SM_DestroyFile(handle, &SM_TMPFILEIDTABLE(handle)[idx]);
            if (e < 0) ERR(handle, e);
        }

    /* Actually deallocate pages in the dealloc list. */
    for (v = 0; v < MAXNUMOFVOLS; v++) {
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == NIL) continue; /* not used */

        /*
         * free pages/trains
         */
        dlElem = SM_PER_THREAD_DS(handle).smMountTable[v].dlHead.next;
        prevdlElem = &SM_PER_THREAD_DS(handle).smMountTable[v].dlHead;

        /*@ loop if dlElem is not NULL */
        while (dlElem != NULL) {

            /* Point to the next element of the list. */
            nextdlElem = dlElem->next;

            if (dlElem->type != DL_FILE) {
                if (dlElem->type == DL_PAGE) {
                    /* Deallocate a page. */
                    e = RDsM_FreeTrain(handle, &(dlElem->elem.pid), PAGESIZE2);
                    if (e < 0) ERR(handle, e);

                } else if (dlElem->type == DL_TRAIN) {
                    /* Deallocate a page. */
                    e = RDsM_FreeTrain(handle, &(dlElem->elem.pid), TRAINSIZE2);
                    if (e < 0) ERR(handle, e);
                }

                /* Delete the current element from the list. */
                e = Util_freeElementToPool(handle, &SM_PER_THREAD_DS(handle).dlPool, dlElem);
                if (e < 0) ERR(handle, e);

                prevdlElem->next = nextdlElem;
            } else
                prevdlElem = dlElem;

            dlElem = nextdlElem;
        }


        /*
         * free files
         */
        dlElem = SM_PER_THREAD_DS(handle).smMountTable[v].dlHead.next;

        /*@ loop if dlElem is not NULL */
        while (dlElem != NULL) {

            assert(dlElem->type == DL_FILE);

            /* Destroy the file specified by 'fid'. */
            e = RDsM_PageIdToExtNo(handle, &(dlElem->elem.pFid), &firstExtNo);
            if (e < 0) ERR(handle, e);

            e = RDsM_DropSegment(handle, dlElem->elem.pFid.volNo, firstExtNo);
            if (e < 0) ERR(handle, e);

            /* Point to the next element of the list. */
            nextdlElem = dlElem->next;

            /* Delete the current element from the list. */
            e = Util_freeElementToPool(handle, &SM_PER_THREAD_DS(handle).dlPool, dlElem);
            if (e < 0) ERR(handle, e);

            dlElem = nextdlElem;
        }

        SM_PER_THREAD_DS(handle).smMountTable[v].dlHead.next = NULL;
    }

    /* Flush all buffer contents. */
    e = BfM_FlushAll(handle);
    if (e < eNOERROR) ERR(handle, e);

#ifdef USE_COHERENCY_VOLUME
    e = BfM_UpdateCoherencyVolume(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif


    return (eNOERROR);
}
