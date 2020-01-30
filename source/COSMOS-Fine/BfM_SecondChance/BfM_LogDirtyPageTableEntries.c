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
 * Module: BfM_LogDirtyPageTableEntries.c
 *
 * Description:
 *  Write the log record containing the dirty page table entries.
 *  The log records is a part of a checkpoint log records.
 *
 * Exports:
 *  Four BfM_LogDirtyPageTableEntries(void)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "xactTable.h"
#include "dirtyPageTable.h"
#include "SHM.h"
#include "BfM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*@
 * Internal Function Prototypes
 */
static Four bfm_LogDirtyPageTableEntries(Four, Four);


Four BfM_LogDirtyPageTableEntries(
    Four		handle)
{
    Four 		e;                     /* error code */
    Four 		type;                  /* buffer type */


    TR_PRINT(handle, TR_BFM, TR1, ("BfM_LogDirtyPageTableEntries()"));


    /* For each buffer pool */
    for (type = 0; type < NUM_BUF_TYPES; type++) {

        /* log dirty page table entries */
        e = bfm_LogDirtyPageTableEntries(handle,type);
        if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* BfM_LogDirtyPageTableEntries() */



Four bfm_LogDirtyPageTableEntries(
    Four		handle,
    Four 		type)                  	/* IN buffer type */
{
    Four 		e;			/* returned error number */
    BufTBLEntry 	*anEntry;		/* Buffer Table Entry to be checked */
    PageID 		aKey;			/* temporary key for conflict detection */
    Four 		nDirtyPages;           	/* number of dirty pages */
    Lsn_T 		lsn;                  	/* LSN of the newly written log record */
    Four 		logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 		/* log record information */
    Four 		i;
    DirtyPage_T 	dirtyPages[LOG_MAX_DIRTY_PAGES_PER_LOG_RECORD]; /* the list of dirty pages */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BFM, TR1, ("bfm_CollectDirtyPageTableEntries(type=%ld)", type));


    /* sacn the given typed Buffer Table */
    nDirtyPages = 0;
    for (i = 0, anEntry = &BI_BTENTRY(type, 0); i < BI_NBUFS(type); i++, anEntry++) {

	/* find dirty page which is not invalid */
	/* and copy its information into dirtyPageList */
	if ( !IS_NILBFMHASHKEY(anEntry->key) && anEntry->dirtyFlag ) {

	    aKey = *((PageID *)&(anEntry->key));
	    if (IS_NILBFMHASHKEY(aKey)) continue;

	    /* Mutex Begin : protection from updating */
	    if ((e = bfm_lock(handle, (TrainID *)&aKey, type)) < eNOERROR) ERR(handle, e);

	    if ( EQUAL_PAGEID(aKey, anEntry->key) &&
		anEntry->dirtyFlag ) { 

		dirtyPages[nDirtyPages].pid = *((PageID*)&(anEntry->key));
		dirtyPages[nDirtyPages].recLsn = anEntry->recLsn;

		nDirtyPages++;

	    } /* if  */

	    /* Mutex End : protection from updating */
	    if ( (e = bfm_unlock(handle, (TrainID *)&aKey, type)) < eNOERROR) ERR(handle, e);

	} /* if */


        if ((nDirtyPages == LOG_MAX_DIRTY_PAGES_PER_LOG_RECORD) || (i+1 == BI_NBUFS(type))) {
            One bufferType_tmp = type;

            LOG_FILL_LOGRECINFO_2(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_CHECKPOINT,
                                  LOG_ACTION_CHKPT_DIRTY_PAGES, LOG_REDO_ONLY,
                                  common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(One), &bufferType_tmp,
                                  nDirtyPages*sizeof(DirtyPage_T), dirtyPages);

            e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            nDirtyPages = 0;
        }

    } /* for */

    return(eNOERROR);

} /* bfm_LogDirtyPageTableEntries() */
