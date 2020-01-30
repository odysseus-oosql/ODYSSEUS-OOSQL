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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: mlgf_DeleteFromDirectory.c
 *
 * Description:
 *  Delete a directory entry from the directory page.
 *
 * Exports:
 *  void mlgf_DeleteFromDirectory(handle, mlgf_DirectoryPage*, MLGF_KeyDesc*, Four)
 *
 * Returns:
 *  None
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "MLGF.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four mlgf_DeleteFromDirectory(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, 	/* IN transaction table entry */
    mlgf_DirectoryPage 	*dirPage, 	/* INOUT where the new entry is deleted */
    MLGF_KeyDesc 	*kdesc,	 	/* IN key descriptor of used index */
    Four 		entryNo,        /* IN entry to be deleted */
    LogParameter_T 	*logParam) 	/* IN log parameter */
{
    Four 		e;              /* error code */
    Four 		entryLen;	/* length of a directory entry */
    mlgf_DirectoryEntry *entry;		/* points to the insertion position */
    Four 		i;
    Lsn_T 		lsn;            /* lsn of the newly written log record */
    Four 		logRecLen;      /* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 	/* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_DeleteFromDirectory()"));


    /* Get the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    /* Points to the deleted entry. */
    entry = MLGF_ITH_DIRENTRY(dirPage, entryNo, entryLen);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        Two tmpEntryNo = entryNo;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_DELETE_DIRECTORY_ENTRY, LOG_REDO_UNDO,
                              dirPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Two), &tmpEntryNo,
                              entryLen, entry);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        /* mark the lsn in the page */
        dirPage->hdr.lsn = lsn;
        dirPage->hdr.logRecLen = logRecLen;
    }

    memmove(entry, (char*)entry + entryLen, (dirPage->hdr.nEntries-entryNo-1)*entryLen);

    dirPage->hdr.nEntries --;

    return(eNOERROR);

} /* mlgf_DeleteFromDirectory() */
