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
 * Module: RDsM_InsertMetaDictEntry.c
 *
 * Description:
 *  insert a meta dictionary entry into the first meta dictionary page of the given volume
 *
 * Exports:
 *  Four RDsM_InsertMetaDictEntry(Four, char*, char*, Four)
 *
 * Note:
 *  The caller should be sure that the name(including NULL character) length
 *  is less than MAX_METADICTENTRY_NAME_SIZE, and data length
 *  MAX_METADICTENTRY_DATA_SIZE.
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "TM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four RDsM_InsertMetaDictEntry(Four, char*, char*, Four)
 *
 * Description:
 *   insert a meta dictionary entry into the first meta dictionary page of the given volume
 *
 * Returns:
 *  Error code
 */
Four	RDsM_InsertMetaDictEntry(
    Four        handle,         /* IN    handle */
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Four	volNo,		/* IN volume number */
    char	*name,		/* IN entry name */
    char	*data,		/* IN data */
    Four	length,		/* IN length of data */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four	e;		/* returned error code */
    Buffer_ACC_CB *aPage_BCBP;	/* Buffer Access BCB holding meta entry */
    MetaDictPage_T *m_page;     /* meta dictionary page */
    MetaDictEntry_T *m_entry;	/* pointer to a meta dictionary entry */
    Four entryNo;               /* entry no of volume table entry corresponding to the given volume */
    PageID metaDictPid;         /* page id of meta dictionary page */
    Four	i;		/* loop index */
    Lsn_T lsn;                  /* LSN of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    /*
     *	check input parameters
     */
    if (name == NULL || data == NULL) ERR(handle, eBADPARAMETER);
    if (strlen(name) >= MAX_METADICTENTRY_NAME_SIZE) ERR(handle, eBADPARAMETER);
    if (length < 0 || length > MAX_METADICTENTRY_DATA_SIZE) ERR(handle, eBADPARAMETER);


    /*
     *	get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	get a page buffer for the meta dictionary page
     */
    metaDictPid = RDSM_VOLTABLE[entryNo].volInfo.dataVol.metaDictPid;
    e = BfM_getAndFixBuffer(handle, &metaDictPid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    m_page = (MetaDictPage_T*)aPage_BCBP->bufPagePtr;

    /*
     *	set a pointer to the corresponding entry
     */
    m_entry = m_page->entries;

    /*
     *	Check if there is an entry which has the same name.
     */
    for (i = 0; i < NUMMETADICTENTRIESPERPAGE; i++, m_entry++)
	if (!strcmp(m_entry->name, name)) { /* found */
            ERRBL1(handle, eDUPMETADICTENTRY_RDSM, aPage_BCBP, PAGE_BUF);
        }

    /*
     *	if there does not exist such a meta entry, make such an entry
     */
    /* set a pointer to the corresponding entry */
    m_entry = m_page->entries;

    /* find an empty entry */
    for (i = 0; i < NUMMETADICTENTRIESPERPAGE; i++, m_entry++)
	if (!strcmp(m_entry->name, "")) break;

    /*
     *	if there is no empty meta entry
     */
    if (i >= NUMMETADICTENTRIESPERPAGE)
        ERRBL1(handle, eNOEMPTYMETAENTRY_RDSM, aPage_BCBP, PAGE_BUF);

    /*
     *	make a new meta entry in the empty entry
     */
    strcpy(m_entry->name, name);
    memcpy(m_entry->data, data, length);


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_RDSM_INSERT_METADICTENTRY, LOG_REDO_ONLY,
                              metaDictPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Four), &i,
                              strlen(name)+1, name,
                              length, data);

	e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

        m_page->hdr.lsn = lsn;
        m_page->hdr.logRecLen = logRecLen;
    }

    /*
     *  set dirty bit
     */
    aPage_BCBP->dirtyFlag = 1;

    /*
     *	free this meta dictionary page
     */
    BFM_FREEBUFFER(handle, aPage_BCBP, PAGE_BUF, e);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* RDsM_InsertMetaDictEntry() */
