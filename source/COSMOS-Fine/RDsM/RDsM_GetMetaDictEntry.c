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
 * Module: RDsM_GetMetaDictEntry.c
 *
 * Description:
 *  get a meta dictionary entry from the meta dictionary page of the given volume
 *
 * Exports:
 *  Four RDsM_GetMetaDictEntry(Four, char*, char*, Four)
 *
 * Note:
 *  The caller should be sure that the data length is less than the maximum
 *  size, MAX_METADICTENTRY_DATA_SIZE.
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"




/*
 * Function: Four RDsM_GetMetaDictEntry(Four, char*, char*, Four)
 *
 * Description:
 *   get a meta dictionary entry from the meta dictionary page of the given volume
 *
 * Returns:
 *  Error code
 */
Four	RDsM_GetMetaDictEntry(
    Four        handle,         /* IN handle */
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Four	volNo,		/* IN volume number */
    char	*name,		/* IN entry name */
    char	*data,		/* OUT data */
    Four	length)		/* IN length of data */
{
    Four	e;		/* returned error code */
    Buffer_ACC_CB *aPage_BCBP;	/* Buffer Access BCB holding meta entry */
    MetaDictPage_T *m_page;     /* meta dictionary page */
    MetaDictEntry_T *m_entry;	/* pointer to a meta dictionary entry */
    Four entryNo;               /* entry no of volume table entry corresponding to the given volume */
    Four	i;		/* loop index */


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_GetMetaDictEntry(volNo=%ld, name=%P, data=%P, length=%ld)",
                            volNo, name, data, length));


    /*
     *	check input parameters
     */
    if (name == NULL || data == NULL) ERR(handle, eBADPARAMETER);
    if (length < 0 || length > MAX_METADICTENTRY_DATA_SIZE) ERR(handle, eBADPARAMETER);


    /*
     *	get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	get a page buffer for the meta dictionary page
     */
    e = BfM_getAndFixBuffer(handle, &RDSM_VOLTABLE[entryNo].volInfo.dataVol.metaDictPid, M_SHARED, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    m_page = (MetaDictPage_T*)aPage_BCBP->bufPagePtr;

    /*
     *	set a pointer to the first entry in the page
     */
    m_entry = m_page->entries;

    /*
     *	find the corresponding meta entry
     */
    for (i = 0; i < NUMMETADICTENTRIESPERPAGE; i++, m_entry++)
	if (!strcmp(m_entry->name, name)) break;

    /*
     *	if such an entry does not exist
     */
    if (i >= NUMMETADICTENTRIESPERPAGE)
	ERRBL1(handle, eMETADICTENTRYNOTFOUND_RDSM, aPage_BCBP, PAGE_BUF);

    /*
     *	copy the corresponding entry to the user's memory
     */
    (void) memcpy(data, m_entry->data, length);

    /*
     *	free this page
     */
    BFM_FREEBUFFER(handle, aPage_BCBP, PAGE_BUF, e);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* RDsM_GetMetaDictEntry() */
