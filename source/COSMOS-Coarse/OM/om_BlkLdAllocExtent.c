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
/*
 * Module: om_BlkLdAllocExtent.c
 *
 * Description:
 *  Allocate number of contiguous pages from disk.
 *
 * Exports:
 *  Four om_BlkLdAllocExtent(FileID, PageID*, Four*, Two, PageID*)
 */

#include <string.h>

#include "common.h"
#include "param.h"
#include "bl_param.h"
#include "RDsM_Internal.h"	
#include "BfM_Internal.h" 	/* TTT */
#include "OM_Internal.h"
#include "BL_OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@========================================
 *  om_BlkLdAllocExtent()
 * =======================================*/
/*
 * Function : Four om_BlkLdAllocExtent(FileID, PageID*, Four*, Two, PageID*)
 *
 * Description :
 *  Allocate number of contiguous pages from disk.
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *  0)
 *  1)
 *
 */

Four om_BlkLdAllocExtent(
    Four handle,
    PhysicalFileID      pFid,              /* IN     physical file ID which allocate extent */ 
    PageID              *pid,              /* IN     page ID for allocate extent in which contains that page */
    Four                *numOfAllocTrains, /* INOUT  number of allocated trains */
    Two                 eff,               /* IN     extent fill factor */
    PageID              *pageIdAry)        /* OUT    page ID array which contain allocated page ID */
{

    Four                e;                 /* error code */
    Four                firstExtNo;        /* first extent number of the file */
    Four                pagesNoInExtent;   /* pages in extent by eff (numOfAllocTrains / BLKLD_WRITEBUFFERSIZE) */
    Four                totalAllocated=0;  /* number of total pages which allocated */
    Four                i;                 /* index variable */

    /* parameter check */
    if(*numOfAllocTrains < 0) ERR(handle, eBADPARAMETER);
    if(pageIdAry == NULL) ERR(handle, eBADPARAMETER);

    /* get first extent number of this data file */
    e = RDsM_PageIdToExtNo(handle, (PageID *)&pFid, &firstExtNo); 
    if (e < 0) ERR(handle, e);

    /* Case 1 : no object exist in data file 
                allocate extent which contains the page of 'pid' */ 
    if (pid != NULL) {

        /* calculate number of pages in extent by eff */ 
        pagesNoInExtent = *numOfAllocTrains / BLKLD_WRITEBUFFERSIZE;

        /* get first extent number of this data file */
        e = RDsM_PageIdToExtNo(handle, (PageID *)&pFid, &firstExtNo); 
        if (e < 0) ERR(handle, e);

        /* allocate except the first page in data file */
        pagesNoInExtent--; 
        e = RDsM_AllocContigTrainsInExt(handle, pFid.volNo, firstExtNo, pid, eff, &pagesNoInExtent, PAGESIZE2, &pageIdAry[1]); 
        if (e < 0) ERR(handle, e);

        /* set the first page of buffer 'pid' */
        pageIdAry[0] = *pid;
        pagesNoInExtent++;

        totalAllocated += pagesNoInExtent;

        /* calculate number of pages in extent by eff */ 
        pagesNoInExtent = *numOfAllocTrains / BLKLD_WRITEBUFFERSIZE;

        for(i = pagesNoInExtent; i < *numOfAllocTrains; i += pagesNoInExtent) {

            /* get first extent number of this data file */
            e = RDsM_PageIdToExtNo(handle, (PageID *)&pFid, &firstExtNo); 
            if (e < 0) ERR(handle, e);

            e = RDsM_AllocContigTrainsInExt(handle, pFid.volNo, firstExtNo, NULL, eff, &pagesNoInExtent, PAGESIZE2, &(pageIdAry[i])); 
            if (e < 0) ERR(handle, e); 

            totalAllocated += pagesNoInExtent;

            /* calculate number of pages in extent by eff */ 
            pagesNoInExtent = *numOfAllocTrains / BLKLD_WRITEBUFFERSIZE;
        }

    /* Case 2 : object already exist in data file 
                allocate new extent */ 
    } else {

        /* calculate number of pages in extent by eff */ 
        pagesNoInExtent = *numOfAllocTrains / BLKLD_WRITEBUFFERSIZE;

        for(i = 0; i < *numOfAllocTrains; i += pagesNoInExtent) {

            /* get first extent number of this data file */
            e = RDsM_PageIdToExtNo(handle, (PageID *)&pFid, &firstExtNo);
            if (e < 0) ERR(handle, e);

            e = RDsM_AllocContigTrainsInExt(handle, pFid.volNo, firstExtNo, NULL, eff, &pagesNoInExtent, PAGESIZE2, &(pageIdAry[i])); 
            if (e < 0) ERR(handle, e); 

            totalAllocated += pagesNoInExtent;

            /* calculate number of pages in extent by eff */ 
            pagesNoInExtent = *numOfAllocTrains / BLKLD_WRITEBUFFERSIZE;

        }

    }

    *numOfAllocTrains = totalAllocated;

    return(eNOERROR);

} /* om_BlkLdAllocExtent() */
