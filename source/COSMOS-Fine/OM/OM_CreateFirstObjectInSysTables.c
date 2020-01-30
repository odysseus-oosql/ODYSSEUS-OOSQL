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
#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "latch.h"
#include "LOG.h"
#include "RDsM.h"		/* for the raw disk manager call */
#include "BfM.h"		/* for the buffer manager call */
#include "OM.h"
#include "LM.h"
#include "TM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: createFirstObjectInSysTables(FileID*, Four, char*, ObjectID*)
 *
 * Description :
 *  Create the first catalog entry in the catalog table SM_SYSTABLES. The
 *  first catalog entry is for the SM_SYSTABLES itself. The OM_CreateObject( )
 *  function in the object manager(OM) request that the catalog entry for
 *  the file where the new object will be put should be in the SM_SYSTABLES.
 *  So we cannot use the OM_CreateObject( ) function for the first catalog entry
 *  of SM_SYSTABLES becase the catalog entry for SM_SYSTABLES does not exist
 *  in SM_SYSTABLES.
 *
 * Return Values :
 *  Error Code
 *    some errors caused by fuction calls
 *
 * Notice:
 *  We assume that the length of the data is less than the half of the page
 *  size. So we don't modify the links of the available space list.
 */
Four OM_CreateFirstObjectInSysTables(
    Four 	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    PhysicalFileID *pFid,	/* IN FileID of the catalog table SM_SYSTABLES*/ 
    ObjectHdr   *objHdr,	/* IN from which tag & properties are set */
    Four	length,		/* IN amount of data */
    char	*data,		/* IN the initial data for the object */
    ObjectID	*oid,		/* OUT the object's ObjectID */
    LockParameter *lockup,      /* IN request lock or not */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four        e;		/* error number */
    Four        neededSpace;    /* space needed to put new object [+ header] */ 
    SlottedPage *apage;		/* pointer to the slotted page buffer */
    Four        alignedLen;	/* aligned length of initial data */
    PageID      pid;            /* PageID in which new object to be inserted */
    Object      *obj;		/* point to the newly created object */
    Four        i;		/* index variable */
    Buffer_ACC_CB *aPage_BCBP;	/* buffer access control block for a page */
    Boolean allocFlag, pageUpdateFlag;


    /* FileID is the PageID of the first page of the file. */
    pid = *pFid; 

    e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    /* calculate the length to be needed in the slotted page. */
    alignedLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(length));
    neededSpace = sizeof(ObjectHdr) + alignedLen;

#ifdef CCRL
    e = om_AcquireSpace(handle, &xactEntry->xactId, apage, neededSpace, 0, &allocFlag, &pageUpdateFlag); 
    assert(allocFlag == TRUE);
    assert(pageUpdateFlag == TRUE);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    /*
     * At this point
     * pid : PageID of the page into which the new object will be placed
     * apage : pointer to the slotted page buffer
     * alignedLen : space for data of the new object
     */
    /* where to put the object[header]? */
    obj = (Object *)&(apage->data[apage->header.free]);

    /* initialize ObjectHdr */
    obj->header.properties = P_CLEAR;
    obj->header.tag = (objHdr != NULL) ? objHdr->tag:0;
    obj->header.length = alignedLen;

    /* copy the data into the object */
    memcpy(obj->data, data, length);

    assert(apage->header.nSlots == 1);

    apage->slot[0].offset = apage->header.free;
    e = om_GetUnique(handle, xactEntry, aPage_BCBP, &(apage->slot[0].unique), logParam);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

    /* update the pointer to the start of contiguous free space */
    apage->header.free += sizeof(ObjectHdr) + alignedLen;

    /* Construct the ObjectID to be returned */
    if(oid != NULL)
	MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, 0, apage->slot[0].unique);

    /* set dirtyFlag */
    aPage_BCBP->dirtyFlag = 1;

    /* free the buffer page */
    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF); 
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* OM_CreateFirstObjectInSysTables() */
