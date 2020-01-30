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
 * Module: om_ChangeObjectSize.c
 *
 * Description:
 *  Change an object size.
 *
 * Exports:
 *  Four om_ChangeObjectSize(Four, SlottedPage*, Four, Four)
 *
 * Returns:
 *
 *
 * Assumption:
 *  We assume that the page can be accomdate the new length.
 *  We assume that the given object is not the moved object; that is,
 *  the object has not P_MOVED property.
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four om_ChangeObjectSize(
    Four 	handle,
    XactID 	*xactId, 	/* IN transaction table entry */
    SlottedPage *apage,		/* IN page containing the object */
    Four        slotNo,		/* IN slot number */
    Four	oldLength,      /* IN old length */
    Four	newLength,      /* IN new length */
    Boolean	calledOnUndoFlag) /* IN called on undo operation if TRUE */
{
    Four 	e;              /* error code */
    Object 	*obj;		/* point to the object in page */
    Four 	offset;		/* offset in data area of the object */
    Four 	deltaLength;    /* difference between old length and new length */
    Boolean 	pageUpdateFlag;
    Boolean 	allocFlag;


    TR_PRINT(handle, TR_OM, TR1, ("om_ChageObjectSize()"));


    deltaLength = newLength - oldLength;

    /* Points to the object. */
    offset = apage->slot[-slotNo].offset;
    obj = (Object *)&(apage->data[offset]);

    if (deltaLength > 0) {	/* grow the size */

#ifdef CCRL
        if (calledOnUndoFlag == TRUE) {
            e = om_UndoRelease(handle, xactId, apage, deltaLength, &pageUpdateFlag);
        } else {
            e = om_AcquireSpace(handle, xactId, apage, deltaLength, 0, &allocFlag, &pageUpdateFlag);
            assert(allocFlag == TRUE);
        }
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	if (apage->header.free == offset+sizeof(ObjectHdr)+oldLength &&
	    deltaLength <= SP_CFREE(apage)) {
	    /* This object is the last object in the page and
	     * the added bytes can be appended without movement. */

	    apage->header.free += deltaLength;

	} else if (newLength + sizeof(ObjectHdr) <= SP_CFREE(apage)) {
	    /* The contiguous free space can accomadate full object */

	    /* copy the original object to the last data space */
	    memcpy(&(apage->data[apage->header.free]), obj,
		   sizeof(ObjectHdr) + oldLength);

	    apage->slot[-slotNo].offset = apage->header.free;
	    apage->header.free += sizeof(ObjectHdr) + newLength;
	    apage->header.unused += sizeof(ObjectHdr) + oldLength;

	} else {
	    /* Complex Case: Compact the data page and insert it */

	    (void) om_CompactPage(handle, apage, slotNo);

	    apage->header.free += deltaLength;
	}

    } else { /* shrink the size */
#ifdef CCRL
        if (calledOnUndoFlag == TRUE) {
            e = om_UndoAcquire(handle, xactId, apage, -deltaLength, &pageUpdateFlag);
        } else {
            e = om_ReleaseSpace(handle, xactId, apage, -deltaLength, &pageUpdateFlag);
        }
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	if (apage->header.free == offset+sizeof(ObjectHdr)+oldLength) {
	    /* this is the last object in the page */
	    /* the space can be reused in the contiguous space */
	    /* apage->header.free -= -deltaLength */
	    apage->header.free += deltaLength;
	} else {
	    /* free the space: apage->header.unused += -deltaLength */
	    apage->header.unused -= deltaLength;
	}
    }

    return(eNOERROR);

} /* om_ChangeObjectSize() */
