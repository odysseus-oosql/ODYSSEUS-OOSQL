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
 * Module : 	lot_ReadObject.c
 *
 * Description :
 *  Read the large object data from disk into the user supplied buffer.
 *
 * Exports:
 *  Four lot_ReadObject(Four, PageID *, Four, Four, char *)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "BfM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_ReadObject( )
 *================================*/
/*
 * Function: Four lot_ReadObject(Four, PageID *, Four, Four, char *)
 *
 * Description :
 *  Read the large object data from disk into the user supplied buffer.
 * The function calls itself recursively. The basis is the lot_ReadDataPage()
 * function call at the leaf node.
 *
 * Returns:
 *	eNOERROR :	No Error
 *	some errors :	by other calls
 *
 * Note :
 *  The parameters are not checked. The caller should pass the correct
 *  parameters. For example, root should not be NIL, start & length must be
 *  less than the object size, and buf may not be NULL.
 */
Four lot_ReadObject(
    Four handle,
    Four volNo,                 /* IN volume number */
    PageID   *root,		/* IN root page's PageID */
    L_O_T_INode *anode,         /* IN root node */
    Four   start,		/* IN starting offset of read */
    Four   length,		/* IN amount of data to read */
    char   *buf)		/* OUT user buffer holding the data */
{
    Four	e;		/* error code */
    L_O_T_INodePage *apage;     /* pointer to buffer holding root page */
    Buffer_ACC_CB *apage_BCBP;
    PageID	childPid;	/* PageID of the root of subtree */
    Four	newStart;	/* starting offset used in child node */
    Four	newLength;	/* length used in child node */
    Four	i;		/* slot index of child node */


    TR_PRINT(handle, TR_LOT, TR1, ("lot_ReadObject()"));


    apage = NULL;               /* initialize for error handling */

    if (root != NULL) {
        /*@ Read a root page into the buffer page from the disk */
        e = BfM_getAndFixBuffer(handle, root, M_FREE, &apage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        apage = (L_O_T_INodePage *)apage_BCBP->bufPagePtr;
        anode = &apage->node;
    }

    i = lot_SearchInNode(handle, anode, start);

    /*
     * Length & Start used in child node for the first loop
     */
    newLength = MIN(anode->entry[i].count - start, length);
    newStart = start - ((i == 0) ? 0:anode->entry[i-1].count);

    while (length > 0) {
	/* childPid denots the PageID of the child node */
	MAKE_PAGEID(childPid, volNo, anode->entry[i].spid);
	if (anode->header.height == 1) {
	    /*
	     * This node is a leaf page; recusive basis
	     * So read the data page into the buffer.
	     */
	    e = lot_ReadData(handle, &childPid, newStart, newLength, buf);
	} else {
	    /*
	     * This node is a internal node; recursive call
	     */
	    e = lot_ReadObject(handle, volNo, &childPid, NULL, newStart, newLength, buf);
	}
        if(e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	/*@
	 * adjust the variables to reflect the above call
	 */
	i++;
	buf += newLength;
	length -= newLength;

	/* set the next time length & start */
	if (length > 0) {
	    newLength = MIN(anode->entry[i].count - anode->entry[i-1].count, length);
	    newStart = 0;
	}
    }

    if (root != NULL) {
        e = BfM_unfixBuffer(handle, apage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

  LABEL_Error:
    if (apage != NULL) {
        (Four)BfM_unfixBuffer(handle, apage_BCBP, PAGE_BUF);
    }
    return(e);

} /* lot_ReadObject() */
