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
 * Module: mlgf_Util.c
 *
 * Description:
 *  Includes various utility functions used in MLGF.
 *
 * Exports:
 *  Boolean mlgf_EqualKeys(Four, MLGF_KeyDesc*, MLGF_HashValue[], MLGF_HashValue[])
 *  Four mlgf_GetObjectFromOverflow(Four, PageID*, MLGF_KeyDesc*, ObjectID*, char*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: mlgf_EqualKeys(handle, MLGF_KeyDesc*f, MLGF_HashValue[], MLGF_HashValue[])
 *
 * Description:
 *  Compare keys of two objects.
 *
 * Returns:
 *  TRUE if keys of object x are equal to the ones of object y.
 *  FALSE otherwise
 */
Boolean mlgf_EqualKeys(
    Four 		handle,
    MLGF_KeyDesc 	*kdesc,		/* IN key descriptor of the given index */
    MLGF_HashValue 	keys_x[],	/* IN keys of x */
    MLGF_HashValue 	keys_y[])	/* IN keys of y */
{
    Four 		k;		/* index variable */


    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_EqualKeys(kdesc=%P, keys_x=%P, keys_y=%P)", kdesc, keys_x, keys_y));


    for (k = 0; k < kdesc->nKeys; k++)
	if (keys_x[k] != keys_y[k]) return(FALSE);

    return(TRUE);

} /* mlgf_EqualKeys() */



/*
 * Function:Four mlgf_GetObjectFromOverflow(Four, PageID*, MLGF_KeyDesc*, ObjectID*, char*)
 *
 * Description:
 *  Get the first object's ObjectID and its extra data of the given overflow
 *  chain.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_GetObjectFromOverflow(
    Four 		handle,
    PageID 		*ovPid,		/* IN first page of overflow chain */
    MLGF_KeyDesc 	*kdesc,		/* IN key descriptor of the given index */
    ObjectID 		*oid,		/* OUT returns the first object's ObjectID */
    char 		*data)		/* OUT returns extra data */
{
    Four 		e;		/* error code */
    char 		*objectItem;	/* points to an element in object array */
    mlgf_OverflowPage 	*opage;		/* an overflow page */
    Buffer_ACC_CB 	*ov_BCB;	/* buffer control block for overflow apge */


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_GetObjectFromOverflow(handle, ovPid=%P, kdesc=%ld, oid=%P, data=%P)",
	      ovPid, kdesc, oid, data));


    e = BfM_getAndFixBuffer(handle, ovPid, M_FREE, &ov_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    opage = (mlgf_OverflowPage*)ov_BCB->bufPagePtr;

    objectItem = MLGF_OVERFLOW_ITH_OBJECTITEM(MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen), opage, 0);

    *oid = *((ObjectID*)objectItem);
    if (data) memcpy(data, objectItem+sizeof(ObjectID), kdesc->extraDataLen); 

    e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_GetObjectFromOverflow( ) */
