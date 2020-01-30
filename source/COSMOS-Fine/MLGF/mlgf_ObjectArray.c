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
 * Module: mlgf_ObjectArray.c
 *
 * Description:
 *  Routines for the object array. The object array is the array of pairs
 *  of ObjectID and its extra data. The objects in one array have the same
 *  keys.
 *
 * Exports:
 *  Four mlgf_ObjectIdComp(Four, ObjectID*, ObjectID*)
 *  Boolean mlgf_BinarySearchObjectArray(Four, char*, ObjectID*, Four, Four, Four*)
 *
 * Returns:
 *  TRUE if we find the given object
 *  FALSE otherwise
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: Four mlgf_ObjectIdComp(Four, ObjectID*, ObjectID*)
 *
 * Description:
 *  Compare two ObjectIDs and returns its result.
 *
 * Returns:
 *  Result of comparison
 *    EQUAL : oid_x and oid_y are same
 *    GREAT : oid_x is greater than oid_y
 *    LESS  : oid_x is less than oid_y
 */
Four mlgf_ObjectIdComp(
    Four 	handle,
    ObjectID 	*oid_x,		/* IN one ObjectID */
    ObjectID 	*oid_y)		/* IN another ObjectID */
{
    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_ObjectIdComp(oid_x=%P, oid_y=%P)", oid_x, oid_y));

   /* Consider PageIDs, if they are identical, compare slotNos */
    if (oid_x->volNo < oid_y->volNo) return(LESS);
    else if (oid_x->volNo > oid_y->volNo) return(GREAT);
    else if( oid_x->pageNo < oid_y->pageNo) return(LESS);
    else if( oid_x->pageNo > oid_y->pageNo) return(GREAT);
    else if( oid_x->slotNo < oid_y->slotNo) return(LESS);
    else if( oid_x->slotNo > oid_y->slotNo) return(GREAT);

    return(eNOERROR);
}


/*
 * Function: Boolean mlgf_BinarySearchObjectArray(Four, char*, ObjectID*, Four, Four, Four*)
 *
 * Description:
 *  Search the object array for an entry whose ObjectID is equal to the given
 *  ObjectID. If found, returns its element No. Otherwise, the entry No whose
 *  ObjectID is smallest but not less than the given ObjectID.
 *
 * Returns:
 *  TRUE if we find the given object
 *  FALSE otherwise
 */
Boolean mlgf_BinarySearchObjectArray(
    Four 	handle,
    char 	*array,			/* IN starting point of array */
    ObjectID 	*oid,			/* IN search for this object */
    Four 	nObjects,		/* IN number of ojbects in the array */
    Four 	elemLen,		/* IN length of an element in array */
    Four 	*elemNo)		/* OUT  */
{
    Four 	low, high, mid;		/* variables used for binary search */
    Four 	cmp;			/* result of comparison */


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_BinarySearchObjectArray(handle, array=%P, oid=%P, nObjects=%ld, elemLen=%ld, elemNo=%P)",
	      array, oid, nObjects, elemLen, elemNo));


    low = 0;
    high = nObjects - 1;

    while (low <= high) {
	mid = (low + high) / 2;

	cmp = mlgf_ObjectIdComp(handle, oid, (ObjectID*)(array + mid*elemLen));

	if (cmp == EQUAL) {
	    *elemNo = mid;
	    return(TRUE);
	}

	if (cmp == GREAT) low = mid + 1;
	else high = mid - 1;
    }

    *elemNo = low;
    return(FALSE);

} /* mlgf_binarySearchObjectArray*() */

