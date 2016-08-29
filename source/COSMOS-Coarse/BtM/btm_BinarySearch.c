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
 * Module: btm_BinarySearch.c
 *
 * Description :
 *  This file has three function about searching. All these functions use the
 *  binary search algorithm. If the entry is found successfully, it returns 
 *  TRUE and the correct position as an index, otherwise, it returns FALSE and
 *  the index whose key value is the largest in the given page but less than
 *  the given key value in the function btm_BinarSearchInternal; in the
 *  function btm_BinarySearchLeaf() the index whose key value is the smallest
 *  in the given page but larger than the given key value.
 *
 * Exports:
 *  Boolean btm_BinarySearchInternal(BtreeInternal*, KeyDesc*, KeyValue*, Two*)
 *  Boolean btm_BinarySearchLeaf(BtreeLeaf*, KeyDesc*, KeyValue*, Two*)
 *  Boolean btm_BinarySearchOidArray(ObjectID[], ObjectID*, Two, Two*)
 */


#include "common.h"
#include "trace.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_BinarySearchInternal()
 *================================*/
/*
 * Function:  Boolean btm_BinarySearchInternal(BtreeInternal*, KeyDesc*,
 *                                             KeyValue*, Two*)
 *
 * Description:
 *  Search the internal entry of which value equals to or less than the given
 *  key value.
 *
 * Returns:
 *  Result of search: TRUE if the same key is found, otherwise FALSE
 *
 * Side effects:
 *  1) parameter idx : slot No of the slot having the key equal to or
 *                     less than the given key value
 *                     
 */
Boolean btm_BinarySearchInternal(
    Four handle,
    BtreeInternal 	*ipage,		/* IN Page Pointer to an internal page */
    KeyDesc       	*kdesc,		/* IN key descriptor */
    KeyValue      	*kval,		/* IN key value */
    Two          	*idx)		/* OUT index to be returned */
{
    Two  		low;			/* low index */
    Two  		mid;			/* mid index */
    Two  		high;			/* high index */
    Four 		cmp;			/* result of comparison */
    btm_InternalEntry 	*entry;	/* an internal entry */


    TR_PRINT(TR_BTM, TR1,
             ("btm_BinarySearchInternal(handle, ipage=%P, kdesc=%P, kval=%P, idx=%P)",
	      ipage, kdesc, kval, idx));
    
    low = 0;
    high = ipage->hdr.nSlots - 1;
    
    if (high >=0) {

        /* first check with last entry for performance of append */
        entry = (btm_InternalEntry*)&(ipage->data[ipage->slot[-high]]);
        if (btm_KeyCompare(handle, kdesc, kval, (KeyValue*)&entry->klen) == GREAT) {
            *idx = high;
            return(FALSE);
        }

	/* find the key value by using binary search */
	do {
	    mid = (low + high)/2;	/*@ get the mid index */

	    entry = (btm_InternalEntry*)&(ipage->data[ipage->slot[-mid]]);

	    cmp = btm_KeyCompare(handle, kdesc, kval, (KeyValue*)&entry->klen);
	    
	    if (cmp != GREAT) high = mid - 1;
	    if (cmp != LESS) low = mid + 1;
	} while (high >= low);
		
	if ((low-high) > 1) {	/* found */
	    *idx = mid;		/* the correct position */
	    return(TRUE);
	    
	} else {
	    *idx = high;	/* the largest key but less than the given key */
	    return(FALSE);
	}
	
    } else {	/* # of entries = 0 */
	*idx = -1;
	return(FALSE);
    }
    
} /* btm_BinarySearchInternal() */



/*@================================
 * btm_BinarySearchLeaf()
 *================================*/
/*
 * Function: Boolean btm_BinarySearchLeaf(BtreeLeaf*, KeyDesc*,
 *                                        KeyValue*, Two*)
 *
 * Description:
 *  Search the leaf item of which value equals to or less than the given
 *  key value.
 *
 * Returns:
 *  Result of search: TRUE if the same key is found, FALSE otherwise
 *
 * Side effects:
 *  1) parameter idx: slot No of the slot having the key equal to or
 *                    less than the given key value
 */
Boolean btm_BinarySearchLeaf(
    Four handle,
    BtreeLeaf 		*lpage,		/* IN Page Pointer to a leaf page */
    KeyDesc   		*kdesc,		/* IN key descriptor */
    KeyValue  		*kval,		/* IN key value */
    Two       		*idx)		/* OUT index to be returned */
{
    Two  		low;		/* low index */
    Two  		mid;		/* mid index */
    Two  		high;		/* high index */
    Four 		cmp;		/* result of comparison */
    btm_LeafEntry 	*entry;		/* a leaf entry */


    TR_PRINT(TR_BTM, TR1,
             ("btm_BinarySearchLeaf(handle, lpage=%P, kdesc=%P, kval=%P, idx=%P)",
	      lpage, kdesc, kval, idx));
    
    low = 0;
    high = lpage->hdr.nSlots - 1;
    
    if (high >= 0) {
	/* find the key value by using binary search. */
	do {
	    mid = (low + high)/2;	/*@ get the mid index */

	    entry = (btm_LeafEntry*)&(lpage->data[lpage->slot[-mid]]);
	    
	    cmp = btm_KeyCompare(handle, kdesc, kval, (KeyValue*)&entry->klen);
	    
	    if(cmp != GREAT) high = mid - 1;
	    if(cmp != LESS) low = mid + 1;
	} while (high >= low);
	
	if ((low-high) > 1) {	/* found */
	    *idx = mid;	/* the correct position */
	    return(TRUE);
	} else {
	    *idx = high;	/* the largest key but less than the given key */
	    return(FALSE);
	}
    } else {	/* # of entries = 0 */
	*idx = -1;
	return(FALSE);
    }
    
} /* btm_BinarySearchLeaf() */



/*@================================
 * btm_BinarySearchOidArray()
 *================================*/
/*
 * Function: Boolean btm_BinarySearchOidArray(ObjectID[], ObjectID*,
 *                                            Two, Two*)
 *
 * Description:
 *  Search the given ObjectID in the ObjectID array by using the binary search
 *  algorithm. If the ObjectID is found, return TRUE and the index, otherwise,
 *  return FALSE and the index whose key is the largest but less than the
 *  search key.
 *
 * Returns:
 *  Result of Search: TRUE if found, FALSE otherwise
 *
 * Side effects:
 *  parameter idx: index of ObjectID array entry having the given ObjectID
 */
Boolean btm_BinarySearchOidArray(
    Four handle,
    ObjectID                    oidArray[],     /* IN an array of object identifiers */
    ObjectID                    *oid,           /* IN The ObjectID to be searched */
    Two                         nObjects,       /* IN # of ObjectIDs in this array */
    Two                         *idx)           /* OUT index to be returned */
{
    Two                         low;            /* low index */
    Two                         mid;            /* mid index */
    Two                         high;           /* high index */
    

    TR_PRINT(TR_BTM, TR1,
             ("btm_BinarySearchOidArray(handle, oidArray=%P, oid=%P, nObjects=%P, idx=%P)",
	      oidArray, oid, nObjects, idx));

    low = 0, high = nObjects - 1;
        
    if(high >= 0) {
	/* Find the ObjectID by using the binary search technique. */
	do {
	    mid = (low + high) / 2;	 /*@ Get the mid index */
	    
	    if(btm_ObjectIdComp(handle, oid, &oidArray[mid]) != GREAT) high = mid - 1;
	    if(btm_ObjectIdComp(handle, oid, &oidArray[mid]) != LESS) low = mid + 1;
	} while(high >= low);
	
	if((low-high) > 1) {    /* found */
	    *idx = mid;
	    return(TRUE);
	} else {	/* not found */
	    *idx = high; /* largest but less than the given ObjectID */
	    return(FALSE);
	}
    } else {	/* # of ObjectIDs = 0 */
	*idx = -1;
	return(FALSE);
    }
    
} /* btm_BinarySearchOidArray() */
