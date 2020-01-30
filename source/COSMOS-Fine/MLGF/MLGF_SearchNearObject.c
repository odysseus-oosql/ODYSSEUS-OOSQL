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
 * Module: MLGF_SearchNearObject.c
 *
 * Description:
 *  For the given object with its keys, returns its near object.
 *
 * Exports:
 *  Four MLGF_SearchNearObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, LockParameter*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Function Prototype */
Four mlgf_SearchNearObjectRecursive(Four, PageID*, MLGF_KeyDesc*, mlgf_MortonValue*, ObjectID*);



/*
 * Function: Four MLGF_SearchNearObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*)
 *
 * Description:
 *  For the given object with its keys, returns its near object.
 *  This routine converts the given key into the morton order value and
 *  calls mlgf_SearchNearObjectRecursive() with the morton value.
 *
 * Returns:
 *  Error Code
 *    eBADPARAM_MLGF
 *    some errors caused by function calls
 */

Four MLGF_SearchNearObject(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, 			/* IN transaction table entry */
    PageID         	*rootPid,			/* IN root of a MLGF index */ 
    MLGF_KeyDesc   	*kdesc,				/* IN key descriptor of a MLGF index */
    MLGF_HashValue 	keys[],				/* IN hash values of the new object */
    ObjectID       	*oid,				/* OUT found near object */
    LockParameter 	*lockup)      			/* IN request lock or not */
{
    Four 		e;				/* error code */
    Four 		i;				/* index variable */
    mlgf_MortonValue 	morton;				/* Morton-order value of the hash values */
    One 		nValidBits[MLGF_MAXNUM_KEYS]; 	/* used for getting morton value */


    TR_PRINT(handle, TR_MLGF, TR1, ("MLGF_SearchNearObject()"));


    if (rootPid == NULL || kdesc == NULL || keys == NULL || oid == NULL)  
	ERR(handle, eBADPARAMETER);


    /* Get the morton value of the searched_for_key. */
    for (i = 0; i < kdesc->nKeys; i++)
	nValidBits[i] = MLGF_MAXNUM_VALIDBITS;

    mlgf_GetMortonValue(handle, keys, nValidBits, &morton, kdesc->nKeys);


    /* call recursion */
    e = mlgf_SearchNearObjectRecursive(handle, rootPid, kdesc, &morton, oid); 
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* MLGF_SearchNearObject() */



/*
 * Function: Four mlgf_SearchNearObjectRecursive(PageID*, MLGF_KeyDesc*,
 *                                               mlgf_MortonValue*, ObjectID*)
 *
 * Description:
 *  For the given object with its morton order value, returns its near object.
 *
 * Returns:
 *  Error code
 *    Some errors caused by function calls
 */
Four mlgf_SearchNearObjectRecursive(
    Four			handle,
    PageID         		*root,		/* IN current page of recursive step */
    MLGF_KeyDesc   		*kdesc,		/* IN key descriptor of a MLGF index */
    mlgf_MortonValue 		*morton,	/* IN Morton Order value for the new object */
    ObjectID       		*oid)		/* OUT found near object */
{
    Four 			e;		/* error code */
    Four 			entryNo;	/* index to entry */
    PageID 			child;		/* PageID of the child page */
    PageID 			ovPid;		/* PageID of the first page of overflow chain */
    mlgf_Page 			*apage;		/* an MLGF page */
    Buffer_ACC_CB 		*bcb;		/* buffer cotnrol block for the current page */
    mlgf_LeafEntry 		*leafEntry;	/* a leaf entry */
    mlgf_DirectoryEntry 	*entryToChild; 	/* directory entry pointing to a child */


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_SearchNearObjectRecursive(root=%P, kdesc=%P, morton=%P, oid=%P",
	      root, kdesc, morton, oid));


    /* Read the current page into the buffer. */
    e = BfM_getAndFixBuffer(handle, root, M_FREE, &bcb, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (mlgf_Page*)bcb->bufPagePtr;

    if (apage->any.hdr.type & MLGF_LEAFPAGE) { /* leaf page */

	mlgf_SearchLeafPageInMortonOrder(handle, &apage->leaf, kdesc, morton, &entryNo);

	if (entryNo == apage->leaf.hdr.nEntries) entryNo--;

	leafEntry = MLGF_ITH_LEAFENTRY(&apage->leaf, entryNo);

	if (leafEntry->nObjects >= 0) {	/* normal entry */

	    *oid = *((ObjectID*)MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, leafEntry, 0));
	} else {	/* overflow entry */

	    MAKE_PAGEID(ovPid, root->volNo,
			MLGF_LEAFENTRY_FIRST_OVERFLOW(kdesc->nKeys, leafEntry));

	    /* Get the ObjectID and its extra data. */
	    e = mlgf_GetObjectFromOverflow(handle, &ovPid, kdesc, oid, NULL);
	    if (e < 0) {
		ERRB1(handle, e, bcb, PAGE_BUF);
	    }
	}

    } else {			/* directory page */

	/* From this point, the page is a directory page. */

	if (apage->directory.hdr.nEntries == 0) {

	    /* Assert that current is the root of the given MLGF index. */

	    /* Return the NIL ObjectID. */
	    SET_NILOBJECTID(*oid);

	} else {

	    /* find region to go next time */
	    (Boolean) mlgf_SearchDirPageInMortonOrder(handle, &apage->directory, kdesc, morton, FALSE, &entryNo); 

	    if (entryNo == apage->directory.hdr.nEntries) entryNo--;

	    entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo,
					     MLGF_DIRENTRY_LENGTH(kdesc->nKeys));

	    MAKE_PAGEID(child, root->volNo, entryToChild->spid);

	    /* go to next page */
	    e = mlgf_SearchNearObjectRecursive(handle, &child, kdesc, morton, oid);
	    if (e < 0) {
		ERRB1(handle, e, bcb, PAGE_BUF);
	    }
	}
    }

    /* Unfix buffer for current page. */
    e = BfM_unfixBuffer(handle, bcb, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_SearchNearObjectRecursive() */
