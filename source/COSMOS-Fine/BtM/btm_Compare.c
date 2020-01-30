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
 * Module: btm_Compare.c
 *
 * Description :
 *  This file includes two compare routines, one for keys used in Btree Index
 *  and another for ObjectIDs.
 *
 * Exports:
 *  Four btm_KeyCompare(Four, KeyDesc*, KeyValue*, KeyValue*)
 *  Four btm_ObjectIdComp(Four, ObjectID*, ObjectID*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * btm_KeyCompare( )
 *================================*/
/*
 * Function: Four btm_KeyCompare(Four, KeyDesc*, KeyValue*, KeyValue*)
 *
 * Description:
 *  Compare key1 with key2.
 *  key1 and key2 are described by the given parameter "kdesc".
 *
 * Returns:
 *  result of omparison (positive numbers)
 *    EQUAL : key1 and key2 are same
 *    GREAT : key1 is greater than key2
 *    LESS  : key1 is less than key2
 *
 * Note:
 *  We assume that the input data are all valid.
 *  User should check the KeyDesc is valid.
 */
Four btm_KeyCompare(
    Four 			handle,
    KeyDesc 			*kdesc,		/* IN key descriptor for key1 and key2 */
    KeyValue 			*key1,		/* IN the first key value */
    KeyValue 			*key2)		/* IN the second key value */
{
    register unsigned char 	*left; 		/* left key value */
    register unsigned char 	*right; 	/* right key value */
    Four 			i;		/* index for # of key parts */
    Four 			j;		/* temporary variable */
    Four 			kpartSize;	/* size of the current kpart */
    Two  			len1, len2;	/* string length */
    Two_Invariable       	s1, s2;         /* variables for '2byte-Short' type */
    Four_Invariable      	i1, i2;         /* variables for '4byte-Int' type */
    Four_Invariable      	l1, l2;         /* variables for '4byte-Long' type */
    Eight_Invariable	 	ll1, ll2;       /* variables for '8byte-Long' type */
    float                	f1, f2;         /* variables for 'float' type */
    double               	d1, d2;         /* variables for 'double' type */
    PageID 			pid1, pid2;	/* PageID values */
    OID    			oid1, oid2;	/* OID values */ 
    Four 			rtn;            /* return value */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_KeyCompare(kdesc=%P, key1=%P, key2=%P)", kdesc, key1, key2));

    /* Sequentially compare each key parts.
     *  If the first satisfying key part is found return TRUE
     */
    rtn = EQUAL;                /* initialize return value */
    left = (unsigned char*)&(key1->val[0]);
    right = (unsigned char*)&(key2->val[0]);

    for(i = 0;i < kdesc->nparts;i++) {

	switch (kdesc->kpart[i].type & SM_TYPE_MASK) {
	  case SM_SHORT:
	    memcpy(&s1, left, sizeof(Two_Invariable));
	    memcpy(&s2, right, sizeof(Two_Invariable));

            if (s1 != s2) rtn = (s1 > s2) ? GREAT : LESS;

	    kpartSize = sizeof(Two_Invariable);

	    break;

	  case SM_INT:
	    memcpy(&i1, left, sizeof(Four_Invariable));
	    memcpy(&i2, right, sizeof(Four_Invariable));

            if (i1 != i2) rtn = (i1 > i2) ? GREAT : LESS;

	    kpartSize = sizeof(Four_Invariable);

	    break;

	  case SM_LONG:
	    memcpy(&l1, left, sizeof(Four_Invariable));
	    memcpy(&l2, right, sizeof(Four_Invariable));

            if (l1 != l2) rtn = (l1 > l2) ? GREAT : LESS;

	    kpartSize = sizeof(Four_Invariable);

	    break;

          case SM_LONG_LONG:
	    memcpy(&ll1, left, sizeof(Eight_Invariable));
	    memcpy(&ll2, right, sizeof(Eight_Invariable));

            if (ll1 != ll2) rtn = (ll1 > ll2) ? GREAT : LESS;

	    kpartSize = sizeof(Eight_Invariable);

	    break;

	  case SM_FLOAT:
	    memcpy(&f1, left, sizeof(float));
	    memcpy(&f2, right, sizeof(float));

            if (f1 != f2) rtn = (f1 > f2) ? GREAT : LESS;

	    kpartSize = sizeof(float);

	    break;

	  case SM_DOUBLE:
	    memcpy(&d1, left, sizeof(double));
	    memcpy(&d2, right, sizeof(double));

            if (d1 != d2) rtn = (d1 > d2) ? GREAT : LESS;

	    kpartSize = sizeof(double);

	    break;

	  case SM_STRING:	/* fixed length string */
	    for (j = 0; j < kdesc->kpart[i].length; j++, left++, right++) {
                if (*left != *right) {
                    rtn = (*left > *right) ? GREAT : LESS;
                    break;
                }
	    }

	    kpartSize = 0;	/* we already changed the pointers */

	    break;

	  case SM_VARSTRING:	/* variable length string */
	    /* get the string length */
	    memcpy(&len1, left, sizeof(Two));
	    memcpy(&len2, right, sizeof(Two));

	    left += sizeof(Two);
	    right += sizeof(Two);

	    for (j = MIN(len1, len2); j > 0; j--, left++, right++) {
                if (*left != *right) {
                    rtn = (*left > *right) ? GREAT : LESS;
                    break;
                }
	    }

	    /* left and right strings are same in MIN(len1, len2) bytes */
            if (j == 0 && len1 != len2) rtn = (len1 > len2) ? GREAT : LESS;

	    kpartSize = 0;	/* we already changed the pointers */

	    break;

	  case SM_PAGEID:
	  case SM_FILEID:
	  case SM_INDEXID:
	    memcpy(&pid1, left, SM_PAGEID_SIZE);
	    memcpy(&pid2, right, SM_PAGEID_SIZE);

            if (pid1.volNo != pid2.volNo) rtn = (pid1.volNo > pid2.volNo) ? GREAT : LESS;

            if (pid1.pageNo != pid2.pageNo) rtn = (pid1.pageNo > pid2.pageNo) ? GREAT : LESS;

	    kpartSize = sizeof(PageID);

	    break;

	  case SM_OID:
	    memcpy(&oid1, left, SM_OID_SIZE);
	    memcpy(&oid2, right, SM_OID_SIZE);

            if (oid1.volNo != oid2.volNo) rtn = (oid1.volNo > oid2.volNo) ? GREAT : LESS;
            if (oid1.pageNo != oid2.pageNo) rtn = (oid1.pageNo > oid2.pageNo) ? GREAT : LESS;
            if (oid1.slotNo != oid2.slotNo) rtn = (oid1.slotNo > oid2.slotNo) ? GREAT : LESS;

	    /* If the three fields have the same values, then two OIDs must be same. */
	    /* That is, the other two fields unique and classID should be same. */

	    kpartSize = sizeof(OID);

	    break;
	}

        if (rtn != EQUAL) {
            if (kdesc->kpart[i].type & SM_DESC) {
                /* reverse the order */
                rtn = (rtn == GREAT) ? LESS : GREAT;
            }
            return(rtn);
        }

	left += kpartSize;
	right += kpartSize;
    }

    return(EQUAL);

}   /* btm_KeyCompare() */



/*@================================
 * btm_ObjectIdComp( )
 *================================*/
/*
 * Fucntion: Four btm_ObjectIdComp(Four, ObjectID*, ObjectID*)
 *
 * Description:
 *  Compare the first ObjectID with the second ObjectID.
 *
 * Returns:
 *  result of comparison
 *    EQUAL : firstOid and secondOid are same
 *    GREAT : firstOid is greater than secondOid
 *    LESS  : firstOid is less than secondOid
 */
Four btm_ObjectIdComp(
    Four 		handle,
    ObjectID 		*firstOid,		/* IN an object to compare */
    ObjectID 		*secondOid)		/* IN the other object to compare */
{
    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_ObjectIdComp(handle, firstOid=%P, secondOid=%P)",
	      firstOid, secondOid));

    /*@ Consider PageIDs, if they are identical, compare offsets */
    if (firstOid->volNo < secondOid->volNo) return(LESS);
    else if (firstOid->volNo > secondOid->volNo) return(GREAT);
    else if( firstOid->pageNo < secondOid->pageNo) return(LESS);
    else if( firstOid->pageNo > secondOid->pageNo) return(GREAT);
    else if( firstOid->slotNo < secondOid->slotNo) return(LESS);
    else if( firstOid->slotNo > secondOid->slotNo) return(GREAT);

    return(EQUAL);

} /* btm_ObjectIdComp() */

