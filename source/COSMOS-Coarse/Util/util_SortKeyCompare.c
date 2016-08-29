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
 * Module : util_SortKeyCompare.c
 *
 * Description :
 *  compare two tuples
 *
 * Exports :
 *  Four util_SortKeyCompare(void*, void*)
 */


#include <string.h> /* for memcpy */
#include <assert.h> /* for assert */

#include "common.h"
#include "Util_Sort.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*==============================================
 * util_SortKeyCompare()
 *=============================================*/

/*
 * Function : util_SortKeyCompare(SortTupleDesc*, void*, void*)
 *
 * Description :
 *  Compare two tuples.
 *
 * Assume :
 *  1) sort key is given by global variable
 *  2) tuple encoding method is fixed
 *  3) order of attributes in 'attrinfo' and in 'sortKeyDesc' are same
 *  4) if 'ascending order', larger one is larger one
 *     but if 'descending order', smaller one is larger one!!
 *         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 * Return Value :
 *  1) -1, if p1 < p2
 *  2)  0, if p1 = p2
 *  3)  1, if p1 > p2
 *
 * Side Effects :
 *  None.
 */
Four util_SortKeyCompare(
    Four handle,
    SortTupleDesc*       sortKeyDesc,    /* IN */
    void*                p1,             /* IN pointer which points object containing tuple1 */
    void*                p2)             /* IN pointer which points object containing tuple2 */
{
    Four                 result;         /* comparision result */ 
    char*                left;           /* pointer which moves in tuple1 */
    char*                right;          /* pointer which moves in tuple2 */
    Two                  i;              /* index variables */
    Two                  j;              /* index variables */
    Two                  kpartSize;      /* size of key part */
    Two                  len1, len2;     /* length of variable length attribute */
    Two_Invariable       s1, s2;         /* variables for '2-byte Short' type */
    Four_Invariable      i1, i2;         /* variables for '4-byte Int' type */
    Four_Invariable      l1, l2;         /* variables for '4-byte Long' type */
    Eight_Invariable     ll1, ll2;       /* variables for '8-byte Long Long' type */
    float                f1, f2;         /* variables for 'float' type */
    double               d1, d2;         /* variables for 'double' type */
    PageID               pid1, pid2;
    OID                  oid1, oid2;
    ObjectID             objId1, objId2; 


    /* NULL is always larger than any value - for 'Tree of Loser' */
    if (p1 == NULL && p2 == NULL) return 0;
    else if (p1 == NULL)          return 1;
    else if (p2 == NULL)          return -1;

    /* move pointer 'left' & 'right' to start point of tuple */
    left = &(((SlottedPageForSortTuple *) p1)->data)[sortKeyDesc->hdrSize];
    right = &(((SlottedPageForSortTuple *) p2)->data)[sortKeyDesc->hdrSize];

    /* for each key part, compare it!! */
    for (i = 0; i < sortKeyDesc->nparts; i++ ) {

        /* assertion check */
        assert(sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC ||
               sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC);

        switch ( sortKeyDesc->parts[i].type ) {

          /* in case of 'Short' type */
          case SM_SHORT :

            memcpy(&s1, left, sizeof(Two_Invariable)); 
            memcpy(&s2, right, sizeof(Two_Invariable)); 

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (s1 > s2)      return(1);
                else if (s1 < s2) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (s1 < s2)      return(1);
                else if (s1 > s2) return(-1);
            }

            kpartSize = sizeof(Two_Invariable); 

            break;


          /* in case of 'Int' type */
          case SM_INT :

            memcpy(&i1, left, sizeof(Four_Invariable)); 
            memcpy(&i2, right, sizeof(Four_Invariable)); 

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (i1 > i2)      return(1);
                else if (i1 < i2) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (i1 < i2)      return(1);
                else if (i1 > i2) return(-1);
            }

            kpartSize = sizeof(Four_Invariable); 

            break;


          /* in case of 'Long' type */
          case SM_LONG :

            memcpy(&l1, left, sizeof(Four_Invariable)); 
            memcpy(&l2, right, sizeof(Four_Invariable)); 

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (l1 > l2)      return(1);
                else if (l1 < l2) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (l1 < l2)      return(1);
                else if (l1 > l2) return(-1);
            }

            kpartSize = sizeof(Four_Invariable);

            break;


          /* in case of 'Long Long' type */
          case SM_LONG_LONG : 

            memcpy(&ll1, left, sizeof(Eight_Invariable)); 
            memcpy(&ll2, right, sizeof(Eight_Invariable)); 

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (ll1 > ll2)      return(1);
                else if (ll1 < ll2) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (ll1 < ll2)      return(1);
                else if (ll1 > ll2) return(-1);
            }

            kpartSize = sizeof(Eight_Invariable); 

            break;


          /* in case of 'Float' type */
          case SM_FLOAT :

            memcpy(&f1, left, sizeof(float));
            memcpy(&f2, right, sizeof(float));

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (f1 > f2)      return(1);
                else if (f1 < f2) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (f1 < f2)      return(1);
                else if (f1 > f2) return(-1);
            }

            kpartSize = sizeof(float);

            break;


          /* in case of 'Double' type */
          case SM_DOUBLE :

            memcpy(&d1, left, sizeof(double));
            memcpy(&d2, right, sizeof(double));

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (d1 > d2)      return(1);
                else if (d1 < d2) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (d1 < d2)      return(1);
                else if (d1 > d2) return(-1);
            }

            kpartSize = sizeof(double);

            break;


          /* in case of 'Fixed String' type */
          case SM_STRING :

            result = memcmp(left, right, sortKeyDesc->parts[i].length);

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (result > 0)      return(1);
                else if (result < 0) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (result < 0)      return(1);
                else if (result > 0) return(-1);
            }

            kpartSize = sortKeyDesc->parts[i].length;

            break;


          /* in case of 'Variable String' type */
          case SM_VARSTRING :

            memcpy(&len1, left, sizeof(Two));
            memcpy(&len2, right, sizeof(Two));

            left += sizeof(Two);
            right += sizeof(Two);

            result = memcmp(left, right, MIN(len1,len2));

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (result > 0)      return(1);
                else if (result < 0) return(-1);

                if (len1 > len2)      return(1);
                else if (len1 < len2) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (result < 0)      return(1);
                else if (result > 0) return(-1);

                if (len1 < len2)      return(1);
                else if (len1 > len2) return(-1);
            }

            /* Note!! at this point, two variable string is exactly equal */
            kpartSize = MIN(len1,len2);

            break;

          /* in case of 'PageID, FileID, IndexID' type */
          case SM_PAGEID:
          case SM_FILEID:
          case SM_INDEXID:

            memcpy((char*)&pid1, (char*)left, SM_PAGEID_SIZE);
            memcpy((char*)&pid2, (char*)right, SM_PAGEID_SIZE);

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {

                if (pid1.volNo > pid2.volNo) return(1);
                else if (pid1.volNo < pid2.volNo) return(-1);

                if (pid1.pageNo > pid2.pageNo) return(1);
                else if (pid1.pageNo < pid2.pageNo) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {

                if (pid1.volNo < pid2.volNo) return(1);
                else if (pid1.volNo > pid2.volNo) return(-1);

                if (pid1.pageNo < pid2.pageNo) return(1);
                else if (pid1.pageNo > pid2.pageNo) return(-1);
            }

            kpartSize = SM_PAGEID_SIZE;

            break;


          /* in case of 'OID' type */
          case SM_OID :

            memcpy((char*)&oid1, (char*)left, SM_OID_SIZE);
            memcpy((char*)&oid2, (char*)right, SM_OID_SIZE);

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {

                if (oid1.volNo > oid2.volNo) return(1);
                else if (oid1.volNo < oid2.volNo) return(-1);

                if (oid1.pageNo > oid2.pageNo) return(1);
                else if (oid1.pageNo < oid2.pageNo) return(-1);

                if (oid1.slotNo > oid2.slotNo) return(1);
                else if (oid1.slotNo < oid2.slotNo) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {

                if (oid1.volNo < oid2.volNo) return(1);
                else if (oid1.volNo > oid2.volNo) return(-1);

                if (oid1.pageNo < oid2.pageNo) return(1);
                else if (oid1.pageNo > oid2.pageNo) return(-1);

                if (oid1.slotNo < oid2.slotNo) return(1);
                else if (oid1.slotNo > oid2.slotNo) return(-1);
            }

            kpartSize = SM_OID_SIZE;

            break;

          /* in case of 'ObjectID' type */
          case SM_OBJECT_ID :

            memcpy((char*)&objId1, (char*)left, SM_OBJECT_ID_SIZE);
            memcpy((char*)&objId2, (char*)right, SM_OBJECT_ID_SIZE);

            if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {

                if (objId1.volNo > objId2.volNo) return(1);
                else if (objId1.volNo < objId2.volNo) return(-1);

                if (objId1.pageNo > objId2.pageNo) return(1);
                else if (objId1.pageNo < objId2.pageNo) return(-1);

                if (objId1.slotNo > objId2.slotNo) return(1);
                else if (objId1.slotNo < objId2.slotNo) return(-1);
            }
            else if (sortKeyDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {

                if (objId1.volNo < objId2.volNo) return(1);
                else if (objId1.volNo > objId2.volNo) return(-1);

                if (objId1.pageNo < objId2.pageNo) return(1);
                else if (objId1.pageNo > objId2.pageNo) return(-1);

                if (objId1.slotNo < objId2.slotNo) return(1);
                else if (objId1.slotNo > objId2.slotNo) return(-1);
            }

            kpartSize = SM_OBJECT_ID_SIZE;

            break;

        } /* switch */

        left += kpartSize;
        right += kpartSize;

    } /* for */


    return(0);

} /* util_SortKeyCompare() */
