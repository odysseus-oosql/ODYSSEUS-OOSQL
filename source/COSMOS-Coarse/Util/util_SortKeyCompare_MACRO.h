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
#include <string.h> /* for memcpy */

#define util_SortKeyCompare(          \
    Four                 e,           \
    SortTupleDesc*       sortKeyDesc, \
    void*                p1,          \
    void*                p2)          \
{ \
    int                  result;      \
    One*                 left;        \
    One*                 right;       \
    Four                 i, j;        \
    Four                 kpartSize;   \
    Two                  len1, len2;  \
    short                s1, s2;      \
    int                  i1, i2;      \
    long                 l1, l2;      \
    float                f1, f2;      \
    double               d1, d2;      \
    PageID               pid1, pid2;  \
    OID                  oid1, oid2;  \
    ObjectID             objId1, objId2; \
 \
    if ((p1) == NULL && (p2) == NULL) { result = 0; goto end; } \
    else if ((p1) == NULL)            { result = 1; goto end; } \
    else if ((p2) == NULL)            { result = -1; goto end; } \
 \
    left = &(((SlottedPageForSortTuple *) (p1))->data)[(sortKeyDesc)->hdrSize]; \
    right = &(((SlottedPageForSortTuple *) (p2))->data)[(sortKeyDesc)->hdrSize]; \
 \
    for (i = 0; i < (sortKeyDesc)->nparts; i++ ) { \
 \
        result = 0; \
 \
        switch ( (sortKeyDesc)->parts[i].type ) { \
 \
          case SM_SHORT : \
            memcpy(&s1, left, sizeof(short)); \
            memcpy(&s2, right, sizeof(short)); \
            if (s1 > s2)      result = 1; \
            else if (s1 < s2) result = -1; \
            kpartSize = sizeof(short); \
            break; \
 \
          case SM_INT : \
            memcpy(&i1, left, sizeof(int)); \
            memcpy(&i2, right, sizeof(int)); \
            if (i1 > i2)      result = 1; \
            else if (i1 < i2) result = -1; \
            kpartSize = sizeof(int); \
            break; \
 \
          case SM_LONG : \
            memcpy(&l1, left, sizeof(long)); \
            memcpy(&l2, right, sizeof(long)); \
            if (l1 > l2)      result = 1; \
            else if (l1 < l2) result = -1; \
            kpartSize = sizeof(long); \
            break; \
 \
          case SM_FLOAT : \
            memcpy(&f1, left, sizeof(float)); \
            memcpy(&f2, right, sizeof(float)); \
            if (f1 > f2)      result = 1; \
            else if (f1 < f2) result = -1; \
            kpartSize = sizeof(float); \
            break; \
 \
          case SM_DOUBLE : \
            memcpy(&d1, left, sizeof(double)); \
            memcpy(&d2, right, sizeof(double)); \
            if (d1 > d2)      result = 1; \
            else if (d1 < d2) result = -1; \
            kpartSize = sizeof(double); \
            break; \
 \
          case SM_STRING : \
            result = memcmp(left, right, (sortKeyDesc)->parts[i].length); \
            if (result > 0)      result = 1; \
            else if (result < 0) result = -1; \
            kpartSize = (sortKeyDesc)->parts[i].length; \
            break; \
 \
          case SM_VARSTRING : \
            memcpy(&len1, left, sizeof(Two)); \
            memcpy(&len2, right, sizeof(Two)); \
            left += sizeof(Two); \
            right += sizeof(Two); \
            result = memcmp(left, right, MIN(len1,len2)); \
            if (result > 0)       result = 1; \
            else if (result < 0)  result = -1; \
            if (len1 > len2)      result = 1; \
            else if (len1 < len2) result = -1; \
            kpartSize = MIN(len1,len2); \
            break; \
 \
          case SM_PAGEID: \
          case SM_FILEID: \
          case SM_INDEXID: \
            memcpy((char*)&pid1, (char*)left, SM_PAGEID_SIZE); \
            memcpy((char*)&pid2, (char*)right, SM_PAGEID_SIZE); \
            if (pid1.volNo > pid2.volNo)        result = 1; \
            else if (pid1.volNo < pid2.volNo)   result = -1; \
            if (pid1.pageNo > pid2.pageNo)      result = 1; \
            else if (pid1.pageNo < pid2.pageNo) result = -1; \
            kpartSize = SM_PAGEID_SIZE; \
            break; \
 \
          case SM_OID :  \
            memcpy((char*)&oid1, (char*)left, SM_OID_SIZE); \
            memcpy((char*)&oid2, (char*)right, SM_OID_SIZE); \
            if (oid1.volNo > oid2.volNo)        result = 1; \
            else if (oid1.volNo < oid2.volNo)   result = -1; \
            if (oid1.pageNo > oid2.pageNo)      result = 1; \
            else if (oid1.pageNo < oid2.pageNo) result = -1; \
            if (oid1.slotNo > oid2.slotNo)      result = 1; \
            else if (oid1.slotNo < oid2.slotNo) result = -1; \
            kpartSize = SM_OID_SIZE; \
            break; \
 \
          case SM_OBJECT_ID :  \
            memcpy((char*)&objId1, (char*)left, SM_OBJECT_ID_SIZE); \
            memcpy((char*)&objId2, (char*)right, SM_OBJECT_ID_SIZE); \
            if (objId1.volNo > objId2.volNo)        result = 1; \
            else if (objId1.volNo < objId2.volNo)   result = -1; \
            if (objId1.pageNo > objId2.pageNo)      result = 1; \
            else if (objId1.pageNo < objId2.pageNo) result = -1; \
            if (objId1.slotNo > objId2.slotNo)      result = 1; \
            else if (objId1.slotNo < objId2.slotNo) result = -1; \
            kpartSize = SM_OBJECT_ID_SIZE; \
            break; \
        }  \
 \
        if (result != 0) { \
            if ((sortKeyDesc)->parts[i].flag & SORTKEYDESC_ATTR_DESC) result *= -1; \
            break; \
        } \
 \
        left += kpartSize; \
        right += kpartSize; \
    } \
 \
end : \
    e = result; \
}
