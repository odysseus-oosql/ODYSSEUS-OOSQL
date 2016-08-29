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
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
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

#ifndef _OOSQL_MINMAXHEAP_INTERNAL_H_
#define _OOSQL_MINMAXHEAP_INTERNAL_H_

/*
    MODULE:
        MinMaxHeap_Internal.hxx

    DESCRIPTION:
        This header defines classes that are internally used for 'MinHeap' class
        which is defined in "MinMaxHeap.hxx".
*/

#include "OOSQL_Common.h"
#include "OOSQL_StorageManager.hxx"

/*
 * internally used constant definitions
 */
#define MINMAXHEAP_ROOTINDEX            1
#define MINMAXHEAP_IS_LEFTCHILD(i)      (~(i) & 0x1)
#define MINMAXHEAP_IS_RIGHTCHILD(i)     ((i) & 0x1)

/*
 * internally used macro definitions
 */
#define MINMAXHEAP_GET_PARENT(i)            ((Four)(i) / 2) /* floor of i/2 */
#define MINMAXHEAP_GET_LEFTCHILD(i)         (2 * (i))
#define MINMAXHEAP_GET_RIGHTCHILD(i)        (2 * (i) + 1)

#define MINMAXHEAP_INDEX_TO_OFFSET(i, l)    ((i - 1) * (l) + 1);
#define MINMAXHEAP_KEYTYPE                                      (entryDesc->fieldInfo[entryDesc->keyField].type)
#define MINMAXHEAP_KEYLENGTH                            (entryDesc->fieldInfo[entryDesc->keyField].length)

/* 
 *  LEVEL TYPE
 */
enum {MINMAXHEAP_MIN_LEVEL, MINMAXHEAP_MAX_LEVEL};

/*
 * type definitions that hides lower layers.
 */

#define eNOERROR                 0
#define eINTERNAL_ERR           -1
#define eHEAP_EMPTY             -2
#define eBAD_PARAMETER          -3
#define eHEAP_ALREADY_CREATED   -4
#define eHEAP_WAS_NOT_CREATED   -5
#define eCANNOT_BE_COMPARED     -6
#define eMEMORY_ERROR           -7

#define ERR(e)                                                              \
    {                                                                       \
        printf("Error Code:%ld File:%s Line:%ld\n", (e), __FILE__, __LINE__); \
        fflush(stdout);                                                     \
        if(1) return(e);                                                    \
    }

#define ERR_EXIT(e)                                                         \
    {                                                                       \
        printf("Error Code:%ld File:%s Line:%ld\n", (e), __FILE__, __LINE__); \
        fflush(stdout);                                                     \
        exit(1);                                                            \
    }

#define CHECK_ERR(e)                                                        \
    if(e < eNOERROR)    ERR(e);

typedef OOSQL_StorageManager::ColListStruct   OOSQL_MinMaxHeap_FieldList;
typedef OOSQL_StorageManager::ColListStruct   OOSQL_MinMaxHeap_KeyValue;

struct  OOSQL_MinMaxHeap_FieldInfo {
    Two     type;
    Four    length;
};


struct  OOSQL_MinMaxHeap_Key {
/*
 * DESCRIPTION:
 *  This struct contains key value information of a heap entry.
 */
    Four                entryIndex; // array index of the entry
    OOSQL_MinMaxHeap_KeyValue keyVal;     // pointer to key value
};

/* 
 * temp file prefix 
 */
#define OOSQL_MINMAXHEAP_TEMPFILE_PREFIX  "minMaxHeapTempFile"

struct  OOSQL_MinMaxHeap_ExternalStorageInfo {
/*
 * DESCRIPTION:
 *  This struct describes all necessary information to manipulate 
 *  the temporary file which contains actual data of the heap.
 */
    /* temporary file information */
    Four						dbID;                       // database id (same as volume id)
    char						tempFileName[MAXCLASSNAME]; // file name
    Four						ocn;                        // open class number
    Four						scanID;                     // sequential scanId
    OOSQL_StorageManager::OID   oid;                        // oid of an object that has varstring

    Four						keyLength;                  // length of key of an entry
    char						*entryBuffer[2];            // encoded entry1
    Four						entryLength;                // length of an entry
};

#endif  /* _OOSQL_MINMAXHEAP_INTERNAL_H_ */



