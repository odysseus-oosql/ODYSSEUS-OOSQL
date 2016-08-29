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

#ifndef _OOSQL_MINMAXHEAP_H_
#define _OOSQL_MINMAXHEAP_H_

/*
    MODULE:
        OOSQL_MinMaxHeap.hxx

    DESCRIPTION:
        This header defines the OOSQL_MinMaxHeap. 
*/


/* includes internally used data structures */
#include "OOSQL_MinMaxHeap_Internal.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

class OOSQL_MinMaxHeap_EntryDesc : public OOSQL_MemoryManagedObject {
/*
 * DESCRIPTION:
 *  This struct enables the user to describe the heap entry structure.
 */
public:
	OOSQL_MinMaxHeap_EntryDesc();
    virtual ~OOSQL_MinMaxHeap_EntryDesc();

    Four						nFields;        // the # of fields in a entry
    Four						keyField;       // key field number
    OOSQL_MinMaxHeap_FieldInfo  fieldInfo[1];   // type information of each field
};


class OOSQL_MinMaxHeap : public OOSQL_MemoryManagedObject {
/* 
 * DESCRIPTION:
 *  This class defines the Min Heap and provides member functions to manipulate it.
 *
 *  NOTE: The Min Max Heap is a complete binary tree which guarantees the following properties.
 *
 *        1) the root node has the smallest key in the heap
 *        2) the second level nodes has two greatest keys int the heap
 *        3) the first level is min level, and the next level is max level and so on.
 *
 */
private:
	OOSQL_StorageManager*	m_storageManager;
    Four					maxHeapSize;        /* current heap size */
    Four					lastEntryIndex;     /* last entry number in heap */

    /* information of external storage which contains the heap data
     * NOTE: we now implemented an external storage by a temporary file.
     */
    OOSQL_MinMaxHeap_ExternalStorageInfo  storage;

    /* description of heap entry structure */
    OOSQL_MinMaxHeap_EntryDesc            *entryDesc;

    /*
     * internally used member functions
     */

    /* functions to manipulate the heap as an 1-dim. array */
    Four    fetchIthEntry(Four index, OOSQL_MinMaxHeap_FieldList* entry);
    Four    fetchIthEntryKeyValue(Four index, OOSQL_MinMaxHeap_KeyValue* entry);
    Four    updateIthEntry(Four index, OOSQL_MinMaxHeap_FieldList* entry);

    Four    exchangeEntries(Four index1, Four index2);

    Four    encodeEntry(OOSQL_MinMaxHeap_FieldList* entry, char* object);
    Four    decodeEntry(char* object, OOSQL_MinMaxHeap_FieldList* entry);
    Four    encodeKey(OOSQL_MinMaxHeap_KeyValue* key, char* object);
    Four    decodeKey(char* object, OOSQL_MinMaxHeap_KeyValue* key);

    /* functions to insert, delete, and replace entry */
    Four    getLevelType(Four index);
    Four    insertEntry(OOSQL_MinMaxHeap_FieldList* entry);
    Four    deleteLastEntry();

    /* functions to maintain the Min Heap properties */
    Four    minOfChildrenAndGrandchildren(Four index);  // subfunction used in minHeapifyFromTop
    Four    maxOfChildrenAndGrandchildren(Four index);  // subfunction used in maxHeapifyFromTop

    Four    minHeapifyFromTop(Four rootNodeIndex);
    Four    minHeapifyFromBottom(Four bottomNodeIndex);
    Four    maxHeapifyFromTop(Four rootNodeIndex);
    Four    maxHeapifyFromBottom(Four bottomNodeIndex);

    /* key comparison */
    Four    keyAssign(Two type, Four length, OOSQL_MinMaxHeap_KeyValue* key1, OOSQL_MinMaxHeap_KeyValue* key2);
    Four    keyCompare(Two type, OOSQL_MinMaxHeap_KeyValue* key1, OOSQL_MinMaxHeap_KeyValue* key2);

    /* miscellaneous */
    Boolean isCreated();
    Boolean isEmpty();
    Boolean isOutOfBound(Four index);

public:
    /* constructor and destructor */
    OOSQL_MinMaxHeap();
    virtual ~OOSQL_MinMaxHeap();

    Four    IsEmpty();

    Four    GetHeapSize() { return lastEntryIndex; }

    Four    Create(Four, OOSQL_MinMaxHeap_EntryDesc* );

    Four    Destroy();

    Four    InsertEntry(OOSQL_MinMaxHeap_FieldList* );

    Four    Dump();

    /* min heap interface */
    Four    RetrieveMinEntry(OOSQL_MinMaxHeap_FieldList* );

    Four    RetrieveMinKeyValue(OOSQL_MinMaxHeap_KeyValue* );

    Four    ExtractMinEntry(OOSQL_MinMaxHeap_FieldList* );

    Four    ReplaceMinEntry(OOSQL_MinMaxHeap_FieldList* );

    /* max heap interface */
    Four    RetrieveMaxEntry(OOSQL_MinMaxHeap_FieldList* );

    Four    RetrieveMaxKeyValue(OOSQL_MinMaxHeap_KeyValue* );

    Four    ExtractMaxEntry(OOSQL_MinMaxHeap_FieldList* );

    Four    ReplaceMaxEntry(OOSQL_MinMaxHeap_FieldList* );
};

#endif  /* _OOSQL_MINMAXHEAP_H_ */

