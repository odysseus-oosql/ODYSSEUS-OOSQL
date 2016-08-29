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

#include <stdio.h>
#include <math.h>
#include <string.h>
#include "OOSQL_MinMaxHeap.hxx"
#include "OOSQL_MinMaxHeap_Internal.hxx"
#include "OOSQL_Error.h"

/*
    MODULE:
        MinMaxHeap_Private.cxx

    DESCRIPTION:
        This module implements private member functions of MinHeap class.

    IMPORTS:

    EXPORTS:
*/

Four    OOSQL_MinMaxHeap::fetchIthEntry( 
    Four                    index,
    OOSQL_MinMaxHeap_FieldList    *entry
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_StorageManager::ColListStruct   clist[1];
    Four            e;
    
#ifdef  TRACE
#endif
    /* check input parameter */
    if(isOutOfBound(index) || entry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if heap created */
    if(!isCreated())
        ERR(eHEAP_WAS_NOT_CREATED);

    /* setup col list struct */
    clist[0].colNo      = 0;
    clist[0].start      = MINMAXHEAP_INDEX_TO_OFFSET(index, storage.entryLength);
    clist[0].dataLength = storage.entryLength;
    clist[0].length     = storage.entryLength;
    clist[0].data.ptr   = storage.entryBuffer[0];

    /* fetch entry */
    e = m_storageManager->FetchObjectByColList(storage.scanID, SM_TRUE, &storage.oid, 1, clist);
    if(e < eNOERROR)    OOSQL_ERR(e);
    
    /* decode entry */
    e = decodeEntry(storage.entryBuffer[0], entry);
    CHECK_ERR(e);

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::fetchIthEntryKeyValue( 
    Four                index,
    OOSQL_MinMaxHeap_KeyValue *keyVal
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_StorageManager::ColListStruct   clist[1];
    Four            e;
    
#ifdef  TRACE
#endif
    /* check input parameter */
    if(isOutOfBound(index) || keyVal == NULL)
        ERR(eBAD_PARAMETER);

    /* check if heap created */
    if(!isCreated())
        ERR(eHEAP_WAS_NOT_CREATED);

    /* setup col list struct */
    clist[0].colNo      = 0;
    clist[0].start      = MINMAXHEAP_INDEX_TO_OFFSET(index, storage.entryLength);
    clist[0].dataLength = storage.entryLength;
    clist[0].length     = storage.entryLength;
    clist[0].data.ptr   = storage.entryBuffer[1];

    /* fetch entry */
    e = m_storageManager->FetchObjectByColList(storage.scanID, SM_TRUE, &storage.oid, 1, clist);
    if(e < eNOERROR)    OOSQL_ERR(e);

    /* decode entry */
    e = decodeKey(storage.entryBuffer[1], keyVal);
    CHECK_ERR(e);
    
    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::updateIthEntry( 
    Four                    index,
    OOSQL_MinMaxHeap_FieldList    *newEntry
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_StorageManager::ColListStruct   clist[1];
    Four            e;
    
#ifdef  TRACE
#endif
    /* check input parameter */
    if(isOutOfBound(index) || newEntry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if heap created */
    if(!isCreated())
        ERR(eHEAP_WAS_NOT_CREATED);

    /* encode entry */
    e = encodeEntry(newEntry, storage.entryBuffer[0]);
    CHECK_ERR(e);

    /* setup col list struct */
	clist[0].nullFlag   = SM_FALSE;
    clist[0].colNo      = 0;
    clist[0].start      = MINMAXHEAP_INDEX_TO_OFFSET(index, storage.entryLength);
    clist[0].dataLength = storage.entryLength;
    if(index < maxHeapSize)
        clist[0].length = storage.entryLength;      // for update it must be entryLength
    else
    {
        clist[0].length = 0;                        // for insert it must be 0
        maxHeapSize ++;
    }
    clist[0].data.ptr   = storage.entryBuffer[0];

    /* fetch entry */
    e = m_storageManager->UpdateObjectByColList(storage.scanID, SM_TRUE, &storage.oid, 1, clist);
    if(e < eNOERROR)    OOSQL_ERR(e);
    
    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::exchangeEntries(
    Four    index1,
    Four    index2
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_StorageManager::ColListStruct   clist1[1];
    OOSQL_StorageManager::ColListStruct   clist2[1];
    Four            e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(isOutOfBound(index1) || isOutOfBound(index2))
        ERR(eBAD_PARAMETER);

    /* check if heap created */
    if(!isCreated())
        ERR(eHEAP_WAS_NOT_CREATED);

    /* setup col list struct */
	clist1[0].nullFlag   = SM_FALSE;
    clist1[0].colNo      = 0;
    clist1[0].start      = MINMAXHEAP_INDEX_TO_OFFSET(index1, storage.entryLength);
    clist1[0].dataLength = storage.entryLength;
    clist1[0].length     = storage.entryLength;     
    clist1[0].data.ptr   = storage.entryBuffer[0];

	clist2[0].nullFlag   = SM_FALSE;
    clist2[0].colNo      = 0;
    clist2[0].start      = MINMAXHEAP_INDEX_TO_OFFSET(index2, storage.entryLength);
    clist2[0].dataLength = storage.entryLength; 
    clist2[0].length     = storage.entryLength;     
    clist2[0].data.ptr   = storage.entryBuffer[1];

    /* fetch entry 1 */
    e = m_storageManager->FetchObjectByColList(storage.scanID, SM_TRUE, &storage.oid, 1, clist1);
    if(e < eNOERROR)    OOSQL_ERR(e);

    /* fetch entry 2 */
    e = m_storageManager->FetchObjectByColList(storage.scanID, SM_TRUE, &storage.oid, 1, clist2);
    if(e < eNOERROR)    OOSQL_ERR(e);
    
    /* exchage position of entry1 and entry2 */
    clist1[0].start = MINMAXHEAP_INDEX_TO_OFFSET(index2, storage.entryLength);
    clist2[0].start = MINMAXHEAP_INDEX_TO_OFFSET(index1, storage.entryLength);

    /* update entry 1 */
    e = m_storageManager->UpdateObjectByColList(storage.scanID, SM_TRUE, &storage.oid, 1, clist1);
    if(e < eNOERROR)    OOSQL_ERR(e);

    /* update entry 2 */
    e = m_storageManager->UpdateObjectByColList(storage.scanID, SM_TRUE, &storage.oid, 1, clist2);
    if(e < eNOERROR)    OOSQL_ERR(e);

    /* return */
    return eNOERROR;
}

Four OOSQL_MinMaxHeap::encodeEntry(OOSQL_MinMaxHeap_FieldList* entry, char* object)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    i;  // index

#ifdef  TRACE
#endif

    for(i = 0; i < entryDesc->nFields; i++)
    {
        switch(entryDesc->fieldInfo[i].type)
        {
          case OOSQL_TYPE_SHORT:
            memcpy(object, &entry[i].data.s, entryDesc->fieldInfo[i].length); 
            break;
          case OOSQL_TYPE_INT:
            memcpy(object, &entry[i].data.i, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_LONG:
            memcpy(object, &entry[i].data.l, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_LONG_LONG:
            memcpy(object, &entry[i].data.ll, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_FLOAT: 
            memcpy(object, &entry[i].data.f, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_DOUBLE:
            memcpy(object, &entry[i].data.d, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_OID:         
            memcpy(object, &entry[i].data.oid, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_PAGEID:    
          case OOSQL_TYPE_FILEID:    
          case OOSQL_TYPE_INDEXID:   
            memcpy(object, &entry[i].data.pid, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_MBR:
            memcpy(object, &entry[i].data.mbr, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_STRING:  
          case OOSQL_TYPE_VARSTRING: 
            memcpy(object, entry[i].data.ptr, entryDesc->fieldInfo[i].length);
            break;
          default:
            ERR(eINTERNAL_ERR);
        }

        object += entryDesc->fieldInfo[i].length;
    }
    return eNOERROR;
}

Four OOSQL_MinMaxHeap::decodeEntry(char* object, OOSQL_MinMaxHeap_FieldList* entry)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    i;  // index

#ifdef  TRACE
#endif

    for(i = 0; i < entryDesc->nFields; i++)
    {
        switch(entryDesc->fieldInfo[i].type)
        {
          case OOSQL_TYPE_SHORT:
            memcpy(&entry[i].data.s, object, entryDesc->fieldInfo[i].length); 
            break;
          case OOSQL_TYPE_INT:
            memcpy(&entry[i].data.i, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_LONG:
            memcpy(&entry[i].data.l, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_LONG_LONG:
            memcpy(&entry[i].data.ll, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_FLOAT: 
            memcpy(&entry[i].data.f, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_DOUBLE:
            memcpy(&entry[i].data.d, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_OID:         
            memcpy(&entry[i].data.oid, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_PAGEID:    
          case OOSQL_TYPE_FILEID:    
          case OOSQL_TYPE_INDEXID:   
            memcpy(&entry[i].data.pid, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_MBR:
            memcpy(&entry[i].data.mbr, object, entryDesc->fieldInfo[i].length);
            break;
          case OOSQL_TYPE_STRING:  
          case OOSQL_TYPE_VARSTRING: 
            memcpy(entry[i].data.ptr, object, entryDesc->fieldInfo[i].length);
            break;
          default:
            ERR(eINTERNAL_ERR);
        }

        object += entryDesc->fieldInfo[i].length;
    }
    return eNOERROR;
}

Four OOSQL_MinMaxHeap::encodeKey(OOSQL_MinMaxHeap_KeyValue* key, char* object)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    i;  // index

#ifdef  TRACE
#endif

    /* setup correct value of object */
    for(i = 0; i < entryDesc->keyField; i++)
    {
        object += entryDesc->fieldInfo[i].length;
    }

    /* assume i has the same value with entryDesc->keyField*/
    switch(entryDesc->fieldInfo[i].type)
    {
      case OOSQL_TYPE_SHORT:
        memcpy(object, &key->data.s, entryDesc->fieldInfo[i].length); 
        break;
      case OOSQL_TYPE_INT:
        memcpy(object, &key->data.i, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_LONG:
        memcpy(object, &key->data.l, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_LONG_LONG:
        memcpy(object, &key->data.ll, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_FLOAT: 
        memcpy(object, &key->data.f, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_DOUBLE:
        memcpy(object, &key->data.d, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_OID:         
        memcpy(object, &key->data.oid, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_PAGEID:    
      case OOSQL_TYPE_FILEID:    
      case OOSQL_TYPE_INDEXID:   
        memcpy(object, &key->data.pid, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_MBR:
        memcpy(object, &key->data.mbr, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_STRING:  
      case OOSQL_TYPE_VARSTRING: 
        memcpy(object, key->data.ptr, entryDesc->fieldInfo[i].length);
        break;
      default:
        ERR(eINTERNAL_ERR);
    }

    return eNOERROR;
}

Four OOSQL_MinMaxHeap::decodeKey(char* object, OOSQL_MinMaxHeap_KeyValue* key)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    i;  // index

#ifdef  TRACE
#endif

    /* setup correct value of object */
    for(i = 0; i < entryDesc->keyField; i++)
    {
        object += entryDesc->fieldInfo[i].length;
    }

    /* assume i has the same value with entryDesc->keyField*/
    switch(entryDesc->fieldInfo[i].type)
    {
      case OOSQL_TYPE_SHORT:
        memcpy(&key->data.s, object, entryDesc->fieldInfo[i].length); 
        break;
      case OOSQL_TYPE_INT:
        memcpy(&key->data.i, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_LONG:
        memcpy(&key->data.l, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_LONG_LONG:
        memcpy(&key->data.ll, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_FLOAT: 
        memcpy(&key->data.f, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_DOUBLE:
        memcpy(&key->data.d, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_OID:         
        memcpy(&key->data.oid, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_PAGEID:    
      case OOSQL_TYPE_FILEID:    
      case OOSQL_TYPE_INDEXID:   
        memcpy(&key->data.pid, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_MBR:
        memcpy(&key->data.mbr, object, entryDesc->fieldInfo[i].length);
        break;
      case OOSQL_TYPE_STRING:  
      case OOSQL_TYPE_VARSTRING: 
        memcpy(key->data.ptr, object, entryDesc->fieldInfo[i].length);
        break;
      default:
        ERR(eINTERNAL_ERR);
    }

    return eNOERROR;
}

Four    OOSQL_MinMaxHeap::getLevelType(
    Four index
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    level;

#ifdef  TRACE
#endif

    level = Four(log(index) / log(2));      // int(log2 index) -> level in complete binary tree

    if(level & 1)       // odd number level  -> max level
        return MINMAXHEAP_MAX_LEVEL;
    else                // even number level -> min level
        return MINMAXHEAP_MIN_LEVEL;
}

Four    OOSQL_MinMaxHeap::insertEntry( 
    OOSQL_MinMaxHeap_FieldList    *newEntry
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  parentKey;
    OOSQL_MinMaxHeap_Key  currKey;
    Four            e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(newEntry == NULL)
        ERR(eBAD_PARAMETER);

    if(isEmpty())
    {
        /* initialize 'lastEntryIndex' */
        lastEntryIndex = 1;

        e = updateIthEntry(1, newEntry);    
        CHECK_ERR(e);
    }
    else
    {
        /* increment 'lastEntryIndex' */
        lastEntryIndex ++;
        
        /* update last entry */
        e = updateIthEntry(lastEntryIndex, newEntry);
        CHECK_ERR(e);

        /* get current entry */
        currKey.entryIndex = lastEntryIndex;
        e = fetchIthEntryKeyValue(currKey.entryIndex, &currKey.keyVal);
        CHECK_ERR(e);

        /* get parent entry */
        parentKey.entryIndex = MINMAXHEAP_GET_PARENT(lastEntryIndex);
        e = fetchIthEntryKeyValue(parentKey.entryIndex, &parentKey.keyVal);
        CHECK_ERR(e);
        
        switch(getLevelType(parentKey.entryIndex))
        {
        case MINMAXHEAP_MIN_LEVEL:
            if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &parentKey.keyVal) < 0)
            {
                e = exchangeEntries(currKey.entryIndex, parentKey.entryIndex);
                CHECK_ERR(e);

                e = minHeapifyFromBottom(parentKey.entryIndex);
                CHECK_ERR(e);
            }
            else
            {
                e = maxHeapifyFromBottom(currKey.entryIndex);
                CHECK_ERR(e);
            }
            break;

        case MINMAXHEAP_MAX_LEVEL:
            if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &parentKey.keyVal) > 0)
            {
                e = exchangeEntries(currKey.entryIndex, parentKey.entryIndex);
                CHECK_ERR(e);

                e = maxHeapifyFromBottom(parentKey.entryIndex);
                CHECK_ERR(e);
            }
            else
            {
                e = minHeapifyFromBottom(currKey.entryIndex);
                CHECK_ERR(e);
            }
            break;

        }
    }
    
    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::deleteLastEntry()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
#ifdef  TRACE
#endif
    /* check if the heap is not empty */
    if(isEmpty())
        ERR(eHEAP_EMPTY);

    /* decrement 'lastEntryIndex' */
    lastEntryIndex --;

    /* return */
    return eNOERROR;
}

Four OOSQL_MinMaxHeap::minOfChildrenAndGrandchildren(Four index)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  leftChild;
    OOSQL_MinMaxHeap_Key  leftChildLeftChild;
    OOSQL_MinMaxHeap_Key  leftChildRightChild;
    OOSQL_MinMaxHeap_Key  rightChild;
    OOSQL_MinMaxHeap_Key  rightChildLeftChild;
    OOSQL_MinMaxHeap_Key  rightChildRightChild;
    OOSQL_MinMaxHeap_Key  minKey;
    Four            e;

#ifdef  TRACE
#endif

    // set entryIndex of each entry
    leftChild.entryIndex            = MINMAXHEAP_GET_LEFTCHILD(index);
    leftChildLeftChild.entryIndex   = MINMAXHEAP_GET_LEFTCHILD(leftChild.entryIndex);
    leftChildRightChild.entryIndex  = MINMAXHEAP_GET_RIGHTCHILD(leftChild.entryIndex);
    rightChild.entryIndex           = MINMAXHEAP_GET_RIGHTCHILD(index);
    rightChildLeftChild.entryIndex  = MINMAXHEAP_GET_LEFTCHILD(rightChild.entryIndex);
    rightChildRightChild.entryIndex = MINMAXHEAP_GET_RIGHTCHILD(rightChild.entryIndex);

    // read each entry's key value
    if(!isOutOfBound(leftChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(leftChild.entryIndex, &leftChild.keyVal);
        CHECK_ERR(e);
    }
    else
        leftChild.entryIndex = -1;  // -1 means invalid entry

    if(!isOutOfBound(leftChildLeftChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(leftChildLeftChild.entryIndex, &leftChildLeftChild.keyVal);   
        CHECK_ERR(e);
    }
    else
        leftChildLeftChild.entryIndex = -1; // -1 means invalid entry

    if(!isOutOfBound(leftChildRightChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(leftChildRightChild.entryIndex, &leftChildRightChild.keyVal); 
        CHECK_ERR(e);
    }
    else
        leftChildRightChild.entryIndex = -1;    // -1 means invalid entry

    if(!isOutOfBound(rightChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(rightChild.entryIndex, &rightChild.keyVal);
        CHECK_ERR(e);
    }
    else
        rightChild.entryIndex = -1; // -1 means invalid entry

    if(!isOutOfBound(rightChildLeftChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(rightChildLeftChild.entryIndex, &rightChildLeftChild.keyVal); 
        CHECK_ERR(e);
    }
    else
        rightChildLeftChild.entryIndex = -1;    // -1 means invalid entry

    if(!isOutOfBound(rightChildRightChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(rightChildRightChild.entryIndex, &rightChildRightChild.keyVal);
        CHECK_ERR(e);
    }
    else
        rightChildRightChild.entryIndex = -1;   // -1 means invalid entry

    // set minKey
    if(leftChild.entryIndex == -1)              // there are no children, an exception
        ERR(eINTERNAL_ERR);

    minKey = leftChild;

    // determin minumum of them
    if(leftChildLeftChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &leftChildLeftChild.keyVal, &minKey.keyVal) < 0)
        minKey = leftChildLeftChild;
    if(leftChildRightChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &leftChildRightChild.keyVal, &minKey.keyVal) < 0)
        minKey = leftChildRightChild;
    if(rightChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &rightChild.keyVal, &minKey.keyVal) < 0)
        minKey = rightChild;
    if(rightChildLeftChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &rightChildLeftChild.keyVal, &minKey.keyVal) < 0)
        minKey = rightChildLeftChild;
    if(rightChildRightChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &rightChildRightChild.keyVal, &minKey.keyVal) < 0)
        minKey = rightChildRightChild;

    return minKey.entryIndex;
}

Four OOSQL_MinMaxHeap::maxOfChildrenAndGrandchildren(Four index)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  leftChild;
    OOSQL_MinMaxHeap_Key  leftChildLeftChild;
    OOSQL_MinMaxHeap_Key  leftChildRightChild;
    OOSQL_MinMaxHeap_Key  rightChild;
    OOSQL_MinMaxHeap_Key  rightChildLeftChild;
    OOSQL_MinMaxHeap_Key  rightChildRightChild;
    OOSQL_MinMaxHeap_Key  maxKey;
    Four            e;

#ifdef  TRACE
#endif

    // set entryIndex of each entry
    leftChild.entryIndex            = MINMAXHEAP_GET_LEFTCHILD(index);
    leftChildLeftChild.entryIndex   = MINMAXHEAP_GET_LEFTCHILD(leftChild.entryIndex);
    leftChildRightChild.entryIndex  = MINMAXHEAP_GET_RIGHTCHILD(leftChild.entryIndex);
    rightChild.entryIndex           = MINMAXHEAP_GET_RIGHTCHILD(index);
    rightChildLeftChild.entryIndex  = MINMAXHEAP_GET_LEFTCHILD(rightChild.entryIndex);
    rightChildRightChild.entryIndex = MINMAXHEAP_GET_RIGHTCHILD(rightChild.entryIndex);

    // read each entry's key value
    if(!isOutOfBound(leftChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(leftChild.entryIndex, &leftChild.keyVal);
        CHECK_ERR(e);
    }
    else
        leftChild.entryIndex = -1;  // -1 means invalid entry

    if(!isOutOfBound(leftChildLeftChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(leftChildLeftChild.entryIndex, &leftChildLeftChild.keyVal);   
        CHECK_ERR(e);
    }
    else
        leftChildLeftChild.entryIndex = -1; // -1 means invalid entry

    if(!isOutOfBound(leftChildRightChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(leftChildRightChild.entryIndex, &leftChildRightChild.keyVal); 
        CHECK_ERR(e);
    }
    else
        leftChildRightChild.entryIndex = -1;    // -1 means invalid entry

    if(!isOutOfBound(rightChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(rightChild.entryIndex, &rightChild.keyVal);
        CHECK_ERR(e);
    }
    else
        rightChild.entryIndex = -1; // -1 means invalid entry

    if(!isOutOfBound(rightChildLeftChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(rightChildLeftChild.entryIndex, &rightChildLeftChild.keyVal); 
        CHECK_ERR(e);
    }
    else
        rightChildLeftChild.entryIndex = -1;    // -1 means invalid entry

    if(!isOutOfBound(rightChildRightChild.entryIndex))
    {
        e = fetchIthEntryKeyValue(rightChildRightChild.entryIndex, &rightChildRightChild.keyVal);
        CHECK_ERR(e);
    }
    else
        rightChildRightChild.entryIndex = -1;   // -1 means invalid entry

    // set minKey
    if(leftChild.entryIndex == -1)              // there are no children, an exception
        ERR(eINTERNAL_ERR);

    maxKey = leftChild;

    // determin minumum of them
    if(leftChildLeftChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &leftChildLeftChild.keyVal, &maxKey.keyVal) > 0)
        maxKey = leftChildLeftChild;
    if(leftChildRightChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &leftChildRightChild.keyVal, &maxKey.keyVal) > 0)
        maxKey = leftChildRightChild;
    if(rightChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &rightChild.keyVal, &maxKey.keyVal) > 0)
        maxKey = rightChild;
    if(rightChildLeftChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &rightChildLeftChild.keyVal, &maxKey.keyVal) > 0)
        maxKey = rightChildLeftChild;
    if(rightChildRightChild.entryIndex != -1 && 
       keyCompare(MINMAXHEAP_KEYTYPE, &rightChildRightChild.keyVal, &maxKey.keyVal) > 0)
        maxKey = rightChildRightChild;

    return maxKey.entryIndex;
}

Four OOSQL_MinMaxHeap::minHeapifyFromTop(
    Four    rootNodeIndex
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  currKey;
    OOSQL_MinMaxHeap_Key  minKey;
    OOSQL_MinMaxHeap_Key  parentKey;
    Four            e;
    Four            i;

#ifdef  TRACE
#endif

    /* check input parameter */

    /* initialize local variables */
    currKey.entryIndex = rootNodeIndex;

    /* read the key value for the 'currKey.entryIndex' */
    e = fetchIthEntryKeyValue(currKey.entryIndex, &currKey.keyVal);
    CHECK_ERR(e);

    /* heapify from root to leaf */
    for(i = rootNodeIndex; i < (lastEntryIndex / 2);)
    {
        /* get minimum entry from children and grand children */
        minKey.entryIndex = minOfChildrenAndGrandchildren(i);
        CHECK_ERR(minKey.entryIndex);
        
        /* read the key value of minimum child */
        e = fetchIthEntryKeyValue(minKey.entryIndex, &minKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &minKey.keyVal) <= 0)
            break;      /* heapified */
        else
        {
            e = exchangeEntries(currKey.entryIndex, minKey.entryIndex);
            CHECK_ERR(e);

            currKey.entryIndex = minKey.entryIndex;

            if(minKey.entryIndex <= MINMAXHEAP_GET_RIGHTCHILD(i))
            {   
                /* minKey is a child of currKey */
                break;  /* heapified */
            }
            else
            {
                /* minKey is a grand child of currKey */
                parentKey.entryIndex = MINMAXHEAP_GET_PARENT(minKey.entryIndex);

                /* read the key value of parent of minKey */
                e = fetchIthEntryKeyValue(parentKey.entryIndex, &parentKey.keyVal);
                CHECK_ERR(e);

                if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &parentKey.keyVal) > 0)
                {
                    e = exchangeEntries(currKey.entryIndex, parentKey.entryIndex);
                    CHECK_ERR(e);
                    
                    e = keyAssign(MINMAXHEAP_KEYTYPE, MINMAXHEAP_KEYLENGTH,
                                  &parentKey.keyVal, &currKey.keyVal);
                    CHECK_ERR(e);
                    
                }

                // update current entry for iteration
                i = minKey.entryIndex;
            }
        }
    }

    return eNOERROR;
}

Four OOSQL_MinMaxHeap::maxHeapifyFromTop(
    Four    rootNodeIndex
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  currKey;
    OOSQL_MinMaxHeap_Key  maxKey;
    OOSQL_MinMaxHeap_Key  parentKey;
    Four            e;
    Four            i;

#ifdef  TRACE
#endif

    /* check input parameter */

    /* initialize local variables */
    currKey.entryIndex = rootNodeIndex;

    /* read the key value for the 'currKey.entryIndex' */
    e = fetchIthEntryKeyValue(currKey.entryIndex, &currKey.keyVal);
    CHECK_ERR(e);

    /* heapify from root to leaf */
    for(i = rootNodeIndex; i < (lastEntryIndex / 2);)
    {
        /* get maximum entry from children and grand children */
        maxKey.entryIndex = maxOfChildrenAndGrandchildren(i);
        CHECK_ERR(maxKey.entryIndex);

        /* read the key value of maximum child */
        e = fetchIthEntryKeyValue(maxKey.entryIndex, &maxKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &maxKey.keyVal) >= 0)
            break;      /* heapified */
        else
        {
            e = exchangeEntries(currKey.entryIndex, maxKey.entryIndex);
            CHECK_ERR(e);

            currKey.entryIndex = maxKey.entryIndex;
            
            if(maxKey.entryIndex <= MINMAXHEAP_GET_RIGHTCHILD(i))
            {   
                /* maxKey is a child of currKey */
                break;  /* heapified */
            }
            else
            {
                /* maxKey is a grand child of currKey */
                parentKey.entryIndex = MINMAXHEAP_GET_PARENT(maxKey.entryIndex);

                /* read the key value of parent of maxKey */
                e = fetchIthEntryKeyValue(parentKey.entryIndex, &parentKey.keyVal);
                CHECK_ERR(e);

                if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &parentKey.keyVal) < 0)
                {
                    e = exchangeEntries(currKey.entryIndex, parentKey.entryIndex);
                    CHECK_ERR(e);

                    e = keyAssign(MINMAXHEAP_KEYTYPE, MINMAXHEAP_KEYLENGTH,
                                  &parentKey.keyVal, &currKey.keyVal);
                    CHECK_ERR(e);
                }

                // update current entry for iteration
                i = maxKey.entryIndex;
            }
        }
    }

    return eNOERROR;
}

Four OOSQL_MinMaxHeap::minHeapifyFromBottom(
    Four    bottomNodeIndex
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  grandParentKey;
    OOSQL_MinMaxHeap_Key  currKey;
    Four            e;

#ifdef  TRACE
#endif

    /* read the key value for the 'currKey.entryIndex' */
    currKey.entryIndex = bottomNodeIndex;
    e = fetchIthEntryKeyValue(currKey.entryIndex, &currKey.keyVal);
    CHECK_ERR(e);

    grandParentKey.entryIndex = bottomNodeIndex / 4;

    while(grandParentKey.entryIndex)
    {
        e = fetchIthEntryKeyValue(grandParentKey.entryIndex, &grandParentKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &grandParentKey.keyVal) < 0)
        {
            e = exchangeEntries(currKey.entryIndex, grandParentKey.entryIndex);
            CHECK_ERR(e);

            // update current entryIndex
            currKey.entryIndex = grandParentKey.entryIndex;

            // update grandParentKey as a grand parent of currKey
            grandParentKey.entryIndex = currKey.entryIndex / 4; 
        }
        else
            break;  // heapified
    }

    return eNOERROR;
}

Four OOSQL_MinMaxHeap::maxHeapifyFromBottom(
    Four    bottomNodeIndex
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  grandParentKey;
    OOSQL_MinMaxHeap_Key  currKey;
    Four            e;

#ifdef  TRACE
#endif

    /* read the key value for the 'currKey.entryIndex' */
    currKey.entryIndex = bottomNodeIndex;
    e = fetchIthEntryKeyValue(currKey.entryIndex, &currKey.keyVal);
    CHECK_ERR(e);

    grandParentKey.entryIndex = bottomNodeIndex / 4;

    while(grandParentKey.entryIndex)
    {
        e = fetchIthEntryKeyValue(grandParentKey.entryIndex, &grandParentKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &currKey.keyVal, &grandParentKey.keyVal) > 0)
        {
            e = exchangeEntries(currKey.entryIndex, grandParentKey.entryIndex);
            CHECK_ERR(e);

            // update current entryIndex
            currKey.entryIndex = grandParentKey.entryIndex;

            // update grandParentKey as a grand parent of currKey
            grandParentKey.entryIndex = MINMAXHEAP_GET_PARENT(MINMAXHEAP_GET_PARENT(currKey.entryIndex));
        }
        else
            break;  // heapified
    }

    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::keyAssign(
    Two                 type,       // IN
    Four                length,     // IN
    OOSQL_MinMaxHeap_KeyValue *key1,      // IN
    OOSQL_MinMaxHeap_KeyValue *key2       // OUT
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    switch(type)
    {
      case OOSQL_TYPE_SHORT:
        key2->data.s = key1->data.s; 
        break;
      case OOSQL_TYPE_INT:
        key2->data.i = key1->data.i;
        break;
      case OOSQL_TYPE_LONG:
        key2->data.l = key1->data.l;
        break;
      case OOSQL_TYPE_LONG_LONG:
        key2->data.ll = key1->data.ll;
        break;
      case OOSQL_TYPE_FLOAT: 
        key2->data.f = key1->data.f;
        break;
      case OOSQL_TYPE_DOUBLE:
        key2->data.d = key1->data.d;
        break;
      case OOSQL_TYPE_OID:         
        memcpy(&key2->data.oid, &key1->data.oid, length);
        break;
      case OOSQL_TYPE_PAGEID:
      case OOSQL_TYPE_FILEID:
      case OOSQL_TYPE_INDEXID:
        memcpy(&key2->data.pid, &key1->data.pid, length);
        break;
      case OOSQL_TYPE_MBR:
        memcpy(&key2->data.mbr, &key1->data.mbr, length);
        break;
      case OOSQL_TYPE_STRING:  
      case OOSQL_TYPE_VARSTRING: 
        memcpy(key2->data.ptr, key1->data.ptr, length);
        break;
      default:
        ERR(eINTERNAL_ERR);
    }
    return eNOERROR;
}

Four    OOSQL_MinMaxHeap::keyCompare(
    Two                 type,
    OOSQL_MinMaxHeap_KeyValue *key1,
    OOSQL_MinMaxHeap_KeyValue *key2
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    switch(type)
    {
      case OOSQL_TYPE_SHORT:
        if(key1->data.s == key2->data.s)
            return 0;
        else if(key1->data.s > key2->data.s)
            return 1;
        else
            return -1;
        
      case OOSQL_TYPE_INT:
        if(key1->data.i == key2->data.i)
            return 0;
        else if(key1->data.i > key2->data.i)
            return 1;
        else
            return -1;
        
      case OOSQL_TYPE_LONG:
        if(key1->data.l == key2->data.l)
            return 0;
        else if(key1->data.l > key2->data.l)
            return 1;
        else
            return -1;
                
      case OOSQL_TYPE_LONG_LONG:
        if(key1->data.ll == key2->data.ll)
            return 0;
        else if(key1->data.ll > key2->data.ll)
            return 1;
        else
            return -1;

      case OOSQL_TYPE_FLOAT: 
        if(key1->data.f == key2->data.f)
            return 0;
        else if(key1->data.f > key2->data.f)
            return 1;
        else
            return -1;
        
      case OOSQL_TYPE_DOUBLE:
        if(key1->data.d == key2->data.d)
            return 0;
        else if(key1->data.d > key2->data.d)
            return 1;
        else
            return -1;

      case OOSQL_TYPE_OID:         
      case OOSQL_TYPE_PAGEID:    
      case OOSQL_TYPE_FILEID:    
      case OOSQL_TYPE_INDEXID:
      case OOSQL_TYPE_MBR:
      case OOSQL_TYPE_STRING:  
      case OOSQL_TYPE_VARSTRING:
        ERR(eCANNOT_BE_COMPARED);
        break;

      default:
        ERR(eINTERNAL_ERR);
        break;
    }

    return eNOERROR;
}

Boolean OOSQL_MinMaxHeap::isCreated()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
#ifdef  TRACE
#endif

    if(strlen(storage.tempFileName))    // if tempFile is created, then OOSQL_MinMaxHeap is created
        return SM_TRUE;
    else
        return SM_FALSE;
}

Boolean OOSQL_MinMaxHeap::isEmpty()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
#ifdef  TRACE
#endif

    if (lastEntryIndex == 0)
        return SM_TRUE;
    else
        return SM_FALSE;
}


Boolean OOSQL_MinMaxHeap::isOutOfBound(
    Four index
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
#ifdef  TRACE
#endif

    if (index > 0 && index <= lastEntryIndex)
        return SM_FALSE;
    else
        return SM_TRUE;
}
