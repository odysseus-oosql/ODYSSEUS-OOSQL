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
#include <string.h>
#include <stdlib.h>
#include "OOSQL_MinMaxHeap.hxx"
#include "OOSQL_Error.h"

/*
    MODULE:
        OOSQL_MinMaxHeap.C

    DESCRIPTION:
        This module implements the public member functions of OOSQL_MinMaxHeap class.

    IMPORTS:

    EXPORTS:
*/

#define MAX_OBJECT_SIZE_IN_KAOSS (LONG_MAX)

/* constructor and destructor */
OOSQL_MinMaxHeap::OOSQL_MinMaxHeap()
{
    lastEntryIndex         = 0;
    maxHeapSize            = 0;
    entryDesc              = NULL;
    storage.dbID           = 0;
    storage.scanID         = 0;
    storage.keyLength      = 0;
    storage.entryLength    = 0;
    storage.entryBuffer[0] = NULL;
    storage.entryBuffer[1] = NULL;
}

OOSQL_MinMaxHeap::~OOSQL_MinMaxHeap()
{
    if(isCreated())
        Destroy();
}

static Four getEntryLength(
	OOSQL_MinMaxHeap_EntryDesc    *heapEntryDesc  // IN:
)
{
    Four	size;
    Four        i;
    
    size = 0;
    for(i = 0; i < heapEntryDesc->nFields; i++)
    {
	size += heapEntryDesc->fieldInfo[i].length;
    }

    return size;
}

static Four getHeapEntryDescSize(
    OOSQL_MinMaxHeap_EntryDesc    *heapEntryDesc  // IN:
)
{
    Four	size;

    size = sizeof(OOSQL_MinMaxHeap_EntryDesc) + sizeof(OOSQL_MinMaxHeap_FieldInfo) * (heapEntryDesc->nFields - 1);

    return size;
}


Four	OOSQL_MinMaxHeap::IsEmpty()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
	return isEmpty();
}

Four    OOSQL_MinMaxHeap::Create( 
    Four						  dbID,				// IN
    OOSQL_MinMaxHeap_EntryDesc    *heapEntryDesc	// IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    static Four							uniqueNo = 1;       // unique number for a temporary file name
    OOSQL_StorageManager::AttrInfo      attrInfo[1];        // column information of temp file
    OOSQL_StorageManager::ColListStruct clist[1];			// col list struct
    OOSQL_StorageManager::LockParameter lockup;             // lock parameter
    Four								heapEntryDescSize;	// size of heapEntryDesc
    Four								e;                  // error code
	Four								tmpClassId;
	char								dummy;

#ifdef  TRACE
#endif
    /* check input parameter */
    if(heapEntryDesc == NULL)
        ERR(eBAD_PARAMETER);

    /* check if heap is already created */
    if(isCreated())
        return eHEAP_ALREADY_CREATED;

    /* construct colInfo for temp file */
    attrInfo[0].complexType = SM_COMPLEXTYPE_BASIC;
    attrInfo[0].type        = OOSQL_TYPE_VARSTRING;
    attrInfo[0].length      = MAX_OBJECT_SIZE_IN_KAOSS;
	strcpy(attrInfo[0].name, "MINMAXHEAP");
	attrInfo[0].domain = OOSQL_TYPE_VARSTRING;

    /* create temporary file for external storage */
    do {
        // make tempFileName
        sprintf(storage.tempFileName, "%s%ld%ld", OOSQL_MINMAXHEAP_TEMPFILE_PREFIX, 
                                                dbID, 
                                                uniqueNo++);

        // create temp file
        e = m_storageManager->CreateClass(dbID, storage.tempFileName, NULL, NULL, 1, attrInfo,
			                              0, NULL, 0, NULL, SM_TRUE, &tmpClassId);
        if(e == eCLASSDUPLICATED_OOSQL)
        {
            printf("\nRelation \"%s\" already exists. Trying another temporary file name...",
                   storage.tempFileName);
            continue;
        }
        else
        {
            if(e < eNOERROR)    OOSQL_ERR(e);
            // otherwise, exit loop
        }
    } while(e == eCLASSDUPLICATED_OOSQL);

    /* open temporary file */
    storage.ocn = m_storageManager->OpenClass(dbID, storage.tempFileName);
    if(storage.ocn < eNOERROR)      OOSQL_ERR(storage.ocn);

    /* open sequential scan for the temporary file */
    lockup.mode     = OOSQL_StorageManager::L_X;
    lockup.duration = OOSQL_StorageManager::L_COMMIT;

    storage.scanID = m_storageManager->OpenSeqScan(storage.ocn, FORWARD, 0, NULL, &lockup);
    if(storage.scanID < eNOERROR)   OOSQL_ERR(storage.scanID);

    /* create object */
	clist[0].nullFlag   = SM_FALSE;
    clist[0].colNo      = 0;
    clist[0].start      = 0;
    clist[0].dataLength = 1;        // cannot create zero size vararray
    clist[0].length     = 1;        //
    clist[0].data.ptr   = &dummy;   // dummy data to write

    e = m_storageManager->CreateObjectByColList(storage.scanID, SM_TRUE, 1, clist, &storage.oid);
    if(e < eNOERROR)    OOSQL_ERR(e);

    /* initialize private variable */
    maxHeapSize             = 0;
    lastEntryIndex          = 0;
    storage.dbID            = dbID;
    storage.entryLength     = getEntryLength(heapEntryDesc);

    // get heapEntryDescSize
    heapEntryDescSize	    = getHeapEntryDescSize(heapEntryDesc);
	OOSQL_NEW(entryDesc, pMemoryManager, OOSQL_MinMaxHeap_EntryDesc);
    if(entryDesc == NULL) ERR(eMEMORY_ERROR);
    memcpy(entryDesc, heapEntryDesc, heapEntryDescSize);

    storage.keyLength       = MINMAXHEAP_KEYLENGTH;

    storage.entryBuffer[0]  = (char*) pMemoryManager->Alloc(storage.entryLength);
    if(storage.entryBuffer[0] == NULL)	ERR(eMEMORY_ERROR);

    storage.entryBuffer[1]  = (char*) pMemoryManager->Alloc(storage.entryLength);
    if(storage.entryBuffer[1] == NULL)	ERR(eMEMORY_ERROR);


    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::Destroy()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    e;

#ifdef  TRACE
#endif

    /* check if the heap is created */
    if(!isCreated())
        return eHEAP_WAS_NOT_CREATED;

    /* close scan */
    e = m_storageManager->CloseScan(storage.scanID);
    if(e < eNOERROR)    OOSQL_ERR(e);

    /* close class */
    e = m_storageManager->CloseClass(storage.ocn);
    if(e < eNOERROR)    OOSQL_ERR(e);
    
    /* destroy temporary file */
    e = m_storageManager->DestroyClass(storage.dbID, storage.tempFileName);
    if(e < eNOERROR)    OOSQL_ERR(e);

    /* initialize tempFileName, this value used for checking heap is created */
    strcpy(storage.tempFileName, "");

    /* deallocate memory */
    // memory manager autometically deallocate
    pMemoryManager->Free(storage.entryBuffer[0]);
    pMemoryManager->Free(storage.entryBuffer[1]);

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::Dump()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four            i;
    Four            e;
    OOSQL_MinMaxHeap_Key  key;
    
    for(i = 1; i <= lastEntryIndex; i++)
    {
        e = fetchIthEntryKeyValue(i, &key.keyVal);
        CHECK_ERR(e);

        switch(MINMAXHEAP_KEYTYPE)
        {
          case OOSQL_TYPE_SHORT:
            printf("%d", (int)key.keyVal.data.s);
            break;
          case OOSQL_TYPE_INT:
            printf("%d", (int)key.keyVal.data.i);
            break;
          case OOSQL_TYPE_LONG:
          case OOSQL_TYPE_LONG_LONG:
            printf("%ld", (long)key.keyVal.data.l);
            break;
          case OOSQL_TYPE_FLOAT:
            printf("%f", (float)key.keyVal.data.f);
            break;
          case OOSQL_TYPE_DOUBLE:
            printf("%f", (float)key.keyVal.data.d);
            break;
          case OOSQL_TYPE_OID:
            printf("...");
          default:
            ERR(eINTERNAL_ERR);
        }

        if(i < lastEntryIndex)
            printf(", ");
    }
            
    return eNOERROR;
}

Four    OOSQL_MinMaxHeap::InsertEntry( 
    OOSQL_MinMaxHeap_FieldList    *newEntry   // IN:
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
    /* check input parameter */
    if(newEntry == NULL)
        ERR(eBAD_PARAMETER);

    return insertEntry(newEntry);
}


Four    OOSQL_MinMaxHeap::RetrieveMinEntry( 
    OOSQL_MinMaxHeap_FieldList    *minEntry   // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(minEntry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;

    /* read the root entry (which has the min. key value) */
    e = fetchIthEntry(MINMAXHEAP_ROOTINDEX, minEntry);
    CHECK_ERR(e);

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::RetrieveMinKeyValue( 
    OOSQL_MinMaxHeap_KeyValue *minKeyValue        // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(minKeyValue == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;

    /* read the key value of the root entry */
    e = fetchIthEntryKeyValue(MINMAXHEAP_ROOTINDEX, minKeyValue);
    CHECK_ERR(e);

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::ExtractMinEntry( 
    OOSQL_MinMaxHeap_FieldList    *minEntry   // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(minEntry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;

    /* read the root entry */
    e = fetchIthEntry(MINMAXHEAP_ROOTINDEX, minEntry);
    CHECK_ERR(e);
    
    /* replace the root entry with the last entry */
    e = exchangeEntries(MINMAXHEAP_ROOTINDEX, lastEntryIndex);
    CHECK_ERR(e);

    e = deleteLastEntry();
    CHECK_ERR(e);

    /* heapify */
    if(!isEmpty())
    {
        e = minHeapifyFromTop(MINMAXHEAP_ROOTINDEX);
        CHECK_ERR(e);
    }
    
    /* return */
    return eNOERROR;
}

Four    OOSQL_MinMaxHeap::ReplaceMinEntry( 
    OOSQL_MinMaxHeap_FieldList    *newEntry   // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    e;

#ifdef  TRACE
#endif
    
    /* check input parameter */
    if(newEntry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;

    /* replace root entry with 'newEntry' */
    e = updateIthEntry(MINMAXHEAP_ROOTINDEX, newEntry);

    /* reorder entries to maintain Min Heap properties */
    e = minHeapifyFromTop(MINMAXHEAP_ROOTINDEX);
    CHECK_ERR(e);

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::RetrieveMaxEntry( 
    OOSQL_MinMaxHeap_FieldList    *maxEntry   // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  leftChildKey;
    OOSQL_MinMaxHeap_Key  rightChildKey;
    Four            rootEntryIndex;
    Four            e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(maxEntry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;

    /* set entryIndex */
    leftChildKey.entryIndex  = MINMAXHEAP_GET_LEFTCHILD(MINMAXHEAP_ROOTINDEX);
    rightChildKey.entryIndex = MINMAXHEAP_GET_RIGHTCHILD(MINMAXHEAP_ROOTINDEX);

    /* determine root entry index */
    if(isOutOfBound(leftChildKey.entryIndex))
        rootEntryIndex = MINMAXHEAP_ROOTINDEX;
    else if(isOutOfBound(rightChildKey.entryIndex))
        rootEntryIndex = leftChildKey.entryIndex;
    else
    {
        /* read left child of root */
        e = fetchIthEntryKeyValue(leftChildKey.entryIndex, &leftChildKey.keyVal);
        CHECK_ERR(e);

        /* read right child of root */
        e = fetchIthEntryKeyValue(rightChildKey.entryIndex, &rightChildKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &leftChildKey.keyVal, &rightChildKey.keyVal) > 0)
            rootEntryIndex = leftChildKey.entryIndex;
        else
            rootEntryIndex = rightChildKey.entryIndex;
    }

    e = fetchIthEntry(rootEntryIndex, maxEntry);
    CHECK_ERR(e);

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::RetrieveMaxKeyValue( 
    OOSQL_MinMaxHeap_KeyValue *maxKeyValue        // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  leftChildKey;
    OOSQL_MinMaxHeap_Key  rightChildKey;
    Four            rootEntryIndex;
    Four            e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(maxKeyValue == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;

    /* set entryIndex */
    leftChildKey.entryIndex  = MINMAXHEAP_GET_LEFTCHILD(MINMAXHEAP_ROOTINDEX);
    rightChildKey.entryIndex = MINMAXHEAP_GET_RIGHTCHILD(MINMAXHEAP_ROOTINDEX);

    /* determine root entry index */
    if(isOutOfBound(leftChildKey.entryIndex))
        rootEntryIndex = MINMAXHEAP_ROOTINDEX;
    else if(isOutOfBound(rightChildKey.entryIndex))
        rootEntryIndex = leftChildKey.entryIndex;
    else
    {
        /* read left child of root */
        e = fetchIthEntryKeyValue(leftChildKey.entryIndex, &leftChildKey.keyVal);
        CHECK_ERR(e);

        /* read right child of root */
        e = fetchIthEntryKeyValue(rightChildKey.entryIndex, &rightChildKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &leftChildKey.keyVal, &rightChildKey.keyVal) > 0)
            rootEntryIndex = leftChildKey.entryIndex;
        else
            rootEntryIndex = rightChildKey.entryIndex;
    }
    
    e = fetchIthEntryKeyValue(rootEntryIndex, maxKeyValue);
    CHECK_ERR(e);

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::ExtractMaxEntry( 
    OOSQL_MinMaxHeap_FieldList    *maxEntry   // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  leftChildKey;
    OOSQL_MinMaxHeap_Key  rightChildKey;
    Four            rootEntryIndex;
    Four            e;

#ifdef  TRACE
#endif

    /* check input parameter */
    if(maxEntry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;
/* set entryIndex */
    leftChildKey.entryIndex  = MINMAXHEAP_GET_LEFTCHILD(MINMAXHEAP_ROOTINDEX);
    rightChildKey.entryIndex = MINMAXHEAP_GET_RIGHTCHILD(MINMAXHEAP_ROOTINDEX);

    /* determine root entry index */
    if(isOutOfBound(leftChildKey.entryIndex))
        rootEntryIndex = MINMAXHEAP_ROOTINDEX;
    else if(isOutOfBound(rightChildKey.entryIndex))
        rootEntryIndex = leftChildKey.entryIndex;
    else
    {
        /* read left child of root */
        e = fetchIthEntryKeyValue(leftChildKey.entryIndex, &leftChildKey.keyVal);
        CHECK_ERR(e);

        /* read right child of root */
        e = fetchIthEntryKeyValue(rightChildKey.entryIndex, &rightChildKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &leftChildKey.keyVal, &rightChildKey.keyVal) > 0)
            rootEntryIndex = leftChildKey.entryIndex;
        else
            rootEntryIndex = rightChildKey.entryIndex;
    }

    /* read the root entry */
    e = fetchIthEntry(rootEntryIndex, maxEntry);
    CHECK_ERR(e);

    /* replace the root entry with the last entry */
    e = exchangeEntries(rootEntryIndex, lastEntryIndex);
    CHECK_ERR(e);

    e = deleteLastEntry();
    CHECK_ERR(e);

    /* heapify */
    if(lastEntryIndex > 3)
    {
        e = maxHeapifyFromTop(rootEntryIndex);
        CHECK_ERR(e);
    }

    /* return */
    return eNOERROR;
}


Four    OOSQL_MinMaxHeap::ReplaceMaxEntry( 
    OOSQL_MinMaxHeap_FieldList    *newEntry   // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_MinMaxHeap_Key  leftChildKey;
    OOSQL_MinMaxHeap_Key  rightChildKey;
    Four            rootEntryIndex;
    Four            e;

#ifdef  TRACE
#endif
    
    /* check input parameter */
    if(newEntry == NULL)
        ERR(eBAD_PARAMETER);

    /* check if the heap is not empty */
    if(isEmpty())
        return eHEAP_EMPTY;

        /* set entryIndex */
    leftChildKey.entryIndex  = MINMAXHEAP_GET_LEFTCHILD(MINMAXHEAP_ROOTINDEX);
    rightChildKey.entryIndex = MINMAXHEAP_GET_RIGHTCHILD(MINMAXHEAP_ROOTINDEX);

    /* determine root entry index */
    if(isOutOfBound(leftChildKey.entryIndex))
        rootEntryIndex = MINMAXHEAP_ROOTINDEX;
    else if(isOutOfBound(rightChildKey.entryIndex))
        rootEntryIndex = leftChildKey.entryIndex;
    else
    {
        /* read left child of root */
        e = fetchIthEntryKeyValue(leftChildKey.entryIndex, &leftChildKey.keyVal);
        CHECK_ERR(e);

        /* read right child of root */
        e = fetchIthEntryKeyValue(rightChildKey.entryIndex, &rightChildKey.keyVal);
        CHECK_ERR(e);

        if(keyCompare(MINMAXHEAP_KEYTYPE, &leftChildKey.keyVal, &rightChildKey.keyVal) > 0)
            rootEntryIndex = leftChildKey.entryIndex;
        else
            rootEntryIndex = rightChildKey.entryIndex;
    }

    /* replace root entry with 'newEntry' */
    e = updateIthEntry(rootEntryIndex, newEntry);

    /* reorder entries to maintain Max Heap properties */
    if(rootEntryIndex != MINMAXHEAP_ROOTINDEX)
    {
        e = maxHeapifyFromTop(rootEntryIndex );
        CHECK_ERR(e);
    }

    /* return */
    return eNOERROR;
}

OOSQL_MinMaxHeap_EntryDesc::OOSQL_MinMaxHeap_EntryDesc()
{
	// nothing to do
}

OOSQL_MinMaxHeap_EntryDesc::~OOSQL_MinMaxHeap_EntryDesc()
{
	// nothing to do
}







