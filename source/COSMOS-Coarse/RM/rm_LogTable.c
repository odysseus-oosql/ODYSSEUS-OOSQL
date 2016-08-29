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
 * Module: rm_LogTable.c
 *
 * Description:
 *  Implements the log table. The log table keeps track of the pairs of
 *  (data page id, the corresponding log page no) which are saved into the
 *  log volume.
 *
 * Exports:
 *  Four rm_InitLogTable(void)
 *  Four rm_FinalLogTable(void)
 *  Boolean rm_LookUpInLogTable(PageID*, PageNo*)
 *  Four rm_InsertIntoLogTable(PageID*, PageNo)
 *  Four rm_DoubleLogTable(void)
 *  Four rm_DeleteAllFromLogTable(void)
 *  Four rm_OpenLogTableScan(void)
 *  Four rm_CloseLogTableScan(void)
 *  Four rm_GetNextEntryFromLogTable(PageID*, PageNo*)
 */


#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


#define INIT_HASH_TABLE_SIZE    (CONSTANT_ONE << 8)             
#define INIT_NUM_OF_ENTRIES     100
#define HASH(p,size_1) 		(((p).volNo + (p).pageNo) & (size_1))

/*@================================
 * rm_InitLogTable()
 *================================*/
/*
 * Function: Four rm_InitLogTable(void)
 *
 * Description:
 *  Initialize the log table.
 *
 * Returns:
 *  error code
 */
Four rm_InitLogTable(Four handle)
{
    Four 		e;                     /* error code */
    Four              	i;                

    
    TR_PRINT(TR_RM, TR1, ("rm_InitLogTable(handle)"));
    

    RM_PER_THREAD_DS(handle).logTable.hashTable = malloc(sizeof(logTableEntry_t*)*INIT_HASH_TABLE_SIZE); 

    if (RM_PER_THREAD_DS(handle).logTable.hashTable == NULL) ERR(handle, eMEMORYALLOCERR); 

    e = Util_initPool(handle, &RM_PER_THREAD_DS(handle).logTable.poolForEntries, sizeof(logTableEntry_t), INIT_NUM_OF_ENTRIES);
    if (e < eNOERROR) {
        free(RM_PER_THREAD_DS(handle).logTable.hashTable);
        ERR(handle, e);
    }
   
    RM_PER_THREAD_DS(handle).logTable.hashTableSize_1 = INIT_HASH_TABLE_SIZE - 1;
    RM_PER_THREAD_DS(handle).logTable.nInsertedEntries = 0;    

    /* initially all hash chains are empty. */
    for (i = 0; i <= RM_PER_THREAD_DS(handle).logTable.hashTableSize_1; i++)
        RM_PER_THREAD_DS(handle).logTable.hashTable[i] = NULL;

    return(eNOERROR);
    
} /* rm_InitLogTable() */



/*@================================
 * rm_FinalLogTable()
 *================================*/
/*
 * Function: Four rm_FinalLogTable(void)
 *
 * Description:
 *  Finalize the log table.
 *
 * Returns:
 *  error code
 */
Four rm_FinalLogTable(Four handle)
{
    Four e;                     /* error code */
    
    
    TR_PRINT(TR_RM, TR1, ("rm_FinalLogTable(handle)"));

    
    (void) free(RM_PER_THREAD_DS(handle).logTable.hashTable); 

    e = Util_finalPool(handle, &RM_PER_THREAD_DS(handle).logTable.poolForEntries); 
    if (e < eNOERROR) ERR(handle, e);
    
    return(eNOERROR);
 
} /* rm_FinalLogTable() */



/*@================================
 * rm_LookUpInLogTable()
 *================================*/
/*
 * Function: Boolean rm_LookUpInLogTable(PageID*, PageNo*)
 *
 * Description:
 *  Look up the given data page id in the log table. If it exist, then
 *  return the corresponding log page no.
 *
 * Returns:
 *  TRUE if the data page id exist in the log table.
 *  FALSE otherwise.
 */
Boolean rm_LookUpInLogTable(
    Four handle,
    PageID *trainId,            /* IN train to look up */
    PageNo *logPageNo)          /* OUT the corresponding log page no */
{
    Four hashValue;             /* hash value of the data page id */
    logTableEntry_t *entryPtr;  /* pointer to an entry */

    
    TR_PRINT(TR_RM, TR1, ("rm_LookUpInLogTable(handle, trainId=%P, logPageNo=%P)", trainId, logPageNo));

    
    hashValue = HASH(*trainId, RM_PER_THREAD_DS(handle).logTable.hashTableSize_1); 

    entryPtr = RM_PER_THREAD_DS(handle).logTable.hashTable[hashValue]; 
    while (entryPtr) {        
        if (EQUAL_PAGEID(*trainId, entryPtr->dataPid)) {
            *logPageNo = entryPtr->logPageNo;
            return(TRUE);
        }
        entryPtr = entryPtr->nextEntry;
    }

    return(FALSE);
                          
} /* rm_LookUpInLogTable() */



/*@================================
 * rm_InsertIntoLogTable()
 *================================*/
/*
 * Function: Four rm_InsertIntoLogTable(PageID*, PageNo)
 *
 * Description:
 *  Insert a pair of (data page id, log page no) into the log table.
 *
 * Returns:
 *  error code
 */
Four rm_InsertIntoLogTable(
    Four handle,
    PageID *trainId,            /* IN train to look up */
    PageNo logPageNo)           /* IN the corresponding log page no */
{
    Four e;                     /* error code */
    Four hashValue;             /* hash value of the data page id */
    logTableEntry_t *entryPtr;  /* pointer to an entry */
    logTableEntry_t *newEntryPtr; /* pointer to the newly allocated etnry */

    
    TR_PRINT(TR_RM, TR1, ("rm_InsertIntoLogTable(handle, trainId=%P, logPageNo=%lD)", trainId, logPageNo));


    /*
     * If there are too many entries, then double the size of the log table.
     */
    if (RM_PER_THREAD_DS(handle).logTable.nInsertedEntries > RM_PER_THREAD_DS(handle).logTable.hashTableSize_1) { 
        e = rm_DoubleLogTable(handle);
        if (e < eNOERROR) ERR(handle, e);
    }

    /*
     * Allocate a new entry for the inseted entry.
     */
    e = Util_getElementFromPool(handle, &RM_PER_THREAD_DS(handle).logTable.poolForEntries, &newEntryPtr); 
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Insert the new entry.
     */
    hashValue = HASH(*trainId, RM_PER_THREAD_DS(handle).logTable.hashTableSize_1);   	

    newEntryPtr->dataPid = *trainId;
    newEntryPtr->logPageNo = logPageNo;
    newEntryPtr->nextEntry = RM_PER_THREAD_DS(handle).logTable.hashTable[hashValue]; 	
    RM_PER_THREAD_DS(handle).logTable.hashTable[hashValue] = newEntryPtr; 		

    return(eNOERROR);
                          
} /* rm_InsertIntoLogTable() */



/*@================================
 * rm_DoubleLogTable()
 *================================*/
/*
 * Function: Four rm_DoubleLogTable(void)
 *
 * Description:
 *  Double the hash table.
 *
 * Returns:
 *  error code
 */
Four rm_DoubleLogTable(Four handle)
{
    logTableEntry_t     **newHashTable;              /* pointer to the new hash table */
    Four                newHashTableSize_1;          /* (size of the new new hash table) - 1*/
    logTableEntry_t     dummyHeadEntry[2];           /* dummy heads of two chains */
    logTableEntry_t     *entryPtr;                   /* pointer to an entry */
    logTableEntry_t     *lastEntryOfFirstHalfChain;  /* pointer to the last entry of the first half chain of the new hash table */
    logTableEntry_t     *lastEntryOfSecondHalfChain; /* pointer to the last entry of the second half chain of the new hash table */
    Four                hashValue;                   /* hash value */
    Four                i;                 

    
    TR_PRINT(TR_RM, TR1, ("rm_DoubleLogTable(handle)"));

    
    newHashTableSize_1 = (RM_PER_THREAD_DS(handle).logTable.hashTableSize_1 + 1) * 2 - 1;

    newHashTable = realloc(RM_PER_THREAD_DS(handle).logTable.hashTable, sizeof(logTableEntry_t)*(newHashTableSize_1+1));
    if (newHashTable == NULL) ERR(handle, eMEMORYALLOCERR);

    /*
     * Rehash the existing entries.
     * We know that one hash entry is divided into two entries:
     *  one is an old entry and the other is located in the second half of
     *  the newly allocated hash table.
     */
    for (i = 0; i <= RM_PER_THREAD_DS(handle).logTable.hashTableSize_1; i++) { 
        
        lastEntryOfFirstHalfChain = &dummyHeadEntry[0];
        lastEntryOfSecondHalfChain = &dummyHeadEntry[1];

        entryPtr = newHashTable[i];
        
        while (entryPtr) {
            hashValue = HASH(entryPtr->dataPid, newHashTableSize_1);

            if (hashValue == i) {
                lastEntryOfFirstHalfChain->nextEntry = entryPtr;
                lastEntryOfFirstHalfChain = entryPtr;
            } else {
                lastEntryOfSecondHalfChain->nextEntry = entryPtr;
                lastEntryOfSecondHalfChain = entryPtr;
            }

            entryPtr = entryPtr->nextEntry;
        }

        lastEntryOfFirstHalfChain->nextEntry = NULL;
        lastEntryOfSecondHalfChain->nextEntry = NULL;

        newHashTable[i] = dummyHeadEntry[0].nextEntry;
        newHashTable[i + RM_PER_THREAD_DS(handle).logTable.hashTableSize_1+1] = dummyHeadEntry[1].nextEntry; 
    }
    
    RM_PER_THREAD_DS(handle).logTable.hashTable = newHashTable; 	    
    RM_PER_THREAD_DS(handle).logTable.hashTableSize_1 = newHashTableSize_1; 
    
    return(eNOERROR);
    
} /* rm_DoubleLogTable() */



/*@================================
 * rm_DeleteAllFromLogTable()
 *================================*/
/*
 * Function: Four rm_DeleteAllFromLogTable(void)
 *
 * Description:
 *  Delete all entries from the log table.
 *
 * Returns:
 *  error code
 */
Four rm_DeleteAllFromLogTable(Four handle)
{
    Four                e;                      /* error code */
    Four                i;                      
    logTableEntry_t     *entryPtr;              /* pointer to an entry */
    logTableEntry_t     *tmpPtr;                /* temporary pointer to an entry */

    
    TR_PRINT(TR_RM, TR1, ("rm_DeleteAllFromLogTable(handle)"));

    
    for (i = 0; i <= RM_PER_THREAD_DS(handle).logTable.hashTableSize_1; i++) {

        entryPtr = RM_PER_THREAD_DS(handle).logTable.hashTable[i]; 

        while (entryPtr) {
            tmpPtr = entryPtr;
            entryPtr = tmpPtr->nextEntry;
            e = Util_freeElementToPool(handle, &RM_PER_THREAD_DS(handle).logTable.poolForEntries, tmpPtr); 
            if (e < eNOERROR) ERR(handle, e);
        }
        
        RM_PER_THREAD_DS(handle).logTable.hashTable[i] = NULL; 
    }
    
    return(eNOERROR);
    
} /* rm_DeleteAllFromLogTable() */



/*@================================
 * rm_OpenLogTableScan()
 *================================*/
/*
 * Function: Four rm_OpenLogTableScan(void)
 *
 * Description:
 *  Open a scan on the log table.
 *  We assume that there is no update between the open and close of the scan.
 *
 * Returns:
 *  error code
 */
Four rm_OpenLogTableScan(Four handle)
{
    Four                i;              
    logTableEntry_t 	*entryPtr;   /* pointer to an entry */
    
    
    
    TR_PRINT(TR_RM, TR1, ("rm_OpenLogTableScan(handle)"));


    entryPtr = NULL;            /* initialize the entry pointer */
    for (i = 0; i <= RM_PER_THREAD_DS(handle).logTable.hashTableSize_1; i++) { 	

        entryPtr = RM_PER_THREAD_DS(handle).logTable.hashTable[i]; 		

        if (entryPtr != NULL) break;
    }

    RM_PER_THREAD_DS(handle).cursor.idx = i;
    RM_PER_THREAD_DS(handle).cursor.entryPtr = entryPtr;
    
    return(eNOERROR);
    
} /* rm_OpenLogTableScan() */



/*@================================
 * rm_GetNextEntryFromLogTable()
 *================================*/
/*
 * Function: Four rm_GetNextEntryFromLogTable(PageID*, PageNo*)
 *
 * Description:
 *  Get next entry from the log table. The cursor is pointing the next entry.
 * So we return the entry pointed by the cursor and move the cursor forward.
 *
 * Returns:
 *  error code
 */
Four      rm_GetNextEntryFromLogTable(          
    Four handle,
    PageID              *dataPid,               /* OUT data page updated */
    PageNo              *logPageNo)             /* OUT the corresponding log page */
{
    Four                i;                      
    logTableEntry_t     *entryPtr;              /* pointer to an entry */
    
    
    
    TR_PRINT(TR_RM, TR1, ("rm_GetNextEntryFromLogTable(handle, dataPid=%P, logPageNo=%P)", dataPid, logPageNo));


    if (RM_PER_THREAD_DS(handle).cursor.entryPtr == NULL) return(RM_NONEXTENTRY); 

    *dataPid = RM_PER_THREAD_DS(handle).cursor.entryPtr->dataPid; 		  
    *logPageNo = RM_PER_THREAD_DS(handle).cursor.entryPtr->logPageNo;		 

    RM_PER_THREAD_DS(handle).cursor.entryPtr = RM_PER_THREAD_DS(handle).cursor.entryPtr->nextEntry;

    /* modify the way of accessing the cursor for multi threading */
    if (RM_PER_THREAD_DS(handle).cursor.entryPtr == NULL) {
        entryPtr = NULL;            /* initialize the entry pointer */
        for (i = RM_PER_THREAD_DS(handle).cursor.idx+1; i <= RM_PER_THREAD_DS(handle).logTable.hashTableSize_1; i++) { 

            entryPtr = RM_PER_THREAD_DS(handle).logTable.hashTable[i];

            if (entryPtr != NULL) break;
        }

        RM_PER_THREAD_DS(handle).cursor.idx = i;
        RM_PER_THREAD_DS(handle).cursor.entryPtr = entryPtr;
    }

    return(eNOERROR);
    
} /* rm_OpenLogTableScan() */


    
/*@================================
 * rm_CloseLogTableScan()
 *================================*/
/*
 * Function: Four rm_CloseLogTableScan(void)
 *
 * Description:
 *  Close a scan on the log table.
 *
 * Returns:
 *  error code
 */
Four rm_CloseLogTableScan(Four handle)
{    
    TR_PRINT(TR_RM, TR1, ("rm_CloseLogTableScan(handle)"));


    /* There is nothing to do. */
    return(eNOERROR);
    
} /* rm_CloseLogTableScan() */




/*@================================
 * rm_DeleteFromLogTable()
 *================================*/
/*
 * Function: Four rm_DeleteFromLogTable(PageID*, PageNo*)
 *
 * Description:
 *  Delete a pair of (data page id, log page no) from the log table.
 *
 * Returns:
 *  error code
 */
Four rm_DeleteFromLogTable(
    Four handle,
    PageID *trainId)            /* IN train to look up */
{
    Four e;                     /* error code */
    Four hashValue;             /* hash value of the data page id */
    logTableEntry_t *entryPtr;  /* pointer to an entry */
    logTableEntry_t *prevEntryPtr; /* pointer to an previous entry */

    
    TR_PRINT(TR_RM, TR1, ("rm_DeleteFromLogTable(handle, trainId=%P)", trainId));

    
    /* get hashValue */
    hashValue = HASH(*trainId, RM_PER_THREAD_DS(handle).logTable.hashTableSize_1); 

    /* initialize entryPtr */
    entryPtr = RM_PER_THREAD_DS(handle).logTable.hashTable[hashValue]; 

    while (entryPtr) {        

        /* if found */
        if (EQUAL_PAGEID(*trainId, entryPtr->dataPid)) {

            /* remove from list */
	    if (entryPtr == RM_PER_THREAD_DS(handle).logTable.hashTable[hashValue])
		RM_PER_THREAD_DS(handle).logTable.hashTable[hashValue] = entryPtr->nextEntry;
	    else
                prevEntryPtr->nextEntry = entryPtr->nextEntry; 

            /* free entry to pool */
            e = Util_freeElementToPool(handle, &RM_PER_THREAD_DS(handle).logTable.poolForEntries, entryPtr);
            if (e < eNOERROR) ERR(handle, e);

            break;
        }

        /* update prevEntryPtr & entryPtr */
        prevEntryPtr = entryPtr;
        entryPtr = entryPtr->nextEntry;
    }


    return(eNOERROR);
                          
} /* rm_DeleteFromLogTable() */

