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
 * Module: OM_NextBulkLoad.c
 *
 * Description:
 *  Process the data file bulk load. 
 *
 * Exports:
 *  Four OM_NextBulkLoad(Four, char*, Four, Boolean, ObjectID*)
 *  Four OM_NextBulkLoadWriteLOT(Four, Four, Four, char*, Boolean, ObjectID*)
 */

#include <string.h> 

#include "common.h"
#include "param.h"
#include "bl_param.h"
#include "RDsM_Internal.h" 	
#include "BfM.h"              
#include "LOT_Internal.h"     	/* TTT for the large object manager call */
#include "OM_Internal.h"
#include "BL_OM_Internal.h"
#include "Util_Sort.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@========================================
 *  OM_NextBulkLoad()
 * =======================================*/

/*
 * Function : Four OM_InitNextLoad(Four, char*, Four, Boolean, ObjectID*)
 *
 * Description :
 *  Process the data file bulk load. 
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *  0)
 *
 */

Four OM_NextBulkLoad(
    Four handle,
    Four        blkLdId,                  /* IN  OM bulkload ID */ 
    char        *objectBuffer,            /* IN  buffer containing object data */
    Four        objectBufferLen,          /* IN  size of data in buffer */
    Boolean     endOfObject,              /* IN  flag indicating this buffer is end of object or not */
    ObjectID    *oid)                     /* OUT the object's ObjectID */
{
    Four        e;                        /* error number */
    Four        neededSpace;              /* space needed to put new object [+ header] */
    Four        alignedLen;               /* aligned length of initial data */
    SlottedPage *apage;                   /* point to buffer holding the page */
    Object      *obj;                     /* point to the newly bulk loaded object */
    char        origObjBuffer[PAGESIZE];  /* original object buffer */
    Object      *origObj;                 /* original object */
    char        tempObj[LRGOBJ_THRESHOLD];/* buffer for temporary object */
    Four        tempObjLen;               /* length of temporary object */
    Boolean     isRootWithHdr;            /* indicate this object will be stored in large object with header form */
    L_O_T_INode *r_node;                  /* pointer to root node */
    L_O_T_INode *anode;                   /* pointer to new root node in slotted page */
    Unique      unique;                   /* space for the returned unique number */
    Four        num;                      /* number of unique numbers newly allocated */
    PageID      *tempPageId;              /* temporary pointer for swap */
    Four        numOfAllocTrains;         /* number of page to allocate for bulk load data file buffer */
    Two         numVarAttrs;              /* number of variable length column in the relation */
    Two         indexKeyLen;              /* length of clustering key of the object */
    SortStreamTuple     sortTuple;        /* tuple for sort stream */
    omSortKeyAttrInfo   attrInfo;
    OM_BlkLdTableEntry* blkLdEntry;       /* entry in which informattion about bulkload is saved */ 


    /*
     * 0. set entry for fast access
     */

    blkLdEntry = &OM_BLKLD_TABLE(handle)[blkLdId]; /* insert a handle into OM_BLKLD_TABLE */


    /*
     * 1. This object buffer is the continue to input an object
     */

    if (endOfObject == FALSE) { 

        /* 1-1. Large object tree is't exist (This is first time input of the object) */
        if(blkLdEntry->rootOfLOT.pageNo == NIL) {

            /* Create the large object tree */ 
            e = LOT_BlkLd_CreateLargeObject(handle, &blkLdEntry->catObjForFile, NULL, &blkLdEntry->rootOfLOT);
            if(e < 0) ERR(handle, e);
            /* Initialize the large object size */ 
            blkLdEntry->largeObjectLen = 0; 

            /* Insert part of the object into large object tree */ 
            e = LOT_BlkLd_AppendToObject(handle, &blkLdEntry->catObjForFile, &blkLdEntry->rootOfLOT, objectBufferLen, objectBuffer); 
            if(e < 0) ERR(handle, e);

            /* increase the large object size */ 
            blkLdEntry->largeObjectLen += objectBufferLen; 

            /* return the object id to NULL because the object creation is not completed */
            oid = NULL;

        /* 1-2. Large object tree is already exist */
        } else {

            /* Insert part of the object into large object tree */
            e = LOT_BlkLd_AppendToObject(handle, &blkLdEntry->catObjForFile, &blkLdEntry->rootOfLOT, objectBufferLen, objectBuffer); 
            if(e < 0) ERR(handle, e);

            /* increase the large object size */ 
            blkLdEntry->largeObjectLen += objectBufferLen; 

            /* return the object id to NULL because the object creation is not completed */
            oid = NULL;

        }


    /*
     * 2. This object buffer is the end of object
     */
    } else {

        /* 2-1. This object is large object */
        if(blkLdEntry->rootOfLOT.pageNo != NIL || ALIGNED_LENGTH(objectBufferLen) + sizeof(SlottedPageSlot) > LRGOBJ_THRESHOLD) {

            if(blkLdEntry->rootOfLOT.pageNo == NIL) {

                /* Create the large object tree */ 
                e = LOT_BlkLd_CreateLargeObject(handle, &blkLdEntry->catObjForFile, NULL, &blkLdEntry->rootOfLOT);
                if(e < 0) ERR(handle, e);

                /* Initialize the large object size */ 
                blkLdEntry->largeObjectLen = 0;  
            }

            /* Insert part of the object into large object tree */
            e = LOT_BlkLd_AppendToObject(handle, &blkLdEntry->catObjForFile, &blkLdEntry->rootOfLOT, objectBufferLen, objectBuffer); 
            if(e < 0) ERR(handle, e);

            /* Increase the large object size */ 
            blkLdEntry->largeObjectLen += objectBufferLen; 

            /* 2-1-1. There is clustering index */
            if (blkLdEntry->isNeedSortFlag == TRUE) {

                /* 
                 *  object encoding to insert sort stream  
                 */
                origObj = (Object*)origObjBuffer;

                /* set the object header */
                origObj->header.properties = P_LRGOBJ;
                origObj->header.length = blkLdEntry->largeObjectLen;

                /* copy the data into the object */
                *((ShortPageID *)(origObj->data)) = blkLdEntry->rootOfLOT.pageNo;

                /*
                 *  convert object to sort temporary object format
                 */
                e = om_GetTempObject(handle, origObj, (Object *)tempObj, &tempObjLen, blkLdEntry->kdesc, blkLdEntry->getKeyAttrs, blkLdEntry->schema);
                if (e < 0) ERR(handle, e);

                /* set sortStreamTuple */
                sortTuple.len = tempObjLen;
                sortTuple.data = tempObj;

                /*
                 *  insert 'tempObj' into sort stream
                 */
                e = Util_PutTuplesIntoSortStream(handle, blkLdEntry->dataFileSortStreamID, 1, &sortTuple);
                if (e < 0) ERR(handle, e);

                /* return the object id to NULL because the object id is not decided */
                oid = NULL;


            /* 2-1-2. There is no clustering index */
            } else {

                /* Get the root page of large object */
                e = BfM_GetTrain(handle, &blkLdEntry->rootOfLOT, (char **)&r_node, PAGE_BUF);
                if (e < 0) ERR(handle, e);

                apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);

                /* If this large object can be stored in root with header form and there is enought space in page 
                   then store the object in root with header form.
                   If this large object can be stored in root with header from but there is not enought spage in page 
                   then store the object in large object form.  
                   If there is not enough space in page for large object form 
                   then try to store in the next buffer page - It is explained in detail */
                if (r_node->header.nEntries <= LOT_MAXENTRIES_ROOTWITHHDR) {

                    /* calculate needed space to store the object in root with header form */
                    alignedLen = ALIGNED_LENGTH(sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry) * r_node->header.nEntries); 
                    neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                    isRootWithHdr = TRUE;

                    if (SP_FREE(apage) < neededSpace || SP_FREE(apage) < blkLdEntry->minFreeSpace) {

                        /* calculate needed space to store the object in large object form */
                        alignedLen = ALIGNED_LENGTH(sizeof(ShortPageID));
                        neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                        isRootWithHdr = FALSE;

                        if(SP_FREE(apage) < neededSpace || SP_FREE(apage) < blkLdEntry->minFreeSpace){

                            /* try to store in the next buffer page */
                            blkLdEntry->fileBufIdx++;

                            /* end of the data file bulk load buffer */
                            if (blkLdEntry->fileBufIdx == blkLdEntry->bufSize) {

                                /*
                                 *  Flush data file bulk load buffer 
                                 */

                                /* swap the page id array to allocate new extent before flush data file bulk load buffer */ 
                                tempPageId = blkLdEntry->allocExtentPageIdAry;
                                blkLdEntry->allocExtentPageIdAry = blkLdEntry->flushExtentPageIdAry;
                                blkLdEntry->flushExtentPageIdAry = tempPageId;

                                /* allocate new extent */
                                numOfAllocTrains = blkLdEntry->bufSize;
                                e = om_BlkLdAllocExtent(handle, blkLdEntry->pFid, NULL, &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry); 
                                if(e < 0) ERR(handle, e);

                                if(numOfAllocTrains != blkLdEntry->bufSize)
                                    ERR(handle, eCANTALLOCEXTENT_BL_OM);

                                /* set the next page of last page in the data file bulk load buffer */
                                blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;

                                /* flush the data file bulk load buffer */
                                e = om_BlkLdFlushBuffer(handle, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize);
                                if(e < 0) ERR(handle, e);

                                /* initialize the data file bulk load buffer */
                                e = om_BlkLdInitDataFileBuffer(handle, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry, blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage);
                                if(e < 0) ERR(handle, e);

                                /* set the last allocated page */
                                blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

                                blkLdEntry->fileBufIdx = 0;
                            }

                            /* calculate needed space to store the object in root with header form */
                            alignedLen = ALIGNED_LENGTH(sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry) * r_node->header.nEntries); 
                            neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                            isRootWithHdr = TRUE;
                        }
                    }

                } else {

                    alignedLen = ALIGNED_LENGTH(sizeof(ShortPageID));
                    neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                    isRootWithHdr = FALSE;

                    if(SP_FREE(apage) < neededSpace || SP_FREE(apage) < blkLdEntry->minFreeSpace){

                        /* try to store in the next buffer page */
                        blkLdEntry->fileBufIdx++;

                        /* end of the data file bulk load buffer */
                        if (blkLdEntry->fileBufIdx == blkLdEntry->bufSize) {

                            /*
                             *  Flush data file bulk load buffer 
                             */

                            /* swap the page id array to allocate new extent before flush data file bulk load buffer */ 
                            tempPageId = blkLdEntry->allocExtentPageIdAry;
                            blkLdEntry->allocExtentPageIdAry = blkLdEntry->flushExtentPageIdAry;
                            blkLdEntry->flushExtentPageIdAry = tempPageId;

                            /* allocate new extent */
                            numOfAllocTrains = blkLdEntry->bufSize;
                            e = om_BlkLdAllocExtent(handle, blkLdEntry->pFid, NULL, &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry); 
                            if(e < 0) ERR(handle, e);

                            if(numOfAllocTrains != blkLdEntry->bufSize) ERR(handle, eCANTALLOCEXTENT_BL_OM);

                            /* set the next page of last page in the data file bulk load buffer */
                            blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;

                            /* flush the data file bulk load buffer */
                            e = om_BlkLdFlushBuffer(handle, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize);
                            if(e < 0) ERR(handle, e);

                            /* initialize the data file bulk load buffer */
                            e = om_BlkLdInitDataFileBuffer(handle, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry, blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage);
                            if(e < 0) ERR(handle, e);

                            /* set the last allocated page */
                            blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

                            blkLdEntry->fileBufIdx = 0;
                        }
                    }
                }           

                /* In this point,
                   omBlkLdFileBufIdx is the index of data file buffer that object will be bulkloaded
                   if isRootWithHdr is TRUE,  this object is root with header object 
                   if isRootWithHdr is FALSE, this object is large object */

                /* Insert "Large With Header Object" into "Data File Bulk Load Buffer" */
                if(isRootWithHdr == TRUE) {

                    /* where to put the object? */

                    apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);
                    obj = (Object *)&(apage->data[apage->header.free]);

                    /* set the object header */
                    obj->header.properties = P_LRGOBJ | P_LRGOBJ_ROOTWITHHDR;
                    obj->header.length = blkLdEntry->largeObjectLen;       

                    /* copy the data into the object */
                    anode = (L_O_T_INode *)obj->data;
                    memcpy((char*)anode, (char*)r_node,
                           sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*r_node->header.nEntries);

                    /* Modify slotted page slot */
                    apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                    /* Set Unique */

                    /* If the unique numbers in this page is exhausted, */
                    /* then request new unique numbers. */
                    if (apage->header.unique >= apage->header.uniqueLimit) {

                        e = RDsM_GetUnique(handle, &(apage->header.pid), &unique, &num);
                        if (e < 0) ERRB1(handle, e, &(apage->header.pid), PAGE_BUF);

                        apage->header.unique = unique;
                        apage->header.uniqueLimit = unique + num;
                    }

                    /*@ Allocate a new unique number. */
                    apage->slot[-(apage->header.nSlots)].unique = (apage->header.unique)++;

                    /* Modify slotted page header */

                    apage->header.nSlots++;      /* increment # of slots */
                    apage->header.free += sizeof(ObjectHdr) + alignedLen;

                    /*@ free the root node */
                    e = BfM_FreeTrain(handle, &blkLdEntry->rootOfLOT, PAGE_BUF);
                    if(e < 0) ERR(handle, e);

                    /* remove page from buffer pool */
                    e = BfM_RemoveTrain(handle, &blkLdEntry->rootOfLOT, PAGE_BUF, FALSE);
                    if (e < 0) ERR(handle, e);

                    /* delete previous root page */
                    e = RDsM_FreeTrain(handle, &blkLdEntry->rootOfLOT, PAGESIZE2);
                    if(e < 0) ERR(handle, e);

                /* Insert "Large Object" into "Data File Bulk Load Buffer" */
                } else {

                    alignedLen = ALIGNED_LENGTH(sizeof(ShortPageID));

                    /* where to put the object[header]? */
                    apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);
                    obj = (Object *)&(apage->data[apage->header.free]);

                    /* set the object header */
                    obj->header.properties = P_LRGOBJ;
                    obj->header.length = blkLdEntry->largeObjectLen;       

                    /* set the object body */

                    *((ShortPageID *)obj->data) = blkLdEntry->rootOfLOT.pageNo;

                    /* Modified slotted page slot */

                    apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                    /* Set Unique */

                    /* If the unique numbers in this page is exhausted, */
                    /* then request new unique numbers. */
                    if (apage->header.unique >= apage->header.uniqueLimit) {

                        e = RDsM_GetUnique(handle, &(apage->header.pid), &unique, &num);
                        if (e < 0) ERRB1(handle, e, &(apage->header.pid), PAGE_BUF);

                        apage->header.unique = unique;
                        apage->header.uniqueLimit = unique + num;
                    }

                    /*@ Allocate a new unique number. */
                    apage->slot[-(apage->header.nSlots)].unique = apage->header.unique++;

                    /* Modified slotted page header */

                    apage->header.nSlots++;      /* increment # of slots */
                    apage->header.free += sizeof(ObjectHdr) + alignedLen;

                    /*@ free the root node */
                    e = BfM_FreeTrain(handle, &blkLdEntry->rootOfLOT, PAGE_BUF);
                    if(e < 0) ERR(handle, e);
                }
            }
          
            blkLdEntry->rootOfLOT.pageNo = NIL;

            /* return the object id of created object */
            if (oid != NULL) {
                oid->pageNo = blkLdEntry->allocExtentPageIdAry[blkLdEntry->fileBufIdx].pageNo;
                oid->volNo  = blkLdEntry->fid.volNo;
                oid->slotNo = apage->header.nSlots - 1;
                oid->unique = apage->slot[-(oid->slotNo)].unique;
            }


        /* 2-2. This object is small object */
        } else {

            /* 2-2-1. There is clustering index */
            if (blkLdEntry->isNeedSortFlag == TRUE) {

                /* 
                 *  object encoding using tuple buffer 
                 */
                origObj = (Object*)origObjBuffer;
				
                /* set the object header */
                origObj->header.properties = P_CLEAR;
                origObj->header.length = objectBufferLen;

                /* copy the data into the object */
                memcpy(origObj->data, objectBuffer, objectBufferLen);

                /*
                 * convert object to sort temporary object format
                 */ 

                /* get key attribute info */
                blkLdEntry->getKeyAttrs(origObj, blkLdEntry->schema, blkLdEntry->kdesc, &attrInfo);

                /* calculate sort key length */
                e = om_CalculateSortKeyLengthFromAttrInfo(handle, &attrInfo, &indexKeyLen, &numVarAttrs);
                if (e < 0) ERR(handle, e);
                tempObjLen = sizeof(ObjectHdr) + OFFSETARRAYSIZE(blkLdEntry->kdesc->nparts) +
                             numVarAttrs*sizeof(Two) + origObj->header.length;

                /* If the size of temporary object is larger then large object threshold, 
                   this small object must be converted to large object */

                if(tempObjLen > LRGOBJ_THRESHOLD) {

                    /* Create large object tree and insert part of object into large object tree */
                    e = LOT_BlkLd_CreateLargeObject(handle, &blkLdEntry->catObjForFile, NULL, &blkLdEntry->rootOfLOT);
                    if(e < 0) ERR(handle, e);

                    e = LOT_BlkLd_AppendToObject(handle, &blkLdEntry->catObjForFile, &blkLdEntry->rootOfLOT, objectBufferLen, objectBuffer);
                    if(e < 0) ERR(handle, e);
                    blkLdEntry->largeObjectLen = objectBufferLen;

                    /*
                     * object encoding to insert sort stream
                     */
                    origObj = (Object*)origObjBuffer;

                    /* set the object header */
                    origObj->header.properties = P_LRGOBJ;
                    origObj->header.length = blkLdEntry->largeObjectLen;

                    /* copy the data into the object */
                    *((ShortPageID *)(origObj->data)) = blkLdEntry->rootOfLOT.pageNo;
                }

                e = om_GetTempObject(handle, origObj, (Object *)tempObj, &tempObjLen, blkLdEntry->kdesc, blkLdEntry->getKeyAttrs, blkLdEntry->schema);
                if (e < 0) ERR(handle, e);

                /* set sortStreamTuple */
                sortTuple.len  = tempObjLen;
                sortTuple.data = tempObj;

                /*
                 * insert into sort stream 
                 */ 

                /* insert 'tempObj' into sortStream */
                e = Util_PutTuplesIntoSortStream(handle, blkLdEntry->dataFileSortStreamID, 1, &sortTuple);
                if (e < 0) ERR(handle, e);

                /* return the object id to NULL because the object id is not decided */
                oid = NULL;

            /* 2-2-2. There is no clustering index */
            } else {

                /*
                 * calculate the length to be needed in the slotted page.
                 * If need to create the large object, the slotted page only contains
                 * object header.
                 */
                alignedLen  = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(objectBufferLen));
                neededSpace = sizeof(ObjectHdr) + alignedLen + sizeof(SlottedPageSlot);

                apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);

                if (SP_FREE(apage) < neededSpace || SP_FREE(apage) < blkLdEntry->minFreeSpace) {

                    /* try to store in the next buffer page */
                    blkLdEntry->fileBufIdx++;

                    /* end of the data file bulk load buffer */
                    if (blkLdEntry->fileBufIdx == blkLdEntry->bufSize) {

                        /*
                        *  Flush data file bulk load buffer 
                        */

                        /* swap the page id array to allocate new extent before flush data file bulk load buffer */ 
                        tempPageId = blkLdEntry->allocExtentPageIdAry;
                        blkLdEntry->allocExtentPageIdAry = blkLdEntry->flushExtentPageIdAry;
                        blkLdEntry->flushExtentPageIdAry = tempPageId;

                        /* allocate new extent */
                        numOfAllocTrains = blkLdEntry->bufSize;
                        e = om_BlkLdAllocExtent(handle, blkLdEntry->pFid, NULL, &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry); 
                        if(e < 0) ERR(handle, e);

                        if(numOfAllocTrains != blkLdEntry->bufSize) ERR(handle, eCANTALLOCEXTENT_BL_OM);

                        /* set the next page of last page in the data file bulk load buffer */
                        blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;

                        /* flush the data file bulk load buffer */
                        e = om_BlkLdFlushBuffer(handle, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize);
                        if(e < 0) ERR(handle, e);

                        /* initialize the data file bulk load buffer */
                        e = om_BlkLdInitDataFileBuffer(handle, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry, blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage);
                        if(e < 0) ERR(handle, e);

                        /* set the last allocated page */
                        blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

                        blkLdEntry->fileBufIdx = 0;
                    }
                    apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);
                }

                /*
                 * At this point
                 * apage : pointer to the slotted page buffer
                 * alignedLen : space for data of the new object
                 */

                /* where to put the object[header]? */
                obj = (Object *)&(apage->data[apage->header.free]);

                /* set the object header */
                obj->header.properties = P_CLEAR;
                obj->header.length = objectBufferLen;
               
                /* copy the data into the object */
                memcpy(obj->data, objectBuffer, objectBufferLen);

                /* Modify slotted page slot */

                apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                /* Set Unique */

                /* If the unique numbers in this page is exhausted, */
                /* then request new unique numbers. */
                if (apage->header.unique >= apage->header.uniqueLimit) {

                    e = RDsM_GetUnique(handle, &(apage->header.pid), &unique, &num);
                    if (e < 0) ERRB1(handle, e, &(apage->header.pid), PAGE_BUF);
                    apage->header.unique = unique;
                    apage->header.uniqueLimit = unique + num;
                }

                /*@ Allocate a new unique number. */
                apage->slot[-(apage->header.nSlots)].unique = (apage->header.unique)++;

                /* Modify slotted page header */
                apage->header.nSlots++; /* increment # of slots */
                apage->header.free += sizeof(ObjectHdr) + alignedLen;

                /* return the object id of created object */
                if (oid != NULL) {
                    oid->pageNo = blkLdEntry->allocExtentPageIdAry[blkLdEntry->fileBufIdx].pageNo;
                    oid->volNo  = blkLdEntry->fid.volNo;
                    oid->slotNo = apage->header.nSlots - 1;
                    oid->unique = apage->slot[-(oid->slotNo)].unique;
                }

            }
        }
    }

    return(eNOERROR);

} /* OM_NextBulkLoad() */ 



/*@========================================
 *  OM_NextBulkLoadWriteLOT()
 * =======================================*/

/*
 * Function : Four OM_NextBulkLoadWriteLOT(Four, Four, Four, char*, Boolean, ObjectID*)
 *
 * Description :
 *
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *  0)
 *
 */

Four OM_NextBulkLoadWriteLOT(
    Four handle,
    Four          blkLdId,        /* IN  OM bulkload ID */ 
    Four          start,          /* IN starting offset of read */
    Four          length,         /* IN amount of data to read */
    char*         data,           /* IN user buffer holding the data */
    Boolean       endOfObject,    /* IN  flag indicating this buffer is end of object or not */
    ObjectID      *oid)           /* OUT the object's ObjectID */
{

    Four        e;                        /* error number */
    Four        neededSpace;              /* space needed to put new object [+ header] */
    Four        alignedLen;               /* aligned length of initial data */
    SlottedPage *apage;                   /* point to buffer holding the page */
    Object      *obj;                     /* point to the newly bulk loaded object */
    char        origObjBuffer[PAGESIZE];  /* original object buffer */
    Object      *origObj;                 /* original object */
    char        tempObj[LRGOBJ_THRESHOLD];/* buffer for temporary object */
    Four        tempObjLen;               /* length of temporary object */
    Boolean     isRootWithHdr;            /* indicate this object will be stored in large object with header form */
    L_O_T_INode *r_node;                  /* pointer to root node */
    L_O_T_INode *anode;                   /* pointer to new root node in slotted page */
    Unique      unique;                   /* space for the returned unique number */
    Four        num;                      /* number of unique numbers newly allocated */
    PageID      *tempPageId;              /* temporary pointer for swap */
    Four        numOfAllocTrains;         /* number of page to allocate for bulk load data file buffer */
    Two         numVarAttrs;              /* number of variable length column in the relation */
    Two         indexKeyLen;              /* length of clustering key of the object */
    SortStreamTuple     sortTuple;        /* tuple for sort stream */
    omSortKeyAttrInfo   attrInfo;
    OM_BlkLdTableEntry* blkLdEntry;      /* entry in which informattion about bulkload is saved */ 


    /*
     * set entry for fast access
     */
    blkLdEntry = &OM_BLKLD_TABLE(handle)[blkLdId]; /* insert a handle into OM_BLKLD_TABLE */


    /*
     * parameter checking
     */
    if (blkLdEntry->rootOfLOT.pageNo == NIL) ERR(handle, eBADPARAMETER);
 

    /* 
     * Write tuple header 
     */
    e = LOT_BlkLd_WriteObject(handle, &blkLdEntry->rootOfLOT, start, length, data);


    /*
     *  This object buffer is the end of object
     */
    if (endOfObject == TRUE) {


            /* 2-1-1. There is clustering index */
            if (blkLdEntry->isNeedSortFlag == TRUE) {

                /* 
                 *  object encoding to insert sort stream  
                 */
                origObj = (Object*)origObjBuffer;

                /* set the object header */
                origObj->header.properties = P_LRGOBJ;
                origObj->header.length = blkLdEntry->largeObjectLen;

                /* copy the data into the object */
                *((ShortPageID *)(origObj->data)) = blkLdEntry->rootOfLOT.pageNo;

                /*
                 *  convert object to sort temporary object format
                 */
                e = om_GetTempObject(handle, origObj, (Object *)tempObj, &tempObjLen, blkLdEntry->kdesc, blkLdEntry->getKeyAttrs, blkLdEntry->schema);
                if (e < 0) ERR(handle, e);

                /* set sortStreamTuple */
                sortTuple.len = tempObjLen;
                sortTuple.data = tempObj;

                /*
                 *  insert 'tempObj' into sort stream
                 */
                e = Util_PutTuplesIntoSortStream(handle, blkLdEntry->dataFileSortStreamID, 1, &sortTuple);
                if (e < 0) ERR(handle, e);

                /* return the object id to NULL because the object id is not decided */
                oid = NULL;

            /* 2-1-2. There is no clustering index */
            } else {

                /* Get the root page of large object */
                e = BfM_GetTrain(handle, &blkLdEntry->rootOfLOT, (char **)&r_node, PAGE_BUF);
                if (e < 0) ERR(handle, e);

                apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);

                /* If this large object can be stored in root with header form and there is enought space in page 
                   then store the object in root with header form.
                   If this large object can be stored in root with header from but there is not enought spage in page 
                   then store the object in large object form.  
                   If there is not enough space in page for large object form 
                   then try to store in the next buffer page - 보강 필요 */
                if (r_node->header.nEntries <= LOT_MAXENTRIES_ROOTWITHHDR) {

                    /* calculate needed space to store the object in root with header form */
                    alignedLen = ALIGNED_LENGTH(sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry) * r_node->header.nEntries); 
                    neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                    isRootWithHdr = TRUE;

                    if (SP_FREE(apage) < neededSpace || SP_FREE(apage) < blkLdEntry->minFreeSpace) {

                        /* calculate needed space to store the object in large object form */
                        alignedLen = ALIGNED_LENGTH(sizeof(ShortPageID));
                        neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                        isRootWithHdr = FALSE;

                        if(SP_FREE(apage) < neededSpace || SP_FREE(apage) < blkLdEntry->minFreeSpace){

                            /* try to store in the next buffer page */
                            blkLdEntry->fileBufIdx++;

                            /* end of the data file bulk load buffer */
                            if (blkLdEntry->fileBufIdx == blkLdEntry->bufSize) {

                                /*
                                 *  Flush data file bulk load buffer 
                                 */

                                /* swap the page id array to allocate new extent before flush data file bulk load buffer */ 
                                tempPageId = blkLdEntry->allocExtentPageIdAry;
                                blkLdEntry->allocExtentPageIdAry = blkLdEntry->flushExtentPageIdAry;
                                blkLdEntry->flushExtentPageIdAry = tempPageId;

                                /* allocate new extent */
                                numOfAllocTrains = blkLdEntry->bufSize;
                                e = om_BlkLdAllocExtent(handle, blkLdEntry->pFid, NULL, &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry); 
                                if(e < 0) ERR(handle, e);

                                if(numOfAllocTrains != blkLdEntry->bufSize) ERR(handle, eCANTALLOCEXTENT_BL_OM);

                                /* set the next page of last page in the data file bulk load buffer */
                                blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;

                                /* flush the data file bulk load buffer */
                                e = om_BlkLdFlushBuffer(handle, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize);
                                if(e < 0) ERR(handle, e);

                                /* initialize the data file bulk load buffer */
                                e = om_BlkLdInitDataFileBuffer(handle, blkLdId, NULL, blkLdEntry->bufSize, 
                                                              blkLdEntry->allocExtentPageIdAry, blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage);
                                if(e < 0) ERR(handle, e);

                                /* set the last allocated page */
                                blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

                                blkLdEntry->fileBufIdx = 0;
                            }

                            /* calculate needed space to store the object in root with header form */
                            alignedLen = ALIGNED_LENGTH(sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry) * r_node->header.nEntries); 
                            neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                            isRootWithHdr = TRUE;
                        }
                    }

                } else {

                    alignedLen = ALIGNED_LENGTH(sizeof(ShortPageID));
                    neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                    isRootWithHdr = FALSE;

                    if(SP_FREE(apage) < neededSpace || SP_FREE(apage) < blkLdEntry->minFreeSpace){

                        /* try to store in the next buffer page */
                        blkLdEntry->fileBufIdx++;

                        /* end of the data file bulk load buffer */
                        if (blkLdEntry->fileBufIdx == blkLdEntry->bufSize) {

                            /*
                             *  Flush data file bulk load buffer 
                             */

                            /* swap the page id array to allocate new extent before flush data file bulk load buffer */ 
                            tempPageId = blkLdEntry->allocExtentPageIdAry;
                            blkLdEntry->allocExtentPageIdAry = blkLdEntry->flushExtentPageIdAry;
                            blkLdEntry->flushExtentPageIdAry = tempPageId;

                            /* allocate new extent */
                            numOfAllocTrains = blkLdEntry->bufSize;
                            e = om_BlkLdAllocExtent(handle, blkLdEntry->pFid, NULL, &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry); 
                            if(e < 0) ERR(handle, e);

                            if(numOfAllocTrains != blkLdEntry->bufSize) ERR(handle, eCANTALLOCEXTENT_BL_OM);

                            /* set the next page of last page in the data file bulk load buffer */
                            blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;

                            /* flush the data file bulk load buffer */
                            e = om_BlkLdFlushBuffer(handle, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize);
                            if(e < 0) ERR(handle, e);

                            /* initialize the data file bulk load buffer */
                            e = om_BlkLdInitDataFileBuffer(handle, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry, blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage);
                            if(e < 0) ERR(handle, e);

                            /* set the last allocated page */
                            blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

                            blkLdEntry->fileBufIdx = 0;
                        }
                    }
                }           

                /* In this point,
                   omBlkLdFileBufIdx is the index of data file buffer that object will be bulkloaded
                   if isRootWithHdr is TRUE,  this object is root with header object 
                   if isRootWithHdr is FALSE, this object is large object */

                /* Insert "Large With Header Object" into "Data File Bulk Load Buffer" */
                if(isRootWithHdr == TRUE) {

                    /* where to put the object? */

                    apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);
                    obj = (Object *)&(apage->data[apage->header.free]);

                    /* set the object header */
                    obj->header.properties = P_LRGOBJ | P_LRGOBJ_ROOTWITHHDR;
                    obj->header.length = blkLdEntry->largeObjectLen;       

                    /* copy the data into the object */
                    anode = (L_O_T_INode *)obj->data;
                    memcpy((char*)anode, (char*)r_node,
                           sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*r_node->header.nEntries);

                    /* Modify slotted page slot */
                    apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                    /* Set Unique */

                    /* If the unique numbers in this page is exhausted, */
                    /* then request new unique numbers. */
                    if (apage->header.unique >= apage->header.uniqueLimit) {

                        e = RDsM_GetUnique(handle, &(apage->header.pid), &unique, &num);
                        if (e < 0) ERRB1(handle, e, &(apage->header.pid), PAGE_BUF);

                        apage->header.unique = unique;
                        apage->header.uniqueLimit = unique + num;
                    }

                    /*@ Allocate a new unique number. */
                    apage->slot[-(apage->header.nSlots)].unique = (apage->header.unique)++;

                    /* Modify slotted page header */

                    apage->header.nSlots++;      /* increment # of slots */
                    apage->header.free += sizeof(ObjectHdr) + alignedLen;

                    /* return the object id of created object */
                    if (oid != NULL) {
                        oid->pageNo = blkLdEntry->allocExtentPageIdAry[blkLdEntry->fileBufIdx].pageNo;
                        oid->volNo  = blkLdEntry->fid.volNo;
                        oid->slotNo = apage->header.nSlots - 1;
                        oid->unique = apage->slot[-(oid->slotNo)].unique;
                    }

                    /*@ free the root node */
                    e = BfM_FreeTrain(handle, &blkLdEntry->rootOfLOT, PAGE_BUF);
                    if(e < 0) ERR(handle, e);

                    /* remove page from buffer pool */
                    e = BfM_RemoveTrain(handle, &blkLdEntry->rootOfLOT, PAGE_BUF, FALSE);
                    if (e < 0) ERR(handle, e);

                    /* delete previous root page */
                    e = RDsM_FreeTrain(handle, &blkLdEntry->rootOfLOT, PAGESIZE2);
                    if(e < 0) ERR(handle, e);

                /* Insert "Large Object" into "Data File Bulk Load Buffer" */
                } else {

                    alignedLen = ALIGNED_LENGTH(sizeof(ShortPageID));

                    /* where to put the object[header]? */
                    apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);
                    obj = (Object *)&(apage->data[apage->header.free]);

                    /* set the object header */
                    obj->header.properties = P_LRGOBJ;
                    obj->header.length = blkLdEntry->largeObjectLen;

                    /* set the object body */

                    *((ShortPageID *)obj->data) = blkLdEntry->rootOfLOT.pageNo;

                    /* Modified slotted page slot */

                    apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                    /* Set Unique */

                    /* If the unique numbers in this page is exhausted, */
                    /* then request new unique numbers. */
                    if (apage->header.unique >= apage->header.uniqueLimit) {

                        e = RDsM_GetUnique(handle, &(apage->header.pid), &unique, &num);
                        if (e < 0) ERRB1(handle, e, &(apage->header.pid), PAGE_BUF);

                        apage->header.unique = unique;
                        apage->header.uniqueLimit = unique + num;
                    }

                    /*@ Allocate a new unique number. */
                    apage->slot[-(apage->header.nSlots)].unique = apage->header.unique++;

                    /* Modified slotted page header */

                    apage->header.nSlots++;      /* increment # of slots */
                    apage->header.free += sizeof(ObjectHdr) + alignedLen;


                    /* return the object id of created object */
                    if (oid != NULL) {
                        oid->pageNo = blkLdEntry->allocExtentPageIdAry[blkLdEntry->fileBufIdx].pageNo;
                        oid->volNo  = blkLdEntry->fid.volNo;
                        oid->slotNo = apage->header.nSlots - 1;
                        oid->unique = apage->slot[-(oid->slotNo)].unique;
                    }

                    /*@ free the root node */
                    e = BfM_FreeTrain(handle, &blkLdEntry->rootOfLOT, PAGE_BUF);
                    if(e < 0) ERR(handle, e);
                }
            }
          
            blkLdEntry->rootOfLOT.pageNo = NIL;
    }

    return(eNOERROR);

} /* OM_NextBulkLoadWriteLOT() */ 
