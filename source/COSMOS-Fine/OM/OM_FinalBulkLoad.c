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
/******************************************************************************//*
 * Module: OM_FinalBulkLoad.c
 *
 * Description:
 *  Finalize the data file bulk load.
 *
 * Exports:
 *  Four OM_FinalBulkLoad()
 */

#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "param.h"
#include "RDsM.h"
#include "BfM.h"
#include "LOT.h"
#include "OM.h"
#include "Util_Sort.h"
#include "BL_OM.h"

#include "error.h"
#include "latch.h"
#include "LOG.h"
#include "LM.h"

#include "perThreadDS.h"
#include "perProcessDS.h"

/*@========================================
 *  OM_FinalBulkLoad()
 * =======================================*/

/*
 * Function : Four OM_FinalBulkLoad()
 *
 * Description :
 *  Finalize the data file bulk load.
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *  0)
 *
 */

Four OM_FinalBulkLoad(
    Four                  handle,                    /* IN handle */
    XactTableEntry_T*     xactEntry,                 /* IN  entry of transaction table */
    Four                  blkLdId,                   /* IN  bulkload ID */ 
    LogParameter_T*       logParam)                  /* IN  log parameter */
{
    Four                  e;                         /* error number */
    Four                  i;                         /* index variable */
    Four                  neededSpace;               /* space needed to put new object [+ header] */
    Four                  alignedLen;                /* aligned length of initial data */
    SlottedPage           *apage;                    /* point to buffer holding the page */
    Object                *obj;                      /* point to the newly created object */
    char                  origObjBuffer[PAGESIZE];   /* buffer for original object */
    Object*               origObj;                   /* object pointer which points original object */
    char                  tempObj[LRGOBJ_THRESHOLD]; /* buffer for temporary object */
    Four                  tempObjLen;                /* length of temporary object */
    Boolean               done = FALSE;              /* flag which indicates sort stream is empty or not */
    Four                  numSortTuples;             /* # of tuples from sort stream */
    SortTupleDesc         sortTupleDesc;             /* key description of sort stream */
    SortStreamTuple       sortTuple;                 /* tuple for sort stream */
    Boolean               isRootWithHdr;             /* indicate this object will be stored in large object with header form */
    PageID                rootOfLOT;                 /* root page id of  large object */
    L_O_T_INodePage       *r_node;                   /* pointer to root node */
    L_O_T_INode           *anode;                    /* pointer to new root node in slotted page */
    Unique                unique;                    /* space for the returned unique number */
    Four                  num;                       /* number of unique numbers newly allocated */
    PageID                *tempPageId;               /* temporary pointer for swap */
    Four                  numOfAllocTrains;          /* number of page to allocate for bulk load data file buffer */
    sm_CatOverlayForData  *catEntry;                 /* pointer to data file catalog information */
    SlottedPage           *catPage;                  /* pointer to buffer containing the catalog */
    Four                  firstExtNo;                /* first extent number of the file */
    omSortKeyAttrInfo     sortAttrInfo;              /* information about sort key attributes */
    OM_BlkLdTableEntry*   blkLdEntry;                /* entry in which informattion about bulkload is saved */
    SlottedPage           *lastPage;                 /* the last page of data file */ 
    SlottedPage           firstPage;                 /* the first page of bulkloaded data file */ 

    Buffer_ACC_CB         *r_node_BCBP;
    Buffer_ACC_CB         *catPage_BCBP;             /* buffer access control block holding catalog data */
    Buffer_ACC_CB         *aPage_BCBP;               /* buffer access control block for a data page */

    Lsn_T                 lsn;                       /* lsn of the newly written log record */
    Four                  logRecLen;                 /* log record length */
    LOG_LogRecInfo_T      logRecInfo;                /* log record information */

    Boolean               allocFlag;
    Boolean               pageUpdateFlag;

    LockReply             lockReply;                 /* lock reply */
    LockMode              oldMode;
    SegmentID_T           pageSegmentID;             /* page segment ID */
    SegmentID_T           trainSegmentID;            /* train segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    /*
     * 0. set entry for fast access
     */

    blkLdEntry = &OM_BLKLD_TABLE(handle)[blkLdId];


    /*
     * 1. There is clustering index
     */

    if(blkLdEntry->isNeedSortFlag == TRUE) {

        /*
         *  Sort it!!
         */
        e = Util_SortingSortStream(handle, xactEntry, blkLdEntry->dataFileSortStreamID, logParam);
        if (e < eNOERROR) ERR(handle, e);

        while (1) {

            /* initialize numSortTuples & sortTuples */
            numSortTuples = 1;
            sortTuple.len = LRGOBJ_THRESHOLD;
            sortTuple.data = tempObj;

            /* get object from sort stream */
            e = Util_GetTuplesFromSortStream(handle, blkLdEntry->dataFileSortStreamID, &numSortTuples, &sortTuple, &done);
            if (e < eNOERROR) ERR(handle, e);

            if (done) break;

            /* Note!! sortAttrInfo doesn't have proper offset value */
            blkLdEntry->getKeyAttrs(handle, NULL, blkLdEntry->schema, blkLdEntry->kdesc, &sortAttrInfo);
            origObj = (Object*) origObjBuffer;

            /* you must remove sort key from each object!! */
	    e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, &blkLdEntry->finfo, &pageSegmentID, PAGESIZE2);
	    if (e < eNOERROR) ERR(handle, e);

	    e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, &blkLdEntry->finfo, &trainSegmentID, TRAINSIZE2);
	    if (e < eNOERROR) ERR(handle, e);

            e = om_RestoreTempObject(handle, xactEntry, &(blkLdEntry->fid), &pageSegmentID, &trainSegmentID, 
				     (Object *)tempObj, origObj, &sortAttrInfo, FALSE, logParam);
            if (e < eNOERROR) ERR(handle, e);

            /* 1.1. This object is large object */
            if(origObj->header.properties & P_LRGOBJ) {

                /* make root page id of large object tree */
                rootOfLOT.volNo  = blkLdEntry->fid.volNo;
                rootOfLOT.pageNo = *((ShortPageID*)(origObj->data));

                /* Get the root page of large object */
#ifdef CCPL
                e = BfM_getAndFixBuffer(handle, (TrainID*)&rootOfLOT, M_FREE, &r_node_BCBP, PAGE_BUF); 
                if(e < 0) ERR(handle, e);
#endif

#ifdef CCRL
                e = BfM_getAndFixBuffer(handle, (TrainID*)&rootOfLOT, M_SHARED, &r_node_BCBP, PAGE_BUF); 
                if(e < 0) ERR(handle, e);
#endif

		r_node = (L_O_T_INodePage *)r_node_BCBP->bufPagePtr;


                apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);

                /* This object can be converted to large object with header - It is explained in detail */
                if (r_node->node.header.nEntries <= LOT_MAXENTRIES_ROOTWITHHDR) {

                    /* calculate needed space to store the object in root with header form */
                    alignedLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry) * r_node->node.header.nEntries));

                    neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                    isRootWithHdr = TRUE;

                    if (SP_FREE(xactEntry->xactId, apage) < neededSpace || SP_FREE(xactEntry->xactId, apage) < blkLdEntry->minFreeSpace) {

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

                            e = om_BlkLdAllocExtent(handle, xactEntry, &blkLdEntry->finfo, blkLdEntry->pFid, NULL,
						    &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry,
                                                    &blkLdEntry->startPageNo, &blkLdEntry->direction, logParam);
                            if (e < eNOERROR) ERR(handle, e);

                            if(numOfAllocTrains != blkLdEntry->bufSize) ERR(handle, eCANTALLOCEXTENT_BL_OM);

                            /* set the next page of last page in the data file bulk load buffer */
                            blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;

                            /* flush the data file bulk load buffer */
                            e = om_BlkLdFlushBuffer(handle, xactEntry, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize, logParam);
                            if (e < eNOERROR) ERR(handle, e);

                            /* initialize the data file bulk load buffer */
                            e = om_BlkLdInitDataFileBuffer(handle, xactEntry, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry,blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage, logParam);
                            if (e < eNOERROR) ERR(handle, e);

                            /* set the last allocated page */
                            blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];
                            blkLdEntry->fileBufIdx = 0;
                        }
                    }

                /* This object must be stored in large object form */
                } else {

                    /* calculate needed space to store the object in large object form */
                    alignedLen = ALIGNED_LENGTH(sizeof(ObjectID));

                    neededSpace= alignedLen + sizeof(SlottedPageSlot) + sizeof(ObjectHdr);
                    isRootWithHdr = FALSE;

                    if (SP_FREE(xactEntry->xactId, apage) < neededSpace || SP_FREE(xactEntry->xactId, apage) < blkLdEntry->minFreeSpace) {

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

                            e = om_BlkLdAllocExtent(handle, xactEntry, &blkLdEntry->finfo, blkLdEntry->pFid, NULL, &numOfAllocTrains,
						    blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry,
						    &blkLdEntry->startPageNo, &blkLdEntry->direction, logParam);
                            if (e < eNOERROR) ERR(handle, e);

                            /* set the next page of last page in the data file bulk load buffer */
                            if(numOfAllocTrains != blkLdEntry->bufSize) ERR(handle, eCANTALLOCEXTENT_BL_OM);

                            /* flush the data file bulk load buffer */
                            blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;
                            e = om_BlkLdFlushBuffer(handle, xactEntry, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize, logParam);
                            if (e < eNOERROR) ERR(handle, e);

                            /* initialize the data file bulk load buffer */
                            e = om_BlkLdInitDataFileBuffer(handle, xactEntry, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry,blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage, logParam);
                            if (e < eNOERROR) ERR(handle, e);

                            /* set the last allocated page */
                            blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

                            blkLdEntry->fileBufIdx = 0;
                        }
                    }
                }

                /* In this point,
                   blkLdEntry->fileBufIdx is the index of data file buffer that object will be bulkloaded
                   if isRootWithHdr is TRUE,  this object is root with header object
                   if isRootWithHdr is FALSE, this object is large object */

                /* Insert "Large With Header Object" into "Data File Bulk Load Buffer" */
                if(isRootWithHdr == TRUE) {

                    /* where to put the object? */
                    apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);
                    obj = (Object *)&(apage->data[apage->header.free]);

                    /* set the object header */
                    obj->header.properties = P_LRGOBJ | P_LRGOBJ_ROOTWITHHDR;
                    obj->header.length = origObj->header.length;

                    /* copy the data into the object */

                    anode = (L_O_T_INode *)obj->data;

                    memcpy((char*)anode, (char*)(&(r_node->node)),
                           sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*r_node->node.header.nEntries);

                    /* Modify slotted page slot */

                    apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                    /* Set Unique */

                    /* If the unique numbers in this page is exhausted, */
                    /* then request new unique numbers. */
                    if (apage->header.unique >= apage->header.uniqueLimit) {

                        e = RDsM_GetUnique(handle, xactEntry, &(apage->header.pid), &unique, &num, logParam);
                        if (e < eNOERROR) ERR(handle, e);

                        apage->header.unique = unique;
                        apage->header.uniqueLimit = unique + num;
                    }

                    /*@ Allocate a new unique number. */
                    apage->slot[-(apage->header.nSlots)].unique = (apage->header.unique)++;

                    /* Modify slotted page header */

                    apage->header.nSlots++;      /* increment # of slots */
                    apage->header.free += sizeof(ObjectHdr) + alignedLen;
#ifdef CCRL
		    apage->header.totalFreeSpace -= neededSpace;
#endif /* CCRL */


                    /*@ free the root node */
#ifdef CCPL
                    e = BfM_unfixBuffer(handle, r_node_BCBP, PAGE_BUF);
                    if(e < eNOERROR) ERR(handle, e);
#endif

#ifdef CCRL
                    e = SHM_releaseLatch(handle, r_node_BCBP->latchPtr, procIndex);
                    if (e < eNOERROR) ERRB1(handle, e, r_node_BCBP, PAGE_BUF);

                    e = BfM_unfixBuffer(handle, r_node_BCBP, PAGE_BUF);
                    if(e < eNOERROR) ERR(handle, e);
#endif

                    /* remove page from buffer pool */
                    e = BfM_RemoveTrain(handle, &rootOfLOT, PAGE_BUF, FALSE);
                    if (e < eNOERROR) ERR(handle, e);

                    /* delete previous root page */
                    e = RDsM_FreeTrain(handle, xactEntry, &rootOfLOT, PAGESIZE2, TRUE, logParam);
                    if (e < eNOERROR) ERR(handle, e);

                /* Insert "Large Object" into "Data File Bulk Load Buffer" */
                } else {

                    alignedLen = ALIGNED_LENGTH(sizeof(ObjectID));

                    /* where to put the object[header]? */
                    apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);
                    obj = (Object *)&(apage->data[apage->header.free]);

                    /* set the object header */
                    obj->header.properties = P_LRGOBJ;
                    obj->header.length = origObj->header.length;

                    /* set the object body */
                    *((ShortPageID *)(obj->data)) = rootOfLOT.pageNo;

                    /* Modified slotted page slot */

                    apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                    /* Set Unique */

                    /* If the unique numbers in this page is exhausted, */
                    /* then request new unique numbers. */
                    if (apage->header.unique >= apage->header.uniqueLimit) {

                        e = RDsM_GetUnique(handle, xactEntry, &(apage->header.pid), &unique, &num, logParam);
                        if (e < eNOERROR) ERR(handle, e);

                        apage->header.unique = unique;
                        apage->header.uniqueLimit = unique + num;
                    }

                    /*@ Allocate a new unique number. */
                    apage->slot[-(apage->header.nSlots)].unique = apage->header.unique++;

                    /* Modified slotted page header */

                    apage->header.nSlots++;      /* increment # of slots */
                    apage->header.free += sizeof(ObjectHdr) + alignedLen;
#ifdef CCRL
                    apage->header.totalFreeSpace -= neededSpace;
#endif /* CCRL */


                    /*@ free the root node */
#ifdef CCPL
                    e = BfM_unfixBuffer(handle, r_node_BCBP, PAGE_BUF);
                    if(e < eNOERROR) ERR(handle, e);
#endif

#ifdef CCRL
                    e = SHM_releaseLatch(handle, r_node_BCBP->latchPtr, procIndex);
                    if (e < eNOERROR) ERRB1(handle, e, r_node_BCBP, PAGE_BUF);

                    e = BfM_unfixBuffer(handle, r_node_BCBP, PAGE_BUF);
                    if(e < eNOERROR) ERR(handle, e);
#endif

                }

            /* 1-2. This object is small object */

            } else {

                /*
                 * calculate the length to be needed in the slotted page.
                 * If need to create the large object, the slotted page only contains
                 * object header.
                 */
                 alignedLen  = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(origObj->header.length));

                neededSpace = sizeof(ObjectHdr) + alignedLen + sizeof(SlottedPageSlot);

                apage = &(blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx]);

                if (SP_FREE(xactEntry->xactId, apage) < neededSpace || SP_FREE(xactEntry->xactId, apage) < blkLdEntry->minFreeSpace) {

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

                        e = om_BlkLdAllocExtent(handle, xactEntry, &blkLdEntry->finfo, blkLdEntry->pFid, NULL, &numOfAllocTrains,
						blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry, &blkLdEntry->startPageNo,
						&blkLdEntry->direction, logParam);
                        if (e < eNOERROR) ERR(handle, e);

                        if(numOfAllocTrains != blkLdEntry->bufSize)
                            ERR(handle, eCANTALLOCEXTENT_BL_OM);

                        /* set the next page of last page in the data file bulk load buffer */
                        blkLdEntry->fileBuffer[blkLdEntry->bufSize -1].header.nextPage = blkLdEntry->allocExtentPageIdAry[0].pageNo;

                        /* flush the data file bulk load buffer */
                        e = om_BlkLdFlushBuffer(handle, xactEntry, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->bufSize, logParam);
                        if (e < eNOERROR) ERR(handle, e);

                        /* initialize the data file bulk load buffer */
                        e = om_BlkLdInitDataFileBuffer(handle, xactEntry, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry, blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage, logParam);
                        if (e < eNOERROR) ERR(handle, e);

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
                obj->header.length = origObj->header.length;

                /* copy the data into the object */
                memcpy(obj->data, origObj->data, origObj->header.length);

                /* Modify slotted page slot */

                apage->slot[-(apage->header.nSlots)].offset = apage->header.free;

                /* Set Unique */

                /* If the unique numbers in this page is exhausted, */
                /* then request new unique numbers. */
                if (apage->header.unique >= apage->header.uniqueLimit) {

                    e = RDsM_GetUnique(handle, xactEntry, &(apage->header.pid), &unique, &num, logParam);
                    if (e < eNOERROR) ERR(handle, e);

                    apage->header.unique = unique;
                    apage->header.uniqueLimit = unique + num;
                }

                /*@ Allocate a new unique number. */
                apage->slot[-(apage->header.nSlots)].unique = (apage->header.unique)++;

                /* Modify slotted page header */

                apage->header.nSlots++; /* increment # of slots */
                apage->header.free += sizeof(ObjectHdr) + alignedLen;
#ifdef CCRL
		apage->header.totalFreeSpace -= neededSpace;
#endif /* CCRL */

	    }
	}

        /* close sort stream */
        e = Util_CloseSortStream(handle, xactEntry, blkLdEntry->dataFileSortStreamID, logParam);
        if (e < eNOERROR) ERR(handle, e);

        /* End of sort stream */


        /*
         *  Flush data file bulk load buffer
         */

        /* swap the page id array to allocate new extent before flush data file bulk load buffer */
        tempPageId = blkLdEntry->allocExtentPageIdAry;
        blkLdEntry->allocExtentPageIdAry = blkLdEntry->flushExtentPageIdAry;
        blkLdEntry->flushExtentPageIdAry = tempPageId;

        /* flush the data file bulk load buffer */
        blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx].header.nextPage = NIL;
        e = om_BlkLdFlushBuffer(handle, xactEntry, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->fileBufIdx+1, logParam);
        if (e < eNOERROR) ERR(handle, e);

        /* remove unused page */
        for(i = blkLdEntry->fileBufIdx+1; i < blkLdEntry->bufSize; i++) {
            e = RDsM_FreeTrain(handle, xactEntry, &(blkLdEntry->flushExtentPageIdAry[i]), PAGESIZE2, TRUE, logParam);
            if (e < eNOERROR) ERR(handle, e);
        }

    /*
     * 2. There is no clustering index
     */

    } else {

        /*
         *  Flush data file bulk load buffer
         */

        /* swap the page id array to allocate new extent before flush data file bulk load buffer */
        tempPageId = blkLdEntry->allocExtentPageIdAry;
        blkLdEntry->allocExtentPageIdAry = blkLdEntry->flushExtentPageIdAry;
        blkLdEntry->flushExtentPageIdAry = tempPageId;

        /* flush the data file bulk load buffer */
        blkLdEntry->fileBuffer[blkLdEntry->fileBufIdx].header.nextPage = NIL;
        e = om_BlkLdFlushBuffer(handle, xactEntry, blkLdId, (char*)blkLdEntry->fileBuffer, blkLdEntry->flushExtentPageIdAry, blkLdEntry->fileBufIdx+1, logParam);
        if (e < eNOERROR) ERR(handle, e);

        /* remove unused page */
        for(i = blkLdEntry->fileBufIdx+1; i < blkLdEntry->bufSize; i++) {
            e = RDsM_FreeTrain(handle, xactEntry, &(blkLdEntry->flushExtentPageIdAry[i]), PAGESIZE2, TRUE, logParam);
            if (e < eNOERROR) ERR(handle, e);
        }
    }


    /*
     * Update page link in last page of original datafile and first page of
     * bulkload datafile when append bulkload occured
     */

     if (blkLdEntry->firstPageId.pageNo != NIL) {


        /* get the last page ID from catalog */
#ifdef CCPL

        /* Request X lock on the page where the catalog entry resides. */

		e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)(&(blkLdEntry->catObjForFile)),
			L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
		if (e < eNOERROR) ERR(handle, e);
		if (lockReply == LR_DEADLOCK) {
			ERR(handle, eDEADLOCK); /* deadlock */
		}

        e = BfM_getAndFixBuffer(handle, (TrainID*)(&(blkLdEntry->catObjForFile)), M_FREE, &catPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;

        GET_PTR_TO_CATENTRY_FOR_DATA((blkLdEntry->catObjForFile.slotNo), catPage, catEntry);
        blkLdEntry->lastAllocatedPage.pageNo = catEntry->lastPage;

        e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
        if(e < eNOERROR) ERR(handle, e);

        /* Release the lock on the catalog page. */

		e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)(&(blkLdEntry->catObjForFile)), L_MANUAL);
		if (e < eNOERROR) ERR(handle, e);


#endif

#ifdef CCRL

        e = BfM_getAndFixBuffer(handle, (TrainID*)(&(blkLdEntry->catObjForFile)), M_SHARED, &catPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;

        GET_PTR_TO_CATENTRY_FOR_DATA((blkLdEntry->catObjForFile.slotNo), catPage, catEntry);
        blkLdEntry->lastAllocatedPage.pageNo = catEntry->lastPage;

        e = SHM_releaseLatch(handle, catPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

        e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
        if(e < eNOERROR) ERR(handle, e);

#endif


        /* set the last page's next page */
#ifdef CCPL

        e = BfM_getAndFixBuffer(handle, (TrainID*)(&(blkLdEntry->lastAllocatedPage)), M_FREE, &aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        lastPage = (SlottedPage *)aPage_BCBP->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_MODIFY_PAGE_LIST_NEXT_LINK, LOG_REDO_UNDO,
                                  blkLdEntry->lastAllocatedPage, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &(blkLdEntry->firstPageId.pageNo),
                                  sizeof(ShortPageID), &(lastPage->header.nextPage));

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

            /* mark the lsn in the page */
            lastPage->header.lsn = lsn;
            lastPage->header.logRecLen = logRecLen;

        }

        lastPage->header.nextPage = blkLdEntry->firstPageId.pageNo;

        /* set dirty bit */
        aPage_BCBP->dirtyFlag = 1;

        e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
        if(e < eNOERROR) ERR(handle, e);
#endif

#ifdef CCRL

        e = BfM_getAndFixBuffer(handle, (TrainID*)(&(blkLdEntry->lastAllocatedPage)), M_SHARED, &aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        lastPage = (SlottedPage *)aPage_BCBP->bufPagePtr;


        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_MODIFY_PAGE_LIST_NEXT_LINK, LOG_REDO_UNDO,
                                  blkLdEntry->lastAllocatedPage, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &(blkLdEntry->firstPageId.pageNo),
                                  sizeof(ShortPageID), &(lastPage->header.nextPage));

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

            /* mark the lsn in the page */
            lastPage->header.lsn = lsn;
            lastPage->header.logRecLen = logRecLen;

        }

        lastPage->header.nextPage = blkLdEntry->firstPageId.pageNo;

        /* set dirty bit */
        aPage_BCBP->dirtyFlag = 1;

        e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

        e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
        if(e < eNOERROR) ERR(handle, e);

#endif

	e = BfM_FlushTrain(handle, &blkLdEntry->firstPageId, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        /* set the first page's previous page */

	e = RDsM_ReadTrain(handle, &blkLdEntry->firstPageId, (char*)&firstPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        firstPage.header.prevPage = blkLdEntry->lastAllocatedPage.pageNo;

	/*
	 *	We don't hove to flush this train,
	 *	because we already flush the train before the first write of this train in this bulkloadk.
	 *
	 * e = BfM_FlushTrain(handle, &blkLdEntry->firstPageId, PAGE_BUF);
	 * if (e < eNOERROR) ERR(handle, e);
	 *
	 */

        e = RDsM_WriteTrain(handle, (char*)&firstPage, &blkLdEntry->firstPageId, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* The above write is the last write of this page in this bulkload.
         * After the last write, the page should be removed from the buffer.
         * The page in the buffer is not dirty because we flush the buffer
         * before the first write of this page in this bulkload.
         */
        e = BfM_RemoveTrain(handle, &blkLdEntry->firstPageId, PAGE_BUF, FALSE);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
     * Update data file catalog information
     */

    /*
    ** Request X lock on the page.
    ** We use the flat page lock because 1) we don't know the file id of
    ** the catalog table and 2) the check of file lock is an overhead.
    */

        e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)(&(blkLdEntry->catObjForFile)),
                             L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERR(handle, e);

        if (lockReply == LR_DEADLOCK) {
            ERR(handle, eDEADLOCK);
        }

    /* Get the file's catalog entry */
    e = BfM_getAndFixBuffer(handle, (TrainID*)(&(blkLdEntry->catObjForFile)), M_FREE, &catPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;

    GET_PTR_TO_CATENTRY_FOR_DATA((blkLdEntry->catObjForFile.slotNo), catPage, catEntry);


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_MODIFY_LAST_PAGE_IN_CATALOG_ENTRY, LOG_REDO_UNDO,
                              catPage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(((blkLdEntry->finfo).catalog.oid.slotNo)), &((blkLdEntry->finfo).catalog.oid.slotNo),
                              sizeof(ShortPageID), &(blkLdEntry->flushExtentPageIdAry[blkLdEntry->fileBufIdx].pageNo),
                              sizeof(ShortPageID), &(catEntry->lastPage));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

        /* mark the lsn in the page */
        catPage->header.lsn = lsn;
        catPage->header.logRecLen = logRecLen;
    }

    /* update catalog infomation */
    catEntry->lastPage         = blkLdEntry->flushExtentPageIdAry[blkLdEntry->fileBufIdx].pageNo;

    /* set dirty bit */
    catPage_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
    if(e < eNOERROR) ERR(handle, e);


    /* Release the lock on the catalog page. */
        e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)(&(blkLdEntry->catObjForFile)), L_MANUAL);
        if (e < eNOERROR) ERR(handle, e);

    /*
     * free the heap memory
     */

    free(blkLdEntry->fileBuffer);
    free(blkLdEntry->allocExtentPageIdAry);
    free(blkLdEntry->flushExtentPageIdAry);


    /*
     * empty entry of OM bulkload table
     */

    blkLdEntry->isUsed = FALSE;


    return(eNOERROR);

}
