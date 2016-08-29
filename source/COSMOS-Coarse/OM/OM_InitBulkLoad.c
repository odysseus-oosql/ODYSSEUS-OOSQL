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
 * Module: OM_InitBulkLoad.c
 *
 * Description:
 *  Initialize data file bulk load.  
 *
 * Exports:
 *  Four OM_InitBulkLoad(VolID, ObjectID*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*, Boolean, Two, Two, PageID*
 */

#include <stdio.h>  
#include <stdlib.h> /* for malloc & free */
#include <string.h> 

#include "common.h"
#include "param.h"
#include "bl_param.h"
#include "RDsM_Internal.h" 	
#include "BfM.h"
#include "OM_Internal.h"
#include "BL_OM_Internal.h"
#include "Util_Sort.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*@========================================
 *  OM_InitBulkLoad()
 * =======================================*/

/*
 * Function : Four OM_InitBulkLoad(VolID, ObjectID*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*, Boolean, Two, Two, PageID*)
 *
 * Description :
 *  Initialize data file bulk loading. Data structure of data file bulk load 
 *  is initialized and set default value. If the input data must be sorted by
 *  clustering index key, open sort stream. And allocate first extent for bulk
 *  load. 
 *
 * Return Values :
 *  OM bulkload ID
 *  error code.
 * 
 * Side Effects :
 *    0) data file bulk load buffer is allocated and initialized
 *    1) if sort is needed, open sort stream 
 *    2) allocate first extent for bulk load
 */


Four OM_InitBulkLoad(
    Four handle,
    VolID      				tmpVolId,               /* IN  temporary volume ID in which sort stream is created */ 
    ObjectID    			*catObjForFile,         /* IN  file that data bulk load is to be processed */
    SortKeyDesc 			*kdesc,                 /* IN  sort key description */
    omGetKeyAttrsFuncPtr_T  getKeyAttrs,  			/* IN  object analysis function */
    void        			*schema,                /* IN  schema for analysis function */
    Boolean     			isNeedSort,             /* IN  flag indicating input data must be sorted by clustering index key */
    Two         			pff,                    /* IN  page fill factor */
    Two         			eff,                    /* IN  extent fill factor */
    PageID      			*firstPageID)           /* OUT first page ID of this bulkload */
{

    Four                 e;               /* error number */
    Two                  i;               /* index variable */
    Two                  idx;
    sm_CatOverlayForData *catEntry;       /* pointer to data file catalog information */
    SlottedPage          *catPage;        /* pointer to buffer containing the catalog */
    SortTupleDesc        sortTupleDesc;   /* key description of sort stream */
    omSortKeyAttrInfo    sortAttrInfo;    /* information about sort key attributes */
    Four                 numOfAllocTrains;/* number of allocated trains */
    SlottedPage          firstPage;       /* the first page of data file */
    SlottedPage          *lastPage;       /* the last page of data file */
    Boolean              isEmpty;         /* flag which indicates data file is empty or not */
    Four                 blkLdId;         /* OM bulkload ID */
    OM_BlkLdTableEntry*  blkLdEntry;      /* entry in which informattion about bulkload is saved */


    /* 
     * 0. parameter checking 
     */

    if (catObjForFile == NULL) ERR(handle, eBADPARAMETER);
    if (isNeedSort == TRUE && (kdesc == NULL || getKeyAttrs == NULL)) ERR(handle, eBADPARAMETER);


    /*
     * 1. Find empty entry from BtM bulkload table
     */

    for (blkLdId = 0; blkLdId < OM_BLKLD_TABLE_SIZE; blkLdId++ ) {
        if (OM_BLKLD_TABLE(handle)[blkLdId].isUsed == FALSE) /* insert a handle into OM_BLKLD_TABLE */
	    break;
    }
    if (blkLdId == OM_BLKLD_TABLE_SIZE) ERR(handle, eBLKLDTABLEFULL);

    /* set entry for fast access */
    blkLdEntry = &OM_BLKLD_TABLE(handle)[blkLdId]; /* insert a handle into OM_BLKLD_TABLE */

    /* set isUsed flag */
    blkLdEntry->isUsed = TRUE;


    /*
     * 2. initailze the main memory data structure used in data file bulk load
     */

    /* Get the file's ID, last allocated page ID */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    blkLdEntry->fid = catEntry->fid;
    MAKE_PHYSICALFILEID(blkLdEntry->pFid, catEntry->fid.volNo, catEntry->firstPage); 
    blkLdEntry->lastAllocatedPage.volNo = blkLdEntry->fid.volNo;

    blkLdEntry->lastAllocatedPage.pageNo = NIL;
    blkLdEntry->catObjForFile = *catObjForFile;

    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* Get the extent size of this volume to allocate buffer */

    e = RDsM_GetSizeOfExt(handle, blkLdEntry->fid.volNo, &(blkLdEntry->sizeOfExt)); 
    if (e < 0) ERR(handle, e);
    
    /* allocate memory for data file bulk load buffer */

    blkLdEntry->bufSize = (blkLdEntry->sizeOfExt * eff) / 100 * BLKLD_WRITEBUFFERSIZE;  
    blkLdEntry->minFreeSpace = PAGESIZE - (PAGESIZE * pff) / 100;
    blkLdEntry->fileBuffer = (SlottedPage *) malloc(PAGESIZE * blkLdEntry->bufSize); 
    blkLdEntry->fileBufIdx = 0;      
    blkLdEntry->allocExtentPageIdAry = (PageID *) malloc(sizeof(PageID) * blkLdEntry->bufSize);
    blkLdEntry->flushExtentPageIdAry = (PageID *) malloc(sizeof(PageID) * blkLdEntry->bufSize);

    /* set global variable using function parameter */
    blkLdEntry->isNeedSortFlag = isNeedSort;

    blkLdEntry->kdesc  = kdesc;
    blkLdEntry->getKeyAttrs = getKeyAttrs;
    blkLdEntry->schema = schema;

    blkLdEntry->pff = pff;                 
    blkLdEntry->eff = eff;

    /* set root of large object to NIL indicating no large object */
    blkLdEntry->rootOfLOT.pageNo = NIL;     
 
    /* set current data file volume number to call OM_ReadLargeObject() */
    OM_PER_THREAD_DS(handle).curVolNo = blkLdEntry->fid.volNo;	


    /*
     * 3. if sort is needed, open sort stream  
     */

    if (isNeedSort == TRUE) {

        /*  Get 'sortAttrinfo' & 'sortTupleDesc' */
        /* Note!! sortAttrInfo doesn't have proper offset value */
        getKeyAttrs(NULL, blkLdEntry->schema, blkLdEntry->kdesc, &sortAttrInfo);

        /* set 'nparts' */
        sortTupleDesc.nparts = sortAttrInfo.nparts;

        /* set 'hdrSize' */
        sortTupleDesc.hdrSize = sizeof(ObjectHdr) + OFFSETARRAYSIZE(sortAttrInfo.nparts);

        /* set each part information */
        for (i = 0; i < sortAttrInfo.nparts; i++ ) {
            sortTupleDesc.parts[i].type = sortAttrInfo.parts[i].type;
            sortTupleDesc.parts[i].length = sortAttrInfo.parts[i].length;
            sortTupleDesc.parts[i].flag = blkLdEntry->kdesc->parts[i].flag;
        }

        /*  Open sort stream */
        blkLdEntry->dataFileSortStreamID = Util_OpenSortStream(handle, tmpVolId, &sortTupleDesc);
        if (blkLdEntry->dataFileSortStreamID < 0) ERR(handle, blkLdEntry->dataFileSortStreamID);
    }


    /*
     * 4. Allocate Extent
     */

    /* read first page of the data file */
    e = BfM_readTrain(handle, &blkLdEntry->pFid, (char*)&firstPage, PAGE_BUF); 
    if (e < 0) ERR(handle, e);

    /* indicate this data file is empty or not empty */

    if (firstPage.header.nextPage == NIL) {

        for (idx = 0; idx < firstPage.header.nSlots; idx++) {
            if (firstPage.slot[-idx].offset != EMPTYSLOT) {
                isEmpty = FALSE;
                break;
            }
        }
        if (idx == firstPage.header.nSlots) {
            isEmpty = TRUE;
        }


    } else {
        isEmpty = FALSE;
    }
   
    /* if the data file is empty, allocate extent which contains first page of the data file 
       if the data file is not empty, allocate new extent */
    if(isEmpty == TRUE) {

        /* allocate extent which contains first page of the data file */
        numOfAllocTrains = blkLdEntry->bufSize;

        e = om_BlkLdAllocExtent(handle, blkLdEntry->pFid, &blkLdEntry->pFid,  
                                &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry); 

        if(e < 0) ERR(handle, e);

        if(numOfAllocTrains != blkLdEntry->bufSize) ERR(handle, eCANTALLOCEXTENT_BL_OM);

        /* initialize bulk load data file buffer */
        e = om_BlkLdInitDataFileBuffer(handle, blkLdId, &blkLdEntry->pFid, blkLdEntry->bufSize,  
                                       blkLdEntry->allocExtentPageIdAry, blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage);
        if(e < 0) ERR(handle, e);

        /* set last allocated page id */
        blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

        /* set first page of bulkloaded page */
        blkLdEntry->firstPageId.volNo  = NIL;
        blkLdEntry->firstPageId.pageNo = NIL;


    } else {

        /* allocate new extent */
        numOfAllocTrains = blkLdEntry->bufSize;

        e = om_BlkLdAllocExtent(handle, blkLdEntry->pFid, NULL, &numOfAllocTrains, blkLdEntry->eff, blkLdEntry->allocExtentPageIdAry);  
        if(e < 0) ERR(handle, e);

        if(numOfAllocTrains != blkLdEntry->bufSize)
            ERR(handle, eCANTALLOCEXTENT_BL_OM);

        /* initialize bulk load data file buffer */
        e = om_BlkLdInitDataFileBuffer(handle, blkLdId, NULL, blkLdEntry->bufSize, blkLdEntry->allocExtentPageIdAry, 
                                       blkLdEntry->fileBuffer, blkLdEntry->lastAllocatedPage);
        if(e < 0) ERR(handle, e);

        /* set last allocated page id */
        blkLdEntry->lastAllocatedPage = blkLdEntry->allocExtentPageIdAry[blkLdEntry->bufSize - 1];

        /* set first page of bulkloaded page */
		blkLdEntry->firstPageId.volNo  = blkLdEntry->allocExtentPageIdAry->volNo;
		blkLdEntry->firstPageId.pageNo = blkLdEntry->allocExtentPageIdAry->pageNo;

    }

    if (firstPageID != NULL) {
        firstPageID->pageNo = blkLdEntry->allocExtentPageIdAry->pageNo;
        firstPageID->volNo  = blkLdEntry->allocExtentPageIdAry->volNo;
    } 

    return(blkLdId);

} /* OM_InitBulkLoad() */
