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
 * Module: lot_InsertInDataPage.c
 *
 * Description:
 *  Append data to data page.
 *
 * Exports:
 *  Four lot_InsertInDataPage(catObjForFile*, Four, Four, char *, L_O_T_ItemList*, Boolean*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "Util.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_InsertInDataPage()
 *================================*/
/*
 * Function: Four lot_InsertInDataPage(catObjForFile*, Four, Four, char *, L_O_T_ItemList*, Boolean*)
 *
 * Description:
 *  Append data to data page.
 *
 * Returns:
 *  error code
 *    eMEMALLOCERR_LOT
 *    some errors caused by function calls
 */
Four lot_InsertInDataPage(
    Four handle,
    ObjectID                    *catObjForFile,         /* IN file info holding the LOT */
    Four                        start,                  /* IN starting offset of insert */
    Four                        length,                 /* IN amount of data to insert */
    char                        *newData,               /* IN data to be inserted */
    L_O_T_ItemList              *list,                  /* INOUT overflowed items to be inserted into parent node */
    Boolean                     *overflow)              /* OUT flag indicating data page has been split */
{
    Four                        e;                      /* error number */
    FileID                      fid;                    /* ID of file containing the LOT */
    Two                         eff;                    /* data file's extent fill factor */
    Four                        firstExt;               /* first Extent No of the file */
    Four                        origBytes;              /* # of bytes in original data page */
    Four                        totalBytes;             /* total # of bytes in original data and new data */
    Four                        nTrains;                /* # of trains to be needed to accomated total Bytes */
    Four                        bytesPerTrain;          /* # of bytes balanced in all the trains */
    Four                        remainBytes;            /* # of bytes after balancing */
    Four                        nBytes;                 /* # of bytes to copy to current data page */
    Four                        movedBytes;             /* # of bytes copied for current node at that time */
    Four                        toStart;                /* yet unused bytes befor start location */
    PageID                      *newPids;               /* PageIDs to be newly allocated */
    PageID                      origPid;                /* Original PageID */
    PageID                      currentPid;             /* PageID being processed currently */
    L_O_T_LNode                 *origPage;              /* pointer to the DataPage buffer */
    L_O_T_LNode                 *currentPage;           /* pointer to the current page's buffer */
    char                        tmpData[LOT_LNODE_MAXFREE]; /* save the original bytes */
    char                        *ptr;                   /* point to bytes to be copied next time */
    Four                        src;                    /* where the bytes comes ? */
    SlottedPage                 *catPage;               /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;              /* overay structure for catalog object access */
    Four                        i, j;
    Four                        n;
    Boolean                     isTmp;                  
    Four                        nOrigTrains;            /* number of original trains */ 

    
    TR_PRINT(TR_LOT, TR1,
             ("lot_InsertInDataPage(handle, catObjForFile=%P, start=%ld, length=%ld, newData=%P, list=%P, overflow=%ld)",
	      catObjForFile, start, length, newData, list, overflow));
      
    /* Get the file ID & extent fill factor from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    eff = catEntry->eff;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    *overflow = FALSE;

    /* check this large object is temporary or not */
    e = lot_IsTemporary(handle, &fid, &isTmp);
    if (e < 0) ERR(handle, e);

    /*@ Read the original Page */
    MAKE_PAGEID(origPid, fid.volNo, list->entry[list->nEntries-1].spid);
    e = BfM_GetTrain(handle, &origPid, (char **)&origPage, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);
    
    if (list->entry[list->nEntries-1].count + length <= LOT_LNODE_MAXFREE) {
	
	/* the new data can be put into the current leaf node */	

	/* prepare space moving bytes after start */
	n = list->entry[list->nEntries-1].count - start;
	memmove(&origPage->data[start+length], &origPage->data[start], n);

	/*@ Insert the new data */	
	memcpy(&origPage->data[start], newData, length);
	
	e = BfM_SetDirty(handle, &origPid, LOT_LEAF_BUF);
	if (e < 0) ERRB1(handle, e, &origPid, LOT_LEAF_BUF);
	
    } else {
	
	/* origDataSize is total # of bytes in original data page(s) */
	origBytes = list->entry[0].count;
	nOrigTrains = list->nEntries; 
	totalBytes = origBytes + length;
	nTrains = (totalBytes/LOT_LNODE_MAXFREE) +
	    (((totalBytes % LOT_LNODE_MAXFREE) > 0 ? 1:0));
	
	/*@ allocate the needed trains */
	/* that is, (nTrains - itemList->nEntries) */
	e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
	if (e < 0) ERRB1(handle, e, &origPid, LOT_LEAF_BUF);

        e = Util_reallocVarArray(handle, &LOT_PER_THREAD_DS(handle).lot_pageidArray, sizeof(PageID), nTrains-list->nEntries);
        if (e < 0) ERR(handle, e);
        
	newPids = (PageID *)LOT_PER_THREAD_DS(handle).lot_pageidArray.ptr; 
	
	e = RDsM_AllocTrains(handle, fid.volNo, firstExt, &origPid,
			     eff, nTrains - list->nEntries,
			     TRAINSIZE2, newPids); 
	if (e < 0) ERRB1(handle, e, &origPid, LOT_LEAF_BUF);

	/* save the ShortPageID in the itemList */
	for (i = list->nEntries, j = 0; i < nTrains; i++, j++)
	    list->entry[i].spid = newPids[j].pageNo;
	/* free(newPids); */
	list->nEntries = nTrains;
	    
	bytesPerTrain = totalBytes / nTrains;
	remainBytes = totalBytes % nTrains;
	
	/* save the original data into tmpData */
	memcpy(tmpData, origPage->data, origBytes);
	
	/* From now the variable 'i' denotes the entry index in the itemList */
	/* At first, take the data from the original data */
	/* If reach start position, use the new data */
	/* After all the new data are used, use the data after start pos. */
	src = 0;
	ptr = &tmpData[0];
	toStart = start;	/* # of bytes unused before start position */
	for (i = 0; i < nTrains; i++) {

	    MAKE_PAGEID(currentPid, fid.volNo, list->entry[i].spid);
	    
	    /* if currentPage is original page, use BfM_GetTrain() */
	    if (i < nOrigTrains) {
		e = BfM_GetTrain(handle, &currentPid, (char **)&currentPage, LOT_LEAF_BUF);
		if (e < 0) ERRB1(handle, e, &origPid, LOT_LEAF_BUF);
	    }
	    /* if currentPage is allocated new page, use BfM_GetNewTrain() */
	    else {
		e = BfM_GetNewTrain(handle, &currentPid, (char **)&currentPage, LOT_LEAF_BUF);
		if (e < 0) ERRB1(handle, e, &origPid, LOT_LEAF_BUF);
	    }

		/* set the page id */
		currentPage->hdr.pid = currentPid;
		
		/* set page type */
		SET_PAGE_TYPE(currentPage, LOT_L_NODE_TYPE);

		/* set temporary flag */
		if( isTmp ) SET_TEMP_PAGE_FLAG(currentPage);
		else        RESET_TEMP_PAGE_FLAG(currentPage);

	    /* get # of bytes for the current page to hold */
	    nBytes = bytesPerTrain + ((remainBytes > i) ? 1:0);

	    movedBytes = 0;
	    while (movedBytes < nBytes) {
				
		switch(src) {
		  case 0:	/* take the entries from the original entries */
		    n = MIN(nBytes - movedBytes, toStart);
		    memcpy(&currentPage->data[movedBytes], ptr, n);
		    ptr += n;
		    toStart -= n;
		    movedBytes += n;
		    
		    if (toStart == 0) {
			src = 1; /* From next, use the new entries */
			ptr = newData;
		    }
		    
		    break;
		    
		  case 1:	/* take the entries from the new entries */
		    n = MIN(nBytes - movedBytes, length);
		    memcpy(&currentPage->data[movedBytes], ptr, n);
		    ptr += n;
		    length -= n;
		    movedBytes += n;
		    
		    if (length == 0) {
			src = 2;	/* From now, use the original entries again */
			ptr = &tmpData[start];
		    }
		    
		    break;
		    
		  case 2:	/* take the entries from the original entries */
		    n = nBytes - movedBytes;
		    memcpy(&currentPage->data[movedBytes], ptr, n);
		    ptr += n;
		    movedBytes += n;
		    
		    break;
		    
		} /* end of switch */
	    }

	    e = BfM_SetDirty(handle, &currentPid, LOT_LEAF_BUF);
	    if (e < 0) ERRB2(handle, e, &currentPid, LOT_LEAF_BUF, &origPid, LOT_LEAF_BUF);

	    e = BfM_FreeTrain(handle, &currentPid, LOT_LEAF_BUF);
	    if (e < 0) ERRB1(handle, e, &origPid, LOT_LEAF_BUF);

	    /*@ construct the itemList */
	    list->entry[i].count = nBytes;
	}
		
	*overflow = TRUE;
    }
	    
    e = BfM_FreeTrain(handle, &origPid, LOT_LEAF_BUF); 
    if (e < 0) ERR(handle, e);
	
    return(eNOERROR);

} /* lot_InsertInDataPage() */
