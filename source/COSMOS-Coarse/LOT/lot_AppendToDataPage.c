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
 * Module: lot_AppendToDataPage.c
 *
 * Description:
 *  Append data to data page.
 *
 * Exports:
 *  Four lot_AppendToDataPage(ObjectID*, Four, char*, L_O_T_ItemList*, Boolean*)
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
 * lot_AppendToDataPage()
 *================================*/
/*
 * Function: Four lot_AppendToDataPage(ObjectID*, Four, char*, L_O_T_ItemList*, Boolean*)
 *
 * Description:
 *  Append data to data page.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four lot_AppendToDataPage(
    Four handle,
    ObjectID                    *catObjForFile,         /* IN file info holding the LOT */
    Four                        length,                 /* IN amount of data to append */
    char                        *newData,               /* IN data to be appended */
    L_O_T_ItemList              *list,                  /* INOUT overflowed items to be inserted into parent node */
    Boolean                     *overflow)              /* OUT flag indicating data page has been split */
{
    Four                        e;                      /* error number */
    FileID                      fid;                    /* ID of file which has the LOT */
    Two                         eff;                    /* data file's extent fill factor */
    Four                        firstExt;               /* first Extent No of the file */
    Four                        origBytes;              /* # of bytes in original data page */
    Four                        totalBytes;             /* total # of bytes in original data and new data */
    Four                        nOrigTrains;            /* # of original trains participating in this operation */
    Four                        nTrains;                /* # of trains to be needed to accomated total Bytes */
    Four                        nPartialTrains;         /* # of trains filled partially */
    Four                        remainBytes;            /* # of bytes in partially filled trains */
    Four                        remain;                 /* bytes remained after balancing the remainBytes */
    Four                        bytesPerTrain;          /* # of bytes balanced in the partially filled trains */
    Four                        nBytes;                 /* # of bytes to copy to current data page */
    PageID                      *newPids;               /* PageIDs to be newly allocated */
    PageID                      origPid;                /* Original PageID */
    PageID                      currentPid;             /* PageID being processed currently */
    L_O_T_LNode                 *origPage;              /* pointer to the DataPage buffer */
    L_O_T_LNode                 *currentPage;           /* pointer to the current page's buffer */
    SlottedPage                 *catPage;               /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;              /* overay structure for catalog object access */
    char                        *currentDataPtr;
    char                        tmpData1[LOT_LNODE_MAXFREE];
    char                        tmpData2[LOT_LNODE_MAXFREE];
    char                        *tmpData1Ptr, *tmpData2Ptr;
    Four                        tmpData1Bytes, tmpData2Bytes;
    Boolean                     Dirty;                  /* Is the page dirty? */
    Four                        i;
    Boolean isTmp;              

    
    TR_PRINT(TR_LOT, TR1,
             ("lot_AppendToDataPage(handle, catObjForFile=%P, length=%ld, newData=%P, list=%P, overflow=%ld)",
	      catObjForFile, length, newData, list, overflow));
    
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

    if (list->entry[list->nEntries-1].count + length <= LOT_LNODE_MAXFREE) {
	
	/* the new data can be put into the last page */	
	MAKE_PAGEID(origPid, fid.volNo, list->entry[list->nEntries-1].spid);
	e = BfM_GetTrain(handle, &origPid, (char **)&origPage, LOT_LEAF_BUF);
	if (e < 0) ERR(handle, e);
	
	memcpy(origPage->data + list->entry[list->nEntries-1].count, newData, length);
	
	e = BfM_SetDirty(handle, &origPid, LOT_LEAF_BUF);
	if (e < 0) ERRB1(handle, e, &origPid, LOT_LEAF_BUF);
	
	e = BfM_FreeTrain(handle, &origPid, LOT_LEAF_BUF); 
	if (e < 0) ERR(handle, e);
	
    } else {
	
	/* origDataSize is total # of bytes in original data page(s) */
	origBytes = list->entry[0].count +
	    ((list->nEntries == 2) ? list->entry[1].count:0);
	nOrigTrains = list->nEntries;
	totalBytes = origBytes + length;
	nTrains = (totalBytes/LOT_LNODE_MAXFREE) +
	    (((totalBytes % LOT_LNODE_MAXFREE) > 0 ? 1:0));
	
	/*@
	 * allocate the needed trains, nTrains
	 */
        e = Util_reallocVarArray(handle, &LOT_PER_THREAD_DS(handle).lot_pageidArray, sizeof(PageID), nTrains); 
        if (e < 0) ERR(handle, e);
        
	newPids = (PageID *)LOT_PER_THREAD_DS(handle).lot_pageidArray.ptr; 
		
	/* Reuse the already allocated leaf nodes */
	for (i = 0; i < nOrigTrains; i++)
	    MAKE_PAGEID(newPids[i], fid.volNo, list->entry[i].spid);

	if(nTrains > nOrigTrains) {
	    /* newly allocated trains: (nTrains - nOrigTrains) */
	    e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
	    if (e < 0) ERR(handle, e);
	    
	    e = RDsM_AllocTrains(handle, fid.volNo, firstExt, &newPids[nOrigTrains-1],
				 eff, nTrains - nOrigTrains,
				 TRAINSIZE2, newPids+nOrigTrains); 
	    if (e < 0) ERR(handle, e);
	}
	
	if ((totalBytes % LOT_LNODE_MAXFREE) > 0) {
	    
	    /* There is a partially filled leaf node(s). */
	    if ((totalBytes % LOT_LNODE_MAXFREE) >= LOT_LNODE_HALFFREE) 
		nPartialTrains = 1;
	    else
		nPartialTrains = 2;
	    
	    remainBytes = (totalBytes % LOT_LNODE_MAXFREE) +
		((nPartialTrains == 2) ? LOT_LNODE_MAXFREE:0);
	    bytesPerTrain = remainBytes / nPartialTrains;
	    
	    /* The 'remain' are 1 or 0. When nPartialTrains is 1,
	       the 2nd last page has 1 more byte than the last page
	       if remainsBytes is odd number. */
	    remain = remainBytes % nPartialTrains;
	    
	} else {
	    
	    /* all nodes are filled completely */
	    nPartialTrains = 0;
	}
	
	/* When two original pages participate,
	   copy the data in the last page into the temporary buffer */
	if (nOrigTrains == 2) {
	    MAKE_PAGEID(origPid, fid.volNo, list->entry[1].spid);
	    
	    e = BfM_GetTrain(handle, &origPid, (char **)&origPage, LOT_LEAF_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    memcpy(tmpData2, origPage->data, list->entry[1].count);
	    tmpData2Bytes = list->entry[1].count;
	    
	    e = BfM_FreeTrain(handle, &origPid, LOT_LEAF_BUF);
	    if (e < 0) ERR(handle, e);
	} else
	    tmpData2Bytes = 0;
	
	/* From now the variable 'i' denotes the entry index in the list */
	tmpData1Bytes = 0;
	for (i = 0; i < nTrains; i++) {
	    
	    Dirty = TRUE;	/* current page is to be dirty mostly */
	    
	    currentPid = newPids[i];
	    
	    /* if currentPage is original page, use BfM_GetTrain() */
	    if (i < nOrigTrains) {
		e = BfM_GetTrain(handle, &currentPid, (char **)&currentPage, LOT_LEAF_BUF);
		if (e < 0) ERR(handle, e);
	    }
	    /* if currentPage is allocated new page, use BfM_GetNewTrain() */
	    else {
		e = BfM_GetNewTrain(handle, &currentPid, (char **)&currentPage, LOT_LEAF_BUF);
		if (e < 0) ERR(handle, e);
	    }

        /* set the page id */
	    currentPage->hdr.pid = currentPid;

            /* set page type */
            SET_PAGE_TYPE(currentPage, LOT_L_NODE_TYPE);

            /* set temporary flag */
            if( isTmp ) SET_TEMP_PAGE_FLAG(currentPage);
            else        RESET_TEMP_PAGE_FLAG(currentPage);
            
	    /* get # of bytes for the current page to hold */
	    nBytes = (i < nTrains - nPartialTrains) ?
		LOT_LNODE_MAXFREE : (bytesPerTrain + ((remain-- > 0) ? 1:0));
	    
	    if (origBytes > 0) { /* use the original data */
		
		if (i == 0) {
		    if (nBytes <= list->entry[0].count) {
			/* move others to the next train */
			tmpData1Bytes = list->entry[0].count - nBytes;
			memcpy(tmpData1, &currentPage->data[nBytes], tmpData1Bytes);
			
			Dirty = FALSE;
		    } else if (nBytes > list->entry[0].count && nOrigTrains == 2) {
			/* borrow from the next page */
			/* Notice that it is sufficient to borrow from the next page */
			Four borrow = nBytes - list->entry[0].count;
			
			memcpy(&currentPage->data[list->entry[0].count], tmpData2, borrow);
			tmpData2Ptr = tmpData2 + borrow;
			tmpData2Bytes -= borrow;
		    }
		} else {
		    currentDataPtr = currentPage->data;
		    
		    if (tmpData1Bytes != 0) {
			memcpy(currentDataPtr, tmpData1, tmpData1Bytes);
			currentDataPtr += tmpData1Bytes;
			tmpData1Bytes = 0;
		    }
		    
		    if (tmpData2Bytes != 0) {
			Four bytes = MIN(nBytes - (currentDataPtr - currentPage->data), tmpData2Bytes);
			
			memcpy(currentDataPtr, tmpData2Ptr, bytes);
			tmpData2Ptr += bytes;
			tmpData2Bytes -= bytes;
		    }
		}
		
		origBytes -= nBytes; /* decrement the origBytes */
		if (origBytes < 0) {
		    /* attach the new data to the end */
		    memcpy(&currentPage->data[nBytes+origBytes], newData, -origBytes);
		    newData += -origBytes;
		}
		
	    } else {		/* use the new Data */
		memcpy(currentPage->data, newData, nBytes);
		newData += nBytes;
		
	    }
	    
	    if (Dirty)
		BfM_SetDirty(handle, &currentPid, LOT_LEAF_BUF);
	    if (e < 0) ERRB1(handle, e, &currentPid, LOT_LEAF_BUF);
	    
	    e = BfM_FreeTrain(handle, &currentPid, LOT_LEAF_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    /*@ construct the L_O_T_ItemList */
	    list->entry[i].spid = newPids[i].pageNo;
	    list->entry[i].count = nBytes;
	}
	
	*overflow = TRUE;

	/*@ construct the L_O_T_ItemList */
	/* nReplaces was already set at caller routine */
	list->nEntries = nTrains;
    }
    
    return(eNOERROR);

} /* lot_AppendToDataPage() */
