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
 * Module : om_FileMapDeletePage.c
 *
 * Description : 
 *  Delete the given page from the double linked list of data pages.
 *
 * Exports:
 *  Four om_FileMapDeletePage(ObjectID*, PageID*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * om_FileMapDeletePage()
 *================================*/
/*
 * Function: Four om_FileMapDeletePage(ObjectID*, PageID*)
 *
 * Description:
 *  Delete the given page from the double linked list of data pages.
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_OM
 *    eBADPAGEID_OM
 *    some errors caused by function calls
 *
 * Side effect:
 *  If the given page is the first page, then ignore and return immediately.
 *  If the given page is the last page, catalog entry's lastPage field is
 *  updated to new last page.
 */
Four om_FileMapDeletePage(
    Four handle,
    ObjectID *catObjForFile,	/* IN Informations about a file */
    PageID   *delPID)			/* IN PageID to be deleted */
{
    Four 					e;			/* error */
    FileID 					fid;		/* ID of a file which has the deleted page */
    PageID 					prevPID;	/* PageID of previous(relative to new page) page */
    PageID 					nextPID;	/* PageID of next(relative to deleted page) page */
    SlottedPage 			*prevPage;	/* point to a previous(relative to deleted page) page */
    SlottedPage 			*delPage;	/* point to a new data page */
    SlottedPage 			*nextPage;	/* point to a next(relative to the deleted page) page */
    sm_CatOverlayForData 	*catEntry;	/* pointer to data file catalog information */
    SlottedPage 			*catPage;	/* pointer to buffer containing the catalog */
    PhysicalFileID 			pFid;		/* physical ID of file */ 
        

    TR_PRINT(TR_OM, TR1,
             ("om_FileMapDeletePage(handle,  catObjForFile=%P, delPID=%P )",
	      catObjForFile, delPID));

    /*@ check parameters */

    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);
    
    if (delPID == NULL || delPID->pageNo == NIL) ERR(handle, eBADPAGEID_OM);
    
    /* Get the pointer to the catalog entry for the data file. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);    

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    /* Get data file's ID. */
    fid = catEntry->fid;
    MAKE_PHYSICALFILEID(pFid, fid.volNo, catEntry->firstPage); 

    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* if the first page, ignore this and return successfully */
    if (EQUAL_PAGEID(*delPID, pFid)) return(eNOERROR); 
	    
    /*@
     * Change the new page
     */
    e = BfM_GetTrain(handle, delPID, (char**)&delPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* get the previous page & next page */
    MAKE_PAGEID(prevPID, delPID->volNo, delPage->header.prevPage);
    MAKE_PAGEID(nextPID, delPID->volNo, delPage->header.nextPage);
    
    e = BfM_FreeTrain(handle, delPID, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /*@
     * Change the previous page
     */
    /* Notice : delPID is not the first page. The prevPID should exist. */
    e = BfM_GetTrain(handle, &prevPID, (char**)&prevPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* change previous page's nextPage into the new page */
    prevPage->header.nextPage = nextPID.pageNo;
    
    e = BfM_SetDirty(handle, &prevPID, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &prevPID, PAGE_BUF);
    e = BfM_FreeTrain(handle, &prevPID, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /*@
     * Change the next page
     */
    if (nextPID.pageNo == NIL) { /* The deleted page was the last page of the file */
	e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);
	
	catEntry->lastPage = prevPID.pageNo;

	e = BfM_SetDirty(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

	e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERR(handle, e);
   
    } else {
	e = BfM_GetTrain(handle, &nextPID, (char**)&nextPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* insert the new page into the double linked list of file map */
	
	nextPage->header.prevPage = prevPID.pageNo;
	
	e = BfM_SetDirty(handle, &nextPID, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &nextPID, PAGE_BUF);
	e = BfM_FreeTrain(handle, &nextPID, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    return(eNOERROR);
    
} /* om_FileMapDeletePage() */
