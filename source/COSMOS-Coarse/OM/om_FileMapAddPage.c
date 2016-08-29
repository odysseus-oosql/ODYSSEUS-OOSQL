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
 * Module : om_FileMapAddPage.c
 *
 * Description : 
 *  Add the new page into the double linked list of data pages.
 *
 * Exports:
 *  Four om_FileMapAddPage(ObjectID*, PageID*, PageID*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * om_FileMapAddPage()
 *================================*/
/* 
 * Function: Four om_FileMapAddPage(ObjectID*, PageID*, PageID*)
 *
 * Description : 
 *  Add the new page into the double linked list of data pages.
 *  If prevPID is NULL, append the new page after the last page.
 *  If not NULL, insert after the prevPID page.
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_OM
 *    eBADPAGEID_OM
 *    some errors caused by function calls
 *
 * Side effect:
 *  If newPID is appended after the last page,
 *  catalog entry's lastPage field is updated with the new page.
 */
Four om_FileMapAddPage(
    Four handle,
    ObjectID                    *catObjForFile,         /* IN Informations about a file */
    PageID                      *prevPID,               /* IN after which newPID is added */
    PageID                      *newPID)                /* IN new PageID to be added */
{
    Four                        e;                      /* error */
    PageID                      lastPID;                /* PageID of last page of the file */
    PageID                      nextPID;                /* PageID of next(relative to the new page) page */
    SlottedPage                 *prevPage;              /* pointer to a previous(relative to new page) page */
    SlottedPage                 *newPage;               /* pointer to a new data page */
    SlottedPage                 *nextPage;              /* pointer to a next(relative to new page) page */
    sm_CatOverlayForData        *catEntry;              /* pointer to data file catalog information */
    SlottedPage                 *catPage;               /* pointer to buffer containing the catalog */
    Four                        volNo;                  /* volume in which the file is placed */


    TR_PRINT(TR_OM, TR1,
             ("om_FileMapAddPage(handle, catObjForFile=%P, prevPID=%P, newPID=%P)",
	      catObjForFile, prevPID, newPID));

    /*@ check parameters */
    
    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);

    if (newPID == NULL || newPID->pageNo == NIL) ERR(handle, eBADPAGEID_OM);

    /*@ Get the volume number */
    volNo = newPID->volNo;
    
    /*@
     * Change the previous page
     */
    if (prevPID == NULL) {	/* append to the last page */
	/* Get the buffer for catalog object access. */
	e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

	/* use the last page */
	MAKE_PAGEID(lastPID, volNo, catEntry->lastPage);

	e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* read the last page into the buffer */
	e = BfM_GetTrain(handle, &lastPID, (char**)&prevPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* change last page's nextPage into the new page : originally NIL*/
	MAKE_PAGEID(nextPID, volNo, prevPage->header.nextPage);
	prevPage->header.nextPage = newPID->pageNo;
	
	e = BfM_SetDirty(handle, &lastPID, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &lastPID, PAGE_BUF);
	e = BfM_FreeTrain(handle, &lastPID, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
    } else {	/* insert after prevPID */
	/* read the previous page */
	e = BfM_GetTrain(handle, prevPID, (char **)&prevPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* change previous page's nextPage into the new page */
	MAKE_PAGEID(nextPID, volNo, prevPage->header.nextPage);
	prevPage->header.nextPage = newPID->pageNo;
	
	e = BfM_SetDirty(handle, prevPID, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, prevPID, PAGE_BUF);
	e = BfM_FreeTrain(handle, prevPID, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    /*@
     * Change the new page
     */
    e = BfM_GetTrain(handle, newPID, (char**)&newPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* insert the new page into the double linked list of file map */
    newPage->header.nextPage = nextPID.pageNo;
    if (prevPID == NULL)
	newPage->header.prevPage = lastPID.pageNo;
    else
	newPage->header.prevPage = prevPID->pageNo;
    
    e = BfM_SetDirty(handle, newPID, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, newPID, PAGE_BUF);
    e = BfM_FreeTrain(handle, newPID, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /*@
     * Change the next page
     */
    if (nextPID.pageNo == NIL) {
	/* The previous page was the last page of the file */

	e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);
	
	catEntry->lastPage = newPID->pageNo;

	e = BfM_SetDirty(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

	e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
    } else {
	e = BfM_GetTrain(handle, &nextPID, (char**)&nextPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* insert the new page into the double linked list of file map */
	nextPage->header.prevPage = newPID->pageNo;
	
	e = BfM_SetDirty(handle, &nextPID, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &nextPID, PAGE_BUF);
	e = BfM_FreeTrain(handle, &nextPID, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);
    
} /* om_FileMapAddPage() */
