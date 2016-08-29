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
 * Module : om_AvailSpaceList.c
 * 
 * Description : 
 *  Two functions OM_PutAvailSpaceList(), OM_RemoveAvailSpaceList() in this
 *  module manage the 'availSpaceList'. 'availSpaceList' is the doubly linked
 *  list of the pages having the available space.
 *  'availSpaceList' is not one list but 5 lists: availSpaceList10,
 *  availSpaceList20, availSpaceList30, availSpaceList40, and availSpaceList50.
 *  What list a given page is inserted in is determined by the ratio of
 *  remained space in the page to the page size.
 *  'availSpaceList10' : pages s.t. >= 10% and < 20%
 *  'availSpaceList20' : pages s.t. >= 20% and < 30%
 *  'availSpaceList30' : pages s.t. >= 30% and < 40%
 *  'availSpaceList40' : pages s.t. >= 40% and < 50%
 *  'availSpaceList50' : pages s.t. > 50%
 *  No list has the pages having less than 10% of free space.
 *
 * Exports:
 *  Four om_PutInAvailSpaceList(ObjectID*, PageID*, SlottedPage*)
 *  Four om_RemoveFromAvailSpaceList(ObjectID*, PageID*, SlottedPage*)
 */

#include "common.h"
#include "trace.h"		/* for tracing : TR_PRINT() macro */
#include "BfM.h"		/* for the buffer manager call */
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@================================
 * om_PutInAvailSpaceList()
 *================================*/
/*
 * Function: Four om_PutInAvailSpaceList(ObjectID*, PageID*, SlottedPage*)
 *
 * Description:
 *  Put a slotted page into the corresponding available space list.
 *
 * Returns:
 *  error code
 *     some errors caused by function calls
 */
Four om_PutInAvailSpaceList(
    Four handle,
    ObjectID	*catObjForFile,	/* IN file containing the page */
    PageID		*pid,			/* IN page to put into the list */
    SlottedPage	*apage)			/* INOUT pointer to buffer holding the page */
{
    Four       				e;			/* error number */
    Two 					remain;		/* The percentage of the free space */
    PageID					nextPID;	/* pageID of the next page */
    SlottedPage 			*npage;		/* pointer to buffer holding the next page */
    sm_CatOverlayForData 	*catEntry; 	/* pointer to data file catalog information */
    SlottedPage 			*catPage;	/* pointer to buffer containing the catalog */
    
    
    TR_PRINT(TR_OM, TR1,
             ("OM_PutInAvailSpaceList(catObjForFile=%P, pid=%P, apage=%P",
	      catObjForFile, pid, apage));

    /*
     * 'remain' represents the percentage of the free space in 'apage'
     * slotted page. Its value is flored at first digit location.
     * So the possible value is 0, 10, 20, ..., 100.
     */
    remain = 10 * ((SP_FREE(apage)*10)/(PAGESIZE-SP_FIXED));

    /* The remain space is less than 10% of the data page size */
    if (remain < 10) return(eNOERROR);

    /* Get the page containing the catalog object for the data file. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);
    
    switch(remain) {
      case 10:			/* 10% <= remain_space < 20% */
	apage->header.spaceListNext = catEntry->availSpaceList10;
	catEntry->availSpaceList10 = pid->pageNo;
	break;
	
      case 20:			/* 20% <= remain_space < 30% */
	apage->header.spaceListNext = catEntry->availSpaceList20;
	catEntry->availSpaceList20 = pid->pageNo;
	break;
	
      case 30:			/* 30% <= remain_space < 40% */
	apage->header.spaceListNext = catEntry->availSpaceList30;
	catEntry->availSpaceList30 = pid->pageNo;
	break;
	
      case 40:			/* 40% <= remain_space < 50% */
	apage->header.spaceListNext = catEntry->availSpaceList40;
	catEntry->availSpaceList40 = pid->pageNo;
	break;
	
      default:			/* 50% <= remain_space */
	apage->header.spaceListNext = catEntry->availSpaceList50;
	catEntry->availSpaceList50 = pid->pageNo;
	break;
    }
    apage->header.spaceListPrev = NIL;	/* It is the first page of the list */

    /* catalog object was modified. */
    e = BfM_SetDirty(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

    /*@ Free the page. */
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    e = BfM_SetDirty(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ set the previous pointer of the next page */
    if (apage->header.spaceListNext != NIL) {
	MAKE_PAGEID(nextPID, catEntry->fid.volNo, apage->header.spaceListNext);
	
	e = BfM_GetTrain(handle,  &nextPID, (char **)&npage, PAGE_BUF );
	if( e < 0 ) ERR(handle, e);
	
	npage->header.spaceListPrev = pid->pageNo;
	
	e = BfM_SetDirty(handle,  &nextPID, PAGE_BUF );
	if( e < 0 ) ERRB1(handle, e, &nextPID, PAGE_BUF);
	    
	e = BfM_FreeTrain(handle, &nextPID, PAGE_BUF);
	if( e < 0 ) ERR(handle, e);
    }
    
    return(eNOERROR);
    
} /* om_PutInAvailSpaceList() */



/*@================================
 * om_RemoveFromAvailSpaceList()
 *================================*/
/*
 * Function: Four om_RemoveFromAvailSpaceList(ObjectID*, PageID*, SlottedPage*)
 *
 * Description:
 *  Delete a slotted page from the available space list.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four om_RemoveFromAvailSpaceList(
    Four handle,
    ObjectID	*catObjForFile,	/* IN file containing the given page */
    PageID		*pid,			/* IN PageID to delete from the list */
    SlottedPage	*apage)			/* INOUT pointer to buffer holding the page */
{
    Four        e;			/* error number */
    PageID		prevPID;	/* PageID of previous page in availSpaceList */
    PageID		nextPID;	/* PageID of next page in availSpaceList */
    SlottedPage *prevPage;	/* pointer to buffer for 'prevPID' page */
    SlottedPage *nextPage;	/* pointer to buffer for 'nextPID' page */
    Two 		remain;		/* The percentage of the free space */
    SlottedPage *catPage;	/* buffer page containing the catalog object */
    sm_CatOverlayForData *catEntry; /* overary strucutre for catalog object access */

    
    TR_PRINT(TR_OM, TR1,
             ("OM_RemoveFromAvailSpaceList(catObjForFile=%P, pid=%P, apage=%P",
	      catObjForFile, pid, apage));

    /*
     * 'remain' represents the percentage of the free space in 'apage'
     * slotted page. Its value is flored at first digit location.
     * So the possible value is 0, 10, 20, ..., 100.
     */
    remain = 10 * (SP_FREE(apage)*10/(PAGESIZE-SP_FIXED));

    /* The remain space is less than 10% of the data page size */
    if (remain < 10) return(eNOERROR);

    /* If the page is already removed from avail space list, do nothing */
    if (apage->header.spaceListPrev == NIL && apage->header.spaceListNext == NIL) {

        Boolean alreadyRemoved = FALSE;

        /* Access the catalog object for the data file. */
        e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

        if (catEntry->availSpaceList10 != pid->pageNo && catEntry->availSpaceList20 != pid->pageNo &&
            catEntry->availSpaceList30 != pid->pageNo && catEntry->availSpaceList40 != pid->pageNo &&
            catEntry->availSpaceList50 != pid->pageNo)
            alreadyRemoved = TRUE;

        e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        if (alreadyRemoved == TRUE) return (eNOERROR);
    }

    /*@ Cut the previous link */
    if (apage->header.spaceListPrev == NIL) {

	/* Access the catalog object for the data file. */
	e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

	/* The page to be deleted is the first page of the list */
	switch(remain) {
	  case 10:			/* 10% <= remain_space < 20% */
	    catEntry->availSpaceList10 = apage->header.spaceListNext;
	    break;
	    
	  case 20:			/* 20% <= remain_space < 30% */
	    catEntry->availSpaceList20 = apage->header.spaceListNext;
	    break;
	    
	  case 30:			/* 30% <= remain_space < 40% */
	    catEntry->availSpaceList30 = apage->header.spaceListNext;
	    break;
	    
	  case 40:			/* 40% <= remain_space < 50% */
	    catEntry->availSpaceList40 = apage->header.spaceListNext;
	    break;
	    
	  default:			/* 50% <= remain_space */
	    catEntry->availSpaceList50 = apage->header.spaceListNext;
	    break;
	}

	e = BfM_SetDirty(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

	e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
    } else {
	prevPID.volNo = pid->volNo;
	prevPID.pageNo = apage->header.spaceListPrev;
	e = BfM_GetTrain(handle, &prevPID, (char**)&prevPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	prevPage->header.spaceListNext = apage->header.spaceListNext;
	e = BfM_SetDirty(handle, &prevPID, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &prevPID, PAGE_BUF);

	e = BfM_FreeTrain(handle, &prevPID, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    /* Cut the next link */
    if (apage->header.spaceListNext != NIL) {
	MAKE_PAGEID(nextPID, pid->volNo, apage->header.spaceListNext);
	
	e = BfM_GetTrain(handle, &nextPID, (char**)&nextPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	nextPage->header.spaceListPrev = apage->header.spaceListPrev;
	e = BfM_SetDirty(handle, &nextPID, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &nextPID, PAGE_BUF);

	e = BfM_FreeTrain(handle, &nextPID, PAGE_BUF);
	if (e < 0) ERR(handle, e);	
    }
    
    /*@ Reset the link of the given page */
    apage->header.spaceListPrev = apage->header.spaceListNext = NIL;
    e = BfM_SetDirty(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* om_RemoveFromAvailSpaceList() */
    
