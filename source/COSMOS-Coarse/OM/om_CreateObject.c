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
 * Module : om_CreateObject.c
 * 
 * Description :
 *  om_CreateObject() creates a new object near the specified object.
 *
 * Exports:
 *  Four om_CreateObject(ObjectID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"			/* for tracing : TR_PRINT() macro */
#include "RDsM_Internal.h"	/* for the raw disk manager call */ 
#include "BfM.h"			/* for the buffer manager call */
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * om_CreateObject()
 *================================*/
/*
 * Function: Four om_CreateObject(ObjectID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*)
 * 
 * Description :
 *  om_CreateObject() creates a new object near the specified object; the near
 *  page is the page holding the near object.
 *  If there is no room in the near page and the near object 'nearObj' is not
 *  NULL, a new page is allocated for object creation (In this case, the newly
 *  allocated page is inserted after the near page in the list of pages
 *  consiting in the file).
 *  If there is no room in the near page and the near object 'nearObj' is NULL,
 *  it trys to create a new object in the page in the available space list. If
 *  fail, then the new object will be put into the newly allocated page(In this
 *  case, the newly allocated page is appended at the tail of the list of pages
 *  cosisting in the file).
 *
 * Returns:
 *  error Code
 *    eBADCATALOGOBJECT_OM
 *    eBADOBJECTID_OM
 *    some errors caused by fuction calls
 */
Four om_CreateObject(
    Four handle,
    ObjectID	*catObjForFile,	/* IN file in which object is to be placed */
    ObjectID 	*nearObj,		/* IN create the new object near this object */
    ObjectHdr	*objHdr,		/* IN from which tag & properties are set */
    Four		length,			/* IN amount of data */
    char		*data,			/* IN the initial data for the object */
    ObjectID	*oid)			/* OUT the object's ObjectID */
{
    Four        	e;				/* error number */
    Four			neededSpace;	/* space needed to put new object [+ header] */
    SlottedPage 	*apage;			/* pointer to the slotted page buffer */
    Four        	alignedLen;		/* aligned length of initial data */
    Boolean     	needToAllocPage;/* Is there a need to alloc a new page? */
    PageID      	pid;            /* PageID in which new object to be inserted */
    PageID      	nearPid;        
    Four        	firstExt;		/* first Extent No of the file */
    Object      	*obj;			/* point to the newly created object */
    Two         	i;				/* index variable */
    sm_CatOverlayForData *catEntry; /* pointer to data file catalog information */
    SlottedPage 	*catPage;		/* pointer to buffer containing the catalog */
    FileID      	fid;			/* ID of file where the new object is placed */
    Two         	eff;			/* extent fill factor of file */
    Boolean     	isTmp;          
    PhysicalFileID 	pFid;        	
    
    
    TR_PRINT(TR_OM, TR1,
             ("om_CreateObject(handle, catObjForFile=%P, nearObj=%P, objHdr=%P, length=%ld, data=%P, oid=%P)",
	      catObjForFile, nearObj, objHdr, length, data, oid));

    /*@ parameter checking */
    
    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);

    if (objHdr == NULL) ERR(handle, eBADOBJECTID_OM);
    
    /*
     * calculate the length to be needed in the slotted page.
     * If need to create the large object, the slotted page only contains
     * object header.
     */
    alignedLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(length));
    neededSpace = sizeof(ObjectHdr) + alignedLen + sizeof(SlottedPageSlot);

    /*@ Get the file's ID and its extent fill factor. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    eff = catEntry->eff;
    MAKE_PHYSICALFILEID(pFid, catEntry->fid.volNo, catEntry->firstPage); 

    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ at first, look up the near page */    
    if (nearObj != NULL) {
        
	/* get the near page's PageID from nearObj */
	pid = *((PageID *)nearObj);
        
    } else {
	/*
	 * If the object length is less than or equal to 50% of PAGESIZE
	 * use one in the availSpaceLists.
	 */

	e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

	if((neededSpace <= SP_10SIZE) && (catEntry->availSpaceList10 >= 0))
	    MAKE_PAGEID(pid, catEntry->fid.volNo, catEntry->availSpaceList10);
	
	else if((neededSpace <= SP_20SIZE) && (catEntry->availSpaceList20 >= 0)) 
	    MAKE_PAGEID(pid, catEntry->fid.volNo, catEntry->availSpaceList20);
	
	else if((neededSpace <= SP_30SIZE) && (catEntry->availSpaceList30 >= 0)) 
	    MAKE_PAGEID(pid, catEntry->fid.volNo, catEntry->availSpaceList30);
	
	else if((neededSpace <= SP_40SIZE) && (catEntry->availSpaceList40 >= 0)) 
	    MAKE_PAGEID(pid, catEntry->fid.volNo, catEntry->availSpaceList40);
	
	else if((neededSpace <= SP_50SIZE) && (catEntry->availSpaceList50 >= 0)) 
	    MAKE_PAGEID(pid, catEntry->fid.volNo, catEntry->availSpaceList50);
        else /* try the last page */
            MAKE_PAGEID(pid, catEntry->fid.volNo, catEntry->lastPage);
        
	e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
        
    /* read the page into the buffer */
    e = BfM_GetTrain(handle, &pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

#ifndef NDEBUG
    if (nearObj != NULL) {
        /* check the nearPage is included in the file */
        if (!EQUAL_FILEID(fid, apage->header.fid))
            ERRB1(handle, eBADOBJECTID_OM, &pid, PAGE_BUF);

	/* check if nearObj is the valid ObjectID */
        if (!IS_VALID_OBJECTID(nearObj, apage))
            ERRB1(handle, eBADOBJECTID_OM, &pid, PAGE_BUF);
    }
#endif /* NDEBUG */

    /*@ initially we set needToAllocPage to FALSE */
    needToAllocPage = FALSE;
    
    /* If the page has no enough room, then use a new page */
    if (SP_FREE(apage) < neededSpace) {

        e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
        if (e < 0) ERR(handle, e);
	    
        needToAllocPage = TRUE;
	    
    } else {

        e = om_RemoveFromAvailSpaceList(handle, catObjForFile, &pid, apage);
        if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);	    
    }
    
    /*
     * If there is no enough room in the near page or available space list
     * request a new page.
     */
    if (needToAllocPage) {
	/* we rallocate the new page and return the page */
	
	/* get the first extent number */
	e = RDsM_PageIdToExtNo(handle, (PageID *)&pFid, &firstExt); 
	if (e < 0) ERR(handle, e);
	
	/* alloc a page */
        if (nearObj != NULL) {
            nearPid = *((PageID *) nearObj);
        }
        else {
            MAKE_PAGEID(nearPid, catEntry->fid.volNo, catEntry->lastPage);
        }

	e = RDsM_AllocTrains(handle, fid.volNo, firstExt, &nearPid, eff, 1, PAGESIZE2, &pid);
	if (e < 0) ERR(handle, e);
	
	/*@ Initialize the page */
	e = BfM_GetNewTrain(handle, &pid, (char **)&apage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* Initialize header */
	apage->header.fid = fid;
	apage->header.nSlots = 1;
	apage->header.free = 0;
	apage->header.unused = 0;
	apage->header.prevPage = NIL;
	apage->header.nextPage = NIL;
	apage->header.spaceListPrev = NIL;
	apage->header.spaceListNext = NIL;
	apage->header.unique = 0;
	apage->header.uniqueLimit = 0;
    apage->header.pid = pid; 
	
        /* check this file is temporary */
        e = om_IsTemporary(handle, &fid, &isTmp);
        if (e < 0) ERR(handle, e);

        /* set page type */
        SET_PAGE_TYPE(apage, SLOTTED_PAGE_TYPE);

        /* set temporary flag */
        if( isTmp ) SET_TEMP_PAGE_FLAG(apage);
        else        RESET_TEMP_PAGE_FLAG(apage);

	/* initialize */
	apage->slot[0].offset = EMPTYSLOT;

	e = om_FileMapAddPage(handle, catObjForFile, (PageID *)nearObj, &pid);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	
    } else {
	if (SP_CFREE(apage) < neededSpace) {
	    /* make space for object insert */
	    
	    e = OM_CompactPage(handle, apage, NIL);
	    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	}
    }
    
    /*
     * At this point
     * pid : PageID of the page into which the new object will be placed
     * apage : pointer to the slotted page buffer
     * alignedLen : space for data of the new object
     */
    /* where to put the object[header]? */
    obj = (Object *)&(apage->data[apage->header.free]);

    /* set the object header */
    obj->header = *objHdr;
    
    /* copy the data into the object */
    memcpy(obj->data, data, length);
    obj->header.length = length;
    
    /* find the slot index and update the slot contents */
    for(i = 0; i < apage->header.nSlots; i++)
	if (apage->slot[-i].offset == EMPTYSLOT) break;
    
    /* if no empty slot, then create the new slot */
    if (i == apage->header.nSlots)
	apage->header.nSlots++;	/* increment # of slots */
    
    apage->slot[-i].offset = apage->header.free;
    e = om_GetUnique(handle, &pid, &(apage->slot[-i].unique));
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

    /* update the pointer to the start of contiguous free space */
    apage->header.free += sizeof(ObjectHdr) + alignedLen;

    /* Construct the ObjectID to be returned */
    if(oid != NULL)
	MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, i, apage->slot[-i].unique);

    /* Put the page into the proper list */
    e = om_PutInAvailSpaceList(handle, catObjForFile, &pid, apage);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
    
    e = BfM_SetDirty(handle, &pid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

    /*@ free the buffer page */
    e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
} /* om_CreateObject() */
