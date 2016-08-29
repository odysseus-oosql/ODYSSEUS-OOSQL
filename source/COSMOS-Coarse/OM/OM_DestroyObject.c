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
 * Module : OM_DestroyObject.c
 * 
 * Description : 
 *  OM_DestroyObject() destroys the specified object.
 *
 * Exports:
 *  Four OM_DestroyObject(ObjectID*, ObjectID*, Pool*, DeallocListElem*)
 */

#include "common.h"
#include "trace.h"		/* for tracing : TR_PRINT() macro */
#include "Util.h"		/* to get Pool */
#include "RDsM_Internal.h"	
#include "BfM.h"		/* for the buffer manager call */
#include "LOT.h"		/* for the large object manager call */
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*@================================
 * OM_DestroyObject()
 *================================*/
/*
 * Function: Four OM_DestroyObject(ObjectID*, ObjectID*, Pool*, DeallocListElem*)
 * 
 * Description : 
 *  (1) What to do?
 *  OM_DestroyObject() destroys the specified object. The specified object
 *  will be removed from the slotted page. The freed space is not merged
 *  to make the contiguous space; it is done when it is needed.
 *  The page's membership to 'availSpaceList' may be changed.
 *  If the destroyed object is the only object in the page, then deallocate
 *  the page.
 *
 *  (2) How to do?
 *  a. Read in the slotted page
 *  b. Remove this page from the 'availSpaceList'
 *  c. Delete the object from the page
 *  d. Update the control information: 'unused', 'freeStart', 'slot offset'
 *  e. IF no more object in this page THEN
 *	   Remove this page from the filemap List
 *	   Dealloate this page
 *    ELSE
 *	   Put this page into the proper 'availSpaceList'
 *    ENDIF
 * f. Return
 *
 * Returns:
 *  error code
 *    eBADFILEID_OM
 *    some errors caused by function calls
 */
Four OM_DestroyObject(
    Four handle,
    ObjectID 			*catObjForFile,	/* IN file containing the object */
    ObjectID 			*oid,			/* IN object to destroy */
    Pool     			*dlPool,		/* INOUT pool of dealloc list elements */
    DeallocListElem 	*dlHead)		/* INOUT head of dealloc list */
{
    Four        			e;			/* error number */
    Two         			i;			/* temporary variable */
    FileID      			fid;		/* ID of file where the object was placed */
    PageID					pid;		/* page on which the object resides */
    SlottedPage 			*apage;		/* pointer to the buffer holding the page */
    Four        			offset;		/* start offset of object in data area */
    Object      			*obj;		/* points to the object in data area */
    Four        			alignedLen;	/* aligned length of object */
    Boolean     			last;		/* indicates the object is the last one */
    SlottedPage 			*catPage;	/* buffer page containing the catalog object */
    sm_CatOverlayForData 	*catEntry; 	/* overlay structure for catalog object access */
    DeallocListElem 		*dlElem;	/* pointer to element of dealloc list */
    PhysicalFileID 			pFid	;	/* physical ID of file */ 
    
    
    TR_PRINT(TR_OM, TR1,
             ("OM_DestroyObject(handle, catObjForFile=%P, oid=%P, dlPool=%P, dlHead=%P)",
	      catObjForFile, oid, dlPool, dlHead));

    /*@ Check parameters. */
    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);

    if (oid == NULL) ERR(handle, eBADOBJECTID_OM);

    /* Get the data file's ID from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    MAKE_PHYSICALFILEID(pFid, fid.volNo, catEntry->firstPage); 
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
      
    /*@ get the PageID of page on which the object resides */
    pid = *((PageID *)oid);

    /*@ Read the slotted page into the buffer */
    e = BfM_GetTrain(handle, &pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* check the given file contains the 'oid' object */
    if (!EQUAL_FILEID(fid, apage->header.fid))
	ERRB1(handle, eBADFILEID_OM, &pid, PAGE_BUF);

    /* check that 'oid' is valid */
    if (!IS_VALID_OBJECTID(oid, apage))
	ERRB1(handle, eBADOBJECTID_OM, &pid, PAGE_BUF);

    /* If the page is in the availSpaceList, remove it from the list */
    e = om_RemoveFromAvailSpaceList(handle, catObjForFile, &pid, apage);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	
    /*@ obj points to the object */
    offset = apage->slot[-(oid->slotNo)].offset;
    obj = (Object *)&(apage->data[offset]);

    /* aligned length is the needed space for align requirement */
    alignedLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(obj->header.length));
    
    /* Is the object the last one in this page? */
    if (offset+sizeof(ObjectHdr)+alignedLen == apage->header.free)
	last = TRUE;
    else
	last = FALSE;
    
    if (obj->header.properties & P_MOVED) {
	/*@ This object is the moved object */

	/* recursively call the OM_DestroyObject */
	e = OM_DestroyObject(handle, catObjForFile, (ObjectID *)(obj->data), dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	/* free the space alocated */
	if (last)
	    apage->header.free -= sizeof(ObjectHdr) + sizeof(ObjectID);
	else
	    apage->header.unused += sizeof(ObjectHdr) + sizeof(ObjectID);

    } else {
	/*@ Normal Object */

	if (obj->header.properties & P_LRGOBJ) {
	    e = LOT_DestroyObject(handle, &pid, oid->slotNo, dlPool, dlHead);
	    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	    
	} else {
	    
	    /* free the allocated space */
	    if (last)
		apage->header.free -= sizeof(ObjectHdr) + alignedLen;
	    else
		apage->header.unused += sizeof(ObjectHdr) + alignedLen;
	}
    }

     /* set the slot to EMPTYSLOT */
    apage->slot[-(oid->slotNo)].offset = EMPTYSLOT;

    /* delete the trailing empty slots */
    for (i = apage->header.nSlots-1; i >=0 && apage->slot[-i].offset==EMPTYSLOT; i--) {
	apage->header.nSlots--;
    }

    /* There is no more tuple in this page. Deallocate it. */
    if ((apage->header.free == apage->header.unused) && !EQUAL_PAGEID(pid, pFid)) {  
	
	/* Remove this page from the filemap list */
	e = om_FileMapDeletePage(handle, catObjForFile, &pid);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	/*
	 * Insert the deallocated page into the dealloc list.
	 */
	e = Util_getElementFromPool(handle, dlPool, &dlElem);
	if (e < 0) ERR(handle, e);

        dlElem->type = DL_PAGE;
	dlElem->elem.pid = pid;			/* save the page identifier */
	dlElem->next = dlHead->next; 	/* insert to the list */
	dlHead->next = dlElem;       	/* new first element of the list */
		
    } else {
	/* Insert this page into the proper 'availSpaceList' */
	e = om_PutInAvailSpaceList(handle, catObjForFile, &pid, apage);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	e = BfM_SetDirty(handle, &pid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);
    
} /* OM_DestroyObject() */
