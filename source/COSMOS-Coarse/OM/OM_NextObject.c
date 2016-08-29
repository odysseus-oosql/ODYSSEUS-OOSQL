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
 * Module: OM_NextObject.c
 *
 * Description:
 *  Return the next Object of the given Current Object. 
 *
 * Export:
 *  Four OM_NextObject(ObjectID*, ObjectID*, ObjectID*, ObjectHdr*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*@================================
 * OM_NextObject()
 *================================*/
/*
 * Function: Four OM_NextObject(ObjectID*, ObjectID*, ObjectID*, ObjectHdr*)
 *
 * Description:
 *  Return the next Object of the given Current Object.  Find the Object in the
 *  same page which has the current Object and  if there  is no next Object in
 *  the same page, find it from the next page. If the Current Object is NULL,
 *  return the first Object of the file.
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_OM
 *    eBADOBJECTID_OM
 *    eBADFILEID_OM
 *    some errors caused by function calls
 *
 * Side effect:
 *  1) parameter nextOID
 *     nextOID is filled with the next object's identifier
 *  2) parameter objHdr
 *     objHdr is filled with the next object's header
 */
Four OM_NextObject(
    Four handle,
    ObjectID  *catObjForFile,	/* IN informations about a data file */
    ObjectID  *curOID,			/* IN a ObjectID of the current Object */
    ObjectID  *nextOID,			/* OUT the next Object of a current Object */
    ObjectHdr *objHdr)			/* OUT the object header of next object */
{
    Four 					e;					/* error */
    Two  					i;					/* index */
    Four 					offset;				/* starting offset of object within a page */
    PageID 					pid;				/* a page identifier */
    PageNo 					pageNo;				/* a temporary var for next page's PageNo */
    SlottedPage 			*apage;				/* a pointer to the data page */
    Object 					*obj;				/* a pointer to the Object */
    PhysicalFileID 			pFid;				/* file in which the objects are located */ 
    SlottedPage 			*catPage;			/* buffer page containing the catalog object */
    sm_CatOverlayForData 	*catEntry; 			/* data structure for catalog object access */


    TR_PRINT(TR_OM, TR1,
             ("OM_NextObject(handle, catObjForFile=%P, curOID=%P, nextOID=%P)",
	      catObjForFile, curOID, nextOID));

    /*@
     * parameter checking
     */
    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);
    
    if (nextOID == NULL) ERR(handle, eBADOBJECTID_OM);

    /* Get the FileID from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    MAKE_PHYSICALFILEID(pFid, catEntry->fid.volNo, catEntry->firstPage);

    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ Get next object. */
    if (curOID == NULL) {
	/* use the first page: 'pFid' is equal to the first page's PageID */
	pid = *((PageID *)&pFid);
	
    } else {			/* curOID != NULL */
	/* At first, look at the curOID page.
	 * If no following Object, see the next page.
	 */
        /*@ Read the page into the buffer page */
        e = BfM_GetTrain(handle, (PageID *)curOID, (char **)&apage, PAGE_BUF);
        if( e < 0 )  ERR(handle,  e );

	/* Find the Object within the page which has the current Object */
	for( i = curOID->slotNo + 1; i < apage->header.nSlots; i++ ) {
	    offset = apage->slot[-i].offset;
	    
	    if( offset != EMPTYSLOT )  {  /* if a Object is found, return it */
		obj = (Object *)&(apage->data[offset]);
		
		if(!(obj->header.properties & P_FORWARDED)) {
		    MAKE_OBJECTID(*nextOID, curOID->volNo, curOID->pageNo,
				  i, apage->slot[-i].unique);

		    /* if objHdr is not NULL, return the object header */
		    if (objHdr != NULL) *objHdr = obj->header;
		    
		    e = BfM_FreeTrain(handle, (PageID *)curOID, PAGE_BUF);
		    if( e < 0 )  ERR(handle,  e );
		    return( eNOERROR );
		}
	    }
	}
	
	/* If there is no next Object within the page, find it from the next pages */
	MAKE_PAGEID(pid, curOID->volNo, apage->header.nextPage);
	e = BfM_FreeTrain(handle, (PageID *)curOID, PAGE_BUF);
	if( e < 0 )  ERR(handle,  e );
    }
    
    while (pid.pageNo != NIL) {
	e = BfM_GetTrain(handle, &pid, (char **)&apage, PAGE_BUF);
	if( e < 0 )  ERR(handle,  e );
	
	for( i = 0; i < apage->header.nSlots; i++ )  {
	    offset = apage->slot[-i].offset;
	    
	    if( offset != EMPTYSLOT )  {  /* if a Object is found, return it */
		obj = (Object *)&(apage->data[offset]);
		
		if(!(obj->header.properties & P_FORWARDED)) {
		    MAKE_OBJECTID(*nextOID, pid.volNo, pid.pageNo,
				  i, apage->slot[-i].unique);

		    /* if objHdr is not NULL, return the object header */
		    if (objHdr != NULL) *objHdr = obj->header;

		    e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
		    if( e < 0 ) ERR(handle,  e );
		    return( eNOERROR );
		}
	    }
	}

	/*@ save the nextPage's PageNo & free the current page */
	pageNo = apage->header.nextPage;
	e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
	if( e < 0 ) ERR(handle,  e );
	
	MAKE_PAGEID(pid, pFid.volNo, pageNo);
    }
    
    return(EOS);		/* end of scan */
    
} /* OM_NextObject() */
